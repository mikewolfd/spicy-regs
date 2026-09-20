"""Reusable extract → stage engine.

``stage_agencies`` is the generic fan-out shared by any agency-partitioned
pipeline: for every (agency, record type) it pumps a :class:`Reader` (built by a
caller-supplied factory) through an optional :class:`Transform` into a
:class:`StagingWriter`, running agencies in parallel. It knows nothing about
*where* records come from, how they are shaped, or how processed keys are
tracked — it just reports the rows staged per record type and the source keys it
consumed, leaving transform/manifest/dedup decisions to the caller.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from spicy_docs.sources.mirrulations import KeyOutcome

from loguru import logger

from spicy_regs.schemas import RecordType
from spicy_regs.sources import StagingWriter
from spicy_regs.transforms.base import Transform


class Reader(Protocol):
    """The stream and completion accounting a staging source supplies."""

    last_keys: list[str]
    failed_keys: list[str]

    def iter_records(self): ...


# Factories the caller provides, keyed by (agency,) record type: build a
# configured Reader (connection details, filters) and the Transform that shapes
# its raw records for staging.
ReaderFactory = Callable[[str, RecordType], Reader]
TransformFactory = Callable[[RecordType], Transform]


@dataclass
class StageResult:
    """Outcome of a staging pass.

    Only ``consumed_keys`` may be manifested. All unresolved outcomes retain
    their agency/type scope so the pipeline can resume them before new keys.
    """

    rows_by_type: dict[str, int]
    consumed_keys: set[str] = field(default_factory=set)
    failed_keys: set[str] = field(default_factory=set)
    parse_failed_keys: set[str] = field(default_factory=set)
    unresolved: dict[tuple[str, str], list["KeyOutcome"]] = field(default_factory=dict)


def stage_agencies(
    agencies: list[str],
    record_types: list[RecordType],
    staging_dir: Path,
    read: ReaderFactory,
    *,
    transform_for: TransformFactory | None = None,
    max_workers: int = 4,
) -> StageResult:
    """Stage every (agency, record type) in parallel; return rows + consumed keys.

    Each record stream flows Reader -> Transform -> StagingWriter. When
    ``transform_for`` is omitted the reader's records are staged as-is.
    """

    def stage_one_agency(agency: str):
        rows: dict[str, int] = {}
        keys: list[str] = []
        failed: list[str] = []
        parse_failed: list[str] = []
        outcomes = {}
        for record_type in record_types:
            reader = read(agency, record_type)
            records = reader.iter_records()
            if transform_for is not None:
                records = transform_for(record_type).apply(records)
            writer = StagingWriter(agency, record_type, staging_dir)
            # write() fully drains the generator, so the reader's key lists are
            # final (including the in-run download retry) by the time we read them.
            writer.write(records)
            rows[record_type.name] = writer.rows_written
            keys.extend(reader.last_keys)
            rt_failed = list(reader.failed_keys)
            rt_parse_failed = list(getattr(reader, "parse_failed_keys", []))
            failed.extend(rt_failed)
            parse_failed.extend(rt_parse_failed)
            outcomes[(agency, record_type.name)] = list(getattr(reader, "unresolved", ()))
            if rt_failed or rt_parse_failed:
                logger.warning(
                    "[{}] {}: {} unresolved keys, {} unreadable/empty; all retry next run",
                    agency,
                    record_type.name,
                    len(rt_failed),
                    len(rt_parse_failed),
                )
            logger.info("[{}] {}: staged {} rows", agency, record_type.name, writer.rows_written)
        return rows, keys, failed, parse_failed, outcomes

    result = StageResult(rows_by_type={rt.name: 0 for rt in record_types})
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(stage_one_agency, agency) for agency in agencies]
        for future in as_completed(futures):
            rows, keys, failed, parse_failed, outcomes = future.result()
            for name, count in rows.items():
                result.rows_by_type[name] += count
            result.consumed_keys.update(keys)
            result.failed_keys.update(failed)
            result.parse_failed_keys.update(parse_failed)
            result.unresolved.update(outcomes)
    return result
