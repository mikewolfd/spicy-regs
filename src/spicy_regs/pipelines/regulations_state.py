"""Local unresolved-key observations, retained across agency/type subsets."""

from collections.abc import Iterable, Mapping
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from spicy_docs.schemas.base import RecordType as SourceRecordType
from spicy_docs.sources.mirrulations import KeyOutcome
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.schemas import RECORD_TYPES, RecordType


def source_record_type(record_type: RecordType) -> SourceRecordType:
    """Supply the source path while keeping this host's schema and extractor."""
    return SourceRecordType(**asdict(record_type))


class UnresolvedKeys:
    """Resume unsuccessful reads first, including legacy failed-key diagnostics.

    A legacy parse failure may also be in the old processed manifest. Supplying
    it through ``unresolved_keys`` bypasses that membership check and repairs
    the old reader's false coverage. Attempts before this format are unknown.
    """

    def __init__(self, output_dir: Path) -> None:
        self.path = output_dir / "failed_keys.parquet"
        self.rows: dict[str, dict] = {}
        if self.path.exists():
            for row in pq.read_table(self.path).to_pylist():
                if "status" not in row:
                    parts = row["key"].split("/")
                    row = {
                        "agency": parts[1],
                        "record_type": next(rt.name for rt in RECORD_TYPES.values() if rt.path_pattern and rt.path_pattern in row["key"]),
                        **asdict(KeyOutcome(
                            row["key"], "transport" if row["kind"] == "transient" else "unreadable",
                            "legacy diagnostic; prior attempts unknown", row["run_at"], 0,
                        )),
                    }
                self.rows[row["key"]] = row

    def for_reader(self, agency: str, record_type: RecordType | SourceRecordType) -> list[KeyOutcome]:
        return [
            KeyOutcome(**{k: row[k] for k in KeyOutcome.__dataclass_fields__})
            for row in self.rows.values()
            if (row["agency"], row["record_type"]) == (agency, record_type.name)
        ]

    def update(self, successful: Iterable[str], outcomes: Mapping[tuple[str, str], Iterable[KeyOutcome]]) -> None:
        for key in successful:
            self.rows.pop(key, None)
        for (agency, record_type), observations in outcomes.items():
            for observation in observations:
                row = asdict(observation)
                row["reason"] = scrub_credential(row["reason"], "")
                self.rows[observation.key] = {"agency": agency, "record_type": record_type, **row}
        if not self.rows:
            self.path.unlink(missing_ok=True)
            return
        temporary = self.path.with_suffix(".tmp.parquet")
        pq.write_table(pa.Table.from_pylist(list(self.rows.values())), temporary, compression="zstd")
        temporary.replace(self.path)
        logger.info("Retained {} unresolved keys; none manifested as coverage", len(self.rows))
