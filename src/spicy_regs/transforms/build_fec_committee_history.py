"""Build ``fec_committee_history`` from the FEC's bulk committee master: every cycle's file, read whole each run.

Owner decision 53. Each run downloads ``cm_header_file.csv`` and every cycle's ``cm<yy>.zip`` from 1980 through
the current cycle, about 13 MB in 25 requests, through SpicyDocs' FEC client. The rollup's evidence transport
retains each response. SpicyDocs' reader verifies every file by digest and size, checks the header, and refuses a
malformed row; the rows are projected onto its ``fec_committee_history`` contract. The table is the whole population
every run, so nothing merges with a prior, and a repeated (committee_id, cycle) refuses.

The current cycle is the even year at or after today. A new cycle's file appears when the FEC opens that cycle, so
a run in the first days of an odd year can refuse until the FEC posts it.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import pyarrow as pa
from loguru import logger

from spicy_regs.transforms.parquet_rows import write_rows

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "fec_committee_history.parquet"
#: cm26, the largest file, is 0.87 MB; a file past this is not a committee master.
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_HEADER_BYTES = 4 * 1024


def current_cycle(today: date) -> int:
    """The two-year cycle ``today`` falls in, named by its closing even year."""
    return today.year + today.year % 2


def _capture(result: dict, representation: str) -> dict:
    """SpicyDocs' capture facts for one downloaded original, as its readers take them."""
    facts = result.get("response") or {}
    if not result.get("downloaded") or not facts.get("observed_at"):
        raise ValueError(f"FEC bulk original was not freshly downloaded: {result['url']}")
    return {
        "requestUrl": result["url"],
        "observedAt": facts["observed_at"],
        "responseSha256": result["sha256"],
        "byteSize": result["bytes"],
        "representation": representation,
    }


def build_fec_committee_history(
    output_dir: Path, *, evidence: CaptureEvidence | None = None, today: date | None = None
) -> Path:
    """Download every cycle's committee master, read and project it, and write the whole table."""
    from rulespec_artifacts import LocalBlobSource
    from spicy_docs.schemas.fec_committee_history import FEC_COMMITTEE_HISTORY, project_committee_master_row
    from spicy_docs.sources.fec.client import FecClient
    from spicy_docs.sources.fec.committee_master import (
        HEADER_URL,
        committee_master_cycles,
        committee_master_url,
        iter_committee_master_rows,
    )

    cycles = committee_master_cycles(current_cycle(today or date.today()))
    output_dir.mkdir(parents=True, exist_ok=True)
    transport = None
    if evidence is not None:
        evidence.event("selection", stage="fec-committee-master", cycles=[cycles[0], cycles[-1]])
        transport = evidence.transport(stage="fec-committee-master-response", max_bytes=MAX_FILE_BYTES)
    with TemporaryDirectory(dir=output_dir) as temporary:
        store = Path(temporary) / "blobs"
        with FecClient(store=store, max_requests=(len(cycles) + 1) * 4, transport=transport) as client:
            header = _capture(client.download(HEADER_URL, max_bytes=MAX_HEADER_BYTES), "opaque")
            captures = {
                cycle: _capture(client.download(committee_master_url(cycle), max_bytes=MAX_FILE_BYTES), "zip")
                for cycle in cycles
            }
        blobs = LocalBlobSource(store)
        counts: dict[int, int] = {}

        def rows() -> Iterator[dict]:
            seen: set[tuple[str | None, str | None]] = set()
            for cycle, capture in captures.items():
                counts[cycle] = 0
                for row in iter_committee_master_rows(
                    capture=capture, header_capture=header, blob_source=blobs, cycle=cycle
                ):
                    shaped = project_committee_master_row(row)
                    key = (shaped["committee_id"], shaped["cycle"])
                    if key in seen:
                        raise ValueError(f"FEC committee master repeats {key[0]} in cycle {key[1]}")
                    seen.add(key)
                    counts[cycle] += 1
                    yield shaped

        schema = pa.schema([(column, pa.string()) for column in FEC_COMMITTEE_HISTORY.columns])
        out = write_rows(rows(), output_dir / OUTPUT, schema)
    if evidence is not None:
        evidence.event("cycle-rows", stage="fec-committee-master", rows_by_cycle={str(k): v for k, v in counts.items()})
    logger.info("FEC committee history: {:,} rows over {} cycles; {}", sum(counts.values()), len(counts),
                json.dumps({c: n for c, n in list(counts.items())[-2:]}))
    return out
