"""Copy upstream's GAO rows once, for the reports the fork never captured (owner decision, 2026-09-28).

The fork's bucket started empty: its first credentialed GAO run, on 2026-09-22,
found no prior table, and the reports feed lists only its latest 25 items, so
the 118 reports upstream's accumulator holds from 2026-07-13 to 2026-09-14 can
no longer be read from the feed. This publishes one ``gao-reports`` generation
through the rollup's own path: pinned prior, source evidence, verified
generation, conditional pointer. The fork's rows win; upstream adds only
reports the fork lacks. The generation names upstream's object as a parent,
and the evidence journal retains its bytes and says the rows were copied from
upstream's publication, not captured by the fork. Refuses unless both tables
have the family's schema, every report both hold is identical in every cell,
and the copied rows are exactly the reviewed ones.

    uv run --frozen python scripts/import_gao_upstream.py                    # dry run: verified local generation
    uv run --frozen python scripts/import_gao_upstream.py --no-skip-upload   # publish
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, ClassVar

import duckdb
import httpx
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.gao_reports import GaoReportsRollup
from spicy_regs.transforms.build_gao_reports import _SCHEMA, COLUMNS, OUTPUT
from spicy_regs.transforms.table_merge import merge_local_prior

UPSTREAM_URL = "https://data.spicy-regs.dev/gao_reports.parquet"
#: Every report copied must fall in this publication window: the reports upstream holds and the fork never captured.
WINDOW = ("2026-07-13", "2026-09-14")
#: :func:`rows_digest` of the 118 reviewed rows, read on 2026-09-28 from upstream's object with ETag
#: ``"e0555502034fb79e90b8a14b26bc8d05"`` (185,431 bytes, last modified 2026-09-27). Their ids equal the trace's
#: DuckDB set difference in both directions. Any other copied set refuses.
ROWS_SHA256 = "sha256:ec48702a097b6ede03c9b4e0100e8f619add6903813363b39af687e9368a7e0c"
RULE = "gao-upstream-publication-copy/1"
LIMITS = (
    "Copied from upstream spicy-regs' published table, not captured from GAO by this fork; no GAO response for "
    "these rows is retained. Reports both tables held were identical in every cell; the fork's rows are kept."
)
MAX_BYTES = 16 * 1024 * 1024


class ImportRefused(ValueError):
    """The upstream copy would not be exactly the reviewed addition."""


def rows_digest(rows: Iterable[Mapping[str, Any]]) -> str:
    """SHA-256 over rows in ``report_id`` order, each a JSON array of the family's columns."""
    lines = (
        json.dumps([row[column] for column in COLUMNS], ensure_ascii=False, separators=(",", ":"))
        for row in sorted(rows, key=lambda row: row["report_id"])
    )
    return "sha256:" + hashlib.sha256("\n".join(lines).encode()).hexdigest()


def _rows(path: Path, label: str) -> dict[str, dict]:
    """Rows by ``report_id``; refuses a schema other than the family's, or a NULL or repeated id."""
    schema = pq.read_schema(path)
    if [(field.name, field.type) for field in schema] != [(field.name, field.type) for field in _SCHEMA]:
        raise ImportRefused(f"{label} schema differs from gao_reports: {schema.names} {schema.types}")
    rows: dict[str, dict] = {}
    for row in pq.read_table(path).to_pylist():
        if row["report_id"] is None or row["report_id"] in rows:
            raise ImportRefused(f"{label} has a NULL or repeated report_id: {row['report_id']}")
        rows[row["report_id"]] = row
    return rows


def import_upstream_rows(prior: Path, upstream: Path, output: Path, *, window: tuple[str, str], rows_sha256: str) -> dict:
    """Write the fork's table plus upstream's rows for the reports it lacks; the fork's rows win."""
    fork, theirs = _rows(prior, "The fork's table"), _rows(upstream, "Upstream's table")
    shared = sorted(fork.keys() & theirs.keys())
    if differ := [key for key in shared if fork[key] != theirs[key]]:
        raise ImportRefused(f"{len(differ)} report(s) both tables hold differ: {', '.join(differ[:10])}")
    added = sorted(theirs.keys() - fork.keys())
    first, last = window
    if outside := [key for key in added if not first <= (theirs[key]["published_date"] or "") <= last]:
        raise ImportRefused(f"Unexpected report(s) outside {first}..{last}: {', '.join(outside[:10])}")
    if (digest := rows_digest(theirs[key] for key in added)) != rows_sha256:
        raise ImportRefused(f"The {len(added)} report(s) to copy differ from the reviewed rows ({digest})")
    con = duckdb.connect()
    try:
        merge_local_prior(
            con, columns=COLUMNS, identity="report_id", order_by="published_date DESC, report_id",
            prior_file=upstream, new_file=prior, out_file=output,
        )
    finally:
        con.close()
    candidate = pq.ParquetFile(output).metadata.num_rows
    if candidate != len(fork) + len(added):
        raise ImportRefused(f"The union holds {candidate} rows, not {len(fork)} + {len(added)}")
    dates = [theirs[key]["published_date"] for key in added]
    return {
        "matched_rows": len(shared), "copied_rows": len(added), "prior_rows": len(fork), "candidate_rows": candidate,
        "first_published": min(dates, default=None), "last_published": max(dates, default=None),
        "copied_rows_sha256": digest, "copied_report_ids": added,
    }


class GaoUpstreamImport(GaoReportsRollup):
    """One ``gao-reports`` generation: the fork's live table plus upstream's rows for the reports it never captured."""

    #: The fork's live table, primed from its pinned generation and recorded as this generation's parent.
    inputs: ClassVar[tuple[str, ...]] = (OUTPUT,)
    #: Tests replace the network; ``None`` reads upstream.
    upstream_transport: ClassVar[httpx.BaseTransport | None] = None
    upstream: dict[str, Any]

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        parents = super()._prime(output_dir, snapshot)
        self.upstream = self._read_upstream(output_dir / "upstream" / OUTPUT)
        parents[UPSTREAM_URL] = {"sha256": self.upstream["sha256"], "byteSize": self.upstream["byte_size"]}
        return parents

    def _read_upstream(self, path: Path) -> dict[str, Any]:
        """One GET, its bytes retained as source evidence; the object's storage version is journaled below."""
        evidence = self.source_evidence
        if evidence is None:
            raise RuntimeError("The upstream copy retains its source evidence")
        evidence.event("selection", stage="gao-upstream-publication", url=UPSTREAM_URL, max_requests=1,
                       max_bytes=MAX_BYTES, timeout_seconds=60)
        transport = evidence.transport(self.upstream_transport, stage="gao-upstream-publication", max_bytes=MAX_BYTES)
        with httpx.Client(transport=transport, timeout=60) as client:
            response = client.get(UPSTREAM_URL)
        if response.status_code != 200:
            raise RuntimeError(f"Upstream GAO table answered HTTP {response.status_code}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(response.content)
        return {
            "source_url": UPSTREAM_URL, "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
            "sha256": "sha256:" + hashlib.sha256(response.content).hexdigest(), "byte_size": len(response.content),
        }

    def build(self, output_dir: Path) -> Path:
        output = output_dir / "import" / OUTPUT
        output.parent.mkdir(parents=True, exist_ok=True)
        report = import_upstream_rows(output_dir / OUTPUT, output_dir / "upstream" / OUTPUT, output,
                                      window=WINDOW, rows_sha256=ROWS_SHA256)
        assert self.source_evidence is not None
        self.source_evidence.event("upstream-publication-copy", rule=RULE, **self.upstream, **report, limits=LIMITS)
        logger.info(
            "GAO upstream copy: {matched_rows} matched, {copied_rows} copied ({first_published}..{last_published}), "
            "{candidate_rows} rows", **report,
        )
        return output


app = make_rollup_app(GaoUpstreamImport)

if __name__ == "__main__":
    app()
