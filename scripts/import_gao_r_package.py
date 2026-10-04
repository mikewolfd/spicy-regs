"""Copy the CetiAlphaFive/gao R package's reports once, for the reports no route of ours holds (owner decision, 2026-09-28).

The R package ``gao`` by Jack T. Rametta (GPL-3.0-or-later, https://github.com/CetiAlphaFive/gao) lists GAO's
product pages from 1922 to 2026. The owner approved one copy and accepted the copyleft caveat. This publishes one
``gao-reports`` generation through the rollup's own path, as the one-time copy of upstream's rows did: the live table
primed from its pinned generation, source evidence, a verified generation, a conditional pointer.

The input is the package's ``inst/extdata/gao_links.rds`` at commit ``6f61230``. It was converted once, outside this
project, to Parquet, and ``receipt.json`` shows the conversion lost nothing. Both files, the receipt and the licence
are retained as evidence, and each is checked against its pinned digest. Nothing here needs ``pyreadr``.

It is the lowest-precedence route (:mod:`spicy_regs.sources.gao_r_package`): it adds only reports no row holds, and
changes no cell of a held row that holds a value. It fills a held row's NULL cells (``FILL_NULLS``, owner decision
2026-09-29), and the report counts every fill by route and column. The copy refuses unless the added rows are exactly the
reviewed ones.

    uv run --frozen python scripts/import_gao_r_package.py                    # dry run: verified local generation
    uv run --frozen python scripts/import_gao_r_package.py --no-skip-upload   # publish
    GAO_R_PACKAGE_DIR=<dir> ...                                               # the input directory, if moved
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Iterable, Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, ClassVar

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.gao_reports import GaoReportsRollup
from spicy_regs.sources import gao_r_package as package
from spicy_regs.transforms.table_merge import merge_local_prior
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder

reports = import_module("spicy_regs.transforms.build_gao_reports")

DEFAULT_DIR = Path("~/Work/corpora/gao-r-package-2026-09-29").expanduser()
RULE = "gao-r-package-copy/1"
#: :func:`rows_digest` of the reviewed additions: 26,871 reports, measured on 2026-09-29 against the Track B build
#: (live ``ac08f37f`` plus GAO's listing, 26,183 rows). Any other set refuses; re-review it before a copy onto
#: another table.
ROWS_SHA256 = "sha256:94ad333b3ad52b69678e070f56f939d2fb78007381133dfe0c1297ccd37d309f"
#: Fill a held row's NULL cells from the package too, never changing a value (owner decision 2026-09-29).
FILL_NULLS = True
#: Every column the package states; a held row's other cells are never touched.
FILL_COLUMNS = (
    "title", "published_date", "abstract", "agencies_json", "topics_json", "report_number", "requester_type",
    "requester_committees_json", "requester_members_json", "recommendation_count", "matters_for_congress_count",
    "page_count", "subject_terms_json",
)
LIMITS = (
    "Copied once from the CetiAlphaFive/gao R package (GPL-3.0-or-later, Jack T. Rametta), not captured from GAO by "
    "this fork; no GAO response for these rows is retained. Legal decisions are left out. Held rows keep every cell."
)


class ImportRefused(ValueError):
    """The package copy would not be exactly the reviewed addition."""


def rows_digest(rows: Iterable[Mapping[str, Any]]) -> str:
    """SHA-256 over rows in ``report_id`` order, each a JSON array of the family's columns."""
    lines = (
        json.dumps([row[column] for column in reports.COLUMNS], ensure_ascii=False, separators=(",", ":"))
        for row in sorted(rows, key=lambda row: row["report_id"])
    )
    return "sha256:" + hashlib.sha256("\n".join(lines).encode()).hexdigest()


@receipt_builder(output_argument="output", dataset="gao_reports")
def import_package_rows(
    prior: Path, records: Iterable[Mapping[str, Any]], output: Path, *, rows_sha256: str, fill_nulls: bool = FILL_NULLS
) -> dict[str, Any]:
    """The live table plus the package's reports it lacks; a held row's cells never change, NULLs only if asked."""
    prior = internal_prior("gao_reports", prior)
    schema = pq.read_schema(prior)
    wanted = {f.name: f.type for f in reports._SCHEMA}
    if any(wanted.get(f.name) != f.type for f in schema) or schema.names != list(reports.COLUMNS)[: len(schema)]:
        raise ImportRefused(f"The live table's columns are not a prefix of gao_reports': {schema.names}")
    # A live table written before the package's columns lacks them: they are NULL there, a backfill.
    held = {row["report_id"]: {**dict.fromkeys(reports.COLUMNS), **row} for row in pq.read_table(prior).to_pylist()}
    rows, counts = package.package_rows(records)
    added: list[dict] = []
    filled: dict[str, dict] = {}
    would_fill: set[tuple[str, str]] = set()
    for row in rows:
        at = package.held_as(row, held)
        if at is None:
            added.append(row)
            continue
        counts["held"] += 1
        counts["held_as_twin"] += at != row["report_id"]
        # A -N twin and its base page are one held row: each NULL cell fills once, from the first page listed.
        for column in FILL_COLUMNS:
            if held[at][column] is None and row[column] is not None and (at, column) not in would_fill:
                would_fill.add((at, column))
                if fill_nulls:
                    filled.setdefault(at, dict(held[at]))[column] = row[column]
    if (digest := rows_digest(added)) != rows_sha256:
        raise ImportRefused(f"The {len(added)} report(s) to add differ from the reviewed rows ({digest})")
    new_file = output.with_name("_package_new.parquet")
    pq.write_table(pa.Table.from_pylist([*added, *filled.values()], schema=reports._SCHEMA), new_file)
    con = duckdb.connect()
    try:
        merge_local_prior(con, columns=reports.COLUMNS, identity="report_id", order_by="published_date DESC, report_id",
                          prior_file=prior, new_file=new_file, out_file=output)
    finally:
        con.close()
        new_file.unlink(missing_ok=True)
    after = {row["report_id"]: row for row in pq.read_table(output).to_pylist()}
    if len(after) != len(held) + len(added):
        raise ImportRefused(f"The union holds {len(after)} rows, not {len(held)} + {len(added)}")
    if changed := [key for key, row in held.items()
                   if any(row[c] is not None and after[key].get(c) != row[c] for c in reports.COLUMNS)]:
        raise ImportRefused(f"{len(changed)} held row(s) would change a stated cell: {changed[:5]}")
    decades = Counter((row["published_date"] or "????")[:3] + "0s" for row in added)
    return {
        **counts, "added": len(added), "prior_rows": len(held), "candidate_rows": len(after),
        "added_by_decade": dict(sorted(decades.items())), "added_rows_sha256": digest,
        "null_fills_available": dict(sorted(Counter(f"{held[at]['source']}.{column}" for at, column in would_fill).items())),
        "null_fills_applied": fill_nulls,
    }


class GaoRPackageImport(GaoReportsRollup):
    """One ``gao-reports`` generation: the live table plus the R package's reports it lacks."""

    #: The live table, primed from its pinned generation. It is this family's own prior, so the root names it as the
    #: prior-generation input, not as a parent.
    inputs: ClassVar[tuple[str, ...]] = (reports.OUTPUT,)
    package_dir: ClassVar[Path] = Path(os.environ.get("GAO_R_PACKAGE_DIR", DEFAULT_DIR))

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        parents = super()._prime(output_dir, snapshot)
        evidence = self.source_evidence
        if evidence is None:
            raise RuntimeError("The package copy retains its source evidence")
        pins = {"gao_links.rds": package.RDS_SHA256, "gao_links.parquet": package.PARQUET_SHA256}
        for name, pinned in pins.items():
            digest = "sha256:" + hashlib.sha256((self.package_dir / name).read_bytes()).hexdigest()
            if digest != pinned:
                raise ImportRefused(f"{name} is not the reviewed file ({digest})")
        receipt = json.loads((self.package_dir / "receipt.json").read_text())
        if not (receipt.get("lossless") and receipt.get("commit") == package.PACKAGE_COMMIT
                and receipt["rds"]["sha256"] == package.RDS_SHA256
                and receipt["parquet"]["sha256"] == package.PARQUET_SHA256):
            raise ImportRefused("receipt.json does not state a lossless conversion of the pinned files")
        for name in ("gao_links.rds", "gao_links.parquet", "receipt.json", "LICENSE.md"):
            evidence.retain_file(self.package_dir / name, stage="gao-r-package", repository=package.REPOSITORY,
                                 commit=package.PACKAGE_COMMIT, license=package.LICENSE)
        parents[package.RDS_URL] = {"sha256": package.RDS_SHA256, "byteSize": package.RDS_BYTES}
        return parents

    @receipt_builder
    def build(self, output_dir: Path) -> tuple[Path, Path]:
        output = output_dir / reports.OUTPUT
        output.parent.mkdir(parents=True, exist_ok=True)
        records = package.read_package(self.package_dir / "gao_links.parquet")
        report = import_package_rows(output_dir / reports.OUTPUT, records, output, rows_sha256=ROWS_SHA256,
                                     fill_nulls=FILL_NULLS)
        assert self.source_evidence is not None
        self.source_evidence.event(
            "r-package-copy", rule=RULE, repository=package.REPOSITORY, commit=package.PACKAGE_COMMIT,
            license=package.LICENSE, attribution=package.ATTRIBUTION, rds_sha256=package.RDS_SHA256,
            parquet_sha256=package.PARQUET_SHA256, limits=LIMITS, **report,
        )
        logger.info("GAO R package copy: {held} held, {added} added, {candidate_rows} rows", **report)
        # gao_decisions is not the package's to change: carry it forward.
        return output, reports._build_decisions(output_dir, [])


app = make_rollup_app(GaoRPackageImport)

if __name__ == "__main__":
    app()
