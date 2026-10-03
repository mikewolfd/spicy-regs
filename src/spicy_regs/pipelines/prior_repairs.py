"""One-time repairs of published rows that adopting spicy-docs 0.51.0 and 0.52.0 owes, each dry-runnable and idempotent.

Run ``uv run --frozen python -m spicy_regs.pipelines.prior_repairs --help``. ``WINDOW1-RUNBOOK.md`` in the adoption's
receipts orders them. Each table repair is a partial writer of the family that publishes the table
(``RollupPipeline.publication_family``): it reads the table from the family's current generation, rewrites only the
values it names, and publishes a generation that carries every other table forward. A second run changes nothing.

* ``respell-bill-family-digests``: spicy-docs spells every digest ``sha256:`` and compares them strictly (its "Every
  published digest is spelled" entry), with no compatibility layer, so each prior bare 64-hex value gains the prefix
  once: ``section_classifications.prompt_hash``, ``bill_summaries.content_hash``, ``diff_summaries.content_hash`` and
  ``content_hash`` in each ``summary_generated`` event. The bill-family run refuses a bare summary digest
  (``build_bill_family._require_spelled_digests``), so this re-spell runs before any compare.
* ``respell-document-digests`` counts each attempt's bare ``source_sha256`` in ``documents.pdf_extraction_results_json``
  and only counts (``Repair.count_only`` says why).
* ``respell-comment-digests``: the same for ``comments.pdf_extraction_results_json`` (each attempt's
  ``source_sha256``, each derived attachment's ``sha256``) in the comments catalog, one agency at a time, through
  the catalog's checked upsert; ``--apply`` writes, and each written row's prior value is kept for ``--restore``.
* ``backfill-found-by``: ``cbo_cost_estimates`` appends ``title_bill_id``, ``found_by`` and ``title_bill_id_rule``;
  every prior BILLSTATUS row takes ``billstatus``, the title rule's reading and the rule's name.
* ``rebuild-record-issues``: ``record_issues.package_id`` follows ``entire_issue_url_stem/2``, re-shaped from each
  row's own ``entire_issue_json`` (seven ``-bk{N}`` ids become the real packages, measured 2026-09-28).
* ``stage-suppression``: a measurement only. The adopting bill-family runs suppress ``stage_changed`` by code
  (``build_bill_family.rederived_stages``); this counts the prior stages the running rule moves, each an event that
  is not emitted.
* ``restore``: the rollback of a table repair or of a dispatched family run. Given the publication index saved before
  it, republishes the named tables' pinned bytes as the family's tables.
* ``missing-bills BILL_ID ... --output-dir DIR``: add only absent, explicitly named bills from Congress.gov detail
  records. Existing rows and family siblings stay unchanged. Source responses are retained; unread history stays
  NULL. It verifies a local generation by default; ``--no-skip-upload`` publishes with the normal stale-base guard.

``--dry-run`` reads the tables (from the live generation, or ``--prior-dir``), prints the counts, and writes nothing.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
from cyclopts import App, Parameter
from loguru import logger

from spicy_regs.pipelines.rollups.base import RollupPipeline

#: A digest spelled before 0.52.0: 64 lowercase hex characters and nothing else.
BARE_DIGEST = re.compile(r"[0-9a-f]{64}")


def respell_digest(value: str | None) -> str | None:
    """``sha256:`` plus a bare 64-hex digest; any other value, a prefixed one included, as it is."""
    return f"sha256:{value}" if isinstance(value, str) and BARE_DIGEST.fullmatch(value) else value


def respell_pdf_results(value: str | None) -> str | None:
    """``pdf_extraction_results_json`` with each bare digest prefixed, or ``value`` itself when none is bare.

    Two shapes are published: this repository's PDF attempts (a list, each with ``source_sha256``,
    ``enrich_pdf._PdfAttempt``) and the derived-text provenance (an object whose ``attachments`` each carry
    ``sha256``, spicy-docs' ``DerivedAttachment``). Both writers spell the JSON with ``json.dumps``' defaults; a value
    spelled otherwise refuses rather than being re-spelled beyond its digests.
    """
    if value is None:
        return None
    parsed = json.loads(value)
    if json.dumps(parsed) != value:
        raise ValueError("pdf_extraction_results_json is not spelled as its writers spell it")
    entries: list[tuple[dict, str]] = []
    if isinstance(parsed, list):
        entries = [(attempt, "source_sha256") for attempt in parsed if isinstance(attempt, dict)]
    elif isinstance(parsed, dict):
        entries = [(attachment, "sha256") for attachment in parsed.get("attachments") or () if isinstance(attachment, dict)]
    changed = False
    for entry, key in entries:
        spelled = respell_digest(entry.get(key))
        if spelled != entry.get(key):
            entry[key] = spelled
            changed = True
    return json.dumps(parsed) if changed else value


def respell_event_data(value: str | None) -> str | None:
    """A ``summary_generated`` event's ``event_data_json`` with its ``content_hash`` prefixed; else as it is."""
    from spicy_docs.schemas.tables import json_column

    if value is None:
        return None
    parsed = json.loads(value)
    if json_column(parsed) != value:
        raise ValueError("event_data_json is not spelled as json_column spells it")
    if not isinstance(parsed, dict) or respell_digest(parsed.get("content_hash")) == parsed.get("content_hash"):
        return value
    return json_column(parsed | {"content_hash": respell_digest(parsed["content_hash"])})


# --------------------------------------------------------------------------- #
# Table rewrites: prior file -> output file, returning what changed.
# --------------------------------------------------------------------------- #


def _rows(prior: Path, columns: Sequence[str]) -> list[dict[str, Any]]:
    """Every prior row with exactly ``columns``, a column the prior lacks read as NULL."""
    table = pq.read_table(prior)
    present = set(table.column_names)
    return [{column: row.get(column) if column in present else None for column in columns} for row in table.to_pylist()]


def _write(rows: Sequence[Mapping[str, Any]], columns: Sequence[str], out: Path, *, like: Path) -> None:
    """All-VARCHAR, as every published table is, keeping the prior file's key-value metadata."""
    schema = pa.schema([(column, pa.string()) for column in columns], metadata=pq.read_schema(like).metadata)
    pq.write_table(pa.Table.from_pylist(list(rows), schema=schema), out, compression="zstd")


def _map_columns(prior: Path, out: Path, repairs: Mapping[str, Callable[[str | None], str | None]]) -> dict[str, int]:
    """Rewrite each named column with its function; counts of values changed, by column."""
    columns = pq.read_schema(prior).names
    changed: Counter[str] = Counter()
    rows = _rows(prior, columns)
    for row in rows:
        for column, repair in repairs.items():
            value = repair(row[column])
            if value != row[column]:
                row[column] = value
                changed[column] += 1
    _write(rows, columns, out, like=prior)
    return {"rows": len(rows), **{f"{column}_respelled": changed[column] for column in repairs}}


def respell_prompt_hashes(prior: Path, out: Path) -> dict[str, int]:
    return _map_columns(prior, out, {"prompt_hash": respell_digest})


def respell_content_hashes(prior: Path, out: Path) -> dict[str, int]:
    return _map_columns(prior, out, {"content_hash": respell_digest})


def respell_summary_events(prior: Path, out: Path) -> dict[str, int]:
    """Only a ``summary_generated`` event states a ``content_hash``; every event is read, and no other moves."""
    return _map_columns(prior, out, {"event_data_json": respell_event_data})


def respell_document_results(prior: Path, out: Path) -> dict[str, int]:
    """``documents`` is two million rows: the column is rewritten in DuckDB, calling Python only on a stated value."""
    import duckdb

    con = duckdb.connect(config={"memory_limit": "4GB", "threads": 2})
    try:
        con.create_function("respell", respell_pdf_results, ["VARCHAR"], "VARCHAR")
        source = f"read_parquet('{prior}')"
        (rows, stated) = con.execute(f"SELECT count(*), count(pdf_extraction_results_json) FROM {source}").fetchone() or (0, 0)
        changed = con.execute(
            f"SELECT count(*) FROM {source} WHERE respell(pdf_extraction_results_json) <> pdf_extraction_results_json"
        ).fetchone()
        con.execute(
            f"COPY (SELECT * REPLACE (respell(pdf_extraction_results_json) AS pdf_extraction_results_json) FROM {source}) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)"
        )
    finally:
        con.close()
    return {"rows": int(rows), "rows_with_results": int(stated),
            "pdf_extraction_results_json_respelled": int(changed[0]) if changed else 0}


def backfill_found_by(prior: Path, out: Path, *, law_bills: Mapping[str, str] | None = None) -> dict[str, int]:
    """Each BILLSTATUS row without ``found_by`` takes ``billstatus``, its title's bill and the rule that read it.

    ``title_bill_id`` is spicy-docs' rule over the row's own title (``title_citation``, a law through the published
    ``laws``), as its shaper reads it; a feed row always carries all three, so only a prior BILLSTATUS row is filled.
    """
    from spicy_docs.schemas.cost_estimate_tables import (
        BILLSTATUS_BULK,
        CBO_COST_ESTIMATES,
        FOUND_BY_BILLSTATUS,
        TITLE_BILL_ID_RULE,
    )

    columns = CBO_COST_ESTIMATES.columns
    rows = _rows(prior, columns)
    filled = titled = 0
    for row in rows:
        if row["found_by"] is not None or row["source"] != BILLSTATUS_BULK:
            continue
        row["found_by"] = FOUND_BY_BILLSTATUS
        row["title_bill_id_rule"] = TITLE_BILL_ID_RULE
        row["title_bill_id"] = title_bill_id(int(row["congress"]), row["title"], law_bills)
        filled += 1
        titled += row["title_bill_id"] is not None
    _write(rows, columns, out, like=prior)
    return {"rows": len(rows), "found_by_filled": filled, "title_bill_id_stated": titled,
            "title_bill_id_differs": sum(1 for row in rows if row["title_bill_id"] not in (None, row["bill_id"]))}


def title_bill_id(congress: int, title: object, law_bills: Mapping[str, str] | None) -> str | None:
    """The bill an estimate's title leads with, keyed as ``bill_id`` is, or ``None``: spicy-docs' ``_title_bill_id``.

    That function is private there, so its reading is restated here once, from the public ``title_citation`` and
    ``law_bill``; ``tests/test_prior_repairs.py`` holds the two to the same answers.
    """
    from spicy_docs.schemas.tables import bill_id
    from spicy_docs.sources.cbo import CboFeedBillError, PublicLawCitation, law_bill, title_citation
    from spicy_docs.sources.congress.bill_status import BillIdentity

    if not isinstance(title, str):
        return None
    try:
        cited = title_citation(congress, title)
    except CboFeedBillError:
        return None
    if isinstance(cited, BillIdentity):
        return bill_id(cited)
    if not isinstance(cited, PublicLawCitation) or law_bills is None:
        return None
    enacted = law_bill(law_bills, cited)
    return None if enacted is None else bill_id(enacted)


def rebuild_record_issues(prior: Path, out: Path) -> dict[str, int]:
    """Re-shape ``package_id`` and its rule from each row's own ``entire_issue_json`` under the running rule."""
    from spicy_docs.schemas.congress_index_tables import shape_record_issue

    columns = pq.read_schema(prior).names
    rows = _rows(prior, columns)
    moved = ruled = 0
    for row in rows:
        if row["entire_issue_json"] is None:
            continue
        shaped = shape_record_issue({}, {"fullIssue": {"entireIssue": json.loads(row["entire_issue_json"])}})
        moved += shaped["package_id"] != row["package_id"]
        ruled += shaped["package_id_rule"] != row["package_id_rule"]
        row["package_id"], row["package_id_rule"] = shaped["package_id"], shaped["package_id_rule"]
    _write(rows, columns, out, like=prior)
    return {"rows": len(rows), "package_id_moved": moved, "package_id_rule_moved": ruled}


# --------------------------------------------------------------------------- #
# The repairs, their families, and the partial writer that publishes one.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Repair:
    """A table repair: the family publishing its tables, each table's rewrite, and the tables it only reads.

    A rewrite reads its prior at ``<priors>/<table>.parquet`` and a read table beside it, when published.
    """

    family: str
    tables: Mapping[str, Callable[[Path, Path], dict[str, int]]]
    reads: tuple[str, ...] = ()
    #: Why this repair only counts: its family is republished from another copy, which a write here would not reach.
    count_only: str | None = None


def _found_by(prior: Path, out: Path) -> dict[str, int]:
    from spicy_regs.transforms.bill_family_cbo import law_bills_from

    laws = prior.parent / "laws.parquet"
    return backfill_found_by(prior, out, law_bills=law_bills_from(laws if laws.exists() else None))


REPAIRS: dict[str, Repair] = {
    "respell-bill-family-digests": Repair("bill-family", {
        "section_classifications": respell_prompt_hashes,
        "bill_summaries": respell_content_hashes,
        "diff_summaries": respell_content_hashes,
        "public_activity_events": respell_summary_events,
    }),
    "respell-document-digests": Repair("documents", {"documents": respell_document_results}, count_only=(
        "the ETL republishes the documents family from its working copy (run-rollup-documents), which a family "
        "write would not reach; on 2026-09-29 no document held a PDF result to re-spell, and enrich_pdf now writes "
        "the prefix, so a nonzero count means the working copy is re-spelled with the ETL paused"
    )),
    "backfill-found-by": Repair("bill-family", {"cbo_cost_estimates": _found_by}, reads=("laws",)),
    "rebuild-record-issues": Repair("record-issues", {"record_issues": rebuild_record_issues}),
}


def apply_repair(repair: Repair, priors: Path, output_dir: Path,
                 download: Callable[[str, Path], bool] | None = None) -> dict[str, dict[str, int]]:
    """Rewrite each of ``repair``'s tables from ``priors`` (or ``download``) into ``output_dir``; counts by table."""
    counts: dict[str, dict[str, int]] = {}
    for table in repair.reads:
        if download is not None and not (priors / f"{table}.parquet").exists():
            download(f"{table}.parquet", priors / f"{table}.parquet")
    for table, rewrite in repair.tables.items():
        prior = priors / f"{table}.parquet"
        if not prior.exists() and (download is None or not download(f"{table}.parquet", prior)):
            raise FileNotFoundError(f"{table}.parquet is not published")
        counts[table] = rewrite(prior, output_dir / f"{table}.parquet")
    return counts


def repair_rollup(name: str, repair: Repair) -> type[RollupPipeline]:
    """The partial writer that publishes ``repair`` over its family's current generation."""

    class PriorRepairRollup(RollupPipeline):
        publication_family: ClassVar[str | None] = repair.family
        outputs: ClassVar[tuple[str, ...]] = tuple(f"{table}.parquet" for table in repair.tables)
        retain_source_evidence: ClassVar[bool] = True

        def build(self, output_dir: Path) -> tuple[Path, ...]:
            from spicy_regs.sources import r2

            priors = output_dir / ".priors"
            priors.mkdir(exist_ok=True)
            counts = apply_repair(repair, priors, output_dir, r2.download)
            logger.info("{}: {}", name, json.dumps(counts, sort_keys=True))
            if self.source_evidence is not None:
                self.source_evidence.event("prior-repair", repair=name, counts=counts)
            return tuple(output_dir / f"{table}.parquet" for table in repair.tables)

    PriorRepairRollup.name = f"prior-repair-{name}"
    return PriorRepairRollup


def add_missing_bills(prior: Path, out: Path, bill_ids: Sequence[str], source: Any) -> dict[str, int]:
    """Add only absent, explicitly named bills through the existing detail reader and bill-family shaper.

    Existing rows keep every value. Every requested detail must succeed before an output is written; unread
    action histories and other sub-routes remain NULL through the ordinary backfill adjustment.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms import build_bill_family as family
    from spicy_regs.transforms.congress_scope import bill_identity

    identities = [bill_identity(key) for key in dict.fromkeys(bill_ids)]
    if not identities:
        raise ValueError("Name at least one bill to repair")
    columns = TABLE_CONTRACTS["congress_bills"].columns
    if pq.read_schema(prior).names != list(columns):
        raise ValueError("The prior congress_bills schema differs; migrate it before this repair")
    rows = _rows(prior, columns)
    held = {row["bill_id"] for row in rows}
    if None in held or len(held) != len(rows):
        raise ValueError("The prior has missing or duplicate bill IDs")
    added = 0
    for identity in identities:
        if family.bill_key(identity) in held:
            continue
        detail, observed_at = source.detail(identity)
        # The production reader checks route identity; keep the same check for injected/replayed sources.
        if (str(detail.get("congress")), str(detail.get("type", "")).lower(), str(detail.get("number"))) != (
            str(identity.congress), identity.bill_type, str(identity.number)
        ):
            raise ValueError(f"Detail identity differs from {family.bill_key(identity)}")
        built = family.build_family(
            family.BillFamilyCapture(status=family.backfill_status(identity, detail), versions=(), observed_at=observed_at),
            engine=family.engine_stamp(), classify=None, summarize=None, summarize_diff=None,
        )
        if len(built.bills) != 1:
            raise ValueError("The detail shaper must produce exactly one bill")
        rows.append(family._backfill_bill_row(built.bills[0], detail))
        added += 1
    _write(rows, columns, out, like=prior)
    return {"rows": len(rows), "added": added, "already_present": len(identities) - added}


def missing_bills_rollup(bill_ids: Sequence[str]) -> type[RollupPipeline]:
    """A bounded partial writer: preserve bill-family siblings and retain every new detail response."""
    class MissingBillsRollup(RollupPipeline):
        name: ClassVar[str] = "prior-repair-missing-bills"
        publication_family: ClassVar[str | None] = "bill-family"
        output: ClassVar[str] = "congress_bills.parquet"
        retain_source_evidence: ClassVar[bool] = True

        def build(self, output_dir: Path) -> Path:
            from spicy_regs.sources import r2
            from spicy_regs.sources.congress_bills import _resolve_api_key
            from spicy_regs.transforms.build_bill_family import CongressListBackfill

            key = _resolve_api_key()
            if not key:
                raise ValueError("Missing Congress.gov API key")
            prior = output_dir / ".prior-congress_bills.parquet"
            if not r2.download(self.output, prior):
                raise FileNotFoundError("congress_bills.parquet is not published")
            if self.source_evidence is not None:
                self.source_evidence.credential = key
                self.source_evidence.event("selection", bill_ids=list(bill_ids))
            out = output_dir / self.output
            with CongressListBackfill(key, evidence=self.source_evidence) as source:
                counts = add_missing_bills(prior, out, bill_ids, source)
            if self.source_evidence is not None:
                self.source_evidence.event("prior-repair", repair="missing-bills", counts=counts)
            logger.info("Missing bill repair: {}", counts)
            return out

    return MissingBillsRollup


def dry_run(name: str, *, prior_dir: Path | None = None) -> dict[str, dict[str, int]]:
    """``name``'s counts over the live generation (``R2_PUBLIC_URL``) or ``prior_dir``; nothing is written."""
    from spicy_regs.sources import publication, r2

    repair = REPAIRS[name]
    with TemporaryDirectory(prefix="prior-repair-") as scratch:
        work = Path(scratch)
        if prior_dir is not None:
            return apply_repair(repair, prior_dir, work)
        public_url = os.environ.get("R2_PUBLIC_URL")
        if not public_url:
            raise RuntimeError("a dry run reads the live generation through R2_PUBLIC_URL, or --prior-dir")
        priors = work / "priors"
        priors.mkdir()
        with publication.snapshot(public_url):
            return apply_repair(repair, priors, work, r2.download)


# --------------------------------------------------------------------------- #
# Comments live in the catalog, not a generation family.
# --------------------------------------------------------------------------- #

#: A bare digest where either writer puts one; the JSON parse decides, this only narrows the rows read.
_BARE_IN_RESULTS = r'"(source_)?sha256": "[0-9a-f]{64}"'


def comment_digest_repairs(con: Any, relation: str, *, agency: str | None = None) -> list[tuple[str, str, str]]:
    """``(comment_id, prior, respelled)`` for each comment in ``relation`` whose results hold a bare digest."""
    where = "" if agency is None else f"AND agency_code = '{agency.replace(chr(39), chr(39) * 2)}'"
    rows = con.execute(
        f"SELECT comment_id, pdf_extraction_results_json FROM {relation} "
        f"WHERE pdf_extraction_results_json IS NOT NULL AND regexp_matches(pdf_extraction_results_json, ?) {where}",
        [_BARE_IN_RESULTS],
    ).fetchall()
    return [
        (comment, prior, spelled)
        for comment, prior in rows
        if (spelled := respell_pdf_results(prior)) is not None and spelled != prior
    ]


def respell_comment_digests(*, agencies: Sequence[str] = (), apply: bool = False,
                            receipt_dir: Path | None = None) -> dict[str, int]:
    """Count, and with ``apply`` write, the comments catalog's re-spelled digests, one agency at a time.

    A write goes through ``iceberg.upsert_comment_text`` with only ``_new_pdf_results`` stated, so the text columns
    keep their values. Each agency's receipt (``comment_id``, the prior value, the written value) is written before its
    upsert, which is what ``restore_comment_digests`` puts back.
    """
    import polars as pl

    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import iceberg

    if apply and receipt_dir is None:
        raise ValueError("--apply keeps each written row's prior value; name --receipt-dir")
    con = iceberg._connect_for_table(COMMENT)
    counts: Counter[str] = Counter()
    try:
        table = iceberg._qualified(COMMENT)
        names = list(agencies) or [row[0] for row in con.execute(
            f"SELECT DISTINCT agency_code FROM {table} WHERE agency_code IS NOT NULL ORDER BY 1").fetchall()]
        for agency in names:
            repairs = comment_digest_repairs(con, table, agency=agency)
            counts["agencies"] += 1
            counts["comments_respelled"] += len(repairs)
            if not repairs:
                continue
            logger.info("comments[{}]: {:,} rows hold a bare digest", agency, len(repairs))
            if apply and receipt_dir is not None:
                receipt = receipt_dir / f"comment-digests-{agency}.parquet"
                receipt_dir.mkdir(parents=True, exist_ok=True)
                pq.write_table(pa.table({"comment_id": [r[0] for r in repairs], "prior": [r[1] for r in repairs],
                                         "written": [r[2] for r in repairs]}), receipt)
                updates = pl.DataFrame({"comment_id": [r[0] for r in repairs], "_new_text": [None] * len(repairs),
                                        "_new_status": [None] * len(repairs), "_new_pdf_results": [r[2] for r in repairs]},
                                       schema={"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8,
                                               "_new_pdf_results": pl.Utf8})
                iceberg.upsert_comment_text(con, COMMENT, agency, updates)
    finally:
        con.close()
    return dict(counts)


def restore_comment_digests(receipt_dir: Path) -> dict[str, int]:
    """Put back each receipt's prior values where the row still holds what the re-spell wrote, agency by agency."""
    import polars as pl

    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import iceberg

    con = iceberg._connect_for_table(COMMENT)
    counts: Counter[str] = Counter()
    try:
        table = iceberg._qualified(COMMENT)
        for receipt in sorted(receipt_dir.glob("comment-digests-*.parquet")):
            agency = receipt.stem.removeprefix("comment-digests-")
            held = pq.read_table(receipt).to_pylist()
            con.register("_restore_receipt", pa.Table.from_pylist(held))
            current = dict(con.execute(
                f"SELECT c.comment_id, c.pdf_extraction_results_json FROM {table} c JOIN _restore_receipt r "
                f"USING (comment_id) WHERE c.agency_code = ?", [agency]).fetchall())
            con.unregister("_restore_receipt")
            back = [row for row in held if current.get(row["comment_id"]) == row["written"]]
            counts["comments_restored"] += len(back)
            counts["comments_since_rewritten"] += len(held) - len(back)
            if back:
                iceberg.upsert_comment_text(con, COMMENT, agency, pl.DataFrame(
                    {"comment_id": [r["comment_id"] for r in back], "_new_text": [None] * len(back),
                     "_new_status": [None] * len(back), "_new_pdf_results": [r["prior"] for r in back]},
                    schema={"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8,
                            "_new_pdf_results": pl.Utf8}))
    finally:
        con.close()
    return dict(counts)


# --------------------------------------------------------------------------- #
# Stage suppression (a measurement) and restore (the rollback).
# --------------------------------------------------------------------------- #


def stage_suppression(bills: Path, actions: Path) -> dict[str, Any]:
    """How many published stages the running rule moves with no new action: each an event the adopting run suppresses."""
    import duckdb

    from spicy_regs.transforms.build_bill_family import rederived_stages

    stages = rederived_stages(actions)
    published = dict(duckdb.sql(f"SELECT bill_id, stage FROM read_parquet('{bills}')").fetchall())
    moves = Counter((published.get(bill), stage) for bill, stage in stages.items() if published.get(bill) != stage)
    return {
        "bills_with_actions": len(stages),
        "stages_moved_by_the_rule": sum(moves.values()),
        "stage_changed_events_emitted_for_them": 0,
        "largest_moves": [{"from": a, "to": b, "bills": n} for (a, b), n in moves.most_common(10)],
    }


def restore_rollup(index: Mapping[str, Any], family: str, tables: Sequence[str]) -> type[RollupPipeline]:
    """A partial writer republishing ``tables`` exactly as the saved ``index`` pins them for ``family``."""
    from spicy_regs.sources.publication import Member, fetch_member

    entry = index["families"][family]
    pins = {}
    for table in tables:
        descriptor = entry["tables"][f"{table}.parquet"]
        if "members" in descriptor:
            raise ValueError(f"{table} is split; restore republishes single-file tables")
        pins[table] = Member(f"{entry['prefix']}/{table}.parquet", descriptor["sha256"], descriptor["byteSize"],
                             descriptor["rows"])

    class RestoreRollup(RollupPipeline):
        publication_family: ClassVar[str | None] = family
        outputs: ClassVar[tuple[str, ...]] = tuple(f"{table}.parquet" for table in tables)
        retain_source_evidence: ClassVar[bool] = True

        def build(self, output_dir: Path) -> tuple[Path, ...]:
            public_url = os.environ.get("R2_PUBLIC_URL")
            if not public_url:
                raise RuntimeError("restore reads the saved generation's members through R2_PUBLIC_URL")
            for table, member in pins.items():
                fetch_member(public_url, member, output_dir / f"{table}.parquet")
            if self.source_evidence is not None:
                self.source_evidence.event("prior-restore", family=family, artifact=entry["artifactDigest"],
                                           tables=sorted(pins))
            return tuple(output_dir / f"{table}.parquet" for table in tables)

    RestoreRollup.name = f"prior-restore-{family}"
    return RestoreRollup


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

app = App(name="prior-repairs", help="One-time repairs of published rows owed by the spicy-docs 0.51.0/0.52.0 adoption.")


def _print(counts: object) -> None:
    print(json.dumps(counts, indent=1, sort_keys=True, default=str))


def _table_repair(name: str):
    def command(
        *,
        dry_run_: Annotated[bool, Parameter(name="--dry-run", help="Print the counts and write nothing")] = False,
        prior_dir: Annotated[Path | None, Parameter(help="Read <table>.parquet from here instead of the bucket")] = None,
        output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
        skip_upload: Annotated[bool, Parameter(help="Build and verify a local generation only")] = True,
    ) -> None:
        if dry_run_:
            _print(dry_run(name, prior_dir=prior_dir))
            return
        if REPAIRS[name].count_only:
            raise SystemExit(f"{name} counts only (--dry-run): {REPAIRS[name].count_only}")
        repair_rollup(name, REPAIRS[name])(output_dir=output_dir, skip_upload=skip_upload).run()

    command.__doc__ = f"Run {name}: a partial writer of the {REPAIRS[name].family} family."
    return command


for _name in REPAIRS:
    app.command(_table_repair(_name), name=_name)


@app.command(name="missing-bills")
def missing_bills_command(
    *bill_ids: str,
    output_dir: Path,
    skip_upload: Annotated[bool, Parameter(help="Build and verify a local generation only")] = True,
) -> None:
    """Add explicitly named absent bills from Congress.gov details, preserving existing rows and family siblings."""
    missing_bills_rollup(bill_ids)(output_dir=output_dir, skip_upload=skip_upload).run()


@app.command(name="respell-comment-digests")
def respell_comment_digests_command(
    *,
    dry_run_: Annotated[bool, Parameter(name="--dry-run", help="Count only")] = False,
    apply: Annotated[bool, Parameter(help="Write the catalog (hold comments-catalog-write)")] = False,
    agency: Annotated[tuple[str, ...], Parameter(help="Only these agency codes")] = (),
    receipt_dir: Annotated[Path | None, Parameter(help="Where each agency's prior values are kept")] = None,
    restore: Annotated[bool, Parameter(help="Put back the receipts' prior values")] = False,
) -> None:
    """Re-spell the comments catalog's pdf_extraction_results_json digests, or restore them from receipts."""
    if restore:
        if receipt_dir is None:
            raise ValueError("--restore reads --receipt-dir")
        _print(restore_comment_digests(receipt_dir))
        return
    _print(respell_comment_digests(agencies=agency, apply=apply and not dry_run_, receipt_dir=receipt_dir))


@app.command(name="stage-suppression")
def stage_suppression_command(
    *,
    dry_run_: Annotated[bool, Parameter(name="--dry-run", help="The measurement is all this does")] = True,
    prior_dir: Annotated[Path | None, Parameter(help="Read congress_bills and bill_actions from here")] = None,
) -> None:
    """Count the published stages the running rule moves: the stage_changed events the adopting runs do not emit."""
    from spicy_regs.sources import publication, r2

    if prior_dir is not None:
        _print(stage_suppression(prior_dir / "congress_bills.parquet", prior_dir / "bill_actions.parquet"))
        return
    public_url = os.environ.get("R2_PUBLIC_URL")
    if not public_url:
        raise RuntimeError("stage-suppression reads the live generation through R2_PUBLIC_URL, or --prior-dir")
    with TemporaryDirectory(prefix="stage-suppression-") as scratch, publication.snapshot(public_url):
        paths = {table: Path(scratch) / f"{table}.parquet" for table in ("congress_bills", "bill_actions")}
        for table, path in paths.items():
            r2.download(f"{table}.parquet", path)
        _print(stage_suppression(paths["congress_bills"], paths["bill_actions"]))


@app.command(name="restore")
def restore_command(
    index: Path,
    family: str,
    *tables: str,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Build and verify a local generation only")] = True,
) -> None:
    """Republish TABLES of FAMILY exactly as the saved publication INDEX (publication.v2.json) pins them."""
    saved = json.loads(index.read_text())
    restore_rollup(saved, family, tables)(output_dir=output_dir, skip_upload=skip_upload).run()


def main() -> None:
    """The CLI entry: the environment's ``.env`` is read here, never at import."""
    from dotenv import load_dotenv

    load_dotenv()
    app()


if __name__ == "__main__":
    main()
