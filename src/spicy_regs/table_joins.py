"""The published tables' cross-table joins: declared once, with the measured baseline each is held to.

A join names a child table and columns and the parent table and columns they
reference. It resolves when a distinct non-null child key occurs in the parent.
Each carries the rate measured on the live fork tables on 2026-09-26
(``BASELINE_RECEIPTS``); its floor is that rate truncated to four decimals, and
``scripts/check_table_joins.py`` fails a join that falls below it. Joins that
are low by design, or by a table's current selection, are declared with that
rate and the reason, so the check still covers them. ``spicy-regs-dict
generate`` bundles them as ``table_joins.json`` beside ``table_metadata.json``
and ``check`` refuses a stale copy. ``references`` is the measurement-free shape
a SpicyDocs contract can adopt. Standard library only: the MCP image imports it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

RECORD = Path(__file__).with_name("table_joins.json")
RECORD_FORMAT = "spicy-regs-table-joins"
BASELINE_DATE = "2026-09-26"
BASELINE_RECEIPTS = (
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/results.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/rule-targets-baseline.json",
)

#: Expected resolution. ``complete``: every key should resolve and an orphan is
#: a defect. ``scope``: the parent holds a narrower selection than the child
#: references. ``design``: the columns deliberately hold values the parent does
#: not key. ``empty``: the child publishes no key yet.
KINDS = ("complete", "scope", "design", "empty")


@dataclass(frozen=True)
class Join:
    child: str
    child_columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]
    baseline_keys: int
    baseline_missing: int
    kind: str = "complete"
    reason: str = ""
    #: A table read in the child's place when it carries the same distinct keys
    #: more cheaply (the comments index for the 26M-row comments table).
    measured_via: str | None = None

    @property
    def name(self) -> str:
        return f"{self.child}.{'+'.join(self.child_columns)} -> {self.parent}.{'+'.join(self.parent_columns)}"

    @property
    def baseline_pct(self) -> float | None:
        if not self.baseline_keys:
            return None
        return 100 * (self.baseline_keys - self.baseline_missing) / self.baseline_keys

    @property
    def floor_pct(self) -> float | None:
        """The baseline truncated to four decimals; ``None`` when the child had no keys to resolve."""
        pct = self.baseline_pct
        return None if pct is None else math.floor(pct * 10_000) / 10_000


def _join(child: str, child_columns: str | tuple[str, ...], parent: str, parent_columns: str | tuple[str, ...],
          keys: int, missing: int, kind: str = "complete", reason: str = "", measured_via: str | None = None) -> Join:
    as_tuple = lambda columns: (columns,) if isinstance(columns, str) else columns  # noqa: E731
    return Join(child, as_tuple(child_columns), parent, as_tuple(parent_columns), keys, missing, kind, reason,
                measured_via)


_MODEL_OUTPUT = "Model output not yet computed (needs GEMINI_API_KEY); the table publishes no rows."
_BILL = "congress_bills"

JOINS: tuple[Join, ...] = (
    # The bill family and the tables that name its bills.
    _join("bill_actions", "bill_id", _BILL, "bill_id", 38_559, 0),
    _join("bill_committee_actions", "bill_id", _BILL, "bill_id", 3_009, 16,
          reason="16 117th-Congress bills (e.g. 117-hr-8593) are absent from congress_bills; traced by the ledger "
                 "session."),
    _join("bill_committees", "bill_id", _BILL, "bill_id", 37_473, 0),
    _join("bill_publisher_summaries", "bill_id", _BILL, "bill_id", 17_839, 0),
    _join("bill_sections", "bill_id", _BILL, "bill_id", 1_849, 0),
    _join("bill_subjects", "bill_id", _BILL, "bill_id", 149_261, 0),
    _join("bill_summaries", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("bill_versions", "bill_id", _BILL, "bill_id", 38_230, 0),
    _join("bill_vote_references", "bill_id", _BILL, "bill_id", 1_036, 0),
    _join("cbo_cost_estimates", "bill_id", _BILL, "bill_id", 2_488, 0),
    _join("committee_reports", "bill_id", _BILL, "bill_id", 137, 0),
    _join("diff_summaries", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("financial_changes", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("hearing_bill_links", "bill_id", _BILL, "bill_id", 85, 0),
    _join("hearing_transcripts", "bill_id", _BILL, "bill_id", 0, 0, "design",
          "Always NULL: a hearing covers a list of bills, which hearing_bill_links hosts one row per bill."),
    _join("laws", "bill_id", _BILL, "bill_id", 113, 0),
    _join("press_releases", "bill_id", _BILL, "bill_id", 3, 0),
    _join("public_activity_events", "bill_id", _BILL, "bill_id", 38_564, 0),
    _join("roll_call_votes", "bill_id", _BILL, "bill_id", 509, 0),
    _join("section_classifications", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("section_diff_items", "bill_id", _BILL, "bill_id", 223, 0),
    _join("section_diffs", "bill_id", _BILL, "bill_id", 223, 0),
    _join("bill_sections", ("bill_id", "version_code"), "bill_versions", ("bill_id", "version_code"), 2_397, 0),
    _join("section_diffs", ("bill_id", "from_version_code"), "bill_versions", ("bill_id", "version_code"), 548, 0),
    _join("section_diffs", ("bill_id", "to_version_code"), "bill_versions", ("bill_id", "version_code"), 548, 0),
    # People, votes and committees.
    _join("amendments", "sponsor_bioguide_id", "members", "bioguide_id", 187, 0),
    _join("congress_bills", "sponsor_bioguide_id", "members", "bioguide_id", 636, 0),
    _join("member_terms", "bioguide_id", "members", "bioguide_id", 12_770, 0),
    _join("member_votes", "bioguide_id", "members", "bioguide_id", 452, 0),
    _join("committee_assignments", "bioguide_id", "members", "bioguide_id", 532, 0),
    _join("member_votes", "vote_id", "roll_call_votes", "vote_id", 1_579, 0),
    _join("committee_assignments", "system_code", "committees", "system_code", 163, 5, "scope",
          "Five House codes (e.g. hsjp00, hsec00) are not in the committees roster; traced by the ledger session."),
    _join("bill_committees", "system_code", "committees", "system_code", 116, 0),
    _join("committee_meetings", "committee_system_code", "committees", "system_code", 212, 0),
    _join("hearing_bill_links", "committee_system_code", "committees", "system_code", 4, 0),
    _join("hearing_bill_links", "package_id", "hearing_transcripts", "package_id", 9, 0),
    _join("hearing_transcripts", "event_id", "committee_meetings", "event_id", 15, 7, "scope",
          "committee_meetings covers a narrower window than the hearings; traced by the ledger session."),
    _join("report_sections", "part_id", "committee_reports", "part_id", 142, 0),
    _join("law_code_sections", "law_id", "laws", "law_id", 70, 0),
    # Regulations.gov and the Federal Register.
    _join("documents", "docket_id", "dockets", "docket_id", 278_625, 137,
          reason="137 dockets have documents in the Mirrulations mirror but no docket record there."),
    _join("comments", "docket_id", "dockets", "docket_id", 60_215, 28, measured_via="comments_index",
          reason="28 dockets have comments in the Mirrulations mirror but no docket record there."),
    _join("fr_docket_links", "docket_id", "dockets", "docket_id", 612_342, 592_102, "design",
          "Raw publisher docket labels (many are agency docket numbers); the normalized bridge is "
          "rule_targets.docket_id -> dockets."),
    _join("rule_targets", "docket_id", "dockets", "docket_id", 143_004, 98,
          reason="Normalized FR-to-Regulations.gov bridge from the materialized rulemaking snapshot."),
    _join("fr_docket_links", "document_number", "federal_register", "document_number", 603_702, 0),
    _join("dockets", "rin", "unified_agenda", "rin", 14_421, 12_761, "scope",
          "unified_agenda holds a single edition, so most historical RINs have no row."),
    _join("federal_register", "rin", "unified_agenda", "rin", 35_859, 33_823, "scope",
          "unified_agenda holds a single edition, so most historical RINs have no row."),
    # FEC.
    _join("org_committee_links", "committee_id", "fec_committees", "committee_id", 3_633, 0),
    _join("fec_source_records", "collection_id", "fec_collections", "collection_id", 647, 0),
    # Courts.
    _join("court_opinions", "cluster_id", "court_opinion_clusters", "cluster_id", 10_069_107, 21,
          reason="21 cluster ids at the top of the id range are beyond the clusters table."),
    _join("court_citations", "cluster_id", "court_opinion_clusters", "cluster_id", 7_844_636, 63,
          reason="63 cluster ids at the top of the id range are beyond the clusters table."),
    _join("court_dockets", "cl_docket_id", "court_opinion_clusters", "cl_docket_id", 11_475, 9_214, "design",
          "court_dockets is a PACER docket selection; most PACER dockets have no published opinion."),
    # Federal spending and registration.
    _join("usaspending_recipients", "uei", "sam_entities", "uei", 7_274, 5_269, "scope",
          "sam_entities holds only the registration years loaded so far."),
)

#: Tables outside the dictionary's registry, read from the materialized rulemaking snapshot.
MATERIALIZED_TABLES = frozenset({"rule_targets"})


def joins_for(table: str) -> dict[str, list[dict]]:
    """The declared joins where ``table`` is the child (``outgoing``) or the parent (``incoming``)."""
    return {
        "outgoing": [record(join) for join in JOINS if join.child == table],
        "incoming": [record(join) for join in JOINS if join.parent == table],
    }


def record(join: Join) -> dict:
    """One join as the bundled record states it."""
    return {
        "child": join.child,
        "child_columns": list(join.child_columns),
        "parent": join.parent,
        "parent_columns": list(join.parent_columns),
        "kind": join.kind,
        "reason": join.reason,
        "measured_via": join.measured_via,
        "baseline_keys": join.baseline_keys,
        "baseline_missing": join.baseline_missing,
        "floor_pct": join.floor_pct,
    }


def references() -> dict[str, list[dict]]:
    """Per child table, ``(child_columns, parent_table, parent_columns)`` with no measurement: the contract shape."""
    shaped: dict[str, list[dict]] = {}
    for join in JOINS:
        shaped.setdefault(join.child, []).append({
            "child_columns": list(join.child_columns),
            "parent_table": join.parent,
            "parent_columns": list(join.parent_columns),
        })
    return shaped


def joins_record() -> dict:
    """The bundled ``table_joins.json`` document."""
    return {
        "format": RECORD_FORMAT,
        "version": 1,
        "baseline": {"date": BASELINE_DATE, "receipts": list(BASELINE_RECEIPTS)},
        "kinds": list(KINDS),
        "joins": [record(join) for join in JOINS],
        "references": references(),
    }


def declaration_errors(schemas: dict[str, list[tuple[str, str]]]) -> list[str]:
    """Each join names known tables and columns, a known kind, and no duplicate."""
    errors: list[str] = []
    seen: set[str] = set()
    for join in JOINS:
        if join.name in seen:
            errors.append(f"duplicate join declaration: {join.name}")
        seen.add(join.name)
        if join.kind not in KINDS:
            errors.append(f"{join.name}: unknown kind {join.kind!r}")
        if len(join.child_columns) != len(join.parent_columns):
            errors.append(f"{join.name}: child and parent column counts differ")
        if join.kind != "complete" and not join.reason:
            errors.append(f"{join.name}: a {join.kind} join must state its reason")
        if not 0 <= join.baseline_missing <= join.baseline_keys:
            errors.append(f"{join.name}: baseline missing count outside 0..keys")
        if (join.kind == "empty") != (join.baseline_keys == 0) and join.kind != "design":
            errors.append(f"{join.name}: an empty join has no baseline keys, and only an empty join may")
        for table, columns in ((join.child, join.child_columns), (join.parent, join.parent_columns)):
            if table in MATERIALIZED_TABLES:
                continue
            known = {name for name, _ in schemas.get(table, [])}
            if not known:
                errors.append(f"{join.name}: {table} is not a dictionary table")
            errors.extend(f"{join.name}: {table}.{column} is not a declared column"
                          for column in columns if known and column not in known)
        if join.measured_via and join.measured_via not in schemas:
            errors.append(f"{join.name}: measured_via {join.measured_via} is not a dictionary table")
    return errors
