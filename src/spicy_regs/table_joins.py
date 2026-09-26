"""The published tables' cross-table joins: declared once, with the measured baseline each is held to.

A join names a child table and columns and the parent table and columns they
reference. It resolves when a distinct non-null child key occurs in the parent.
Each carries the rate measured on the live fork tables on 2026-09-26
(``BASELINE_RECEIPTS``); its floor is that rate truncated to four decimals, and
``scripts/check_table_joins.py`` fails a join that falls below it. Joins that
are low by design, or by a table's current selection, are declared with that
rate and the reason, so the check still covers them. ``spicy-regs-dict
generate`` bundles them as ``table_joins.json`` beside ``table_metadata.json``
and ``check`` refuses a stale copy. ``references`` is the measurement-free
shape; SpicyDocs 0.36.0 contracts state the joins onto a parent's identity as
``TableContract.references``, and ``tests/test_table_joins.py`` holds each of
those to a join declared here. Standard library only: the MCP image imports it.
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
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/unified-agenda-rin-after-backfill.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/dockets-after-gap-fill.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/sam-entities-after-backfill.json",
)

#: Expected resolution. ``complete``: every key should resolve and an orphan is
#: a defect. ``scope``: the parent holds a narrower selection than the child
#: references. ``design``: the columns deliberately hold values the parent does
#: not key. ``empty``: the child publishes no key yet.
KINDS = ("complete", "scope", "design", "empty")

#: A complete join reports up to this many new orphans, or this share of its
#: keys if larger, as LAG instead of failing (see ``Join.lag_allowance``).
LAG_MIN = 3
LAG_SHARE = 0.0001


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

    def lag_allowance(self, keys: int) -> int:
        """New orphans a complete join tolerates as lag before it fails.

        A growing child can name a key its parent publishes a run later (a
        report citing a bill BILLSTATUS has not served yet, a document naming a
        docket before the gap fill reaches it), and failing on each such orphan
        trains people to ignore the check. Scope and design joins keep the pure
        rate floor: their missing count grows with the child by definition.
        """
        return max(LAG_MIN, math.ceil(keys * LAG_SHARE)) if self.kind == "complete" else 0


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
          reason="11 are real 117th-Congress bills congress_bills lacks: its rows below the 118th are the retired "
                 "list walk's, which misses 1,299 BILLSTATUS bills across the 108th-117th (199 in the 117th) until "
                 "the bill family is run for them. 2 are Senate reports keyed to their filing Congress, fixed at "
                 "9818678 and re-keyed by the print-citation runs; 2 are bills a House report prints under a "
                 "'116th Congress' subheading, keyed to the report's Congress; 1 (118-hr-14106) is the publisher's "
                 "misprint. Receipt join-gaps-2026-09-26/d/."),
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
    _join("committee_assignments", "system_code", "committees", "system_code", 163, 5, "design",
          "28 seats have no Congress.gov code to join and are never matched by name: 25 House seats on the "
          "Clerk's joint committees EC00, IT00, JL00 and JP00 (typed joint with no Congress.gov code, so they keep "
          "the legacy hs spelling; the Joint Economic Committee has three Congress.gov codes) and 3 Senate seats on "
          "JSIK00, the 2024 inaugural committee, which Congress.gov lists in no Congress. Receipt "
          "join-gaps-2026-09-26/e/."),
    _join("bill_committees", "system_code", "committees", "system_code", 116, 0),
    _join("committee_meetings", "committee_system_code", "committees", "system_code", 212, 0),
    _join("hearing_bill_links", "committee_system_code", "committees", "system_code", 4, 0),
    _join("hearing_bill_links", "package_id", "hearing_transcripts", "package_id", 9, 0),
    _join("hearing_transcripts", "event_id", "committee_meetings", "event_id", 15, 7,
          reason="The 7 are 118th-Congress House meetings Congress.gov updated in 2026 when their transcripts "
                 "printed; committee_meetings listed only the current Congress. It now keeps the previous one, and "
                 "its next run lists them. Receipt join-gaps-2026-09-26/f/."),
    _join("report_sections", "part_id", "committee_reports", "part_id", 142, 0),
    _join("law_code_sections", "law_id", "laws", "law_id", 70, 0),
    # Regulations.gov and the Federal Register.
    _join("documents", "docket_id", "dockets", "docket_id", 278_651, 114,
          reason="The mirror lacks these dockets' records, and fill-docket-gaps asked the Regulations.gov API for "
                 "each (docket_gap_outcomes.parquet, re-asked after 30 days): 47 answer 404, which the publisher "
                 "does not publish, and 67 answer 400 Invalid ID, legacy -RULEMAKING/-NONRULEMAKING ids whose "
                 "unsuffixed forms it does not serve either. It served 23 more, merged 2026-09-26. Receipt "
                 "join-map-2026-09-26/dockets-after-gap-fill.json."),
    _join("comments", "docket_id", "dockets", "docket_id", 60_221, 28, measured_via="comments_index",
          reason="Asked of the Regulations.gov API by fill-docket-gaps like documents' orphans: 6 answer 404 and 22 "
                 "answer 400 Invalid ID (legacy -RULEMAKING/-NONRULEMAKING ids)."),
    _join("fr_docket_links", "docket_id", "dockets", "docket_id", 612_342, 592_102, "design",
          "Raw publisher docket labels (many are agency docket numbers); the normalized bridge is "
          "rule_targets.docket_id -> dockets."),
    _join("rule_targets", "docket_id", "dockets", "docket_id", 143_012, 83,
          reason="Normalized FR-to-Regulations.gov bridge from the materialized rulemaking snapshot. Each orphan "
                 "is one fill-docket-gaps asked for: 16 answer 404 and 67 answer 400 Invalid ID."),
    _join("fr_docket_links", "document_number", "federal_register", "document_number", 603_702, 0),
    _join("dockets", "rin", "unified_agenda", "rin", 14_421, 994, "scope",
          "unified_agenda holds every readable edition, Fall 1995 to the newest (not Spring 1995 or Spring 2012, "
          "never published, nor the two 2004 editions SpicyDocs refuses). Of the 994 RINs no edition lists, 536 "
          "are NMFS's 0648-X series, which it assigns to in-season actions and never puts on the agenda, 115 name "
          "2026 dockets newer than the newest edition, and 9 are malformed. Receipt "
          "join-map-2026-09-26/unified-agenda-rin-after-backfill.json."),
    _join("federal_register", "rin", "unified_agenda", "rin", 35_859, 6_090, "scope",
          "unified_agenda holds every readable edition, Fall 1995 to the newest. Of the 6,090 RINs no edition "
          "lists, 2,211 are NMFS's 0648-X in-season series (every one), 1,409 first appear before Fall 1995, 179 "
          "first appear in 2026, after the newest edition, and 460 are malformed as printed (3206-XXXX, "
          "7100 AG80). Receipt join-map-2026-09-26/unified-agenda-rin-after-backfill.json."),
    # FEC.
    _join("org_committee_links", "committee_id", "fec_committees", "committee_id", 3_633, 0),
    _join("fec_source_records", "collection_id", "fec_collections", "collection_id", 647, 0),
    # Courts.
    _join("court_opinions", "cluster_id", "court_opinion_clusters", "cluster_id", 10_069_107, 21, "scope",
          "The publisher cuts each export at a different hour, clusters first (2026-06-30: 08:16 UTC, opinions "
          "09:56), so 21 opinions name newer clusters. All 21 are RECAP trial-court opinions that opinion search, "
          "the clusters' catch-up, does not index, so they resolve only with the next export. Receipt "
          "join-gaps-2026-09-26/i/."),
    _join("court_citations", "cluster_id", "court_opinion_clusters", "cluster_id", 7_844_636, 63, "scope",
          "The publisher cuts citations hours after clusters (2026-06-30: 19:14 vs 08:16 UTC), so 63 name newer "
          "clusters. The clusters' id-keyed catch-up adds the 43 search indexes; the other 20 were merged away "
          "since or are not indexed. Re-record at 20 once that catch-up is published. Receipt "
          "join-gaps-2026-09-26/i/."),
    _join("court_dockets", "cl_docket_id", "court_opinion_clusters", "cl_docket_id", 11_475, 9_214, "design",
          "court_dockets is a PACER docket selection; most PACER dockets have no published opinion."),
    # Federal spending and registration.
    _join("usaspending_recipients", "uei", "sam_entities", "uei", 7_274, 884, "scope",
          "sam_entities holds SAM's public active registrations, every registration year from 1996 plus two "
          "older ones. A recipient can name a UEI with none: of a 40-UEI sample of these orphans, the entity API "
          "held no public record for 38, one was Inactive, and one was activated after the 2026 year was then "
          "last read. Receipts join-map-2026-09-26/sam-entities-after-backfill.json and "
          "usaspending-unresolved-uei-sample-2026-09-26.txt."),
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
