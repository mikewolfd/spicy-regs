"""Look up already parsed citation keys in explicitly selected target tables.

This module does not extract citations or reinterpret ``target_resolved``. That
legacy field means a spelling was settled, not that a hosted record exists.
Only trusted table/column declarations form SQL; occurrence keys are parameters.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from spicy_regs.identifiers import normalize_rin, section_order_sql
from spicy_regs.citation_sources import TEXT_SOURCES
from spicy_regs.gao_citation_keys import CITATION_PRODUCT_SQL

#: /2: a usc_section citation is looked up in the published U.S. Code (release point sections) rather than in
#: the current Congress's OLRC classification rows, so ``found`` means the release point prints the section.
RESOLUTION_RULE = "selected-target-lookup/2"
#: The rule native_legal_references records: migrated to /2 (:data:`NATIVE_ROUTES`) so printed release-point
#: sections and range stubs resolve identically to ``resolve_document_citations``.
NATIVE_RESOLUTION_RULE = RESOLUTION_RULE
#: Each ``document_citations.document_kind`` a writer emits, to the table holding its documents: the print-citations
#: rollup's two GovInfo families (spicy-docs ``GOVINFO_PACKAGE``, ``BUDGET_VOLUME``; literal here because the server
#: does not require spicy-docs) and every held-field kind. Its keys are the kinds the citation tool accepts.
SOURCE_TABLES = {"govinfo_package": "house_activity_reports", "budget_volume": "budget_volumes",
                 **{kind: source.table for kind, source in TEXT_SOURCES.items()}}
#: Each ``document_citations.cite_kind`` a writer emits: spicy-docs ``DOCUMENT_CITATION_KINDS``, literal for the same
#: reason, and held equal to it by ``tests/test_citation_parity.py``. A kind without a route (case_docket_number)
#: is held but never looked up.
CITE_KINDS = ("bill_number", "public_law", "statutes_at_large", "usc_section", "cfr_section", "federal_register_cite",
              "rin", "gao_product_id", "crs_report_id", "docket_number", "case_docket_number", "us_reports_cite",
              "committee_name")


@dataclass(frozen=True)
class Route:
    table: str
    key_sql: str
    identity: tuple[str, ...]
    expected_cardinality: str = "one"
    predicate: str = "true"
    grain: str = "one target record"
    #: Columns ``predicate`` reads that an older generation of ``table`` may lack; lacking one, every keyed row counts.
    predicate_columns: tuple[str, ...] = ()
    #: A second way a row matches a key: SQL over the row ``t`` and the key ``r.lookup_key``, true where the key falls
    #: inside a range the row states. A row the key matches this way only is stated as such (``match_basis``).
    range_sql: str | None = None
    #: Columns ``range_sql`` reads that an older generation may lack; lacking one, a key matches ``key_sql`` alone.
    range_columns: tuple[str, ...] = ()
    #: What ``match_basis`` says when the rows matched by key only, by range only, or both.
    match_bases: tuple[str, str, str] = ("first_page", "page_range", "first_page_and_page_range")
    #: The published table whose pin stands for ``table`` when ``table`` is a view over it.
    pinned_by: str | None = None


ROUTES = {
    "bill_number": Route("congress_bills", "bill_id", ("bill_id",)),
    "public_law": Route("laws", "law_id", ("congress", "law_type", "number")),
    # A pinpoint inside a law: its first page, or a page within its range where laws states the last one (M8).
    "statutes_at_large": Route("laws", "statutes_at_large_volume || '-' || statutes_at_large_page",
                               ("congress", "law_type", "number"),
                               grain="a law, by the first page of its Statutes at Large citation or, where laws states "
                                     "its last page, by a page within that range in the cited volume (match_basis "
                                     "says which); a page two laws share reads ambiguous, both listed",
                               range_sql="t.statutes_at_large_last_page IS NOT NULL "
                                         "AND split_part(r.lookup_key, '-', 1) = t.statutes_at_large_volume "
                                         "AND TRY_CAST(split_part(r.lookup_key, '-', 2) AS INTEGER) "
                                         "BETWEEN TRY_CAST(t.statutes_at_large_page AS INTEGER) "
                                         "AND TRY_CAST(t.statutes_at_large_last_page AS INTEGER)",
                               range_columns=("statutes_at_large_last_page",)),
    # A section the held OLRC release point prints, or one inside a range stub it prints (match_basis says which).
    "usc_section": Route("usc_release_point_sections", "usc_title || '-' || usc_section_key",
                         ("usc_title", "usc_section_key"), "many",
                         grain="a section the held OLRC release point prints, or a range stub (low...high, usually "
                               "repealed or omitted sections) spanning it; not Code text or current applicability. "
                               "Missing means that release point does not print it: a later law, an appendix title "
                               "or an annual edition may (usc_citation_verdicts)",
                         range_sql="t.range_low IS NOT NULL "
                                   "AND t.usc_title = regexp_extract(r.lookup_key, '^([^-]+)-', 1) AND "
                                   + section_order_sql("regexp_extract(r.lookup_key, '^[^-]+-(.+)$', 1)")
                                   + " BETWEEN t.range_low AND t.range_high",
                         match_bases=("printed_section", "range_stub", "printed_section_and_range_stub"),
                         pinned_by="uscode_edition_sections"),
    # A part citation (2-200) keys every structural row of the part; its own granule is the target.
    "cfr_section": Route("cfr_sections", "cfr_ref", ("package_id", "granule_id"),
                         predicate="section IS NOT NULL OR part_granule = 'true'",
                         grain="a section's granule, or a part's own granule, per held annual edition: a target "
                               "held in two editions or split across volumes reads ambiguous; package_id names each",
                         predicate_columns=("part_granule",)),
    "federal_register_cite": Route("federal_register", "volume || '-' || start_page",
                                   ("document_number", "publication_date")),
    "rin": Route("unified_agenda", "rin", ("rin", "agenda_edition"), "many", grain="agenda editions for a RIN"),
    # spicy-docs keys a GAO citation on the printed number, upper-cased; a row stating none falls back to its slug.
    "gao_product_id": Route("gao_reports", CITATION_PRODUCT_SQL, ("report_id",)),
    # A report citation still names one report when its table retains several dated revisions.
    "crs_report_id": Route("crs_reports", "report_id", ("report_id",),
                           predicate="t.update_date = (SELECT max(r.update_date) FROM crs_reports r "
                                     "WHERE r.report_id = t.report_id)",
                           predicate_columns=("update_date",), grain="latest retained revision of a CRS report"),
    "docket_number": Route("dockets", "docket_id", ("docket_id",)),
    "committee_name": Route("committees", "system_code", ("system_code",)),
    "us_reports_cite": Route("court_citations", "volume || '-' || page", ("cluster_id",),
                        predicate="reporter = 'U.S.'", grain="CourtListener clusters named by U.S. reporter citations"),
}
#: Before /2, a usc_section citation named the OLRC classification rows. Kept so carried /1 rows in
#: native_legal_references retain their typed candidate keys (congress, session, seq) and explorer navigation
#: keeps linking legacy candidates recorded against law_code_sections.
LEGACY_USC_SECTION_ROUTE = Route(
    "law_code_sections", "usc_title || '-' || usc_section_key", ("congress", "session", "seq"), "many",
    grain="OLRC classification rows naming this section; not hosted Code text. A section no row classifies may "
          "still be in the Code: usc_citation_verdicts says whether the held release point prints it")
NATIVE_ROUTES = dict(ROUTES)

#: Statutory lookup routes backed by RefSpec's SHA-256-pinned vendored Parquet tables. Kept separate from
#: :data:`ROUTES` / :data:`CITE_KINDS` so document_citations writer-parity contracts stay invariant.
ACT_SECTION_ROUTE = Route(
    "usc_act_index",
    "act_key || ':' || act_section",
    ("usc_title", "usc_section"),
    "one",
    grain="a U.S. Code section resolved from an Act-relative citation via OLRC Popular Names, Table III, "
          "and U.S. Code source credits using composite (law, division, enclosing_section) identity; "
          "ambiguous or disagreeing sources refuse explicitly",
    pinned_by="usc_act_sections",
)
USC_DISPOSITION_ROUTE = Route(
    "usc_disposition_tables",
    "former_title || '-' || former_section",
    ("title", "section"),
    "many",
    grain="source-backed disposition of a former U.S.C. section across all 9 pinned recodification / "
          "reclassification tables (8 families); preserves all 1-to-many successors and verbatim caveats",
)
STATUTORY_ROUTES = {**ROUTES, "act_section": ACT_SECTION_ROUTE, "usc_disposition": USC_DISPOSITION_ROUTE}

_USC_KEY_RE = re.compile(r"^([1-9][0-9]*)([a-zA-Z]?)-(.+)$")
_FORMER_TITLE_RE = re.compile(r"^([1-9][0-9]*)\s*(A|App\.?)?$", re.IGNORECASE)
_ACT_PHRASE_BEFORE_RE = re.compile(
    r"^\s*(?:div(?:ision)?\.?\s+(?P<div>[A-Z]{1,3})\s+of\s+(?:the\s+)?)?"
    r"(?P<act>.+?)\s+(?:sec(?:tion)?s?\.?|§{1,2})\s*"
    r"(?P<sec>(?:\d+[A-Za-z]*\[[^\[\]]+\]|\d+[A-Za-z]*(?:\([^()]+\))*))\s*$",
    re.IGNORECASE,
)
_ACT_PHRASE_AFTER_RE = re.compile(
    r"^\s*(?:sec(?:tion)?s?\.?|§{1,2})\s*"
    r"(?P<sec>(?:\d+[A-Za-z]*\[[^\[\]]+\]|\d+[A-Za-z]*(?:\([^()]+\))*))\s+"
    r"(?:of\s+(?:the\s+)?|,\s*(?:the\s+)?)"
    r"(?:div(?:ision)?\.?\s+(?P<div>[A-Z]{1,3})\s+of\s+(?:the\s+)?)?(?P<act>.+?)\s*$",
    re.IGNORECASE,
)


def _snapshot(snapshots: Mapping[str, Mapping[str, Any]], route: Route) -> Mapping[str, Any] | None:
    """The pin of the route's table, or of the published table a view route reads (``pinned_by``)."""
    return snapshots.get(route.table) or (snapshots.get(route.pinned_by) if route.pinned_by else None)


def _occurrence_key(item: Mapping[str, Any]) -> str:
    fields = ("document_kind", "document_key", "text_sha256", "cite_kind", "target_key", "span_start")
    return hashlib.sha256(json.dumps([item.get(k) for k in fields], ensure_ascii=False).encode()).hexdigest()


def _holds(cursor: Any, route: Route, columns: tuple[str, ...]) -> bool:
    """Whether the selected table has every one of ``columns`` (an older generation can predate one)."""
    if not columns:
        return True
    held = {column[0] for column in cursor.execute(f'SELECT * FROM "{route.table}" LIMIT 0').description}
    return set(columns) <= held


def _predicate(cursor: Any, route: Route) -> str:
    """The route's predicate, or every keyed row when the selected table predates a column the predicate reads."""
    return route.predicate if _holds(cursor, route, route.predicate_columns) else "true"


def _lookup(cursor: Any, route: Route, keys: list[str], max_candidates: int) -> dict[str, dict]:
    """One parameterized read per distinct-key batch; retain bounded candidates and actual match counts.

    A route with a range form also matches a row whose range holds the key (and whose key is not it), and each
    result says how its rows matched (``route.match_bases``).
    """
    predicate = _predicate(cursor, route)
    columns = ', '.join(f'CAST(t."{c}" AS VARCHAR) AS "{c}"' for c in route.identity)
    order = ', '.join(f'"{c}"' for c in route.identity)
    keyed = f"CAST(({route.key_sql}) AS VARCHAR)=r.lookup_key"
    ranged = "" if route.range_sql is None or not _holds(cursor, route, route.range_columns) else f"""
                    UNION ALL SELECT r.lookup_key, {columns}, true AS by_range
                    FROM requested r JOIN "{route.table}" t ON ({route.range_sql}) AND NOT coalesce({keyed}, false)
                    WHERE {predicate}"""
    sql = f"""WITH requested AS (SELECT unnest(?::VARCHAR[]) AS lookup_key),
        matches AS (SELECT r.lookup_key, {columns}, false AS by_range
                    FROM requested r JOIN "{route.table}" t ON {keyed}
                    WHERE {predicate}{ranged}),
        ranked AS (SELECT *, count(*) OVER (PARTITION BY lookup_key) AS match_count,
                   bool_or(by_range) OVER (PARTITION BY lookup_key) AS any_range,
                   bool_and(by_range) OVER (PARTITION BY lookup_key) AS all_range,
                   row_number() OVER (PARTITION BY lookup_key ORDER BY {order}) AS candidate_index FROM matches)
        SELECT * EXCLUDE(candidate_index, by_range) FROM ranked WHERE candidate_index <= ?
        ORDER BY lookup_key, {order}"""
    found: dict[str, dict] = {}
    for lookup_key, *identity, count, any_range, all_range in cursor.execute(sql, [keys, max_candidates]).fetchall():
        result = found.setdefault(lookup_key, {"candidate_keys": [], "match_count": int(count)})
        if route.range_sql is not None:
            by_key, by_range, both = route.match_bases
            result["match_basis"] = both if any_range and not all_range else by_range if any_range else by_key
        candidate = dict(zip(route.identity, identity))
        if candidate not in result["candidate_keys"]:
            result["candidate_keys"].append(candidate)
    return found


def _parse_former_title(former_title: int | str, appendix: bool) -> tuple[int, bool]:
    if isinstance(former_title, bool):
        raise ValueError(f"invalid U.S.C. title: {former_title!r}")
    if isinstance(former_title, int):
        if former_title < 1:
            raise ValueError(f"invalid U.S.C. title: {former_title!r}")
        return former_title, appendix
    match = _FORMER_TITLE_RE.fullmatch(str(former_title or "").strip())
    if match is None:
        raise ValueError(f"invalid U.S.C. title: {former_title!r}")
    return int(match.group(1)), appendix or bool(match.group(2))


def _serialize_disposition(disp: Any, tables: Any, *, include_provenance: bool = True) -> dict[str, Any]:
    from spicy_regs.ontology.act_resolution import canonical_usc_iri
    from spicy_regs.ontology.usc_dispositions import DISPOSITION_RULE, disposition_table_digests

    def _succ_dict(s: Any) -> dict[str, Any]:
        try:
            iri: str | None = canonical_usc_iri(s.title, s.section)
        except ValueError:
            iri = None
        return {
            "title": s.title,
            "section": s.section,
            "subsection": s.subsection,
            "status": s.status,
            "target_key": f"{s.title}-{s.section}",
            "iri": iri,
        }

    def _row_dict(r: Any) -> dict[str, Any]:
        return {
            "former_section": r.former_section,
            "former_subsection": r.former_subsection,
            "former_note": r.former_note,
            "status": r.status,
            "former_text": r.former_text,
            "new_text": r.new_text,
            "page": r.page,
            "column": r.column,
            "successor": _succ_dict(r.successor) if r.successor is not None else None,
        }

    payload: dict[str, Any] = {
        "route": "usc_disposition",
        "rule": DISPOSITION_RULE,
        "former_title": disp.former_title,
        "former_section": disp.former_section,
        "former_section_end": disp.former_section_end,
        "subsection": list(disp.subsection),
        "verdict": disp.verdict,
        "answered": disp.answered,
        "recodification": disp.recodification,
        "subsection_resolved": disp.subsection_resolved,
        "covers": list(disp.covers),
        "caveats": list(disp.caveats),
        "successors": [_succ_dict(s) for s in disp.successors],
        "rows": [_row_dict(r) for r in disp.rows],
    }
    if disp.members:
        payload["members"] = [_serialize_disposition(m, tables, include_provenance=False) for m in disp.members]
    if include_provenance:
        rec_names = [name for name in (disp.recodification or "").split("+") if name]
        rec_meta = []
        for name in rec_names:
            rec = tables.recodification_metadata(name)
            if rec is not None:
                rec_meta.append({
                    "name": rec.name,
                    "table": rec.table,
                    "table_sha256": rec.digest,
                    "enacted_by": rec.enacted_by,
                    "deeming_provision": rec.deeming_provision,
                    "is_editorial_reclassification": rec.is_editorial_reclassification,
                    "enacted_title": rec.enacted_title,
                    "covers_appendix": rec.covers_appendix,
                    "source_url": rec.source_url,
                    "source_digest": rec.source_digest,
                    "source_bytes": rec.source_bytes,
                })
        all_pins = disposition_table_digests()
        relevant_pins = {item["table"]: item["table_sha256"] for item in rec_meta} if rec_meta else all_pins
        payload["provenance"] = {
            "rule": DISPOSITION_RULE,
            "recodifications": rec_meta,
            "table_pins": relevant_pins,
        }
    return payload


def resolve_usc_disposition(
    former_title: int | str,
    former_section: str,
    subsection: object = None,
    *,
    section_end: object = None,
    appendix: bool = False,
) -> dict[str, Any]:
    """Resolve a historical U.S.C. section against the 9 SHA-256-pinned disposition tables."""
    from spicy_regs.ontology.usc_dispositions import load_disposition_tables

    title_num, effective_appendix = _parse_former_title(former_title, appendix)
    tables = load_disposition_tables()
    disp = tables.disposition(
        title_num,
        str(former_section),
        subsection,
        section_end=section_end,
        appendix=effective_appendix,
    )
    out = _serialize_disposition(disp, tables)
    out["appendix"] = effective_appendix
    return out


def _disposition_for_target_key(key: str) -> dict[str, Any] | None:
    """Resolve a normalized ``usc_section`` target_key (e.g. ``'49A-1432'``, ``'10-593'``) via disposition tables."""
    match = _USC_KEY_RE.fullmatch(key.strip())
    if match is None:
        return None
    title_num = int(match.group(1))
    suffix = match.group(2).upper()
    if suffix and suffix != "A":
        return None
    appendix = suffix == "A"
    sec_raw = match.group(3)
    section_end: str | None = None
    subsection: str | None = None
    if "..." in sec_raw:
        sec_start, _, section_end = sec_raw.partition("...")
        sec_raw = sec_start
    if "(" in sec_raw:
        base, _, tail = sec_raw.partition("(")
        sec_raw = base
        subsection = f"({tail}"
    return resolve_usc_disposition(title_num, sec_raw, subsection, section_end=section_end, appendix=appendix)


def _parse_act_phrase(text: str) -> tuple[str, str, str | None, str | None] | None:
    for pattern in (_ACT_PHRASE_AFTER_RE, _ACT_PHRASE_BEFORE_RE):
        match = pattern.fullmatch(text)
        if match is not None:
            act_name = match.group("act").strip().strip(",;.")
            sec_raw = match.group("sec").strip()
            div = match.group("div")
            enclosing: str | None = None
            enc_match = re.fullmatch(r"([1-9]\d*[A-Za-z]*)\[([^\[\]]+)\]", sec_raw)
            if enc_match is not None:
                enclosing, sec_raw = enc_match.group(1), enc_match.group(2)
            return act_name, sec_raw, div.upper() if div else None, enclosing
    return None


def _serialize_act_resolution(res: Any, *, text: str | None = None) -> dict[str, Any]:
    from spicy_regs.ontology.act_resolution import (
        ALIAS_PRECEDENCE_RULE,
        ALIAS_YEAR_RULE,
        DIVISION_RULE,
        ENCLOSING_SOURCE_IDENTITY_RULE,
        SOURCE_COMPOSITION_RULE,
        act_resolution_pins,
    )

    target_key = f"{res.usc_title}-{res.usc_section}" if res.iri and res.usc_title and res.usc_section else None
    return {
        "route": "act_section",
        "rule": ENCLOSING_SOURCE_IDENTITY_RULE,
        "composition_rule": SOURCE_COMPOSITION_RULE,
        "input": {
            "text": text,
            "act_name": res.citation.act_name,
            "act_key": res.citation.act_key,
            "section": res.citation.section,
            "division": res.citation.division,
            "enclosing_section": res.citation.enclosing_section,
        },
        "resolved": res.iri is not None,
        "status": "resolved" if res.iri is not None else "refused",
        "iri": res.iri,
        "usc_title": res.usc_title,
        "usc_section": res.usc_section,
        "target_key": target_key,
        "act_key": res.act_key,
        "table3_key": res.table3_key,
        "act_division": res.act_division,
        "enclosing_section": res.enclosing_section,
        "answered_by": res.answered_by,
        "unresolved_reason": res.unresolved_reason,
        "table3_reason": res.table3_reason,
        "source_credit_status": res.source_credit_status,
        "statutes_at_large_volume": res.statutes_at_large_volume,
        "statutes_at_large_page": res.statutes_at_large_page,
        "conflicting_targets": dict(res.conflicting_targets),
        "table3_candidate_iri": res.table3_candidate_iri,
        "candidate_resolutions": [_serialize_act_resolution(c) for c in res.candidate_resolutions],
        "provenance": {
            "artifact": "usc-act-index-2026-08-22+usc-source-credit-index-2026-08-02",
            "rules": {
                "enclosing_source_identity": ENCLOSING_SOURCE_IDENTITY_RULE,
                "source_composition": SOURCE_COMPOSITION_RULE,
                "alias_precedence": ALIAS_PRECEDENCE_RULE,
                "alias_year": ALIAS_YEAR_RULE,
                "division": DIVISION_RULE,
            },
            "table_pins": act_resolution_pins(),
        },
    }


def resolve_act_citation(
    text: str | None = None,
    *,
    act_name: str | None = None,
    section: str | None = None,
    division: str | None = None,
    enclosing_section: str | None = None,
) -> dict[str, Any]:
    """Resolve a named-Act citation (text phrase or structured fields) via pinned OLRC tables."""
    from spicy_regs.ontology.act_resolution import (
        ActRelativeCitation,
        find_act_relative_occurrences,
        load_act_index,
        load_source_credit_index,
        normalize_popular_name,
        resolve_act_relative_citation,
    )

    index = load_act_index()
    credits = load_source_credit_index()

    if act_name is not None or section is not None:
        if not act_name or section is None or not str(section).strip():
            raise ValueError("structured act resolution requires both act_name and non-empty section")
        citation = ActRelativeCitation(
            act_name=act_name,
            act_key=normalize_popular_name(act_name),
            section=str(section).strip(),
            division=division,
            enclosing_section=enclosing_section,
        )
    elif text is not None:
        occs = find_act_relative_occurrences(text, act_names=index.act_names)
        if len(occs) == 1:
            base = occs[0].citation
            citation = ActRelativeCitation(
                act_name=base.act_name,
                act_key=base.act_key,
                section=base.section,
                division=division if division is not None else base.division,
                enclosing_section=enclosing_section if enclosing_section is not None else base.enclosing_section,
            )
        elif len(occs) > 1:
            raise ValueError(f"Expected a single act-relative citation in {text!r}, found {len(occs)}")
        else:
            parsed = _parse_act_phrase(text)
            if parsed is None:
                raise ValueError(f"Could not parse an act-relative citation from {text!r}")
            p_act, p_sec, p_div, p_enc = parsed
            citation = ActRelativeCitation(
                act_name=p_act,
                act_key=normalize_popular_name(p_act),
                section=p_sec,
                division=division if division is not None else p_div,
                enclosing_section=enclosing_section if enclosing_section is not None else p_enc,
            )
    else:
        raise ValueError("Provide either text or (act_name, section)")

    resolution = resolve_act_relative_citation(citation, index=index, source_credits=credits)
    return _serialize_act_resolution(resolution, text=text)


def resolve_citations(cursor: Any, occurrences: Sequence[Mapping[str, Any]],
                      target_snapshots: Mapping[str, Mapping[str, Any]], *,
                      source_digests: Mapping[tuple[str, str], str] | None = None,
                      max_target_keys: int = 500, max_candidates: int = 100,
                      routes: Mapping[str, Route] = ROUTES, rule: str = RESOLUTION_RULE,
                      enrich_dispositions: bool | None = None) -> dict:
    """Resolve current-text occurrences, reporting skipped and unread scope explicitly.

    Text citations require the held source digest. Native-field callers can omit
    text_sha256 and source_digests. Unknown/legacy-unversioned target selections
    remain not_checked; callers must supply a real target identity, not invent one.
    Partial parsed citations remain not_checked even if a coincidental key exists.
    ``routes`` and ``rule`` default to the tool's; native_legal_references passes its own.
    """
    if not 1 <= max_target_keys <= 10_000 or not 1 <= max_candidates <= 1_000:
        raise ValueError("Resolution bounds must be positive and within the supported limits")
    should_enrich_dispositions = (rule == RESOLUTION_RULE) if enrich_dispositions is None else enrich_dispositions
    outputs = []
    batches: dict[Route, dict[str, list[int]]] = defaultdict(dict)
    admitted: set[tuple[Route, str]] = set()
    act_indices: list[int] = []
    for item in occurrences:
        kind, key = str(item.get("cite_kind", "")), str(item.get("target_key") or "")
        route = routes.get(kind)
        snap = dict(_snapshot(target_snapshots, route) or {}) if route else None
        if kind == "act_section" and route is not None and not snap:
            from spicy_regs.ontology.act_resolution import act_resolution_pins
            snap = {"status": "vendored_refspec_pins", "table_pins": act_resolution_pins()}
        row = {**item, "occurrence_key": str(item.get("occurrence_key") or _occurrence_key(item)), "target_kind": kind, "normalized_key": key,
               "target_snapshot": snap,
               "target_status": "not_checked", "candidate_keys": [], "resolution_rule": rule,
               "expected_cardinality": route.expected_cardinality if route else None,
               "target_table_selected": route.table if route else None, "target_grain": route.grain if route else None,
               "source_status": "native_field", "reason": None, "match_count": None}
        outputs.append(row)
        if "text_sha256" in item:
            held = (source_digests or {}).get((str(item.get("document_kind", "")), str(item.get("document_key", ""))))
            if not held or not item.get("text_sha256"):
                row.update(source_status="unread_source", reason="source_digest_unavailable")
                continue
            if held != item["text_sha256"]:
                row.update(source_status="stale_source", reason="source_digest_mismatch")
                continue
            row["source_status"] = "current_text"
        if route is None:
            row.update(target_status="unsupported", reason="unsupported_citation_kind")
            continue
        if kind == "act_section":
            has_structured = bool(item.get("act_name") and item.get("section"))
            if (not key and not has_structured) or str(item.get("target_resolved", "true")).lower() != "true":
                row["reason"] = "unsettled_key"
                continue
            pair = (route, key or f"{item.get('act_name')}:{item.get('section')}")
            if pair not in admitted and len(admitted) >= max_target_keys:
                row["reason"] = "target_key_limit"
                continue
            admitted.add(pair)
            act_indices.append(len(outputs) - 1)
            continue
        if not key or str(item.get("target_resolved", "true")).lower() != "true":
            row["reason"] = "unsettled_key"
            continue
        if kind == "rin" and normalize_rin(key) != key:
            row["reason"] = "unsettled_key"
            continue
        snapshot = _snapshot(target_snapshots, route)
        if not snapshot or snapshot.get("status") in {"legacy_unversioned", "local_unversioned", "unavailable"}:
            row["reason"] = "target_snapshot_unavailable"
            continue
        pair = (route, key)
        if pair not in admitted and len(admitted) >= max_target_keys:
            row["reason"] = "target_key_limit"
            continue
        admitted.add(pair)
        batches[route].setdefault(pair[1], []).append(len(outputs)-1)
    reads = 0
    read_keys = 0
    timed_out = False
    for index in act_indices:
        row = outputs[index]
        try:
            act_res = resolve_act_citation(
                text=str(row["target_key"]) if not (row.get("act_name") and row.get("section")) else None,
                act_name=str(row["act_name"]) if row.get("act_name") else None,
                section=str(row["section"]) if row.get("section") else None,
                division=str(row["division"]) if row.get("division") else None,
                enclosing_section=str(row["enclosing_section"]) if row.get("enclosing_section") else None,
            )
        except ValueError:
            row["reason"] = "unsettled_key"
            continue
        read_keys += 1
        row["act_resolution"] = act_res
        if act_res["resolved"]:
            row.update(
                target_status="found",
                candidate_keys=[{"usc_title": act_res["usc_title"], "usc_section": act_res["usc_section"]}],
                match_count=1,
                reason=None,
            )
        else:
            reason = act_res["unresolved_reason"]
            status = "ambiguous" if reason in {"act_section_ambiguous", "act_name_ambiguous", "sources_disagree"} else "missing"
            row.update(target_status=status, candidate_keys=[], match_count=0, reason=reason)
    for route, keys in batches.items():
        if timed_out:
            for indexes in keys.values():
                for index in indexes:
                    outputs[index].update(reason="target_timeout", error_type="TimeoutError")
            continue
        reads += 1
        try:
            found = _lookup(cursor, route, list(keys), max_candidates)
            read_keys += len(keys)
        except Exception as exc:
            # Surface bounded diagnostics without SQL, paths or credential-bearing exception text.
            name = type(exc).__name__
            timed_out = name in {"InterruptException", "TimeoutError"}
            reason = "target_timeout" if timed_out else "target_read_failure"
            for indexes in keys.values():
                for index in indexes:
                    outputs[index].update(reason=reason, error_type=name)
            continue
        for key, indexes in keys.items():
            result = found.get(key, {"candidate_keys": [], "match_count": 0})
            count = result["match_count"]
            incomplete = count > max_candidates
            invalid_identity = any(value is None for candidate in result["candidate_keys"] for value in candidate.values())
            status = "not_checked" if incomplete or invalid_identity else "missing" if not count else (
                "found" if route.expected_cardinality == "many" or count == 1 else "ambiguous")
            disposition = (
                _disposition_for_target_key(key)
                if should_enrich_dispositions and route is ROUTES.get("usc_section") and status == "missing"
                else None
            )
            for index in indexes:
                outputs[index].update(result, target_status=status, reason="candidate_limit" if incomplete else "target_identity_incomplete" if invalid_identity else None)
                if disposition is not None:
                    outputs[index]["disposition"] = disposition
    statuses = Counter(row["target_status"] for row in outputs)
    reasons = Counter(row["reason"] for row in outputs if row["reason"])
    return {"occurrences": outputs, "coverage": {"input_occurrences": len(occurrences),
            "target_status_counts": dict(statuses), "reason_counts": dict(reasons), "distinct_target_keys_read": read_keys, "distinct_target_keys_selected": len(admitted),
            "target_batches": reads, "partial": bool(reasons), "max_target_keys": max_target_keys,
            "max_candidates": max_candidates}, "resolution_rule": rule}
