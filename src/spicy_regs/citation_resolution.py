"""Look up already parsed citation keys in explicitly selected target tables.

This module does not extract citations or reinterpret ``target_resolved``. That
legacy field means a spelling was settled, not that a hosted record exists.
Only trusted table/column declarations form SQL; occurrence keys are parameters.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from spicy_regs.identifiers import normalize_rin
from spicy_regs.citation_sources import TEXT_SOURCES

RESOLUTION_RULE = "selected-target-lookup/1"
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


ROUTES = {
    "bill_number": Route("congress_bills", "bill_id", ("bill_id",)),
    "public_law": Route("laws", "law_id", ("congress", "law_type", "number")),
    "statutes_at_large": Route("laws", "statutes_at_large_volume || '-' || statutes_at_large_page",
                               ("congress", "law_type", "number")),
    "usc_section": Route("law_code_sections", "usc_title || '-' || usc_section_key", ("congress", "session", "seq"),
                         "many", grain="OLRC classification rows naming this section; not hosted Code text"),
    "cfr_section": Route("cfr_sections", "cfr_ref", ("package_id", "granule_id")),
    "federal_register_cite": Route("federal_register", "volume || '-' || start_page",
                                   ("document_number", "publication_date")),
    "rin": Route("unified_agenda", "rin", ("rin", "agenda_edition"), "many", grain="agenda editions for a RIN"),
    # spicy-docs keys a GAO citation on the printed number, upper-cased; a row stating none falls back to its slug.
    "gao_product_id": Route("gao_reports", "upper(coalesce(report_number, report_id))", ("report_id",)),
    "crs_report_id": Route("crs_reports", "report_id", ("report_id",)),
    "docket_number": Route("dockets", "docket_id", ("docket_id",)),
    "committee_name": Route("committees", "system_code", ("system_code",)),
    "us_reports_cite": Route("court_citations", "volume || '-' || page", ("cluster_id",),
                        predicate="reporter = 'U.S.'", grain="CourtListener clusters named by U.S. reporter citations"),
}


def _route(kind: str) -> Route | None:
    return ROUTES.get(kind)


def _occurrence_key(item: Mapping[str, Any]) -> str:
    fields = ("document_kind", "document_key", "text_sha256", "cite_kind", "target_key", "span_start")
    return hashlib.sha256(json.dumps([item.get(k) for k in fields], ensure_ascii=False).encode()).hexdigest()


def _lookup(cursor: Any, route: Route, keys: list[str], max_candidates: int) -> dict[str, dict]:
    """One parameterized read per distinct-key batch; retain bounded candidates and actual match counts."""
    columns = ', '.join(f'CAST(t."{c}" AS VARCHAR) AS "{c}"' for c in route.identity)
    order = ', '.join(f'"{c}"' for c in route.identity)
    sql = f"""WITH requested AS (SELECT unnest(?::VARCHAR[]) AS lookup_key),
        matches AS (SELECT r.lookup_key, {columns}
                    FROM requested r JOIN "{route.table}" t ON CAST(({route.key_sql}) AS VARCHAR)=r.lookup_key
                    WHERE {route.predicate}),
        ranked AS (SELECT *, count(*) OVER (PARTITION BY lookup_key) AS match_count,
                   row_number() OVER (PARTITION BY lookup_key ORDER BY {order}) AS candidate_index FROM matches)
        SELECT * EXCLUDE(candidate_index) FROM ranked WHERE candidate_index <= ? ORDER BY lookup_key, {order}"""
    found: dict[str, dict] = {}
    for row in cursor.execute(sql, [keys, max_candidates]).fetchall():
        result = found.setdefault(row[0], {"candidate_keys": [], "match_count": int(row[-1])})
        candidate = dict(zip(route.identity, row[1:-1]))
        if candidate not in result["candidate_keys"]:
            result["candidate_keys"].append(candidate)
    return found


def resolve_citations(cursor: Any, occurrences: Sequence[Mapping[str, Any]],
                      target_snapshots: Mapping[str, Mapping[str, Any]], *,
                      source_digests: Mapping[tuple[str, str], str] | None = None,
                      max_target_keys: int = 500, max_candidates: int = 100) -> dict:
    """Resolve current-text occurrences, reporting skipped and unread scope explicitly.

    Text citations require the held source digest. Native-field callers can omit
    text_sha256 and source_digests. Unknown/legacy-unversioned target selections
    remain not_checked; callers must supply a real target identity, not invent one.
    Partial parsed citations remain not_checked even if a coincidental key exists.
    """
    if not 1 <= max_target_keys <= 10_000 or not 1 <= max_candidates <= 1_000:
        raise ValueError("Resolution bounds must be positive and within the supported limits")
    outputs = []
    batches: dict[Route, dict[str, list[int]]] = defaultdict(dict)
    admitted: set[tuple[Route, str]] = set()
    for item in occurrences:
        kind, key = str(item.get("cite_kind", "")), str(item.get("target_key") or "")
        route = _route(kind)
        row = {**item, "occurrence_key": str(item.get("occurrence_key") or _occurrence_key(item)), "target_kind": kind, "normalized_key": key,
               "target_snapshot": dict(target_snapshots.get(route.table, {})) if route else None,
               "target_status": "not_checked", "candidate_keys": [], "resolution_rule": RESOLUTION_RULE,
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
        if not key or str(item.get("target_resolved", "true")).lower() != "true":
            row["reason"] = "unsettled_key"
            continue
        if kind == "rin" and normalize_rin(key) != key:
            row["reason"] = "unsettled_key"
            continue
        snapshot = target_snapshots.get(route.table)
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
            for index in indexes:
                outputs[index].update(result, target_status=status, reason="candidate_limit" if incomplete else "target_identity_incomplete" if invalid_identity else None)
    statuses = Counter(row["target_status"] for row in outputs)
    reasons = Counter(row["reason"] for row in outputs if row["reason"])
    return {"occurrences": outputs, "coverage": {"input_occurrences": len(occurrences),
            "target_status_counts": dict(statuses), "reason_counts": dict(reasons), "distinct_target_keys_read": read_keys, "distinct_target_keys_selected": len(admitted),
            "target_batches": reads, "partial": bool(reasons), "max_target_keys": max_target_keys,
            "max_candidates": max_candidates}, "resolution_rule": RESOLUTION_RULE}
