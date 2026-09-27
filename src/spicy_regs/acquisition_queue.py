"""Bounded acquisition planning from qualified missing-target observations.

No network or provider dependency is loaded. Routes name existing SpicyDocs APIs;
workers must inspect retained evidence and enforce those APIs' source budgets.
A missing OLRC classification row does not establish missing U.S. Code text.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping
from typing import Any

QUEUE_RULE = "qualified-missing-target-queue/1"


def _pin(value: Any) -> bool:
    return isinstance(value, Mapping) and bool(value) and value.get("status") not in {
        "legacy_unversioned", "local_unversioned", "unavailable",
    }


def _route(kind: str, key: str) -> dict:
    if kind == "public_law" and (match := re.fullmatch(r"([1-9][0-9]*)-(public|private)-([1-9][0-9]*)", key)):
        return {"provider": "govinfo", "native_identifier": key, "edition": None,
                "api": "spicy_docs.sources.govinfo.uslm_acquisition.UslmAcquirer.acquire_public_law",
                "selection_type": "spicy_docs.sources.govinfo.uslm.PublicLawSelection",
                "arguments": {"congress": int(match[1]), "kind": match[2], "number": int(match[3])}}
    if kind == "gao_product_id":
        return {"provider": "gao", "native_identifier": key.lower(), "edition": None,
                "api": "spicy_docs.sources.gao.files.GaoReportFileAcquirer.acquire_report_pdf",
                "arguments": {"product_id": key.lower()}}
    return {"provider": None, "native_identifier": key, "edition": None, "api": None,
            "reason": "classification_is_not_code_text_and_edition_unselected" if kind == "usc_section" else "unsupported_acquisition_identity"}


def build_missing_target_queue(resolution: Mapping[str, Any], *,
                               input_snapshots: Mapping[str, Mapping[str, Any]],
                               retained_targets: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
                               intended_query: str | None = None, max_items: int = 100) -> dict:
    """Plan only qualified missing targets; retain every requesting occurrence.

    ``input_snapshots`` is keyed by occurrence document_kind. ``retained_targets``
    is keyed by (target_kind, normalized_key), with inspection status ``absent``
    or ``retained`` and optional receipt/digest. Omission means uninspected, never
    an implicit absence. Source and target pins are carried exactly as supplied.
    Ranking uses affected unique occurrences, then unique source records.
    """
    if not 1 <= max_items <= 1_000:
        raise ValueError("max_items must be between 1 and 1000")
    groups: dict[str, dict] = {}
    excluded: Counter = Counter()
    for row in resolution.get("occurrences", []):
        if row.get("target_status") != "missing" or row.get("match_count") != 0:
            excluded["not_qualified_missing"] += 1
            continue
        if row.get("source_status") not in {"current_text", "native_field"}:
            excluded["source_unqualified"] += 1
            continue
        source_pin = input_snapshots.get(row.get("document_kind"))
        if not _pin(source_pin) or not _pin(row.get("target_snapshot")):
            excluded["snapshot_unavailable"] += 1
            continue
        assert source_pin is not None
        kind, key = row.get("target_kind"), row.get("normalized_key")
        occurrence = row.get("occurrence_key")
        if not kind or not key or not occurrence or not row.get("document_key"):
            excluded["identity_incomplete"] += 1
            continue
        route = _route(kind, key)
        identity = [kind, route["provider"], route["native_identifier"], route["edition"], row["target_snapshot"]]
        item_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        if item_id not in groups:
            retained = dict((retained_targets or {}).get((kind, key), {}))
            state = retained.get("status")
            status = "unsupported" if not route["api"] else (
                "retained" if state == "retained" and retained.get("sha256") and retained.get("receipt") else
                "planned" if state == "absent" else "needs_retained_lookup")
            groups[item_id] = {"item_id": item_id, "target_kind": kind, "normalized_key": key,
                               "target_snapshot": dict(row["target_snapshot"]), "provider_route": route,
                               "status": status, "acquisition_outcome": "not_attempted",
                               "retained": retained, "intended_query": intended_query,
                               "requesting_occurrences": [], "queue_rule": QUEUE_RULE}
        item = groups[item_id]
        request = {"occurrence_key": occurrence, "document_kind": row["document_kind"],
                   "document_key": row["document_key"], "matched_text": row.get("matched_text"),
                   "text_sha256": row.get("text_sha256"), "span_start": row.get("span_start"),
                   "span_end": row.get("span_end"), "input_snapshot": dict(source_pin),
                   "resolution_rule": row.get("resolution_rule"), "source_status": row["source_status"]}
        if request not in item["requesting_occurrences"]:
            item["requesting_occurrences"].append(request)
    for item in groups.values():
        requests = item["requesting_occurrences"]
        item["affected_occurrences"] = len({r["occurrence_key"] for r in requests})
        item["affected_records"] = len({(r["document_kind"], r["document_key"]) for r in requests})
    ranked = sorted(groups.values(), key=lambda item: (-item["affected_occurrences"], -item["affected_records"], item["item_id"]))
    return {"items": ranked[:max_items], "queue_rule": QUEUE_RULE,
            "coverage": {"input_occurrences": len(resolution.get("occurrences", [])),
                         "excluded_counts": dict(excluded), "qualified_target_groups": len(ranked),
                         "returned_groups": min(max_items, len(ranked)), "max_items": max_items,
                         "partial": len(ranked) > max_items or bool(resolution.get("coverage", {}).get("partial")),
                         "resolution_coverage": dict(resolution.get("coverage", {}))}}
