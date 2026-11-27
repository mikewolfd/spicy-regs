"""Generation-bound, paginated exports with a fixed public field allowlist."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .storage import connect, stable
from .coverage import coverage
from .scope import select_retained_responses


def pick(value: dict, mapping: dict[str, str]) -> dict:
    return {public: value[internal] for internal, public in mapping.items() if value.get(internal) is not None}


def public_url(value: str | None) -> str | None:
    if not value:
        return None
    parts = urlsplit(value)
    # The registered CourtListener bulk listing uses a public directory selector.
    # Preserve this exact scope; other queries can contain credentials or tokens.
    public_listing = (
        parts.netloc == "com-courtlistener-storage.s3-us-west-2.amazonaws.com"
        and parts.path == "/list.html"
        and parts.query == "prefix=bulk-data/"
    )
    if (parts.scheme not in ["http", "https"] or parts.username or parts.password or parts.fragment):
        return None
    if parts.query and not public_listing:
        if parts.netloc != "books.google.com":
            return None
        # The source reader owns the exact public PDF locator: book id and
        # output=pdf only. Do not widen other origins or duplicate its policy.
        try:
            from spicy_docs.sources.disbursements_reports import disbursements_report_locator

            disbursements_report_locator(value)
        except (ImportError, ValueError):
            return None
    return value


def safe_text(value: object) -> str:
    text = str(value)
    without_urls = re.sub(r"https?://[^\s\"\']+", "", text)
    if re.search(r"(^|[\s\"\'=:(])/(?!/)[^\s]", without_urls) or re.search(
        r"(^|[\s\"\'])(([A-Za-z]:[\\/])|\\\\)", text
    ):
        return "[internal evidence withheld]"
    if any(
        mark in text.lower()
        for mark in [
            "/users/",
            "/home/",
            "file://",
            "api_key=",
            "apikey=",
            "token=",
            "bearer ",
            "x-amz-signature",
            "password=",
            "secret=",
        ]
    ):
        return "[internal evidence withheld]"
    return text


def validate_public(value: object) -> None:
    """Reject internal references or credentials even inside allowed text fields."""
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("Public export keys must be strings")
            if key.lower() in {
                "localpath",
                "localpaths",
                "internalonly",
                "origin",
                "raw",
                "local_path",
                "authorization",
            }:
                raise ValueError(f"Internal field in public export: {key}")
            validate_public(item)
    elif isinstance(value, list):
        for item in value:
            validate_public(item)
    elif isinstance(value, str) and safe_text(value) != value:
        raise ValueError("Internal path or credential in public export")



def file_provenance(facts: dict, *, status: str, local: bool) -> dict:
    """Describe this retained alias only; identical hashes never join captures.

    The manifest reference identifies the exact indexed row. An original receipt
    attached by a legacy hash match remains provisional, even when its URL exists.
    """
    mapping = facts.get("mapping") or {}
    context = facts.get("receipt_context") or {}
    urls = []
    for owner in (facts, context):
        for key in ("source_url", "requested_url", "resolved_url", "url"):
            value = owner.get(key)
            if isinstance(value, str) and (url := public_url(value)) and url not in urls:
                urls.append(url)
    if isinstance(facts.get("original_location"), str):
        url = public_url(facts["original_location"])
        if url and url not in urls:
            urls.append(url)
    # This field was attached by legacy content-hash reconciliation. It is a
    # useful candidate origin, never proof of this alias's acquisition.
    candidate = facts.get("receipt_source_url")
    if isinstance(candidate, str) and (url := public_url(candidate)) and url not in urls:
        urls.append(url)
    ref = mapping.get("receipt_evidence_ref") or {}
    receipt = {}
    if isinstance(ref.get("sha256"), str) and re.fullmatch(r"sha256:[a-f0-9]{64}", ref["sha256"]):
        receipt["sha256"] = ref["sha256"]
        if type(ref.get("entry")) is int and ref["entry"] > 0:
            receipt["entry"] = ref["entry"]
    provisional = facts.get("receipt_status") == "provisional" or bool(facts.get("receipt_matches"))
    direct_receipt = str(facts.get("receipt_object", "")).startswith("acquisitions/sha256/")
    basis = "provisional" if provisional else "receipt-backed" if direct_receipt and receipt else "inferred" if urls else "unknown"
    availability = ("external" if status == "external-by-choice" else "not-acquired" if status == "known-not-acquired"
                    else "missing" if status == "missing-bytes" else "local" if local else "unknown")
    result: dict[str, Any] = {"basis": basis, "sourceUrls": urls, "availability": availability}
    if receipt:
        result["receipt"] = receipt
    original = re.fullmatch(r"objects/sha256/[a-f0-9]{2}/([a-f0-9]{64})(?:\.gz)?", str(facts.get("receipt_object", "")))
    documented_original = context.get("original_receipt_sha256")
    if isinstance(documented_original, str) and re.fullmatch(r"sha256:[a-f0-9]{64}", documented_original):
        result["originalReceiptSha256"] = documented_original
    elif original:
        result["originalReceiptSha256"] = "sha256:" + original[1]
    # Mapping normalizes exact per-row receipt timestamps and leaves conflicts
    # unknown. Never infer capture time from a filename, mtime or content date.
    captured_at = mapping.get("acquired_at")
    if basis == "receipt-backed" and isinstance(captured_at, str):
        result["capturedAt"] = captured_at
    return result


def write_json(path: Path, value: dict, *, internal: bool, emit=None) -> None:
    if not internal:
        validate_public(value)
    body = json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    if emit is not None:
        emit(path, body.encode("utf-8"))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    if path.is_file() and path.name != "manifest.json":
        if path.read_text() != body:
            raise ValueError(f"Immutable export collision: {path}")
        return
    temporary.write_text(body)
    os.replace(temporary, path)


def public_publication(item: dict) -> dict:
    """Keep retained identities; discard all unrelated internal evidence fields."""
    clean = pick(
        item,
        {
            key: key
            for key in [
                "id",
                "family",
                "captureAt",
                "familyPublishedAt",
                "serverDate",
                "responseSha256",
                "artifactDigest",
                "artifactPrefix",
                "associationStatus",
            ]
        },
    )
    clean["tables"] = []
    for table in item["tables"]:
        member = pick(table, {key: key for key in ["table", "bytes", "rows", "sha256", "partitionColumns"]})
        if "members" in table:
            member["members"] = [
                pick(part, {key: key for key in ["key", "sha256", "bytes", "rows", "partition"]})
                for part in table["members"]
            ]
        clean["tables"].append(member)

    def check(value):
        if isinstance(value, dict):
            for key, nested in value.items():
                if re.search(r"credential|token|password|secret", key, re.I):
                    raise ValueError("Private publication metadata field")
                check(nested)
        elif isinstance(value, list):
            for nested in value:
                check(nested)
        elif isinstance(value, str):
            for url in re.findall(r"https?://[^\s\"\']+", value):
                if public_url(url) is None:
                    raise ValueError("Private URL in publication metadata")

    check(clean)
    validate_public(clean)
    return clean


def public_reconciliation(item: dict) -> dict:
    """Expose the pinned result, never receipt bodies or local evidence references."""
    if item.get("scope") not in {"current", "historical"}:
        raise ValueError("Invalid reconciliation scope")
    if not isinstance(item.get("version"), str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", item["version"]):
        raise ValueError("Invalid reconciliation version")
    if item.get("status") not in {"verified", "missing_local_files", "missing_provenance", "conflicting_evidence"}:
        raise ValueError("Invalid reconciliation status")
    clean = pick(item, {key: key for key in ["scope", "version", "status"]})
    for key in ["sourceFiles", "matchedSourceFiles", "missingSourceFiles", "capturedSourceFiles"]:
        if type(item.get(key)) is not int or item[key] < 0:
            raise ValueError("Invalid reconciliation file count")
        clean[key] = item[key]
    if clean["matchedSourceFiles"] + clean["missingSourceFiles"] != clean["sourceFiles"]:
        raise ValueError("Reconciliation file counts disagree")
    if type(item.get("inputClosureComplete")) is not bool or clean["capturedSourceFiles"] > clean["sourceFiles"]:
        raise ValueError("Invalid reconciliation input completeness")
    clean["inputClosureComplete"] = item["inputClosureComplete"]
    if clean["status"] == "verified" and (not clean["sourceFiles"] or clean["missingSourceFiles"] or not clean["inputClosureComplete"]):
        raise ValueError("Verified reconciliation requires matching local originals")
    reasons = item.get("reasons", [])
    if not isinstance(reasons, list) or any(not isinstance(reason, str) or not reason.strip() for reason in reasons):
        raise ValueError("Invalid reconciliation reasons")
    clean["reasons"] = [safe_text(reason) for reason in reasons]
    links = item.get("evidenceLinks", [])
    if not isinstance(links, list):
        raise ValueError("Invalid reconciliation evidence links")
    clean["evidenceLinks"] = [
        {"label": safe_text(link["label"]), "url": url}
        for link in links
        if isinstance(link, dict) and isinstance(link.get("label"), str) and link["label"].strip()
        and isinstance(link.get("url"), str) and (url := public_url(link["url"]))
    ]
    validate_public(clean)
    return clean


def export_period(item: dict, *, internal: bool) -> dict:
    """Keep the period allowlist and withhold private display text publicly."""
    clean = pick(item, {key: key for key in ["kind", "label", "start", "end", "basis"]})
    if not internal:
        for field in ["label", "basis"]:
            if isinstance(clean.get(field), str):
                clean[field] = safe_text(clean[field])
    return clean


def period_summary(periods: list[dict]) -> dict:
    """Bound list metadata without combining separately qualified date scopes."""
    observed = {
        json.dumps(item, sort_keys=True): item for item in periods
        if item["kind"] == "observed-retained-scope" and item.get("start")
    }
    summary: dict[str, Any] = {"count": len(periods)}
    if len(observed) == 1:
        summary["observedRetained"] = next(iter(observed.values()))
    return summary


def public_coverage(report: dict) -> dict:
    """Publish only named measurement fields; raw scopes and proof paths stay private."""
    result = {"completenessDenominator": None, "stages": {}}
    scope_fields = {
        "collection_id": "collectionId",
        "table": "table",
        "observation_id": "observationId",
        "captured_at": "capturedAt",
        "qualification_scope": "qualificationScope",
    }

    def safe_measurement_text(value):
        return (
            isinstance(value, str)
            and bool(value.strip())
            and safe_text(value) == value
            and all(public_url(url) is not None for url in re.findall(r'https?://[^\s"<>]+', value))
        )

    for stage, data in report["stages"].items():
        measurements = []
        for item in data["measurements"]:
            scope = item.get("scope")
            # Removing a selected query from its scope would turn a narrow
            # count into an apparent collection-wide measurement. Keep that
            # measurement private until its full scope has a public form.
            if (
                not isinstance(scope, dict)
                or not scope
                or any(
                    key not in scope_fields or (value is not None and not isinstance(value, str))
                    for key, value in scope.items()
                )
            ):
                continue
            if any(value is not None and not safe_measurement_text(value) for value in scope.values()):
                continue
            period = item.get("period")
            if period is not None and (
                not isinstance(period, dict)
                or not period
                or any(
                    key not in {"kind", "label", "start", "end"} or not isinstance(value, str)
                    for key, value in period.items()
                )
            ):
                continue
            if period is not None and any(not safe_measurement_text(value) for value in period.values()):
                continue
            if any(
                not safe_measurement_text(item[key])
                for key in ("unit", "route_id", "version", "verification")
                if item.get(key) is not None
            ):
                continue
            clean: dict[str, object] = pick(
                item,
                {
                    "count": "count",
                    "unit": "unit",
                    "route_id": "routeId",
                    "version": "version",
                    "verification": "evidenceBasis",
                },
            )
            clean = {
                key: safe_text(value) if isinstance(value, str) else value
                for key, value in clean.items()
                if isinstance(value, str) or (key == "count" and type(value) is int)
            }
            clean["scope"] = {
                key: safe_text(value)
                for key, value in pick(scope if isinstance(scope, dict) else {}, scope_fields).items()
                if isinstance(value, str)
            }
            if not clean["scope"]:
                continue
            clean["period"] = (
                {
                    key: safe_text(value)
                    for key, value in pick(
                        period if isinstance(period, dict) else {},
                        {"kind": "kind", "label": "label", "start": "start", "end": "end"},
                    ).items()
                    if isinstance(value, str)
                }
                if period
                else None
            )
            measurements.append(clean)
        result["stages"][stage] = {
            "status": "scope-withheld" if data["measurements"] and not measurements else data["status"],
            "measurements": measurements,
        }
    return result


_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}")
_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)
_ROUTE_SIGNATURE_KEYS = (
    "id",
    "delivery_method",
    "lifecycle",
    "custodian",
    "content_behavior",
    "access_url",
)


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else None


def _schema_signature(
    entry: dict, *, descriptor_key: str
) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]] | None:
    container = entry.get("schema") if isinstance(entry.get("schema"), dict) else entry.get(descriptor_key)
    if not isinstance(container, dict):
        return None
    raw_columns = container.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        return None
    columns: list[tuple[str, str]] = []
    for column in raw_columns:
        if (
            not isinstance(column, (list, tuple))
            or len(column) != 2
            or not isinstance(column[0], str)
            or not column[0].strip()
            or not isinstance(column[1], str)
            or not column[1].strip()
        ):
            return None
        columns.append((column[0], column[1]))
    raw_partitions = (
        container["partitionColumns"]
        if "partitionColumns" in container
        else container.get("partition_columns")
    )
    if raw_partitions is None:
        partitions: tuple[str, ...] = ()
    elif isinstance(raw_partitions, list) and all(isinstance(col, str) and col.strip() for col in raw_partitions):
        partitions = tuple(raw_partitions)
    else:
        return None
    return (tuple(columns), partitions)


def _policy_signature(entry: dict) -> tuple[bool, str | None]:
    policy_fields: dict[str, Any] = {}
    for key in ("policy", "policy_version", "schema_policy"):
        if key not in entry or entry[key] is None:
            continue
        value = entry[key]
        if key == "policy_version":
            if not isinstance(value, str) or not value.strip():
                return False, None
        elif isinstance(value, str):
            if not value.strip():
                return False, None
        elif not isinstance(value, dict) or not value:
            return False, None
        policy_fields[key] = value
    if not policy_fields:
        return True, None
    return True, json.dumps(policy_fields, sort_keys=True, separators=(",", ":"))


def _route_definition_signature(route: dict) -> tuple[str | None, ...]:
    return tuple(
        route[key] if isinstance(route.get(key), str) and route[key].strip() else None
        for key in _ROUTE_SIGNATURE_KEYS
    )


def _explicit_route_binding(entry: dict) -> tuple[bool, tuple[str, Any] | None]:
    present = [
        key
        for key in ("route_id", "route_ids", "acquisition_route", "acquisition_routes", "routes")
        if key in entry and entry[key] is not None
    ]
    if not present:
        return True, None
    normalized: dict[str, Any] = {}
    for key in present:
        value = entry[key]
        if key == "route_id":
            if not isinstance(value, str) or not value.strip():
                return False, None
            normalized["ids"] = (value,)
        elif key == "route_ids":
            if not isinstance(value, list) or not value or any(not isinstance(v, str) or not v.strip() for v in value):
                return False, None
            normalized["ids"] = tuple(sorted(set(value)))
        elif key == "acquisition_route":
            if isinstance(value, str) and value.strip():
                normalized["values"] = (value,)
            elif isinstance(value, dict) and value:
                normalized["defs"] = (_route_definition_signature(value),)
            else:
                return False, None
        else:
            if not isinstance(value, list) or not value:
                return False, None
            if all(isinstance(v, str) and v.strip() for v in value):
                normalized["ids" if key == "routes" else "values"] = tuple(sorted(set(value)))
            elif all(isinstance(v, dict) and v for v in value):
                normalized["defs"] = tuple(sorted(_route_definition_signature(v) for v in value))
            else:
                return False, None
    if len(normalized) == 1:
        kind, payload = next(iter(normalized.items()))
        return True, (kind, payload)
    return True, ("combined", tuple(sorted(normalized.items())))


def _parent_table_names(values: Any) -> tuple[str, ...] | None:
    if not isinstance(values, list):
        return None
    names: set[str] = set()
    for item in values:
        raw = item.get("table") if isinstance(item, dict) else item
        if not isinstance(raw, str) or not raw.strip():
            return None
        name = raw.removesuffix(".parquet")
        if name != "etl_receipts" and not name.endswith("/etl_receipts"):
            names.add(name)
    return tuple(sorted(names))


def _source_route_signatures(values: Any) -> tuple[str, ...] | None:
    if not isinstance(values, list):
        return None
    signatures: list[str] = []
    for item in values:
        if isinstance(item, str) and item.strip():
            signatures.append(item)
        elif isinstance(item, dict) and item:
            core = {
                key: item[key]
                for key in ("source_id", "publisher", "locator", "kind", "selection_rules")
                if key in item and item[key] is not None
            }
            if not core:
                return None
            signatures.append(json.dumps(core, sort_keys=True, separators=(",", ":")))
        else:
            return None
    return tuple(sorted(signatures))


def _source_identifiers(values: Any) -> tuple[str, ...] | None:
    if not isinstance(values, list) or any(not isinstance(v, str) or not v.strip() for v in values):
        return None
    return tuple(sorted(set(values)))


def _routes_match(
    table: dict,
    obs: dict,
    *,
    routes: list[dict] | None,
    evidence: list[tuple[str, dict]],
) -> bool:
    table_route_ok, table_explicit = _explicit_route_binding(table)
    obs_route_ok, obs_explicit = _explicit_route_binding(obs)
    if not table_route_ok or not obs_route_ok:
        return False

    obs_source_routes = _source_route_signatures(obs["source_routes"]) if "source_routes" in obs else None
    obs_source_ids = _source_identifiers(obs["source_identifiers"]) if "source_identifiers" in obs else None
    obs_parents = _parent_table_names(obs["parent_tables"]) if "parent_tables" in obs else None
    obs_family_parents = (
        _parent_table_names(obs["recorded_family_parent_tables"])
        if "recorded_family_parent_tables" in obs
        else None
    )

    if (
        obs_explicit is None
        and not obs_source_routes
        and not obs_source_ids
        and not obs_parents
    ):
        return False

    collection_route_ids = (
        tuple(sorted({r["id"] for r in routes if isinstance(r.get("id"), str) and r["id"].strip()}))
        if routes is not None
        else None
    )
    collection_route_defs = (
        tuple(sorted(_route_definition_signature(r) for r in routes))
        if routes is not None
        else None
    )
    collection_routes_by_id = (
        {r["id"]: _route_definition_signature(r) for r in routes if isinstance(r.get("id"), str) and r["id"].strip()}
        if routes is not None
        else None
    )

    def _check_against_collection(binding: tuple[str, Any], *, require_full_set: bool) -> bool:
        kind, payload = binding
        if kind == "combined":
            return all(_check_against_collection(part, require_full_set=require_full_set) for part in payload)
        if routes is None:
            return not require_full_set
        if not routes:
            return False
        if kind in {"ids", "values"}:
            if require_full_set:
                return collection_route_ids == payload
            return collection_routes_by_id is not None and all(rid in collection_routes_by_id for rid in payload)
        if kind == "defs":
            if require_full_set:
                return collection_route_defs == payload
            return all(
                collection_routes_by_id is not None
                and sig[0] is not None
                and collection_routes_by_id.get(sig[0]) == sig
                for sig in payload
            )
        return False

    if table_explicit is not None:
        if obs_explicit != table_explicit:
            return False
        if routes is not None and not _check_against_collection(table_explicit, require_full_set=False):
            return False
    elif obs_explicit is not None:
        if not _check_against_collection(obs_explicit, require_full_set=True):
            return False

    if "source_routes" in table and table["source_routes"] is not None:
        table_source_routes = _source_route_signatures(table["source_routes"])
        if table_source_routes is None or obs_source_routes != table_source_routes:
            return False

    if "source_identifiers" in table and table["source_identifiers"] is not None:
        table_source_ids = _source_identifiers(table["source_identifiers"])
        if table_source_ids is None or obs_source_ids != table_source_ids:
            return False

    if "parent_tables" in table and table["parent_tables"] is not None:
        table_parents = _parent_table_names(table["parent_tables"])
        if table_parents is None or table_parents != (obs_parents or ()):
            return False
    else:
        derived_parents = tuple(
            sorted(
                {
                    item["to_id"].removesuffix(".parquet")
                    for kind, item in evidence
                    if kind == "relationships"
                    and item.get("kind") == "derived-from"
                    and item.get("from_id") == table["id"]
                    and isinstance(item.get("to_id"), str)
                    and item["to_id"].strip()
                }
            )
        )
        if derived_parents and derived_parents != (obs_parents or ()):
            return False

    if "family_parent_tables" in table and table["family_parent_tables"] is not None:
        table_family_parents = _parent_table_names(table["family_parent_tables"])
        if table_family_parents is None:
            return False
        if "recorded_family_parent_tables" in obs:
            if table_family_parents != (obs_family_parents or ()):
                return False

    return True


def resolve_source_coverage_note(
    table: dict,
    evidence: list[tuple[str, dict]],
    *,
    routes: list[dict] | None = None,
) -> dict[str, Any] | None:
    """Resolve a reviewed source-coverage note for a published table version.

    Exact ``table_version`` matches always win without being flagged as carried.
    Across routine data-only republishes, the latest non-superseded review at or
    before the live generation carries forward only when family, table schema,
    policy, and acquisition route bindings remain unchanged; route, schema, or
    policy changes invalidate carry-forward and require a fresh review.
    """
    version = table.get("artifact_digest")
    if not _is_sha256(version):
        return None
    assert isinstance(version, str)

    candidates = [
        item
        for kind, item in evidence
        if kind == "observations"
        and item.get("metadata_kind") == "source-coverage"
        and item.get("table") == table["id"]
    ]
    if not candidates:
        return None

    exact = [item for item in candidates if item.get("table_version") == version]
    if exact:
        notes: set[str] = set()
        for item in exact:
            note = item.get("note")
            if not isinstance(note, str) or not note.strip():
                raise ValueError("Source coverage observation requires a note")
            notes.add(note)
        if len(notes) > 1:
            raise ValueError("Conflicting source coverage notes for the same table version")
        return {
            "note": next(iter(notes)),
            "match": "exact",
            "carried": False,
            "reviewed_version": version,
        }

    table_family = table.get("family")
    if not isinstance(table_family, str) or not table_family.strip():
        return None
    table_schema = _schema_signature(table, descriptor_key="publication_descriptor")
    if table_schema is None:
        return None
    table_policy_ok, table_policy = _policy_signature(table)
    if not table_policy_ok:
        return None
    table_published_at: datetime | None = None
    if "published_at" in table and table["published_at"] is not None:
        table_published_at = _parse_timestamp(table["published_at"])
        if table_published_at is None:
            return None

    eligible: list[tuple[dict, datetime | None]] = []
    for item in candidates:
        obs_version = item.get("table_version")
        if not _is_sha256(obs_version):
            continue
        if item.get("family") != table_family:
            continue
        obs_schema = _schema_signature(item, descriptor_key="publication_member")
        if obs_schema is None or obs_schema != table_schema:
            continue
        obs_policy_ok, obs_policy = _policy_signature(item)
        if not obs_policy_ok or obs_policy != table_policy:
            continue
        if not _routes_match(table, item, routes=routes, evidence=evidence):
            continue
        obs_recorded_at: datetime | None = None
        if "recorded_at" in item and item["recorded_at"] is not None:
            obs_recorded_at = _parse_timestamp(item["recorded_at"])
            if obs_recorded_at is None:
                continue
        if table_published_at is not None and obs_recorded_at is not None and obs_recorded_at > table_published_at:
            continue
        note = item.get("note")
        if not isinstance(note, str) or not note.strip():
            raise ValueError("Source coverage observation requires a note")
        eligible.append((item, obs_recorded_at))

    if not eligible:
        return None

    superseded = {
        item["prior_review"]
        for item, _ in eligible
        if isinstance(item.get("prior_review"), str) and item["prior_review"].strip()
    }
    current = [(item, recorded) for item, recorded in eligible if item.get("id") not in superseded]
    if not current:
        raise ValueError("Conflicting source coverage notes for the same table")

    if len(current) > 1:
        if all(recorded is not None for _, recorded in current):
            latest_time = max(recorded for _, recorded in current if recorded is not None)
            current = [(item, recorded) for item, recorded in current if recorded == latest_time]

    notes = {item["note"] for item, _ in current}
    versions = {item["table_version"] for item, _ in current}
    if len(notes) > 1 or len(versions) > 1:
        raise ValueError("Conflicting source coverage notes for the same table")

    chosen = current[0][0]
    return {
        "note": chosen["note"],
        "match": "carried",
        "carried": True,
        "reviewed_version": chosen["table_version"],
    }


def source_coverage_note(
    table: dict,
    evidence: list[tuple[str, dict]],
    *,
    routes: list[dict] | None = None,
) -> str | None:
    """Return the reviewed source-coverage note text for a published table, if eligible."""
    resolved = resolve_source_coverage_note(table, evidence, routes=routes)
    return resolved["note"] if resolved is not None else None


def export_file_pages(db, output: Path, *, cid: str, folder: str, common: dict, internal: bool, page_size: int, retained: bool = False, references=None, publisher=None, emit=None, page_rows=None, legacy_provenance=True) -> list[dict]:
    """Write both inventories through the same bounded, public-safe file format."""
    if not internal and common["version"] == 3:
        from .file_inventory import export_pages
        return export_pages(db, output, dataset_id=cid, publisher=publisher, folder=folder,
                            generation=common["generation"], page_size=page_size,
                            inventory_kind="attention" if retained else "active", references=references, emit=emit,
                            page_rows=page_rows)
    pages = []
    # Select only the per-alias provenance facts needed by public pages.
    # Receipt bodies and content evidence may be large and remain private.
    provenance_fields = """json_object(
        'mapping', json_object('acquired_at', json_extract(data,'$.mapping.acquired_at'),
            'receipt_evidence_ref', json(json_extract(data,'$.mapping.receipt_evidence_ref'))),
        'receipt_context', json_object(
            'source_url', json_extract(data,'$.receipt_context.source_url'),
            'requested_url', json_extract(data,'$.receipt_context.requested_url'),
            'resolved_url', json_extract(data,'$.receipt_context.resolved_url'),
            'url', json_extract(data,'$.receipt_context.url'),
            'original_receipt_sha256', json_extract(data,'$.receipt_context.original_receipt_sha256')),
        'source_url', json_extract(data,'$.source_url'),
        'requested_url', json_extract(data,'$.requested_url'),
        'resolved_url', json_extract(data,'$.resolved_url'),
        'original_location', json_extract(data,'$.original_location'),
        'receipt_source_url', json_extract(data,'$.receipt_source_url'),
        'receipt_status', json_extract(data,'$.receipt_status'),
        'receipt_matches', json_extract(data,'$.receipt_matches'),
        'receipt_object', json_extract(data,'$.receipt_object')) AS provenance_facts"""
    file_fields = ("*" if internal else "id,name,status,sha256,bytes,route_id") + ", (json_extract(data,'$.encoded_bytes') IS NOT NULL) AS local," + provenance_fields
    source = "retained_response_files" if retained else "files"
    cursor = db.execute(f"SELECT {file_fields} FROM {source} WHERE collection_id=? ORDER BY id", (cid,))
    page_number = 0
    sizes = iter(page_rows) if page_rows is not None else None
    while True:
        size = next(sizes, None) if sizes is not None else page_size
        if size is None:
            if cursor.fetchone() is not None:
                raise ValueError("Staged public pages omit candidate rows")
            break
        if not 0 < size <= 5000:
            raise ValueError("Invalid staged public page size")
        batch = cursor.fetchmany(size)
        if not batch:
            if sizes is not None:
                raise ValueError("Staged public pages exceed candidate rows")
            break
        if sizes is not None and len(batch) != size:
            raise ValueError("Staged public page size differs from candidate rows")
        files = []
        for row in batch:
            item = {"id": row["id"], "name": safe_text(row["name"]), "status": row["status"]}
            for sql, public in [("sha256", "sha256"), ("bytes", "bytes"), ("route_id", "routeId")]:
                if row[sql] is not None:
                    item[public] = row[sql]
            if internal or legacy_provenance:
                facts = json.loads(row["provenance_facts"])
                if not internal and references is not None:
                    references.indexed((facts.get("mapping") or {}).get("receipt_evidence_ref") or {})
                item["provenance"] = file_provenance(facts, status=row["status"], local=bool(row["local"]))
            if internal:
                item.update(
                    pick(
                        json.loads(row["data"]),
                        {"encoded_bytes": "encodedBytes", "verification": "verification"},
                    )
                )
                if row["local_path"]:
                    item["localPath"] = row["local_path"]
            files.append(item)
        page_prefix = "retained-files" if retained else "files"
        page_path = f"{folder}/{page_prefix}-{page_number:05d}.json"
        write_json(
            output / page_path,
            {"format": "spicy-source-files", **common, "collectionId": cid, "files": files},
            internal=internal, emit=emit,
        )
        pages.append({"path": page_path, "rows": len(files)})
        page_number += 1
    return pages


def export(index: Path, output: Path, *, internal: bool = False, page_size: int = 5000, emit=None, public_version=3, page_layout=None, reference_dependencies=None) -> dict:
    if page_size < 1 or page_size > 5000:
        raise ValueError("page_size must be between 1 and 5000")
    db = connect(index, readonly=True)
    try:
        metadata = {r["key"]: json.loads(r["value"]) for r in db.execute("SELECT * FROM metadata")}
        generation = metadata["generation"]
        prefix = "generations/" + generation.removeprefix("sha256:")
        if public_version not in (1, 2, 3):
            raise ValueError("Unsupported public version")
        common = {"version": 2 if internal else public_version, "generation": generation}
        if internal:
            common["audience"] = "internal"
        # Coverage already supports the whole catalog. Share its global
        # evidence reads instead of repeating them for every collection.
        coverage_by_collection = {item["collection_id"]: item for item in coverage(db)["collections"]}
        from .file_inventory import ReceiptReferences
        references = None if internal else ReceiptReferences(db, reference_dependencies)
        retained_responses = select_retained_responses(db)
        summaries: list[dict[str, Any]] = []
        for collection in db.execute("SELECT * FROM collections ORDER BY id"):
            cid = collection["id"]
            data = json.loads(collection["data"])
            folder = prefix + "/" + stable(cid)[:24]
            count = db.execute("SELECT count(*) FROM files WHERE collection_id=?", (cid,)).fetchone()[0]
            unique = db.execute(
                """SELECT count(*),coalesce(sum(bytes),0) FROM objects WHERE sha256 IN
                                  (SELECT sha256 FROM files WHERE collection_id=? AND status NOT IN
                                   ('missing-bytes','receipt-only','known-not-acquired','external-by-choice'))""",
                (cid,),
            ).fetchone()
            missing = db.execute(
                "SELECT count(*) FROM files WHERE collection_id=? AND status='missing-bytes'", (cid,)
            ).fetchone()[0]
            statuses = {r[0] for r in db.execute("SELECT DISTINCT status FROM files WHERE collection_id=?", (cid,))}
            evidence = [
                (r["kind"], json.loads(r["data"]))
                for r in db.execute("SELECT * FROM evidence WHERE collection_id=? ORDER BY id", (cid,))
            ]
            for kind, item in evidence:
                if kind in ["observations", "errors"] and item.get("status"):
                    statuses.add(item["status"])
            # A publication entry can have its originals inventoried in another
            # collection. A missing association leaves acquisition unresolved.
            if not count and not statuses.intersection({"known-not-acquired", "external-by-choice"}):
                statuses.add("inventory-unresolved")
            elif unique[0]:
                has_table = db.execute("SELECT 1 FROM tables WHERE collection_id=? LIMIT 1", (cid,)).fetchone()
                if not has_table:
                    explicit_not_integrated = (
                        "held-not-integrated" in statuses
                        or data.get("status") == "held-not-integrated"
                        or any(item.get("status") == "held-not-integrated" for _, item in evidence)
                    )
                    statuses.add("held-not-integrated" if explicit_not_integrated else "integration-unresolved")
                if has_table:
                    statuses.update(
                        json.loads(r[0])["status"]
                        for r in db.execute("SELECT data FROM tables WHERE collection_id=?", (cid,))
                    )
            if not any(k == "periods" for k, _ in evidence):
                statuses.add("unknown-scope")
            periods = [
                export_period(item, internal=internal)
                for kind, item in evidence
                if kind == "periods"
            ]
            if not periods:
                periods = [{"kind": "unknown", "label": "Unknown", "basis": "no-qualified-period-assertion"}]
            routes = []
            raw_routes: list[dict] = []
            for row in db.execute("SELECT data FROM routes WHERE collection_id=? ORDER BY id", (cid,)):
                item = json.loads(row[0])
                raw_routes.append(item)
                route = {
                    "id": item["id"],
                    "label": safe_text(item.get("label", item["id"])),
                    "deliveryMethod": item.get("delivery_method", "unknown"),
                    "lifecycle": item.get("lifecycle", "unknown"),
                    "custodian": safe_text(item.get("custodian", "unknown")),
                    "contentBehavior": item.get("content_behavior", "unknown"),
                    "periods": [
                        export_period(p, internal=internal)
                        for p in (
                            item.get("periods", [])
                            + [
                                period
                                for kind, period in evidence
                                if kind == "periods" and period.get("route_id") == item["id"]
                            ]
                        )
                    ],
                }
                url = public_url(item.get("access_url"))
                if url:
                    route["accessUrl"] = url
                for field in ["status", "note"]:
                    if item.get(field):
                        route[field] = safe_text(item[field])
                routes.append(route)
            tables = []
            for row in db.execute("SELECT data FROM tables WHERE collection_id=? ORDER BY id", (cid,)):
                item = json.loads(row[0])
                table = pick(
                    item,
                    {
                        "id": "id",
                        "family": "family",
                        "status": "status",
                        "artifact_digest": "artifactDigest",
                        "coverage_note": "coverageNote",
                    },
                )
                resolved = resolve_source_coverage_note(item, evidence, routes=raw_routes)
                if resolved is not None:
                    table["coverageNote"] = resolved["note"] if internal else safe_text(resolved["note"])
                    if resolved["carried"]:
                        table["coverageNoteMatch"] = "carried"
                        table["coverageNoteReviewedVersion"] = resolved["reviewed_version"]
                if item.get("publication") is not None:
                    table["publication"] = public_reconciliation(item["publication"])
                    if table.get("artifactDigest") != table["publication"]["version"]:
                        raise ValueError("Reconciliation version differs from the table publication")
                tables.append(table)
            summary: dict[str, Any] = {
                "id": cid,
                "title": safe_text(collection["title"]),
                "statuses": sorted(statuses),
                "families": sorted({t["family"] for t in tables} | set(data.get("families", []))),
                "tables": sorted(t["id"] for t in tables),
                "routeCount": len(routes),
                "holdings": {"aliases": count, "objects": unique[0], "decodedBytes": unique[1], "missing": missing},
                "periodSummary": period_summary(periods),
                "detailPath": folder + "/collection.json",
            }
            for key in ["publisher", "description"]:
                if data.get(key):
                    display = data[key]
                    if key == "publisher" and isinstance(display, dict):
                        display = display.get("name") or display.get("title") or "Unknown publisher"
                    summary[key] = safe_text(display)
            if not internal and public_version == 1:
                summary.pop("periodSummary")
                summary["periods"] = periods
            summaries.append(summary)
            layout = (page_layout or {}).get((cid, "active"), {})
            pages = export_file_pages(db, output, cid=cid, folder=folder, common=common,
                                      internal=internal, page_size=page_size, references=references, publisher=summary.get("publisher"), emit=emit,
                                      page_rows=layout.get("rows"), legacy_provenance=layout.get("provenance", True))
            detail: dict[str, Any] = {
                "format": "spicy-source-collection",
                **common,
                "collection": {**summary, "periods": periods},
                "routes": routes,
                "tables": tables,
                "files": {"pages": pages, "total": count},
                "planned": [],
                "observations": [],
                "relationships": [],
                "coverage": public_coverage(coverage_by_collection[cid]),
            }
            if retained_responses:
                retained_count = db.execute("SELECT count(*) FROM retained_response_files WHERE collection_id=?", (cid,)).fetchone()[0]
                if retained_count:
                    layout = (page_layout or {}).get((cid, "attention"), {})
                    retained_pages = export_file_pages(db, output, cid=cid, folder=folder, common=common,
                                                      internal=internal, page_size=page_size, retained=True, references=references, publisher=summary.get("publisher"), emit=emit,
                                                      page_rows=layout.get("rows"), legacy_provenance=layout.get("provenance", True))
                    detail["retainedFiles"] = {"pages": retained_pages, "total": retained_count}
            publications = [item for kind, item in evidence if kind == "publication_observation"]
            if publications:
                detail["publicationObservations"] = [public_publication(item) for item in publications]
            mappings = {
                "planned": {
                    "id": "id",
                    "table": "table",
                    "family": "family",
                    "status": "status",
                    "reason": "reason",
                    "route_id": "routeId",
                    "destination_ids": "destinationIds",
                },
                "observations": {
                    "id": "id",
                    "status": "status",
                    "observed_at": "observedAt",
                    "scope": "scope",
                    "declared_count": "declaredCount",
                    "observed_count": "observedCount",
                    "note": "note",
                },
                "relationships": {
                    "id": "id",
                    "kind": "kind",
                    "from_id": "fromId",
                    "to_id": "toId",
                    "scope": "scope",
                    "note": "note",
                },
            }
            for kind, item in evidence:
                # Source-coverage reviews appear only beside their resolved
                # table; the generic observation shape omits version/route pins.
                if kind == "observations" and item.get("metadata_kind") == "source-coverage":
                    continue
                if kind == "errors":
                    kind = "observations"
                if kind in mappings:
                    clean = pick(item, mappings[kind])
                    clean.setdefault("id", kind + ":" + stable(item))
                    if kind == "planned":
                        clean.setdefault("status", "planned")
                    if kind == "observations":
                        clean.setdefault("status", "unknown-scope")
                    # Scope is literal metadata, not arbitrary nested raw evidence.
                    if "scope" in clean and not isinstance(clean["scope"], str):
                        clean["scope"] = safe_text(json.dumps(clean["scope"], sort_keys=True))
                    for key, item_value in list(clean.items()):
                        if isinstance(item_value, str):
                            clean[key] = safe_text(item_value)
                    if kind == "planned" and item.get("tables"):
                        for table in item["tables"]:
                            detail[kind].append(dict(clean, id=clean["id"] + ":" + table, table=table))
                    else:
                        detail[kind].append(clean)
            if internal:
                detail["internalOnly"] = {
                    "evidence": [{"evidenceKind": k, **v} for k, v in evidence],
                    "lineage": [
                        json.loads(r[0])
                        for r in db.execute("SELECT data FROM lineage WHERE collection_id=? ORDER BY id", (cid,))
                    ],
                }
            write_json(output / summary["detailPath"], detail, internal=internal, emit=emit)
        # Probe files_sha for each object instead of materializing all eligible aliases.
        total_objects = db.execute("""SELECT count(*),coalesce(sum(objects.bytes),0) FROM objects
                                    WHERE EXISTS (SELECT 1 FROM files
                                    WHERE files.sha256=objects.sha256 AND files.status NOT IN
                                    ('missing-bytes','receipt-only','known-not-acquired','external-by-choice'))""").fetchone()
        manifest = {
            "format": "spicy-source-catalog",
            **common,
            "generatedAt": metadata["generatedAt"],
            "collections": summaries,
            "totals": {
                "collections": len(summaries),
                "aliases": sum(s["holdings"]["aliases"] for s in summaries),
                "objects": total_objects[0],
                "decodedBytes": total_objects[1],
                "missing": sum(s["holdings"]["missing"] for s in summaries),
            },
        }
        # The switch occurs after every immutable partition has passed validation.
        write_json(output / "manifest.json", manifest, internal=internal, emit=emit)
        return manifest
    finally:
        db.close()
