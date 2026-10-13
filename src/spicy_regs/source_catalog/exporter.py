"""Generation-bound, paginated exports with a fixed public field allowlist."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .storage import connect, stable
from .coverage import coverage


def pick(value: dict, mapping: dict[str, str]) -> dict:
    return {public: value[internal] for internal, public in mapping.items() if value.get(internal) is not None}


def public_url(value: str | None) -> str | None:
    if not value:
        return None
    parts = urlsplit(value)
    # Catalog URLs containing queries may carry credentials or signed tokens.
    # Preserve their exact values internally; publish only credential-free links.
    if parts.scheme not in ["http", "https"] or parts.username or parts.password or parts.query or parts.fragment:
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


def write_json(path: Path, value: dict, *, internal: bool) -> None:
    if not internal:
        validate_public(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    body = json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
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


def export(index: Path, output: Path, *, internal: bool = False, page_size: int = 500) -> dict:
    if page_size < 1 or page_size > 5000:
        raise ValueError("page_size must be between 1 and 5000")
    db = connect(index, readonly=True)
    try:
        metadata = {r["key"]: json.loads(r["value"]) for r in db.execute("SELECT * FROM metadata")}
        generation = metadata["generation"]
        prefix = "generations/" + generation.removeprefix("sha256:")
        common = {"version": 1, "generation": generation}
        if internal:
            common["audience"] = "internal"
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
            if not count:
                statuses.add("known-not-acquired")
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
                pick(item, {"kind": "kind", "label": "label", "start": "start", "end": "end", "basis": "basis"})
                for kind, item in evidence
                if kind == "periods"
            ]
            if not periods:
                periods = [{"kind": "unknown", "label": "Unknown", "basis": "no-qualified-period-assertion"}]
            routes = []
            for row in db.execute("SELECT data FROM routes WHERE collection_id=? ORDER BY id", (cid,)):
                item = json.loads(row[0])
                route = {
                    "id": item["id"],
                    "label": safe_text(item.get("label", item["id"])),
                    "deliveryMethod": item.get("delivery_method", "unknown"),
                    "lifecycle": item.get("lifecycle", "unknown"),
                    "custodian": safe_text(item.get("custodian", "unknown")),
                    "contentBehavior": item.get("content_behavior", "unknown"),
                    "periods": [
                        pick(p, {"kind": "kind", "label": "label", "start": "start", "end": "end", "basis": "basis"})
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
                tables.append(
                    pick(
                        json.loads(row[0]),
                        {
                            "id": "id",
                            "family": "family",
                            "status": "status",
                            "artifact_digest": "artifactDigest",
                            "coverage_note": "coverageNote",
                        },
                    )
                )
            summary: dict[str, Any] = {
                "id": cid,
                "title": safe_text(collection["title"]),
                "statuses": sorted(statuses),
                "families": sorted({t["family"] for t in tables} | set(data.get("families", []))),
                "tables": sorted(t["id"] for t in tables),
                "routeCount": len(routes),
                "holdings": {"aliases": count, "objects": unique[0], "decodedBytes": unique[1], "missing": missing},
                "periods": periods,
                "detailPath": folder + "/collection.json",
            }
            for key in ["publisher", "description"]:
                if data.get(key):
                    display = data[key]
                    if key == "publisher" and isinstance(display, dict):
                        display = display.get("name") or display.get("title") or "Unknown publisher"
                    summary[key] = safe_text(display)
            summaries.append(summary)
            pages = []
            cursor = db.execute("SELECT * FROM files WHERE collection_id=? ORDER BY id", (cid,))
            page_number = 0
            while batch := cursor.fetchmany(page_size):
                files = []
                for row in batch:
                    item = {"id": row["id"], "name": safe_text(row["name"]), "status": row["status"]}
                    for sql, public in [("sha256", "sha256"), ("bytes", "bytes"), ("route_id", "routeId")]:
                        if row[sql] is not None:
                            item[public] = row[sql]
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
                page_path = f"{folder}/files-{page_number:05d}.json"
                write_json(
                    output / page_path,
                    {"format": "spicy-source-files", **common, "collectionId": cid, "files": files},
                    internal=internal,
                )
                pages.append({"path": page_path, "rows": len(files)})
                page_number += 1
            detail: dict[str, Any] = {
                "format": "spicy-source-collection",
                **common,
                "collection": summary,
                "routes": routes,
                "tables": tables,
                "files": {"pages": pages, "total": count},
                "planned": [],
                "observations": [],
                "relationships": [],
                "coverage": public_coverage(coverage(db, cid)["collections"][0]),
            }
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
            write_json(output / summary["detailPath"], detail, internal=internal)
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
        write_json(output / "manifest.json", manifest, internal=internal)
        return manifest
    finally:
        db.close()
