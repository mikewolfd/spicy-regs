"""Evidence-scoped coverage accounting over a pinned offline catalog.

Measurements with different units, routes or periods are never divided. The
index records retained evidence, not a fresh integrity or publisher census.
"""

from __future__ import annotations

import json

from .scope import UNUSABLE_STATUSES

STAGES = ("expected", "advertised", "acquired", "processed", "published")


def coverage(db, collection_id: str | None = None) -> dict:
    metadata = {row["key"]: json.loads(row["value"]) for row in db.execute("SELECT * FROM metadata")}
    raw_archive = db.execute("SELECT id FROM evidence WHERE kind='raw-archive' LIMIT 1").fetchone()
    capture = {}
    capture_id = None
    for row in db.execute("SELECT id,data FROM evidence WHERE kind='corpus_catalog' ORDER BY id"):
        selected = json.loads(row[1]).get("publication_capture", {})
        if selected:
            if capture and selected != capture:
                raise ValueError("Ambiguous publication captures in the selected index")
            capture, capture_id = selected, row[0]
    params = (collection_id,) if collection_id else ()
    where = " WHERE id=?" if collection_id else ""
    results = []
    for row in db.execute("SELECT id FROM collections" + where + " ORDER BY id", params):
        cid = row[0]
        stages = {stage: {"status": "unknown", "measurements": []} for stage in STAGES}
        evidence = [
            (r[0], r[1], json.loads(r[2]))
            for r in db.execute("SELECT id,kind,data FROM evidence WHERE collection_id=? ORDER BY id", (cid,))
        ]
        periods = [{"evidence_id": ident, **item} for ident, kind, item in evidence if kind == "periods"]

        def add(stage, *, count, unit, scope, evidence_ids, version=None, verification, route_id=None, period=None):
            stages[stage]["status"] = "evidence-recorded"
            stages[stage]["measurements"].append(
                {
                    "count": count,
                    "unit": unit,
                    "scope": scope,
                    "route_id": route_id,
                    "period": period,
                    "version": version,
                    "evidence_ids": evidence_ids,
                    "verification": verification,
                }
            )

        # Explicit denominator assertions must state their unit and selected scope.
        # Merely observing a date range or a publisher total does not create one.
        for ident, kind, item in evidence:
            assertion = item.get("coverage_measurement") if kind == "observations" else None
            if not assertion:
                continue
            stage = assertion.get("stage")
            count = assertion.get("count")
            if (
                stage != "expected"
                or type(count) is not int
                or count < 0
                or not isinstance(assertion.get("scope"), dict)
                or not assertion["scope"]
                or any(
                    not isinstance(assertion.get(k), str) or not assertion[k].strip()
                    for k in ("unit", "basis", "version")
                )
            ):
                raise ValueError(f"Incomplete coverage measurement: {ident}")
            add(
                stage,
                count=count,
                unit=assertion["unit"],
                scope=assertion["scope"],
                evidence_ids=[ident],
                version=assertion["version"],
                verification=assertion["basis"],
                route_id=assertion.get("route_id"),
                period=assertion.get("period"),
            )

        files = (
            (r, json.loads(r["data"]))
            for r in db.execute("SELECT * FROM files WHERE collection_id=? ORDER BY id", (cid,))
        )
        advertised = {}
        acquired = {}
        for file, facts in files:
            if facts.get("observation_id"):
                key = (file["route_id"], facts["observation_id"])
                advertised.setdefault(key, set()).add(facts.get("id", file["id"]))
            if (
                "encoded_bytes" in facts
                and file["sha256"]
                and file["status"]
                not in UNUSABLE_STATUSES
            ):
                # Raw archive membership is already bound by one manifest evidence
                # record. Keep the count without copying every file ID into each
                # public collection document; the paged file listing carries them.
                if raw_archive:
                    acquired[file["route_id"]] = acquired.get(file["route_id"], 0) + 1
                else:
                    acquired.setdefault(file["route_id"], []).append(file["id"])
        for (route, observation), items in sorted(advertised.items(), key=lambda pair: str(pair[0])):
            add(
                "advertised",
                count=len(items),
                unit="publisher-metadata-item",
                scope={"collection_id": cid, "observation_id": observation},
                route_id=route,
                evidence_ids=[observation],
                version=observation,
                verification="one retained observation; not a complete publisher inventory",
            )
        for route, ids in sorted(acquired.items(), key=lambda pair: str(pair[0])):
            add(
                "acquired",
                count=ids if raw_archive else len(ids),
                unit="retained-file-record",
                scope={"collection_id": cid},
                route_id=route,
                evidence_ids=[raw_archive[0]] if raw_archive else ids,
                version=metadata.get("generation"),
                verification="manifest hash and local presence recorded at index build; bytes not rehashed",
            )
        for ident, kind, item in evidence:
            if kind != "planned" or not item.get("qualification_input"):
                continue
            for candidate in item.get("local_candidate_outputs", []):
                if not candidate.get("table") or not candidate.get("sha256"):
                    continue
                rows = candidate.get("rows")
                if rows is not None and (type(rows) is not int or rows < 0):
                    raise ValueError(f"Invalid qualified candidate row count: {ident}")
                add(
                    "processed",
                    count=rows if rows is not None else 1,
                    unit="qualified-candidate-table-record"
                    if rows is not None
                    else "qualified-candidate-table-version",
                    scope={
                        "collection_id": cid,
                        "table": candidate["table"],
                        "qualification_scope": item.get("qualification_scope"),
                    },
                    route_id=item.get("route_id"),
                    evidence_ids=[ident],
                    version=candidate["sha256"],
                    verification="recorded local qualification; candidate and proof not revalidated by this report",
                )
        # Importer-validated observations add dated snapshots, never replace old ones.
        for ident, kind, item in evidence:
            if kind != "publication_observation" or item.get("associationStatus") != "exact-family":
                continue
            for member in item["tables"]:
                add(
                    "published",
                    count=1,
                    unit="captured-table-artifact-version",
                    scope={
                        "collection_id": cid,
                        "table": member["table"].removesuffix(".parquet"),
                        "captured_at": item["captureAt"],
                    },
                    period={"kind": "family-publication-instant", "label": item["familyPublishedAt"]},
                    evidence_ids=[ident],
                    version=item["artifactDigest"],
                    verification="retained publication descriptor; member content and native qualification not revalidated",
                )
        for table in db.execute("SELECT id,data FROM tables WHERE collection_id=? ORDER BY id", (cid,)):
            facts = json.loads(table[1])
            if facts.get("artifact_digest") and capture.get("sha256") and capture.get("at"):
                add(
                    "published",
                    count=1,
                    unit="captured-table-artifact-version",
                    scope={"collection_id": cid, "table": table[0], "captured_at": capture["at"]},
                    evidence_ids=[capture_id],
                    version=facts["artifact_digest"],
                    verification="retained publication snapshot; no live readback performed",
                )
        results.append(
            {
                "collection_id": cid,
                "period_assertions": periods,
                "stages": stages,
                "completeness_denominator": None,
                "limit": "Measurements apply only to their scope; unknown periods and denominators remain unknown.",
            }
        )
    return {
        "format": "spicy-source-coverage",
        "version": 1,
        "catalog_generation": metadata.get("generation"),
        "publication_capture": capture,
        "collections": results,
    }
