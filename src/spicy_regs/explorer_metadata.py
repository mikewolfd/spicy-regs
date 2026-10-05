"""Versioned public descriptions and navigation metadata, separate from data pointers.

The publication index decides which tables exist. A description never creates a
table, a matching column name never creates a join, and a missing description
never hides a newly published table. Only the JSON bundle is written by this
publisher; no Parquet, container image, or publication pointer is changed.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

KEY = "explorer-metadata.v1.json"
FORMAT = "spicy-regs-explorer-metadata"
ROOT = Path(__file__).parent
NAME = re.compile(r"[a-z][a-z0-9_]*\Z")


def read_json(name: str) -> dict:
    return json.loads((ROOT / name).read_text())


def public_url(value: str) -> bool:
    """Only web links, with no embedded credentials, can be published in the UI."""
    parsed = urlsplit(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.hostname) and not parsed.username and not parsed.password


def published_tables(index: dict) -> dict:
    tables = {}
    if index.get("format") != "spicy-regs-publication" or index.get("version") != 2:
        raise ValueError("Expected a version-2 publication index")
    for family, entry in index["families"].items():
        for filename, descriptor in entry["tables"].items():
            name = filename.removesuffix(".parquet")
            if not filename.endswith(".parquet") or not NAME.fullmatch(name) or name in tables:
                raise ValueError(f"Invalid or repeated table: {filename}")
            columns = descriptor["columns"]
            if (not isinstance(columns, list) or not columns or
                    any(not isinstance(c, list) or len(c) != 2 or not all(isinstance(x, str) and x for x in c)
                        for c in columns) or len({c[0] for c in columns}) != len(columns)):
                raise ValueError(f"Invalid published schema: {name}")
            tables[name] = {"family": family, "schema": columns, "descriptor": descriptor}
    return tables


def validate_join(join: dict, schemas: dict[str, list]) -> tuple:
    """Validate the entire composite key, without inventing a cardinality claim."""
    for side in ("child", "parent"):
        table, columns = join.get(side), join.get(f"{side}_columns")
        if table not in schemas:
            raise ValueError(f"Join names an undocumented table: {table}")
        if (not isinstance(columns, list) or not columns or not all(isinstance(c, str) for c in columns)
                or len(set(columns)) != len(columns)):
            raise ValueError(f"Join has invalid {side} columns: {join}")
        missing = set(columns) - {c[0] for c in schemas[table]}
        if missing:
            raise ValueError(f"Join {table} names missing columns: {sorted(missing)}")
    if len(join["child_columns"]) != len(join["parent_columns"]):
        raise ValueError("Join composite keys must have the same number of columns")
    if join.get("expected_cardinality", "unspecified") not in ("unspecified", "one", "many"):
        raise ValueError("Join cardinality must be one, many, or unspecified")
    if join.get("kind") not in ("complete", "scope", "design", "empty", "unmeasured"):
        raise ValueError("Unknown join resolution kind")
    return (join["child"], tuple(join["child_columns"]), join["parent"], tuple(join["parent_columns"]))


def build_bundle(index: dict, *, descriptions: dict | None = None, registry: dict | None = None,
                 join_record: dict | None = None, audit: dict | None = None,
                 scorecard_publishers: list[dict] | None = None, source_revision: str = "unknown",
                 generated_at: str | None = None) -> dict:
    """Pure construction and validation; reads committed dictionaries by default."""
    descriptions = read_json("table_metadata.json") if descriptions is None else descriptions
    registry = read_json("explorer_sources.json") if registry is None else registry
    join_record = read_json("table_joins.json") if join_record is None else join_record
    if audit is None:
        audit = read_json("join_audit.json") if (ROOT / "join_audit.json").exists() else {}
    known = descriptions
    live = published_tables(index)
    sources = registry["sources"]
    for source in sources.values():
        if not source.get("name") or not public_url(source.get("url", "")):
            raise ValueError(f"Invalid source attribution: {source}")
    for source in scorecard_publishers or []:
        if not source.get("name") or not public_url(source.get("url", "")):
            raise ValueError("Invalid published scorecard publisher")

    tables = {}
    for name, live_table in sorted(live.items()):
        description = deepcopy(known.get(name, {}))
        schema = live_table["schema"]
        documented_columns = {c["column_name"]: c for c in description.get("columns", [])}
        # The live index owns field presence/types, including new undocumented fields.
        description["columns"] = [{**documented_columns.get(column, {}), "column_name": column,
                                   "column_type": typ} for column, typ in schema]
        identity = description.get("identity_columns", [])
        missing_identity = sorted(set(identity) - {column for column, _ in schema})
        if missing_identity:
            # A composite identity is indivisible; never advertise a partial key.
            description.pop("identity_columns", None)
            description["unavailableIdentity"] = {
                "columns": identity, "missing_columns": missing_identity,
                "reason": "Selected published schema does not expose the complete declared identity.",
            }
        family = live_table["family"]
        origin = {**registry.get("families", {}).get(family, {}), **registry.get("tables", {}).get(name, {})}
        source_entries = [deepcopy(sources[key]) for key in origin.get("sources", [])]
        if family in ("scorecards", "scorecard-analysis"):
            source_entries = deepcopy(scorecard_publishers or []) + source_entries
        inputs = origin.get("inputs", [])
        if not isinstance(inputs, list) or any(not isinstance(t, str) or not NAME.fullmatch(t) for t in inputs):
            raise ValueError(f"Invalid derived inputs: {name}")
        missing_inputs = set(inputs) - set(known) - set(live)
        if missing_inputs or name in inputs:
            raise ValueError(f"Unknown or self-referential derived inputs for {name}: {missing_inputs}")
        table = {**description, "table": name, "family": family, "publicationSchema": schema,
                 "sources": source_entries, "inputs": inputs,
                 "transformation": origin.get("transformation"),
                 "modelGenerated": origin.get("modelGenerated", False),
                 "sourceStatus": "documented" if source_entries else "unknown",
                 "metadataStatus": "documented" if name in known else "unknown"}
        if origin.get("evidence"):
            table["sourceEvidence"] = origin["evidence"]
        if name in audit:
            table["joinAudit"] = audit[name]
        if origin.get("emptyReason"):
            table["emptyReason"] = origin["emptyReason"]
        tables[name] = table

    schemas = {name: [[c["column_name"], c["column_type"]] for c in desc.get("columns", [])]
               for name, desc in known.items()}
    for name, entry in live.items():
        if not schemas.get(name):
            schemas[name] = entry["schema"]
    joins, omitted, seen = [], [], set()
    for join in join_record["joins"]:
        identity = validate_join(join, schemas)
        if identity in seen:
            raise ValueError(f"Duplicate join declaration: {identity}")
        seen.add(identity)
        if join["child"] not in live or join["parent"] not in live:
            omitted.append({"child": join["child"], "parent": join["parent"],
                            "reason": "One or both tables are outside publication.v2.json."})
            continue
        missing = {side: sorted(set(join[f"{side}_columns"]) -
                               {c[0] for c in live[join[side]]["schema"]}) for side in ("child", "parent")}
        if any(missing.values()):
            omitted.append({"child": join["child"], "parent": join["parent"],
                            "reason": "Selected published schemas do not expose the declared join fields.",
                            "missing_columns": missing})
            continue
        joins.append(deepcopy(join))
    # Availability belongs to this index, while the audit's evidence belongs to
    # its reviewed snapshot. A disappearing parent must not leave 'connected'.
    for name, table in tables.items():
        available = sum(join["child"] == name or join["parent"] == name for join in joins)
        unavailable = [join for join in omitted if name in (join["child"], join["parent"])]
        reviewed = table.get("joinAudit", {})
        if not isinstance(reviewed, dict):
            reviewed = {}
        if available:
            table["joinAudit"] = {**reviewed, "status": "connected", "availableJoins": available,
                                  "reviewedReason": reviewed.get("reason"),
                                  "reason": f"{available} declared relationships have both endpoints in the current public index."}
        elif unavailable:
            causes = []
            if any("missing_columns" in join for join in unavailable):
                causes.append("selected schemas lack declared join fields")
            if any("missing_columns" not in join for join in unavailable):
                causes.append("endpoints are outside the current public index")
            table["joinAudit"] = {**reviewed, "status": "missing", "availableJoins": 0,
                                  "reviewedReason": reviewed.get("reason"),
                                  "reason": "Navigation is unavailable because " + " and ".join(causes) + "."}
        elif reviewed.get("status") == "connected":
            table["joinAudit"] = {**reviewed, "status": "missing", "availableJoins": 0,
                                  "reviewedReason": reviewed.get("reason"),
                                  "reason": "The reviewed connections are no longer declared for this publication."}
        if unavailable:
            table["unavailableJoins"] = unavailable
    return {"format": FORMAT, "version": 1,
            "generatedAt": generated_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "sourceRevision": source_revision,
            "publication": {"sha256": "sha256:" + hashlib.sha256(canonical_bytes(index)).hexdigest(),
                            "families": {f: e["artifactDigest"] for f, e in index["families"].items()}},
            "tables": tables, "joins": joins, "omittedJoins": omitted}


def canonical_bytes(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def same_metadata(previous: dict, proposed: dict) -> bool:
    """Data/metadata changes republish; a timer alone does not bump generatedAt."""
    return ({k: v for k, v in previous.items() if k != "generatedAt"}
            == {k: v for k, v in proposed.items() if k != "generatedAt"})
