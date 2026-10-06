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

from spicy_regs.table_joins import KINDS

KEY = "explorer-metadata.v1.json"
FORMAT = "spicy-regs-explorer-metadata"
ROOT = Path(__file__).parent
NAME = re.compile(r"[a-z][a-z0-9_]*\Z")


def read_json(name: str) -> dict:
    return json.loads((ROOT / name).read_text())


def public_url(value: str, *, allow_http: bool = False) -> bool:
    """Require HTTPS for data access; allow saved HTTP publisher links explicitly."""
    parsed = urlsplit(value)
    schemes = {"http", "https"} if allow_http else {"https"}
    return parsed.scheme in schemes and bool(parsed.hostname) and not parsed.username and not parsed.password


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
    if join.get("kind") not in KINDS:
        raise ValueError("Unknown join resolution kind")
    return (join["child"], tuple(join["child_columns"]), join["parent"], tuple(join["parent_columns"]))


def publication_descriptions(live: dict) -> dict:
    """Include canonical prose and roles for every published table."""
    from .fec_query_catalog import table_category

    descriptions = read_json("table_metadata.json")
    missing = set(live) - set(descriptions)
    if missing:
        from .data_dictionary import load_curated_descriptions
        curated = load_curated_descriptions()
        for name in sorted(missing & set(curated)):
            entry = curated[name]
            descriptions[name] = {key: entry[key] for key in ("label", "summary", "coverage", "kind", "category", "row_unit", "data_quality")
                                  if key in entry}
            descriptions[name]["columns"] = [
                {"column_name": column, "column_type": typ,
                 "description": entry.get("columns", {}).get(column, "")}
                # Old curated field names validate declarations but do not
                # become fields in the current output schema below.
                for column, typ in {**dict.fromkeys(entry.get("columns", {}), ""),
                                    **dict(live[name]["schema"])}.items()
            ]
    for name in live.keys() & descriptions.keys():
        category = table_category(name)
        if category:
            descriptions[name]["category"] = category
    return descriptions


def build_bundle(index: dict, *, descriptions: dict | None = None, registry: dict | None = None,
                 join_record: dict | None = None, audit: dict | None = None,
                 scorecard_publishers: list[dict] | None = None, source_revision: str = "unknown",
                 generated_at: str | None = None, extra_tables: dict | None = None) -> dict:
    """Pure construction and validation; reads committed dictionaries by default."""
    live = published_tables(index)
    extra_tables = {name: entry for name, entry in (extra_tables or {}).items() if name not in live}
    for name, entry in extra_tables.items():
        # Use the same exact schema validation as the main publication index.
        extra = published_tables({"format": "spicy-regs-publication", "version": 2,
                                  "families": {entry["family"]: {"tables": {
                                      name + ".parquet": {"columns": entry["publicationSchema"]}}}}})
        identity = "sha256:" + hashlib.sha256(canonical_bytes(entry["descriptor"])).hexdigest()
        if identity != entry["publicationIdentity"] or entry["descriptor"]["family"] != entry["family"]:
            raise ValueError(f"Separate publication identity does not match its descriptor: {name}")
        live.update(extra)
    descriptions = publication_descriptions(live) if descriptions is None else descriptions
    registry = read_json("explorer_sources.json") if registry is None else registry
    join_record = read_json("table_joins.json") if join_record is None else join_record
    if audit is None:
        audit = read_json("join_audit.json") if (ROOT / "join_audit.json").exists() else {}
    known = descriptions
    sources = registry["sources"]
    for source in sources.values():
        if not source.get("name") or not public_url(source.get("url", "")):
            raise ValueError(f"Invalid source attribution: {source}")
    for source in scorecard_publishers or []:
        if not source.get("name") or not public_url(source.get("url", ""), allow_http=True):
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
        if name in extra_tables:
            table["publicationIdentity"] = extra_tables[name]["publicationIdentity"]
        if origin.get("evidence"):
            table["sourceEvidence"] = origin["evidence"]
        if name in audit:
            table["joinAudit"] = audit[name]
        if origin.get("emptyReason"):
            table["emptyReason"] = origin["emptyReason"]
        tables[name] = table

    schemas = {name: [[c["column_name"], c["column_type"]] for c in desc.get("columns", [])]
               for name, desc in known.items()}
    # Validate declarations against recorded or current fields. A publication
    # can remove a previously documented key without making the declaration
    # malformed; that connection is unavailable in this release.
    for name, entry in live.items():
        fields = dict(schemas.get(name, []))
        fields.update(entry["schema"])
        schemas[name] = [[column, typ] for column, typ in fields.items()]
    joins, omitted, seen = [], [], set()
    for join in join_record["joins"]:
        identity = validate_join(join, schemas)
        if identity in seen:
            raise ValueError(f"Duplicate join declaration: {identity}")
        seen.add(identity)
        if join["child"] not in live or join["parent"] not in live:
            omitted.append({"child": join["child"], "parent": join["parent"],
                            "reason": "One or both tables are outside the current public catalogs."})
            continue
        missing_keys = [f"{join[side]}.{column}" for side in ("child", "parent")
                        for column in join[f"{side}_columns"]
                        if column not in {c[0] for c in live[join[side]]["schema"]}]
        if missing_keys:
            omitted.append({"child": join["child"], "parent": join["parent"],
                            "reason": f"Fields no longer published: {', '.join(missing_keys)}."})
            continue
        joins.append(deepcopy(join))
    # Availability belongs to this index, while the audit's evidence belongs to
    # its reviewed snapshot. A disappearing parent must not leave 'connected'.
    from spicy_regs.explorer_navigation import published_navigation
    navigation = published_navigation(join_record.get("navigation", []),
                                      {name: entry["schema"] for name, entry in live.items()})
    receipt_fields = read_json("receipt_fields.json").get("tables", {})
    for name, table in tables.items():
        table["receiptIdentity"] = table.get("identity_columns", [])
        table["receiptContainers"] = receipt_fields.get(name, {}).get("containers", {})
        available = sum(join["child"] == name or join["parent"] == name for join in joins)
        available += sum(spec["source"] == name and spec["available"] and any(t["available"] for t in spec["targets"])
                         or spec["available"] and any(t["table"] == name and t["available"] for t in spec["targets"])
                         for spec in navigation)
        unavailable = [join for join in omitted if name in (join["child"], join["parent"])]
        reviewed = table.get("joinAudit", {})
        if not isinstance(reviewed, dict):
            reviewed = {}
        if available:
            table["joinAudit"] = {**reviewed, "status": "connected", "availableJoins": available,
                                  "reviewedReason": reviewed.get("reason"),
                                  "reason": f"{available} declared relationships are available in the current explorer catalog."}
        elif unavailable:
            table["joinAudit"] = {**reviewed, "status": "missing", "availableJoins": 0,
                                  "reviewedReason": reviewed.get("reason"),
                                  "reason": "Declared relationships are unavailable in the current publication. See each connection’s reason."}
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
            "extra_tables": extra_tables, "tables": tables, "joins": joins, "omittedJoins": omitted,
            "navigation": navigation}


def canonical_bytes(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def same_metadata(previous: dict, proposed: dict) -> bool:
    """Data/metadata changes republish; a timer alone does not bump generatedAt."""
    return ({k: v for k, v in previous.items() if k != "generatedAt"}
            == {k: v for k, v in proposed.items() if k != "generatedAt"})
