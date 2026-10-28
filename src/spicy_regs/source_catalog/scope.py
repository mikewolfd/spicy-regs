"""Separate current source use from custody without moving or deleting originals."""

from __future__ import annotations

import json
import re

# These records remain searchable in the retained inventory. They cannot supply
# usable source coverage, even when their collection has a current consumer.
UNUSABLE_STATUSES = (
    "receipt-only", "missing-bytes", "known-not-acquired", "external-by-choice",
    "failed-response", "incomplete-response", "refused-source", "partial-source",
    "conflicting-receipt", "conflicting-metadata", "supporting-evidence", "unclassified-source",
)

SUPPORTING_ROLES = ("supporting-original", "discovery-index", "processing-evidence")


def install_scope(db, policy: dict) -> None:
    """Store a reviewed selection; preserve every underlying inventory row.

    The registry pins this policy on rebuild. Collections added later default to
    retained until reviewed; a new published collection requires a scope update.
    """
    if (not isinstance(policy, dict) or policy.get("format") != "spicy-source-scope"
            or type(policy.get("version")) is not int or policy["version"] not in {1, 2}
            or not isinstance(policy.get("collections"), list)
            or not isinstance(policy.get("excluded_objects"), list)):
        raise ValueError("Invalid source scope policy")
    collections = []
    for item in policy["collections"]:
        if (not isinstance(item, dict) or not isinstance(item.get("id"), str)
                or item.get("disposition") not in {"active", "retained"}
                or not isinstance(item.get("reason"), str) or not item["reason"].strip()):
            raise ValueError("Invalid source collection decision")
        collections.append((item["id"], int(item["disposition"] == "active"), item["reason"]))
    if len({item[0] for item in collections}) != len(collections):
        raise ValueError("Duplicate source collection decision")
    active = {item[0] for item in collections if item[1]}
    published = {row[0] for row in db.execute("SELECT DISTINCT collection_id FROM main.tables")}
    if not published <= active:
        raise ValueError("Source scope omits a published collection: " + ", ".join(sorted(published - active)))
    exclusions = []
    for item in policy["excluded_objects"]:
        if (not isinstance(item, dict)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(item.get("sha256", "")))
                or not isinstance(item.get("reason"), str) or not item["reason"].strip()):
            raise ValueError("Invalid excluded source object")
        collection_id = item.get("collection_id")
        if "collection_id" in item and (
            policy["version"] != 2 or not isinstance(collection_id, str)
            or collection_id not in {row[0] for row in collections}
        ):
            raise ValueError("Invalid excluded source collection")
        exclusions.append((item["sha256"], collection_id, item["reason"]))
    if len({item[:2] for item in exclusions}) != len(exclusions):
        raise ValueError("Duplicate excluded source object")
    db.execute("CREATE TABLE IF NOT EXISTS catalog_scope_collections(id TEXT PRIMARY KEY, active INTEGER NOT NULL, reason TEXT NOT NULL)")
    db.execute("DELETE FROM catalog_scope_collections")
    # This table is a disposable copy of the fully validated policy. Replacing
    # it also upgrades older indexes without rewriting source observations.
    db.execute("DROP TABLE IF EXISTS catalog_scope_exclusions")
    db.execute("CREATE TABLE catalog_scope_exclusions(sha256 TEXT NOT NULL, collection_id TEXT, reason TEXT NOT NULL, PRIMARY KEY(sha256,collection_id))")
    db.executemany("INSERT INTO catalog_scope_collections VALUES(?,?,?)", collections)
    db.executemany("INSERT INTO catalog_scope_exclusions VALUES(?,?,?)", exclusions)
    db.execute("INSERT OR REPLACE INTO metadata VALUES('source_scope',?)", (json.dumps(policy, sort_keys=True),))


def _object_exclusion(db) -> str:
    """Use the same reviewed object exclusions for usable and retained views."""
    # Old policies exclude a hash globally; current policies may scope it.
    columns = {row[1] for row in db.execute("PRAGMA main.table_info(catalog_scope_exclusions)")}
    collection_match = (
        " AND (x.collection_id IS NULL OR x.collection_id=f.collection_id)"
        if "collection_id" in columns else ""
    )
    return f"NOT EXISTS(SELECT 1 FROM main.catalog_scope_exclusions x WHERE x.sha256=f.sha256{collection_match})"


def select_retained_responses(db) -> bool:
    """Expose reviewed active sources' failed, missing or unchecked responses.

    This separate view never changes usable holdings. Receipt and processing
    records stay private, and reviewed object exclusions still apply.
    """
    if not db.execute("SELECT 1 FROM main.sqlite_master WHERE type='table' AND name='catalog_scope_collections'").fetchone():
        return False
    statuses = (
        "missing-bytes", "failed-response", "incomplete-response", "refused-source",
        "partial-source", "conflicting-receipt", "conflicting-metadata", "unclassified-source",
    )
    excluded_roles = (*SUPPORTING_ROLES, "supporting-evidence", "acquisition-receipt", "request-body")
    status_values = ",".join("'" + status + "'" for status in statuses)
    role_values = ",".join("'" + role + "'" for role in excluded_roles)
    db.execute(f"""CREATE TEMP VIEW retained_response_files AS SELECT f.* FROM main.files f
        WHERE f.collection_id IN (SELECT id FROM main.catalog_scope_collections WHERE active=1)
          AND f.status IN ({status_values})
          AND coalesce(json_extract(f.data,'$.role'), '') NOT IN ({role_values})
          AND coalesce(json_extract(f.data,'$.mapping.role'), '') NOT IN ({role_values})
          AND coalesce(json_extract(f.data,'$.mapping.content_metadata.role'), '') NOT IN ({role_values})
          AND {_object_exclusion(db)}""")
    return True


def select_scope(db, scope: str) -> None:
    """Apply read-only SQL views to the same complete inventory.

    No copied catalog, path rewrite, or raw-file mutation is needed. Explicit
    main.* access remains available for custody audits and publication binding.
    """
    if scope not in {"active", "retained", "all"}:
        raise ValueError("scope must be active, retained or all")
    if scope == "all":
        return
    installed = db.execute("SELECT 1 FROM main.sqlite_master WHERE type='table' AND name='catalog_scope_collections'").fetchone()
    if not installed:
        if scope == "retained":
            raise ValueError("This catalog has no reviewed source scope")
        return
    statuses = ",".join("'" + status + "'" for status in UNUSABLE_STATUSES)
    active = f"""f.collection_id IN (SELECT id FROM main.catalog_scope_collections WHERE active=1)
        AND f.status NOT IN ({statuses})
        AND {_object_exclusion(db)}"""
    selected = active if scope == "active" else f"NOT ({active})"
    db.execute(f"CREATE TEMP VIEW files AS SELECT f.* FROM main.files f WHERE {selected}")
    if scope == "active":
        collection_filter = "id IN (SELECT id FROM main.catalog_scope_collections WHERE active=1)"
    else:
        collection_filter = "id NOT IN (SELECT id FROM main.catalog_scope_collections WHERE active=1) OR id IN (SELECT collection_id FROM temp.files)"
    db.execute(f"CREATE TEMP VIEW collections AS SELECT * FROM main.collections WHERE {collection_filter}")
    for table in ("routes", "evidence", "tables", "lineage"):
        db.execute(f"CREATE TEMP VIEW {table} AS SELECT * FROM main.{table} WHERE collection_id IS NULL OR collection_id IN (SELECT id FROM temp.collections)")
    if db.execute("SELECT 1 FROM main.sqlite_master WHERE type='table' AND name='raw_periods'").fetchone():
        db.execute("CREATE TEMP VIEW raw_periods AS SELECT p.* FROM main.raw_periods p WHERE EXISTS(SELECT 1 FROM temp.files f WHERE f.id=p.file_id)")
