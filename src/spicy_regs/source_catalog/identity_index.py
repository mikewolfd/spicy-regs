"""Rebuild-only lookup copies of known native identity and version facts.

The original JSON remains authoritative for evidence and identity/version
association. These tables only select candidates for that existing predicate.
"""

import sqlite3


def prepare(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE file_identity_scalars(file_id TEXT PRIMARY KEY REFERENCES files,
          native_id, source_version);
        CREATE TABLE file_identity_assertions(file_id TEXT REFERENCES files,
          position INTEGER, field TEXT NOT NULL, value, PRIMARY KEY(file_id,position));
    """)


def add(db: sqlite3.Connection, file_id: str, data: str) -> None:
    # Keep SQLite's JSON value types and equality semantics. In particular, do
    # not coerce numbers or booleans to strings. Position preserves repetitions;
    # files.data retains each original assertion, locator and receipt reference.
    db.execute(
        "INSERT INTO file_identity_scalars SELECT ?, native_id, source_version FROM "
        "(SELECT json_extract(?,'$.mapping.native_id') AS native_id, "
        "json_extract(?,'$.mapping.source_version') AS source_version) "
        "WHERE native_id IS NOT NULL OR source_version IS NOT NULL",
        (file_id, data, data),
    )
    db.execute(
        "INSERT INTO file_identity_assertions SELECT ?, e.key, "
        "json_extract(e.value,'$.target'), json_extract(e.value,'$.value') "
        "FROM json_each(?,'$.mapping.evidence') e "
        "WHERE json_extract(e.value,'$.origin')='content' "
        "AND json_extract(e.value,'$.target') IN ('native_id','source_version')",
        (file_id, data),
    )


def finish(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE INDEX file_identity_native ON file_identity_scalars(native_id,file_id);
        CREATE INDEX file_identity_version ON file_identity_scalars(source_version,file_id);
        CREATE INDEX file_identity_assertion_value ON file_identity_assertions(field,value,file_id);
    """)
    db.execute("INSERT INTO metadata VALUES('native_identity_index','1')")


def available(db: sqlite3.Connection) -> bool:
    return db.execute(
        "SELECT 1 FROM main.metadata WHERE key='native_identity_index' AND value='1'"
    ).fetchone() is not None


def candidates(field: str) -> str:
    if field not in {"native_id", "source_version"}:
        raise ValueError("Unsupported native identity filter")
    # IN drives an indexed lookup of files.id, including through scoped views.
    # A correlated EXISTS would still visit every file in the selected catalog.
    return (
        "id IN (SELECT file_id FROM main.file_identity_scalars WHERE " + field + "=? "
        "UNION SELECT file_id FROM main.file_identity_assertions WHERE field=? AND value=?)"
    )
