"""Cloudflare R2 Data Catalog (Apache Iceberg) connector.

The internal, write-side table format for the ETL. The catalog is **R2 Data
Catalog** — a managed Iceberg REST catalog built into the same R2 bucket the
project already publishes Parquet to, so no separate metastore (Glue/Nessie/
Postgres) has to be stood up.

DuckDB's ``iceberg`` and ``httpfs`` extensions provide catalog access. Version
1.5.3 or later supports the nullable-column addition used by PDF diagnostics.
The current upsert implementation uses a checked atomic MERGE, keeping the row with
the most recent ``modify_date`` for each primary key.

This module is the "Iceberg load" stage only, mirroring the thin-wrapper style
of :mod:`spicy_regs.sources.r2`:

* :func:`merge_and_export` — ensure the table exists, MERGE the per-agency
  staging Parquet in, then export a public ``{name}.parquet`` snapshot so the
  no-credentials CLI / MCP read path keeps working (the "dual model").
* :func:`merge_comments` upserts staged comments without rebuilding public files.
  :func:`export_public_comments` builds the compatible mirror and index together
  at the end of a successful ingestion sweep.

Credentials are read from the environment, alongside the existing ``R2_*`` vars:

* ``R2_CATALOG_URI``        — the Iceberg REST catalog endpoint (catalog-uri)
* ``R2_CATALOG_WAREHOUSE``  — the warehouse name
* ``R2_CATALOG_TOKEN``      — an R2 API token with R2 + data-catalog permissions
* ``R2_CATALOG_NAMESPACE``  — Iceberg namespace/schema (optional, default ``default``)
"""

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from loguru import logger

from spicy_regs.schemas import RecordType
from spicy_regs.duckdb_settings import ExportResources

if TYPE_CHECKING:
    import polars as pl

# DuckDB alias the attached catalog is addressed by (``<alias>.<namespace>.<table>``).
_CATALOG_ALIAS = "reg_catalog"

# Required environment variables for the catalog connection.
_REQUIRED_ENV = ("R2_CATALOG_URI", "R2_CATALOG_WAREHOUSE", "R2_CATALOG_TOKEN")


def is_configured() -> bool:
    """True when every credential needed to reach the catalog is present."""
    return all(getenv(var) for var in _REQUIRED_ENV)


def _namespace() -> str:
    # `or "default"` (not getenv's default arg) so an env var set to an empty
    # string — e.g. a GitHub Actions `${{ secrets.R2_CATALOG_NAMESPACE }}` that
    # resolves to "" when the secret is unset — still falls back to "default".
    return getenv("R2_CATALOG_NAMESPACE") or "default"


def _schema_ref() -> str:
    """Quoted ``alias."namespace"`` reference (the default namespace is a keyword)."""
    return f'{_CATALOG_ALIAS}."{_namespace()}"'


def _sql_str(value: str) -> str:
    """Escape a value for inlining inside a single-quoted SQL literal."""
    return value.replace("'", "''")


def _connect():
    """Open a DuckDB connection with the R2 Data Catalog attached.

    ``CREATE SECRET`` / ``ATTACH`` do not accept bind parameters, so the
    credentials are inlined with single-quote escaping. The token never leaves
    this process — it is read from the environment, used to attach, and the
    connection is closed by the caller.
    """
    import duckdb

    if not is_configured():
        missing = [var for var in _REQUIRED_ENV if not getenv(var)]
        raise RuntimeError("R2 Data Catalog is not configured; missing env var(s): " + ", ".join(missing))

    uri = getenv("R2_CATALOG_URI", "")
    warehouse = getenv("R2_CATALOG_WAREHOUSE", "")
    token = getenv("R2_CATALOG_TOKEN", "")

    con = duckdb.connect()
    # avro is installed explicitly before iceberg: DuckDB 1.5's iceberg extension
    # pulls in `avro` to read Iceberg manifests and otherwise auto-installs it
    # lazily during LOAD, a nested install that fails in sandboxes without a
    # writable home directory. Provisioning it on the top-level path avoids that.
    try:
        con.execute("INSTALL avro; LOAD avro;")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{_sql_str(token)}');")
        con.execute(f"ATTACH '{_sql_str(warehouse)}' AS {_CATALOG_ALIAS} (TYPE ICEBERG, ENDPOINT '{_sql_str(uri)}');")
    except Exception:
        con.close()
        raise
    return con


CATALOG_LOCK_ENV = "SPICY_REGS_CATALOG_LOCK"
_warned_unlocked: set[str] = set()


def _warn_unlocked_write(record_type: RecordType, operation: str) -> None:
    """Warn once per table when this process writes the catalog without the workflow lock.

    Workflows in the ``comments-catalog-write`` concurrency group set
    ``SPICY_REGS_CATALOG_LOCK``. Any other write (rows or schema) can land
    while the ETL or mirror job exports, and that export then refuses with
    "Catalog changed during export" (ETL 36351853866). The write proceeds.
    """
    if getenv(CATALOG_LOCK_ENV) or record_type.name in _warned_unlocked:
        return
    _warned_unlocked.add(record_type.name)
    logger.warning(
        "{}: writing catalog table {} without {} (the comments-catalog-write group). A concurrent ETL or "
        "mirror export will refuse with 'Catalog changed during export'. Run catalog repairs and migrations "
        "only when no ETL or mirror run holds comments-catalog-write.",
        operation, record_type.name, CATALOG_LOCK_ENV,
    )


def _qualified(record_type: RecordType) -> str:
    """Fully-qualified catalog table identifier: ``alias."namespace"."name"``."""
    return f'{_schema_ref()}."{record_type.name}"'


def _ensure_table(con, record_type: RecordType) -> str | None:
    """Create the namespace + table (all columns VARCHAR) if they don't exist.

    The schema mirrors the published Parquet: every column is a UTF-8 string
    (see :mod:`spicy_regs.schemas.regulations`), so a flat ``VARCHAR`` table is
    a faithful representation and keeps upserts/export type-safe. Returns the
    nullable column added to an existing table, if any. Iceberg callers must
    reopen after each change; :func:`_connect_for_table` handles this.
    """
    columns = ", ".join(f'"{col}" VARCHAR' for col in record_type.schema)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_schema_ref()};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {_qualified(record_type)} ({columns});")
    return _ensure_nullable_column(con, record_type)


_PDF_RESULTS_COLUMN = "pdf_extraction_results_json"
#: Nullable comment fields added after the catalog table was created; a row written before reads them as NULL (unread).
_COMMENT_ADDED_COLUMNS = (
    "comment_on_document_id", "comment_on_object_id", "original_document_id", "comment_reference_values_json",
    "subtype", "duplicate_comments",
)


def _column_types(con, record_type: RecordType) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f"DESCRIBE {_qualified(record_type)}").fetchall()}


def _ensure_nullable_column(con, record_type: RecordType, *, existing: dict[str, str] | None = None) -> str | None:
    """Add one explicitly supported nullable field; never invent source values.

    A migration uses one ALTER per connection because Iceberg can cache the old
    schema. Validate all existing migration fields before changing any of them.
    """
    if record_type.name not in ("documents", "comments"):
        return None
    columns = (_PDF_RESULTS_COLUMN,) + (
        _COMMENT_ADDED_COLUMNS if record_type.name == "comments" else ("attachment_records_json",)
    )
    columns = tuple(column for column in columns if column in record_type.schema)
    table = _qualified(record_type)
    existing = _column_types(con, record_type) if existing is None else existing
    for column in columns:
        if column in existing and existing[column] != "VARCHAR":
            raise ValueError(f"{table}.{column} must be VARCHAR, found {existing[column]}")
    for column in columns:
        if column not in existing:
            con.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" VARCHAR;')
            return column
    return None


def _connect_for_table(record_type: RecordType):
    """Prepare the table and return a connection with its current schema.

    DuckDB's Iceberg catalog caches the pre-ALTER schema on the connection.
    Reopen after each migration, then verify that field before adding another.
    A failed reopen leaves completed DDL for the next run.
    """
    _warn_unlocked_write(record_type, "prepare for write")
    con = _connect()
    try:
        if dedupe_recovery_pending(con, record_type):
            raise RuntimeError(f"Unfinished dedupe for {record_type.name}; recover it before writing or exporting")
        changed = _ensure_table(con, record_type)
    except Exception:
        con.close()
        raise
    while changed is not None:
        con.close()
        con = _connect()
        try:
            existing = _column_types(con, record_type)
            existing_type = existing.get(changed)
            if existing_type != "VARCHAR":
                raise ValueError(
                    f"{_qualified(record_type)}.{changed} must be VARCHAR after migration, "
                    f"found {existing_type or 'missing'}"
                )
            changed = _ensure_nullable_column(con, record_type, existing=existing)
        except Exception:
            con.close()
            raise
    return con


def _staging_files(staging_dir: Path, record_type: RecordType) -> list[Path]:
    """Per-agency staging Parquet files for this record type (see write_staging)."""
    staging_type_dir = staging_dir / record_type.name
    if not staging_type_dir.exists():
        return []
    return sorted(staging_type_dir.glob("*.parquet"))


def _scope_predicate(record_type: RecordType, scope: Mapping[str, str] | None, alias: str = "") -> str:
    """Render column-equals-literal constraints as a trusted SQL predicate (``TRUE`` when unscoped)."""
    if not scope:
        return "TRUE"
    unknown = sorted(set(scope) - set(record_type.schema))
    if unknown:
        raise ValueError(f"Catalog replacement scope names unknown columns: {', '.join(unknown)}")
    return " AND ".join(f"{alias}\"{column}\" = '{_sql_str(value)}'" for column, value in scope.items())


def replace_rows(
    con, record_type: RecordType, source: str, *, expected_prior: str | None = None,
    scope: Mapping[str, str] | None = None,
) -> None:
    """Atomically replace selected unique keys, validating values before commit.

    ``source`` must be a self-contained temp table, never a projection over the
    live table (see :func:`upsert_comment_text`). Shared by the ETL upsert
    (:func:`_merge`), the text fills and the comment source repair.
    Read-modify-write callers pass a self-contained ``expected_prior``; its rows
    and absences must still match inside the write transaction. Direct
    replacements may omit it.

    ``scope`` (column → value, e.g. one ``agency_code``) restricts every catalog
    read and the MERGE match to rows with those values; every source row must
    carry them. A same-key row outside the scope is neither read nor replaced.

    Inside the transaction the target rows for the source keys are copied once;
    the prior and duplicate checks read that copy. Only the MERGE and its
    readback read the catalog table again, so an unscoped call scans it three
    times, not four.
    """
    key = record_type.dedup_key
    tbl = _qualified(record_type)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)
    in_scope = _scope_predicate(record_type, scope)
    target_in_scope = _scope_predicate(record_type, scope, "t.")
    count, distinct, missing, outside = con.execute(f'''
        SELECT count(*),count(DISTINCT "{key}"),count(*) FILTER (WHERE "{key}" IS NULL OR trim("{key}")=''),
               count(*) FILTER (WHERE ({in_scope}) IS NOT TRUE)
        FROM {source}
    ''').fetchone()
    if missing or count != distinct:
        raise ValueError("Catalog replacements require distinct, nonblank source identities")
    if outside:
        raise ValueError("Catalog replacement source rows fall outside the requested scope")
    if not count:
        return
    _warn_unlocked_write(record_type, "replace rows")
    target = f"_replace_target_{record_type.name}"
    con.execute("BEGIN")
    try:
        # One catalog read inside the write transaction; the checks below use
        # this copy. The temp catalog is exempt from DuckDB's one-database-per-
        # transaction write rule, and ROLLBACK discards it with the write.
        con.execute(f'CREATE OR REPLACE TEMP TABLE {target} AS SELECT {col_list} FROM {tbl} '
                    f'WHERE "{key}" IN (SELECT "{key}" FROM {source}) AND {in_scope}')
        if expected_prior is not None:
            # Compare the prepared preimage inside this write transaction, including
            # expected absence. A newly inserted, deleted or changed row must not be
            # overwritten by a winner selected before the transaction began.
            actual = f"SELECT {col_list} FROM {target}"
            expected = (f'SELECT {col_list} FROM {expected_prior} '
                        f'WHERE "{key}" IN (SELECT "{key}" FROM {source}) AND {in_scope}')
            changed = con.execute(f"SELECT 1 FROM (({actual} EXCEPT ALL {expected}) "
                                  f"UNION ALL ({expected} EXCEPT ALL {actual})) LIMIT 1").fetchone()
            if changed:
                raise RuntimeError("Catalog replacement prior changed after preparation; rerun the operation")
        duplicate = con.execute(f'SELECT "{key}" FROM {target} GROUP BY "{key}" HAVING count(*)<>1 LIMIT 1').fetchone()
        if duplicate:
            raise ValueError("Catalog replacement found duplicate prior identities; reconcile them before writing")
        assignments = ", ".join(f'"{c}"=s."{c}"' for c in record_type.schema)
        values = ", ".join(f's."{c}"' for c in record_type.schema)
        con.execute(f'''MERGE INTO {tbl} t USING {source} s ON t."{key}"=s."{key}" AND {target_in_scope}
            WHEN MATCHED THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({values})''')
        same = " AND ".join(f't."{c}" IS NOT DISTINCT FROM s."{c}"' for c in record_type.schema)
        found, matching = con.execute(f'''
            SELECT count(*),count(*) FILTER (WHERE {same})
            FROM {tbl} t JOIN {source} s ON t."{key}"=s."{key}"
            WHERE {target_in_scope}
        ''').fetchone()
        if found != count or matching != count:
            raise RuntimeError("Catalog replacement did not preserve one exact row per source identity")
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
            # ROLLBACK normally discards the copy; drop any survivor explicitly.
            con.execute(f"DROP TABLE IF EXISTS {target}")
        except Exception:
            # Failed COMMIT can already abort the transaction. Preserve the
            # original write/conflict error rather than the cleanup failure.
            pass
        raise
    con.execute(f"DROP TABLE IF EXISTS {target}")


def _merge(con, staging_files: list[Path], record_type: RecordType) -> int:
    """Row-level upsert of the staged rows into the Iceberg table.

    Uses a single MERGE with transactional readback (:func:`replace_rows`).

    Mirrors the dedup semantics of ``transforms.merge_staging_files``: collapse
    the staging rows to one per key (latest ``modify_date`` wins), keep only the
    keys whose incoming row is brand-new or strictly newer than the table's, then
    replace exactly those keys. Deleting by key (not by agency) is essential — a
    since-year-filtered run stages only a slice, so an agency-wide delete would
    drop the rows it isn't re-inserting. ``modify_date`` is an ISO-8601 string,
    so the lexical ``>`` comparison orders chronologically.

    Older DELETE/INSERT writes could leave physical duplicates: on the R2 Data
    Catalog a DELETE did not reliably remove the prior row (f70e2e1, 903f28a).
    MERGE support requires DuckDB >=1.5.3; this path refuses affected prior
    duplicates rather than guessing which historical row to keep. Existing
    duplicates remain the responsibility of the explicitly invoked
    reconciliation path. The in-memory tests cannot exercise the Iceberg engine;
    ``scripts/probe_catalog_replace.py`` runs this write path against a
    throwaway catalog table in the integration workflow.

    Table reads per call: the prior below (outside the transaction), then the
    in-transaction copy, the MERGE and its readback in :func:`replace_rows`.
    """
    cols = list(record_type.schema)
    key = record_type.dedup_key
    tbl = _qualified(record_type)
    # Temp names are per-record-type so a dockets + comments run on one
    # connection can't collide.
    staged = f"_staged_{record_type.name}"
    winners = f"_winners_{record_type.name}"
    prior = f"_prior_{record_type.name}"

    files_sql = ", ".join(f"'{_sql_str(str(p))}'" for p in staging_files)
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)

    # 1. Collapse staging to one row per key (latest modify_date wins).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {staged} AS
        SELECT {col_select}
        FROM read_parquet([{files_sql}], union_by_name=true)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "{key}"
            ORDER BY modify_date DESC NULLS LAST
        ) = 1;
        """
    )
    con.execute(f'CREATE OR REPLACE TEMP TABLE {prior} AS SELECT * FROM {tbl} '
                f'WHERE "{key}" IN (SELECT "{key}" FROM {staged})')
    # 2. Keep only rows that should win over the table: a new key, or one whose
    #    incoming modify_date is strictly newer (matching the old MERGE guard).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {winners} AS
        SELECT s.*
        FROM {staged} s
        LEFT JOIN {prior} t ON t."{key}" = s."{key}"
        WHERE t."{key}" IS NULL
           OR t.modify_date IS NULL
           OR s.modify_date > t.modify_date;
        """
    )
    changed = con.execute(f"SELECT count(*) FROM {winners}").fetchone()[0]
    try:
        # Avoid empty catalog commits: the snapshot is also the mirror's input identity.
        if changed:
            replace_rows(con, record_type, winners, expected_prior=prior)
        return changed
    finally:
        con.execute(f"DROP TABLE IF EXISTS {staged};")
        con.execute(f"DROP TABLE IF EXISTS {winners};")
        con.execute(f"DROP TABLE IF EXISTS {prior};")


def _published_projection(record_type: RecordType, missing: frozenset[str] = frozenset()) -> str:
    """The published columns in the record type's order and types; a field in ``missing`` is NULL of its type.

    The catalog stores every column as VARCHAR, so a typed column (``comments.duplicate_comments``, INTEGER) is cast
    back on the way out. The cast is strict: a stored value that is not one refuses the export.
    """
    def column(name: str) -> str:
        sql_type = record_type.sql_type(name)
        if name in missing:
            return f'NULL::{sql_type} AS "{name}"'
        return f'"{name}"' if sql_type == "VARCHAR" else f'CAST("{name}" AS {sql_type}) AS "{name}"'

    return ", ".join(column(name) for name in record_type.schema)


def _export_parquet(con, record_type: RecordType, output_dir: Path) -> Path:
    """Write the full table back out as the public ``{name}.parquet`` snapshot.

    Reuses the published layout's sort + compression (zstd, sorted by
    ``agency_code, modify_date`` for dockets) so downstream consumers — the CLI
    ``download`` and the anonymous MCP server — see byte-for-byte the same shape
    they do today. This is what makes the "dual model" work: Iceberg is the
    system of record, public Parquet is the read mirror.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{record_type.name}.parquet"

    sort_cols = [c for c in ("agency_code", "modify_date") if c in record_type.schema]
    order_by = f"ORDER BY {', '.join(sort_cols)}" if sort_cols else ""
    columns = _published_projection(record_type)

    con.execute(
        f"""
        COPY (SELECT {columns} FROM {_qualified(record_type)} {order_by})
        TO '{_sql_str(str(out_file))}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
        """
    )
    return out_file


def _build_comments_index(con, record_type: RecordType, output_dir: Path, *, source_sql: str | None = None) -> Path:
    """Rebuild ``comments_index.parquet`` from the catalog comments table.

    The index is the small per-``(agency_code, docket_id, year, month)`` row-count
    artifact that ``build_feed_summary`` / ``build_agency_rollups`` read instead
    of scanning the full comments table. This is the published index's only
    builder: it is derived straight from the table with the schema
    (agency_code, docket_id, year, month, row_count).

    ``year`` / ``month`` come from ``posted_date``; ``docket_id`` is trimmed of
    stray quotes. NULL docket IDs and dates retain NULL groups. Written atomically
    via a temp file so a crashed rebuild can't leave a half-written index in place.
    """
    from spicy_regs.transforms.comment_partitions import validate_comment_coordinates

    source_sql = source_sql or f"SELECT * FROM {_qualified(record_type)}"
    validate_comment_coordinates(con, source_sql)
    index_file = output_dir / "comments_index.parquet"
    tmp_file = index_file.with_suffix(".tmp.parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                agency_code,
                TRIM(docket_id, '"') AS docket_id,
                EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS year,
                EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS month,
                CAST(COUNT(*) AS BIGINT) AS row_count
            FROM ({source_sql})
            GROUP BY 1, 2, 3, 4
        ) TO '{_sql_str(str(tmp_file))}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    tmp_file.replace(index_file)
    return index_file


def seed_comments_from_parquet(
    con, source_glob: str, record_type: RecordType, agency: str | None = None, *, replace: bool = False
) -> int:
    """Bulk-load published comment Parquet into the catalog table; return its count.

    One-time cutover helper: it copies already-published comments straight into
    the catalog ``comments`` table instead of re-ingesting from Mirrulations.
    ``source_glob`` names one file or a glob of files. It is read with
    ``hive_partitioning=false`` because the files already carry ``agency_code``
    and ``docket_id`` as columns, so no path segment becomes a column.

    ``agency`` inserts only that ``agency_code``'s source rows, so a source
    holding every agency (the monolithic ``comments.parquet``) loads one agency
    at a time. ``replace`` first deletes the
    agency's existing rows: the loader runs one agency at a time, so this makes a
    re-run (after a timeout, or over an already-seeded table) replace that
    agency's rows instead of duplicating them, since the plain ``INSERT`` does no
    dedup. The caller passes it only when the agency has rows, because each
    ``DELETE`` scans the catalog table.

    Columns absent from every file in the glob (an older file written before a
    column was added) are inserted as ``NULL``, so a mixed-vintage source loads
    cleanly. The connection + any S3 secret are set up by the caller
    so this stays testable against a local catalog and local files.
    """
    _warn_unlocked_write(record_type, "seed")
    columns = list(record_type.schema)
    esc = _sql_str(source_glob)
    if replace and agency is None:
        raise ValueError("replace needs the agency whose rows it replaces")
    agency_filter = "" if agency is None else f"WHERE agency_code = '{_sql_str(agency)}'"
    if replace:
        con.execute(f"DELETE FROM {_qualified(record_type)} {agency_filter};")
    present = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)"
        ).fetchall()
    }
    projection = ", ".join(
        f'CAST("{c}" AS VARCHAR) AS "{c}"' if c in present else f'CAST(NULL AS VARCHAR) AS "{c}"' for c in columns
    )
    col_list = ", ".join(f'"{c}"' for c in columns)
    con.execute(
        f"""
        INSERT INTO {_qualified(record_type)} ({col_list})
        SELECT {projection}
        FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)
        {agency_filter};
        """
    )
    return con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]


_REQUIRED_S3_ENV = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")


def create_s3_secret(con) -> None:
    """Register R2's S3 API as a DuckDB secret so bucket objects can be read/globbed.

    Reads over the public HTTPS URL can't list directories (the whole reason the
    catalog cutover exists), so source Parquet is read via the S3 API, which does
    support listing/globbing. Shared by the catalog seeding scripts.
    """
    missing = [v for v in _REQUIRED_S3_ENV if not getenv(v)]
    if missing:
        raise RuntimeError("Missing R2 S3 env var(s): " + ", ".join(missing))
    # R2_ENDPOINT is a full URL for boto3; DuckDB wants the bare host + USE_SSL.
    endpoint = getenv("R2_ENDPOINT", "")
    host = urlparse(endpoint).netloc or endpoint.replace("https://", "").replace("http://", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2_s3_secret (
            TYPE S3,
            KEY_ID '{_sql_str(getenv("R2_ACCESS_KEY_ID", ""))}',
            SECRET '{_sql_str(getenv("R2_SECRET_ACCESS_KEY", ""))}',
            ENDPOINT '{_sql_str(host)}',
            URL_STYLE 'path',
            USE_SSL true,
            REGION 'auto'
        );
        """
    )


def backfill_missing_from_parquet(con, source_uri: str, record_type: RecordType) -> tuple[int, int]:
    """Insert published-Parquet rows the catalog table is missing; return ``(inserted, total)``.

    The cutover counterpart to :func:`seed_comments_from_parquet`, for a table
    that was routed through the catalog without ever being seeded from the
    Parquet that preceded it. ``dockets`` was exactly that: the Iceberg path
    landed while ``comments`` got a real seed and ``dockets`` got none, so the
    catalog table only ever accumulated rows staged *after* the cutover (~5.4k of
    ~276k). Every run then exported that sliver as the "full" public snapshot and
    the shrink guard refused it, freezing the published table for eight weeks.

    Rows are matched on ``record_type.dedup_key`` and only source keys **absent**
    from the table are inserted. That ordering matters: the catalog's rows are
    *newer* than the frozen snapshot (they are the post-cutover updates), so
    overwriting them from Parquet would silently roll back eight weeks of
    ingested changes. Backfilling only what is missing keeps the newer row
    authoritative in every case, and makes a re-run a no-op rather than a
    duplicate load — this does a plain ``INSERT``, with no dedup of its own.

    Expressed as an anti-join rather than ``NOT IN`` because a single NULL key on
    either side makes ``NOT IN`` return NULL for every row and quietly insert
    nothing. The source is de-duplicated on its own key (latest ``modify_date``
    wins) so a snapshot with repeats can't fan out into duplicate rows.
    """
    _warn_unlocked_write(record_type, "backfill missing rows")
    key = record_type.dedup_key
    columns = list(record_type.schema)
    esc = _sql_str(source_uri)
    qualified = _qualified(record_type)

    present = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)"
        ).fetchall()
    }
    if key not in present:
        raise RuntimeError(f"Source {source_uri} has no '{key}' column; refusing to backfill {record_type.name}")

    projection = ", ".join(
        f'CAST("{c}" AS VARCHAR) AS "{c}"' if c in present else f'CAST(NULL AS VARCHAR) AS "{c}"' for c in columns
    )
    col_list = ", ".join(f'"{c}"' for c in columns)

    before = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    con.execute(
        f"""
        INSERT INTO {qualified} ({col_list})
        WITH src AS (
            SELECT {projection}
            FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)
            QUALIFY row_number() OVER (PARTITION BY "{key}" ORDER BY "modify_date" DESC NULLS LAST) = 1
        )
        SELECT {col_list} FROM src
        WHERE src."{key}" IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {qualified} t WHERE t."{key}" = src."{key}");
        """
    )
    total = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    return total - before, total


def upsert_comment_text(con, record_type: RecordType, agency: str, updates: "pl.DataFrame | Path") -> None:
    """Upsert filled ``text_content`` / ``text_extraction_status`` for one agency.

    Shared helper for the durable text-fill paths (derived-data backfill and PDF
    enrichment). ``updates`` is a polars DataFrame, or a Parquet file read without
    loading it into Python, with columns ``comment_id, _new_text, _new_status``
    and optionally ``_new_pdf_results`` (both fill paths supply it: PDF attempts,
    or the derived-text provenance). Every row whose ``comment_id`` matches
    the agency's rows gets ``text_content`` / ``text_extraction_status`` refreshed
    (``COALESCE`` keeps the existing value when the incoming column is NULL). It
    uses the checked atomic replacement (:func:`replace_rows`) with an
    ``agency_code`` scope: the prior read, the in-transaction copy, the MERGE
    match and the readback all carry the agency predicate, so Iceberg can skip
    files whose ``agency_code`` bounds exclude it instead of reading the whole
    tens-of-millions-row table for each agency. A same ``comment_id`` under
    another agency is neither read nor replaced. No-ops on an empty frame; the
    caller is expected to have handled that case already.

    CRITICAL — self-contained temp table: the MERGE reads from an independent
    ``_uct_replacement`` temp table (a full snapshot of the affected rows with the
    requested columns overridden in place), **never** from a projection over the live
    catalog table. An earlier version projected the overrides straight off
    ``{tbl} t JOIN updates`` in the INSERT's SELECT; on the R2 Data Catalog that
    dropped column writes — ``text_content`` landed but ``text_extraction_status``
    came back NULL, so incremental re-runs kept re-selecting already-filled rows.
    Snapshotting into a self-contained temp table removes the live-table reference
    from the write path and fixes it (see PR #117). Plain DuckDB does not reproduce
    the catalog behavior, so this invariant is verified by a scoped catalog run,
    not the unit test — keep the ``_uct_replacement`` indirection intact.
    """
    if isinstance(updates, Path):
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _uct_updates AS SELECT * FROM read_parquet('{_sql_str(str(updates))}');"
        )
    else:
        con.register("_uct_updates_src", updates.to_arrow())
        try:
            con.execute("CREATE OR REPLACE TEMP TABLE _uct_updates AS SELECT * FROM _uct_updates_src;")
        finally:
            con.unregister("_uct_updates_src")
    if not con.execute("SELECT count(*) FROM _uct_updates").fetchone()[0]:
        con.execute("DROP TABLE _uct_updates;")
        return

    tbl = _qualified(record_type)
    ag = _sql_str(agency)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)
    update_columns = {row[0] for row in con.execute("DESCRIBE _uct_updates").fetchall()}
    pdf_assignment = (
        ", pdf_extraction_results_json = COALESCE(u._new_pdf_results, r.pdf_extraction_results_json)"
        if "_new_pdf_results" in update_columns
        else ""
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _uct_prior AS
        SELECT {col_list} FROM {tbl}
        WHERE agency_code = '{ag}' AND comment_id IN (SELECT comment_id FROM _uct_updates);
        """
    )
    con.execute("CREATE OR REPLACE TEMP TABLE _uct_replacement AS SELECT * FROM _uct_prior")
    con.execute(
        f"""
        UPDATE _uct_replacement AS r
        SET text_content = COALESCE(u._new_text, r.text_content),
            text_extraction_status = COALESCE(u._new_status, r.text_extraction_status){pdf_assignment}
        FROM _uct_updates u
        WHERE r.comment_id = u.comment_id;
        """
    )
    replace_rows(con, record_type, "_uct_replacement", expected_prior="_uct_prior", scope={"agency_code": agency})
    con.execute("DROP TABLE IF EXISTS _uct_updates;")
    con.execute("DROP TABLE IF EXISTS _uct_replacement;")
    con.execute("DROP TABLE IF EXISTS _uct_prior;")


def merge_comments(staging_dir: Path, record_type: RecordType) -> int:
    """Upsert staged comments; final mirror publication owns the index recount."""
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        return 0
    from spicy_regs.transforms.comment_partitions import validate_staged_comments

    validate_staged_comments(staging_dir)
    con = _connect_for_table(record_type)
    try:
        changed = _merge(con, staging_files, record_type)
        logger.info("iceberg: merged {:,} winning {} rows", changed, record_type.name)
        return changed
    finally:
        con.close()


@dataclass(frozen=True)
class CatalogSnapshot:
    """Exact source version, including identity across table recreation."""

    table_uuid: str
    snapshot_id: int
    schema_id: int


def _read_snapshot(con, record_type: RecordType) -> CatalogSnapshot:
    if dedupe_recovery_pending(con, record_type):
        raise RuntimeError(f"Unfinished dedupe for {record_type.name}; recover before exporting")
    raw = con.execute("SELECT metadata FROM iceberg_load_table_response(?)", [
        f"{_CATALOG_ALIAS}.{_namespace()}.{record_type.name}",
    ]).fetchone()[0]
    metadata = json.loads(raw) if isinstance(raw, str) else raw
    snapshot = CatalogSnapshot(metadata["table-uuid"], int(metadata["current-snapshot-id"]),
                               int(metadata["current-schema-id"]))
    if not snapshot.table_uuid or snapshot.snapshot_id < 0:
        raise RuntimeError("Comments catalog has no usable snapshot")
    return snapshot


def catalog_snapshot(record_type: RecordType) -> CatalogSnapshot:
    """Read metadata without creating/migrating any catalog tables."""
    con = _connect()
    try:
        return _read_snapshot(con, record_type)
    finally:
        con.close()


def _snapshot_query(record_type: RecordType, snapshot: CatalogSnapshot) -> str:
    return f"SELECT * FROM {_qualified(record_type)} AT (VERSION => {int(snapshot.snapshot_id)})"


def export_public_comments(
    output_dir: Path, record_type: RecordType, *, memory_limit: str = ExportResources.memory,
    threads: int = ExportResources.threads,
    snapshot: CatalogSnapshot | None = None, resources: ExportResources | None = None,
) -> dict[str, Path]:
    """Scan one pinned snapshot into agencies; sort once, assemble, then recount.

    Every payload read from the catalog includes its Iceberg deletes. Remaining
    work reads local files. Anonymous readers retain their existing paths/schema.
    """
    import duckdb
    from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies

    resources = resources or ExportResources(memory=memory_limit, threads=threads)
    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="comments-export-", dir=output_dir) as work:
        work_dir = Path(work)
        con = _connect()
        try:
            current = _read_snapshot(con, record_type)
            if snapshot is not None and current != snapshot:
                raise RuntimeError("Catalog snapshot changed before export; retry")
            resources.configure(con, work_dir / "spill")
            source = _snapshot_query(record_type, current)
            snapshot_columns = set(con.sql(source).columns)
            missing = set(record_type.schema) - snapshot_columns
            unsupported = missing - set(_COMMENT_ADDED_COLUMNS)
            if unsupported:
                raise ValueError("Unsupported missing comment snapshot columns: " + ", ".join(sorted(unsupported)))
            # Schema-only DDL does not advance the data snapshot. Its older
            # schema cannot name newly added nullable fields; keep them unread.
            # The record type's order is the mirror's column contract (spicy-docs
            # 0.50.0 COMMENT: reference fields after docket_id), not the catalog's
            # physical order, where ALTER ADD COLUMN appends them.
            columns = list(record_type.schema)
            source = f"SELECT {_published_projection(record_type, frozenset(missing))} FROM ({source})"
            stage_comment_agencies(con, source, work_dir / "staging", resources=resources)
        finally:
            con.close()
        partitions = sort_comment_agencies(work_dir / "staging", output_dir, resources=resources)
        with duckdb.connect() as con:
            resources.configure(con, work_dir / "spill")
            monolith = assemble_comments(con, partitions, output_dir, columns, resources=resources)
            con.from_parquet(str(monolith)).create_view("comments_export")
            index_file = _build_comments_index(con, record_type, output_dir,
                                               source_sql="SELECT * FROM comments_export")
        return {"comments": monolith, "index": index_file, "partitions": partitions}


def audit_duplicates(con, record_type: RecordType) -> list[tuple[str, int, int]]:
    """Per-agency (agency_code, rows, distinct_keys) where rows exceed distinct keys.

    A read-only duplication report over the catalog table: any agency whose row
    count is above its distinct ``dedup_key`` count carries duplicate rows. Sorted
    by the number of duplicate rows, worst first. Empty when the table is clean.
    """
    key = record_type.dedup_key
    tbl = _qualified(record_type)
    rows = con.execute(
        f"""
        SELECT agency_code,
               count(*) AS rows,
               count(DISTINCT "{key}") AS distinct_keys
        FROM {tbl}
        WHERE agency_code IS NOT NULL
        GROUP BY agency_code
        HAVING count(*) > count(DISTINCT "{key}")
        ORDER BY count(*) - count(DISTINCT "{key}") DESC
        """
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def _table_exists(con, name: str) -> bool:
    """Inspect metadata; a permission or transport failure must propagate."""
    return bool(con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [_CATALOG_ALIAS, _namespace(), name],
    ).fetchone()[0])


def dedupe_recovery_pending(con, record_type: RecordType) -> bool:
    """A retained candidate or journal requires explicit recovery, even if live IDs are unique."""
    return any(_table_exists(con, record_type.name + suffix) for suffix in ("_dedup", "_dedup_state"))


def dedupe_table(con, record_type: RecordType) -> tuple[int, int]:
    """Collapse the catalog table to one row per ``dedup_key`` (latest modify_date).

    Builds a fresh deduped sibling table one agency at a time, then replaces the
    live table with it. Three constraints drive the shape:

    * Never ``DELETE``. The historical duplication came from loads whose ``DELETE``
      didn't remove prior rows on the R2 Data Catalog, so a delete-based cleanup
      could double the problem.
    * Never touch tens of millions of rows in one statement. A global
      ``ROW_NUMBER() OVER (PARTITION BY key)`` OOMs the runner on the read side,
      and a single ``CREATE OR REPLACE ... AS SELECT`` of the whole table OOMs it
      on the Iceberg *write* side. The build works per agency, and — because even
      one large agency's dedup window buffers whole rows (comment text included)
      and overflows the runner's memory limit despite on-disk spill — each agency
      is further split into hash buckets of the dedup key sized to
      ``DEDUP_ROWS_PER_BATCH`` rows. A key hashes to exactly one bucket and
      duplicates only ever share a key, so per-bucket dedup equals per-agency
      equals global dedup here; small agencies stay a single write.
    * Never ``ALTER TABLE RENAME``. DuckDB's Iceberg REST integration does not
      implement it (``NotImplementedException: Alter Schema Entry``), so the swap
      replaces the live table by ``DROP`` + ``CREATE`` + per-agency ``INSERT``
      from the sibling — all operations this catalog supports. The sibling is the
      durable copy: it is dropped only after the rebuilt table's row count is
      verified. An append-only journal marks the candidate complete before
      replacement starts. Recovery resumes from that candidate even when live
      exists but is incomplete. Unjournaled legacy candidates require manual
      reconciliation and are never discarded automatically.

    Returns ``(rows_before, rows_after)``; ``rows_after`` equals the number of
    distinct keys when the rebuild succeeds.
    """
    _warn_unlocked_write(record_type, "dedupe")
    key = record_type.dedup_key
    name = record_type.name
    tbl = _qualified(record_type)
    dedup_tbl = f'{_schema_ref()}."{name}_dedup"'
    state_tbl = f'{_schema_ref()}."{name}_dedup_state"'
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in record_type.schema)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)

    def _verify(ident: str, expected: int) -> None:
        rows, distinct = con.execute(f'SELECT count(*), count(DISTINCT "{key}") FROM {ident}').fetchone()
        if rows != expected or distinct != expected:
            raise RuntimeError(f"dedupe verification failed: {ident} has {rows} rows / {distinct} IDs; expected {expected}")

    def _copy_agency(agency: str | None, rows: int) -> None:
        # One agency's INSERT from the sibling, retried on a catalog error
        # (upstream #198: the 2026-09-06 swap died on a single 502). Each INSERT
        # is one Iceberg commit, so after an error the agency holds none of its
        # rows or all of them. Every retry backs off, then counts before it
        # inserts: a commit that landed despite the error is not repeated, and a
        # count that meets the same outage is itself retried rather than
        # guessed past. A partial agency, anything but a DuckDB error, or the
        # last failed attempt propagates, and the journal resumes the next run.
        import duckdb

        where = "agency_code IS NULL" if agency is None else f"agency_code = '{_sql_str(agency)}'"
        attempts = int(getenv("DEDUP_SWAP_RETRIES", "3"))
        for attempt in range(1, attempts + 1):
            try:
                if attempt > 1:
                    have = con.execute(f"SELECT count(*) FROM {tbl} WHERE {where}").fetchone()[0]
                    if have == rows:
                        return
                    if have:
                        raise RuntimeError(f"swap INSERT for {agency} left {have:,} of {rows:,} rows")
                con.execute(f"INSERT INTO {tbl} ({col_list}) SELECT {col_list} FROM {dedup_tbl} WHERE {where};")
                return
            except duckdb.Error as error:
                if attempt == attempts:
                    raise
                logger.warning("iceberg: swap for {} failed ({}); retry {}/{}", agency, error, attempt, attempts - 1)
                time.sleep(2**attempt)

    def _replace_live_from_sibling() -> int:
        # Swap without RENAME: rebuild the live table from the deduped sibling
        # using DROP/CREATE/INSERT (per-agency, so no whole-table statement). The
        # sibling still holds every row throughout, so this is safe to re-run if
        # interrupted; it is dropped only once the rebuilt row count matches.
        expected = con.execute(f"SELECT row_count FROM {state_tbl} WHERE phase = 'ready'").fetchone()[0]
        _verify(dedup_tbl, expected)
        per_agency = con.execute(f"SELECT agency_code, count(*) FROM {dedup_tbl} GROUP BY agency_code").fetchall()
        con.execute(f"DROP TABLE IF EXISTS {tbl};")
        con.execute(f"CREATE TABLE {tbl} ({col_defs});")
        for agency, rows in per_agency:
            _copy_agency(agency, rows)
        _verify(tbl, expected)
        con.execute(f"DROP TABLE IF EXISTS {dedup_tbl};")
        con.execute(f"DROP TABLE {state_tbl};")
        return expected

    # The append-only journal distinguishes a partial candidate from a complete
    # one. Record readiness BEFORE dropping live. Never infer readiness from
    # whether live exists: it can exist and still be only partly restored.
    if _table_exists(con, name + "_dedup") and not _table_exists(con, name + "_dedup_state"):
        raise RuntimeError(f"Unjournaled dedupe candidate {dedup_tbl}; preserve it and reconcile before repair")
    con.execute(f"CREATE TABLE IF NOT EXISTS {state_tbl} (phase VARCHAR, row_count BIGINT)")
    journal = con.execute(f"SELECT phase, row_count FROM {state_tbl}").fetchall()
    state = dict(journal)
    if len(journal) != len(state) or set(state) - {"building", "ready"}:
        raise RuntimeError(f"Invalid dedupe journal {state_tbl}; refusing to overwrite recovery data")
    if "ready" in state:
        if "building" not in state:
            raise RuntimeError(f"Missing original count in {state_tbl}")
        if _table_exists(con, name + "_dedup"):
            after = _replace_live_from_sibling()
        else:
            # The copy completed and the sibling was removed, but cleanup was interrupted.
            _verify(tbl, state["ready"])
            after = state["ready"]
            con.execute(f"DROP TABLE {state_tbl}")
        return state["building"], after

    before = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
    if "building" in state and state["building"] != before:
        raise RuntimeError("Live population changed during dedupe preparation; preserve recovery data")
    if "building" not in state:
        con.execute(f"INSERT INTO {state_tbl} VALUES ('building', {before})")

    # Build the deduped sibling fresh (discarding any partial one from an aborted
    # run), one agency at a time.
    con.execute(f"DROP TABLE IF EXISTS {dedup_tbl};")
    con.execute(f"CREATE TABLE {dedup_tbl} ({col_defs});")

    agencies = [r[0] for r in con.execute(f"SELECT DISTINCT agency_code FROM {tbl}").fetchall()]
    counts = dict(con.execute(f"SELECT agency_code, count(*) FROM {tbl} GROUP BY agency_code").fetchall())
    # A single agency's dedup window buffers whole rows (comment text included), so
    # the largest agencies overflow the runner's memory limit even with spilling on.
    # Split each agency into ceil(rows / DEDUP_ROWS_PER_BATCH) hash buckets of the
    # dedup key to bound each window's input; a key hashes to one bucket and dupes
    # share a key, so this doesn't change the result. Small agencies -> 1 bucket.
    rows_per_batch = int(getenv("DEDUP_ROWS_PER_BATCH", "100000"))
    logger.info("iceberg: rebuilding {} deduplicated across {} agency bucket(s)", name, len(agencies))
    for agency in agencies:
        where = "agency_code IS NULL" if agency is None else f"agency_code = '{_sql_str(agency)}'"
        n_buckets = max(1, -(-counts.get(agency, 0) // rows_per_batch))  # ceil division
        for bucket in range(n_buckets):
            # hash() is UBIGINT, so the modulo is non-negative; skip the filter
            # entirely for single-bucket agencies to keep their write unchanged.
            bucket_filter = "" if n_buckets == 1 else f' AND hash("{key}") % {n_buckets} = {bucket}'
            con.execute(
                f"""
                INSERT INTO {dedup_tbl} ({col_list})
                SELECT {col_list}
                FROM {tbl}
                WHERE {where}{bucket_filter}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY "{key}"
                    ORDER BY modify_date DESC NULLS LAST
                ) = 1;
                """
            )

    after = con.execute(f"SELECT count(*) FROM {dedup_tbl}").fetchone()[0]
    expected = con.execute(f'SELECT count(DISTINCT "{key}") FROM {tbl}').fetchone()[0]
    _verify(dedup_tbl, expected)
    con.execute(f"INSERT INTO {state_tbl} VALUES ('ready', {expected})")

    # Replace the live table with the deduped sibling (no RENAME — see docstring).
    logger.info("iceberg: swapping deduped {} into place ({:,} -> {:,} rows)", name, before, after)
    _replace_live_from_sibling()
    return before, after


def merge_and_export(staging_dir: Path, output_dir: Path, record_type: RecordType) -> Path | None:
    """Upsert staged rows into the catalog table, then export the public Parquet.

    Returns the path to the exported ``{name}.parquet`` (so the pipeline can
    publish it via the existing R2 upload), or ``None`` when there was nothing
    staged for this record type.
    """
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        logger.info("iceberg: no staging files for {}; skipping merge", record_type.name)
        return None

    con = _connect_for_table(record_type)
    try:
        logger.info(
            "iceberg: MERGE {} staging file(s) into {}",
            len(staging_files),
            _qualified(record_type),
        )
        _merge(con, staging_files, record_type)
        total = con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]
        logger.info("iceberg: {} now holds {:,} rows", record_type.name, total)
        out_file = _export_parquet(con, record_type, output_dir)
        logger.info("iceberg: exported public snapshot to {}", out_file)
        return out_file
    finally:
        con.close()
