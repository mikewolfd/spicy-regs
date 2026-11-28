"""Cloudflare R2 Data Catalog (Apache Iceberg) connector.

The internal, write-side table format for the ETL. The catalog is **R2 Data
Catalog** — a managed Iceberg REST catalog built into the same R2 bucket the
project already publishes Parquet to, so no separate metastore (Glue/Nessie/
Postgres) has to be stood up.

DuckDB's ``iceberg`` and ``httpfs`` extensions provide catalog access. Version
1.5.3 or later supports the atomic MERGE used for paired writes.
The current upsert implementation uses a checked atomic MERGE, keeping the row with
the most recent ``modify_date`` for each primary key.

This module is the "Iceberg load" stage only, mirroring the thin-wrapper style
of :mod:`spicy_regs.sources.r2`:

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
import re
import time
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, TypeVar, cast
from urllib.parse import urlparse, urlsplit
from uuid import uuid4

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
        "mirror export will refuse with 'Catalog changed during export'. Run catalog writes "
        "only when no ETL or mirror run holds comments-catalog-write.",
        operation, record_type.name, CATALOG_LOCK_ENV,
    )


def _catalog_namespace(record_type):
    from .regulatory_catalog import namespace, supports
    if not supports(record_type):
        raise ValueError(f'Unsupported regulatory catalog dataset: {record_type.name}')
    return namespace()


def _qualified(record_type: RecordType) -> str:
    from .regulatory_catalog import qualified
    _catalog_namespace(record_type)
    return qualified(record_type)


def _ensure_table(con, record_type: RecordType) -> None:
    """Initialize the declared native subject and receipt tables."""
    from .regulatory_catalog import ensure_native
    ensure_native(con, record_type)




def _column_types(con, record_type: RecordType) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f"DESCRIBE {_qualified(record_type)}").fetchall()}


_T = TypeVar("_T")

# A catalog operation that hits a transient R2 error is retried from a fresh
# connection, sized to ride out a short outage (7e3b417a5 / #238).
_CATALOG_MAX_ATTEMPTS = 12
_CATALOG_MAX_BACKOFF_S = 60
_TRANSIENT_HTTP_STATUS = re.compile(r"\bHTTP (?:code )?(?:5\d\d|429)\b")


def _is_transient_catalog_error(exc: BaseException) -> bool:
    """True for a DuckDB HTTP error carrying a 5xx/429 status."""
    import duckdb

    return isinstance(exc, duckdb.HTTPException) and bool(_TRANSIENT_HTTP_STATUS.search(str(exc)))


def _with_catalog_retry(what: str, work: Callable[[Any], _T], *, connect: Callable[[], Any] = _connect) -> _T:
    """Run ``work(con)`` on a fresh catalog connection, retrying transient errors."""
    for attempt in range(1, _CATALOG_MAX_ATTEMPTS + 1):
        con = None
        try:
            con = connect()
            return work(con)
        except Exception as exc:
            if attempt == _CATALOG_MAX_ATTEMPTS or not _is_transient_catalog_error(exc):
                raise
            backoff = min(2**attempt, _CATALOG_MAX_BACKOFF_S)
            logger.warning(
                "iceberg: {} failed ({}), attempt {}/{}; retrying in {}s",
                what,
                exc,
                attempt,
                _CATALOG_MAX_ATTEMPTS,
                backoff,
            )
        finally:
            if con is not None:
                con.close()
        time.sleep(backoff)
    raise AssertionError("unreachable")  # pragma: no cover


def _connect_for_table(record_type: RecordType):
    """Open one connection and initialize its native dataset if it is new."""
    _warn_unlocked_write(record_type, "prepare for write")
    con = _connect()
    try:
        _ensure_table(con, record_type)
    except BaseException:
        con.close()
        raise
    return con


def _staging_files(staging_dir: Path, record_type: RecordType) -> list[Path]:
    """Per-agency staging Parquet files for this record type (see write_staging)."""
    staging_type_dir = staging_dir / record_type.name
    if not staging_type_dir.exists():
        return []
    return sorted(staging_type_dir.glob("*.parquet"))


@contextmanager
def _transaction(con) -> Iterator[None]:
    """Commit every enclosed write together; roll back even on interruption."""
    con.execute("BEGIN")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        try:
            con.execute("ROLLBACK")
        except Exception as rollback_exc:
            logger.warning("iceberg: ROLLBACK failed after an aborted write: {}", rollback_exc)
        raise


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
    scope: Mapping[str, str] | None = None, expected_snapshot: "CatalogSnapshot | None" = None,
    source_files: Sequence[str] | None = None, before_files: Collection[str] | None = None,
    file_column: str = "filename",
) -> None:
    """Atomically replace selected unique keys, validating values before commit.

    ``source`` must be a self-contained temp table, never a projection over the
    live table (see :func:`upsert_comment_text`). Shared by the ETL upsert
    (:func:`_merge`), the text fills and the comment source repair.
    Read-modify-write callers pass a self-contained ``expected_prior``; its rows
    and absences must still match inside the write transaction. Direct
    replacements may omit it.

    ``expected_snapshot`` refuses inside the transaction unless the table is still
    at that snapshot, so nothing committed since the caller looked is overwritten
    (the comment fill's undo passes the snapshot its operator reviewed).

    ``scope`` (column → value, e.g. one ``agency_code``) restricts every catalog
    read and the MERGE match to rows with those values; every source row must
    carry them. A same-key row outside the scope is neither read nor replaced.

    Inside the transaction the target rows for the source keys are copied once;
    the prior and duplicate checks read that copy. Only the MERGE and its
    readback read the catalog table again, so an unscoped call scans it three
    times, not four.
    """
    from .regulatory_catalog import replace_native
    _warn_unlocked_write(record_type, "replace rows")
    return replace_native(con, record_type, source, expected_prior=expected_prior,
                          scope=scope, expected_snapshot=expected_snapshot,
                          source_files=source_files, before_files=before_files,
                          file_column=file_column)


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
    duplicates require a fresh qualified source build; the live runtime never
    chooses a winner among corrupted stored rows. The in-memory tests cannot exercise the Iceberg engine;
    ``scripts/probe_catalog_replace.py`` runs this write path against a
    throwaway catalog table in the integration workflow.

    Table reads per call: the prior below (outside the transaction), then the
    in-transaction copy, the MERGE and its readback in :func:`replace_rows`.
    """
    key = record_type.dedup_key
    from .regulatory_catalog import processing_table
    tbl = _qualified(record_type)
    # Temp names are per-record-type so a dockets + comments run on one
    # connection can't collide.
    staged = f"_staged_{record_type.name}"
    winners = f"_winners_{record_type.name}"
    prior = f"_prior_{record_type.name}"

    files_sql = ", ".join(f"'{_sql_str(str(p))}'" for p in staging_files)
    source = f"read_parquet([{files_sql}], union_by_name=true)"
    from .regulatory_catalog import _validate_source_columns
    _validate_source_columns(con, source, record_type.name)

    # 1. Collapse staging to one row per key (latest modify_date wins).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {staged} AS
        SELECT *
        FROM {source}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "{key}"
            ORDER BY modify_date DESC NULLS LAST
        ) = 1;
        """
    )
    tbl = processing_table(con, record_type, where=f'"{key}" IN (SELECT "{key}" FROM {staged})')
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
        from .regulatory_catalog import replace_native
        rejected = f'(SELECT * FROM {source} EXCEPT ALL SELECT * FROM {winners})'
        replace_native(con, record_type, winners, expected_prior=prior, rejected_source=rejected)
        return changed
    finally:
        con.execute(f"DROP TABLE IF EXISTS {staged};")
        con.execute(f"DROP TABLE IF EXISTS {winners};")
        con.execute(f"DROP TABLE IF EXISTS {prior};")
        con.execute(f"DROP TABLE IF EXISTS {tbl}")


def _export_parquet(con, record_type: RecordType, output_dir: Path) -> Path:
    """Export a checked native pair and its byte-identical subject file."""
    import shutil
    from uuid import uuid4
    from . import regulatory_catalog as native
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = native.export_pair(con, record_type, output_dir / '.catalog-pairs' / record_type.name,
                                  generation_id='catalog-' + uuid4().hex)
    output = output_dir / f'{record_type.name}.parquet'
    shutil.copyfile(selected.subjects[0], output)
    return output


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

    from .regulatory_catalog import processing_table
    tbl = processing_table(con, record_type, where=f"agency_code = '{_sql_str(agency)}' AND comment_id IN (SELECT comment_id FROM _uct_updates)")
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
    con.execute(f"DROP TABLE {tbl}")


def merge_comments(staging_dir: Path, record_type: RecordType) -> int:
    """Upsert staged comments; final mirror publication owns the index recount."""
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        return 0
    from spicy_regs.transforms.comment_partitions import validate_staged_comments

    validate_staged_comments(staging_dir)

    def work(con) -> int:
        changed = _merge(con, staging_files, record_type)
        logger.info("iceberg: merged {:,} winning {} rows", changed, record_type.name)
        return changed

    return _with_catalog_retry(
        f"merge {record_type.name}", work, connect=lambda: _connect_for_table(record_type)
    )


@dataclass(frozen=True)
class CatalogSnapshot:
    """Exact source version, including identity across table recreation."""

    table_uuid: str
    snapshot_id: int
    schema_id: int


@dataclass(frozen=True)
class CatalogPairSnapshot(CatalogSnapshot):
    """Subject identity plus the shared receipt table's independently changing identity.

    The comments mirror records both, but publishes the subject alone (ss-juz8): only :meth:`subject` decides
    whether a publication is current.
    """

    receipts: CatalogSnapshot

    def subject(self) -> CatalogSnapshot:
        """The comments table's own identity, without the receipt table's."""
        return CatalogSnapshot(self.table_uuid, self.snapshot_id, self.schema_id)


def _selected_catalog_namespace(con, record_type: RecordType) -> str:
    """Only a checked native initialization establishes read authority."""
    from .regulatory_catalog import require_initialized
    require_initialized(con, record_type)
    return _catalog_namespace(record_type)


def _table_metadata(con, record_type: RecordType, *, namespace: str | None = None) -> dict:
    selected_namespace = namespace if namespace is not None else _selected_catalog_namespace(con, record_type)
    return _catalog_metadata(con, selected_namespace, record_type.name)


def _catalog_metadata(con, namespace: str, table: str) -> dict:
    raw = con.execute("SELECT metadata FROM iceberg_load_table_response(?)", [
        f"{_CATALOG_ALIAS}.{namespace}.{table}",
    ]).fetchone()[0]
    return json.loads(raw) if isinstance(raw, str) else raw


def _receipt_metadata(con, record_type: RecordType, *, namespace: str | None = None) -> dict:
    selected_namespace = namespace if namespace is not None else _selected_catalog_namespace(con, record_type)
    return _catalog_metadata(con, selected_namespace, "etl_receipts")


def _current_snapshot(metadata: dict) -> CatalogSnapshot:
    return CatalogSnapshot(metadata["table-uuid"], int(metadata["current-snapshot-id"]),
                           int(metadata["current-schema-id"]))


def _read_snapshot(con, record_type: RecordType, *, namespace: str | None = None) -> CatalogSnapshot:
    snapshot = _current_snapshot(_table_metadata(con, record_type, namespace=namespace))
    if not snapshot.table_uuid or snapshot.snapshot_id < 0:
        raise RuntimeError("Comments catalog has no usable snapshot")
    return snapshot


def _read_pair_snapshot(con, record_type: RecordType, *, namespace: str | None = None) -> CatalogPairSnapshot:
    """Pin both tables; an explicit ``namespace`` skips the initialization check (a rebuild reads an uninitialized pair)."""
    subject = _read_snapshot(con, record_type, namespace=namespace)
    receipts = _current_snapshot(_receipt_metadata(con, record_type, namespace=namespace))
    if not receipts.table_uuid or receipts.snapshot_id < 0:
        raise RuntimeError("Catalog receipts have no usable snapshot")
    return CatalogPairSnapshot(subject.table_uuid, subject.snapshot_id, subject.schema_id, receipts)


def catalog_snapshot(record_type: RecordType) -> CatalogPairSnapshot:
    """Pin subject and receipt metadata in one transaction, reading table metadata only.

    The initialization check is skipped (explicit namespace): it scans every receipt row for one marker, 78 s on
    the 26.5M-row table (2026-11-08), and the comments mirror, the one caller that publishes, refuses an unseeded
    or partial table through its own row floor and retained-ID check instead (ss-juz8).
    """
    con = _connect()
    try:
        with _transaction(con):
            return _read_pair_snapshot(con, record_type, namespace=_catalog_namespace(record_type))
    finally:
        con.close()


def _compacted_only(metadata: dict, snapshot: CatalogSnapshot) -> bool:
    """True when the current snapshot descends from ``snapshot`` through ``replace`` commits alone.

    Iceberg's ``replace`` rewrites data and delete files without changing the
    table's rows; R2's managed compaction commits one about hourly (ETL 36760140108
    and 36827148539 refused on it). Any other operation, a recreated table, a
    schema change or a current snapshot outside ``snapshot``'s descendants is a change.
    """
    current = _current_snapshot(metadata)
    if (current.table_uuid, current.schema_id) != (snapshot.table_uuid, snapshot.schema_id):
        return False
    parents = {int(s["snapshot-id"]): s for s in metadata.get("snapshots", [])}
    step = current.snapshot_id
    for _ in range(len(parents)):  # a chain visits each snapshot once; a parent cycle must not spin forever
        if step == snapshot.snapshot_id:
            return True
        commit = parents.get(step)
        if commit is None or (commit.get("summary") or {}).get("operation") != "replace" \
                or commit.get("parent-snapshot-id") is None:
            return False
        step = int(commit["parent-snapshot-id"])
    return step == snapshot.snapshot_id


def compacted_since(record_type: RecordType, snapshot: CatalogPairSnapshot, *,
                    declared_schema: bool = False) -> tuple[bool, bool]:
    """Whether the subject and the receipt table each moved past ``snapshot`` by compaction alone.

    Reads table metadata only. ``snapshot`` was pinned under the initialization check, and a pair reached through
    ``replace`` commits alone keeps that table and initialization receipt, so the check's scan of every receipt row
    (78 s on the 26.5M-row table, 2026-11-08) proves nothing more here. The declared policy is code and can move
    without a commit: ``declared_schema`` also refuses a subject schema that differs from it, by the rule
    :func:`regulatory_catalog.require_initialized` applies.
    """
    from .regulatory_catalog import schema_difference
    namespace = _catalog_namespace(record_type)
    con = _connect()
    try:
        with _transaction(con):
            if declared_schema and (difference := schema_difference(con, record_type)):
                raise ValueError("Native catalog schema differs from declared policy: " + difference)
            return (_compacted_only(_table_metadata(con, record_type, namespace=namespace), snapshot),
                    _compacted_only(_receipt_metadata(con, record_type, namespace=namespace), snapshot.receipts))
    finally:
        con.close()


def rows_unchanged_since(record_type: RecordType, snapshot: CatalogPairSnapshot, *,
                         declared_schema: bool = False) -> bool:
    """Require unchanged subject rows, allowing compaction.

    The receipt table is no publication input since the comments mirror stopped exporting receipts (ss-juz8), so a
    receipt-only commit (a rejected attempt, a retained refusal) leaves the published table current. Pass
    ``declared_schema=True`` to also refuse when the live table schema differs from the declared policy.
    """
    return compacted_since(record_type, snapshot, declared_schema=declared_schema)[0]


def _snapshot_query(record_type: RecordType, snapshot: CatalogSnapshot, *, namespace: str | None = None) -> str:
    table = (_qualified(record_type) if namespace is None
             else f'{_CATALOG_ALIAS}."{namespace}"."{record_type.name}"')
    return f"SELECT * FROM {table} AT (VERSION => {int(snapshot.snapshot_id)})"


def _stated_rows(metadata: dict, snapshot_id: int) -> int | None:
    """The rows the writer recorded for ``snapshot_id`` in its Iceberg summary, when that alone determines them.

    ``total-records`` counts every row of the snapshot's data files, so it is the table's row count only for a
    snapshot that states no delete file. With delete files the subtraction does not hold: after R2's compaction on
    2026-11-09 the dockets table stated 45 records beside 25 delete files and 51 position deletes. Summary fields
    are optional in Iceberg; any one absent returns None, as does any delete.
    """
    for entry in metadata.get("snapshots", []):
        if int(entry.get("snapshot-id", -1)) != snapshot_id:
            continue
        summary = entry.get("summary") or {}
        try:
            records, delete_files = int(summary["total-records"]), int(summary["total-delete-files"])
            deletes = sum(int(summary.get(name, 0)) for name in ("total-position-deletes", "total-equality-deletes"))
        except (KeyError, TypeError, ValueError):
            return None
        return records if records >= 0 and not delete_files and not deletes else None
    return None


def _pinned_row_count(con, record_type: RecordType, snapshot: CatalogSnapshot, *, namespace: str) -> tuple[int, str]:
    """How many rows the pinned comments snapshot holds, by a count other than the export's own scan.

    The writer's count first: the snapshot's Iceberg summary, metadata only, present on every comments snapshot
    the live catalog held on 2026-11-09 (DuckDB's appends and R2's compactions alike). Otherwise, or with a delete
    file, or on a catalog that is not Iceberg, the accepted receipts of the dataset in the same transaction: the
    ETL writes exactly one per row in the commit that writes the row (``replace_native``), and the 2026-11-08
    rebuild proved 26,418,078 of each. That is a scan of another table, about 78 s on 26.5M receipts; a second
    ``count(*)`` of the comments table would share the scan it is meant to check.
    """
    from .regulatory_catalog import _remote_catalog, receipts_table

    if _remote_catalog(con):
        stated = _stated_rows(_table_metadata(con, record_type, namespace=namespace), snapshot.snapshot_id)
        if stated is not None:
            return stated, "its Iceberg summary (total-records, no delete files)"
    accepted = con.execute(f"SELECT count(*) FROM {receipts_table()} WHERE dataset = ? AND outcome = 'accepted'",
                           [record_type.name]).fetchone()
    if accepted is None:
        raise RuntimeError("The catalog's receipt table returned no count")
    return accepted[0], "its accepted receipts (the snapshot's summary states no usable count)"


_BYTE_SIZE_UNITS = {
    "B": 1,
    "KB": 1000,
    "KIB": 1024,
    "MB": 1000**2,
    "MIB": 1024**2,
    "GB": 1000**3,
    "GIB": 1024**3,
    "TB": 1000**4,
    "TIB": 1024**4,
}
_BYTE_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?i?B)\s*$", re.IGNORECASE)


def _parse_byte_size(raw: str) -> int:
    match = _BYTE_SIZE_RE.fullmatch(raw)
    if match is None:
        raise ValueError(f"Invalid byte size: {raw!r}")
    return max(1, int(float(match.group(1)) * _BYTE_SIZE_UNITS[match.group(2).upper()]))


def _parse_s3_uri(uri: str, expected_bucket: str) -> str:
    """Validate ``s3://<expected_bucket>/<key>`` with no query/fragment and return ``<key>``."""
    parsed = urlsplit(uri)
    key = parsed.path.lstrip("/")
    if (
        parsed.scheme != "s3"
        or parsed.netloc != expected_bucket
        or parsed.query
        or parsed.fragment
        or not key
    ):
        raise ValueError(f"Refusing Iceberg object path outside s3://{expected_bucket}/: {uri!r}")
    return key


def _align_array_to_type(
    array: Any,
    target_type: Any,
    *,
    path: str,
    optional_struct_fields: frozenset[str] = frozenset(),
) -> Any:
    """Align a PyArrow array (preserving list offsets and parent null bitmaps) to ``target_type``."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if pa.types.is_list(target_type):
        if not (pa.types.is_list(array.type) or pa.types.is_large_list(array.type)):
            raise ValueError(f"{path}: expected list, got {array.type}")
        if len(array) == 0:
            _align_array_to_type(
                array.values.slice(0, 0),
                target_type.value_type,
                path=f"{path}[]",
                optional_struct_fields=optional_struct_fields,
            )
            return pa.array([], type=target_type)
        offsets = array.offsets
        start = offsets[0].as_py()
        end = offsets[-1].as_py()
        values = array.values.slice(start, end - start)
        aligned_values = _align_array_to_type(
            values,
            target_type.value_type,
            path=f"{path}[]",
            optional_struct_fields=optional_struct_fields,
        )
        zeroed_offsets = cast(Any, pc).subtract(offsets, start).cast(pa.int32())
        return pa.ListArray.from_arrays(
            zeroed_offsets,
            aligned_values,
            type=target_type,
            mask=cast(Any, pc).is_null(array),
        )

    if pa.types.is_struct(target_type):
        if not pa.types.is_struct(array.type):
            raise ValueError(f"{path}: expected struct, got {array.type}")
        actual_names = [field.name for field in array.type]
        if len(actual_names) != len(set(actual_names)):
            raise ValueError(f"{path}: duplicate struct fields in Parquet batch")
        target_names = {field.name for field in target_type}
        if unexpected := set(actual_names) - target_names:
            raise ValueError(f"{path}: unexpected struct fields {sorted(unexpected)}")
        aligned_children = []
        for field in target_type:
            idx = array.type.get_field_index(field.name)
            if idx >= 0:
                aligned_children.append(
                    _align_array_to_type(
                        array.field(idx),
                        field.type,
                        path=f"{path}.{field.name}",
                    )
                )
            else:
                if not (field.nullable and field.name in optional_struct_fields):
                    raise ValueError(f"{path}: missing required struct field {field.name!r}")
                aligned_children.append(pa.nulls(len(array), type=field.type))
        return pa.StructArray.from_arrays(
            aligned_children,
            fields=list(target_type),
            mask=cast(Any, pc).is_null(array),
        )

    if array.type.equals(target_type):
        return array
    if pa.types.is_large_string(array.type) and pa.types.is_string(target_type):
        return array.cast(target_type)
    raise ValueError(f"{path}: incompatible column type {array.type} (expected {target_type})")


def _align_comments_batch_to_schema(batch: Any, target_schema: Any) -> Any:
    """Align a comments Parquet batch (schema_id 0 or 1) strictly to ``target_schema``."""
    import pyarrow as pa
    from spicy_regs.transforms.regulations_shape import COMMENT_ATTACHMENT_METADATA

    actual_names = batch.schema.names
    if len(actual_names) != len(set(actual_names)):
        raise ValueError("Duplicate columns in comments Parquet batch")
    target_names = set(target_schema.names)
    if unexpected := set(actual_names) - target_names:
        raise ValueError(f"Unexpected columns in comments Parquet batch: {sorted(unexpected)}")
    if missing := [name for name in target_schema.names if name not in set(actual_names)]:
        raise ValueError(f"Missing columns in comments Parquet batch: {missing}")

    optional_attachment_fields = frozenset(name for _, name, _ in COMMENT_ATTACHMENT_METADATA)
    aligned_columns = []
    for field in target_schema:
        col = batch.column(batch.schema.get_field_index(field.name))
        aligned_columns.append(
            _align_array_to_type(
                col,
                field.type,
                path=field.name,
                optional_struct_fields=optional_attachment_fields if field.name == "attachments" else frozenset(),
            )
        )
    aligned = pa.RecordBatch.from_arrays(aligned_columns, schema=target_schema)
    if not aligned.schema.equals(target_schema):
        raise ValueError("Aligned comments batch schema differs from target schema")
    return aligned


def _read_position_deletes(
    client: Any,
    bucket: str,
    delete_uris: Sequence[str],
    planned_paths: set[str],
    work_dir: Path,
) -> dict[str, Any]:
    """Download each unique position-delete Parquet file once, parse ``(file_path, pos)``, and unlink immediately."""
    import numpy as np
    import pyarrow.parquet as pq

    grouped: dict[str, list[int]] = {}
    for delete_uri in delete_uris:
        key = _parse_s3_uri(delete_uri, bucket)
        tmp_del = work_dir / f"delete-{uuid4().hex}.parquet"
        try:
            client.download_file(bucket, key, str(tmp_del))
            pf = pq.ParquetFile(tmp_del)
            col_names = set(pf.schema_arrow.names)
            if not {"file_path", "pos"}.issubset(col_names):
                raise ValueError(f"Position-delete file {delete_uri!r} lacks required (file_path, pos) columns")
            for batch in pf.iter_batches(columns=["file_path", "pos"]):
                paths_col = batch.column(0)
                pos_col = batch.column(1)
                if paths_col.null_count or pos_col.null_count:
                    raise ValueError(f"Position-delete file {delete_uri!r} contains NULL entries")
                for fp, pos in zip(paths_col.to_pylist(), pos_col.to_pylist(), strict=True):
                    if not isinstance(fp, str) or not isinstance(pos, int) or pos < 0:
                        raise ValueError(f"Position-delete file {delete_uri!r} contains invalid (file_path, pos)")
                    if fp in planned_paths:
                        grouped.setdefault(fp, []).append(pos)
        finally:
            tmp_del.unlink(missing_ok=True)
    return {fp: np.unique(np.asarray(positions, dtype=np.int64)) for fp, positions in grouped.items()}


def _copy_pinned_iceberg_snapshot(
    record_type: RecordType,
    current: CatalogSnapshot,
    namespace: str,
    subjects: Path,
    *,
    work_dir: Path,
    resources: ExportResources,
) -> Path:
    """Export a pinned Iceberg snapshot via PyIceberg planning + direct R2 Parquet streaming.

    Bypasses DuckDB 1.5.5's ``IcebergMultiFileReader`` crash across evolved nested struct
    schemas (``schema_id: 0`` -> ``schema_id: 1``) while verifying table pin, S3 storage
    boundaries, position-delete completeness, and strict schema alignment.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.manifest import DataFileContent
    from spicy_regs.transforms.regulations_receipts import policy
    from . import r2

    if not is_configured():
        missing = [var for var in _REQUIRED_ENV if not getenv(var)]
        raise RuntimeError("R2 Data Catalog is not configured; missing env var(s): " + ", ".join(missing))
    bucket = getenv("R2_BUCKET_NAME", "")
    if not bucket:
        raise RuntimeError("R2_BUCKET_NAME is required for pinned Iceberg export")
    missing_s3 = [var for var in _REQUIRED_S3_ENV if not getenv(var)]
    if missing_s3:
        raise RuntimeError("Missing R2 S3 env var(s): " + ", ".join(missing_s3))

    catalog = RestCatalog(
        "comments",
        uri=getenv("R2_CATALOG_URI", ""),
        warehouse=getenv("R2_CATALOG_WAREHOUSE", ""),
        token=getenv("R2_CATALOG_TOKEN", ""),
    )
    table = catalog.load_table((namespace, record_type.name))
    observed_pin = (
        str(table.metadata.table_uuid),
        table.metadata.current_snapshot_id,
        table.metadata.current_schema_id,
    )
    expected_pin = (current.table_uuid, current.snapshot_id, current.schema_id)
    if observed_pin != expected_pin:
        raise RuntimeError(
            f"Catalog snapshot changed before export (expected {expected_pin}, got {observed_pin}); retry"
        )

    tasks = list(table.scan(snapshot_id=current.snapshot_id).plan_files())
    if not tasks:
        raise RuntimeError(f"Iceberg snapshot {current.snapshot_id} planned zero data files; refusing empty scan")

    data_paths: set[str] = set()
    delete_paths: set[str] = set()
    for task in tasks:
        if task.file.content != DataFileContent.DATA:
            raise ValueError(f"Refusing non-data Iceberg task content {task.file.content!r}: {task.file.file_path!r}")
        _parse_s3_uri(task.file.file_path, bucket)
        if task.file.file_path in data_paths:
            raise ValueError(f"Duplicate data file in Iceberg scan plan: {task.file.file_path!r}")
        data_paths.add(task.file.file_path)
        for delete_file in task.delete_files:
            if delete_file.content != DataFileContent.POSITION_DELETES:
                raise ValueError(
                    f"Refusing unsupported Iceberg delete file content {delete_file.content!r}: "
                    f"{delete_file.file_path!r}"
                )
            _parse_s3_uri(delete_file.file_path, bucket)
            delete_paths.add(delete_file.file_path)

    tasks.sort(key=lambda t: t.file.file_path)
    client = r2.get_r2_client()
    deleted_by_file = (
        _read_position_deletes(client, bucket, sorted(delete_paths), data_paths, work_dir)
        if delete_paths
        else {}
    )

    target_schema = policy(record_type.name).subject_schema
    max_group_bytes = _parse_byte_size(resources.row_group_bytes)
    read_batch_size = min(resources.row_group_rows, 2048)

    with pq.ParquetWriter(subjects, target_schema, compression="zstd") as writer:
        pending: list[Any] = []
        pending_rows = 0
        pending_bytes = 0

        def flush() -> None:
            nonlocal pending_rows, pending_bytes
            if pending:
                writer.write_table(
                    pa.Table.from_batches(pending, schema=target_schema),
                    row_group_size=resources.row_group_rows,
                )
                pending.clear()
                pending_rows = 0
                pending_bytes = 0

        for task in tasks:
            key = _parse_s3_uri(task.file.file_path, bucket)
            deleted_positions = deleted_by_file.get(task.file.file_path)
            tmp_data = work_dir / f"data-{uuid4().hex}.parquet"
            try:
                client.download_file(bucket, key, str(tmp_data))
                pf = pq.ParquetFile(tmp_data)
                expected_file_rows = getattr(task.file, "record_count", None)
                if expected_file_rows is not None and expected_file_rows >= 0 and pf.metadata.num_rows != expected_file_rows:
                    raise RuntimeError(
                        f"Data file {task.file.file_path!r} holds {pf.metadata.num_rows} rows, "
                        f"expected {expected_file_rows} from Iceberg manifest"
                    )
                offset = 0
                for batch in pf.iter_batches(batch_size=read_batch_size):
                    n = batch.num_rows
                    if deleted_positions is not None and len(deleted_positions):
                        lo = int(np.searchsorted(deleted_positions, offset, side="left"))
                        hi = int(np.searchsorted(deleted_positions, offset + n, side="left"))
                        if hi > lo:
                            if hi - lo == n:
                                offset += n
                                continue
                            keep = np.ones(n, dtype=bool)
                            keep[deleted_positions[lo:hi] - offset] = False
                            batch = batch.filter(pa.array(keep))
                    offset += n
                    if batch.num_rows == 0:
                        continue
                    aligned = _align_comments_batch_to_schema(batch, target_schema)
                    pending.append(aligned)
                    pending_rows += aligned.num_rows
                    pending_bytes += aligned.nbytes
                    if pending_rows >= resources.row_group_rows or pending_bytes >= max_group_bytes:
                        flush()
            finally:
                tmp_data.unlink(missing_ok=True)
        flush()

    return subjects


def export_public_comments(
    output_dir: Path, record_type: RecordType, *, memory_limit: str = ExportResources.memory,
    threads: int = ExportResources.threads,
    snapshot: CatalogPairSnapshot | None = None, resources: ExportResources | None = None,
) -> dict[str, Path]:
    """Copy the pinned comments table once, check the copy is complete, stage it by agency, sort, assemble, recount.

    Reads the comments table, and of the shared receipt table its metadata (the pinned pair's identity) and, only
    when the snapshot's summary states no usable row count, the number of accepted receipts (ss-juz8). It reads no
    receipt row's contents. The paired export it replaced copied the 26.5M receipts beside the table, rebound each
    in Python, admitted both and replayed every row, about 138 of 170 measured minutes at full size (M4 Pro, 3GB /
    1 thread), to publish rows it never changed. What those passes checked is owned elsewhere now:

    * completeness: the copy must hold the rows :func:`_pinned_row_count` gives, or the export refuses with both
      numbers. A row the published IDs never held (everything after 2026-10-10 on the first run) would otherwise
      be lost to a partial read without notice, since the index is built from the same copy;
    * the declared schema, here; unique non-null IDs, index coverage and every previously published ID, in
      :func:`spicy_regs.pipelines.comments_mirror.validate_export`;
    * each row's values when they are written, by ``regulatory_catalog.replace_native``'s readback inside the
      writing transaction.

    Not caught: a value changed outside ``replace_native``; and, when the summary decides, a row inserted or
    deleted outside it, since the summary counts the table as written (the receipt count sees such a row unless
    a receipt changed with it).

    The copy is bounded as the paired one was (``resources``' row and byte targets, ss-7flp) and is deleted once
    staged, so the copy no longer outlives the export. Every payload read from the catalog includes its Iceberg
    deletes. The published files keep their paths and take the policy's schema, which the fixed root files did
    not have before this route first rewrote them (docs/comments-receipt-archive.md).
    """
    import duckdb
    import pyarrow.parquet as pq
    from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies
    from spicy_regs.transforms.regulations_receipts import policy
    from . import regulatory_catalog as native

    resources = resources or ExportResources(memory=memory_limit, threads=threads)
    output_dir.mkdir(parents=True, exist_ok=True)
    columns = policy(record_type.name).subject_schema.names
    with TemporaryDirectory(prefix="comments-export-", dir=output_dir) as work:
        work_dir = Path(work)
        subjects = work_dir / f"{record_type.name}.parquet"
        con = _connect()
        try:
            resources.configure(con, work_dir / "spill")
            namespace = _catalog_namespace(record_type)
            with _transaction(con):
                if difference := native.schema_difference(con, record_type):
                    raise ValueError("Native catalog schema differs from declared policy: " + difference)
                current = _read_pair_snapshot(con, record_type, namespace=namespace)
                # Only the comments table is read, so only its identity binds the export.
                if snapshot is not None and current.subject() != snapshot.subject():
                    raise RuntimeError("Catalog snapshot changed before export; retry")
                logger.info("Comments export: copying the pinned table (snapshot {})", current.snapshot_id)
                if native._remote_catalog(con) and is_configured():
                    _copy_pinned_iceberg_snapshot(
                        record_type, current, namespace, subjects, work_dir=work_dir, resources=resources
                    )
                else:
                    native._copy(con, f"SELECT * FROM {_qualified(record_type)}", subjects, resources=resources)
                copied = pq.ParquetFile(subjects).metadata.num_rows
                expected, source = _pinned_row_count(con, record_type, current, namespace=namespace)
                logger.info("Comments export: copied {:,} rows; snapshot {} holds {:,} by {}",
                            copied, current.snapshot_id, expected, source)
                if copied != expected:
                    raise RuntimeError(f"Comments export copied {copied:,} rows but snapshot {current.snapshot_id} "
                                       f"holds {expected:,} by {source}; refusing an incomplete read")
            stage_comment_agencies(con, f"SELECT * FROM read_parquet('{_sql_str(str(subjects))}')",
                                   work_dir / "staging", resources=resources)
        finally:
            con.close()
        subjects.unlink()
        partitions = sort_comment_agencies(work_dir / "staging", output_dir, resources=resources)
        with duckdb.connect() as con:
            resources.configure(con, work_dir / "spill")
            monolith = assemble_comments(con, partitions, output_dir, columns, resources=resources)
            con.from_parquet(str(monolith)).create_view("comments_export")
            index_file = _build_comments_index(con, record_type, output_dir,
                                               source_sql="SELECT * FROM comments_export")
        return {"comments": monolith, "index": index_file, "partitions": partitions}


def check_comments_against_indexes(record_type: RecordType, indexes: Mapping[str, Sequence[str]], *, agency: str | None = None,
                                   limit: int = 100, threads: int = 8) -> list[str]:
    """Raw catalog IDs and agency/docket/month counts against each labelled public index's file URLs.

    Remote Parquet and Iceberg scans wait on one ranged GET per column chunk; parallel requests, not CPU, set their
    pace (the full catalog check took 53.6 s at 2 threads and 18.8 s at 8, 2026-11-08).
    """
    from spicy_regs.comments_health import check_comments
    from spicy_regs.duckdb_settings import load_public_http
    from .publication import parquet_scan
    from .regulatory_catalog import require_initialized

    where = f" WHERE agency_code = '{_sql_str(agency)}'" if agency else ""
    con = _connect()
    try:
        load_public_http(con)
        con.execute(f"SET memory_limit='4GB'; SET threads={int(threads)}; SET preserve_insertion_order=false")
        require_initialized(con, record_type)
        return check_comments(con, f"SELECT * FROM {_qualified(record_type)}{where}",
                              {label: f"SELECT * FROM {parquet_scan(urls)}{where}" for label, urls in indexes.items()},
                              limit=limit)
    finally:
        con.close()


def audit_duplicates(con, record_type: RecordType) -> list[tuple[str, int, int]]:
    """Per-agency (agency_code, rows, distinct_keys) where rows exceed distinct keys.

    A read-only duplication report over the catalog table: any agency whose row
    count is above its distinct ``dedup_key`` count carries duplicate rows. Sorted
    by the number of duplicate rows, worst first. Empty when the table is clean.
    """
    from .regulatory_catalog import require_initialized
    require_initialized(con, record_type)
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
