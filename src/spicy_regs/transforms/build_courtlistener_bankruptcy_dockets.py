"""Build the typed ``courtlistener_bankruptcy_dockets`` table from the 2022-09-19 export.

Streams the retained bzip2-compressed PostgreSQL CSV export (``ESCAPE '"'``) through
``spicy_docs.sources.courtlistener.local.CourtListenerLocalDump``, applies fail-closed
batch validation and lossless typing in DuckDB, writes one unpartitioned zstd Parquet
file in source order, and verifies primary-key uniqueness before atomic rename.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

FAMILY = "courtlistener-bankruptcy-dockets"
TABLE = "courtlistener_bankruptcy_dockets"
OUTPUT = TABLE + ".parquet"
DUMP_DATE = "2022-09-19"
PUBLISHER_URL = "https://storage.courtlistener.com/bulk-data/randoms/bankruptcy-dockets-2022-09-19.csv.bz2"
PUBLISHER_S3_URL = (
    "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/bulk-data/randoms/"
    "bankruptcy-dockets-2022-09-19.csv.bz2"
)
EXPECTED_SHA256 = "sha256:cbea8540ac7cfc668788a92faf1cf9a62e22e3dbeb0ba830fd16ed6fa518ff43"
EXPECTED_BYTES = 1_347_654_605
EXPECTED_ETAG = '"4f0ff5433963ece1ec081a7f6184c44e-161"'

ROW_GROUP_SIZE = 100_000
DEFAULT_PIECE_BYTES = 128 << 20
DEFAULT_PARSE_WORKERS = 4
DEFAULT_DECOMPRESSION_THREADS = 4
DUCKDB_MEMORY_LIMIT = "2GB"

#: Exact 18-column header of CourtListener's ``bankruptcy-dockets-2022-09-19.csv.bz2`` export.
SOURCE_COLUMNS: tuple[str, ...] = (
    "id",
    "date_created",
    "date_modified",
    "assigned_to_str",
    "referred_to_str",
    "date_filed",
    "date_terminated",
    "case_name_short",
    "case_name",
    "case_name_full",
    "docket_number",
    "docket_number_core",
    "pacer_case_id",
    "date_blocked",
    "blocked",
    "assigned_to_id",
    "court_id",
    "referred_to_id",
)

#: Non-federal-bankruptcy courts captured by CourtListener's ``court_id LIKE '%b'`` export predicate.
NON_FB_COURT_IDS: tuple[str, ...] = (
    "neb",
    "mspb",
    "pactcomplcolumb",
    "paorphctcolumb",
    "nyoytermctalb",
)

SCHEMA = pa.schema(
    [
        ("cl_docket_id", pa.string()),
        ("court_id", pa.string()),
        ("docket_number", pa.string()),
        ("docket_number_core", pa.string()),
        ("pacer_case_id", pa.string()),
        ("case_name", pa.string()),
        ("case_name_short", pa.string()),
        ("case_name_full", pa.string()),
        ("date_filed", pa.date32()),
        ("date_terminated", pa.date32()),
        ("assigned_to_id", pa.string()),
        ("assigned_to_str", pa.string()),
        ("referred_to_id", pa.string()),
        ("referred_to_str", pa.string()),
        ("blocked", pa.bool_()),
        ("date_blocked", pa.date32()),
        ("date_created", pa.timestamp("us", tz="UTC")),
        ("date_modified", pa.timestamp("us", tz="UTC")),
        ("dump_date", pa.string()),
        ("source_sha256", pa.string()),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {TABLE: SCHEMA}

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

_NON_FB_SQL = ", ".join(f"'{code}'" for code in NON_FB_COURT_IDS)

_VALIDATION_SQL = f"""
SELECT
    COUNT(*) FILTER (WHERE id IS NULL OR NOT regexp_full_match(id, '[1-9][0-9]*')) AS bad_id,
    COUNT(*) FILTER (WHERE court_id IS NULL OR court_id = '') AS bad_court_id,
    COUNT(*) FILTER (WHERE blocked IS NULL OR blocked NOT IN ('t', 'f')) AS bad_blocked,
    COUNT(*) FILTER (
        WHERE date_filed IS NOT NULL
          AND (NOT regexp_full_match(date_filed, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')
               OR TRY_CAST(date_filed AS DATE) IS NULL)
    ) AS bad_date_filed,
    COUNT(*) FILTER (
        WHERE date_terminated IS NOT NULL
          AND (NOT regexp_full_match(date_terminated, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')
               OR TRY_CAST(date_terminated AS DATE) IS NULL)
    ) AS bad_date_terminated,
    COUNT(*) FILTER (
        WHERE date_blocked IS NOT NULL
          AND (NOT regexp_full_match(date_blocked, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')
               OR TRY_CAST(date_blocked AS DATE) IS NULL)
    ) AS bad_date_blocked,
    COUNT(*) FILTER (
        WHERE date_created IS NULL
          OR NOT regexp_full_match(
              date_created,
              '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}(\\.[0-9]{{1,6}})?\\+00'
          )
          OR TRY_CAST(date_created AS TIMESTAMPTZ) IS NULL
    ) AS bad_date_created,
    COUNT(*) FILTER (
        WHERE date_modified IS NULL
          OR NOT regexp_full_match(
              date_modified,
              '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}(\\.[0-9]{{1,6}})?\\+00'
          )
          OR TRY_CAST(date_modified AS TIMESTAMPTZ) IS NULL
    ) AS bad_date_modified,
    COUNT(*) FILTER (
        WHERE assigned_to_id IS NOT NULL AND NOT regexp_full_match(assigned_to_id, '[1-9][0-9]*')
    ) AS bad_assigned_to_id,
    COUNT(*) FILTER (
        WHERE referred_to_id IS NOT NULL AND NOT regexp_full_match(referred_to_id, '[1-9][0-9]*')
    ) AS bad_referred_to_id,
    COUNT(*) FILTER (WHERE assigned_to_str IS NULL) AS null_assigned_to_str,
    COUNT(*) FILTER (WHERE referred_to_str IS NULL) AS null_referred_to_str,
    COUNT(*) FILTER (WHERE case_name_short IS NULL) AS null_case_name_short,
    COUNT(*) FILTER (WHERE case_name IS NULL) AS null_case_name,
    COUNT(*) FILTER (WHERE case_name_full IS NULL) AS null_case_name_full,
    COUNT(*) FILTER (WHERE docket_number_core IS NULL) AS null_docket_number_core,
    COUNT(*) FILTER (WHERE docket_number IS NULL) AS docket_number_null,
    COUNT(*) FILTER (WHERE docket_number = '') AS docket_number_empty,
    COUNT(*) FILTER (WHERE blocked = 't') AS blocked_true,
    COUNT(*) FILTER (WHERE blocked = 'f') AS blocked_false,
    COUNT(*) FILTER (WHERE court_id IN ({_NON_FB_SQL})) AS non_fb_rows
FROM _batch
"""

_TRANSFORM_SQL = """
SELECT
    id::VARCHAR AS cl_docket_id,
    court_id::VARCHAR AS court_id,
    docket_number::VARCHAR AS docket_number,
    docket_number_core::VARCHAR AS docket_number_core,
    pacer_case_id::VARCHAR AS pacer_case_id,
    case_name::VARCHAR AS case_name,
    case_name_short::VARCHAR AS case_name_short,
    case_name_full::VARCHAR AS case_name_full,
    CAST(date_filed AS DATE) AS date_filed,
    CAST(date_terminated AS DATE) AS date_terminated,
    assigned_to_id::VARCHAR AS assigned_to_id,
    assigned_to_str::VARCHAR AS assigned_to_str,
    referred_to_id::VARCHAR AS referred_to_id,
    referred_to_str::VARCHAR AS referred_to_str,
    (blocked = 't')::BOOLEAN AS blocked,
    CAST(date_blocked AS DATE) AS date_blocked,
    CAST(date_created AS TIMESTAMPTZ) AS date_created,
    CAST(date_modified AS TIMESTAMPTZ) AS date_modified,
    ?::VARCHAR AS dump_date,
    ?::VARCHAR AS source_sha256
FROM _batch
"""

_GUARD_MESSAGES: tuple[tuple[str, str], ...] = (
    ("bad_id", "cl_docket_id (id) must be non-null positive decimal digits matching ^[1-9][0-9]*$"),
    ("bad_court_id", "court_id must be non-null and non-empty"),
    ("bad_blocked", "blocked must be non-null canonical 't' or 'f'"),
    ("bad_date_filed", "date_filed must be NULL or valid ISO YYYY-MM-DD DATE"),
    ("bad_date_terminated", "date_terminated must be NULL or valid ISO YYYY-MM-DD DATE"),
    ("bad_date_blocked", "date_blocked must be NULL or valid ISO YYYY-MM-DD DATE"),
    ("bad_date_created", "date_created must be non-null UTC (+00) TIMESTAMPTZ"),
    ("bad_date_modified", "date_modified must be non-null UTC (+00) TIMESTAMPTZ"),
    ("bad_assigned_to_id", "assigned_to_id must be NULL or match ^[1-9][0-9]*$"),
    ("bad_referred_to_id", "referred_to_id must be NULL or match ^[1-9][0-9]*$"),
    ("null_assigned_to_str", "assigned_to_str is NOT NULL in source schema and must not contain SQL NULL"),
    ("null_referred_to_str", "referred_to_str is NOT NULL in source schema and must not contain SQL NULL"),
    ("null_case_name_short", "case_name_short is NOT NULL in source schema and must not contain SQL NULL"),
    ("null_case_name", "case_name is NOT NULL in source schema and must not contain SQL NULL"),
    ("null_case_name_full", "case_name_full is NOT NULL in source schema and must not contain SQL NULL"),
    ("null_docket_number_core", "docket_number_core is NOT NULL in source schema and must not contain SQL NULL"),
)


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _flush_buffer(
    writer: pq.ParquetWriter,
    buffered: list[pa.Table],
    *,
    flush_all: bool,
) -> list[pa.Table]:
    if not buffered:
        return []
    combined = buffered[0] if len(buffered) == 1 else pa.concat_tables(buffered)
    if flush_all:
        if combined.num_rows > 0:
            writer.write_table(combined, row_group_size=ROW_GROUP_SIZE)
        return []
    offset = 0
    total = combined.num_rows
    while total - offset >= ROW_GROUP_SIZE:
        writer.write_table(combined.slice(offset, ROW_GROUP_SIZE), row_group_size=ROW_GROUP_SIZE)
        offset += ROW_GROUP_SIZE
    return [combined.slice(offset)] if offset < total else []


def _verify_unique_docket_ids(parquet_path: Path, expected_rows: int, *, temp_dir: Path) -> None:
    with duckdb.connect() as con:
        con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        con.execute("SET threads = 4")
        con.execute("SET preserve_insertion_order = false")
        escaped_temp = str(temp_dir).replace("'", "''")
        con.execute(f"SET temp_directory = '{escaped_temp}'")
        row = con.execute(
            "SELECT COUNT(*)::BIGINT, COUNT(DISTINCT cl_docket_id)::BIGINT FROM read_parquet(?)",
            [str(parquet_path)],
        ).fetchone()
    if row is None:
        raise ValueError("Post-write verification query returned no rows")
    total_rows, distinct_ids = int(row[0]), int(row[1])
    if total_rows != expected_rows:
        raise ValueError(f"Written Parquet row count ({total_rows}) differs from decoded row count ({expected_rows})")
    if distinct_ids != total_rows:
        raise ValueError(
            f"Duplicate cl_docket_id values detected in {TABLE}: "
            f"{total_rows} rows vs {distinct_ids} distinct IDs"
        )


def build_courtlistener_bankruptcy_dockets(
    local_file: Path,
    output_dir: Path,
    *,
    dump_date: str = DUMP_DATE,
    source_sha256: str | None = None,
    piece_bytes: int = DEFAULT_PIECE_BYTES,
    parse_workers: int = DEFAULT_PARSE_WORKERS,
    decompression_threads: int = DEFAULT_DECOMPRESSION_THREADS,
) -> tuple[Path, dict[str, Any]]:
    """Stream, validate, type, and write ``courtlistener_bankruptcy_dockets.parquet``.

    Returns ``(output_path, stats)`` after post-write schema and primary-key uniqueness
    checks succeed and the temporary Parquet file is atomically renamed into place.
    """
    source_path = Path(local_file)
    if source_path.is_symlink() or not source_path.is_file():
        raise ValueError("Retained CourtListener bankruptcy export must be a regular file")
    if not isinstance(dump_date, str) or not _ISO_DATE_RE.fullmatch(dump_date):
        raise ValueError(f"dump_date must be an ISO YYYY-MM-DD date, got {dump_date!r}")
    if source_sha256 is not None and (not isinstance(source_sha256, str) or not _SHA256_RE.fullmatch(source_sha256)):
        raise ValueError("source_sha256 must be a canonical sha256:<64 hex> digest")

    observed_sha256 = _file_sha256(source_path)
    if source_sha256 is not None and source_sha256 != observed_sha256:
        raise ValueError("Retained CourtListener bankruptcy export SHA-256 differs from expected source_sha256")
    resolved_sha256 = observed_sha256

    dest_dir = Path(output_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / OUTPUT
    tmp_path = dest_dir / f".{OUTPUT}.{uuid4().hex}.tmp"

    written_rows = 0
    docket_number_null = 0
    docket_number_empty = 0
    blocked_true = 0
    blocked_false = 0
    non_fb_rows = 0

    try:
        with TemporaryDirectory(dir=dest_dir, prefix=".cl-bankruptcy-work-") as work_dir:
            work_root = Path(work_dir)
            pieces_dir = work_root / "pieces"
            pieces_dir.mkdir()
            spill_dir = work_root / "duckdb-temp"
            spill_dir.mkdir()

            dump = CourtListenerLocalDump(
                source_path,
                piece_bytes=piece_bytes,
                parse_workers=parse_workers,
                decompression_threads=decompression_threads,
                work_dir=pieces_dir,
                escape='"',
            )

            with duckdb.connect() as con:
                con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
                con.execute(f"SET threads = {max(1, min(4, int(parse_workers)))}")
                con.execute("SET TimeZone = 'UTC'")

                header_checked = False
                buffered: list[pa.Table] = []
                buffered_rows = 0
                with pq.ParquetWriter(tmp_path, SCHEMA, compression="zstd") as writer:
                    for batch_index, batch in enumerate(dump.iter_batches()):
                        if not header_checked:
                            if tuple(dump.header) != SOURCE_COLUMNS or tuple(batch.schema.names) != SOURCE_COLUMNS:
                                raise ValueError(
                                    f"CourtListener bankruptcy export header differs from expected 18 columns: "
                                    f"expected {SOURCE_COLUMNS}, got {tuple(dump.header)}"
                                )
                            header_checked = True
                        if batch.num_rows == 0:
                            continue

                        con.register("_batch", batch)
                        try:
                            check_row = con.execute(_VALIDATION_SQL).fetchone()
                            if check_row is None:
                                raise ValueError(f"Batch {batch_index}: validation query returned no result")
                            for offset, (label, message) in enumerate(_GUARD_MESSAGES):
                                bad_count = int(check_row[offset])
                                if bad_count:
                                    raise ValueError(
                                        f"Batch {batch_index} failed {label} guard ({bad_count} row(s)): {message}"
                                    )
                            stats_offset = len(_GUARD_MESSAGES)
                            docket_number_null += int(check_row[stats_offset])
                            docket_number_empty += int(check_row[stats_offset + 1])
                            blocked_true += int(check_row[stats_offset + 2])
                            blocked_false += int(check_row[stats_offset + 3])
                            non_fb_rows += int(check_row[stats_offset + 4])

                            typed_table = con.execute(
                                _TRANSFORM_SQL,
                                [dump_date, resolved_sha256],
                            ).to_arrow_table()
                        finally:
                            con.unregister("_batch")

                        if not typed_table.schema.equals(SCHEMA):
                            typed_table = typed_table.cast(SCHEMA)

                        written_rows += typed_table.num_rows
                        buffered.append(typed_table)
                        buffered_rows += typed_table.num_rows
                        if buffered_rows >= ROW_GROUP_SIZE:
                            buffered = _flush_buffer(writer, buffered, flush_all=False)
                            buffered_rows = sum(tbl.num_rows for tbl in buffered)

                    if not header_checked and tuple(dump.header) != SOURCE_COLUMNS:
                        raise ValueError(
                            f"CourtListener bankruptcy export header differs from expected 18 columns: "
                            f"expected {SOURCE_COLUMNS}, got {tuple(dump.header)}"
                        )
                    _flush_buffer(writer, buffered, flush_all=True)

            if not dump.completed:
                raise ValueError("CourtListenerLocalDump did not reach completed=True")
            if written_rows != dump.rows:
                raise ValueError(f"Written row count ({written_rows}) differs from dump.rows ({dump.rows})")
            if written_rows == 0:
                raise ValueError("CourtListener bankruptcy export contains zero data rows")

            actual_schema = pq.read_schema(tmp_path)
            if not actual_schema.equals(SCHEMA):
                raise ValueError(f"Written Parquet schema differs from declared SCHEMA: {actual_schema}")

            _verify_unique_docket_ids(tmp_path, written_rows, temp_dir=spill_dir)

        tmp_path.replace(out_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    stats: dict[str, Any] = {
        "table": TABLE,
        "rows": written_rows,
        "pieces": dump.pieces,
        "canary_rows": dump.canary_rows,
        "decompressed_bytes": dump.decompressed_bytes,
        "docket_number_null": docket_number_null,
        "docket_number_empty": docket_number_empty,
        "blocked_true": blocked_true,
        "blocked_false": blocked_false,
        "non_fb_rows": non_fb_rows,
        "dump_date": dump_date,
        "source_sha256": resolved_sha256,
        "output_bytes": out_path.stat().st_size,
    }
    return out_path, stats
