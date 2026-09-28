"""Re-read every Mirrulations comment once, keeping everything a later column decision could need (owner, 2026-09-28).

The catalog replaces a row only when its ``modify_date`` moves, and the manifest skips keys already read, so rows
ingested before a field was retained keep it NULL: the four comment-reference columns, ``subtype`` and
``duplicate_comments`` (``iceberg._COMMENT_ADDED_COLUMNS``), and ``attachments_json`` wherever the row's first read
lacked the attachments. One pass keeps, per object: its key and the GET's ETag and size; the whole thin-table row
through ``COMMENT.extract`` (one spelling), with the body as its SHA-256 and length; and every other stated attribute
as compact JSON (``attributes_json``: the keys the extract does not map, non-null only), which ``comment_attributes``
is built from.

Two phases, so the long read holds no lock and the catalog write holds it briefly:

* ``plan`` + ``read`` touch no catalog. ``plan`` splits the ETL manifest's comment keys into fixed chunks per agency;
  ``read`` fetches each chunk through the ETL's Mirrulations reader and writes one Parquet part per chunk. A part is
  written whole or not at all, so a rerun reads only the chunks that have no part, and a chunk with transport
  failures stays unwritten for the next run. Answers that are not transport failures (unreadable or empty objects)
  are kept in the chunk's journal line. A plan is named by the manifest digest and the record shape, so a changed
  shape is a new plan, never parts of two shapes stitched together.
* ``write`` (see :func:`fill_catalog`) runs under the catalog lock.
"""

from __future__ import annotations

import hashlib
import json
import resource as rusage
import shutil
import sys
import time
import zlib
from collections.abc import Iterator
from pathlib import Path

import duckdb
import polars as pl
from cyclopts import App
from loguru import logger

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg

#: The columns a fill writes: the nullable columns added after the catalog table was created, and attachments_json.
FILL_COLUMNS = (*iceberg._COMMENT_ADDED_COLUMNS, "attachments_json")
#: Keys per chunk: the resume unit, about 17 s of reads at the measured rate.
CHUNK_KEYS = 20_000
#: The part record's shape; a plan carries it, so a new shape is a new plan.
RECORD_SHAPE = "s2"
#: The ``data.attributes`` keys ``COMMENT.extract`` maps into the thin row; every other stated key is kept in
#: ``attributes_json``. ``test_extracted_attributes_are_the_keys_the_extract_reads`` derives this set independently.
EXTRACTED_ATTRIBUTES = frozenset({
    "agencyId", "category", "comment", "commentOn", "commentOnDocumentId", "docketId", "documentType",
    "duplicateComments", "firstName", "lastName", "modifyDate", "organization", "originalDocumentId", "postedDate",
    "receiveDate", "subtype", "title",
})
#: Host enrichment columns: never in the source record, so never read here.
_HOST_COLUMNS = ("text_content", "text_extraction_status", "pdf_extraction_results_json")
PART_SCHEMA = {
    "key": pl.Utf8, "etag": pl.Utf8, "size": pl.Int64,
    **{c: t for c, t in COMMENT.schema.items() if c not in (*_HOST_COLUMNS, "comment")},
    "comment_sha256": pl.Utf8, "comment_length": pl.Int64, "attributes_json": pl.Utf8,
}


def part_row(keyed) -> dict:
    """One part row for a keyed Mirrulations payload: identity, the thin row, the body's digest, the other attributes."""
    row = COMMENT.extract(keyed.payload)
    body = row["comment"]
    attributes = (keyed.payload.get("data") or {}).get("attributes") or {}
    stated = {key: value for key, value in attributes.items() if key not in EXTRACTED_ATTRIBUTES and value is not None}
    return {
        "key": keyed.key, "etag": keyed.etag, "size": keyed.size,
        **{column: row[column] for column in PART_SCHEMA if column in row and column != "comment"},
        "comment_sha256": None if body is None else hashlib.sha256(body.encode()).hexdigest(),
        "comment_length": None if body is None else len(body),
        "attributes_json": json.dumps(stated, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
    }


def _sql(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def plan(workdir: Path, manifest: Path, *, chunk_keys: int = CHUNK_KEYS) -> Path:
    """Add the manifest's comment keys that no earlier plan holds, as new fixed chunks; return the plan file.

    A plan is immutable and named by the manifest's digest, so a rerun on the same manifest is a no-op and a newer
    manifest adds only the keys ingested since. Chunks are contiguous in key order within an agency directory.
    """
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    plans = workdir / "plan"
    target = plans / f"{digest[:16]}-{RECORD_SHAPE}.parquet"
    if target.exists():
        return target
    plans.mkdir(parents=True, exist_ok=True)
    earlier = sorted(plans.glob(f"*-{RECORD_SHAPE}.parquet"))
    temporary = target.with_suffix(".tmp")
    with duckdb.connect() as con:
        con.execute("SET preserve_insertion_order=false")
        known = f"SELECT key FROM read_parquet([{', '.join(map(_sql, earlier))}])" if earlier else "SELECT NULL::VARCHAR AS key WHERE false"
        con.execute(f"""
            COPY (
                SELECT key, split_part(key, '/', 2) AS agency,
                       (row_number() OVER (PARTITION BY split_part(key, '/', 2) ORDER BY key) - 1) // {int(chunk_keys)} AS chunk
                FROM read_parquet({_sql(manifest)})
                WHERE key LIKE '%/comments/%' AND key LIKE '%.json' AND key NOT IN ({known})
                ORDER BY agency, chunk, key
            ) TO {_sql(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(chunk_keys)})
        """)
        keys, chunks = con.execute(
            f"SELECT count(*), count(DISTINCT (agency, chunk)) FROM read_parquet({_sql(temporary)})").fetchall()[0]
    temporary.replace(target)
    (plans / f"{digest[:16]}-{RECORD_SHAPE}.json").write_text(json.dumps(
        {"manifest": str(manifest), "manifest_sha256": digest, "record_shape": RECORD_SHAPE, "keys": keys,
         "chunks": chunks, "chunk_keys": chunk_keys,
         "earlier_plans": [p.name for p in earlier]}, indent=2) + "\n")
    logger.info("plan {}: {:,} new comment keys in {:,} chunks", target.name, keys, chunks)
    return target


def _chunks(
    workdir: Path, agencies: set[str] | None, shard: tuple[int, int] | None = None,
) -> Iterator[tuple[str, str, int, list[str]]]:
    """Every planned chunk as ``(plan id, agency, chunk, keys)``, in plan, agency and chunk order.

    ``shard`` ``(i, n)`` keeps the chunks whose CRC-32 of ``agency/chunk`` is ``i`` modulo ``n``: chunks, not
    agencies, so the largest agencies spread across every process.
    """
    for plan_file in sorted((workdir / "plan").glob(f"*-{RECORD_SHAPE}.parquet")):
        with duckdb.connect() as con:
            index = con.execute(f"SELECT DISTINCT agency, chunk FROM {_sql(plan_file)} ORDER BY 1, 2").fetchall()
            for agency, chunk in index:
                if agencies is not None and agency not in agencies:
                    continue
                if shard is not None and zlib.crc32(f"{agency}/{chunk}".encode()) % shard[1] != shard[0]:
                    continue
                keys = [row[0] for row in con.execute(
                    f"SELECT key FROM {_sql(plan_file)} WHERE agency = ? AND chunk = ? ORDER BY key", [agency, chunk]
                ).fetchall()]
                yield plan_file.stem, agency, chunk, keys


def part_path(workdir: Path, plan_id: str, agency: str, chunk: int) -> Path:
    return workdir / "parts" / f"agency={agency}" / f"{plan_id}-{chunk:05d}.parquet"


def _peak_rss_mb() -> float:
    peak = rusage.getrusage(rusage.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10


def read(
    workdir: Path, *, workers: int = 64, agencies: list[str] | None = None, max_chunks: int | None = None,
    max_rss_mb: float = 2048, min_free_gb: float = 4, shard: tuple[int, int] | None = None, resource=None,
    progress_name: str = "progress.json",
) -> dict:
    """Read every planned chunk that has no part yet; return this run's counts.

    ``agencies`` restricts the run (a shard); ``resource`` replaces the anonymous S3 resource in tests. The process
    stops cleanly before a chunk when the disk holding ``workdir`` has less than ``min_free_gb`` free, and after the
    chunk that takes its peak RSS past ``max_rss_mb``; a rerun resumes where it stopped.
    """
    from spicy_docs.sources import mirrulations

    from spicy_regs.pipelines.regulations_state import source_record_type

    resource = resource or mirrulations.s3_resource(max_pool_connections=workers)
    source = source_record_type(COMMENT)
    journal = workdir / "journal.jsonl"
    progress = workdir / progress_name
    totals: dict = {"chunks": 0, "keys": 0, "rows": 0, "unresolved": 0, "deferred_chunks": 0, "stopped": None}
    started = time.monotonic()
    wanted = set(agencies) if agencies is not None else None
    for plan_id, agency, chunk, keys in _chunks(workdir, wanted, shard):
        target = part_path(workdir, plan_id, agency, chunk)
        if target.exists():
            continue
        if max_chunks is not None and totals["chunks"] + totals["deferred_chunks"] >= max_chunks:
            break
        free_gb = shutil.disk_usage(workdir).free / 2**30
        if free_gb < min_free_gb:
            totals["stopped"] = f"{free_gb:.1f} GB free, below {min_free_gb:.1f} GB"
            logger.warning("stopping before {} chunk {}: {}", agency, chunk, totals["stopped"])
            break
        begun = time.monotonic()
        reader = mirrulations.MirrulationsReader(
            resource, mirrulations.BUCKET, mirrulations.PREFIX, agency, source,
            key_lister=lambda keys=keys: keys, download_workers=workers,
        )
        rows = [part_row(keyed) for keyed in reader.iter_keyed_records()]
        transport = [o for o in reader.unresolved if o.status == mirrulations.STATUS_TRANSPORT]
        record = {"plan": plan_id, "agency": agency, "chunk": chunk, "keys": len(keys), "rows": len(rows),
                  "seconds": round(time.monotonic() - begun, 2),
                  "unresolved": [{"key": o.key, "status": o.status} for o in reader.unresolved]}
        if transport:
            # Unwritten, so the next run reads the whole chunk again; nothing it holds is lost or doubled.
            totals["deferred_chunks"] += 1
            logger.warning("{} chunk {}: {} keys failed in transport; deferred", agency, chunk, len(transport))
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        pl.DataFrame(rows, schema=PART_SCHEMA, orient="row").write_parquet(temporary, compression="zstd")
        temporary.replace(target)
        with journal.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        totals["chunks"] += 1
        totals["keys"] += len(keys)
        totals["rows"] += len(rows)
        totals["unresolved"] += len(reader.unresolved)
        elapsed = time.monotonic() - started
        progress.write_text(json.dumps({**totals, "seconds": round(elapsed, 1),
                                        "keys_per_second": round(totals["keys"] / elapsed, 1) if elapsed else None,
                                        "last": {k: v for k, v in record.items() if k != "unresolved"},
                                        "peak_rss_mb": round(_peak_rss_mb()),
                                        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                                       indent=2) + "\n")
        if _peak_rss_mb() > max_rss_mb:
            totals["stopped"] = f"peak RSS {_peak_rss_mb():.0f} MB above {max_rss_mb:.0f} MB"
            logger.warning("stopping after {} chunk {}: {}", agency, chunk, totals["stopped"])
            break
    return totals


def status(workdir: Path) -> dict:
    """Planned and read chunks and keys, from the plans and the parts on disk."""
    with duckdb.connect() as con:
        plans = sorted((workdir / "plan").glob(f"*-{RECORD_SHAPE}.parquet"))
        if not plans:
            return {"planned_keys": 0, "planned_chunks": 0, "read_chunks": 0, "read_keys": 0}
        planned = con.execute(f"""
            SELECT agency, chunk, split_part(filename, '/', -1) AS plan_file, count(*) AS n
            FROM read_parquet([{', '.join(map(_sql, plans))}], filename=true) GROUP BY ALL""").fetchall()
    done = {(path.parent.name.removeprefix("agency="), path.stem) for path in (workdir / "parts").glob("agency=*/*.parquet")}
    read_keys = sum(n for agency, chunk, plan_file, n in planned
                    if (agency, f"{plan_file.removesuffix('.parquet')}-{chunk:05d}") in done)
    return {"planned_keys": sum(row[3] for row in planned), "planned_chunks": len(planned),
            "read_chunks": len(done), "read_keys": read_keys}



# --------------------------------------------------------------------------- #
# Write phase: fill only what the catalog holds as NULL, for the version the read saw.
# --------------------------------------------------------------------------- #

#: The Iceberg scan's virtual column naming each row's data file; batches are whole files (see :func:`write`).
FILE_COLUMN = "filename"
#: Rows per batch when files are packed together; a file larger than this is still one batch.
BATCH_ROWS = 1_500_000


def _fill_dir(workdir: Path) -> Path:
    return workdir / "fill"


def _catalog_value(column: str) -> str:
    """A part value spelled as the catalog stores it: every catalog column is VARCHAR."""
    return f'CAST(s."{column}" AS VARCHAR)'


def prepare(workdir: Path, *, scope: dict[str, str] | None = None, con=None) -> dict:
    """Compute the fill at the catalog's current snapshot; write ``fill/fill.parquet`` and a counts-only receipt.

    One narrow scan reads each catalog row's key, ``agency_code``, ``docket_id``, ``modify_date``, fill columns and
    data file. A read row fills a catalog row only at the same ``comment_id`` and ``modify_date`` (a copy of another
    version is skipped and counted), only into a column the catalog holds as NULL, and never when the copies read for
    that version disagree (listed in ``fill/conflicts.parquet``). ``scope`` (column → value, e.g. a docket) narrows a
    pilot. Touches no catalog row and needs no lock; :func:`write` re-checks every condition inside its transactions.
    """
    own = con is None
    con = con or iceberg._connect()
    out = _fill_dir(workdir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        snapshot = iceberg._read_snapshot(con, COMMENT)
        where = " AND ".join(f'"{column}" = {_sql(value)}' for column, value in (scope or {}).items()) or "TRUE"
        fill = ", ".join(f'"{column}"' for column in FILL_COLUMNS)
        source = iceberg._snapshot_query(COMMENT, snapshot).replace("SELECT *", f"SELECT *, {FILE_COLUMN} AS _file", 1)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _cat AS SELECT comment_id, agency_code, docket_id, modify_date,
                        {fill}, _file FROM ({source}) WHERE {where}""")
        duplicated = con.execute("SELECT count(*) FROM (SELECT comment_id FROM _cat GROUP BY 1 HAVING count(*) > 1)"
                                 ).fetchone()[0]
        if duplicated:
            raise RuntimeError(f"{duplicated} comment ids appear more than once in the catalog; run the dedupe first")
        parts = f"read_parquet({_sql(workdir / 'parts' / '*' / '*.parquet')}, hive_partitioning=false)"
        values = ", ".join(f'{_catalog_value(c)} AS "{c}"' for c in FILL_COLUMNS)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _read AS SELECT s.key, s.comment_id, s.modify_date, {values}
                        FROM {parts} s SEMI JOIN _cat USING (comment_id)""")
        # Copies of one version that disagree on a fill value fill nothing for that version.
        packed = "{" + ", ".join(f"'{column}': \"{column}\"" for column in FILL_COLUMNS) + "}"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _versions AS
            SELECT comment_id, modify_date, count(DISTINCT {packed}) AS spellings, list(key ORDER BY key) AS keys,
                   any_value({packed}) AS v
            FROM _read GROUP BY comment_id, modify_date""")
        con.execute(f"""COPY (SELECT comment_id, modify_date, keys FROM _versions WHERE spellings > 1 ORDER BY 1)
                        TO {_sql(out / 'conflicts.parquet')} (FORMAT PARQUET)""")
        need = " OR ".join(f'(c."{col}" IS NULL AND v.v."{col}" IS NOT NULL)' for col in FILL_COLUMNS)
        filled = ", ".join(f'COALESCE(c."{col}", v.v."{col}") AS "{col}"' for col in FILL_COLUMNS)
        con.execute(f"""COPY (
            SELECT c.comment_id, c.agency_code, c.docket_id, c.modify_date, c._file, {filled}
            FROM _cat c JOIN _versions v ON v.comment_id = c.comment_id AND v.modify_date IS NOT DISTINCT FROM c.modify_date
            WHERE v.spellings = 1 AND ({need})
            ORDER BY c._file, c.comment_id
        ) TO {_sql(out / 'fill.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)""")
        counts = con.execute("""
            SELECT count(*) AS catalog_rows,
                   count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM _read r WHERE r.comment_id = c.comment_id)) AS unread,
                   count(*) FILTER (WHERE EXISTS (SELECT 1 FROM _read r WHERE r.comment_id = c.comment_id)
                                    AND NOT EXISTS (SELECT 1 FROM _versions v WHERE v.comment_id = c.comment_id
                                                    AND v.modify_date IS NOT DISTINCT FROM c.modify_date)) AS other_version_only,
                   count(*) FILTER (WHERE EXISTS (SELECT 1 FROM _versions v WHERE v.comment_id = c.comment_id
                                    AND v.modify_date IS NOT DISTINCT FROM c.modify_date AND v.spellings > 1)) AS conflicted
            FROM _cat c""").fetchall()[0]
        to_fill, files = con.execute(f"SELECT count(*), count(DISTINCT _file) FROM read_parquet({_sql(out / 'fill.parquet')})"
                                     ).fetchall()[0]
        per_column = con.execute(f"""SELECT {', '.join(f'count(*) FILTER (WHERE c."{col}" IS NULL AND f."{col}" IS NOT NULL)'
                                                       for col in FILL_COLUMNS)}
            FROM read_parquet({_sql(out / 'fill.parquet')}) f JOIN _cat c USING (comment_id)""").fetchall()[0]
    finally:
        if own:
            con.close()
    receipt = {"snapshot": snapshot.__dict__, "scope": scope or {},
               **dict(zip(("catalog_rows", "unread", "other_version_only", "conflicted"), counts)),
               "rows_to_fill": to_fill, "files": files, "cells_by_column": dict(zip(FILL_COLUMNS, per_column)),
               "prepared_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (out / "prepare.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def _batches(fill: Path, batch_rows: int, *, by_file: bool) -> list[list[str]]:
    """Whole data files packed up to ``batch_rows``; or, for a small scoped pilot, one batch of everything."""
    with duckdb.connect() as con:
        files = con.execute(f"SELECT _file, count(*) FROM read_parquet({_sql(fill)}) GROUP BY 1 ORDER BY 1").fetchall()
    if not by_file:
        return [[file for file, _ in files]] if files else []
    batches: list[list[str]] = []
    size = 0
    for file, rows in files:
        if not batches or size + rows > batch_rows:
            batches.append([])
            size = 0
        batches[-1].append(file)
        size += rows
    return batches


def write(workdir: Path, *, batch_rows: int = BATCH_ROWS, by_file: bool = True, con=None) -> dict:
    """Apply ``fill/fill.parquet`` under the catalog lock, one data-file batch per transaction; verified and resumable.

    Big O: the table is unpartitioned and its files mix agencies, and a MERGE rewrites each matched row whole
    (merge-on-read), so its scan fetches every column of every row group holding a matched row. A batch of whole
    data files, matched on ``filename`` as well as the key, touches only its own files, so each file is read a fixed
    number of times over the run (once to capture its batch rows, once by the MERGE, and the new rows once to verify):
    O(table). An agency per batch would read every row group once per agency it holds: O(agencies x table).

    Each batch, in one transaction: refuse unless the catalog is at the snapshot this fill last saw (no other writer);
    capture the batch rows; MERGE, updating a row only where its ``comment_id``, ``modify_date`` and data file match
    and a fill column is NULL, to ``COALESCE(catalog, read)``; check the count. After commit, the new rows must equal
    the captured rows with the fill applied, every column compared. A journal line per batch makes a rerun skip
    verified batches; an unverified committed batch MERGEs nothing again (its NULLs are filled) and is re-verified.
    """
    from os import getenv

    if not getenv(iceberg.CATALOG_LOCK_ENV):
        raise RuntimeError(f"write holds the comments catalog: set {iceberg.CATALOG_LOCK_ENV} only while holding the "
                           "lock (docs/comment-fields-fill.md)")
    out = _fill_dir(workdir)
    fill = out / "fill.parquet"
    prepared = json.loads((out / "prepare.json").read_text())
    journal = out / "write-journal.jsonl"
    done = {tuple(line["files"]) for line in map(json.loads, journal.read_text().splitlines()) if line.get("verified")
            } if journal.exists() else set()
    lines = [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
    expected = lines[-1]["snapshot_after"] if lines else prepared["snapshot"]
    own = con is None
    con = con or iceberg._connect_for_table(COMMENT)
    table = iceberg._qualified(COMMENT)
    columns = list(COMMENT.schema)
    col_list = ", ".join(f'"{c}"' for c in columns)
    sets = ", ".join(f'"{c}" = COALESCE(t."{c}", s."{c}")' for c in FILL_COLUMNS)
    need = " OR ".join(f'(t."{c}" IS NULL AND s."{c}" IS NOT NULL)' for c in FILL_COLUMNS)
    totals = {"batches": 0, "rows_changed": 0, "skipped_verified": 0}
    try:
        for files in _batches(fill, batch_rows, by_file=by_file):
            if tuple(files) in done:
                totals["skipped_verified"] += 1
                continue
            current = iceberg._read_snapshot(con, COMMENT)
            if current.__dict__ != expected:
                raise RuntimeError(f"the comments catalog moved from {expected} to {current.__dict__}: another writer; "
                                   "stop and prepare again under the lock")
            file_list = ", ".join(map(_sql, files))
            con.execute(f"""CREATE OR REPLACE TEMP TABLE _batch AS SELECT * EXCLUDE (agency_code, docket_id)
                            FROM read_parquet({_sql(fill)}) WHERE _file IN ({file_list})""")
            on_file = f" AND t.{FILE_COLUMN} = s._file" if by_file else ""
            in_files = f"{FILE_COLUMN} IN ({file_list}) AND " if by_file else ""
            con.execute(f"""CREATE OR REPLACE TEMP TABLE _prior AS SELECT {col_list} FROM {table}
                            WHERE {in_files}comment_id IN (SELECT comment_id FROM _batch)""")
            planned = con.execute("SELECT count(*) FROM _batch").fetchone()[0]
            con.execute("BEGIN")
            try:
                changed = con.execute(f"""MERGE INTO {table} t USING _batch s
                    ON t.comment_id = s.comment_id AND t.modify_date IS NOT DISTINCT FROM s.modify_date{on_file}
                    WHEN MATCHED AND ({need}) THEN UPDATE SET {sets}""").fetchone()[0]
                if changed > planned:
                    raise RuntimeError(f"MERGE changed {changed} rows for {planned} planned")
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            after = iceberg._read_snapshot(con, COMMENT) if changed else current
            verified = _verify(con, table, columns, _added_files(con, after) if changed else [])
            record = {"files": files, "planned": planned, "changed": changed, "snapshot_before": current.__dict__,
                      "snapshot_after": after.__dict__, "verified": verified,
                      "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            with journal.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
            if not verified:
                raise RuntimeError(f"batch {files[:1]}...: the written rows differ from the captured rows with the fill "
                                   f"applied; the prior snapshot is {current.__dict__}")
            expected = after.__dict__
            totals["batches"] += 1
            totals["rows_changed"] += changed
    finally:
        if own:
            con.close()
    return totals


def _added_files(con, snapshot) -> list[str] | None:
    """The data files ``snapshot`` added, to verify by reading only them; None where the catalog cannot say."""
    try:
        rows = con.execute(f"""SELECT file_path FROM iceberg_metadata('{iceberg._CATALOG_ALIAS}.{iceberg._namespace()}.comments')
            WHERE status = 'ADDED' AND content = 'EXISTING' AND manifest_sequence_number =
                  (SELECT max(manifest_sequence_number) FROM iceberg_metadata('{iceberg._CATALOG_ALIAS}.{iceberg._namespace()}.comments'))
        """).fetchall()
    except duckdb.Error:
        return None
    return [row[0] for row in rows]


def _verify(con, table: str, columns: list[str], added: list[str] | None) -> bool:
    """Every batch row now reads as its captured row with the fill applied, compared on every column."""
    expected = ", ".join(
        f'COALESCE(p."{c}", b."{c}") AS "{c}"' if c in FILL_COLUMNS else f'p."{c}"' for c in columns)
    within = f"{FILE_COLUMN} IN ({', '.join(map(_sql, added))})" if added else "TRUE"
    mismatched = con.execute(f"""
        WITH want AS (SELECT {expected} FROM _prior p JOIN _batch b USING (comment_id)),
             got AS (SELECT {', '.join(f'"{c}"' for c in columns)} FROM {table}
                     WHERE {within} AND comment_id IN (SELECT comment_id FROM _batch))
        SELECT count(*) FROM ((SELECT * FROM want EXCEPT ALL SELECT * FROM got)
                              UNION ALL (SELECT * FROM got EXCEPT ALL SELECT * FROM want))""").fetchone()[0]
    return mismatched == 0

app = App(name="fill-comment-fields", help=__doc__)


@app.command(name="plan")
def plan_command(*, workdir: Path, manifest: Path, chunk_keys: int = CHUNK_KEYS) -> None:
    """Add a manifest's unplanned comment keys as new chunks."""
    plan(workdir, manifest, chunk_keys=chunk_keys)


@app.command(name="read")
def read_command(
    *, workdir: Path, workers: int = 64, shard: int | None = None, shards: int | None = None,
    max_rss_mb: float = 2048, min_free_gb: float = 4,
) -> None:
    """Read the unread chunks, or one ``--shard`` of ``--shards`` (chunks dealt by CRC-32)."""
    if (shard is None) != (shards is None):
        raise ValueError("--shard and --shards go together")
    pair = (shard, shards) if shard is not None and shards is not None else None
    if pair is not None and not 0 <= pair[0] < pair[1]:
        raise ValueError(f"shard {pair[0]} is outside 0..{pair[1] - 1}")
    name = f"progress-{shard}.json" if shard is not None else "progress.json"
    totals = read(workdir, workers=workers, max_rss_mb=max_rss_mb, min_free_gb=min_free_gb, shard=pair,
                  progress_name=name)
    print(json.dumps(totals))
    if totals["deferred_chunks"] or totals["stopped"]:
        raise SystemExit(3)


@app.command(name="prepare")
def prepare_command(*, workdir: Path, docket: str | None = None, agency: str | None = None) -> None:
    """Compute the fill at the catalog's current snapshot (no write); ``--docket``/``--agency`` scope a pilot."""
    from dotenv import load_dotenv

    load_dotenv()
    scope = {k: v for k, v in (("docket_id", docket), ("agency_code", agency)) if v}
    print(json.dumps(prepare(workdir, scope=scope or None), indent=2))


@app.command(name="write")
def write_command(*, workdir: Path, batch_rows: int = BATCH_ROWS, by_file: bool = True) -> None:
    """Apply the prepared fill under the catalog lock; ``--no-by-file`` for a small scoped pilot."""
    from dotenv import load_dotenv

    load_dotenv()
    print(json.dumps(write(workdir, batch_rows=batch_rows, by_file=by_file), indent=2))


@app.command(name="status")
def status_command(*, workdir: Path) -> None:
    """Planned and read chunks and keys."""
    print(json.dumps(status(workdir), indent=2))


if __name__ == "__main__":
    app()
