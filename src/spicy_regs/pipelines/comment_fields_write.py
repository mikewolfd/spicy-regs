"""Write phase of the comment re-read: fill only the catalog cells a row predates, verified before each commit.

:func:`prepare` reads the catalog narrowly at one snapshot and writes ``fill/fill.parquet``, the rows to fill, with a
counts-only ``fill/prepare.json`` naming it by digest. :func:`write` applies it under the catalog lock, one batch of
whole data files per transaction, and checks every batch inside its transaction before COMMIT, so a wrong write is
rolled back rather than committed. :func:`undo` restores a committed batch from its pre-image as a new, checked
commit. docs/comment-fields-fill.md is the runbook.

**The rules.** A read row fills a catalog row only at the same ``comment_id`` and ``modify_date`` (a copy of another
version is counted and skipped), only into a column the catalog holds as NULL, and only for a column on which every
copy read for that version agrees; copies that disagree on a column are listed in ``fill/conflicts.parquet`` and fill
nothing in that column. A stated 0 is a value; NULL stays NULL.

**Big O.** The table is unpartitioned and R2's compacted files each span most agencies, and DuckDB writes Iceberg
merge-on-read: a MERGE rewrites nothing in place but writes each changed row anew with a positional delete. A batch
per agency would fetch every row group once per agency in it, O(agencies x table). A batch of whole data files,
matched on ``filename`` as well as the key, fetches the full rows of only its own files. Every scan still opens every
data file (a ``filename`` filter prunes no file, 2026-09-28 review), so each batch also reads the narrow key and file
columns of the whole table a fixed number of times: the pre-image, the MERGE, the read-back and one count. That is
O(table) per batch, O(batches x table) in all, with the batch count bounded by packing whole files up to
:data:`BATCH_BYTES`.

**Each batch, in one transaction.** Refuse unless the catalog is at the snapshot this fill last committed; capture the
batch's rows and write that pre-image to disk; journal the batch as pending; MERGE; require the MERGE's count to equal
the planned count; then read the batch's rows back, in the transaction, from its old files and the files the MERGE
wrote, and require them to equal the pre-image with the fill applied on every column (a duplicate, a lost row, a
changed non-fill cell or an overwrite fails); require the new files to hold only batch rows; and require the table to
hold the rows it held at prepare (a row lost anywhere fails). Any failure rolls back and journals the batch as
failed. After COMMIT, the catalog must be at the one child of the checked snapshot, and its summary must add exactly
the changed records and position deletes; otherwise the batch is journaled failed with ``committed: true``, which
stops every later prepare and write, in any journal, until :func:`undo` restores it or an operator clears it.

**Resume.** Each prepare gets its own id (snapshot, fill digest and a nonce) and journal, which records files, so a
changed batch budget never redoes passed work and a fresh prepare never inherits a stale journal. A run that finds its
own pending batch committed (the parent and the summary match) verifies it against the stored pre-image and journals
it; any other commit (an ETL run, R2's compaction) stops the run, and preparing again finds only the cells still NULL.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from os import getenv
from pathlib import Path

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg

#: The columns a fill writes: the nullable columns added after the catalog table was created, and attachments_json.
FILL_COLUMNS = (
    "comment_on_document_id", "comment_on_object_id", "original_document_id", "comment_reference_values_json",
    "subtype", "duplicate_comments", "attachments_json",
)
#: What :func:`prepare` reads of each read copy: the object key naming the copy, the version, and the fill values.
#: The staged fill input (runbook) holds exactly these.
READ_COLUMNS = ("key", "comment_id", "modify_date", *FILL_COLUMNS)
#: The Iceberg scan's virtual column naming each row's data file.
FILE_COLUMN = "filename"
#: Compressed bytes of catalog data files per batch; a larger file is still one batch.
BATCH_BYTES = 512 * 2**20
#: The write connection's DuckDB budget and spill directory (``fill/spill``). A whole-table scan with eight threads ran
#: out of memory at 4.6 GiB on 2026-09-28; one batch is at most about 512 MiB of compressed files.
WRITE_RESOURCES = ExportResources(memory="6GB", threads=4)


class FillVerificationError(RuntimeError):
    """A batch's rows, read back inside its transaction, differ from its pre-image with the fill applied."""


def _sql(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def fill_dir(workdir: Path) -> Path:
    return workdir / "fill"


def _row_md5(values: dict[str, str]) -> str:
    """One row's digest over every column (``values`` maps each to its expression), NULL and text told apart."""
    return "md5(to_json(struct_pack(" + ", ".join(f'"{c}" := {e}' for c, e in values.items()) + ")))"


def _columns_md5(columns: list[str]) -> str:
    return _row_md5({c: f'"{c}"' for c in columns})


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


# --------------------------------------------------------------------------- #
# Catalog metadata. Each is a seam the hermetic tests replace; the local-Iceberg test runs them for real.
# --------------------------------------------------------------------------- #
def _table_metadata(con) -> dict:
    raw = con.execute("SELECT metadata FROM iceberg_load_table_response(?)", [
        f"{iceberg._CATALOG_ALIAS}.{iceberg._selected_catalog_namespace(con, COMMENT)}.{COMMENT.name}"]).fetchone()[0]
    return json.loads(raw) if isinstance(raw, str) else raw


def commit_record(con, parent_id: int) -> dict | None:
    """The commit made on top of ``parent_id`` (its id, schema and summary), found in the table's history.

    Found by parent rather than read as the current snapshot, so a writer that commits right after this one does not
    hide it; None when no snapshot has that parent.
    """
    metadata = _table_metadata(con)
    children = [s for s in metadata["snapshots"] if s.get("parent-snapshot-id") == parent_id]
    if len(children) != 1:
        return None
    child = children[0]
    return {"table_uuid": metadata["table-uuid"], "snapshot_id": child["snapshot-id"], "schema_id": child["schema-id"],
            "summary": child.get("summary", {})}


def storage_access(con) -> None:
    """Let the connection read data-file footers on R2 (the catalog attach alone vends no S3 secret)."""
    if all(getenv(name) for name in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")):
        iceberg.create_s3_secret(con)


def live_files(con) -> set[str]:
    """Every live data file of the current snapshot, from the table's manifests (no data file is opened).

    Spelled exactly as the scan's ``filename`` (live, 2026-09-29: the same 43 strings byte for byte).
    """
    table = f"{iceberg._CATALOG_ALIAS}.{iceberg._selected_catalog_namespace(con, COMMENT)}.{COMMENT.name}"
    return {row[0] for row in con.execute(
        f"SELECT file_path FROM iceberg_metadata('{table}') WHERE manifest_content = 'DATA' AND status <> 'DELETED'"
    ).fetchall()}


def data_files(con) -> dict[str, int]:
    """Every live data file with its compressed bytes, from its Parquet footer (one footer read per file)."""
    paths = sorted(live_files(con))
    if not paths:
        return {}
    listed = ", ".join(map(_sql, paths))
    return dict(con.execute(
        f"SELECT file_name, sum(total_compressed_size) FROM parquet_metadata([{listed}]) GROUP BY 1").fetchall())


# --------------------------------------------------------------------------- #
# Journals
# --------------------------------------------------------------------------- #
def _journal(workdir: Path, prepared: dict) -> Path:
    return fill_dir(workdir) / f"write-journal-{prepared['prepare_id']}.jsonl"


def _lines(journal: Path) -> list[dict]:
    return [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []


def _append(path: Path, line: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(line, default=str) + "\n")


def _unresolved_failures(workdir: Path) -> list[tuple[Path, dict]]:
    """Every ``failed`` line, in any prepare's journal, that no later ``cleared`` or ``undone`` line for its files
    resolves."""
    found = []
    for journal in sorted(fill_dir(workdir).glob("write-journal-*.jsonl")):
        lines = _lines(journal)
        for index, line in enumerate(lines):
            if line["state"] == "failed" and not any(
                    later["state"] in ("cleared", "undone") and later["files"] == line["files"]
                    for later in lines[index + 1:]):
                found.append((journal, line))
    return found


def _bucket() -> str:
    return getenv("R2_BUCKET_NAME", "spicy-regs")


def fetch_staged(workdir: Path, key: str, sha256: str, *, client=None) -> Path:
    """Download the staged fill input (runbook) to ``workdir/reads.parquet``; refuse any bytes but ``sha256``'s."""
    from spicy_regs.sources import r2

    path = workdir / "reads.parquet"
    workdir.mkdir(parents=True, exist_ok=True)
    (client or r2.get_r2_client()).download_file(_bucket(), key, str(path))
    if _sha256(path) != sha256:
        path.unlink()
        raise RuntimeError(f"{key} is not the staged read with sha256 {sha256}")
    return path


def sync_journals(workdir: Path, prefix: str, *, push: bool, client=None) -> list[str]:
    """Pull every prepare's journal from ``prefix`` in the data bucket, or push this workdir's.

    A runner starts each run with an empty workdir; kept beside the staged read, the journals let a later run see
    an earlier run's committed failure (and its undo), which :func:`prepare` and :func:`write` refuse to pass. Runs
    hold the catalog lock one at a time, so the last push is the whole story.
    """
    from spicy_regs.sources import r2

    client = client or r2.get_r2_client()
    out = fill_dir(workdir)
    out.mkdir(parents=True, exist_ok=True)
    moved = []
    if push:
        for path in sorted(out.glob("write-journal-*.jsonl")):
            client.upload_file(str(path), _bucket(), f"{prefix}/{path.name}")
            moved.append(path.name)
        return moved
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=f"{prefix}/"):
        for item in page.get("Contents", []):
            name = item["Key"].rsplit("/", 1)[-1]
            if name.startswith("write-journal-") and name.endswith(".jsonl"):
                client.download_file(_bucket(), item["Key"], str(out / name))
                moved.append(name)
    return moved


def _refuse_committed_failures(workdir: Path) -> None:
    committed = [(journal, line) for journal, line in _unresolved_failures(workdir) if line.get("committed")]
    if committed:
        journal, line = committed[0]
        raise RuntimeError(
            f"batch {line.get('batch')} ({journal.name}) committed and then failed verification: {line['reason']}. "
            "Run `fill-comment-fields undo` for it (runbook), or restore it by hand and rerun write with "
            "--clear-failure, before preparing or writing again")


# --------------------------------------------------------------------------- #
# Prepare
# --------------------------------------------------------------------------- #
def prepare(workdir: Path, *, scope: dict[str, str] | None = None, reads: Path | None = None, con=None) -> dict:
    """Compute the fill at the catalog's current snapshot; return and write the counts-only receipt.

    ``reads`` is the staged fill input (:data:`READ_COLUMNS`); by default the read parts under ``workdir/parts``.
    Refuses a snapshot that lacks a fill column: the columns arrive with the deployed branch's first data commit
    (runbook), never from this tool. Refuses while any batch committed and failed its check. Touches no catalog row
    and needs no lock.
    """
    _refuse_committed_failures(workdir)
    own = con is None
    con = con or iceberg._connect()
    out = fill_dir(workdir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        WRITE_RESOURCES.configure(con, out / "spill")
        storage_access(con)
        namespace = iceberg._selected_catalog_namespace(con, COMMENT)
        snapshot = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        source = iceberg._snapshot_query(COMMENT, snapshot, namespace=namespace)
        from spicy_regs.sources.regulatory_catalog import processing_table
        processing = processing_table(con, COMMENT)
        if iceberg._read_snapshot(con, COMMENT) != snapshot:
            raise RuntimeError('Catalog changed while preparing field fill; retry')
        missing = [c for c in FILL_COLUMNS if c not in con.sql(f'SELECT * FROM {processing}').columns]
        if missing:
            raise RuntimeError(f"the comments catalog lacks {missing} at snapshot {snapshot.snapshot_id}: deploy the "
                               "branch and let one ETL data commit migrate the table before preparing "
                               "(docs/comment-fields-fill.md)")
        table_rows = con.execute(f"SELECT count(*) FROM ({source})").fetchone()[0]
        where = " AND ".join(f'"{column}" = {_sql(value)}' for column, value in (scope or {}).items()) or "TRUE"
        cols = ", ".join(f'"{c}"' for c in FILL_COLUMNS)
        physical = source.replace("SELECT *", f"SELECT comment_id, {FILE_COLUMN} AS _file", 1)
        with_file = f'SELECT p.*, f._file FROM {processing} p JOIN ({physical}) f USING (comment_id)'
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _cat AS SELECT comment_id, agency_code, docket_id, modify_date,
                        {cols}, _file FROM ({with_file}) WHERE {where}""")
        con.execute(f"DROP TABLE {processing}")
        duplicated = con.execute("SELECT count(*) FROM (SELECT comment_id FROM _cat GROUP BY 1 HAVING count(*) > 1)"
                                 ).fetchone()[0]
        if duplicated:
            raise RuntimeError(f"{duplicated} comment ids appear more than once in the catalog; run the dedupe first")
        read_from = reads or workdir / "parts" / "*" / "*.parquet"
        parts = f"read_parquet({_sql(read_from)}, hive_partitioning=false)"
        values = ", ".join(f'CAST(s."{c}" AS VARCHAR) AS "{c}"' for c in FILL_COLUMNS)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _read AS SELECT s.key, s.comment_id, s.modify_date, {values}
                        FROM {parts} s SEMI JOIN _cat USING (comment_id)""")
        # Per column: how many spellings the copies of one version state (NULL counted as one), and the value. All
        # but 842 of the re-read's 26.6M versions were read once and state one spelling each, so only versions read
        # more than once are aggregated: a DISTINCT count over every version ran out of 6 GB (2026-09-29).
        same = "m.comment_id = r.comment_id AND m.modify_date IS NOT DISTINCT FROM r.modify_date"
        con.execute("""CREATE OR REPLACE TEMP TABLE _multi AS SELECT comment_id, modify_date FROM _read
                       GROUP BY comment_id, modify_date HAVING count(*) > 1""")
        once = ", ".join(f'1 AS "n_{c}", "{c}" AS "v_{c}"' for c in FILL_COLUMNS)
        spell = ", ".join(
            f"""count(DISTINCT CASE WHEN "{c}" IS NULL THEN 'n' ELSE 'v' || "{c}" END) AS "n_{c}", """
            f"""any_value("{c}") AS "v_{c}\""""
            for c in FILL_COLUMNS)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _versions AS
            SELECT comment_id, modify_date, [key] AS keys, {once} FROM _read r
            WHERE NOT EXISTS (SELECT 1 FROM _multi m WHERE {same})
            UNION ALL
            SELECT comment_id, modify_date, list(key ORDER BY key) AS keys, {spell} FROM _read r
            WHERE EXISTS (SELECT 1 FROM _multi m WHERE {same}) GROUP BY comment_id, modify_date""")
        conflicts = " UNION ALL ".join(
            f"""SELECT comment_id, modify_date, '{c}' AS "column", keys FROM _versions WHERE "n_{c}" > 1"""
            for c in FILL_COLUMNS)
        con.execute(f"COPY (SELECT * FROM ({conflicts}) ORDER BY 1, 3) TO {_sql(out / 'conflicts.parquet')} "
                    "(FORMAT PARQUET)")
        fills = {c: f"""(c."{c}" IS NULL AND v."n_{c}" = 1 AND v."v_{c}" IS NOT NULL)""" for c in FILL_COLUMNS}
        # Only the value to fill, NULL elsewhere: a write that overwrote instead of filling would blank a cell.
        filled = ", ".join(f'CASE WHEN {fills[c]} THEN v."v_{c}" END AS "{c}"' for c in FILL_COLUMNS)
        con.execute(f"""COPY (
            SELECT c.comment_id, c.modify_date, c._file, {filled}
            FROM _cat c JOIN _versions v ON v.comment_id = c.comment_id AND v.modify_date IS NOT DISTINCT FROM c.modify_date
            WHERE {' OR '.join(fills.values())}
            ORDER BY c._file, c.comment_id
        ) TO {_sql(out / 'fill.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)""")
        counts = con.execute("""
            SELECT count(*),
                   count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM _read r WHERE r.comment_id = c.comment_id)),
                   count(*) FILTER (WHERE EXISTS (SELECT 1 FROM _read r WHERE r.comment_id = c.comment_id)
                                    AND NOT EXISTS (SELECT 1 FROM _versions v WHERE v.comment_id = c.comment_id
                                                    AND v.modify_date IS NOT DISTINCT FROM c.modify_date))
            FROM _cat c""").fetchall()[0]
        cells = dict(zip(FILL_COLUMNS, con.execute(
            f"SELECT {', '.join(f'count(*) FILTER (WHERE {fills[c]})' for c in FILL_COLUMNS)} "
            "FROM _cat c JOIN _versions v ON v.comment_id = c.comment_id "
            "AND v.modify_date IS NOT DISTINCT FROM c.modify_date").fetchall()[0]))
        conflicted = dict(zip(FILL_COLUMNS, con.execute(
            f"SELECT {', '.join(f'count(*) FILTER (WHERE n_{c} > 1)' for c in FILL_COLUMNS)} FROM _versions"
        ).fetchall()[0]))
        to_fill = con.execute(f"SELECT count(*) FROM read_parquet({_sql(out / 'fill.parquet')})").fetchone()[0]
        sizes = data_files(con)
        con.execute(f"""COPY (SELECT _file AS file, count(*) AS rows FROM read_parquet({_sql(out / 'fill.parquet')})
                        GROUP BY 1 ORDER BY 1) TO {_sql(out / 'files.parquet')} (FORMAT PARQUET)""")
        files = con.execute(f"SELECT file, rows FROM read_parquet({_sql(out / 'files.parquet')})").fetchall()
        if iceberg._read_snapshot(con, COMMENT) != snapshot:
            raise RuntimeError('Catalog changed while preparing field fill; retry')
    finally:
        if own:
            con.close()
    unsized = [file for file, _ in files if sizes.get(file) is None]
    if unsized:
        raise RuntimeError(f"no size read for {len(unsized)} data files (first {unsized[0]}); the batches pack by "
                           "size, so prepare again")
    fill_sha256 = _sha256(out / "fill.parquet")
    receipt = {
        "prepare_id": f"{snapshot.snapshot_id}-{fill_sha256[:12]}-{secrets.token_hex(4)}", "fill_sha256": fill_sha256,
        "snapshot": snapshot.__dict__, "scope": scope or {}, "reads": str(read_from),
        "table_rows": table_rows, "catalog_rows": counts[0], "unread": counts[1], "other_version_only": counts[2],
        "rows_to_fill": to_fill, "cells_by_column": cells, "conflicted_versions_by_column": conflicted,
        "files": {file: {"rows": rows, "bytes": sizes[file]} for file, rows in files},
        "prepared_at": _now(),
    }
    (out / "prepare.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


# --------------------------------------------------------------------------- #
# Write
# --------------------------------------------------------------------------- #
def batches(files: dict[str, dict], batch_bytes: int, *, by_file: bool) -> list[list[str]]:
    """Whole data files packed up to ``batch_bytes`` compressed; or, for a scoped pilot, one batch of everything."""
    names = sorted(files)
    if not by_file:
        return [names] if names else []
    packed: list[list[str]] = []
    size = 0
    for name in names:
        weight = files[name]["bytes"]
        if weight is None:
            raise RuntimeError(f"no size known for {name}; prepare again")
        if not packed or size + weight > batch_bytes:
            packed.append([])
            size = 0
        packed[-1].append(name)
        size += weight
    return packed


def write(
    workdir: Path, *, batch_bytes: int = BATCH_BYTES, by_file: bool = True, clear_failure: bool = False,
    resources: ExportResources | None = None, con=None,
) -> dict:
    """Apply the prepared fill under the catalog lock; see the module docstring. Never migrates the table."""
    if not getenv(iceberg.CATALOG_LOCK_ENV):
        raise RuntimeError(f"write holds the comments catalog: set {iceberg.CATALOG_LOCK_ENV} only while holding the "
                           "lock (docs/comment-fields-fill.md)")
    out = fill_dir(workdir)
    prepared = json.loads((out / "prepare.json").read_text())
    if _sha256(out / "fill.parquet") != prepared["fill_sha256"]:
        raise RuntimeError("fill.parquet is not the one prepare.json names; prepare again")
    if not by_file and not prepared["scope"]:
        raise RuntimeError("--no-by-file is for a scoped pilot: prepare with --docket or --agency first")
    journal = _journal(workdir, prepared)
    blocking = [(path, line) for path, line in _unresolved_failures(workdir)
                if path == journal or line.get("committed")]
    if blocking and not clear_failure:
        _refuse_committed_failures(workdir)
        raise RuntimeError(f"a batch failed verification ({journal.name}); nothing of it committed. Rerun with "
                           "--clear-failure once the cause is understood")
    for path, line in blocking:
        _append(path, {"state": "cleared", "files": line["files"], "batch": line.get("batch"),
                       "prepare_id": prepared["prepare_id"], "at": _now()})
    lines = _lines(journal)
    verified = [line for line in lines if line["state"] == "verified"]
    done = {file for line in verified for file in line["files"]}
    expected = verified[-1]["snapshot_after"] if verified else prepared["snapshot"]
    own = con is None
    con = con or iceberg._connect()  # never _connect_for_table: a migration here would move schema_id mid-run
    resources = resources or WRITE_RESOURCES
    resources.configure(con, out / "spill")
    storage_access(con)
    table = iceberg._qualified(COMMENT)
    from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS
    missing = [c for c in FILL_COLUMNS if c not in dict(SOURCE_COLUMNS[COMMENT.name])]
    if missing:
        raise RuntimeError(f"the comments catalog lacks {missing}; the fill never migrates it")
    totals = {"batches": 0, "rows_changed": 0, "files_skipped_verified": len(done)}
    try:
        pending = lines[-1] if lines and lines[-1]["state"] == "pending" else None
        if pending is not None:
            expected = _recover(con, journal, pending, prepared, table, expected)
            done |= set(pending["files"]) if expected != pending["snapshot_before"] else set()
        todo = {file: meta for file, meta in prepared["files"].items() if file not in done}
        for files in batches(todo, batch_bytes, by_file=by_file):
            after, changed = _write_batch(con, workdir, journal, prepared, table, files, expected, by_file)
            expected = after
            totals["batches"] += 1
            totals["rows_changed"] += changed
    finally:
        if own:
            con.close()
    return totals


def _batch_id(files: list[str]) -> str:
    return hashlib.sha256("\n".join(files).encode()).hexdigest()[:16]


def _write_batch(con, workdir, journal, prepared, table, files, expected, by_file) -> tuple[dict, int]:
    columns = list(COMMENT.schema)
    col_list = ", ".join(f'"{c}"' for c in columns)
    file_list = ", ".join(map(_sql, files))
    batch = _batch_id(files)
    preimage = fill_dir(workdir) / "preimage" / f"{prepared['prepare_id']}-{batch}.parquet"
    postimage = preimage.with_name(f"{preimage.stem}-written.parquet")
    preimage.parent.mkdir(parents=True, exist_ok=True)
    fill = fill_dir(workdir) / "fill.parquet"
    in_files = f"{FILE_COLUMN} IN ({file_list}) AND " if by_file else ""
    ids = {"batch": batch, "files": files, "prepare_id": prepared["prepare_id"]}
    # BEGIN first: the transaction reads one snapshot from here on and its COMMIT is refused (409) if the table moved
    # since, so the checked snapshot, the pre-image and the MERGE all see the same table.
    con.execute("BEGIN")
    try:
        current = iceberg._read_snapshot(con, COMMENT)
        if current.__dict__ != expected:
            raise RuntimeError(f"the comments catalog moved from {expected} to {current.__dict__}: another writer or "
                               "R2 compaction committed; prepare again under the lock")
        before_files = live_files(con) if by_file else None
        con.execute(f"CREATE OR REPLACE TEMP TABLE _batch AS SELECT * FROM read_parquet({_sql(fill)}) "
                    f"WHERE _file IN ({file_list})")
        from spicy_regs.sources import regulatory_catalog as native
        selected_ids = f'SELECT comment_id FROM {table} WHERE {in_files}comment_id IN (SELECT comment_id FROM _batch)'
        processing = native.processing_table(con, COMMENT, where=f'comment_id IN ({selected_ids})', in_transaction=True)
        con.execute(f"COPY (SELECT {col_list} FROM {processing}) TO {_sql(preimage)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.execute(f"DROP TABLE {processing}")
        con.execute(f"CREATE OR REPLACE TEMP VIEW _prior AS SELECT * FROM read_parquet({_sql(preimage)})")
        kept = {"preimage": str(preimage), "preimage_sha256": _sha256(preimage)}
        need = " OR ".join(f'(p."{c}" IS NULL AND b."{c}" IS NOT NULL)' for c in FILL_COLUMNS)
        planned = con.execute(f"""SELECT count(*) FROM _prior p JOIN _batch b
            ON p.comment_id = b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date WHERE {need}""").fetchone()[0]
        _append(journal, {"state": "pending", **ids, "planned": planned, "snapshot_before": current.__dict__, **kept,
                          "before_files": sorted(before_files or ()), "by_file": by_file})
        changed = 0
        if planned:
            values = ', '.join(f'COALESCE(p."{c}", CAST(b."{c}" AS {COMMENT.sql_type(c)})) AS "{c}"' if c in FILL_COLUMNS else f'p."{c}"'
                               for c in columns)
            con.execute(f'CREATE OR REPLACE TEMP TABLE _filled AS SELECT {values} FROM _prior p JOIN _batch b '
                        f'ON p.comment_id=b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date WHERE {need}')
            if con.execute('SELECT count(*) FROM _filled').fetchone()[0] != planned:
                raise FillVerificationError('Replacement rows differ from planned count')
            try:
                native.replace_native(con, COMMENT, '_filled', expected_prior='_prior', in_transaction=True)
            except (ValueError, RuntimeError) as error:
                raise FillVerificationError(f'Native subject and receipt differ: {error}') from error
            changed = planned
            _check(con, table, columns, files, before_files, by_file, prepared["table_rows"], postimage=postimage)
            kept |= {"postimage": str(postimage), "postimage_sha256": _sha256(postimage)}
        con.execute("COMMIT")
    except Exception as error:
        try:
            con.execute("ROLLBACK")
        except Exception:  # a failed COMMIT can already have aborted the transaction; keep the original error
            pass
        from spicy_regs.sources.regulatory_catalog import retain_refusal
        retain_refusal(con, error)
        postimage.unlink(missing_ok=True)  # it describes rows that were never committed
        if isinstance(error, FillVerificationError):
            _append(journal, {"state": "failed", **ids, "committed": False, "reason": str(error)[:500],
                              "snapshot_before": expected})
        raise
    if not changed:
        _append(journal, {"state": "verified", **ids, "planned": 0, "changed": 0, "snapshot_before": expected,
                          "snapshot_after": expected})
        return expected, 0
    after, problem = _our_commit(con, current.__dict__, changed)
    if problem:
        _append(journal, {"state": "failed", **ids, "committed": True, "reason": problem, **kept,
                          "snapshot_before": current.__dict__, "snapshot_after": after})
        raise FillVerificationError(f"batch {batch} committed but {problem}; undo it from its pre-image (runbook)")
    _append(journal, {"state": "verified", **ids, "planned": planned, "changed": changed, **kept,
                      "snapshot_before": current.__dict__, "snapshot_after": after, "at": _now()})
    return after, changed


def _check(con, table: str, columns: list[str], files: list[str], before_files: set[str] | None, by_file: bool,
           table_rows: int, *, postimage: Path, at: int | None = None) -> None:
    """Inside the transaction: the batch's rows equal the pre-image with the fill applied, new files hold nothing
    else, and the table holds the ``table_rows`` it held at prepare.

    Rows are read from the batch's old files (those the MERGE left) and from files that did not exist before the
    transaction (those it wrote). ``before_files`` is None for a scoped pilot, which reads the batch's keys anywhere.
    One scan counts the table and the new files' rows outside the batch. ``at`` reads the table as of that snapshot,
    for a batch checked after later commits. The rows read back are what a commit leaves; each one's key and digest
    go to ``postimage`` before any comparison, so :func:`undo` can tell them from a later write even when they are
    wrong.
    """
    if at is not None:
        table = f"(SELECT *, {FILE_COLUMN} FROM {table} AT (VERSION => {int(at)}))"
    if by_file:
        if before_files is None:
            raise FillVerificationError("the catalog's data files are unknown; refusing to check by full scan")
        old = ", ".join(map(_sql, files))
        new = ", ".join(map(_sql, sorted(before_files))) or "''"
        where = f"({FILE_COLUMN} IN ({old}) OR {FILE_COLUMN} NOT IN ({new}))"
        total, strays = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE {FILE_COLUMN} NOT IN ({new})
                                        AND comment_id NOT IN (SELECT comment_id FROM _batch)) FROM {table}""").fetchone()
        if strays:
            raise FillVerificationError(f"the MERGE wrote {strays} rows outside the batch")
    else:
        where = "TRUE"
        total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    if total != table_rows:
        raise FillVerificationError(f"the table holds {total} rows, not the {table_rows} it held at prepare")
    from spicy_regs.sources import regulatory_catalog as native
    if at is not None and iceberg._read_snapshot(con, COMMENT).snapshot_id != at:
        raise FillVerificationError('A later commit superseded the batch; qualify its current pair before recovery')
    table = native.processing_table(con, COMMENT, where='comment_id IN (SELECT comment_id FROM _prior)',
                                    in_transaction=at is None)
    where = 'TRUE'
    # Rows are compared by key and whole-row digest: a multiset of narrow pairs, not of wide rows (live, 2026-09-29:
    # the wide EXCEPT ALL took 206-301 s of a 4.5-5.5M-row batch's 260-360 s).
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _got AS SELECT comment_id, {_columns_md5(columns)} AS row_md5
                    FROM {table} WHERE {where} AND comment_id IN (SELECT comment_id FROM _prior)""")
    con.execute(f"COPY _got TO {_sql(postimage)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.execute(f"DROP TABLE {table}")
    want = {c: f'CASE WHEN p."{c}" IS NULL THEN CAST(b."{c}" AS {COMMENT.sql_type(c)}) ELSE p."{c}" END' if c in FILL_COLUMNS else f'p."{c}"'
            for c in columns}
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _want AS SELECT p.comment_id, {_row_md5(want)} AS row_md5
                    FROM _prior p LEFT JOIN _batch b
                    ON p.comment_id = b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date""")
    mismatched = con.execute("""SELECT (SELECT count(*) FROM (SELECT * FROM _want EXCEPT ALL SELECT * FROM _got))
                                     + (SELECT count(*) FROM (SELECT * FROM _got EXCEPT ALL SELECT * FROM _want))
                             """).fetchone()[0]
    if mismatched:
        raise FillVerificationError(f"{mismatched} rows differ from the pre-image with the fill applied")


def _our_commit(con, before: dict, changed: int, *, deletes: int | None = None) -> tuple[dict, str | None]:
    """The snapshot this batch committed, and why it is not exactly the batch (None when it is).

    It must be the one child of the checked snapshot, adding exactly ``changed`` records and ``deletes`` (by default
    ``changed``: every changed row is rewritten) position deletes. Found by parent, so a writer committing right after
    this one is not taken for this batch; the next batch's snapshot check stops the run for it. A catalog that records
    no commits (the hermetic tests' stand-in) is read as its current snapshot.
    """
    record = commit_record(con, before["snapshot_id"])
    if record is None:
        current = iceberg._read_snapshot(con, COMMENT).__dict__
        if current == before or _records_commits(con):
            return current, f"no single commit on top of {before['snapshot_id']}"
        return current, None
    after = {k: record[k] for k in ("table_uuid", "snapshot_id", "schema_id")}
    summary = record["summary"]
    added, deleted = int(summary.get("added-records", 0)), int(summary.get("added-position-deletes", 0))
    deletes = changed if deletes is None else deletes
    if (added, deleted) != (changed, deletes):
        return after, (f"it added {added} records and {deleted} position deletes for {changed} written rows, "
                       f"{deletes} of them replacing a row")
    return after, None


def _records_commits(con) -> bool:
    """Whether the catalog keeps a snapshot history (Iceberg does; the hermetic stand-in does not)."""
    try:
        _table_metadata(con)
        return True
    except Exception:
        return False


def _recover(con, journal: Path, pending: dict, prepared: dict, table: str, expected: dict) -> dict:
    """A run that stopped after journaling a batch as pending: find whether that batch committed, and account for it."""
    current = iceberg._read_snapshot(con, COMMENT).__dict__
    ids = {k: pending.get(k) for k in ("batch", "files", "prepare_id")}
    if current == pending["snapshot_before"]:
        _append(journal, {"state": "abandoned", **ids})
        return expected
    planned = pending["planned"]
    if commit_record(con, pending["snapshot_before"]["snapshot_id"]) is None:
        raise RuntimeError(f"the catalog moved to {current} after a pending batch, and not by that batch; "
                           "prepare again under the lock")
    after, problem = _our_commit(con, pending["snapshot_before"], planned)
    if problem:
        raise RuntimeError(f"the commit after a pending batch is not that batch ({problem}); prepare again")
    # Our commit, never checked: compare the rows it wrote with the stored pre-image and the fill.
    kept = {k: pending[k] for k in ("preimage", "preimage_sha256")}
    if _sha256(Path(kept["preimage"])) != kept["preimage_sha256"]:
        raise RuntimeError(f"the pre-image of pending batch {ids['batch']} is not the one journaled; stop (runbook)")
    con.execute(f"CREATE OR REPLACE TEMP VIEW _prior AS SELECT * FROM read_parquet({_sql(kept['preimage'])})")
    file_list = ", ".join(map(_sql, pending["files"]))
    con.execute(f"CREATE OR REPLACE TEMP TABLE _batch AS SELECT * FROM read_parquet("
                f"{_sql(fill_dir(journal.parent.parent) / 'fill.parquet')}) WHERE _file IN ({file_list})")
    postimage = Path(kept["preimage"]).with_name(f"{Path(kept['preimage']).stem}-written.parquet")
    try:
        _check(con, table, list(COMMENT.schema), pending["files"], set(pending["before_files"]), pending["by_file"],
               prepared["table_rows"], postimage=postimage, at=after["snapshot_id"])
    except FillVerificationError as error:
        kept |= {"postimage": str(postimage), "postimage_sha256": _sha256(postimage)}
        _append(journal, {"state": "failed", **ids, "committed": True, "reason": f"recovered commit: {error}", **kept,
                          "snapshot_before": pending["snapshot_before"], "snapshot_after": after})
        raise FillVerificationError(f"committed batch {ids['batch']} differs from its pre-image with the fill "
                                    "applied; undo it (runbook)") from error
    kept |= {"postimage": str(postimage), "postimage_sha256": _sha256(postimage)}
    _append(journal, {"state": "verified", **ids, "planned": planned, "changed": planned, **kept,
                      "snapshot_before": pending["snapshot_before"], "snapshot_after": after, "recovered": True})
    return after


# --------------------------------------------------------------------------- #
# Undo
# --------------------------------------------------------------------------- #
def _committed_batch(workdir: Path, batch: str) -> tuple[Path, dict]:
    """The journal and the line recording ``batch``'s commit (verified, or failed after COMMIT), if not undone."""
    found = None
    for journal in sorted(fill_dir(workdir).glob("write-journal-*.jsonl")):
        for line in _lines(journal):
            if line.get("batch") != batch:
                continue
            if line["state"] == "undone":
                raise RuntimeError(f"batch {batch} is already undone ({journal.name})")
            if line["state"] == "verified" and line.get("changed") or line["state"] == "failed" and line.get(
                    "committed"):
                found = (journal, line)
    if found is None:
        raise RuntimeError(f"no committed batch {batch} in {fill_dir(workdir)}'s journals")
    return found


def undo(workdir: Path, batch: str, *, expected_snapshot: int, con=None) -> dict:
    """Restore a committed batch's rows from its pre-image, as a new commit checked like any catalog replacement.

    Moving the table's ref back instead leaves DuckDB reading the newer snapshot and refusing its next write (409), so
    the undo writes forward, through :func:`iceberg.replace_rows`. Every row the batch's commit wrote (its post-image
    digests, taken by the check) must still be in the table as written; a row changed since (an ETL write, an edit)
    refuses the whole undo, for a hand repair. Only the rows the commit changed are rewritten, whole, damaged or not;
    a row the commit lost is inserted again. ``expected_snapshot`` is the snapshot the operator reviewed: the undo
    refuses unless the catalog is still at it, before and inside its transaction. Needs the catalog lock like
    :func:`write`.
    """
    if not getenv(iceberg.CATALOG_LOCK_ENV):
        raise RuntimeError(f"undo writes the comments catalog: set {iceberg.CATALOG_LOCK_ENV} only while holding the "
                           "lock (docs/comment-fields-fill.md)")
    journal, line = _committed_batch(workdir, batch)
    images = {}
    for name in ("preimage", "postimage"):
        images[name] = Path(line[name])
        if _sha256(images[name]) != line[f"{name}_sha256"]:
            raise RuntimeError(f"{images[name]} is not the {name} batch {batch} journaled; stop (runbook)")
    columns = list(COMMENT.schema)
    col_list = ", ".join(f'"{c}"' for c in columns)
    own = con is None
    con = con or iceberg._connect()
    try:
        WRITE_RESOURCES.configure(con, fill_dir(workdir) / "spill")
        storage_access(con)
        current = iceberg._read_snapshot(con, COMMENT)
        if current.snapshot_id != expected_snapshot:
            raise RuntimeError(f"the comments catalog is at snapshot {current.snapshot_id}, not the expected "
                               f"{expected_snapshot}: review it and rerun with that snapshot")
        table = iceberg._qualified(COMMENT)
        con.execute(f"CREATE OR REPLACE TEMP TABLE _undo_pre AS SELECT {col_list} FROM read_parquet("
                    f"{_sql(images['preimage'])})")
        con.execute(f"CREATE OR REPLACE TEMP TABLE _undo_written AS SELECT * FROM read_parquet("
                    f"{_sql(images['postimage'])})")
        from spicy_regs.sources.regulatory_catalog import processing_table
        table = processing_table(con, COMMENT, where="comment_id IN (SELECT comment_id FROM _undo_pre)")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _undo_now AS SELECT {col_list} FROM {table}
                        WHERE comment_id IN (SELECT comment_id FROM _undo_pre)""")
        con.execute(f"DROP TABLE {table}")
        # The table must hold, key for key, exactly what the commit wrote: same digests, same number of rows.
        now = f"SELECT comment_id, {_columns_md5(columns)} AS row_md5 FROM _undo_now"
        changed = con.execute(f"""SELECT count(*) FROM (
            (SELECT * FROM _undo_written EXCEPT ALL {now}) UNION ALL ({now} EXCEPT ALL SELECT * FROM _undo_written))
        """).fetchone()[0]
        if changed:
            raise RuntimeError(f"{changed} rows of batch {batch} changed since its commit; nothing was undone: "
                               "restore them by hand (runbook)")
        pre = f"SELECT comment_id, {_columns_md5(columns)} AS row_md5 FROM _undo_pre"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _undo_keys AS SELECT DISTINCT comment_id FROM (
            (SELECT * FROM _undo_written EXCEPT ALL {pre}) UNION ALL ({pre} EXCEPT ALL SELECT * FROM _undo_written))""")
        con.execute("CREATE OR REPLACE TEMP TABLE _undo_source AS SELECT * FROM _undo_pre SEMI JOIN _undo_keys "
                    "USING (comment_id)")
        con.execute("CREATE OR REPLACE TEMP TABLE _undo_expected AS SELECT * FROM _undo_now SEMI JOIN _undo_keys "
                    "USING (comment_id)")
        restored, replaced = con.execute("SELECT (SELECT count(*) FROM _undo_source), "
                                         "(SELECT count(*) FROM _undo_expected)").fetchone()
        after, problem = current.__dict__, None
        if restored:
            iceberg.replace_rows(con, COMMENT, "_undo_source", expected_prior="_undo_expected",
                                 expected_snapshot=current)
            after, problem = _our_commit(con, current.__dict__, restored, deletes=replaced)
    finally:
        if own:
            con.close()
    ids = {"batch": batch, "files": line["files"], "prepare_id": line["prepare_id"]}
    _append(journal, {"state": "undone", **ids, "rows_restored": restored, "snapshot_before": current.__dict__,
                      "snapshot_after": after, "at": _now()})
    if problem:
        _append(journal, {"state": "failed", **ids, "committed": True, "reason": f"undo: {problem}",
                          **{k: line[k] for k in ("preimage", "preimage_sha256", "postimage", "postimage_sha256")},
                          "snapshot_before": current.__dict__, "snapshot_after": after})
        raise FillVerificationError(f"the undo of batch {batch} committed but {problem}; stop (runbook)")
    return {"batch": batch, "rows_restored": restored, "snapshot_before": current.__dict__, "snapshot_after": after}
