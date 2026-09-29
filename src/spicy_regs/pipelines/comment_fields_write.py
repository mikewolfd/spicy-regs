"""Write phase of the comment re-read: fill only the catalog cells a row predates, verified before each commit.

:func:`prepare` reads the catalog narrowly at one snapshot and writes ``fill/fill.parquet``, the rows to fill, with a
counts-only ``fill/prepare.json`` naming it by digest. :func:`write` applies it under the catalog lock, one batch of
whole data files per transaction, and checks every batch inside its transaction before COMMIT, so a wrong write is
rolled back rather than committed. docs/comment-fields-fill.md is the runbook.

**The rules.** A read row fills a catalog row only at the same ``comment_id`` and ``modify_date`` (a copy of another
version is counted and skipped), only into a column the catalog holds as NULL, and only for a column on which every
copy read for that version agrees; copies that disagree on a column are listed in ``fill/conflicts.parquet`` and fill
nothing in that column. A stated 0 is a value; NULL stays NULL.

**Big O.** The table is unpartitioned and R2's compacted files each span most agencies, and DuckDB writes Iceberg
merge-on-read: a MERGE rewrites nothing in place but fetches every column of every row group holding a matched row. A
batch per agency would read every row group once per agency in it, O(agencies x table). A batch of whole data files,
matched on ``filename`` as well as the key (DuckDB then scans only those files), reads each file a fixed number of
times (the pre-image, the MERGE, the check), and batches are packed by bytes: O(table).

**Each batch, in one transaction.** Refuse unless the catalog is at the snapshot this fill last committed; capture the
batch's rows and write that pre-image to disk; journal the batch as pending; MERGE; require the MERGE's count to equal
the planned count; then read the batch's rows back, in the transaction, from its old files and the files the MERGE
wrote, and require them to equal the pre-image with the fill applied on every column (a duplicate, a lost row, a
changed non-fill cell or an overwrite fails), and the new files to hold only batch rows. Any failure rolls back and
journals the batch as failed, which stops every later run until an operator clears it. After COMMIT, the snapshot's
parent must be the checked one, and its summary must add exactly the changed records and position deletes.

**Resume.** The journal is per prepare (named by the prepare's digest) and records files, so a changed batch budget
never redoes passed work. A run that finds its own pending batch committed (the parent and the summary match)
verifies it against the stored pre-image and journals it; any other commit (an ETL run, R2's compaction) stops the
run, and preparing again finds only the cells still NULL.
"""

from __future__ import annotations

import hashlib
import json
import time
from os import getenv
from pathlib import Path

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg

#: The columns a fill writes: the nullable columns added after the catalog table was created, and attachments_json.
FILL_COLUMNS = (*iceberg._COMMENT_ADDED_COLUMNS, "attachments_json")
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


def fill_dir(workdir: Path) -> Path:
    return workdir / "fill"


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
        f"{iceberg._CATALOG_ALIAS}.{iceberg._namespace()}.{COMMENT.name}"]).fetchone()[0]
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


def data_files(con) -> dict[str, int]:
    """Every live data file of the current snapshot with its compressed bytes (Parquet footers)."""
    table = f"{iceberg._CATALOG_ALIAS}.{iceberg._namespace()}.{COMMENT.name}"
    paths = [row[0] for row in con.execute(
        f"SELECT file_path FROM iceberg_metadata('{table}') WHERE content = 'EXISTING' AND status <> 'DELETED'").fetchall()]
    if not paths:
        return {}
    listed = ", ".join(map(_sql, paths))
    return dict(con.execute(
        f"SELECT file_name, sum(total_compressed_size) FROM parquet_metadata([{listed}]) GROUP BY 1").fetchall())


# --------------------------------------------------------------------------- #
# Prepare
# --------------------------------------------------------------------------- #
def prepare(workdir: Path, *, scope: dict[str, str] | None = None, con=None) -> dict:
    """Compute the fill at the catalog's current snapshot; return and write the counts-only receipt.

    Refuses a catalog that lacks a fill column: the columns arrive with the deployed branch's first data commit
    (runbook), never from this tool. Touches no catalog row and needs no lock.
    """
    own = con is None
    con = con or iceberg._connect()
    out = fill_dir(workdir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        storage_access(con)
        missing = [c for c in FILL_COLUMNS if c not in iceberg._column_types(con, COMMENT)]
        if missing:
            raise RuntimeError(f"the comments catalog lacks {missing}: deploy the branch and let one ETL data commit "
                               "migrate the table before preparing (docs/comment-fields-fill.md)")
        snapshot = iceberg._read_snapshot(con, COMMENT)
        where = " AND ".join(f'"{column}" = {_sql(value)}' for column, value in (scope or {}).items()) or "TRUE"
        cols = ", ".join(f'"{c}"' for c in FILL_COLUMNS)
        source = iceberg._snapshot_query(COMMENT, snapshot).replace("SELECT *", f"SELECT *, {FILE_COLUMN} AS _file", 1)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _cat AS SELECT comment_id, agency_code, docket_id, modify_date,
                        {cols}, _file FROM ({source}) WHERE {where}""")
        duplicated = con.execute("SELECT count(*) FROM (SELECT comment_id FROM _cat GROUP BY 1 HAVING count(*) > 1)"
                                 ).fetchone()[0]
        if duplicated:
            raise RuntimeError(f"{duplicated} comment ids appear more than once in the catalog; run the dedupe first")
        parts = f"read_parquet({_sql(workdir / 'parts' / '*' / '*.parquet')}, hive_partitioning=false)"
        values = ", ".join(f'CAST(s."{c}" AS VARCHAR) AS "{c}"' for c in FILL_COLUMNS)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _read AS SELECT s.key, s.comment_id, s.modify_date, {values}
                        FROM {parts} s SEMI JOIN _cat USING (comment_id)""")
        # Per column: how many spellings the copies of one version state (NULL counted as one), and the value.
        spell = ", ".join(
            f"""count(DISTINCT CASE WHEN "{c}" IS NULL THEN 'n' ELSE 'v' || "{c}" END) AS "n_{c}", """
            f"""any_value("{c}") AS "v_{c}\""""
            for c in FILL_COLUMNS)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _versions AS SELECT comment_id, modify_date,
                        list(key ORDER BY key) AS keys, {spell} FROM _read GROUP BY comment_id, modify_date""")
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
    finally:
        if own:
            con.close()
    fill_sha256 = _sha256(out / "fill.parquet")
    receipt = {
        "prepare_id": f"{snapshot.snapshot_id}-{fill_sha256[:12]}", "fill_sha256": fill_sha256,
        "snapshot": snapshot.__dict__, "scope": scope or {},
        "catalog_rows": counts[0], "unread": counts[1], "other_version_only": counts[2],
        "rows_to_fill": to_fill, "cells_by_column": cells, "conflicted_versions_by_column": conflicted,
        "files": {file: {"rows": rows, "bytes": sizes.get(file)} for file, rows in files},
        "prepared_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
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


def _journal(workdir: Path, prepared: dict) -> Path:
    return fill_dir(workdir) / f"write-journal-{prepared['prepare_id']}.jsonl"


def _append(path: Path, line: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(line, default=str) + "\n")


def _snapshot_dict(snapshot) -> dict:
    return snapshot.__dict__ if hasattr(snapshot, "__dict__") else dict(snapshot)


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
    lines = [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
    failed = [line for line in lines if line["state"] == "failed"]
    cleared = {tuple(line["files"]) for line in lines if line["state"] == "cleared"}
    if [line for line in failed if tuple(line["files"]) not in cleared]:
        if not clear_failure:
            raise RuntimeError(f"a batch failed verification ({journal.name}); roll back to its snapshot_before "
                               "(runbook), then rerun with --clear-failure")
        for line in failed:
            _append(journal, {"state": "cleared", "files": line["files"], "prepare_id": prepared["prepare_id"],
                              "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    verified = [line for line in lines if line["state"] == "verified"]
    done = {file for line in verified for file in line["files"]}
    expected = verified[-1]["snapshot_after"] if verified else prepared["snapshot"]
    own = con is None
    con = con or iceberg._connect()  # never _connect_for_table: a migration here would move schema_id mid-run
    resources = resources or WRITE_RESOURCES
    resources.configure(con, out / "spill")
    storage_access(con)
    table = iceberg._qualified(COMMENT)
    missing = [c for c in FILL_COLUMNS if c not in iceberg._column_types(con, COMMENT)]
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


def _write_batch(con, workdir, journal, prepared, table, files, expected, by_file) -> tuple[dict, int]:
    columns = list(COMMENT.schema)
    col_list = ", ".join(f'"{c}"' for c in columns)
    file_list = ", ".join(map(_sql, files))
    batch_id = hashlib.sha256("\n".join(files).encode()).hexdigest()[:16]
    preimage = fill_dir(workdir) / "preimage" / f"{prepared['prepare_id']}-{batch_id}.parquet"
    preimage.parent.mkdir(parents=True, exist_ok=True)
    fill = fill_dir(workdir) / "fill.parquet"
    in_files = f"{FILE_COLUMN} IN ({file_list}) AND " if by_file else ""
    con.execute("BEGIN")
    try:
        current = iceberg._read_snapshot(con, COMMENT)
        if current.__dict__ != expected:
            raise RuntimeError(f"the comments catalog moved from {expected} to {current.__dict__}: another writer or "
                               "R2 compaction committed; prepare again under the lock")
        before_files = set(data_files(con)) if by_file else None
        con.execute(f"CREATE OR REPLACE TEMP TABLE _batch AS SELECT * FROM read_parquet({_sql(fill)}) "
                    f"WHERE _file IN ({file_list})")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _prior AS SELECT {col_list} FROM {table}
                        WHERE {in_files}comment_id IN (SELECT comment_id FROM _batch)""")
        con.execute(f"COPY _prior TO {_sql(preimage)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        need = " OR ".join(f'(p."{c}" IS NULL AND b."{c}" IS NOT NULL)' for c in FILL_COLUMNS)
        planned = con.execute(f"""SELECT count(*) FROM _prior p JOIN _batch b
            ON p.comment_id = b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date WHERE {need}""").fetchone()[0]
        _append(journal, {"state": "pending", "files": files, "planned": planned, "snapshot_before": current.__dict__,
                          "preimage": str(preimage), "before_files": sorted(before_files or ()),
                          "by_file": by_file, "prepare_id": prepared["prepare_id"]})
        changed = 0
        if planned:
            on_file = f" AND t.{FILE_COLUMN} = s._file" if by_file else ""
            sets = ", ".join(f'"{c}" = COALESCE(t."{c}", s."{c}")' for c in FILL_COLUMNS)
            need_t = " OR ".join(f'(t."{c}" IS NULL AND s."{c}" IS NOT NULL)' for c in FILL_COLUMNS)
            changed = con.execute(f"""MERGE INTO {table} t USING _batch s
                ON t.comment_id = s.comment_id AND t.modify_date IS NOT DISTINCT FROM s.modify_date{on_file}
                WHEN MATCHED AND ({need_t}) THEN UPDATE SET {sets}""").fetchone()[0]
            if changed != planned:
                raise FillVerificationError(f"MERGE changed {changed} rows for {planned} planned")
            _check(con, table, columns, files, before_files, by_file)
        con.execute("COMMIT")
    except Exception as error:
        try:
            con.execute("ROLLBACK")
        except Exception:  # a failed COMMIT can already have aborted the transaction; keep the original error
            pass
        if isinstance(error, FillVerificationError):
            _append(journal, {"state": "failed", "files": files, "committed": False, "reason": str(error)[:500],
                              "snapshot_before": expected, "prepare_id": prepared["prepare_id"]})
        raise
    if not changed:
        _append(journal, {"state": "verified", "files": files, "planned": 0, "changed": 0, "snapshot_before": expected,
                          "snapshot_after": expected, "prepare_id": prepared["prepare_id"]})
        return expected, 0
    after, problem = _our_commit(con, current.__dict__, changed)
    if problem:
        _append(journal, {"state": "failed", "files": files, "committed": True, "reason": problem,
                          "snapshot_before": current.__dict__, "snapshot_after": after,
                          "prepare_id": prepared["prepare_id"]})
        raise FillVerificationError(f"batch committed but {problem}; roll back to {current.__dict__} (runbook)")
    _append(journal, {"state": "verified", "files": files, "planned": planned, "changed": changed,
                      "snapshot_before": current.__dict__, "snapshot_after": after,
                      "prepare_id": prepared["prepare_id"], "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    return after, changed


def _check(con, table: str, columns: list[str], files: list[str], before_files: set[str] | None, by_file: bool,
           *, at: int | None = None) -> None:
    """Inside the transaction: the batch's rows equal the pre-image with the fill applied; new files hold nothing else.

    Rows are read from the batch's old files (those the MERGE left) and from files that did not exist before the
    transaction (those it wrote). ``before_files`` is None for a scoped pilot, which reads the batch's keys anywhere.
    ``at`` reads the table as of that snapshot, for a batch checked after later commits.
    """
    if at is not None:
        table = f"(SELECT *, {FILE_COLUMN} FROM {table} AT (VERSION => {int(at)}))"
    want = ", ".join(f'CASE WHEN p."{c}" IS NULL THEN b."{c}" ELSE p."{c}" END AS "{c}"' if c in FILL_COLUMNS
                     else f'p."{c}"' for c in columns)
    if by_file:
        if before_files is None:
            raise FillVerificationError("the catalog's data files are unknown; refusing to check by full scan")
        old = ", ".join(map(_sql, files))
        new = ", ".join(map(_sql, sorted(before_files))) or "''"
        where = f"({FILE_COLUMN} IN ({old}) OR {FILE_COLUMN} NOT IN ({new}))"
        strays = con.execute(f"""SELECT count(*) FROM {table} WHERE {FILE_COLUMN} NOT IN ({new})
                                 AND comment_id NOT IN (SELECT comment_id FROM _batch)""").fetchone()[0]
        if strays:
            raise FillVerificationError(f"the MERGE wrote {strays} rows outside the batch")
    else:
        where = "TRUE"
    col_list = ", ".join(f'"{c}"' for c in columns)
    mismatched = con.execute(f"""
        WITH want AS (SELECT {want} FROM _prior p LEFT JOIN _batch b
                      ON p.comment_id = b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date),
             got AS (SELECT {col_list} FROM {table} WHERE {where} AND comment_id IN (SELECT comment_id FROM _prior))
        SELECT (SELECT count(*) FROM (SELECT * FROM want EXCEPT ALL SELECT * FROM got))
             + (SELECT count(*) FROM (SELECT * FROM got EXCEPT ALL SELECT * FROM want))""").fetchone()[0]
    if mismatched:
        raise FillVerificationError(f"{mismatched} rows differ from the pre-image with the fill applied")


def _our_commit(con, before: dict, changed: int) -> tuple[dict, str | None]:
    """The snapshot this batch committed, and why it is not exactly the batch (None when it is).

    It must be the one child of the checked snapshot, adding exactly ``changed`` records and position deletes. A
    catalog that records no commits (the hermetic tests' stand-in) is read as its current snapshot.
    """
    record = commit_record(con, before["snapshot_id"])
    if record is None:
        current = iceberg._read_snapshot(con, COMMENT).__dict__
        if current == before or _records_commits(con):
            return current, f"no single commit on top of {before['snapshot_id']}"
        return current, None
    after = {k: record[k] for k in ("table_uuid", "snapshot_id", "schema_id")}
    summary = record["summary"]
    added, deletes = int(summary.get("added-records", 0)), int(summary.get("added-position-deletes", 0))
    if (added, deletes) != (changed, changed):
        return after, f"it added {added} records and {deletes} position deletes for {changed} changed rows"
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
    if current == pending["snapshot_before"]:
        _append(journal, {"state": "abandoned", "files": pending["files"], "prepare_id": prepared["prepare_id"]})
        return expected
    planned = pending["planned"]
    if commit_record(con, pending["snapshot_before"]["snapshot_id"]) is None:
        raise RuntimeError(f"the catalog moved to {current} after a pending batch, and not by that batch; "
                           "prepare again under the lock")
    after, problem = _our_commit(con, pending["snapshot_before"], planned)
    if problem:
        raise RuntimeError(f"the commit after a pending batch is not that batch ({problem}); prepare again")
    # Our commit, never checked: compare the rows it wrote with the stored pre-image and the fill.
    con.execute(f"CREATE OR REPLACE TEMP TABLE _prior AS SELECT * FROM read_parquet({_sql(pending['preimage'])})")
    file_list = ", ".join(map(_sql, pending["files"]))
    con.execute(f"CREATE OR REPLACE TEMP TABLE _batch AS SELECT * FROM read_parquet("
                f"{_sql(fill_dir(journal.parent.parent) / 'fill.parquet')}) WHERE _file IN ({file_list})")
    try:
        _check(con, table, list(COMMENT.schema), pending["files"], set(pending["before_files"]), pending["by_file"],
               at=after["snapshot_id"])
        mismatched = 0
    except FillVerificationError as error:
        mismatched = str(error)
    if mismatched:
        _append(journal, {"state": "failed", "files": pending["files"], "committed": True,
                          "reason": f"recovered commit: {mismatched}", "snapshot_before":
                          pending["snapshot_before"], "snapshot_after": current, "prepare_id": prepared["prepare_id"]})
        raise FillVerificationError(f"a committed batch differs from its pre-image; roll back to "
                                    f"{pending['snapshot_before']} (runbook)")
    _append(journal, {"state": "verified", "files": pending["files"], "planned": planned, "changed": planned,
                      "snapshot_before": pending["snapshot_before"], "snapshot_after": after, "recovered": True,
                      "prepare_id": prepared["prepare_id"]})
    return after


# --------------------------------------------------------------------------- #
# Rollback: move the table's main ref back to a snapshot, through the Iceberg REST catalog.
# --------------------------------------------------------------------------- #
def rollback(to_snapshot_id: int, *, expected_current: int, uri: str | None = None, warehouse: str | None = None,
             token: str | None = None, namespace: str | None = None, client=None) -> dict:
    """Set ``comments``' main branch to ``to_snapshot_id``, refusing unless it is still at ``expected_current``.

    One REST commit (``set-snapshot-ref`` with an ``assert-ref-snapshot-id`` requirement), so a writer that moved the
    table in between makes it refuse. The rolled-back snapshots stay in the table's history.
    """
    import httpx

    uri = (uri or getenv("R2_CATALOG_URI", "")).rstrip("/")
    warehouse = warehouse or getenv("R2_CATALOG_WAREHOUSE", "")
    token = token or getenv("R2_CATALOG_TOKEN", "")
    namespace = namespace or iceberg._namespace()
    http = client or httpx.Client(timeout=60, headers={"Authorization": f"Bearer {token}"})
    config = http.get(f"{uri}/v1/config", params={"warehouse": warehouse}).raise_for_status().json()
    prefix = (config.get("overrides") or {}).get("prefix") or (config.get("defaults") or {}).get("prefix")
    base = f"{uri}/v1/{prefix}" if prefix else f"{uri}/v1"
    body = {
        "identifier": {"namespace": [namespace], "name": COMMENT.name},
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": expected_current}],
        "updates": [{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch",
                     "snapshot-id": to_snapshot_id}],
    }
    response = http.post(f"{base}/namespaces/{namespace}/tables/{COMMENT.name}", json=body)
    response.raise_for_status()
    return {"metadata_location": response.json().get("metadata-location"), "snapshot_id": to_snapshot_id}
