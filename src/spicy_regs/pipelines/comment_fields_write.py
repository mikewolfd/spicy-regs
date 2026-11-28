"""Write phase of the comment re-read: fill only the catalog cells a row predates, verified before each commit.

:func:`prepare` reads the catalog narrowly at one snapshot and writes ``fill/fill.parquet``, the rows to fill, with a
counts-only ``fill/prepare.json`` naming it by digest. :func:`write` applies it under the catalog lock, one batch of
whole data files per transaction, and checks every batch inside its transaction before COMMIT, so a wrong write is
rolled back rather than committed. :func:`undo` restores a committed batch from its pre-image as a new, checked
commit. docs/comment-fields-fill.md is the runbook.

**The rules.** A read row fills a catalog row only at the same ``comment_id`` and ``modify_date`` (a copy of another
version is counted and skipped), only into a column the catalog holds as NULL, and only for a column on which every
copy read for that version agrees; copies that disagree on a column are listed in ``fill/conflicts.parquet`` and fill
nothing in that column. A stated 0 is a value; NULL stays NULL. One rule replaces a held value: in an :data:`ADDITIVE`
column, under the same version and agreement, a read that only adds to the held value (the held value is the read's
as an earlier extract spelled it, byte for byte) replaces it; any other read value that differs is refused, counted
and listed in ``fill/refusals.parquet``.

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
import shutil
import time
from collections.abc import Mapping
from contextlib import suppress
from functools import cache
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.transforms.regulations_shape import (
    COMMENT_ATTACHMENTS, COMMENT_ATTACHMENT_ID, COMMENT_ATTACHMENT_METADATA, COMMENT_ATTACHMENT_RESTRICTIONS,
    _native, _validate,
)

#: The catalog's key and version, which :func:`prepare` selects on their own and a fill never writes.
KEY_COLUMNS = ("comment_id", "agency_code", "docket_id", "modify_date")
#: Host enrichment columns: never in the source record, so never in a read part and never filled.
HOST_COLUMNS = ("text_content", "text_extraction_status", "pdf_extraction_results_json")
#: The columns a fill writes: every column of the table a read part holds, so a cell the row predates is filled
#: whichever column it is in. That is the table minus its key, the body (a part keeps only its digest) and the host's
#: columns; the parts' own shape is ``comment_fields.PART_SCHEMA``, and a test holds the two to each other. Until
#: 2026-10-03 this was the six columns added after the table was created plus ``attachments_json``, which left the
#: submitter's ``first_name``, ``last_name``, ``organization`` and ``category`` NULL on every row ingested before the
#: extract mapped them (ff812e5, 2026-06-15) although the 2026-09-28 re-read had them in hand.
FILL_COLUMNS = tuple(c for c in COMMENT.schema if c not in (*KEY_COLUMNS, "comment", *HOST_COLUMNS))
#: The fill columns a fill considers, by name. ``attachments`` is the 2026-10 re-read's fill (owner, 2026-10-03): only
#: ``attachments_json`` moved under SpicyDocs b19b092, so its staged read carries nothing else.
PROFILES = {"all": FILL_COLUMNS, "attachments": ("attachments_json",)}


def fill_columns(profile: str) -> tuple[str, ...]:
    """The fill columns ``profile`` names; an unknown profile is refused."""
    if profile not in PROFILES:
        raise ValueError(f"no fill profile {profile!r}; one of {sorted(PROFILES)}")
    return PROFILES[profile]


def read_columns(profile: str) -> tuple[str, ...]:
    """What :func:`prepare` reads of each read copy: the object key naming the copy, the version, and the profile's
    fill values. A staged fill input (runbook) holds exactly these."""
    return ("key", "comment_id", "modify_date", *fill_columns(profile))


READ_COLUMNS = read_columns("all")


_METADATA_KEYS = frozenset(source for source, _, _ in COMMENT_ATTACHMENT_METADATA)
_ADDED_KEYS = _METADATA_KEYS | {COMMENT_ATTACHMENT_ID, *COMMENT_ATTACHMENT_RESTRICTIONS}


def _listed(value: str | None) -> list:
    """The list ``value`` spells, only in the extract's own spelling (``json.dumps``); raises ValueError otherwise."""
    listed = json.loads(value or "null")
    if json.dumps(listed) != value or not isinstance(listed, list):
        raise ValueError("not an attachment list in the extract's spelling")
    return listed


def _without_keys(listed: list, keys: set | frozenset) -> str:
    return json.dumps([{k: v for k, v in entry.items() if k not in keys} for entry in listed])


def downloadable_attachments(value: str | None) -> str | None:
    """``attachments_json`` less what offers no file: every rendition with no ``url``, every entry left with none (a
    withheld attachment states no ``fileFormats``), and each entry's restriction and ``attachment_id``.

    That is what the rule before SpicyDocs b19b092 (2026-03-15 to 0.53.0) wrote for the same record, NULL for no entry.
    Only removals: the value must be in the extract's own spelling (``json.dumps``), and what is kept keeps its keys and
    order. None for a value of any other spelling or shape, which then equals no held value.
    """
    try:
        kept = []
        for entry in _listed(value):
            formats = [rendition for rendition in entry["formats"] or [] if rendition["url"]]
            if formats:
                kept.append({**{k: v for k, v in entry.items() if k not in _ADDED_KEYS}, "formats": formats})
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    return json.dumps(kept) if kept else None


@cache
def _v2_attachment_type():
    from spicy_regs.policy_migrations import earlier_policy

    return earlier_policy("comments", "regulations-native-v2").subject_schema.field("attachments").type


def _matches_promoted_attachments(held: str, *earlier_spellings: str | None) -> bool:
    """The v2 promotion encoded the exact native struct in compact JSON with all fields."""
    v2 = _v2_attachment_type()
    decoded = json.loads(held)
    if not isinstance(decoded, list) or any(
        not isinstance(entry, dict) or list(entry) != v2.value_type.names for entry in decoded
    ):
        return False
    import pyarrow as pa

    for spelling in earlier_spellings:
        candidate = _native("comments", "attachments_json", spelling)
        _validate(candidate, COMMENT_ATTACHMENTS, "attachments")
        normalized = pa.array([candidate], type=v2).to_pylist()[0]
        encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
        if encoded == held:
            return True
        from spicy_regs.transforms.regulations_shape import normalize_legacy_comment_sizes

        earlier = normalize_legacy_comment_sizes(decoded, spelling)
        if encoded == json.dumps(earlier, ensure_ascii=False, separators=(",", ":")):
            return True
    return False


def only_adds_attachments(read: str | None, held: str | None, promoted_v2: bool = False) -> bool:
    """Compare the read against the exact JSON written by an earlier extract."""
    if held is None:
        return False
    try:
        listed = _listed(read)
        prior = _without_keys(listed, _METADATA_KEYS)
        before_ids = _without_keys(listed, _METADATA_KEYS | {COMMENT_ATTACHMENT_ID})
        downloadable = downloadable_attachments(read)
        return held in (prior, before_ids, downloadable) or (
            promoted_v2 and _matches_promoted_attachments(held, prior, before_ids, downloadable)
        )
    except (ValueError, TypeError, AttributeError):
        return False


#: Fill columns whose held value a read may replace, each with whether a read only adds to the held value. Only
#: ``attachments_json``: b19b092 lists every attachment a record lists (the rule before it dropped a withheld one and
#: every rendition without a file) and 0.85.0 adds each entry's id, and the ETL rewrites a row only when its
#: ``modify_date`` moves, so a row read earlier keeps the earlier spelling until the re-read replaces it.
ADDITIVE = {"attachments_json": only_adds_attachments}
#: Supported prepared write operations: standard Mirrulations re-read fill and bounded empty-string nullification.
FILL_OPERATION = "fill"
EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION = "empty_original_document_id"
OPERATIONS = frozenset({FILL_OPERATION, EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION})
#: DuckDB TRIM strips only ASCII space by default; strip all ASCII whitespace (matching Python ``str.strip``).
_ASCII_WHITESPACE_SQL = "' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)"
#: The Iceberg scan's virtual column naming each row's data file.
FILE_COLUMN = "filename"
#: Compressed bytes of catalog data files per batch; a larger file is still one batch.
BATCH_BYTES = 512 * 2**20
#: The write connection's DuckDB budget and spill directory (``fill/spill``). A whole-table scan with eight threads ran
#: out of memory at 4.6 GiB on 2026-09-28; one batch is at most about 512 MiB of compressed files.
WRITE_RESOURCES = ExportResources(memory="6GB", threads=4)
#: Disjoint ``comment_id`` hash buckets for duplicate checks and bounded joins/aggregations (must be a power of two).
HASH_BUCKETS = 32
#: Streaming Arrow record-batch size when writing partitioned Parquet intermediates and outputs directly.
ARROW_BATCH_ROWS = 16_384


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


def _prepared_operation(prepared: dict) -> str:
    """Validate and return the prepared operation (defaulting older receipts to ``fill``)."""
    operation = prepared.get("operation", FILL_OPERATION)
    if operation not in OPERATIONS:
        raise RuntimeError(f"unsupported prepared operation {operation!r}; expected one of {sorted(OPERATIONS)}")
    if operation == EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION and prepared.get("columns") != ["original_document_id"]:
        raise RuntimeError(
            f"{EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION} may only modify ['original_document_id'], "
            f"got {prepared.get('columns')!r}"
        )
    return operation


def _is_blank_sql(expr: str) -> str:
    return f"({expr} IS NOT NULL AND trim({expr}, {_ASCII_WHITESPACE_SQL}) = '')"


def _filled(column: str, old: str, new: str, *, operation: str = FILL_OPERATION) -> str:
    """The cell after the fill, for row alias ``old`` and fill-row alias ``new`` (whose ``_held_`` value an
    :data:`ADDITIVE` replacement or empty-string nullification expects to find)."""
    if operation == EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION:
        if column != "original_document_id":
            raise ValueError(f"{operation} may only update original_document_id, not {column!r}")
        held, kept = f'{new}."_held_{column}"', f'{old}."{column}"'
        return (
            f"CASE WHEN {_is_blank_sql(kept)} AND {kept} = {held} "
            f"THEN CAST(NULL AS {COMMENT.sql_type(column)}) ELSE {kept} END"
        )
    held, kept, read = f'{new}."_held_{column}"', f'{old}."{column}"', f'{new}."{column}"'
    read = f"CAST({read} AS {COMMENT.sql_type(column)})"
    if column not in ADDITIVE:
        return f"COALESCE({kept}, {read})"
    return f"CASE WHEN {held} IS NULL THEN COALESCE({kept}, {read}) WHEN {kept} = {held} THEN {read} ELSE {kept} END"


def _changes(column: str, old: str, new: str, *, operation: str = FILL_OPERATION) -> str:
    """Whether the fill changes the cell (aliases as in :func:`_filled`)."""
    if operation == EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION:
        if column != "original_document_id":
            raise ValueError(f"{operation} may only update original_document_id, not {column!r}")
        held, kept = f'{new}."_held_{column}"', f'{old}."{column}"'
        return f"({_is_blank_sql(kept)} AND {kept} = {held})"
    held, kept, read = f'{new}."_held_{column}"', f'{old}."{column}"', f'{new}."{column}"'
    fills = f"({kept} IS NULL AND {read} IS NOT NULL)"
    return fills if column not in ADDITIVE else f"(CASE WHEN {held} IS NULL THEN {fills} ELSE {kept} = {held} END)"


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
def _bucket_mask(buckets: int = HASH_BUCKETS) -> tuple[int, int]:
    if buckets < 1 or (buckets & (buckets - 1)) != 0:
        raise ValueError(f"HASH_BUCKETS must be a positive power of two: {buckets}")
    return buckets, buckets - 1


def _count_duplicate_comment_ids(con, relation: str, *, buckets: int = HASH_BUCKETS) -> int:
    """Count distinct ``comment_id`` values (including NULL) appearing more than once, partitioned by hash bucket."""
    count, mask = _bucket_mask(buckets)
    null_count = con.execute(f"SELECT count(*) FROM {relation} WHERE comment_id IS NULL").fetchone()[0]
    duplicated = 1 if null_count > 1 else 0
    for bucket in range(count):
        duplicated += con.execute(
            f"SELECT count(*) FROM ("
            f"SELECT comment_id FROM {relation} "
            f"WHERE comment_id IS NOT NULL AND (hash(comment_id) & {mask}) = {bucket} "
            f"GROUP BY 1 HAVING count(*) > 1)"
        ).fetchone()[0]
    return duplicated


def _write_partitioned_parquet(
    con,
    target: Path,
    bucket_sql,
    *,
    buckets: int = HASH_BUCKETS,
    batch_size: int = ARROW_BATCH_ROWS,
) -> None:
    """Run ``bucket_sql(bucket, mask)`` across disjoint hash buckets and stream into one ZSTD Parquet file."""
    import pyarrow.parquet as pq

    count, mask = _bucket_mask(buckets)
    writer: pq.ParquetWriter | None = None
    try:
        for bucket in range(count):
            reader = con.execute(bucket_sql(bucket, mask)).to_arrow_reader(batch_size=batch_size)
            if writer is None:
                writer = pq.ParquetWriter(target, reader.schema, compression="zstd")
            for batch in reader:
                if batch.num_rows:
                    writer.write_batch(batch)
            del reader
    finally:
        if writer is not None:
            writer.close()


def prepare(
    workdir: Path, *, scope: dict[str, str] | None = None, reads: Path | None = None, profile: str = "all", con=None,
    resources: ExportResources | None = None,
) -> dict:
    """Compute the fill at the catalog's current snapshot; return and write the counts-only receipt.

    ``reads`` is the staged fill input (:func:`read_columns` of ``profile``); by default the read parts under
    ``workdir/parts``. ``profile`` names the fill columns considered (:data:`PROFILES`); the receipt keeps them for
    :func:`write`. Refuses a snapshot that lacks a fill column: the columns arrive with the deployed branch's first
    data commit (runbook), never from this tool. Refuses a read that lacks one, and while any batch committed and
    failed its check. Touches no catalog row and needs no lock. ``resources`` overrides the default query targets
    for a bounded execution; these settings do not enforce a whole-process memory limit.
    """
    columns = fill_columns(profile)
    additive = [c for c in columns if c in ADDITIVE]
    _refuse_committed_failures(workdir)
    own = con is None
    con = con or iceberg._connect()
    out = fill_dir(workdir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        (resources or WRITE_RESOURCES).configure(con, out / "spill")
        storage_access(con)
        namespace = iceberg._selected_catalog_namespace(con, COMMENT)
        snapshot = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        source = iceberg._snapshot_query(COMMENT, snapshot, namespace=namespace)
        read_from = reads or workdir / "parts" / "*" / "*.parquet"
        parts = f"read_parquet({_sql(read_from)}, hive_partitioning=false)"
        lacking = [c for c in read_columns(profile) if c not in con.sql(f"SELECT * FROM {parts}").columns]
        if lacking:
            raise RuntimeError(f"the reads {read_from} lack {lacking}: stage them for the {profile!r} profile "
                               "(docs/comment-fields-fill.md)")
        table_rows = con.execute(f"SELECT count(*) FROM ({source})").fetchone()[0]
        where = " AND ".join(f'"{column}" = {_sql(value)}' for column, value in (scope or {}).items()) or "TRUE"
        # Every intermediate is a Parquet file the next step streams, never an in-memory table: three whole-table
        # temp tables (the catalog's 15 fill values, the read copies with their object keys, and every version with
        # its key list) ran the runner out of 6 GB at the fill's COPY (run 37105894011, 2026-10-03). They sit in
        # ``fill/work``, outside the artifact's ``fill/*.parquet``, and go once the receipt is written.
        work = out / "work"
        work.mkdir(exist_ok=True)
        index_file = work / "catalog-index.parquet"
        index = f"read_parquet({_sql(index_file)})"
        matched_index_file, candidates_file, cat_file, read_file, versions_file, differs_file = (
            work / f"{name}.parquet"
            for name in ("matched-index", "candidates", "catalog", "reads", "versions", "differs"))
        matched_index, candidates_rel, catalog, read_rows, versions, differs = (
            f"read_parquet({_sql(path)})"
            for path in (matched_index_file, candidates_file, cat_file, read_file, versions_file, differs_file))
        # Resolve scope and versions before restoring any wide subject or receipt. The pinned native columns are
        # selection hints only; the existing strict receipt reader still verifies every possible fill row below.
        # Keep all scoped keys here so unread/other-version counts and duplicate detection do not shrink to fills.
        physical = source.replace("SELECT *", f"SELECT comment_id, modify_date, {FILE_COLUMN} AS _file", 1)
        con.execute(f"""COPY (SELECT comment_id, modify_date, _file FROM ({physical} WHERE {where}))
                        TO {_sql(index_file)} (FORMAT PARQUET, COMPRESSION ZSTD)""")
        catalog_rows = con.execute(f"SELECT count(*) FROM {index}").fetchone()[0]
        duplicated = _count_duplicate_comment_ids(con, index)
        if duplicated:
            raise RuntimeError(f"{duplicated} comment ids appear more than once in the catalog; run the dedupe first")
        # Restrict the narrow catalog index to comment_ids present in the reads across disjoint hash buckets so no
        # downstream join or accounting query scans or builds hash state over the full unpartitioned catalog.
        _write_partitioned_parquet(
            con,
            matched_index_file,
            lambda b, m: (
                f"SELECT c.comment_id, c.modify_date, c._file "
                f"FROM (SELECT comment_id, modify_date, _file FROM {index} "
                f"      WHERE comment_id IS NOT NULL AND (hash(comment_id) & {m}) = {b}) c "
                f"SEMI JOIN (SELECT comment_id FROM {parts} "
                f"           WHERE comment_id IS NOT NULL AND (hash(comment_id) & {m}) = {b}) s "
                f"USING (comment_id)"
            ),
        )
        index_file.unlink()
        values = ", ".join(f'CAST(s."{c}" AS VARCHAR) AS "{c}"' for c in columns)
        source_cols = ", ".join(f'"{c}"' for c in columns)
        # The copies of the comments in scope, less their object keys: only a version read more than once can
        # conflict, so only those keys are read again, below.
        _write_partitioned_parquet(
            con,
            read_file,
            lambda b, m: (
                f"SELECT s.comment_id, s.modify_date, {values} "
                f"FROM (SELECT comment_id, modify_date, {source_cols} FROM {parts} "
                f"      WHERE comment_id IS NOT NULL AND (hash(comment_id) & {m}) = {b}) s "
                f"SEMI JOIN (SELECT comment_id FROM {matched_index} "
                f"           WHERE (hash(comment_id) & {m}) = {b}) c "
                f"USING (comment_id)"
            ),
        )
        # Per column: how many spellings the copies of one version state (NULL counted as one), and the value. All
        # but 842 of the re-read's 26.6M versions were read once and state one spelling each, so only versions read
        # more than once are aggregated: a DISTINCT count over every version ran out of 6 GB (2026-09-29).
        same = "m.comment_id = r.comment_id AND m.modify_date IS NOT DISTINCT FROM r.modify_date"
        buckets, mask = _bucket_mask()
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _multi AS SELECT comment_id, modify_date FROM {read_rows} WHERE false"
        )
        for bucket in range(buckets):
            con.execute(
                f"INSERT INTO _multi SELECT comment_id, modify_date FROM {read_rows} "
                f"WHERE (hash(comment_id) & {mask}) = {bucket} GROUP BY comment_id, modify_date HAVING count(*) > 1"
            )
        spell = ", ".join(
            f"""count(DISTINCT CASE WHEN "{c}" IS NULL THEN 'n' ELSE 'v' || "{c}" END) AS "n_{c}", """
            f"""any_value("{c}") AS "v_{c}\""""
            for c in columns)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _spellings AS SELECT comment_id, modify_date, {spell}
                        FROM {read_rows} r WHERE EXISTS (SELECT 1 FROM _multi m WHERE {same})
                        GROUP BY comment_id, modify_date""")
        # Each version's value per column: a version read once states it; one read more often states it only where
        # its copies agree, so a conflicted column holds NULL and fills nothing.
        agreed = ", ".join(f'CASE WHEN "n_{c}" = 1 THEN "v_{c}" END AS "{c}"' for c in columns)
        stated = ", ".join(f'"{c}"' for c in columns)
        con.execute(f"""COPY (
            SELECT comment_id, modify_date, {stated} FROM {read_rows} r
            WHERE NOT EXISTS (SELECT 1 FROM _multi m WHERE {same})
            UNION ALL
            SELECT comment_id, modify_date, {agreed} FROM _spellings
        ) TO {_sql(versions_file)} (FORMAT PARQUET, COMPRESSION ZSTD)""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _keys AS SELECT comment_id, modify_date, list(key ORDER BY key) AS keys
                        FROM {parts} r WHERE EXISTS (SELECT 1 FROM _multi m WHERE {same}) GROUP BY ALL""")
        conflicts = " UNION ALL ".join(
            f"""SELECT comment_id, modify_date, '{c}' AS "column" FROM _spellings WHERE "n_{c}" > 1"""
            for c in columns)
        con.execute(f"""COPY (SELECT x.comment_id, x.modify_date, x."column", k.keys FROM ({conflicts}) x
                              JOIN _keys k ON k.comment_id = x.comment_id
                              AND k.modify_date IS NOT DISTINCT FROM x.modify_date ORDER BY 1, 3)
                        TO {_sql(out / 'conflicts.parquet')} (FORMAT PARQUET)""")
        on_version = "v.comment_id = c.comment_id AND v.modify_date IS NOT DISTINCT FROM c.modify_date"
        has_value = " OR ".join(f'v."{c}" IS NOT NULL' for c in columns)
        _write_partitioned_parquet(
            con,
            candidates_file,
            lambda b, m: (
                f"SELECT c.comment_id, c._file "
                f"FROM (SELECT comment_id, modify_date, _file FROM {matched_index} "
                f"      WHERE (hash(comment_id) & {m}) = {b}) c "
                f"JOIN (SELECT comment_id, modify_date FROM {versions} v "
                f"      WHERE ({has_value}) AND (hash(v.comment_id) & {m}) = {b}) v "
                f"ON {on_version}"
            ),
        )
        candidates = f"SELECT comment_id FROM {candidates_rel}"
        candidate_files = f"SELECT DISTINCT _file FROM {candidates_rel}"
        from spicy_regs.sources.regulatory_catalog import processing_table
        processing = processing_table(con, COMMENT,
                                      where=f'comment_id IN ({candidates}) AND {FILE_COLUMN} IN ({candidate_files})',
                                      include_comment_promotion="attachments_json" in additive)
        try:
            if iceberg._read_snapshot(con, COMMENT) != snapshot:
                raise RuntimeError('Catalog changed while preparing field fill; retry')
            missing = [c for c in columns if c not in con.sql(f'SELECT * FROM {processing}').columns]
            if missing:
                raise RuntimeError(f"the comments catalog lacks {missing} at snapshot {snapshot.snapshot_id}: deploy the "
                                   "branch and let one ETL data commit migrate the table before preparing "
                                   "(docs/comment-fields-fill.md)")
            nulls = ", ".join(f'p."{c}" IS NULL AS "null_{c}"' for c in columns)
            held = "".join(f', p."{c}" AS "held_{c}"' for c in additive)
            if additive:
                held += ", p._promoted_v2"
            _write_partitioned_parquet(
                con,
                cat_file,
                lambda b, m: (
                    f"SELECT p.comment_id, p.modify_date, {nulls}{held}, i._file "
                    f"FROM (SELECT * FROM {processing} WHERE (hash(comment_id) & {m}) = {b}) p "
                    f"JOIN (SELECT comment_id, _file FROM {candidates_rel} WHERE (hash(comment_id) & {m}) = {b}) i "
                    f"USING (comment_id)"
                ),
            )
        finally:
            con.execute(f"DROP TABLE {processing}")
        # Where a replaceable cell holds a value the version's agreed read differs from: whether the read only adds
        # to it, and the value it would replace. Few rows, so the rule runs in Python on these alone, and the held
        # value travels from here: carried through the whole-table join below it doubled that join's time (a
        # stand-in of the 26.3M-row catalog, 2026-10-03: 6.0 s against 13.2 s).
        for c in additive:
            con.create_function(f"_additive_{c}", ADDITIVE[c], ["VARCHAR", "VARCHAR", "BOOLEAN"], "BOOLEAN",
                                null_handling="special")
        if additive:
            _write_partitioned_parquet(
                con,
                differs_file,
                lambda b, m: " UNION ALL ".join(
                    f"""SELECT c.comment_id, c.modify_date, '{c}' AS "column",
                               _additive_{c}(v."{c}", c."held_{c}", c._promoted_v2) AS additive,
                               c."held_{c}" AS held
                        FROM (SELECT comment_id, modify_date, "held_{c}", _promoted_v2 FROM {catalog}
                              WHERE "held_{c}" IS NOT NULL AND (hash(comment_id) & {m}) = {b}) c
                        JOIN (SELECT comment_id, modify_date, "{c}" FROM {versions}
                              WHERE "{c}" IS NOT NULL AND (hash(comment_id) & {m}) = {b}) v
                        ON {on_version}
                        WHERE v."{c}" <> c."held_{c}\""""
                    for c in additive
                ),
            )
        else:
            none = ('SELECT NULL::VARCHAR AS comment_id, NULL::VARCHAR AS modify_date, NULL::VARCHAR AS "column", '
                    "NULL::BOOLEAN AS additive, NULL::VARCHAR AS held WHERE false")
            con.execute(f"COPY ({none}) TO {_sql(differs_file)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.execute(f"""COPY (SELECT comment_id, modify_date, "column" FROM {differs} WHERE NOT additive ORDER BY 1, 3)
                        TO {_sql(out / 'refusals.parquet')} (FORMAT PARQUET)""")
        replaces = {c: f"r_{c}.comment_id IS NOT NULL" for c in additive}
        fills = {c: f"""(c."null_{c}" AND v."{c}" IS NOT NULL)""" for c in columns}
        changes = {c: f"({fills[c]} OR {replaces[c]})" if c in replaces else fills[c] for c in columns}
        # Only the value to write, NULL elsewhere: a write that overwrote instead of filling would blank a cell. A
        # replacement also carries the value it replaces, which the write requires the row still to hold.
        filled = ", ".join(f'CASE WHEN {changes[c]} THEN v."{c}" END AS "{c}"' for c in columns)
        expected = "".join(f', r_{c}.held AS "_held_{c}"' for c in additive)
        # Unsorted: a sort of the 17.3M rows to fill (2026-10-03) spilled past 14 GB under a 3 GB budget, and a
        # batch reads this file filtered on ``_file`` whatever its order.
        _write_partitioned_parquet(
            con,
            out / "fill.parquet",
            lambda b, m: (
                f"SELECT c.comment_id, c.modify_date, c._file, {filled}{expected} "
                f"FROM (SELECT * FROM {catalog} WHERE (hash(comment_id) & {m}) = {b}) c "
                f"JOIN (SELECT * FROM {versions} WHERE (hash(comment_id) & {m}) = {b}) v "
                f"ON {on_version}"
                + "".join(
                    f""" LEFT JOIN (SELECT comment_id, modify_date, held FROM {differs}
                                    WHERE "column" = '{c}' AND additive AND (hash(comment_id) & {m}) = {b}) r_{c}
                         ON r_{c}.comment_id = c.comment_id AND r_{c}.modify_date IS NOT DISTINCT FROM c.modify_date"""
                    for c in additive
                )
                + f" WHERE {' OR '.join(changes.values())}"
            ),
        )
        # Exact accounting via set algebra on unique catalog comment_ids and unique (comment_id, modify_date) versions:
        # avoids correlated EXISTS / LEFT_DELIM_JOIN hash deduplication over the full 27M-row catalog.
        read_matched = con.execute(f"SELECT count(*) FROM {matched_index}").fetchone()[0]
        version_matched = 0
        for bucket in range(buckets):
            version_matched += con.execute(
                f"SELECT count(*) "
                f"FROM (SELECT comment_id, modify_date FROM {matched_index} "
                f"      WHERE (hash(comment_id) & {mask}) = {bucket}) c "
                f"JOIN (SELECT comment_id, modify_date FROM {versions} "
                f"      WHERE (hash(comment_id) & {mask}) = {bucket}) v "
                f"ON {on_version}"
            ).fetchone()[0]
        counts = (catalog_rows, catalog_rows - read_matched, read_matched - version_matched)
        fill_rows = f"read_parquet({_sql(out / 'fill.parquet')})"
        # A fill cell is non-NULL exactly where its rule held, so the fill file counts its own cells: a NULL filled
        # where it carries no replaced value, a replacement where it does.
        counted = [f'count("{c}") FILTER (WHERE "_held_{c}" IS NULL)' if c in replaces else f'count("{c}")'
                   for c in columns] + [f'count("_held_{c}")' for c in additive]
        to_fill, *by_column = con.execute(f"SELECT count(*), {', '.join(counted)} FROM {fill_rows}").fetchall()[0]
        cells = dict(zip(columns, by_column))
        replaced_cells = dict(zip(additive, by_column[len(columns):]))
        refused = dict(con.execute(f"""SELECT "column", count(*) FILTER (WHERE NOT additive) FROM {differs}
                                       GROUP BY 1""").fetchall())
        conflicted = dict(zip(columns, con.execute(
            f"SELECT {', '.join(f'count(*) FILTER (WHERE n_{c} > 1)' for c in columns)} FROM _spellings"
        ).fetchall()[0]))
        sizes = data_files(con)
        con.execute(f"""COPY (SELECT _file AS file, count(*) AS rows FROM {fill_rows}
                        GROUP BY 1 ORDER BY 1) TO {_sql(out / 'files.parquet')} (FORMAT PARQUET)""")
        files = con.execute(f"SELECT file, rows FROM read_parquet({_sql(out / 'files.parquet')})").fetchall()
        if iceberg._read_snapshot(con, COMMENT) != snapshot:
            raise RuntimeError('Catalog changed while preparing field fill; retry')
    finally:
        for c in additive:
            with suppress(Exception):
                con.remove_function(f"_additive_{c}")
        if own:
            con.close()
    unsized = [file for file, _ in files if sizes.get(file) is None]
    if unsized:
        raise RuntimeError(f"no size read for {len(unsized)} data files (first {unsized[0]}); the batches pack by "
                           "size, so prepare again")
    fill_sha256 = _sha256(out / "fill.parquet")
    receipt = {
        "prepare_id": f"{snapshot.snapshot_id}-{fill_sha256[:12]}-{secrets.token_hex(4)}", "fill_sha256": fill_sha256,
        "snapshot": snapshot.__dict__, "scope": scope or {}, "reads": str(read_from), "profile": profile,
        "columns": list(columns),
        "table_rows": table_rows, "catalog_rows": counts[0], "unread": counts[1], "other_version_only": counts[2],
        "rows_to_fill": to_fill, "cells_by_column": cells, "replaced_by_column": replaced_cells,
        "refused_not_additive_by_column": {c: refused.get(c, 0) for c in additive},
        "conflicted_versions_by_column": conflicted,
        "files": {file: {"rows": rows, "bytes": sizes[file]} for file, rows in files},
        "prepared_at": _now(),
    }
    (out / "prepare.json").write_text(json.dumps(receipt, indent=2) + "\n")
    shutil.rmtree(out / "work")
    return receipt


def prepare_empty_original_document_id(
    workdir: Path,
    *,
    scope: dict[str, str] | None = None,
    con=None,
    resources: ExportResources | None = None,
) -> dict:
    """Plan nullification of blank/whitespace-only ``original_document_id`` cells at one catalog snapshot.

    Reads only narrow columns (``comment_id``, ``modify_date``, ``original_document_id``, and ``filename``) from the
    catalog so the 27M-row live table prepares without wide payload scans or an external staged read. Only rows where
    ``original_document_id IS NOT NULL AND trim(original_document_id) = ''`` are scheduled to become ``NULL``;
    ``comment_reference_values_json`` and every other column remain completely untouched. Touches no catalog row and
    needs no lock.
    """
    _refuse_committed_failures(workdir)
    if scope:
        unknown = [column for column in scope if column not in COMMENT.schema]
        if unknown:
            raise ValueError(f"unknown scope column(s) for comments: {unknown}")
    own = con is None
    con = con or iceberg._connect()
    out = fill_dir(workdir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        (resources or WRITE_RESOURCES).configure(con, out / "spill")
        storage_access(con)
        namespace = iceberg._selected_catalog_namespace(con, COMMENT)
        snapshot = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        source = iceberg._snapshot_query(COMMENT, snapshot, namespace=namespace)
        if "original_document_id" not in con.sql(f"SELECT * FROM ({source}) LIMIT 0").columns:
            raise RuntimeError(f"the comments catalog lacks original_document_id at snapshot {snapshot.snapshot_id}")
        table_rows = con.execute(f"SELECT count(*) FROM ({source})").fetchone()[0]
        where = " AND ".join(f'"{column}" = {_sql(value)}' for column, value in (scope or {}).items()) or "TRUE"
        work = out / "work"
        work.mkdir(exist_ok=True)
        index_file = work / "catalog-index.parquet"
        index = f"read_parquet({_sql(index_file)})"
        physical = source.replace(
            "SELECT *",
            f"SELECT comment_id, modify_date, original_document_id, {FILE_COLUMN} AS _file",
            1,
        )
        con.execute(
            f"""COPY (SELECT comment_id, modify_date, original_document_id, _file
                      FROM ({physical} WHERE {where}))
                TO {_sql(index_file)} (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
        duplicated = _count_duplicate_comment_ids(con, index)
        if duplicated:
            raise RuntimeError(f"{duplicated} comment ids appear more than once in the catalog; run the dedupe first")
        con.execute(
            f"""COPY (
                SELECT comment_id, modify_date, _file,
                       CAST(NULL AS VARCHAR) AS original_document_id,
                       original_document_id AS _held_original_document_id
                FROM {index}
                WHERE {_is_blank_sql('original_document_id')}
            ) TO {_sql(out / 'fill.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
        (
            catalog_rows,
            empty_string_rows,
            whitespace_only_rows,
            non_empty_rows,
            already_null_rows,
        ) = con.execute(
            f"""SELECT count(*),
                       count(*) FILTER (WHERE original_document_id = ''),
                       count(*) FILTER (
                           WHERE original_document_id <> ''
                             AND {_is_blank_sql('original_document_id')}
                       ),
                       count(*) FILTER (
                           WHERE original_document_id IS NOT NULL
                             AND NOT {_is_blank_sql('original_document_id')}
                       ),
                       count(*) FILTER (WHERE original_document_id IS NULL)
                FROM {index}"""
        ).fetchone()
        fill_rows = f"read_parquet({_sql(out / 'fill.parquet')})"
        to_fill = con.execute(f"SELECT count(*) FROM {fill_rows}").fetchone()[0]
        if to_fill != empty_string_rows + whitespace_only_rows:
            raise RuntimeError(
                f"planned empty_original_document_id rows ({to_fill}) differ from blank count "
                f"({empty_string_rows + whitespace_only_rows})"
            )
        sizes = data_files(con)
        con.execute(
            f"""COPY (SELECT _file AS file, count(*) AS rows FROM {fill_rows}
                      GROUP BY 1 ORDER BY 1) TO {_sql(out / 'files.parquet')} (FORMAT PARQUET)"""
        )
        files = con.execute(f"SELECT file, rows FROM read_parquet({_sql(out / 'files.parquet')})").fetchall()
        if iceberg._read_snapshot(con, COMMENT, namespace=namespace) != snapshot:
            raise RuntimeError("Catalog changed while preparing empty_original_document_id nullification; retry")
    finally:
        if own:
            con.close()
    unsized = [file for file, _ in files if sizes.get(file) is None]
    if unsized:
        raise RuntimeError(
            f"no size read for {len(unsized)} data files (first {unsized[0]}); the batches pack by "
            "size, so prepare again"
        )
    fill_sha256 = _sha256(out / "fill.parquet")
    receipt = {
        "operation": EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION,
        "prepare_id": f"{snapshot.snapshot_id}-{fill_sha256[:12]}-{secrets.token_hex(4)}",
        "fill_sha256": fill_sha256,
        "snapshot": snapshot.__dict__,
        "scope": scope or {},
        "profile": EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION,
        "columns": ["original_document_id"],
        "table_rows": table_rows,
        "catalog_rows": catalog_rows,
        "rows_to_fill": to_fill,
        "empty_string_rows": empty_string_rows,
        "whitespace_only_rows": whitespace_only_rows,
        "non_empty_rows": non_empty_rows,
        "already_null_rows": already_null_rows,
        "cells_by_column": {"original_document_id": 0},
        "nullified_by_column": {"original_document_id": to_fill},
        "files": {file: {"rows": rows, "bytes": sizes[file]} for file, rows in files},
        "prepared_at": _now(),
    }
    (out / "prepare.json").write_text(json.dumps(receipt, indent=2) + "\n")
    shutil.rmtree(out / "work")
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
    if "columns" not in prepared:
        raise RuntimeError("prepare.json names no fill columns (it predates fill profiles); prepare again")
    _prepared_operation(prepared)
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
    missing = [c for c in prepared["columns"] if c not in dict(SOURCE_COLUMNS[COMMENT.name])]
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
    operation = _prepared_operation(prepared)
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
    with TemporaryDirectory(prefix="comment-fill-batch-") as batch_tmp:
        batch_work = Path(batch_tmp)
        filled_file = batch_work / f"{prepared['prepare_id']}-{batch}-filled.parquet"
        con.execute("BEGIN")
        try:
            current = iceberg._read_snapshot(con, COMMENT)
            if current.__dict__ != expected:
                raise RuntimeError(f"the comments catalog moved from {expected} to {current.__dict__}: another writer or "
                                   "R2 compaction committed; prepare again under the lock")
            before_files = live_files(con) if by_file else None
            con.execute(f"CREATE OR REPLACE TEMP VIEW _batch AS SELECT * FROM read_parquet({_sql(fill)}) "
                        f"WHERE _file IN ({file_list})")
            from spicy_regs.sources import regulatory_catalog as native
            selected_ids = f'SELECT comment_id FROM {table} WHERE {in_files}comment_id IN (SELECT comment_id FROM _batch)'
            preimage_proc_dir = batch_work / "preimage-processing"
            processing = native.processing_table(
                con, COMMENT, where=f'{in_files}comment_id IN ({selected_ids})',
                in_transaction=True, output_dir=preimage_proc_dir,
            )
            native._copy(
                con, f"SELECT {col_list} FROM {processing}", preimage,
                resources=native._layout(COMMENT.name), ordered=False,
            )
            native._drop_relation(con, processing)
            shutil.rmtree(preimage_proc_dir, ignore_errors=True)
            con.execute(f"CREATE OR REPLACE TEMP VIEW _prior AS SELECT * FROM read_parquet({_sql(preimage)})")
            kept = {"preimage": str(preimage), "preimage_sha256": _sha256(preimage)}
            need = " OR ".join(_changes(c, "p", "b", operation=operation) for c in prepared["columns"])
            planned = con.execute(f"""SELECT count(*) FROM _prior p JOIN _batch b
                ON p.comment_id = b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date WHERE {need}""").fetchone()[0]
            _append(journal, {"state": "pending", **ids, "planned": planned, "snapshot_before": current.__dict__, **kept,
                              "before_files": sorted(before_files or ()), "by_file": by_file})
            changed = 0
            if planned:
                values = ", ".join(
                    f'{_filled(c, "p", "b", operation=operation)} AS "{c}"' if c in prepared["columns"] else f'p."{c}"'
                    for c in columns
                )
                filled_sql = (
                    f"SELECT {values} FROM _prior p JOIN _batch b "
                    f"ON p.comment_id=b.comment_id AND p.modify_date IS NOT DISTINCT FROM b.modify_date WHERE {need}"
                )
                native._copy(
                    con, filled_sql, filled_file,
                    resources=native._layout(COMMENT.name), ordered=False,
                )
                con.execute(f"CREATE OR REPLACE TEMP VIEW _filled AS SELECT * FROM read_parquet({_sql(filled_file)})")
                if con.execute("SELECT count(*) FROM _filled").fetchone()[0] != planned:
                    raise FillVerificationError("Replacement rows differ from planned count")
                try:
                    native.replace_native(
                        con,
                        COMMENT,
                        "_filled",
                        expected_prior="_prior",
                        in_transaction=True,
                        source_files=files if by_file else None,
                        before_files=before_files if by_file else None,
                        file_column=FILE_COLUMN,
                    )
                except (ValueError, RuntimeError) as error:
                    raise FillVerificationError(f"Native subject and receipt differ: {error}") from error
                con.execute("DROP VIEW _filled")
                filled_file.unlink(missing_ok=True)
                changed = planned
                _check(
                    con,
                    table,
                    columns,
                    prepared["columns"],
                    files,
                    before_files,
                    by_file,
                    prepared["table_rows"],
                    postimage=postimage,
                    operation=operation,
                )
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
        finally:
            filled_file.unlink(missing_ok=True)
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


def _check(con, table: str, columns: list[str], filled: list[str], files: list[str], before_files: set[str] | None,
           by_file: bool, table_rows: int, *, postimage: Path, at: int | None = None,
           operation: str = FILL_OPERATION) -> None:
    """Inside the transaction: the batch's rows equal the pre-image with the fill applied to its ``filled`` columns,
    new files hold nothing else, and the table holds the ``table_rows`` it held at prepare.

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
    check_where = (
        f"{where} AND comment_id IN (SELECT comment_id FROM _prior)"
        if by_file else "comment_id IN (SELECT comment_id FROM _prior)"
    )
    with TemporaryDirectory(prefix="comment-fill-check-") as check_tmp:
        table = native.processing_table(
            con, COMMENT, where=check_where, in_transaction=at is None, output_dir=Path(check_tmp)
        )
        # Rows are compared by key and whole-row digest: a multiset of narrow pairs, not of wide rows (live, 2026-09-29:
        # the wide EXCEPT ALL took 206-301 s of a 4.5-5.5M-row batch's 260-360 s).
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _got AS SELECT comment_id, {_columns_md5(columns)} AS row_md5
                        FROM {table} WHERE comment_id IN (SELECT comment_id FROM _prior)""")
        con.execute(f"COPY _got TO {_sql(postimage)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        native._drop_relation(con, table)
    want = {c: _filled(c, "p", "b", operation=operation) if c in filled else f'p."{c}"' for c in columns}
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


def _require_sha256_pin(label: str, value: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise ValueError(f"{label} must be a 64-character lowercase hex SHA-256 pin")
    return value


def _has_symlink_under(root: Path, target: Path) -> bool:
    cur = root
    for part in target.relative_to(root).parts:
        cur = cur / part
        if cur.is_symlink():
            return True
    return False


def _verify_receipt_member(
    receipt_dir: Path, receipt_root: Path, pinned_files: Mapping[str, Any], path: Path, label: str,
) -> str:
    if not path.is_file() or path.is_symlink():
        raise RuntimeError(f"{label} is missing or invalid relative to the verified recovery receipt")
    abs_path = path.absolute()
    if abs_path.is_relative_to(receipt_dir):
        matched_root = receipt_dir
    elif abs_path.is_relative_to(receipt_root):
        matched_root = receipt_root
    else:
        raise RuntimeError(f"{label} lies outside the verified recovery receipt root")
    rel = abs_path.relative_to(matched_root)
    if not rel.parts or ".." in rel.parts or _has_symlink_under(matched_root, abs_path):
        raise RuntimeError(f"{label} traverses a symlink inside the verified recovery receipt root")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(receipt_root) or resolved.relative_to(receipt_root) != rel:
        raise RuntimeError(f"{label} resolves outside or differently from the verified recovery receipt root")
    member_key = str(rel)
    entry = pinned_files.get(member_key)
    if not isinstance(entry, dict):
        raise RuntimeError(f"{label} is absent from the verified recovery receipt")
    expected_sha = entry.get("sha256")
    expected_bytes = entry.get("bytes")
    if not isinstance(expected_sha, str) or type(expected_bytes) is not int:
        raise RuntimeError(f"{label} has an invalid pin entry in the verified recovery receipt")
    _require_sha256_pin(f"receipt[{member_key}].sha256", expected_sha)
    if resolved.stat().st_size != expected_bytes or _sha256(resolved) != expected_sha:
        raise RuntimeError(f"{label} differs from the pinned recovery receipt")
    return expected_sha


def reconcile_pending(
    workdir: Path,
    *,
    receipt_path: Path,
    receipt_sha256: str,
    resources: ExportResources | None = None,
    con=None,
) -> dict:
    """Reconcile a receipt-verified pending write journal entry against the live catalog without starting new batches."""
    if not getenv(iceberg.CATALOG_LOCK_ENV):
        raise RuntimeError(f"write holds the comments catalog: set {iceberg.CATALOG_LOCK_ENV} only while holding the "
                           "lock (docs/comment-fields-fill.md)")
    _require_sha256_pin("receipt_sha256", receipt_sha256)
    receipt_path = Path(receipt_path)
    if not receipt_path.is_file() or receipt_path.is_symlink() or _sha256(receipt_path) != receipt_sha256:
        raise RuntimeError("recovery receipt is missing or differs from receipt_sha256")
    receipt = json.loads(receipt_path.read_text())
    pinned_files = receipt.get("files") if isinstance(receipt, dict) else None
    if not isinstance(pinned_files, dict):
        raise RuntimeError("recovery receipt lacks a valid files map")
    receipt_dir = receipt_path.parent.absolute()
    receipt_root = receipt_path.parent.resolve(strict=True)
    out = fill_dir(workdir)
    prepare_path = out / "prepare.json"
    _verify_receipt_member(receipt_dir, receipt_root, pinned_files, prepare_path, "prepare.json")
    prepared = json.loads(prepare_path.read_text())
    if _sha256(out / "fill.parquet") != prepared["fill_sha256"]:
        raise RuntimeError("fill.parquet is not the one prepare.json names; prepare again")
    if "columns" not in prepared:
        raise RuntimeError("prepare.json names no fill columns (it predates fill profiles); prepare again")
    _prepared_operation(prepared)
    _refuse_committed_failures(workdir)
    journal = _journal(workdir, prepared)
    _verify_receipt_member(receipt_dir, receipt_root, pinned_files, journal, journal.name)
    lines = _lines(journal)
    pending = lines[-1] if lines and lines[-1]["state"] == "pending" else None
    if pending is None:
        raise ValueError(f"no pending write to reconcile in {journal.name}")
    if pending.get("prepare_id") != prepared["prepare_id"]:
        raise RuntimeError(f"pending batch {pending.get('batch')} in {journal.name} differs from prepare.json; stop (runbook)")
    preimage = Path(pending["preimage"]) if pending.get("preimage") else None
    if preimage is None:
        raise RuntimeError(f"the pre-image of pending batch {pending.get('batch')} is missing; stop (runbook)")
    pinned_preimage_sha = _verify_receipt_member(
        receipt_dir, receipt_root, pinned_files, preimage, f"the pre-image of pending batch {pending.get('batch')}",
    )
    if pending.get("preimage_sha256") != pinned_preimage_sha:
        raise RuntimeError(f"the pre-image of pending batch {pending.get('batch')} differs from the journal; stop (runbook)")
    verified = [line for line in lines if line["state"] == "verified"]
    expected = verified[-1]["snapshot_after"] if verified else prepared["snapshot"]
    own = con is None
    con = con or iceberg._connect()
    resources = resources or WRITE_RESOURCES
    resources.configure(con, out / "spill")
    storage_access(con)
    table = iceberg._qualified(COMMENT)
    try:
        after = _recover(con, journal, pending, prepared, table, expected)
    finally:
        if own:
            con.close()
    terminal = _lines(journal)[-1]
    return {
        "outcome": terminal["state"],
        "batch": pending["batch"],
        "prepare_id": prepared["prepare_id"],
        "snapshot_before": pending["snapshot_before"],
        "snapshot_after": after,
    }


def _recover(con, journal: Path, pending: dict, prepared: dict, table: str, expected: dict) -> dict:
    """A run that stopped after journaling a batch as pending: find whether that batch committed, and account for it."""
    ids = {k: pending.get(k) for k in ("batch", "files", "prepare_id")}
    if ids["prepare_id"] != prepared["prepare_id"]:
        raise RuntimeError(f"pending batch {ids['batch']} in {journal.name} differs from prepare.json; stop (runbook)")
    preimage = Path(pending["preimage"]) if pending.get("preimage") else None
    if preimage is None or not preimage.is_file() or _sha256(preimage) != pending.get("preimage_sha256"):
        raise RuntimeError(f"the pre-image of pending batch {ids['batch']} is missing or differs from the journal; "
                           "stop (runbook)")
    kept = {k: pending[k] for k in ("preimage", "preimage_sha256")}
    current = iceberg._read_snapshot(con, COMMENT).__dict__
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
    con.execute(f"CREATE OR REPLACE TEMP VIEW _prior AS SELECT * FROM read_parquet({_sql(kept['preimage'])})")
    file_list = ", ".join(map(_sql, pending["files"]))
    con.execute(f"CREATE OR REPLACE TEMP VIEW _batch AS SELECT * FROM read_parquet("
                f"{_sql(fill_dir(journal.parent.parent) / 'fill.parquet')}) WHERE _file IN ({file_list})")
    postimage = preimage.with_name(f"{preimage.stem}-written.parquet")
    try:
        _check(con, table, list(COMMENT.schema), prepared["columns"], pending["files"], set(pending["before_files"]),
               pending["by_file"], prepared["table_rows"], postimage=postimage, at=after["snapshot_id"],
               operation=_prepared_operation(prepared))
    except FillVerificationError as error:
        has_postimage = postimage.exists()
        kept |= {"postimage": str(postimage) if has_postimage else None,
                 "postimage_sha256": _sha256(postimage) if has_postimage else None}
        _append(journal, {"state": "failed", **ids, "committed": True, "reason": f"recovered commit: {error}", **kept,
                          "snapshot_before": pending["snapshot_before"], "snapshot_after": after})
        raise FillVerificationError(f"committed batch {ids['batch']} failed verification: {error}") from error
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
        if not line.get(name):
            raise RuntimeError(f"batch {batch} has no recorded {name}; restore it by hand (runbook)")
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
        con.execute(f"CREATE OR REPLACE TEMP VIEW _undo_pre AS SELECT {col_list} FROM read_parquet("
                    f"{_sql(images['preimage'])})")
        con.execute(f"CREATE OR REPLACE TEMP TABLE _undo_written AS SELECT * FROM read_parquet("
                    f"{_sql(images['postimage'])})")
        undo_files = [
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT {FILE_COLUMN} FROM {table} "
                f"WHERE comment_id IN (SELECT comment_id FROM _undo_pre) AND {FILE_COLUMN} IS NOT NULL"
            ).fetchall()
        ]
        undo_before_files = live_files(con) if undo_files else None
        if undo_files:
            undo_files_sql = ", ".join(map(_sql, undo_files))
            undo_where = f"{FILE_COLUMN} IN ({undo_files_sql}) AND comment_id IN (SELECT comment_id FROM _undo_pre)"
        else:
            undo_where = "FALSE"
        from spicy_regs.sources import regulatory_catalog as native
        with TemporaryDirectory(prefix="comment-fill-undo-") as undo_tmp:
            table = native.processing_table(con, COMMENT, where=undo_where, output_dir=Path(undo_tmp))
            con.execute(f"""CREATE OR REPLACE TEMP TABLE _undo_now AS SELECT {col_list} FROM {table}
                            WHERE comment_id IN (SELECT comment_id FROM _undo_pre)""")
            native._drop_relation(con, table)
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
            iceberg.replace_rows(
                con,
                COMMENT,
                "_undo_source",
                expected_prior="_undo_expected",
                expected_snapshot=current,
                source_files=undo_files or None,
                before_files=undo_before_files,
                file_column=FILE_COLUMN,
            )
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


def _cli_app():
    from cyclopts import App

    cli = App(name="comment-fields-write", help=__doc__)

    @cli.command(name="prepare-empty-original-document-id")
    def prepare_empty_original_document_id_command(
        *, workdir: Path, docket: str | None = None, agency: str | None = None,
    ) -> None:
        """Prepare nullification of empty/whitespace-only original_document_id cells (no write)."""
        from dotenv import load_dotenv

        load_dotenv()
        scope = {k: v for k, v in (("docket_id", docket), ("agency_code", agency)) if v}
        print(json.dumps(prepare_empty_original_document_id(workdir, scope=scope or None), indent=2))

    @cli.command(name="write")
    def write_command(
        *, workdir: Path, batch_bytes: int = BATCH_BYTES, by_file: bool = True, clear_failure: bool = False,
    ) -> None:
        """Apply the prepared comment catalog write under the catalog lock."""
        from dotenv import load_dotenv

        load_dotenv()
        print(json.dumps(write(workdir, batch_bytes=batch_bytes, by_file=by_file, clear_failure=clear_failure), indent=2))

    @cli.command(name="undo")
    def undo_command(*, workdir: Path, batch: str, expected_snapshot: int) -> None:
        """Restore a committed batch from its pre-image at ``--expected-snapshot``."""
        from dotenv import load_dotenv

        load_dotenv()
        print(json.dumps(undo(workdir, batch, expected_snapshot=expected_snapshot), indent=2))

    @cli.command(name="journals")
    def journals_command(*, workdir: Path, prefix: str, push: bool = False) -> None:
        """Pull or push write journals in the data bucket under ``--prefix``."""
        from dotenv import load_dotenv

        load_dotenv()
        print(json.dumps(sync_journals(workdir, prefix, push=push)))

    return cli


if __name__ == "__main__":
    _cli_app()()
