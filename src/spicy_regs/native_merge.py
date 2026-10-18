"""Upsert a fresh native subject/receipt pair over a prior one without restoring either to rows.

A nightly that fetches only a window (lobbying's posted-date window, a day's regulatory attribute parts) has the prior
generation's subject and receipt files already admitted. :func:`native_merge` is that merge at the native level, all in
DuckDB and Arrow:

* subjects are the prior ones whose identity the fresh pair does not carry, plus the fresh ones, in ``order_by`` order
  (what ``table_merge.merge_local_prior`` yields on the same columns);
* accepted receipts are those of the kept prior subjects, verbatim, plus ``carry_receipt_history`` of the fresh receipts
  against the prior receipts they replace: a refetched row that did not change keeps its prior receipt, a changed one
  references only its direct predecessor, a new one is fresh. Accepted receipts follow their subjects' order, as the
  restore readers expect; the fresh pair's other receipts (refusals, observations) come last, in their own order.

Receipt work is O(fresh rows). What stays O(all rows) is one Arrow/DuckDB copy and the sort. The caller admits the
result once, as it admits any generation; nothing here validates it beyond refusing a subject with no receipt.
The prior pair must already be admitted, and its receipts scoped to the dataset.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, DatasetPolicy, subject_identity
from spicy_regs.receipt_history import carry_receipt_history

_BATCH = 50_000


class Merged(NamedTuple):
    subjects: Path
    receipts: Path
    kept: int  # prior subjects the fresh pair did not replace
    replaced: int  # prior subjects it did
    added: int  # fresh subjects with no prior


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _identities(con, policy: DatasetPolicy, view: str, path: Path) -> None:
    """``<view>_ids(_o, _rid)``: each subject's record id, from SQL where it spells as Python does, else from Python."""
    record_id, _, _ = etl_bulk.identity_sql(policy)
    reference = etl_bulk._subject_reference_sql(policy)
    con.execute(
        f"CREATE TABLE {view}_ids AS SELECT _o, {record_id} AS _rid, ({reference}) IS NOT FALSE AS _ref FROM {view}"
    )
    routed = {ordinal for (ordinal,) in con.execute(f"SELECT _o FROM {view}_ids WHERE _ref").fetchall()}
    if routed:
        decided, start = [], 0
        with pq.ParquetFile(path) as source:
            for batch in source.iter_batches(batch_size=_BATCH):
                for offset, row in enumerate(batch.to_pylist()):
                    if start + offset in routed:
                        decided.append((subject_identity(policy, row)[0], start + offset))
                start += batch.num_rows
        con.executemany(f"UPDATE {view}_ids SET _rid = ?, _ref = false WHERE _o = ?", decided)
    if con.execute(f"SELECT count(*) - count(DISTINCT _rid) FROM {view}_ids").fetchone()[0]:
        raise ValueError(f"{policy.dataset}: {view} subjects repeat an identity")


def _write(con, sql: str, path: Path, schema: pa.Schema, *, statistics: Sequence[str] | bool = True) -> int:
    """Stream ``sql``'s ordered rows into ``path`` as ``schema``; return how many."""
    rows = 0
    with pq.ParquetWriter(path, schema, compression="zstd", write_statistics=statistics) as writer:
        for batch in con.execute(sql).to_arrow_reader(_BATCH):
            writer.write_table(pa.Table.from_batches([batch]).select(schema.names).cast(schema))
            rows += batch.num_rows
    return rows


def native_merge(
    policy: DatasetPolicy,
    *,
    fresh: tuple[Path, Path],
    prior: tuple[Path, Path] | None,
    order_by: Sequence[str],
    subjects: Path,
    receipts: Path,
) -> Merged:
    """Write the merged pair to ``subjects`` and ``receipts``; ``fresh`` and ``prior`` are ``(subjects, receipts)`` files.

    ``prior=None`` is a cold start: the fresh pair, sorted. ``order_by`` names subject columns, ascending.
    """
    if policy.receipt_only:
        raise ValueError(f"{policy.dataset}: a receipt-only dataset has no native subjects to merge")
    names = policy.subject_schema.names
    if any(name.startswith("_") for name in names) or not set(order_by) <= set(names) or not order_by:
        raise ValueError(f"{policy.dataset}: order_by must name subject columns, and subject columns may not start with _")
    fresh_subjects, fresh_receipts = fresh
    prior_subjects, prior_receipts = prior if prior is not None else (None, None)
    lit = etl_bulk._literal
    keys = ", ".join(_q(name) for name in order_by)
    columns = ", ".join(_q(name) for name in names)
    receipt_columns = ", ".join(_q(name) for name in RECEIPT_SCHEMA.names)

    with etl_bulk.bulk_connection(Path(subjects).parent) as (con, root):
        con.execute("SET preserve_insertion_order = false")
        def view(name, path, cold=False):
            con.execute(f"CREATE VIEW {name} AS SELECT *, file_row_number AS _o "
                        f"FROM read_parquet({lit(str(path))}, file_row_number = true)" + (" WHERE false" if cold else ""))

        view("fresh_s", fresh_subjects)
        view("fresh_r", fresh_receipts)
        view("prior_s", prior_subjects or fresh_subjects, cold=prior is None)
        view("prior_r", prior_receipts or fresh_receipts, cold=prior is None)
        if con.execute(f"SELECT count(*) FROM prior_r WHERE dataset <> {lit(policy.dataset)}").fetchone()[0]:
            raise ValueError(f"{policy.dataset}: prior receipts must be scoped to the dataset")
        _identities(con, policy, "fresh_s", fresh_subjects)
        if prior is None:
            con.execute("CREATE TABLE prior_s_ids (_o BIGINT, _rid VARCHAR)")
        else:
            assert prior_subjects is not None
            _identities(con, policy, "prior_s", prior_subjects)

        # Receipts the fresh pair replaces, as the history carrier's prior: the prior accepted receipts of replaced
        # identities and every non-accepted one (a refusal or observation repeated verbatim keeps its prior receipt).
        replaced = root / "replaced.parquet"
        replaced_rows = _write(
            con,
            f"SELECT {receipt_columns} FROM prior_r WHERE outcome <> 'accepted' OR record_id IN "
            "(SELECT record_id FROM fresh_r WHERE outcome = 'accepted') ORDER BY _o",
            replaced, RECEIPT_SCHEMA)
        carried = carry_receipt_history(Path(fresh_receipts), [replaced] if replaced_rows else [], root / "carried.parquet")
        con.execute(f"CREATE VIEW carried_r AS SELECT *, file_row_number AS _o "
                    f"FROM read_parquet({lit(str(carried))}, file_row_number = true)")

        con.execute(
            f"CREATE TABLE merged AS SELECT _src, _o, _rid, row_number() OVER (ORDER BY {keys}, _src, _o) AS _pos FROM ("
            f"SELECT 1 AS _src, _o, _rid, {keys} FROM fresh_s JOIN fresh_s_ids USING (_o) UNION ALL "
            f"SELECT 0, _o, _rid, {keys} FROM prior_s JOIN prior_s_ids USING (_o) "
            "WHERE _rid NOT IN (SELECT _rid FROM fresh_s_ids))")
        kept, added, total = con.execute(
            "SELECT count(*) FILTER (WHERE _src = 0), count(*) FILTER (WHERE _src = 1 AND _rid NOT IN "
            "(SELECT _rid FROM prior_s_ids)), count(*) FROM merged").fetchone()
        written = _write(
            con,
            f"SELECT {columns} FROM (SELECT {columns}, 0 AS _src, _o FROM prior_s UNION ALL BY NAME "
            f"SELECT {columns}, 1 AS _src, _o FROM fresh_s) JOIN merged USING (_src, _o) ORDER BY _pos",
            subjects, policy.subject_schema)
        assert written == total
        accepted = _write(
            con,
            f"SELECT {receipt_columns} FROM ("
            f"SELECT _pos, {receipt_columns} FROM merged JOIN prior_r ON merged._src = 0 AND prior_r.record_id = merged._rid "
            "AND prior_r.outcome = 'accepted' UNION ALL "
            f"SELECT _pos, {receipt_columns} FROM merged JOIN carried_r ON merged._src = 1 AND carried_r.record_id = merged._rid "
            "AND carried_r.outcome = 'accepted') ORDER BY _pos",
            root / "accepted.parquet", RECEIPT_SCHEMA)
        if accepted != total:
            raise ValueError(f"{policy.dataset}: {total} merged subjects but {accepted} accepted receipts for them")
        others = _write(
            con, f"SELECT {receipt_columns} FROM carried_r WHERE outcome <> 'accepted' ORDER BY _o",
            root / "others.parquet", RECEIPT_SCHEMA)
        with pq.ParquetWriter(receipts, RECEIPT_SCHEMA, compression="zstd", write_statistics=etl_bulk._STATISTICS) as out:
            for part, rows in ((root / "accepted.parquet", accepted), (root / "others.parquet", others)):
                if rows:
                    with pq.ParquetFile(part) as source:
                        for batch in source.iter_batches(batch_size=_BATCH):
                            out.write_batch(batch)
    return Merged(Path(subjects), Path(receipts), kept, total - kept - added, added)
