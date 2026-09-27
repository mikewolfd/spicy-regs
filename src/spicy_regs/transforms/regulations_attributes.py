"""Project each Regulations.gov record's attributes while the ETL reads it (owner decisions 65-67).

The ETL's document and docket passes read each Mirrulations record, a Regulations.gov API detail response, once.
:class:`TeeAttributes` reads the keyed stream, passes each raw payload on to the thin-table extract and projects its
``data.attributes`` through SpicyDocs' contract projection into a part file of the attributes table under the staging
directory, in bounded batches: one read, two rows. The part files merge with the table's working copy like any other
staging.

The mirror can hold one record id in more than one file (174 of BIS's 88,330 document files, 2026-09-27). Each part
row carries the copy's ``modifyDate``, its S3 write time and SpicyDocs' record digest, and the merge chooses among a
record's copies by SpicyDocs' volatile-tie rule (:func:`newest_copy_sql`). Those columns never leave the merge. The
thin tables choose their copy independently (``merge_staging_files``), so an id's thin and attribute rows can come
from different copies, but they can differ only on fields the thin table lacks: every tie measured on 2026-09-27
differed only in ``openForComment``/``withinCommentPeriod``, and SpicyDocs refuses a tie that differs in anything else.

A record the contract's projection refuses (``TableContractError``: an empty id, a list where text belongs, an
impossible date) loses only its attributes row. The payload still reaches the thin table, and the tee keeps the id
and reason in ``refused`` for its caller to report, so one malformed record never stops the base ETL.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from spicy_regs.transforms.base import Transform

if TYPE_CHECKING:
    import pyarrow as pa
    from spicy_docs.sources.mirrulations import KeyedPayload

#: The attributes table each base record type feeds, keyed by that record type's name.
ATTRIBUTE_TABLES = {"documents": "document_attributes", "dockets": "docket_attributes"}
#: Part-file columns that order a record's copies in the merge, with their Arrow types; never published.
ORDER_COLUMNS = {"_modify_date": "string", "_written_at": "int64", "_record_digest": "string"}


def _with_order_columns(schema: pa.Schema) -> pa.Schema:
    import pyarrow as pa

    for column, type_name in ORDER_COLUMNS.items():
        schema = schema.append(pa.field(column, getattr(pa, type_name)()))
    return schema


def _projection_and_digest(table: str):
    """The table's contract projection and SpicyDocs' record digest for its collection."""
    from spicy_docs.schemas.regulations_attribute_tables import project_docket_attributes, project_document_attributes
    from spicy_docs.source_native.regulations_gov import docket_source_record_digest, document_source_record_digest

    return {
        "document_attributes": (project_document_attributes, document_source_record_digest),
        "docket_attributes": (project_docket_attributes, docket_source_record_digest),
    }[table]


def contract(table: str):
    from spicy_docs.schemas import TABLE_CONTRACTS

    return TABLE_CONTRACTS[table]


class TeeAttributes(Transform):
    """Yield every keyed record's bare payload, writing its projected attributes row to ``staging_dir/<table>/``."""

    keyed = True

    def __init__(self, table: str, staging_dir: Path, *, batch_size: int = 2_000) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self.table, self.staging_dir, self.batch_size = table, staging_dir, batch_size
        self.rows_written = 0
        self.refused: list[tuple[str | None, str]] = []

    def apply(self, records: Iterable[KeyedPayload]) -> Iterator[dict]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        from spicy_docs.schemas.tables import TableContractError
        from loguru import logger

        from spicy_regs.contract_types import arrow_schema

        project, digest = _projection_and_digest(self.table)
        schema = _with_order_columns(arrow_schema(contract(self.table)))
        target = self.staging_dir / self.table / f"part-{uuid4().hex}.parquet"
        writer = None
        batch: list[dict] = []

        def flush() -> None:
            nonlocal writer
            if not batch:
                return
            if writer is None:
                target.parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(target, schema, compression="zstd")
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            self.rows_written += len(batch)
            batch.clear()

        try:
            for record in records:
                payload = record.payload
                data = payload.get("data") or {}
                attributes = data.get("attributes") or {}
                try:
                    row = project(data.get("id"), attributes)
                except TableContractError as error:
                    self.refused.append((data.get("id"), str(error)[:300]))
                    yield payload
                    continue
                row["_modify_date"] = attributes.get("modifyDate")
                row["_written_at"] = int(record.last_modified.timestamp()) if record.last_modified else None
                try:
                    row["_record_digest"] = digest(payload)
                except ValueError:  # canonical JSON refuses it (a float); SpicyDocs would too. It loses any tie.
                    row["_record_digest"] = None
                batch.append(row)
                if len(batch) >= self.batch_size:
                    flush()
                yield payload
            flush()
        finally:
            if writer is not None:
                writer.close()
            if self.refused:
                logger.warning("{}: {} records refused by the contract, e.g. {}", self.table, len(self.refused),
                               self.refused[:3])


#: Rows per written row group. A document row reaches 900 KB (a long comment); 20,000 rows keeps a group far
#: below DocSpec's admission bound, which ``regulatory_base`` checks before a family publishes.
ROW_GROUP_ROWS = 20_000


def newest_copy_sql(source: str, table: str) -> str:
    """One row per identity from ``source`` (SQL), in the contract's columns, by SpicyDocs' volatile-tie rule.

    The newest ``_modify_date`` wins. Among the copies tied at it, when every one states its write time, a copy
    written more than ``VOLATILE_TIE_MARGIN_SECONDS`` before the newest is out; the smallest record digest among the
    rest wins, as ``spicy_docs.releases.observations.volatile_tie_choice`` chooses (document policy 1.3). So a copy
    written over an hour after every other wins, and copies the April 2025 bulk upload wrote seconds apart take the
    content choice, not a guessed order. The tie key is the raw ``modifyDate`` text, NULLs last; SpicyDocs ties on
    the normalized UTC instant and falls back to ``postedDate``, so one instant spelled two ways, or a missing
    ``modifyDate``, can group differently there.
    """
    from spicy_docs.source_native.regulations_gov import VOLATILE_TIE_MARGIN_SECONDS

    table_contract = contract(table)
    columns = ", ".join(f'"{column}"' for column in table_contract.columns)
    keys = ", ".join(f'"{column}"' for column in table_contract.identity)
    return (
        f"SELECT {columns} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} "
        "ORDER BY _modify_date DESC NULLS LAST, _outside_margin, _record_digest NULLS LAST) AS _rn "
        "FROM (SELECT *, count(_written_at) OVER tie = count(*) OVER tie "
        f"AND max(_written_at) OVER tie - _written_at > {VOLATILE_TIE_MARGIN_SECONDS} AS _outside_margin "
        f"FROM {source} WINDOW tie AS (PARTITION BY {keys}, _modify_date))) WHERE _rn = 1"
    )


def merge_attribute_parts(table: str, parts: Path, prior: Path | None, out: Path, *, keep_order: bool = False) -> int:
    """Merge the part files under ``parts`` over ``prior`` into ``out`` by the contract's identity; the fresh row wins.

    Types are the contract's: the parts are written with its Arrow schema, and DuckDB keeps each column's type
    through the union. ``keep_order`` writes a table with no prior that keeps every copy and ``ORDER_COLUMNS``, for a
    sweep shard: the mirror files a few documents under two agencies, and ``combine`` must choose among all of an
    id's copies at once, because the margin is measured from the newest copy, which may be in another shard.
    Returns the rows written.
    """
    import duckdb
    import pyarrow.parquet as pq

    from spicy_regs.contract_types import arrow_schema
    from spicy_regs.transforms.table_merge import merge_local_prior

    if keep_order and prior is not None:
        raise ValueError("keep_order writes a whole table; it takes no prior")
    table_contract = contract(table)
    keys = ", ".join(f'"{column}"' for column in table_contract.identity)
    target = str(out).replace("'", "''")
    if not any(parts.glob("*.parquet")):
        # No record projected: an empty table on first write, else the prior unchanged.
        if prior is None:
            schema = arrow_schema(table_contract)
            pq.write_table((_with_order_columns(schema) if keep_order else schema).empty_table(), out,
                           compression="zstd")
        elif prior != out:
            out.write_bytes(prior.read_bytes())
        return pq.ParquetFile(out).metadata.num_rows
    source = f"read_parquet('{str(parts / '*.parquet').replace(chr(39), chr(39) * 2)}')"
    with duckdb.connect() as con:
        con.execute("SET preserve_insertion_order=false")
        if keep_order:
            columns = ", ".join(f'"{column}"' for column in (*table_contract.columns, *ORDER_COLUMNS))
            con.execute(f"COPY (SELECT {columns} FROM {source} ORDER BY {keys}) TO '{target}' "
                        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_ROWS})")
            return pq.ParquetFile(out).metadata.num_rows
        staged = out.with_name(f".{out.name}.merging")
        fresh = out.with_name(f".{out.name}.fresh")
        con.execute(f"COPY ({newest_copy_sql(source, table)}) TO "
                    f"'{str(fresh).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        merge_local_prior(
            con,
            columns=table_contract.columns,
            identity=table_contract.identity,
            order_by=keys,
            prior_file=prior,
            new_file=fresh,
            out_file=staged,
            row_group_size=ROW_GROUP_ROWS,
        )
    fresh.unlink()
    staged.replace(out)
    return pq.ParquetFile(out).metadata.num_rows
