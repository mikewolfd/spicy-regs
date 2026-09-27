"""Project each Regulations.gov record's attributes while the ETL reads it (owner decisions 65-67).

The ETL's document and docket passes read each Mirrulations record, a Regulations.gov API detail response, once.
:class:`TeeAttributes` passes the raw payload on unchanged to the thin-table extract and projects its
``data.attributes`` through SpicyDocs' contract projection into a part file of the attributes table under the staging
directory, in bounded batches: one read, two rows. The part files merge with the table's working copy like any other
staging.

The mirror can hold one record id in more than one file (174 of BIS's 88,330 document files, 2026-09-27). The thin
tables keep the copy with the newest ``modifyDate`` (``merge_staging_files``), so each part row also carries that
date and a digest of its attributes, and the merge keeps the newest date, then the higher digest, so the pick matches
the thin table's and is the same every run. Those two columns never leave the merge.

A record the contract's projection refuses (``TableContractError``: an empty id, a list where text belongs, an
impossible date) loses only its attributes row. The payload still reaches the thin table, and the tee keeps the id
and reason in ``refused`` for its caller to report, so one malformed record never stops the base ETL.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from uuid import uuid4

from spicy_regs.transforms.base import Transform

#: The attributes table each base record type feeds, keyed by that record type's name.
ATTRIBUTE_TABLES = {"documents": "document_attributes", "dockets": "docket_attributes"}
#: Part-file columns that order duplicate ids in the merge; never published.
ORDER_COLUMNS = ("_modify_date", "_attributes_sha256")


def _projection(table: str):
    from spicy_docs.schemas.regulations_attribute_tables import project_docket_attributes, project_document_attributes

    return {"document_attributes": project_document_attributes, "docket_attributes": project_docket_attributes}[table]


def contract(table: str):
    from spicy_docs.schemas import TABLE_CONTRACTS

    return TABLE_CONTRACTS[table]


class TeeAttributes(Transform):
    """Yield every payload unchanged, writing its projected attributes row to ``staging_dir/<table>/``."""

    def __init__(self, table: str, staging_dir: Path, *, batch_size: int = 2_000) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self.table, self.staging_dir, self.batch_size = table, staging_dir, batch_size
        self.rows_written = 0
        self.refused: list[tuple[str | None, str]] = []

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        from spicy_docs.schemas.tables import TableContractError
        from loguru import logger

        from spicy_regs.contract_types import arrow_schema

        project = _projection(self.table)
        schema = arrow_schema(contract(self.table))
        for column in ORDER_COLUMNS:
            schema = schema.append(pa.field(column, pa.string()))
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
            for payload in records:
                data = payload.get("data") or {}
                attributes = data.get("attributes") or {}
                try:
                    row = project(data.get("id"), attributes)
                except TableContractError as error:
                    self.refused.append((data.get("id"), str(error)[:300]))
                    yield payload
                    continue
                row["_modify_date"] = attributes.get("modifyDate")
                row["_attributes_sha256"] = hashlib.sha256(
                    json.dumps(attributes, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
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


def newest_sql(source: str, table: str, *, keep_order: bool) -> str:
    """One row per identity from ``source`` (SQL): the newest ``modifyDate``, then the higher attributes digest."""
    table_contract = contract(table)
    columns = ", ".join(f'"{column}"' for column in (*table_contract.columns, *(ORDER_COLUMNS if keep_order else ())))
    keys = ", ".join(f'"{column}"' for column in table_contract.identity)
    return (
        f"SELECT {columns} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} "
        f"ORDER BY _modify_date DESC NULLS LAST, _attributes_sha256 DESC) AS _rn FROM {source}) WHERE _rn = 1"
    )


def merge_attribute_parts(table: str, parts: Path, prior: Path | None, out: Path, *, keep_order: bool = False) -> int:
    """Merge the part files under ``parts`` over ``prior`` into ``out`` by the contract's identity; the fresh row wins.

    Types are the contract's: the parts are written with its Arrow schema, and DuckDB keeps each column's type
    through the union. ``keep_order`` writes a table with no prior that keeps ``ORDER_COLUMNS``, so a sweep shard's
    rows can still be ordered against another shard's (the mirror files a few documents under two agencies).
    Returns the rows written.
    """
    import duckdb
    import pyarrow as pa
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
            for column in ORDER_COLUMNS if keep_order else ():
                schema = schema.append(pa.field(column, pa.string()))
            pq.write_table(schema.empty_table(), out, compression="zstd")
        elif prior != out:
            out.write_bytes(prior.read_bytes())
        return pq.ParquetFile(out).metadata.num_rows
    source = f"read_parquet('{str(parts / '*.parquet').replace(chr(39), chr(39) * 2)}')"
    with duckdb.connect() as con:
        con.execute("SET preserve_insertion_order=false")
        if keep_order:
            con.execute(f"COPY ({newest_sql(source, table, keep_order=True)} ORDER BY {keys}) TO '{target}' "
                        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_ROWS})")
            return pq.ParquetFile(out).metadata.num_rows
        staged = out.with_name(f".{out.name}.merging")
        fresh = out.with_name(f".{out.name}.fresh")
        con.execute(f"COPY ({newest_sql(source, table, keep_order=False)}) TO "
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
