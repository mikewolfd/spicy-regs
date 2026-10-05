"""A published generation with shared receipts on local disk, served to the tools through one DuckDB connection."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import mcp_server as server
from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, write_dataset

GENERATION = "generation-under-test"


def context(attempt: str, generation: str = GENERATION) -> ReceiptContext:
    return ReceiptContext(generation, attempt, "test-processor",
                          [{"source_id": "fixture", "source_uri": None, "sha256": "sha256:" + "0" * 64,
                            "locator": attempt, "body_version": None}])


def written(directory: Path, table: str, rows: list[dict], *, policy=None) -> tuple[Path, Path]:
    """Subject and receipt files for ``rows`` through the shared writer, under the installed policy by default.

    Each row gives subject values and receipt fields together; a subject column it omits is NULL.
    """
    policy = policy or installed_policies()[table]
    records = [({**dict.fromkeys(policy.subject_schema.names), **row}, context(f"{table}:{ordinal}"))
               for ordinal, row in enumerate(rows)]
    subject, receipts = write_dataset(records, directory / table, policy)
    assert subject is not None
    return subject, receipts


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _columns(path: Path) -> list[list[str]]:
    with duckdb.connect() as con:
        return [[row[0], row[1]] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]


def published(root: Path, family: str, subjects: dict[str, Path], receipts: pa.Table, *, row_group: int = 2000,
              generation: str = GENERATION) -> dict:
    """One family's index entry, its files laid out under ``root`` as the bucket lays out a generation."""
    digest = hashlib.sha256(family.encode()).hexdigest()
    prefix = f"generations/{family}/{digest}"
    (root / prefix).mkdir(parents=True)
    tables = {}
    for table, source in subjects.items():
        target = root / prefix / f"{table}.parquet"
        target.write_bytes(source.read_bytes())
        tables[f"{table}.parquet"] = {"sha256": _digest(target), "byteSize": target.stat().st_size,
                                      "rows": pq.ParquetFile(target).metadata.num_rows, "columns": _columns(target)}
    member = root / prefix / "etl_receipts.parquet"
    pq.write_table(receipts.cast(RECEIPT_SCHEMA), member, row_group_size=row_group, compression="zstd")
    return {
        "prefix": prefix, "logicalId": f"urn:test:{family}", "artifactDigest": "sha256:" + digest, "tables": tables,
        "etlReceipts": {"key": "etl_receipts.parquet", "sha256": _digest(member), "byteSize": member.stat().st_size,
                        "rows": receipts.num_rows, "columns": _columns(member), "generationId": generation,
                        "datasets": sorted(set(receipts.column("dataset").to_pylist()))},
    }


def index_of(families: dict[str, dict]) -> dict:
    return {"format": "spicy-regs-publication", "version": 2, "families": families}


def serving(monkeypatch, root: Path, index: dict) -> duckdb.DuckDBPyConnection:
    """A connection holding each published table as a view and the index as its pin, as a remote build leaves it.

    ``R2_BASE_URL`` is ``root``, so the member location the tool derives from the index is the local file.
    """
    monkeypatch.setattr(server, "R2_BASE_URL", str(root))
    con = duckdb.connect()
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
    for entry in index["families"].values():
        for key in entry["tables"]:
            path = str(root / entry["prefix"] / key).replace("'", "''")
            con.execute(f'CREATE VIEW "{key.removesuffix(".parquet")}" AS SELECT * FROM read_parquet(\'{path}\')')
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    return con


def one_family(tmp_path: Path, monkeypatch, tables: dict[str, list[dict]], *, family: str = "family-under-test",
               row_group: int = 2000, edit=None) -> duckdb.DuckDBPyConnection:
    """Publish ``tables`` as one family and serve it. ``edit`` changes the combined receipt table before it is
    published, for members a writer would refuse to produce."""
    subjects, parts = {}, []
    for table, rows in tables.items():
        subjects[table], receipts = written(tmp_path / "work", table, rows)
        parts.append(pq.read_table(receipts))
    combined = pa.concat_tables(parts)
    if edit is not None:
        combined = edit(combined)
    root = tmp_path / "bucket"
    return serving(monkeypatch, root, index_of({family: published(root, family, subjects, combined, row_group=row_group)}))
