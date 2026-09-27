"""Seed the Regulations.gov attribute tables with one full pass over the mirror (owner decisions 65-67).

The daily ETL reads only new and changed keys, so it can't fill a document it never re-reads. That includes the
records posted before 1990 and the ones unchanged since DocSpec's capture: 59,781 documents and 822 dockets on
2026-09-27. This pass reads every document and docket record in the Mirrulations mirror, projects each record's
attributes through SpicyDocs' contract, and writes ``document_attributes.parquet`` and ``docket_attributes.parquet``
whole. The thin rows the same read yields are staged and discarded. After this seeds the working copies, the daily
ETL merges its rows into them.

A record the contract refuses loses only its attributes row; the sweep lists each id and reason in
``attribute_refusals.json`` beside the tables, with every file the mirror serves unreadable or empty (a publisher
defect a re-read cannot fix). A key that fails in transport is re-read in up to ``RETRY_PASSES`` passes, each
skipping every key already read; one still unread after them refuses the sweep before it writes anything. Its readers never touch the manifest, so a
missed key would never be read again. Usage:
``uv run --frozen run-attributes-sweep --output-dir out [--agency EPA] [--no-skip-upload]``.
"""

from __future__ import annotations

import json
from pathlib import Path
from shutil import rmtree
from time import monotonic

from cyclopts import App
from loguru import logger

from spicy_regs.pipelines.regulations_state import source_record_type
from spicy_regs.pipelines.staging import stage_agencies
from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.sources import r2
from spicy_regs.transforms import Chain, ExtractRecords
from spicy_regs.transforms.regulations_attributes import ATTRIBUTE_TABLES, TeeAttributes, merge_attribute_parts

#: Mirror reads in flight per agency; 48 measured 559 records a second from a laptop (2026-09-27).
DOWNLOAD_WORKERS = 48
#: Passes over the keys a pass failed to read in transport (the full run of 2026-09-27 lost 17 CMS dockets to a
#: connection drop in one burst).
RETRY_PASSES = 3


def sweep(
    output_dir: Path,
    *,
    agencies: list[str] | None = None,
    max_workers: int = 4,
    download_workers: int = DOWNLOAD_WORKERS,
    read_factory=None,
    keep_order: bool = False,
) -> dict[str, int]:
    """Read every document and docket record and write both attribute tables whole; return rows per table.

    ``read_factory(consumed)`` builds the ``read(agency, record_type)`` for one pass, skipping ``consumed`` keys;
    the default is the mirror's bounded reader. ``keep_order`` keeps every copy with its ordering columns, for a
    shard that ``combine`` orders against the others.
    """
    from spicy_docs.sources import mirrulations

    record_types = [RECORD_TYPES["dockets"], RECORD_TYPES["documents"]]
    source_types = {rt.name: source_record_type(rt) for rt in record_types}

    def reader(consumed: frozenset[str]):
        if read_factory is not None:
            return read_factory(consumed)
        return mirrulations.reader_factory(
            list(source_types.values()), processed_keys=consumed or None, download_workers=download_workers,
            bounded=True,
        )

    staging = output_dir / "attributes-staging"
    rmtree(staging, ignore_errors=True)
    tees: list[TeeAttributes] = []

    def transform_for(rt):
        tees.append(tee := TeeAttributes(ATTRIBUTE_TABLES[rt.name], staging))
        return Chain(tee, ExtractRecords(rt))

    started = monotonic()
    pending = agencies if agencies is not None else mirrulations.discover_agencies()
    consumed: set[str] = set()
    unreadable: set[str] = set()
    transport: set[str] = set()
    rows_read: dict[str, int] = {}
    for attempt in range(1, RETRY_PASSES + 1):
        read = reader(frozenset(consumed))
        result = stage_agencies(
            pending, record_types, staging, lambda agency, rt: read(agency, source_types[rt.name]),
            transform_for=transform_for, max_workers=max_workers,
        )
        for name, count in result.rows_by_type.items():
            rows_read[name] = rows_read.get(name, 0) + count
        consumed |= result.consumed_keys
        unreadable |= set(result.parse_failed_keys)
        transport = set(result.failed_keys) - consumed - unreadable
        if not transport:
            break
        pending = sorted({key.split("/")[1] for key in transport})
        logger.warning("Attribute sweep pass {}: {} keys failed in transport; re-reading {}", attempt, len(transport),
                       pending)
    if transport:
        raise RuntimeError(f"Attribute sweep left {len(transport)} keys unread after {RETRY_PASSES} passes; "
                           "nothing was written")
    logger.info("Attribute sweep read {} in {:.0f}s", rows_read, monotonic() - started)
    receipt = {
        "refused": [{"table": tee.table, "id": key, "reason": reason} for tee in tees for key, reason in tee.refused],
        "unreadable_keys": sorted(unreadable),
    }
    (output_dir / "attribute_refusals.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    rows = {}
    for table in ATTRIBUTE_TABLES.values():
        rows[table] = merge_attribute_parts(table, staging / table, None, output_dir / f"{table}.parquet",
                                            keep_order=keep_order)
        logger.info("{}: {:,} rows", table, rows[table])
    rmtree(staging, ignore_errors=True)
    return rows


def shard_agencies(agencies: list[str], shard: int, shards: int) -> list[str]:
    """Agencies dealt round-robin into ``shards`` in name order, so the largest agencies land in different shards."""
    if not 0 <= shard < shards:
        raise ValueError(f"shard {shard} is outside 0..{shards - 1}")
    return sorted(agencies)[shard::shards]


def combine(shard_dirs: list[Path], output_dir: Path) -> dict[str, int]:
    """Union each table's copies across the shards, keeping one row per id by the sweep's own rule; merge receipts.

    The mirror files a few documents under two agencies (2 on 2026-09-27), so an id's copies can reach two shards.
    Every shard keeps all its copies, and the rule chooses among them once, as a single sweep would.
    """
    import duckdb
    import pyarrow.parquet as pq

    from spicy_regs.transforms.regulations_attributes import ORDER_COLUMNS, ROW_GROUP_ROWS, contract, newest_copy_sql

    output_dir.mkdir(parents=True, exist_ok=True)
    rows = {}
    for table in ATTRIBUTE_TABLES.values():
        files = [str(directory / f"{table}.parquet").replace("'", "''") for directory in shard_dirs]
        source = "read_parquet([" + ", ".join(f"'{f}'" for f in files) + "], filename=true)"
        keys = ", ".join(f'"{column}"' for column in contract(table).identity)
        out = output_dir / f"{table}.parquet"
        with duckdb.connect() as con:
            names = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
            if not set(ORDER_COLUMNS) <= names:
                raise RuntimeError(f"{table}: shard outputs lack {list(ORDER_COLUMNS)}; sweep shards with --shard")
            repeated = con.execute(
                f"SELECT count(*) FROM (SELECT {keys} FROM {source} GROUP BY {keys} HAVING count(DISTINCT filename) > 1)"
            ).fetchone()
            con.execute(
                f"COPY ({newest_copy_sql(source, table)} ORDER BY {keys}) TO "
                f"'{str(out).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_ROWS})"
            )
        rows[table] = pq.ParquetFile(out).metadata.num_rows
        logger.info("{}: {:,} rows from {} shards; {} ids reached more than one shard", table, rows[table],
                    len(shard_dirs), repeated[0] if repeated else 0)
    receipt: dict[str, list] = {"refused": [], "unreadable_keys": []}
    for directory in shard_dirs:
        shard = json.loads((directory / "attribute_refusals.json").read_text(encoding="utf-8"))
        receipt["refused"] += shard["refused"]
        receipt["unreadable_keys"] += shard["unreadable_keys"]
    (output_dir / "attribute_refusals.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return rows


def _publish(output_dir: Path) -> None:
    tables = list(ATTRIBUTE_TABLES.values())
    r2.preflight_uploads(output_dir, r2.dataset_files(output_dir, tables))
    r2.upload_dataset(output_dir, tables)


app = App(name="run-attributes-sweep", help=__doc__)


@app.default
def main(
    *,
    output_dir: Path = Path("output"),
    agency: list[str] | None = None,
    shard: int | None = None,
    shards: int | None = None,
    max_workers: int = 4,
    skip_upload: bool = True,
) -> None:
    """Sweep the mirror, or one ``--shard`` of ``--shards``, into both attribute tables; optionally publish them."""
    from spicy_docs.sources import mirrulations

    output_dir.mkdir(parents=True, exist_ok=True)
    if (shard is None) != (shards is None):
        raise ValueError("--shard and --shards go together")
    if shard is not None and shards is not None:
        agency = shard_agencies(agency or mirrulations.discover_agencies(), shard, shards)
    sweep(output_dir, agencies=agency, max_workers=max_workers, keep_order=shard is not None)
    if not skip_upload:
        _publish(output_dir)


@app.command(name="combine")
def combine_command(shard_dir: list[Path], *, output_dir: Path = Path("output"), skip_upload: bool = True) -> None:
    """Combine shard sweeps into both attribute tables; optionally publish them."""
    combine(shard_dir, output_dir)
    if not skip_upload:
        _publish(output_dir)


if __name__ == "__main__":
    app()
