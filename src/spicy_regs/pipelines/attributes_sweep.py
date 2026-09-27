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
) -> dict[str, int]:
    """Read every document and docket record and write both attribute tables whole; return rows per table.

    ``read_factory(consumed)`` builds the ``read(agency, record_type)`` for one pass, skipping ``consumed`` keys;
    the default is the mirror's bounded reader.
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
        rows[table] = merge_attribute_parts(table, staging / table, None, output_dir / f"{table}.parquet")
        logger.info("{}: {:,} rows", table, rows[table])
    rmtree(staging, ignore_errors=True)
    return rows


app = App(name="run-attributes-sweep", help=__doc__)


@app.default
def main(
    *,
    output_dir: Path = Path("output"),
    agency: list[str] | None = None,
    max_workers: int = 4,
    skip_upload: bool = True,
) -> None:
    """Sweep the mirror into both attribute tables; with ``--no-skip-upload``, publish their working copies."""
    output_dir.mkdir(parents=True, exist_ok=True)
    sweep(output_dir, agencies=agency, max_workers=max_workers)
    if not skip_upload:
        tables = list(ATTRIBUTE_TABLES.values())
        r2.preflight_uploads(output_dir, r2.dataset_files(output_dir, tables))
        r2.upload_dataset(output_dir, tables)


if __name__ == "__main__":
    app()
