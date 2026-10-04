"""CourtListener bulk domain tables with generation-bound ETL receipts.

Each complete export becomes one subject table plus shared receipts. Subject
rows retain native identifiers, legal descriptions and typed domain values.
Dump editions, publisher record timestamps, body references and extraction
provenance belong to receipts. Retained source bytes preserve the publisher's
literal spellings, nulls and empty strings. Receipt conversion inputs preserve
numeric and boolean literals when the subject uses native types.

A source must be read completely before the subject/receipt pair replaces its
predecessor. Cluster-naming tables still refuse exports ahead of the selected
cluster publication. Downloads retain their listed size and ETag checks and
the existing disk floor; downloaded and pre-existing source bytes remain held.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.court_receipts import file_witness, finish_court_output, record_source_failure
from spicy_regs.transforms._courtlistener_writer import check_headroom

if TYPE_CHECKING:
    from spicy_docs.sources.courtlistener.listing import BulkObject

#: Directory of retained exports, named as the publisher names them (``citations-2026-06-30.csv.bz2``).
RETAINED_DIR_ENV = "COURTLISTENER_BULK_DIR"
#: Rows per Parquet row group.
BATCH_ROWS = 100_000
#: The published table whose ``cluster_id`` a cluster-naming export must not run ahead of.
CLUSTERS_TABLE = "court_opinion_clusters.parquet"


@dataclass(frozen=True)
class BulkTable:
    """One publisher table: the export it comes from and the source field behind each column."""

    dataset: str
    output: str
    fields: tuple[tuple[str, str], ...]

    @property
    def input_columns(self) -> tuple[str, ...]:
        return (*(column for column, _ in self.fields), "dump_date")

    @property
    def input_schema(self) -> pa.Schema:
        return pa.schema([(column, pa.string()) for column in self.input_columns])

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.schema.names)

    @property
    def schema(self) -> pa.Schema:
        return SUBJECT_SCHEMAS[self.output.removesuffix('.parquet')]

    @property
    def names_clusters(self) -> bool:
        return "cluster_id" in self.columns


CITATIONS = BulkTable(
    "citations",
    "court_citations.parquet",
    (
        ("citation_id", "id"),
        ("cluster_id", "cluster_id"),
        ("volume", "volume"),
        ("reporter", "reporter"),
        ("page", "page"),
        ("citation_type", "type"),
        ("date_created", "date_created"),
        ("date_modified", "date_modified"),
    ),
)
CITATION_MAP = BulkTable(
    "citation-map",
    "court_citation_map.parquet",
    (
        ("citing_opinion_id", "citing_opinion_id"),
        ("cited_opinion_id", "cited_opinion_id"),
        ("depth", "depth"),
    ),
)
PARENTHETICALS = BulkTable(
    "parentheticals",
    "court_parentheticals.parquet",
    (
        ("parenthetical_id", "id"),
        ("described_opinion_id", "described_opinion_id"),
        ("describing_opinion_id", "describing_opinion_id"),
        ("text", "text"),
        ("score", "score"),
        ("group_id", "group_id"),
    ),
)
OPINIONS = BulkTable(
    "opinions",
    "court_opinions.parquet",
    (
        ("opinion_id", "id"),
        ("cluster_id", "cluster_id"),
        ("opinion_type", "type"),
        ("author_id", "author_id"),
        ("author_str", "author_str"),
        ("per_curiam", "per_curiam"),
        ("joined_by_str", "joined_by_str"),
        ("page_count", "page_count"),
        ("sha1", "sha1"),
        ("download_url", "download_url"),
        ("local_path", "local_path"),
        ("extracted_by_ocr", "extracted_by_ocr"),
        ("date_created", "date_created"),
        ("date_modified", "date_modified"),
    ),
)


def latest_common_dump_date(objects: Sequence[BulkObject], datasets: Sequence[str]) -> date:
    """The newest export date that publishes every one of ``datasets``, so one run reads one edition."""
    dated = [{obj.dump_date for obj in objects if obj.dataset == dataset and obj.dump_date} for dataset in datasets]
    common = set.intersection(*dated) if dated else set()
    if not common:
        raise RuntimeError(f"CourtListener bulk: no export date publishes all of {list(datasets)}")
    return max(common)


def _highest_cluster_id(parquet: list[str]) -> int | None:
    import duckdb

    from spicy_regs.sources.publication import parquet_scan

    with duckdb.connect() as con:
        if any(path.startswith("https://") for path in parquet):
            load_public_http(con)
        row = con.execute(f"SELECT max(TRY_CAST(cluster_id AS BIGINT)) FROM {parquet_scan(parquet)}").fetchone()
        return None if row is None else row[0]


def published_cluster_ceiling() -> int | None:
    """The highest cluster id in the published ``court_opinion_clusters``, or ``None`` when none is published.

    Reads that one column over the public URL (about 55 MB for ten million clusters).
    """
    from spicy_regs.public_url import resolve_r2_base_url
    from spicy_regs.sources.publication import load_index, table_members

    base = resolve_r2_base_url()
    members = table_members(load_index(base), CLUSTERS_TABLE)
    return None if members[0].sha256 is None else _highest_cluster_id([f"{base}/{m.path}" for m in members])


def _bucket_url(url: str) -> str:
    from spicy_docs.sources.courtlistener.listing import BULK_BASE_URL

    if not url.startswith(BULK_BASE_URL + "/"):
        raise ValueError("CourtListener exports are read only from the publisher's bulk-data bucket")
    return url


def export_file(published: BulkObject, work_dir: Path) -> tuple[Path, bool]:
    """A local copy of one listed export, and whether this call downloaded it.

    A retained copy must match the listed size; the bytes themselves are checked as they decode,
    by bzip2's block checksums. A download is bound to the listed ETag and size, and refused when it
    would take the volume below the disk floor.
    """
    retained_dir = os.environ.get(RETAINED_DIR_ENV)
    if retained_dir:
        retained = Path(retained_dir) / published.filename
        if retained.is_file():
            if retained.stat().st_size != published.size:
                raise RuntimeError(
                    f"CourtListener bulk: retained {retained} is {retained.stat().st_size} bytes, "
                    f"not the listed {published.size}"
                )
            logger.info("CourtListener bulk: using retained {}", retained)
            return retained, False
    from spicy_docs.transport.download import BoundedAcquirer

    check_headroom(published.size, path=work_dir)
    store = work_dir / "courtlistener-exports"
    logger.info("CourtListener bulk: downloading {} ({:.2f} GiB)", published.filename, published.size / 2**30)
    with BoundedAcquirer(validate_url=_bucket_url, timeout=120) as acquirer:
        receipt = acquirer.download(
            published.url,
            store=store,
            max_bytes=published.size,
            expected_size=published.size,
            etag=published.etag,
        )
    return store / receipt["blob_path"], True


def build_court_bulk_table(
    table: BulkTable, output_dir: Path, *, local_file: Path, dump_date: date, cluster_ceiling: int | None = None,
    generation_id: str | None = None,
) -> Path:
    """Write ``table.output`` from one whole export; the previous file survives any failure.

    With ``cluster_ceiling``, a table that names clusters is refused when any is above it.
    """
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / table.output
    staging = output_dir / f".{table.output}.{uuid4().hex}.source.parquet"
    dump = CourtListenerLocalDump(local_file, columns=[source for _, source in table.fields], work_dir=output_dir)
    edition = dump_date.isoformat()
    pending: list[pa.RecordBatch] = []

    def flush(writer: pq.ParquetWriter) -> None:
        # A piece of opinion text yields a few thousand metadata rows; gather them into row groups.
        writer.write_table(pa.Table.from_batches(pending, schema=table.input_schema), row_group_size=BATCH_ROWS)
        pending.clear()

    try:
        with pq.ParquetWriter(staging, table.input_schema, compression="zstd") as writer:
            for batch in dump.iter_batches():
                stamped = [*batch.columns, pa.array([edition] * batch.num_rows, pa.string())]
                pending.append(pa.RecordBatch.from_arrays(stamped, schema=table.input_schema))
                if sum(part.num_rows for part in pending) >= BATCH_ROWS:
                    flush(writer)
            if pending:
                flush(writer)
        if not dump.completed:
            raise RuntimeError(f"CourtListener bulk: {local_file.name} was not read to its end")
        if cluster_ceiling is not None and table.names_clusters:
            newest = _highest_cluster_id([str(staging)])
            if newest is not None and newest > cluster_ceiling:
                raise RuntimeError(
                    f"{table.output}: the {edition} export names cluster {newest:,}, above the published "
                    f"{CLUSTERS_TABLE} (to {cluster_ceiling:,}); run run-rollup-court-opinion-clusters first"
                )
    except BaseException as error:
        if isinstance(error, Exception):
            try:
                record_source_failure(table.output.removesuffix('.parquet'), output_dir,
                                       witnesses=[file_witness(local_file)], error=error)
            except Exception as receipt_error:
                error.add_note(f'Failed to retain source failure receipt: {receipt_error}')
        staging.unlink(missing_ok=True)
        raise
    out_file = finish_court_output(table.output.removesuffix('.parquet'), staging, output_dir,
                                  witnesses=[file_witness(local_file), file_witness(staging)],
                                  generation_id=generation_id)
    logger.info(
        "{}: {:,} rows from {} ({:.2f} GB of CSV, {} pieces)",
        table.output,
        dump.rows,
        local_file.name,
        dump.decompressed_bytes / 1e9,
        dump.pieces,
    )
    return out_file


def build_court_bulk_tables(
    tables: Sequence[BulkTable], output_dir: Path, *, dump_date: date | None = None
) -> tuple[Path, ...]:
    """Build one complete edition and retain its source bytes and shared receipts."""
    from spicy_docs.sources.courtlistener.bulk import find_dump, list_bulk_dumps

    objects = list_bulk_dumps()
    edition = dump_date or latest_common_dump_date(objects, [table.dataset for table in tables])
    ceiling = None
    if any(table.names_clusters for table in tables):
        ceiling = published_cluster_ceiling()
        if ceiling is None:
            raise RuntimeError(f"CourtListener bulk: {CLUSTERS_TABLE} is not published; publish it before its children")
    built = []
    generation_id = uuid4().hex
    for table in tables:
        published = find_dump(objects, table.dataset, edition)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {table.dataset} export for {edition}")
        local_file, _ = export_file(published, output_dir)
        built.append(
            build_court_bulk_table(
                table, output_dir, local_file=local_file, dump_date=edition, cluster_ceiling=ceiling,
                generation_id=generation_id,
            )
        )
    return tuple(built)
