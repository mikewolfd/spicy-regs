"""Transform: CourtListener tables copied field for field from one quarterly bulk export.

Four publisher tables, each renamed to this repository's column names with ``dump_date`` added:
reporter citations (``court_citations``, keyed to the cluster, i.e. the decision), the opinion
citation map (``court_citation_map``), parentheticals (``court_parentheticals``) and a text-free
opinion index (``court_opinions``). The map and the parentheticals name *opinions*; only the
``opinions`` export maps an opinion to its cluster, and it is 54.6 GB, so the index is built from a
retained copy rather than on a CI runner. Opinion text is not kept (decision 6 in
docs/research/fork-delivery-decisions-2026-09-22.md); ``local_path`` and ``download_url`` link out.

Each export is a snapshot, not a delta, so a table is rebuilt whole from one export and never merged
with its prior. Values are the publisher's strings: every column is VARCHAR, NULL and ``""`` stay
distinct, and nothing is normalized. The citation map drops only its surrogate row id: it has 77.5
million rows and the publisher makes each citing/cited pair unique. SpicyDocs'
``CourtListenerLocalDump`` decodes the export: parallel bzip2, cut at record starts into pieces that
DuckDB parses in parallel, each piece's row count and opening records held to the reference
decoder. A table replaces its predecessor only after the whole export has been read.

An export comes from ``COURTLISTENER_BULK_DIR`` when that directory holds it under the publisher's
filename at the listed size, and is otherwise downloaded, bound to the listed ETag and size.

A table that names clusters waits for them: the publisher cuts each export at a different hour of
the export day, clusters first, so its citations and opinions name clusters created after the
cluster export. Such a table is refused while it names a cluster id above the published
``court_opinion_clusters``, whose search catch-up adds every cluster created since its export; run
that rollup first.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

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
    def columns(self) -> tuple[str, ...]:
        return (*(column for column, _ in self.fields), "dump_date")

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([(column, pa.string()) for column in self.columns])

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


def _highest_cluster_id(parquet: str) -> int | None:
    import duckdb

    with duckdb.connect() as con:
        if parquet.startswith("https://"):
            con.execute("INSTALL httpfs; LOAD httpfs")
        path = parquet.replace("'", "''")
        row = con.execute(f"SELECT max(TRY_CAST(cluster_id AS BIGINT)) FROM read_parquet('{path}')").fetchone()
        return None if row is None else row[0]


def published_cluster_ceiling() -> int | None:
    """The highest cluster id in the published ``court_opinion_clusters``, or ``None`` when none is published.

    Reads that one column over the public URL (about 55 MB for ten million clusters).
    """
    from spicy_regs.public_url import resolve_r2_base_url
    from spicy_regs.sources.publication import load_index, table_location

    base = resolve_r2_base_url()
    location, descriptor = table_location(load_index(base), CLUSTERS_TABLE)
    return None if descriptor is None else _highest_cluster_id(f"{base}/{location}")


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
    table: BulkTable, output_dir: Path, *, local_file: Path, dump_date: date, cluster_ceiling: int | None = None
) -> Path:
    """Write ``table.output`` from one whole export; the previous file survives any failure.

    With ``cluster_ceiling``, a table that names clusters is refused when any is above it.
    """
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / table.output
    staging = output_dir / f".{table.output}.partial"
    dump = CourtListenerLocalDump(local_file, columns=[source for _, source in table.fields], work_dir=output_dir)
    edition = dump_date.isoformat()
    pending: list[pa.RecordBatch] = []

    def flush(writer: pq.ParquetWriter) -> None:
        # A piece of opinion text yields a few thousand metadata rows; gather them into row groups.
        writer.write_table(pa.Table.from_batches(pending, schema=table.schema), row_group_size=BATCH_ROWS)
        pending.clear()

    try:
        with pq.ParquetWriter(staging, table.schema, compression="zstd") as writer:
            for batch in dump.iter_batches():
                stamped = [*batch.columns, pa.array([edition] * batch.num_rows, pa.string())]
                pending.append(pa.RecordBatch.from_arrays(stamped, schema=table.schema))
                if sum(part.num_rows for part in pending) >= BATCH_ROWS:
                    flush(writer)
            if pending:
                flush(writer)
        if not dump.completed:
            raise RuntimeError(f"CourtListener bulk: {local_file.name} was not read to its end")
        if cluster_ceiling is not None and table.names_clusters:
            newest = _highest_cluster_id(str(staging))
            if newest is not None and newest > cluster_ceiling:
                raise RuntimeError(
                    f"{table.output}: the {edition} export names cluster {newest:,}, above the published "
                    f"{CLUSTERS_TABLE} (to {cluster_ceiling:,}); run run-rollup-court-opinion-clusters first"
                )
    except BaseException:
        staging.unlink(missing_ok=True)
        raise
    staging.replace(out_file)
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
    """Build each table from the newest export that publishes all of them, deleting what it downloaded."""
    from spicy_docs.sources.courtlistener.bulk import find_dump, list_bulk_dumps

    objects = list_bulk_dumps()
    edition = dump_date or latest_common_dump_date(objects, [table.dataset for table in tables])
    ceiling = None
    if any(table.names_clusters for table in tables):
        ceiling = published_cluster_ceiling()
        if ceiling is None:
            raise RuntimeError(f"CourtListener bulk: {CLUSTERS_TABLE} is not published; publish it before its children")
    built = []
    for table in tables:
        published = find_dump(objects, table.dataset, edition)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {table.dataset} export for {edition}")
        local_file, downloaded = export_file(published, output_dir)
        try:
            built.append(
                build_court_bulk_table(
                    table, output_dir, local_file=local_file, dump_date=edition, cluster_ceiling=ceiling
                )
            )
        finally:
            if downloaded:
                local_file.unlink(missing_ok=True)
    return tuple(built)
