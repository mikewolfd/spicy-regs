"""Transform: build ``court_opinion_clusters.parquet`` from the CourtListener bulk dumps.

A CourtListener *cluster* is one decision — the case-level record that a set of
sibling opinions (majority, concurrence, dissent) hang off. It is the join that
the corpus was missing: a cluster carries ``docket_id``, so it is what connects
opinion text to the APA litigation already in ``court_dockets``, and through the
docket's party names to the agencies in ``agency_stats``.

**Bulk-first, because bulk is the only road.** The v4 ``/clusters/`` endpoint
answers ``401`` without an API token, so the quarterly CSV dump is not a
performance choice — it is the only keyless source. An incremental
``/search/?type=o`` catch-up (keyless, cursor-paginated) tops the table up for
decisions filed after the dump date, mirroring the incremental-merge idiom in
``build_courtlistener`` / ``build_lobbying_filings``:

1. Best-effort download the prior ``court_opinion_clusters.parquet`` from R2.
2. Read the bulk dump (whole file — clusters are ~2.3 GiB compressed, which
   streams in about 20 minutes and never lands decompressed).
3. Fetch clusters filed since the dump date over the search API.
4. Dedup the union on ``cluster_id``, preferring the freshest row.

Rows are written in batches rather than materialized at once: the dump is on the
order of ten million clusters, and one ``from_pylist`` over that is an
out-of-memory error, not a table.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.courtlistener import CourtListenerOpinionSearchReader
from spicy_regs.sources.courtlistener_bulk import (
    CourtListenerBulkReader,
    find_dump,
    latest_dump_date,
    list_bulk_dumps,
)

OUTPUT = "court_opinion_clusters.parquet"
DATASET = "opinion-clusters"

CL_BASE_URL = "https://www.courtlistener.com"

#: Re-scan this many days before the dump date on the search catch-up, so
#: decisions indexed or corrected just after the dump are still picked up.
OVERLAP_DAYS = 7

#: Rows buffered before each parquet batch write.
BATCH_ROWS = 25_000

# Published schema: all VARCHAR, keyed by cluster_id. ``cl_docket_id`` is the
# bulk dump's ``docket_id``, renamed to match ``court_dockets.cl_docket_id`` so
# the join reads the same on both sides.
COLUMNS = (
    "cluster_id",
    "cl_docket_id",
    "case_name",
    "case_name_short",
    "case_name_full",
    "date_filed",
    "date_filed_is_approximate",
    "judges",
    "nature_of_suit",
    "precedential_status",
    "citation_count",
    "scdb_id",
    "scdb_decision_direction",
    "scdb_votes_majority",
    "scdb_votes_minority",
    "source",
    "procedural_history",
    "attorneys",
    "posture",
    "syllabus",
    "headnotes",
    "summary",
    "disposition",
    "history",
    "other_dates",
    "cross_reference",
    "correction",
    "arguments",
    "headmatter",
    "blocked",
    "date_blocked",
    "slug",
    "absolute_url",
    "date_created",
    "date_modified",
    "ingest_source",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL."""
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def _cluster_url(cluster_id: str | None, slug: str | None) -> str | None:
    """Build the canonical courtlistener.com opinion URL for a cluster."""
    if not cluster_id:
        return None
    return f"{CL_BASE_URL}/opinion/{cluster_id}/{slug or ''}".rstrip("/") + "/"


def _shape_bulk(row: dict) -> dict:
    """Map one bulk ``opinion-clusters`` CSV row onto the published columns."""
    cluster_id = _s(row.get("id"))
    slug = _s(row.get("slug"))
    return {
        "cluster_id": cluster_id,
        "cl_docket_id": _s(row.get("docket_id")),
        "case_name": _s(row.get("case_name")),
        "case_name_short": _s(row.get("case_name_short")),
        "case_name_full": _s(row.get("case_name_full")),
        "date_filed": _s(row.get("date_filed")),
        "date_filed_is_approximate": _s(row.get("date_filed_is_approximate")),
        "judges": _s(row.get("judges")),
        "nature_of_suit": _s(row.get("nature_of_suit")),
        "precedential_status": _s(row.get("precedential_status")),
        "citation_count": _s(row.get("citation_count")),
        "scdb_id": _s(row.get("scdb_id")),
        "scdb_decision_direction": _s(row.get("scdb_decision_direction")),
        "scdb_votes_majority": _s(row.get("scdb_votes_majority")),
        "scdb_votes_minority": _s(row.get("scdb_votes_minority")),
        "source": _s(row.get("source")),
        "procedural_history": _s(row.get("procedural_history")),
        "attorneys": _s(row.get("attorneys")),
        "posture": _s(row.get("posture")),
        "syllabus": _s(row.get("syllabus")),
        "headnotes": _s(row.get("headnotes")),
        "summary": _s(row.get("summary")),
        "disposition": _s(row.get("disposition")),
        "history": _s(row.get("history")),
        "other_dates": _s(row.get("other_dates")),
        "cross_reference": _s(row.get("cross_reference")),
        "correction": _s(row.get("correction")),
        "arguments": _s(row.get("arguments")),
        "headmatter": _s(row.get("headmatter")),
        "blocked": _s(row.get("blocked")),
        "date_blocked": _s(row.get("date_blocked")),
        "slug": slug,
        "absolute_url": _cluster_url(cluster_id, slug),
        "date_created": _s(row.get("date_created")),
        "date_modified": _s(row.get("date_modified")),
        "ingest_source": "bulk",
    }


def _shape_search(result: dict) -> dict:
    """Map one ``/search/?type=o`` result onto the published columns.

    The search surface is narrower than the dump: it has no syllabus, headnotes,
    or headmatter. Those stay NULL rather than being invented, so a row's
    provenance is legible from ``ingest_source``.
    """
    cluster_id = _s(result.get("cluster_id"))
    absolute = _s(result.get("absolute_url"))
    if absolute and absolute.startswith("/"):
        absolute = f"{CL_BASE_URL}{absolute}"
    row = dict.fromkeys(COLUMNS)
    row.update(
        {
            "cluster_id": cluster_id,
            "cl_docket_id": _s(result.get("docket_id")),
            "case_name": _s(result.get("caseName")),
            "case_name_full": _s(result.get("caseNameFull")),
            "date_filed": _s(result.get("dateFiled")),
            "judges": _s(result.get("judge")),
            "nature_of_suit": _s(result.get("suitNature")),
            "precedential_status": _s(result.get("status")),
            "citation_count": _s(result.get("citeCount")),
            "scdb_id": _s(result.get("scdb_id")),
            "source": _s(result.get("source")),
            "procedural_history": _s(result.get("procedural_history")),
            "attorneys": _s(result.get("attorney")),
            "posture": _s(result.get("posture")),
            "syllabus": _s(result.get("syllabus")),
            "absolute_url": absolute,
            "ingest_source": "search",
        }
    )
    return row


class _BatchWriter:
    """Append shaped rows to a parquet file in bounded batches."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows: list[dict] = []
        self.written = 0
        self._writer: pq.ParquetWriter | None = None

    def add(self, row: dict) -> None:
        self.rows.append(row)
        if len(self.rows) >= BATCH_ROWS:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=_SCHEMA)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, _SCHEMA, compression="zstd")
        self._writer.write_table(table)
        self.written += len(self.rows)
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is None:
            pq.write_table(_SCHEMA.empty_table(), self.path, compression="zstd")
        else:
            self._writer.close()


def build_court_opinion_clusters(
    output_dir: Path,
    *,
    dump_date: date | None = None,
    local_file: Path | None = None,
    max_records: int | None = None,
    skip_search_catchup: bool = False,
) -> Path:
    """Build ``court_opinion_clusters.parquet`` (bulk dump + search catch-up)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_clusters_prior.parquet"
    new_file = output_dir / "_clusters_new.parquet"

    # 1. Prior table (absence just means first build).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    logger.info(
        "Opinion clusters: {}",
        f"merging against prior table {prior_file}" if have_prior else "no prior table — full build",
    )

    # 2. Resolve which published dump to read.
    if local_file is None:
        objects = list_bulk_dumps()
        resolved = dump_date or latest_dump_date(objects, DATASET)
        if resolved is None:
            raise RuntimeError(f"CourtListener bulk: no published {DATASET} dump found")
        published = find_dump(objects, DATASET, resolved)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {DATASET} dump for {resolved}")
        logger.info(
            "Opinion clusters: reading dump {} ({:.3f} GiB compressed)",
            published.filename,
            published.size / 2**30,
        )
    else:
        resolved = dump_date
        logger.info("Opinion clusters: reading local dump {}", local_file)

    # 3. Stream the dump into the staging table, batch by batch.
    writer = _BatchWriter(new_file)
    reader = CourtListenerBulkReader(
        DATASET, dump_date=resolved, local_file=local_file, max_records=max_records
    )
    for row in reader.iter_records():
        writer.add(_shape_bulk(row))
    bulk_rows = writer.written + len(writer.rows)

    # 4. Search catch-up for decisions filed after the dump was cut.
    search_rows = 0
    if not skip_search_catchup and resolved is not None:
        since = resolved - timedelta(days=OVERLAP_DAYS)
        logger.info("Opinion clusters: search catch-up for decisions filed since {}", since)
        for result in CourtListenerOpinionSearchReader(since=since).iter_records():
            writer.add(_shape_search(result))
            search_rows += 1
    writer.close()
    logger.info(
        "Opinion clusters: staged {:,} rows ({:,} bulk + {:,} search)",
        writer.written,
        bulk_rows,
        search_rows,
    )

    # 5. Merge prior + new, dedup on cluster_id preferring the freshest row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY cluster_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE cluster_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY date_filed DESC, cluster_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court opinion clusters: {:,} rows", total)
    return out_file
