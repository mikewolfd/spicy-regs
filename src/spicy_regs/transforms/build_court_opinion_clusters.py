"""Transform: build ``court_opinion_clusters.parquet`` from the CourtListener bulk dumps.

A CourtListener *cluster* is one decision — the case-level record that sibling
opinions hang off — and its ``docket_id`` is what connects opinion text to the
APA litigation in ``court_dockets`` and, through the docket's party names, to
the agencies in ``agency_stats``.

**Bulk-first, because bulk is the only road.** The v4 ``/clusters/`` endpoint
answers ``401`` without a token, so the quarterly CSV dump is the only keyless
source; an incremental ``/search/?type=o`` catch-up (keyless, cursor-paginated)
tops the table up with every cluster whose id is above the export's highest.
The publisher numbers clusters in creation order and cuts each export table at
a different hour of the export day (2026-06-30: clusters by 08:16 UTC, opinions
09:56, citations 19:14), so the sibling tables name clusters created after this
one was cut; a filing-date catch-up missed those filed earlier. Best-effort
prior from R2, then dedup on ``cluster_id`` preferring the freshest row; rows
are written in batches because the dump runs to ten million clusters and one
``from_pylist`` over that is an out-of-memory error. The dump has no
``court_id`` — it lives on the docket, in a different multi-GiB file — so
``court_id`` / ``court_jurisdiction`` / ``court_is_federal`` are resolved from
a docket→court map (see :mod:`spicy_regs.transforms.court_scope`) while each
row is shaped; ``skip_court_scope`` opts out and leaves the three columns NULL
at the cost of a ~46-minute map build. A scheduled run whose prior already
holds the newest export reads neither dump and only catches up.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms._courtlistener_writer import CourtListenerTableWriter
from spicy_regs.sources.courtlistener import CourtListenerOpinionSearchReader
from spicy_regs.transforms.court_scope import (
    CourtScope,
    build_docket_court_map,
    court_jurisdictions,
)
from spicy_regs.transforms.table_merge import merge_local_prior

OUTPUT = "court_opinion_clusters.parquet"
DATASET = "opinion-clusters"

CL_BASE_URL = "https://www.courtlistener.com"

#: Rows buffered before each parquet batch write.
BATCH_ROWS = 25_000

# Published schema: all VARCHAR, keyed by cluster_id. ``cl_docket_id`` is the
# bulk dump's ``docket_id``, renamed to match ``court_dockets.cl_docket_id`` so
# the join reads the same on both sides.
COLUMNS = (
    "cluster_id",
    "cl_docket_id",
    "court_id",
    "court_jurisdiction",
    "court_is_federal",
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
    """Table policy: empty strings and missing values are both NULL."""
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def _cluster_url(cluster_id: str | None, slug: str | None) -> str | None:
    """Build the canonical courtlistener.com opinion URL for a cluster."""
    if not cluster_id:
        return None
    return f"{CL_BASE_URL}/opinion/{cluster_id}/{slug or ''}".rstrip("/") + "/"


def _shape_bulk(row: dict, *, scope: CourtScope | None = None) -> dict:
    """Map one bulk ``opinion-clusters`` CSV row onto the published columns."""
    cluster_id = _s(row.get("id"))
    slug = _s(row.get("slug"))
    cl_docket_id = _s(row.get("docket_id"))
    court_id, jurisdiction, federal = scope.for_docket(cl_docket_id) if scope else (None, None, None)
    return {
        "cluster_id": cluster_id,
        "cl_docket_id": cl_docket_id,
        "court_id": court_id,
        "court_jurisdiction": jurisdiction,
        "court_is_federal": federal,
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


def _shape_search(result: dict, *, scope: CourtScope | None = None) -> dict:
    """Map one ``/search/?type=o`` result onto the published columns.

    The search surface is narrower than the dump: it has no syllabus, headnotes,
    or headmatter. Those stay NULL rather than being invented, so a row's
    provenance is legible from ``ingest_source``.

    It is *wider* in exactly one place: it names the court outright, so a
    catch-up row does not need the docket map to be scoped — but it must be
    classified by the same rule, or the two halves of the table would disagree
    about what federal means.
    """
    cluster_id = _s(result.get("cluster_id"))
    absolute = _s(result.get("absolute_url"))
    if absolute and absolute.startswith("/"):
        absolute = f"{CL_BASE_URL}{absolute}"
    court_id, jurisdiction, federal = (
        scope.for_court(_s(result.get("court_id"))) if scope else (_s(result.get("court_id")), None, None)
    )
    row = dict.fromkeys(COLUMNS)
    row.update(
        {
            "cluster_id": cluster_id,
            "cl_docket_id": _s(result.get("docket_id")),
            "court_id": court_id,
            "court_jurisdiction": jurisdiction,
            "court_is_federal": federal,
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


def held_export(table: Path) -> tuple[int | None, str | None]:
    """A clusters table's highest bulk ``cluster_id`` and the day of its newest bulk ``date_created``.

    Together they name the export the table holds without a stamp: the id is
    where that export ended, and the export was cut on its dump date, so a table
    whose newest bulk row was created on or after a dump's date already holds it.
    One scan of two columns.
    """
    import duckdb

    path = str(table).replace("'", "''")
    with duckdb.connect() as con:
        high, created = con.execute(
            f"""SELECT max(TRY_CAST(cluster_id AS BIGINT)) FILTER (WHERE ingest_source = 'bulk'),
                       max(date_created) FILTER (WHERE ingest_source = 'bulk')
                FROM read_parquet('{path}')"""
        ).fetchone() or (None, None)
    return high, created[:10] if created else None


def build_court_opinion_clusters(
    output_dir: Path,
    *,
    dump_date: date | None = None,
    local_file: Path | None = None,
    max_records: int | None = None,
    skip_search_catchup: bool = False,
    docket_court_map: Path | None = None,
    courts_local_file: Path | None = None,
    skip_court_scope: bool = False,
    include_prior: bool = True,
) -> Path:
    """Build ``court_opinion_clusters.parquet`` (bulk dump + search catch-up).

    ``skip_court_scope`` leaves the three court columns NULL and skips the
    46-minute ``dockets`` read. It is the honest way to build the table without
    the scope, and it is not the default: a decision table that cannot say which
    court decided is a table nobody can ask the obvious question of.

    With neither ``dump_date`` nor ``local_file``, a prior that already holds the
    newest listed export (:func:`held_export`) is only caught up: no dump is
    read, and the search rows above that export's highest id are merged over it.
    Naming an edition or a file always reads it.

    ``courts_local_file`` pairs a retained courts dump with a supplied docket
    map. ``include_prior=False`` builds the selected source edition alone,
    without downloading, reading or removing a retained prior table. Combine
    these with ``local_file`` and ``skip_search_catchup=True`` for an offline
    build from pinned originals.
    """
    import duckdb

    from spicy_docs.sources.courtlistener.bulk import (
        CourtListenerBulkReader,
        find_dump,
        latest_dump_date,
        list_bulk_dumps,
    )

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_clusters_prior.parquet"
    new_file = output_dir / "_clusters_new.parquet"

    # 1. Prior table (absence just means first build).
    have_prior = include_prior and (prior_file.exists() or r2.download(OUTPUT, prior_file))
    logger.info(
        "Opinion clusters: {}",
        f"merging against prior table {prior_file}" if have_prior else "no prior table — full build",
    )

    # 2. Resolve which published dump to read.
    held_high: int | None = None
    catch_up_only = False
    if local_file is None:
        objects = list_bulk_dumps()
        resolved = dump_date or latest_dump_date(objects, DATASET)
        if resolved is None:
            raise RuntimeError(f"CourtListener bulk: no published {DATASET} dump found")
        published = find_dump(objects, DATASET, resolved)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {DATASET} dump for {resolved}")
        held_high, held_day = held_export(prior_file) if have_prior and dump_date is None else (None, None)
        catch_up_only = held_high is not None and held_day is not None and held_day >= resolved.isoformat()
        if catch_up_only:
            logger.info("Opinion clusters: prior holds the {} export (to id {:,}); catch-up only", resolved, held_high)
        else:
            logger.info(
                "Opinion clusters: reading dump {} ({:.3f} GiB compressed)",
                published.filename,
                published.size / 2**30,
            )
    else:
        resolved = dump_date
        logger.info("Opinion clusters: reading local dump {}", local_file)

    # 3. Load the court scope, so every row can say which court decided it.
    #
    # The cluster dump has no court_id — that lives on the docket, one join and
    # a different 4.67 GiB file away. Resolving it *while shaping* rather than
    # afterwards is what keeps the first build's promote path intact: joining
    # ten million clusters against seventy-two million dockets in duckdb would
    # rewrite the whole 3.9 GB table, and the whole reason that path exists is
    # that this machine cannot afford to hold two copies of it.
    scope: CourtScope | None = None
    if not skip_court_scope:
        jurisdictions = court_jurisdictions(dump_date=resolved, local_file=courts_local_file)
        if catch_up_only:
            # Search rows name their court; only bulk rows need the docket map.
            scope = CourtScope.for_courts(jurisdictions)
        else:
            map_file = docket_court_map or build_docket_court_map(output_dir, dump_date=resolved)
            scope = CourtScope.from_map(map_file, jurisdictions)

    # 4. Stream the dump into the staging table, batch by batch, noting where it ends.
    writer = CourtListenerTableWriter(new_file, schema=_SCHEMA, batch_size=BATCH_ROWS)
    export_high = held_high if catch_up_only else None
    try:
        if not catch_up_only:
            reader = CourtListenerBulkReader(
                DATASET, dump_date=resolved, local_file=local_file, max_records=max_records
            )
            with closing(cast(Generator[dict, None, None], reader.iter_records())) as source_rows:
                for row in source_rows:
                    shaped = _shape_bulk(row, scope=scope)
                    writer.add(shaped)
                    cluster_id = shaped["cluster_id"]
                    if cluster_id and cluster_id.isdecimal():
                        export_high = max(export_high or 0, int(cluster_id))

        bulk_rows = writer.written + len(writer.rows)

        # 5. Search catch-up for every cluster created after the export was cut.
        # The range is disjoint from the export's ids, so no bulk row is replaced
        # by the narrower search row and the first build below stays duplicate-free.
        search_rows = 0
        if not skip_search_catchup and export_high is not None:
            logger.info("Opinion clusters: search catch-up for clusters above id {:,}", export_high)
            for result in CourtListenerOpinionSearchReader(above=export_high).iter_records():
                writer.add(_shape_search(result, scope=scope))
                search_rows += 1
        writer.close()
    except BaseException:
        try:
            writer.abort()
        except Exception:
            logger.exception("Could not finish cleaning up failed CourtListener staging output")
        raise
    logger.info(
        "Opinion clusters: staged {:,} rows ({:,} bulk + {:,} search)",
        writer.written,
        bulk_rows,
        search_rows,
    )

    # 6. Merge prior + new, dedup on cluster_id preferring the freshest row.
    #
    # A first build has nothing to merge *against*: one dump, whose cluster_id is
    # the publisher's primary key, and a catch-up above its highest id, so the
    # dedup is a no-op. Running the merge anyway would hold the staged copy and
    # the merged copy on disk at once — 7.1 GiB rather than 3.6 GiB at the
    # 2026-06-30 dump's size — which is the difference between fitting inside
    # the project's free-space floor and not.
    # So the first build promotes the staged file instead, and pays for that with
    # dump order rather than date order.
    if not have_prior:
        new_file.replace(out_file)
        total = pq.ParquetFile(out_file).metadata.num_rows
        logger.info("Court opinion clusters: {:,} rows (first build, dump order)", total)
        return out_file

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    merge_local_prior(
        con,
        columns=COLUMNS,
        identity="cluster_id",
        order_by="date_filed DESC, cluster_id",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court opinion clusters: {:,} rows", total)
    return out_file
