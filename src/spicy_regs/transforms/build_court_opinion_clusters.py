"""Transform: build ``court_opinion_clusters.parquet`` from the CourtListener bulk dumps.

A CourtListener *cluster* is one decision — the case-level record that sibling
opinions hang off — and its ``docket_id`` is what connects opinion text to the
APA litigation in ``court_dockets`` and, through the docket's party names, to
the agencies in ``agency_stats``.

Bulk supplies the complete export. Indexed clusters of every publisher-defined
status above the published table's highest id are added a day at a time from
authenticated ``/search/?type=o`` by
:mod:`spicy_regs.transforms.catch_up_court_opinion_clusters`, never by this
build: a search that cannot finish inside the account's quota must not discard
a multi-hour export read (ss-jran). Search can omit unindexed RECAP clusters,
and historical corrections arrive with later bulk exports. Search exhaustion
and a maximum-ID check do not prove complete citation membership. Direct
authenticated ``/clusters/`` is a valid alternative, but has a different count
representation and requires a docket lookup for the court; it is not the
catch-up's source shape.
The publisher numbers clusters in creation order and cuts each export table at
a different hour of the export day (2026-06-30: clusters by 08:16 UTC, opinions
09:56, citations 19:14), so the sibling tables name clusters created after this
one was cut. Best-effort
prior from R2, then dedup on ``cluster_id`` preferring the freshest row; rows
are written in batches because the dump runs to ten million clusters and one
``from_pylist`` over that is an out-of-memory error. The dump has no
``court_id`` — it lives on the docket, in a different multi-GiB file — so
``court_id`` / ``court_jurisdiction`` / ``court_is_federal`` are resolved from
a docket→court map (see :mod:`spicy_regs.transforms.court_scope`) while each
row is shaped; ``skip_court_scope`` opts out and leaves the three columns NULL
at the cost of a ~46-minute map build. A build whose prior already holds the
newest export reads neither dump and republishes the prior's rows.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from datetime import date
from functools import partial
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs import court_bulk
from spicy_regs.sources import r2
from spicy_regs.transforms._courtlistener_writer import CourtListenerTableWriter
from spicy_regs.sources.courtlistener import record_json
from spicy_regs.transforms.court_scope import (
    CourtScope,
    build_docket_court_map,
    court_jurisdictions,
)
from spicy_regs.transforms.table_merge import merge_local_prior
from spicy_regs.court_subjects import SUBJECT_SCHEMAS, normalize_court_row
from spicy_regs.court_receipts import (
    prior_receipt_selection, read_court_rows, restore_processing_input,
)

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "court_opinion_clusters.parquet"
DATASET = "opinion-clusters"
#: The journal event a build leaves for the next run: the export edition its table holds and its highest id.
EXPORT_EVENT = "export-selection"

CL_BASE_URL = "https://www.courtlistener.com"

#: Rows buffered before each parquet batch write.
BATCH_ROWS = 25_000

# Published schema: all VARCHAR, keyed by cluster_id. ``cl_docket_id`` is the
# bulk dump's ``docket_id``, renamed to match ``court_dockets.cl_docket_id`` so
# the join reads the same on both sides.
INPUT_COLUMNS = (
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
    "raw_source_record",
)
_SCHEMA = pa.schema([(c, pa.list_(pa.string()) if c == 'attorneys' else pa.string()) for c in INPUT_COLUMNS])
_NATIVE_SCHEMA = SUBJECT_SCHEMAS['court_opinion_clusters']
COLUMNS = tuple(_NATIVE_SCHEMA.names)
_RETIRED_PRIOR_COLUMNS = frozenset({'cluster_id', 'date_created', 'ingest_source'})


def _s(value: object) -> str | None:
    """Keep the source's null versus empty distinction."""
    if value is None:
        return None
    text = str(value)
    return text


def _counsel(value):
    """Keep free prose whole, and preserve actual arrays without splitting names."""
    return value if value is None or isinstance(value, list) else [str(value)]


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
        "attorneys": _counsel(row.get("attorneys")),
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
        "raw_source_record": None,
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
    row = dict.fromkeys(INPUT_COLUMNS)
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
            "attorneys": _counsel(result.get("attorney")),
            "posture": _s(result.get("posture")),
            "syllabus": _s(result.get("syllabus")),
            "absolute_url": absolute,
            "ingest_source": "search",
            "raw_source_record": record_json(result),
        }
    )
    return row


def held_export(table: Path, *, receipt_path: Path | None = None,
                generation_id: str | None = None) -> tuple[int | None, str | None]:
    """A clusters table's highest bulk ``cluster_id`` and the day of its newest bulk ``date_created``.

    Together they name the export the table holds without a stamp: the id is
    where that export ended, and the export was cut on its dump date, so a table
    whose newest bulk row was created on or after a dump's date already holds it.
    One scan of two columns.
    """
    if receipt_path is None and _RETIRED_PRIOR_COLUMNS <= set(pq.read_schema(table).names):
        return _held_restored_export(table)
    high, created = None, None
    for row in read_court_rows(table, dataset='court_opinion_clusters', receipt_path=receipt_path,
                              generation_id=generation_id):
        if row.get('ingest_source') == 'bulk':
            identity = row.get('cluster_id')
            if identity and identity.isdecimal():
                high = max(high or 0, int(identity))
            if row.get('date_created'):
                created = max(created or '', row['date_created'])
    return high, created[:10] if created else None


def _held_restored_export(table: Path) -> tuple[int | None, str | None]:
    """Scan the already restored inputs without another receipt admission or replay."""
    from spicy_regs.etl_bulk import _literal, bulk_connection

    with bulk_connection() as (con, _):
        con.execute('CREATE VIEW held AS SELECT cluster_id,date_created FROM read_parquet('
                    + _literal(str(table)) + ") WHERE ingest_source='bulk'")
        high, created = con.execute("SELECT max(TRY_CAST(cluster_id AS HUGEINT)) FILTER "
                                   "(WHERE regexp_full_match(cluster_id, '[0-9]+')), max(date_created) FROM held").fetchone()
        # Preserve Python's Unicode decimal and arbitrary-size integer behavior
        # for the exceptional keys that the bounded SQL integer cannot express.
        for batch in con.execute("SELECT cluster_id FROM held WHERE cluster_id IS NOT NULL AND "
                                 "(NOT regexp_full_match(cluster_id, '[0-9]+') OR "
                                 "TRY_CAST(cluster_id AS HUGEINT) IS NULL)").to_arrow_reader(2000):
            for identity in batch.column(0).to_pylist():
                if identity and identity.isdecimal():
                    high = max(high or 0, int(identity))
    return high, created[:10] if created else None


def _to_native_table(source: Path, destination: Path, work_dir: Path, *,
                     exceptions: Path | None = None) -> tuple[int, int]:
    """Convert a staged or restored clusters table into the typed native schema with no receipts."""
    if exceptions is not None:
        exceptions.unlink(missing_ok=True)
        return court_bulk.write_table(
            'court_opinion_clusters',
            source,
            destination,
            constants={},
            exceptions=exceptions,
            attempt=partial(normalize_court_row, 'court_opinion_clusters'),
        )
    with TemporaryDirectory(prefix='.clusters-exc-', dir=work_dir) as tmp:
        return court_bulk.write_table(
            'court_opinion_clusters',
            source,
            destination,
            constants={},
            exceptions=Path(tmp) / 'exceptions.parquet',
            attempt=partial(normalize_court_row, 'court_opinion_clusters'),
        )


def build_court_opinion_clusters(
    output_dir: Path,
    *,
    dump_date: date | None = None,
    local_file: Path | None = None,
    max_records: int | None = None,
    docket_court_map: Path | None = None,
    courts_local_file: Path | None = None,
    skip_court_scope: bool = False,
    include_prior: bool = True,
    evidence: CaptureEvidence | None = None,
) -> Path:
    """Build ``court_opinion_clusters.parquet`` from a bulk dump, merged over the prior table.

    ``skip_court_scope`` leaves the three court columns NULL and skips the
    46-minute ``dockets`` read. It is the honest way to build the table without
    the scope, and it is not the default: a decision table that cannot say which
    court decided is a table nobody can ask the obvious question of.

    With neither ``dump_date`` nor ``local_file``, a prior that already holds the
    newest listed export (:func:`held_export`) is republished as it stands: no
    dump is read. Naming an edition or a file always reads it. The prior's rows
    from search, above the export, are carried through the merge.

    ``courts_local_file`` pairs a retained courts dump with a supplied docket
    map. ``include_prior=False`` builds the selected source edition alone,
    without downloading, reading or removing a retained prior table. Combine
    these with ``local_file`` for an offline build from pinned originals.

    ``evidence`` receives ``EXPORT_EVENT`` for a dated edition, so the next run
    knows which export the table holds without restoring it.
    """
    import duckdb

    from spicy_docs.sources.courtlistener.bulk import (
        CourtListenerBulkReader,
        find_dump,
        latest_dump_date,
        list_bulk_dumps,
    )
    from spicy_regs.transforms.catch_up_court_opinion_clusters import held_selection

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / OUTPUT
    prior_file = out_file if out_file.exists() else output_dir / "_clusters_prior.parquet"
    new_file = output_dir / f".clusters-new-{uuid4().hex}.parquet"

    # 1. Resolve which published dump to read before downloading or restoring any prior.
    resolved: date | None
    published = None
    if local_file is None:
        objects = list_bulk_dumps()
        resolved = dump_date or latest_dump_date(objects, DATASET)
        if resolved is None:
            raise RuntimeError(f"CourtListener bulk: no published {DATASET} dump found")
        published = find_dump(objects, DATASET, resolved)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {DATASET} dump for {resolved}")
    else:
        resolved = dump_date

    # 2. Check the journal's held export before downloading or restoring 10M+ prior rows.
    journal_held = (
        held_selection(evidence)
        if include_prior and local_file is None and dump_date is None and evidence is not None
        else None
    )
    held_high: int | None = None
    holds_newest = False
    if journal_held is not None and resolved is not None and journal_held[0] >= resolved:
        held_high, holds_newest = journal_held[1], True

    # 3. Prior table (absence just means first build).
    downloaded_prior = False
    have_prior = False
    if include_prior:
        if prior_file.exists():
            have_prior = True
        elif r2.download(OUTPUT, prior_file):
            have_prior = True
            downloaded_prior = prior_file != out_file
    if not have_prior:
        holds_newest = False

    restored_prior: Path | None = None
    prior_is_native = False
    prior_has_export_cols = False
    if have_prior:
        prior_schema = pq.read_schema(prior_file)
        prior_is_native = prior_schema.equals(_NATIVE_SCHEMA)
        prior_has_export_cols = _RETIRED_PRIOR_COLUMNS <= set(prior_schema.names)
        if prior_has_export_cols:
            if not holds_newest and local_file is None and dump_date is None:
                held_high, held_day = _held_restored_export(prior_file)
                holds_newest = (
                    held_high is not None and held_day is not None
                    and resolved is not None and held_day >= resolved.isoformat()
                )
        else:
            receipt_path, generation_id = prior_receipt_selection(prior_file, dataset='court_opinion_clusters')
            restored_prior = restore_processing_input(
                prior_file,
                output_dir / f'.clusters-prior-{uuid4().hex}.parquet',
                dataset='court_opinion_clusters',
                schema=_SCHEMA,
                receipt_path=receipt_path,
                generation_id=generation_id,
            )
            if receipt_path is not None and not (prior_file.is_symlink() or (prior_file.parent / 'court-build.json').exists() or (prior_file.parent / 'court-family.json').exists()):
                receipt_path.unlink(missing_ok=True)
            if not holds_newest and local_file is None and dump_date is None:
                held_high, held_day = _held_restored_export(restored_prior)
                holds_newest = (
                    held_high is not None and held_day is not None
                    and resolved is not None and held_day >= resolved.isoformat()
                )

    if holds_newest:
        logger.info("Opinion clusters: prior holds the {} export (to id {:,}); no dump is read", resolved, held_high)
        staged_prior = output_dir / f'.clusters-prior-copy-{uuid4().hex}.parquet'
        try:
            source_prior = restored_prior or prior_file
            if restored_prior is None and prior_is_native:
                if prior_file != out_file:
                    shutil.copyfile(prior_file, staged_prior)
                    staged_prior.replace(out_file)
            else:
                _to_native_table(source_prior, staged_prior, output_dir)
                staged_prior.replace(out_file)
        finally:
            staged_prior.unlink(missing_ok=True)
            if restored_prior is not None:
                restored_prior.unlink(missing_ok=True)
            if downloaded_prior:
                prior_file.unlink(missing_ok=True)
        total = pq.ParquetFile(out_file).metadata.num_rows
        logger.info("Court opinion clusters: {:,} rows", total)
        _journal_export(evidence, resolved, out_file)
        return out_file

    logger.info(
        "Opinion clusters: {}",
        f"merging against prior table {prior_file}" if have_prior else "no prior table — full build",
    )
    if local_file is None:
        assert published is not None
        logger.info(
            "Opinion clusters: reading dump {} ({:.3f} GiB compressed)",
            published.filename,
            published.size / 2**30,
        )
    else:
        logger.info("Opinion clusters: reading local dump {}", local_file)

    # 4. Load the court scope, so every row can say which court decided it.
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
        map_file = docket_court_map or build_docket_court_map(output_dir, dump_date=resolved)
        scope = CourtScope.from_map(map_file, jurisdictions)

    # 5. Stream the dump into the staging table, batch by batch.
    writer = CourtListenerTableWriter(new_file, schema=_SCHEMA, batch_size=BATCH_ROWS)
    try:
        reader = CourtListenerBulkReader(
            DATASET, dump_date=resolved, local_file=local_file, max_records=max_records
        )
        with closing(cast(Generator[dict, None, None], reader.iter_records())) as source_rows:
            for row in source_rows:
                writer.add(_shape_bulk(row, scope=scope))
        if reader.stopped_early:
            raise RuntimeError('Opinion cluster dump was not read to its end')

        writer.close()
    except BaseException:
        try:
            writer.abort()
        except Exception:
            logger.exception("Could not finish cleaning up failed CourtListener staging output")
        if restored_prior is not None:
            restored_prior.unlink(missing_ok=True)
        raise
    logger.info("Opinion clusters: staged {:,} bulk rows", writer.written)

    # 6. Convert staged bulk rows directly to native schema (recording refusals/respellings in exceptions),
    # skipping the redundant admit_court_updates round-trip.
    exceptions = output_dir / "court_opinion_clusters.exceptions.parquet"
    typed_new = output_dir / f".clusters-typed-{uuid4().hex}.parquet"
    try:
        _, recorded_exceptions = _to_native_table(new_file, typed_new, output_dir, exceptions=exceptions)
    except BaseException:
        if restored_prior is not None:
            restored_prior.unlink(missing_ok=True)
        raise
    finally:
        new_file.unlink(missing_ok=True)
    if evidence is not None and recorded_exceptions:
        evidence.retain_file(exceptions, stage="courtlistener-export-exceptions", table="court_opinion_clusters")

    # 7. Merge prior + new, dedup on cluster_id preferring the freshest row.
    #
    # A first build has nothing to merge *against*: one dump, whose cluster_id is
    # the publisher's primary key, so the dedup is a no-op. Running the merge
    # anyway would hold the staged copy and the merged copy on disk at once —
    # 7.1 GiB rather than 3.6 GiB at the 2026-06-30 dump's size — which is the
    # difference between fitting inside the project's free-space floor and not.
    # So the first build promotes the typed file instead, and pays for that with
    # dump order rather than date order.
    if not have_prior:
        typed_new.replace(out_file)
        total = pq.ParquetFile(out_file).metadata.num_rows
        logger.info("Court opinion clusters: {:,} rows (first build, dump order)", total)
        _journal_export(evidence, resolved, out_file)
        return out_file

    typed_prior: Path | None = None
    try:
        if restored_prior is not None or not prior_is_native:
            typed_prior = output_dir / f".clusters-prior-typed-{uuid4().hex}.parquet"
            _to_native_table(restored_prior or prior_file, typed_prior, output_dir)
        merge_prior = typed_prior or prior_file

        spill_dir = output_dir / ".duckdb_tmp"
        spill_dir.mkdir(exist_ok=True)
        con = duckdb.connect()
        try:
            con.execute("SET memory_limit='4GB'")
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET threads=2")
            con.execute(f"SET temp_directory='{spill_dir}'")

            staged = output_dir / f'.clusters-merged-{uuid4().hex}.parquet'
            merge_local_prior(
                con,
                columns=COLUMNS,
                identity="cluster_id",
                order_by="date_filed DESC, cluster_id",
                prior_file=merge_prior,
                new_file=typed_new,
                out_file=staged,
            )
        finally:
            con.close()
    finally:
        typed_new.unlink(missing_ok=True)
        if typed_prior is not None:
            typed_prior.unlink(missing_ok=True)
        if restored_prior is not None:
            restored_prior.unlink(missing_ok=True)

    staged.replace(out_file)
    if downloaded_prior:
        prior_file.unlink(missing_ok=True)
    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court opinion clusters: {:,} rows", total)
    _journal_export(evidence, resolved, out_file)
    return out_file


def _journal_export(evidence: CaptureEvidence | None, edition: date | None, table: Path) -> None:
    """State the edition a built table holds and its highest id, where the daily catch-up resumes."""
    from spicy_regs.transforms.build_court_bulk_tables import _highest_cluster_id

    if evidence is None or edition is None:
        return
    through = _highest_cluster_id([str(table)])
    if through is not None:
        evidence.event(EXPORT_EVENT, source_edition=edition.isoformat(), through=through,
                       coverage="Bulk export merged over the prior; the prior's search rows above it are carried.")
