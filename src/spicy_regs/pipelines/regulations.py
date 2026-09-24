"""The regulations.gov ETL, expressed through the Reader/Writer/Pipeline base classes.

This is also the run file: invoke it via the ``run-pipeline`` console script,
e.g. ``uv run run-pipeline --skip-upload --since-year 2025``. ``run()`` reads
top-to-bottom as the data flow: prime the incremental Manifest and existing
output, extract every agency's record streams in parallel into staging Parquet,
merge and deduplicate the staging, then save the Manifest and publish to R2
(``_publish``, which advances the manifest strictly last). Publishing stays off
until this path is vetted; pass ``--no-skip-upload`` to turn it on. The
reusable pieces live elsewhere — connection details and the reader factory in
``spicy_docs.sources.mirrulations``, the json→record transform in
``transforms.extract``, the parallel fan-out in ``pipelines.staging``, R2 in
``sources.r2``, processed-key tracking in ``manifest`` — so this module is just
the wiring.
"""

from contextlib import ExitStack
from os import getenv
from pathlib import Path
from shutil import rmtree
from time import monotonic
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_docs.sources import mirrulations

from spicy_regs.manifest import Manifest
from spicy_regs.pipelines.regulations_state import UnresolvedKeys, source_record_type
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.pipelines.staging import stage_agencies
from spicy_regs.schemas import RECORD_TYPES, RecordType
from spicy_regs.sources import iceberg, r2
from spicy_regs.pipelines.comment_text import PendingCommentText, PENDING_TEXT_FILE
from spicy_regs.transforms.derived_text_pool import DerivedTextPool
from spicy_regs.transforms.comment_partitions import validate_staged_comments
from spicy_regs.transforms.reviewed_comments import ExcludeReviewedComments
from spicy_regs.transforms import (
    Chain,
    EnrichCommentText,
    ExtractRecords,
    Transform,
    merge_comments_partitioned,
    merge_staging_files,
    update_comments_index,
    write_staging,
)

# With ``batch_count``, a batch's size is derived from the discovered agencies so
# none falls off the end: a fixed ``--batch-size 22`` over 15 batches (330 slots)
# silently left VCNP, VETS, WAPA, WCPO and WHD in no batch once discovery
# returned 335 agencies (2026-09-24). The ceiling makes a jump in the mirror's
# agency list a refusal to act on (raise BATCH_COUNT) instead of quietly longer jobs.
MAX_DERIVED_BATCH_SIZE = 25


class RegulationsPipeline(Pipeline):
    """Mirrulations S3 → Parquet ETL, composed from Readers, Writers, and transforms."""

    name: ClassVar[str] = "regulations"

    def __init__(
        self,
        *,
        agency: str | None = None,
        output_dir: Path | None = None,
        since_year: int | None = None,
        skip_upload: bool = True,
        skip_comments: bool = False,
        only_comments: bool = False,
        batch_number: int | None = None,
        batch_size: int = 45,
        batch_count: int | None = None,
        full_refresh: bool = False,
        allow_fresh_start: bool = False,
        max_workers: int = 4,
        text_workers: int = 8,
        use_iceberg: bool = False,
        enrich_text: bool = True,
        chunk_size: int = 0,
        verbose: bool = False,
    ) -> None:
        self.agency = agency
        self.output_dir = output_dir
        self.since_year = since_year
        self.skip_upload = skip_upload
        self.skip_comments = skip_comments
        self.only_comments = only_comments
        self.batch_number = batch_number
        self.batch_size = batch_size
        self.batch_count = batch_count
        self.full_refresh = full_refresh
        self.allow_fresh_start = allow_fresh_start
        self.max_workers = max_workers
        self.text_workers = text_workers
        self.use_iceberg = use_iceberg
        self.enrich_text = enrich_text
        self.chunk_size = chunk_size
        self.verbose = verbose
        self._pending_text: PendingCommentText | None = None
        self._text_pool: DerivedTextPool | None = None

    def run(self, *, manifest: Manifest | None = None) -> None:
        """Run one batch; local batches sharing output_dir may reuse their manifest."""
        if manifest is not None and self.full_refresh:
            raise ValueError("A reused manifest cannot be combined with full_refresh")
        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        self._pending_text: PendingCommentText | None = None
        self._text_pool: DerivedTextPool | None = None
        started = monotonic()
        if manifest is None:
            manifest = Manifest.empty() if self.full_refresh else Manifest.load(
                output_dir, allow_fresh_start=self.allow_fresh_start,
            )
        logger.info("ETL manifest ready in {:.1f}s", monotonic() - started)
        with ExitStack() as resources:
            if self.enrich_text and not self.skip_comments:
                self._pending_text = PendingCommentText(output_dir)
                self._text_pool = resources.enter_context(
                    DerivedTextPool(mirrulations.s3_resource, max_workers=self.text_workers)
                )
            self._run(manifest)
        logger.info("ETL batch completed in {:.1f}s", monotonic() - started)

    def _run(self, manifest: Manifest) -> None:
        output_dir = self.output_dir or (Path.cwd() / "output")
        staging_dir = output_dir / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)

        record_types = self._record_types()
        agencies = self._agencies()

        # Chunked comments path (see _ingest_comments_chunked). Only the Iceberg
        # comments-only case can use it: the catalog is a row-level upsert
        # surface, so each chunk commits on its own.
        if self.chunk_size and self.only_comments and self.use_iceberg:
            for agency in agencies:
                self._ingest_comments_chunked(agency, output_dir, staging_dir, manifest)
            self._retry_text(output_dir, agencies)
            if self._pending_text is not None:
                self._pending_text.save()
                if not self.skip_upload:
                    self._publish(output_dir, record_types, {}, [])
            rmtree(staging_dir, ignore_errors=True)
            logger.info("Done!")
            return

        # 1. Prime: load processed-key manifest + existing output for incremental work.
        started = monotonic()
        if self.full_refresh:
            logger.info("Full refresh — ignoring manifest and existing output")
        else:
            self._download_existing(output_dir, record_types)

        logger.info("ETL priming completed in {:.1f}s", monotonic() - started)
        started = monotonic()
        # 2. Extract → stage: fan agencies out, pumping each source into staging.
        logger.info(
            "Processing {} agencies × {} record types ({} workers)",
            len(agencies),
            len(record_types),
            self.max_workers,
        )
        unresolved = UnresolvedKeys(output_dir)
        source_types = {rt.name: source_record_type(rt) for rt in record_types}
        read = mirrulations.reader_factory(
            list(source_types.values()), processed_keys=manifest, unresolved_keys=unresolved.for_reader,
            since_year=self.since_year, verbose=self.verbose,
        )
        result = stage_agencies(
            agencies,
            record_types,
            staging_dir,
            lambda agency, rt: read(agency, source_types[rt.name]),
            transform_for=self._transform_for,
            max_workers=self.max_workers,
        )
        logger.info("ETL source staging completed in {:.1f}s", monotonic() - started)
        started = monotonic()
        # 3. Transform: merge per-agency staging into the deduplicated dataset.
        staged = result.rows_by_type
        changed_comments: list[Path] = []
        if any(staged.values()):
            changed_comments = self._merge(staging_dir, output_dir, record_types, staged)
            rmtree(staging_dir, ignore_errors=True)
            # Rollups (feed_summary, agency_stats, agency_monthly_volume,
            # docket_search, rulemaking_lifecycles, discovery_signals,
            # fr_docket_links) are no longer built here. Each is materialized by
            # its own decoupled pipeline (`run-rollup-*`) on an independent cron,
            # reading the base tables this ETL publishes below.
        else:
            logger.info("No new records staged; skipping merge.")

        changed_comments = sorted(set(changed_comments) | set(self._retry_text(output_dir, agencies)))
        logger.info("ETL merge and text retries completed in {:.1f}s", monotonic() - started)
        started = monotonic()
        # 4. Load: persist the manifest, then publish to R2 (off by default while vetting).
        unresolved.update(result.consumed_keys, result.unresolved)
        if self._pending_text is not None:
            self._pending_text.save()
        manifest.record(result.consumed_keys)
        manifest.save(output_dir)
        if self.skip_upload:
            logger.info("skip_upload=True — output left in {}", output_dir)
        else:
            logger.info("Uploading to R2...")
            self._publish(output_dir, record_types, staged, changed_comments)

        logger.info("ETL checkpoint and publication completed in {:.1f}s", monotonic() - started)
        logger.info("Done!")

    def _retry_text(self, output_dir: Path, agencies: list[str]) -> list[Path]:
        if self._pending_text is None or self._text_pool is None:
            return []
        return self._pending_text.retry(self._text_pool, output_dir, agencies, use_iceberg=self.use_iceberg)

    def _publish(
        self,
        output_dir: Path,
        record_types: list[RecordType],
        staged: dict[str, int],
        changed_comments: list[Path],
    ) -> None:
        """Publish this run's output to R2, advancing the manifest strictly last.

        ``manifest.parquet`` is the restart checkpoint: a fresh workspace pulls
        it from R2 and skips every key it lists, so publishing it retires the
        work this run consumed. Publish it while a data file is still missing
        and the next run skips those keys as already processed — the rows stay
        missing from the bucket until a ``--full-refresh``. So every planned
        object clears its size guard first, and the manifest uploads last, once
        every data upload has returned.
        """
        base_data_types = [rt.name for rt in record_types if rt.name != "comments" and staged.get(rt.name, 0)]
        index_file = output_dir / "comments_index.parquet"
        manifest_file = output_dir / "manifest.parquet"
        unresolved_file = output_dir / "failed_keys.parquet"
        text_pending_file = output_dir / PENDING_TEXT_FILE
        # The Iceberg comments path MERGEs rows through the catalog rather than
        # under the public comments/ prefix, so it changes no partition files —
        # only the refreshed index needs publishing.
        publish_index = bool(changed_comments) or (self.use_iceberg and staged.get("comments", 0) > 0)
        if publish_index and not index_file.exists():
            raise RuntimeError("Expected comments_index.parquet after publishing comment data")
        # Manifest.save writes only when this run recorded keys, so a missing
        # file means there is no checkpoint to advance.
        publish_manifest = manifest_file.exists()

        planned = r2.dataset_files(output_dir, base_data_types) + changed_comments
        if publish_index:
            planned.append(index_file)
        if unresolved_file.exists():
            planned.append(unresolved_file)
        if text_pending_file.exists():
            planned.append(text_pending_file)
        if publish_manifest:
            planned.append(manifest_file)
        r2.preflight_uploads(output_dir, planned)

        # Redundant-looking, but --only-comments leaves base_data_types empty and
        # upload_dataset would log a misleading "no files to publish" warning.
        if base_data_types:
            r2.upload_dataset(output_dir, base_data_types)
        # Comments are partitioned, not monolithic: publish the partitions
        # changed this run plus the refreshed index.
        if changed_comments:
            logger.info("Uploading {} changed comment partitions...", len(changed_comments))
            r2.upload_comment_partitions(output_dir, changed_comments)
        elif publish_index:
            logger.info("Uploading refreshed comments index (Iceberg path)...")
            r2.upload_file(index_file, remote_key="comments_index.parquet")
        if unresolved_file.exists():
            r2.upload_file(unresolved_file, remote_key=unresolved_file.name)
        if text_pending_file.exists():
            r2.upload_file(text_pending_file, remote_key=text_pending_file.name)
        if publish_manifest:
            logger.info("Uploading manifest after all data files succeeded...")
            r2.upload_file(manifest_file, remote_key="manifest.parquet")

    def _ingest_comments_chunked(self, agency: str, output_dir: Path, staging_dir: Path, manifest: Manifest) -> None:
        """Ingest one agency's comments in bounded key-chunks, committing each.

        Downloads at most ``chunk_size`` comment files at a time, stages them,
        MERGEs them into the catalog, then deletes the staging chunk — so peak
        memory stays at about one chunk and a multi-million-comment agency, which
        would otherwise buffer every record and OOM, ingests in durable pieces.
        """
        comment_rt = RECORD_TYPES["comments"]
        resource = mirrulations.s3_resource()
        keys = mirrulations.list_agency_files_by_type(
            resource,
            mirrulations.BUCKET,
            mirrulations.PREFIX,
            agency,
            [source_record_type(comment_rt)],
            processed_keys=manifest,
            since_year=self.since_year,
            verbose=self.verbose,
        )["comments"]
        transform = self._transform_for(comment_rt)
        total = len(keys)
        logger.info("[{}] comments: {} files, ingesting in chunks of {}", agency, total, self.chunk_size)

        unresolved = UnresolvedKeys(output_dir)
        previous = unresolved.for_reader(agency, comment_rt)
        keys = list(dict.fromkeys([*(item.key for item in previous), *keys]))
        total = len(keys)
        for start in range(0, total, self.chunk_size):
            chunk = keys[start : start + self.chunk_size]
            reader = mirrulations.MirrulationsReader(
                resource, mirrulations.BUCKET, mirrulations.PREFIX, agency, source_record_type(comment_rt),
                key_lister=lambda: chunk,
                unresolved_keys=[item for item in previous if item.key in chunk],
            )
            records = list(transform.apply(reader.iter_records()))
            if records:
                write_staging(agency, comment_rt.name, records, staging_dir, comment_rt.schema)
                iceberg.merge_comments(staging_dir, output_dir, comment_rt)
                rmtree(staging_dir / comment_rt.name, ignore_errors=True)
            # Only a completed read and successful merge can retire a key.
            unresolved.update(reader.last_keys, {(agency, comment_rt.name): reader.unresolved})
            if self._pending_text is not None:
                self._pending_text.save()
            manifest.record(reader.last_keys)
            manifest.save(output_dir)
            if not self.skip_upload:
                self._publish(output_dir, [comment_rt], {comment_rt.name: len(records)}, [])
            logger.info("[{}] comments: committed {}/{}", agency, start + len(chunk), total)

    # -- regulations-specific wiring ---------------------------------------

    def _transform_for(self, record_type: RecordType) -> Transform:
        """Build the staging transform for one record type.

        All agencies share the run's bounded comment-text pool and retry state.
        """
        extract: Transform = ExtractRecords(record_type)
        if record_type.name != "comments":
            return extract
        extract = Chain(ExcludeReviewedComments(), extract)
        if self._text_pool is not None and self._pending_text is not None:
            return Chain(extract, EnrichCommentText(self._text_pool, observe=self._pending_text.observe))
        return extract

    def _record_types(self) -> list[RecordType]:
        """The record types to process, honoring skip/only-comments."""
        names = list(RECORD_TYPES)
        if self.skip_comments:
            names = [n for n in names if n != "comments"]
        elif self.only_comments:
            names = ["comments"]
        return [RECORD_TYPES[n] for n in names]

    def _agencies(self) -> list[str]:
        """Agencies to process: explicit, AGENCIES env, or S3 discovery; then batched.

        ``batch_count`` splits the list into that many contiguous batches of
        ``ceil(len / batch_count)`` agencies, overriding ``batch_size``.
        """
        if self.agency is not None:
            agencies = [self.agency]
        elif (agencies_env := getenv("AGENCIES")) is not None:
            agencies = agencies_env.split(",")
        else:
            agencies = mirrulations.discover_agencies()

        if self.batch_number is not None:
            size = self.batch_size
            if self.batch_count is not None:
                if not 0 <= self.batch_number < self.batch_count:
                    raise ValueError(f"batch_number {self.batch_number} is outside 0..{self.batch_count - 1}")
                size = -(-len(agencies) // self.batch_count)
                if size > MAX_DERIVED_BATCH_SIZE:
                    raise RuntimeError(
                        f"{len(agencies)} agencies over {self.batch_count} batches needs {size} per batch, "
                        f"above the ceiling of {MAX_DERIVED_BATCH_SIZE}; raise BATCH_COUNT in etl-new-pipeline.yml"
                    )
            start = self.batch_number * size
            agencies = agencies[start : start + size]
        return agencies

    def _download_existing(self, output_dir: Path, record_types: list[RecordType]) -> None:
        """Fetch existing output from R2 so an incremental run appends to it.

        Monolithic ``{type}.parquet`` files are pulled whole. Comment
        *partitions* are large and fetched on demand during the merge, but the
        global comment index must be primed here: ``update_comments_index``
        rebuilds the index by keeping the existing rows for partitions this run
        didn't touch, reading them from the local ``comments_index.parquet``.
        Without the remote index on disk, a batch that stages new comments
        rewrites the index down to only its own ~21 agencies' partitions — and
        the upload shrink-guard then (correctly) aborts the run.
        """
        for rt in record_types:
            if rt.name == "comments":
                index_file = output_dir / "comments_index.parquet"
                if not index_file.exists():
                    r2.download("comments_index.parquet", index_file)
                continue
            local = output_dir / f"{rt.name}.parquet"
            if not local.exists():
                r2.download(f"{rt.name}.parquet", local)

    def _merge(
        self,
        staging_dir: Path,
        output_dir: Path,
        record_types: list[RecordType],
        staged: dict[str, int],
    ) -> list[Path]:
        """Merge staging files: dockets/documents monolithically, comments partitioned.

        When ``use_iceberg`` is set, the ``dockets`` table is routed through the
        R2 Data Catalog (Iceberg ``MERGE INTO`` + public Parquet export) instead
        of the whole-file ``merge_staging_files`` rewrite, and ``comments`` are
        routed through :func:`iceberg.merge_comments` (row-level upsert into the
        catalog + index rebuild, no monolithic export) instead of the
        partitioned ``merge_comments_partitioned`` path. ``documents`` stay on
        the existing whole-file path until the Iceberg flow is vetted for them.

        Returns the comment partition files changed this run (empty when no
        comments were staged or when comments went through Iceberg), so the
        caller can publish exactly those to R2.
        """
        names = [rt.name for rt in record_types]
        if "comments" in names:
            validate_staged_comments(staging_dir)

        non_comment = [n for n in names if n != "comments"]

        iceberg_names = []
        if self.use_iceberg and "dockets" in non_comment:
            iceberg_names = ["dockets"]
            non_comment = [n for n in non_comment if n != "dockets"]

        if non_comment:
            schemas = {n: RECORD_TYPES[n].schema for n in non_comment}
            dedup_keys = {n: RECORD_TYPES[n].dedup_key for n in non_comment}
            merge_staging_files(staging_dir, output_dir, non_comment, schemas, dedup_keys)

        # Iceberg path: MERGE into the catalog, then export the public Parquet
        # snapshot that the existing R2 upload (dual model) will publish.
        for name in iceberg_names:
            iceberg.merge_and_export(staging_dir, output_dir, RECORD_TYPES[name])

        changed_comments: list[Path] = []
        if "comments" in names and staged.get("comments", 0) > 0:
            if self.use_iceberg:
                # Row-level upsert into the catalog table (the read surface) and
                # rebuild the index. No partition files are produced, so
                # changed_comments stays empty and the caller publishes only the
                # refreshed index.
                iceberg.merge_comments(staging_dir, output_dir, RECORD_TYPES["comments"])
            else:
                changed_comments = merge_comments_partitioned(
                    staging_dir,
                    output_dir,
                    schema=RECORD_TYPES["comments"].schema,
                    dedup_key=RECORD_TYPES["comments"].dedup_key,
                )
                if changed_comments:
                    update_comments_index(output_dir, changed_comments)
        return changed_comments


# --- CLI / run file --------------------------------------------------------

app = App(name="run-pipeline", help="Run the regulations.gov ETL through the Pipeline contract.")


@app.default
def main(
    *,
    agency: Annotated[str | None, Parameter(help="Process only this agency")] = None,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    since_year: Annotated[int | None, Parameter(help="Only process dockets from this year onward")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    skip_comments: Annotated[bool, Parameter(help="Skip comments")] = False,
    only_comments: Annotated[bool, Parameter(help="Only process comments")] = False,
    batch_number: Annotated[int | None, Parameter(help="Batch number (0-indexed)")] = None,
    batch_size: Annotated[int, Parameter(help="Agencies per batch")] = 45,
    batch_count: Annotated[
        int | None,
        Parameter(help="Split the agencies into this many batches, deriving each batch's size (overrides batch-size)"),
    ] = None,
    full_refresh: Annotated[bool, Parameter(help="Ignore manifest + existing output")] = False,
    allow_fresh_start: Annotated[
        bool,
        Parameter(help="Start from an empty manifest when none exists locally or on R2 (first run or bootstrap)"),
    ] = False,
    max_workers: Annotated[int, Parameter(help="Agencies processed in parallel")] = 4,
    text_workers: Annotated[int, Parameter(help="Comment text reads in parallel across all agencies (at most 16)")] = 8,
    use_iceberg: Annotated[bool, Parameter(help="Route the dockets table through R2 Data Catalog (Iceberg)")] = False,
    enrich_text: Annotated[
        bool,
        Parameter(help="Fill comment text_content inline from Mirrulations derived-data extracted text"),
    ] = True,
    chunk_size: Annotated[
        int,
        Parameter(
            help="Ingest comments in bounded key-chunks of this size (0 = whole agency at once). "
            "Use for agencies too large to buffer in memory; requires --only-comments --use-iceberg."
        ),
    ] = 0,
    verbose: Annotated[bool, Parameter(name=["--verbose", "-v"], help="Verbose logging")] = False,
) -> None:
    """Run the regulations.gov ETL pipeline."""
    load_dotenv()
    RegulationsPipeline(
        agency=agency,
        output_dir=output_dir,
        since_year=since_year,
        skip_upload=skip_upload,
        skip_comments=skip_comments,
        only_comments=only_comments,
        batch_number=batch_number,
        batch_size=batch_size,
        batch_count=batch_count,
        full_refresh=full_refresh,
        allow_fresh_start=allow_fresh_start,
        max_workers=max_workers,
        text_workers=text_workers,
        use_iceberg=use_iceberg,
        enrich_text=enrich_text,
        chunk_size=chunk_size,
        verbose=verbose,
    ).run()


if __name__ == "__main__":
    app()
