"""Base class for decoupled, per-rollup pipelines, each on its own cron.

Each rollup (``feed_summary``, ``agency_stats``, ``rulemaking_lifecycles``, ...)
is materialized by its own :class:`RollupPipeline` subclass with its own console
entry (``run-rollup-*``) and GitHub Actions workflow, so it can be re-run or
backfilled alone and a failure stays isolated to one artifact. ``run()`` reads
top-to-bottom as the data flow: prime the base tables this rollup reads from R2
(skipping any already present locally), materialize the artifact(s) via a
transform, retain one complete immutable generation, then — unless
``skip_upload`` is set — verify uploaded bytes before atomically replacing its
public pointer.

**Contract:** a rollup reads only tables no other rollup *derives* — the ETL's
published base tables (``dockets``, ``documents``, ``comments_index``) and the
published output of an **ingest** rollup, which fetches from a publisher and
has no upstream dependency inside this repository. Reading a derived rollup's
output stays forbidden, because that is the read that makes two pipelines race,
whereas an ingest table's content comes from outside, so the worst a stale copy
costs is one cron's lag, which the offsets already handle. A best-effort
merge-time join whose absence must not fail the run is declared in
``soft_inputs``, not ``inputs``: it primes nothing, fails nothing, and leaves
the linkage columns NULL so the rollup still publishes everything its own
source establishes — it exists so the read is *stated* rather than buried in a
transform, and so ``tests/test_hosted_rollups.py`` can hold each entry to being
an ingest rollup's output whose cron runs before this one's. Multi-output
rollups declare ``outputs`` instead of ``output`` and return a tuple of paths
from ``build()``; every declared output must be present, including successful
empty tables, and each goes through the same per-file shrink guard on upload.
Uploads use immutable generation paths, and ``publication.json`` changes only
after the full family passes local schema checks, shrink checks and remote byte
verification; readers capture that index once per operation. The old bare
Parquet URLs are not rewritten by this path.
"""

from abc import abstractmethod
from contextlib import nullcontext
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, ClassVar
from uuid import uuid4

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.pipelines.base import Pipeline
from spicy_regs.sources import r2


class RollupPipeline(Pipeline):
    """A single materialized rollup, run standalone from the base tables on R2."""

    #: Base-table Parquet files this rollup reads (R2 remote keys, e.g.
    #: ``"documents.parquet"``). Primed from R2 before the build, and a
    #: missing one fails the run.
    inputs: ClassVar[tuple[str, ...]] = ()

    #: Published tables this rollup reads **best-effort** at merge time, to
    #: fill columns its own source does not carry (R2 remote keys). Not
    #: primed and never fatal: absence leaves those columns NULL and the
    #: rollup still publishes everything its own source establishes. Each
    #: entry must name an *ingest* rollup's output whose cron runs before
    #: this one's — ``tests/test_hosted_rollups.py`` holds both.
    soft_inputs: ClassVar[tuple[str, ...]] = ()

    #: Zero or more artifacts a multi-output rollup writes and publishes (R2
    #: remote keys, same convention as ``output``). Leave empty and declare
    #: ``output`` directly for the (default) single-output case; declaring
    #: ``outputs`` instead picks up the ``output`` property below, which
    #: reads as ``outputs[0]`` for anything that only needs one key (e.g. the
    #: freshness checker).
    outputs: ClassVar[tuple[str, ...]] = ()

    #: A partial writer can update this existing complete family. Unchanged
    #: siblings are carried forward from its captured generation, never rebuilt.
    publication_family: ClassVar[str | None] = None
    generation_tables: ClassVar[bool] = True
    retain_source_evidence: ClassVar[bool] = False

    #: The single artifact this rollup writes and publishes (e.g.
    #: ``"feed_summary.parquet"``). Its R2 remote key is the same filename.
    #: Single-output rollups set this directly, which — because Python
    #: resolves a class attribute from the most-derived class first —
    #: shadows the ``output`` property below entirely; it never runs for them.
    output: ClassVar[str]

    @property
    def output(self) -> str:  # noqa: F811 — intentional redefinition, see docstring above
        """``outputs[0]``, for callers that only need this rollup's first/only key."""
        if self.outputs:
            return self.outputs[0]
        raise AttributeError(f"{type(self).__name__} must set 'output' or 'outputs'")

    def __init__(self, *, output_dir: Path | None = None, skip_upload: bool = True) -> None:
        self.output_dir = output_dir
        self.skip_upload = skip_upload
        self.source_evidence = None

    def run(self) -> None:
        if not self.generation_tables:
            # Docket search is a browser gzip object, not a table family. Keep
            # its existing single-object path until that consumer is migrated.
            output_dir = self.output_dir or (Path.cwd() / "output")
            output_dir.mkdir(parents=True, exist_ok=True)
            self._prime(output_dir)
            built = self.build(output_dir)
            if isinstance(built, tuple) or built.name != self.output or self.outputs:
                raise ValueError("Legacy object rollups must produce exactly their declared single output")
            if not self.skip_upload:
                r2.upload_file(built, remote_key=self.output)
            logger.info("Legacy unversioned object retained: {}", built)
            return

        from spicy_regs.source_evidence import CaptureEvidence

        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        if self.retain_source_evidence:
            self.source_evidence = CaptureEvidence(output_dir, self.publication_family or self.name)
        try:
            self._run_tables(output_dir)
        except BaseException as error:
            if self.source_evidence:
                self.source_evidence.finish(error)
            raise
        else:
            if self.source_evidence:
                self.source_evidence.finish()

    def _run_tables(self, output_dir: Path) -> None:
        from rulespec_artifacts import publish_directory_no_replace
        from spicy_regs.data_dictionary import expected_schemas
        from spicy_regs.generations import build_generation, verify_generation
        from spicy_regs.sources import publication

        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        public_url = getenv("R2_PUBLIC_URL")
        context = publication.snapshot(public_url) if public_url else nullcontext(publication.empty_index())
        with context as prior_index:
            if self.source_evidence:
                self.source_evidence.inherit(prior_index, public_url=public_url)
            # Bespoke ingest builders also cache priors. An isolated build
            # directory covers all of them without deleting user evidence.
            build_dir = output_dir
            if public_url:
                build_dir = output_dir / ".builds" / uuid4().hex
                build_dir.mkdir(parents=True)
            self._prime(build_dir)
            logger.info("Building rollup {}...", self.output)
            built = self.build(build_dir)
            out_paths = built if isinstance(built, tuple) else (built,)
            family = self.name
            expected_keys = self.outputs or (self.output,)
            if len(out_paths) != len(expected_keys) or {p.name for p in out_paths} != set(expected_keys):
                raise publication.PublicationError("Build outputs differ from its declared complete set")
            carried_forward = {}
            publication_status = "complete-family"
            if self.publication_family:
                prior = prior_index["families"].get(self.publication_family)
                if prior is None:
                    if not self.skip_upload:
                        raise publication.PublicationError(
                            f"Publish a complete {self.publication_family} generation before its partial writer"
                        )
                    logger.warning("Retaining a local partial candidate; no complete publication family exists")
                    publication_status = "local-partial"
                else:
                    family = self.publication_family
                    expected_keys = tuple(prior["tables"])
                    if not {p.name for p in out_paths} <= set(expected_keys):
                        raise publication.PublicationError("Partial writer output is outside its existing family")
                    for key in expected_keys:
                        if key not in {p.name for p in out_paths}:
                            path = build_dir / key
                            if not r2.download(key, path):
                                raise publication.PublicationError(f"Missing carried-forward member: {key}")
                            out_paths += (path,)
                            carried_forward[key] = prior["artifactDigest"]
        generations = output_dir / "generations"
        generations.mkdir(exist_ok=True)
        with TemporaryDirectory(prefix=".generation-", dir=output_dir) as staging:
            directory = Path(staging) / "artifact"
            artifact = build_generation(
                directory, family=family, files=out_paths,
                expected_keys=expected_keys, schemas=expected_schemas(),
                read_snapshot=prior_index, carried_forward=carried_forward,
                publication_status=publication_status,
                inputs=self.source_evidence.inputs() if self.source_evidence else (),
            )
            destination = generations / artifact.pin.artifact_digest.removeprefix("sha256:")
            if destination.exists():
                verify_generation(destination, expected_pin=artifact.pin)
            else:
                publish_directory_no_replace(directory, destination)
        if self.skip_upload:
            logger.info("Verified local generation {}; upload skipped", destination)
        else:
            if not getenv("R2_ACCESS_KEY_ID") or not public_url:
                raise RuntimeError("Generation publication requires R2 credentials and R2_PUBLIC_URL")
            publication.publish_generation(
                destination, client=r2.get_r2_client(),
                bucket=getenv("R2_BUCKET_NAME", "spicy-regs"), prior_index=prior_index,
                evidence_directories=(self.source_evidence.artifact_dir,) if self.source_evidence else (),
            )
            from spicy_regs.sources.cloudflare import purge_urls

            purge_urls([f"{public_url.rstrip('/')}/{publication.INDEX_KEY}"])
        logger.info("Done!")

    def _prime(self, output_dir: Path) -> None:
        """Download each required base table from R2 unless already local.

        A missing base table is fatal: a rollup built from absent inputs would
        publish a truncated artifact that the upload shrink-guard would then
        (correctly) reject — better to fail loudly here with a clear message.
        """
        for remote_key in self.inputs:
            local = output_dir / remote_key
            if local.exists():
                logger.info("Using local {} (skipping download)", remote_key)
                continue
            if not r2.download(remote_key, local):
                raise RuntimeError(
                    f"Rollup {self.output!r}: required base table {remote_key!r} "
                    f"not found on R2 and not present locally in {output_dir}"
                )

    @abstractmethod
    def build(self, output_dir: Path) -> Path | tuple[Path, ...]:
        """Materialize the rollup into ``output_dir``.

        Return the single artifact's path, or — for a rollup that declares
        ``outputs`` — a tuple of paths, one per declared key, in any order.
        """
        ...


def make_rollup_app(pipeline_cls: type[RollupPipeline]) -> App:
    """Build the ``run-rollup-*`` cyclopts CLI for one rollup pipeline.

    All rollups share the same two flags, so each module just does::

        app = make_rollup_app(FeedSummaryRollup)
    """
    app = App(
        name=f"run-rollup-{pipeline_cls.name}",
        help=(pipeline_cls.__doc__ or "").strip().splitlines()[0] if pipeline_cls.__doc__ else "",
    )

    @app.default
    def main(
        *,
        output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
        skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    ) -> None:
        load_dotenv()
        pipeline_cls(output_dir=output_dir, skip_upload=skip_upload).run()

    return app
