"""Publish complete scorecard editions with explicit publisher evidence policy."""

from pathlib import Path
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.scorecards.etl import POLICIES, SOURCE_NAMES
from spicy_regs.scorecards.registry import REGISTRY
from spicy_regs.scorecards.acquisition import MAX_BYTES, MAX_REQUESTS, fetch_for_publishers, validate_limits
from spicy_regs.transforms.build_scorecards import OUTPUTS, NoScorecardsDue, build_scorecards


class ScorecardsRollup(RollupPipeline):
    """Refresh qualified congressional scorecard sources as one atomic family."""

    name: ClassVar[str] = "scorecards"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = OUTPUTS
    retain_source_evidence: ClassVar[bool] = True
    receipt_policies: ClassVar[tuple] = tuple(POLICIES[name] for name in SOURCE_NAMES)
    receipt_only_tables: ClassVar[tuple[str, ...]] = ("scorecard_snapshots.parquet",)

    def __init__(
        self,
        *,
        output_dir=None,
        skip_upload=True,
        registry=REGISTRY,
        publishers=(),
        editions=(),
        historical_backfill=False,
        force=False,
        provider=None,
        fetch_factory=None,
        zyte_publishers=(),
        zyte_browser_publishers=(),
        pdf_extractor=None,
        retain_extraction=None,
        max_bytes=MAX_BYTES,
        max_requests=MAX_REQUESTS,
    ):
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        validate_limits(max_bytes, max_requests)
        self.max_bytes, self.max_requests = max_bytes, max_requests
        self.registry, self.publishers, self.editions = registry, tuple(publishers), tuple(editions)
        self.historical_backfill, self.force = historical_backfill, force
        if fetch_factory is not None and (zyte_publishers or zyte_browser_publishers):
            raise ValueError("Choose an injected fetch factory or explicit Zyte publishers")
        self.provider = provider
        self.fetch_factory = (
            fetch_for_publishers(
                zyte_publishers,
                browser_publishers=zyte_browser_publishers,
                max_bytes=max_bytes,
                max_requests=max_requests,
            )
            if zyte_publishers or zyte_browser_publishers
            else fetch_factory
        )
        self.pdf_extractor, self.retain_extraction = pdf_extractor, retain_extraction
        self.no_op = False

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        if self.source_evidence is None:
            raise RuntimeError("Scorecard refresh requires initialized source evidence")
        options = {"fetch_factory": self.fetch_factory} if self.fetch_factory is not None else {}
        return build_scorecards(
            output_dir,
            evidence=self.source_evidence,
            registry=self.registry,
            publishers=self.publishers,
            editions=self.editions,
            historical_backfill=self.historical_backfill,
            force=self.force,
            provider=self.provider,
            pdf_extractor=self.pdf_extractor,
            retain_extraction=self.retain_extraction,
            receipt_generation_id=self.receipt_generation_id,
            max_bytes=self.max_bytes,
            max_requests=self.max_requests,
            **options,
        )

    def _run_tables(self, output_dir: Path) -> None:
        try:
            super()._run_tables(output_dir)
        except NoScorecardsDue:
            self.no_op = True
            if self.source_evidence is None:
                raise RuntimeError("Scorecard no-op requires initialized source evidence")
            self.source_evidence.seal(outcome="no-op")
            logger.info("No scorecard sources due; prior generation unchanged")


app = App(name="run-rollup-scorecards", help=ScorecardsRollup.__doc__)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Retain a local candidate only")] = True,
    registry: Annotated[Path, Parameter(help="Qualified publisher registry")] = REGISTRY,
    publishers: Annotated[list[str] | None, Parameter(name="--publisher", help="Select an enabled publisher")] = None,
    editions: Annotated[
        list[str] | None, Parameter(name="--edition", help="Select literal edition or scorecard IDs")
    ] = None,
    historical_backfill: bool = False,
    force: Annotated[bool, Parameter(help="Ignore cadence; qualification still required")] = False,
    zyte_publishers: Annotated[
        list[str] | None,
        Parameter(
            name="--zyte-publisher", help="Use bounded Zyte publisher bytes for this publisher; requires ZYTE_TOKEN"
        ),
    ] = None,
    zyte_browser_publishers: Annotated[
        list[str] | None,
        Parameter(
            name="--zyte-browser-publisher", help="Use explicit browser API rendition for IJM; requires ZYTE_TOKEN"
        ),
    ] = None,
    max_response_bytes: Annotated[int, Parameter(help="Maximum bytes per publisher response")] = MAX_BYTES,
    max_requests: Annotated[int, Parameter(help="Maximum requests per publisher in this run")] = MAX_REQUESTS,
) -> None:
    load_dotenv()
    ScorecardsRollup(
        output_dir=output_dir,
        skip_upload=skip_upload,
        registry=registry,
        publishers=publishers or (),
        editions=editions or (),
        historical_backfill=historical_backfill,
        force=force,
        zyte_publishers=zyte_publishers or (),
        zyte_browser_publishers=zyte_browser_publishers or (),
        max_bytes=max_response_bytes,
        max_requests=max_requests,
    ).run()


if __name__ == "__main__":
    app()
