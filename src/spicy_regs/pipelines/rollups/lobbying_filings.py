"""Rollup pipeline: lobbying_filings.parquet (Senate LDA REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_lobbying_filings``.
``LDA_FILING_YEAR`` reads one whole filing year instead of the posted-date
window, the unit of the history backfill.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import date_env, int_env
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_lobbying_filings


class LobbyingFilingsRollup(RollupPipeline):
    """Senate Lobbying Disclosure Act filings ingested from lda.gov (key optional)."""

    name: ClassVar[str] = "lobbying-filings"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = (
        "lobbying_filings.parquet", "lobbying_activities.parquet", "lobbying_activity_lobbyists.parquet",
    )
    # Decision 47: the activities and their lobbyists join the family as their own tables.
    added_tables: ClassVar[tuple[str, ...]] = ("lobbying_activities.parquet", "lobbying_activity_lobbyists.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_lobbying_filings(
            output_dir,
            evidence=self.source_evidence,
            since=date_env("LDA_SINCE"),
            until=date_env("LDA_UNTIL"),
            filing_year=int_env("LDA_FILING_YEAR"),
        )


app = make_rollup_app(LobbyingFilingsRollup)

if __name__ == "__main__":
    app()
