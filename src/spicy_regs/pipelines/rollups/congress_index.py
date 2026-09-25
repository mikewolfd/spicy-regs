"""Rollup pipelines: the five Congress.gov index tables, one rollup each.

Five rollups over one transform (``transforms/build_congress_index.py``)
rather than one rollup with five outputs: the tables share a reader class and
nothing else -- no acquisition pass, no budget, no parsed body -- so a
multi-output rollup would only couple five publishers' routes into one
failure, and the reason rollups are separate at all (``base.py``) is that a
failure stays isolated to one artifact. They live in one module because each
class is four lines and its console entry names it by attribute.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_congress_index import (
    build_committee_meetings,
    build_house_communications,
    build_nominations,
    build_record_issues,
    build_treaties,
)


class HouseCommunicationsRollup(RollupPipeline):
    """House executive communications and their regulatory bridge (RIN, referral, requirement), from Congress.gov."""

    name: ClassVar[str] = "house-communications"
    output: ClassVar[str] = "house_communications.parquet"

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        return build_house_communications(output_dir, evidence=self.source_evidence)


class CommitteeMeetingsRollup(RollupPipeline):
    """Committee meetings with their hearing jackets, bills and documents, from Congress.gov."""

    name: ClassVar[str] = "committee-meetings"
    output: ClassVar[str] = "committee_meetings.parquet"

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        return build_committee_meetings(output_dir, evidence=self.source_evidence)


class RecordIssuesRollup(RollupPipeline):
    """Daily Congressional Record issues, the legislative-day calendar, from Congress.gov."""

    name: ClassVar[str] = "record-issues"
    output: ClassVar[str] = "record_issues.parquet"

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        return build_record_issues(output_dir, evidence=self.source_evidence)


class TreatiesRollup(RollupPipeline):
    """Treaty documents and their GovInfo CDOC package, from Congress.gov."""

    name: ClassVar[str] = "treaties"
    output: ClassVar[str] = "treaties.parquet"

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        return build_treaties(output_dir, evidence=self.source_evidence)


class NominationsRollup(RollupPipeline):
    """Nominations and their latest action, from the Congress.gov nomination list."""

    name: ClassVar[str] = "nominations"
    output: ClassVar[str] = "nominations.parquet"

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        return build_nominations(output_dir, evidence=self.source_evidence)


house_communications_app = make_rollup_app(HouseCommunicationsRollup)
committee_meetings_app = make_rollup_app(CommitteeMeetingsRollup)
record_issues_app = make_rollup_app(RecordIssuesRollup)
treaties_app = make_rollup_app(TreatiesRollup)
nominations_app = make_rollup_app(NominationsRollup)
