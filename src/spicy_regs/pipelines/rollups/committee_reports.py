"""Rollup pipeline: committee_reports, committee_report_cbo_letters, report_sections and hearing_transcripts.

Every output from one pass: the two GovInfo collections share an acquirer and
its pacing budget, and the agency blocks are parsed from a report body that is
only in hand during the pass that fetched it.
"""

import os
from pathlib import Path
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv

from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.pipelines.rollups.base import file_scope_digest
from spicy_regs.transforms.build_committee_reports import build_committee_reports


class CommitteeReportsRollup(LegislativeReceiptRollup):
    """GovInfo committee reports, their agency blocks, and hearing transcripts (api.data.gov key)."""

    name: ClassVar[str] = "committee-reports"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    receipt_policies = family_policies('committee_reports', 'report_sections', 'hearing_transcripts', 'hearing_bill_links', 'committee_report_cbo_letters', 'committee_report_reads')
    outputs: ClassVar[tuple[str, ...]] = (
        "committee_reports.parquet",
        "report_sections.parquet",
        "hearing_transcripts.parquet",
        "hearing_bill_links.parquet",
        "committee_report_cbo_letters.parquet",
        "committee_report_reads.parquet",
    )

    receipt_only_tables = ("committee_report_reads.parquet",)
    # The first generation with this table is a migration; publication refuses any other change of membership.
    added_tables: ClassVar[tuple[str, ...]] = ("committee_report_cbo_letters.parquet",)

    #: Optional audited offline retained-hearing selection pins (``selected-capture-pins.json`` or bundle dir).
    retained_bundle: Path | None = None
    #: Optional override directory holding ``evidence-events.jsonl`` and ``originals/<sha256>``.
    retained_bundle_root: Path | None = None
    #: Optional fallback ``CentralRawArchiveStore`` root when blobs are not stored unpacked under ``originals/``.
    retained_raw_archive: Path | None = None

    def _env_path(self, explicit: Path | None, env_var: str) -> Path | None:
        if explicit is not None:
            return explicit
        raw = os.environ.get(env_var, "").strip()
        return Path(raw) if raw else None

    def invocation_scope(self) -> dict:
        scope = super().invocation_scope()
        retained_bundle = self._env_path(self.retained_bundle, "COMMITTEE_REPORTS_RETAINED_BUNDLE")
        retained_bundle_root = self._env_path(self.retained_bundle_root, "COMMITTEE_REPORTS_RETAINED_BUNDLE_ROOT")
        retained_raw_archive = self._env_path(self.retained_raw_archive, "COMMITTEE_REPORTS_RETAINED_RAW_ARCHIVE")
        scope["committee_reports"] = {
            "retained_bundle": file_scope_digest(retained_bundle),
            "retained_bundle_root": str(retained_bundle_root) if retained_bundle_root is not None else None,
            "retained_raw_archive": str(retained_raw_archive) if retained_raw_archive is not None else None,
        }
        return scope

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        # Comma-separated package ids to re-read alone; unset is the normal discovery run.
        reread = [key.strip() for key in os.environ.get("COMMITTEE_REPORTS_REREAD", "").split(",") if key.strip()]
        retained_bundle = self._env_path(self.retained_bundle, "COMMITTEE_REPORTS_RETAINED_BUNDLE")
        retained_bundle_root = self._env_path(self.retained_bundle_root, "COMMITTEE_REPORTS_RETAINED_BUNDLE_ROOT")
        retained_raw_archive = self._env_path(self.retained_raw_archive, "COMMITTEE_REPORTS_RETAINED_RAW_ARCHIVE")
        # Work left pending (cap-deferred, or refused and retried next run) is a warning the build names. A
        # collection that completed none of the packages it attempted fails the run after publishing.
        self.acquisition_status = {}
        extra: dict[str, Path] = {}
        if retained_bundle is not None:
            extra["retained_bundle"] = retained_bundle
        if retained_bundle_root is not None:
            extra["retained_bundle_root"] = retained_bundle_root
        if retained_raw_archive is not None:
            extra["retained_raw_archive"] = retained_raw_archive
        result = self.build_receipts(output_dir, build_committee_reports, evidence=self.source_evidence,
                                     reread=reread, coverage=self.acquisition_status, **extra)
        if failed := self.acquisition_status.get("nothing_read"):
            self.deferred_failure = RuntimeError(
                f"Committee reports read none of the packages attempted in {', '.join(failed)}; see acquisition-coverage")
        return result


# The shared two flags (make_rollup_app's) plus optional offline retained-bundle merge flags.
app = App(
    name=f"run-rollup-{CommitteeReportsRollup.name}",
    help=(CommitteeReportsRollup.__doc__ or "").strip().splitlines()[0],
)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    resume: Annotated[
        bool, Parameter(help="Resume a verified retained generation candidate before rebuilding")
    ] = False,
    retained_bundle: Annotated[
        Path | None,
        Parameter(help="Path to audited selected-capture-pins.json (or bundle dir) for offline hearing merge"),
    ] = None,
    retained_bundle_root: Annotated[
        Path | None,
        Parameter(help="Optional directory holding evidence-events.jsonl and originals/<sha256>"),
    ] = None,
    retained_raw_archive: Annotated[
        Path | None,
        Parameter(help="Optional CentralRawArchiveStore root fallback for retained original blobs"),
    ] = None,
) -> None:
    load_dotenv()
    rollup = CommitteeReportsRollup(output_dir=output_dir, skip_upload=skip_upload, resume=resume)
    rollup.retained_bundle = retained_bundle
    rollup.retained_bundle_root = retained_bundle_root
    rollup.retained_raw_archive = retained_raw_archive
    rollup.run()


if __name__ == "__main__":
    app()
