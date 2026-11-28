"""Parity and lifecycle tests for Wave 4A Slice 3A (ss-vse3): non-bill legislative receipt retirement.

Covers all 4 families (17 published tables + 1 receipt-only dataset = 18 policies):
- ``laws`` (5 policies)
- ``senate-expenditures`` (2 policies)
- ``native-legal-references`` (3 policies)
- ``committee-reports`` (7 published tables + ``committee_report_reads`` receipt-only state = 8 policies)
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from spicy_regs import legislative_tables, subject_catalog
from spicy_regs.generations import verify_generation
from spicy_regs.legislative_documents import field_registry, subject_schema
from spicy_regs.legislative_rollups import LegislativeReceiptRollup, LegislativeTableRollup
from spicy_regs.navigation_read_outcomes import FILE_POLICIES, POLICIES as OUTCOME_POLICIES
from spicy_regs.pipelines.rollups import committee_reports, laws, native_legal_references, senate_expenditures
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
from spicy_regs.receipt_switch import receipt_files, receipt_free_prior, table_differences
from spicy_regs.sources.publication import PublicationError, empty_index
from tests.test_receipts_retired import pin_archive

RETIRED_SLICE_3A = frozenset({
    "laws",
    "law_code_sections",
    "table3_records",
    "law_sections",
    "laws_document_file_outcomes",
    "senate_expenditures",
    "senate_expenditures_document_file_outcomes",
    "native_legal_references",
    "native_legal_reference_reads",
    "native_legal_references_document_file_outcomes",
    "committee_reports",
    "report_sections",
    "hearing_transcripts",
    "hearing_bill_links",
    "committee_report_cbo_letters",
    "committee_report_read_outcomes",
    "committee_reports_document_file_outcomes",
    "committee_report_reads",
})


def _identities(dataset: str) -> tuple[str, ...]:
    if dataset in OUTCOME_POLICIES:
        return tuple(OUTCOME_POLICIES[dataset].identity_fields)
    if dataset in FILE_POLICIES:
        return tuple(FILE_POLICIES[dataset].identity_fields)
    return tuple(field_registry()[dataset]["identity_fields"])


def _strip_kv_metadata(path: Path) -> None:
    """Strip non-Arrow custom Parquet KV metadata to simulate pre-retirement live R2 public tables."""
    table = pq.read_table(path)
    pq.write_table(table.replace_schema_metadata(None), path)


def _strip_columns(path: Path, columns: list[str], dest: Path) -> Path:
    table = pq.read_table(path)
    pq.write_table(table.drop_columns(columns), dest)
    return dest


def test_all_18_slice_3a_policies_are_retired_and_all_4_rollups_are_plain():
    """All 18 policies carry receipts_retired; all 4 rollups are LegislativeTableRollup; unretired subclasses fail."""
    assert RETIRED_SLICE_3A <= subject_catalog.retired_receipt_datasets()
    rollups = (
        laws.LawsRollup,
        senate_expenditures.SenateExpendituresRollup,
        native_legal_references.NativeLegalReferencesRollup,
        committee_reports.CommitteeReportsRollup,
    )
    for cls in rollups:
        assert issubclass(cls, LegislativeTableRollup)
        assert cls.receipt_policies == ()
        assert cls.receipt_only_tables == ()
    assert "committee_report_reads.parquet" not in committee_reports.CommitteeReportsRollup.outputs
    assert "committee_report_reads.parquet" in committee_reports.CommitteeReportsRollup.source_outputs
    assert "committee_report_reads" in committee_reports.CommitteeReportsRollup.retired_receipt_datasets
    assert "committee_report_read_outcomes.parquet" in committee_reports.CommitteeReportsRollup.outputs

    with pytest.raises(ValueError, match="still publish receipts"):

        class _UnretiredLegislative(LegislativeTableRollup):
            name = "bill-family"
            outputs = ("bill_sections.parquet",)


def test_laws_parity_and_night1_night2_checkpoint_carry_without_receipts(tmp_path, monkeypatch):
    """Old receipt route vs plain LawsRollup match bit-for-bit; Night-1 file-outcome & Night-2 KV checkpoints work."""
    from spicy_regs.transforms.build_laws import build_laws
    from tests.test_laws import LISTED_119, StubListingReader, StubOlrc, StubUslm

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    def _make_builder(uslm_calls: list[StubUslm], olrc_calls: list[StubOlrc]):
        def _builder(directory: Path, **kwargs: Any):
            uslm = StubUslm(unavailable={110, 109, 104})
            olrc = StubOlrc()
            uslm_calls.append(uslm)
            olrc_calls.append(olrc)
            return build_laws(directory, reader=StubListingReader({119: LISTED_119}), uslm=uslm, olrc=olrc, **kwargs)

        return _builder

    # 1. Old receipt route (unretired marker lifted locally)
    class _LawsReceiptRoute(LegislativeReceiptRollup):
        name = "laws"
        retain_source_evidence = True
        outputs = ("laws.parquet", "law_code_sections.parquet", "table3_records.parquet", "law_sections.parquet")

        def build(self, output_dir: Path):
            return self.build_receipts(output_dir, _make_builder([], []), evidence=self.source_evidence)

    old_dir = tmp_path / "old"
    retired = subject_catalog.retired_receipt_datasets()
    with monkeypatch.context() as patch:
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired - {
            "laws", "law_code_sections", "table3_records", "law_sections", "laws_document_file_outcomes"
        })
        _LawsReceiptRoute(output_dir=old_dir, skip_upload=True).run()

    # 2. New plain-table route
    uslm_calls: list[StubUslm] = []
    olrc_calls: list[StubOlrc] = []
    monkeypatch.setattr(laws, "build_laws", _make_builder(uslm_calls, olrc_calls))
    new_dir = tmp_path / "new"
    laws.LawsRollup(output_dir=new_dir, skip_upload=True).run()
    (gen_new,) = (new_dir / "generations").iterdir()
    assert receipt_files(new_dir) == []
    assert "etlReceipts" not in verify_generation(gen_new).root["spec"]

    for dataset in ("laws", "law_code_sections", "table3_records", "law_sections"):
        assert table_differences(
            new_dir / f"{dataset}.parquet", old_dir / f"{dataset}.parquet", key=_identities(dataset)
        ) == [], dataset
    old_fo = pq.read_table(old_dir / "laws_document_file_outcomes.parquet")
    new_fo = pq.read_table(new_dir / "laws_document_file_outcomes.parquet")
    assert new_fo.schema.equals(FILE_POLICIES["laws_document_file_outcomes"].subject_schema)
    assert sorted(new_fo["source_dataset"].to_pylist()) == sorted(old_fo["source_dataset"].to_pylist())
    assert sum(new_fo["row_count"].to_pylist()) == sum(old_fo["row_count"].to_pylist())

    # 3. Simulate Night-1 live prior (receipt-free, empty KV metadata on table3_records.parquet):
    #    SelectedPriors recovers laws-table3 checkpoint from laws_document_file_outcomes.parquet.
    night1_prior = receipt_free_prior(new_dir, tmp_path / "night1-prior")
    _strip_kv_metadata(night1_prior / "table3_records.parquet")
    assert not legislative_tables.has_checkpoint_metadata(night1_prior / "table3_records.parquet")
    restored_t3 = SelectedPriors(tmp_path / "probe-t3", root=night1_prior, public_url="").get("table3_records")
    assert legislative_tables.has_checkpoint_metadata(restored_t3)

    # 4. Night-2 incremental run on receipt-free prior skips re-fetching settled USLM law 1 and preserves tables.
    laws.LawsRollup(output_dir=night1_prior, skip_upload=True).run()
    assert receipt_files(night1_prior) == []
    assert 1 in [s.number for s in uslm_calls[0].selections]
    assert 1 not in [s.number for s in uslm_calls[1].selections]
    assert legislative_tables.has_checkpoint_metadata(night1_prior / "table3_records.parquet")
    for dataset in ("laws", "law_code_sections", "table3_records", "law_sections"):
        assert table_differences(
            night1_prior / f"{dataset}.parquet", new_dir / f"{dataset}.parquet", key=_identities(dataset)
        ) == [], dataset


def test_senate_expenditures_parity_and_night1_night2_without_receipts(tmp_path, monkeypatch):
    """Old receipt route vs plain SenateExpendituresRollup match bit-for-bit; Night-1 file-outcome fallback works."""
    from spicy_regs.transforms.build_senate_expenditures import build_senate_expenditures
    from tests.test_senate_expenditures import PACKAGE, PART_ONE, _Acquirer, _Extractor, _granule, _listing, _Reader

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    def _builder(directory: Path, **kwargs: Any):
        return build_senate_expenditures(
            directory,
            reader=_Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}),
            acquirer=_Acquirer(),
            extractor=_Extractor(page_count=3),
            **kwargs,
        )

    class _SenateReceiptRoute(LegislativeReceiptRollup):
        name = "senate-expenditures"
        retain_source_evidence = True
        output = "senate_expenditures.parquet"

        def build(self, output_dir: Path):
            return self.build_receipts(output_dir, _builder, evidence=self.source_evidence)

    old_dir = tmp_path / "old"
    retired = subject_catalog.retired_receipt_datasets()
    with monkeypatch.context() as patch:
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired - {
            "senate_expenditures", "senate_expenditures_document_file_outcomes"
        })
        _SenateReceiptRoute(output_dir=old_dir, skip_upload=True).run()

    monkeypatch.setattr(senate_expenditures, "build_senate_expenditures", _builder)
    new_dir = tmp_path / "new"
    senate_expenditures.SenateExpendituresRollup(output_dir=new_dir, skip_upload=True).run()
    assert receipt_files(new_dir) == []
    assert table_differences(
        new_dir / "senate_expenditures.parquet",
        old_dir / "senate_expenditures.parquet",
        key=_identities("senate_expenditures"),
    ) == []

    # Simulate Night-1 live prior with empty KV metadata on senate_expenditures.parquet.
    night1 = receipt_free_prior(new_dir, tmp_path / "night1")
    _strip_kv_metadata(night1 / "senate_expenditures.parquet")
    assert not legislative_tables.has_checkpoint_metadata(night1 / "senate_expenditures.parquet")
    restored = SelectedPriors(tmp_path / "probe", root=night1, public_url="").get("senate_expenditures")
    assert legislative_tables.has_checkpoint_metadata(restored)

    senate_expenditures.SenateExpendituresRollup(output_dir=night1, skip_upload=True).run()
    assert receipt_files(night1) == []
    assert legislative_tables.has_checkpoint_metadata(night1 / "senate_expenditures.parquet")
    assert table_differences(
        night1 / "senate_expenditures.parquet",
        new_dir / "senate_expenditures.parquet",
        key=_identities("senate_expenditures"),
    ) == []


def test_native_legal_references_parity_and_roundtrip_without_receipts(tmp_path, monkeypatch):
    """Old receipt route vs plain NativeLegalReferencesRollup match bit-for-bit across runs."""
    from spicy_regs.transforms.native_legal_references import build_native_legal_references
    from tests.test_native_legal_references import FIXTURES

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    manifest = FIXTURES / "manifest.json"

    class _NativeReceiptRoute(LegislativeReceiptRollup):
        name = "native-legal-references"
        retain_source_evidence = True
        outputs = ("native_legal_references.parquet", "native_legal_reference_reads.parquet")

        def build(self, output_dir: Path):
            ev = self.source_evidence
            assert ev is not None
            return self.build_receipts(
                output_dir,
                lambda d, *, download_prior: build_native_legal_references(
                    manifest, d, evidence=ev, download_prior=download_prior
                ),
            )

    old_dir = tmp_path / "old"
    retired = subject_catalog.retired_receipt_datasets()
    with monkeypatch.context() as patch:
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired - {
            "native_legal_references",
            "native_legal_reference_reads",
            "native_legal_references_document_file_outcomes",
        })
        _NativeReceiptRoute(output_dir=old_dir, skip_upload=True).run()

    new_dir = tmp_path / "new"
    native_legal_references.NativeLegalReferencesRollup(manifest=manifest, output_dir=new_dir, skip_upload=True).run()
    assert receipt_files(new_dir) == []
    for dataset in ("native_legal_references", "native_legal_reference_reads"):
        assert table_differences(
            new_dir / f"{dataset}.parquet", old_dir / f"{dataset}.parquet", key=_identities(dataset)
        ) == [], dataset

    # Run 2 against receipt-free prior restores both tables cleanly through legislative_tables.as_builder_input.
    run2 = receipt_free_prior(new_dir, tmp_path / "run2")
    native_legal_references.NativeLegalReferencesRollup(manifest=manifest, output_dir=run2, skip_upload=True).run()
    assert receipt_files(run2) == []
    for dataset in ("native_legal_references", "native_legal_reference_reads"):
        assert table_differences(
            run2 / f"{dataset}.parquet", new_dir / f"{dataset}.parquet", key=_identities(dataset)
        ) == [], dataset


def test_committee_reports_parity_read_checkpoints_and_hearing_body_reuse_without_receipts(tmp_path, monkeypatch):
    """Parity across all 7 tables, committee_report_reads carry via read_outcomes, and PR #330 detail_refused retry."""
    from spicy_docs.reading.paged_json import PagedJsonSourceError
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_regs.transforms.build_committee_reports import build_committee_reports
    from spicy_regs.transforms.committee_report_reads import DETAIL_REFUSED
    from tests.test_committee_reports import CHRG_ID, StubBodyAcquirer, StubDiscovery, StubHearings

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    monkeypatch.delenv("COMMITTEE_REPORTS_REREAD", raising=False)

    class _EvidencedDiscovery(StubDiscovery):
        def packages(self, url: str, *, max_pages: int = 40):
            from types import SimpleNamespace
            import json

            for page in super().packages(url, max_pages=max_pages):
                cap = CapturedBodyResponse(
                    requested_url=url,
                    resolved_url=url,
                    status_code=200,
                    observed_at="2026-11-28T00:00:00Z",
                    content_type="application/json",
                    body=json.dumps(page.records).encode(),
                )
                yield SimpleNamespace(records=page.records, capture=cap)

    def _make_builder(acquirers: list[StubBodyAcquirer], hearings_factory=StubHearings):
        def _builder(directory: Path, **kwargs: Any):
            acquirer = StubBodyAcquirer()
            acquirers.append(acquirer)
            return build_committee_reports(
                directory, reader=_EvidencedDiscovery(), acquirer=acquirer, hearings=hearings_factory(), **kwargs
            )

        return _builder

    # 1. Old receipt route vs new plain-table route
    class _CrptReceiptRoute(LegislativeReceiptRollup):
        name = "committee-reports"
        retain_source_evidence = True
        outputs = (
            "committee_reports.parquet",
            "report_sections.parquet",
            "hearing_transcripts.parquet",
            "hearing_bill_links.parquet",
            "committee_report_cbo_letters.parquet",
            "committee_report_reads.parquet",
        )
        added_tables = ("committee_report_cbo_letters.parquet",)

        def build(self, output_dir: Path):
            return self.build_receipts(output_dir, _make_builder([]), evidence=self.source_evidence)

    old_dir = tmp_path / "old"
    retired = subject_catalog.retired_receipt_datasets()
    crpt_datasets = {
        "committee_reports",
        "report_sections",
        "hearing_transcripts",
        "hearing_bill_links",
        "committee_report_cbo_letters",
        "committee_report_read_outcomes",
        "committee_reports_document_file_outcomes",
        "committee_report_reads",
    }
    with monkeypatch.context() as patch:
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired - crpt_datasets)
        _CrptReceiptRoute(output_dir=old_dir, skip_upload=True).run()

    acquirers: list[StubBodyAcquirer] = []
    monkeypatch.setattr(committee_reports, "build_committee_reports", _make_builder(acquirers))
    new_dir = tmp_path / "new"
    committee_reports.CommitteeReportsRollup(output_dir=new_dir, skip_upload=True).run()
    assert receipt_files(new_dir) == []
    assert not (new_dir / "committee_report_reads.parquet").exists()

    for dataset in (
        "committee_reports",
        "report_sections",
        "hearing_transcripts",
        "hearing_bill_links",
        "committee_report_cbo_letters",
    ):
        assert table_differences(
            new_dir / f"{dataset}.parquet", old_dir / f"{dataset}.parquet", key=_identities(dataset)
        ) == [], dataset

    # Compare committee_report_read_outcomes modulo per-run generation_id and wall-clock observed_at.
    assert all(pq.read_table(new_dir / "committee_report_read_outcomes.parquet")["observed_at"].to_pylist())
    assert table_differences(
        _strip_columns(
            new_dir / "committee_report_read_outcomes.parquet", ["generation_id", "observed_at"], tmp_path / "ro_new.parquet"
        ),
        _strip_columns(
            old_dir / "committee_report_read_outcomes.parquet", ["generation_id", "observed_at"], tmp_path / "ro_old.parquet"
        ),
        key=("package_id",),
    ) == []

    # 2. Run 2 on receipt-free prior restores committee_report_reads from committee_report_read_outcomes.parquet
    #    and asks GovInfo for zero already-settled packages!
    run2 = receipt_free_prior(new_dir, tmp_path / "run2")
    committee_reports.CommitteeReportsRollup(output_dir=run2, skip_upload=True).run()
    assert receipt_files(run2) == []
    assert acquirers[0].requested != []
    assert acquirers[1].requested == []
    for dataset in (
        "committee_reports",
        "report_sections",
        "hearing_transcripts",
        "hearing_bill_links",
        "committee_report_cbo_letters",
    ):
        assert table_differences(
            run2 / f"{dataset}.parquet", new_dir / f"{dataset}.parquet", key=_identities(dataset)
        ) == [], dataset

    # 3. PR #330 detail_refused retry + hearing-body-reuse KV checkpoint without receipts:
    #    Run A hits HTTP 500 on hearing-detail -> publishes transcript + detail_refused outcome.
    #    Run B retries hearing-detail and reuses stored transcript body without calling acquirer again!
    class _Detail500(StubHearings):
        def records(self, route, url: str, *, max_pages: int = 1):
            raise PagedJsonSourceError("stub: HTTP 500")
            yield  # pragma: no cover

    retry_dir = tmp_path / "retry"
    retry_acquirers: list[StubBodyAcquirer] = []
    monkeypatch.setattr(committee_reports, "build_committee_reports", _make_builder(retry_acquirers, _Detail500))
    committee_reports.CommitteeReportsRollup(output_dir=retry_dir, skip_upload=True).run()
    outcomes_a = {
        r["package_id"]: r["outcome"]
        for r in pq.read_table(retry_dir / "committee_report_read_outcomes.parquet").to_pylist()
    }
    assert outcomes_a[CHRG_ID] == DETAIL_REFUSED
    assert CHRG_ID in retry_acquirers[0].requested

    # Run B succeeds on hearing-detail: CRPT stays settled, CHRG retries detail and reuses stored body text
    # via the hearing-body-reuse Parquet KV checkpoint (proving body-text-reused without receipts).
    monkeypatch.setattr(committee_reports, "build_committee_reports", _make_builder(retry_acquirers, StubHearings))
    committee_reports.CommitteeReportsRollup(output_dir=retry_dir, skip_upload=True).run()
    assert receipt_files(retry_dir) == []
    assert retry_acquirers[1].requested == [CHRG_ID]
    import json

    reused_events = [
        json.loads(line)
        for journal in sorted((retry_dir / "source-evidence").rglob("journal.jsonl"))
        for line in journal.read_text().splitlines()
        if '"body-text-reused"' in line
    ]
    assert any(ev.get("package_id") == CHRG_ID for ev in reused_events), (
        "hearing-body-reuse KV checkpoint must fire body-text-reused on detail_refused retry without receipts"
    )
    outcomes_b = {
        r["package_id"]: r["outcome"]
        for r in pq.read_table(retry_dir / "committee_report_read_outcomes.parquet").to_pylist()
    }
    assert outcomes_b[CHRG_ID] == "complete"


def test_bill_family_still_on_receipts_reads_retired_laws_table_cleanly(tmp_path, monkeypatch):
    """BillFamilyRollup (SubjectReceiptRollup) reads retired laws.parquet as soft input without None crash."""
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from spicy_regs.transforms.build_laws import build_laws
    from tests.test_bill_family import StubBodyAcquirer, StubBulkAcquirer
    from tests.test_laws import LISTED_119, StubListingReader, StubOlrc, StubUslm

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "R2_PUBLIC_URL"):
        monkeypatch.delenv(key, raising=False)

    # 1. Produce retired, receipt-free laws.parquet via LawsRollup.
    laws_dir = tmp_path / "shared-out"
    monkeypatch.setattr(
        laws,
        "build_laws",
        lambda d, **kw: build_laws(
            d, reader=StubListingReader({119: LISTED_119}), uslm=StubUslm(unavailable={110, 109, 104}), olrc=StubOlrc(), **kw
        ),
    )
    laws.LawsRollup(output_dir=laws_dir, skip_upload=True).run()
    assert receipt_files(laws_dir) == []

    # 2. Verify SelectedPriors restores retired laws.parquet without receipts.
    prior = SelectedPriors(tmp_path / "probe", root=laws_dir, public_url="")
    restored_laws = prior.get("laws")
    assert prior.selections["laws"][1:] == (None, None)
    assert pq.read_table(restored_laws).num_rows == pq.read_table(laws_dir / "laws.parquet").num_rows

    # 3. Run BillFamilyRollup (still a SubjectReceiptRollup) in a directory holding the retired laws.parquet.
    bill_dir = tmp_path / "bills"
    bill_dir.mkdir()
    shutil.copyfile(laws_dir / "laws.parquet", bill_dir / "laws.parquet")
    outputs = BillFamilyRollup().build_receipts(
        bill_dir, build_bill_family, bulk_acquirer=StubBulkAcquirer(), body_acquirer=StubBodyAcquirer()
    )
    assert (bill_dir / "congress_bills.parquet") in outputs
    assert (bill_dir / "etl_receipts.parquet").is_file()


@pytest.mark.parametrize(
    ("family", "rollup_cls", "stale_dataset", "init_kwargs"),
    [
        ("laws", laws.LawsRollup, "laws", {}),
        ("committee-reports", committee_reports.CommitteeReportsRollup, "committee_report_reads", {}),
    ],
    ids=["table-pin", "receipt-only-pin"],
)
def test_legislative_table_rollup_refuses_stale_archive_pin_before_build(
    tmp_path, monkeypatch, family, rollup_cls, stale_dataset, init_kwargs
):
    """_prime enforces docs/pins/receipt-archive.json against live snapshot for both tables and receipt_only datasets."""
    live_digest = "sha256:" + "11" * 32
    stale_digest = "sha256:" + "22" * 32
    datasets = list(rollup_cls.retired_receipt_datasets)
    index = empty_index()
    index["families"][family] = {
        "prefix": f"generations/{family}/" + "11" * 32,
        "artifactDigest": live_digest,
        "tables": {key: {"sha256": "sha256:" + hashlib.sha256(key.encode()).hexdigest(), "byteSize": 1, "rows": 0}
                   for key in rollup_cls.outputs},
        "etlReceipts": {
            "key": "etl_receipts.parquet",
            "sha256": "sha256:" + "33" * 32,
            "byteSize": 1,
            "rows": 0,
            "generationId": "g-live",
            "datasets": datasets,
        },
    }

    def _pin(digest: str) -> dict[str, str]:
        return {
            "family": family,
            "artifactDigest": digest,
            "publishedAt": "2026-11-28T00:00:00Z",
            "receipts": f"generations/{family}/{digest.removeprefix('sha256:')}/etl_receipts.parquet",
        }

    # Stale pin on one dataset (including receipt_only committee_report_reads) must refuse before build().
    archives_stale = {name: _pin(stale_digest if name == stale_dataset else live_digest) for name in datasets}
    pin_archive(monkeypatch, tmp_path, archives_stale)
    pipeline = rollup_cls(output_dir=tmp_path / "stale", skip_upload=True, **init_kwargs)
    with pytest.raises(PublicationError, match=f"{stale_dataset} leaves its receipts in {family} generation {live_digest}"):
        pipeline._prime(tmp_path / "stale", index)

    # Matching live pins allow _prime to proceed cleanly.
    archives_live = {name: _pin(live_digest) for name in datasets}
    pin_archive(monkeypatch, tmp_path, archives_live)
    ok_pipeline = rollup_cls(output_dir=tmp_path / "live", skip_upload=True, **init_kwargs)
    ok_pipeline._prime(tmp_path / "live", index)
    assert ok_pipeline.outputs == rollup_cls.outputs
    assert set(ok_pipeline.generation_schemas()) == {k.removesuffix(".parquet") for k in rollup_cls.outputs}
    # Confirm subject_schema still matches every non-outcome dataset in outputs.
    for key in ok_pipeline.outputs:
        ds = key.removesuffix(".parquet")
        if ds not in OUTCOME_POLICIES and ds not in FILE_POLICIES:
            assert subject_schema(ds).names == [col for col, _ in ok_pipeline.generation_schemas()[ds]]
