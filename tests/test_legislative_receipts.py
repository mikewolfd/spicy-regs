"""Receipts bind actual legislative producer writes and incremental reads."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext, read_with_receipts, select_receipts
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import (
    admit_bundle,
    build_with_receipts,
    write_legislative_outputs,
    policy,
    restore_prior,
    split_source_row,
)


def row(dataset, **values):
    return {f["name"]: None for f in field_registry()[dataset]["fields"]} | values


def retained(directory, dataset, rows, metadata=None):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{dataset}.parquet"
    schema = pa.schema([(f["name"], pa.string()) for f in field_registry()[dataset]["fields"]], metadata=metadata)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def test_receipt_split_keeps_invalid_input_without_subject():
    context = ReceiptContext("g", "a", "test", [{"source_id": "x", "sha256": "a" * 64}])
    source = row("budget_volumes", package_id="x", associated_bills_json="not-json")
    subject, receipt = split_source_row("budget_volumes", source, context)
    assert subject is None and receipt["outcome"] == "refused"
    assert "not-json" in receipt["processing_json"]


def test_roundtrip_preserves_checkpoint_only_rows_and_exact_parquet_metadata(tmp_path):
    source = row(
        "laws",
        congress="119",
        law_type="public",
        number="1",
        law_id="119-public-1",
        uslm_outcome="request_failed",
        uslm_reason="timeout",
        uslm_citable_as_json="[]",
    )
    law = retained(tmp_path / "source", "laws", [source], {b"checkpoints": b'{"rule":"r1","complete":false}'})
    checkpoint = row("committee_report_reads", package_id="CRPT-119hrpt1", outcome="complete", rule_version="rule-1")
    read = retained(tmp_path / "source", "committee_report_reads", [checkpoint])
    manifest = write_legislative_outputs([law, read], tmp_path / "bundle", generation_id="g1")
    assert not (tmp_path / "bundle/committee_report_reads.parquet").exists()
    assert manifest["subjects"]["committee_report_reads"] == []
    assert "uslm_outcome" not in pq.read_schema(tmp_path / "bundle/laws.parquet").names
    restored = restore_prior(tmp_path / "bundle", tmp_path / "prior")
    assert pq.read_table(restored["laws"]).to_pylist() == [source]
    assert pq.read_schema(restored["laws"]).metadata == pq.read_schema(law).metadata
    assert pq.read_table(restored["committee_report_reads"]).to_pylist() == [checkpoint]


def test_wrong_generation_and_missing_receipt_refuse(tmp_path):
    source = retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")])
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g1")
    receipts = select_receipts(
        tmp_path / "bundle/etl_receipts.parquet", tmp_path / "selected.parquet", dataset="table3_records"
    )
    with pytest.raises(ValueError, match="generation"):
        list(
            read_with_receipts(
                [tmp_path / "bundle/table3_records.parquet"], [receipts], policy("table3_records"), generation_id="g2"
            )
        )
    pq.write_table(pq.read_table(receipts).slice(0, 0), receipts)
    with pytest.raises(ValueError, match="Missing"):
        list(
            read_with_receipts(
                [tmp_path / "bundle/table3_records.parquet"], [receipts], policy("table3_records"), generation_id="g1"
            )
        )


def test_invalid_input_bundle_cannot_become_a_qualified_prior(tmp_path):
    source = retained(
        tmp_path / "source", "budget_volumes", [row("budget_volumes", package_id="x", associated_bills_json="{}")]
    )
    manifest = write_legislative_outputs([source], tmp_path / "bundle", generation_id="g1")
    assert manifest["refused_rows"] == {"budget_volumes": 1}
    assert pq.read_table(tmp_path / "bundle/budget_volumes.parquet").num_rows == 0
    with pytest.raises(ValueError, match="conversion refusals"):
        restore_prior(tmp_path / "bundle", tmp_path / "prior")


def test_changed_subject_refuses_before_incremental_reader(tmp_path):
    source = retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")])
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g1")
    subject = tmp_path / "bundle/table3_records.parquet"
    table = pq.read_table(subject)
    changed = table.to_pylist()[0] | {"status": "changed"}
    pq.write_table(pa.Table.from_pylist([changed], schema=table.schema), subject)
    with pytest.raises(ValueError, match="receipt"):
        restore_prior(tmp_path / "bundle", tmp_path / "prior")


def test_actual_laws_builder_reuses_only_verified_receipt_prior(tmp_path, monkeypatch):
    # Existing retained publisher fixtures exercise the actual source-owner
    # shapers, Table III safeguards, merge writer, and USLM retry decision.
    from .test_laws import StubListingReader, StubUslm, StubOlrc, LISTED_119
    from spicy_regs.transforms.build_laws import build_laws

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    first_uslm = StubUslm(unavailable={110, 109, 104})
    first = build_with_receipts(
        build_laws,
        tmp_path / "run1",
        tmp_path / "bundle1",
        generation_id="g1",
        builder_kwargs={"reader": StubListingReader({119: LISTED_119}), "uslm": first_uslm, "olrc": StubOlrc()},
    )
    second_uslm = StubUslm(unavailable={110, 109, 104})
    second = build_with_receipts(
        build_laws,
        tmp_path / "run2",
        tmp_path / "bundle2",
        generation_id="g2",
        prior_bundle=tmp_path / "bundle1",
        builder_kwargs={"reader": StubListingReader({119: LISTED_119}), "uslm": second_uslm, "olrc": StubOlrc()},
    )
    assert (
        set(first["datasets"])
        == set(second["datasets"])
        == {"laws", "law_code_sections", "table3_records", "law_sections"}
    )
    assert all(n == 0 for n in second["refused_rows"].values())
    assert 1 in [s.number for s in first_uslm.selections]
    assert 1 not in [s.number for s in second_uslm.selections]


def test_actual_committee_producer_keeps_empty_read_checkpoints(tmp_path, monkeypatch):
    from .test_committee_reports import StubDiscovery, StubBodyAcquirer, StubHearings
    from spicy_regs.transforms.build_committee_reports import build_committee_reports

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    kwargs = {"reader": StubDiscovery(), "acquirer": StubBodyAcquirer(), "hearings": StubHearings()}
    first = build_with_receipts(
        build_committee_reports, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    second = build_with_receipts(
        build_committee_reports,
        tmp_path / "run2",
        tmp_path / "bundle2",
        generation_id="g2",
        prior_bundle=tmp_path / "bundle1",
        builder_kwargs=kwargs,
    )
    assert set(first["datasets"]) == set(second["datasets"])
    assert "committee_report_reads" in second["datasets"]
    assert not (tmp_path / "bundle2/committee_report_reads.parquet").exists()
    assert not any(second["refused_rows"].values())
    admit_bundle(tmp_path / "bundle2", tmp_path / "generation")


def test_actual_native_reference_reader_emits_one_receipt_only_scope(tmp_path):
    from .test_native_legal_references import FIXTURES
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.native_legal_references import build_native_legal_references

    def builder(directory, *, download_prior):
        return build_native_legal_references(
            FIXTURES / "manifest.json",
            directory,
            evidence=CaptureEvidence(directory, "native-legal-references"),
            download_prior=download_prior,
        )

    manifest = build_with_receipts(builder, tmp_path / "source", tmp_path / "bundle", generation_id="g")
    assert not any(manifest["refused_rows"].values())
    assert pq.read_metadata(tmp_path / "bundle/native_legal_references.parquet").num_rows == 33
    assert manifest["subjects"]["native_legal_reference_reads"] == []
    restored = restore_prior(tmp_path / "bundle", tmp_path / "prior")
    assert pq.read_metadata(restored["native_legal_reference_reads"]).num_rows == 2


def test_actual_senate_producer_preserves_incremental_read_limits(tmp_path):
    from .test_senate_expenditures import _Reader, _listing, _granule, PACKAGE, PART_ONE, _Acquirer, _Extractor
    from spicy_regs.transforms.build_senate_expenditures import build_senate_expenditures

    kwargs = {
        "reader": _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}),
        "acquirer": _Acquirer(),
        "extractor": _Extractor(page_count=3),
    }
    first = build_with_receipts(
        build_senate_expenditures, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    second = build_with_receipts(
        build_senate_expenditures,
        tmp_path / "run2",
        tmp_path / "bundle2",
        generation_id="g2",
        prior_bundle=tmp_path / "bundle1",
        builder_kwargs=kwargs,
    )
    assert not any(first["refused_rows"].values()) and not any(second["refused_rows"].values())
    assert pq.read_table(tmp_path / "bundle1/senate_expenditures.parquet").equals(
        pq.read_table(tmp_path / "bundle2/senate_expenditures.parquet")
    )


def test_actual_print_producer_and_native_citations(tmp_path):
    from .test_print_citations import _Reader, _listing, _Acquirer, _package, _NoRosters, CRPT_ID, REPORT_PAGES
    from spicy_regs.transforms.build_print_citations import build_print_citations

    kwargs = {
        "reader": _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")]}),
        "acquirer": _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)}),
        "rosters": _NoRosters(),
    }
    manifest = build_with_receipts(
        build_print_citations, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    assert not any(manifest["refused_rows"].values())
    assert pq.read_metadata(tmp_path / "bundle1/document_citations.parquet").num_rows > 0
    assert pq.read_metadata(tmp_path / "bundle1/bill_committee_actions.parquet").num_rows > 0


def test_actual_bill_producer_preserves_partitioned_sections_and_names_other_owners(tmp_path, monkeypatch):
    from .test_bill_family import StubBulkAcquirer, StubBodyAcquirer
    from spicy_regs.transforms.build_bill_family import build_bill_family

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    kwargs = {"bulk_acquirer": StubBulkAcquirer(), "body_acquirer": StubBodyAcquirer()}
    first = build_with_receipts(
        build_bill_family, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    assert first["unowned_outputs"]
    assert first["subjects"]["bill_sections"]
    assert not any(first["refused_rows"].values())
    restored = restore_prior(tmp_path / "bundle1", tmp_path / "prior")
    assert restored["bill_sections"].is_dir()
    admit_bundle(tmp_path / "bundle1", tmp_path / "generation")
    with pytest.raises(ValueError, match="integrated family prior"):
        build_with_receipts(
            build_bill_family,
            tmp_path / "run2",
            tmp_path / "bundle2",
            generation_id="g2",
            prior_bundle=tmp_path / "bundle1",
            builder_kwargs=kwargs,
        )
