import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
from spicy_regs.transforms.fec_identity_receipts import (
    IdentityReceiptWriter,
    read_identity_rows,
    read_identity_processing,
)

WITNESS = {
    "source_id": "held-input",
    "source_uri": None,
    "sha256": "sha256:" + "a" * 64,
    "locator": None,
    "body_version": None,
}


def test_every_table_subject_or_processing_roundtrip(tmp_path):
    output = tmp_path / "all"
    with IdentityReceiptWriter(output, generation_id="g1", tables=REGISTRY) as writer:
        for name, rules in REGISTRY.items():
            row = dict.fromkeys(rules["input_fields"])
            for key in rules["identity_fields"]:
                if key in row:
                    row[key] = "2024" if key == "cycle" else name + key
            if name == "fec_research_source_pages":
                row.update(content_status="body_extracted", text="Public document")
            writer.emit(name, row, input_witness=WITNESS, source_input={"original": None})
    for name, rules in REGISTRY.items():
        if rules["receipt_only"]:
            assert not (output / (name + ".parquet")).exists()
            assert len(list(read_identity_processing(output, name, generation_id="g1"))) == 1
        else:
            stored = pq.read_table(output / (name + ".parquet"))
            assert stored.num_rows == 1
            assert stored.schema.names == list(rules["subject_fields"])
            assert len(list(read_identity_rows(output, name, generation_id="g1"))) == 1
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == len(REGISTRY)


def test_native_list_nulls_and_multiple_context_witnesses_survive(tmp_path):
    output = tmp_path / "identity"
    table = "fec_committee_observations"
    row = {
        "record_id": "source-observation",
        "candidate_ids_json": '["H4NC05146","N/A",null,"H4NC05146"]',
        "cycles_json": "[2024,true,null,2024]",
    }
    evidence = [
        {"collection_id": "part-a", "witness_sha256": "sha256:" + "b" * 64, "role": "item_fields"},
        {"collection_id": "part-b", "witness_sha256": "sha256:" + "c" * 64, "role": "item_fields"},
    ]
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(table, row, input_witness=WITNESS, evidence=evidence)
    subject = pq.read_table(output / (table + ".parquet")).to_pylist()[0]
    assert subject["candidate_ids"] == ["H4NC05146", "N/A", None, "H4NC05146"]
    assert subject["cycles"] == [2024, None, None, 2024]
    assert "cycles_json" not in subject
    receipt = pq.read_table(output / "etl_receipts.parquet").to_pylist()[0]
    assert [w["source_id"] for w in receipt["witnesses"]] == ["held-input", "part-a", "part-b"]
    internal = list(read_identity_rows(output, table, generation_id="g1"))[0]
    assert internal["cycles_json"] == row["cycles_json"]
    assert internal["conversion_diagnostics"]["cycles"][0]["ordinal"] == 1
    with pytest.raises(ValueError):
        list(read_identity_rows(output, table, generation_id="wrong"))


def test_failed_pages_have_receipts_without_subjects(tmp_path):
    output = tmp_path / "pages"
    table = "fec_research_source_pages"
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(
            table,
            {"record_id": "failed", "content_status": "failed_page", "title": "Server error"},
            input_witness=WITNESS,
        )
        writer.emit(
            table, {"record_id": "unsupported", "content_status": "unsupported_body_boundaries"}, input_witness=WITNESS
        )
    assert pq.read_table(output / (table + ".parquet")).num_rows == 0
    assert [r["outcome"] for r in pq.read_table(output / "etl_receipts.parquet").to_pylist()] == ["refused", "refused"]


def test_subject_tamper_missing_receipt_duplicate_identity_and_existing_output_refuse(tmp_path):
    table = "fec_quality_notices"
    output = tmp_path / "notices"
    row = {
        "record_id": "notice",
        "notice_kind": "publisher_false_fictitious_filings_list",
        "notice_scope": "source_listed_committee",
        "exclusion_status": "no_automatic_exclusion",
    }
    with pytest.raises(ValueError):
        with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
            writer.emit(table, row, input_witness=WITNESS)
            writer.emit(table, row, input_witness=WITNESS)
    assert not output.exists()
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(table, row, input_witness=WITNESS)
    with pytest.raises(FileExistsError):
        with IdentityReceiptWriter(output, generation_id="g2", tables=[table]):
            pass
    internal = list(read_identity_rows(output, table, generation_id="g1"))[0]
    assert internal["exclusion_status"] == "no_automatic_exclusion"
    schema = pq.read_schema(output / (table + ".parquet"))
    subject = pq.read_table(output / (table + ".parquet")).to_pylist()[0]
    subject["notice_kind"] = "tampered"
    import pyarrow as pa

    pq.write_table(pa.Table.from_pylist([subject], schema=schema), output / (table + ".parquet"))
    with pytest.raises(ValueError):
        list(read_identity_rows(output, table, generation_id="g1"))


def test_real_mapping_build_and_generation_admission(tmp_path):
    import hashlib
    import pyarrow as pa
    from spicy_regs.transforms.build_fec_identity_context import build_fec_identity_context
    from spicy_regs.transforms.fec_identity_receipts import seal_identity_context
    from spicy_regs.generations import verify_generation
    from tests.test_fec_candidate_observations import entry, row, GEN

    source = tmp_path / "source.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [row({"candidate_id": "H2AK01158", "candidate_status": "C", "cycles": [2024, 2024, None]})]
        ),
        source,
    )
    spec = {
        "version": 1,
        "generation_id": "build-g1",
        "tables": [
            "fec_candidate_api_observations",
            "fec_api_response_controls",
            "fec_source_records",
            "fec_record_evidence",
        ],
        "inputs": [
            {
                "mode": "source_records",
                "table": "fec_source_records",
                "path": str(source),
                "rows": 1,
                "sha256": "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest(),
                "source_generation_pin": GEN,
                "jobs": [{"kind": "candidate_api", "entry": entry()}],
            }
        ],
    }
    manifest = tmp_path / "selection.json"
    manifest.write_text(json.dumps(spec))
    output = tmp_path / "result"
    build_fec_identity_context(manifest, output)
    subject = pq.read_table(output / "fec_candidate_api_observations.parquet").to_pylist()[0]
    assert subject["candidate_status"] == "C"
    assert subject["cycles"] == [2024, 2024, None]
    assert not (output / "fec_api_response_controls.parquet").exists()
    seal_identity_context(output, tmp_path / "generation")
    artifact = verify_generation(tmp_path / "generation")
    assert artifact.root["spec"]["etlReceipts"]["generationId"] == "build-g1"
    assert artifact.root["spec"]["publicationStatus"] == "local-partial"
    spec["inputs"][0]["sha256"] = "sha256:" + "f" * 64
    manifest.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="selected regular-file bytes"):
        build_fec_identity_context(manifest, tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def test_notice_consumer_preserves_financial_guard_and_refuses_wrong_generation(tmp_path):
    from tests.test_fec_financial_policy import bulk, row as financial_fixture
    from spicy_regs.transforms.fec_identity_consumers import quality_notice_effect_with_receipts
    from spicy_regs.transforms.fec_financial_policy import quality_notice_effect

    table = "fec_quality_notices"
    notice = financial_fixture(
        "notice",
        notice_kind="publisher_false_fictitious_filings_list",
        notice_scope="source_listed_committee",
        exclusion_status="no_automatic_exclusion",
        committee_id="C00000001",
    )
    # The generic financial fixture includes financial-only fields that are not notice fields.
    notice = {k: v for k, v in notice.items() if k in REGISTRY[table]["input_fields"]}
    financial = bulk(reporting_committee_id="C00000001")
    output = tmp_path / "notice"
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(table, notice, input_witness=WITNESS)
    actual = quality_notice_effect_with_receipts(
        financial, directory=output, notice_id=notice["record_id"], generation_id="g1"
    )
    assert actual == quality_notice_effect(financial, notice)
    assert not actual["automatic_exclusion"]
    assert not actual["donor_identity_inferred"]
    with pytest.raises(ValueError):
        quality_notice_effect_with_receipts(
            financial, directory=output, notice_id=notice["record_id"], generation_id="wrong"
        )


def test_existing_catalog_producer_has_actual_receipt_writing_entrypoint(tmp_path):
    from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup

    result = build_fec_identity_rollup("fec_source_catalog", tmp_path / "catalog", generation_id="g1")
    assert not (result / "fec_source_catalog.parquet").exists()
    rows = list(read_identity_processing(result, "fec_source_catalog", generation_id="g1"))
    assert rows and all(row["source_family"] for row in rows)
    assert all("source_metadata_json" in row for row in rows)


def test_committee_increment_requires_exact_prior_receipts_before_producer(tmp_path, monkeypatch):
    import shutil
    import importlib
    from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup
    from spicy_regs.transforms.build_fec_committees import _shape

    table = "fec_committees"
    prior = tmp_path / "prior"
    row = _shape(
        {"committee_id": "C00000001", "cycles": [2024, 2024], "candidate_ids": None, "first_file_date": "2024-01-02"}
    )
    with IdentityReceiptWriter(prior, generation_id="prior-g", tables=[table]) as writer:
        writer.emit(table, row, input_witness=WITNESS)
    calls = []

    def producer(stage, **kwargs):
        calls.append(stage)
        held = pq.read_table(stage / "_fec_prior.parquet").to_pylist()[0]
        assert held["cycles_json"] == "[2024, 2024]"
        assert held["candidate_ids_json"] == "null"
        assert held["first_file_date"] == "2024-01-02"
        result = stage / "fec_committees.parquet"
        shutil.copyfile(stage / "_fec_prior.parquet", result)
        return result

    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    monkeypatch.setattr(module, "build_fec_committees", producer)
    with pytest.raises(ValueError, match="prior receipt bundle"):
        build_fec_identity_rollup(table, tmp_path / "absent", generation_id="g")
    with pytest.raises(ValueError):
        build_fec_identity_rollup(
            table, tmp_path / "wrong", generation_id="g", prior_bundle=prior, prior_generation_id="wrong"
        )
    assert not calls
    output = build_fec_identity_rollup(
        table, tmp_path / "next", generation_id="next-g", prior_bundle=prior, prior_generation_id="prior-g"
    )
    subject = pq.read_table(output / "fec_committees.parquet").to_pylist()[0]
    assert subject["cycles"] == [2024, 2024]
    assert subject["candidate_ids"] is None
    from spicy_regs.etl_receipts import resolve_receipt_witness

    [before] = pq.read_table(prior / "etl_receipts.parquet").to_pylist()
    [after] = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert after["witnesses"][: len(before["witnesses"])] == before["witnesses"]
    reference = next(w for w in after["witnesses"] if w["body_version"] == before["receipt_id"])
    assert resolve_receipt_witness(after, reference)
