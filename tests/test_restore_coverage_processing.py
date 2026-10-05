"""Coverage bridge uses selected receipts, with exact original source values."""

import hashlib
import importlib.util
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import write_legislative_outputs

spec = importlib.util.spec_from_file_location(
    "coverage_bridge", Path(__file__).parents[1] / "scripts/restore_coverage_processing.py"
)
assert spec is not None and spec.loader is not None
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def member(path):
    return {
        "path": str(path.resolve()),
        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
        "byteSize": path.stat().st_size,
    }


def fixture(tmp_path, dataset, values):
    raw = {f["name"]: None for f in field_registry()[dataset]["fields"]} | values
    source = tmp_path / (dataset + ".parquet")
    schema = pa.schema([(f["name"], pa.string()) for f in field_registry()[dataset]["fields"]])
    pq.write_table(pa.Table.from_pylist([raw], schema=schema), source)
    manifest = write_legislative_outputs([source], tmp_path / "native", generation_id="coverage-test")
    request = {
        "dataset": dataset,
        "generationId": "coverage-test",
        "subjects": [member(tmp_path / "native" / p) for p in manifest["subjects"][dataset]],
        "receipts": member(tmp_path / "native" / "etl_receipts.parquet"),
        "destination": str(tmp_path / "restored"),
    }
    return raw, request


def test_restores_source_spelling_and_native_pin_separately(tmp_path):
    raw, request = fixture(
        tmp_path,
        "bill_versions",
        {"bill_id": "hr1-119", "version_code": "is", "source": "GovInfo original label", "version_date": "2025-01-03"},
    )
    result = bridge.restore(request)
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]
    assert "printing_id" in dict(result["nativeSchema"]) and "source" not in dict(result["nativeSchema"])
    assert "source" in dict(result["processingSchema"])
    assert result["selection"] == {"subjects": request["subjects"], "receipts": request["receipts"]}


def test_restores_receipt_only_reads_without_a_public_subject(tmp_path):
    raw, request = fixture(
        tmp_path,
        "document_citation_reads",
        {"document_kind": "bill_section", "document_key": "original scope", "text_sha256": "sha256:" + "a" * 64},
    )
    assert not request["subjects"]
    result = bridge.restore(request)
    assert result["receiptOnly"] and result["nativeSchema"] == []
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]


@pytest.mark.parametrize("change", ["sha", "size", "generation"])
def test_refuses_changed_or_wrong_generation_inputs(tmp_path, change):
    _, request = fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})
    if change == "sha":
        request["receipts"]["sha256"] = "sha256:" + "f" * 64
    elif change == "size":
        request["receipts"]["byteSize"] += 1
    else:
        request["generationId"] = "unrelated-generation"
    with pytest.raises(ValueError):
        bridge.restore(request)


def test_schema_cli_describes_both_native_policy_families():
    import json
    import subprocess
    import sys

    for dataset, version in [("bill_versions", "legislative-documents/1"), ("bill_actions", "congress-subjects/1")]:
        result = json.loads(
            subprocess.check_output(
                [
                    sys.executable,
                    str(Path(__file__).parents[1] / "scripts/restore_coverage_processing.py"),
                    "--schema",
                    dataset,
                ]
            )
        )
        assert result["nativeSchema"] and result["policyVersion"] == version
        assert result["policy"]["dataset"] == dataset and result["implementationSha256"].startswith("sha256:")


def test_congress_native_source_fields_restore_using_its_maintained_reader(tmp_path):
    from spicy_regs.congress_receipts import write_congress_dataset

    source = tmp_path / "source.parquet"
    raw = {"bill_id": "hr1-119", "subjects_json": "[]", "url": "https://example.gov/original-bill"}
    pq.write_table(pa.Table.from_pylist([raw]), source)
    subject, receipts = write_congress_dataset(
        source, tmp_path / "native", dataset="congress_bills", generation_id="congress-coverage"
    )
    request = {
        "dataset": "congress_bills",
        "generationId": "congress-coverage",
        "subjects": [member(subject)],
        "receipts": member(receipts),
        "destination": str(tmp_path / "restored"),
    }
    result = bridge.restore(request)
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]
    assert result["policyVersion"] == "congress-subjects/1"
