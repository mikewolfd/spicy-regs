"""Coverage bridge uses selected receipts, with exact original source values."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs import coverage_reader as bridge
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import write_legislative_outputs


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
        {
            "document_kind": "bill_section",
            "document_key": '["hr1-119","is","GovInfo original label","1"]',
            "text_sha256": "sha256:" + "a" * 64,
        },
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
    for dataset, version in [("bill_versions", "legislative-documents/1"), ("bill_actions", "congress-subjects/1")]:
        result = json.loads(
            subprocess.check_output([sys.executable, "-m", "spicy_regs.coverage_reader", "--schema", dataset])
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


def rewrite_receipts(request, edit):
    """Rewrite the fixture receipts through `edit(table) -> table` and re-pin the member."""
    path = Path(request["receipts"]["path"])
    pq.write_table(edit(pq.read_table(path)), path)
    request["receipts"] = member(path)


def replaced(table, name, values):
    return table.set_column(
        table.schema.get_field_index(name), name, pa.array(values, type=table.schema.field(name).type)
    )


def bill_versions_request(tmp_path):
    return fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})[1]


def test_refuses_undeclared_policy_receipts(tmp_path):
    request = bill_versions_request(tmp_path)
    rewrite_receipts(request, lambda t: replaced(t, "policy_version", ["legislative-documents/0"] * t.num_rows))
    with pytest.raises(ValueError, match="Unsupported coverage receipt policy"):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_declared_earlier_policy_receipts_are_admitted_and_undeclared_ones_refused(tmp_path):
    """Carried receipts keep the version they were written under; the call is direct because relabelling a row
    breaks its digest, which a full restore would refuse."""
    from spicy_regs.congress_receipts import write_congress_dataset

    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_pylist([{"congress": "119", "chamber": "house", "event_id": "7"}]), source)
    _, receipts = write_congress_dataset(
        source, tmp_path / "native", dataset="committee_meetings", generation_id="congress-coverage"
    )
    policy = bridge.dataset_policy("committee_meetings")
    assert policy.policy_version == "congress-subjects/2"
    table = pq.read_table(receipts)
    assert set(table["policy_version"].to_pylist()) == {"congress-subjects/2"}

    def relabelled(name, version):
        path = tmp_path / name
        pq.write_table(replaced(table, "policy_version", [version] + table["policy_version"].to_pylist()[1:]), path)
        return path

    bridge.current_receipts(receipts, "committee_meetings", policy, "congress-coverage")
    bridge.current_receipts(
        relabelled("declared.parquet", "congress-subjects/1"), "committee_meetings", policy, "congress-coverage"
    )
    with pytest.raises(ValueError, match="Unsupported coverage receipt policy"):
        bridge.current_receipts(
            relabelled("undeclared.parquet", "congress-subjects/0"), "committee_meetings", policy, "congress-coverage"
        )
    with pytest.raises(ValueError, match="differ from the selected generation"):
        bridge.current_receipts(
            relabelled("declared.parquet", "congress-subjects/1"), "committee_meetings", policy, "other"
        )


def test_refuses_receipts_missing_a_current_field(tmp_path):
    request = bill_versions_request(tmp_path)
    rewrite_receipts(request, lambda t: t.drop_columns(["policy_version"]))
    with pytest.raises(ValueError, match="Unsupported coverage receipt format"):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_other_dataset_rows_are_ignored_but_mixed_generation_refuses(tmp_path):
    raw, request = fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})
    path = Path(request["receipts"]["path"])
    original = path.read_bytes()

    def append_unrelated(table):
        row = table.slice(0, 1).to_pylist()[0] | {"dataset": "unrelated", "generation_id": "other"}
        return pa.concat_tables([table, pa.Table.from_pylist([row], schema=table.schema)])

    rewrite_receipts(request, append_unrelated)
    result = bridge.restore(request)
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]

    path.write_bytes(original)
    request["receipts"] = member(path)
    request["destination"] = str(tmp_path / "mixed")
    rewrite_receipts(
        request, lambda t: replaced(t, "generation_id", ["other"] + t.column("generation_id").to_pylist()[1:])
    )
    with pytest.raises(ValueError, match="differ from the selected generation"):
        bridge.restore(request)


@pytest.mark.parametrize(
    ("column", "message"),
    [
        ("policy_version", "Unsupported coverage receipt policy"),
        ("generation_id", "differ from the selected generation"),
    ],
)
def test_null_policy_or_generation_refuses(tmp_path, column, message):
    request = bill_versions_request(tmp_path)
    rewrite_receipts(request, lambda t: replaced(t, column, [None] + t.column(column).to_pylist()[1:]))
    with pytest.raises(ValueError, match=message):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_malformed_request_refused_before_hashing(tmp_path):
    request = bill_versions_request(tmp_path)
    cases = [
        ({k: v for k, v in request.items() if k != "destination"}, "Unsupported coverage restoration request"),
        (request | {"extra": 1}, "Unsupported coverage restoration request"),
        (request | {"generationId": ""}, "Missing coverage receipt generation"),
        (request | {"subjects": {}}, "Coverage subjects must be explicit"),
    ]
    with patch.object(
        bridge, "checked_member", side_effect=AssertionError("hashed before the request was admitted")
    ) as hashed:
        for bad, message in cases:
            with pytest.raises(ValueError, match=message):
                bridge.restore(bad)
        hashed.assert_not_called()


def test_one_subject_requests_refuse_other_counts(tmp_path):
    request = bill_versions_request(tmp_path)
    with pytest.raises(ValueError, match="Coverage subjects must be explicit"):
        bridge.checked_request(request | {"subjects": request["subjects"] * 2}, bridge.dataset_policy, one_subject=True)


def test_existing_destination_is_refused_before_hashing(tmp_path):
    request = bill_versions_request(tmp_path)
    Path(request["destination"]).mkdir()
    with patch.object(bridge, "checked_member", side_effect=AssertionError("hashed before the destination check")):
        with pytest.raises(ValueError, match="new absolute directory"):
            bridge.restore(request)
