"""Queryable policy observations preserve native distinctions and fail atomically."""

import gzip
import hashlib
import json

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.administration_policy import AdministrationPolicyError, metadata_url
from spicy_regs.sources import retained_collections as reader

COMMIT = "a" * 40
ADMIN = "47-Trump"


def manifest(tmp_path, records, *, compressed=False):
    body = json.dumps(records).encode()  # JSON is a supported subset of YAML.
    path = tmp_path / ("metadata.yaml.gz" if compressed else "metadata.yaml")
    path.write_bytes(gzip.compress(body) if compressed else body)
    source = {
        "kind": "administration-policy",
        "path": path.name,
        "commit": COMMIT,
        "administration": ADMIN,
        "url": metadata_url(COMMIT, ADMIN),
        "observed_at": "2026-09-28T17:32:12Z",
        "bytes": len(body),
        "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
    }
    pin = tmp_path / "selection.json"
    pin.write_text(json.dumps({"format": "spicy-retained-collection-inputs", "version": 1, "sources": [source]}))
    return pin


def statement(**extra):
    return {
        "congress": 119,
        "bills": ["hr1"],
        "document_title": "Literal title",
        "date_issued": "2026-01-02",
        "url": "https://example.test/policy",
        **extra,
    }


def rows(output, table):
    path = output / table / (table + ".parquet")
    return pq.read_table(path).to_pylist() if path.exists() else []


def test_native_statements_and_repeated_bill_links_survive_with_receipts(tmp_path):
    records = [
        statement(bills=["hr1", "s2", "hr1"], rescinded=True, unknown={"x": 1}),
        statement(bills=[], date_issued="literal unknown date"),
        statement(),
    ]
    pin = manifest(tmp_path, records, compressed=True)
    output = tmp_path / "candidate"
    result = reader.build(pin, output)
    statements = rows(output, "administration_policy_statements")
    links = rows(output, "administration_policy_bill_links")
    assert [s["source_record_index"] for s in statements] == [0, 1, 2]
    assert [json.loads(s["source_record_json"]) for s in statements] == records
    assert statements[0]["rescinded"] is True and statements[2]["rescinded"] is None
    assert statements[1]["date_issued"] == "literal unknown date"
    assert statements[1]["date_issued_status"] != "valid"
    assert [s["native_bill_id"] for s in links] == ["hr1-119", "s2-119", "hr1-119", "hr1-119"]
    assert [s["bill_id"] for s in links] == ["119-hr-1", "119-s-2", "119-hr-1", "119-hr-1"]
    assert [s["source_link_index"] for s in links] == [0, 1, 2, 0]
    assert len({s["link_id"] for s in links}) == 4
    assert all(link["statement_id"] in {s["statement_id"] for s in statements} for link in links)
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == 7 and all(r["generation_id"] == result["generation"] for r in receipts)
    assert all(r["witnesses"][0]["sha256"] == statements[0]["source_sha256"] for r in receipts)
    second = reader.build(pin, tmp_path / "second")
    assert second["generation"] == result["generation"] and second["outputs"] == result["outputs"]
    with pytest.raises(FileExistsError):
        reader.build(pin, output)


@pytest.mark.parametrize(
    "change,match",
    [
        ({"sha256": "sha256:" + "0" * 64}, "hash/size"),
        ({"bytes": 1}, "hash/size"),
        ({"url": "https://example.test/wrong"}, "locator"),
        ({"kind": "cbo"}, "kind"),
        ({"observed_at": ""}, "observation time"),
    ],
)
def test_bad_selections_leave_no_partial_candidate(tmp_path, change, match):
    pin = manifest(tmp_path, [statement()])
    value = json.loads(pin.read_text())
    value["sources"][0].update(change)
    pin.write_text(json.dumps(value))
    with pytest.raises(ValueError, match=match):
        reader.build(pin, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
    assert not list(tmp_path.glob(".retained-collections-*"))


def test_later_malformed_input_does_not_publish_earlier_valid_records(tmp_path):
    pin = manifest(tmp_path, [statement(), statement(congress="119")])
    with pytest.raises(AdministrationPolicyError):
        reader.build(pin, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_duplicate_selection_is_refused(tmp_path):
    pin = manifest(tmp_path, [statement()])
    value = json.loads(pin.read_text())
    value["sources"] *= 2
    pin.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="duplicate"):
        reader.build(pin, tmp_path / "candidate")


def test_original_change_during_build_is_refused(tmp_path, monkeypatch):
    pin = manifest(tmp_path, [statement()])
    original = reader.write_dataset

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        (tmp_path / "metadata.yaml").write_bytes(b"[]")
        return result

    monkeypatch.setattr(reader, "write_dataset", mutate)
    with pytest.raises(ValueError, match="changed during build"):
        reader.build(pin, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_bound_applies_to_decoded_gzip_input(tmp_path):
    pin = manifest(tmp_path, [statement()], compressed=True)
    (tmp_path / "metadata.yaml.gz").write_bytes(
        gzip.compress(b" " * (reader.administration_policy.MAX_METADATA_BYTES + 1))
    )
    with pytest.raises(ValueError, match="byte bound"):
        reader.build(pin, tmp_path / "candidate")


@pytest.mark.parametrize("bill_type", ["hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"])
def test_all_native_bill_types_map_to_canonical_application_keys(tmp_path, bill_type):
    pin = manifest(tmp_path, [statement(bills=[f"{bill_type}123"])])
    reader.build(pin, tmp_path / "candidate")
    link = rows(tmp_path / "candidate", "administration_policy_bill_links")[0]
    assert link["bill_id"] == f"119-{bill_type}-123"
    assert link["native_bill_id"] == f"{bill_type}123-119"


def test_unlinked_only_selection_keeps_empty_link_schema(tmp_path):
    pin = manifest(tmp_path, [statement(bills=[])])
    result = reader.build(pin, tmp_path / "candidate")
    output = result["outputs"]["administration_policy_bill_links"]
    assert output["rows"] == 0 and output["path"] and output["sha256"]
    table = pq.read_table(tmp_path / "candidate" / output["path"])
    assert table.num_rows == 0 and table.schema == reader.SCHEMAS["administration_policy_bill_links"]
    assert len(rows(tmp_path / "candidate", "administration_policy_statements")) == 1


def test_code_change_during_build_refuses_candidate(tmp_path, monkeypatch):
    pin = manifest(tmp_path, [statement()])
    calls = 0

    def changing_pins():
        nonlocal calls
        calls += 1
        return {"changed": calls}

    monkeypatch.setattr(reader, "_code_pins", changing_pins)
    with pytest.raises(ValueError, match="processing code changed"):
        reader.build(pin, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
