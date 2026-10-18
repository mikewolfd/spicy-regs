"""The maintained mappers plus SQL receipts equal the independent row writers."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import congress_bulk
from spicy_regs.congress_receipts import write_congress_dataset
from spicy_regs.legislative_receipts import restore_prior, write_legislative_outputs
from tests.test_congress_receipts import shaped, RETURNED
from tests.test_legislative_receipts import retained, row


CONGRESS = [
    ("members", {"bioguide_id": "X", "fec_ids_json": '["id",null,"id"]', "name_first": "A"}),
    ("committee_meetings", {"congress": "119", "chamber": "house", "event_id": "1",
                             "witnesses_json": '[{"name":"A","position":null}]'}),
    ("roll_call_votes", {"vote_id": "v", "tallies_json": '{"Yea":10,"Nay":2}'}),
    ("public_activity_events", {"bill_id": "b", "event_type": "summary_generated", "subject_id": "x"}),
    *[(name, data[1]) for name, data in RETURNED.items()],
]


def _spy(monkeypatch):
    calls = []
    original = congress_bulk.write_mapped_bundle
    def write(*args, **kwargs):
        calls.append(kwargs["policy"].dataset)
        return original(*args, **kwargs)
    monkeypatch.setattr(congress_bulk, "write_mapped_bundle", write)
    return calls


@pytest.mark.parametrize("dataset,raw", CONGRESS)
def test_congress_hybrid_matches_exact_receipts(tmp_path, monkeypatch, dataset, raw):
    calls = _spy(monkeypatch)
    # Raw control text, missing keys, and an extra source key all retain the
    # row writer's own conversion decision and exact source bytes.
    source = shaped(tmp_path / "source.parquet", [raw, dict.fromkeys(raw)])
    outputs = [write_congress_dataset(source, tmp_path / str(bulk), dataset=dataset,
                                      generation_id="same", bulk=bulk) for bulk in (False, True)]
    if not congress_bulk.eligible(dataset, pq.read_schema(source)):
        assert calls == [dataset]
    for left, right in zip(*outputs, strict=True):
        assert (left is None) == (right is None)
        if left is not None:
            assert pq.read_table(left).equals(pq.read_table(right), check_metadata=True)


LEGISLATIVE = [
    ("table3_records", dict(act_key="119-1", seq="0", status="repealed")),
    ("budget_volumes", dict(package_id="BUDGET-2027", associated_laws_json='["a",null,"a"]',
                            associated_bills_json="[]")),
    ("hearing_transcripts", dict(package_id="CHRG-119hhrg1", witnesses_json='["A",null,"A"]')),
    ("committee_report_reads", dict(package_id="CRPT-119hrpt1", outcome="complete", rule_version="r")),
    ("section_diff_items", dict(bill_id="119-hr-1", from_version_code="ih", from_source="govinfo",
                                to_version_code="enr", to_source="govinfo", seq="0", moved="false",
                                financial_from_amounts_json='["9007199254740993.01",null,"-0.01"]')),
    ("senate_expenditures", dict(package_id="GPO-CDOC-119sdoc3", file_name="p.pdf", page="1",
                                 table_ordinal="0", row_ordinal="2", text_sha256="a" * 64,
                                 amounts_json='[null,[],[{"amount":"-88657.02","text":"-88,657.02"},null]]')),
]


@pytest.mark.parametrize("dataset,values", LEGISLATIVE)
def test_legislative_hybrid_matches_exact_bundle(tmp_path, monkeypatch, dataset, values):
    calls = _spy(monkeypatch)
    source = retained(tmp_path / "source", dataset, [row(dataset, **values),
                      row(dataset, package_id="other") if dataset == "committee_report_reads" else row(dataset)])
    manifests = [write_legislative_outputs([source], tmp_path / str(bulk), generation_id="same", bulk=bulk)
                 for bulk in (False, True)]
    assert calls == [dataset]
    assert manifests[0] == manifests[1]
    for path in (tmp_path / "False").rglob("*.parquet"):
        other = tmp_path / "True" / path.relative_to(tmp_path / "False")
        assert pq.ParquetFile(path).read().equals(pq.ParquetFile(other).read(), check_metadata=True)


@pytest.mark.parametrize("dataset", ["bill_sections", "section_diff_items"])
@pytest.mark.parametrize("exceptional", [False, True])
def test_bill_unit_separators_keep_exact_receipts_without_row_decisions(tmp_path, monkeypatch, dataset, exceptional):
    base = (dict(version_code="enr", source="govinfo") if dataset == "bill_sections" else
            dict(from_version_code="ih", from_source="govinfo", to_version_code="enr", to_source="govinfo"))
    paths = ["\x1f", "\\u001F", "\\\x1f\\\\u001F", "\x1f\x1f", 'section"one']
    rows = [row(dataset, bill_id="119-hr-1", seq=str(i), **base,
                match_path="section\x1f" + path, match_path_json=json.dumps(["section", path]))
            for i, path in enumerate(paths)]
    if exceptional:
        rows[1]["match_path_json"] = "not-json"  # The reference mapper must still refuse this row.
        rows[3]["match_path"] += "\x1b"  # Other unproven controls still take the reference route.
    source = retained(tmp_path / "source", dataset, rows, {b"source": b"unit-separator-fixture"})
    decisions, original = [], congress_bulk.write_mapped_bundle

    def write(*args, **kwargs):
        count = original(*args, **kwargs)
        decisions.append(count)
        return count

    monkeypatch.setattr(congress_bulk, "write_mapped_bundle", write)
    manifests = [write_legislative_outputs([source], tmp_path / str(bulk), generation_id="same", bulk=bulk)
                 for bulk in (False, True)]
    assert decisions == [2 if exceptional else 0]
    assert manifests[0] == manifests[1]
    for path in (tmp_path / "False").rglob("*.parquet"):
        other = tmp_path / "True" / path.relative_to(tmp_path / "False")
        assert pq.ParquetFile(path).read().equals(pq.ParquetFile(other).read(), check_metadata=True)
    if exceptional:
        for bulk in (False, True):
            with pytest.raises(ValueError, match="conversion refusals"):
                restore_prior(tmp_path / str(bulk), tmp_path / f"restored-{bulk}")
    else:
        restored = restore_prior(tmp_path / "True", tmp_path / "restored")
        assert pq.read_table(restored[dataset]).equals(pq.read_table(source), check_metadata=True)


@pytest.mark.parametrize("dataset", ["members", "committee_report_reads"])
def test_hybrid_escape_rows_keep_order_across_batches(tmp_path, monkeypatch, dataset):
    calls = _spy(monkeypatch)
    if dataset == "members":
        rows = [{"bioguide_id": str(i), "name_first": "a\x1b" if i % 201 == 0 else "A"} for i in range(2011)]
        source = shaped(tmp_path / "source.parquet", rows)
        def write(bulk):
            return write_congress_dataset(source, tmp_path / str(bulk), dataset=dataset,
                                           generation_id="same", bulk=bulk, validate=False)[1]
    else:
        rows = [row(dataset, package_id=str(i), rule_version="a\x1b" if i % 201 == 0 else "A") for i in range(2011)]
        source = retained(tmp_path / "source", dataset, rows)
        def write(bulk):
            write_legislative_outputs([source], tmp_path / str(bulk), generation_id="same", bulk=bulk, validate=False)
            return tmp_path / str(bulk) / "etl_receipts.parquet"
    first, second = write(False), write(True)
    assert calls == [dataset]
    assert pq.read_table(first).equals(pq.read_table(second), check_metadata=True)


def test_hybrid_unknown_raw_columns_remain_exact_refusals(tmp_path):
    source = shaped(tmp_path / "source.parquet", [{"bioguide_id": "X", 'unknown"column': "x\x1b"}])
    outputs = [write_congress_dataset(source, tmp_path / str(bulk), dataset="members", generation_id="same", bulk=bulk)
               for bulk in (False, True)]
    assert pq.read_table(outputs[0][1]).equals(pq.read_table(outputs[1][1]))
    assert pq.read_table(outputs[1][1])["outcome"].to_pylist() == ["observed", "refused"]


def test_hybrid_empty_input_retains_metadata(tmp_path):
    source = tmp_path / "source.parquet"
    pq.write_table(pa.table({"bioguide_id": pa.array([], type=pa.string())}).replace_schema_metadata({b"empty": b"yes"}), source)
    outputs = [write_congress_dataset(source, tmp_path / str(bulk), dataset="members", generation_id="same", bulk=bulk)
               for bulk in (False, True)]
    assert pq.read_table(outputs[0][1]).equals(pq.read_table(outputs[1][1]), check_metadata=True)


@pytest.mark.parametrize("dataset", ["members", "budget_volumes"])
def test_decoded_json_control_routes_before_subject_digest(tmp_path, dataset):
    # The retained input contains literal JSON escape text; only the mapped
    # native list contains the actual control character.
    if dataset == "members":
        source = shaped(tmp_path / "source.parquet", [{"bioguide_id": "X", "fec_ids_json": '["\\u001b"]'}])
        paths = [write_congress_dataset(source, tmp_path / str(bulk), dataset=dataset, generation_id="same", bulk=bulk)
                 for bulk in (False, True)]
        assert pq.read_table(paths[0][1]).equals(pq.read_table(paths[1][1]))
    else:
        source = retained(tmp_path / "source", dataset, [row(dataset, package_id="X", associated_laws_json='["\\u001b"]')])
        for bulk in (False, True):
            write_legislative_outputs([source], tmp_path / str(bulk), generation_id="same", bulk=bulk)
        assert pq.read_table(tmp_path / "False/etl_receipts.parquet").equals(pq.read_table(tmp_path / "True/etl_receipts.parquet"))


@pytest.mark.parametrize("dataset", ["members", "member_votes", "budget_volumes"])
def test_required_bulk_allows_bounded_exceptional_row_decisions(tmp_path, dataset):
    from spicy_regs import etl_bulk

    with etl_bulk.bulk_limits(threads=2, memory_limit="512MiB", require_bulk=True):
        if dataset == "budget_volumes":
            source = retained(tmp_path / "source", dataset, [row(dataset, package_id="X", associated_laws_json='["\\u001b"]')])
            write_legislative_outputs([source], tmp_path / "bundle", generation_id="same", bulk=True, validate=False)
            receipt = tmp_path / "bundle/etl_receipts.parquet"
        else:
            raw = ({"bioguide_id": "X", "name_first": "A\x1b"} if dataset == "members" else
                   {"vote_id": "v", "member_key": "k", "member_name": "A\x1b"})
            source = shaped(tmp_path / "source.parquet", [raw])
            _, receipt = write_congress_dataset(source, tmp_path / "bundle", dataset=dataset,
                                                  generation_id="same", bulk=True, validate=False)
    assert "accepted" in pq.read_table(receipt)["outcome"].to_pylist()
