"""Quarterly selection uses native identity and report meaning, never latest posting alone."""

from datetime import datetime, UTC
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import build_lobbying_quarterly_profiles as m


def filing(id="original", **changes):
    return dict.fromkeys(m.FIELDS) | dict(
        filing_uuid=id, filing_type="Q1", filing_year=2025, filing_period="first_quarter",
        dt_posted=datetime(2025, 4, 20, tzinfo=UTC), registrant_id="10", client_id="20",
        registrant_name="Same name", client_name="Same client name", income="9000.00",
        url="https://lda.gov/filings/public/filing/" + id + "/print/",
    ) | changes


def selection(**changes):
    return dict(filing_year=2025, periods=["first_quarter"], cutoff="2026-11-13T00:00:00+00:00",
                registrant_ids=[], source_table_generation="sha256:" + "a" * 64,
                source_table_sha256="sha256:" + "b" * 64) | changes


def amended(**changes):
    return filing("amendment", **(dict(filing_type="1A", income="10000.00",
                  dt_posted=datetime(2026, 6, 19, tzinfo=UTC)) | changes))


def test_late_amendment_restatement_and_cutoff():
    old, new = filing(), amended()
    row = m.profile([old, new], selection())
    assert (row["income"], row["income_amendment_change"]) == ("10000.00", "1000.00")
    assert row["selection_reason"] == "unique_amendment_restatement"
    assert json.loads(row["witnesses_json"])[0]["income"] == "9000.00"
    earlier = m.profile([old, new], selection(cutoff="2025-12-31T00:00:00+00:00"))
    assert earlier["selected_filing_uuid"] == "original"
    assert json.loads(earlier["excluded_filing_uuids_json"]) == ["amendment"]
    assert earlier["income_amendment_change"] is None


@pytest.mark.parametrize("rows,reason", [
    ([filing(), amended(), filing("another", filing_type="1A", dt_posted=datetime(2026, 8, 1, tzinfo=UTC))], "competing_amendments_no_predecessor_link"),
    ([amended()], "missing_original"),
    ([filing(), filing("second", dt_posted=datetime(2026, 1, 1, tzinfo=UTC))], "competing_originals"),
    ([filing(), amended(dt_posted=datetime(2025, 4, 20, tzinfo=UTC))], "amendment_order_unresolved"),
    ([filing(dt_posted=datetime(1905, 1, 1, tzinfo=UTC))], "unresolved_posting_time"),
    ([filing(client_id=None)], "missing_native_identity"),
])
def test_unresolved_competitors_do_not_pick_latest(rows, reason):
    result = m.profile(rows, selection())
    assert result["selection_reason"] == reason
    assert result["selection_status"] == "unresolved"
    assert result["income"] is None and result["selected_filing_uuid"] is None
    assert len(json.loads(result["witnesses_json"])) == len(rows)


def test_termination_and_no_activity_do_not_erase_amount():
    original = filing(filing_type="1T", termination_date="2025-03-31")
    restated = amended(filing_type="1@Y", termination_date="2025-03-31")
    result = m.profile([original, restated], selection())
    assert result["selected_filing_type"] == "1@Y"
    assert result["income"] == "10000.00" and result["termination_date"] == "2025-03-31"
    assert m.profile([original], selection())["income"] == "9000.00"


@pytest.mark.parametrize("value,status", [(None, "source_null"), ("", "source_empty"), ("0.00", "below_threshold_or_zero_unresolved"),
    ("4999", "below_threshold_or_zero_unresolved"), ("-1", "invalid_amount"), ("NaN", "invalid_amount"), (12.5, "unsupported_number")])
def test_amount_missing_threshold_and_invalid_states(value, status):
    result = m.profile([filing(income=value)], selection())
    assert result["income"] is None and result["income_status"] == status
    assert json.loads(result["witnesses_json"])[0]["income"] == value


def test_expense_method_and_dual_measure_qualifications():
    for method in ["A", "a", "B", "c", None, ""]:
        result = m.profile([filing(income=None, expenses="9007199254740993.01", expenses_method=method)], selection())
        assert result["expenses_method"] == method
        assert (result["expenses"] is not None) == (method not in [None, ""])
    both = m.profile([filing(expenses="10000.00", expenses_method="A")], selection())
    assert both["income"] is None and both["expenses"] is None
    assert both["income_status"] == "both_measures_reported_unresolved"


def test_native_ids_quarter_types_schema_and_input_integrity(tmp_path):
    source, output = tmp_path / "source.parquet", tmp_path / m.OUTPUT
    rows = [filing(), amended(), filing("registration", filing_type="RA", income="999999.00"),
            filing("other-firm", registrant_id="11", client_id="21", income="50000.00")]
    pq.write_table(pa.Table.from_pylist(rows), source)
    selected = selection(source_table_sha256=m.digest_file(source))
    summary = m.build_profiles(selected, source, output)
    assert summary["filings"] == 3 and summary["profiles"] == 2
    actual = pq.read_table(output).to_pylist()
    assert [row["income"] for row in actual] == ["10000.00", "50000.00"]
    assert all(row["coverage_status"] == "incomplete_retained_population" for row in actual)
    prior = output.read_bytes()
    with pytest.raises(ValueError, match="SHA-256"):
        m.build_profiles(selected | {"source_table_sha256": "sha256:" + "0" * 64}, source, output)
    assert output.read_bytes() == prior
    with pytest.raises(ValueError, match="empty"):
        m.build_profiles(selected | {"registrant_ids": ["99"]}, source, output)
    assert output.read_bytes() == prior
    pq.write_table(pa.Table.from_pylist([filing(), filing()]), source)
    with pytest.raises(ValueError, match="repeats"):
        m.build_profiles(selected | {"source_table_sha256": m.digest_file(source)}, source, output)
    assert output.read_bytes() == prior


def test_reject_missing_source_fields_and_undeclared_scope(tmp_path):
    path = tmp_path / "old.parquet"
    pq.write_table(pa.Table.from_pylist([{"filing_uuid": "a"}]), path)
    with pytest.raises(ValueError, match="source columns"):
        m.build_profiles(selection(source_table_sha256=m.digest_file(path)), path, tmp_path / m.OUTPUT)
    with pytest.raises(ValueError, match="timezone"):
        m.validate_selection(selection(cutoff="2026-11-13"))
    with pytest.raises(ValueError, match="2008"):
        m.validate_selection(selection(filing_year=2007))


def test_quarter_four_and_method_changes_remain_distinct():
    q4 = m.profile([filing(filing_period="fourth_quarter", filing_type="Q4", dt_posted=datetime(2026, 1, 20, tzinfo=UTC))],
                   selection(periods=["fourth_quarter"]))
    assert str(q4["period_start"]) == "2025-10-01" and str(q4["period_end"]) == "2025-12-31"
    old = filing(income=None, expenses="10000.00", expenses_method="a")
    new = amended(income=None, expenses="20000.00", expenses_method="B")
    result = m.profile([old, new], selection())
    assert result["expenses"] == "20000.00" and result["expenses_amendment_change"] is None


def test_amendment_difference_keeps_full_native_decimal_precision():
    before = "99999999999999999999999999999999.999999"
    after = "99999999999999999999999999999998.999998"
    result = m.profile([filing(income=before), amended(income=after)], selection())
    assert result["income"] == after and result["original_income"] == before
    assert result["income_amendment_change"] == "-1.000001"


def test_generation_binding_publication_and_ordinary_mcp(tmp_path, monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.contract_types import described_schema
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.pipelines import lobbying_quarterly_profiles as pipeline
    from spicy_regs.sources import publication
    from spicy_regs.transforms.build_lobbying_filings import map_subject, SUBJECT_SCHEMAS
    from tests.generation_fakes import Store
    from tests.test_mcp_query_results import call
    from tests.test_mcp_server import _records
    from starlette.testclient import TestClient

    source = tmp_path / "lobbying_filings.parquet"
    schema = SUBJECT_SCHEMAS["lobbying_filings"]
    native = [map_subject("lobbying_filings", row | {"lobbying_activities_json": "null", "government_entities_json": "null"})
              for row in [filing(), amended()]]
    pq.write_table(pa.Table.from_pylist(native, schema=schema), source)
    root = build_generation(tmp_path / "source-generation", family="lobbying-filings", files=(source,),
                            expected_keys=(source.name,), schemas={"lobbying_filings": described_schema(schema)})
    source = tmp_path / "source-generation" / source.name
    selected = selection(source_table_generation=root.pin.artifact_digest, source_table_sha256=m.digest_file(source))
    artifact, evidence, summary = pipeline.build(selected, source, tmp_path / "profiles")
    assert summary["profiles"] == 1
    assert verify_generation(tmp_path / "profiles/generation").pin == artifact.pin
    # Exercise the existing publisher against a bounded private store; no external writes.
    store = Store()
    published = publication.publish_generation(tmp_path / "profiles/generation", client=store, bucket="fixture",
        prior_index=publication.empty_index(), evidence_directories=(evidence,))
    assert published["families"][pipeline.FAMILY]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "profiles/generation")
    with mcp_server._build_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        with TestClient(mcp_server.build_app()) as client:
            result = call(client, "query_sql", {"sql": "SELECT income, income_amendment_change FROM lobbying_quarterly_profiles"})
            assert result["isError"] is False
            assert _records(result["structuredContent"]) == [{"income": "10000.000000", "income_amendment_change": "1000.000000"}]
            assert call(client, "query_sql", {"sql": "DELETE FROM lobbying_quarterly_profiles"})["isError"] is True
    with pytest.raises(ValueError, match="source generation"):
        pipeline.build(selected | {"source_table_generation": "sha256:" + "0" * 64}, source, tmp_path / "wrong-source")
