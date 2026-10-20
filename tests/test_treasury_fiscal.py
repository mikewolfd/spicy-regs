"""Bounded Treasury fixtures exercise values, periods, revisions and existing serving."""
from copy import deepcopy
from datetime import date
from decimal import Decimal
import hashlib
import json

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import mcp_server
from spicy_regs.pipelines import treasury_fiscal as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_treasury_fiscal as module
from tests.generation_fakes import Store

DEBT_DATE = date(2025, 10, 31)
ISSUE = date(2025, 10, 31)
QUALIFICATION = {"sha256": "sha256:" + "a" * 64, "url": "https://fiscaldata.treasury.gov/fixture.pdf"}


class Reader:
    def __init__(self, dataset, when, rows, *, revision="first"):
        self.body = json.dumps(rows).encode()
        digest = hashlib.sha256(self.body + revision.encode()).hexdigest()
        self.pin = ArtifactPin("urn:test:treasury:" + digest, "sha256:" + digest)
        self.records = [{
            "sourceRecordId": dataset + ":" + when.isoformat() + ":" + str(index),
            "record": {
                "dataset": dataset, "selection": {"dataset": dataset, "record_date": when.isoformat(), "page_size": 50},
                "raw": raw, "field_metadata": {"labels": {"record_date": "Record Date"},
                                             "dataTypes": {"record_date": "DATE"}, "dataFormats": {}},
                "source_locator": {"request_url": "https://api.fiscaldata.treasury.gov/fixture",
                                   "response_sha256": "sha256:" + hashlib.sha256(self.body).hexdigest(),
                                   "observed_at": "2026-10-20T00:00:00Z", "byte_size": len(self.body),
                                   "json_pointer": f"/data/{index}"},
            },
        } for index, raw in enumerate(rows)]

    def iter_records(self):
        yield from deepcopy(self.records)

    def iter_record_evidence(self):
        for item in self.records:
            yield {"sourceRecordId": item["sourceRecordId"],
                   "evidenceBlobRef": "sha256:" + hashlib.sha256(self.body).hexdigest()}

    def read_evidence(self, ref, *, max_bytes):
        assert len(self.body) <= max_bytes
        return self.body


def debt():
    return Reader("debt_to_penny", DEBT_DATE, [{
        "record_date": DEBT_DATE.isoformat(), "tot_pub_debt_out_amt": "3.03",
        "debt_held_public_amt": "2.02", "intragov_hold_amt": "1.01",
    }])


def statement(when=ISSUE, *, fiscal_year=2026, receipt="1.01", revision="first"):
    parent = {
        "record_date": when.isoformat(), "classification_id": "header", "parent_id": "null",
        "classification_desc": f"FY {fiscal_year}", "record_fiscal_year": str(when.year + (when.month >= 10)),
        "table_nbr": "1", "data_type_cd": "S", "record_type_cd": "SL", "sequence_number_cd": "1",
        **dict.fromkeys(module.BUDGET_FIELDS.values(), "null"),
    }
    rows, months = [parent], []
    for month in (*range(10, 13), *range(1, 10)):
        if date(fiscal_year - (month >= 10), month, 1) <= when:
            months.append(month)
    for index, month in enumerate(months, 1):
        rows.append(parent | {
            "classification_id": str(index), "parent_id": "header",
            "classification_desc": next(name for name, number in module._MONTHS.items() if number == month),
            "data_type_cd": "D", "record_type_cd": "MTH", "sequence_number_cd": f"1.{index}",
            "current_month_gross_rcpt_amt": receipt, "current_month_gross_outly_amt": "2.02",
            "current_month_dfct_sur_amt": str(Decimal("2.02") - Decimal(receipt)),
        })
    rows.append(parent | {
        "classification_id": "ytd", "parent_id": "header", "classification_desc": "Year-to-Date",
        "data_type_cd": "T", "sequence_number_cd": f"1.{len(months) + 1}",
        **{field: str(sum(Decimal(row[field]) for row in rows[1:])) for field in module.BUDGET_FIELDS.values()},
    })
    return Reader("mts_table_1", when, rows, revision=revision)


def control(when=ISSUE):
    return Reader("mts_table_2", when, [{"record_date": when.isoformat(), "classification_id": "forecast",
                                       "current_year_budget_est_amt": "0.00"}])


def write(readers, tmp_path, issues=(ISSUE,)):
    return module.build_treasury_fiscal(
        readers, tmp_path, debt_dates=(DEBT_DATE,), report_dates=issues,
        qualifications={issue: QUALIFICATION for issue in issues},
    )


@pytest.mark.parametrize(("raw", "status"), [(None, "json_null"), ("null", "publisher_null"), ("", "blank")])
def test_literal_nulls_exact_decimals_and_raw_context(tmp_path, raw, status):
    source = statement()
    source.records[1]["record"]["raw"]["current_month_gross_rcpt_amt"] = raw
    source.records[-1]["record"]["raw"]["current_month_gross_rcpt_amt"] = raw
    write([debt(), source, control()], tmp_path)
    row = pq.read_table(tmp_path / (module.STATEMENTS + ".parquet")).to_pylist()[0]
    assert row["budget_receipts_usd"] is None
    assert row["budget_outlays_usd"] == Decimal("2.02")
    assert json.loads(row["amount_statuses_json"])["budget_receipts_usd"] == status
    assert json.loads(row["raw_row_json"])["current_month_gross_rcpt_amt"] == raw
    assert json.loads(row["parent_context_json"])["record"]["raw"]["classification_desc"] == "FY 2026"
    assert row["source_release_digest"] == source.pin.artifact_digest
    assert row["qualification_report_sha256"] == QUALIFICATION["sha256"]
    assert pq.read_table(tmp_path / (module.DEBT + ".parquet")).to_pylist()[0]["total_debt_usd"] == Decimal("3.03")


def test_parent_fiscal_year_and_sequence_stay_distinct_from_report_context(tmp_path):
    issue = date(2026, 9, 30)
    write([debt(), statement(issue, fiscal_year=2025), control(issue)], tmp_path, (issue,))
    rows = pq.read_table(tmp_path / (module.STATEMENTS + ".parquet")).to_pylist()
    july, = [row for row in rows if row["classification_desc"] == "July"]
    assert july["sequence_number_cd_literal"] == "1.10"
    assert july["economic_fiscal_year"] == 2025 and july["period_start"] == date(2025, 7, 1)
    assert july["report_date"] == issue
    assert json.loads(july["raw_row_json"])["record_fiscal_year"] == "2026"
    assert rows[-1]["period_kind"] == "fiscal_year_to_date"
    assert rows[-1]["period_start"] == date(2024, 10, 1) and rows[-1]["period_end"] == date(2025, 9, 30)


@pytest.mark.parametrize("same_issue", [False, True])
def test_issue_and_same_issue_revisions_preserve_both_observations(tmp_path, same_issue):
    later = ISSUE if same_issue else date(2025, 11, 30)
    first, revised = statement(), statement(later, receipt="1.11", revision="revised")
    readers = [debt(), first, revised, control()]
    if not same_issue:
        readers.append(control(later))
    write(readers, tmp_path, tuple(sorted({ISSUE, later})))
    rows = pq.read_table(tmp_path / (module.STATEMENTS + ".parquet")).to_pylist()
    october = [row for row in rows if row["classification_desc"] == "October"]
    assert [row["budget_receipts_usd"] for row in october] == [Decimal("1.01"), Decimal("1.11")]
    assert [row["report_date"] for row in october] == [ISSUE, later]
    assert len({row["source_release_digest"] for row in october}) == 2


@pytest.mark.parametrize("defect", ["period", "parent", "date", "controls", "decimal", "deficit", "debt_total"])
def test_refusal_preserves_prior_tables(tmp_path, defect):
    write([debt(), statement(), control()], tmp_path)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    source, debt_reader = statement(), debt()
    readers = [debt_reader, source, control()]
    if defect == "period":
        source.records.pop(1)
    elif defect == "parent":
        source.records.pop(0)
    elif defect == "date":
        source.records[-1]["record"]["raw"]["record_date"] = "2025-11-30"
    elif defect == "controls":
        readers.pop()
    elif defect == "decimal":
        source.records[1]["record"]["raw"]["current_month_gross_rcpt_amt"] = "1.001"
    elif defect == "deficit":
        source.records[1]["record"]["raw"]["current_month_dfct_sur_amt"] = "-1.01"
    else:
        debt_reader.records[0]["record"]["raw"]["tot_pub_debt_out_amt"] = "3.00"
    with pytest.raises(ValueError):
        write(readers, tmp_path)
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


def test_decimal_arithmetic_keeps_cents_beyond_default_context_precision(tmp_path):
    source = debt()
    source.records[0]["record"]["raw"].update({
        "tot_pub_debt_out_amt": "9" * 36 + ".99", "debt_held_public_amt": "9" * 36 + ".98",
        "intragov_hold_amt": "0.01",
    })
    write([source, statement(), control()], tmp_path)
    row = pq.read_table(tmp_path / (module.DEBT + ".parquet")).to_pylist()[0]
    assert row["total_debt_usd"] == Decimal("9" * 36 + ".99")
    assert row["public_debt_usd"] == Decimal("9" * 36 + ".98")


def test_changed_report_control_refuses_before_writing_tables(tmp_path):
    report = tmp_path / "report.pdf"
    report.write_bytes(b"changed control")
    output = tmp_path / "build"
    with pytest.raises(ValueError, match="report control"):
        pipeline.build([debt(), statement(), control()], output, debt_dates=(DEBT_DATE,), report_dates=(ISSUE,),
                       reports={ISSUE: QUALIFICATION | {"path": str(report)}})
    assert not (output / "generation").exists()
    assert not list(output.glob("*.parquet"))


def test_ordinary_generation_publication_and_local_query(tmp_path, monkeypatch):
    report = tmp_path / "control.pdf"
    report.write_bytes(b"%PDF synthetic report control")
    qualified = QUALIFICATION | {"path": str(report), "sha256": "sha256:" + hashlib.sha256(report.read_bytes()).hexdigest()}
    output = tmp_path / "build"
    artifact, evidence = pipeline.build(
        [debt(), statement(), control()], output, debt_dates=(DEBT_DATE,), report_dates=(ISSUE,),
        reports={ISSUE: qualified},
    )
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture", prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["treasury-fiscal"]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        values = connection.execute(f"""
            SELECT report_date, period_start, budget_receipts_usd, budget_outlays_usd, budget_deficit_usd,
                   json_extract_string(source_locator_json, '$.request_url')
            FROM {module.STATEMENTS} WHERE period_kind='month'
        """).fetchall()
    assert values == [(ISSUE, date(2025, 10, 1), Decimal("1.01"), Decimal("2.02"), Decimal("1.01"),
                       "https://api.fiscaldata.treasury.gov/fixture")]
    for name, schema in module.SCHEMAS.items():
        assert pq.read_schema(output / (name + ".parquet")).equals(schema)
