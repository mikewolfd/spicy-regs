"""The release-time list of status columns that hold one value while their description does not say so (round 6, M1).

Built on local Parquet files, so the footer reading and the "says so" rule are tested without the publisher.
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import check_status_constants as constants


def _metadata(**descriptions: str) -> dict:
    return {"t": {"columns": [{"column_name": name, "description": text} for name, text in descriptions.items()]}}


def _file(tmp_path, columns: dict, **options) -> list[str]:
    path = tmp_path / "t.parquet"
    pq.write_table(pa.table(columns), path, row_group_size=2, **options)
    return [str(path)]


def test_a_constant_status_whose_description_does_not_say_so_is_listed(tmp_path):
    """filing_link_status was `unresolved` on every row of 27 FEC tables while its text described an outcome."""
    urls = _file(tmp_path, {
        "filing_link_status": ["unresolved"] * 5,
        "loan_link_status": ["unresolved"] * 5,
        "mapping_status": ["mapped", "mapped", "partial", "mapped", "mapped"],
        "amount": ["1", "1", "1", "1", "1"],
    })
    metadata = _metadata(
        filing_link_status="Status of the relationship to a submitted filing version.",
        loan_link_status="Always `unresolved`: set when the row is mapped.",
        mapping_status="Whether the row mapped.",
        amount="A constant that is not a status.",
    )
    listed = constants.check(duckdb.connect(), metadata, {"t": urls}.get)
    assert [(item["column"], item["value"], item["named"]) for item in listed] == [
        ("filing_link_status", "unresolved", False)]


def test_naming_the_value_is_not_saying_it_is_the_only_one(tmp_path):
    urls = _file(tmp_path, {"read_status": ["complete"] * 4})
    listed = constants.check(duckdb.connect(), _metadata(read_status="`complete` or `refused`."), {"t": urls}.get)
    assert [(item["column"], item["named"]) for item in listed] == [("read_status", True)]


def test_a_column_whose_footer_states_no_statistics_cannot_be_judged(tmp_path):
    urls = _file(tmp_path, {"x_status": ["same"] * 4}, write_statistics=False)
    assert constants.check(duckdb.connect(), _metadata(x_status="A status."), {"t": urls}.get) == []


def test_an_unpublished_table_is_skipped(tmp_path):
    assert constants.check(duckdb.connect(), _metadata(x_status="A status."), {}.get) == []


def test_the_report_states_one_line_per_column_and_value_across_tables():
    listed = [
        {"table": table, "column": "current_record_status", "value": "unqualified", "nulls": 0, "rows": 10,
         "named": False} for table in ("fec_b", "fec_a")
    ]
    assert constants.report_lines(listed) == [
        "UNSTATED  current_record_status = 'unqualified' (value not named) on 2 tables: fec_a, fec_b"]
