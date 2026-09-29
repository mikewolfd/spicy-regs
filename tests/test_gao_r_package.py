"""The R package's rows as gao_reports rows, the one-time copy's refusals, and the lowest route's precedence.

The package rows below are shaped like CetiAlphaFive/gao's ``gao_links`` (commit 6f61230), read 2026-09-28: R's
``NA`` as None or NaN, empty strings for unstated text, and a percent-encoded page for a joint number.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import gao_r_package as package
from tests.test_gao_reports import _feed_row, _run, module

_spec = importlib.util.spec_from_file_location(
    "import_gao_r_package", Path(__file__).parents[1] / "scripts" / "import_gao_r_package.py"
)
assert _spec and _spec.loader
importer = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(importer)


def _record(url: str, number: str, **fields) -> dict:
    return {"url": url, "report_id": number, "title": f"Title {number}", "released": "1984-01-04",
            "published": "1983-12-20", "summary": "", "topics": "", "agencies_affected": float("nan"),
            "requester_type": "congressional_request", **fields}


def _shaped(record: dict) -> dict:
    row = package.shape(record)
    assert row is not None
    return row


def test_a_package_row_is_keyed_on_its_page_and_states_nothing_it_leaves_empty():
    row = _shaped(_record("https://www.gao.gov/products/14508%2C-14803", "A-14508,A-14803",
                          summary="A summary.", topics="Human Capital",
                          agencies_affected="Department of Labor; Architect of the Capitol",
                          requester_members="Charles H. Percy (Senate); Ernest F . Hollings (Senate)",
                          requester_committees=None, n_recommendations=2, n_matters=0, page_count=19,
                          subject_terms="Cost control; Financial management"))
    assert row == {
        "report_id": "14508,-14803", "title": "Title A-14508,A-14803", "report_type": "Report",
        "published_date": "1984-01-04", "abstract": "A summary.",
        "agencies_json": json.dumps(["Department of Labor", "Architect of the Capitol"]),
        "topics_json": '["Human Capital"]', "url": "https://www.gao.gov/products/14508%2C-14803",
        "source": "gao_r_package", "product_type": None, "report_number": "A-14508,A-14803",
        "requester_type": "congressional_request", "requester_committees_json": None,
        "requester_members_json": '["Charles H. Percy (Senate)", "Ernest F . Hollings (Senate)"]',
        "recommendation_count": 2, "matters_for_congress_count": 0, "page_count": 19,
        "subject_terms_json": '["Cost control", "Financial management"]',
    }
    empty = _shaped(_record("https://www.gao.gov/products/t-ggd-99-93", "T-GGD-99-93", released=""))
    assert (empty["abstract"], empty["topics_json"], empty["agencies_json"]) == (None, None, None)
    assert (empty["report_type"], empty["published_date"]) == ("Testimony", "1983-12-20")


@pytest.mark.parametrize("fields", [{"requester_type": "legal_decision"}, {}])
def test_a_legal_decision_is_left_out(fields):
    assert package.shape(_record("https://www.gao.gov/products/b-285725-0", "B-285725", **fields)) is None


def test_a_twin_page_of_a_held_product_with_its_number_is_that_product():
    held = {"gao-16-75sp": {"report_number": "GAO-16-75SP"}, "gao-14-280r": {"report_number": "GAO-14-280R"}}
    twin = _shaped(_record("https://www.gao.gov/products/gao-16-75sp-0", "GAO-16-75SP"))
    other = _shaped(_record("https://www.gao.gov/products/gao-14-280r-0", "GAO-99-1R"))
    assert package.held_as(twin, held) == "gao-16-75sp" and package.held_as(other, held) is None


def test_a_page_listed_twice_refuses():
    with pytest.raises(package.GaoRPackageError, match="twice"):
        package.package_rows([_record("https://www.gao.gov/products/24669", "A-24669")] * 2)


def test_only_the_reviewed_conversion_is_read(tmp_path):
    other = tmp_path / "gao_links.parquet"
    pq.write_table(pa.table({"url": ["x"]}), other)
    with pytest.raises(package.GaoRPackageError, match="not the reviewed conversion"):
        package.read_package(other)


def _prior(tmp_path, *, columns: int | None = None) -> Path:
    """A live table holding one listing row; ``columns`` keeps only the first N, as a table published before these."""
    listed = {**_feed_row("gao-16-75sp"), "abstract": None, "source": "gao_listing", "product_type": None,
              "report_number": "GAO-16-75SP"}
    table = pa.Table.from_pylist([listed], schema=module._SCHEMA)
    path = tmp_path / "prior.parquet"
    pq.write_table(table.select(list(module.COLUMNS[:columns])) if columns else table, path)
    return path


def test_a_live_table_without_the_package_columns_gains_them_and_changes_no_cell(tmp_path):
    """The 11-column live table: the copy appends the new columns, NULL on held rows unless the package fills them."""
    records = [_record("https://www.gao.gov/products/gao-16-75sp", "GAO-16-75SP", page_count=7),
               _record("https://www.gao.gov/products/24669", "A-24669")]
    out = tmp_path / "out.parquet"
    added = [_shaped(records[1])]
    importer.import_package_rows(_prior(tmp_path, columns=11), records, out, rows_sha256=importer.rows_digest(added))
    assert pq.read_schema(out).names == list(module.COLUMNS)
    rows = {r["report_id"]: r for r in pq.read_table(out).to_pylist()}
    assert (rows["gao-16-75sp"]["page_count"], rows["gao-16-75sp"]["title"]) == (7, "Feed gao-16-75sp")


def test_the_copy_adds_only_what_no_row_holds_and_changes_no_held_cell(tmp_path):
    records = [
        _record("https://www.gao.gov/products/gao-16-75sp", "GAO-16-75SP", summary="Package summary."),
        _record("https://www.gao.gov/products/gao-16-75sp-0", "GAO-16-75SP"),
        _record("https://www.gao.gov/products/24669", "A-24669"),
        _record("https://www.gao.gov/products/b-285725-0", "B-285725", requester_type="legal_decision"),
    ]
    added = [_shaped(records[2])]
    out = tmp_path / "out.parquet"
    report = importer.import_package_rows(_prior(tmp_path), records, out, rows_sha256=importer.rows_digest(added),
                                          fill_nulls=False)
    rows = {row["report_id"]: row for row in pq.read_table(out).to_pylist()}
    assert set(rows) == {"gao-16-75sp", "24669"} and rows["gao-16-75sp"]["abstract"] is None
    assert (report["added"], report["held"], report["held_as_twin"], report["legal_decisions_left_out"]) == (1, 2, 1, 1)
    assert report["null_fills_available"] == {"gao_listing.abstract": 1, "gao_listing.requester_type": 1}

    filled = tmp_path / "filled.parquet"
    importer.import_package_rows(_prior(tmp_path), records, filled, rows_sha256=importer.rows_digest(added),
                                 fill_nulls=True)
    row = {r["report_id"]: r for r in pq.read_table(filled).to_pylist()}["gao-16-75sp"]
    assert (row["abstract"], row["title"], row["source"]) == ("Package summary.", "Feed gao-16-75sp", "gao_listing")
    assert (row["requester_type"], row["report_number"]) == ("congressional_request", "GAO-16-75SP")


def test_any_other_set_of_additions_refuses(tmp_path):
    with pytest.raises(importer.ImportRefused, match="differ from the reviewed rows"):
        importer.import_package_rows(_prior(tmp_path), [_record("https://www.gao.gov/products/24669", "A-24669")],
                                     tmp_path / "out.parquet", rows_sha256="sha256:0")


def test_a_route_of_ours_takes_a_package_row_over_and_the_package_fills_its_nulls(tmp_path, monkeypatch):
    from tests.test_gao_listing import _product

    copied = {**_feed_row("gao-26-9"), "title": "Package title", "abstract": "Package summary.",
              "agencies_json": '["Department of Labor"]', "topics_json": '["Human Capital"]',
              "source": "gao_r_package", "report_number": "GAO-26-9"}
    fed = {**_feed_row("gao-26-8"), "abstract": "Package abstract.", "source": "gao_r_package", "report_number": None}
    prior = pa.Table.from_pylist([copied, fed], schema=module._SCHEMA)
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-8"], listed=[_product("gao-26-9")])
    listed = rows["gao-26-9"]
    assert (listed["source"], listed["title"], listed["topics_json"]) == ("gao_listing", "Label: Heading of gao-26-9",
                                                                        '["Education"]')
    assert (listed["abstract"], listed["agencies_json"]) == ("Package summary.", '["Department of Labor"]')
    assert (rows["gao-26-8"]["source"], rows["gao-26-8"]["abstract"]) == ("gao_rss", "What GAO Found.")


def test_evidence_retains_a_local_input_file(tmp_path):
    from spicy_regs.source_evidence import CaptureEvidence

    source = tmp_path / "gao_links.parquet"
    source.write_bytes(b"exact bytes")
    evidence = CaptureEvidence(tmp_path, "gao-reports")
    blob = evidence.retain_file(source, stage="gao-r-package", license="GPL-3.0-or-later")
    assert blob == {"sha256": "sha256:" + __import__("hashlib").sha256(b"exact bytes").hexdigest(), "byte_size": 11}
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    assert journal[-1]["event"] == "retained-file" and journal[-1]["license"] == "GPL-3.0-or-later"
