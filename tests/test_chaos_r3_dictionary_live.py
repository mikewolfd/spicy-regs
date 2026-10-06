"""Live checks of what the dictionary declares against the publisher the output ledger names (round 3, scout A).

Integration tier (network): each test reads the public index or one small
Parquet file and holds a declared statement to the data, never the other way
round. Run with ``uv run --frozen pytest -m integration tests/test_chaos_r3_dictionary_live.py``.
"""

from __future__ import annotations

import re
from datetime import date

import duckdb
import pytest

from scripts import check_table_joins as live
from spicy_regs import data_dictionary as dd
from spicy_regs import output_ledger, table_joins
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication

pytestmark = pytest.mark.integration

ISO_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


@pytest.fixture(scope="module")
def base_url() -> str:
    return output_ledger.ledger_destination(output_ledger.LEDGER.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    load_public_http(connection)
    return connection


def _kinds() -> dict[str, str | None]:
    return {table: dd.coverage_kind(entry["coverage"]) for table, entry in dd.table_coverage(dd.load_descriptions()).items()}


def test_an_empty_kind_means_zero_published_rows_and_zero_rows_mean_empty(base_url):
    """A `sampled` table with 0 rows reads as a thin sample; an `empty` one with rows reads as a lie."""
    rows = dd.published_row_counts(base_url)
    assert rows, "the index names no tables"
    assert dd.kind_index_errors(dd.load_descriptions(), rows) == []


def test_a_table_points_at_its_receipt_only_where_the_index_shows_it_with_receipts(base_url):
    """A pointer on a table that has published no receipts sends a reader to query nothing (lobbying_filings, 2026-10-05)."""
    index = publication.load_index(base_url)
    with_receipts = dd.published_receipt_datasets(index)
    assert with_receipts, "the index names no receipts"
    published = dd.published_row_counts(base_url, index)
    assert dd.receipt_index_errors(dd.load_descriptions(), published, with_receipts) == []


def test_comments_coverage_states_no_end_bound_the_data_has_passed(base_url, con):
    """The comments mirror is rebuilt daily; a date in its coverage prose other than the stated floor must not lag the data."""
    entry = dd.table_coverage(dd.load_descriptions())["comments"]
    year, month = con.execute(
        f"SELECT year, month FROM read_parquet('{base_url}/comments_index.parquet') "
        "WHERE year IS NOT NULL AND year <= 2100 ORDER BY year DESC, month DESC LIMIT 1"
    ).fetchone()
    live_month = date(int(year), int(month), 1)
    stated = {date.fromisoformat(found) for found in ISO_DATE.findall(entry["coverage"])}
    lagging = sorted(d for d in stated if d > date(1990, 1, 1) and d < live_month)
    assert lagging == [], f"coverage states {lagging} as a bound; the data reaches {live_month:%Y-%m}"


def test_house_communications_is_a_window_held_whole(base_url, con, tmp_path):
    """Window means every listed communication is held, not a per-run slice: numbers contiguous from 1, no list-only row."""
    from spicy_regs.transforms.build_congress_index import INDEX_SPECS, _read_sql

    assert _kinds()["house_communications"] == "window"
    index = publication.load_index(base_url)
    owner = publication.table_owner(index, "house_communications.parquet")
    if owner is not None and "etlReceipts" in owner[1]:
        from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

        # Detail-read markers live in the paired receipt. Restore the captured pair through
        # the maintained processing reader before applying the original completeness rule.
        processing = SelectedPriors(tmp_path / "house-prior", index=index, public_url=base_url).get("house_communications")
        assert processing is not None
        scan = publication.parquet_scan([str(processing)])
    else:
        scan = publication.parquet_scan([f"{base_url}/{member.path}"
                                         for member in publication.table_members(index, "house_communications.parquet")])
    # A read detail that states no committees leaves committees_json NULL (spicy-docs 0.54.0); the run's own rule says.
    columns = {str(row[0]) for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()}
    read = _read_sql(columns, INDEX_SPECS["house_communications"].detail_marker or "")
    shape = con.execute(
        f"SELECT communication_type, count(*), count(DISTINCT number), min(CAST(number AS INT)), "
        f"max(CAST(number AS INT)), count_if(({read}) IS NOT TRUE) FROM {scan} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert shape, "no rows"
    for kind, rows, distinct, lowest, highest, list_only in shape:
        assert (rows, distinct, lowest, list_only) == (highest, highest, 1, 0), (kind, rows, distinct, lowest, highest, list_only)


def test_every_law_section_names_a_law_the_laws_table_holds(base_url, con):
    """The declaration says `complete`, one parent per key; the live family must agree."""
    (join,) = [join for join in table_joins.JOINS if join.name == "law_sections.law_id -> laws.law_id"]
    urls = publication.published_urls(base_url)
    result = live.measure(con, join, urls.__getitem__)
    assert result["status"] == "OK", result
    assert result["missing"] == 0 and result["max_parent_multiplicity"] == 1
