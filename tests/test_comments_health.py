"""Health checks must detect loss even when the latest month and total count agree."""

import duckdb
import pytest

from spicy_regs.comments_health import check_comments, check_retained_ids


@pytest.fixture
def con():
    with duckdb.connect() as connection:
        connection.execute('CREATE TABLE comments (comment_id VARCHAR, agency_code VARCHAR, docket_id VARCHAR, posted_date VARCHAR)')
        connection.execute('CREATE TABLE idx (agency_code VARCHAR, docket_id VARCHAR, year BIGINT, month BIGINT, row_count BIGINT)')
        yield connection


def test_duplicate_and_same_month_loss_are_both_reported(con):
    con.execute("INSERT INTO comments VALUES ('a','EPA','EPA-1','2026-09-01'), ('a','EPA','EPA-1','2026-09-01')")
    con.execute("INSERT INTO idx VALUES ('EPA','EPA-1',2026,9,100)")
    errors = check_comments(con, 'SELECT * FROM comments', 'SELECT * FROM idx')
    assert any('unique IDs' in e for e in errors)
    assert any('index=100, actual=2' in e for e in errors)


def test_full_coverage_includes_null_dates(con):
    con.execute("INSERT INTO comments VALUES ('a','EPA','EPA-1',NULL), ('b','EPA','EPA-2','2026-09-01')")
    con.execute("INSERT INTO idx VALUES ('EPA','EPA-1',NULL,NULL,1), ('EPA','EPA-2',2026,9,1)")
    assert check_comments(con, 'SELECT * FROM comments', 'SELECT * FROM idx') == []


def test_missing_docket_cannot_hide_behind_same_agency_total(con):
    con.execute("INSERT INTO comments VALUES ('a','EPA','EPA-1','2026-09-01'), ('b','EPA','EPA-1','2026-09-01')")
    con.execute("INSERT INTO idx VALUES ('EPA','EPA-1',2026,9,1), ('EPA','EPA-2',2026,9,1)")
    assert len(check_comments(con, 'SELECT * FROM comments', 'SELECT * FROM idx')) == 2


def test_extra_unindexed_rows_fail(con):
    con.execute("INSERT INTO comments VALUES ('a','EPA','EPA-1','2026-09-01')")
    assert check_comments(con, 'SELECT * FROM comments', 'SELECT * FROM idx')


def test_same_size_export_cannot_discard_old_ids(con):
    assert check_retained_ids(con, "SELECT 'old' AS comment_id", "SELECT 'new' AS comment_id")
    assert check_retained_ids(con, "SELECT 'old' AS comment_id", "SELECT * FROM (VALUES ('old'), ('new')) t(comment_id)") == []
