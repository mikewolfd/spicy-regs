"""The live enactment-date check excuses only a law whose latest action is recent; an unknown age is not excused."""
import duckdb
import pytest

from tests.test_chaos_r4_dictionary_live import OLDER_THAN_WINDOW, PENDING_DAYS


@pytest.mark.parametrize("latest_action, counted", [
    ("current_date - INTERVAL 1 DAY", False),
    (f"current_date - INTERVAL {PENDING_DAYS - 1} DAY", False),
    (f"current_date - INTERVAL {PENDING_DAYS + 1} DAY", True),
    ("NULL", True),
    ("'not a date'", True),
])
def test_only_a_recent_latest_action_excuses_an_undated_public_law(latest_action, counted):
    [(result,)] = duckdb.sql(
        f"SELECT count(*) FILTER (WHERE {OLDER_THAN_WINDOW}) FROM (SELECT ({latest_action})::VARCHAR AS latest_action_date) l"
    ).fetchall()
    assert result == int(counted)
