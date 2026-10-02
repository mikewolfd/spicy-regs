"""Execute the documented consumer SQL against fixed calendar counterexamples."""

import re

import duckdb
import pytest

from spicy_regs.data_dictionary import load_descriptions


@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles"])
def test_documented_calendar_sql_preserves_source_convention_and_unknowns(zone):
    # Eastern calendar expectations from the October 2 independent deadline audit.
    # The source-specific bare-midnight convention is deliberately different from
    # an explicit UTC offset. Unsupported offset-free forms stay unknown here.
    cases = [
        ("region5", "2026-10-05T03:59:59Z", "2026-10-04"),
        ("cache", "2026-10-16T03:59:59Z", "2026-10-15"),
        ("npl", "2026-10-25T03:59:59Z", "2026-10-24"),
        ("winter", "2026-11-24T04:59:59Z", "2026-11-23"),
        ("first", "2026-10-03T03:59:59Z", "2026-10-02"),
        ("last", "2026-10-17T03:59:59Z", "2026-10-16"),
        ("outside", "2026-10-18T03:59:59Z", "2026-10-17"),
        ("before", "2026-10-02T03:59:59Z", "2026-10-01"),
        ("date", "2026-10-09", "2026-10-09"),
        ("bare", "2026-10-09T00:00:00Z", "2026-10-09"),
        ("offset", "2026-10-09T00:00:00+00:00", "2026-10-08"),
        ("negative", "2026-10-09T23:59:59-04:00", "2026-10-09"),
        ("trimmed", " 2026-10-09 ", "2026-10-09"),
        ("dst", "2026-03-08T07:30:00Z", "2026-03-08"),
        ("missing", None, None), ("empty", "", None), ("invalid", "bad", None),
        ("bad_day", "2026-02-30", None), ("year_zero", "0000-12-30T00:00:00Z", None),
        ("offset_free", "2026-10-09T23:59:59", None),
    ]
    prose = load_descriptions()["documents"]["data_quality"]
    match = re.search(r"```sql\n(.*?)\n```", prose, re.S)
    assert match is not None, "Keep the documented executable example under regression coverage"
    sql = match.group(1)
    cte, _ = sql.split("\nSELECT document_id, end_day FROM calendar", 1)
    with duckdb.connect(config={"enable_external_access": False, "threads": 1}) as con:
        con.execute("SET TimeZone = ?", [zone])
        con.execute("CREATE TABLE documents(document_id VARCHAR, comment_end_date VARCHAR)")
        con.executemany("INSERT INTO documents VALUES (?, ?)", [(key, raw) for key, raw, _ in cases])
        actual = {key: str(day) if day is not None else None
                  for key, day in con.execute(cte + " SELECT document_id, end_day FROM calendar").fetchall()}
        assert actual == {key: day for key, _, day in cases}
        assert {key for key, _ in con.execute(sql).fetchall()} == {
            "region5", "cache", "first", "last", "date", "bare", "offset", "negative", "trimmed",
        }
