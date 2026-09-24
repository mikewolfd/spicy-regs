"""The day rule and the narrowed Parquet reader the rulemaking builders share."""

from __future__ import annotations

import pytest

from spicy_regs.ontology.common import eastern_day, eastern_day_text, iter_parquet_rows, write_parquet_rows


@pytest.mark.parametrize(
    "stamp,day",
    [
        ("2016-11-26T04:59:59Z", "2016-11-25"),  # 11:59:59 PM EST closes the 25th
        ("2018-09-23T03:59:59Z", "2018-09-22"),  # 11:59:59 PM EDT closes the 22nd
        ("2016-09-27T04:00:00Z", "2016-09-27"),  # Eastern midnight opens the 27th
        ("2020-01-06T00:00:00Z", "2020-01-06"),  # a bare UTC midnight is a date-only value
        ("2022-01-14", "2022-01-14"),
        ("2024-03-10T06:30:00Z", "2024-03-10"),  # the spring-forward night is still the 10th
        ("2024-03-10T04:30:00Z", "2024-03-09"),  # and half an hour earlier, the 9th under EST
        ("0000-12-30T00:00:00Z", None),  # Regulations.gov's placeholder (8 documents) is no day
        ("", None),
        (None, None),
        ("not a date", None),
    ],
)
def test_regulations_gov_comment_instants_are_eastern_days(stamp, day):
    result = eastern_day(stamp)
    assert (result.isoformat() if result else None) == day
    assert eastern_day_text(stamp) == day


def test_a_narrowed_read_leaves_out_a_column_the_file_lacks(tmp_path):
    path = tmp_path / "rows.parquet"
    write_parquet_rows(path, columns=("a", "b", "c"), rows=[{"a": "1", "b": "2", "c": "3"}])

    assert list(iter_parquet_rows(path, columns=("a", "missing"))) == [{"a": "1"}]
    assert list(iter_parquet_rows(path, columns=("missing",))) == [{}]
    assert list(iter_parquet_rows(path)) == [{"a": "1", "b": "2", "c": "3"}]
