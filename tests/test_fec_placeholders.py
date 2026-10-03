"""The FEC placeholder predicate reads the same in Python and in DuckDB, on FEC's own spellings."""

from __future__ import annotations

import duckdb
import pytest

from spicy_regs.transforms.fec_placeholders import not_stated, not_stated_sql

#: Every placeholder spelling FEC committee history held on 2026-10-03, and stated names that resemble one.
PLACEHOLDERS = ["NONE", "None", "none", "None.", '"NONE"', "NONE ", "BLANK", "(BLANK)", "N/A", "NA", "N A", "-", ".",
                "......................................", "/", "$", "\\", "0", "", "   ", None]
STATED = ["SAME", "DELTA AIRLINES", "NONEXISTENT LLC", "N.A. HOLDINGS", "0 GRAVITY INC", "DRIVE", "NATIONAL ASSN"]


@pytest.mark.parametrize("value", PLACEHOLDERS + STATED)
def test_python_and_sql_agree(value: str | None) -> None:
    with duckdb.connect() as con:
        row = con.execute(f"SELECT {not_stated_sql('$value')}", {"value": value}).fetchone()
    assert row is not None and row[0] == not_stated(value) == (value in PLACEHOLDERS)
