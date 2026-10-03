"""Readback accepts the hosted API's positional rows and preserves exact cells."""

import pytest

from readback_hrc_mcp import query_rows


@pytest.mark.parametrize("rows", [[["N/A", None]], [{"value_text": "N/A", "value_number": None}]])
def test_query_row_representations_preserve_values(rows):
    body = {"columns": ["value_text", "value_number"], "rows": rows}
    assert query_rows(body) == [{"value_text": "N/A", "value_number": None}]
    assert body["rows"] is rows


@pytest.mark.parametrize(
    "body",
    [
        {"columns": ["value", "value"], "rows": [["N/A", None]]},
        {"columns": ["value"], "rows": [["N/A", None]]},
        {"columns": ["value"], "rows": [{"other": "N/A"}]},
    ],
)
def test_query_readback_refuses_ambiguous_or_mismatched_columns(body):
    with pytest.raises(ValueError):
        query_rows(body)
