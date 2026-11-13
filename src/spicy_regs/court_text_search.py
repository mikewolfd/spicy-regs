"""Search opinion Markdown as visible text without another stored body or full-text index.

The body dialect is GFM plus footnotes. Marko already qualifies that dialect in
SpicyDocs' fidelity checks. A regex over Markdown would include link destinations,
miss phrases split by emphasis or hard breaks, and consume footnote definitions
as CommonMark link definitions. Keep the original Markdown for retrieval; this
function produces whitespace-normalized text only while a query runs.
"""

from __future__ import annotations

import html

import marko
import pyarrow as pa

SQL_NAME = "court_opinion_search_text"


def _visible(element) -> str:
    name = type(element).__name__
    if name == "LineBreak":
        return " "
    if name == "FootnoteRef":
        return ""
    if name == "InlineHTML" and element.children in {"<em>", "</em>", "<strong>", "</strong>"}:
        return ""  # Converter 003's exact GFM emphasis spans are formatting.
    children = getattr(element, "children", "")
    if isinstance(children, str):
        return html.unescape(children)
    text = "".join(_visible(child) for child in children)
    # Inline links/emphasis concatenate; blocks and table cells separate words.
    return text + (" " if isinstance(element, marko.block.BlockElement) else "")


def court_opinion_search_text(value: str | None) -> str | None:
    """Visible text with whitespace folded; NULL and empty stay distinct.

    Link labels, repeated citations, footnote bodies and literal escaped text
    survive. Destinations, footnote markers and formatting do not become search
    terms. Matching policy (literal phrase, case, court/date filters) stays SQL.
    """
    if value is None:
        return None
    parser = marko.Markdown(extensions=["gfm", "footnote"])
    return " ".join(_visible(parser.parse(value)).split())


def register_court_text_search(connection) -> None:
    """Expose the same function in ordinary query_sql connections and local DuckDB."""
    def batch(values: pa.Array | pa.ChunkedArray) -> pa.Array:
        # Each call owns its parser: Marko maintains per-document parser state.
        parser = marko.Markdown(extensions=["gfm", "footnote"])
        return pa.array([None if value is None else " ".join(_visible(parser.parse(value)).split())
                         for value in values.to_pylist()], type=pa.string())

    connection.create_function(SQL_NAME, batch, ["VARCHAR"], "VARCHAR", type="arrow",
                               null_handling="special", side_effects=False)
