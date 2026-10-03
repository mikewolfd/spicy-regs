"""Plain text from a Regulations.gov comment body, which the publisher serves as an HTML fragment.

The publisher escapes every ``&`` (none bare in FDA's 1,809,176 or FTC's 228,956 bodies, 2026-10-03), writes
apostrophes as ``&#39;`` and breaks lines with ``<br/>``, so ``comments.comment`` is HTML and a ``LIKE '%I''m%'``
misses ``I&#39;m``. :func:`html_text` is what a reader sees: one decode of every character reference
(``html.parser`` with ``convert_charrefs``), ``<br>`` and the end of each block element as a newline, a no-break space
as a space, tags dropped. One decode only: a double-escaped ``&amp;#39;`` (2,176 FDA bodies) stays ``&#39;``, as the
publisher's own page shows it, and an escaped tag (``&lt;br&gt;``, 213 FDA bodies) stays the text ``<br>``. Links lose
their targets and lists their bullets. Standard library only: the export registers it as a DuckDB function.
"""

from __future__ import annotations

from html.parser import HTMLParser

import pyarrow as pa

#: Void elements that break a line where they stand.
LINE_BREAKS = frozenset({"br", "hr"})
#: Elements whose end starts a new line: the block elements a comment body can hold.
BLOCK_ELEMENTS = frozenset({
    "address", "article", "blockquote", "dd", "div", "dl", "dt", "footer", "h1", "h2", "h3", "h4", "h5", "h6",
    "header", "li", "ol", "p", "pre", "section", "table", "tr", "ul",
})

#: The name the export calls it by in SQL.
SQL_NAME = "html_text"


class _TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in LINE_BREAKS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in BLOCK_ELEMENTS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def html_text(value: str | None) -> str | None:
    """The fragment's text: references decoded once, line-breaking markup as newlines, other tags dropped.

    ``None`` stays ``None``. A value with no ``<`` or ``&`` has nothing to parse and is only trimmed, like every other.
    """
    if value is None:
        return None
    if "<" in value or "&" in value:
        collector = _TextCollector()
        collector.feed(value)
        collector.close()
        value = "".join(collector.parts)
    return value.replace("\xa0", " ").strip()


def _arrow_html_text(values: pa.Array | pa.ChunkedArray) -> pa.Array:
    return pa.array([html_text(value) for value in values.to_pylist()], type=pa.string())


def register_html_text(con) -> None:
    """Make :func:`html_text` callable as ``html_text(VARCHAR)`` on a DuckDB connection, NULL passed through."""
    con.create_function(SQL_NAME, _arrow_html_text, ["VARCHAR"], "VARCHAR", type="arrow", null_handling="special",
                        side_effects=False)
