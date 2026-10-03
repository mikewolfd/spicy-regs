"""``html_text``: a Regulations.gov comment body (an HTML fragment) read as plain text, once."""

import duckdb
import pytest

from spicy_regs.transforms.html_text import html_text, register_html_text


@pytest.mark.parametrize(("body", "text"), [
    # FDA's form-letter family as the publisher serves it.
    ("I&#39;m writing as a responsible tobacco retailer.<br/><br/>Thank you.",
     "I'm writing as a responsible tobacco retailer.\n\nThank you."),
    ("Tom &amp; Jerry &quot;quoted&quot; &rsquo;s &mdash; end", "Tom & Jerry \"quoted\" ’s — end"),
    ("<p>one</p><p>two</p>", "one\ntwo"),
    ("<ul><li>a</li><li>b</li></ul>", "a\nb"),
    ("a<hr>b", "a\nb"),
    ("see <a href=\"https://example.gov\">the rule</a>", "see the rule"),
    ("a&nbsp;b&nbsp;", "a b"),
    # One decode only: what the publisher escaped twice keeps one level, and typed markup stays text.
    ("&amp;#39;", "&#39;"),
    ("&lt;br&gt; is a tag", "<br> is a tag"),
    ("a < b and c > d", "a < b and c > d"),
    ("  plain text, nothing to parse  ", "plain text, nothing to parse"),
    ("", ""),
])
def test_the_body_reads_as_its_text(body, text):
    assert html_text(body) == text


def test_null_stays_null():
    assert html_text(None) is None


def test_the_sql_function_is_the_python_one_with_null_passed_through():
    with duckdb.connect() as con:
        register_html_text(con)
        rows = con.execute("SELECT html_text(b) FROM (VALUES ('I&#39;m<br>here'), (NULL), ('')) t(b)").fetchall()
    assert rows == [("I'm\nhere",), (None,), ("",)]
