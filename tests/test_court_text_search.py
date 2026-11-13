"""Ordinary query_sql phrase searches must read the opinion's declared dialect."""

import asyncio

import duckdb
import marko
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server
from spicy_regs.court_text_search import court_opinion_search_text, register_court_text_search
from tests.test_mcp_server import _records, _tool_data


def test_visible_phrase_includes_citations_and_notes_but_not_destinations():
    markdown = ('A *due*\\\n&#32;process [claim](opinion:123)[^1].\n\n'
                '[^1]: Repeated [claim](cite:U.S./2/3), with **evidence**.\n')
    assert court_opinion_search_text(markdown) == 'A due process claim. Repeated claim, with evidence.'
    assert court_opinion_search_text(None) is None
    assert court_opinion_search_text('') == ''
    assert court_opinion_search_text('&amp;lt;literal&amp;gt;') == '&lt;literal&gt;'
    assert court_opinion_search_text('| a | b |\n| --- | --- |\n| c | d |') == 'a b c d'
    assert court_opinion_search_text('<em>“</em>as plaintiffs then') == '“as plaintiffs then'
    assert court_opinion_search_text('[373 <em>A.</em>2d](opinion:1) and <strong>\\*</strong>2') == '373 A.2d and *2'
    assert court_opinion_search_text('&lt;em&gt;literal&lt;/em&gt;') == '<em>literal</em>'


def test_arrow_nulls_and_request_cursor_see_the_registered_function():
    with duckdb.connect() as con:
        register_court_text_search(con)
        con.register('source', pa.table({'md': [None, '', '*due*\\\nprocess', '[123](opinion:42)']}))
        con.execute('CREATE TABLE bodies AS SELECT * FROM source')
        assert con.cursor().execute('SELECT court_opinion_search_text(md) FROM bodies').fetchall() == [
            (None,), ('',), ('due process',), ('123',)]


def test_actual_local_query_consumer_searches_and_retrieves_with_parent_selection(tmp_path, monkeypatch):
    # Two opinions share wording but only the first has a federal parent. A third
    # has unresolved jurisdiction; no-text remains a row, separate from a miss.
    pq.write_table(pa.table({
        'opinion_id': ['11', '12', '13', '14'], 'cluster_id': ['21', '22', '23', '21'],
        'opinion_block': ['0'] * 4, 'markdown': ['*due*\\\nprocess claim', 'due process', 'due process', None],
        'text_source': ['html', 'plain_text', 'html', 'none'],
    }), tmp_path / 'court_opinion_texts.parquet')
    pq.write_table(pa.table({'opinion_id': ['11', '12', '13', '14'], 'cluster_id': ['21', '22', '23', '21']}),
                   tmp_path / 'court_opinions.parquet')
    pq.write_table(pa.table({'cluster_id': ['21', '22', '23'], 'court_id': ['ca1', 'state', None],
                            'court_is_federal': [True, False, None]}), tmp_path / 'court_opinion_clusters.parquet')
    monkeypatch.setattr(mcp_server, 'DATA_DIR', tmp_path)
    parse = marko.Markdown.parse

    def parse_selected(self, text):
        assert text != 'due process', 'excluded state/unresolved bodies must not be parsed'
        return parse(self, text)

    monkeypatch.setattr(marko.Markdown, 'parse', parse_selected)
    mcp_server._reset_connection_cache()
    try:
        server = mcp_server.build_server()
        result = _tool_data(server, 'query_sql', {'sql': """
            SELECT t.opinion_id, t.cluster_id, c.court_id,
                   'https://www.courtlistener.com/opinion/' || c.cluster_id || '/' AS original_url,
                   court_opinion_search_text(t.markdown) AS passage
            FROM court_opinion_texts t
            JOIN court_opinions o ON t.opinion_id=o.opinion_id AND t.cluster_id=o.cluster_id
            JOIN court_opinion_clusters c ON o.cluster_id=c.cluster_id
            WHERE c.court_is_federal AND contains(lower(court_opinion_search_text(
                CASE WHEN c.court_is_federal THEN t.markdown END)), 'due process')
            LIMIT 5
        """})
        assert _records(result) == [{'opinion_id': '11', 'cluster_id': '21', 'court_id': 'ca1',
                                     'original_url': 'https://www.courtlistener.com/opinion/21/',
                                     'passage': 'due process claim'}]
        assert set(result['publication']) == {'court_opinion_texts', 'court_opinions', 'court_opinion_clusters'}
        monkeypatch.setattr(marko.Markdown, 'parse', parse)
        point = _tool_data(server, 'query_sql', {'sql': "SELECT markdown FROM court_opinion_texts "
                                                "WHERE opinion_block='0' AND opinion_id='11'"})
        assert _records(point) == [{'markdown': '*due*\\\nprocess claim'}]
        empty = _tool_data(server, 'query_sql', {'sql': "SELECT opinion_id FROM court_opinion_texts "
                                                "WHERE contains(court_opinion_search_text(markdown), 'absent phrase')"})
        assert empty['row_count_shown'] == 0
        with pytest.raises(ToolError, match='read-only'):
            asyncio.run(server.call_tool('query_sql', {'sql': 'DELETE FROM court_opinion_texts'}))
    finally:
        mcp_server._reset_connection_cache()
