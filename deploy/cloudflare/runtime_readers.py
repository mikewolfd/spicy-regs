"""Exercise request-time native receipt selection using only the image's packages."""

from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import hashlib
import asyncio
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, select_receipts


def check_public_download():
    from spicy_regs.local_data import file_state_from_stat
    from spicy_regs.sources import publication

    data = b"checked runtime download"
    member = publication.Member("fixture.parquet", "sha256:" + hashlib.sha256(data).hexdigest(), len(data), 1)
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def iter_bytes(self): yield data
    with TemporaryDirectory() as temp, patch.object(publication.httpx, "stream", return_value=Response()):
        target = Path(temp) / "download.parquet"
        checked = publication.fetch_member("https://fixture.invalid", member, target)
        assert isinstance(checked, publication.DownloadedMember)
        assert checked.sha256 == member.sha256 and checked.byte_size == len(data)
        assert target.read_bytes() == data
        assert checked.state == file_state_from_stat(target.lstat())
    assert "rulespec_artifacts" not in sys.modules


def check_native_selection():
    # Selection must retain all outcomes, in file order, including failed
    # attempts without subjects. Validation belongs to the subsequent reader.
    rows = [
        {"dataset": "document_citations", "outcome": "accepted", "receipt_id": "first"},
        {"dataset": "other", "outcome": "accepted", "receipt_id": "unrelated"},
        {"dataset": "document_citations", "outcome": "error", "receipt_id": "failed"},
    ]
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source, selected = root / "shared.parquet", root / "selected.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), source)
        select_receipts(source, selected, dataset="document_citations")
        result = pq.read_table(selected)
        assert result.schema == RECEIPT_SCHEMA
        assert result.column("receipt_id").to_pylist() == ["first", "failed"]
        select_receipts(source, selected, dataset="absent")
        assert pq.read_table(selected).num_rows == 0
    assert "spicy_regs.transforms" not in sys.modules


def check_mcp_tools():
    """Use both ordinary connections with only the image's installed packages."""
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp.types import CallToolResult
    from spicy_regs import mcp_server

    async def exercise():
        server = mcp_server.build_server()
        async def data(name, arguments):
            result = await server.call_tool(name, arguments)
            assert isinstance(result, CallToolResult) and isinstance(result.structured_content, dict)
            return result.structured_content

        discovery = await data('list_sources', {})
        assert any('agency_stats' in item['tables'] for item in discovery['subjects'])
        sql = "SELECT agency_code, court_opinion_search_text('*due* [process](opinion:123)') AS passage FROM agency_stats LIMIT 2"
        arguments = {'sql': sql, 'max_rows': 2}
        first = await data('query_sql', arguments)
        assert first['rows'] == [['EPA', 'due process']] and not first['truncated']
        try:
            await server.call_tool('query_sql', {'sql': 'SELECT * FROM runtime_qualification_missing_table LIMIT 1'})
        except ToolError as error:
            assert 'runtime_qualification_missing_table' in str(error)
        else:
            raise AssertionError('Unknown-table query must remain a tool error')
        recovered = await data('query_sql', arguments)
        assert recovered['rows'] == first['rows']
        assert mcp_server._cached_discovery_connection is not None
        assert mcp_server._cached_connection is not None
        assert mcp_server._cached_discovery_connection is not mcp_server._cached_connection

    with TemporaryDirectory() as temp:
        root = Path(temp)
        pq.write_table(pa.table({'agency_code': ['EPA']}), root / 'agency_stats.parquet')
        with patch.object(mcp_server, 'DATA_DIR', root):
            mcp_server._reset_connection_cache()
            try:
                asyncio.run(exercise())
            finally:
                mcp_server._reset_connection_cache()


def check_rule_tools():
    """Exercise prepare_comment and get_rule_text offline using only image packages."""
    from datetime import datetime, timezone
    from mcp.types import CallToolResult
    from spicy_regs import mcp_rule_tools, mcp_server

    sample_govinfo = (
        "<html><body><pre>\n"
        "SUMMARY: Smoke fixture rule.\n\n"
        "DATES: Comments close November 3, 2026.\n\n"
        "PART 60--RESIDENCE CRITERIA\n\n"
        "Sec.  60.4  Foreign citizens.\n\n"
        "    (a) Smoke rule text.\n\n"
        "[FR Doc. 2026-18481 Filed 9-9-26; 8:45 am]\n"
        "</pre></body></html>"
    )
    pinned_now = datetime(2026, 10, 12, 16, 0, tzinfo=timezone.utc)

    async def exercise():
        server = mcp_server.build_server()

        async def data(name, arguments):
            result = await server.call_tool(name, arguments)
            assert isinstance(result, CallToolResult) and isinstance(result.structured_content, dict)
            return result.structured_content

        prepared = await data('prepare_comment', {'docket_id': 'USBC-2026-0628', 'draft': 'Short comment.'})
        assert prepared['status'] == 'open'
        assert prepared['open_documents'][0]['document_id'] == 'USBC-2026-0628-0001'
        assert prepared['open_documents'][0]['subtype'] == 'Notice of Proposed Rulemaking (NPRM)'
        assert prepared['draft_check']['fits_comment_box'] is True

        outline = await data('get_rule_text', {'document': 'USBC-2026-0628'})
        assert outline['mode'] == 'outline'
        assert outline['fr_document_number'] == '2026-18481'
        assert outline['rins'] == ['0607-AA75']
        assert outline['sections'] == [{'section': '60.4', 'title': 'Foreign citizens.', 'part': '60'}]

        sec = await data('get_rule_text', {'document': 'USBC-2026-0628', 'section': '60.4'})
        assert 'Smoke rule text.' in sec['text']

    with TemporaryDirectory() as temp:
        root = Path(temp)
        pq.write_table(
            pa.table({
                'docket_id': ['USBC-2026-0628'],
                'agency_code': ['USBC'],
                'title': ['Residence criteria'],
            }),
            root / 'dockets.parquet',
        )
        pq.write_table(
            pa.table({
                'document_id': ['USBC-2026-0628-0001'],
                'docket_id': ['USBC-2026-0628'],
                'document_type': ['Proposed Rule'],
                'title': ['Residence criteria'],
                'posted_date': ['2026-09-10T04:00:00Z'],
                'comment_start_date': ['2026-09-10T04:00:00Z'],
                'comment_end_date': ['2026-11-03T04:59:59Z'],
                'withdrawn': [False],
                'publisher_status': ['listed'],
                'fr_doc_num': ['2026-18481'],
            }),
            root / 'documents.parquet',
        )
        pq.write_table(
            pa.table({
                'document_id': ['USBC-2026-0628-0001'],
                'subtype': ['Notice of Proposed Rulemaking (NPRM)'],
            }),
            root / 'document_attributes.parquet',
        )
        pq.write_table(
            pa.table({
                'document_number': ['2026-18481'],
                'title': ['Residence criteria'],
                'document_type': ['Proposed Rule'],
                'publication_date': ['2026-09-10'],
                'comments_close_on': ['2026-11-03'],
                'abstract': ['Smoke fixture rule.'],
                'cfr_references': [[{'title': 15, 'part': '60', 'chapter': None}]],
                'regulation_id_numbers': [['0607-AA75']],
                'html_url': ['https://www.federalregister.gov/d/2026-18481'],
                'body_html_url': ['https://www.federalregister.gov/documents/full_text/html/2026/09/10/2026-18481.html'],
            }),
            root / 'federal_register.parquet',
        )

        mcp_rule_tools._fetch_rule_text_with_source.cache_clear()
        with (
            patch.object(mcp_server, 'DATA_DIR', root),
            patch.object(mcp_rule_tools, '_utc_now', return_value=pinned_now),
            patch.object(
                mcp_rule_tools,
                '_fetch_url_text',
                return_value=sample_govinfo,
            ),
        ):
            mcp_server._reset_connection_cache()
            try:
                asyncio.run(exercise())
            finally:
                mcp_server._reset_connection_cache()
                mcp_rule_tools._fetch_rule_text_with_source.cache_clear()
    assert 'spicy_docs' not in sys.modules
    assert 'polars' not in sys.modules
    assert 'boto3' not in sys.modules


if __name__ == "__main__":
    check_native_selection()
    check_public_download()
    print("Native citation receipt selection and checked public download passed")
    check_mcp_tools()
    print("Ordinary MCP discovery, query, expected error and recovery passed")
    check_rule_tools()
    print("Offline prepare_comment and get_rule_text smoke check passed")
