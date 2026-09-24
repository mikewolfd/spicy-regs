"""Publication refuses identity loss and changing predecessors, even at equal row counts."""

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.publish_comments_mirror import validate_export


def write_candidate(root, ids):
    pq.write_table(pa.table({'comment_id': ids, 'agency_code': ['EPA'] * len(ids),
                             'docket_id': ['EPA-1'] * len(ids), 'posted_date': ['2026-09-01'] * len(ids)}),
                   root / 'comments.parquet')
    pq.write_table(pa.table({'agency_code': ['EPA'], 'docket_id': ['EPA-1'], 'year': [2026],
                             'month': [9], 'row_count': [len(ids)]}), root / 'comments_index.parquet')


def head(etag):
    return httpx.Response(200, headers={'etag': etag}, request=httpx.Request('HEAD', 'https://example.org/comments'))


def test_export_retains_previous_ids(tmp_path, monkeypatch):
    old = tmp_path / 'old.parquet'
    pq.write_table(pa.table({'comment_id': ['a']}), old)
    write_candidate(tmp_path, ['a', 'b'])
    monkeypatch.setattr(httpx, 'head', lambda *_a, **_k: head('stable'))
    assert validate_export(tmp_path, str(old)) == 'stable'
    write_candidate(tmp_path, ['b'])
    with pytest.raises(RuntimeError, match='discard 1 previously published IDs'):
        validate_export(tmp_path, str(old))


def test_changed_predecessor_refuses_publication(tmp_path, monkeypatch):
    old = tmp_path / 'old.parquet'
    pq.write_table(pa.table({'comment_id': ['a']}), old)
    write_candidate(tmp_path, ['a'])
    responses = iter([head('old'), head('changed')])
    monkeypatch.setattr(httpx, 'head', lambda *_a, **_k: next(responses))
    with pytest.raises(RuntimeError, match='changed during validation'):
        validate_export(tmp_path, str(old))
