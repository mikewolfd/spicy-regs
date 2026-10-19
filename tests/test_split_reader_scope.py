"""The deployed query scope and split receipt reader share one captured selection."""

import pytest

from spicy_regs import mcp_server
from spicy_regs.sources import publication
from tests import test_receipt_member_publication as fixture
from tests.generation_fakes import Store
from tests.test_generation_mcp import connection_fixture
from tests.test_generation_publication import publish

votes = fixture.votes


def test_scoped_vote_query_expands_to_every_split_receipt_without_opening_unrelated_sources(
    tmp_path, monkeypatch, votes
):
    directory = tmp_path / "generation"
    fixture.generation(directory, votes)
    index = publish(Store(), directory)
    subjects = publication.table_members(index, fixture.DATASET + ".parquet")
    receipts = publication.receipt_members(index)
    locations = {member.path: directory / member.key for member in (*subjects, *receipts)}
    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    connection_fixture(monkeypatch, index, locations, tables=(fixture.DATASET, "unrelated"))
    token = mcp_server._requested_relations.set({fixture.DATASET})
    initial = expanded = None
    try:
        initial = mcp_server._build_connection()
        assert initial.execute("SELECT count(*) FROM member_votes").fetchone() == (3,)
        assert initial.seen == [f"{mcp_server.R2_BASE_URL}/{member.path}" for member in subjects]
        assert "etl_receipts" not in mcp_server._available_tables(initial)
        monkeypatch.setattr(mcp_server, "_read_publication", lambda: pytest.fail("Pins must not refresh"))
        mcp_server._requested_relations.set({"etl_receipts"})
        expanded = mcp_server._expand_query_connection(initial)
        assert expanded is not initial
        assert mcp_server._connection_index(expanded) == index
        assert expanded.execute("SELECT count(*) FROM etl_receipts").fetchone() == (4,)
        assert set(expanded.seen) == {f"{mcp_server.R2_BASE_URL}/{path}" for path in locations}
        assert "unrelated" not in mcp_server._available_tables(expanded)
    finally:
        mcp_server._requested_relations.reset(token)
        if expanded is not None:
            expanded.close()
        if initial is not None:
            initial.close()
