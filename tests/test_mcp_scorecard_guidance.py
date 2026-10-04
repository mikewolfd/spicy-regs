"""Scorecard guidance uses canonical native joins and checks their source lineage."""

import pytest

from spicy_regs import mcp_server, table_joins


@pytest.mark.parametrize("child,parent,columns", [
    ("scorecard_member_links", "scorecard_members", ["scorecard_id", "publisher_member_key"]),
    ("scorecard_item_links", "scorecard_items", ["scorecard_id", "item_id"]),
])
def test_scorecard_guidance_uses_canonical_native_joins_and_generation_compatibility(monkeypatch, child, parent, columns):
    monkeypatch.setattr(mcp_server, "_joins", table_joins.joins_record)
    description = mcp_server._table_joins(child, measurements=True)
    join = next(row for row in description["outgoing"] if row["parent"] == parent)
    assert join["child_columns"] == join["parent_columns"] == columns
    assert join["kind"] == "unmeasured"
    assert join["measurement"] is None
    assert "describe_table's canonical joins" in mcp_server.INSTRUCTIONS
    assert "source parent matches the selected scorecards generation" in mcp_server.INSTRUCTIONS
    assert "compatibility is unavailable or differs" in mcp_server.INSTRUCTIONS
    assert "source_snapshot_id matches snapshot_id" not in mcp_server.INSTRUCTIONS
