"""Readers resolve a table stored as several files through the version-2 index (multi-file design §4.1, §4.5)."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.sources import publication as pub
from tests.test_generation_mcp import connection_fixture

DIGEST = "sha256:" + "b" * 64
PREFIX = f"generations/bills/{'b' * 64}"
COLUMNS = [["bill_id", "VARCHAR"], ["congress", "VARCHAR"], ["body", "VARCHAR"]]


def _member(congress: str, rows: int, size: int) -> dict:
    return {"key": f"bill_sections/congress={congress}/part-000000.parquet", "sha256": "sha256:" + congress[-1] * 64,
            "byteSize": size, "rows": rows, "partition": {"congress": congress}}


def _index(**table) -> dict:
    members = [_member("118", 2, 10), _member("119", 1, 5)]
    split = {"byteSize": 15, "rows": 3, "columns": COLUMNS, "partitionColumns": ["congress"], "members": members}
    single = {"sha256": "sha256:" + "c" * 64, "byteSize": 7, "rows": 1, "columns": [["bill_id", "VARCHAR"]]}
    return {"format": "spicy-regs-publication", "version": 2, "families": {"bills": {
        "prefix": PREFIX, "logicalId": "urn:spicy-regs:family:bills", "artifactDigest": DIGEST,
        "tables": {"bill_sections.parquet": split | table, "congress_bills.parquet": single}}}}


def _parse(index: dict) -> dict:
    return pub.parse_index(json.dumps(index).encode())


def test_a_split_table_resolves_to_its_members_and_a_single_table_to_itself():
    index = _parse(_index())
    assert pub.table_members(index, "bill_sections.parquet") == (
        pub.Member(f"{PREFIX}/bill_sections/congress=118/part-000000.parquet", "sha256:" + "8" * 64, 10, 2),
        pub.Member(f"{PREFIX}/bill_sections/congress=119/part-000000.parquet", "sha256:" + "9" * 64, 5, 1),
    )
    assert pub.single_member(index, "congress_bills.parquet") == pub.Member(
        f"{PREFIX}/congress_bills.parquet", "sha256:" + "c" * 64, 7, 1)
    with pytest.raises(pub.PublicationError, match="2 files; read it through table_members"):
        pub.single_member(index, "bill_sections.parquet")


@pytest.mark.parametrize("broken", [
    {"rows": 4},  # members' rows sum to 3
    {"byteSize": 14},
    {"partitionColumns": ["session"]},  # not a declared column
    {"members": [_member("118", 2, 10), {**_member("119", 1, 5), "partition": {"congress": "118"}}]},
    {"members": [_member("118", 2, 10), {**_member("119", 1, 5), "key": "bill_sections/congress=119/x.parquet"}]},
    {"members": [_member("118", 3, 15), _member("118", 0, 0)]},  # one key twice
    {"members": [{**_member("118", 3, 15), "partition": {"congress": "../118"}}]},
])
def test_a_split_table_whose_members_do_not_add_up_or_name_their_partition_refuses(broken):
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        _parse(_index(**broken))


def test_only_version_2_may_list_a_split_table():
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        _parse({**_index(), "version": 1})


def test_readers_prefer_the_version_2_index_and_fall_back_to_version_1(monkeypatch):
    v1 = {**_index(), "version": 1}
    v1["families"]["bills"]["tables"].pop("bill_sections.parquet")
    served = {pub.INDEX_KEY: v1, pub.INDEX_V2_KEY: _index()}
    monkeypatch.setattr(pub, "_bounded_get", lambda url, **_: (
        json.dumps(served[key]).encode() if (key := url.rsplit("/", 1)[1]) in served else None))
    assert "bill_sections.parquet" in pub.load_index("https://test")["families"]["bills"]["tables"]
    del served[pub.INDEX_V2_KEY]
    assert pub.load_index("https://test")["version"] == 1


def test_the_mcp_serves_one_view_over_every_member_without_a_hive_column(tmp_path, monkeypatch):
    """Members sit under ``congress=<N>/`` directories; the view's columns are still exactly the declared ones."""
    index = _parse(_index())
    locations = {}
    for congress, bills in (("118", ["118-hr1", "118-hr2"]), ("119", ["119-s5"])):
        path = tmp_path / f"congress={congress}" / "part-000000.parquet"
        path.parent.mkdir()
        pq.write_table(pa.table({"bill_id": bills, "congress": [congress] * len(bills), "body": ["text"] * len(bills)}),
                       path)
        locations[f"{PREFIX}/bill_sections/congress={congress}/part-000000.parquet"] = path
    single = tmp_path / "congress_bills.parquet"
    pq.write_table(pa.table({"bill_id": ["119-s5"]}), single)
    locations[f"{PREFIX}/congress_bills.parquet"] = single
    built = connection_fixture(monkeypatch, index, locations, tables=())
    mcp_server._build_connection()
    [con] = built
    assert [row[0] for row in con.inner.execute("DESCRIBE bill_sections").fetchall()] == ["bill_id", "congress", "body"]
    assert con.inner.execute("SELECT congress, count(*) FROM bill_sections GROUP BY 1 ORDER BY 1").fetchall() == [
        ("118", 2), ("119", 1)]
    con.inner.close()
