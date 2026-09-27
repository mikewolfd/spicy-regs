"""Tables stored as several files: the version-2 index, readers, building, publishing and the audit (multi-file design)."""

import json
from contextlib import contextmanager

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.generation_audit import Declaration, audit
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication as pub, r2
from tests.generation_fakes import Store
from tests.test_generation_audit import public_base
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


# --------------------------------------------------------------------------- #
# Producing and publishing a split table (multi-file design §4.2-§4.4).
# --------------------------------------------------------------------------- #
SPLIT = {"bill_sections.parquet": ["congress"]}
KEYS = ["bill_sections.parquet", "congress_bills.parquet"]


def _sections(directory, by_congress: dict[str, list[str]]) -> None:
    """Write one ``congress=<N>/part-000000.parquet`` member per congress, its rows holding that congress."""
    for congress, bills in by_congress.items():
        path = directory / "bill_sections" / f"congress={congress}" / "part-000000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"bill_id": bills, "congress": [congress] * len(bills),
                                 "body": [f"text of {bill}" for bill in bills]}), path)


def _outputs(directory, by_congress=None, *, bills=("119-s5",)) -> list:
    """A bills family's outputs under ``directory``: split sections (unless already there) and one single table."""
    if by_congress is not None:
        _sections(directory, by_congress)
    pq.write_table(pa.table({"bill_id": list(bills)}), directory / "congress_bills.parquet")
    return [directory / "bill_sections", directory / "congress_bills.parquet"]


def _publish(tmp_path, store, name, files, prior=None):
    directory = tmp_path / name
    build_generation(directory, family="bills", files=files, expected_keys=KEYS, read_snapshot=prior,
                     partitioned=SPLIT)
    return pub.publish_generation(directory, client=store, bucket="test", prior_index=prior or pub.empty_index())


def _serve(monkeypatch, store) -> None:
    """Answer public GETs from the fake bucket, as r2.dev does; the index is read the way readers read it."""
    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test/")
        request = httpx.Request(method, url)
        yield (httpx.Response(200, content=store.objects[key], request=request) if key in store.objects
               else httpx.Response(404, request=request))

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    monkeypatch.setattr(pub, "load_index", lambda url: pub.parse_index(store.objects[pub.INDEX_V2_KEY]))


def test_a_split_table_publishes_in_version_2_and_an_unchanged_partition_is_copied_not_uploaded(tmp_path,
                                                                                                  monkeypatch):
    store = Store()
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1", "118-hr2"],
                                                                           "119": ["119-s5"]}))
    split = first["families"]["bills"]["tables"]["bill_sections.parquet"]
    assert (split["rows"], split["partitionColumns"]) == (3, ["congress"])
    assert [(m["key"], m["rows"], m["partition"]) for m in split["members"]] == [
        ("bill_sections/congress=118/part-000000.parquet", 2, {"congress": "118"}),
        ("bill_sections/congress=119/part-000000.parquet", 1, {"congress": "119"})]
    v1 = pub.parse_index(store.objects[pub.INDEX_KEY])
    assert list(v1["families"]["bills"]["tables"]) == ["congress_bills.parquet"]

    # The builder's nightly shape: fetch the prior partitions, rewrite only the sitting Congress.
    _serve(monkeypatch, store)
    rebuilt = tmp_path / "out2"
    fetched = r2.download_members("bill_sections.parquet", rebuilt)
    assert [path.relative_to(rebuilt).as_posix() for path in fetched] == [m["key"] for m in split["members"]]
    _sections(rebuilt, {"119": ["119-s5", "119-s6"]})
    store.copies.clear()
    second = _publish(tmp_path, store, "second", _outputs(rebuilt), prior=first)

    prefix = second["families"]["bills"]["prefix"]
    assert sorted(store.copies) == [f"{prefix}/bill_sections/congress=118/part-000000.parquet",
                                    f"{prefix}/congress_bills.parquet"]
    assert f"{prefix}/bill_sections/congress=119/part-000000.parquet" in store.writes
    assert second["families"]["bills"]["tables"]["bill_sections.parquet"]["rows"] == 4


def test_the_audit_reconciles_a_split_table_member_by_member(tmp_path):
    store = Store()
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": ["119-s5"]}))
    _publish(tmp_path, store, "second", _outputs(tmp_path / "out2", {"118": ["118-hr1"], "119": ["119-s5", "119-s6"]}),
             prior=first)
    declared = {"bill_sections": Declaration((("bill_id", "VARCHAR"), ("congress", "VARCHAR"), ("body", "VARCHAR")),
                                             ("bill_id",), "test")}

    report = audit(public_base(store, tmp_path / "public"), family="bills", table="bill_sections",
                   declarations=declared)

    table = report["sections"]["publication"]["tables"]["bill_sections.parquet"]
    assert (table["index_equals_manifest"], table["index_equals_root"], table["observed_bytes_equal_index"]) == (
        True, True, True)
    assert len(table["manifest"]) == 2
    conservation = report["sections"]["conservation"]["bill_sections"]
    assert (conservation["rows"], conservation["prior_rows"], conservation["bytes_equal"]) == (3, 2, False)
    assert (conservation["identities_added"], conservation["added_sample"]) == (1, [{"bill_id": "119-s6"}])
    assert report["findings"] == []


def test_the_shrink_guard_sums_a_split_tables_members(tmp_path):
    store = Store()
    many = [f"119-hr{n}" for n in range(3000)]
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": many}))
    with pytest.raises(RuntimeError, match="shrink"):
        _publish(tmp_path, store, "second", _outputs(tmp_path / "out2", {"118": ["118-hr1"]}), prior=first)


@pytest.mark.parametrize(("layout", "message"), [
    ({"bill_sections/congress=118/part-000000.parquet": "119"}, "differ from the partition"),
    ({"bill_sections/congress=118/data.parquet": "118"}, "is not <col>=<value>"),
    ({"bill_sections/session=1/part-000000.parquet": "118"}, "is not <col>=<value>"),
    ({"bill_sections/part-000000.parquet": "118"}, "is not <col>=<value>"),
])
def test_a_split_output_whose_layout_or_rows_disagree_with_its_partitions_refuses(tmp_path, layout, message):
    out = tmp_path / "out"
    for relative, congress in layout.items():
        (out / relative).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"bill_id": ["x"], "congress": [congress], "body": ["b"]}), out / relative)
    with pytest.raises(ValueError, match=message):
        build_generation(tmp_path / "artifact", family="bills", files=_outputs(out), expected_keys=KEYS,
                         partitioned=SPLIT)


def test_members_of_one_split_table_must_share_their_columns(tmp_path):
    out = tmp_path / "out"
    _sections(out, {"118": ["118-hr1"]})
    other = out / "bill_sections" / "congress=119" / "part-000000.parquet"
    other.parent.mkdir(parents=True)
    pq.write_table(pa.table({"bill_id": ["119-s5"], "congress": ["119"]}), other)
    with pytest.raises(ValueError, match="differ in columns"):
        build_generation(tmp_path / "artifact", family="bills", files=_outputs(out), expected_keys=KEYS,
                         partitioned=SPLIT)
