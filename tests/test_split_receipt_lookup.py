"""Bounded vote lookup across exact ordered members, without a source or network run."""

from copy import deepcopy
from dataclasses import replace
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import receipt_lookup as lookup
from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.sources import publication
from tests.receipt_lookup_fixtures import GENERATION, _columns, _digest, index_of, published, serving, written
from tests.test_mcp_receipt_lookup import POLICIES, call, congress
from tests.test_mcp_server import _tool_data
from spicy_regs import mcp_server as server


def vote(table, member, position="Yea"):
    values = {"vote_id": f"119-house-1-{member}", "congress": "119", "chamber": "house"}
    if table == "member_votes":
        values.update(member_key=f"name:M{member}", member_name=f"M{member}", position=position)
    else:
        values.update(question=f"Question {member}")
    return congress(table, **values)


def key(table, member):
    row = vote(table, member)
    return {name: row[name] for name in POLICIES[table].identity_fields}


def split_selection(tmp_path, monkeypatch, *, table="member_votes", mode="remote", edit=None, policies=None):
    subjects, receipts = [], []
    for ordinal, policy in enumerate(policies or (None, None)):
        subject, receipt = written(tmp_path / f"work-{ordinal}", table, [vote(table, ordinal)], policy=policy)
        subjects.append(pq.read_table(subject))
        receipts.append(pq.read_table(receipt))
    subject = tmp_path / "subjects.parquet"
    pq.write_table(pa.concat_tables(subjects), subject)
    if edit:
        receipts = edit(receipts)
    root = tmp_path / "bucket"
    entry = published(root, "votes", {table: subject}, pa.concat_tables(receipts), row_group=1)
    folder = root / entry["prefix"] / "etl_receipts"
    folder.mkdir()
    declarations, paths = [], []
    for ordinal, rows in enumerate(receipts):
        path = folder / f"{ordinal}.parquet"
        pq.write_table(rows, path, row_group_size=1)
        paths.append(path)
        declarations.append(
            {
                "key": f"etl_receipts/{ordinal}.parquet",
                "sha256": _digest(path),
                "byteSize": path.stat().st_size,
                "rows": rows.num_rows,
            }
        )
    entry["etlReceipts"] = {
        "version": 2,
        "members": declarations,
        "rows": sum(p.num_rows for p in receipts),
        "columns": _columns(paths[0]),
        "generationId": GENERATION,
        "datasets": [table],
    }
    index = publication.parse_index(json.dumps(index_of({"votes": entry})).encode())
    con = serving(monkeypatch, root, index)
    local = None
    if mode == "native":
        local = {
            "native": {
                table: {
                    "subjects": [str(subject)],
                    "receipts": list(map(str, paths)),
                    "generation_id": GENERATION,
                    "key_index": None,
                    "key_index_descriptor": None,
                }
            }
        }
    elif mode == "download":
        local = {
            "selected_tables": [table],
            "receipt_members": {
                entry["prefix"] + "/" + item["key"]: str(path) for item, path in zip(declarations, paths, strict=True)
            },
        }
    return con, index, local, str(root), paths


def read(con, index, local, root, *, keys=None, fields=None, table="member_votes"):
    return lookup.read_fields(
        con,
        table=table,
        keys=keys or [key(table, 1)],
        fields=fields or ["position"],
        index=index,
        local=local,
        base_url=root,
        entry={},
        plain=lambda value: value,
    )


@pytest.mark.parametrize("mode", ["remote", "download", "native"])
@pytest.mark.parametrize(
    "table, field, value", [("member_votes", "position", "Yea"), ("roll_call_votes", "question", "Question 1")]
)
def test_later_member_exact_fields_request_order_and_binding(tmp_path, monkeypatch, mode, table, field, value):
    con, index, local, root, paths = split_selection(tmp_path, monkeypatch, table=table, mode=mode)
    try:
        members = lookup.selected_members(index, local, root, table)
        assert [member.location for member in members] == list(map(str, paths))
        assert [member.sha256 for member in members] == (
            [_digest(path) for path in paths] if mode != "native" else [None, None]
        )
        asked = [key(table, 1), key(table, 0), key(table, 1)]
        reply = read(con, index, local, root, keys=asked, fields=[field], table=table)
        assert [item["key"] for item in reply["keys"]] == asked
        assert reply["keys"][0]["fields"][field] == {"state": "stated", "value": value}
        assert reply["keys"][0] == reply["keys"][2]
        assert reply["receipts"]["generation_id"] == GENERATION
        assert reply["receipts"]["artifact_digest"] == (
            None if mode == "native" else index["families"]["votes"]["artifactDigest"]
        )
        assert reply["receipts"]["members"] == [{"location": m.location, "sha256": m.sha256} for m in members]
        assert table in lookup.tables_with_receipts(index, local, root)
        if mode == "remote":
            assert call(table, asked, [field])["keys"] == reply["keys"]
            description = _tool_data(server.build_server(), "describe_table", {"table": table})
            assert description["receipt_fields"]["available"] is True
    finally:
        con.close()


def test_cross_member_ambiguity_never_chooses_an_occurrence(tmp_path, monkeypatch):
    def edit(parts):
        return [parts[0], pa.concat_tables([parts[0], parts[1]])]

    con, index, local, root, _ = split_selection(tmp_path, monkeypatch, edit=edit)
    try:
        reply = read(con, index, local, root, keys=[key("member_votes", 0), key("member_votes", 1)])
        assert reply["keys"][0] == {"key": key("member_votes", 0), "receipt": "ambiguous", "receipts_found": 2}
        assert reply["keys"][1]["receipt"] == "found"
    finally:
        con.close()


def test_missing_receipt_and_not_in_table_remain_distinct(tmp_path, monkeypatch):
    con, index, local, root, _ = split_selection(
        tmp_path, monkeypatch, edit=lambda parts: [parts[0], parts[1].slice(0, 0)]
    )
    try:
        reply = read(con, index, local, root, keys=[key("member_votes", i) for i in (0, 1, 9)])
        assert [row["receipt"] for row in reply["keys"]] == ["found", "receipt_missing", "not_in_table"]
    finally:
        con.close()


def test_one_aggregate_budget_refuses_before_any_identity_or_key_scan(tmp_path, monkeypatch):
    con, index, local, root, _ = split_selection(tmp_path, monkeypatch)
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 1)
    monkeypatch.setattr(lookup, "_stated_identities", lambda *_a: pytest.fail("receipt identity scan before budget"))
    monkeypatch.setattr(lookup, "_accepted", lambda *_a: pytest.fail("key scan before budget"))
    try:
        with pytest.raises(ValueError, match="2, over the 1-receipt bound"):
            read(con, index, local, root)
    finally:
        con.close()


def test_key_limit_still_refuses_before_footers(tmp_path, monkeypatch):
    con, index, local, root, _ = split_selection(tmp_path, monkeypatch)
    monkeypatch.setattr(lookup, "_footer", lambda *_a: pytest.fail("footer before key bound"))
    try:
        with pytest.raises(ValueError, match="keys holds 101 keys"):
            read(con, index, local, root, keys=[key("member_votes", 0)] * 101)
    finally:
        con.close()


@pytest.mark.parametrize("incompatible", [False, True])
def test_earlier_policy_in_later_member_is_checked_even_if_unrequested(tmp_path, monkeypatch, incompatible):
    policy = POLICIES["member_votes"]
    earlier = replace(
        policy,
        policy_version="congress/earlier",
        **({"identity_fields": (*policy.identity_fields, "member_name")} if incompatible else {}),
    )
    con, index, local, root, _ = split_selection(tmp_path, monkeypatch, policies=(policy, earlier))
    try:
        if incompatible:
            with pytest.raises(ValueError, match="whose row identity"):
                read(con, index, local, root, keys=[key("member_votes", 0)])
        else:
            reply = read(con, index, local, root)
            assert reply["keys"][0]["receipt"] == "found"
            assert reply["receipts"]["policy_version"] == sorted([policy.policy_version, earlier.policy_version])
    finally:
        con.close()


def test_same_policy_version_cannot_hide_incompatible_earlier_member(tmp_path, monkeypatch):
    policy = POLICIES["member_votes"]
    other = replace(policy, identity_fields=(*policy.identity_fields, "member_name"))
    con, index, local, root, _ = split_selection(tmp_path, monkeypatch, policies=(other, policy))
    try:
        with pytest.raises(ValueError, match="whose row identity"):
            read(con, index, local, root)
    finally:
        con.close()


@pytest.mark.parametrize("failure", ["missing-member", "bad-later-value"])
def test_late_member_failure_returns_no_partial_success(tmp_path, monkeypatch, failure):
    def edit(parts):
        rows = parts[1].to_pylist()
        rows[0]["processing_json"] = "broken retained JSON"
        return [parts[0], pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA)]

    con, index, local, root, paths = split_selection(
        tmp_path, monkeypatch, edit=edit if failure == "bad-later-value" else None
    )
    try:
        if failure == "missing-member":
            paths[-1].unlink()
            error = duckdb.IOException
        else:
            error = json.JSONDecodeError
        with pytest.raises(error):
            read(con, index, local, root, keys=[key("member_votes", i) for i in (0, 1)])
    finally:
        con.close()


def test_partial_local_selection_is_unavailable_without_reading_a_subset(tmp_path, monkeypatch):
    con, index, local, root, _ = split_selection(tmp_path, monkeypatch, mode="download")
    local["receipt_members"].pop(next(reversed(local["receipt_members"])))
    monkeypatch.setattr(lookup, "_footer", lambda *_a: pytest.fail("partial membership read"))
    try:
        assert lookup.selected_members(index, local, root, "member_votes") == ()
        assert "member_votes" not in lookup.tables_with_receipts(index, local, root)
        with pytest.raises(ValueError, match="has no receipts in this connection"):
            read(con, index, local, root)
    finally:
        con.close()


@pytest.mark.parametrize("mode", ["remote", "native"])
def test_split_index_refuses_before_scanning(tmp_path, monkeypatch, mode):
    con, index, local, root, _ = split_selection(tmp_path, monkeypatch, mode=mode)
    if local:
        local["native"]["member_votes"]["key_index_descriptor"] = {}
    else:
        index["families"]["votes"]["etlReceipts"]["keyIndex"] = {}
    monkeypatch.setattr(lookup, "_footer", lambda *_a: pytest.fail("split index scan"))
    try:
        with pytest.raises(ValueError, match="Split receipt indexing is unsupported"):
            read(con, index, local, root)
    finally:
        con.close()


def test_changed_captured_pin_uses_its_own_member_cache_identity(tmp_path, monkeypatch):
    con, index, local, root, paths = split_selection(tmp_path, monkeypatch)
    try:
        original = lookup.selected_members(index, local, root, "member_votes")
        read(con, index, local, root)
        successor = deepcopy(index)
        successor["families"]["votes"]["etlReceipts"]["members"][1]["sha256"] = "sha256:" + "f" * 64
        # Metadata-only: prove a new captured pin cannot reuse the old per-member footer/identity cache.
        changed = lookup.selected_members(successor, local, root, "member_votes")
        assert changed[0] == original[0] and changed[1].location == str(paths[1])
        assert changed[1].sha256 != original[1].sha256
        assert (changed[1].location, changed[1].sha256) not in lookup._FOOTERS
        assert (changed[1].location, changed[1].sha256, "member_votes") not in lookup._STATED
    finally:
        con.close()


def test_other_split_families_stay_unavailable(tmp_path, monkeypatch):
    con, index, _, root, _ = split_selection(tmp_path, monkeypatch)
    try:
        entry = index["families"]["votes"]
        entry["tables"] = {"fcc_filings.parquet": entry["tables"]["member_votes.parquet"]}
        entry["etlReceipts"]["datasets"] = ["fcc_filings"]
        assert lookup.selected_members(index, None, root, "fcc_filings") == ()
        assert "fcc_filings" not in lookup.tables_with_receipts(index, None, root)
    finally:
        con.close()
