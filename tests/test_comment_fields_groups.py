"""Physical-file groups feed the maintained consumer without losing conflicting or withheld copies."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines import comment_fields_groups as groups
from spicy_regs.pipelines import comment_fields_write as cfw
from tests.test_comment_fields_write import LISTED, T0, reads, row, seed, table
from tests.test_comment_fields_write import catalog as _catalog

catalog = _catalog


def _stage(tmp_path, rows):
    source = tmp_path / "source"
    reads(source, rows)
    stage = tmp_path / "stage"
    cf.stage(source, stage, profile="attachments")
    return stage


def test_groups_keep_all_copies_and_use_current_catalog_for_each_write(tmp_path, catalog, monkeypatch):
    from spicy_regs.sources import regulatory_catalog as native

    processing_table = native.processing_table
    selected_predicates = []

    def selected(*args, **kwargs):
        if kwargs.get("include_comment_promotion"):
            selected_predicates.append(kwargs["where"])
        return processing_table(*args, **kwargs)

    monkeypatch.setattr(native, "processing_table", selected)
    seed(catalog, [row("A"), row("B"), row("C", agency="CMS"), row("N"), row("O")])
    stage = _stage(tmp_path, [
        {"key": "a", "comment_id": "A", "attachments_json": LISTED},
        {"key": "a(1)", "comment_id": "A", "attachments_json": None},
        {"key": "b", "comment_id": "B", "attachments_json": LISTED},
        {"key": "c", "comment_id": "C", "attachments_json": LISTED},
        {"key": "n", "comment_id": "N"},
        {"key": "o", "comment_id": "O", "modify_date": "2021-01-01", "attachments_json": LISTED},
        {"key": "x", "comment_id": "X", "attachments_json": LISTED},
    ])
    work = tmp_path / "groups"
    planned = groups.plan(work, stage, max_rows=1, max_read_bytes=100_000)
    assert planned["outcomes"] == {"candidate": 3, "no-stated-value": 1, "other-version-only": 1, "absent-from-catalog": 1}
    assert [g["original_file"] for g in planned["groups"]] == ["CMS", "EPA", "EPA"]
    members = pl.read_parquet(work / "members.parquet").to_dicts()
    assert [(m["comment_id"], m["read_copies"]) for m in members] == [("C", 1), ("A", 2), ("B", 1)]
    # Another maintained write moves B's physical-file surrogate after grouping. The group keeps its ID; fresh
    # prepare must use the current location and preserve the changed non-fill field.
    seed(catalog, [row("B", agency="MOVED")])
    changed = 0
    for group in planned["groups"]:
        out = tmp_path / f"stage-{group['group_id']}"
        staged = groups.stage_group(work, group["group_id"], out)
        assert staged["rows"] == group["read_copies"]
        execution = tmp_path / f"write-{group['group_id']}"
        prepared = cfw.prepare(execution, reads=out / "reads.parquet", profile="attachments")
        assert prepared["snapshot"] != planned["snapshot"]
        if any(m["comment_id"] == "B" and m["group_id"] == group["group_id"] for m in members):
            assert list(prepared["files"]) == ["MOVED"]
        written = cfw.write(execution)
        changed += written["rows_changed"]
        assert cfw.write(execution)["rows_changed"] == 0  # maintained verified-journal resume
    actual = table(catalog)
    assert changed == 2
    assert actual["A"]["attachments_json"] is None  # the null copy conflicts; it was not partitioned away
    assert actual["B"]["attachments_json"] == actual["C"]["attachments_json"] == LISTED
    assert actual["B"]["agency_code"] == "MOVED"
    assert actual["N"]["attachments_json"] is actual["O"]["attachments_json"] is None
    assert len(selected_predicates) == 3
    assert all("AND agency_code IN (SELECT DISTINCT _file" in where for where in selected_predicates)


@pytest.mark.parametrize("target", ["reads", "membership"])
def test_group_stage_refuses_changed_source_or_membership(tmp_path, catalog, target):
    seed(catalog, [row("A")])
    stage = _stage(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    work = tmp_path / "groups"
    groups.plan(work, stage, max_rows=2, max_read_bytes=100_000)
    changed = stage / "reads.parquet" if target == "reads" else work / "members.parquet"
    changed.write_bytes(changed.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="differs|changed"):
        groups.stage_group(work, 0, tmp_path / "out")


def test_group_plan_refuses_an_identity_whose_copies_exceed_its_budget(tmp_path, catalog):
    seed(catalog, [row("A")])
    stage = _stage(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    work = tmp_path / "groups"
    with pytest.raises(ValueError, match="identity's copies exceed"):
        groups.plan(work, stage, max_rows=1, max_read_bytes=10)
    assert not (work / "groups.json").exists()


def test_group_byte_bound_splits_only_within_one_physical_file(tmp_path, catalog):
    seed(catalog, [row(f"C-{i}") for i in range(4)])
    stage = _stage(tmp_path, [{"key": str(i), "comment_id": f"C-{i}", "attachments_json": LISTED} for i in range(4)])
    size = sum(len(v.encode()) for v in ("0", "C-0", T0, LISTED))
    work = tmp_path / "groups"
    planned = groups.plan(work, stage, max_rows=100, max_read_bytes=size * 2)
    assert len(planned["groups"]) >= 2
    assert all(g["original_file"] == "EPA" and g["read_value_bytes"] <= size * 2 for g in planned["groups"])
    assert sum(g["read_copies"] for g in planned["groups"]) == 4


def test_actual_attachment_originals_keep_every_source_copy(tmp_path, catalog):
    originals = {
        "HHS-OASH-2022-0006-0002": "ce146ada03ede758a871f124ce51ad087ba8fa8d397500750f5a04052db79d06",
        "FWS-R4-ES-2020-0109-0210": "c852d64b7d38e4aecbdc0b3bc7b72611cfeaaaf3839fe6153ab644970b26b1f6",
    }
    source, expected = tmp_path / "source", []
    for comment_id, digest in originals.items():
        body = (Path(__file__).parent / "fixtures/comment_attachment_unknown_sizes" / f"{comment_id}.json").read_bytes()
        assert hashlib.sha256(body).hexdigest() == digest
        raw = json.loads(body)
        part = cf._attachment_part_row(SimpleNamespace(key=f"{comment_id}.json", payload=raw))
        agency = raw["data"]["attributes"]["agencyId"]
        seed(catalog, [row(comment_id, agency=agency, modify=part["modify_date"])])
        copies = [part, {**part, "key": f"retained-copy/{comment_id}.json"}]
        reads(source, copies, agency=agency)
        expected.extend({key: item[key] for key in groups.COLUMNS} for item in copies)
    stage = tmp_path / "stage"
    cf.stage(source, stage, profile="attachments")
    work = tmp_path / "groups"
    planned = groups.plan(work, stage, max_rows=100, max_read_bytes=100_000)
    assert planned["outcomes"] == {"candidate": 2}
    assert (planned["source_rows"], planned["source_identities"]) == (4, 2)
    actual = []
    for group in planned["groups"]:
        out = tmp_path / f"group-{group['group_id']}"
        staged = groups.stage_group(work, group["group_id"], out)
        assert staged["rows"] == 2 and staged["distinct_comment_ids"] == 1
        actual.extend(pl.read_parquet(out / "reads.parquet").to_dicts())
    assert sorted(actual, key=lambda item: item["key"]) == sorted(expected, key=lambda item: item["key"])


def test_group_plan_still_refuses_duplicate_native_ids_with_repeated_source_ids(tmp_path, catalog, monkeypatch):
    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import iceberg

    seed(catalog, [row("A")])
    stage = _stage(tmp_path, [{"key": key, "comment_id": "A", "attachments_json": LISTED} for key in ("a", "a-copy")])
    native = iceberg._qualified(COMMENT)
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda *_args, **_kw:
                        f"SELECT * FROM (SELECT * FROM {native} UNION ALL SELECT * FROM {native})")
    with pytest.raises(ValueError, match="appears more than once in the native catalog"):
        groups.plan(tmp_path / "groups", stage, max_rows=100, max_read_bytes=100_000)


def test_group_plan_still_refuses_a_moving_native_snapshot(tmp_path, catalog, monkeypatch):
    from spicy_regs.sources import iceberg

    seed(catalog, [row("A")])
    stage = _stage(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    read_snapshot = iceberg._read_snapshot
    calls = 0

    def moving(*args, **kwargs):
        nonlocal calls
        held = read_snapshot(*args, **kwargs)
        calls += 1
        return iceberg.CatalogSnapshot(held.table_uuid, held.snapshot_id + (calls > 1), held.schema_id)

    monkeypatch.setattr(iceberg, "_read_snapshot", moving)
    with pytest.raises(RuntimeError, match="Catalog moved while grouping"):
        groups.plan(tmp_path / "groups", stage, max_rows=100, max_read_bytes=100_000)
    assert not (tmp_path / "groups/groups.json").exists()
