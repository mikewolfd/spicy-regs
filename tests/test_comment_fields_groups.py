"""Physical-file groups feed the maintained consumer without losing conflicting or withheld copies."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
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


@pytest.mark.parametrize("oversized", [False, True])
def test_membership_explain_is_retained_before_copy_and_does_not_execute_it(tmp_path, catalog, oversized):
    seed(catalog, [row("A")])
    stage = _stage(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    work = tmp_path / "groups"
    executed = []
    with catalog() as con:
        class StopBeforeCopy:
            def execute(self, sql, *args):
                if sql.startswith("EXPLAIN COPY"):
                    result = con.execute(sql, *args)
                    assert not (work / "catalog-index.parquet").exists()
                    executed.append(sql.removeprefix("EXPLAIN "))
                    rows = result.fetchall()
                    if oversized:
                        rows.append(("bounded diagnostic probe", "\U0001f642" * 100_000))
                    return SimpleNamespace(fetchall=lambda: rows)
                if sql.startswith("COPY (SELECT n.*"):
                    assert executed == [sql]
                    assert not (work / "catalog-index.parquet").exists()
                    evidence = json.loads((work / "membership-explain.json").read_text())
                    assert evidence["analyze"] is False and evidence["truncated"] is oversized
                    assert evidence["snapshot"]["table_uuid"] == "local"
                    assert evidence["resources"] == {"memory": groups.RESOURCES.memory,
                                                     "threads": groups.RESOURCES.threads, "max_spill": "3GB"}
                    assert "HASH_JOIN" in evidence["text"] and "COPY" in evidence["text"]
                    raise RuntimeError("planted membership refusal before COPY")
                return con.execute(sql, *args)
        with pytest.raises(RuntimeError, match="planted membership refusal"):
            groups.plan(work, stage, max_rows=100, max_read_bytes=100_000, con=StopBeforeCopy())
    assert (work / "membership-explain.json").stat().st_size <= 65_536
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


def _bucket_witness_ids(buckets: int) -> dict[int, str]:
    """Find one deterministic comment_id per hash bucket 0..buckets-1."""
    mask = buckets - 1
    found: dict[int, str] = {}
    with duckdb.connect() as con:
        rows = con.execute(
            f"SELECT 'BUCKET-' || i::VARCHAR AS cid, (hash('BUCKET-' || i::VARCHAR) & {mask})::INT AS b "
            "FROM range(2000) t(i)"
        ).fetchall()
    for cid, bucket in rows:
        found.setdefault(bucket, cid)
        if len(found) == buckets:
            break
    assert len(found) == buckets
    return found


def test_partitioned_plan_matches_unpartitioned_reference_exactly(tmp_path, catalog, monkeypatch):
    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import iceberg

    witnesses = _bucket_witness_ids(groups.CLASSIFY_BUCKETS)
    utf8_listed = json.dumps([{"attachment_id": "0900006400000099", "title": "Résumé — café 日本語", "fileFormats": None}])
    large_listed = json.dumps([{"attachment_id": "0900006400000100", "title": "X" * 320, "fileFormats": None}])

    native_rows = []
    stage_rows = []
    agencies = ("A_SOLO", "CMS", "DOT", "EPA", "FDA", "NOAA")

    # Seed one candidate per hash bucket across agencies (ensuring every bucket 0..31 is populated).
    for bucket, cid in sorted(witnesses.items()):
        agency = "EPA" if bucket < 7 else ("FDA" if bucket < 11 else agencies[bucket % len(agencies)])
        native_rows.append(row(cid, agency=agency, modify=T0))
        payload = large_listed if agency == "FDA" else (utf8_listed if bucket % 5 == 0 else LISTED)
        stage_rows.append({"key": f"{cid}/primary", "comment_id": cid, "modify_date": T0, "attachments_json": payload})
        if bucket % 4 == 0:
            # Duplicate copy with same version and value.
            stage_rows.append({"key": f"{cid}/dup", "comment_id": cid, "modify_date": T0, "attachments_json": payload})
        elif bucket % 4 == 1:
            # Conflicting NULL copy alongside valid attachment (must remain candidate & keep both copies).
            stage_rows.append({"key": f"{cid}/null-copy", "comment_id": cid, "modify_date": T0, "attachments_json": None})
        elif bucket % 4 == 2:
            # Stale version alongside matching version (must remain candidate & keep both copies).
            stage_rows.append({"key": f"{cid}/stale", "comment_id": cid, "modify_date": "2020-01-01", "attachments_json": LISTED})

    # Explicit edge cases: NULL modify_date matches NULL modify_date; mismatches; no-stated-value; absent.
    native_rows.extend([
        row("NULL-CAND", agency="CMS", modify=None),
        row("NULL-NO-VAL", agency="CMS", modify=None),
        row("NULL-MISMATCH-STAGE", agency="DOT", modify=T0),
        row("NULL-MISMATCH-CAT", agency="DOT", modify=None),
        row("OTHER-VER", agency="NOAA", modify=T0),
        row("NO-VAL-MULTI", agency="NOAA", modify=T0),
    ])
    stage_rows.extend([
        {"key": "null-cand/1", "comment_id": "NULL-CAND", "modify_date": None, "attachments_json": utf8_listed},
        {"key": "null-no-val/1", "comment_id": "NULL-NO-VAL", "modify_date": None, "attachments_json": None},
        {"key": "null-mis-s/1", "comment_id": "NULL-MISMATCH-STAGE", "modify_date": None, "attachments_json": LISTED},
        {"key": "null-mis-c/1", "comment_id": "NULL-MISMATCH-CAT", "modify_date": T0, "attachments_json": LISTED},
        {"key": "other-ver/1", "comment_id": "OTHER-VER", "modify_date": "2019-01-01", "attachments_json": LISTED},
        {"key": "other-ver/2", "comment_id": "OTHER-VER", "modify_date": "2019-02-02", "attachments_json": None},
        {"key": "no-val/1", "comment_id": "NO-VAL-MULTI", "modify_date": T0, "attachments_json": None},
        {"key": "no-val/2", "comment_id": "NO-VAL-MULTI", "modify_date": T0, "attachments_json": None},
        {"key": "absent/1", "comment_id": "ABSENT-ONE", "modify_date": T0, "attachments_json": LISTED},
        {"key": "absent/2a", "comment_id": "ABSENT-TWO", "modify_date": T0, "attachments_json": LISTED},
        {"key": "absent/2b", "comment_id": "ABSENT-TWO", "modify_date": None, "attachments_json": None},
    ])

    seed(catalog, native_rows)
    stage = _stage(tmp_path, stage_rows)
    # Force multiple contiguous _file batches in _write_members_batched AND both row & byte splits.
    monkeypatch.setattr(groups, "MEMBERS_BATCH_ROWS", 4)
    max_rows = 3
    max_read_bytes = 1800

    work = tmp_path / "groups"
    planned = groups.plan(work, stage, max_rows=max_rows, max_read_bytes=max_read_bytes)

    # Compute independent unpartitioned reference SQL directly from stage/reads.parquet and catalog.
    reads_rel = groups._relation(stage / "reads.parquet")
    bytes_expr = groups._bytes()
    with catalog() as con:
        namespace = iceberg._selected_catalog_namespace(con, COMMENT)
        snapshot = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        native = iceberg._snapshot_query(COMMENT, snapshot, namespace=namespace).replace(
            "SELECT *", f"SELECT comment_id, modify_date, {cfw.FILE_COLUMN} AS _file", 1
        )
        ref_classified_sql = f"""
            SELECT r.comment_id, any_value(c.modify_date) AS modify_date, any_value(c._file) AS _file,
                   count(*)::BIGINT AS read_copies, sum({bytes_expr})::BIGINT AS read_value_bytes,
                   CASE WHEN count(c.comment_id)=0 THEN 'absent-from-catalog'
                        WHEN NOT bool_or(r.modify_date IS NOT DISTINCT FROM c.modify_date) THEN 'other-version-only'
                        WHEN NOT bool_or(r.modify_date IS NOT DISTINCT FROM c.modify_date
                                         AND r.attachments_json IS NOT NULL) THEN 'no-stated-value'
                        ELSE 'candidate' END AS outcome
            FROM {reads_rel} r
            LEFT JOIN (
                SELECT n.* FROM ({native}) n
                SEMI JOIN (SELECT comment_id FROM {reads_rel}) sr USING (comment_id)
            ) c USING (comment_id)
            GROUP BY r.comment_id
        """
        ref_members = con.execute(f"""
            WITH classified AS ({ref_classified_sql})
            SELECT comment_id, modify_date, _file, read_copies, read_value_bytes,
                   (dense_rank() OVER (ORDER BY _file, row_bucket, byte_bucket) - 1)::BIGINT AS group_id
            FROM (
                SELECT *, (row_number() OVER w - 1) // {max_rows} AS row_bucket,
                       (sum(read_value_bytes) OVER w) // {max_read_bytes // 2} AS byte_bucket
                FROM classified WHERE outcome='candidate'
                WINDOW w AS (PARTITION BY _file ORDER BY comment_id ROWS UNBOUNDED PRECEDING)
            ) ORDER BY group_id, comment_id
        """).to_arrow_table().to_pylist()
        ref_excluded = con.execute(
            f"WITH classified AS ({ref_classified_sql}) "
            "SELECT * FROM classified WHERE outcome <> 'candidate' ORDER BY comment_id"
        ).to_arrow_table().to_pylist()
        ref_groups = con.execute(f"""
            WITH classified AS ({ref_classified_sql}),
            members AS (
                SELECT comment_id, modify_date, _file, read_copies, read_value_bytes,
                       (dense_rank() OVER (ORDER BY _file, row_bucket, byte_bucket) - 1)::BIGINT AS group_id
                FROM (
                    SELECT *, (row_number() OVER w - 1) // {max_rows} AS row_bucket,
                           (sum(read_value_bytes) OVER w) // {max_read_bytes // 2} AS byte_bucket
                    FROM classified WHERE outcome='candidate'
                    WINDOW w AS (PARTITION BY _file ORDER BY comment_id ROWS UNBOUNDED PRECEDING)
                )
            )
            SELECT group_id, min(_file) AS original_file, count(*)::BIGINT AS identities,
                   sum(read_copies)::BIGINT AS read_copies, sum(read_value_bytes)::BIGINT AS read_value_bytes
            FROM members GROUP BY group_id ORDER BY group_id
        """).to_arrow_table().to_pylist()
        ref_outcomes = dict(con.execute(
            f"WITH classified AS ({ref_classified_sql}) SELECT outcome, count(*) FROM classified GROUP BY outcome"
        ).fetchall())

    actual_members = pq.read_table(work / "members.parquet").to_pylist()
    actual_excluded = sorted(pq.read_table(work / "excluded.parquet").to_pylist(), key=lambda r: r["comment_id"])
    assert actual_members == ref_members
    assert actual_excluded == ref_excluded
    assert planned["groups"] == ref_groups
    assert planned["outcomes"] == ref_outcomes
    assert planned["source_rows"] == len(stage_rows)
    assert planned["source_identities"] == len(witnesses) + 8
    # Verify both row-bucket and byte-bucket splits occurred across multiple files.
    assert sum(1 for g in planned["groups"] if g["original_file"] == "EPA") >= 3
    assert sum(1 for g in planned["groups"] if g["original_file"] == "FDA") >= 2
    assert len(planned["groups"]) > len(agencies)
    # Verify exact Parquet Arrow schemas (integer counts/bytes, string identifiers).
    members_schema = pq.read_schema(work / "members.parquet")
    assert [(f.name, f.type) for f in members_schema] == [
        ("comment_id", pa.string()),
        ("modify_date", pa.string()),
        ("_file", pa.string()),
        ("read_copies", pa.int64()),
        ("read_value_bytes", pa.int64()),
        ("group_id", pa.int64()),
    ]
    excluded_schema = pq.read_schema(work / "excluded.parquet")
    assert [(f.name, f.type) for f in excluded_schema] == [
        ("comment_id", pa.string()),
        ("modify_date", pa.string()),
        ("_file", pa.string()),
        ("read_copies", pa.int64()),
        ("read_value_bytes", pa.int64()),
        ("outcome", pa.string()),
    ]
    # Verify stage_group produces valid checked stages totaling every candidate copy.
    staged_copies = 0
    for g in planned["groups"]:
        staged = groups.stage_group(work, g["group_id"], tmp_path / f"staged-{g['group_id']}")
        assert staged["rows"] == g["read_copies"]
        assert staged["distinct_comment_ids"] == g["identities"]
        staged_copies += staged["rows"]
    assert staged_copies == sum(m["read_copies"] for m in actual_members)


def test_partitioned_plan_handles_zero_candidates_with_valid_empty_members_schema(tmp_path, catalog):
    seed(catalog, [row("N"), row("O")])
    stage = _stage(tmp_path, [
        {"key": "n", "comment_id": "N", "attachments_json": None},
        {"key": "o", "comment_id": "O", "modify_date": "2019-01-01", "attachments_json": LISTED},
        {"key": "x", "comment_id": "X", "attachments_json": LISTED},
    ])
    work = tmp_path / "groups"
    planned = groups.plan(work, stage, max_rows=10, max_read_bytes=100_000)
    assert planned["groups"] == []
    assert planned["outcomes"] == {"no-stated-value": 1, "other-version-only": 1, "absent-from-catalog": 1}
    assert (planned["source_rows"], planned["source_identities"]) == (3, 3)
    members_table = pq.read_table(work / "members.parquet")
    assert members_table.num_rows == 0
    assert [(f.name, f.type) for f in members_table.schema] == [
        ("comment_id", pa.string()),
        ("modify_date", pa.string()),
        ("_file", pa.string()),
        ("read_copies", pa.int64()),
        ("read_value_bytes", pa.int64()),
        ("group_id", pa.int64()),
    ]
    assert pq.read_table(work / "excluded.parquet").num_rows == 3


def test_partitioned_plan_refuses_duplicate_native_ids_in_every_hash_bucket(tmp_path, catalog, monkeypatch):
    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import iceberg

    witnesses = _bucket_witness_ids(groups.CLASSIFY_BUCKETS)
    seed(catalog, [row(cid) for cid in witnesses.values()])
    stage = _stage(
        tmp_path,
        [{"key": f"{cid}.json", "comment_id": cid, "attachments_json": LISTED} for cid in witnesses.values()],
    )
    native = iceberg._qualified(COMMENT)
    for bucket, cid in sorted(witnesses.items()):
        monkeypatch.setattr(
            iceberg,
            "_snapshot_query",
            lambda *_args, _cid=cid, **_kw: (
                f"SELECT * FROM (SELECT * FROM {native} UNION ALL SELECT * FROM {native} WHERE comment_id = '{_cid}')"
            ),
        )
        work = tmp_path / f"dup-bucket-{bucket}"
        with pytest.raises(ValueError, match="appears more than once in the native catalog"):
            groups.plan(work, stage, max_rows=100, max_read_bytes=100_000)
        assert not (work / "groups.json").exists()


@pytest.mark.parametrize("location", ["stage", "index"])
def test_partitioned_plan_refuses_null_comment_id_fail_closed(tmp_path, catalog, location):
    seed(catalog, [row("A")])
    stage = tmp_path / "stage"
    stage.mkdir()
    reads_path = stage / "reads.parquet"
    if location == "stage":
        pl.DataFrame(
            [
                {"key": "a", "comment_id": "A", "modify_date": T0, "attachments_json": LISTED},
                {"key": "null-row", "comment_id": None, "modify_date": T0, "attachments_json": LISTED},
            ],
            schema={c: pl.Utf8 for c in groups.COLUMNS},
        ).write_parquet(reads_path, compression="zstd")
        receipt = {
            "profile": "attachments", "columns": list(groups.COLUMNS), "record_shape": "s5",
            "rows": 2, "distinct_keys": 2, "distinct_comment_ids": 1, "checks": {"synthetic": True},
            "file": {"name": "reads.parquet", "bytes": reads_path.stat().st_size, "sha256": cfw._sha256(reads_path)},
        }
        (stage / "staging.json").write_text(json.dumps(receipt))
        work = tmp_path / "groups-null-stage"
        with pytest.raises(ValueError, match="staged comment row has a NULL comment_id"):
            groups.plan(work, stage, max_rows=10, max_read_bytes=100_000)
        assert not (work / "groups.json").exists()
    else:
        stage = _stage(tmp_path / "valid", [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
        work = tmp_path / "groups-null-index"
        with catalog() as con:
            class InjectNullIndex:
                def execute(self, sql, *args):
                    if sql.startswith("COPY (SELECT n.*"):
                        return con.execute(
                            f"COPY (SELECT 'A'::VARCHAR AS comment_id, '{T0}'::VARCHAR AS modify_date, 'EPA'::VARCHAR AS _file "
                            "UNION ALL SELECT NULL::VARCHAR, NULL::VARCHAR, 'EPA'::VARCHAR) "
                            f"TO {cfw._sql(work / 'catalog-index.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)"
                        )
                    return con.execute(sql, *args)

            with pytest.raises(ValueError, match="native catalog row has a NULL comment_id"):
                groups.plan(work, stage, max_rows=10, max_read_bytes=100_000, con=InjectNullIndex())
        assert not (work / "groups.json").exists()


def test_partitioned_plan_refuses_null_native_file_on_candidate(tmp_path, catalog):
    seed(catalog, [row("A", agency=None)])
    stage = _stage(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    work = tmp_path / "groups-null-file"
    with pytest.raises(ValueError, match="lacks a native file identity"):
        groups.plan(work, stage, max_rows=10, max_read_bytes=100_000)
    assert not (work / "groups.json").exists()
