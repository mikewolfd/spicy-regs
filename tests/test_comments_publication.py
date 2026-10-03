"""Mirror retry/no-op behavior, the agency-first physical layout, and the published members' one typed footer."""

from dataclasses import asdict, replace
import hashlib
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comments_mirror as mirror
from spicy_regs.schemas import COMMENT
from spicy_regs.schemas.regulations import COMMENT_MIRROR_COLUMNS
from spicy_regs.sources import iceberg
from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies
from spicy_regs.transforms.write_staging import write_staging


def build(con, root, resources):
    resources.configure(con, root / "spill")
    from tempfile import TemporaryDirectory
    from pathlib import Path

    with TemporaryDirectory(dir=root) as work:
        staging = Path(work) / "staging"
        stage_comment_agencies(con, "SELECT * FROM source", staging, resources=resources)
        partitions = sort_comment_agencies(staging, root, resources=resources)
    flat = assemble_comments(con, partitions, root, con.sql("SELECT * FROM source").columns, resources=resources)
    index = iceberg._build_comments_index(con, COMMENT, root, source_sql="SELECT * FROM source")
    return {"comments": flat, "partitions": partitions, "index": index}


def test_partitioning_preserves_values_nulls_schema_and_order(tmp_path):
    text = "wide source value " * 150_000
    source = pa.table({"comment_id": ["z", "a", "b", "c"], "agency_code": ["EPA", "EPA", "012", "EPA"],
                       "docket_id": ["EPA-1", "EPA-1", None, None],
                       "posted_date": ["2026-09-01", "2026-09-01", None, None],
                       "text_content": [text, "new", None, "unknown docket"]})
    stale = tmp_path / "comments/agency/agency_code=STALE/part-0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"stale")
    with duckdb.connect() as con:
        con.register("source", source)
        result = build(con, tmp_path, ExportResources("128MB", 1, row_group_bytes="1MB"))
        flat = pq.read_table(result["comments"])
        assert flat.schema == source.schema
        assert sorted(flat.to_pylist(), key=lambda r: r["comment_id"]) == sorted(source.to_pylist(), key=lambda r: r["comment_id"])
        epa = pq.ParquetFile(result["partitions"] / "agency_code=EPA/part-0.parquet").read()
        assert epa["comment_id"].to_pylist() == ["a", "z", "c"]
        assert "agency_code" not in epa.column_names
        assert not stale.exists()
        assert (result["partitions"] / "agency_code=012/part-0.parquet").exists()


@pytest.mark.parametrize("agency", [None, "__HIVE_DEFAULT_PARTITION__", "../bad"])
def test_invalid_partition_preserves_previous_files(tmp_path, agency):
    old = tmp_path / "comments/agency/agency_code=EPA/part-0.parquet"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    with duckdb.connect() as con:
        con.register("source", pa.table({"comment_id": ["a"], "agency_code": pa.array([agency], type=pa.string()),
                                         "docket_id": ["EPA-1"], "posted_date": ["2026-09-01"]}))
        with pytest.raises(ValueError, match="invalid coordinates"):
            build(con, tmp_path, ExportResources("64MB", 1))
    assert old.read_bytes() == b"old"
    assert not list(tmp_path.glob("comments-build-*"))


@pytest.fixture
def publication(tmp_path, monkeypatch):
    snapshot = iceberg.CatalogSnapshot("table-uuid", 41, 0)
    state: dict = {"snapshot": snapshot, "objects": {}, "receipt": None, "builds": 0, "uploads": [], "fail": None, "agency": "EPA", "text": "first"}
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(mirror, "resolve_r2_base_url", lambda: "https://public.example")
    monkeypatch.setattr(mirror, "MIN_EXPECTED_ROWS", 1)
    monkeypatch.setattr(iceberg, "catalog_snapshot", lambda _: state["snapshot"])
    monkeypatch.setattr(iceberg, "rows_unchanged_since", lambda rt, pinned: iceberg.catalog_snapshot(rt) == pinned)
    monkeypatch.setattr(mirror.r2, "read_json_object", lambda _: state["receipt"])
    monkeypatch.setattr(mirror.r2, "object_version", lambda key: state["objects"].get(key))
    monkeypatch.setattr(mirror.r2, "public_object_version", lambda url: state["objects"].get(url.removeprefix("https://public.example/")))
    state["objects"]["comments.parquet"] = {"etag": "prior", "bytes": 99}
    monkeypatch.setattr(mirror, "validate_export", lambda *a, **kw: mirror.PublicPredecessor(state["objects"]["comments.parquet"]["etag"], frozenset({"EPA"})))
    monkeypatch.setattr(mirror.r2, "preflight_uploads", lambda *a: None)

    def export(root, rt, **kw):
        state["builds"] += 1
        root.mkdir(parents=True, exist_ok=True)
        with duckdb.connect() as con:
            con.register("source", pa.table({"comment_id": ["a"], "agency_code": [state["agency"]], "text_content": [state["text"]],
                                             "docket_id": pa.array([None], type=pa.string()),
                                             "posted_date": pa.array([None], type=pa.string())}))
            return build(con, root, ExportResources("64MB", 1))

    def upload(path, remote_key=None, **kw):
        key = remote_key or path.name
        if state["fail"] == key:
            raise OSError("interrupted upload")
        data = path.read_bytes()
        state["objects"][key] = {"etag": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
        state["uploads"].append(key)
        if key == mirror.RECEIPT_KEY:
            state["receipt"] = json.loads(data)

    def verify(path, key, base_url):
        if state["fail"] == "readback":
            raise OSError("readback failed")
        return {**state["objects"][key], "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def partitions(root, files):
        for path in files:
            upload(path, path.relative_to(root).as_posix())
        upload(root / "comments_index.parquet")

    monkeypatch.setattr(iceberg, "export_public_comments", export)
    monkeypatch.setattr(mirror.r2, "upload_file", upload)
    monkeypatch.setattr(mirror.r2, "upload_comment_partitions", partitions)
    monkeypatch.setattr(mirror.r2, "verify_public_file", verify)
    return state, tmp_path


def test_exact_retry_uses_metadata_and_text_only_change_rebuilds(publication):
    state, root = publication
    assert mirror.publish_comments_mirror(root)
    uploads = list(state["uploads"])
    assert uploads[-1] == mirror.RECEIPT_KEY
    assert state["receipt"]["source"] == asdict(state["snapshot"])
    assert not mirror.publish_comments_mirror(root)
    assert state["builds"] == 1 and state["uploads"] == uploads
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["text"] = "repaired attachment text"
    assert mirror.publish_comments_mirror(root)
    assert state["builds"] == 2
    assert pq.read_table(root / "comments.parquet")["text_content"].to_pylist() == [state["text"]]


@pytest.mark.parametrize("change", ["missing", "replaced", "schema", "table", "format", "inventory"])
def test_changed_publication_rebuilds(publication, change):
    state, root = publication
    mirror.publish_comments_mirror(root)
    key = "comments/agency/agency_code=EPA/part-0.parquet"
    if change == "missing":
        del state["objects"][key]
    elif change == "replaced":
        state["objects"][key] = {"etag": "different", "bytes": state["objects"][key]["bytes"]}
    elif change == "schema":
        state["snapshot"] = replace(state["snapshot"], schema_id=1)
    elif change == "table":
        state["snapshot"] = replace(state["snapshot"], table_uuid="new-table")
    elif change == "format":
        state["receipt"]["format_version"] = 0
    elif change == "inventory":
        del state["receipt"]["files"][key]
    assert mirror.publish_comments_mirror(root)
    assert state["builds"] == 2


@pytest.mark.parametrize("failure", ["comments_index.parquet", "readback"])
def test_failed_publication_does_not_advance_receipt_and_retry_finishes(publication, failure):
    state, root = publication
    mirror.publish_comments_mirror(root)
    original = state["receipt"]
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["fail"] = failure
    with pytest.raises(OSError):
        mirror.publish_comments_mirror(root)
    assert state["receipt"] is original
    state["fail"] = None
    assert mirror.publish_comments_mirror(root)
    assert state["receipt"]["source"]["snapshot_id"] == 42


def test_skip_upload_builds_without_changing_receipt(publication):
    state, root = publication
    mirror.publish_comments_mirror(root)
    original, uploads = state["receipt"], list(state["uploads"])
    assert mirror.publish_comments_mirror(root, skip_upload=True)
    assert state["builds"] == 2 and state["uploads"] == uploads
    assert state["receipt"] is original
    assert json.loads((root / "comments-build.json").read_text())["source"] == asdict(state["snapshot"])


def test_moving_catalog_refuses_before_upload(publication, monkeypatch):
    state, root = publication
    values = iter([state["snapshot"], replace(state["snapshot"], snapshot_id=42)])
    monkeypatch.setattr(iceberg, "catalog_snapshot", lambda _: next(values))
    with pytest.raises(RuntimeError, match="Catalog changed"):
        mirror.publish_comments_mirror(root)
    assert state["uploads"] == []


def test_compaction_during_export_publishes_the_pinned_snapshot(publication, monkeypatch):
    """The catalog moved past the pinned snapshot by compaction alone (``iceberg._compacted_only``)."""
    state, root = publication
    checked = []
    monkeypatch.setattr(iceberg, "rows_unchanged_since", lambda rt, s: checked.append(s) or True)
    assert mirror.publish_comments_mirror(root)
    assert checked == [state["snapshot"]]
    assert state["receipt"]["source"] == asdict(state["snapshot"])


def test_force_can_recover_from_an_unreadable_receipt(publication, monkeypatch):
    state, root = publication
    monkeypatch.setattr(mirror.r2, "read_json_object", lambda _: pytest.fail("force read corrupt receipt"))
    assert mirror.publish_comments_mirror(root, force=True)
    assert state["receipt"] is not None


def test_agency_move_clears_old_url_and_keeps_a_valid_receipt(publication):
    state, root = publication
    mirror.publish_comments_mirror(root)
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["agency"] = "FDA"
    assert mirror.publish_comments_mirror(root)
    old = "comments/agency/agency_code=EPA/part-0.parquet"
    new = "comments/agency/agency_code=FDA/part-0.parquet"
    assert pq.ParquetFile(root / old).metadata.num_rows == 0
    assert state["receipt"]["files"][old]["rows"] == 0
    assert state["receipt"]["files"][new]["rows"] == 1
    assert not mirror.publish_comments_mirror(root)


_EXPORT = iceberg.export_public_comments  # the publication fixture fakes it; the member tests below run the real one
_SUBMITTER = ("subtype", "duplicate_comments")


def _comment(comment_id, agency, subtype, count, comment=None):
    row = dict.fromkeys(COMMENT.schema)
    row.update(comment_id=comment_id, agency_code=agency, docket_id=f"{agency}-2025-1", posted_date="2025-01-15T00:00:00Z",
               modify_date="2025-01-15T00:00:00Z", subtype=subtype, duplicate_comments=count, comment=comment)
    return row


def _footer(path) -> dict[str, tuple[str, str, str]]:
    """Each column's Arrow type, Parquet physical type and DuckDB type (DocSpec's lens), in footer order."""
    parquet = pq.ParquetFile(path)
    leaves = [parquet.metadata.schema.column(i) for i in range(parquet.metadata.num_columns)]
    with duckdb.connect() as con:
        bound = con.execute("SELECT name, duckdb_type FROM parquet_schema(?) ORDER BY column_id", [str(path)]).fetchall()[1:]
    arrow = parquet.schema_arrow
    assert arrow.names == [leaf.name for leaf in leaves] == [name for name, _ in bound]
    return {field.name: (str(field.type), leaf.physical_type, kind)
            for field, leaf, (_, kind) in zip(arrow, leaves, bound, strict=True)}


@pytest.fixture(params=["current", "legacy"])
def published(request, publication, monkeypatch):
    """Publish a pinned snapshot through the real export: EPA states both fields (a 0 among them), CMS neither,
    DOT's last rows are gone so its fixed URL is cleared, and a write lands after the pin. A legacy snapshot
    predates both fields."""
    state, root = publication
    catalog = root / "catalog"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{catalog / 'catalog.duckdb'}' AS {iceberg._CATALOG_ALIAS}")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    rows = {"EPA": [_comment("EPA-1", "EPA", "Mass Mail Campaign", 15851, "I&#39;m writing<br/>&amp; asking."),
                    _comment("EPA-2", "EPA", "Public Comment", 0)],
            "CMS": [_comment("CMS-1", "CMS", None, None), _comment("CMS-2", "CMS", None, None)]}
    for agency, agency_rows in rows.items():
        write_staging(agency, COMMENT.name, agency_rows, catalog / "staging", COMMENT.schema)
    iceberg.merge_comments(catalog / "staging", COMMENT)
    live = iceberg._qualified(COMMENT)
    projection = ", ".join(f'"{c}"' for c in COMMENT.schema if request.param == "current" or c not in _SUBMITTER)
    with connect() as con:
        # The pinned version's stand-in; the live table then moves past it.
        con.execute(f"CREATE TABLE {iceberg._CATALOG_ALIAS}.pinned_{state['snapshot'].snapshot_id} AS "
                    f"SELECT {projection} FROM {live}")
        con.execute(f"INSERT INTO {live} (comment_id, agency_code, docket_id, posted_date) "
                    "VALUES ('late', 'EPA', 'EPA-2025-1', '2025-01-16T00:00:00Z')")
    scans = []

    def snapshot_query(rt, snapshot):
        scans.append(snapshot)
        return f"SELECT * FROM {iceberg._CATALOG_ALIAS}.pinned_{snapshot.snapshot_id}"

    monkeypatch.setattr(iceberg, "_read_snapshot", lambda con, rt: state["snapshot"])
    monkeypatch.setattr(iceberg, "_snapshot_query", snapshot_query)
    monkeypatch.setattr(iceberg, "export_public_comments", _EXPORT)
    monkeypatch.setattr(mirror, "validate_export", lambda *a, **kw: mirror.PublicPredecessor("prior", frozenset({"EPA", "DOT"})))
    assert mirror.publish_comments_mirror(root, resources=ExportResources("64MB", 1))
    return request.param, state, root, scans


def test_duplicate_comments_is_int32_in_every_published_member(published):
    """spicy-docs 0.52.0 types it INTEGER; DocSpec refuses a BIGINT footer."""
    _, state, root, _ = published
    tables = [key for key in state["receipt"]["files"] if key != "comments_index.parquet"]
    assert len(tables) == 4
    for key in tables:
        assert _footer(root / key)["duplicate_comments"] == ("int32", "INT32", "INTEGER"), key


def test_submitter_fields_are_typed_where_every_value_is_null(published):
    """An untyped NULL column is refused: an agency (or a snapshot) that states neither field still writes them typed."""
    shape, state, root, _ = published
    stated = shape == "current"
    flat = {row["comment_id"]: (row["subtype"], row["duplicate_comments"]) for row in pq.read_table(root / "comments.parquet").to_pylist()}
    assert flat == {"EPA-1": ("Mass Mail Campaign", 15851) if stated else (None, None),
                    "EPA-2": ("Public Comment", 0) if stated else (None, None),
                    "CMS-1": (None, None), "CMS-2": (None, None)}
    for agency in ("CMS", "DOT") if stated else ("CMS", "DOT", "EPA"):
        path = root / f"comments/agency/agency_code={agency}/part-0.parquet"
        assert all(value is None for row in pq.read_table(path, columns=list(_SUBMITTER)).to_pylist() for value in row.values())
        footer = _footer(path)
        assert footer["subtype"] == ("string", "BYTE_ARRAY", "VARCHAR"), agency
        assert footer["duplicate_comments"] == ("int32", "INT32", "INTEGER"), agency


def test_every_member_is_republished_together_from_the_pinned_snapshot(published):
    """DocSpec refuses a member set whose footers disagree; the index must count the members' own snapshot."""
    _, state, root, scans = published
    receipt = state["receipt"]
    assert scans == [state["snapshot"]] and receipt["source"] == asdict(state["snapshot"])
    agencies = {code: f"comments/agency/agency_code={code}/part-0.parquet" for code in ("CMS", "DOT", "EPA")}
    assert receipt["files"].keys() == {"comments.parquet", "comments_index.parquet", *agencies.values()}
    flat = _footer(root / "comments.parquet")
    assert list(flat) == [*COMMENT.schema, *COMMENT_MIRROR_COLUMNS]
    del flat["agency_code"]  # the partition key, carried by each agency file's path
    for key in agencies.values():
        assert list(_footer(root / key).items()) == list(flat.items()), key
    # Every member counts the pinned rows; none sees the write that landed after the pin.
    with duckdb.connect() as con:
        index = dict(con.execute("SELECT agency_code, sum(row_count)::INTEGER FROM read_parquet(?) GROUP BY 1",
                                 [str(root / "comments_index.parquet")]).fetchall())
    assert index == {"CMS": 2, "EPA": 2}
    assert {code: receipt["files"][key]["rows"] for code, key in agencies.items()} == {**index, "DOT": 0}
    assert receipt["files"]["comments.parquet"]["rows"] == sum(index.values())


def test_comment_text_is_the_body_read_as_plain_text_in_every_member(published):
    """The publisher serves the body as HTML; the mirror adds its text and keeps the publisher's bytes beside it."""
    _, state, root, _ = published
    flat = {row["comment_id"]: (row["comment"], row["comment_text"])
            for row in pq.read_table(root / "comments.parquet", columns=["comment_id", "comment", "comment_text"]).to_pylist()}
    assert flat["EPA-1"] == ("I&#39;m writing<br/>&amp; asking.", "I'm writing\n& asking.")
    assert flat["EPA-2"] == (None, None)
    epa = pq.read_table(root / "comments/agency/agency_code=EPA/part-0.parquet", columns=["comment_id", "comment_text"])
    assert dict(zip(*epa.to_pydict().values())) == {"EPA-1": "I'm writing\n& asking.", "EPA-2": None}
    assert all(_footer(root / key)["comment_text"] == ("string", "BYTE_ARRAY", "VARCHAR")
               for key in state["receipt"]["files"] if key != "comments_index.parquet")


def _member(path, rows: int, row_group: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"comment_id": [str(i) for i in range(rows)]}), path, row_group_size=row_group)
    return {"rows": rows}


def test_a_receipt_whose_agency_rows_do_not_sum_to_the_monolith_is_refused(tmp_path):
    files = {"comments.parquet": _member(tmp_path / "comments.parquet", 3, 2),
             "comments_index.parquet": _member(tmp_path / "comments_index.parquet", 1, 1),
             "comments/agency/agency_code=EPA/part-0.parquet":
                 _member(tmp_path / "comments/agency/agency_code=EPA/part-0.parquet", 2, 1)}
    with pytest.raises(RuntimeError, match="agency files hold 2 rows; the export validated 3"):
        mirror.check_receipt_rows(tmp_path, files, 3)
    files["comments/agency/agency_code=CMS/part-0.parquet"] = _member(
        tmp_path / "comments/agency/agency_code=CMS/part-0.parquet", 1, 1)
    mirror.check_receipt_rows(tmp_path, files, 3)
    with pytest.raises(RuntimeError, match="comments.parquet states 3 rows; the export validated 4"):
        mirror.check_receipt_rows(tmp_path, files, 4)
    files["comments_index.parquet"]["rows"] = 2
    with pytest.raises(RuntimeError, match="comments_index.parquet states 2 rows and its row groups hold 1"):
        mirror.check_receipt_rows(tmp_path, files, 3)


def test_a_refused_receipt_uploads_nothing(publication, monkeypatch):
    """The row check runs on the local files before any upload, so a refusal leaves the public files untouched."""
    state, root = publication
    monkeypatch.setattr(mirror, "check_receipt_rows", lambda *a: (_ for _ in ()).throw(RuntimeError("Refusing the comments receipt: test")))
    with pytest.raises(RuntimeError, match="Refusing the comments receipt"):
        mirror.publish_comments_mirror(root)
    assert state["uploads"] == [] and state["receipt"] is None


def test_export_refuses_a_catalog_moved_from_the_pinned_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg, "_connect", duckdb.connect)
    monkeypatch.setattr(iceberg, "_read_snapshot", lambda con, rt: iceberg.CatalogSnapshot("table-uuid", 42, 0))
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda *a: pytest.fail("scanned a snapshot other than the pinned one"))
    with pytest.raises(RuntimeError, match="changed before export"):
        iceberg.export_public_comments(tmp_path, COMMENT, snapshot=iceberg.CatalogSnapshot("table-uuid", 41, 0))
    assert not list(tmp_path.rglob("*.parquet"))
