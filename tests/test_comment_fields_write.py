"""The comment fill's write phase against a local DuckDB catalog: every guard, each able to catch a wrong write.

Plain DuckDB has no data files, so ``FILE_COLUMN`` stands in with ``agency_code`` (one "file" per agency) and the
snapshot is a digest of the table's rows. ``test_comment_fields_iceberg.py`` runs the same write on a real Iceberg
catalog, where rows move files and commits carry parents and summaries.
"""

import json
import re
from pathlib import Path

import duckdb
import polars as pl
import pytest

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines import comment_fields_write as cfw
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg



T0 = "2020-01-01T00:00:00Z"

# One comment's attachments in both shapes. HELD is the rule before SpicyDocs b19b092 (2026-03-15 to 0.53.0): only
# downloadable renditions, an entry with none dropped. LISTED is b19b092's: every attachment the record lists, a
# withheld one with ``formats`` null and its restriction, a rendition with no file with ``url`` null.
_PDF = {"url": "https://downloads.regulations.gov/EPA-X-0001-0002/attachment_1.pdf", "format": "pdf", "size": 1000}
HELD = json.dumps([{"title": "Letter", "formats": [_PDF]}])
LISTED = json.dumps([
    {"title": "Letter", "formats": [_PDF, {"url": None, "format": "docx", "size": 900}]},
    {"title": "Study", "formats": None, "restrictReasonType": "Copyrighted", "restrictReason": "Copyrighted material"},
])


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    path = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS {iceberg._CATALOG_ALIAS}")
        return con

    def snapshot(con, record_type, **_options):
        digest = con.execute(f"SELECT coalesce(sum(hash(t)), 0) FROM {iceberg._qualified(record_type)} t").fetchone()[0]
        return iceberg.CatalogSnapshot("local", int(digest % 2**62), 1)

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setattr(iceberg, "_read_snapshot", snapshot)
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, _, **kw: f"SELECT * FROM {iceberg._qualified(rt)}")
    monkeypatch.setattr(cfw, "FILE_COLUMN", "agency_code")
    files = lambda con: {r[0] for r in con.execute(  # noqa: E731
        f"SELECT DISTINCT agency_code FROM {iceberg._qualified(COMMENT)}").fetchall()}
    monkeypatch.setattr(cfw, "live_files", files)
    monkeypatch.setattr(cfw, "data_files", lambda con: dict.fromkeys(files(con), 1000))
    monkeypatch.setattr(cfw, "commit_record", lambda con, parent_id: None)  # plain DuckDB records no commits
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    monkeypatch.setenv(iceberg.CATALOG_LOCK_ENV, "test")
    with connect() as con:
        iceberg._ensure_table(con, COMMENT)
    return connect


def row(comment_id, agency="EPA", modify=T0, **values):
    return {**dict.fromkeys(COMMENT.schema), "comment_id": comment_id, "agency_code": agency, "modify_date": modify,
            "comment": f"body of {comment_id}", "text_content": f"text of {comment_id}", "title": "t", **values}


def seed(connect, rows):
    with connect() as con:
        con.register("seed", pl.DataFrame(rows, schema=dict.fromkeys(COMMENT.schema, pl.Utf8)).to_arrow())
        iceberg.replace_rows(con, COMMENT, "seed")


def reads(tmp_path, rows, agency="EPA"):
    """One read part, named as ``read`` names this record shape's parts."""
    part = tmp_path / "parts" / f"agency={agency}" / f"p-{cf.RECORD_SHAPE}-{len(list(tmp_path.glob('parts/*/*'))):05d}.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    full = [{**dict.fromkeys(cf.PART_SCHEMA), "agency_code": agency, "modify_date": T0, **r} for r in rows]
    pl.DataFrame(full, schema=cf.PART_SCHEMA).write_parquet(part)


def table(connect) -> dict[str, dict]:
    with connect() as con:
        from spicy_regs.sources.regulatory_catalog import processing_table
        return {r["comment_id"]: r for r in con.execute(f"SELECT * FROM {processing_table(con, COMMENT)}").pl().to_dicts()}


def fill(tmp_path, connect, **options):
    prepared = cfw.prepare(tmp_path)
    with connect() as con:
        return prepared, cfw.write(tmp_path, con=con, **options)


class Proxy:
    """A connection whose statements can be rewritten, to model a faulty engine or a crash."""

    def __init__(self, con, rewrite=None, after=None):
        self.con, self.rewrite, self.after = con, rewrite, after

    def execute(self, sql, *args):
        result = self.con.execute(self.rewrite(sql) if self.rewrite else sql, *args)
        if self.after:
            self.after(sql)
        return result

    def __getattr__(self, name):
        return getattr(self.con, name)


def journal(tmp_path) -> list[dict]:
    (path,) = (tmp_path / "fill").glob("write-journal-*.jsonl")
    return [json.loads(line) for line in path.read_text().splitlines()]


# --------------------------------------------------------------------------- #
# The rules
# --------------------------------------------------------------------------- #
def test_only_null_cells_are_filled_and_a_stated_zero_is_a_value(tmp_path, catalog):
    seed(catalog, [row("A", subtype="Kept"), row("B"), row("C")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "subtype": "Read", "duplicate_comments": 3, "attachments_json": '[{"title":"attachment"}]'},
        {"key": "b", "comment_id": "B", "subtype": "Public Comment", "duplicate_comments": 0},
        {"key": "c", "comment_id": "C"},
    ])
    prepared, written = fill(tmp_path, catalog)
    rows = table(catalog)
    assert rows["A"]["subtype"] == "Kept"
    assert (rows["A"]["duplicate_comments"], rows["A"]["attachments_json"]) == (3, '[{"title":"attachment"}]')
    assert (rows["B"]["subtype"], rows["B"]["duplicate_comments"]) == ("Public Comment", 0)
    assert rows["C"]["duplicate_comments"] is None
    assert (rows["A"]["comment"], rows["A"]["text_content"]) == ("body of A", "text of A")
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (2, 2)
    assert prepared["cells_by_column"] == {**dict.fromkeys(cfw.FILL_COLUMNS, 0), "subtype": 1,
                                           "duplicate_comments": 2, "attachments_json": 1}


def test_submitter_fields_the_row_predates_are_filled_and_a_stated_one_is_kept(tmp_path, catalog):
    """Rows ingested before the extract mapped the submitter (ff812e5, 2026-06-15) hold NULL where the object states
    a name: FWS-HQ-ES-2025-0034-212973 states Earthjustice / Kristen / Boyles at the publisher and NULL in the catalog.
    The read copy fills them; a value the catalog already states is never overwritten."""
    seed(catalog, [row("A"), row("B", organization="Kept, Inc.")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "organization": "Earthjustice", "first_name": "Kristen", "last_name": "Boyles"},
        {"key": "b", "comment_id": "B", "organization": "Other"},
    ])
    prepared, written = fill(tmp_path, catalog)
    rows = table(catalog)
    assert (rows["A"]["organization"], rows["A"]["first_name"], rows["A"]["last_name"]) == ("Earthjustice", "Kristen", "Boyles")
    assert rows["B"]["organization"] == "Kept, Inc."
    assert {c: prepared["cells_by_column"][c] for c in ("organization", "first_name", "last_name")} == {
        "organization": 1, "first_name": 1, "last_name": 1}
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (1, 1)


def test_prepare_leaves_only_its_outputs_where_the_artifact_collects_them(tmp_path, catalog):
    """Its whole-table intermediates stream through ``fill/work`` and go: the artifact uploads ``fill/*.parquet``."""
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}, {"key": "a(1)", "comment_id": "A", "subtype": "T"},
                     {"key": "b", "comment_id": "B", "subtype": "S"}])
    prepared = cfw.prepare(tmp_path)
    assert (prepared["rows_to_fill"], prepared["conflicted_versions_by_column"]["subtype"]) == (1, 1)
    assert sorted(path.name for path in (tmp_path / "fill").glob("*.parquet")) == [
        "conflicts.parquet", "files.parquet", "fill.parquet", "refusals.parquet"]
    assert not (tmp_path / "fill" / "work").exists()


def test_prepare_uses_explicit_resource_targets_through_actual_catalog_read(tmp_path, catalog):
    from spicy_regs.duckdb_settings import ExportResources

    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED},
                     {"key": "b", "comment_id": "B", "modify_date": "2021-01-01", "attachments_json": LISTED}])
    with catalog() as con:
        prepared = cfw.prepare(tmp_path, profile="attachments", con=con,
                               resources=ExportResources(memory="128MiB", threads=1))
        memory, threads = con.execute("SELECT current_setting('memory_limit'), current_setting('threads')").fetchall()[0]
        assert memory == "128.0 MiB"
        assert threads == 1
    assert prepared["rows_to_fill"] == 1
    assert prepared["other_version_only"] == 1
    assert pl.read_parquet(tmp_path / "fill" / "fill.parquet")["attachments_json"].to_list() == [LISTED]
    assert all(value["attachments_json"] is None for value in table(catalog).values())


def test_a_read_of_another_version_fills_nothing(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2021-06-01T00:00:00Z", "subtype": "Read"}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["subtype"] is None
    assert (prepared["other_version_only"], prepared["rows_to_fill"], written["rows_changed"]) == (1, 0, 0)


def test_copies_that_disagree_on_one_column_block_only_that_column(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "subtype": "S", "attachments_json": '[{"title":"attachment"}]'},
        {"key": "a(1)", "comment_id": "A", "subtype": "S", "attachments_json": "[2]"},
    ])
    prepared, _ = fill(tmp_path, catalog)
    assert (table(catalog)["A"]["subtype"], table(catalog)["A"]["attachments_json"]) == ("S", None)
    conflicts = pl.read_parquet(tmp_path / "fill" / "conflicts.parquet").to_dicts()
    assert [(c["comment_id"], c["column"], list(c["keys"])) for c in conflicts] == [("A", "attachments_json",
                                                                                     ["a", "a(1)"])]
    assert prepared["conflicted_versions_by_column"]["attachments_json"] == 1


def test_a_read_can_add_metadata_to_an_id_bearing_attachment_without_changing_the_id():
    previous = {"title": "held", "formats": None, "attachment_id": "a"}
    held = json.dumps([previous])
    added = previous | {"agencyNote": "note", "authors": ["A", "A"], "docOrder": 0}
    assert cfw.only_adds_attachments(json.dumps([added]), held)
    assert not cfw.only_adds_attachments(json.dumps([added | {"attachment_id": "b"}]), held)
    assert not cfw.only_adds_attachments(json.dumps([added | {"title": "changed"}]), held)


def test_v2_promotion_fill_compares_all_historical_attachment_values():
    previous = {"title": "café", "formats": None, "attachment_id": None,
                "restrict_reason": None, "restrict_reason_type": "Copyrighted"}
    held = json.dumps([previous], ensure_ascii=False, separators=(",", ":"))
    added = {"title": "café", "formats": None, "restrictReasonType": "Copyrighted",
             "attachment_id": "a", "authors": ["A", "A"], "docOrder": 0}
    assert not cfw.only_adds_attachments(json.dumps([added]), held)
    assert cfw.only_adds_attachments(json.dumps([added]), held, True)
    assert not cfw.only_adds_attachments(json.dumps([added | {"restrictReasonType": "Other"}]), held, True)
    assert not cfw.only_adds_attachments(json.dumps([added | {"title": "different"}]), held, True)
    held_with_id = json.dumps([previous | {"attachment_id": "prior"}], ensure_ascii=False, separators=(",", ":"))
    assert not cfw.only_adds_attachments(json.dumps([added]), held_with_id, True)


def test_a_read_that_only_adds_withheld_attachments_replaces_the_held_list(tmp_path, catalog):
    """The 2026-10 re-read: the held list is the read's with its withheld entries and file-less renditions removed."""
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == LISTED
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (1, 1)
    assert (prepared["replaced_by_column"], prepared["cells_by_column"]["attachments_json"]) == ({"attachments_json": 1}, 0)
    assert prepared["refused_not_additive_by_column"] == {"attachments_json": 0}


def test_the_downloadable_part_of_a_listed_value_is_the_earlier_rules_value():
    """Both shapes: b19b092's value less what offers no file is the earlier rule's, and the earlier rule's is its own."""
    assert cfw.downloadable_attachments(LISTED) == HELD
    assert cfw.downloadable_attachments(HELD) == HELD
    only_withheld = json.dumps([{"title": "Study", "formats": None, "restrictReasonType": "Copyrighted"}])
    assert cfw.downloadable_attachments(only_withheld) is None  # the earlier rule wrote NULL for it
    for other in ("{}", "[1]", '[{"title": "x"}]', "not json", None):
        assert cfw.downloadable_attachments(other) is None


def test_the_installed_extract_projects_to_the_earlier_rules_value():
    """Whichever SpicyDocs is installed, before or after b19b092 or 0.85.0 (the entry's ``attachment_id``), its value for
    a record listing a withheld attachment and a rendition with no file projects to the earlier rule's value."""
    record = {"data": {"id": "EPA-X-0001-0002", "attributes": {"modifyDate": T0}}, "included": [
        {"id": "09000064b8f4997d", "type": "attachments", "attributes": {"title": "Letter", "fileFormats": [
            {"fileUrl": _PDF["url"], "format": "pdf", "size": 1000}, {"fileUrl": None, "format": "docx", "size": 900}]}},
        {"id": "09000064b8f4997e", "type": "attachments", "attributes": {"title": "Study", "fileFormats": None,
                                               "restrictReasonType": "Copyrighted",
                                               "restrictReason": "Copyrighted material"}},
    ]}
    assert cfw.downloadable_attachments(COMMENT.extract(record)["attachments_json"]) == HELD


def _identified(listed: str) -> str:
    """``listed`` as SpicyDocs 0.85.0 spells it: each entry ends with its own ``attachment_id``."""
    return json.dumps([{**entry, "attachment_id": f"0900006400000{n:03d}"} for n, entry in enumerate(json.loads(listed))])


# b19b092-to-0.84.0 lists (no id): one holding a withheld entry, one a rendition with no file.
_WITHHELD_LISTED = json.dumps([
    {"title": "Letter", "formats": [_PDF]},
    {"title": "Study", "formats": None, "restrictReasonType": "Copyrighted", "restrictReason": "Copyrighted material"},
])
_URLLESS_LISTED = json.dumps([{"title": "Letter", "formats": [_PDF, {"url": None, "format": "docx", "size": 900}]}])


@pytest.mark.parametrize(("held", "read"), [
    (_WITHHELD_LISTED, _identified(_WITHHELD_LISTED)),  # held by 0.54.0-0.84.0
    (_URLLESS_LISTED, _identified(_URLLESS_LISTED)),  # held by 0.54.0-0.84.0
    (LISTED, _identified(LISTED)),  # held by 0.54.0-0.84.0, both at once
    (HELD, _identified(LISTED)),  # held before b19b092
    (HELD, LISTED),  # held before b19b092, read by 0.54.0-0.84.0
], ids=["084-withheld", "084-urlless", "084-both", "pre-b19b092", "pre-b19b092-read-084"])
def test_a_read_that_only_adds_ids_or_withheld_entries_replaces_either_eras_list(tmp_path, catalog, held, read):
    """0.85.0 adds each entry's ``attachment_id``: a list held in either earlier spelling is replaced by the full read."""
    assert cfw.only_adds_attachments(read, held)
    seed(catalog, [row("A", attachments_json=held)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": read}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == read
    assert (prepared["replaced_by_column"], prepared["refused_not_additive_by_column"], written["rows_changed"]) == (
        {"attachments_json": 1}, {"attachments_json": 0}, 1)


_CHANGED = json.dumps([{**entry, "formats": [{**_PDF, "url": _PDF["url"] + "?v=2"}] if entry["formats"] else None}
                       for entry in json.loads(_WITHHELD_LISTED)])
_DROPPED = json.dumps(json.loads(_WITHHELD_LISTED)[:1])


@pytest.mark.parametrize(("held", "read"), [
    (_WITHHELD_LISTED, _identified(_CHANGED)),  # held by 0.54.0-0.84.0; the read changes a file as well as adding ids
    (HELD, _identified(_CHANGED)),  # held before b19b092; the same read
    (_WITHHELD_LISTED, _identified(_DROPPED)),  # held by 0.54.0-0.84.0; the read no longer lists the withheld entry
], ids=["084-changed-file", "pre-b19b092-changed-file", "084-entry-gone"])
def test_a_read_that_changes_more_than_an_earlier_spelling_is_refused_in_either_era(tmp_path, catalog, held, read):
    assert not cfw.only_adds_attachments(read, held)
    seed(catalog, [row("A", attachments_json=held)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": read}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == held
    assert (prepared["refused_not_additive_by_column"], written["rows_changed"]) == ({"attachments_json": 1}, 0)


@pytest.mark.parametrize("read", [
    json.dumps([{"title": "Letter", "formats": [{**_PDF, "url": _PDF["url"] + "?v=2"}]}]),  # a changed file
    json.dumps([{"title": "Study", "formats": None, "restrictReasonType": "Copyrighted"}]),  # the held entry gone
    json.dumps([{"title": "Letter", "formats": [_PDF]}], separators=(",", ":")),  # another spelling of the same list
    json.dumps([{"formats": [_PDF], "title": "Letter"}]),  # the same entry, its keys reordered
    json.dumps([{"title": "Letter", "formats": [{**_PDF, "checksum": "x"}]}]),  # a rendition with more than a file
])
def test_a_read_that_does_more_than_add_is_refused_and_counted(tmp_path, catalog, read):
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": read}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == HELD
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (0, 0)
    assert prepared["refused_not_additive_by_column"] == {"attachments_json": 1}
    assert pl.read_parquet(tmp_path / "fill" / "refusals.parquet").to_dicts() == [
        {"comment_id": "A", "modify_date": T0, "column": "attachments_json"}]


def test_a_read_of_another_version_replaces_nothing(tmp_path, catalog):
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2021-06-01T00:00:00Z", "attachments_json": LISTED}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == HELD
    assert (prepared["other_version_only"], prepared["replaced_by_column"], written["rows_changed"]) == (
        1, {"attachments_json": 0}, 0)


def test_copies_that_disagree_replace_nothing(tmp_path, catalog):
    longer = json.dumps([*json.loads(LISTED), {"title": "Annex", "formats": None, "restrictReasonType": "Other"}])
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED},
                     {"key": "a(1)", "comment_id": "A", "attachments_json": longer}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == HELD
    assert prepared["conflicted_versions_by_column"]["attachments_json"] == 1
    assert (prepared["replaced_by_column"], prepared["refused_not_additive_by_column"]) == (
        {"attachments_json": 0}, {"attachments_json": 0})


def test_a_null_held_list_still_fills_and_an_equal_one_is_left_alone(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", attachments_json=LISTED)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED},
                     {"key": "b", "comment_id": "B", "attachments_json": LISTED}])
    prepared, written = fill(tmp_path, catalog)
    assert (table(catalog)["A"]["attachments_json"], table(catalog)["B"]["attachments_json"]) == (LISTED, LISTED)
    assert (prepared["cells_by_column"]["attachments_json"], prepared["replaced_by_column"]) == (
        1, {"attachments_json": 0})
    assert written["rows_changed"] == 1


def test_a_replacement_is_written_only_over_the_value_it_replaces(tmp_path, catalog):
    """A stale fill row expecting another held value matches no MERGE update, so the cell keeps what it holds."""
    seed(catalog, [row("A", attachments_json=HELD), row("B", attachments_json="[]")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)  # B holds "[]"
    _restamp(tmp_path)
    with catalog() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
    assert (table(catalog)["A"]["attachments_json"], table(catalog)["B"]["attachments_json"]) == (LISTED, "[]")


def _is_filled_copy(sql: str) -> bool:
    return sql.lstrip().startswith("COPY (") and "-filled.parquet" in sql


def _strip_filled_where(sql: str) -> str:
    return re.sub(r" WHERE .+?\) TO ", ") TO ", sql, count=1)


def test_a_merge_that_replaces_without_the_held_guard_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A", attachments_json=HELD), row("B", attachments_json="[]")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)
    _restamp(tmp_path)
    unguarded = lambda sql: re.sub(  # noqa: E731
        r'WHEN p\."attachments_json" = b\."_held_attachments_json" THEN', "WHEN TRUE THEN",
        _strip_filled_where(sql)) if _is_filled_copy(sql) else sql
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=unguarded))
    assert (table(catalog)["A"]["attachments_json"], table(catalog)["B"]["attachments_json"]) == (HELD, "[]")


def test_a_stated_null_disagreeing_with_a_value_is_a_conflict(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": None}, {"key": "a(1)", "comment_id": "A", "subtype": "S"}])
    fill(tmp_path, catalog)
    assert table(catalog)["A"]["subtype"] is None


# --------------------------------------------------------------------------- #
# The guards: each test is a wrong write that must be refused before COMMIT.
# --------------------------------------------------------------------------- #
def _merge(sql: str) -> bool:
    return sql.lstrip().startswith("MERGE INTO")


def test_a_merge_that_damages_a_non_fill_column_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}, {"key": "b", "comment_id": "B", "subtype": "S"}])
    cfw.prepare(tmp_path)
    blank = lambda sql: sql.replace('"text_content"=s."text_content"', '"text_content"=NULL') if _merge(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="differ"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=blank))
    assert {r["text_content"] for r in table(catalog).values()} == {"text of A", "text of B"}  # nothing committed
    assert journal(tmp_path)[-1]["state"] == "failed" and journal(tmp_path)[-1]["committed"] is False


def test_an_overwriting_merge_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A", subtype="Kept")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "Read", "duplicate_comments": 1}])
    cfw.prepare(tmp_path)
    overwrite = lambda sql: sql.replace('COALESCE(p."subtype", CAST(b."subtype" AS VARCHAR))', 'b."subtype"') if _is_filled_copy(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=overwrite))
    assert table(catalog)["A"]["subtype"] == "Kept"


def test_a_merge_that_rewrites_rows_needing_nothing_is_refused_by_the_count(tmp_path, catalog):
    """Rewriting a row identically leaves every value right; only the planned count sees the extra row."""
    seed(catalog, [row("A"), row("B", subtype="S", duplicate_comments="0")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    extra = pl.read_parquet(fill_file)
    stale = extra.with_columns(pl.lit("B").alias("comment_id"), pl.lit(None, pl.Utf8).alias("subtype"))
    pl.concat([extra, stale]).write_parquet(fill_file)  # B needs nothing: a stale fill row
    _restamp(tmp_path)
    unguarded = lambda sql: _strip_filled_where(sql) if _is_filled_copy(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="planned"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=unguarded))


def test_a_stale_row_that_needs_nothing_is_left_alone(tmp_path, catalog):
    """A fill row whose cells the catalog already holds matches no MERGE update: the guard, not the count, skips it."""
    seed(catalog, [row("A"), row("B", subtype="S", duplicate_comments="0")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)
    _restamp(tmp_path)
    with catalog() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
    assert journal(tmp_path)[-1]["state"] == "verified"


def test_a_merge_that_ignores_modify_date_is_refused(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", modify="2021-06-01T00:00:00Z")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)  # B at T0: stale
    _restamp(tmp_path)
    loose = lambda sql: sql.replace(' AND p.modify_date IS NOT DISTINCT FROM b.modify_date', '') if _is_filled_copy(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=loose))
    with catalog() as con:
        cfw.write(tmp_path, con=con, clear_failure=True)  # the true MERGE leaves the other version alone
    assert (table(catalog)["A"]["subtype"], table(catalog)["B"]["subtype"]) == ("S", None)


def _restamp(tmp_path: Path) -> None:
    """Point prepare.json at a hand-edited fill.parquet, as a stale fill would be."""
    path = tmp_path / "fill" / "prepare.json"
    prepared = json.loads(path.read_text())
    prepared["fill_sha256"] = cfw._sha256(tmp_path / "fill" / "fill.parquet")
    path.write_text(json.dumps(prepared))


# --------------------------------------------------------------------------- #
# Fail-closed, resume and refusals
# --------------------------------------------------------------------------- #
def test_a_failed_batch_stops_every_later_run_until_cleared(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    blank = lambda sql: sql.replace('"comment"=s."comment"', '"comment"=NULL') if _merge(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=blank))
    with catalog() as con, pytest.raises(RuntimeError, match="clear-failure"):
        cfw.write(tmp_path, con=con)
    with catalog() as con:
        assert cfw.write(tmp_path, con=con, clear_failure=True)["rows_changed"] == 1
    assert table(catalog)["A"]["subtype"] == "S"


def test_the_preimage_is_on_disk_before_the_merge(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    seen = []
    watch = lambda sql: seen.append(list((tmp_path / "fill" / "preimage").glob("*.parquet"))) if _merge(sql) else None  # noqa: E731
    with catalog() as con:
        cfw.write(tmp_path, con=Proxy(con, after=watch))
    (preimage,) = seen[0]
    assert pl.read_parquet(preimage).to_dicts()[0]["subtype"] is None  # the row as it was before


def test_passed_work_is_kept_by_file_across_a_changed_batch_budget(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", agency="CMS"), row("C", agency="FDA")])
    for agency, cid in (("EPA", "A"), ("CMS", "B"), ("FDA", "C")):
        reads(tmp_path, [{"key": cid, "comment_id": cid, "subtype": "S"}], agency=agency)
    cfw.prepare(tmp_path)
    fail_second = {"n": 0}

    def crash(sql):
        if _merge(sql):
            fail_second["n"] += 1
            if fail_second["n"] == 2:
                raise ConnectionError("network")

    with catalog() as con, pytest.raises(ConnectionError):
        cfw.write(tmp_path, con=Proxy(con, after=crash), batch_bytes=1000)
    with catalog() as con:
        again = cfw.write(tmp_path, con=con, batch_bytes=10**9)
    assert again["files_skipped_verified"] == 1 and again["rows_changed"] == 2
    assert {r["subtype"] for r in table(catalog).values()} == {"S"}


def test_another_writer_between_batches_stops_the_run_and_a_new_prepare_resumes(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(tmp_path)
    seed(catalog, [row("Z")])  # an ETL commit lands after prepare
    with catalog() as con, pytest.raises(RuntimeError, match="another writer"):
        cfw.write(tmp_path, con=con)
    _, written = fill(tmp_path, catalog)
    assert written["rows_changed"] == 2


def test_the_write_needs_the_lock_the_columns_and_for_a_pilot_a_scope(tmp_path, catalog, monkeypatch):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    with catalog() as con, pytest.raises(RuntimeError, match="scoped pilot"):
        cfw.write(tmp_path, con=con, by_file=False)
    monkeypatch.delenv(iceberg.CATALOG_LOCK_ENV)
    with catalog() as con, pytest.raises(RuntimeError, match="lock"):
        cfw.write(tmp_path, con=con)


def test_a_catalog_without_the_new_columns_is_refused_not_migrated(tmp_path, catalog):
    with catalog() as con:
        con.execute(f"ALTER TABLE {iceberg._qualified(COMMENT)} DROP COLUMN subtype")
    with pytest.raises(ValueError, match="schema differs"):
        cfw.prepare(tmp_path)
    with catalog() as con:
        assert "subtype" not in iceberg._column_types(con, COMMENT)


def test_a_scoped_pilot_writes_one_batch(tmp_path, catalog):
    seed(catalog, [row("A", docket_id="D-1"), row("B", agency="CMS", docket_id="D-2")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(tmp_path, scope={"docket_id": "D-1"})
    with catalog() as con:
        assert cfw.write(tmp_path, con=con, by_file=False)["batches"] == 1
    assert (table(catalog)["A"]["subtype"], table(catalog)["B"]["subtype"]) == ("S", None)


@pytest.mark.parametrize("scope", [None, {"docket_id": "D-1"}])
def test_prepare_restores_only_stated_matching_versions_without_losing_scope_counts(tmp_path, catalog, monkeypatch, scope):
    from spicy_regs.sources import regulatory_catalog as native

    seed(catalog, [row(cid, docket_id="D-1") for cid in ("A", "unread", "old", "null", "conflict")]
         + [row("outside", docket_id="D-2")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "subtype": "S"},
        {"key": "old", "comment_id": "old", "modify_date": "another version", "subtype": "S"},
        {"key": "null", "comment_id": "null"},
        {"key": "conflict1", "comment_id": "conflict", "subtype": "S"},
        {"key": "conflict2", "comment_id": "conflict", "subtype": "T"},
        {"key": "outside", "comment_id": "outside", "subtype": "S"},
    ])
    # An unrelated broken receipt must not be opened merely to count an unread native key.
    with catalog() as con:
        con.execute(f"UPDATE {native.receipts_table()} SET subject_version='unsealed damage' "
                    "WHERE json_extract_string(identity_json, '$[1][0][1][1][1]')='unread'")
    original = native.processing_table
    selected = []

    def restore(con, record_type, **options):
        assert options.get("where"), "prepare restored the entire catalog before selecting its candidates"
        selected.extend(row[0] for row in con.execute(
            f'SELECT comment_id FROM {native.qualified(record_type)} WHERE {options["where"]} ORDER BY 1'
        ).fetchall())
        return original(con, record_type, **options)

    monkeypatch.setattr(native, "processing_table", restore)
    prepared = cfw.prepare(tmp_path, scope=scope)
    assert selected == (["A"] if scope else ["A", "outside"])
    assert prepared["catalog_rows"] == (5 if scope else 6)
    assert (prepared["table_rows"], prepared["unread"], prepared["other_version_only"]) == (6, 1, 1)
    assert prepared["conflicted_versions_by_column"]["subtype"] == 1
    assert prepared["rows_to_fill"] == len(selected)


@pytest.mark.parametrize("damage", ["digest", "missing", "duplicate"])
def test_candidate_selection_still_requires_exact_one_to_one_receipts(tmp_path, catalog, damage):
    from spicy_regs.sources import regulatory_catalog as native

    seed(catalog, [row("A"), row("unread")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    with catalog() as con:
        table = native.receipts_table()
        selected = "json_extract_string(identity_json, '$[1][0][1][1][1]')='A'"
        if damage == "digest":
            con.execute(f"UPDATE {table} SET subject_version='unsealed damage' WHERE {selected}")
        elif damage == "missing":
            con.execute(f"DELETE FROM {table} WHERE {selected}")
        else:
            con.execute(f"INSERT INTO {table} SELECT * FROM {table} WHERE {selected}")
    with pytest.raises(ValueError):
        cfw.prepare(tmp_path)
    assert not (tmp_path / "fill" / "prepare.json").exists()


def test_batches_pack_whole_files_by_bytes():
    files = {"f1": {"rows": 3, "bytes": 300}, "f2": {"rows": 1, "bytes": 100}, "f3": {"rows": 5, "bytes": 900}}
    assert cfw.batches(files, 400, by_file=True) == [["f1", "f2"], ["f3"]]
    assert cfw.batches(files, 400, by_file=False) == [["f1", "f2", "f3"]]


# --------------------------------------------------------------------------- #
# Round 2: damage outside the batch's rows, the commit after COMMIT, one journal per prepare, the undo
# --------------------------------------------------------------------------- #
def _before_merge(statement: str):
    """Run ``statement`` in the batch's transaction just before its MERGE; the MERGE's count is still the one read."""
    return lambda sql: f"{statement}; {sql}" if _merge(sql) else sql


def _table() -> str:
    return iceberg._qualified(COMMENT)


def test_a_transaction_that_also_deletes_an_unread_row_in_the_batch_file_is_rolled_back(tmp_path, catalog):
    """The review's scenario Z: B shares A's file but needs nothing; only the row count sees it go."""
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="held"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=_before_merge(f"DELETE FROM {_table()} WHERE comment_id = 'B'")))
    assert set(table(catalog)) == {"A", "B"} and table(catalog)["A"]["subtype"] is None


def test_a_transaction_that_deletes_a_row_in_another_file_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A"), row("C", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="held"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=_before_merge(f"DELETE FROM {_table()} WHERE comment_id = 'C'")))
    assert set(table(catalog)) == {"A", "C"}


def test_a_row_the_transaction_moves_to_a_new_file_outside_the_batch_is_rolled_back(tmp_path, catalog):
    """C keeps its values and the row count holds (a moved row); only the new files' contents show it."""
    seed(catalog, [row("A"), row("C", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    move = _before_merge(f"UPDATE {_table()} SET agency_code = 'NEW' WHERE comment_id = 'C'")
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="outside the batch"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=move))
    assert table(catalog)["C"]["agency_code"] == "CMS"


def _recorded(summary: dict):
    """A ``commit_record`` stand-in: the one child of the checked snapshot is the catalog's current snapshot."""

    def record(con, parent_id):
        current = iceberg._read_snapshot(con, COMMENT)
        return {"table_uuid": current.table_uuid, "snapshot_id": current.snapshot_id, "schema_id": current.schema_id,
                "summary": summary}

    return record


@pytest.mark.parametrize(("record", "problem"), [
    (_recorded({"added-records": "1", "added-position-deletes": "1"}), None),
    (_recorded({"added-records": "2", "added-position-deletes": "2"}), "added 2 records"),
    (_recorded({"added-records": "1", "added-position-deletes": "0"}), "0 position deletes"),
    (None, "no single commit"),
])
def test_the_commit_must_be_the_one_child_of_the_checked_snapshot_adding_exactly_the_batch(
    tmp_path, catalog, monkeypatch, record, problem,
):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    monkeypatch.setattr(cfw, "commit_record", record or (lambda con, parent_id: None))
    monkeypatch.setattr(cfw, "_records_commits", lambda con: True)
    if problem is None:
        with catalog() as con:
            assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
        assert journal(tmp_path)[-1]["state"] == "verified"
        return
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match=problem):
        cfw.write(tmp_path, con=con)
    assert journal(tmp_path)[-1] | {"reason": None} == journal(tmp_path)[-1] | {
        "state": "failed", "committed": True, "reason": None}


def test_a_committed_failure_blocks_every_prepare_and_write_until_it_is_undone(tmp_path, catalog, monkeypatch):
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(tmp_path)
    monkeypatch.setattr(cfw, "commit_record", _recorded({"added-records": "9", "added-position-deletes": "9"}))
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=con, batch_bytes=1000)  # CMS first: it commits, then fails the post-commit check
    monkeypatch.setattr(cfw, "commit_record", lambda con, parent_id: None)
    with pytest.raises(RuntimeError, match="undo"):
        cfw.prepare(tmp_path)  # a fresh prepare no longer escapes it
    with catalog() as con, pytest.raises(RuntimeError, match="undo"):
        cfw.write(tmp_path, con=con)
    (failed,) = [line for line in journal(tmp_path) if line["state"] == "failed"]
    with catalog() as con:
        cfw.undo(tmp_path, failed["batch"], expected_snapshot=iceberg._read_snapshot(con, COMMENT).snapshot_id,
                 con=con)
    assert (table(catalog)["B"]["subtype"], table(catalog)["A"]["subtype"]) == (None, None)
    _, written = fill(tmp_path, catalog)
    assert written["rows_changed"] == 2


def test_write_refuses_a_committed_failure_whose_journal_arrived_after_its_prepare(tmp_path, catalog, monkeypatch):
    """Another prepare's failure, pulled in late (a runner's journals, a copied workdir), still stops the write."""
    first, second = tmp_path / "first", tmp_path / "second"
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(first, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(second, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(second)
    cfw.prepare(first)
    monkeypatch.setattr(cfw, "commit_record", _recorded({"added-records": "9", "added-position-deletes": "9"}))
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(first, con=con)
    monkeypatch.setattr(cfw, "commit_record", lambda con, parent_id: None)
    for path in (first / "fill").glob("write-journal-*.jsonl"):
        (second / "fill" / path.name).write_bytes(path.read_bytes())
    with catalog() as con, pytest.raises(RuntimeError, match="undo"):
        cfw.write(second, con=con)
    assert table(catalog)["B"]["subtype"] is None


def test_a_fresh_prepare_never_reuses_an_earlier_journal(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    first, second = cfw.prepare(tmp_path), cfw.prepare(tmp_path)
    assert first["fill_sha256"] == second["fill_sha256"] and first["prepare_id"] != second["prepare_id"]


def test_prepare_refuses_a_file_whose_size_it_cannot_read(tmp_path, catalog, monkeypatch):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    monkeypatch.setattr(cfw, "data_files", lambda con: {})
    with pytest.raises(RuntimeError, match="size"):
        cfw.prepare(tmp_path)


def test_prepare_reads_the_staged_fill_input_as_it_reads_the_parts(tmp_path, catalog):
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S", "duplicate_comments": 0},
                     {"key": "b", "comment_id": "B", "attachments_json": '[{"title":"attachment"}]'}])
    from_parts = cfw.prepare(tmp_path)
    staged = tmp_path / "reads.parquet"
    pl.read_parquet(tmp_path / "parts" / "*" / "*.parquet").select(cfw.READ_COLUMNS).write_parquet(staged)
    assert cfw.prepare(tmp_path, reads=staged)["fill_sha256"] == from_parts["fill_sha256"]


# --------------------------------------------------------------------------- #
# Profiles and the staged read
# --------------------------------------------------------------------------- #
def test_the_attachments_profile_stages_four_columns_and_fills_only_attachments(tmp_path, catalog):
    seed(catalog, [row("A", attachments_json=HELD), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED, "subtype": "S"},
                     {"key": "b", "comment_id": "B", "attachments_json": LISTED, "subtype": "S"},
                     {"key": "b(1)", "comment_id": "B", "attachments_json": LISTED, "first_name": "Kristen"}])
    staged = cf.stage(tmp_path, tmp_path / "staged", profile="attachments")
    assert staged["columns"] == ["key", "comment_id", "modify_date", "attachments_json"] and all(staged["checks"].values())
    assert pl.read_parquet(tmp_path / "staged" / "reads.parquet").columns == staged["columns"]
    assert (staged["rows"], staged["distinct_comment_ids"], staged["non_null_by_column"]["attachments_json"]) == (3, 2, 3)
    from_parts = cfw.prepare(tmp_path, profile="attachments")
    prepared = cfw.prepare(tmp_path, reads=tmp_path / "staged" / "reads.parquet", profile="attachments")
    assert prepared["fill_sha256"] == from_parts["fill_sha256"] and prepared["columns"] == ["attachments_json"]
    assert (prepared["replaced_by_column"], prepared["cells_by_column"]) == ({"attachments_json": 1},
                                                                            {"attachments_json": 1})
    with catalog() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 2
    rows = table(catalog)
    assert (rows["A"]["attachments_json"], rows["B"]["attachments_json"]) == (LISTED, LISTED)
    assert (rows["A"]["subtype"], rows["B"]["subtype"], rows["B"]["first_name"]) == (None, None, None)


def test_a_staged_read_lacking_a_profile_column_is_refused(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    cf.stage(tmp_path, tmp_path / "staged", profile="attachments")
    with pytest.raises(RuntimeError, match="lack"):
        cfw.prepare(tmp_path, reads=tmp_path / "staged" / "reads.parquet")  # the "all" profile
    with pytest.raises(ValueError, match="profile"):
        cfw.prepare(tmp_path, profile="attachment")


def test_no_profile_stages_a_private_column_and_the_check_would_catch_one(tmp_path, monkeypatch):
    """The private-attributes rule: a staged read holds the key and columns ``comments`` publishes, never the parts'
    attribute JSON (email, phone, fax), ETag, size or body digest."""
    staged = {c for profile in cfw.PROFILES for c in cfw.read_columns(profile)}
    assert staged - {"key"} <= set(COMMENT.schema) - {"comment"}
    assert not {c for c in staged if any(word in c for word in cf.PRIVATE_WORDS)}
    private = set(cf.PART_SCHEMA) - set(COMMENT.schema) - {"key"}
    assert private == {"etag", "size", "comment_sha256", "comment_length", "attributes_json", "source_sha256"}
    assert all(any(word in c for word in cf.PRIVATE_WORDS) for c in private)
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attributes_json": '{"email": "x@example.org"}'}])
    monkeypatch.setitem(cfw.PROFILES, "leaky", ("attachments_json", "attributes_json"))
    with pytest.raises(RuntimeError, match="private"):
        cf.stage(tmp_path, tmp_path / "staged", profile="leaky")
    assert not (tmp_path / "staged" / "reads.parquet").exists()


def test_a_staged_read_is_built_once_and_uploaded_once_never_over_an_object(tmp_path):
    from tests.generation_fakes import Store

    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    out = tmp_path / "staged"
    staged = cf.stage(tmp_path, out, profile="attachments")
    with pytest.raises(RuntimeError, match="already holds"):
        cf.stage(tmp_path, out, profile="attachments")
    store = Store()
    uploaded = cf.upload_staged(out, client=store)
    sha = staged["file"]["sha256"]
    assert uploaded["key"] == f"staging/comment-fields-fill-{sha[:12]}/reads.parquet"
    assert store.objects[uploaded["key"]] == (out / "reads.parquet").read_bytes()
    assert store.objects[uploaded["key"] + ".sha256"] == f"{sha}  reads.parquet\n".encode()
    assert json.loads((out / "staging.json").read_text())["upload"] == uploaded
    with pytest.raises(RuntimeError, match="already holds"):
        cf.upload_staged(out, client=store)
    (out / "reads.parquet").write_bytes(b"other bytes")
    with pytest.raises(RuntimeError, match="not the staged read"):
        cf.upload_staged(out, client=Store())


def _filled(tmp_path, catalog) -> dict:
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S", "duplicate_comments": 3}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    before = table(catalog)
    fill(tmp_path, catalog, batch_bytes=1000)
    (epa,) = [line for line in journal(tmp_path) if line["state"] == "verified" and line["files"] == ["EPA"]]
    return {"before": before, "batch": epa["batch"]}


def _current(catalog) -> int:
    with catalog() as con:
        return iceberg._read_snapshot(con, COMMENT).snapshot_id


def test_undo_restores_a_committed_batch_from_its_preimage_as_a_new_commit(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    expected = _current(catalog)
    with catalog() as con:
        undone = cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=con)
    assert table(catalog)["A"] == filled["before"]["A"] and table(catalog)["B"]["subtype"] == "S"
    assert undone["rows_restored"] == 1 and journal(tmp_path)[-1]["state"] == "undone"
    expected = _current(catalog)
    with catalog() as con, pytest.raises(RuntimeError, match="already undone"):
        cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=con)


def test_undo_needs_the_snapshot_the_operator_reviewed(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    with catalog() as con, pytest.raises(RuntimeError, match="expected"):
        cfw.undo(tmp_path, filled["batch"], expected_snapshot=7, con=con)
    assert table(catalog)["A"]["subtype"] == "S"


def test_undo_refuses_a_commit_between_its_check_and_its_transaction(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    expected = _current(catalog)

    def foreign(sql):
        if "_undo_now" in sql and sql.lstrip().startswith("CREATE"):
            proxy.con.execute(f"UPDATE {_table()} SET title = 'etl' WHERE comment_id = 'B'")

    with catalog() as con:
        proxy = Proxy(con, after=foreign)
        with pytest.raises(RuntimeError, match="snapshot changed"):
            cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=proxy)
    with catalog() as con:
        assert con.execute(f"SELECT subtype FROM {_table()} WHERE comment_id='A'").fetchone() == ('S',)


def test_undo_refuses_a_row_changed_since_the_fill(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    with catalog() as con:
        con.execute(f"UPDATE {_table()} SET title = 'edited' WHERE comment_id = 'A'")
    expected = _current(catalog)
    with catalog() as con, pytest.raises(ValueError, match="subject receipt"):
        cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=con)
    with catalog() as con:
        assert con.execute(f"SELECT subtype,title FROM {_table()} WHERE comment_id='A'").fetchone() == ('S', 'edited')


class _Bucket:
    """The three S3 calls the journal sync and the staged fetch make, over a dict."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def upload_file(self, path, bucket, key):
        self.objects[key] = Path(path).read_bytes()

    def download_file(self, bucket, key, path):
        Path(path).write_bytes(self.objects[key])

    def get_paginator(self, name):
        objects = self.objects

        class Pages:
            def paginate(self, Bucket, Prefix):  # noqa: N803 (boto3's spelling)
                return [{"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}]

        return Pages()


def test_a_committed_failure_in_an_earlier_runs_journal_blocks_a_fresh_workdir(tmp_path, catalog, monkeypatch):
    """On a runner every run starts empty; the journals kept beside the staged read carry the failure over."""
    first, second, bucket = tmp_path / "run1", tmp_path / "run2", _Bucket()
    seed(catalog, [row("A")])
    reads(first, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(first)
    monkeypatch.setattr(cfw, "commit_record", _recorded({"added-records": "9", "added-position-deletes": "9"}))
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(first, con=con)
    assert cfw.sync_journals(first, "staging/x", push=True, client=bucket)
    reads(second, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.sync_journals(second, "staging/x", push=False, client=bucket)
    with pytest.raises(RuntimeError, match="undo"):
        cfw.prepare(second)


def test_the_staged_read_is_refused_unless_its_digest_matches(tmp_path):
    bucket = _Bucket({"staging/x/reads.parquet": b"staged bytes"})
    good = cfw.hashlib.sha256(b"staged bytes").hexdigest()
    assert cfw.fetch_staged(tmp_path, "staging/x/reads.parquet", good, client=bucket).read_bytes() == b"staged bytes"
    with pytest.raises(RuntimeError, match="sha256"):
        cfw.fetch_staged(tmp_path, "staging/x/reads.parquet", "0" * 64, client=bucket)
    assert not (tmp_path / "reads.parquet").exists()


# --------------------------------------------------------------------------- #
# Empty-string original_document_id nullification (ss-otwa)
# --------------------------------------------------------------------------- #
def test_empty_original_document_id_nullifies_only_blank_cells_and_keeps_reference_json(tmp_path, catalog):
    raw_empty_ref = json.dumps({"commentOnDocumentId": "EPA-D1-0001", "originalDocumentId": ""})
    raw_space_ref = json.dumps({"commentOnDocumentId": "CMS-D2-0001", "originalDocumentId": "   "})
    raw_valid_ref = json.dumps({"originalDocumentId": "EPA_FRDOC_0001-0004"})
    seed(
        catalog,
        [
            row(
                "A-EMPTY",
                agency="EPA",
                docket_id="EPA-D1",
                comment_on_document_id="EPA-D1-0001",
                original_document_id="",
                comment_reference_values_json=raw_empty_ref,
                subtype="Public Comment",
                duplicate_comments=2,
                attachments_json=HELD,
            ),
            row(
                "B-SPACE",
                agency="CMS",
                docket_id="CMS-D2",
                comment_on_document_id="CMS-D2-0001",
                original_document_id="   \t",
                comment_reference_values_json=raw_space_ref,
                first_name="Jane",
            ),
            row(
                "C-LEGACY-EMPTY",
                agency="CMS",
                docket_id="CMS-D2",
                original_document_id="",
                comment_reference_values_json=None,
            ),
            row(
                "D-VALID",
                agency="EPA",
                docket_id="EPA-D1",
                original_document_id="EPA_FRDOC_0001-0004",
                comment_reference_values_json=raw_valid_ref,
            ),
            row(
                "E-NULL",
                agency="DOT",
                docket_id="DOT-D3",
                original_document_id=None,
                comment_reference_values_json="{}",
            ),
        ],
    )
    before = table(catalog)

    prepared = cfw.prepare_empty_original_document_id(tmp_path)
    assert prepared["operation"] == cfw.EMPTY_ORIGINAL_DOCUMENT_ID_OPERATION
    assert prepared["columns"] == ["original_document_id"]
    assert prepared["table_rows"] == 5
    assert prepared["catalog_rows"] == 5
    assert prepared["rows_to_fill"] == 3
    assert prepared["empty_string_rows"] == 2
    assert prepared["whitespace_only_rows"] == 1
    assert prepared["non_empty_rows"] == 1
    assert prepared["already_null_rows"] == 1
    assert prepared["nullified_by_column"] == {"original_document_id": 3}
    assert sorted(prepared["files"]) == ["CMS", "EPA"]
    assert not (tmp_path / "fill" / "work").exists()

    with catalog() as con:
        written = cfw.write(tmp_path, con=con, batch_bytes=1000)
    assert written == {"batches": 2, "rows_changed": 3, "files_skipped_verified": 0}
    assert [line["state"] for line in journal(tmp_path)] == ["pending", "verified", "pending", "verified"]

    after = table(catalog)
    assert len(after) == len(before) == 5
    assert after["A-EMPTY"]["original_document_id"] is None
    assert after["B-SPACE"]["original_document_id"] is None
    assert after["C-LEGACY-EMPTY"]["original_document_id"] is None
    assert after["D-VALID"]["original_document_id"] == "EPA_FRDOC_0001-0004"
    assert after["E-NULL"]["original_document_id"] is None

    # comment_reference_values_json and every single other column remain 100% untouched.
    assert after["A-EMPTY"]["comment_reference_values_json"] == raw_empty_ref
    assert after["B-SPACE"]["comment_reference_values_json"] == raw_space_ref
    assert after["C-LEGACY-EMPTY"]["comment_reference_values_json"] is None
    assert after["D-VALID"]["comment_reference_values_json"] == raw_valid_ref
    for comment_id, prior_row in before.items():
        for column in COMMENT.schema:
            if column == "original_document_id" and comment_id in {"A-EMPTY", "B-SPACE", "C-LEGACY-EMPTY"}:
                continue
            assert after[comment_id][column] == prior_row[column], (comment_id, column)

    # Re-running prepare + write on the clean catalog is an idempotent no-op.
    second_prepare = cfw.prepare_empty_original_document_id(tmp_path)
    assert second_prepare["rows_to_fill"] == 0
    assert second_prepare["files"] == {}
    with catalog() as con:
        second_write = cfw.write(tmp_path, con=con)
    assert second_write == {"batches": 0, "rows_changed": 0, "files_skipped_verified": 0}


def test_empty_original_document_id_pilot_scope_and_undo(tmp_path, catalog):
    seed(
        catalog,
        [
            row("A", agency="EPA", docket_id="EPA-PILOT", original_document_id="", comment_reference_values_json='{"originalDocumentId":""}'),
            row("B", agency="EPA", docket_id="EPA-OTHER", original_document_id="", comment_reference_values_json='{"originalDocumentId":""}'),
            row("C", agency="EPA", docket_id="EPA-PILOT", original_document_id="KEEP-1"),
        ],
    )
    before = table(catalog)
    prepared = cfw.prepare_empty_original_document_id(tmp_path, scope={"docket_id": "EPA-PILOT"})
    assert prepared["table_rows"] == 3
    assert prepared["catalog_rows"] == 2
    assert prepared["rows_to_fill"] == 1
    with catalog() as con:
        written = cfw.write(tmp_path, con=con, by_file=False)
    assert written == {"batches": 1, "rows_changed": 1, "files_skipped_verified": 0}

    mid = table(catalog)
    assert mid["A"]["original_document_id"] is None
    assert mid["A"]["comment_reference_values_json"] == '{"originalDocumentId":""}'
    assert mid["B"]["original_document_id"] == ""
    assert mid["C"]["original_document_id"] == "KEEP-1"

    # Undo restores the pilot batch cleanly back to its exact pre-image.
    (verified,) = [line for line in journal(tmp_path) if line["state"] == "verified"]
    expected = _current(catalog)
    with catalog() as con:
        undone = cfw.undo(tmp_path, verified["batch"], expected_snapshot=expected, con=con)
    assert undone["rows_restored"] == 1
    assert table(catalog) == before


def test_empty_original_document_id_rejects_bad_scope_tampered_columns_and_mutation_of_reference_json(tmp_path, catalog):
    seed(
        catalog,
        [
            row("A", agency="EPA", original_document_id="", comment_reference_values_json='{"originalDocumentId":""}'),
        ],
    )
    with pytest.raises(ValueError, match="unknown scope column"):
        cfw.prepare_empty_original_document_id(tmp_path, scope={"nonexistent": "x"})

    cfw.prepare_empty_original_document_id(tmp_path)

    # A faulty write that alters comment_reference_values_json is caught and rolled back.
    corrupt = lambda sql: (  # noqa: E731
        sql.replace('"comment_reference_values_json"=s."comment_reference_values_json"', '"comment_reference_values_json"=NULL')
        if sql.lstrip().startswith("MERGE")
        else sql
    )
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=corrupt))
    after = table(catalog)
    assert after["A"]["original_document_id"] == ""
    assert after["A"]["comment_reference_values_json"] == '{"originalDocumentId":""}'

    # Tampering prepare.json to claim another column refuses before opening the catalog.
    tampered = tmp_path / "tampered"
    cfw.prepare_empty_original_document_id(tampered)
    prep_path = tampered / "fill" / "prepare.json"
    data = json.loads(prep_path.read_text())
    data["columns"] = ["comment_reference_values_json"]
    prep_path.write_text(json.dumps(data))
    with catalog() as con, pytest.raises(RuntimeError, match="may only modify"):
        cfw.write(tampered, con=con)


def test_empty_original_document_id_workflow_and_cli_plumbing():
    import subprocess
    import sys
    import yaml

    repo_root = Path(__file__).resolve().parents[1]
    workflow_path = repo_root / ".github" / "workflows" / "fill-comment-fields.yml"
    raw_text = workflow_path.read_text(encoding="utf-8")
    workflow = yaml.safe_load(raw_text)
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert {
        "prepare",
        "pilot",
        "fill",
        "undo",
        "prepare-empty-original-document-id",
        "pilot-empty-original-document-id",
        "fill-empty-original-document-id",
    } <= set(inputs["operation"]["options"])
    assert workflow["concurrency"] == {"group": "comments-catalog-write", "cancel-in-progress": False, "queue": "max"}
    assert workflow["env"]["SPICY_REGS_CATALOG_LOCK"] == "comments-catalog-write"

    steps = {step["name"]: step for step in workflow["jobs"]["fill"]["steps"] if "name" in step}
    for name, step in steps.items():
        if "run" in step:
            assert "${{ inputs." not in step["run"], f"{name} must not interpolate raw inputs.* in run:"
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)

    suspend = steps["Save and suspend maintenance on both selected native tables"]
    restore = steps["Restore exact prior maintenance on both native tables"]
    assert "prepare-empty-original-document-id" in suspend["if"]
    assert restore["if"].startswith("always()") and "prepare-empty-original-document-id" in restore["if"]
    assert "prepare-empty-original-document-id" in steps["Prepare (reads the catalog at one snapshot; writes nothing)"]["run"]
    assert "fill-empty-original-document-id" in steps["Write, one verified batch at a time"]["if"]
    assert "pilot-empty-original-document-id" in steps["Write, one verified batch at a time"]["if"]

    help_out = subprocess.run(
        [
            sys.executable,
            "-m",
            "spicy_regs.pipelines.comment_fields_write",
            "prepare-empty-original-document-id",
            "--help",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert "--workdir" in help_out.stdout
    assert "--docket" in help_out.stdout
    assert "--agency" in help_out.stdout


def test_empty_original_document_id_recovers_after_crash_between_commit_and_journal(tmp_path, catalog, monkeypatch):
    seed(
        catalog,
        [
            row("A", agency="EPA", original_document_id="", comment_reference_values_json='{"originalDocumentId":""}'),
            row("B", agency="CMS", original_document_id="  \t", comment_reference_values_json='{"originalDocumentId":"  \\t"}'),
        ],
    )
    cfw.prepare_empty_original_document_id(tmp_path)
    commits = {"n": 0}

    def crash(sql):
        if sql == "COMMIT":
            commits["n"] += 1
            if commits["n"] == 1:
                raise KeyboardInterrupt

    with catalog() as con, pytest.raises(KeyboardInterrupt):
        cfw.write(tmp_path, con=Proxy(con, after=crash), batch_bytes=1000)
    assert [line["state"] for line in journal(tmp_path)] == ["pending"]
    with catalog() as con:
        snap = iceberg._read_snapshot(con, COMMENT)
    monkeypatch.setattr(
        cfw,
        "commit_record",
        lambda con, parent_id: {
            "table_uuid": snap.table_uuid,
            "snapshot_id": snap.snapshot_id,
            "schema_id": snap.schema_id,
            "summary": {"added-records": "1", "added-position-deletes": "1"},
        }
        if parent_id == journal(tmp_path)[0]["snapshot_before"]["snapshot_id"]
        else None,
    )
    strip_at = lambda sql: re.sub(r"\s+AT\s+\(VERSION\s*=>\s*\d+\)", "", sql)  # noqa: E731
    with catalog() as con:
        resumed = cfw.write(tmp_path, con=Proxy(con, rewrite=strip_at), batch_bytes=1000)
    assert resumed["rows_changed"] == 1
    assert any(line.get("recovered") is True for line in journal(tmp_path))
    after = table(catalog)
    assert after["A"]["original_document_id"] is None and after["B"]["original_document_id"] is None
    assert after["A"]["comment_reference_values_json"] == '{"originalDocumentId":""}'
    assert after["B"]["comment_reference_values_json"] == '{"originalDocumentId":"  \\t"}'


@pytest.mark.parametrize(("damage", "problem"), [
    ("missing preimage", "pre-image.*is missing or differs"),
    ("different preimage bytes", "pre-image.*is missing or differs"),
    ("different prepare id", "differs from prepare.json"),
])
def test_reconcile_refuses_uncommitted_pending_with_invalid_evidence(tmp_path, catalog, damage, problem):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    prepared = cfw.prepare(tmp_path)
    path = cfw._journal(tmp_path, prepared)
    preimage = path.parent / "preimage.parquet"
    preimage.write_bytes(b"retained preimage")
    pending = {"state": "pending", "prepare_id": prepared["prepare_id"], "batch": "pending-batch", "files": ["EPA"],
               "snapshot_before": prepared["snapshot"], "preimage": str(preimage), "preimage_sha256": cfw._sha256(preimage)}
    if damage == "missing preimage":
        preimage.unlink()
    elif damage == "different preimage bytes":
        preimage.write_bytes(b"different preimage")
    else:
        pending["prepare_id"] = "some-other-prepare"
    cfw._append(path, pending)
    initial = path.read_bytes()

    with pytest.raises(RuntimeError, match=problem):
        cfw.reconcile_pending(tmp_path)
    assert path.read_bytes() == initial


# --------------------------------------------------------------------------- #
# Partitioned spill-free prepare() equivalence, duplicate guards, and tight-budget stress (ss-flsg)
# --------------------------------------------------------------------------- #
def _bucket_witness_ids(buckets: int) -> dict[int, str]:
    """Return one deterministic comment_id for each hash bucket 0..buckets-1."""
    mask = buckets - 1
    found: dict[int, str] = {}
    with duckdb.connect() as con:
        rows = con.execute(
            f"SELECT 'W-' || i::VARCHAR AS cid, (hash('W-' || i::VARCHAR) & {mask})::INT AS b "
            "FROM range(2000) t(i)"
        ).fetchall()
    for cid, bucket in rows:
        found.setdefault(bucket, cid)
        if len(found) == buckets:
            break
    assert len(found) == buckets
    return found


def test_partitioned_prepare_matches_reference_semantics_across_edge_cases(tmp_path, catalog, monkeypatch):
    from spicy_regs.sources import regulatory_catalog as native

    witnesses = _bucket_witness_ids(cfw.HASH_BUCKETS)
    agencies = ("CMS", "DOT", "EPA", "FDA")
    changed_non_additive = json.dumps([
        {"title": "Letter", "formats": [{**_PDF, "url": f"{_PDF['url']}?v=2"}]},
    ])
    catalog_rows = []
    read_rows = []

    # Populate every hash bucket 0..HASH_BUCKETS-1 with varied outcomes.
    for bucket, cid in sorted(witnesses.items()):
        agency = agencies[bucket % len(agencies)]
        if bucket % 6 == 0:
            # Additive replacement (HELD -> LISTED)
            catalog_rows.append(row(cid, agency=agency, attachments_json=HELD))
            read_rows.append({"key": f"{cid}/1", "comment_id": cid, "modify_date": T0, "attachments_json": LISTED})
        elif bucket % 6 == 1:
            # Non-additive refusal (HELD -> changed_non_additive)
            catalog_rows.append(row(cid, agency=agency, attachments_json=HELD))
            read_rows.append(
                {"key": f"{cid}/1", "comment_id": cid, "modify_date": T0, "attachments_json": changed_non_additive}
            )
        elif bucket % 6 == 2:
            # Already equal held value (no fill, no refusal)
            catalog_rows.append(row(cid, agency=agency, attachments_json=LISTED))
            read_rows.append({"key": f"{cid}/1", "comment_id": cid, "modify_date": T0, "attachments_json": LISTED})
        elif bucket % 6 == 3:
            # Multi-copy agreeing on matching version + an extra stale version (still fills once, not other_version_only)
            catalog_rows.append(row(cid, agency=agency))
            read_rows.extend([
                {"key": f"{cid}/a", "comment_id": cid, "modify_date": T0, "attachments_json": LISTED},
                {"key": f"{cid}/b", "comment_id": cid, "modify_date": T0, "attachments_json": LISTED},
                {"key": f"{cid}/old", "comment_id": cid, "modify_date": "2019-01-01T00:00:00Z", "attachments_json": HELD},
            ])
        elif bucket % 6 == 4:
            # Multi-copy disagreeing on matching version (conflict -> blocks fill, not other_version_only)
            catalog_rows.append(row(cid, agency=agency))
            read_rows.extend([
                {"key": f"{cid}/a", "comment_id": cid, "modify_date": T0, "attachments_json": LISTED},
                {"key": f"{cid}/b", "comment_id": cid, "modify_date": T0, "attachments_json": None},
            ])
        else:
            # Plain NULL fill
            catalog_rows.append(row(cid, agency=agency))
            read_rows.append({"key": f"{cid}/1", "comment_id": cid, "modify_date": T0, "attachments_json": LISTED})

    # Explicit NULL modify_date, multi-wrong-version, stated-NULL, unread, and absent edge cases.
    catalog_rows.extend([
        row("NULL-MD-MATCH", agency="CMS", modify=None),
        row("NULL-MD-MULTI-AGREE", agency="CMS", modify=None),
        row("NULL-MD-MULTI-CONFLICT", agency="CMS", modify=None),
        row("NULL-MD-CAT-ONLY", agency="DOT", modify=None),
        row("NULL-MD-READ-ONLY", agency="DOT", modify=T0),
        row("MULTI-WRONG-VER", agency="EPA", modify=T0),
        row("STATED-NULL-SAME-VER", agency="EPA", modify=T0),
        row("UNREAD-1", agency="FDA", modify=T0),
        row("UNREAD-NULL-MD", agency="FDA", modify=None),
    ])
    read_rows.extend([
        {"key": "nm/1", "comment_id": "NULL-MD-MATCH", "modify_date": None, "attachments_json": LISTED},
        {"key": "nma/1", "comment_id": "NULL-MD-MULTI-AGREE", "modify_date": None, "attachments_json": LISTED},
        {"key": "nma/2", "comment_id": "NULL-MD-MULTI-AGREE", "modify_date": None, "attachments_json": LISTED},
        {"key": "nmc/1", "comment_id": "NULL-MD-MULTI-CONFLICT", "modify_date": None, "attachments_json": LISTED},
        {"key": "nmc/2", "comment_id": "NULL-MD-MULTI-CONFLICT", "modify_date": None, "attachments_json": HELD},
        {"key": "nmco/1", "comment_id": "NULL-MD-CAT-ONLY", "modify_date": T0, "attachments_json": LISTED},
        {"key": "nmro/1", "comment_id": "NULL-MD-READ-ONLY", "modify_date": None, "attachments_json": LISTED},
        {"key": "mwv/1", "comment_id": "MULTI-WRONG-VER", "modify_date": "2019-01-01T00:00:00Z", "attachments_json": LISTED},
        {"key": "mwv/2", "comment_id": "MULTI-WRONG-VER", "modify_date": "2019-06-01T00:00:00Z", "attachments_json": HELD},
        {"key": "sn/1", "comment_id": "STATED-NULL-SAME-VER", "modify_date": T0, "attachments_json": None},
        {"key": "abs/1", "comment_id": "ABSENT-ID", "modify_date": T0, "attachments_json": LISTED},
        {"key": "null-cid-read/1", "comment_id": None, "modify_date": T0, "attachments_json": LISTED},
    ])

    seed(catalog, catalog_rows)
    staged_reads = tmp_path / "reads.parquet"
    pl.DataFrame(
        [{c: r.get(c) for c in cfw.read_columns("attachments")} for r in read_rows],
        schema={c: pl.Utf8 for c in cfw.read_columns("attachments")},
    ).write_parquet(staged_reads)

    # Inject a single NULL comment_id row into the snapshot query (valid under SQL GROUP BY HAVING count(*) > 1,
    # and must count toward table_rows, catalog_rows, and unread — never other_version_only).
    qualified = iceberg._qualified(COMMENT)
    one_null_row = (
        f"(SELECT * REPLACE (NULL::VARCHAR AS comment_id, NULL::VARCHAR AS modify_date, 'EPA'::VARCHAR AS agency_code) "
        f"FROM {qualified} LIMIT 1)"
    )
    snapshot_with_one_null = f"SELECT * FROM (SELECT * FROM {qualified} UNION ALL {one_null_row})"
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda *_a, **_kw: snapshot_with_one_null)

    # Ensure processing_table still reads cleanly from the valid non-null native table rows.
    prepared = cfw.prepare(tmp_path / "run", reads=staged_reads, profile="attachments")

    # Compute independent legacy reference counts directly using the original correlated EXISTS SQL.
    with catalog() as con:
        parts_rel = f"read_parquet({cfw._sql(staged_reads)}, hive_partitioning=false)"
        index_sql = f"SELECT comment_id, modify_date, agency_code AS _file FROM ({snapshot_with_one_null})"
        ref_reads_sql = (
            f"SELECT s.comment_id, s.modify_date, CAST(s.attachments_json AS VARCHAR) AS attachments_json "
            f"FROM {parts_rel} s SEMI JOIN ({index_sql}) c USING (comment_id)"
        )
        ref_versions_sql = f"""
            SELECT comment_id, modify_date,
                   CASE WHEN count(DISTINCT CASE WHEN attachments_json IS NULL THEN 'n' ELSE 'v' || attachments_json END) = 1
                        THEN any_value(attachments_json) END AS attachments_json
            FROM ({ref_reads_sql})
            GROUP BY comment_id, modify_date
        """
        ref_counts = con.execute(f"""
            WITH idx AS ({index_sql}),
                 rr AS ({ref_reads_sql}),
                 vv AS ({ref_versions_sql})
            SELECT count(*),
                   count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM rr r WHERE r.comment_id = c.comment_id)),
                   count(*) FILTER (
                       WHERE EXISTS (SELECT 1 FROM rr r WHERE r.comment_id = c.comment_id)
                         AND NOT EXISTS (
                             SELECT 1 FROM vv v
                             WHERE v.comment_id = c.comment_id AND v.modify_date IS NOT DISTINCT FROM c.modify_date
                         )
                   )
            FROM idx c
        """).fetchone()

    assert (prepared["catalog_rows"], prepared["unread"], prepared["other_version_only"]) == ref_counts
    assert prepared["table_rows"] == len(catalog_rows) + 1
    assert prepared["unread"] == 3  # UNREAD-1, UNREAD-NULL-MD, and the single NULL comment_id row
    assert prepared["other_version_only"] == 3  # NULL-MD-CAT-ONLY, NULL-MD-READ-ONLY, MULTI-WRONG-VER

    fill_df = pl.read_parquet(tmp_path / "run" / "fill" / "fill.parquet").sort("comment_id")
    filled_ids = set(fill_df["comment_id"].to_list())
    assert {"NULL-MD-MATCH", "NULL-MD-MULTI-AGREE"} <= filled_ids
    assert "NULL-MD-MULTI-CONFLICT" not in filled_ids
    assert "STATED-NULL-SAME-VER" not in filled_ids

    conflicts_df = pl.read_parquet(tmp_path / "run" / "fill" / "conflicts.parquet")
    assert "NULL-MD-MULTI-CONFLICT" in set(conflicts_df["comment_id"].to_list())
    assert prepared["conflicted_versions_by_column"]["attachments_json"] == len(conflicts_df)

    refusals_df = pl.read_parquet(tmp_path / "run" / "fill" / "refusals.parquet")
    assert prepared["refused_not_additive_by_column"]["attachments_json"] == len(refusals_df)
    assert prepared["replaced_by_column"]["attachments_json"] == sum(1 for b in witnesses if b % 6 == 0)
    assert prepared["rows_to_fill"] == len(fill_df)
    assert sum(prepared["cells_by_column"].values()) + sum(prepared["replaced_by_column"].values()) == len(fill_df)
    assert not (tmp_path / "run" / "fill" / "work").exists()

    # Verify the write path applies the prepared fill cleanly (on the non-null snapshot).
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, _, **kw: f"SELECT * FROM {iceberg._qualified(rt)}")
    all_reads_dir = tmp_path / "all-profile"
    reads(all_reads_dir, [
        {"key": "a", "comment_id": "UNREAD-1", "subtype": "Public Comment", "attachments_json": LISTED},
        {"key": "b", "comment_id": "NULL-MD-MATCH", "modify_date": None, "subtype": "Public Comment"},
    ])
    all_prepared = cfw.prepare(all_reads_dir, profile="all")
    assert (all_prepared["rows_to_fill"], all_prepared["unread"], all_prepared["other_version_only"]) == (
        2, len(catalog_rows) - 2, 0,
    )
    assert native.processing_table is not None


def test_partitioned_prepare_handles_zero_matching_reads_and_empty_catalog(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", agency="CMS")])
    absent_reads = tmp_path / "absent.parquet"
    pl.DataFrame(
        [
            {"key": "x", "comment_id": "NOT-IN-CATALOG", "modify_date": T0, "attachments_json": LISTED},
            {"key": "n", "comment_id": None, "modify_date": T0, "attachments_json": LISTED},
        ],
        schema={c: pl.Utf8 for c in cfw.read_columns("attachments")},
    ).write_parquet(absent_reads)

    prepared = cfw.prepare(tmp_path / "no-match", reads=absent_reads, profile="attachments")
    assert (prepared["table_rows"], prepared["catalog_rows"], prepared["unread"], prepared["other_version_only"]) == (
        2, 2, 2, 0,
    )
    assert prepared["rows_to_fill"] == 0
    assert prepared["files"] == {}
    assert pl.read_parquet(tmp_path / "no-match" / "fill" / "fill.parquet").height == 0

    # Empty catalog (0 rows) with non-empty reads.
    with catalog() as con:
        con.execute(f"DELETE FROM {iceberg._qualified(COMMENT)}")
    empty_prepared = cfw.prepare(tmp_path / "empty-cat", reads=absent_reads, profile="attachments")
    assert (
        empty_prepared["table_rows"],
        empty_prepared["catalog_rows"],
        empty_prepared["unread"],
        empty_prepared["other_version_only"],
        empty_prepared["rows_to_fill"],
    ) == (0, 0, 0, 0, 0)
    assert empty_prepared["files"] == {}


@pytest.mark.parametrize("mode", ["two-nulls", "buckets-and-nulls"])
def test_partitioned_duplicate_id_guard_refuses_null_and_bucket_duplicates(tmp_path, catalog, monkeypatch, mode):
    witnesses = _bucket_witness_ids(cfw.HASH_BUCKETS)
    dup_ids = [witnesses[0], witnesses[17], witnesses[cfw.HASH_BUCKETS - 1]]
    seed(catalog, [row(cid) for cid in dup_ids] + [row("UNIQUE-1")])
    reads(tmp_path / "prep", [{"key": "u", "comment_id": "UNIQUE-1", "attachments_json": LISTED}])

    qualified = iceberg._qualified(COMMENT)
    one_null_row = (
        f"(SELECT * REPLACE (NULL::VARCHAR AS comment_id, NULL::VARCHAR AS modify_date, 'EPA'::VARCHAR AS agency_code) "
        f"FROM {qualified} LIMIT 1)"
    )
    if mode == "two-nulls":
        snap_sql = (
            f"SELECT * FROM (SELECT * FROM {qualified} "
            f"UNION ALL {one_null_row} UNION ALL {one_null_row})"
        )
        expected_dups = 1
    else:
        in_list = ", ".join(cfw._sql(cid) for cid in dup_ids)
        snap_sql = (
            f"SELECT * FROM (SELECT * FROM {qualified} "
            f"UNION ALL SELECT * FROM {qualified} WHERE comment_id IN ({in_list}) "
            f"UNION ALL {one_null_row} UNION ALL {one_null_row})"
        )
        expected_dups = len(dup_ids) + 1

    monkeypatch.setattr(iceberg, "_snapshot_query", lambda *_a, **_kw: snap_sql)

    with pytest.raises(RuntimeError, match=f"^{expected_dups} comment ids appear more than once"):
        cfw.prepare(tmp_path / "prep", profile="attachments")
    assert not (tmp_path / "prep" / "fill" / "prepare.json").exists()

    with pytest.raises(RuntimeError, match=f"^{expected_dups} comment ids appear more than once"):
        cfw.prepare_empty_original_document_id(tmp_path / "empty-odid")
    assert not (tmp_path / "empty-odid" / "fill" / "prepare.json").exists()


def test_prepare_stays_spill_free_on_large_catalog_under_tight_budget(tmp_path, monkeypatch):
    """Reproduce the correlated-EXISTS spill OOM on a 500K-row catalog and verify new prepare() runs spill-free."""
    from spicy_regs.duckdb_settings import ExportResources
    from spicy_regs.sources import regulatory_catalog as native

    n_catalog = 500_000
    n_reads = 15_000
    index_src = tmp_path / "synthetic-catalog.parquet"
    reads_src = tmp_path / "synthetic-reads.parquet"
    changed_non_additive = json.dumps([
        {"title": "Letter", "formats": [{**_PDF, "url": f"{_PDF['url']}?v=2"}]},
    ])

    with duckdb.connect() as con:
        con.execute(f"""
            COPY (
                SELECT
                    'EPA-HQ-OAR-2023-' || lpad((hash(i) % 9999)::VARCHAR, 4, '0') || '-' || lpad(i::VARCHAR, 7, '0') AS comment_id,
                    CASE WHEN i % 1000 = 0 THEN NULL
                         ELSE '2024-01-' || lpad(((i % 28) + 1)::VARCHAR, 2, '0') || 'T12:00:00Z' END AS modify_date,
                    's3://spicy-regs/data/part-' || lpad((i % 20)::VARCHAR, 3, '0') || '.parquet' AS _file,
                    CASE
                        WHEN i < 100 THEN {cfw._sql(HELD)}
                        WHEN i < 150 THEN {cfw._sql(HELD)}
                        ELSE NULL
                    END AS attachments_json
                FROM range({n_catalog}) t(i)
            ) TO {cfw._sql(index_src)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        # 15,000 read copies covering 14,800 catalog IDs + 100 agreeing dup copies + 50 conflicting dup copies + 50 absent IDs:
        #   i in 0..99: additive replacement (HELD -> LISTED) -> 100 replaced
        #   i in 100..149: non-additive refusal (HELD -> changed_non_additive) -> 50 refused
        #   i in 150..199: conflicting multi-copy (LISTED vs NULL) -> 50 conflicts
        #   i in 200..299: agreeing multi-copy (LISTED & LISTED) -> 100 fills
        #   i in 300..799: other-version-only (wrong modify_date) -> 500 other_version_only
        #   i in 800..14799: plain NULL fills -> 14,000 fills
        #   50 extra absent IDs -> ignored
        con.execute(f"""
            COPY (
                WITH base AS (
                    SELECT row_number() OVER () - 1 AS idx, comment_id, modify_date
                    FROM read_parquet({cfw._sql(index_src)})
                    LIMIT 14800
                )
                SELECT 'k/' || idx::VARCHAR AS key, comment_id,
                       CASE WHEN idx BETWEEN 300 AND 799 THEN '2019-01-01T00:00:00Z' ELSE modify_date END AS modify_date,
                       CASE WHEN idx BETWEEN 100 AND 149 THEN {cfw._sql(changed_non_additive)} ELSE {cfw._sql(LISTED)} END AS attachments_json
                FROM base
                UNION ALL
                SELECT 'k/dup-conflict/' || idx::VARCHAR, comment_id, modify_date, NULL::VARCHAR
                FROM base WHERE idx BETWEEN 150 AND 199
                UNION ALL
                SELECT 'k/dup-agree/' || idx::VARCHAR, comment_id, modify_date, {cfw._sql(LISTED)}
                FROM base WHERE idx BETWEEN 200 AND 299
                UNION ALL
                SELECT 'k/absent/' || j::VARCHAR, 'ABSENT-' || j::VARCHAR, {cfw._sql(T0)}, {cfw._sql(LISTED)}
                FROM range(50) t(j)
            ) TO {cfw._sql(reads_src)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    # 1. Confirm the old correlated-EXISTS counts query hits the 32MB spill cap on this exact 500K catalog.
    legacy_spill = tmp_path / "legacy-spill"
    legacy_spill.mkdir()
    with duckdb.connect() as con:
        ExportResources(memory="64MB", threads=2).configure(con, legacy_spill)
        con.execute("SET max_temp_directory_size='32MB'")
        with pytest.raises(duckdb.OutOfMemoryException, match="max_temp_directory_size"):
            con.execute(f"""
                SELECT count(*),
                       count(*) FILTER (WHERE NOT EXISTS (
                           SELECT 1 FROM read_parquet({cfw._sql(reads_src)}) r WHERE r.comment_id = c.comment_id
                       )),
                       count(*) FILTER (WHERE EXISTS (
                           SELECT 1 FROM read_parquet({cfw._sql(reads_src)}) r WHERE r.comment_id = c.comment_id
                       ) AND NOT EXISTS (
                           SELECT 1 FROM read_parquet({cfw._sql(reads_src)}) v
                           WHERE v.comment_id = c.comment_id AND v.modify_date IS NOT DISTINCT FROM c.modify_date
                       ))
                FROM read_parquet({cfw._sql(index_src)}) c
            """).fetchall()

    # 2. Run new prepare() end-to-end under the EXACT same 64MB RAM / 32MB spill cap.
    snap = iceberg.CatalogSnapshot("stress", 8949044782115210564, 1)
    monkeypatch.setattr(iceberg, "_selected_catalog_namespace", lambda con, rt: "default_native")
    monkeypatch.setattr(iceberg, "_read_snapshot", lambda con, rt, namespace=None: snap)
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, s, namespace=None: f"SELECT * FROM read_parquet({cfw._sql(index_src)})")
    monkeypatch.setattr(cfw, "FILE_COLUMN", "_file")
    monkeypatch.setattr(cfw, "storage_access", lambda con: None)
    monkeypatch.setattr(
        cfw,
        "data_files",
        lambda con: {f"s3://spicy-regs/data/part-{i:03d}.parquet": 10_000_000 for i in range(20)},
    )

    def synthetic_processing(con, rt, *, where=None, include_comment_promotion=False, **_kw):
        tbl = "_stress_processing"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tbl} AS
            SELECT comment_id, modify_date, attachments_json, FALSE AS _promoted_v2
            FROM read_parquet({cfw._sql(index_src)})
            WHERE {where}
        """)
        return tbl

    monkeypatch.setattr(native, "processing_table", synthetic_processing)

    workdir = tmp_path / "stress-run"
    with duckdb.connect() as con:
        con.execute("SET max_temp_directory_size='32MB'")
        prepared = cfw.prepare(
            workdir,
            reads=reads_src,
            profile="attachments",
            con=con,
            resources=ExportResources(memory="64MB", threads=2),
        )
        spill_files = list((workdir / "fill" / "spill").glob("*"))
        assert spill_files == []

    assert prepared["table_rows"] == n_catalog
    assert prepared["catalog_rows"] == n_catalog
    assert prepared["unread"] == n_catalog - 14_800
    assert prepared["other_version_only"] == 500
    assert prepared["conflicted_versions_by_column"] == {"attachments_json": 50}
    assert prepared["refused_not_additive_by_column"] == {"attachments_json": 50}
    assert prepared["replaced_by_column"] == {"attachments_json": 100}
    assert prepared["cells_by_column"] == {"attachments_json": 14_100}
    assert prepared["rows_to_fill"] == 14_200
    assert not (workdir / "fill" / "work").exists()
    assert n_reads == 15_000


def test_write_batch_and_undo_scope_processing_replace_and_check_by_file(tmp_path, catalog, monkeypatch):
    """All wide catalog reads and MERGE in _write_batch, _check, and undo stay scoped to the batch files."""
    from spicy_regs.sources import regulatory_catalog as native

    seed(catalog, [row("A", agency="EPA"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "Public Comment", "attachments_json": LISTED}])
    # Corrupt CMS's receipt: any unscoped read over non-batch files would fail receipt admission.
    with catalog() as con:
        con.execute(
            f"UPDATE {native.receipts_table()} SET subject_version='corrupted-outside-batch' "
            "WHERE json_extract_string(identity_json, '$[1][0][1][1][1]')='B'"
        )
    cfw.prepare(tmp_path)

    wheres: list[str] = []
    original_processing = native.processing_table

    def spy_processing(con, record_type, **options):
        wheres.append(options.get("where") or "")
        return original_processing(con, record_type, **options)

    monkeypatch.setattr(native, "processing_table", spy_processing)

    merges: list[str] = []
    with catalog() as con:
        written = cfw.write(
            tmp_path,
            con=Proxy(con, after=lambda sql: merges.append(sql) if _merge(sql) else None),
        )
    assert written["rows_changed"] == 1
    assert len(wheres) == 4  # preimage, replace_native prior, replace_native verified, _check
    assert "agency_code IN ('EPA')" in wheres[0]
    assert '"agency_code" IN (\'EPA\')' in wheres[1]
    assert "(\"agency_code\" IN ('EPA') OR \"agency_code\" NOT IN ('CMS', 'EPA'))" in wheres[2]
    assert "(agency_code IN ('EPA') OR agency_code NOT IN ('CMS', 'EPA')) AND comment_id IN (SELECT comment_id FROM _prior)" == wheres[3]
    assert len(merges) == 1
    assert 'AND t."agency_code" IN (\'EPA\')' in merges[0]

    # Undo also stays file-scoped (CMS's damaged receipt remains untouched).
    wheres.clear()
    merges.clear()
    verified_line = [line for line in journal(tmp_path) if line["state"] == "verified"][-1]
    expected = _current(catalog)
    with catalog() as con:
        undone = cfw.undo(
            tmp_path,
            verified_line["batch"],
            expected_snapshot=expected,
            con=Proxy(con, after=lambda sql: merges.append(sql) if _merge(sql) else None),
        )
    assert undone["rows_restored"] == 1
    assert len(wheres) == 3  # undo_now, replace_native prior, replace_native verified
    assert "agency_code IN ('EPA')" in wheres[0]
    assert '"agency_code" IN (\'EPA\')' in wheres[1]
    assert "(\"agency_code\" IN ('EPA') OR \"agency_code\" NOT IN ('CMS', 'EPA'))" in wheres[2]
    assert len(merges) == 1 and 'AND t."agency_code" IN (\'EPA\')' in merges[0]


def test_write_batch_streams_filled_via_temp_parquet_and_cleans_up_on_success_and_failure(tmp_path, catalog):
    """_filled streams through a temporary Parquet-backed TEMP VIEW and is removed on both success and failure."""
    seed(catalog, [row("A", agency="EPA")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "Public Comment"}])
    cfw.prepare(tmp_path)

    captured_paths: list[Path] = []
    seen_sql: list[str] = []

    def record(sql: str) -> None:
        seen_sql.append(sql)
        if _is_filled_copy(sql):
            match = re.search(r" TO '([^']+)'", sql)
            assert match is not None
            captured_paths.append(Path(match.group(1)))

    # 1. Failure path: rolled-back write cleans up the temp _filled Parquet file.
    blank = lambda sql: sql.replace('"comment"=s."comment"', '"comment"=NULL') if _merge(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=blank, after=record))
    assert len(captured_paths) == 1
    assert not captured_paths[0].exists()
    assert not any("CREATE OR REPLACE TEMP TABLE _filled" in sql for sql in seen_sql)
    assert any("CREATE OR REPLACE TEMP VIEW _filled" in sql for sql in seen_sql)

    # 2. Success path: verified write also cleans up the temp _filled Parquet file and leaves only preimage/postimage.
    captured_paths.clear()
    seen_sql.clear()
    with catalog() as con:
        written = cfw.write(tmp_path, con=Proxy(con, after=record), clear_failure=True)
    assert written["rows_changed"] == 1
    assert len(captured_paths) == 1
    assert not captured_paths[0].exists()
    assert not list((tmp_path / "fill" / "preimage").glob("*-filled.parquet"))
    assert table(catalog)["A"]["subtype"] == "Public Comment"
