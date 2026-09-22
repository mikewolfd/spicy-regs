"""H13: exact source variants survive shaping, materialization and replacement."""

import bz2
from datetime import date
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.courtlistener.bulk import CourtListenerBulkReader

from spicy_regs.transforms import build_court_opinion_bodies as build_bodies
from spicy_regs.transforms.build_court_opinion_bodies import (
    COLUMNS,
    OUTPUT,
    SCHEMA_VERSION_KEY,
    _SCHEMA,
    _shape,
)

TEXT_FIELDS = (
    "plain_text",
    "html",
    "html_lawbox",
    "html_columbia",
    "html_anon_2020",
    "html_with_citations",
    "xml_harvard",
    "xml_scan",
)
FIXTURE = Path(__file__).parent / "fixtures/courtlistener_bulk/opinion-380204.csv.bz2"
DUMP_DATE = date(2026, 6, 30)


def _dump(path, records):
    def cell(value):
        return "" if value is None else '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

    body = ",".join(records[0]) + "\n"
    body += "".join(",".join(cell(v) for v in r.values()) + "\n" for r in records)
    path.write_bytes(bz2.compress(body.encode()))
    return path


def test_native_html_and_all_text_fields_survive_materialized_output(tmp_path, ample_disk_space):
    assert (
        sha256(FIXTURE.read_bytes()).hexdigest() == "62cbca0ba7f3fb6c047d5e3448dbf48aad089b26bfceeaadd009d77a7fab44ab"
    )
    [native] = CourtListenerBulkReader("opinions", local_file=FIXTURE).iter_records()
    assert native["id"] == "380204"
    assert len(native["html"]) == 17461
    assert (
        sha256(native["html"].encode()).hexdigest()
        == "716ae706f3d0aad0c2bc5fd5fa8f5a447e1f9d434fcc32bd007fe21c38084e8b"
    )
    output = build_bodies(tmp_path, local_file=FIXTURE, dump_date=DUMP_DATE, rebuild=True)
    table = pq.read_table(output)
    [stored] = table.to_pylist()
    assert {f: stored[f] for f in TEXT_FIELDS} == {f: native[f] for f in TEXT_FIELDS}
    assert stored["opinion_id"] == "380204"
    assert stored["dump_date"] == "2026-06-30"
    assert stored["date_modified"] == native["date_modified"]
    assert table.schema.metadata[SCHEMA_VERSION_KEY.encode()] == b"2"
    assert all(table.schema.field(f).type == pa.string() for f in TEXT_FIELDS)
    # Existing callers can continue selecting the two original named fields.
    assert table.select(["plain_text", "html_with_citations"]).to_pylist() == [
        {f: native[f] for f in ("plain_text", "html_with_citations")}
    ]


@pytest.mark.parametrize("field", TEXT_FIELDS)
@pytest.mark.parametrize("value", [None, "", "  \n", '<p a="é">raw &amp; markup\\path</p>\r\n'])
def test_availability_counts_only_nonempty_retained_source_values(field, value):
    row = _shape({"id": "1", field: value}, dump_date=None)
    assert row[field] == value
    assert all(row[f] is None for f in TEXT_FIELDS if f != field)
    assert row["available_text_fields"] == (field if value else None)
    assert row["text_char_count"] == str(len(value) if value else 0)


def test_multiple_variants_keep_distinct_values_and_longest_codepoint_count():
    native = {"id": "1", "plain_text": "é", "html": "<p>é</p>", "xml_scan": "<scan>雪</scan>"}
    row = _shape(native, dump_date=None)
    assert row["available_text_fields"] == "plain_text,html,xml_scan"
    assert row["text_char_count"] == str(len(native["xml_scan"]))
    assert row["plain_text"] == "é"  # HTML is never substituted as plain text.


@pytest.mark.parametrize("value", [False, 0, [], {}])
def test_nonstring_body_refuses_instead_of_inventing_text(value):
    with pytest.raises(TypeError, match="html must be a string or NULL"):
        _shape({"id": "1", "html": value}, dump_date=None)


def test_legacy_prior_refuses_before_source_read_and_preserves_artifacts(tmp_path, monkeypatch):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("prior already retained"))
    prior = tmp_path / "_bodies_prior.parquet"
    old = {k: v for k, v in _shape({"id": "1", "html": "lost"}, dump_date=DUMP_DATE).items() if k in COLUMNS[:19]}
    pq.write_table(pa.Table.from_pylist([old], schema=pa.schema([(c, pa.string()) for c in COLUMNS[:19]])), prior)
    before = prior.read_bytes()
    output = tmp_path / OUTPUT
    output.write_bytes(b"old published generation")
    with pytest.raises(ValueError, match="pass rebuild=True"):
        build_bodies(tmp_path, local_file=tmp_path / "must-not-be-read")
    assert prior.read_bytes() == before
    assert output.read_bytes() == b"old published generation"
    assert not (tmp_path / "_bodies_new.parquet").exists()


def test_explicit_rebuild_skips_remote_prior_and_retains_legacy_scratch(tmp_path, monkeypatch, ample_disk_space):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("rebuild must not acquire a prior"))
    prior = tmp_path / "_bodies_prior.parquet"
    prior.write_bytes(b"retained legacy artifact")
    output = build_bodies(tmp_path, local_file=FIXTURE, dump_date=DUMP_DATE, rebuild=True)
    assert pq.read_table(output)["opinion_id"].to_pylist() == ["380204"]
    assert prior.read_bytes() == b"retained legacy artifact"


def test_version_two_merge_replaces_removed_variants_and_preserves_unread_rows(tmp_path, monkeypatch, ample_disk_space):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("prior already retained"))
    held = _shape({"id": "2", "xml_scan": "<scan>keep</scan>"}, dump_date=DUMP_DATE)
    changed = _shape({"id": "1", "html": "<p>remove</p>"}, dump_date=DUMP_DATE)
    pq.write_table(pa.Table.from_pylist([held, changed], schema=_SCHEMA), tmp_path / "_bodies_prior.parquet")
    fresh = {"id": "1", **dict.fromkeys(TEXT_FIELDS), "plain_text": "corrected", "html": ""}
    output = build_bodies(tmp_path, local_file=_dump(tmp_path / "fresh.csv.bz2", [fresh]), dump_date=DUMP_DATE)
    table = pq.read_table(output)
    actual = {r["opinion_id"]: r for r in table.to_pylist()}
    assert actual["2"] == held
    assert actual["1"]["html"] == ""
    assert actual["1"]["xml_scan"] is None
    assert actual["1"]["available_text_fields"] == "plain_text"
    assert actual["1"]["text_char_count"] == "9"
    assert table.schema.metadata[SCHEMA_VERSION_KEY.encode()] == b"2"
