"""Offline typed candidates preserve identity, order, field coverage and failures."""

import hashlib
import json
from datetime import date

import duckdb
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.federal_register import native
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.transforms.build_federal_register import build_federal_register_layer1
from tests.test_federal_register import _RAW_DOC


def document(**changes):
    return {**dict.fromkeys(native.DOCUMENT_FIELDS), **_RAW_DOC, "publication_date": "2024-03-01",
            "effective_on": "2024-04-01", "executive_order_number": "14101", **changes}


def inputs(tmp_path, records, *, repeated=False, body_changes=None):
    scope = {"publishedFrom": "2024-03-01", "publishedThrough": "2024-03-02"}
    url = native.federal_register_documents_url(scope)
    body = {"count": len(records), "total_pages": 1, "results": records, **(body_changes or {})}
    raw = json.dumps(body).encode()
    sha = "sha256:" + hashlib.sha256(raw).hexdigest()
    archive = tmp_path / "raw"
    CentralRawArchiveStore(archive).put_blob(sha, len(raw), [raw])
    page = {"url": url, "sha256": sha}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"query_scope": scope, "pages": [page, page] if repeated else [page]}))
    return manifest, archive, sha


def test_typed_source_values_and_children(tmp_path):
    raw = document(cfr_references=[{"title": 40, "part": 60, "chapter": "I", "citation_url": None},
                                    {"title": 40, "part": "60a", "chapter": 2, "citation_url": "https://example.org"}],
                   docket_ids=["Docket No. EPA-HQ-OAR-2024-0001"],
                   regulations_dot_gov_info={"docket_id": "EPA-HQ-OAR-2024-0002"})
    manifest, archive, sha = inputs(tmp_path, [raw])
    out = tmp_path / "candidate"
    build_federal_register_layer1(manifest, archive, out)
    with duckdb.connect() as con:
        assert con.execute("SELECT typeof(effective_on), effective_on, typeof(executive_order_number), "
                           "executive_order_number, source_record_id FROM read_parquet(?)", [str(out / "federal_register_documents.parquet")]).fetchone() == (
                               "DATE", date(2024, 4, 1), "BIGINT", 14101, "2024-00001@2024-03-01")
        assert con.execute("SELECT position, chapter, citation_url, typeof(chapter), typeof(citation_url) "
                           "FROM read_parquet(?) ORDER BY position", [str(out / "federal_register_cfr_references.parquet")]).fetchall() == [
                               (0, "I", None, "VARCHAR", "VARCHAR"), (1, "2", "https://example.org", "VARCHAR", "VARCHAR")]
    child = pq.read_table(out / "federal_register_agencies.parquet").to_pylist()
    assert [r["position"] for r in child] == [0, 1]
    assert all(r["source_sha256"] == sha and r["source_record_id"] == "2024-00001@2024-03-01" for r in child)
    assert child[1]["source_locator"] == "/results/0/agencies/1"
    receipt = json.loads((out / "inputs.json").read_text())
    assert all(c["present"] == 1 and c["absent"] == 0 and not c["unrequested"] for c in receipt["coverage"].values())
    row = pq.read_table(out / "federal_register_documents.parquet").to_pylist()[0]
    assert json.loads(row["regulations_dot_gov_info"])["docket_id"] == "EPA-HQ-OAR-2024-0002"
    candidate, = pq.read_table(out / "federal_register_docket_candidates.parquet").to_pylist()
    assert (candidate["label"], candidate["docket_id"]) == ("Docket No. EPA-HQ-OAR-2024-0001", "EPA-HQ-OAR-2024-0001")


def test_dated_identity_duplicate_and_conflict(tmp_path):
    raw = document()
    records = [raw, dict(reversed(list(raw.items()))), document(publication_date="2024-03-02")]
    manifest, archive, _ = inputs(tmp_path, records)
    out = tmp_path / "candidate"
    build_federal_register_layer1(manifest, archive, out)
    assert pq.read_table(out / "federal_register_documents.parquet").num_rows == 2
    assert json.loads((out / "inputs.json").read_text())["duplicate_records"] == 1
    manifest, archive, _ = inputs(tmp_path, [raw, document(title="Conflicting title")])
    with pytest.raises(ValueError, match="Conflicting Federal Register"):
        build_federal_register_layer1(manifest, archive, tmp_path / "refused")
    assert not (tmp_path / "refused").exists()


@pytest.mark.parametrize("failure", ["cursor", "cyclic", "date", "inventory", "bounds"])
def test_refuses_incomplete_or_ambiguous_inputs(tmp_path, failure):
    record = document()
    if failure == "date":
        record["effective_on"] = "unknown"
    body_changes = {"count": 2} if failure == "inventory" else None
    if failure == "cyclic":
        scope = {"publishedFrom": "2024-03-01", "publishedThrough": "2024-03-02"}
        body_changes = {"next_page_url": native.federal_register_documents_url(scope)}
    manifest, archive, _ = inputs(tmp_path, [record], repeated=failure == "cursor", body_changes=body_changes)
    with pytest.raises(ValueError):
        build_federal_register_layer1(manifest, archive, tmp_path / "refused", max_records=0 if failure == "bounds" else 10000)
    assert not (tmp_path / "refused").exists()


def test_refuses_unconsumed_page_with_current_fields(tmp_path):
    manifest, archive, _ = inputs(tmp_path, [document()])
    selection = json.loads(manifest.read_text())
    first_page, = selection["pages"]
    selection["pages"].append({**first_page, "url": f"{first_page['url']}&page=2"})
    manifest.write_text(json.dumps(selection))

    destination = tmp_path / "refused"
    with pytest.raises(ValueError, match="Retained Federal Register manifest contains unconsumed pages"):
        build_federal_register_layer1(manifest, archive, destination)
    assert not destination.exists()


def test_sparse_source_distinguishes_absent_from_null(tmp_path):
    record = document(action=None)
    del record["title"]
    manifest, archive, _ = inputs(tmp_path, [record])
    out = tmp_path / "candidate"
    build_federal_register_layer1(manifest, archive, out)
    row = pq.read_table(out / "federal_register_documents.parquet").to_pylist()[0]
    assert row["title"] is None and row["action"] is None
    assert "title" not in row["member_names"] and "action" in row["member_names"]
    coverage = json.loads((out / "inputs.json").read_text())["coverage"]
    assert coverage["title"] == {"present": 0, "null": 0, "populated": 0, "absent": 1, "unrequested": False}
    assert coverage["action"]["present"] == coverage["action"]["null"] == 1


def test_refuses_changed_field_set_in_continuation(tmp_path):
    scope = {"publishedFrom": "2024-03-01", "publishedThrough": "2024-03-02"}
    initial = native.federal_register_documents_url(scope)
    continuation = initial.replace("&fields%5B%5D=full_text_xml_url", "") + "&page=2"
    manifest, archive, _ = inputs(tmp_path, [document()], body_changes={
        "count": 2, "total_pages": 2, "next_page_url": continuation,
    })
    second = json.dumps({
        "count": 2, "total_pages": 2, "results": [document(publication_date="2024-03-02")],
    }).encode()
    sha = "sha256:" + hashlib.sha256(second).hexdigest()
    CentralRawArchiveStore(archive).put_blob(sha, len(second), [second])
    selection = json.loads(manifest.read_text())
    selection["pages"].append({"url": continuation, "sha256": sha})
    manifest.write_text(json.dumps(selection))

    destination = tmp_path / "refused"
    with pytest.raises(ValueError, match="request field set differs from current fields"):
        build_federal_register_layer1(manifest, archive, destination)
    assert not destination.exists()


@pytest.mark.parametrize("symlink", [False, True])
def test_refuses_output_in_raw_archive(tmp_path, symlink):
    manifest, archive, _ = inputs(tmp_path, [document()])
    selected = tmp_path / "raw-link" if symlink else archive
    if symlink:
        selected.symlink_to(archive, target_is_directory=True)
    before = sorted(p.relative_to(archive) for p in archive.rglob("*"))
    with pytest.raises(ValueError, match="outside the raw archive"):
        build_federal_register_layer1(manifest, archive, selected / "derived")
    assert sorted(p.relative_to(archive) for p in archive.rglob("*")) == before


def test_failed_write_leaves_no_output_and_retry_succeeds(tmp_path, monkeypatch):
    from spicy_regs.transforms import federal_register_layer1 as layer1

    manifest, archive, _ = inputs(tmp_path, [document()])
    out = tmp_path / "candidate"
    original = layer1.pq.write_table
    writes = []

    def interrupted(table, path, **options):
        writes.append(path)
        if len(writes) == 2:
            raise OSError("injected second-table write failure")
        original(table, path, **options)

    monkeypatch.setattr(layer1.pq, "write_table", interrupted)
    with pytest.raises(OSError, match="injected"):
        build_federal_register_layer1(manifest, archive, out)
    assert not out.exists() and not list(tmp_path.glob(".candidate-*"))
    monkeypatch.setattr(layer1.pq, "write_table", original)
    build_federal_register_layer1(manifest, archive, out)
    assert (out / "inputs.json").is_file()
    receipt = (out / "inputs.json").read_bytes()
    with pytest.raises(FileExistsError):
        build_federal_register_layer1(manifest, archive, out)
    assert (out / "inputs.json").read_bytes() == receipt


def test_competing_destination_is_preserved(tmp_path, monkeypatch):
    from spicy_regs.transforms import federal_register_layer1 as layer1

    manifest, archive, _ = inputs(tmp_path, [document()])
    out = tmp_path / "candidate"
    original = layer1.pq.write_table

    def competing_writer(table, path, **options):
        out.mkdir(exist_ok=True)
        (out / "other-owner").write_text("preserve")
        original(table, path, **options)

    monkeypatch.setattr(layer1.pq, "write_table", competing_writer)
    with pytest.raises(FileExistsError):
        build_federal_register_layer1(manifest, archive, out)
    assert list(out.iterdir()) == [out / "other-owner"]
    assert (out / "other-owner").read_text() == "preserve"


# ---------------------------------------------------------------------------
# Wave 4C Slice 2 (ss-q44t): A2–A9 planted-break tests, streaming R2-blob
# replay / incremental merge, rollup wiring, ODCS/DDK parity, Finding 5.
# ---------------------------------------------------------------------------


def _page_blob(records: list[dict], *, body_changes: dict | None = None) -> tuple[bytes, str]:
    body = {"count": len(records), "total_pages": 1, "results": records, **(body_changes or {})}
    raw = json.dumps(body).encode("utf-8")
    return raw, "sha256:" + hashlib.sha256(raw).hexdigest()


def _write_pinned_fr(path, rows: list[dict[str, str | None]]) -> None:
    import pyarrow as pa

    schema = pa.schema([
        ("document_number", pa.string()),
        ("publication_date", pa.string()),
        ("source_sha256", pa.string()),
    ])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def test_a2_maps_all_31_fields_without_drops(tmp_path):
    """A2: Every one of the 31 native.DOCUMENT_FIELDS maps into federal_register_documents with exact types."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    full = document(
        document_number="2025-04200",
        publication_date="2025-02-14",
        effective_on="2025-03-17",
        comments_close_on="2025-04-15",
        signing_date="2025-02-10",
        title="Full 31-Field Rule",
        abstract="Exercises every retained field.",
        type="Rule",
        subtype="Executive Order",
        action="Final rule.",
        correction_of="https://www.federalregister.gov/api/v1/documents/2025-00001.json",
        corrections=["https://www.federalregister.gov/api/v1/documents/2025-04299.json"],
        significant=True,
        full_text_xml_url="https://www.federalregister.gov/documents/full_text/xml/2025/02/14/2025-04200.xml",
        html_url="https://www.federalregister.gov/d/2025-04200",
        pdf_url="https://www.govinfo.gov/content/pkg/FR-2025-02-14/pdf/2025-04200.pdf",
        body_html_url="https://www.federalregister.gov/documents/full_text/html/2025/02/14/2025-04200.html",
        agency_names=["Environmental Protection Agency"],
        topics=["Air pollution control", "Reporting and recordkeeping requirements"],
        president={"identifier": "donald-trump", "name": "Donald Trump"},
        disposition_notes="Revokes EO 14000.",
        executive_order_notes="See also EO 14001.",
        executive_order_number="14200",
        regulations_dot_gov_info={"docket_id": "EPA-HQ-OAR-2025-0100", "comments_count": 7},
        volume=90,
        start_page=9100,
        end_page=9115,
    )
    assert set(full) == native.DOCUMENT_FIELDS
    raw, sha = _page_blob([full])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": "2025-04200", "publication_date": "2025-02-14", "source_sha256": sha}])

    docs_path, *_ = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda digest: {sha: raw}[digest]
    )
    table = pq.read_table(docs_path)
    assert native.DOCUMENT_FIELDS <= set(table.schema.names)
    row = table.to_pylist()[0]
    assert row["action"] == "Final rule."
    assert row["correction_of"] == full["correction_of"]
    assert row["corrections"] == full["corrections"]
    assert row["significant"] is True
    assert row["full_text_xml_url"] == full["full_text_xml_url"]
    assert row["agency_names"] == ["Environmental Protection Agency"]
    assert row["topics"] == ["Air pollution control", "Reporting and recordkeeping requirements"]
    assert json.loads(row["president"]) == {"identifier": "donald-trump", "name": "Donald Trump"}
    assert row["disposition_notes"] == "Revokes EO 14000."
    assert row["executive_order_notes"] == "See also EO 14001."
    assert row["executive_order_number"] == 14200
    assert row["comments_close_on"] == date(2025, 4, 15)
    assert row["signing_date"] == date(2025, 2, 10)
    assert json.loads(row["regulations_dot_gov_info"])["comments_count"] == 7
    assert set(row["member_names"]) == native.DOCUMENT_FIELDS


def test_a2_refuses_legacy_22_field_pre_13_and_cursor_drift():
    """A2/A4 planted-break: legacy 22-field, pre-1.3 28-field, cursor fields[] drift, and extra keys refuse fail-closed."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    legacy_22 = {k: v for k, v in document().items() if k not in layer1.LEGACY_22_ABSENT_FIELDS}
    raw_22, sha_22 = _page_blob([legacy_22])
    with pytest.raises(ValueError, match="legacy 22-field"):
        list(layer1.iter_typed_page_records(raw_22, sha_22))

    legacy_28 = {k: v for k, v in document().items() if k not in layer1.SCHEMA_13_ADDED_FIELDS}
    raw_28, sha_28 = _page_blob([legacy_28])
    with pytest.raises(ValueError, match="pre-1.3"):
        list(layer1.iter_typed_page_records(raw_28, sha_28))

    # Even if records happen to have 31 keys, a cursor URL requesting only 22 fields[] must be refused.
    scope = {"publishedFrom": "2024-03-01", "publishedThrough": "2024-03-02"}
    bad_next = native.federal_register_documents_url(scope)
    for dropped in layer1.LEGACY_22_ABSENT_FIELDS:
        bad_next = bad_next.replace(f"&fields%5B%5D={dropped}", "")
    raw_cursor, sha_cursor = _page_blob([document()], body_changes={"next_page_url": bad_next + "&page=2"})
    with pytest.raises(ValueError, match="legacy 22-field"):
        list(layer1.iter_typed_page_records(raw_cursor, sha_cursor))

    # Unclassified extra field must also refuse fail-closed.
    extra = {**document(), "unmapped_future_field": "surprise"}
    raw_extra, sha_extra = _page_blob([extra])
    with pytest.raises(ValueError, match="unclassified"):
        list(layer1.iter_typed_page_records(raw_extra, sha_extra))


def test_a3_real_00_111_collision_fixture_preserves_both_dates_and_children(tmp_path):
    """A3: Real 00-111 publisher collision fixture survives on both dates with distinct agencies and CFR children."""
    from pathlib import Path

    from spicy_regs.transforms import federal_register_layer1 as layer1

    raw_fixture = json.loads((Path(__file__).parent / "fixtures/federal_register/00-111.json").read_text())
    records = [{**dict.fromkeys(native.DOCUMENT_FIELDS), **rec} for rec in raw_fixture]
    raw, sha = _page_blob(records)
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "00-111", "publication_date": "2000-01-14", "source_sha256": sha},
            {"document_number": "00-111", "publication_date": "2000-01-18", "source_sha256": sha},
        ],
    )
    docs_path, ag_path, cfr_path, _ = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda d: {sha: raw}[d]
    )
    with duckdb.connect() as con:
        docs = con.execute(
            "SELECT document_number, publication_date, source_record_id FROM read_parquet(?) ORDER BY publication_date",
            [str(docs_path)],
        ).fetchall()
        assert docs == [
            ("00-111", date(2000, 1, 14), "00-111@2000-01-14"),
            ("00-111", date(2000, 1, 18), "00-111@2000-01-18"),
        ]
        ag_by_date = con.execute(
            "SELECT publication_date, list(slug ORDER BY position) FROM read_parquet(?) "
            "GROUP BY publication_date ORDER BY publication_date",
            [str(ag_path)],
        ).fetchall()
        assert ag_by_date == [
            (date(2000, 1, 14), ["treasury-department", "internal-revenue-service"]),
            (date(2000, 1, 18), ["interior-department", "land-management-bureau"]),
        ]
        cfr_counts = con.execute(
            "SELECT publication_date, count(*) FROM read_parquet(?) GROUP BY publication_date",
            [str(cfr_path)],
        ).fetchall()
        assert cfr_counts == [(date(2000, 1, 14), 2)]


def test_a5_agencies_preserve_order_repeated_canonical_ids_urls_and_member_names(tmp_path):
    """A5 planted-break: publisher order, repeated canonical id/slug, URLs, and child member_names survive uncollapsed."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    rec = document(
        agencies=[
            {
                "raw_name": "ENVIRONMENTAL PROTECTION AGENCY — OFFICE OF AIR",
                "name": "Environmental Protection Agency",
                "id": 145,
                "parent_id": None,
                "slug": "environmental-protection-agency",
                "url": "https://www.federalregister.gov/agencies/environmental-protection-agency",
                "json_url": "https://www.federalregister.gov/api/v1/agencies/145",
            },
            {
                "raw_name": "DEPARTMENT OF TRANSPORTATION",
                "name": "Transportation Department",
                "id": 492,
                "parent_id": None,
                "slug": "transportation-department",
                "url": "https://www.federalregister.gov/agencies/transportation-department",
                "json_url": "https://www.federalregister.gov/api/v1/agencies/492",
            },
            {
                # Same canonical id/slug as position 0, distinct raw_name, and omits parent_id entirely.
                "raw_name": "ENVIRONMENTAL PROTECTION AGENCY — OFFICE OF WATER",
                "name": "Environmental Protection Agency",
                "id": 145,
                "slug": "environmental-protection-agency",
                "url": "https://www.federalregister.gov/agencies/environmental-protection-agency",
                "json_url": "https://www.federalregister.gov/api/v1/agencies/145",
            },
        ]
    )
    raw, sha = _page_blob([rec])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": rec["document_number"], "publication_date": rec["publication_date"], "source_sha256": sha}])
    _, ag_path, _, _ = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda d: {sha: raw}[d]
    )
    rows = pq.read_table(ag_path).to_pylist()
    assert len(rows) == 3
    assert [r["position"] for r in rows] == [0, 1, 2]
    assert [r["id"] for r in rows] == [145, 492, 145]
    assert [r["slug"] for r in rows] == [
        "environmental-protection-agency",
        "transportation-department",
        "environmental-protection-agency",
    ]
    assert [r["source_locator"] for r in rows] == [
        "/results/0/agencies/0",
        "/results/0/agencies/1",
        "/results/0/agencies/2",
    ]
    assert rows[0]["url"] == "https://www.federalregister.gov/agencies/environmental-protection-agency"
    assert rows[0]["json_url"] == "https://www.federalregister.gov/api/v1/agencies/145"
    # Position 0 had explicit parent_id=None; position 2 omitted parent_id completely.
    assert rows[0]["parent_id"] is None and "parent_id" in rows[0]["member_names"]
    assert rows[2]["parent_id"] is None and "parent_id" not in rows[2]["member_names"]


def test_a6_native_column_types_and_mixed_cfr_spellings(tmp_path):
    """A6 planted-break: native DATE/BIGINT/BOOLEAN/INTEGER types and mixed CFR part/chapter VARCHAR + citation_url."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    rec = document(
        comments_close_on="2024-05-01",
        signing_date="2024-02-28",
        significant=False,
        cfr_references=[
            {"title": 40, "part": 60, "chapter": "I", "citation_url": None},
            {"title": 40, "part": "60a", "chapter": 2, "citation_url": "https://www.ecfr.gov/current/title-40/part-60a"},
            {"title": 14, "part": "1-199", "chapter": 0},  # citation_url omitted vs explicit null above
        ],
    )
    raw, sha = _page_blob([rec])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": rec["document_number"], "publication_date": rec["publication_date"], "source_sha256": sha}])
    docs_path, ag_path, cfr_path, dkt_path = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda d: {sha: raw}[d]
    )
    with duckdb.connect() as con:
        doc_types = con.execute(
            """SELECT typeof(publication_date), typeof(effective_on), typeof(comments_close_on), typeof(signing_date),
                      typeof(volume), typeof(start_page), typeof(end_page), typeof(executive_order_number),
                      typeof(significant)
               FROM read_parquet(?)""",
            [str(docs_path)],
        ).fetchone()
        assert doc_types == ("DATE", "DATE", "DATE", "DATE", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BOOLEAN")

        cfr_rows = con.execute(
            """SELECT position, typeof(position), title, typeof(title), part, typeof(part),
                      chapter, typeof(chapter), citation_url, typeof(citation_url), member_names
               FROM read_parquet(?) ORDER BY position""",
            [str(cfr_path)],
        ).fetchall()
        assert [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]) for r in cfr_rows] == [
            (0, "INTEGER", 40, "BIGINT", "60", "VARCHAR", "I", "VARCHAR", None, "VARCHAR"),
            (1, "INTEGER", 40, "BIGINT", "60a", "VARCHAR", "2", "VARCHAR", "https://www.ecfr.gov/current/title-40/part-60a", "VARCHAR"),
            (2, "INTEGER", 14, "BIGINT", "1-199", "VARCHAR", "0", "VARCHAR", None, "VARCHAR"),
        ]
        assert "citation_url" in cfr_rows[0][10] and "citation_url" not in cfr_rows[2][10]
        assert con.execute("SELECT typeof(position) FROM read_parquet(?) LIMIT 1", [str(ag_path)]).fetchone() == ("INTEGER",)
        assert con.execute(
            "SELECT typeof(label_position), typeof(candidate_position) FROM read_parquet(?) LIMIT 1",
            [str(dkt_path)],
        ).fetchone() == ("INTEGER", "INTEGER")


def test_a7_a8_distinguishes_absent_null_false_empty_list_and_empty_object(tmp_path):
    """A7/A8 planted-break: absent key vs explicit null vs false vs [] vs {} remain distinguishable."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    sparse = document(
        document_number="2025-00001",
        publication_date="2025-02-01",
        action=None,
        significant=False,
        topics=[],
        corrections=[],
        docket_ids=[],
        regulations_dot_gov_info={},
    )
    del sparse["title"]  # Absent on record 0, present on record 1 so page-level 31-field union holds.

    explicit_nulls = document(
        document_number="2025-00002",
        publication_date="2025-02-01",
        title=None,
        action=None,
        significant=None,
        topics=None,
        corrections=None,
        docket_ids=None,
        regulations_dot_gov_info=None,
    )
    raw, sha = _page_blob([sparse, explicit_nulls])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-00001", "publication_date": "2025-02-01", "source_sha256": sha},
            {"document_number": "2025-00002", "publication_date": "2025-02-01", "source_sha256": sha},
        ],
    )
    docs_path, *_ = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda d: {sha: raw}[d]
    )
    by_doc = {r["document_number"]: r for r in pq.read_table(docs_path).to_pylist()}
    r0, r1 = by_doc["2025-00001"], by_doc["2025-00002"]

    # Absent vs explicit null on title
    assert r0["title"] is None and "title" not in r0["member_names"]
    assert r1["title"] is None and "title" in r1["member_names"]
    # Explicit null on action
    assert r0["action"] is None and "action" in r0["member_names"]
    # Explicit false vs explicit null on significant
    assert r0["significant"] is False and r1["significant"] is None
    # Explicit [] vs explicit null on lists
    assert r0["topics"] == [] and r1["topics"] is None
    assert r0["corrections"] == [] and r1["corrections"] is None
    assert r0["docket_ids"] == [] and r1["docket_ids"] is None
    # Explicit {} vs explicit null on JSON objects
    assert r0["regulations_dot_gov_info"] == "{}" and r1["regulations_dot_gov_info"] is None


def test_a9_docket_ids_literal_multi_docket_normalization_and_regs_info_separation(tmp_path):
    """A9 planted-break: docket_ids stays literal, multi-docket labels normalize with positions, regs_info stays separate."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    labels = [
        "A-570-893",  # Non-regulations.gov ITA case number: stays in docket_ids, yields 0 candidates.
        "Docket Nos. EPA-HQ-OAR-2024-0001 and FAA-2024-0002",  # Multi-docket label -> candidate_position 0 and 1.
        "Docket ID: DoD-2024-OS-0147",  # Case-folded canonical docket ID.
    ]
    rec = document(
        docket_ids=labels,
        regulations_dot_gov_info={"docket_id": "CMS-2024-9999", "document_id": "CMS-2024-9999-0001"},
    )
    raw, sha = _page_blob([rec])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": rec["document_number"], "publication_date": rec["publication_date"], "source_sha256": sha}])
    docs_path, _, _, dkt_path = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda d: {sha: raw}[d]
    )
    doc_row = pq.read_table(docs_path).to_pylist()[0]
    assert doc_row["docket_ids"] == labels
    assert json.loads(doc_row["regulations_dot_gov_info"])["docket_id"] == "CMS-2024-9999"

    candidates = pq.read_table(dkt_path).to_pylist()
    assert [(c["label_position"], c["candidate_position"], c["label"], c["docket_id"]) for c in candidates] == [
        (1, 0, labels[1], "EPA-HQ-OAR-2024-0001"),
        (1, 1, labels[1], "FAA-2024-0002"),
        (2, 0, labels[2], "DOD-2024-OS-0147"),
    ]
    assert all(c["docket_id"] != "CMS-2024-9999" for c in candidates)


def test_streaming_r2_builder_matches_offline_build_row_for_row(tmp_path):
    """Parity test: streaming R2-blob builder (with forced multi-chunk flushes) equals offline build() row-for-row."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    scope = {"publishedFrom": "2025-02-01", "publishedThrough": "2025-04-30"}
    page1_url = native.federal_register_documents_url(scope)
    page2_url = f"{page1_url}&page=2"
    page1_records = [
        document(
            document_number="2025-02001",
            publication_date="2025-02-10",
            effective_on="2025-03-10",
            significant=True,
            docket_ids=["Docket Nos. EPA-HQ-OAR-2025-0001 and FAA-2025-0002"],
            cfr_references=[{"title": 40, "part": 60, "chapter": "I", "citation_url": None}],
            executive_order_number=None,
        ),
        document(
            document_number="2025-02002",
            publication_date="2025-03-05",
            effective_on=None,
            signing_date="2025-03-01",
            significant=False,
            executive_order_number="14210",
            president={"identifier": "donald-trump", "name": "Donald Trump"},
            docket_ids=["A-570-893"],
            cfr_references=[],
        ),
    ]
    page2_records = [
        document(
            document_number="2025-02001",
            publication_date="2025-04-02",  # Same document_number on a second date!
            effective_on=None,
            significant=None,
            docket_ids=["Docket ID: DoD-2025-OS-0010"],
            cfr_references=[{"title": 32, "part": "199a", "chapter": 1, "citation_url": "https://example.org/32/199a"}],
            executive_order_number=None,
        ),
    ]
    raw1, sha1 = _page_blob(page1_records, body_changes={"count": 3, "total_pages": 2, "next_page_url": page2_url})
    raw2, sha2 = _page_blob(page2_records, body_changes={"count": 3, "total_pages": 2, "next_page_url": None})

    archive = tmp_path / "raw"
    store = CentralRawArchiveStore(archive)
    store.put_blob(sha1, len(raw1), [raw1])
    store.put_blob(sha2, len(raw2), [raw2])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "query_scope": scope,
        "pages": [{"url": page1_url, "sha256": sha1}, {"url": page2_url, "sha256": sha2}],
    }))
    offline_dir = tmp_path / "offline"
    build_federal_register_layer1(manifest, archive, offline_dir)

    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-02001", "publication_date": "2025-02-10", "source_sha256": sha1},
            {"document_number": "2025-02002", "publication_date": "2025-03-05", "source_sha256": sha1},
            {"document_number": "2025-02001", "publication_date": "2025-04-02", "source_sha256": sha2},
        ],
    )
    stream_dir = tmp_path / "stream"
    layer1.build_federal_register_typed_from_r2(
        stream_dir,
        fr_path=fr_path,
        fetch_blob=lambda digest: {sha1: raw1, sha2: raw2}[digest],
        flush_rows=1,  # Force chunked flushes on every single record across all 4 writers.
        max_workers=2,
    )

    sort_keys = {
        layer1.DOCUMENTS_TABLE: lambda r: (r["publication_date"], r["document_number"]),
        layer1.AGENCIES_TABLE: lambda r: (r["publication_date"], r["document_number"], r["position"]),
        layer1.CFR_REFERENCES_TABLE: lambda r: (r["publication_date"], r["document_number"], r["position"]),
        layer1.DOCKET_CANDIDATES_TABLE: lambda r: (
            r["publication_date"],
            r["document_number"],
            r["label_position"],
            r["candidate_position"],
        ),
    }
    for name in layer1.TABLES:
        offline_tbl = pq.read_table(offline_dir / f"{name}.parquet")
        stream_tbl = pq.read_table(stream_dir / f"{name}.parquet")
        assert stream_tbl.schema.equals(offline_tbl.schema, check_metadata=True), name
        assert sorted(stream_tbl.to_pylist(), key=sort_keys[name]) == sorted(
            offline_tbl.to_pylist(), key=sort_keys[name]
        ), name


def test_streaming_r2_multi_blob_overlap_selects_pinned_winning_sha(tmp_path):
    """Multi-blob overlap: a document appearing in an older and newer blob takes only its pinned winning source_sha256."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    doc_a_v1 = document(document_number="2025-10001", publication_date="2025-03-01", title="Old Draft Title")
    doc_d = document(document_number="2025-10002", publication_date="2025-03-01", title="Only In Blob 1")
    doc_a_v2 = document(document_number="2025-10001", publication_date="2025-03-01", title="Updated Published Title")
    doc_b = document(document_number="2025-10003", publication_date="2025-03-02", title="Only In Blob 2")

    raw1, sha1 = _page_blob([doc_a_v1, doc_d])
    raw2, sha2 = _page_blob([doc_a_v2, doc_b])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-10002", "publication_date": "2025-03-01", "source_sha256": sha1},
            {"document_number": "2025-10001", "publication_date": "2025-03-01", "source_sha256": sha2},
            {"document_number": "2025-10003", "publication_date": "2025-03-02", "source_sha256": sha2},
        ],
    )
    docs_path, *_ = layer1.build_federal_register_typed_from_r2(
        tmp_path / "out", fr_path=fr_path, fetch_blob=lambda d: {sha1: raw1, sha2: raw2}[d]
    )
    got = {r["document_number"]: (r["title"], r["source_sha256"]) for r in pq.read_table(docs_path).to_pylist()}
    assert got == {
        "2025-10001": ("Updated Published Title", sha2),
        "2025-10002": ("Only In Blob 1", sha1),
        "2025-10003": ("Only In Blob 2", sha2),
    }


def test_incremental_merge_fetches_only_changed_blobs_and_reconciles_all_four_tables(tmp_path):
    """Incremental mode fetches only new/changed blobs and reconciles unchanged/updated/added/removed across all 4 tables."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    doc_keep = document(
        document_number="2025-01001",
        publication_date="2025-01-10",
        title="Unchanged Doc",
        docket_ids=["EPA-HQ-OAR-2025-0001"],
        cfr_references=[{"title": 40, "part": 50, "chapter": "I", "citation_url": None}],
    )
    doc_update_v1 = document(
        document_number="2025-01002",
        publication_date="2025-01-11",
        title="Before Update",
        docket_ids=["FAA-2025-0001"],
        cfr_references=[{"title": 14, "part": 39, "chapter": "I", "citation_url": None}],
    )
    doc_remove = document(
        document_number="2025-01003",
        publication_date="2025-01-12",
        title="Removed Doc",
        docket_ids=["CMS-2025-0001"],
        cfr_references=[{"title": 42, "part": 412, "chapter": "IV", "citation_url": None}],
    )
    raw_keep, sha_keep = _page_blob([doc_keep])
    raw_old, sha_old = _page_blob([doc_update_v1, doc_remove])

    out_dir = tmp_path / "run"
    out_dir.mkdir()
    fr_path = out_dir / "federal_register.parquet"
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-01001", "publication_date": "2025-01-10", "source_sha256": sha_keep},
            {"document_number": "2025-01002", "publication_date": "2025-01-11", "source_sha256": sha_old},
            {"document_number": "2025-01003", "publication_date": "2025-01-12", "source_sha256": sha_old},
        ],
    )
    blobs = {sha_keep: raw_keep, sha_old: raw_old}
    layer1.merge_federal_register_typed(
        out_dir,
        fetch_blob=lambda d: blobs[d],
        download_prior=lambda *_: False,
    )

    # Next day: doc_keep unchanged; doc_update_v2 replaces doc_update_v1; doc_remove dropped; doc_add added.
    doc_update_v2 = document(
        document_number="2025-01002",
        publication_date="2025-01-11",
        title="After Update",
        agencies=[{"raw_name": "UPDATED AGENCY", "name": "Updated Agency", "id": 999, "slug": "updated-agency"}],
        docket_ids=["FAA-2025-9999"],
        cfr_references=[{"title": 14, "part": 91, "chapter": "I", "citation_url": "https://example.org/14/91"}],
    )
    doc_add = document(
        document_number="2025-01004",
        publication_date="2025-01-13",
        title="Brand New Doc",
        docket_ids=["FCC-2025-0007"],
        cfr_references=[{"title": 47, "part": 73, "chapter": "I", "citation_url": None}],
    )
    raw_new, sha_new = _page_blob([doc_update_v2, doc_add])
    blobs[sha_new] = raw_new
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-01001", "publication_date": "2025-01-10", "source_sha256": sha_keep},
            {"document_number": "2025-01002", "publication_date": "2025-01-11", "source_sha256": sha_new},
            {"document_number": "2025-01004", "publication_date": "2025-01-13", "source_sha256": sha_new},
        ],
    )

    fetched: list[str] = []

    def tracking_fetch(digest: str) -> bytes:
        fetched.append(digest)
        return blobs[digest]

    docs_p, ag_p, cfr_p, dkt_p = layer1.merge_federal_register_typed(
        out_dir,
        fetch_blob=tracking_fetch,
        download_prior=lambda *_: False,  # Picks up existing local outputs as prior.
    )
    assert fetched == [sha_new], f"Expected only changed blob {sha_new}, fetched {fetched}"

    docs = {r["document_number"]: r for r in pq.read_table(docs_p).to_pylist()}
    assert set(docs) == {"2025-01001", "2025-01002", "2025-01004"}
    assert docs["2025-01001"]["title"] == "Unchanged Doc"
    assert docs["2025-01002"]["title"] == "After Update"
    assert docs["2025-01004"]["title"] == "Brand New Doc"

    ag_by_doc: dict[str, list[str]] = {}
    for row in pq.read_table(ag_p).to_pylist():
        ag_by_doc.setdefault(row["document_number"], []).append(row["raw_name"])
    assert set(ag_by_doc) == {"2025-01001", "2025-01002", "2025-01004"}
    assert ag_by_doc["2025-01002"] == ["UPDATED AGENCY"]

    cfr_by_doc = {r["document_number"]: (r["title"], r["part"], r["citation_url"]) for r in pq.read_table(cfr_p).to_pylist()}
    assert cfr_by_doc == {
        "2025-01001": (40, "50", None),
        "2025-01002": (14, "91", "https://example.org/14/91"),
        "2025-01004": (47, "73", None),
    }

    dkt_by_doc = {r["document_number"]: r["docket_id"] for r in pq.read_table(dkt_p).to_pylist()}
    assert dkt_by_doc == {
        "2025-01001": "EPA-HQ-OAR-2025-0001",
        "2025-01002": "FAA-2025-9999",
        "2025-01004": "FCC-2025-0007",
    }

    # force_rebuild=True ignores prior and replays both blobs cleanly.
    fetched.clear()
    layer1.merge_federal_register_typed(
        out_dir,
        fetch_blob=tracking_fetch,
        download_prior=lambda *_: False,
        force_rebuild=True,
    )
    assert set(fetched) == {sha_keep, sha_new}


def test_streaming_r2_refuses_null_sha_digest_mismatch_missing_key_conflict_and_corrupt_prior(tmp_path):
    """Fail-closed refusals protect outputs against corrupt inputs, tampered blobs, missing keys, and orphan priors."""
    import pyarrow as pa

    from spicy_regs.transforms import federal_register_layer1 as layer1

    rec = document()
    raw, sha = _page_blob([rec])

    # 1. NULL source_sha256 in federal_register.parquet
    null_fr = tmp_path / "null_fr.parquet"
    _write_pinned_fr(null_fr, [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": None}])
    with pytest.raises(ValueError, match="NULL or malformed source_sha256"):
        layer1.build_federal_register_typed_from_r2(tmp_path / "out1", fr_path=null_fr, fetch_blob=lambda _: raw)

    # 2. Malformed source_sha256
    bad_fr = tmp_path / "bad_fr.parquet"
    _write_pinned_fr(bad_fr, [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": "md5:1234"}])
    with pytest.raises(ValueError, match="NULL or malformed source_sha256"):
        layer1.build_federal_register_typed_from_r2(tmp_path / "out2", fr_path=bad_fr, fetch_blob=lambda _: raw)

    # 3. Tampered blob bytes (SHA-256 mismatch)
    good_fr = tmp_path / "good_fr.parquet"
    _write_pinned_fr(good_fr, [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": sha}])
    with pytest.raises(ValueError, match="digest differs"):
        layer1.build_federal_register_typed_from_r2(tmp_path / "out3", fr_path=good_fr, fetch_blob=lambda _: b"{}")

    # 4. Blob missing an expected key pinned by federal_register.parquet
    missing_fr = tmp_path / "missing_fr.parquet"
    _write_pinned_fr(
        missing_fr,
        [
            {"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": sha},
            {"document_number": "2024-99999", "publication_date": "2024-03-01", "source_sha256": sha},
        ],
    )
    with pytest.raises(ValueError, match="missing 1 expected key"):
        layer1.build_federal_register_typed_from_r2(tmp_path / "out4", fr_path=missing_fr, fetch_blob=lambda _: raw)

    # 5. Conflicting same-date observations inside one blob
    conflict_raw, conflict_sha = _page_blob([rec, document(title="Conflicting Same-Date Title")])
    conflict_fr = tmp_path / "conflict_fr.parquet"
    _write_pinned_fr(
        conflict_fr,
        [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": conflict_sha}],
    )
    with pytest.raises(ValueError, match="Conflicting Federal Register observations"):
        layer1.build_federal_register_typed_from_r2(
            tmp_path / "out5", fr_path=conflict_fr, fetch_blob=lambda _: conflict_raw
        )

    # 6. Corrupted prior with orphan agency row refuses fail-closed during incremental merge
    prior_dir = tmp_path / "corrupt_prior"
    layer1.build_federal_register_typed_from_r2(prior_dir, fr_path=good_fr, fetch_blob=lambda _: raw)
    ag_path = prior_dir / "federal_register_agencies.parquet"
    ag_rows = pq.read_table(ag_path).to_pylist()
    orphan = dict(ag_rows[0], document_number="2024-ORPHAN", source_record_id="2024-ORPHAN@2024-03-01")
    pq.write_table(
        pa.Table.from_pylist([*ag_rows, orphan], schema=layer1.agencies_schema()),
        ag_path,
    )
    with pytest.raises(ValueError, match="orphan row"):
        layer1.merge_federal_register_typed(
            prior_dir, fr_path=good_fr, fetch_blob=lambda _: raw, download_prior=lambda *_: False
        )


def test_http_and_s3_blob_fetch_enforce_user_agent_retry_and_sha_verification():
    """HTTP transport sends explicit User-Agent and retries 503; S3 transport checks SHA-256 and byte bounds."""
    import io

    import httpx

    from spicy_regs.sources.publication import PUBLIC_HEADERS
    from spicy_regs.transforms import federal_register_layer1 as layer1

    raw, sha = _page_blob([document()])
    attempts: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append({"url": str(request.url), "ua": request.headers.get("user-agent", "")})
        if len(attempts) == 1:
            return httpx.Response(503, content=b"try again")
        return httpx.Response(200, content=raw)

    with httpx.Client(headers=PUBLIC_HEADERS, transport=httpx.MockTransport(handler)) as client:
        got = layer1._fetch_r2_blob_http(
            client,
            "https://data.spicygov.ai",
            sha,
            max_bytes=1024 * 1024,
            sleep=lambda _: None,
        )
    assert got == raw
    assert len(attempts) == 2
    assert attempts[0]["ua"] == PUBLIC_HEADERS["User-Agent"]
    assert attempts[0]["url"].endswith(f"/source-evidence/blobs/sha256/{sha.removeprefix('sha256:')}")

    class _FakeS3:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        def get_object(self, *, Bucket: str, Key: str) -> dict:
            assert Bucket == "bucket"
            assert Key == f"source-evidence/blobs/sha256/{sha.removeprefix('sha256:')}"
            return {"Body": io.BytesIO(self.payload)}

    assert layer1._fetch_r2_blob_s3(_FakeS3(raw), "bucket", sha, max_bytes=1024 * 1024) == raw
    with pytest.raises(ValueError, match="digest mismatch"):
        layer1._fetch_r2_blob_s3(_FakeS3(b"corrupted"), "bucket", sha, max_bytes=1024 * 1024)
    with pytest.raises(ValueError, match="exceeds max byte bound"):
        layer1._fetch_r2_blob_s3(_FakeS3(raw), "bucket", sha, max_bytes=10)


def test_federal_register_typed_rollup_and_workflows_wiring(tmp_path, monkeypatch):
    """Receipts-free rollup wiring, console entry, workflow jobs, and local hermetic execution."""
    from pathlib import Path
    import tomllib

    import yaml

    from spicy_regs.pipelines.rollups import base as rollup_base
    from spicy_regs.pipelines.rollups.federal_register_typed import FederalRegisterTypedRollup
    from spicy_regs.transforms import federal_register_layer1 as layer1

    repo_root = Path(__file__).resolve().parents[1]
    assert FederalRegisterTypedRollup.name == "federal-register-typed"
    assert FederalRegisterTypedRollup.inputs == ("federal_register.parquet",)
    assert FederalRegisterTypedRollup.outputs == layer1.OUTPUTS
    assert FederalRegisterTypedRollup.receipt_policies == ()
    assert FederalRegisterTypedRollup.retain_source_evidence is False
    assert "FR_TYPED_FORCE_REBUILD" in rollup_base.ROLLUP_SCOPE_ENV_VARS

    scripts = tomllib.loads((repo_root / "pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["run-rollup-federal-register-typed"] == "spicy_regs.pipelines.rollups.federal_register_typed:app"

    fr_workflow = yaml.safe_load((repo_root / ".github/workflows/rollup-federal-register.yml").read_text())
    typed_job = fr_workflow["jobs"]["typed-layer1"]
    assert typed_job["needs"] == "run"
    assert typed_job["if"] == "${{ !inputs.skip_upload }}"
    assert typed_job["uses"] == "./.github/workflows/_rollup.yml"
    assert typed_job["with"]["command"] == "run-rollup-federal-register-typed"
    assert typed_job["with"]["producer_ref"] == "main"

    manual_workflow = yaml.safe_load((repo_root / ".github/workflows/rollup-federal-register-typed.yml").read_text())
    triggers = manual_workflow.get("on", manual_workflow.get(True))
    assert set(triggers) == {"workflow_dispatch"}
    dispatch_inputs = triggers["workflow_dispatch"]["inputs"]
    assert dispatch_inputs["force_rebuild"]["type"] == "boolean"
    assert dispatch_inputs["force_rebuild"]["default"] is False
    assert manual_workflow["permissions"] == {"contents": "read", "actions": "read"}
    assert manual_workflow["jobs"]["run"]["with"]["command"] == "run-rollup-federal-register-typed"
    assert "inputs.force_rebuild" in manual_workflow["jobs"]["run"]["with"]["fr_typed_force_rebuild"]

    reusable_workflow = yaml.safe_load((repo_root / ".github/workflows/_rollup.yml").read_text())
    reusable_inputs = reusable_workflow.get("on", reusable_workflow.get(True))["workflow_call"]["inputs"]
    assert reusable_inputs["fr_typed_force_rebuild"]["type"] == "boolean"
    assert reusable_inputs["fr_typed_force_rebuild"]["default"] is False
    run_step = next(
        step for step in reusable_workflow["jobs"]["rollup"]["steps"] if step.get("name") == "Run rollup"
    )
    assert run_step["env"]["FR_TYPED_FORCE_REBUILD"] == "${{ inputs.fr_typed_force_rebuild }}"

    from spicy_regs.env_values import flag_env

    for raw_val, expected in (("", False), ("false", False), ("true", True)):
        monkeypatch.setenv("FR_TYPED_FORCE_REBUILD", raw_val)
        assert flag_env("FR_TYPED_FORCE_REBUILD") is expected
    monkeypatch.delenv("FR_TYPED_FORCE_REBUILD", raising=False)

    # Hermetic build execution through FederalRegisterTypedRollup.build
    raw, sha = _page_blob([document()])
    out_dir = tmp_path / "rollup_out"
    out_dir.mkdir()
    _write_pinned_fr(
        out_dir / "federal_register.parquet",
        [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": sha}],
    )
    orig_merge = layer1.merge_federal_register_typed
    monkeypatch.setattr(
        "spicy_regs.pipelines.rollups.federal_register_typed.merge_federal_register_typed",
        lambda od, **kw: orig_merge(od, fetch_blob=lambda _: raw, download_prior=lambda *_: False, **kw),
    )
    paths = FederalRegisterTypedRollup(output_dir=out_dir, skip_upload=True).build(out_dir)
    assert tuple(p.name for p in paths) == layer1.OUTPUTS
    assert all(p.is_file() for p in paths)


def test_odcs_ddk_relationships_finding5_and_legacy_federal_register_unchanged(tmp_path):
    """ODCS v3.2.0 schema parity, all 3 DATE==DATE DDK relationships, Finding 5 doc fixes, and legacy FR untouched."""
    from pathlib import Path

    from spicy_regs import ddk_bridge, mcp_server, odcs_contracts, table_joins
    from spicy_regs.transforms import federal_register_layer1 as layer1

    repo_root = Path(__file__).resolve().parents[1]

    # 1. Legacy federal_register ODCS Arrow schema hash is 100% untouched.
    fr_odcs = json.loads((repo_root / "data_dictionary/odcs/families/federal-register.odcs.json").read_text())
    fr_schema_props = {cp["property"]: cp["value"] for cp in fr_odcs["schema"][0]["customProperties"]}
    assert fr_schema_props["arrowSchemaSha256"] == "6868ddc71c7d7e1df8de73ae0bc7ea7352973c87522ee6f27dc8916b8ce7ca8f"

    # 2. All 4 typed tables registered in MCP, explorer_sources.json, table_joins, and ODCS FAMILY_TABLES.
    assert set(layer1.TABLES) <= set(mcp_server.TABLES)
    assert set(odcs_contracts.FAMILY_TABLES["federal-register-typed"]) == set(layer1.TABLES)
    explorer = json.loads((repo_root / "src/spicy_regs/explorer_sources.json").read_text())
    assert "federal-register-typed" in explorer["families"]
    incoming = table_joins.joins_for("federal_register_documents")["incoming"]
    assert {j["child"] for j in incoming} == {
        "federal_register_agencies",
        "federal_register_cfr_references",
        "federal_register_docket_candidates",
    }

    # 3. Export federal-register-typed ODCS contract, verify parity on built Parquets, and verify 3 DDK relationships.
    odcs_dir = tmp_path / "odcs"
    odcs_contracts.export_odcs_contracts(odcs_dir, families=["federal-register-typed"])
    contract_path = odcs_dir / "families" / "federal-register-typed.odcs.json"
    contract = json.loads(contract_path.read_text())
    raw, sha = _page_blob([document()])
    fr_path = tmp_path / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": sha}])
    built_paths = layer1.build_federal_register_typed_from_r2(
        tmp_path / "typed", fr_path=fr_path, fetch_blob=lambda _: raw
    )
    declared = {s["name"]: s for s in contract["schema"]}
    candidates = {p.stem: p for p in built_paths}
    parity = odcs_contracts.verify_parquet_schema_parity(declared, candidates)
    assert set(parity) == set(layer1.TABLES)

    schemas = layer1.layer1_schemas()
    collected = [
        ddk_bridge.CollectedTable(
            family="federal-register-typed",
            table_name=s["name"],
            domain="spicygov",
            contract=contract,
            contract_path=contract_path,
            contract_sha256="sha256:" + "0" * 64,
            schema_obj=s,
            arrow_schema=schemas[s["name"]],
            partition_keys=(),
            members=(),
            generation_digest="sha256:" + "0" * 64,
        )
        for s in contract["schema"]
    ]
    rels = ddk_bridge._extract_ddk_relationships(collected)
    assert len(rels) == 3
    assert {r["id"] for r in rels} == {
        "urn:spicy-regs:rel:federal_register_agencies:document_number.publication_date:to:federal_register_documents:document_number.publication_date",
        "urn:spicy-regs:rel:federal_register_cfr_references:document_number.publication_date:to:federal_register_documents:document_number.publication_date",
        "urn:spicy-regs:rel:federal_register_docket_candidates:document_number.publication_date:to:federal_register_documents:document_number.publication_date",
    }
    assert all(r["cardinality"] == "many_to_one" for r in rels)

    # 4. Finding 5 doc fixes hold cleanly.
    fr_doc = (repo_root / "docs/tables/federal_register.md").read_text()
    assert "`citation_url` is omitted on `federal_register`" in fr_doc
    lifecycles_doc = (repo_root / "docs/tables/rulemaking_lifecycles.md").read_text()
    assert "current `federal_register` and `federal_register_documents` carry `action`" in lifecycles_doc
    agencies_doc = (repo_root / "docs/tables/federal_register_agencies.md").read_text()
    assert "federal_register_agencies_occurrences" in agencies_doc
    assert "federal_register_agencies_pairs" in agencies_doc


def test_prior_null_sha_is_refused_reconcile_guards_null_and_force_rebuild_repairs(tmp_path):
    """BLOCKER 1 regression: prior source_sha256=NULL is refused (never silently published), reconcile uses IS DISTINCT FROM."""
    import pyarrow as pa

    from spicy_regs.transforms import federal_register_layer1 as layer1

    rec = document()
    raw, sha = _page_blob([rec])
    out_dir = tmp_path / "run"
    out_dir.mkdir()
    fr_path = out_dir / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": "2024-00001", "publication_date": "2024-03-01", "source_sha256": sha}])
    layer1.build_federal_register_typed_from_r2(out_dir, fr_path=fr_path, fetch_blob=lambda _: raw)

    # Corrupt prior documents table by setting source_sha256 = None
    docs_path = out_dir / "federal_register_documents.parquet"
    rows = pq.read_table(docs_path).to_pylist()
    rows[0]["source_sha256"] = None
    pq.write_table(
        pa.Table.from_pylist(rows, schema=layer1.document_schema().with_metadata({"layer1.version": layer1.VERSION})),
        docs_path,
    )

    # 1. Incremental merge refuses corrupted prior fail-closed (never carries forward NULL source_sha256)
    with pytest.raises(ValueError, match="NULL or invalid"):
        layer1.merge_federal_register_typed(
            out_dir, fr_path=fr_path, fetch_blob=lambda _: raw, download_prior=lambda *_: False
        )

    # 2. Defense-in-depth: _reconcile_merged_tables uses IS DISTINCT FROM so NULL source_sha256 never passes SQL <>
    with duckdb.connect() as con:
        layer1._validate_and_load_target_keys(con, fr_path)
        merged_paths = {name: out_dir / f"{name}.parquet" for name in layer1.TABLES}
        with pytest.raises(ValueError, match="source_sha256 differs"):
            layer1._reconcile_merged_tables(con, merged_paths, 1)

    # 3. force_rebuild=True repairs the corrupted prior cleanly
    docs_out, *_ = layer1.merge_federal_register_typed(
        out_dir,
        fr_path=fr_path,
        fetch_blob=lambda _: raw,
        download_prior=lambda *_: False,
        force_rebuild=True,
    )
    repaired = pq.read_table(docs_out).to_pylist()
    assert len(repaired) == 1 and repaired[0]["source_sha256"] == sha


def test_incremental_update_drops_children_to_zero_without_stale_rows(tmp_path):
    """WARNING 5 regression: when an updated document drops agencies/CFR/dockets to empty, all stale child rows vanish."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    v1 = document(
        document_number="2025-30001",
        publication_date="2025-05-01",
        cfr_references=[{"title": 40, "part": 60, "chapter": "I", "citation_url": None}],
        docket_ids=["EPA-HQ-OAR-2025-0001"],
    )
    raw1, sha1 = _page_blob([v1])
    out_dir = tmp_path / "run"
    out_dir.mkdir()
    fr_path = out_dir / "federal_register.parquet"
    _write_pinned_fr(fr_path, [{"document_number": "2025-30001", "publication_date": "2025-05-01", "source_sha256": sha1}])
    docs_p, ag_p, cfr_p, dkt_p = layer1.merge_federal_register_typed(
        out_dir, fetch_blob=lambda _: raw1, download_prior=lambda *_: False
    )
    assert pq.read_table(ag_p).num_rows == 2
    assert pq.read_table(cfr_p).num_rows == 1
    assert pq.read_table(dkt_p).num_rows == 1

    # Updated version drops all three child arrays to empty [].
    v2 = document(
        document_number="2025-30001",
        publication_date="2025-05-01",
        title="Childless Update",
        agencies=[],
        cfr_references=[],
        docket_ids=[],
    )
    raw2, sha2 = _page_blob([v2])
    _write_pinned_fr(fr_path, [{"document_number": "2025-30001", "publication_date": "2025-05-01", "source_sha256": sha2}])
    docs_p, ag_p, cfr_p, dkt_p = layer1.merge_federal_register_typed(
        out_dir, fetch_blob=lambda d: {sha2: raw2}[d], download_prior=lambda *_: False
    )
    assert pq.read_table(docs_p).to_pylist()[0]["title"] == "Childless Update"
    assert pq.read_table(ag_p).num_rows == 0
    assert pq.read_table(cfr_p).num_rows == 0
    assert pq.read_table(dkt_p).num_rows == 0


def test_incremental_delete_only_merge_fetches_zero_blobs_and_cleans_children(tmp_path):
    """WARNING 5 regression: delete-only incremental merge performs zero blob fetches and removes parent + child rows."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    doc_keep = document(
        document_number="2025-40001",
        publication_date="2025-06-01",
        cfr_references=[{"title": 40, "part": 60, "chapter": "I", "citation_url": None}],
        docket_ids=["EPA-HQ-OAR-2025-0010"],
    )
    doc_drop = document(
        document_number="2025-40002",
        publication_date="2025-06-02",
        cfr_references=[{"title": 14, "part": 39, "chapter": "I", "citation_url": None}],
        docket_ids=["FAA-2025-0020"],
    )
    raw, sha = _page_blob([doc_keep, doc_drop])
    out_dir = tmp_path / "run"
    out_dir.mkdir()
    fr_path = out_dir / "federal_register.parquet"
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-40001", "publication_date": "2025-06-01", "source_sha256": sha},
            {"document_number": "2025-40002", "publication_date": "2025-06-02", "source_sha256": sha},
        ],
    )
    layer1.merge_federal_register_typed(out_dir, fetch_blob=lambda _: raw, download_prior=lambda *_: False)

    # Remove doc_drop while leaving doc_keep pinned to the exact same sha.
    _write_pinned_fr(fr_path, [{"document_number": "2025-40001", "publication_date": "2025-06-01", "source_sha256": sha}])
    fetched: list[str] = []

    def fail_if_called(digest: str) -> bytes:
        fetched.append(digest)
        raise AssertionError(f"Delete-only merge should not fetch blob {digest}")

    docs_p, ag_p, cfr_p, dkt_p = layer1.merge_federal_register_typed(
        out_dir, fetch_blob=fail_if_called, download_prior=lambda *_: False
    )
    assert fetched == []
    assert [r["document_number"] for r in pq.read_table(docs_p).to_pylist()] == ["2025-40001"]
    assert {r["document_number"] for r in pq.read_table(ag_p).to_pylist()} == {"2025-40001"}
    assert {r["document_number"] for r in pq.read_table(cfr_p).to_pylist()} == {"2025-40001"}
    assert {r["document_number"] for r in pq.read_table(dkt_p).to_pylist()} == {"2025-40001"}


def test_incremental_shared_sha_addition_merges_new_sibling_without_duplicating_existing(tmp_path):
    """WARNING 5 regression: adding a new key sharing an existing prior source_sha256 fetches and merges cleanly."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    doc_existing = document(
        document_number="2025-50001",
        publication_date="2025-07-01",
        title="Existing Sibling",
        cfr_references=[{"title": 40, "part": 60, "chapter": "I", "citation_url": None}],
        docket_ids=["EPA-HQ-OAR-2025-0101"],
    )
    doc_added = document(
        document_number="2025-50002",
        publication_date="2025-07-01",
        title="Newly Pinned Sibling In Same Blob",
        cfr_references=[{"title": 21, "part": 101, "chapter": "I", "citation_url": None}],
        docket_ids=["FDA-2025-0202"],
    )
    raw, sha = _page_blob([doc_existing, doc_added])
    out_dir = tmp_path / "run"
    out_dir.mkdir()
    fr_path = out_dir / "federal_register.parquet"

    # Initial pin includes only doc_existing from shared blob `sha`.
    _write_pinned_fr(fr_path, [{"document_number": "2025-50001", "publication_date": "2025-07-01", "source_sha256": sha}])
    layer1.merge_federal_register_typed(out_dir, fetch_blob=lambda _: raw, download_prior=lambda *_: False)

    # Next pin adds doc_added pointing to the SAME `sha` alongside doc_existing.
    _write_pinned_fr(
        fr_path,
        [
            {"document_number": "2025-50001", "publication_date": "2025-07-01", "source_sha256": sha},
            {"document_number": "2025-50002", "publication_date": "2025-07-01", "source_sha256": sha},
        ],
    )
    fetched: list[str] = []

    def tracking_fetch(digest: str) -> bytes:
        fetched.append(digest)
        return raw

    docs_p, ag_p, cfr_p, dkt_p = layer1.merge_federal_register_typed(
        out_dir, fetch_blob=tracking_fetch, download_prior=lambda *_: False
    )
    assert fetched == [sha]
    docs = {r["document_number"]: r["title"] for r in pq.read_table(docs_p).to_pylist()}
    assert docs == {
        "2025-50001": "Existing Sibling",
        "2025-50002": "Newly Pinned Sibling In Same Blob",
    }
    assert pq.read_table(ag_p).num_rows == 4
    assert {r["document_number"]: r["title"] for r in pq.read_table(cfr_p).to_pylist()} == {
        "2025-50001": 40,
        "2025-50002": 21,
    }
    assert {r["document_number"]: r["docket_id"] for r in pq.read_table(dkt_p).to_pylist()} == {
        "2025-50001": "EPA-HQ-OAR-2025-0101",
        "2025-50002": "FDA-2025-0202",
    }


@pytest.mark.parametrize("bad_rins", ["2060-AA01", [12345], ["2060-AA01", None]])
def test_malformed_regulation_id_numbers_refused_with_contextual_error(bad_rins):
    """WARNING 4 regression: bare string or non-string items in regulation_id_numbers refuse instead of splitting chars."""
    from spicy_regs.transforms import federal_register_layer1 as layer1

    rec = document(regulation_id_numbers=bad_rins)
    raw, sha = _page_blob([rec])
    with pytest.raises(ValueError, match="regulation_id_numbers"):
        list(layer1.iter_typed_page_records(raw, sha))
