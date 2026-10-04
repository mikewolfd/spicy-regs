"""New source readers preserve native coordinates and NULLs through existing receiving tables."""

import hashlib
import io
import json
from zipfile import ZipFile

import pytest

from spicy_regs.transforms.build_fec_observations import _shape, build_fec_observations
from tests.test_fec_agency_observations import FIXTURES
from tests.test_fec_observations import _manifest, _rows

WORD = b"""<pkg:package xmlns:pkg="http://schemas.microsoft.com/office/2006/xmlPackage" xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><pkg:part pkg:name="/word/document.xml" pkg:contentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"><pkg:xmlData><w:document><w:body><w:p><w:r><w:t xml:space="preserve">  literal 01 </w:t><w:tab/><w:delText>deleted</w:delText></w:r></w:p></w:body></w:document></pkg:xmlData></pkg:part></pkg:package>"""


@pytest.mark.parametrize("format", ["word-flat-opc", "foia-zip-member"])
def test_agency_document_preserves_members_and_word_token_roles(tmp_path, format):
    from spicy_docs.sources.fec.agency_document_profile import agency_document_scope
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    raw, member = WORD, None
    if format == "foia-zip-member":
        buffer = io.BytesIO()
        with ZipFile(buffer, "w") as archive:
            archive.writestr("other.xml", b"not selected")
            archive.writestr("fec-2010.xml", (FIXTURES / "foia-fec-2010.xml").read_bytes())
        raw = buffer.getvalue()
        member = {"ordinal": 1, "name": "fec-2010.xml"}
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")
    store.put_blob(digest, len(raw), [raw])
    capture = {
        "requestUrl": "https://www.foia.gov/2010-FOIASetFull.zip" if member else "https://www.fec.gov/report.xml",
        "responseSha256": digest,
        "byteSize": len(raw),
        "observedAt": "2026-09-30T00:00:00Z",
        "representation": "zip" if member else "opaque",
    }
    item = {
        "collection_id": "document",
        "source_family": "fec_agency_reports",
        "profile": "agency-document",
        "blob_root": "blobs",
        "scope": agency_document_scope(capture, format=format, member=member),
    }
    records, collections, relationships = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert collections[0]["record_count"] == len(records) and relationships == []
    for ordinal, record in enumerate(records):
        loc = json.loads(record["source_locator_json"])
        assert (loc["ordinal"], loc.get("member"), loc["format"]) == (ordinal, member, format)
        native = json.loads(record["source_record_json"])["record"]["record"]
        assert json.loads(record["metadata_json"]) == native
    if member:
        assert all("/member/000001/" in row["source_record_id"] for row in records)
    else:
        bodies = [body for row in records for body in json.loads(row["embedded_bodies_json"])]
        assert [body["text"] for body in bodies] == ["  literal 01 ", None, "deleted"]


def test_postgres_maps_exact_verified_column_names_without_current_relationship_semantics():
    capture = {
        "requestUrl": "https://www.fec.gov/history.dump",
        "responseSha256": "sha256:" + "a" * 64,
        "observedAt": "2026-09-12T00:00:00Z",
    }
    native = {
        "kind": "postgres-copy",
        "fields": ["C00000001", r"\N", "", "{}", r"a\tb"],
        "values": ["C00000001", None, "", "{}", "a\tb"],
        "source": {
            "sha256": "sha256:" + "b" * 64,
            "original_sha256": capture["responseSha256"],
            "byte_offset": 11,
            "byte_length": 41,
            "table": "disclosure.ofec_committee_history",
        },
    }
    wrapped = {"sourceRecordId": "native-row", "record": {"ordinal": 0, "member": None, "record": native}}
    scope = {
        "capture": capture,
        "derivation": {
            "columns": [{"name": name} for name in ["committee_id", "missing", "empty", "array", "escaped"]],
            "outputs": {"schema": {"sha256": "sha256:" + "c" * 64}},
        },
    }
    row, locator = _shape(
        {"collection_id": "history", "source_family": "fec_committees", "profile": "postgres"},
        wrapped,
        {"requestedScope": scope},
    )
    metadata = json.loads(row["metadata_json"])
    assert metadata["named_fields"] == {
        "committee_id": "C00000001",
        "missing": None,
        "empty": "",
        "array": "{}",
        "escaped": "a\tb",
    }
    assert {k: v for k, v in metadata.items() if k != "named_fields"} == native
    assert row["committee_id"] == "C00000001"
    assert json.loads(row["source_record_json"]) == wrapped
    assert locator["original_sha256"] == capture["responseSha256"] and locator["sha256"] == native["source"]["sha256"]
    assert locator["column_schema_sha256"] == scope["derivation"]["outputs"]["schema"]["sha256"]


def test_partial_api_document_keeps_native_fields_bodies_assets_and_source_identity(tmp_path):
    from spicy_docs.sources.fec.document_profile import document_scope
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    value = {
        "results": [
            {
                "committee_id": "C00000001",
                "text": " Literal body ",
                "document_url": "https://www.fec.gov/literal.pdf",
                "zero": 0,
                "missing": None,
            }
        ],
        "pagination": {"page": 1, "pages": 20, "count": 20, "per_page": 1},
    }
    raw = json.dumps(value).encode()
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")
    store.put_blob(digest, len(raw), [raw])
    item = {
        "collection_id": "partial",
        "source_family": "fec_committees",
        "profile": "document",
        "blob_root": "blobs",
        "scope": document_scope(
            {
                "requestUrl": "https://api.open.fec.gov/v1/committee/?page=1",
                "responseSha256": digest,
                "byteSize": len(raw),
                "observedAt": "2026-09-12T00:00:00Z",
                "representation": "opaque",
            },
            format="api-json",
            api_mode="page",
        ),
    }
    records, collections, relationships = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert collections[0]["record_count"] == len(records) and relationships == []
    native = [json.loads(r["metadata_json"]) for r in records]
    assert {r["field"]: r["value"] for r in native if r["kind"] == "api-response-field"} == {
        "pagination": value["pagination"]
    }
    assert all(r["query_completeness"] == "not-asserted" for r in native)
    selected = next(r for r in records if json.loads(r["metadata_json"])["kind"] == "api-record-observation")
    assert selected["committee_id"] == "C00000001"
    assert json.loads(selected["embedded_bodies_json"])[0]["text"] == " Literal body "
    assert json.loads(selected["assets_json"])[0]["url"] == "https://www.fec.gov/literal.pdf"
    assert json.loads(selected["source_locator_json"])["pointer"] == "/results/0"


@pytest.mark.parametrize("native", [None, "literal", ["one", None], 23])
def test_document_nonobject_metadata_is_preserved_without_identity_inference(native):
    capture = {
        "requestUrl": "https://api.open.fec.gov/v1/example/",
        "responseSha256": "sha256:" + "a" * 64,
        "observedAt": "2026-09-12T00:00:00Z",
    }
    record = {
        "kind": "api-record-observation",
        "metadata": native,
        "source": {"sha256": capture["responseSha256"], "pointer": "/results/0"},
        "query_completeness": "not-asserted",
    }
    wrapped = {"sourceRecordId": "row", "record": {"ordinal": 0, "member": None, "record": record}}
    row, _ = _shape(
        {"collection_id": "capture", "source_family": "fec_committees", "profile": "document"},
        wrapped,
        {"requestedScope": {"capture": capture}},
    )
    assert json.loads(row["metadata_json"])["metadata"] == native
    assert all(
        row[key] is None for key in ["committee_id", "candidate_id", "filing_id", "legal_doc_id", "audit_case_id"]
    )


def test_existing_query_assets_remain_reader_owned_when_metadata_has_assets():
    capture = {
        "requestUrl": "https://api.open.fec.gov/v1/committees/",
        "responseSha256": "sha256:" + "a" * 64,
        "observedAt": "2026-09-12T00:00:00Z",
    }
    native = {
        "capture": capture,
        "source_pointer": "/results/0",
        "metadata": {"committee_id": "C00000001", "assets": "native metadata value"},
        "assets": [{"url": "https://www.fec.gov/retained.pdf"}],
    }
    row, _ = _shape(
        {"collection_id": "existing", "source_family": "fec_committees", "profile": "committee"},
        {"sourceRecordId": "row", "record": native},
        {"requestedScope": {}},
    )
    assert json.loads(row["assets_json"]) == native["assets"]
    assert json.loads(row["metadata_json"])["assets"] == "native metadata value"
