"""Tests for the Mirrulations derived-data comment-text enrichment.

Covers the fetcher (:class:`DerivedCommentText`) over spicy-docs' reader, the
streaming transform (:class:`EnrichCommentText`), and the :class:`Chain`
combinator, against an in-memory S3 resource shaped like spicy-docs' own fake
(listed ETag and size, ``IfMatch`` honoured, byte-order listing).
"""

from __future__ import annotations

import hashlib
import json

import pytest
from botocore.exceptions import ClientError
from spicy_docs.sources.mirrulations import (
    DEFAULT_MAX_OBJECT_BYTES,
    MirrulationsAccessRefusedError,
    comments_extracted_prefix,
)

from spicy_regs.sources.derived_text import (
    DERIVED_STATUS,
    MAX_ATTACHMENT_BYTES,
    DerivedCommentText,
    DerivedTextUnavailable,
)
from spicy_regs.transforms import Chain, EnrichCommentText, ExtractRecords
from spicy_regs.transforms.base import Transform

BUCKET = "mirrulations"


# --- fake S3 (the boto3 resource surface spicy-docs' reader uses) -------------


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self, size: int | None = None) -> bytes:
        return self._data if size is None else self._data[:size]

    def close(self) -> None:
        pass


class _FakeObj:
    def __init__(self, key: str, content: bytes) -> None:
        self.key = key
        self._content = content
        self.e_tag = f'"etag:{key}"'
        self.size = len(content)

    def get(self, **kwargs: str) -> dict:
        if "IfMatch" in kwargs and kwargs["IfMatch"] != self.e_tag:
            raise ValueError("precondition failed")
        return {"Body": _FakeBody(self._content), "ContentLength": len(self._content), "ETag": self.e_tag}


class _FakeObjects:
    def __init__(self, resource: _FakeS3Resource) -> None:
        self._resource = resource

    def filter(self, Prefix: str):  # noqa: N803 — mirrors boto3 kwarg
        self._resource.listed.append(Prefix)
        if self._resource.listing_status is not None:
            status = self._resource.listing_status
            raise ClientError(
                {"Error": {"Code": str(status)}, "ResponseMetadata": {"HTTPStatusCode": status}}, "ListObjects"
            )
        # S3 lists in key byte order, which is what puts attachment_10 before attachment_2.
        for key in sorted(self._resource.store):
            if key.startswith(Prefix):
                yield _FakeObj(key, self._resource.store[key])


class _FakeBucket:
    def __init__(self, resource: _FakeS3Resource) -> None:
        self.objects = _FakeObjects(resource)


class _FakeS3Resource:
    def __init__(self, store: dict[str, bytes], *, listing_status: int | None = None) -> None:
        self.store = store
        self.listing_status = listing_status
        self.listed: list[str] = []
        self.gets: list[str] = []

    def Bucket(self, name: str) -> _FakeBucket:  # noqa: N802 — mirrors boto3 API
        return _FakeBucket(self)

    def Object(self, name: str, key: str) -> _FakeObj:  # noqa: N802 — mirrors boto3 API
        self.gets.append(key)
        return _FakeObj(key, self.store[key])


def derived_key(agency: str, docket: str, tool: str, comment: str, attachment: int | str) -> str:
    return f"{comments_extracted_prefix(agency, docket)}{tool}/{docket}-{comment}_attachment_{attachment}_extracted.txt"


ACF = ("ACF", "ACF-2025-0038")


def _store() -> dict[str, bytes]:
    # ACF: 0004 one attachment; 0015 two (pypdf); 0020 blank; 0030 attachments 1, 2 and 10;
    # 0040 both tools (pypdf wins), pdfminer alone has attachment 3.
    # EPA: 0009 extracted by pdfminer only.
    acf = {
        ("pypdf", "0004", 1): b"Wisconsin DCF comment body\n",
        ("pypdf", "0015", 1): b"first attachment",
        ("pypdf", "0015", 2): b"second attachment",
        ("pypdf", "0020", 1): b"   \n  ",
        ("pypdf", "0030", 1): b"part 1",
        ("pypdf", "0030", 2): b"part 2",
        ("pypdf", "0030", 10): b"part 10",
        ("pypdf", "0040", 1): b"pypdf one",
        ("pdfminer", "0040", 1): b"pdfminer one\x0c",
        ("pdfminer", "0040", 3): b"pdfminer three",
    }
    store = {derived_key(*ACF, tool, comment, n): body for (tool, comment, n), body in acf.items()}
    store[derived_key("EPA", "EPA-HQ-OA-2024-0001", "pdfminer", "0009", 1)] = b"EPA comment text"
    return store


def _fill(resource: _FakeS3Resource, comment: str, **kwargs):
    return DerivedCommentText(resource, BUCKET, **kwargs).fill_for(*ACF, f"ACF-2025-0038-{comment}")


# --- DerivedCommentText ------------------------------------------------------


def test_fill_single_attachment_is_stripped_and_marked_derived() -> None:
    fill = _fill(_FakeS3Resource(_store()), "0004")
    assert fill is not None
    assert fill.text == "Wisconsin DCF comment body"
    assert DERIVED_STATUS == "derived"


def test_fill_orders_attachments_by_number_not_listing_order() -> None:
    resource = _FakeS3Resource(_store())
    fill = _fill(resource, "0030")
    assert fill is not None
    assert fill.text == "part 1\n\npart 2\n\npart 10"
    assert [a["attachment"] for a in json.loads(fill.provenance)["attachments"]] == [1, 2, 10]


def test_fill_takes_one_tool_by_the_pinned_order_and_records_provenance() -> None:
    store = _store()
    fill = _fill(_FakeS3Resource(store), "0040")
    assert fill is not None
    assert fill.text == "pypdf one"  # never concatenated with pdfminer's text for the same attachment
    key = derived_key(*ACF, "pypdf", "0040", 1)
    assert json.loads(fill.provenance) == {
        "comment_id": "ACF-2025-0038-0040",
        "tool": "pypdf",
        "available_tools": ["pypdf", "pdfminer"],
        "attachments": [
            {
                "attachment": 1,
                "tool": "pypdf",
                "key": key,
                "size": len(store[key]),
                "etag": f'"etag:{key}"',
                "sha256": hashlib.sha256(store[key]).hexdigest(),
            }
        ],
        "only_in_other_tools": [3],
    }


def test_fill_discovers_a_tool_it_does_not_prefer() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    fill = fetcher.fill_for("EPA", "EPA-HQ-OA-2024-0001", "EPA-HQ-OA-2024-0001-0009")
    assert fill is not None
    assert fill.text == "EPA comment text"
    assert json.loads(fill.provenance)["tool"] == "pdfminer"


def test_blank_missing_or_unidentified_comments_have_no_fill() -> None:
    resource = _FakeS3Resource(_store())
    fetcher = DerivedCommentText(resource, BUCKET)
    assert fetcher.fill_for(*ACF, "ACF-2025-0038-0020") is None  # every part blank
    assert fetcher.fill_for(*ACF, "ACF-2025-0038-9999") is None  # not in the listing
    assert fetcher.fill_for(None, "ACF-2025-0038", "ACF-2025-0038-0004") is None
    assert fetcher.fill_for("ACF", None, "ACF-2025-0038-0004") is None
    assert fetcher.fill_for("ACF", "ACF-2025-0038", None) is None


def test_docket_is_listed_once_per_fetcher() -> None:
    resource = _FakeS3Resource(_store())
    fetcher = DerivedCommentText(resource, BUCKET)
    fetcher.fill_for(*ACF, "ACF-2025-0038-0004")
    fetcher.fill_for(*ACF, "ACF-2025-0038-0015")
    assert resource.listed == [comments_extracted_prefix(*ACF)]


def test_strict_listing_refusal_fails_the_docket_never_no_text() -> None:
    """A key outside the layout may hide text, so every comment of the docket is unavailable, not blank."""
    store = _store() | {comments_extracted_prefix(*ACF) + "pypdf/README.txt": b"moved"}
    resource = _FakeS3Resource(store)
    fetcher = DerivedCommentText(resource, BUCKET)
    for comment in ("0004", "9999"):
        with pytest.raises(DerivedTextUnavailable, match="outside the layout"):
            fetcher.fill_for(*ACF, f"ACF-2025-0038-{comment}")
    assert resource.listed == [comments_extracted_prefix(*ACF)]  # the refusal is cached, not re-listed
    assert resource.gets == []


def test_a_failed_attachment_fails_the_comment_rather_than_dropping_a_part() -> None:
    class _ChangedAfterListing(_FakeS3Resource):
        def Object(self, name: str, key: str) -> _FakeObj:  # noqa: N802
            obj = super().Object(name, key)
            if key.endswith("_attachment_2_extracted.txt"):
                obj.e_tag = '"replaced"'
            return obj

    with pytest.raises(DerivedTextUnavailable, match="ACF-2025-0038-0015"):
        _fill(_ChangedAfterListing(_store()), "0015")


def test_the_cap_is_passed_explicitly_and_admits_objects_over_the_reader_default() -> None:
    large = b"x" * (DEFAULT_MAX_OBJECT_BYTES + 1)
    store = {derived_key(*ACF, "pdfminer", "0050", 1): large}
    assert MAX_ATTACHMENT_BYTES > len(large)
    fill = _fill(_FakeS3Resource(store), "0050")
    assert fill is not None and len(fill.text) == len(large)
    with pytest.raises(DerivedTextUnavailable, match="byte cap"):
        _fill(_FakeS3Resource(store), "0050", max_bytes=DEFAULT_MAX_OBJECT_BYTES)


def test_access_refusal_ends_the_run() -> None:
    with pytest.raises(MirrulationsAccessRefusedError):
        _fill(_FakeS3Resource(_store(), listing_status=403), "0004")


# --- EnrichCommentText transform --------------------------------------------


def _comment_record(comment_id: str, *, attachments: bool, text: str | None = None) -> dict:
    return {
        "comment_id": comment_id,
        "docket_id": "ACF-2025-0038",
        "agency_code": "ACF",
        "comment": "See attached file(s)",
        "attachments_json": json.dumps([{"title": "x"}]) if attachments else None,
        "text_content": text,
        "text_extraction_status": "ok" if text else None,
        "pdf_extraction_results_json": None,
    }


def _enrich(record: dict, resource: _FakeS3Resource | None = None) -> dict:
    fetcher = DerivedCommentText(resource or _FakeS3Resource(_store()), BUCKET)
    (out,) = list(EnrichCommentText(fetcher).apply([record]))
    return out


def test_enrich_fills_text_status_and_provenance() -> None:
    out = _enrich(_comment_record("ACF-2025-0038-0004", attachments=True))
    assert out["text_content"] == "Wisconsin DCF comment body"
    assert out["text_extraction_status"] == "derived"
    assert json.loads(out["pdf_extraction_results_json"])["tool"] == "pypdf"


def test_enrich_skips_comments_without_attachments() -> None:
    # No attachments: even if a stray extraction existed, we don't look it up.
    out = _enrich(_comment_record("ACF-2025-0038-0004", attachments=False))
    assert out["text_content"] is None
    assert out["text_extraction_status"] is None


def test_enrich_leaves_status_none_when_no_derived_text() -> None:
    # Left pending so the backfill and the PDF-download fallback can fill it later.
    out = _enrich(_comment_record("ACF-2025-0038-9999", attachments=True))
    assert out["text_content"] is None
    assert out["text_extraction_status"] is None


def test_enrich_leaves_a_refused_docket_pending() -> None:
    store = _store() | {comments_extracted_prefix(*ACF) + "stray.txt": b"?"}
    out = _enrich(_comment_record("ACF-2025-0038-0004", attachments=True), _FakeS3Resource(store))
    assert (out["text_content"], out["text_extraction_status"], out["pdf_extraction_results_json"]) == (None,) * 3


def test_enrich_does_not_overwrite_existing_text() -> None:
    out = _enrich(_comment_record("ACF-2025-0038-0004", attachments=True, text="already here"))
    assert out["text_content"] == "already here"
    assert out["text_extraction_status"] == "ok"


# --- Chain -------------------------------------------------------------------


class _AddOne(Transform):
    def apply(self, records):
        for r in records:
            yield {**r, "n": r.get("n", 0) + 1}


def test_chain_applies_transforms_in_order() -> None:
    out = list(Chain(_AddOne(), _AddOne()).apply([{"n": 0}, {"n": 5}]))
    assert [r["n"] for r in out] == [2, 7]


def test_chain_extract_then_enrich_end_to_end() -> None:
    """The exact composition the pipeline wires for comments."""
    from spicy_regs.schemas import COMMENT

    payload = {
        "data": {
            "id": "ACF-2025-0038-0004",
            "attributes": {
                "docketId": "ACF-2025-0038",
                "agencyId": "ACF",
                "comment": "See attached file(s)",
            },
            "relationships": {"attachments": {"data": [{"id": "a", "type": "attachments"}]}},
        },
        "included": [
            {
                "id": "a",
                "type": "attachments",
                "attributes": {
                    "title": "t",
                    "fileFormats": [{"fileUrl": "https://x/a.pdf", "format": "pdf", "size": 10}],
                },
            }
        ],
    }
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    chain = Chain(ExtractRecords(COMMENT), EnrichCommentText(fetcher))
    (out,) = list(chain.apply([payload]))
    assert out["comment_id"] == "ACF-2025-0038-0004"
    assert out["text_content"] == "Wisconsin DCF comment body"
    assert out["text_extraction_status"] == "derived"
