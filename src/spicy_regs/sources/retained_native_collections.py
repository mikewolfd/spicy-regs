"""Integrate finite retained publisher captures through maintained native readers.

No acquisition occurs here. Capture declarations belong to a caller-qualified
selection; byte identity alone does not prove an acquisition time or origin.
"""

from __future__ import annotations

import argparse
import base64
import io
from uuid import UUID
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import httpx
from spicy_docs.reading.json_input import load_decimal_json
from spicy_docs.sources.usitc_edis.records import check_edis_id, parse_documents, parse_attachments
from spicy_docs.sources.usitc_edis.bulk import inspect_bulk_archive
from spicy_docs.reading.pdf_bytes import check_pdf_bytes
from spicy_docs.sources.cftc_comments.acquisition import CftcPortalAcquirer, CftcPortalBudget
from spicy_docs.sources.cftc_comments.reader import CftcCommentsReader
from spicy_docs.sources.cftc_comments.pages import view_comment_url
from spicy_docs.sources.cftc_comments.attachments import CftcPdfError, pdf_locator
from spicy_docs.reading.media_types import bare_media_type
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.reading.refusals import attach_refused_response
from spicy_docs.sources.ferc.download import FercElibraryDownloadBudget
from spicy_docs.sources.ferc.elibrary import (
    ACCESSION_FIELD,
    FercElibraryReader,
    docket_id,
    search_hit_reference,
)
from spicy_docs.sources.ferc.originals import FercElibraryOriginalAcquirer
from rulespec_artifacts import publish_directory_no_replace
from spicy_docs.sources.sec_comments.acquisition import read_comment_file
from spicy_docs.sources.sec_comments.pages import (
    check_continuation,
    comment_index_url,
    parse_comment_listing_page,
    parse_rule_page,
)
from spicy_docs.transport.captured import CapturedBodyResponse, attach_capture, refused_capture

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, exact_json, write_dataset
from spicy_regs.sources.retained_collections import _code_pins, _digest, _file_digest, _json, _read

VERSION = "retained-native-collections-v1"
MAX_MANIFEST = 2 * 1024**2
MAX_INPUT_READS = 1024**3
MAX_CAPTURE = 16 * 1024**2
MAX_JSON_ROWS = 64 * 1024**2
MAX_OUTPUT = 512 * 1024**2
SCHEMAS = {
    "sec_rulemakings": pa.schema(
        [
            (key, pa.string())
            for key in (
                "rulemaking_id",
                "file_number",
                "title",
                "rule_url",
                "discovery_method",
                "source_uri",
                "source_sha256",
                "observed_at",
                "source_record_json",
            )
        ]
    ),
    "sec_comment_files": pa.schema(
        [
            (key, pa.string())
            for key in (
                "file_id",
                "rulemaking_id",
                "file_url",
                "format",
                "pdf_version",
                "body_sha256",
                "observed_at",
                "occurrences_json",
            )
        ]
        + [("bytes", pa.int64()), ("linearized_length", pa.int64())]
    ),
}
for _table, _fields in {
    "ferc_search_documents": ("observation_id", "docket", "reference", "accession", "description", "source_page_json"),
    "ferc_accession_files": ("observation_id", "accession", "file_guid", "source_page_json"),
    "ferc_original_files": ("observation_id", "accession", "file_guid", "filename", "media_type", "body_sha256"),
}.items():
    SCHEMAS[_table] = pa.schema(
        [(key, pa.string()) for key in (*_fields, "source_uri", "source_sha256", "observed_at")]
        + [("record_index", pa.int64()) if _table != "ferc_original_files" else ("bytes", pa.int64())]
    )
for _table, (_strings, _ints) in {
    "cftc_rule_observations": (("observation_id", "scope", "native_listings_json", "observed_at"), ("rule_id",)),
    "cftc_comments": (
        (
            "observation_id",
            "rule_observation_id",
            "native_listing_row_json",
            "native_detail_json",
            "detail_state",
            "observed_at",
        ),
        ("comment_id", "rule_id"),
    ),
    "cftc_comment_files": (
        (
            "observation_id",
            "rule_observation_id",
            "filename",
            "file_url",
            "body_sha256",
            "pdf_version",
            "provider_evidence_json",
            "observed_at",
        ),
        ("comment_id", "rule_id", "file_id", "bytes"),
    ),
}.items():
    SCHEMAS[_table] = pa.schema([(key, pa.string()) for key in _strings] + [(key, pa.int64()) for key in _ints])

for _table, (_strings, _ints) in {
    "edis_documents": (
        ("observation_id", "native_document_json", "source_xml_base64", "source_uri", "source_sha256", "observed_at"),
        ("document_id",),
    ),
    "edis_attachments": (
        (
            "observation_id",
            "document_observation_id",
            "native_attachment_json",
            "source_xml_base64",
            "source_uri",
            "source_sha256",
            "observed_at",
        ),
        ("document_id", "attachment_id"),
    ),
    "edis_attachment_files": (
        (
            "observation_id",
            "document_observation_id",
            "source_artifact_id",
            "source_uri",
            "source_sha256",
            "observed_at",
            "observation_time_state",
            "member_name",
            "body_sha256",
            "pdf_version",
            "native_index_row_json",
            "source_archive_index_base64",
            "member_inventory_json",
        ),
        ("document_id", "attachment_id", "member_ordinal", "bytes"),
    ),
}.items():
    SCHEMAS[_table] = pa.schema([(key, pa.string()) for key in _strings] + [(key, pa.int64()) for key in _ints])

POLICIES = {
    table: DatasetPolicy(
        table,
        schema,
        ("rulemaking_id",)
        if table == "sec_rulemakings"
        else ("file_id",)
        if table == "sec_comment_files"
        else ("observation_id",),
        (),
        policy_version=VERSION,
    )
    for table, schema in SCHEMAS.items()
}


class Captures:
    """Read pinned originals in place with a cumulative decoded-byte limit."""

    def __init__(self, manifest_path: Path):
        self.base = manifest_path.parent
        self.read_bytes = 0
        self.inputs: dict[str, dict] = {}
        self.input_kinds: dict[str, str] = {}

    def read(self, entry: dict) -> CapturedBodyResponse:
        if not isinstance(entry, dict):
            raise ValueError("capture declaration required")
        size = entry.get("bytes")
        if type(size) is not int or not 0 < size <= MAX_CAPTURE:
            raise ValueError("capture size bound")
        if entry.get("status_code") != 200 or type(entry.get("status_code")) is not int:
            raise ValueError("selection requires a successful retained capture")
        method = entry.get("method", "GET")
        if method not in ("GET", "POST") or entry.get("content_encoding", "identity") != "identity":
            raise ValueError("unsupported capture framing")
        if (method == "GET" and entry.get("request_body") is not None) or (
            method == "POST" and not isinstance(entry.get("request_body"), str)
        ):
            raise ValueError("capture request body differs from method")
        url, final = entry["requested_url"], entry["resolved_url"]
        if not isinstance(url, str) or final != url:
            raise ValueError("capture locator differs")
        instant = datetime.fromisoformat(entry["observed_at"].replace("Z", "+00:00"))
        if instant.tzinfo is None:
            raise ValueError("capture observation requires a timezone")
        body = self._body(entry, "capture")
        return CapturedBodyResponse(
            url,
            final,
            200,
            entry["content_type"],
            entry["observed_at"],
            body,
            method=method,
            request_body=entry["request_body"].encode() if method == "POST" else None,
        )

    def count_reads(self, size):
        if type(size) is not int or size < 0:
            raise ValueError("invalid decoded read size")
        self.read_bytes += size
        if self.read_bytes > MAX_INPUT_READS:
            raise ValueError("selected original read budget")

    def _body(self, entry, kind):
        if not isinstance(entry, dict):
            raise ValueError("retained input declaration required")
        size = entry.get("bytes")
        if type(size) is not int or not 0 < size <= MAX_CAPTURE:
            raise ValueError("capture size bound")
        path = Path(entry["path"])
        if not path.is_absolute():
            path = self.base / path
        self.count_reads(size)
        body = _read(path, size)
        if len(body) != size or _digest(body) != entry["sha256"]:
            raise ValueError("retained capture hash/size differs")
        source = {**entry, "path": str(path.resolve())}
        identity = _json(source)
        if identity in self.input_kinds and self.input_kinds[identity] != kind:
            raise ValueError("retained input framing changed")
        self.inputs[identity] = source
        self.input_kinds[identity] = kind
        return body

    def read_blob(self, entry):
        # Retained archives have byte identity without a complete HTTP capture.
        if not isinstance(entry, dict) or not isinstance(entry.get("source_uri"), str) or not entry["source_uri"]:
            raise ValueError("retained blob source declaration required")
        if any(key in entry for key in ("status_code", "requested_url", "resolved_url", "method", "request_body")):
            raise ValueError("retained blob must not invent HTTP capture framing")
        if (
            entry.get("observed_at") is not None
            or entry.get("observation_time_state") != "not-recorded-in-retained-transfer-evidence"
        ):
            raise ValueError("retained blob must preserve unrecorded observation time")
        return self._body(entry, "blob")

    def recheck(self) -> None:
        for identity, entry in list(self.inputs.items()):
            if self.input_kinds[identity] == "blob":
                self.read_blob(entry)
            else:
                self.read(entry)


def witness(capture: CapturedBodyResponse, locator: str) -> dict:
    return {
        "source_id": capture.sha256,
        "sha256": capture.sha256,
        "source_uri": capture.requested_url,
        "locator": locator,
    }


def sec_rows(unit: dict, captures: Captures):
    """Require a complete selected listing chain and every distinct stated file."""
    docket = unit["docket"]
    if any(
        entry.get("method", "GET") != "GET"
        for entry in [*unit["listings"], *unit["files"], *([unit["rule"]] if unit.get("rule") else [])]
    ):
        raise ValueError("SEC selection requires GET captures")
    listing_entries = unit["listings"]
    if not isinstance(listing_entries, list) or not 1 <= len(listing_entries) <= 100:
        raise ValueError("bounded listing selection required")
    first = listing_entries[0]["requested_url"]
    native_rule, rule_capture = None, None
    if unit.get("rule") is not None:
        rule_capture = captures.read(unit["rule"])
        native_rule = parse_rule_page(rule_capture.body, url=rule_capture.requested_url)
        if native_rule.file_number is not None and native_rule.file_number.casefold() != docket.casefold():
            raise ValueError("rule file number differs")
        if native_rule.comment_listing_urls != (first,):
            raise ValueError("select the complete single listing stated by this rule")
        discovery = "publisher-stated"
    else:
        if first != comment_index_url(docket):
            raise ValueError("legacy selection must name its explicit derived listing")
        discovery = "legacy-derived-listing"
    occurrences, stating_captures = defaultdict(list), defaultdict(list)
    pages, next_url = [], first
    seen_pages = set()
    for entry in listing_entries:
        if entry["requested_url"] != next_url or next_url in seen_pages:
            raise ValueError("missing, repeated or unordered listing continuation")
        seen_pages.add(next_url)
        capture = captures.read(entry)
        page = parse_comment_listing_page(capture.body, url=capture.requested_url)
        if page.docket.casefold() != docket.casefold():
            raise ValueError("listing docket differs")
        pages.append(page)
        for role, values in (("sec-comment-section", page.sections), ("sec-comment", page.comments)):
            for index, value in enumerate(values):
                occurrences[value.url].append(
                    {
                        "kind": role,
                        "index": index,
                        "listing_url": page.url,
                        "listing_sha256": capture.sha256,
                        "native": asdict(value),
                    }
                )
                stating_captures[value.url].append(witness(capture, f"{role}:{index}"))
        next_url = page.pager.next_url
        if next_url is not None:
            check_continuation(first, next_url, "selected SEC listing")
    if next_url is not None:
        raise ValueError("selected listing is not terminal")
    files = unit["files"]
    if not isinstance(files, list) or not 1 <= len(files) <= 5000:
        raise ValueError("bounded file selection required")
    urls = [entry["requested_url"] for entry in files]
    if len(set(urls)) != len(urls) or set(urls) != set(occurrences):
        raise ValueError("selected file set differs from all listing declarations")
    if type(unit.get("expected_files")) is not int or len(files) != unit["expected_files"]:
        raise ValueError("selected file count differs")
    anchor = rule_capture or captures.read(listing_entries[0])
    rulemaking_id = _digest(
        _json(["SEC", docket, first, anchor.sha256, [entry["sha256"] for entry in listing_entries]]).encode()
    )
    rule_row = {
        "rulemaking_id": rulemaking_id,
        "file_number": native_rule.file_number if native_rule else docket,
        "title": native_rule.title if native_rule else pages[0].title,
        "rule_url": native_rule.url if native_rule else None,
        "discovery_method": discovery,
        "source_uri": anchor.requested_url,
        "source_sha256": anchor.sha256,
        "observed_at": anchor.observed_at,
        "source_record_json": _json(
            {"rule": asdict(native_rule) if native_rule else None, "listings": [asdict(page) for page in pages]}
        ),
    }
    rule_witnesses = [witness(anchor, "rule" if native_rule else "legacy-listing")]
    # All listing captures participate in the rule observation, including a
    # terminal empty page. File witnesses also retain every repeated occurrence.
    rule_witnesses += [
        {
            "source_id": entry["sha256"],
            "sha256": entry["sha256"],
            "source_uri": entry["requested_url"],
            "locator": f"listing:{index}",
        }
        for index, entry in enumerate(listing_entries)
    ]
    yield "sec_rulemakings", rule_row, rule_witnesses
    for entry in files:
        capture = captures.read(entry)
        fmt, pdf_version, length = read_comment_file(
            capture.body,
            url=capture.requested_url,
            media_type=capture.content_type,
            final_url=capture.resolved_url,
            max_bytes=MAX_CAPTURE,
        )
        file_id = _digest(_json([rulemaking_id, capture.requested_url, capture.sha256]).encode())
        yield (
            "sec_comment_files",
            {
                "file_id": file_id,
                "rulemaking_id": rulemaking_id,
                "file_url": capture.requested_url,
                "format": fmt,
                "pdf_version": pdf_version,
                "body_sha256": capture.sha256,
                "observed_at": capture.observed_at,
                "occurrences_json": _json(occurrences[capture.requested_url]),
                "bytes": capture.byte_size,
                "linearized_length": length,
            },
            [*rule_witnesses[:1], *stating_captures[capture.requested_url], witness(capture, "file")],
        )


def _no_network(request):
    raise RuntimeError("retained reader attempted acquisition")


class _RetainedValidatedCapture:
    """Replace acquisition only; native traversal and validation still execute.

    The selection supplies original request framing and captured responses.
    The client never sends a request or invents a new observation time.
    """

    def retained_init(self, entries, captures):
        self.retained_entries = iter(entries)
        self.retained_captures = captures
        self.retained_count = 0

    def capture_validated(self, url, *, media_types, parse, max_bytes, method="GET", content=None, **kwargs):
        try:
            entry = next(self.retained_entries)
        except StopIteration:
            raise ValueError("missing retained native response") from None
        if entry["requested_url"] != url or entry.get("method", "GET") != method:
            raise ValueError("retained native request locator/method differs")
        original_body = entry.get("request_body")
        if method == "POST":
            if content is None or exact_json(
                load_decimal_json(original_body.encode(), source="retained native request")
            ) != exact_json(load_decimal_json(content, source="native request")):
                raise ValueError("retained native POST body differs")
        elif original_body is not None or content is not None:
            raise ValueError("unexpected retained native GET body")
        capture = self.retained_captures.read(entry)
        self.retained_count += 1
        try:
            if capture.byte_size > max_bytes or bare_media_type(capture.content_type) not in media_types:
                raise ValueError("retained native response media/size differs")
            return parse(capture, max_bytes), capture
        except Exception as error:
            attach_capture(error, capture)
            attach_refused_response(error, refused_capture(capture, stage="source-validation"))
            raise

    def require_consumed(self):
        if next(self.retained_entries, None) is not None:
            raise ValueError("unused retained native response")


class _RetainedFercReader(_RetainedValidatedCapture, FercElibraryReader):
    pass


class _RetainedFercOriginal(_RetainedValidatedCapture, FercElibraryOriginalAcquirer):
    pass


def source_fields(capture):
    return {"source_uri": capture.requested_url, "source_sha256": capture.sha256, "observed_at": capture.observed_at}


def ferc_rows(unit: dict, captures: Captures):
    """Walk the complete selected search; validate every stated public original."""
    entries, files = unit["pages"], unit["files"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 100:
        raise ValueError("bounded FERC pages required")
    if not isinstance(files, list) or not 1 <= len(files) <= 5000:
        raise ValueError("bounded FERC files required")
    docket = docket_id(unit["scope"]["docket"])
    body = load_decimal_json(entries[0]["request_body"].encode(), source="retained FERC search")
    expected_scope = [{"documentClass": unit["scope"]["class"], "documentType": unit["scope"]["type"]}]
    if (
        body.get("docketSearches") != [{"docketNumber": docket, "subDocketNumbers": []}]
        or body.get("classTypes") != expected_scope
    ):
        raise ValueError("selected FERC search scope differs")
    search_witnesses, stated, documents = {}, {}, []
    with _RetainedFercReader(
        budget=PagedJsonBudget(len(entries), MAX_CAPTURE, 10, 0), transport=httpx.MockTransport(_no_network)
    ) as reader:
        reader.retained_init(entries, captures)
        for page in reader.search_pages(body, max_pages=len(entries)):
            for index, record in enumerate(page.records):
                accession = record[ACCESSION_FIELD]
                reference = search_hit_reference(record)
                witnesses = [witness(page.capture, f"searchHits:{index}")]
                documents.append((record, page.capture, index, reference, witnesses))
                if accession in search_witnesses:
                    raise ValueError("selected FERC accession repeated")
                search_witnesses[accession] = witnesses
                for item in record.get("transmittals", []):
                    key = (accession, item["fileId"])
                    if key in stated:
                        raise ValueError("selected FERC file repeated")
                    stated[key] = item
        reader.require_consumed()
    selected = [(item["accession"], item["file_id"]) for item in files]
    if len(set(selected)) != len(selected) or set(selected) != set(stated):
        raise ValueError("selected FERC originals differ from search transmittals")
    if type(unit.get("expected_files")) is not int or len(files) != unit["expected_files"]:
        raise ValueError("selected FERC file count differs")
    for record, capture, index, reference, witnesses in documents:
        yield (
            "ferc_search_documents",
            {
                "observation_id": _digest(_json([reference, capture.sha256, index]).encode()),
                "docket": docket,
                "reference": reference,
                "accession": record[ACCESSION_FIELD],
                "description": record.get("description"),
                "source_page_json": capture.body.decode("utf-8"),
                "record_index": index,
                **source_fields(capture),
            },
            witnesses,
        )
    for item in files:
        accession, guid = item["accession"], item["file_id"]
        with _RetainedFercReader(
            budget=PagedJsonBudget(1, MAX_CAPTURE, 10, 0), transport=httpx.MockTransport(_no_network)
        ) as reader:
            reader.retained_init([item["metadata"]], captures)
            file_list = reader.file_list(accession)
            reader.require_consumed()
        native_ids = [(accession, record["ID"]) for record in file_list.records]
        if len(set(native_ids)) != len(native_ids) or set(native_ids) != {key for key in stated if key[0] == accession}:
            raise ValueError("FERC file list differs from complete search transmittals")
        with _RetainedFercOriginal(
            budget=FercElibraryDownloadBudget(MAX_CAPTURE, 10, 0), transport=httpx.MockTransport(_no_network)
        ) as acquirer:
            acquirer.retained_init([item["original"]], captures)
            original = acquirer.acquire(file_list, file_id=guid)
            acquirer.require_consumed()
        row = original.metadata
        declaration = stated[(accession, guid)]
        if declaration["fileSize"] != row["File_Size_Num"] or declaration["fileName"] != row["Orig_File_Name"]:
            raise ValueError("FERC search/file-list metadata differs")
        if (
            item["declared_bytes"] != row["File_Size_Num"]
            or item["filename"] != row["Orig_File_Name"]
            or original.requested_empty
        ):
            raise ValueError("FERC selected original declaration differs or is empty")
        index = next(index for index, record in enumerate(file_list.records) if record["ID"] == guid)
        witnesses = [*search_witnesses[accession], witness(file_list.capture, f"DataList:{index}")]
        yield (
            "ferc_accession_files",
            {
                "observation_id": _digest(_json([accession, guid, file_list.capture.sha256]).encode()),
                "accession": accession,
                "file_guid": guid,
                "source_page_json": file_list.capture.body.decode("utf-8"),
                "record_index": index,
                **source_fields(file_list.capture),
            },
            witnesses,
        )
        yield (
            "ferc_original_files",
            {
                "observation_id": _digest(_json([accession, guid, original.sha256]).encode()),
                "accession": accession,
                "file_guid": guid,
                "filename": row["Orig_File_Name"],
                "media_type": row["MimeType"],
                "body_sha256": original.sha256,
                "bytes": original.capture.byte_size,
                **source_fields(original.capture),
            },
            [*witnesses, witness(original.capture, "original-file")],
        )


# The scoped listing walk retains the maintained pagination rules.
class _RetainedCftcPages(_RetainedValidatedCapture, CftcPortalAcquirer):
    def capture_validated(self, *args, **kwargs):
        page, capture = super().capture_validated(*args, **kwargs)
        self.parsed_pages.append((page, capture))
        return page, capture


def cftc_rows(unit, captures):
    listings, details, files = unit["listings"], unit["details"], unit["files"]
    scope, rule = unit["scope"], unit["rule_id"]
    if scope not in ("listing-only", "details-and-files"):
        raise ValueError("unsupported CFTC selected scope")
    if not isinstance(listings, list) or not 1 <= len(listings) <= 100:
        raise ValueError("bounded CFTC listings required")
    if scope == "listing-only" and (details or files):
        raise ValueError("CFTC listing-only selection must exclude detail and file bodies")
    for entry in [*listings, *details, *files]:
        if entry.get("method", "GET") != "GET":
            raise ValueError("CFTC selection requires GET captures")
    with _RetainedCftcPages(
        budget=CftcPortalBudget(len(listings), MAX_CAPTURE, 10, 0), transport=httpx.MockTransport(_no_network)
    ) as acquirer:
        acquirer.retained_init(listings, captures)
        acquirer.parsed_pages = []
        # The maintained scoped walk carries all pagination/count/uniqueness rules.
        reader = CftcCommentsReader(
            acquirer,
            years=[datetime.fromisoformat(listings[0]["observed_at"].replace("Z", "+00:00")).year],
            fetch_details=False,
            max_listing_pages=len(listings),
        )
        native_rows = reader._listing_rows(rule)
        acquirer.require_consumed()
        pages = list(acquirer.parsed_pages)
    if type(unit.get("expected_comments")) is not int or len(native_rows) != unit["expected_comments"]:
        raise ValueError("selected CFTC comment count differs")
    rule_id = _digest(_json([rule, scope, [capture.sha256 for page, capture in pages]]).encode())
    all_witnesses = [witness(capture, f"listing:{index}") for index, (page, capture) in enumerate(pages)]
    yield (
        "cftc_rule_observations",
        {
            "observation_id": rule_id,
            "rule_id": rule,
            "scope": scope,
            "native_listings_json": _json([asdict(page) for page, capture in pages]),
            "observed_at": pages[0][1].observed_at,
        },
        all_witnesses,
    )
    details_by_id = {}
    if scope == "details-and-files":
        if len(details) != len(native_rows):
            raise ValueError("selected CFTC detail count differs")
        with _RetainedCftcPages(
            budget=CftcPortalBudget(len(details), MAX_CAPTURE, 10, 0), transport=httpx.MockTransport(_no_network)
        ) as acquirer:
            # Read in listing order, regardless of manifest details order.
            entries = {entry["requested_url"]: entry for entry in details}
            if len(entries) != len(details) or set(entries) != {
                view_comment_url(row.comment_id) for row in native_rows
            }:
                raise ValueError("selected CFTC detail set differs from listing")
            acquirer.retained_init([entries[view_comment_url(row.comment_id)] for row in native_rows], captures)
            acquirer.parsed_pages = []
            for row in native_rows:
                acquisition = acquirer.view_comment_page(row.comment_id)
                detail = acquisition.page
                if (detail.comment_id, detail.rule_id) != (row.comment_id, rule):
                    raise ValueError("CFTC detail/listing identity differs")
                details_by_id[row.comment_id] = (detail, acquisition.capture)
            acquirer.require_consumed()
    declared_files = {}
    for row in native_rows:
        page, capture = next(
            (page, capture) for page, capture in pages if any(x.comment_id == row.comment_id for x in page.rows)
        )
        index = next(index for index, x in enumerate(page.rows) if x.comment_id == row.comment_id)
        detail, detail_capture = details_by_id.get(row.comment_id, (None, None))
        witnesses = [witness(capture, f"comment:{index}")]
        if detail is not None:
            witnesses.append(witness(detail_capture, "detail"))
            for item in detail.files:
                if item.url in declared_files:
                    raise ValueError("CFTC selected file link repeated")
                declared_files[item.url] = (item, row.comment_id, witnesses)
        yield (
            "cftc_comments",
            {
                "observation_id": _digest(_json([rule_id, row.comment_id]).encode()),
                "rule_observation_id": rule_id,
                "comment_id": row.comment_id,
                "rule_id": rule,
                "native_listing_row_json": _json(asdict(row)),
                "native_detail_json": _json(asdict(detail)) if detail is not None else None,
                "detail_state": "complete" if detail is not None else "not-selected",
                "observed_at": capture.observed_at,
            },
            witnesses,
        )
    selected = {entry["requested_url"]: entry for entry in files}
    if len(selected) != len(files) or set(selected) != set(declared_files):
        raise ValueError("CFTC selected PDF set differs from all detail declarations")
    for url, entry in selected.items():
        item, comment, witnesses = declared_files[url]
        locator = pdf_locator(url)
        if locator.file_id != item.file_id:
            raise ValueError("CFTC stated PDF identity differs")
        capture = captures.read(entry)
        try:
            if bare_media_type(capture.content_type) != "application/pdf":
                raise ValueError("CFTC stated PDF media differs")
            version = check_pdf_bytes(capture.body, error_type=CftcPdfError, label="retained CFTC letter")
        except Exception as error:
            attach_capture(error, capture)
            attach_refused_response(error, refused_capture(capture, stage="source-validation"))
            raise
        yield (
            "cftc_comment_files",
            {
                "observation_id": _digest(_json([rule_id, item.file_id, capture.sha256]).encode()),
                "rule_observation_id": rule_id,
                "comment_id": comment,
                "rule_id": rule,
                "file_id": item.file_id,
                "filename": item.file_name,
                "file_url": url,
                "body_sha256": capture.sha256,
                "bytes": capture.byte_size,
                "pdf_version": version,
                "provider_evidence_json": _json(entry.get("provider_evidence")),
                "observed_at": capture.observed_at,
            },
            [*witnesses, witness(capture, "letter-file")],
        )


def blob_witness(entry, locator):
    return {
        "source_id": entry["sha256"],
        "sha256": entry["sha256"],
        "source_uri": entry["source_uri"],
        "locator": locator,
    }


def edis_rows(unit, captures):
    document_id = check_edis_id(unit["document_id"])
    for entry in [unit["document"], unit["attachments"]]:
        if entry.get("method", "GET") != "GET":
            raise ValueError("EDIS metadata requires GET captures")
    document_capture = captures.read(unit["document"])
    if document_capture.requested_url != f"https://edis.usitc.gov/data/document/{document_id}":
        raise ValueError("EDIS selected document locator differs")
    documents = parse_documents(document_capture.body, max_bytes=MAX_CAPTURE)
    if len(documents) != 1 or documents[0].id != document_id or documents[0].security_level != "Public":
        raise ValueError("EDIS selection requires exactly the stated public document")
    document = documents[0]
    attachment_capture = captures.read(unit["attachments"])
    if attachment_capture.requested_url != document.attachment_list_url:
        raise ValueError("EDIS attachment locator differs from document")
    attachments = parse_attachments(attachment_capture.body, document_id=document_id, max_bytes=MAX_CAPTURE)
    ids = [item.id for item in attachments]
    expected = unit["expected_attachment_ids"]
    if not isinstance(expected, list) or not expected or any(type(value) is not int for value in expected):
        raise ValueError("EDIS exact attachment selection required")
    if len(set(ids)) != len(ids) or len(set(expected)) != len(expected) or set(ids) != set(expected):
        raise ValueError("EDIS complete attachment metadata differs from selection")
    archive_entry = unit["archive"]
    artifact_id = str(UUID(archive_entry["artifact_id"]))
    if (
        artifact_id != archive_entry["artifact_id"]
        or archive_entry["source_uri"]
        != f"https://edis.usitc.gov/external/attachment/download.zip?artifactId={artifact_id}"
    ):
        raise ValueError("EDIS retained archive artifact locator differs")
    archive_body = captures.read_blob(archive_entry)
    decoded_limit = min(MAX_CAPTURE, MAX_INPUT_READS - captures.read_bytes)
    archive = inspect_bulk_archive(
        io.BytesIO(archive_body),
        byte_size=len(archive_body),
        expected_attachments=attachments,
        max_entries=100,
        max_decoded_bytes=decoded_limit,
        max_metadata_bytes=1024**2,
        max_index_bytes=1024**2,
    )
    decoded_bytes = sum(member["byteSize"] for member in archive.inventory["members"])
    captures.count_reads(decoded_bytes)
    retained = unit["retained_member_expectations"]
    actual = [
        {key: member[key] for key in ("ordinal", "name", "byteSize", "sha256", "crc32")}
        for member in archive.inventory["members"]
    ]
    if actual != retained:
        raise ValueError("EDIS complete archive membership differs from retained result")
    doc_observation = _digest(_json(["EDIS", document_id, document_capture.sha256]).encode())
    document_witnesses = [witness(document_capture, "document:0")]
    yield (
        "edis_documents",
        {
            "observation_id": doc_observation,
            "document_id": document_id,
            "native_document_json": _json(asdict(document)),
            "source_xml_base64": base64.b64encode(document_capture.body).decode(),
            **source_fields(document_capture),
        },
        document_witnesses,
    )
    by_attachment = {item.id: (index, item) for index, item in enumerate(attachments)}
    for index, item in enumerate(attachments):
        yield (
            "edis_attachments",
            {
                "observation_id": _digest(_json([doc_observation, item.id, attachment_capture.sha256]).encode()),
                "document_observation_id": doc_observation,
                "document_id": document_id,
                "attachment_id": item.id,
                "native_attachment_json": _json(asdict(item)),
                "source_xml_base64": base64.b64encode(attachment_capture.body).decode(),
                **source_fields(attachment_capture),
            },
            [*document_witnesses, witness(attachment_capture, f"attachment:{index}")],
        )
    for row in archive.rows:
        index, item = by_attachment[row.attachment_id]
        member = archive.inventory["members"][row.member_ordinal]
        if row.document_id != document_id or member["name"] != row.member_name or not member["crcVerified"]:
            raise ValueError("EDIS archive member identity/CRC differs")
        yield (
            "edis_attachment_files",
            {
                "observation_id": _digest(
                    _json([doc_observation, item.id, archive_entry["sha256"], member["sha256"]]).encode()
                ),
                "document_observation_id": doc_observation,
                "document_id": document_id,
                "attachment_id": item.id,
                "source_artifact_id": artifact_id,
                "source_uri": archive_entry["source_uri"],
                "source_sha256": archive_entry["sha256"],
                "observed_at": archive_entry.get("observed_at"),
                "observation_time_state": archive_entry["observation_time_state"],
                "member_name": row.member_name,
                "member_ordinal": row.member_ordinal,
                "body_sha256": member["sha256"],
                "bytes": member["byteSize"],
                "pdf_version": row.pdf_version,
                "native_index_row_json": _json(asdict(row)),
                "source_archive_index_base64": base64.b64encode(archive.index_bytes).decode(),
                "member_inventory_json": _json(member),
            },
            [
                *document_witnesses,
                witness(attachment_capture, f"attachment:{index}"),
                blob_witness(archive_entry, f"zip-member:{row.member_ordinal}:{row.member_name}"),
            ],
        )


PROCESSORS = {
    "sec-comments": sec_rows,
    "ferc-originals": ferc_rows,
    "cftc-comments": cftc_rows,
    "edis-document-bulk": edis_rows,
}
TABLES = {
    "edis-document-bulk": ("edis_documents", "edis_attachments", "edis_attachment_files"),
    "cftc-comments": ("cftc_rule_observations", "cftc_comments", "cftc_comment_files"),
    "sec-comments": ("sec_rulemakings", "sec_comment_files"),
    "ferc-originals": ("ferc_search_documents", "ferc_accession_files", "ferc_original_files"),
}


def build(manifest_path: Path, output_dir: Path) -> dict:
    raw = _read(manifest_path, MAX_MANIFEST)
    selection = json.loads(raw)
    if selection.get("format") != "spicy-retained-native-inputs" or selection.get("version") != 1:
        raise ValueError("unsupported native selection")
    units = selection.get("sources")
    if not isinstance(units, list) or not 1 <= len(units) <= 32:
        raise ValueError("bounded native source units required")
    if output_dir.exists():
        raise FileExistsError("candidate already exists")
    pins = {**_code_pins(), "native_adapter": _file_digest(Path(__file__))}
    generation = _digest(_json([VERSION, _digest(raw), pins]).encode())
    captures = Captures(manifest_path)
    if any(unit.get("kind") not in PROCESSORS for unit in units):
        raise ValueError("unsupported native source kind")
    data = {table: [] for unit in units for table in TABLES[unit["kind"]]}
    keys, json_bytes = set(), 0
    for unit in units:
        for table, row, witnesses in PROCESSORS[unit["kind"]](unit, captures):
            key = row[POLICIES[table].identity_fields[0]]
            if (table, key) in keys:
                raise ValueError("duplicate native observation")
            keys.add((table, key))
            context = ReceiptContext(generation, key, VERSION, witnesses)
            json_bytes += len(_json([row, asdict(context)]).encode())
            if json_bytes > MAX_JSON_ROWS:
                raise ValueError("native row memory/output budget")
            data[table].append((row, context))
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output_dir.parent, prefix=".native-collections-") as scratch:
        stage = Path(scratch) / "candidate"
        stage.mkdir()
        outputs, receipts = {}, []
        for table in data:
            policy = POLICIES[table]
            subject, receipt = write_dataset(iter(data[table]), stage / table, policy, validation_backend="reference")
            outputs[table] = {
                "rows": len(data[table]),
                "path": str(subject.relative_to(stage)),
                "sha256": _file_digest(subject),
            }
            receipts.append(receipt)
        combined = combine_receipts(receipts, stage / "etl_receipts.parquet")
        captures.recheck()
        if _read(manifest_path, MAX_MANIFEST) != raw or pins != {
            **_code_pins(),
            "native_adapter": _file_digest(Path(__file__)),
        }:
            raise ValueError("selection or code changed during build")
        result = {
            "format": "spicy-retained-native-build",
            "version": 1,
            "generation": generation,
            "state": "local-selected-candidate-built",
            "manifest_sha256": _digest(raw),
            "code_pins": pins,
            "inputs": list(captures.inputs.values()),
            "outputs": outputs,
            "receipts_sha256": _file_digest(combined),
            "source_read_bytes": captures.read_bytes,
            "baseline_inputs": [],
            "limits": [
                "Complete selected capture sets only; publisher-wide coverage unknown.",
                "No acquisition, current publisher assertion or inferred Regulations.gov identity.",
                "Local candidate does not establish public serving or publication.",
            ],
        }
        (stage / "selection.json").write_bytes(raw)
        (stage / "build.json").write_text(json.dumps(result, indent=2) + "\n")
        if sum(path.stat().st_size for path in stage.rglob("*") if path.is_file()) > MAX_OUTPUT:
            raise ValueError("native candidate output budget")
        publish_directory_no_replace(stage, output_dir)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.manifest, args.output)
    print(_json({"generation": result["generation"], "outputs": result["outputs"]}))


if __name__ == "__main__":
    main()
