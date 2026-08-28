"""Federal Register source semantics for the source-native release.

This module classifies source fields and derives source-owned records.  It does
not build the common artifact container; :mod:`spicy_regs.source_native`
delegates that work to ``rulespec_artifacts``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Final, Mapping, cast
from urllib.parse import parse_qs, urlencode, urlparse

from rulespec_artifacts import FramedSection, canonical_json_bytes, framed_section_digest, schema_bundle_digest

SOURCE_SYSTEM_ID: Final = "https://www.federalregister.gov/api/v1"
SOURCE_SYSTEM_VERSION: Final = "v1"
MAX_RESULTS_PER_PAGE: Final = 1_000
MAX_RECONCILIATION_TRAVERSALS: Final = 3
RESULT_CAP: Final = 10_000
MAX_WINDOW_DAYS: Final = 90
MAX_PAGES_PER_TRAVERSAL: Final = 10_000
FEDERAL_REGISTER_DOCUMENTS_URL: Final = f"{SOURCE_SYSTEM_ID}/documents.json"
SCOPE_ID: Final = "federal-register-documents"
SCHEMA_NAME: Final = "federal-register-document"
SCHEMA_VERSION: Final = "1.0"
SCHEMA_PATH: Final = "sources/federal-register-document-1.0.schema.json"

API_RESPONSE_FIELDS: Final = frozenset(
    {
        "count",
        "description",
        "next_page_url",
        "previous_page_url",
        "results",
        "total_pages",
    }
)
DOCUMENT_FIELDS: Final = frozenset(
    {
        "abstract",
        "agencies",
        "agency_names",
        "body_html_url",
        "cfr_references",
        "comments_close_on",
        "docket_ids",
        "document_number",
        "effective_on",
        "end_page",
        "executive_order_number",
        "html_url",
        "pdf_url",
        "publication_date",
        "regulation_id_numbers",
        "signing_date",
        "start_page",
        "subtype",
        "title",
        "topics",
        "type",
        "volume",
    }
)
_RIN: Final = re.compile(r"^[0-9]{4}-[A-Z][A-Z0-9]{3}$")
_ASCII_ID: Final = re.compile(r"^[A-Za-z0-9._-]+$")
_AGENCY_FIELDS: Final = frozenset(
    {"id", "json_url", "name", "parent_id", "raw_name", "slug", "url"}
)
_CFR_REFERENCE_FIELDS: Final = frozenset(
    {"chapter", "citation_url", "part", "title"}
)
_TEXT_FIELDS: Final = frozenset(
    {
        "abstract",
        "body_html_url",
        "comments_close_on",
        "effective_on",
        "executive_order_number",
        "html_url",
        "pdf_url",
        "signing_date",
        "subtype",
        "title",
        "type",
    }
)
_INTEGER_FIELDS: Final = frozenset({"end_page", "start_page", "volume"})
_TEXT_ARRAY_FIELDS: Final = frozenset(
    {"agency_names", "docket_ids", "topics"}
)


class FederalRegisterSourceError(ValueError):
    """The carried Federal Register evidence is not safely publishable."""


FederalRegisterFetch = Callable[[str], bytes]


@dataclass(frozen=True, slots=True)
class FederalRegisterPage:
    """One exact API response and the source cursor that requested it."""

    traversal_index: int
    page_index: int
    request_key: str
    source_cursor: str | None
    response_bytes: bytes
    window_index: int = 0
    window_page_index: int = 0

    @property
    def evidence_media_type(self) -> str:
        return "application/json"

    def __post_init__(self) -> None:
        if min(self.traversal_index, self.page_index, self.window_index, self.window_page_index) < 0:
            raise FederalRegisterSourceError("traversal, window, and page indexes must be non-negative")
        if (self.window_page_index == 0) != (self.source_cursor is None):
            raise FederalRegisterSourceError("Federal Register window boundary differs from its source cursor")
        if not self.request_key:
            raise FederalRegisterSourceError("request_key must be nonempty")
        if not self.response_bytes:
            raise FederalRegisterSourceError("Federal Register evidence must not be empty")


@dataclass(slots=True)
class FederalRegisterTraversalCheck:
    """Check the source-declared inventory while a traversal streams."""

    declared_count: int | None = None
    declared_page_count: int | None = None
    observed_count: int = 0
    observed_page_count: int = 0

    def add(self, response: Mapping[str, Any], *, page_index: int) -> None:
        count = response.get("count")
        total_pages = response.get("total_pages")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise FederalRegisterSourceError("Federal Register response count is invalid")
        if isinstance(total_pages, bool) or not isinstance(total_pages, int) or total_pages < 1:
            raise FederalRegisterSourceError("Federal Register response total_pages is invalid")
        if self.declared_count is None:
            self.declared_count = count
            self.declared_page_count = total_pages
        elif count != self.declared_count or total_pages != self.declared_page_count:
            raise FederalRegisterSourceError("Federal Register page inventory declarations changed")
        if page_index != self.observed_page_count:
            raise FederalRegisterSourceError("Federal Register page indexes are not contiguous")
        self.observed_count += len(response["results"])
        self.observed_page_count += 1

    def finish(self) -> None:
        if self.declared_count != self.observed_count:
            raise FederalRegisterSourceError("Federal Register declared and observed record counts differ")
        if self.declared_page_count != self.observed_page_count:
            raise FederalRegisterSourceError("Federal Register declared and observed page counts differ")


def federal_register_query_scope(value: Mapping[str, Any]) -> dict[str, str]:
    """Validate the one closed Federal Register date-window query."""

    if set(value) != {"publishedFrom", "publishedThrough"}:
        raise FederalRegisterSourceError("Federal Register query scope fields differ")
    try:
        published_from = date.fromisoformat(str(value["publishedFrom"]))
        published_through = date.fromisoformat(str(value["publishedThrough"]))
    except ValueError as error:
        raise FederalRegisterSourceError("Federal Register query scope dates are invalid") from error
    if published_through < published_from:
        raise FederalRegisterSourceError("Federal Register query scope is reversed")
    return {
        "publishedFrom": published_from.isoformat(),
        "publishedThrough": published_through.isoformat(),
    }


def federal_register_documents_url(
    query_scope: Mapping[str, Any],
    *,
    per_page: int = MAX_RESULTS_PER_PAGE,
) -> str:
    """Build the deterministic initial API request for a source date window."""

    scope = federal_register_query_scope(query_scope)
    if isinstance(per_page, bool) or not isinstance(per_page, int) or not 1 <= per_page <= MAX_RESULTS_PER_PAGE:
        raise FederalRegisterSourceError("Federal Register per_page is outside the source bound")
    parameters = [
        ("per_page", str(per_page)),
        ("order", "newest"),
        ("conditions[publication_date][gte]", scope["publishedFrom"]),
        ("conditions[publication_date][lte]", scope["publishedThrough"]),
        *(("fields[]", field) for field in sorted(DOCUMENT_FIELDS)),
    ]
    return f"{FEDERAL_REGISTER_DOCUMENTS_URL}?{urlencode(parameters)}"


def federal_register_request_window(value: str) -> tuple[date, date]:
    """Parse one canonical initial window request and refuse query drift."""

    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.federalregister.gov"
        or parsed.port is not None
        or parsed.path != "/api/v1/documents.json"
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise FederalRegisterSourceError("Federal Register window request URL is invalid")
    parameters = parse_qs(parsed.query, keep_blank_values=True)
    expected_fields = {
        "conditions[publication_date][gte]",
        "conditions[publication_date][lte]",
        "fields[]",
        "order",
        "per_page",
    }
    if set(parameters) != expected_fields:
        raise FederalRegisterSourceError("Federal Register window request fields differ")
    per_page_values = parameters["per_page"]
    if len(per_page_values) != 1 or not per_page_values[0].isdigit():
        raise FederalRegisterSourceError("Federal Register window per_page is invalid")
    per_page = int(per_page_values[0])
    scope = federal_register_query_scope(
        {
            "publishedFrom": parameters["conditions[publication_date][gte]"][0]
            if len(parameters["conditions[publication_date][gte]"]) == 1
            else "",
            "publishedThrough": parameters["conditions[publication_date][lte]"][0]
            if len(parameters["conditions[publication_date][lte]"]) == 1
            else "",
        }
    )
    if parameters["order"] != ["newest"] or parameters["fields[]"] != sorted(DOCUMENT_FIELDS):
        raise FederalRegisterSourceError("Federal Register window request policy differs")
    if federal_register_documents_url(scope, per_page=per_page) != value:
        raise FederalRegisterSourceError("Federal Register window request is not canonical")
    return date.fromisoformat(scope["publishedFrom"]), date.fromisoformat(scope["publishedThrough"])


def validate_federal_register_window_partition(
    windows: list[tuple[tuple[date, date], bool]],
    *,
    query_scope: Mapping[str, Any],
) -> None:
    """Require ordered, nonoverlapping leaf windows that exactly cover the query."""

    scope = federal_register_query_scope(query_scope)
    published_from = date.fromisoformat(scope["publishedFrom"])
    published_through = date.fromisoformat(scope["publishedThrough"])
    if not windows:
        raise FederalRegisterSourceError("Federal Register acquisition has no date windows")
    position = 0

    def consume(expected: tuple[date, date]) -> None:
        nonlocal position
        if position >= len(windows) or windows[position][0] != expected:
            raise FederalRegisterSourceError(
                "Federal Register split windows are missing, overlapping, or unordered"
            )
        _, records_included = windows[position]
        position += 1
        window_start, window_end = expected
        if records_included:
            return
        if window_start == window_end:
            raise FederalRegisterSourceError(
                f"Federal Register result cap is ambiguous for {window_start.isoformat()}"
            )
        midpoint = window_start + (window_end - window_start) // 2
        consume((window_start, midpoint))
        consume((midpoint + timedelta(days=1), window_end))

    window_start = published_from
    while window_start <= published_through:
        window_end = min(
            window_start + timedelta(days=MAX_WINDOW_DAYS - 1),
            published_through,
        )
        consume((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    if position != len(windows):
        raise FederalRegisterSourceError("Federal Register acquisition has extra date windows")


@dataclass(slots=True)
class FederalRegisterAcquisitionCheck:
    """Replay the exact pre-order split tree, including capped probes."""

    windows: list[tuple[tuple[date, date], bool]] = field(default_factory=list)

    def add_window(
        self,
        response: Mapping[str, Any],
        *,
        page_window: object | None,
        records_included: bool,
        response_bytes: bytes,
    ) -> None:
        del response_bytes
        if (
            not isinstance(page_window, tuple)
            or len(page_window) != 2
            or not all(isinstance(value, date) for value in page_window)
        ):
            raise FederalRegisterSourceError(
                "Federal Register page lacks a validated date window"
            )
        expected = response.get("count", RESULT_CAP) < RESULT_CAP
        if records_included is not expected:
            raise FederalRegisterSourceError(
                "Federal Register split decision differs from its response count"
            )
        self.windows.append((cast(tuple[date, date], page_window), records_included))

    def finish(self, *, query_scope: Mapping[str, Any]) -> None:
        validate_federal_register_window_partition(self.windows, query_scope=query_scope)


def federal_register_records_included(
    response: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> bool:
    del query_scope, page_window
    count = response.get("count")
    return isinstance(count, int) and not isinstance(count, bool) and count < RESULT_CAP


def federal_register_acquisition_policy(
    query_scope: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "initialQueryScope": federal_register_query_scope(query_scope),
        "maxTraversals": MAX_RECONCILIATION_TRAVERSALS,
        "maxWindowDays": MAX_WINDOW_DAYS,
        "resultCap": RESULT_CAP,
        "strategy": "date-window-cap-split-stable-reconciliation",
    }


def _validated_page_url(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise FederalRegisterSourceError("Federal Register next_page_url must be null or nonempty text")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.federalregister.gov"
        or parsed.port is not None
        or parsed.path not in {"/api/v1/documents", "/api/v1/documents.json"}
        or not parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise FederalRegisterSourceError("Federal Register returned an unsafe page cursor")
    return value


def federal_register_next_page_url(
    response: Mapping[str, Any],
    *,
    seen_urls: set[str],
) -> str | None:
    """Return one trusted next cursor and refuse a repeated URL."""

    next_url = _validated_page_url(response.get("next_page_url"))
    if next_url is not None:
        if next_url in seen_urls:
            raise FederalRegisterSourceError("Federal Register returned a cyclic page cursor")
        seen_urls.add(next_url)
    return next_url


def iter_federal_register_pages(
    fetch: FederalRegisterFetch,
    *,
    query_scope: Mapping[str, Any],
    traversals: int = 2,
    per_page: int = MAX_RESULTS_PER_PAGE,
    max_pages_per_traversal: int = MAX_PAGES_PER_TRAVERSAL,
) -> Iterator[FederalRegisterPage]:
    """Fetch bounded exact API pages through cap-safe date-window traversal."""

    if (
        isinstance(traversals, bool)
        or not isinstance(traversals, int)
        or not 1 <= traversals <= MAX_RECONCILIATION_TRAVERSALS
    ):
        raise FederalRegisterSourceError("Federal Register traversal count is outside the source bound")
    if (
        isinstance(max_pages_per_traversal, bool)
        or not isinstance(max_pages_per_traversal, int)
        or max_pages_per_traversal < 1
    ):
        raise FederalRegisterSourceError("Federal Register page bound must be positive")
    scope = federal_register_query_scope(query_scope)
    published_from = date.fromisoformat(scope["publishedFrom"])
    published_through = date.fromisoformat(scope["publishedThrough"])
    for traversal_index in range(traversals):
        emitted_pages = 0
        emitted_windows = 0

        def fetch_bytes(request_url: str) -> bytes:
            response_bytes = fetch(request_url)
            if not isinstance(response_bytes, bytes) or not response_bytes:
                raise FederalRegisterSourceError("Federal Register fetch returned no response bytes")
            return response_bytes

        def window_pages(window_start: date, window_end: date) -> Iterator[FederalRegisterPage]:
            nonlocal emitted_pages, emitted_windows
            window_scope = {
                "publishedFrom": window_start.isoformat(),
                "publishedThrough": window_end.isoformat(),
            }
            initial_url = federal_register_documents_url(window_scope, per_page=per_page)
            first_bytes = fetch_bytes(initial_url)
            first_response = parse_page_response(first_bytes)
            declared_count = first_response["count"]
            if emitted_pages >= max_pages_per_traversal:
                raise FederalRegisterSourceError(
                    "Federal Register acquisition exceeded its evidence-page bound"
                )
            window_index = emitted_windows
            emitted_windows += 1
            if declared_count >= RESULT_CAP:
                yield FederalRegisterPage(
                    traversal_index=traversal_index,
                    page_index=emitted_pages,
                    request_key=initial_url,
                    source_cursor=None,
                    response_bytes=first_bytes,
                    window_index=window_index,
                    window_page_index=0,
                )
                emitted_pages += 1
                if window_start == window_end:
                    raise FederalRegisterSourceError(
                        f"Federal Register result cap is ambiguous for {window_start.isoformat()}"
                    )
                midpoint = window_start + (window_end - window_start) // 2
                yield from window_pages(window_start, midpoint)
                yield from window_pages(midpoint + timedelta(days=1), window_end)
                return

            cursor: str | None = None
            request_url = initial_url
            response_bytes = first_bytes
            response = first_response
            seen_urls = {initial_url}
            inventory = FederalRegisterTraversalCheck()
            window_page_index = 0
            while True:
                if emitted_pages >= max_pages_per_traversal:
                    raise FederalRegisterSourceError(
                        "Federal Register acquisition exceeded its evidence-page bound"
                    )
                inventory.add(response, page_index=window_page_index)
                next_url = federal_register_next_page_url(response, seen_urls=seen_urls)
                yield FederalRegisterPage(
                    traversal_index=traversal_index,
                    page_index=emitted_pages,
                    request_key=request_url,
                    source_cursor=cursor,
                    response_bytes=response_bytes,
                    window_index=window_index,
                    window_page_index=window_page_index,
                )
                emitted_pages += 1
                if next_url is None:
                    inventory.finish()
                    return
                cursor = next_url
                request_url = next_url
                response_bytes = fetch_bytes(request_url)
                response = parse_page_response(response_bytes)
                window_page_index += 1

        window_start = published_from
        while window_start <= published_through:
            window_end = min(window_start + timedelta(days=MAX_WINDOW_DAYS - 1), published_through)
            yield from window_pages(window_start, window_end)
            window_start = window_end + timedelta(days=1)


def _reject_float(value: str) -> None:
    raise FederalRegisterSourceError(f"Federal Register JSON contains unsupported float {value!r}")


def _reject_constant(value: str) -> None:
    raise FederalRegisterSourceError(f"Federal Register JSON contains non-finite number {value!r}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FederalRegisterSourceError(f"Federal Register JSON repeats field {key!r}")
        result[key] = value
    return result


def parse_page_response(raw: bytes) -> Mapping[str, Any]:
    """Parse one bounded source response without changing its evidence bytes."""

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except FederalRegisterSourceError:
        raise
    except (UnicodeError, json.JSONDecodeError) as error:
        raise FederalRegisterSourceError(f"invalid Federal Register response JSON: {error}") from error
    if not isinstance(value, Mapping):
        raise FederalRegisterSourceError("Federal Register response must be an object")
    unknown = set(value) - API_RESPONSE_FIELDS
    if unknown:
        raise FederalRegisterSourceError(f"unclassified Federal Register response fields: {sorted(unknown)}")
    count = value.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise FederalRegisterSourceError("Federal Register response count is invalid")
    results = value.get("results", [] if count == 0 else None)
    if not isinstance(results, list):
        raise FederalRegisterSourceError("Federal Register response results must be an array")
    if len(results) > MAX_RESULTS_PER_PAGE:
        raise FederalRegisterSourceError("Federal Register response exceeds the page record bound")
    normalized = dict(value)
    normalized["results"] = results
    normalized.setdefault("next_page_url", None)
    normalized.setdefault("total_pages", 1 if count == 0 else None)
    _validated_page_url(normalized.get("next_page_url"))
    return normalized


def classify_document(value: object) -> dict[str, Any]:
    """Return one faithful, closed source record and reject schema drift."""

    if not isinstance(value, Mapping):
        raise FederalRegisterSourceError("Federal Register result must be an object")
    source = cast(Mapping[str, Any], value)
    unknown = set(source) - DOCUMENT_FIELDS
    if unknown:
        raise FederalRegisterSourceError(f"unclassified Federal Register document fields: {sorted(unknown)}")
    document_number = source.get("document_number")
    if (
        not isinstance(document_number, str)
        or _ASCII_ID.fullmatch(document_number) is None
    ):
        raise FederalRegisterSourceError("Federal Register result lacks document_number")
    publication_date = source.get("publication_date")
    if not isinstance(publication_date, str) or not publication_date:
        raise FederalRegisterSourceError("Federal Register result lacks publication_date")
    try:
        parsed_publication_date = date.fromisoformat(publication_date)
    except ValueError as error:
        raise FederalRegisterSourceError("Federal Register publication_date is invalid") from error
    if parsed_publication_date.isoformat() != publication_date:
        raise FederalRegisterSourceError("Federal Register publication_date is not canonical")
    for field_name in _TEXT_FIELDS:
        field_value = source.get(field_name)
        if field_value is not None and not isinstance(field_value, str):
            raise FederalRegisterSourceError(
                f"Federal Register {field_name} must be text or null"
            )
    for field_name in _INTEGER_FIELDS:
        field_value = source.get(field_name)
        if field_value is not None and (
            isinstance(field_value, bool) or not isinstance(field_value, int)
        ):
            raise FederalRegisterSourceError(
                f"Federal Register {field_name} must be an integer or null"
            )
    for field_name in _TEXT_ARRAY_FIELDS:
        field_value = source.get(field_name)
        if field_value is not None and (
            not isinstance(field_value, list)
            or any(not isinstance(item, str) for item in field_value)
        ):
            raise FederalRegisterSourceError(
                f"Federal Register {field_name} must be a text array or null"
            )
    for field_name, allowed_fields in (
        ("agencies", _AGENCY_FIELDS),
        ("cfr_references", _CFR_REFERENCE_FIELDS),
    ):
        values = source.get(field_name)
        if values is None:
            continue
        if not isinstance(values, list):
            raise FederalRegisterSourceError(
                f"Federal Register {field_name} must be an array or null"
            )
        for item in values:
            if not isinstance(item, Mapping) or not set(item) <= allowed_fields:
                raise FederalRegisterSourceError(
                    f"unclassified Federal Register {field_name} fields"
                )
            for nested_name, nested_value in item.items():
                valid_type = nested_value is None or (
                    not isinstance(nested_value, bool)
                    and isinstance(nested_value, (int, str))
                )
                if not valid_type:
                    raise FederalRegisterSourceError(
                        f"Federal Register {field_name}.{nested_name} has an unsupported type"
                    )
    record = dict(source)
    canonical_json_bytes(record)
    return record


def field_diagnostics(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Describe malformed RIN values without removing their source evidence."""

    values = record.get("regulation_id_numbers")
    if values is None:
        return []
    if not isinstance(values, list):
        return [
            {
                "code": "malformed-rin-container",
                "field": "regulation_id_numbers",
                "value": values,
            }
        ]
    diagnostics = []
    for value in values:
        if not isinstance(value, str) or _RIN.fullmatch(value) is None:
            diagnostics.append(
                {
                    "code": "malformed-rin",
                    "field": "regulation_id_numbers",
                    "value": value,
                }
            )
    return diagnostics


def source_record(record: Mapping[str, Any], *, schema_digest: str) -> dict[str, Any]:
    """Wrap classified source fields with their source schema reference."""

    document_number = str(record["document_number"])
    return {
        "fieldDiagnostics": field_diagnostics(record),
        "record": dict(record),
        "schemaDigest": schema_digest,
        "schemaName": SCHEMA_NAME,
        "schemaVersion": SCHEMA_VERSION,
        "scopeId": SCOPE_ID,
        "sourceRecordId": document_number,
    }


def rendition_rows(record: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    """Preserve every source-stated locator field, including explicit absence."""

    source_record_id = str(record["document_number"])
    definitions = (
        ("body-html", "body_html_url", "text/html"),
        ("html", "html_url", "text/html"),
        ("pdf", "pdf_url", "application/pdf"),
    )
    rows: list[dict[str, Any]] = []
    for rendition_id, source_field, media_type in definitions:
        locator = record.get(source_field)
        if locator is not None and (not isinstance(locator, str) or not locator):
            raise FederalRegisterSourceError(
                f"Federal Register {source_field} must be null or nonempty text"
            )
        rows.append(
            {
                "expectedByteSize": None,
                "expectedSha256": None,
                "locator": locator,
                "mediaType": media_type,
                "renditionId": rendition_id,
                "sourceField": source_field,
                "sourceRecordId": source_record_id,
            }
        )
    return tuple(rows)


_NULLABLE_TEXT_SCHEMA: Final = {"type": ["string", "null"]}
_TEXT_ARRAY_SCHEMA: Final = {
    "items": {"type": "string"},
    "type": ["array", "null"],
}

FEDERAL_REGISTER_DOCUMENT_SCHEMA: Final[dict[str, Any]] = {
    "$id": "urn:spicy-regs:schema:federal-register-document:1.0",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "additionalProperties": False,
    "properties": {
        "abstract": _NULLABLE_TEXT_SCHEMA,
        "agencies": {
            "items": {
                "additionalProperties": False,
                "properties": {
                    field: {"type": ["integer", "string", "null"]}
                    for field in sorted(_AGENCY_FIELDS)
                },
                "type": "object",
            },
            "type": ["array", "null"],
        },
        "agency_names": _TEXT_ARRAY_SCHEMA,
        "body_html_url": _NULLABLE_TEXT_SCHEMA,
        "cfr_references": {
            "items": {
                "additionalProperties": False,
                "properties": {
                    field: {"type": ["integer", "string", "null"]}
                    for field in sorted(_CFR_REFERENCE_FIELDS)
                },
                "type": "object",
            },
            "type": ["array", "null"],
        },
        "comments_close_on": _NULLABLE_TEXT_SCHEMA,
        "docket_ids": _TEXT_ARRAY_SCHEMA,
        "document_number": {"minLength": 1, "type": "string"},
        "effective_on": _NULLABLE_TEXT_SCHEMA,
        "end_page": {"type": ["integer", "null"]},
        "executive_order_number": _NULLABLE_TEXT_SCHEMA,
        "html_url": _NULLABLE_TEXT_SCHEMA,
        "pdf_url": _NULLABLE_TEXT_SCHEMA,
        "publication_date": {"format": "date", "type": "string"},
        # The source has emitted malformed containers and members. Preserve the
        # exact JSON value and attach nonfatal field diagnostics instead of
        # silently dropping source evidence.
        "regulation_id_numbers": {},
        "signing_date": _NULLABLE_TEXT_SCHEMA,
        "start_page": {"type": ["integer", "null"]},
        "subtype": _NULLABLE_TEXT_SCHEMA,
        "title": _NULLABLE_TEXT_SCHEMA,
        "topics": _TEXT_ARRAY_SCHEMA,
        "type": _NULLABLE_TEXT_SCHEMA,
        "volume": {"type": ["integer", "null"]},
    },
    "required": ["document_number", "publication_date"],
    "type": "object",
    "x-spicy-record-order": [
        {
            "fieldPath": "/document_number",
            "nullOrder": "forbidden",
            "tupleComparison": "utf16-code-unit",
            "valueType": "string",
        }
    ],
}


def source_schema_digest() -> str:
    """Use the installed Rulespec schema-family identity implementation."""

    return schema_bundle_digest({SCHEMA_PATH: FEDERAL_REGISTER_DOCUMENT_SCHEMA})


def source_schema_declaration() -> dict[str, str]:
    return {
        "schemaDigest": source_schema_digest(),
        "schemaName": SCHEMA_NAME,
        "schemaVersion": SCHEMA_VERSION,
    }


def source_record_digest(record: Mapping[str, Any]) -> str:
    """Digest one source result through Rulespec's sole ordered-record digester."""

    return framed_section_digest(
        "spicyregs-federal-register-record/1",
        (FramedSection("record", 1, (dict(record),)),),
    )


__all__ = [
    "API_RESPONSE_FIELDS",
    "DOCUMENT_FIELDS",
    "FEDERAL_REGISTER_DOCUMENT_SCHEMA",
    "FEDERAL_REGISTER_DOCUMENTS_URL",
    "FederalRegisterAcquisitionCheck",
    "FederalRegisterFetch",
    "MAX_RESULTS_PER_PAGE",
    "MAX_PAGES_PER_TRAVERSAL",
    "MAX_WINDOW_DAYS",
    "RESULT_CAP",
    "FederalRegisterPage",
    "FederalRegisterSourceError",
    "FederalRegisterTraversalCheck",
    "SCHEMA_NAME",
    "SCHEMA_PATH",
    "SCHEMA_VERSION",
    "SCOPE_ID",
    "SOURCE_SYSTEM_ID",
    "SOURCE_SYSTEM_VERSION",
    "classify_document",
    "federal_register_documents_url",
    "federal_register_acquisition_policy",
    "federal_register_next_page_url",
    "federal_register_query_scope",
    "federal_register_request_window",
    "federal_register_records_included",
    "field_diagnostics",
    "iter_federal_register_pages",
    "parse_page_response",
    "rendition_rows",
    "source_record",
    "source_record_digest",
    "source_schema_declaration",
    "source_schema_digest",
    "validate_federal_register_window_partition",
]
