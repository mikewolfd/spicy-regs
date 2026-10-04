"""Useful published observations from sealed retained FEC collection context.

Inputs are native facts in fec_collections, not files at workstation paths.
Context witnesses never become fictional fec_source_records. Historical workbook
rows, complete CSV prefixes, public research metadata and meeting listings retain
separate grains and do not qualify current financial totals or legal identities.
"""

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import re
from typing import NotRequired, TypedDict
from urllib.parse import urljoin, urlsplit

import pyarrow as pa

from .fec_query import AMOUNT_TYPE, _digest, exact_amount
from .fec_context_shape import (
    CSV_FIELDS,
    filing_announcement,
    iso_timestamp,
    nonnegative_integer,
    source_page_body,
)

IDENTITY_VERSION = "fec-research-context/1"
VERSION = "fec-research-context/2"
ROOT = "/receiverDisposition/callerContext/facts"
COMMON = "record_id mapping_version collection_id source_authority source_url source_sha256 context_sha256 source_context_pointer observed_at_json".split()
HISTORICAL_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "worksheet_name worksheet_member worksheet_sha256 row_grain population methodology_text current_total_status reporting_native_id reporting_name filer_type candidate_native_id support_oppose report_type amendment_indicator image_number amount_raw amount_status amount_formula unit reported_date_status native_cells_json".split()
    ]
    + [
        ("worksheet_ordinal", pa.int32()),
        ("worksheet_row", pa.int32()),
        ("amount", AMOUNT_TYPE),
        ("reported_date", pa.date32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
    ]
)
CSV_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "observation_type reporting_committee_id reporting_committee_name counterparty_name transaction_id native_sub_id amount_raw amount_status date_raw date_status native_row_locator_json coverage_status current_total_status".split()
    ]
    + [(n, pa.string()) for n in CSV_FIELDS]
    + [("report_year", pa.int32()), ("report_year_status", pa.string())]
    + [("amount", AMOUNT_TYPE), ("reported_date", pa.date32()), ("source_row_ordinal", pa.int32())]
)
RESEARCH_DOCUMENT_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "document_native_id title description canonical_url source_publisher source_created_at source_updated_at original_extension body_status fec_relationship_status language evidence_use page_count_status".split()
    ]
    + [
        ("page_count", pa.int64()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
MEETING_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON + "meeting_type title_raw reported_status dates_json date_status links_json".split()
    ]
    + [("table_ordinal", pa.int32()), ("table_row", pa.int32())]
)
SOURCE_PAGE_SCHEMA = pa.schema(
    [(n, pa.string()) for n in COMMON + "page_type title text content_scope content_status links_json".split()]
)
RESPONSE_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON + "profile_refusal query_completeness payload_shape outcome_status capture_disposition".split()
    ]
    + [("observed_payload_records", pa.int32())]
)
RSS_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + "title link guid published_at_raw current_record_status reported_filer_id committee_id candidate_id source_label_status filing_id form_type report_type coverage_start_raw coverage_end_raw parsing_status filing_link_status".split()
    ]
    + [
        ("coverage_start_date", pa.date32()),
        ("coverage_end_date", pa.date32()),
        ("published_at", pa.timestamp("us", tz="UTC")),
    ]
)
DISPOSITION_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in "collection_id mapping_status mapping_reason source_url context_sha256 outputs_json".split()
    ]
    + [("source_fact_count", pa.int64())]
)
SCHEMAS = {
    "fec_research_source_pages": SOURCE_PAGE_SCHEMA,
    "fec_research_response_outcomes": RESPONSE_SCHEMA,
    "fec_research_filing_feed_items": RSS_SCHEMA,
    "fec_historical_ie_statistics": HISTORICAL_SCHEMA,
    "fec_retained_csv_observations": CSV_SCHEMA,
    "fec_research_document_observations": RESEARCH_DOCUMENT_SCHEMA,
    "fec_research_meeting_observations": MEETING_SCHEMA,
    "fec_research_context_dispositions": DISPOSITION_SCHEMA,
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _sha(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
        value = "sha256:" + value
    return _digest(value)


@dataclass
class ContextMapping:
    tables: dict = field(default_factory=lambda: {name: [] for name in SCHEMAS})
    evidence: list = field(default_factory=list)


class _Context:
    def __init__(self, collection, source_generation_pin):
        self.generation_pin = _digest(source_generation_pin)
        self.collection_id = collection["collection_id"]
        outcome = json.loads(collection["collection_outcome_json"])
        caller = outcome["receiverDisposition"]["callerContext"]
        self.pin = _digest(caller["pin"]["sha256"])
        self.facts = caller["facts"]
        self.parsing = self.facts.get("parsing", {})
        self.capture = self.facts.get("source_capture", {})
        self.url = self.capture.get("url")
        self.source_sha = _sha(self.capture["sha256"]) if self.capture.get("sha256") else None
        self.authority = (
            self.capture.get("authority") or self.facts.get("source_authority") or urlsplit(self.url or "").hostname
        )
        if not isinstance(self.authority, str):
            self.authority = _json(self.authority)
        self.result = ContextMapping()

    def emit(self, table, pointer, values, *, context=()):
        pointer = ROOT + pointer
        identity = [IDENTITY_VERSION, table, self.collection_id, self.source_sha, self.pin, pointer]
        rid = "sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest()
        row = dict(
            record_id=rid,
            mapping_version=VERSION,
            collection_id=self.collection_id,
            source_authority=self.authority,
            source_url=self.url,
            source_sha256=self.source_sha,
            context_sha256=self.pin,
            source_context_pointer=pointer,
            observed_at_json=_json(self.capture.get("observed_at")),
            **values,
        )
        self.result.tables[table].append(row)
        for p, role in [(pointer, "primary"), *[(ROOT + p, "field_definition") for p in context]]:
            self.result.evidence.append(
                dict(
                    target_table=table,
                    target_record_id=rid,
                    target_generation_scope="self",
                    witness_generation_scope="external",
                    witness_generation_pin=self.generation_pin,
                    role=role,
                    endpoint_kind="collection_context",
                    collection_id=self.collection_id,
                    source_record_id=None,
                    context_column="collection_outcome_json",
                    context_pointer=p,
                    witness_sha256=self.pin,
                    witness_locator_json=_json(dict(context_sha256=self.pin, source_sha256=self.source_sha, pointer=p)),
                )
            )
        return row

    def disposition(self, status, reason):
        count = len(self.parsing.get("facts", self.parsing.get("native_events", [])))
        self.result.tables["fec_research_context_dispositions"].append(
            dict(
                collection_id=self.collection_id,
                mapping_status=status,
                mapping_reason=reason,
                source_url=self.url,
                context_sha256=self.pin,
                source_fact_count=count,
                outputs_json=_json({k: len(v) for k, v in self.result.tables.items() if v}),
            )
        )
        return self.result


def _native_number(cell):
    """Read serialized spreadsheet numbers, never rounded Python float values."""
    native = cell.get("native_cell") or {}
    values = [c.get("text") for c in native.get("children", []) if c["tag"] == "v"]
    formulas = [c.get("text") for c in native.get("children", []) if c["tag"] == "f"]
    if len(values) != 1:
        return (
            None,
            "native_value_absent" if not values else "ambiguous_native_value",
            None,
            formulas[0] if len(formulas) == 1 else None,
        )
    raw = values[0]
    formula = formulas[0] if len(formulas) == 1 else None
    # OOXML <v> also stores shared-string indices, booleans and error caches.
    # Dates can use a numeric XML cell while the workbook reader marks them as
    # dates. Neither encoding establishes a reported monetary amount.
    native_type = native.get("attributes", {}).get("t")
    decoded_type = cell.get("data_type")
    decoded = cell.get("value")
    if (
        native_type not in (None, "n")
        or decoded_type not in (None, "n", "f")
        or isinstance(decoded, bool)
        or isinstance(decoded, dict)
    ):
        return None, "unsupported_native_numeric_type", raw, formula
    value, status = exact_amount(raw)
    if formulas and status == "exact":
        status = "source_formula_cache"
    return value, status, raw, formula


def _cell_text(cell):
    value = cell.get("value") if cell else None
    if value is None or isinstance(value, str):
        return value
    # Native numeric IDs (including image numbers) preserve XML spelling.
    _, status, raw, _ = _native_number(cell)
    return raw if status == "exact" else None


def _cell_date(cell):
    value = cell.get("value") if cell else None
    if value is None:
        return None, "source_null"
    if isinstance(value, dict) and value.get("type") == "datetime":
        try:
            dt = datetime.fromisoformat(value["iso8601"])
            return dt.date(), "native_typed_date"
        except (ValueError, KeyError):
            return None, "invalid_native_date"
    return None, "unsupported_native_date"


def _workbooks(ctx):
    if ctx.parsing.get("status") != "complete-native-workbook":
        return ctx.disposition("unsupported", "Workbook native parse is incomplete")
    table1 = ["ID #", "Committee/Individual", "Filer", "Date", "Amount", "Image", "Support / Oppose", "Candidate"]
    table2 = table1[:6] + ["Report", "Amendment"] + table1[6:]
    prepared = []
    for si, sheet in enumerate(ctx.parsing["worksheets"]):
        rows = sheet["rows"]
        keyed = {r["row"]: (i, r) for i, r in enumerate(rows)}
        if len(keyed) != len(rows) or not {1, 2, 3, 5, 6} <= keyed.keys():
            return ctx.disposition("unsupported", "Worksheet row addresses or required definitions are missing")

        def cells(n):
            return keyed[n][1]["cells"]

        header = [_cell_text(c) for c in cells(6)]
        if header not in (table1, table2):
            return ctx.disposition(
                "unsupported", "Historical independent-expenditure worksheet header is not qualified"
            )
        text = _cell_text(cells(3)[0])
        period = re.fullmatch(r"from (.+) through (.+)", text or "")
        if not period:
            return ctx.disposition("unsupported", "Worksheet period lacks the qualified explicit date range")
        try:
            start, end = (datetime.strptime(period[i], "%B %d, %Y").date() for i in (1, 2))
        except ValueError:
            return ctx.disposition("unsupported", "Worksheet period has invalid dates")
        prepared.append((si, sheet, keyed, header, start, end))
    for si, sheet, keyed, header, start, end in prepared:
        base = f"/parsing/worksheets/{si}"
        definitions = [f"{base}/rows/{keyed[n][0]}" for n in (1, 2, 3, 5, 6)]
        population = _cell_text(keyed[2][1]["cells"][0])
        methodology = _cell_text(keyed[5][1]["cells"][0])
        for row_number, (ri, native_row) in sorted(keyed.items()):
            if row_number <= 6:
                continue
            row_cells = native_row["cells"]
            nonempty = [c for c in row_cells if c.get("value") is not None]
            if not nonempty:
                continue
            by_column = {}
            for cell in row_cells:
                coordinate = cell["coordinate"]
                match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)", coordinate)
                if not match or int(match[2]) != row_number or match[1] in by_column:
                    raise ValueError("Historical cell has a conflicting worksheet address")
                by_column[match[1]] = cell

            def column(n):
                return by_column.get(chr(ord("A") + n), {})

            amount, status, raw, formula = _native_number(column(4))
            total = _cell_text(column(3)) in {"Total", "Total "}
            if not total and _cell_text(column(0)) is None:
                raise ValueError("Nonempty historical worksheet row has no admitted detail or total grain")
            reported_date, date_status = (None, "not_applicable_total") if total else _cell_date(column(3))
            support_ix = header.index("Support / Oppose")
            ctx.emit(
                "fec_historical_ie_statistics",
                f"{base}/rows/{ri}",
                dict(
                    worksheet_name=sheet["name"],
                    worksheet_member=sheet["worksheet_member"],
                    worksheet_sha256=sheet["worksheet_sha256"],
                    worksheet_ordinal=sheet["sheet_ordinal"],
                    worksheet_row=row_number,
                    row_grain="published_total" if total else "published_detail",
                    population=population,
                    methodology_text=methodology,
                    current_total_status="not_qualified_source_total"
                    if total
                    else "not_qualified_originals_and_amendments"
                    if header == table2
                    else "publisher_amendment_methodology_only",
                    reporting_native_id=_cell_text(column(0)),
                    reporting_name=_cell_text(column(1)),
                    filer_type=_cell_text(column(2)),
                    candidate_native_id=_cell_text(column(support_ix + 1)),
                    support_oppose=_cell_text(column(support_ix)),
                    report_type=_cell_text(column(6)) if header == table2 else None,
                    amendment_indicator=_cell_text(column(7)) if header == table2 else None,
                    image_number=_cell_text(column(5)),
                    amount_raw=raw,
                    amount=amount,
                    amount_status=status,
                    amount_formula=formula,
                    unit="USD" if "$" in column(4).get("number_format", "") else "currency_not_stated",
                    reported_date=reported_date,
                    reported_date_status=date_status,
                    period_start=start,
                    period_end=end,
                    native_cells_json=_json(row_cells),
                ),
                context=definitions,
            )
    return ctx.disposition(
        "mapped",
        "Historical published worksheet detail and source totals retained separately; header, period and methodology cells linked; no transaction/current-total adoption",
    )


def _csv(ctx):
    records = ctx.parsing["complete_records"]
    if not records or len(records) != ctx.parsing["complete_record_count"]:
        return ctx.disposition("unsupported", "CSV complete-record count does not match retained records")
    header = records[0]["fields"]
    if len(set(header)) != len(header):
        return ctx.disposition("unsupported", "CSV header contains duplicate names")
    if "contribution_receipt_amount" in header and "contribution_receipt_date" in header:
        kind, amount_field, date_field, party_field = (
            "receipt_preview",
            "contribution_receipt_amount",
            "contribution_receipt_date",
            "contributor_name",
        )
    elif "disbursement_amount" in header and "disbursement_date" in header:
        kind, amount_field, date_field, party_field = (
            "disbursement_preview",
            "disbursement_amount",
            "disbursement_date",
            "recipient_name",
        )
    else:
        return ctx.disposition("unsupported", "CSV prefix lacks a qualified receipt or disbursement header")
    if any(len(r["fields"]) != len(header) for r in records[1:]):
        return ctx.disposition("unsupported", "Complete CSV record has a different width than its header")
    for i, native in enumerate(records[1:], 1):
        if _sha(native["source"]["sha256"]) != ctx.source_sha:
            raise ValueError("CSV row witness has a different capture digest")
        values = dict(zip(header, native["fields"], strict=True))
        amount, status = exact_amount(values[amount_field])
        raw_date = values[date_field]
        try:
            dt, ds = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S").date(), "source_date"
        except (ValueError, TypeError):
            dt, ds = (
                None,
                "source_empty" if raw_date == "" else "source_null" if raw_date is None else "unsupported_spelling",
            )
        ctx.emit(
            "fec_retained_csv_observations",
            f"/parsing/complete_records/{i}",
            dict(
                observation_type=kind,
                reporting_committee_id=values.get("committee_id"),
                reporting_committee_name=values.get("committee_name"),
                counterparty_name=values.get(party_field),
                transaction_id=values.get("transaction_id"),
                native_sub_id=values.get("sub_id"),
                amount_raw=values[amount_field],
                amount=amount,
                amount_status=status,
                date_raw=raw_date,
                reported_date=dt,
                date_status=ds,
                **{name: values.get(name) or None for name in CSV_FIELDS},
                report_year=int(values["report_year"])
                if re.fullmatch(r"[1-9][0-9]{3}", values.get("report_year") or "")
                else None,
                report_year_status="parsed"
                if re.fullmatch(r"[1-9][0-9]{3}", values.get("report_year") or "")
                else "missing"
                if not values.get("report_year")
                else "unsupported_year",
                native_row_locator_json=_json(native["source"]),
                source_row_ordinal=i,
                coverage_status="complete_records_of_truncated_capture",
                current_total_status="unqualified_partial_capture",
            ),
            context=("/parsing/complete_records/0",),
        )
    return ctx.disposition(
        "mapped_partial_capture",
        "Only provider-confirmed complete CSV rows mapped; unconfirmed trailing bytes and parser refusal remain source context, not data rows",
    )


def _documents(ctx):
    matched = False
    for fi, fact in enumerate(ctx.parsing.get("facts", [])):
        if (
            fact.get("kind") != "json-field"
            or fact.get("field") != "results"
            or not isinstance(fact.get("value"), list)
        ):
            continue
        matched = True
        for i, native in enumerate(fact["value"]):
            if not isinstance(native, dict) or not {"id", "title"} <= native.keys():
                raise ValueError("DocumentCloud result lacks native document identity/title")
            extension = native.get("original_extension")
            ctx.emit(
                "fec_research_document_observations",
                f"/parsing/facts/{fi}/value/{i}",
                dict(
                    document_native_id=str(native["id"]),
                    title=native["title"],
                    description=native.get("description"),
                    canonical_url=native.get("canonical_url"),
                    source_publisher=native.get("source"),
                    source_created_at=native.get("created_at"),
                    source_updated_at=native.get("updated_at"),
                    original_extension=extension,
                    body_status="deferred_pdf" if extension and extension.lower() == "pdf" else "body_not_qualified",
                    fec_relationship_status="search_result_only_no_verified_matter_join",
                    language=native.get("language"),
                    page_count=nonnegative_integer(native.get("page_count")),
                    page_count_status="parsed"
                    if nonnegative_integer(native.get("page_count")) is not None
                    else "missing"
                    if native.get("page_count") is None
                    else "invalid_count",
                    created_at=iso_timestamp(native.get("created_at")),
                    updated_at=iso_timestamp(native.get("updated_at")),
                    evidence_use="discovery_only",
                ),
            )
    return ctx.disposition(
        "mapped" if matched else "unsupported",
        "DocumentCloud source search-result metadata; PDF bodies deferred and FEC legal identity not inferred"
        if matched
        else "No DocumentCloud results array in native JSON fields",
    )


def _meeting_dates(text):
    match = re.match(r"^([A-Za-z]+) (\d{1,2})(?:( and |\-)(\d{1,2}))?, (\d{4})", " ".join(text.split()))
    if not match:
        return "[]", "unsupported_spelling"
    try:
        first = datetime.strptime(f"{match[1]} {match[2]} {match[5]}", "%B %d %Y").date()
        if not match[3]:
            return _json([first.isoformat()]), "source_single_date"
        last = datetime.strptime(f"{match[1]} {match[4]} {match[5]}", "%B %d %Y").date()
        return _json([first.isoformat(), last.isoformat()]), "source_listed_dates" if match[
            3
        ].strip() == "and" else "source_date_range"
    except ValueError:
        return "[]", "invalid_date"


class _ContextLink(TypedDict):
    href: str
    url: str
    source_fact_index: int
    body_status: NotRequired[str]


class _MeetingCell(TypedDict):
    text: str
    links: list[_ContextLink]
    attributes: object
    source_fact_indices: list[int]


class _PageHeading(TypedDict):
    tag: str
    text: str
    source_fact_index: int


class _PageLink(_ContextLink):
    label: str
    label_status: str


class _FeedItem(TypedDict):
    anchor: _Context
    index: int
    fields: dict[str, list[str]]
    parts: dict[str, list[int]]


def _meetings(ctx):
    events = ctx.parsing.get("facts", [])
    table = -1
    in_table = False
    heading = None
    heading_text = []
    current_heading = None
    heading_pointer = None
    heading_context = []
    cell: _MeetingCell | None = None
    cells: list[_MeetingCell] = []
    row_pointer = None
    row_number = 0
    count = 0
    for i, e in enumerate(events):
        kind, name = e.get("kind"), e.get("name")
        if kind == "start" and name in {"h2", "h3"}:
            heading, heading_text, heading_pointer = name, [], i
        if kind == "text" and heading:
            heading_text.append(e.get("text") or "")
        if kind == "end" and name == heading:
            current_heading, heading = "".join(heading_text).strip(), None
            assert heading_pointer is not None
            heading_context = list(range(heading_pointer, i + 1))
        if kind == "start" and name == "table":
            if in_table:
                raise ValueError("Nested meeting table is not qualified")
            table, row_number, in_table = table + 1, 0, True
            if current_heading not in {"Open meetings", "Public hearings", "Executive sessions"}:
                raise ValueError("Meeting table has no qualified source heading")
        if not in_table:
            continue
        if kind == "start" and name == "tr":
            cells, row_pointer = [], i
        if kind == "start" and name in {"td", "th"}:
            cell = dict(text="", links=[], attributes=e.get("attributes"), source_fact_indices=[i])
        if cell is not None and kind == "text":
            cell["text"] += e.get("text") or ""
            cell["source_fact_indices"].append(i)
        if cell is not None and kind == "start" and name == "a":
            attrs = dict(e.get("attributes", []))
            if "href" in attrs:
                link: _ContextLink = dict(href=attrs["href"], url=urljoin(ctx.url, attrs["href"]), source_fact_index=i)
                link["body_status"] = (
                    "deferred_pdf"
                    if urlsplit(link["url"]).path.lower().endswith(".pdf")
                    else "linked_body_not_qualified"
                )
                cell["links"].append(link)
        if cell is not None and kind == "end" and name in {"td", "th"}:
            cell["text"] = cell["text"].strip()
            cells.append(cell)
            cell = None
        if kind == "end" and name == "tr":
            if len(cells) != 2 or row_pointer is None:
                raise ValueError("Meeting table row does not have the qualified date/link cell shape")
            title = cells[0]["text"]
            dates, date_status = _meeting_dates(title)
            ctx.emit(
                "fec_research_meeting_observations",
                f"/parsing/facts/{row_pointer}",
                dict(
                    meeting_type=current_heading,
                    title_raw=title,
                    reported_status="canceled" if "(Canceled)" in title else "not_stated",
                    dates_json=dates,
                    date_status=date_status,
                    links_json=_json([link for c in cells for link in c["links"]]),
                    table_ordinal=table,
                    table_row=row_number,
                ),
                context=(
                    *[f"/parsing/facts/{n}" for n in heading_context],
                    *[f"/parsing/facts/{n}" for n in range(row_pointer + 1, i + 1)],
                ),
            )
            row_number, count = row_number + 1, count + 1
        if kind == "end" and name == "table":
            in_table = False
    if in_table or cell is not None:
        raise ValueError("Meeting table is truncated")
    return ctx.disposition(
        "mapped" if count else "unsupported",
        "Native dated meeting listings preserve cancellation, multiple dates, source table cells and linked notice body deferral"
        if count
        else "No qualified meeting rows",
    )


def _source_page(ctx, page_type):
    events = ctx.parsing.get("facts", ctx.parsing.get("native_events", []))
    if not any(e.get("kind") in {"start", "end", "text"} and "name" in e for e in events):
        return ctx.disposition(
            "requires_native_reader",
            "Only original HTML bytes or non-markup facts retained; source-native parsing belongs to SpicyDocs",
        )
    headings: list[_PageHeading] = []
    links: list[_PageLink] = []
    times, texts = [], []
    active_heading: _PageHeading | None = None
    active_link: _PageLink | None = None
    ignored = []
    for i, event in enumerate(events):
        kind, tag = event.get("kind"), event.get("name")
        attrs = dict(event.get("attributes", []))
        if kind == "start" and tag in {"script", "style"}:
            ignored.append(tag)
        if kind == "end" and ignored and tag == ignored[-1]:
            ignored.pop()
            continue
        if ignored:
            continue
        if kind == "start" and tag in {"title", "h1", "h2", "h3"}:
            active_heading = dict(tag=tag, text="", source_fact_index=i)
        if kind == "start" and tag == "time":
            times.append(dict(attributes=attrs, source_fact_index=i))
        if kind == "start" and tag == "a" and "href" in attrs:
            active_link = dict(
                href=attrs["href"],
                url=urljoin(ctx.url, attrs["href"]),
                label="",
                source_fact_index=i,
                label_status="partial_context_part",
            )
            active_link["body_status"] = (
                "deferred_pdf"
                if urlsplit(active_link["url"]).path.lower().endswith(".pdf")
                else "linked_body_not_qualified"
            )
            links.append(active_link)
        if kind == "text":
            text = event.get("text") or ""
            if text.strip():
                texts.append(text)
            if active_heading is not None:
                active_heading["text"] += text
            if active_link is not None:
                active_link["label"] += text
        if kind == "end" and active_heading is not None and tag == active_heading["tag"]:
            headings.append(active_heading)
            active_heading = None
        if kind == "end" and tag == "a" and active_link is not None:
            active_link["label_status"] = "complete_anchor_text"
            active_link = None
    title = next((h["text"].strip() for h in headings if h["tag"] == "h1"), None)
    title = title or next((h["text"].strip() for h in headings if h["tag"] == "title"), None)
    body, content_status = source_page_body(title, "\n".join(texts), ctx.url)
    # Exact events stay in the collection witness. Only links visibly contained
    # in the extracted body enter ordinary subject output.
    body_links = [
        link
        for link in links
        if body
        and link["label"].strip()
        and link["label"].strip() in body
        and link["href"]
        and not link["href"].startswith("#")
    ]
    key = "facts" if "facts" in ctx.parsing else "native_events"
    ctx.emit(
        "fec_research_source_pages",
        f"/parsing/{key}",
        dict(
            page_type=page_type,
            title=title,
            text=body,
            content_status=content_status,
            content_scope="retained_native_context_part",
            links_json=_json(body_links),
        ),
    )
    return ctx.disposition(
        "mapped_source_reference",
        "Native source-page text, headings, stated times and links; no inferred legal matter, filing, publication date or current status",
    )


def _response_outcome(ctx):
    found = False
    for i, fact in enumerate(ctx.parsing.get("facts", [])):
        if "value" not in fact or "profile_refusal" not in fact:
            continue
        found = True
        value = fact["value"]
        count, shape = None, "other_native_payload"
        if isinstance(value, dict):
            if isinstance(value.get("results"), list):
                count = len(value["results"])
                shape = "results_empty" if count == 0 else "results_present"
            elif isinstance(value.get("docs"), list):
                count, shape = len(value["docs"]), "docs_present"
            elif "message" in value:
                shape = "error_message"
        ctx.emit(
            "fec_research_response_outcomes",
            f"/parsing/facts/{i}",
            dict(
                profile_refusal=fact["profile_refusal"],
                query_completeness=fact.get("query_completeness"),
                payload_shape=shape,
                outcome_status="refused_native_observation_not_qualified_empty_success",
                observed_payload_records=count,
                capture_disposition="refused"
                if fact["profile_refusal"]
                else "empty_payload_not_verified_success"
                if shape == "results_empty"
                else "captured_payload_not_verified_success",
            ),
        )
    return ctx.disposition(
        "mapped_refusal_outcome" if found else "reference_documentation",
        "Provider refusal and native payload retained; empty results do not establish successful coverage"
        if found
        else "OpenFEC documentation context contains no qualified source response observation",
    )


def map_filing_feed_contexts(collections, source_generation_pin):
    """Join retained RSS event parts from one capture; never parse original XML."""
    contexts = [_Context(row, source_generation_pin) for row in collections]
    if not contexts:
        raise ValueError("Filing feed requires captured context parts")
    if len({(c.source_sha, c.url) for c in contexts}) != 1:
        raise ValueError("Filing feed parts belong to different captures")
    observations = []
    for ctx in contexts:
        for i, event in enumerate(ctx.parsing.get("facts", [])):
            if not isinstance(event.get("byte_start"), int):
                raise ValueError("Filing feed fact lacks a byte address")
            observations.append((event["byte_start"], ctx, i, event))
    observations.sort(key=lambda item: item[0])
    if len({item[0] for item in observations}) != len(observations):
        raise ValueError("Filing feed parts overlap native event addresses")
    item: _FeedItem | None = None
    field_name = None
    fragments = 0
    for _, ctx, index, event in observations:
        kind, name = event.get("kind"), event.get("name")
        if kind == "start" and name == "item":
            if item is not None:
                raise ValueError("Nested RSS item")
            item = dict(anchor=ctx, index=index, fields={}, parts={})
        if item is None:
            continue
        item["parts"].setdefault(ctx.collection_id, []).append(index)
        if kind == "start" and name != "item":
            field_name = name
            item["fields"].setdefault(name, []).append("")
        elif kind in {"text", "cdata"} and field_name:
            item["fields"][field_name][-1] += event.get("text") or ""
        elif kind == "end" and name == field_name:
            field_name = None
        if kind == "end" and name == "item":
            anchor = item["anchor"]
            fields = item["fields"]

            def single(name):
                values = fields.get(name, [])
                return values[0] if len(values) == 1 else None

            mapped = anchor.emit(
                "fec_research_filing_feed_items",
                f"/parsing/facts/{item['index']}",
                dict(
                    title=single("title"),
                    link=single("link"),
                    guid=single("guid"),
                    published_at_raw=single("pubDate"),
                    **filing_announcement(single("description"), single("pubDate")),
                    current_record_status="source_feed_observation_only",
                ),
            )
            # Each contributing part is an existing collection context witness.
            for witness in contexts:
                if witness.collection_id not in item["parts"]:
                    continue
                anchor.result.evidence.append(
                    dict(
                        target_table="fec_research_filing_feed_items",
                        target_record_id=mapped["record_id"],
                        target_generation_scope="self",
                        witness_generation_scope="external",
                        witness_generation_pin=anchor.generation_pin,
                        role="item_fields",
                        endpoint_kind="collection_context",
                        collection_id=witness.collection_id,
                        source_record_id=None,
                        context_column="collection_outcome_json",
                        context_pointer=ROOT + "/parsing/facts",
                        witness_sha256=witness.pin,
                        witness_locator_json=_json(
                            dict(event_indices=item["parts"][witness.collection_id], source_sha256=witness.source_sha)
                        ),
                    )
                )
            item = None
    if item is not None:
        fragments += 1
    result = ContextMapping()
    for ctx in contexts:
        ctx.disposition(
            "mapped_feed_context",
            "Complete RSS item metadata across retained capture parts; "
            + str(fragments)
            + " unclosed trailing item(s) excluded; no full-feed or filing adoption claim",
        )
        for table, rows in ctx.result.tables.items():
            result.tables[table].extend(rows)
        result.evidence.extend(ctx.result.evidence)
    return result


def map_research_context(collection, source_generation_pin):
    """Map supported source context; emit one explicit disposition otherwise."""
    ctx = _Context(collection, source_generation_pin)
    host = urlsplit(ctx.url or "").hostname
    path = urlsplit(ctx.url or "").path
    if ctx.collection_id.startswith("retained-ie-workbook-"):
        return _workbooks(ctx)
    if "complete_records" in ctx.parsing:
        return _csv(ctx)
    if host == "api.www.documentcloud.org" and path == "/api/documents/search/":
        return _documents(ctx)
    if host == "www.fec.gov" and path == "/meetings/":
        return _meetings(ctx)
    if (
        ctx.collection_id.startswith(("filing-format-", "retained-financial-reference-"))
        or host == "raw.githubusercontent.com"
    ):
        return ctx.disposition(
            "definition_context",
            "Native layout definitions, source code or methodological documentation; not economic observations",
        )
    if host == "api.open.fec.gov":
        return _response_outcome(ctx)
    if host == "efilingapps.fec.gov" and path == "/rss/generate":
        return ctx.disposition(
            "requires_capture_part_group", "Map retained RSS event parts together using map_filing_feed_contexts"
        )
    if host == "efilingapps.fec.gov" and path == "/registration/softwarelogs.htm":
        return ctx.disposition(
            "reference_documentation",
            "Filing software/vendor directory and developer requirements; retained source documentation, "
            "not report submission or receipt events. Original HTML and native reader refusal remain in context",
        )
    if ctx.parsing.get("status") == "literal-html-bytes":
        return ctx.disposition(
            "requires_native_reader",
            "Only retained HTML bytes are exposed; SpicyDocs must supply native events before semantic mapping",
        )
    if host == "docquery.fec.gov":
        if path in {"/cgi-bin/senate_forms/", "/senate/index.html"}:
            return ctx.disposition(
                "reference_inventory", "Senate filing archive inventory remains source discovery context"
            )
        return _source_page(ctx, "filing_form_reference")
    if host == "www.oversight.gov" and path.startswith("/reports/federal"):
        return ctx.disposition(
            "reference_inventory",
            "Oversight report index remains reference context; report editions are mapped by the agency owner",
        )
    if host == "www.fec.gov" and "court-case-alphabetical-index" in path:
        return ctx.disposition(
            "reference_inventory",
            "Court-case alphabetical index remains a source inventory, not duplicate case records",
        )
    if host == "www.fec.gov" and "/court-cases/" in path:
        return _source_page(ctx, "official_court_case_reference")
    if host == "www.fec.gov" and "/data/legal/" in path:
        if any(t in path for t in ("/search/", "/advisory-opinions/")):
            return ctx.disposition("reference_inventory", "Legal search/interface page remains discovery context")
        return _source_page(ctx, "official_legal_document_reference")
    if host == "www.oversight.gov" and "/reports/" in path:
        return _source_page(ctx, "official_oversight_report_reference")
    if path.lower().endswith(".pdf"):
        return ctx.disposition("deferred_pdf", "PDF body processing deferred")
    if not ctx.capture or ctx.parsing.get("status") == "physical-line-fallback":
        return ctx.disposition("provenance_context", "Selection/parse disposition; no new domain observation")
    if (
        ctx.collection_id.startswith(("archive-index-", "agency-archive-inventory-"))
        or "sitemap" in path
        or "index.commoncrawl.org" == host
    ):
        return ctx.disposition(
            "acquisition_inventory",
            "Retained archive/member/capture inventory supports discovery, not source dataset rows",
        )
    return ctx.disposition(
        "reference_or_research_context",
        "Retained source documentation, catalog/index or research discovery content; reference-only context is preserved",
    )
