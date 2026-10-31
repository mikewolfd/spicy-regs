"""FEC historical and reference tables built from their retained originals (ss-j4vz B7-B9).

The retained fec-query conversion builds these four tables here (REBUILT_TABLES in
fec_native_conversion), reading the original bytes from the shared raw archive
through the committed selection (fec_original_selection.json). An original is
admitted only when its acquisition receipt shows a complete response with HTTP
status below 400; an original without a receipt is refused, never admitted on its
content. Each row is its own witness: record_id hashes the original's digest and the
row's native position, so no collection context pointer or receipt digest is carried
(owner decision Q7, one identity bump: fec-original-tables/1).

collection_id is the capture collection that retained the original, the first part
when one original was captured in several parts (the RSS feed's nine parts, one
two-part page); it is a join key to fec_collections, not the part that held a row.

The selection also states how many rows its admitted originals hold per table; the
conversion's qualification refuses a generation whose rebuilt table holds any other count.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import NotRequired, TypedDict
from urllib.parse import urljoin, urlsplit

import pyarrow as pa

from .fec_context_shape import filing_announcement, source_page_body
from .fec_query import AMOUNT_TYPE, exact_amount

IDENTITY_VERSION = "fec-original-tables/1"
VERSION = "fec-original-tables/1"
SELECTION_PATH = Path(__file__).with_name("fec_original_selection.json")
TABLES = (
    "fec_historical_ie_statistics",
    "fec_research_meeting_observations",
    "fec_reference_filing_feed_items",
    "fec_reference_source_pages",
)
# Native position that identifies a row within its original.
IDENTITY_FIELDS = {
    "fec_historical_ie_statistics": ("worksheet_name", "worksheet_row"),
    "fec_research_meeting_observations": ("table_ordinal", "table_row"),
    "fec_reference_filing_feed_items": ("guid",),
    "fec_reference_source_pages": (),
}
COMMON = "record_id mapping_version collection_id source_authority source_url source_sha256 observed_at_json".split()


def _strings(names):
    return [(name, pa.string()) for name in names.split()]


SCHEMAS = {
    "fec_historical_ie_statistics": pa.schema(
        _strings(" ".join(COMMON))
        + _strings(
            "worksheet_name row_grain population methodology_text current_total_status reporting_native_id "
            "reporting_name filer_type candidate_native_id support_oppose report_type amendment_indicator image_number "
            "amount_raw amount_status amount_formula unit reported_date_status"
        )
        + [
            ("worksheet_ordinal", pa.int32()),
            ("worksheet_row", pa.int32()),
            ("amount", AMOUNT_TYPE),
            ("reported_date", pa.date32()),
            ("period_start", pa.date32()),
            ("period_end", pa.date32()),
        ]
    ),
    "fec_research_meeting_observations": pa.schema(
        _strings(" ".join(COMMON) + " meeting_type title_raw reported_status dates_json date_status links_json")
        + [("table_ordinal", pa.int32()), ("table_row", pa.int32())]
    ),
    "fec_reference_filing_feed_items": pa.schema(
        _strings(
            " ".join(COMMON) + " title link guid published_at_raw current_record_status reported_filer_id committee_id "
            "candidate_id source_label_status filing_id form_type report_type coverage_start_raw coverage_end_raw "
            "parsing_status filing_link_status"
        )
        + [
            ("coverage_start_date", pa.date32()),
            ("coverage_end_date", pa.date32()),
            ("published_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "fec_reference_source_pages": pa.schema(
        _strings(" ".join(COMMON) + " page_type title text content_scope content_status links_json")
    ),
}
IE_TABLE1 = ["ID #", "Committee/Individual", "Filer", "Date", "Amount", "Image", "Support / Oppose", "Candidate"]
IE_TABLE2 = IE_TABLE1[:6] + ["Report", "Amendment"] + IE_TABLE1[6:]


@dataclass
class Rebuilt:
    rows: list = field(default_factory=list)
    # (source_sha256, reason) for every selected original or row that was not published.
    refusals: list = field(default_factory=list)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _document(path=SELECTION_PATH):
    document = json.loads(Path(path).read_text())
    if document.get("format") != "fec-original-selection/1":
        raise ValueError("Unsupported FEC originals selection")
    if set(document.get("expected_rows", {})) != set(TABLES):
        raise ValueError("FEC originals selection must state the expected rows of every rebuilt table")
    return document


def selection(path=SELECTION_PATH):
    return _document(path)["originals"]


def expected_rows(table, path=SELECTION_PATH):
    """Rows the selected originals hold for one rebuilt table, as measured against live 784b23da."""
    count = _document(path)["expected_rows"][table]
    if type(count) is not int or count <= 0:
        raise ValueError(f"{table}: the selection must expect a positive row count")
    return count


def _part_order(collection_id):
    """Capture parts are numbered '<capture>-NNNN'; order by capture, then part number."""
    stem, _, part = collection_id.rpartition("-")
    return (stem, int(part)) if part.isdigit() else (collection_id, -1)


def receipt_context(store, entry):
    """The original's transport facts from its own acquisition receipt, rehashed; None without one."""
    key = entry.get("acquisition_receipt")
    if key is None:
        return None
    with store.open_member(key) as stream:
        payload = stream.read(1024 * 1024 + 1)
    if hashlib.sha256(payload).hexdigest() != key.rsplit("/", 1)[-1]:
        raise ValueError(f"Acquisition receipt differs from its identity: {key}")
    digest = entry["source_sha256"].removeprefix("sha256:")
    files = [f for f in json.loads(payload)["files"] if f["sha256"] == digest]
    if len(files) != 1:
        raise ValueError(f"Acquisition receipt does not name exactly one {entry['source_sha256']}")
    return files[0].get("receipt_context") or {}


def receipt_refusal(context):
    """Why an original is not admitted; None when its receipt shows a complete, successful response."""
    if context is None:
        return "no_acquisition_receipt"
    status = context.get("http_status")
    if type(status) is not int:
        return "no_http_status"
    if status >= 400:
        return f"http_status_{status}"
    if context.get("response_complete") is not True:
        return "incomplete_response"
    return None


def read_original(store, entry):
    """Whole original bytes, rehashed and size-checked; refuses any drift from the selection."""
    with store.open(entry["source_sha256"].removeprefix("sha256:")) as stream:
        body = stream.read(entry["bytes"] + 1)
    if len(body) != entry["bytes"] or "sha256:" + hashlib.sha256(body).hexdigest() != entry["source_sha256"]:
        raise ValueError(f"Original differs from its selection: {entry['source_sha256']}")
    return body


class _Original:
    """The emit/disposition interface the shared row rules use, bound to one original."""

    def __init__(self, table, entry, observed_at):
        self.table = table
        self.entry = entry
        self.url = entry["source_url"]
        self.parsing = {}
        self.observed_at_json = _json(observed_at)
        self.rows = []
        self.disposition_status = None

    def emit(self, values):
        position = [values[name] for name in IDENTITY_FIELDS[self.table]]
        identity = [IDENTITY_VERSION, self.table, self.entry["source_sha256"], *position]
        row = dict(
            record_id="sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest(),
            mapping_version=VERSION,
            collection_id=min(self.entry["collection_ids"], key=_part_order),
            source_authority=self.entry["source_authority"],
            source_url=self.url,
            source_sha256=self.entry["source_sha256"],
            observed_at_json=self.observed_at_json,
            **values,
        )
        self.rows.append(row)
        return row

    def disposition(self, status, reason):
        self.disposition_status = (status, reason)
        return self


def _events(body):
    from spicy_docs.reading.markup import read_html_events

    return [asdict(event) for event in read_html_events(body).events]


def _ie_number(cell):
    """Literal workbook number; never a float. Dates and non-numeric cells are not amounts."""
    if cell is None or cell.value is None:
        return None, "native_value_absent", None, None if cell is None else cell.formula
    if cell.kind != "n" or cell.is_date:
        return None, "unsupported_native_numeric_type", cell.value, cell.formula
    value, status = exact_amount(cell.value)
    if cell.formula is not None and status == "exact":
        status = "source_formula_cache"
    return value, status, cell.value, cell.formula


def _ie_text(cell):
    if cell is None or cell.value is None:
        return None
    if cell.kind in {"s", "str", "inlineStr"}:
        return cell.value
    if cell.kind == "n" and not cell.is_date:
        # Native numeric IDs (including image numbers) keep their XML spelling.
        return cell.value if exact_amount(cell.value)[1] == "exact" else None
    return None


def _ie_date(cell):
    if cell is None or cell.value is None:
        return None, "source_null"
    if cell.is_date:
        return (cell.date_value, "native_typed_date") if cell.date_value is not None else (None, "invalid_native_date")
    return None, "unsupported_native_date"


def historical_ie_rows(ctx, body):
    """Published IE worksheet detail and total rows; header, period and methodology qualify each sheet."""
    from datetime import datetime

    from spicy_docs.sources.scorecards.workbooks import literal_sheets

    prepared = []
    for ordinal, (name, cells) in enumerate(literal_sheets(body).items()):
        grid = {}
        for coordinate, cell in cells.items():
            match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)", coordinate)
            if not match:
                raise ValueError("Historical cell has an unsupported worksheet address")
            grid.setdefault(int(match[2]), {})[match[1]] = cell
        header_row = grid.get(6, {})
        header = [_ie_text(header_row.get(chr(ord("A") + i))) for i in range(len(header_row))]
        if header not in (IE_TABLE1, IE_TABLE2):
            return ctx.disposition("unsupported", "Historical independent-expenditure worksheet header is not qualified")
        period = re.fullmatch(r"from (.+) through (.+)", _ie_text(grid.get(3, {}).get("A")) or "")
        if not period:
            return ctx.disposition("unsupported", "Worksheet period lacks the qualified explicit date range")
        try:
            start, end = (datetime.strptime(period[i], "%B %d, %Y").date() for i in (1, 2))
        except ValueError:
            return ctx.disposition("unsupported", "Worksheet period has invalid dates")
        prepared.append((ordinal, name, grid, header, start, end))
    for ordinal, name, grid, header, start, end in prepared:
        population = _ie_text(grid.get(2, {}).get("A"))
        methodology = _ie_text(grid.get(5, {}).get("A"))
        support_ix = header.index("Support / Oppose")
        for row_number, row in sorted(grid.items()):
            if row_number <= 6 or all(cell.value is None for cell in row.values()):
                continue

            def column(n, row=row):
                return row.get(chr(ord("A") + n))

            amount, status, raw, formula = _ie_number(column(4))
            total = _ie_text(column(3)) in {"Total", "Total "}
            if not total and _ie_text(column(0)) is None:
                raise ValueError("Nonempty historical worksheet row has no admitted detail or total grain")
            reported_date, date_status = (None, "not_applicable_total") if total else _ie_date(column(3))
            amount_format = (column(4).number_format if column(4) is not None else None) or ""
            ctx.emit(
                dict(
                    worksheet_name=name,
                    worksheet_ordinal=ordinal,
                    worksheet_row=row_number,
                    row_grain="published_total" if total else "published_detail",
                    population=population,
                    methodology_text=methodology,
                    current_total_status="not_qualified_source_total"
                    if total
                    else "not_qualified_originals_and_amendments"
                    if header == IE_TABLE2
                    else "publisher_amendment_methodology_only",
                    reporting_native_id=_ie_text(column(0)),
                    reporting_name=_ie_text(column(1)),
                    filer_type=_ie_text(column(2)),
                    candidate_native_id=_ie_text(column(support_ix + 1)),
                    support_oppose=_ie_text(column(support_ix)),
                    report_type=_ie_text(column(6)) if header == IE_TABLE2 else None,
                    amendment_indicator=_ie_text(column(7)) if header == IE_TABLE2 else None,
                    image_number=_ie_text(column(5)),
                    amount_raw=raw,
                    amount=amount,
                    amount_status=status,
                    amount_formula=formula,
                    unit="USD" if "$" in amount_format else "currency_not_stated",
                    reported_date=reported_date,
                    reported_date_status=date_status,
                    period_start=start,
                    period_end=end,
                ),
            )
    return ctx.disposition("mapped", "Historical published worksheet detail and source totals")


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


def _meetings(ctx):
    events = ctx.parsing.get("facts", [])
    table = -1
    in_table = False
    heading = None
    heading_text = []
    current_heading = None
    cell: _MeetingCell | None = None
    cells: list[_MeetingCell] = []
    row_pointer = None
    row_number = 0
    count = 0
    for i, e in enumerate(events):
        kind, name = e.get("kind"), e.get("name")
        if kind == "start" and name in {"h2", "h3"}:
            heading, heading_text = name, []
        if kind == "text" and heading:
            heading_text.append(e.get("text") or "")
        if kind == "end" and name == heading:
            current_heading, heading = "".join(heading_text).strip(), None
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
                dict(
                    meeting_type=current_heading,
                    title_raw=title,
                    reported_status="canceled" if "(Canceled)" in title else "not_stated",
                    dates_json=dates,
                    date_status=date_status,
                    links_json=_json([link for c in cells for link in c["links"]]),
                    table_ordinal=table,
                    table_row=row_number,
                )
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
    # Only links visibly contained in the extracted body enter subject output.
    body_links = [
        link
        for link in links
        if body
        and link["label"].strip()
        and link["label"].strip() in body
        and link["href"]
        and not link["href"].startswith("#")
    ]
    ctx.emit(
        dict(
            page_type=page_type,
            title=title,
            text=body,
            content_status=content_status,
            content_scope="whole_original",
            links_json=_json(body_links),
        ),
    )
    return ctx.disposition(
        "mapped_source_reference",
        "Native source-page text, headings, stated times and links; no inferred legal matter, filing, publication date or current status",
    )


def meeting_rows(ctx, body):
    ctx.parsing = {"facts": _events(body)}
    return _meetings(ctx)


def source_page_rows(ctx, body):
    ctx.parsing = {"facts": _events(body)}
    return _source_page(ctx, ctx.entry["page_type"])


def feed_rows(ctx, body):
    """Every RSS item of the whole original, keyed by guid (unique within the capture)."""
    from spicy_docs.reading.rss import read_rss2_channel

    _, items = read_rss2_channel(body, max_bytes=len(body), error_type=ValueError, label="FEC filing RSS", max_items=100_000)
    for item in items:
        fields = {}
        for child in item:
            fields.setdefault(child.tag, []).append(child.text or "")

        def single(name, fields=fields):
            values = fields.get(name, [])
            return values[0] if len(values) == 1 else None

        if single("guid") is None:
            raise ValueError("Filing feed item has no single guid")
        ctx.emit(
            dict(
                title=single("title"),
                link=single("link"),
                guid=single("guid"),
                published_at_raw=single("pubDate"),
                **filing_announcement(single("description"), single("pubDate")),
                current_record_status="source_feed_observation_only",
            ),
        )
    guids = [row["guid"] for row in ctx.rows]
    if len(set(guids)) != len(guids):
        raise ValueError("Filing feed guid repeats within one capture")
    return ctx.disposition("mapped", "Complete RSS item metadata from the whole original")


BUILDERS = {
    "fec_historical_ie_statistics": historical_ie_rows,
    "fec_research_meeting_observations": meeting_rows,
    "fec_reference_filing_feed_items": feed_rows,
    "fec_reference_source_pages": source_page_rows,
}


def rebuild(table, *, store=None, originals=None):
    """Rows of one table from its selected originals: one receipt and one original read each."""
    if table not in BUILDERS:
        raise ValueError(f"No originals builder for {table}")
    if store is None:
        from spicy_docs.storage.raw_archive import CentralRawArchiveStore

        store = CentralRawArchiveStore(create=False)
    result = Rebuilt()
    for entry in originals if originals is not None else selection():
        if entry["table"] != table:
            continue
        context = receipt_context(store, entry)
        refusal = receipt_refusal(context)
        if refusal is not None:
            result.refusals.append((entry["source_sha256"], refusal))
            continue
        if context.get("source_url", entry["source_url"]) != entry["source_url"]:
            # The receipt records what was fetched; a selection URL copied from elsewhere must agree with it.
            raise ValueError(f"Selected source_url differs from its acquisition receipt: {entry['source_sha256']}")
        ctx = _Original(table, entry, context.get("acquired_at"))
        BUILDERS[table](ctx, read_original(store, entry))
        status, reason = ctx.disposition_status or ("unsupported", "No disposition")
        if not status.startswith("mapped"):
            result.refusals.append((entry["source_sha256"], f"{status}: {reason}"))
            continue
        if table == "fec_reference_source_pages":
            # Second check behind the receipt: only a page whose body was extracted is a page
            # (a title saying the request failed, an empty body, unknown body boundaries).
            kept = [row for row in ctx.rows if row["content_status"] == "body_extracted"]
            for row in ctx.rows:
                if row["content_status"] != "body_extracted":
                    result.refusals.append((entry["source_sha256"], row["content_status"]))
            ctx.rows = kept
        result.rows.extend(ctx.rows)
    return result
