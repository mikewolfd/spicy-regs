"""GAO product pages, read through Zyte for the rows missing a count, a page count or subject terms.

``gao_reports`` took its recommendation and matters-for-Congress counts, page count and subject terms from the
CetiAlphaFive/gao R package for the reports that copy lists; a report issued after it (about 30 a week) has none.
A product page states them as fields, and spicy-docs reads them (``sources.gao.product_details``, rule
``PRODUCT_PAGE_DETAILS_RULE``). The owner asked for a daily read on 2026-09-29 and ruled on 2026-10-04 that a
page-read value outranks the package's. The rules below are the reader's importer note (spicy-docs
``docs/decisions.md``, "A GAO product page that may not be whole refuses"), which follows the hand validation of
2026-10-05 (``~/Work/corpora/gao-product-pages-validation-2026-10-05/``).

**A value written is never read again**, so every rule here is about not writing a wrong one, and the last is
about taking one back.

**Two switches, and both must be on.** Nothing here runs unless ``build_gao_reports`` is given
``product_pages=True``, which the rollup takes from ``GAO_PRODUCT_PAGES``; the workflow passes ``false`` and gives
the job no Zyte token. And asked or not, the pass does not run under a reader rule before
:data:`MIN_RULE_VERSION` (:func:`reader_refusal`): rule ``/1`` reads 0 recommendations from a page whose
recommendations wrapper GAO renamed or whose body was cut before it, and reads through a pager it does not know;
``/2`` refuses those.

**Known pages first.** Each pass first reads :data:`KNOWN_PAGES`, whose values are known, and stops before it reads
or writes a row unless each reads exactly so (:func:`known_page_failure`). It is the one guard against what the
reader cannot see: a theme that renames a view's wrapper and rewords its heading reads 0 on every page, and a
reworded Full Report label or subject terms printed another way read as none.

**Which rows.** A row is pending while any of :data:`FILL_COLUMNS` is NULL and no read of its page is recorded.
Each run asks for the newest by ``published_date``, at most :data:`PAGES_PER_RUN` requests, :data:`SPACING_SECONDS`
apart (the owner's standing rulings for GAO's pages, 2026-09-28 and 2026-10-03; the library default is the site's
420-second crawl delay).

**Not on its release day, and with a second witness in its first week** (:func:`not_yet`, :func:`agrees`). No page
was read on its release day, so none is written from one: a product is read from the day after GAO dates it. Up to
:data:`FRESH_DAYS` days old, the page's recommendations and matters together must equal the product's rows in
GAO's open-recommendations export, which the host holds as ``gao_recommendations`` and which listed every new
product's recommendations on its release day. The export must have been stamped after the release day, or a page
and an export that both say none would agree about nothing. A page that differs, and a product whose export is not
held yet, write nothing and wait for a later run.

**What a read sets.** Every one of the four the page states, over whatever the row held: the page outranks the
package. A page states both counts always (0 where it prints neither heading), so they are set on every read; a
page count or term list it does not state leaves the cell as held, never emptied. The row keeps its ``source``.
The page's topic and affected agencies are not read into the table: a page prints one topic, and the table's
``topics_json`` holds another route's several (the owner's call, not a fill).

**What a read records.** ``product_page_json`` is a receipt field, not a column: the reader's rule, the outcome,
the page's url and digest, when it was captured, which columns it stated and, as ``before``, what each held.
``unavailable`` is GAO's 404 or 410 for an id it does not serve (``opa-97-2``, a GovInfo spelling): recorded, so
the row is not asked for again each day.

**A refusal writes nothing and is asked again.** A page the reader refuses changes no cell and records nothing on
the row; its reason goes to the run journal, and a later run asks again. So does a failed request (a proxy error, a
timeout); :data:`MAX_CONSECUTIVE_FAILURES` in a row end the pass. More than one :data:`THEME_REASONS` refusal in a
pass is GAO's theme changing: the pass stops and writes nothing it read.

**Taking a read back.** :func:`undone` puts back what ``before`` holds and clears the record, for every read
captured in a named day or hour, or under a named rule; those rows are pending again. It needs no request and runs
with the pass off (``GAO_PRODUCT_PAGES_UNDO``).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from spicy_regs.sources import gao_major_rule_letters, r2

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: The columns a product page states.
FILL_COLUMNS = ("recommendation_count", "matters_for_congress_count", "page_count", "subject_terms_json")
#: The receipt field that holds the reading.
RECEIPT = "product_page_json"
STAGE = "gao-product-page"
KNOWN_STAGE = "gao-product-page-known"
#: The reader's rule name, and the first version of it this pass runs under.
RULE_NAME = "gao-product-page-details"
MIN_RULE_VERSION = 2
#: The pages every pass reads first, and what each must state: recommendations, matters, pages of the Full Report,
#: and how many subject terms. The first is the R package's largest recommendation count, the second its largest
#: count of matters, so each view is witnessed by a page that prints it.
KNOWN_PAGES: tuple[tuple[str, tuple[int, int, int, int]], ...] = (
    ("gao-04-49", (239, 0, 148, 10)),
    ("ggd-91-26", (0, 35, 200, 10)),
)
#: Requests a run makes for rows: at two seconds apart and about a second a request, under ten minutes of a
#: 30-minute job. The backlog on 2026-10-05 (11,594 pending rows once the major-rule reports join) drains in about
#: 78 daily runs.
PAGES_PER_RUN = 150
SPACING_SECONDS = 2.0
#: Failed requests in a row that end a pass: the proxy or GAO is down, and the rest of the budget would be wasted.
MAX_CONSECUTIVE_FAILURES = 5
#: Days after its release that a product's page needs the export's agreement.
FRESH_DAYS = 7
#: The refusals that say a table may not be whole; more than one in a pass is GAO's theme changing.
THEME_REASONS = frozenset({"unread-section", "conflicting-row-count", "paged"})
READ, REFUSED, UNAVAILABLE = "read", "refused", "unavailable"
#: The reason spicy-docs gives a failure that is not a reading of the page.
ACQUISITION = "acquisition"
#: GAO's own table of open recommendations, as the host publishes it.
EXPORT = "gao_recommendations.parquet"
#: GAO dates a product in Washington's time.
GAO_ZONE = ZoneInfo("America/New_York")
#: What an undo may name beside a rule: a day, or a day and as much of a capture time as wanted. Never less than a day.
_OBSERVED = re.compile(r"\d{4}-\d{2}-\d{2}(?:T[\d:.+Z-]*)?")


def _rule() -> str:
    from spicy_docs.sources.gao.product_details import PRODUCT_PAGE_DETAILS_RULE

    return PRODUCT_PAGE_DETAILS_RULE


def rule_version(rule: object) -> int | None:
    """The version of a ``gao-product-page-details/N`` rule id; None for anything else."""
    name, _, version = str(rule).rpartition("/")
    return int(version) if name == RULE_NAME and version.isdecimal() else None


def reader_refusal() -> str | None:
    """Why the installed reader may not run the pass, or None where it may: its rule is version 2 or later."""
    rule = _rule()
    version = rule_version(rule)
    if version is None or version < MIN_RULE_VERSION:
        return f"the reader's rule is {rule}; the pass runs from {RULE_NAME}/{MIN_RULE_VERSION}"
    return None


def known_page_failure(acquirer: Any, product_id: str, expected: tuple[int, int, int, int]) -> tuple[str | None, Any]:
    """Read one known page: why the pass must stop, or None, and the capture or error to retain."""
    from spicy_docs.sources.gao.product_details import GaoProductDetailsError

    try:
        details, capture = acquirer.acquire_details(product_id)
    except GaoProductDetailsError as error:
        return f"{product_id} was not read: {getattr(error, 'reason', ACQUISITION)}", error
    terms = details.subject_terms
    stated = (details.recommendation_count, details.matters_for_congress_count, details.page_count,
              None if terms is None else len(terms))
    if stated != expected:
        return (f"{product_id} states {stated} (recommendations, matters, pages, subject terms), not {expected}",
                capture)
    return None, capture


def today() -> date:
    """The day it is where GAO dates its products."""
    return datetime.now(GAO_ZONE).date()


@dataclass(frozen=True, slots=True)
class OpenRecommendations:
    """GAO's open-recommendations export as the host holds it."""

    #: The newest day GAO stamped an export the host read, as an ISO date.
    as_of: str
    #: Each product's rows, by ``report_id``; a product it never listed has none.
    rows: Mapping[str, int]


def open_recommendations(directory: Path) -> OpenRecommendations | None:
    """The published ``gao_recommendations`` table's rows per product and newest stamp; None where none is held.

    Read from a copy already in ``directory``, else downloaded; the copy is scratch and is removed. O(table), one
    pass over two columns.
    """
    path = directory / f"_{EXPORT}"
    if not (path.exists() or r2.download(EXPORT, path)):
        return None
    try:
        table = pq.read_table(path, columns=["report_id", "last_seen"])
    finally:
        path.unlink(missing_ok=True)
    rows: dict[str, int] = {}
    for report_id in table.column("report_id").to_pylist():
        rows[report_id] = rows.get(report_id, 0) + 1
    stamps = [str(seen)[:10] for seen in table.column("last_seen").to_pylist() if seen is not None]
    return OpenRecommendations(max(stamps), rows) if stamps else None


def _age(row: Mapping[str, Any], on: date) -> int | None:
    """Days since GAO dated the product; None for an undated row, which no rule here holds back."""
    released = row.get("published_date")
    return None if released is None else (on - date.fromisoformat(released)).days


def not_yet(row: Mapping[str, Any], on: date, export: OpenRecommendations | None) -> str | None:
    """Why the row's page is not asked for on day ``on``, or None where it is.

    ``released_today``: the product is dated ``on`` or later. ``waiting_for_export``: it is in its first
    :data:`FRESH_DAYS` days and the host holds no export stamped after its release day.
    """
    age = _age(row, on)
    if age is None:
        return None
    if age < 1:
        return "released_today"
    if age <= FRESH_DAYS and (export is None or export.as_of <= row["published_date"]):
        return "waiting_for_export"
    return None


def agrees(row: Mapping[str, Any], details: Any, on: date, export: OpenRecommendations | None) -> bool:
    """Whether a page may be written: past its first week, or its counts together equal its rows in the export."""
    age = _age(row, on)
    if age is None or age > FRESH_DAYS:
        return True
    stated = details.recommendation_count + details.matters_for_congress_count
    return export is not None and stated == export.rows.get(row["report_id"], 0)


def pending(row: Mapping[str, Any]) -> bool:
    """Whether the row's page is to be read: one of the four is NULL and no read of the page is recorded."""
    return row.get(RECEIPT) is None and any(row.get(column) is None for column in FILL_COLUMNS)


def newest_first(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The pending rows, newest ``published_date`` first, an undated row last; ties in ``report_id`` order."""
    wanted = sorted((row for row in rows if pending(row)), key=lambda row: row["report_id"])
    return sorted(wanted, key=lambda row: row["published_date"] or "", reverse=True)


def _record(outcome: str, capture: Any, **fields: Any) -> dict[str, Any]:
    reading = {"rule": _rule(), "outcome": outcome, "url": capture.requested_url, "sha256": capture.sha256,
               "observed_at": capture.observed_at, **fields}
    return {RECEIPT: json.dumps(reading, ensure_ascii=False, sort_keys=True)}


def page_cells(row: Mapping[str, Any], details: Any, capture: Any) -> dict[str, Any]:
    """The cells one read sets on ``row``: each of the four the page states, and the receipt field."""
    terms = details.subject_terms
    stated = {
        "recommendation_count": details.recommendation_count,
        "matters_for_congress_count": details.matters_for_congress_count,
        "page_count": details.page_count,
        "subject_terms_json": None if terms is None else json.dumps(list(terms), ensure_ascii=False),
    }
    cells = {column: value for column, value in stated.items() if value is not None}
    before = {column: row.get(column) for column in cells}
    return {**cells, **_record(READ, capture, stated=sorted(cells), before=before)}


def unavailable_cells(capture: Any) -> dict[str, Any]:
    """The receipt field for a product GAO serves no page for (``capture`` is its 404 or 410), so the row is not
    asked for again."""
    return _record(UNAVAILABLE, capture)


def undo_selector(value: str) -> str:
    """``value`` as an undo names its reads: a reader rule, a day, or a day and the start of a time; else refused."""
    if rule_version(value) is None and _OBSERVED.fullmatch(value) is None:
        raise ValueError(
            "GAO_PRODUCT_PAGES_UNDO must be a day, a day and hour (2026-10-12, 2026-10-12T17) or a reader rule "
            f"({RULE_NAME}/2), got {value!r}"
        )
    return value


def undone(row: Mapping[str, Any], selector: str) -> dict[str, Any] | None:
    """``row`` as it was before its page read, where ``selector`` names that read; None otherwise.

    A read is named by the rule it was made under, or by the start of the time it was captured. Each column it set
    takes back what ``before`` holds, the record is cleared, and so is a letter read from the same page bytes.
    """
    held = row.get(RECEIPT)
    if held is None:
        return None
    reading = json.loads(held)
    by_rule = rule_version(selector) is not None
    if not (reading["rule"] == selector if by_rule else reading["observed_at"].startswith(selector)):
        return None
    restored = {**row, **reading.get("before", {}), RECEIPT: None}
    letter = row.get(gao_major_rule_letters.RECEIPT)
    if letter is not None and json.loads(letter)["sha256"] == reading["sha256"]:
        restored |= {**dict.fromkeys(gao_major_rule_letters.CELLS), gao_major_rule_letters.RECEIPT: None}
    return restored


@contextmanager
def page_acquirer(evidence: CaptureEvidence | None) -> Iterator[Any]:
    """spicy-docs' product-page acquirer over Zyte, bounded to the known pages and :data:`PAGES_PER_RUN` requests;
    the token is read from ``ZYTE_TOKEN`` and scrubbed from the evidence journal."""
    from spicy_docs.sources.gao.product_details import GaoProductPageAcquirer, GaoProductPageBudget
    from spicy_docs.sources.zyte import ZyteHttpFetcher, require_zyte_token_from_environment
    from spicy_docs.transport.zyte import ZyteBudget, ZyteTransport

    budget = GaoProductPageBudget(min_request_interval_seconds=SPACING_SECONDS)
    token = require_zyte_token_from_environment()
    if evidence is not None:
        evidence.credential = token
    transport = ZyteTransport(
        ZyteHttpFetcher(token=token),
        max_bytes=budget.max_page_bytes,
        timeout_seconds=budget.timeout_seconds,
        budget=ZyteBudget(PAGES_PER_RUN + len(KNOWN_PAGES)),
    )
    with GaoProductPageAcquirer(budget=budget, transport=transport) as acquirer:
        yield acquirer
