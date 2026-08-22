"""Read official Supreme Court opinion metadata and PDF source bytes.

The Court's term index is the authority for case identity, docket number,
decision date, reporter citation, holding summary, and the official PDF URL.
Beautiful Soup handles the source HTML; this adapter owns only the small,
source-specific mapping into raw records. PDF text extraction belongs to the
transform layer.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from contextlib import nullcontext
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from spicy_regs.sources.base import Reader

SCOTUS_BASE_URL = "https://www.supremecourt.gov"
TERM_INDEX_URL = SCOTUS_BASE_URL + "/opinions/slipopinion/{term_code}"
_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
_MAX_RETRIES = 5


def current_term_year(today: date | None = None) -> int:
    """Return the calendar year in which the current October Term began."""
    value = today or date.today()
    return value.year if value.month >= 10 else value.year - 1


def term_code(term_year: int) -> str:
    """Return the two-digit official-site code for a term year."""
    if not 2000 <= term_year <= 2099:
        raise ValueError("Supreme Court term year must be between 2000 and 2099")
    return str(term_year)[-2:]


def _iso_date(value: str) -> str:
    try:
        return datetime.strptime(value.strip(), "%m/%d/%y").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid Supreme Court decision date: {value!r}") from exc


#: Document kinds the term index links to, keyed by the path prefix that
#: identifies each. The slip-opinion prefix is term-specific and is added per
#: parse; these two are fixed.
#:
#: A term index does not link one PDF per opinion the way OT2021+ does. From
#: OT2017 through OT2020 most rows point *into* a bound volume or preliminary
#: print — one PDF holding a whole volume of the U.S. Reports, with the opinion
#: identified by a ``#page=N`` fragment. Measured 2026-08-22, OT2017-OT2020 hold
#: 260 index rows, of which 53 are slip PDFs and 207 point into 16 volume PDFs.
#: The two layouts are therefore both real and must both be recognised; anything
#: else is still refused.
VOLUME_PREFIXES: dict[str, str] = {
    "/opinions/preliminaryprint/": "preliminary-print",
    "/opinions/boundvolumes/": "bound-volume",
}
SLIP_KIND = "slip-opinion"


def _classify_source(pdf_url: str, *, code: str) -> tuple[str, str, str]:
    """Recognise one index link, or refuse it.

    Returns ``(kind, document_url, page)`` where ``document_url`` is what is
    actually fetched — the fragment is an instruction about *where in* the
    document the opinion begins, not part of the resource.

    The guard is unchanged in what it refuses: https only, the Court's own host
    only, a ``.pdf`` path only, and only under a path this function names. It
    gains recognised shapes; it gives up nothing.
    """
    parsed = urlparse(pdf_url)
    if (
        parsed.scheme != "https"
        or parsed.netloc.casefold() != "www.supremecourt.gov"
        or not parsed.path.casefold().endswith(".pdf")
    ):
        raise ValueError(f"unsafe Supreme Court opinion URL: {pdf_url}")

    document_url = f"https://{parsed.netloc}{parsed.path}"
    if parsed.path.startswith(f"/opinions/{code}pdf/"):
        # A slip opinion is its own PDF; a page anchor on one would be a layout
        # this parser has not seen and must not guess at.
        if parsed.fragment:
            raise ValueError(f"unsafe Supreme Court opinion URL: {pdf_url}")
        return SLIP_KIND, document_url, ""

    for prefix, kind in VOLUME_PREFIXES.items():
        if parsed.path.startswith(prefix):
            page = _page_anchor(parsed.fragment, pdf_url)
            return kind, document_url, page

    raise ValueError(f"unsafe Supreme Court opinion URL: {pdf_url}")


def _page_anchor(fragment: str, pdf_url: str) -> str:
    """The ``page=N`` fragment a volume link must carry, as a 1-based string.

    Without it the row would name a volume and claim to be one opinion, which
    is the failure this whole branch exists to avoid — so a volume link with no
    usable anchor is refused rather than stored whole.
    """
    if not fragment.startswith("page="):
        raise ValueError(f"Supreme Court volume link has no page anchor: {pdf_url}")
    value = fragment[len("page=") :]
    if not value.isdigit() or int(value) < 1:
        raise ValueError(f"Supreme Court volume link has no page anchor: {pdf_url}")
    return value


def _assign_page_ends(records: list[dict[str, str]]) -> None:
    """Close each volume opinion's page range against the next one in the volume.

    The index gives a start page and no end. Within one volume the opinions run
    back to back, so the next start minus one is the end; the last opinion in a
    volume runs to the document's end, recorded as an empty string rather than a
    guessed number.

    A volume that is split across two term indexes is bounded per term, so the
    last opinion of the earlier term over-reaches into the next one. That is
    visible in ``source_page_end`` rather than hidden, which is the only honest
    option when the denominator is one term's index.
    """
    by_document: dict[str, list[dict[str, str]]] = {}
    for record in records:
        if record["source_page_start"]:
            by_document.setdefault(record["source_url"], []).append(record)
    for group in by_document.values():
        group.sort(key=lambda r: int(r["source_page_start"]))
        for current, following in zip(group, group[1:], strict=False):
            current["source_page_end"] = str(int(following["source_page_start"]) - 1)


def parse_term_index(html: str, *, term_year: int) -> list[dict[str, str]]:
    """Parse one official term index into stable source metadata."""
    code = term_code(term_year)
    index_url = TERM_INDEX_URL.format(term_code=code)
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, str]] = []
    for row in soup.select("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 6:
            continue
        anchor = cells[3].find("a", href=True)
        if anchor is None:
            continue
        href = str(anchor.get("href") or "").strip()
        pdf_url = urljoin(SCOTUS_BASE_URL, href)
        kind, document_url, page = _classify_source(pdf_url, code=code)
        records.append(
            {
                "term_year": str(term_year),
                "release_number": cells[0].get_text(" ", strip=True),
                "date_decided": _iso_date(cells[1].get_text(" ", strip=True)),
                "docket_number": cells[2].get_text(" ", strip=True),
                "case_name": anchor.get_text(" ", strip=True),
                "holding": str(anchor.get("title") or "").strip(),
                "author_code": cells[4].get_text(" ", strip=True),
                "citation": cells[5].get_text(" ", strip=True),
                "source_index_url": index_url,
                "source_url": document_url,
                "source_document_kind": kind,
                "source_page_start": page,
                "source_page_end": "",
            }
        )
    if not records:
        raise ValueError(
            f"Supreme Court term index {term_year} contained no opinion rows"
        )
    _assign_page_ends(records)
    return records


class _MissingDocument(Exception):
    """A document the index links to that the Court's own server does not serve."""


class SupremeCourtOpinionsReader(Reader):
    """Yield official opinion metadata plus exact PDF bytes for selected terms."""

    def __init__(
        self,
        *,
        term_years: Sequence[int] | None = None,
        max_records: int | None = None,
        client: httpx.Client | None = None,
        skip_missing_documents: bool = False,
    ) -> None:
        years = tuple(
            term_years
            if term_years is not None
            else (current_term_year(),)
        )
        if not years:
            raise ValueError("at least one Supreme Court term year is required")
        if max_records is not None and max_records <= 0:
            raise ValueError("max_records must be positive")
        for year in years:
            term_code(year)
        self.term_years = tuple(sorted(set(years), reverse=True))
        self.max_records = max_records
        self.client = client
        #: Four of the sixteen volume PDFs the OT2017-OT2020 indexes link to
        #: answer 404 at the Court's own URL (the ``*_final.pdf`` prints of
        #: volumes 584 and 585, covering 56 index rows, measured 2026-08-22).
        #: That is an upstream broken link, not a fetch this reader can retry
        #: its way out of. Raising by default keeps a missing document a
        #: problem; setting this makes it a *recorded* problem, so one dead
        #: link cannot cost a whole term.
        self.skip_missing_documents = skip_missing_documents
        #: Index rows skipped because their document 404s, in ``(url, rows)``
        #: form. A run that skipped anything must be able to say what.
        self.missing_documents: dict[str, int] = {}

    def _get(self, client: httpx.Client, url: str) -> httpx.Response:
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = client.get(url)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        "retryable Supreme Court response",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                # A 404 is an answer, not a hiccup. Four of the volume PDFs the
                # pre-2021 indexes link to are permanently missing upstream, and
                # backing off 30 seconds each before believing it costs minutes
                # per run to learn nothing.
                if (
                    exc.response is not None
                    and 400 <= exc.response.status_code < 500
                    and exc.response.status_code != 429
                ):
                    raise
                if attempt == _MAX_RETRIES:
                    raise
                time.sleep(min(2**attempt, 30))
            except httpx.HTTPError:
                if attempt == _MAX_RETRIES:
                    raise
                time.sleep(min(2**attempt, 30))
        raise AssertionError("unreachable retry loop")

    def iter_records(self) -> Iterator[dict[str, Any]]:
        owned = self.client is None
        context = (
            httpx.Client(
                timeout=_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": "spicy-regs/0.1 court-opinion-ingest"},
            )
            if owned
            else nullcontext(self.client)
        )
        count = 0
        # One volume PDF backs up to 29 index rows. Fetching it per row would
        # pull the same 5 MiB twenty-nine times off a court's web server for no
        # new bytes, so a document is fetched once per run and reused.
        documents: dict[str, tuple[bytes, str | None, str | None]] = {}
        with context as client:
            assert client is not None
            for year in self.term_years:
                index_url = TERM_INDEX_URL.format(
                    term_code=term_code(year)
                )
                index_response = self._get(client, index_url)
                records = parse_term_index(
                    index_response.text,
                    term_year=year,
                )
                logger.info(
                    "Supreme Court: term {} index has {} opinions across {} documents",
                    year,
                    len(records),
                    len({record["source_url"] for record in records}),
                )
                for record in records:
                    url = record["source_url"]
                    if url not in documents:
                        try:
                            fetched = self._fetch_document(client, url)
                        except _MissingDocument:
                            self.missing_documents[url] = (
                                self.missing_documents.get(url, 0) + 1
                            )
                            continue
                        documents[url] = fetched
                    elif url in self.missing_documents:
                        self.missing_documents[url] += 1
                        continue
                    content, etag, last_modified = documents[url]
                    yield {
                        **record,
                        "source_bytes": content,
                        "etag": etag,
                        "last_modified": last_modified,
                    }
                    count += 1
                    if (
                        self.max_records is not None
                        and count >= self.max_records
                    ):
                        return
        if self.missing_documents:
            logger.warning(
                "Supreme Court: {} index rows skipped — {} documents 404 upstream: {}",
                sum(self.missing_documents.values()),
                len(self.missing_documents),
                ", ".join(sorted(self.missing_documents)),
            )

    def _fetch_document(
        self, client: httpx.Client, url: str
    ) -> tuple[bytes, str | None, str | None]:
        """Fetch one opinion or volume PDF, or report it missing upstream."""
        try:
            response = self._get(client, url)
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                if self.skip_missing_documents:
                    logger.warning("Supreme Court: document 404 upstream: {}", url)
                    raise _MissingDocument(url) from exc
                raise ValueError(
                    f"Supreme Court opinion document is missing upstream: {url}"
                ) from exc
            raise
        media_type = (
            response.headers.get("content-type", "")
            .split(";", 1)[0]
            .strip()
            .casefold()
        )
        if media_type not in {"application/pdf", "application/octet-stream"}:
            raise ValueError(
                "Supreme Court opinion response is not a PDF: "
                f"{url} ({media_type or 'missing'})"
            )
        return (
            response.content,
            response.headers.get("etag"),
            response.headers.get("last-modified"),
        )
