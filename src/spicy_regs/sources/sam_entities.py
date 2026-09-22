"""SAM.gov Entity Management API v4 acquisition adapter.

The reader yields raw ``entityData`` records; the transform owns table shaping.
``extract`` requests an asynchronous JSON file for each selected registration
year. ``partition`` subdivides date windows under a conservative page threshold
and walks each window to its declared ``totalRecords``. Neither mode establishes
a frozen publisher snapshot or registry-wide coverage outside its selection.

Missing credentials, unsuccessful requests, malformed records, unknown response
shapes and unfinished downloads or pagination raise ``SamEntitiesError``. A
recognized zero-count response remains valid. The transform must exhaust the
iterator before writing output; records yielded before a failure are partial.

Callers choose registration years and an optional ``max_records`` selection
limit. The record limit bounds emitted records, not downloaded extract bytes.
Every selected extract is parsed and checked against its declared count before
records are emitted. A single-day partition above the conservative threshold
refuses rather than publishing only the reachable records.

SAM requires a key authorized for its Entity API. The adapter retains the
existing environment fallback order, but a generic data.gov key does not itself
establish SAM access. Source links have their masked key replaced only on the
SAM API host. Request errors and logs omit credential-bearing URLs.

SpicyDocs owns the newer evidence-preserving paged SAM reader. This legacy
adapter also supports asynchronous extracts; these refusal repairs do not claim
provider migration, resumable acquisition, or a qualified initial load.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import time
import zipfile
from collections.abc import Iterator
from datetime import date
from typing import cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://api.sam.gov/entity-information/v4"

# Max records the synchronous /entities page accepts per request.
PER_PAGE = 10

# The synchronous list endpoint stops paginating around this many records for a
# single filtered query, no matter how large ``totalRecords`` is. The partitioned
# walk subdivides any window whose ``totalRecords`` exceeds this so every matching
# entity stays reachable via ``nextLink``.
PAGE_CEILING = 5_000

# Earliest plausible registrationDate year to window over for a full extract. SAM
# registrations (carried over from the CCR/DUNS era) predate the UEI transition, so
# we start comfortably early; empty early windows are cheap (one extract each).
_MIN_REGISTRATION_YEAR = 2000

# The literal placeholder SAM embeds in the extract download URL in place of the key.
_API_KEY_PLACEHOLDER = "REPLACE_WITH_API_KEY"

# Env vars checked in order for the api.data.gov key. SAM.gov needs a key that is
# specifically associated with a SAM.gov account holding the Entity API role, so
# the SAM-dedicated var is preferred first; a generic api.data.gov key (which
# works against regulations.gov / Congress.gov) is only a fallback and returns a
# bare 404 here if it isn't SAM-authorized.
API_KEY_ENV_VARS = (
    "SAM_API_KEY",
    "API_GOV",  # the shared api.data.gov key, under the name RefSpec/.env uses
    "DATA_GOV_API_KEY",
    "REGULATIONS_GOV_API_KEY",
)

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping entities.
_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# Safety cap on pages walked in a single run, independent of ``max_records`` — a
# backstop against a runaway loop on an unexpectedly large window.
_MAX_PAGES = 100_000

# Async extracts are generated server-side; the download URL may not be ready on
# the first GET. Poll it with backoff up to this many attempts before giving up.
_EXTRACT_POLL_MAX = 60
_EXTRACT_POLL_INTERVAL = 10.0


def _resolve_api_key() -> str | None:
    """Return the first api.data.gov key set in :data:`API_KEY_ENV_VARS`, or None."""
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    return None


class SamEntitiesReader(Reader):
    """Yields raw SAM.gov entity dicts (the ``entityData[]`` records).

    ``mode`` selects the ingest mechanism: ``"extract"`` (default) uses SAM's bulk
    async extract (``format=json``) — one request per ``registrationDate`` window,
    up to 1M records each, well past the 5K synchronous ceiling; ``"partition"``
    walks the paginated endpoint, adaptively subdividing windows so each stays under
    that ceiling. ``max_records`` bounds a run; ``since_year``/``until_year`` bound
    the window range so scheduled runs can advance coverage. Missing access and
    incomplete source responses raise ``SamEntitiesError``.
    """

    def __init__(
        self,
        *,
        mode: str = "extract",
        registration_status: str = "A",
        since_year: int | None = None,
        until_year: int | None = None,
        year_windows: bool = True,
        max_records: int | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
    ) -> None:
        if mode not in ("extract", "partition"):
            raise ValueError(f"mode must be 'extract' or 'partition', got {mode!r}")
        for name, value in (("since_year", since_year), ("until_year", until_year)):
            if value is not None and (type(value) is not int or not 1 <= value <= 9999):
                raise ValueError(f"{name} must be a valid calendar year")
        if (since_year or _MIN_REGISTRATION_YEAR) > (until_year or date.today().year):
            raise ValueError("since_year must not exceed until_year")
        if max_records is not None and (type(max_records) is not int or max_records <= 0):
            raise ValueError("max_records must be a positive integer or None")
        if type(per_page) is not int or per_page <= 0:
            raise ValueError("per_page must be a positive integer")
        self.mode = mode
        self.registration_status = registration_status
        self.since_year = since_year
        self.until_year = until_year
        self.year_windows = year_windows
        self.max_records = max_records
        self.per_page = min(per_page, PER_PAGE)
        self.api_key = api_key or _resolve_api_key()
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        self._seen = 0
        if not self.api_key or not self.api_key.strip():
            raise SamEntitiesError("SAM entities require a SAM-authorized API key; set SAM_API_KEY")
        logger.info(
            "SAM entities: mode={} registrationStatus={} years={}..{} (max_records={})",
            self.mode,
            self.registration_status,
            self.since_year if self.since_year is not None else "min",
            self.until_year if self.until_year is not None else "now",
            self.max_records if self.max_records is not None else "all",
        )
        # follow_redirects: extract download URLs commonly 302 to a signed blob URL.
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}, follow_redirects=True) as client:
            self._client = client
            if self.mode == "extract":
                yield from self._iter_extract()
            else:
                yield from self._iter_partitioned()
        logger.info("SAM entities: yielded {:,} entities", self._seen)

    # -- shared helpers ------------------------------------------------------

    def _year_range(self) -> tuple[int, int]:
        """Inclusive [since, until] year range to window over."""
        since = self.since_year if self.since_year is not None else _MIN_REGISTRATION_YEAR
        until = self.until_year if self.until_year is not None else date.today().year
        return since, until

    def _budget_left(self) -> bool:
        return self.max_records is None or self._seen < self.max_records

    def _emit(self, record: dict) -> Iterator[dict]:
        """Yield one record honouring ``max_records`` and progress logging."""
        if not self._budget_left():
            return
        _validate_entity(record)
        self._seen += 1
        if self._seen % _PROGRESS_EVERY == 0:
            logger.info("SAM entities: {:,} entities so far...", self._seen)
        yield record

    def _base_params(self) -> dict[str, object]:
        params: dict[str, object] = {"api_key": self.api_key}
        if self.registration_status:
            params["registrationStatus"] = self.registration_status
        return params

    # -- A. bulk extract -----------------------------------------------------

    def _iter_extract(self) -> Iterator[dict]:
        """Fetch the registry via SAM's async extract, one window per request.

        By default windows over ``registrationDate`` years so no single downloaded
        file is unbounded in memory; ``year_windows=False`` requests a single extract
        for the whole ``registrationStatus`` filter.
        """
        if not self.year_windows:
            yield from self._extract_window(None)
            return
        since, until = self._year_range()
        for year in range(since, until + 1):
            if not self._budget_left():
                return
            if self.verbose:
                logger.debug("SAM entities: extract window year={}", year)
            yield from self._extract_window(year)

    def _extract_window(self, year: int | None) -> Iterator[dict]:
        """Request + download one extract (optionally scoped to a registration year)."""
        params = self._base_params()
        params["format"] = "json"
        if year is not None:
            params["registrationDate"] = _year_range_literal(year)
        trigger = self._get(f"{API_BASE}/entities", params)
        total = _total_records(trigger)
        download_url = _find_download_url(trigger)
        if download_url:
            records = list(self._download_extract(download_url))
        else:
            records = _entity_data(trigger)
        if len(records) != total:
            raise SamEntitiesError("SAM extract record count differs from totalRecords")
        if len({_validate_entity(record) for record in records}) != total:
            raise SamEntitiesError("SAM extract repeats an entity identifier")
        for record in records:
            if not self._budget_left():
                return
            yield from self._emit(record)

    def _download_extract(self, download_url: str) -> Iterator[dict]:
        """Download an extract file and yield its entity records (defensive parse)."""
        assert self._client is not None
        url = self._reinject_key(download_url)
        for attempt in range(1, _EXTRACT_POLL_MAX + 1):
            try:
                resp = self._client.get(url)
            except httpx.HTTPError:
                if attempt == _EXTRACT_POLL_MAX:
                    raise SamEntitiesError("SAM extract transport retries exhausted") from None
                logger.warning("SAM entities: extract transport failed (attempt {})", attempt)
                time.sleep(_EXTRACT_POLL_INTERVAL)
                continue
            # The file may still be generating: SAM answers 202/404 until ready.
            if resp.status_code in (202, 404, 429) or resp.status_code >= 500:
                if attempt == _EXTRACT_POLL_MAX:
                    raise SamEntitiesError("SAM extract did not finish within its poll budget")
                time.sleep(_EXTRACT_POLL_INTERVAL)
                continue
            if resp.status_code != 200:
                raise SamEntitiesError(f"SAM extract refused with HTTP {resp.status_code}")
            yield from _parse_extract_bytes(resp.content)
            return

    # -- B. partitioned walk -------------------------------------------------

    def _iter_partitioned(self) -> Iterator[dict]:
        """Adaptive ``registrationDate`` window recursion over the paginated endpoint."""
        since, until = self._year_range()
        yield from self._fetch_window(date(since, 1, 1), date(until, 12, 31))

    def _fetch_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Walk one registration-date window, subdividing when it exceeds the ceiling.

        We first read the window's ``totalRecords``; if it is above
        :data:`PAGE_CEILING` the paginated walk cannot reach every record, so we halve
        the window and recurse (down to a single day). Otherwise we page it fully.
        """
        if not self._budget_left():
            return
        total = self._window_total(gte, lte)
        if total > PAGE_CEILING and gte < lte:
            mid = gte + (lte - gte) // 2
            if self.verbose:
                logger.debug(
                    "SAM entities: window {}..{} has {} > {}, splitting at {}", gte, lte, total, PAGE_CEILING, mid
                )
            yield from self._fetch_window(gte, mid)
            yield from self._fetch_window(_next_day(mid), lte)
            return
        if total > PAGE_CEILING:
            raise SamEntitiesError("SAM single-day selection exceeds the reachable page limit; use extract mode")
        yield from self._page_window(gte, lte)

    def _window_total(self, gte: date, lte: date) -> int:
        """Return ``totalRecords`` for a registration-date window (a cheap size=1 probe)."""
        params = self._base_params()
        params["registrationDate"] = _range_literal(gte, lte)
        params["page"] = 0
        params["size"] = 1
        payload = self._get(f"{API_BASE}/entities", params)
        records = _entity_data(payload)
        total = _total_records(payload)
        if len(records) > total:
            raise SamEntitiesError("SAM size probe contains more records than totalRecords")
        return total

    def _page_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Follow ``links.nextLink`` through one window until exhausted or the ceiling."""
        url = f"{API_BASE}/entities"
        params: dict[str, object] | None = self._base_params()
        assert params is not None
        params["registrationDate"] = _range_literal(gte, lte)
        params["page"] = 0
        params["size"] = self.per_page

        expected = None
        observed = 0
        seen_ids: set[str] = set()
        seen_links: set[str] = set()
        for _ in range(_MAX_PAGES):
            if not self._budget_left():
                return
            payload = self._get(url, params)
            total = _total_records(payload)
            records = _entity_data(payload)
            if expected is None:
                expected = total
            if total != expected:
                raise SamEntitiesError("SAM totalRecords changed during the selected page traversal")
            observed += len(records)
            if observed > total:
                raise SamEntitiesError("SAM pages exceed totalRecords")
            for record in records:
                if not self._budget_left():
                    return
                identity = _validate_entity(record)
                if identity in seen_ids:
                    raise SamEntitiesError("SAM pages repeat an entity identifier")
                seen_ids.add(identity)
                yield from self._emit(record)
            if observed == total or not self._budget_left():
                return
            links = payload.get("links")
            next_link = links.get("nextLink") if isinstance(links, dict) else None
            if not records or not isinstance(next_link, str) or not next_link:
                raise SamEntitiesError("SAM pagination ended before totalRecords")
            if next_link in seen_links:
                raise SamEntitiesError("SAM pagination repeated its continuation")
            seen_links.add(next_link)
            url = self._reinject_key(next_link)
            params = None  # nextLink already carries page/size/filter as a query string
        else:
            raise SamEntitiesError("SAM page budget exhausted before the selected traversal completed")

    # -- transport -----------------------------------------------------------

    def _reinject_key(self, link: str) -> str:
        """Return ``link`` with the real api_key re-injected.

        SAM returns links with their ``api_key`` masked — either as a query-param
        placeholder (``nextLink``) or as the literal ``REPLACE_WITH_API_KEY`` token
        (extract download URLs). We overwrite the query param and swap the token so
        the link is usable, leaving the rest (page/size/filter/fileName) intact.
        """
        parts = urlparse(link)
        if parts.scheme != "https" or parts.hostname != "api.sam.gov" or parts.username or parts.password:
            raise SamEntitiesError("SAM response link is outside the authorized API host")
        link = link.replace(_API_KEY_PLACEHOLDER, self.api_key or "")
        parts = urlparse(link)
        query = parse_qs(parts.query, keep_blank_values=True)
        query["api_key"] = [self.api_key or ""]
        return urlunparse(parts._replace(query=urlencode(query, doseq=True)))

    def _get(self, url: str, params: dict | None) -> dict:
        """GET with bounded retries; failures never become successful empty data."""
        assert self._client is not None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                if resp.status_code != 200:
                    raise SamEntitiesError(
                        f"SAM request refused with HTTP {resp.status_code}; verify SAM-specific access"
                    )
                payload = resp.json()
                if not isinstance(payload, dict):
                    raise SamEntitiesError("SAM response must be a JSON object")
                return payload
            except (httpx.HTTPError, ValueError):
                if attempt == _MAX_RETRIES:
                    raise SamEntitiesError("SAM request retries exhausted without a valid response") from None
                backoff = min(2**attempt, 30)
                # Exceptions and URLs may contain the query credential.
                logger.warning(
                    "SAM entities: request failed (attempt {}/{}), retrying in {}s", attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        raise SamEntitiesError("SAM request retry budget exhausted")


# -- module-level pure helpers (unit-testable without a client) ---------------


class SamEntitiesError(RuntimeError):
    """The selected SAM population could not be acquired and validated."""


def _total_records(payload: object) -> int:
    """Return the payload's nonnegative integer ``totalRecords``; a reported source error refuses."""
    _refuse_source_error(payload)
    total = cast(dict[str, object], payload).get("totalRecords") if isinstance(payload, dict) else None
    if type(total) is not int or total < 0:
        raise SamEntitiesError("SAM response requires a nonnegative integer totalRecords")
    return total


def _validate_entity(record: object) -> str:
    """Return the record's nonempty ``entityRegistration.ueiSAM`` or refuse the record."""
    registration = cast(dict[str, object], record).get("entityRegistration") if isinstance(record, dict) else None
    uei = cast(dict[str, object], registration).get("ueiSAM") if isinstance(registration, dict) else None
    if not isinstance(uei, str) or not uei.strip():
        raise SamEntitiesError("SAM entity requires a nonempty entityRegistration.ueiSAM")
    return uei


def _entity_data(payload: object) -> list[dict]:
    """Return the payload's ``entityData`` list after validating every record; a missing array refuses."""
    _refuse_source_error(payload)
    records = cast(dict[str, object], payload).get("entityData") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        raise SamEntitiesError("SAM response requires an entityData array")
    for record in records:
        _validate_entity(record)
    return cast(list[dict], records)


def _refuse_source_error(payload: object) -> None:
    if isinstance(payload, dict) and any(
        cast(dict[str, object], payload).get(key) for key in ("error", "errors", "errorCode", "errorMessage")
    ):
        raise SamEntitiesError("SAM response reports a source error")


def _us_date(d: date) -> str:
    """Format a date as SAM's MM/DD/YYYY."""
    return f"{d.month:02d}/{d.day:02d}/{d.year}"


def _range_literal(gte: date, lte: date) -> str:
    """SAM ``registrationDate`` range literal ``[MM/DD/YYYY,MM/DD/YYYY]``."""
    return f"[{_us_date(gte)},{_us_date(lte)}]"


def _year_range_literal(year: int) -> str:
    """SAM ``registrationDate`` range literal spanning a whole calendar year."""
    return _range_literal(date(year, 1, 1), date(year, 12, 31))


def _next_day(d: date) -> date:
    return date.fromordinal(d.toordinal() + 1)


def _find_download_url(payload: object) -> str | None:
    """Recursively locate an extract download URL in a trigger response.

    SAM returns the async-extract download link embedded in the JSON envelope,
    carrying the literal ``REPLACE_WITH_API_KEY`` placeholder. The exact key path
    has varied across API versions, so we walk the structure and return the first
    string that looks like that download URL (placeholder preferred; otherwise any
    http(s) URL that mentions download/extract).
    """
    fallback: str | None = None

    def walk(node: object) -> str | None:
        nonlocal fallback
        if isinstance(node, str):
            if _API_KEY_PLACEHOLDER in node and node.startswith("http"):
                return node
            if (
                fallback is None
                and node.startswith("http")
                and ("download" in node.lower() or "extract" in node.lower())
            ):
                fallback = node
            return None
        if isinstance(node, dict):
            for key, value in node.items():
                if key in {"selfLink", "nextLink", "prevLink", "previousLink"}:
                    continue
                hit = walk(value)
                if hit:
                    return hit
        elif isinstance(node, list):
            for value in node:
                hit = walk(value)
                if hit:
                    return hit
        return None

    return walk(payload) or fallback


def _parse_extract_bytes(raw: bytes) -> Iterator[dict]:
    """Yield entity dicts from a downloaded extract file (defensive across formats).

    Handles gzip- and zip-compressed payloads, then parses the inner text as either
    a JSON envelope (``{"entityData": [...]}``), a bare JSON array of entities, or
    newline-delimited JSON. Malformed or unrecognized input refuses the selection.
    """
    text = _decompress_extract(raw)
    if not text.strip():
        raise SamEntitiesError("SAM extract is empty without an explicit source population")
    stripped = text.lstrip()
    if stripped[:1] in ("{", "["):
        try:
            doc = json.loads(stripped)
        except ValueError:
            yield from _iter_ndjson(text)
            return
        if isinstance(doc, dict):
            if "entityData" in doc:
                records = _entity_data(doc)
                if "totalRecords" in doc and len(records) != _total_records(doc):
                    raise SamEntitiesError("SAM extract entityData differs from totalRecords")
                yield from records
            elif _looks_like_entity(doc):
                _validate_entity(doc)
                yield doc
            else:
                raise SamEntitiesError("SAM extract has no recognized entity population")
            return
        if isinstance(doc, list):
            for record in doc:
                _validate_entity(record)
                yield record
            return
    yield from _iter_ndjson(text)


def _decompress_extract(raw: bytes) -> str:
    """Return the extract's inner text, transparently decompressing gzip/zip."""
    if raw[:2] == b"\x1f\x8b":  # gzip magic
        try:
            return gzip.decompress(raw).decode("utf-8")
        except (OSError, EOFError, UnicodeError):
            raise SamEntitiesError("SAM gzip extract is malformed") from None
    if raw[:2] == b"PK":  # zip magic
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                members = [member for member in zf.infolist() if not member.is_dir()]
                if len(members) != 1:
                    raise SamEntitiesError("SAM ZIP extract requires exactly one data member")
                return zf.read(members[0]).decode("utf-8")
        except (zipfile.BadZipFile, UnicodeError):
            raise SamEntitiesError("SAM ZIP extract is malformed") from None
    try:
        return raw.decode("utf-8")
    except UnicodeError:
        raise SamEntitiesError("SAM extract is not valid UTF-8") from None


def _iter_ndjson(text: str) -> Iterator[dict]:
    """Yield each non-blank line as a validated entity; malformed JSON refuses the extract."""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            raise SamEntitiesError("SAM extract contains malformed JSON records") from None
        _validate_entity(obj)
        yield obj


def _looks_like_entity(doc: dict) -> bool:
    return "entityRegistration" in doc or "ueiSAM" in doc
