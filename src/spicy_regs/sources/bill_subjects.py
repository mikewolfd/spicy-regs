"""Reader-side fetcher for one bill's CRS policy area + legislative subjects.

``congress_bills.parquet`` is built list-level only: the ``/bill`` list payload
carries no subject signal at all, so the published table can say *what* a bill is
but never *what it is about*. Both missing fields are detail-endpoint-only, and
this module is the per-bill fetch that
:mod:`spicy_regs.transforms.enrich_bill_subjects` walks the table with.

**Two carriers, one vocabulary.** The Library of Congress assigns each bill one
``policyArea`` (a ~33-term controlled list) and any number of ``legislative
subjects`` (a large controlled vocabulary). Two publishers serve those same
assignments, and the reader picks whichever the environment can actually reach:

``congress-api``
    ``GET /v3/bill/{congress}/{type}/{number}/subjects`` returns **both** fields
    in one response (``subjects.policyArea.name`` and
    ``subjects.legislativeSubjects[].name``), so a bill costs one request, not
    two — the ``/bill/{congress}/{type}/{number}` detail endpoint carries only
    the policy area and is not needed. Coverage runs back to the 93rd Congress.
    Requires an api.data.gov key, resolved through the same fallback chain as
    :mod:`spicy_regs.sources.congress_bills` and sent, like there, only as the
    ``X-Api-Key`` header, never in the URL. Pagination defaults to 20 subjects
    per page, so the request asks for the 250-item maximum and walks ``offset``
    for the rare bill that carries more.

``govinfo-billstatus``
    SpicyDocs acquires bounded BILLSTATUS XML and validates the requested bill
    identity. This consumer cleans and deduplicates the returned policy area
    and subjects for its existing table. No API key is needed. The transform's
    configured coverage floor remains the 108th Congress (2003).

Without a key the keyless carrier is chosen automatically, so a CI run is never
a no-op — it just enriches a shallower slice of the archive.

**Three outcomes, not two.** :meth:`BillSubjectsFetcher.subjects_for` returns a
:class:`BillSubjects` when the carrier *answered* (including an answer of "no
assignments"), and ``None`` when it did not (timeout, 5xx, exhausted retries).
The transform writes a row only for an answer, so a network wobble leaves the
bill un-enriched for the next run instead of pinning an empty answer to it.
Malformed or wrong-bill XML also leaves no row; credential refusal (401/403)
from either carrier aborts the run on the first answer, without retry. The
subject table retains carrier/time, not captured XML bytes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx
from loguru import logger
from spicy_regs.sources.congress_bills import API_BASE, API_KEY_ENV_VARS, _resolve_api_key

if TYPE_CHECKING:
    from spicy_docs.sources.congress.bill_acquisition import BillAcquirer

#: Carrier names, stored verbatim in ``bill_subjects.carrier`` so a reader can
#: tell which publisher supplied a row (and the transform can re-ask a bill the
#: *other* carrier had nothing for).
CARRIER_API = "congress-api"
CARRIER_BULKDATA = "govinfo-billstatus"

#: Earliest Congress each carrier answers for. GPO's bulk-data repository starts
#: at the 108th; the Congress.gov API's subject assignments start at the 93rd,
#: when CRS began indexing legislation.
FIRST_CONGRESS = {CARRIER_API: 93, CARRIER_BULKDATA: 108}

#: An honest identifying User-Agent on every request, per the repo's other
#: scraped sources (``uscode_olrc``, ``gao_reports``, ``courtlistener_bulk``).
_USER_AGENT = "spicy-regs-etl/1.0 (+https://github.com/civictechdc/spicy-regs)"

# Transport hygiene, matching ``sources.congress_bills``: bound every request and
# retry transient failures with backoff.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 4

#: Max subjects the API returns per page (the ``limit`` ceiling), and how many
#: pages to walk before settling for what we have. 4 x 250 = 1,000 subjects is
#: far past the fattest bill observed; the bound just stops a runaway loop.
_SUBJECTS_PER_PAGE = 250
_MAX_SUBJECT_PAGES = 4

#: Requests an hour Congress.gov documents for a keyed client. It is a stated
#: budget, not a guess, and every attempt spends from it — retries included.
API_HOURLY_BUDGET = 5_000

#: Seconds to wait between requests, per carrier. The API carrier crawls at
#: 1.33 requests a second, comfortably under the 1.39/s the hourly budget
#: allows, so a run of retries cannot walk the pipeline into 429s. GPO's bulk
#: data publishes no budget at all; 0.15s is a deliberate crawl rather than
#: whatever the pipe happens to allow.
DELAY_SECONDS = {CARRIER_API: 0.75, CARRIER_BULKDATA: 0.15}


class _Absent:
    """Sentinel: the carrier answered, and does not hold this bill."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<absent>"


_ABSENT = _Absent()


@dataclass(frozen=True)
class BillSubjects:
    """One bill's subject assignment, as a carrier actually answered it.

    An answer of ``policy_area=None, subjects=()`` is a real answer, and it has
    two distinguishable causes: the carrier holds the bill and no terms were
    assigned to it (``held=True``), or the carrier has no record of the bill at
    all (``held=False`` — a 404). Both publish the same null, but a coverage
    number that cannot tell them apart is a coverage number nobody can read, so
    the run report counts them separately. A carrier that never answered is
    ``None`` from :meth:`BillSubjectsFetcher.subjects_for` instead, and is left
    for the next run.
    """

    policy_area: str | None
    subjects: tuple[str, ...]
    carrier: str
    held: bool = True


@dataclass
class FetchCounts:
    """Per-run tally of how each fetch landed. Reported by the transform."""

    answered: int = 0
    with_policy_area: int = 0
    subjects_only: int = 0
    unassigned: int = 0
    not_held: int = 0
    failed: int = 0
    policy_areas: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, int]:
        return {
            "answered": self.answered,
            "with_policy_area": self.with_policy_area,
            "subjects_only": self.subjects_only,
            "unassigned": self.unassigned,
            "not_held": self.not_held,
            "failed": self.failed,
        }


class BillSubjectsFetcher:
    """Fetches one bill's policy area + legislative subjects from either carrier.

    Build one per run and reuse it: the HTTP client is opened lazily on the
    first fetch and held for connection reuse, and the run tallies accumulate on
    :attr:`counts`. Not shared-safe across threads. An unknown carrier name is
    refused at construction rather than falling through to the BILLSTATUS path.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        carrier: str | None = None,
        delay: float | None = None,
        client: httpx.Client | None = None,
        bill_acquirer: BillAcquirer | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.carrier = carrier or (CARRIER_API if self.api_key else CARRIER_BULKDATA)
        if self.carrier not in (CARRIER_API, CARRIER_BULKDATA):
            raise ValueError(f"unknown carrier {self.carrier!r}; expected {CARRIER_API!r} or {CARRIER_BULKDATA!r}")
        if self.carrier == CARRIER_API and not self.api_key:
            raise ValueError(
                f"carrier {CARRIER_API!r} needs an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})"
            )
        self.delay = DELAY_SECONDS[self.carrier] if delay is None else delay
        if self.carrier == CARRIER_BULKDATA and client is not None:
            raise ValueError("Use bill_acquirer to inject the BILLSTATUS client")
        if self.carrier == CARRIER_API and bill_acquirer is not None:
            raise ValueError("bill_acquirer is only valid for the BILLSTATUS carrier")
        self.counts = FetchCounts()
        self._client = client
        self._owns_client = client is None
        self._bill_acquirer = bill_acquirer
        self._owns_bill_acquirer = bill_acquirer is None

    @property
    def first_congress(self) -> int:
        """Earliest Congress this carrier can answer for."""
        return FIRST_CONGRESS[self.carrier]

    def __enter__(self) -> BillSubjectsFetcher:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None and self._owns_client:
            self._client.close()
            self._client = None
        if self._bill_acquirer is not None and self._owns_bill_acquirer:
            self._bill_acquirer.close()
            self._bill_acquirer = None

    # -- fetch ---------------------------------------------------------------

    def subjects_for(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None:
        """Return this bill's assignment, or None if the carrier never answered."""
        if self.carrier == CARRIER_API:
            result = self._from_api(congress, bill_type, bill_number)
        else:
            result = self._from_bulkdata(congress, bill_type, bill_number)
        self._tally(result)
        return result

    def _tally(self, result: BillSubjects | None) -> None:
        """Sort one answer into exactly one bucket, so the four always sum."""
        if result is None:
            self.counts.failed += 1
            return
        self.counts.answered += 1
        if result.policy_area:
            self.counts.with_policy_area += 1
            self.counts.policy_areas[result.policy_area] = self.counts.policy_areas.get(result.policy_area, 0) + 1
        elif result.subjects:
            self.counts.subjects_only += 1
        elif result.held:
            self.counts.unassigned += 1
        else:
            self.counts.not_held += 1

    def _from_api(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None:
        """One ``/subjects`` call (plus offset pages) for both fields."""
        url = f"{API_BASE}/bill/{congress}/{str(bill_type).lower()}/{bill_number}/subjects"
        policy_area: str | None = None
        names: list[str] = []
        for page in range(_MAX_SUBJECT_PAGES):
            params = {
                "offset": page * _SUBJECTS_PER_PAGE,
                "limit": _SUBJECTS_PER_PAGE,
                "format": "json",
            }
            payload = self._get_json(url, params=params)
            if isinstance(payload, _Absent):
                return BillSubjects(None, (), self.carrier, held=False)
            if payload is None:
                # A later page failing must not publish a truncated subject list.
                return None
            block = payload.get("subjects") or {}
            policy_area = policy_area or _clean((block.get("policyArea") or {}).get("name"))
            names.extend(
                name
                for item in block.get("legislativeSubjects") or []
                if isinstance(item, dict) and (name := _clean(item.get("name")))
            )
            total = (payload.get("pagination") or {}).get("count")
            if not isinstance(total, int) or len(names) >= total:
                break
        return BillSubjects(policy_area, _dedup(names), self.carrier)

    def _from_bulkdata(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None:
        """Use the shared source operation; keep table-specific cleanup here."""
        try:
            from spicy_docs.sources.congress.bill_acquisition import (
                BillAcquirer,
                BillAcquisitionBudget,
                BillSourceUnavailableError,
            )
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "BILLSTATUS acquisition requires the pinned source wheels. "
                    "Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        from spicy_docs.sources.congress.bill_status import BillIdentity, BillSourceError
        from spicy_docs.transport.credentials import CredentialRefusedError

        try:
            identity = BillIdentity(congress=int(congress), bill_type=str(bill_type).lower(), number=int(bill_number))
        except (ValueError, TypeError) as error:
            logger.warning("BILLSTATUS input identity is invalid: {}", type(error).__name__)
            return None
        if self._bill_acquirer is None:
            self._bill_acquirer = BillAcquirer(
                budget=BillAcquisitionBudget(
                    max_requests=_MAX_RETRIES,
                    max_status_bytes=24 * 1024 * 1024,
                    max_text_bytes=24 * 1024 * 1024,
                    timeout_seconds=60,
                    min_request_interval_seconds=self.delay,
                )
            )
        try:
            acquired = self._bill_acquirer.acquire_status(identity)
        except CredentialRefusedError:
            raise
        except BillSourceUnavailableError:
            return BillSubjects(None, (), self.carrier, held=False)
        except (BillSourceError, httpx.HTTPError, ConnectionError) as error:
            # Do not make malformed/wrong-bill XML look like an unassigned bill.
            logger.warning("BILLSTATUS acquisition failed for {}: {}", identity, type(error).__name__)
            return None
        policy_area = _clean(acquired.status.policy_area)
        subjects = _dedup([name for value in acquired.status.subjects if (name := _clean(value))])
        return BillSubjects(policy_area, subjects, self.carrier)

    # -- transport -----------------------------------------------------------

    def _get_json(self, url: str, *, params: dict) -> dict | _Absent | None:
        """Fetch a JSON body. A body that will not parse counts as no answer."""
        response = self._request(url, params=params)
        if response is None or isinstance(response, _Absent):
            return response
        try:
            payload = response.json()
        except ValueError as exc:
            logger.warning("Bill subjects: unparseable JSON from {}: {}", url, exc)
            return None
        return payload if isinstance(payload, dict) else None

    def _request(self, url: str, *, params: dict | None) -> httpx.Response | _Absent | None:
        """GET with bounded retries + exponential backoff.

        Returns the response, :data:`_ABSENT` on a definitive 404 (the carrier
        does not hold this bill), or ``None`` when no answer was obtained. A
        401/403 raises ``CredentialRefusedError`` on the first answer: a refused
        key is not transient, and every retry spends the hourly budget.

        The key travels only as the ``X-Api-Key`` header, and redirects are not
        followed because httpx forwards custom headers to a cross-origin
        redirect. Logs name method, host, path and status, never the exception
        text, which renders the whole request URL.
        """
        from spicy_docs.transport.credentials import ACCESS_REFUSED_STATUSES, CredentialRefusedError, refusal_message

        if self._client is None:
            self._client = httpx.Client(
                timeout=_TIMEOUT,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json, text/xml"},
            )
        target = httpx.URL(url)
        where = f"GET {target.host}{target.path}"
        key_header = {"X-Api-Key": self.api_key} if self.api_key else {}
        for attempt in range(1, _MAX_RETRIES + 1):
            if self.delay:
                time.sleep(self.delay)
            try:
                resp = self._client.get(url, params=params, headers=key_header, follow_redirects=False)
                if resp.status_code in ACCESS_REFUSED_STATUSES:
                    raise CredentialRefusedError(refusal_message("congress.gov", resp.status_code, target.path))
                if resp.status_code == 404:
                    return _ABSENT
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp
            except httpx.HTTPError as exc:
                reason = (
                    f"HTTP {exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
                )
                if attempt == _MAX_RETRIES:
                    logger.error("Bill subjects: giving up on {} after {} attempts: {}", where, attempt, reason)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Bill subjects: {} {} (attempt {}/{}), retrying in {}s",
                    where,
                    reason,
                    attempt,
                    _MAX_RETRIES,
                    backoff,
                )
                time.sleep(backoff)
        return None


def _clean(value: object) -> str | None:
    """Collapse a possibly-absent XML/JSON text node to a non-empty string."""
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedup(names: list[str]) -> tuple[str, ...]:
    """Distinct names in first-seen order (a bill can list a term twice)."""
    return tuple(dict.fromkeys(names))


def resolve_carrier(api_key: str | None = None) -> str:
    """Name the carrier a run would use, without opening a client."""
    key = api_key if api_key is not None else _resolve_api_key()
    return CARRIER_API if key else CARRIER_BULKDATA
