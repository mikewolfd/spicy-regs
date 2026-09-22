# Frozen verbatim from 78c90f9:src/spicy_regs/sources/unified_agenda.py.
# Test-only independent mapping/walk oracle; do not refactor with production.
"""Frozen pre-migration Unified Agenda reader kept as a test-only oracle.

Its reader logic is verbatim from
``78c90f9:src/spicy_regs/sources/unified_agenda.py`` (pinned by the header
comment above) and must not be refactored with production: the tests compare
the current shared reader and its normalization against it. It stream-parses
each edition's public ``REGINFO_RIN_DATA_{edition}.xml`` export from
reginfo.gov with :func:`xml.etree.ElementTree.iterparse`, yielding one
normalized RIN dict per record with exactly the keys
:func:`~spicy_regs.transforms.build_unified_agenda._shape` reads.
"""

from __future__ import annotations

import io
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# --------------------------------------------------------------------------- #
# Endpoint constants — validated live against reginfo.gov (public, no API key).
# --------------------------------------------------------------------------- #
API_BASE = "https://www.reginfo.gov/public/do"

# The per-edition Unified Agenda XML export. ``{edition}`` is YYYYMM; the file
# name pattern is REGINFO_RIN_DATA_{edition}.xml served through XMLViewFileAction.
XML_EXPORT_ENDPOINT = f"{API_BASE}/XMLViewFileAction"
XML_FILE_TEMPLATE = "REGINFO_RIN_DATA_{edition}.xml"

# Root element and the repeated per-RIN record element in the export.
_ROOT_TAG = "REGINFO_RIN_DATA"
_RECORD_TAG = "RIN_INFO"

# The most recent agenda edition to fetch when a caller does not specify one.
# Editions are ``YYYYMM`` with MM in {04 (Spring), 10 (Fall)}. Fall 2025 is the
# most recent edition validated against the live export.
DEFAULT_EDITION = "202510"

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky fetch fails slow-then-recovers rather than dropping records.
_TIMEOUT = httpx.Timeout(300.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000


class UnifiedAgendaReader(Reader):
    """Yields normalized Unified Agenda RIN dicts for one or more agenda editions.

    ``editions`` defaults to :data:`DEFAULT_EDITION`. Callers doing incremental
    runs pass only the edition(s) newer than what is already stored; the
    transform handles merging with the prior table and dedup on
    (``rin``, ``agenda_edition``).
    """

    def __init__(
        self,
        *,
        editions: tuple[str, ...] | None = None,
        verbose: bool = False,
    ) -> None:
        self.editions = editions or (DEFAULT_EDITION,)
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        logger.info("Unified Agenda: fetching editions {}", ", ".join(self.editions))
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
            self._client = client
            for edition in self.editions:
                yield from self._fetch_edition(edition)
        logger.info("Unified Agenda: yielded {:,} records", self._seen)

    # -- edition fetching ----------------------------------------------------

    def _fetch_edition(self, edition: str) -> Iterator[dict]:
        """Download one edition's XML export and stream-parse its RIN records.

        Each yielded dict is annotated with the ``agenda_edition`` it came from,
        since it is half of the (``rin``, ``agenda_edition``) dedup key.
        """
        data = self._download(edition)
        if data is None:
            return
        collected = 0

        # iterparse over the in-memory bytes; grab the root on the first event so
        # we can drop processed record shells and keep the tree from growing.
        context = ET.iterparse(io.BytesIO(data), events=("start", "end"))
        _, root = next(context)  # first ("start", <REGINFO_RIN_DATA>)
        for event, elem in context:
            if event != "end" or elem.tag != _RECORD_TAG:
                continue
            record = _normalize(elem, edition)
            collected += 1
            self._seen += 1
            if self._seen % _PROGRESS_EVERY == 0:
                logger.info("Unified Agenda: {:,} records so far...", self._seen)
            yield record
            # Free this record's subtree and the root's accumulated child shells.
            elem.clear()
            root.clear()
        logger.info("Unified Agenda: edition {} yielded {:,} records", edition, collected)

    def _download(self, edition: str) -> bytes | None:
        """GET one edition's XML file with bounded retries + exponential backoff.

        Returns the raw XML bytes, or None if every attempt failed.
        """
        assert self._client is not None
        params = {"f": XML_FILE_TEMPLATE.format(edition=edition)}
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(XML_EXPORT_ENDPOINT, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                data = resp.content
                if not data.lstrip().startswith(b"<?xml"):
                    # A non-XML body (e.g. the HTML listing page) means the edition
                    # is missing or the endpoint changed — surface it, don't parse.
                    logger.error(
                        "Unified Agenda: edition {} did not return XML (got {} bytes starting {!r})",
                        edition,
                        len(data),
                        data[:60],
                    )
                    return None
                return data
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("Unified Agenda: giving up on edition {} after {} attempts: {}", edition, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Unified Agenda: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        return None


# --------------------------------------------------------------------------- #
# XML → normalized dict. Keys here are exactly what ``_shape`` reads.
# --------------------------------------------------------------------------- #


def _text(elem: ET.Element | None, path: str) -> str | None:
    """Return the stripped text at ``path`` under ``elem``, or None if empty."""
    if elem is None:
        return None
    value = elem.findtext(path)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _text_list(elem: ET.Element, path: str) -> list[str]:
    """Return the stripped, non-empty text of every element matching ``path``."""
    out: list[str] = []
    for child in elem.findall(path):
        if child.text and child.text.strip():
            out.append(child.text.strip())
    return out


def _normalize(elem: ET.Element, edition: str) -> dict:
    """Map one ``<RIN_INFO>`` element onto the keys ``_shape`` consumes.

    ``agency_code``/``agency_name`` come from the RIN-owning ``<AGENCY>`` (its
    ``<ACRONYM>``, falling back to the numeric ``<CODE>``, and ``<NAME>``).
    """
    agency = elem.find("AGENCY")
    timetable = [
        {
            "action": _text(tt, "TTBL_ACTION"),
            "date": _text(tt, "TTBL_DATE"),
            "fr_citation": _text(tt, "FR_CITATION"),
        }
        for tt in elem.findall("TIMETABLE_LIST/TIMETABLE")
    ]
    return {
        "rin": _text(elem, "RIN"),
        "agency_code": _text(agency, "ACRONYM") or _text(agency, "CODE"),
        "agency_name": _text(agency, "NAME"),
        "title": _text(elem, "RULE_TITLE"),
        "abstract": _text(elem, "ABSTRACT"),
        "priority_category": _text(elem, "PRIORITY_CATEGORY"),
        "rin_status": _text(elem, "RIN_STATUS"),
        "rule_stage": _text(elem, "RULE_STAGE"),
        "major": _text(elem, "MAJOR"),
        "publication_id": _text(elem, "PUBLICATION/PUBLICATION_ID"),
        "agenda_edition": edition,
        "cfr_references": _text_list(elem, "CFR_LIST/CFR"),
        "legal_authority": _text_list(elem, "LEGAL_AUTHORITY_LIST/LEGAL_AUTHORITY"),
        "timetable": timetable,
    }
