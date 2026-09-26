"""Normalize SpicyDocs' Unified Agenda observations for the SpicyRegs table.

SpicyDocs fetches and checks each edition's XML. This adapter retains the
published table's field choices and whitespace rules. A failed edition raises
before any of its rows are yielded; earlier completed editions remain yielded.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    from spicy_docs.sources.unified_agenda.records import UnifiedAgendaField, UnifiedAgendaRecordObservation

#: The newest edition when this was written; ``newest_edition`` moves past it.
DEFAULT_EDITION = "202510"

#: The publisher's page naming each edition's XML file. It is the publisher's own
#: statement of what exists: an unpublished edition's export answers 200 with an
#: HTML page, so the export cannot be probed. The page omits Spring 2018 (201804),
#: which the export still serves, so it moves the newest edition, never the series.
EDITION_REPORT_URL = "https://www.reginfo.gov/public/do/eAgendaXmlReport"
_LISTED_EDITION = re.compile(r"REGINFO_RIN_DATA_((?:19|20)[0-9]{2}(?:04|10))\.xml")


def newest_edition(*, transport: httpx.BaseTransport | None = None) -> str:
    """The newest edition the publisher's report page names, never older than ``DEFAULT_EDITION``.

    A page that cannot be read, or names no edition, keeps ``DEFAULT_EDITION`` and says so in the
    run log: the run still refreshes the edition it knows.
    """
    try:
        with httpx.Client(transport=transport, timeout=60, follow_redirects=True) as client:
            response = client.get(EDITION_REPORT_URL)
            response.raise_for_status()
    except httpx.HTTPError as error:
        logger.warning("Unified Agenda: edition report unreadable ({}); newest edition stays {}", error, DEFAULT_EDITION)
        return DEFAULT_EDITION
    listed = _LISTED_EDITION.findall(response.text)
    if not listed:
        logger.warning("Unified Agenda: edition report names no edition; newest edition stays {}", DEFAULT_EDITION)
        return DEFAULT_EDITION
    return max(DEFAULT_EDITION, *listed)


class UnifiedAgendaReader(Reader):
    """Read explicit editions; the default selects the held Fall 2025 edition."""

    def __init__(
        self,
        *,
        editions: tuple[str, ...] | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.editions = editions or (DEFAULT_EDITION,)
        self.verbose = verbose
        self.transport = transport

    def iter_records(self) -> Iterator[dict]:
        # Base CLI/MCP installations can import source names without the optional
        # owner wheel. Fetching an edition requires the source-readers extra.
        try:
            from spicy_docs.sources.unified_agenda import (
                UnifiedAgendaAcquirer,
                UnifiedAgendaBudget,
                UnifiedAgendaEdition,
            )
            from spicy_docs.sources.unified_agenda.records import scan_unified_agenda_records
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "Unified Agenda requires spicy-regs[source-readers]. "
                    "Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise

        budget = UnifiedAgendaBudget(
            max_requests=5,
            max_bytes=64 * 1024**2,
            timeout_seconds=300,
            min_request_interval_seconds=1,
        )
        with UnifiedAgendaAcquirer(budget=budget, transport=self.transport) as source:
            for edition in self.editions:
                acquired = source.acquire_edition(UnifiedAgendaEdition(edition))
                # The edition every record states: the off-pattern ``2012`` file is Fall 2012 (201210).
                publication = acquired.edition.publication_id
                rows: list[dict] = []
                scan_unified_agenda_records(
                    acquired.capture.body, on_record=lambda record: rows.append(_normalize(record, publication))
                )
                logger.info("Unified Agenda: edition {} yielded {:,} records", edition, len(rows))
                yield from rows


def _find(fields: Sequence[UnifiedAgendaField], *path: str) -> UnifiedAgendaField | None:
    # ElementTree.find("parent/child") skips parents without a matching child.
    for field in fields:
        if field.element.tag == path[0]:
            if len(path) == 1:
                return field
            match = _find(field.children, *path[1:])
            if match is not None:
                return match
    return None


def _text(fields: Sequence[UnifiedAgendaField], *path: str) -> str | None:
    field = _find(fields, *path)
    value = field.leading_text.strip() if field is not None else ""
    return value or None


def _items(fields: Sequence[UnifiedAgendaField], container: str, name: str) -> Iterator[UnifiedAgendaField]:
    # ElementTree.findall("container/item") read every matching container in
    # the old adapter. Repeated source containers must keep that ordering.
    for field in fields:
        if field.element.tag == container:
            yield from (child for child in field.children if child.element.tag == name)


def _normalize(record: UnifiedAgendaRecordObservation, edition: str) -> dict:
    """Shape one observed record into the published row, preferring agency ``ACRONYM`` over ``CODE``."""
    fields = record.fields
    agency = _find(fields, "AGENCY")
    agency_fields = agency.children if agency is not None else ()
    timetable = [
        {
            "action": _text(entry.children, "TTBL_ACTION"),
            "date": _text(entry.children, "TTBL_DATE"),
            "fr_citation": _text(entry.children, "FR_CITATION"),
        }
        for entry in _items(fields, "TIMETABLE_LIST", "TIMETABLE")
    ]
    return {
        "rin": _text(fields, "RIN"),
        "agency_code": _text(agency_fields, "ACRONYM") or _text(agency_fields, "CODE"),
        "agency_name": _text(agency_fields, "NAME"),
        "title": _text(fields, "RULE_TITLE"),
        "abstract": _text(fields, "ABSTRACT"),
        "priority_category": _text(fields, "PRIORITY_CATEGORY"),
        "rin_status": _text(fields, "RIN_STATUS"),
        "rule_stage": _text(fields, "RULE_STAGE"),
        "major": _text(fields, "MAJOR"),
        "publication_id": _text(fields, "PUBLICATION", "PUBLICATION_ID"),
        "agenda_edition": edition,
        "cfr_references": [text for field in _items(fields, "CFR_LIST", "CFR") if (text := field.leading_text.strip())],
        "legal_authority": [
            text
            for field in _items(fields, "LEGAL_AUTHORITY_LIST", "LEGAL_AUTHORITY")
            if (text := field.leading_text.strip())
        ],
        "timetable": timetable,
    }
