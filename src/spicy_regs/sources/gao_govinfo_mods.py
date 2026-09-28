"""Each GAOREPORTS package's MODS: what a GovInfo history row gains from one keyless read.

GovInfo serves every package's MODS as a keyless static file, byte-identical to
the keyed API's (checked 2026-09-28 for GAO-08-919R), so this pass spends no
api.data.gov quota. In a sample of 205 packages read on 2026-09-28, every MODS
stated a report number and a product type, 98% an abstract and 97% subjects.
The standard ``subject/topic`` list is read, not the extension ``subject`` list:
it holds every extension term plus GAO's program and place terms. MODS names no
agency, so ``agencies_json`` stays NULL.

Reads are sequential and paced at no more than three a second through
SpicyDocs' bounded client, which retries 429, 5xx and transport failures with
capped, jittered backoff. The builder picks the rows still unread, so an
interrupted pass resumes where the published table left off.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from spicy_docs.transport.source_acquirer import SourceAcquirer

from spicy_regs.sources.gao_govinfo import GaoGovInfoError

if TYPE_CHECKING:
    import httpx
    from spicy_docs.transport.captured import CapturedBodyResponse

MODS_URL = "https://www.govinfo.gov/metadata/pkg/{}/mods.xml"
#: The sample's largest MODS was 9,645 bytes.
MODS_MAX_BYTES = 1024 * 1024
#: At most three requests a second, sequential. The sample's median response took 0.09 s.
MODS_INTERVAL_SECONDS = 0.34
#: Attempts per package: SpicyDocs retries 429, 5xx and transport failures with capped, jittered backoff.
MODS_ATTEMPTS = 5


class GaoModsUnavailableError(GaoGovInfoError):
    """GovInfo answered 404 or 410 for a package's MODS; its row stays unread."""


@dataclass(frozen=True, slots=True)
class ModsFacts:
    """What one package's MODS adds to its row; ``None`` where the MODS states nothing."""

    report_number: str | None
    product_type: str | None
    abstract: str | None
    topics: tuple[str, ...]


def _collapsed(text: str) -> str | None:
    return " ".join(text.split()) or None


def _one(values: Iterable[str], package_id: str, name: str) -> str | None:
    found = list(dict.fromkeys(value for value in map(_collapsed, values) if value))
    if len(found) > 1:
        raise GaoGovInfoError(f"GovInfo MODS for {package_id} states more than one {name}: {found}")
    return found[0] if found else None


def mods_facts(body: bytes, package_id: str, *, max_bytes: int = MODS_MAX_BYTES) -> ModsFacts:
    """Read one package's MODS, refusing a record that names another package.

    Whitespace runs in the publisher's text collapse to one space; a repeated
    topic is kept once, in the order the record states it.
    """
    from spicy_docs.sources.govinfo.mods import GovInfoModsError, parse_govinfo_mods

    try:
        record = parse_govinfo_mods(body, max_bytes=max_bytes).package
    except GovInfoModsError as error:
        raise GaoGovInfoError(f"GovInfo MODS for {package_id} is unreadable: {error}") from error
    stated = {_collapsed(element.text) for path in (("recordInfo", "recordIdentifier"), ("extension", "accessId"))
              for element in record.fields(*path)}
    if stated != {package_id}:
        raise GaoGovInfoError(f"GovInfo MODS for {package_id} identifies {sorted(map(str, stated))}")
    abstracts = [text for element in record.fields("abstract") if (text := _collapsed(element.text))]
    topics = (_collapsed(topic.text) for subject in record.subjects for topic in subject.findall(
        "{http://www.loc.gov/mods/v3}topic"))
    return ModsFacts(
        report_number=_one((element.text for element in record.fields("extension", "reportNumber")), package_id,
                           "report number"),
        product_type=_one((element.text for element in record.fields("extension", "type")), package_id, "type"),
        abstract=" ".join(abstracts) or None,
        topics=tuple(dict.fromkeys(topic for topic in topics if topic)),
    )


class GaoModsAcquirer(SourceAcquirer):
    """Keyless, sequential, paced reads of GAOREPORTS package MODS through SpicyDocs' bounded client."""

    def __init__(self, *, transport: httpx.BaseTransport | None = None) -> None:
        super().__init__(
            max_requests=MODS_ATTEMPTS,
            timeout_seconds=30.0,
            min_request_interval_seconds=MODS_INTERVAL_SECONDS,
            user_agent="spicy-regs-gao-mods/1.0",
            label="GovInfo MODS",
            error_type=GaoGovInfoError,
            context_key="gao_mods_acquisition",
            transport=transport,
            keyless=True,
        )

    def capture(self, package_id: str) -> tuple[ModsFacts, CapturedBodyResponse]:
        """One package's MODS and its capture; 404/410 raise :class:`GaoModsUnavailableError`."""
        url = MODS_URL.format(package_id)

        def parse(capture: CapturedBodyResponse, max_bytes: int) -> ModsFacts:
            if capture.resolved_url != url:
                raise GaoGovInfoError(f"GovInfo MODS for {package_id} resolved to {capture.resolved_url}")
            return mods_facts(capture.body, package_id, max_bytes=max_bytes)

        return self.capture_validated(
            url,
            media_types=("application/xml", "text/xml"),
            parse=parse,
            max_bytes=MODS_MAX_BYTES,
            unavailable=lambda _capture: GaoModsUnavailableError(f"GovInfo serves no MODS for {package_id}"),
            context={"packageId": package_id},
        )
