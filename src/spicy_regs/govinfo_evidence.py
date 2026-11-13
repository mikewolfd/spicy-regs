"""Query selected GovInfo preservation and budget originals from pinned capture receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from spicy_docs.reading.json_input import load_decimal_json
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonUnavailableError
from spicy_docs.sources.govinfo.bodies import parse_granule_identity
from spicy_docs.sources.govinfo.budget_tables import budget_table_offers, read_budget_table
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, package_granules_url
from spicy_docs.sources.govinfo.premis import PREMIS_NAMESPACE
from spicy_docs.sources.govinfo.preservation import GovInfoPreservation, read_package_preservation
from spicy_docs.storage.blobs import iter_verified_blob
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.govinfo_revisions import revision_instant

_METADATA_LIMIT = 4 * 1024**2
_FILE_LIMIT = 16 * 1024**2
_JOURNAL_LIMIT = 8 * 1024**2


@dataclass(frozen=True, slots=True)
class JournalEntry:
    """A caller-pinned capture receipt and its one-based position in the journal."""

    path: Path
    sha256: str
    entry: int

    def read(self, archive: CentralRawArchiveStore, *, max_bytes: int) -> CapturedBodyResponse:
        if type(self.entry) is not int or self.entry < 1:
            raise ValueError("GovInfo capture entry must be one-based")
        with self.path.open("rb") as stream:
            journal = stream.read(_JOURNAL_LIMIT + 1)
        if len(journal) > _JOURNAL_LIMIT or self.sha256 != "sha256:" + hashlib.sha256(journal).hexdigest():
            raise ValueError("GovInfo capture journal differs from its pinned SHA-256 or size bound")
        lines = journal.splitlines()
        if self.entry > len(lines):
            raise ValueError("GovInfo capture entry is outside its pinned journal")
        record = json.loads(lines[self.entry - 1])
        if not isinstance(record, dict) or (
            record.get("event") != "capture"
            or record.get("response_complete") is not True
            or record.get("body_retained") is not True
            or record.get("request_body") is not None
            or record.get("blob_store") != "central-raw"
            or type(record.get("byte_size")) is not int
            or not 0 <= record["byte_size"] <= max_bytes
            or type(record.get("status_code")) is not int
            or not 100 <= record["status_code"] <= 599
        ):
            raise ValueError("GovInfo receipt does not pin a complete bounded original response")
        body = b"".join(iter_verified_blob(archive, record["sha256"], record["byte_size"]))
        return CapturedBodyResponse(
            **{
                key: record[key]
                for key in (
                    "requested_url",
                    "resolved_url",
                    "status_code",
                    "content_type",
                    "observed_at",
                    "content_encoding",
                    "method",
                )
            },
            body=body,
        )


def _source(capture: CapturedBodyResponse) -> dict:
    state = (
        "retained" if capture.status_code == 200 else "unavailable" if capture.status_code in (404, 410) else "refused"
    )
    return {
        "state": state,
        "status_code": capture.status_code,
        "url": capture.resolved_url,
        "sha256": capture.sha256,
        "byte_size": capture.byte_size,
        "observed_at": capture.observed_at,
    }


def _sources(observation: GovInfoPreservation) -> dict:
    result = {}
    for name in ("summary", "mods", "premis", "closing_summary"):
        capture = getattr(observation, name + "_capture")
        result[name] = {"state": "unrequested"} if capture is None else _source(capture)
    return result


def preservation_facts(observation: GovInfoPreservation) -> dict:
    """Dates, native preservation events, files and relationships with source paths."""
    summary, mods, premis = observation.summary, observation.mods, observation.premis
    raw_summary = (
        load_decimal_json(observation.summary_capture.body, source="GovInfo SUMMARY", error_type=ValueError)
        if summary is not None and observation.summary_capture is not None
        else {}
    )
    if not isinstance(raw_summary, dict):
        raise ValueError("GovInfo SUMMARY is not an object")
    native_dates = []
    for field in ("dateIssued", "lastModified"):
        value = raw_summary.get(field)
        if summary is None:
            state = (
                "unrequested" if observation.summary_capture is None else _source(observation.summary_capture)["state"]
            )
        elif field not in raw_summary:
            state = "missing"
        elif value is None:
            state = "null"
        elif value == "":
            state = "empty"
        elif not isinstance(value, str) or (field == "lastModified" and observation.last_modified is None):
            state = "invalid"
        else:
            state = "value"
        native_dates.append(
            {
                "field": f"SUMMARY/{field}",
                "value": value,
                "locator": f"/{field}",
                "state": state,
            }
        )
    mods_relationships = []
    if mods is not None:
        for record in (mods.package, *mods.constituents):
            for group in ("originInfo", "recordInfo", "extension"):
                for node in record.fields(group):
                    for child in node.children:
                        name = child.name.split("}")[-1]
                        if "date" in name.lower():
                            native_dates.append(
                                {
                                    "field": f"MODS/{group}/{name}",
                                    "value": child.text,
                                    "state": "empty" if child.text == "" else "value",
                                    "attributes": child.attributes,
                                    "element_path": child.path,
                                }
                            )
            mods_relationships.extend(asdict(rel.element) for rel in record.related_items)
            mods_relationships.extend(asdict(node) for node in record.fields("extension", "eventId"))
    return {
        "package_id": observation.identity.package_id,
        "collection": observation.identity.collection,
        "last_modified": observation.last_modified,
        "date_issued": summary.date_issued if summary else None,
        "summary_bracket": observation.bracket,
        "coherence": "separate publisher requests",
        "sources": _sources(observation),
        "native_dates": native_dates,
        "mods_relationships": mods_relationships,
        "preservation_objects": None if premis is None else [asdict(obj.element) for obj in premis.objects],
        "preservation_events": None
        if premis is None
        else [asdict(event) for event in premis.element.findall(f"{{{PREMIS_NAMESPACE}}}event")],
    }


def budget_granules(package_id: str, pages: tuple[CapturedBodyResponse, ...], *, page_size: int) -> dict:
    """Reconcile exact retained publisher pages offline, including a valid-empty traversal."""
    if not pages:
        return {"status": "unrequested", "pages": [], "granules": []}
    if len(pages) > 20:
        raise ValueError("Select at most 20 budget inventory pages")
    if any(page.byte_size > _METADATA_LIMIT for page in pages):
        raise ValueError("GovInfo granule response exceeds the metadata byte bound")
    first = package_granules_url(package_id, page_size=page_size)
    index = 0
    url_path = urlsplit(first).path
    observed = tuple(revision_instant(page.observed_at) for page in pages)
    if any(a > b for a, b in zip(observed, observed[1:])):
        raise ValueError("GovInfo granule captures are out of request order")

    def replay(request: httpx.Request) -> httpx.Response:
        nonlocal index
        if index == len(pages):
            raise ValueError("GovInfo granule traversal needs an unretained page")
        capture = pages[index]
        index += 1
        if (
            capture.requested_url != str(request.url)
            or capture.resolved_url != capture.requested_url
            or urlsplit(capture.resolved_url).path != url_path
            or capture.method != "GET"
            or capture.content_encoding != "identity"
            or capture.request_body is not None
        ):
            raise ValueError("GovInfo granule capture differs from the requested package or page")
        return httpx.Response(
            capture.status_code,
            headers={"Content-Type": capture.content_type or ""},
            stream=httpx.ByteStream(capture.body),
            extensions={"spicy_docs_retained_observed_at": capture.observed_at},
        )

    result = []
    refusal = None
    try:
        with GovInfoDiscoveryReader(
            budget=PagedJsonBudget(len(pages), _METADATA_LIMIT, 5, 0),
            api_key="offline-no-credential",
            transport=httpx.MockTransport(replay),
        ) as reader:
            result.extend(reader.granules(first, max_pages=len(pages)))
    except PagedJsonUnavailableError:
        refusal = _source(pages[index - 1]) | {"page": index, "declared_count": None}
    if index != len(pages):
        raise ValueError("Unrequested pages cannot enter a complete GovInfo granule traversal")
    granules = [record for page in result for record in page.records]
    for record in granules:
        parse_granule_identity(package_id, record.get("granuleId"))
        if "packageId" in record and record["packageId"] != package_id:
            raise ValueError("GovInfo granule listing states a different package")
    witnesses = [
        _source(capture) | {"page": page.page_index + 1, "declared_count": page.declared_count}
        for page, capture in zip(result, pages, strict=refusal is None)
    ]
    if refusal is not None:
        witnesses.append(refusal)
    return {
        "status": "complete" if refusal is None else "unavailable",
        "pages": witnesses,
        "granules": granules,
    }


def budget_facts(
    observation: GovInfoPreservation,
    *,
    pages: tuple[CapturedBodyResponse, ...],
    page_size: int = 100,
    file: CapturedBodyResponse | None = None,
    granule_id: str | None = None,
    sheets: tuple[str, ...] = (),
) -> dict:
    """Locate one exact file by package, optional granule and publisher URL."""
    mods = observation.mods_identity
    if mods is None:
        raise ValueError("Budget inventory requires validated MODS")
    package_id = observation.identity.package_id
    inventory = budget_granules(package_id, pages, page_size=page_size)
    offers = budget_table_offers(observation)
    fiscal_year = observation.identity.fiscal_year
    if mods.fiscal_year is not None and mods.fiscal_year != fiscal_year:
        raise ValueError("Budget MODS fiscal year differs from the package identity")
    result: dict[str, Any] = {
        "package_id": package_id,
        "last_modified": observation.last_modified,
        "summary_bracket": observation.bracket,
        "fiscal_year": fiscal_year,
        "mods_fiscal_year": mods.fiscal_year,
        "root_document_formats": mods.offered_formats,
        "granule_inventory": inventory,
        "native_files": [],
    }
    selected = []
    for offer in offers:
        same_file = file is not None and offer.url == file.requested_url and offer.granule_id == granule_id
        if same_file:
            selected.append(offer)
        result["native_files"].append(
            {
                **asdict(offer),
                "status": (_source(file)["state"] if same_file and file is not None else "advertised-unrequested"),
            }
        )
    if file is not None:
        if not selected:
            raise ValueError("Budget file does not identify a publisher-stated package/granule offer")
        if len(selected) != 1:
            raise ValueError("Budget file matches more than one publisher-stated package/granule offer")
        if (
            granule_id is not None
            and inventory["status"] == "complete"
            and not any(record["granuleId"] == granule_id for record in inventory["granules"])
        ):
            raise ValueError("Selected budget granule differs from the complete publisher listing")
        (offer,) = selected
        comparison = observation.compare_file(file)
        inside_bracket = observation.brackets(file)
        result["file"] = {
            **_source(file),
            "comparison": comparison,
            "observed_within_summary_bracket": inside_bracket,
        }
        result["table_status"] = result["file"]["state"]
        if file.status_code == 200:
            if observation.last_modified is None or not inside_bracket:
                result["table_status"] = "revision-unproved"
                return result
            if comparison["status"] != "consistent":
                result["table_status"] = "fixity-unproved"
                return result
            if offer.format == "xls":
                result["table_status"] = "unsupported-format"
                return result
            cells = read_budget_table(offer, file, sheets=sheets)
            result["table"] = (
                {
                    sheet: {address: asdict(cell) for address, cell in values.cells.items()}
                    for sheet, values in cells.items()
                }
                if isinstance(cells, dict)
                else cells
            )
            if isinstance(cells, dict):
                result["merged_ranges"] = {sheet: value.merged_ranges for sheet, value in cells.items()}
            result["table_status"] = "read"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package_id")
    parser.add_argument("selection", type=Path, help="JSON object of named, pinned capture journal entries")
    parser.add_argument("--archive-root", type=Path, default=default_raw_archive_root())
    parser.add_argument("--budget", action="store_true")
    parser.add_argument("--sheet", action="append", default=[])
    parser.add_argument("--granule-id")
    parser.add_argument("--page-size", type=int, default=100)
    args = parser.parse_args()
    archive = CentralRawArchiveStore(args.archive_root, create=False)
    with args.selection.open("rb") as stream:
        raw = stream.read(_METADATA_LIMIT + 1)
    if len(raw) > _METADATA_LIMIT:
        raise ValueError("GovInfo selection exceeds the metadata byte bound")
    selection = json.loads(raw)
    if not isinstance(selection, dict):
        raise ValueError("GovInfo selection must be an object of pinned receipts")

    def pin(spec: dict) -> JournalEntry:
        return JournalEntry(Path(spec["path"]), spec["sha256"], spec["entry"])

    def capture(name: str, limit: int = _METADATA_LIMIT) -> CapturedBodyResponse | None:
        return pin(selection[name]).read(archive, max_bytes=limit) if name in selection else None

    observation = read_package_preservation(
        args.package_id,
        summary=capture("summary"),
        mods=capture("mods"),
        premis=capture("premis"),
        closing_summary=capture("closing_summary"),
    )
    result = preservation_facts(observation)
    file = capture("file", _FILE_LIMIT)
    if args.budget:
        specs = selection.get("granule_pages", [])
        if len(specs) > 20:
            raise ValueError("Select at most 20 budget inventory pages")
        pages = tuple(pin(spec).read(archive, max_bytes=_METADATA_LIMIT) for spec in specs)
        result["budget"] = budget_facts(
            observation,
            pages=pages,
            page_size=args.page_size,
            file=file,
            granule_id=args.granule_id,
            sheets=tuple(args.sheet),
        )
    elif file is not None:
        result["file_comparison"] = observation.compare_file(file)
        result["file_observed_within_summary_bracket"] = observation.brackets(file)
    if "historical_summary_sha256" in selection or "historical_mods_sha256" in selection:
        if "historical_summary_sha256" not in selection or "historical_mods_sha256" not in selection:
            raise ValueError("Select both historical SUMMARY and MODS digests")
        result["historical_metadata"] = observation.historical_metadata(
            summary_sha256=selection["historical_summary_sha256"],
            mods_sha256=selection["historical_mods_sha256"],
        )
    result["capture_receipts"] = selection
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
