"""Replay retained SpicyDocs pages using the shared lossless evidence encoding.

The page structure follows DocSpec's retained OCR observations. Bytes, raw model
values and measured geometry stay exact through SpicyRegs' existing evidence
codec; no pickle execution, model request or alternate extraction engine occurs.
"""

from dataclasses import asdict
from hashlib import sha256
from typing import Any

from spicy_docs.extraction.model import (
    Box,
    Observation,
    PageContent,
    PageResult,
    ProcessingIssue,
    Raster,
    TableCell,
    TableObservation,
    TextBlock,
)

from spicy_regs.etl_receipts import decode_exact_json, exact_json
from spicy_regs.scorecards.replay import ScorecardReplayError

FORMAT = "scorecard-page-observations/1"


def page_bytes(page: PageResult) -> bytes:
    return exact_json(dict(format_version=FORMAT, page=asdict(page))).encode()


def _fields(value: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return {**value, **changes}


def _box(value) -> Box | None:
    return None if value is None else Box(**value)


def _block(value) -> TextBlock:
    return TextBlock(**_fields(value, box=_box(value["box"]), inspected_region=_box(value["inspected_region"])))


def _table(value) -> TableObservation:
    return TableObservation(
        **_fields(
            value,
            bbox=_box(value["bbox"]),
            cells=tuple(tuple(row) for row in value["cells"]),
            cell_boxes=tuple(tuple(_box(box) for box in row) for row in value["cell_boxes"]),
            cell_details=tuple(TableCell(**_fields(cell, box=_box(cell["box"]))) for cell in value["cell_details"]),
        )
    )


def _observation(value) -> Observation:
    return Observation(
        **_fields(
            value,
            blocks=tuple(_block(block) for block in value["blocks"]),
            images=tuple(Raster(**_fields(image, box=_box(image["box"]))) for image in value["images"]),
            issues=tuple(ProcessingIssue(**issue) for issue in value["issues"]),
            inspected_region=_box(value["inspected_region"]),
            tables=tuple(_table(table) for table in value["tables"]),
        )
    )


def read_page(body: bytes) -> PageResult:
    """Reconstruct only the explicit, installed SpicyDocs page data types."""
    try:
        record = decode_exact_json(body.decode())
        if set(record) != {"format_version", "page"} or record["format_version"] != FORMAT:
            raise ValueError("Unknown page observation format")
        value = record["page"]
        page = PageResult(
            **_fields(
                value,
                content=PageContent(
                    blocks=tuple(_block(block) for block in value["content"]["blocks"]),
                    observations=tuple(_observation(item) for item in value["content"]["observations"]),
                ),
                tables=tuple(_table(table) for table in value["tables"]),
            )
        )
        if page_bytes(page) != body:
            raise ValueError("Page fields changed during reconstruction")
        return page
    except (KeyError, TypeError, ValueError, UnicodeError) as error:
        raise ScorecardReplayError("Retained extraction page has an invalid typed shape") from error


class PageObservationReplay:
    """Implement the existing extractor interface for hash-bound complete pages."""

    def __init__(self, pages):
        self.sources = {}
        for page in pages:
            metadata = page.metadata
            self.sources.setdefault(metadata["source_sha256"], []).append(page)
        if not self.sources:
            raise ScorecardReplayError("Retained extraction has no pages")
        for pages in self.sources.values():
            total = len(pages)
            if any(
                page.metadata.get("page") != index
                or page.metadata.get("page_count") != total
                or page.metadata.get("source_size_bytes") != pages[0].metadata.get("source_size_bytes")
                or page.metadata.get("media_type") != "application/pdf"
                for index, page in enumerate(pages, 1)
            ):
                raise ScorecardReplayError("Retained extraction pages are incomplete, reordered or inconsistent")
        self.used = set()

    def extract(self, source, *, media_type, pages=None, overrides=None):
        digest = sha256(source).hexdigest()
        selected = self.sources.get(digest)
        if media_type != "application/pdf" or pages is not None or overrides is not None or selected is None:
            raise ScorecardReplayError("Extraction request differs from the retained complete PDF")
        if selected[0].metadata["source_size_bytes"] != len(source):
            raise ScorecardReplayError("Extraction source size differs from its retained observation")
        self.used.add(digest)
        yield from selected

    def complete(self):
        if self.used != set(self.sources):
            raise ScorecardReplayError("Not every selected PDF observation was replayed")
