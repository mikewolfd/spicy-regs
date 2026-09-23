"""Comment ``text_content`` from Mirrulations' own attachment extraction (``derived-data``), with provenance.

spicy-docs lists a docket's extracted text once, picks one tool per comment by its
pinned ``DERIVED_TEXT_TOOLS`` order, orders attachments by number and fetches them
pinned to their listed ETags (``list_docket_derived_text``, ``fetch_derived_text``).
This module decides what a comment row gets from that:

* ``text_content`` -- the chosen tool's attachments, each stripped, blank ones dropped,
  joined with a blank line;
* ``text_extraction_status`` -- ``derived``: SpicyRegs did not run the extractor, so the
  text is not ``ok`` (decision 19, ``docs/research/fork-delivery-decisions-2026-09-22.md``);
* ``pdf_extraction_results_json`` -- the comment's provenance record: chosen tool,
  available tools, each attachment's key, size, ETag and SHA-256, and the attachment
  numbers only another tool has.

A refused docket listing or a failed fetch raises :class:`DerivedTextUnavailable`, so the
comment stays pending rather than being recorded as having no text. A 401/403 raises
spicy-docs' ``MirrulationsAccessRefusedError`` and ends the run. spicy-docs is imported
when a fetcher is built, so importing the transforms needs no source readers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, Final

from loguru import logger

if TYPE_CHECKING:
    from spicy_docs.sources.mirrulations import CommentDerivedText

#: ``text_extraction_status`` for text taken from Mirrulations' extraction rather than ours.
DERIVED_STATUS: Final = "derived"

#: Per-object GET cap. spicy-docs' 16 MiB default refuses three objects of its 2026-09-23
#: sample, all ``pdfminer``, the largest 57,500,861 bytes; this admits them.
MAX_ATTACHMENT_BYTES: Final = 64 * 1024 * 1024

#: Attachments join with a blank line, as the PDF path joins pages (``transforms.pdf_text.PAGE_SEPARATOR``).
PART_SEPARATOR: Final = "\n\n"


class DerivedTextUnavailable(Exception):
    """A docket listing was refused or a fetch failed: the comment stays pending, never "no text"."""


@dataclass(frozen=True, slots=True)
class DerivedFill:
    """The values one comment row takes: its text and the provenance JSON."""

    text: str
    provenance: str


def provenance_json(comment: CommentDerivedText) -> str:
    """The comment's ``pdf_extraction_results_json``; digests are null until its attachments are fetched."""
    return json.dumps(comment.to_json())


def derived_fill(comment: CommentDerivedText) -> DerivedFill | None:
    """The fill for a fetched comment, or ``None`` when every chosen attachment is blank."""
    parts = [text for attachment in comment.attachments if (text := (attachment.text or "").strip())]
    return DerivedFill(PART_SEPARATOR.join(parts), provenance_json(comment)) if parts else None


def _mirrulations() -> ModuleType:
    """spicy-docs' Mirrulations reader, or a refusal naming the install that provides it."""
    try:
        from spicy_docs.sources import mirrulations
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "Mirrulations derived text requires spicy-regs[source-readers]. "
                "Run `uv sync --frozen` in a SpicyRegs checkout."
            ) from None
        raise
    return mirrulations


class DerivedCommentText:
    """Fills comments from their docket's listing, listed once per instance.

    The cache is plain dict state, so build one per worker thread. A refused listing
    is cached as refused: the docket is not listed again for each of its comments.
    ``bucket`` defaults to spicy-docs' Mirrulations bucket.
    """

    def __init__(self, s3_resource: Any, bucket: str | None = None, *, max_bytes: int = MAX_ATTACHMENT_BYTES) -> None:
        self._reader = _mirrulations()
        self._resource = s3_resource
        self._bucket = self._reader.BUCKET if bucket is None else bucket
        self._max_bytes = max_bytes
        self._dockets: dict[tuple[str, str], dict[str, CommentDerivedText] | str] = {}

    def listed(self, agency: str, docket_id: str) -> dict[str, CommentDerivedText]:
        """The docket's comments with derived text, from one strict listing; raises if it was refused."""
        key = (agency, docket_id)
        if key not in self._dockets:
            try:
                self._dockets[key] = self._reader.list_docket_derived_text(
                    self._resource, agency, docket_id, bucket=self._bucket
                ).comments
            except self._reader.MirrulationsAccessRefusedError:
                raise
            except Exception as exc:  # noqa: BLE001 -- any refusal leaves the docket pending
                logger.warning(
                    "derived-data listing refused for {}/{}; its comments stay pending: {}", agency, docket_id, exc
                )
                self._dockets[key] = f"{agency}/{docket_id}: {exc}"
        listed = self._dockets[key]
        if isinstance(listed, str):
            raise DerivedTextUnavailable(listed)
        return listed

    def fill_for(self, agency: str | None, docket_id: str | None, comment_id: str | None) -> DerivedFill | None:
        """The comment's fill; ``None`` when the listing has no text for it or its text is blank."""
        if not (agency and docket_id and comment_id):
            return None
        comment = self.listed(agency, docket_id).get(comment_id)
        if comment is None:
            return None
        try:
            fetched = self._reader.fetch_derived_text(
                self._resource, comment, bucket=self._bucket, max_bytes=self._max_bytes
            )
        except self._reader.MirrulationsAccessRefusedError:
            raise
        except Exception as exc:  # noqa: BLE001 -- a failed attachment fails the comment, never drops a part
            logger.warning("derived-data fetch failed for {}; it stays pending: {}", comment_id, exc)
            raise DerivedTextUnavailable(f"{comment_id}: {exc}") from exc
        return derived_fill(fetched)
