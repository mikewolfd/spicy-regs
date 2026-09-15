"""Pure PDF byte → text extraction.

This is the core of the "extract PDF text" pipeline step (issue #9). It takes
the *bytes* of a PDF and returns the embedded text, page by page. Fetching
those bytes from a URL is a :mod:`spicy_regs.sources` concern, not a transform —
keeping this module pure (bytes in, text out) means it has no network or
filesystem dependency and is trivially testable.

Scope notes (matching the issue):
  * Embedded text only. Scanned/image-only PDFs carry no text layer and come
    back :attr:`PdfTextStatus.EMPTY` — OCR is explicitly out of scope.
  * Basic structure is preserved by joining pages with :data:`PAGE_SEPARATOR`;
    table/column reconstruction is out of scope.
  * Corrupt, truncated, or password-protected PDFs never raise — they return a
    result with a non-OK status so a batch enrichment run can record the
    outcome and move on.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# Pages are joined with a blank line so the boundary survives in the stored
# text without inventing structure the source PDF didn't have.
PAGE_SEPARATOR = "\n\n"


class PdfTextStatus(str, Enum):
    """Outcome of an extraction attempt.

    ``str`` mixin so the value serialises straight into Parquet/JSON as a
    plain string (``"ok"``, ``"empty"``, ...).
    """

    OK = "ok"
    """Parsed and produced some text."""
    EMPTY = "empty"
    """Parsed fine but no extractable text — almost always a scanned/image PDF."""
    ENCRYPTED = "encrypted"
    """Password-protected and could not be opened with an empty password."""
    ERROR = "error"
    """Not a PDF, truncated, or otherwise unparseable."""


@dataclass(frozen=True)
class PdfTextResult:
    """Result of extracting text from one PDF's bytes."""

    status: PdfTextStatus
    text: str
    page_count: int
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status is PdfTextStatus.OK


def extract_pdf_text(data: bytes) -> PdfTextResult:
    """Extract embedded text from PDF ``data``.

    Source and backend failures return a non-OK status. A failed page refuses
    the whole PDF so partial text cannot be mistaken for successful extraction.
    """
    if not data:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error="empty input")

    try:
        from spicy_docs.extraction.pypdf import PdfEncryptedError, PdfPageError, PdfReadError, PypdfReader
    except ImportError:
        return PdfTextResult(
            PdfTextStatus.ERROR, "", 0,
            error="PDF extraction requires spicy-regs[source-readers]",
        )

    page_count = 0
    try:
        # Preserve the established empty-password attempt for permissions-only PDFs.
        with PypdfReader().open(data, password="") as document:
            page_count = document.page_count
            parts = [document.read_page(page) or "" for page in range(1, page_count + 1)]
    except PdfEncryptedError as exc:
        return PdfTextResult(PdfTextStatus.ENCRYPTED, "", 0, error=str(exc))
    except PdfPageError as exc:
        return PdfTextResult(PdfTextStatus.ERROR, "", page_count, error=f"page {exc.page}: {exc}")
    except (PdfReadError, ValueError) as exc:
        return PdfTextResult(PdfTextStatus.ERROR, "", page_count, error=str(exc))

    text = PAGE_SEPARATOR.join(p.strip() for p in parts).strip()
    if not text:
        return PdfTextResult(PdfTextStatus.EMPTY, "", page_count)
    return PdfTextResult(PdfTextStatus.OK, text, page_count)
