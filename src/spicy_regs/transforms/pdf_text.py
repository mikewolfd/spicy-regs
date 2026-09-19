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

from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

# Pages are joined with a blank line so the boundary survives in the stored
# text without inventing structure the source PDF didn't have.
PAGE_SEPARATOR = "\n\n"

#: The pypdf release this repository pins and checks before parsing, so PDF
#: text and PDF failure behavior are the same in a checkout and a package
#: install. `pyproject.toml` pins the same string; `vendor/README.md` says why.
PYPDF_VERSION = "6.14.2"


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
            PdfTextStatus.ERROR,
            "",
            0,
            error="PDF extraction requires spicy-regs[source-readers]",
        )

    page_count = 0
    try:
        # Preserve the established empty-password attempt for permissions-only PDFs.
        with PypdfReader(expected_backend_version=PYPDF_VERSION).open(data, password="") as document:
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


class PypdfPageExtractor:
    """``spicy_docs.extraction.Extractor`` over the pypdf provider this repository pins.

    ``extraction.body_text.body_text`` derives a PDF body's text by running an
    extractor and then ``gpo_normalize``. Its *default* extractor is
    ``DocumentExtractor(NativeText())``, whose default reader opens PDFs with
    PyMuPDF — a provider this repository does not install: it pins the narrow
    ``pdf-pypdf`` provider extra instead (`vendor/README.md`), so the default
    path raises ``ModuleNotFoundError`` on the first PDF body and every caller
    that swallows the failure would simply never derive PDF text. Passing this
    extractor is what makes the PDF branch reachable here.

    It is an ``Extractor``, not a ``DocumentReader``: ``PypdfReader`` takes
    ``open(source, *, password=...)`` while a ``DocumentReader`` takes
    ``open(source, media_type)``, and the reader seam would also drag in the
    page-geometry and rendering surface that native text extraction does not
    use. ``body_text`` reads only each ``PageResult``'s text, so one text block
    per page is the whole contract.

    ``password=""`` keeps the empty-password attempt :func:`extract_pdf_text`
    already makes, so a permissions-only PDF reads the same way through both
    doors.
    """

    def extract(self, source: bytes, *, media_type: str, **_unused: Any) -> Iterator[Any]:
        from spicy_docs.extraction.model import Box, PageContent, PageResult, TextBlock
        from spicy_docs.extraction.pypdf import PypdfReader

        if media_type != "application/pdf":
            raise ValueError(f"expected application/pdf, got {media_type!r}")
        with PypdfReader(expected_backend_version=PYPDF_VERSION).open(source, password="") as document:
            for number in range(1, document.page_count + 1):
                text = document.read_page(number) or ""
                blocks = (TextBlock(text, Box()),) if text else ()
                yield PageResult(
                    metadata={"page": number, "backend": "pypdf", "backend_version": document.backend_version},
                    content=PageContent(blocks, ()),
                )
