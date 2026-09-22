"""Pure PDF byte → text extraction.

The core of the "extract PDF text" pipeline step: bytes in, embedded text out,
with no network or filesystem dependency, so it is trivially testable.
Fetching those bytes from a URL is a :mod:`spicy_regs.sources` concern.
Embedded text only — scanned/image-only PDFs carry no text layer and come back
:attr:`PdfTextStatus.EMPTY`, OCR being explicitly out of scope; basic structure
is preserved by joining pages with :data:`PAGE_SEPARATOR`, while table/column
reconstruction is out of scope; corrupt, truncated or password-protected PDFs
never raise — they return a result with a non-OK status so a batch enrichment
run can record the outcome and move on.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# Pages are joined with a blank line so the boundary survives in the stored
# text without inventing structure the source PDF didn't have.
PAGE_SEPARATOR = "\n\n"

#: The pypdf release this repository pins and checks before parsing, so PDF
#: text and PDF failure behavior are the same in a checkout and a package
#: install. `pyproject.toml` pins the same string; `vendor/README.md` says why.
#: Used only by :func:`extract_pdf_text`, the regulations.gov attachment path
#: — the GovInfo body path (`extraction.body_text`) uses spicy-docs's default
#: PyMuPDF extractor instead; see ``vendor/README.md`` for why.
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
