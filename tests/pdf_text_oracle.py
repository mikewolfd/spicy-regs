"""Frozen SpicyRegs page policy at 979872c0700b2ed1031ce9f2f0817505fdbaec38.

The swallowed-page-error behavior is retained only to prove its intentional correction.
"""

import io

from pypdf import PdfReader
from pypdf.errors import PyPdfError

from spicy_regs.transforms.pdf_text import PAGE_SEPARATOR, PdfTextResult, PdfTextStatus


def extract_pdf_text(data: bytes) -> PdfTextResult:
    """Extract embedded text from PDF ``data``.

    Never raises: every failure mode is mapped to a :class:`PdfTextResult`
    carrying a non-OK :class:`PdfTextStatus`.
    """
    if not data:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error="empty input")

    try:
        reader = PdfReader(io.BytesIO(data))
    except (PyPdfError, OSError, ValueError) as exc:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error=str(exc))

    if reader.is_encrypted:
        # Many regulations.gov PDFs are "encrypted" only with an empty owner
        # password (encrypted for permissions, not secrecy); try to open them.
        try:
            if reader.decrypt("") == 0:  # 0 == PasswordType.NOT_DECRYPTED
                return PdfTextResult(PdfTextStatus.ENCRYPTED, "", 0, error="password required")
        except (PyPdfError, NotImplementedError) as exc:
            return PdfTextResult(PdfTextStatus.ENCRYPTED, "", 0, error=str(exc))

    try:
        pages = reader.pages
        page_count = len(pages)
        parts: list[str] = []
        for page in pages:
            # One bad page shouldn't sink the whole document.
            try:
                parts.append(page.extract_text() or "")
            except Exception:  # noqa: BLE001 - pypdf raises a wide variety here
                parts.append("")
    except (PyPdfError, OSError, ValueError) as exc:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error=str(exc))

    text = PAGE_SEPARATOR.join(p.strip() for p in parts).strip()
    if not text:
        return PdfTextResult(PdfTextStatus.EMPTY, "", page_count)
    return PdfTextResult(PdfTextStatus.OK, text, page_count)
