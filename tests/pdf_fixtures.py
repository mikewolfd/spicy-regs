"""Helpers to synthesize tiny, valid PDFs for tests.

Building PDFs by hand (rather than committing binary fixtures) keeps the test
data transparent and avoids a heavyweight PDF-authoring dependency. The output
is minimal but real: pypdf parses it and extracts the embedded text.
"""

from __future__ import annotations

import io


def make_pdf(pages_text: list[str]) -> bytes:
    """Return the bytes of a minimal PDF with one text line per page."""
    n_pages = len(pages_text)
    font_obj = 3
    page_ids = [4 + i for i in range(n_pages)]
    content_ids = [4 + n_pages + i for i in range(n_pages)]

    parts: dict[int, bytes] = {}
    parts[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    kids = b" ".join(b"%d 0 R" % pid for pid in page_ids)
    parts[2] = b"<< /Type /Pages /Kids [" + kids + b"] /Count %d >>" % n_pages
    parts[font_obj] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
    for i in range(n_pages):
        parts[page_ids[i]] = (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 %d 0 R >> >> /Contents %d 0 R >>" % (font_obj, content_ids[i])
        )
        text = pages_text[i].encode("latin-1", "replace")
        stream = b"BT /F1 12 Tf 72 720 Td (" + text + b") Tj ET"
        parts[content_ids[i]] = b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream)

    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for oid in sorted(parts):
        offsets[oid] = out.tell()
        out.write(b"%d 0 obj\n" % oid + parts[oid] + b"\nendobj\n")
    xref_pos = out.tell()
    max_id = max(parts)
    out.write(b"xref\n0 %d\n" % (max_id + 1))
    out.write(b"0000000000 65535 f \n")
    for oid in range(1, max_id + 1):
        out.write(b"%010d 00000 n \n" % offsets[oid])
    out.write(b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (max_id + 1, xref_pos))
    return out.getvalue()


def make_multiline_pdf(pages_lines: list[list[str]]) -> bytes:
    """Return the bytes of a minimal PDF with several physical text lines per page.

    ``make_pdf`` puts a whole page's text in one ``Tj`` call, which is enough
    for the pypdf-oriented tests it serves but reads back as a single physical
    line — no adjacency for ``gpo_normalize`` to see. This writes each line
    with its own ``Td`` vertical move, so a real PDF reader's line-grouped
    text extraction (PyMuPDF's ``get_text("dict")``, which ``body_text``'s
    default extractor uses) reports one line per list entry, the shape a real
    GPO gutter-numbered page has: a content line immediately followed by its
    own bare digit line.
    """
    n_pages = len(pages_lines)
    font_obj = 3
    page_ids = [4 + i for i in range(n_pages)]
    content_ids = [4 + n_pages + i for i in range(n_pages)]

    parts: dict[int, bytes] = {}
    parts[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    kids = b" ".join(b"%d 0 R" % pid for pid in page_ids)
    parts[2] = b"<< /Type /Pages /Kids [" + kids + b"] /Count %d >>" % n_pages
    parts[font_obj] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
    for i in range(n_pages):
        parts[page_ids[i]] = (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 %d 0 R >> >> /Contents %d 0 R >>" % (font_obj, content_ids[i])
        )
        ops = [b"BT /F1 12 Tf 72 720 Td"]
        for j, line in enumerate(pages_lines[i]):
            if j > 0:
                ops.append(b"0 -14 Td")
            ops.append(b"(" + line.encode("latin-1", "replace") + b") Tj")
        ops.append(b"ET")
        stream = b"\n".join(ops)
        parts[content_ids[i]] = b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream)

    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for oid in sorted(parts):
        offsets[oid] = out.tell()
        out.write(b"%d 0 obj\n" % oid + parts[oid] + b"\nendobj\n")
    xref_pos = out.tell()
    max_id = max(parts)
    out.write(b"xref\n0 %d\n" % (max_id + 1))
    out.write(b"0000000000 65535 f \n")
    for oid in range(1, max_id + 1):
        out.write(b"%010d 00000 n \n" % offsets[oid])
    out.write(b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (max_id + 1, xref_pos))
    return out.getvalue()


def make_textless_pdf() -> bytes:
    """A valid single-page PDF with no text content stream (image-only stand-in)."""
    parts: dict[int, bytes] = {
        1: b"<< /Type /Catalog /Pages 2 0 R >>",
        2: b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        3: b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>",
    }
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for oid in sorted(parts):
        offsets[oid] = out.tell()
        out.write(b"%d 0 obj\n" % oid + parts[oid] + b"\nendobj\n")
    xref_pos = out.tell()
    max_id = max(parts)
    out.write(b"xref\n0 %d\n" % (max_id + 1))
    out.write(b"0000000000 65535 f \n")
    for oid in range(1, max_id + 1):
        out.write(b"%010d 00000 n \n" % offsets[oid])
    out.write(b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (max_id + 1, xref_pos))
    return out.getvalue()
