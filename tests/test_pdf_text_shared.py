"""Shared PDF reading preserves normalization and records page failures."""

import subprocess
import sys
from io import BytesIO
from pathlib import Path

import pypdf
import pytest

from spicy_regs.transforms.pdf_text import PdfTextStatus, extract_pdf_text
from tests.pdf_fixtures import make_pdf, make_textless_pdf
from tests.pdf_text_oracle import extract_pdf_text as frozen_extract

SAMPLES = Path(__file__).parents[1] / "sample-data/mirrulations"


def _rewrite(source: bytes, *, password: str | None = None, broken_page: int | None = None) -> bytes:
    from pypdf.generic import NameObject, NumberObject

    with pypdf.PdfReader(BytesIO(source)) as reader, pypdf.PdfWriter() as writer:
        writer.append_pages_from_reader(reader)
        if broken_page is not None:
            writer.pages[broken_page - 1][NameObject("/Resources")] = NumberObject(9)
        if password is not None:
            writer.encrypt(user_password=password, owner_password="owner", algorithm="RC4-128")
        output = BytesIO()
        writer.write(output)
        return output.getvalue()


@pytest.mark.parametrize("filename", [
    "comment-ACF-2025-0038-0004_attachment_1.pdf",
    "document-ACF-2025-0038-0001_content.pdf",
])
def test_complete_retained_pdfs_preserve_text_and_page_counts(filename):
    source = (SAMPLES / filename).read_bytes()
    assert extract_pdf_text(source) == frozen_extract(source)


@pytest.mark.parametrize("source", [
    make_pdf(["  First  ", "", "   ", "Last"]), make_textless_pdf(), make_pdf([]),
])
def test_actual_blank_pages_whitespace_and_zero_pages_preserve_policy(source):
    assert extract_pdf_text(source) == frozen_extract(source)


@pytest.mark.parametrize("password", ["", "secret"])
def test_empty_password_attempt_remains_explicit_and_matches_frozen_policy(password):
    source = _rewrite(make_pdf(["Protected"]), password=password)
    expected = frozen_extract(source)
    actual = extract_pdf_text(source)
    assert (actual.status, actual.text, actual.page_count) == (expected.status, expected.text, expected.page_count)
    assert actual.status is (PdfTextStatus.OK if password == "" else PdfTextStatus.ENCRYPTED)
    if password:
        assert actual.error


@pytest.mark.parametrize("source", [b"", b"not a PDF", b"%PDF-1.4 truncated"])
def test_unreadable_input_remains_an_explicit_failure(source):
    previous = frozen_extract(source)
    actual = extract_pdf_text(source)
    assert actual.status is previous.status is PdfTextStatus.ERROR
    assert actual.text == previous.text == ""
    assert actual.page_count == previous.page_count == 0
    assert actual.error


@pytest.mark.parametrize("texts,broken_page,previous_status", [
    (["First", "Bad", "Third"], 2, PdfTextStatus.OK),
    (["Bad"], 1, PdfTextStatus.EMPTY),
])
def test_intentional_correction_failed_pages_are_not_successful_blank_pages(texts, broken_page, previous_status):
    source = _rewrite(make_pdf(texts), broken_page=broken_page)
    assert frozen_extract(source).status is previous_status
    actual = extract_pdf_text(source)
    assert actual.status is PdfTextStatus.ERROR
    assert not actual.ok
    assert actual.text == ""
    assert actual.page_count == len(texts)
    assert actual.error is not None
    assert f"page {broken_page}" in actual.error


@pytest.mark.parametrize("text", [None, "", "  \n"])
def test_backend_none_and_whitespace_are_successful_empty_pages(monkeypatch, text):
    monkeypatch.setattr(pypdf.PageObject, "extract_text", lambda _: text)
    source = make_textless_pdf()
    actual = extract_pdf_text(source)
    assert actual == frozen_extract(source)
    assert actual.status is PdfTextStatus.EMPTY
    assert actual.page_count == 1


def test_backend_version_mismatch_refuses_before_parsing(monkeypatch):
    monkeypatch.setattr(pypdf, "__version__", "different")
    monkeypatch.setattr(pypdf, "PdfReader", lambda *a, **kw: pytest.fail("parsed with unidentified backend"))
    result = extract_pdf_text(make_pdf(["Text"]))
    assert result.status is PdfTextStatus.ERROR
    assert result.error is not None
    assert "version differs" in result.error


def test_new_input_cap_refuses_before_backend_construction(monkeypatch):
    source = b"%PDF" + b" " * (64 * 1024**2)
    monkeypatch.setattr(pypdf, "PdfReader", lambda *a, **kw: pytest.fail("parsed oversized input"))
    result = extract_pdf_text(source)
    assert result.status is PdfTextStatus.ERROR
    assert result.error is not None
    assert "max_input_bytes" in result.error


def test_base_import_is_independent_and_missing_reader_is_actionable():
    script = """
import importlib.abc
import sys
class NoPdfDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in ('spicy_docs', 'pypdf'):
            raise ImportError('PDF dependencies intentionally unavailable')
sys.meta_path.insert(0, NoPdfDependencies())
import spicy_regs.cli
from spicy_regs.transforms.pdf_text import extract_pdf_text, PdfTextStatus
assert not any(name.split('.')[0] in ('pypdf', 'spicy_docs') for name in sys.modules)
result = extract_pdf_text(b'%PDF')
assert result.status is PdfTextStatus.ERROR
assert result.text == ''
assert 'spicy-regs[source-readers]' in result.error
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_unqualified_backend_refuses_before_pdf_construction(monkeypatch):
    source = make_pdf(["Text"])
    monkeypatch.setattr("spicy_docs.extraction.pypdf.version", lambda _: "6.18.1")
    monkeypatch.setattr(pypdf, "__version__", "6.18.1")
    monkeypatch.setattr(pypdf, "PdfReader", lambda *a, **kw: pytest.fail("parsed with unqualified backend"))
    result = extract_pdf_text(source)
    assert result.status is PdfTextStatus.ERROR
    assert result.text == ""
    assert result.page_count == 0
    assert result.error is not None
    assert "expected_backend_version" in result.error


def test_backend_exception_aborts_without_partial_text(monkeypatch):
    source = make_pdf(["First", "Second", "Third"])
    original = pypdf.PageObject.extract_text
    pages = []

    def fail_second_page(page):
        pages.append(len(pages) + 1)
        if len(pages) == 2:
            raise ValueError("backend page failure")
        return original(page)

    monkeypatch.setattr(pypdf.PageObject, "extract_text", fail_second_page)
    result = extract_pdf_text(source)
    assert pages == [1, 2]
    assert result.status is PdfTextStatus.ERROR
    assert result.text == ""
    assert result.page_count == 3
    assert result.error is not None
    assert "page 2" in result.error and "backend page failure" in result.error
