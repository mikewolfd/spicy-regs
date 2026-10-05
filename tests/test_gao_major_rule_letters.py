"""A major-rule report's letter to three cells and a receipt field, read from captured pages by reference.

The pages are GAO's own, captured on 2026-10-04 (``fixtures/gao_major_rule_letters/README.md``); spicy-docs' reader
is the real one. What its rules read is tested there; these tests hold what the host does with a reading.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from spicy_docs.sources.gao.major_rule_letters import LETTER_RULE

from spicy_regs.sources import gao_major_rule_letters as letters
from spicy_regs.sources import r2

PAGES = Path(__file__).parent / "fixtures/gao_major_rule_letters"
PRODUCTS = "https://www.gao.gov/products/"
PDF_URL = "https://www.gao.gov/assets/ogc-01-6.pdf"


def page(product_id: str) -> bytes:
    return (PAGES / f"{product_id}.html").read_bytes()


def capture(directory: Path, bodies: dict[str, bytes], *, failed: tuple[str, ...] = ()) -> Path:
    """A capture directory as the 2026-10-04 driver wrote one: a receipt line per fetch, bytes by digest."""
    blobs = directory / "blobs/sha256"
    blobs.mkdir(parents=True)
    lines = [json.dumps({"url": url, "status_code": 520}) for url in failed]
    for url, body in bodies.items():
        digest = hashlib.sha256(body).hexdigest()
        (blobs / digest).write_bytes(body)
        lines.append(json.dumps({"url": url, "status_code": 200, "sha256": digest, "bytes": len(body)}))
    (directory / "receipts.jsonl").write_text("".join(line + "\n" for line in lines))
    return directory


def reading(cells: dict) -> dict:
    return json.loads(cells[letters.RECEIPT])


def test_a_letter_gives_its_agency_as_printed_and_its_rins_and_citations_as_lists():
    cells = letters.letter_cells(page("gao-04-193r"), product_id="gao-04-193r", product_number="GAO-04-193R")
    assert {column: cells[column] for column in letters.CELLS} == {
        "major_rule_agency": "Department of Health and Human Services, Food and Drug Administration (FDA)",
        "major_rule_rins_json": '["0910-AC40"]',
        "major_rule_fr_citations_json": '["68-58894"]',
    }


def test_each_value_keeps_where_it_was_read():
    """The receipt field: the rule, the bytes read, the letter text's digest, and each value's span in that text."""
    body = page("gao-04-193r")
    read = reading(letters.letter_cells(body, product_id="gao-04-193r", product_number="GAO-04-193R"))
    assert (read["rule"], read["outcome"], read["source"]) == (LETTER_RULE, "read", "product-page")
    assert read["url"] == PRODUCTS + "gao-04-193r"
    assert read["sha256"] == "sha256:" + hashlib.sha256(body).hexdigest()
    assert read["text_sha256"].startswith("sha256:")
    [citation] = read["federal_register"]["values"]
    assert citation["value"] == "68-58894" and citation["printed"] == "68 Fed. Reg. 58894"
    assert citation["end"] - citation["start"] == len(citation["printed"]) and citation["page"] is None
    assert [read[field]["status"] for field in ("agency", "rins", "federal_register")] == ["stated"] * 3


def test_a_blank_is_null_and_the_receipt_says_why():
    """GAO-01-300R names an FCC docket where the RIN would be: no RIN is guessed, and the status is kept."""
    cells = letters.letter_cells(page("gao-01-300r"), product_id="gao-01-300r", product_number="GAO-01-300R")
    assert cells["major_rule_rins_json"] is None
    assert (cells["major_rule_agency"], cells["major_rule_fr_citations_json"]) == (
        "Federal Communications Commission (FCC)", '["66-2322"]')
    assert reading(cells)["rins"] == {"status": "not-stated", "values": []}


def test_a_report_on_two_rules_holds_both_citations_and_the_lists_are_not_pairs():
    """OGC-97-44 states one RIN for two rules: the second citation has no RIN of its own in the list."""
    cells = letters.letter_cells(page("ogc-97-44"), product_id="ogc-97-44", product_number="OGC-97-44")
    assert json.loads(cells["major_rule_fr_citations_json"]) == ["62-24746", "61-52190"]
    assert json.loads(cells["major_rule_rins_json"]) == ["0579-AA83"]


def test_a_page_that_prints_another_reports_letter_is_refused_with_all_three_null():
    cells = letters.letter_cells(page("b-330560"), product_id="b-330560", product_number="B-330560")
    assert all(cells[column] is None for column in letters.CELLS)
    read = reading(cells)
    assert (read["outcome"], read["reason"], read["rule"]) == ("refused", "not-this-letter", LETTER_RULE)
    assert "agency" not in read and read["url"] == PRODUCTS + "b-330560"


def test_a_page_whose_letter_has_no_opening_is_read_from_its_full_report_pdf(tmp_path):
    """OGC-01-6's page prints ``801( a)( 2)( A)``; the Full Report it links states the letter plainly."""
    pdf = (PAGES / "ogc-01-6.pdf").read_bytes()
    held = letters.LetterPages([capture(tmp_path / "pdfs", {PDF_URL: pdf})])
    cells = letters.letter_cells(page("ogc-01-6"), product_id="ogc-01-6", product_number="OGC-01-6", pages=held)
    read = reading(cells)
    assert (read["source"], read["url"]) == ("report-pdf", PDF_URL)
    assert read["sha256"] == "sha256:" + hashlib.sha256(pdf).hexdigest()
    assert cells["major_rule_agency"] == "Federal Communications Commission (FCC)"
    assert json.loads(cells["major_rule_fr_citations_json"]) == ["65-59350"]
    assert cells["major_rule_rins_json"] is None and read["rins"]["status"] == "not-stated"
    assert read["federal_register"]["values"][0]["page"] == 1  # a PDF's values keep their page


def test_without_the_pdf_the_page_alone_is_read_and_its_blanks_say_no_opening():
    cells = letters.letter_cells(page("ogc-01-6"), product_id="ogc-01-6", product_number="OGC-01-6")
    assert all(cells[column] is None for column in letters.CELLS)
    read = reading(cells)
    assert (read["outcome"], read["source"], read["agency"]["status"]) == ("read", "product-page", "no-opening")


def test_a_page_that_prints_no_letter_and_has_no_pdf_held_is_no_letter():
    bare = b'<html><head><link rel="canonical" href="https://www.gao.gov/products/b-336972" /></head><body></body></html>'
    cells = letters.letter_cells(bare, product_id="b-336972", product_number="B-336972", pages=letters.LetterPages())
    assert all(cells[column] is None for column in letters.CELLS)
    assert reading(cells) == {"rule": LETTER_RULE, "outcome": "no-letter", "url": PRODUCTS + "b-336972",
                              "sha256": "sha256:" + hashlib.sha256(bare).hexdigest()}


@pytest.mark.parametrize(
    ("held", "expected"),
    [
        (None, True),  # no page read
        ({"rule": LETTER_RULE, "outcome": "read"}, False),
        ({"rule": LETTER_RULE, "outcome": "refused", "reason": "not-this-letter"}, False),
        ({"rule": LETTER_RULE, "outcome": "no-letter"}, True),  # a later capture may hold the PDF
        ({"rule": "gao-major-rule-letter/2", "outcome": "read"}, True),  # a new rule reads every letter again
    ],
)
def test_a_row_is_unread_until_the_current_rule_has_read_its_letter(held, expected):
    row = {letters.RECEIPT: None if held is None else json.dumps(held)}
    assert letters.unread(row) is expected


def test_pages_are_found_in_a_walk_first_then_in_each_capture_and_every_read_is_checked(tmp_path):
    first = capture(tmp_path / "letters", {PRODUCTS + "gao-04-193r": page("gao-04-193r")},
                    failed=(PRODUCTS + "gao-01-300r",))
    second = capture(tmp_path / "letters/pdfs", {PDF_URL: b"%PDF"})
    held = letters.LetterPages([first, second], walked={PRODUCTS + "ogc-97-44": b"walked"})
    assert held and not letters.LetterPages()
    assert held.body(PRODUCTS + "ogc-97-44") == b"walked"
    assert held.body(PRODUCTS + "gao-04-193r") == page("gao-04-193r") and held.body(PDF_URL) == b"%PDF"
    assert held.body(PRODUCTS + "gao-01-300r") is None  # a 520 is no page
    # A blob that no longer matches its receipt is refused, never read.
    digest = hashlib.sha256(page("gao-04-193r")).hexdigest()
    (first / "blobs/sha256" / digest).write_bytes(b"changed")
    with pytest.raises(ValueError, match="differs from its receipt"):
        letters.LetterPages([first]).body(PRODUCTS + "gao-04-193r")


def test_each_capture_read_is_an_input_of_the_run_under_its_own_name(tmp_path):
    """Two ``pdfs`` directories are two campaigns; each is noted by the receipt lines of what was read from it."""
    one = capture(tmp_path / "letters/pdfs", {PDF_URL: b"%PDF-one"})
    two = capture(tmp_path / "early/pdfs", {PDF_URL + "?2": b"%PDF-two"})
    held = letters.LetterPages([one, two])
    held.body(PDF_URL)
    with r2.recorded_reads() as reads:
        noted = held.record()
    assert [entry["campaign"] for entry in noted] == ["letters/pdfs", "early/pdfs"]
    line = (one / "receipts.jsonl").read_text()
    assert reads["letters/pdfs"] == {"sha256": "sha256:" + hashlib.sha256(line.encode()).hexdigest(),
                                     "byteSize": len(line)}
    assert reads["early/pdfs"]["byteSize"] == 0  # named, nothing read from it
