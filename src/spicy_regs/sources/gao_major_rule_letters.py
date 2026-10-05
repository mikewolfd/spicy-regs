"""What a GAO major-rule report's letter states of its rule, read by reference from captured product pages.

GAO's listings state no agency, RIN or Federal Register citation for a major-rule report; the report's letter does,
in its opening paragraph, and spicy-docs reads it (``sources.gao.major_rule_letters``, rule ``LETTER_RULE``; the hand
checks, mutation run and Federal Register cross-check are in its ``docs/decisions.md``). The owner gave
``gao_reports`` the three as columns on 2026-10-04, every rule's values where a report is on several:

- ``major_rule_agency``: the agency clause as the letter prints it, one string, abbreviations kept. It is GAO's
  wording, not an agency key, and a joint rule's agencies are not split;
- ``major_rule_rins_json``: every RIN, folded to ``NNNN-LLNN``, in the letter's order;
- ``major_rule_fr_citations_json``: every citation as ``{volume}-{page}``, in the letter's order.

The two lists are not pairs: eleven of the seventeen reports on several rules state one RIN for two citations.

**Where a letter comes from.** The report's product page prints it under View Decision. Where the page prints none,
or none with the letter's opening, the letter is in the page's Full Report PDF. gao.gov refuses plain clients, so
pages and PDFs are captured outside the rollup, through Zyte, into directories of the shape
:class:`~spicy_regs.sources.gao_decision_pages.DecisionPageCapture` reads (``receipts.jsonl`` and
``blobs/sha256/<hex>``), and read in place, never copied: the capture of 2026-10-04
(``~/Work/corpora/fork-execution-2026-09-21/gao-cra-major-rules/letters-2026-10-04/``, its ``pdfs/``, and
``early-2026-10-04/pdfs/``). The old index walk's own product pages (:mod:`spicy_regs.sources.gao_listing`) are read
first. Each capture read is put into the run's read ledger as the decision pages' capture is.

**A blank is NULL, with its reason in the row's receipt.** ``major_rule_letter_json`` is a receipt field, not a
column: the rule, the bytes the letter was read from (url and digest), the letter text's digest, and for each of the
three its status and each value's printed spelling and span in that text. A status other than ``stated`` is why the
column is NULL (``not-stated``: the letter names a docket; ``unreadable``: GAO's typo; ``not-published``;
``no-citation``; ``no-agency-clause``; ``no-opening``). ``outcome`` is ``refused`` with spicy-docs' reason where the
page or PDF is not this report's letter (b-330560's page prints B-330546's), and ``no-letter`` where the page prints
none and no PDF of it is held. A row with no receipt field has had no page read.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from spicy_regs.sources.gao_decision_pages import DecisionPageCapture

#: The columns a letter states, each NULL unless its field's status is ``stated``.
CELLS = ("major_rule_agency", "major_rule_rins_json", "major_rule_fr_citations_json")
#: The receipt field that holds the reading.
RECEIPT = "major_rule_letter_json"
STAGE = "gao-major-rule-letters"
READ, REFUSED, NO_LETTER = "read", "refused", "no-letter"


class LetterPages:
    """Product pages and report PDFs by URL: a walk's retained pages first, then each capture in the order named.

    O(receipts) to open; one digest-checked read per page asked for.
    """

    def __init__(self, captures: Sequence[Path] = (), walked: Mapping[str, bytes] | None = None) -> None:
        self._walked = dict(walked or {})
        self._captures = [DecisionPageCapture(directory) for directory in captures]
        for capture in self._captures:
            # Two captures' ``pdfs`` directories are different campaigns: name each by its parent too.
            capture.campaign = "/".join(capture.directory.parts[-2:])

    def __bool__(self) -> bool:
        return bool(self._walked or self._captures)

    def body(self, url: str) -> bytes | None:
        """The bytes held for ``url``, or None where nothing holds it."""
        if url in self._walked:
            return self._walked[url]
        return next((body for capture in self._captures if (body := capture.page(url)) is not None), None)

    def record(self) -> list[dict[str, Any]]:
        """Note each capture in the read ledger by the receipts of what was read from it; say what was noted."""
        noted = []
        for capture in self._captures:
            digest, size = capture.record()
            noted.append({"campaign": capture.campaign, "receipts_sha256": digest, "receipts_bytes": size})
        return noted


def unread(row: Mapping[str, Any]) -> bool:
    """Whether the row holds no reading of its letter under the current rule, or one that found no letter to read."""
    from spicy_docs.sources.gao.major_rule_letters import LETTER_RULE

    held = row.get(RECEIPT)
    if held is None:
        return True
    reading = json.loads(held)
    return reading["rule"] != LETTER_RULE or reading["outcome"] == NO_LETTER


def _field(field: Any) -> dict[str, Any]:
    return {
        "status": field.status,
        "values": [
            {"value": value.value, "printed": value.printed, "start": value.start, "end": value.end, "page": value.page}
            for value in field.values
        ],
    }


def letter_cells(page: bytes, *, product_id: str, product_number: str, pages: LetterPages | None = None) -> dict:
    """The three columns and the receipt field one report's product page gives, its Full Report PDF read where needed.

    ``product_number`` is the number GAO's listing states, which the letter must carry. ``pages`` holds the PDF; with
    none, a page that prints no letter is ``no-letter``. O(page): one pass of spicy-docs' reader.
    """
    from spicy_docs.schemas.gao_decision_tables import GAO_SITE
    from spicy_docs.sources.gao.major_rule_letters import (
        LETTER_RULE,
        GaoMajorRuleLetterError,
        read_page_letter,
        read_pdf_letter,
    )
    from spicy_docs.sources.gao.native import gao_product_url
    from spicy_docs.sources.gao.product_metadata import full_report_path

    url = gao_product_url(product_id)
    read_from, body = url, page
    reading: dict[str, Any] = {"rule": LETTER_RULE}
    cells: dict[str, str | None] = dict.fromkeys(CELLS)
    try:
        letter = read_page_letter(page, product_id=product_id, product_number=product_number)
        if (letter is None or letter.agency.status == "no-opening") and pages is not None:
            try:
                pdf_url = GAO_SITE + full_report_path(page, url)
            except ValueError:
                pdf_url = None  # the page links no Full Report
            pdf = None if pdf_url is None else pages.body(pdf_url)
            if pdf is not None:
                read_from, body = pdf_url, pdf
                letter = read_pdf_letter(pdf, product_id=product_id, product_number=product_number)
    except GaoMajorRuleLetterError as error:
        reading |= {"outcome": REFUSED, "reason": error.reason}
    else:
        if letter is None:
            reading["outcome"] = NO_LETTER
        else:
            reading |= {
                "outcome": READ,
                "source": letter.source,
                "text_sha256": letter.text_sha256,
                "agency": _field(letter.agency),
                "rins": _field(letter.rins),
                "federal_register": _field(letter.federal_register),
            }
            lists = (letter.rins, letter.federal_register)
            cells = dict(
                zip(
                    CELLS,
                    (
                        letter.agency.value,
                        *(json.dumps([v.value for v in field.values]) if field.values else None for field in lists),
                    ),
                    strict=True,
                )
            )
    reading |= {"url": read_from, "sha256": "sha256:" + hashlib.sha256(body).hexdigest()}
    return {**cells, RECEIPT: json.dumps(reading, ensure_ascii=False, sort_keys=True)}


def tally(counts: Counter[str], cells: Mapping[str, Any]) -> None:
    """Count one reading: its outcome, each column it fills, and each blank's reason."""
    reading = json.loads(cells[RECEIPT])
    counts[reading["outcome"]] += 1
    if reading["outcome"] == REFUSED:
        counts[f"refused_{reading['reason']}"] += 1
    for column, field in zip(CELLS, ("agency", "rins", "federal_register"), strict=True):
        counts[f"with_{column}"] += cells[column] is not None
        if reading["outcome"] == READ and cells[column] is None:
            counts[f"{field}_{reading[field]['status']}"] += 1
