"""Transform: fill comment ``text_content`` from Mirrulations derived-data.

Sits after :class:`~spicy_regs.transforms.extract.ExtractRecords` in the comment
stream: for each flattened comment that has an attachment but no text yet, it
takes the attachment text Mirrulations already extracted (via
:class:`~spicy_regs.sources.derived_text.DerivedCommentText`) and sets
``text_content``, ``text_extraction_status = 'derived'`` and the provenance in
``pdf_extraction_results_json``.

This is the *primary* source of comment attachment text — free (the same
anonymous S3 bucket the ETL already reads), no PDF download or parsing, and
inline during staging. A comment with no derived text, or whose docket listing
or fetch failed, is left untouched (``text_extraction_status`` stays ``None``) so
the derived-text backfill and the PDF-download fallback (:mod:`spicy_regs.enrich_pdf`)
can still fill it.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from spicy_regs.sources.derived_text import DERIVED_STATUS, DerivedCommentText, DerivedTextUnavailable
from spicy_regs.transforms.base import Transform


class EnrichCommentText(Transform):
    """Fills ``text_content`` from Mirrulations pre-extracted attachment text."""

    def __init__(self, fetcher: DerivedCommentText) -> None:
        self.fetcher = fetcher

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        for record in records:
            # Only attachment-bearing comments have extractable text; comments
            # that already carry text (e.g. a re-run) are left as-is.
            if record.get("attachments_json") and record.get("text_content") is None:
                try:
                    fill = self.fetcher.fill_for(
                        record.get("agency_code"),
                        record.get("docket_id"),
                        record.get("comment_id"),
                    )
                except DerivedTextUnavailable:
                    fill = None  # logged by the fetcher; the comment stays pending
                if fill:
                    record = {
                        **record,
                        "text_content": fill.text,
                        "text_extraction_status": DERIVED_STATUS,
                        "pdf_extraction_results_json": fill.provenance,
                    }
            yield record
