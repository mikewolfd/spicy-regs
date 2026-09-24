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

from collections.abc import Callable, Iterable, Iterator

from spicy_regs.sources.derived_text import DERIVED_STATUS
from spicy_regs.transforms.base import Transform
from spicy_regs.transforms.derived_text_pool import DerivedTextPool, TextResult, needs_comment_text


class EnrichCommentText(Transform):
    """Fills ``text_content`` from Mirrulations pre-extracted attachment text."""

    def __init__(self, pool: DerivedTextPool, *, observe: Callable[[TextResult], None] | None = None) -> None:
        self.pool = pool
        self.observe = observe

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        for result in self.pool.map(records, select=needs_comment_text):
            if self.observe is not None:
                self.observe(result)
            record = result.record
            if result.fill is not None:
                record = {
                    **record,
                    "text_content": result.fill.text,
                    "text_extraction_status": DERIVED_STATUS,
                    "pdf_extraction_results_json": result.fill.provenance,
                }
            yield record
