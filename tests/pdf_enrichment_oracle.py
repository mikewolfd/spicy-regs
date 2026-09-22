"""Frozen oracle for a row's per-PDF text/status aggregation at ee933b7; test-only, independent of new storage."""

from enum import Enum

PAGE_SEPARATOR = "\n\n"


class PdfTextStatus(str, Enum):
    OK = "ok"
    EMPTY = "empty"
    ENCRYPTED = "encrypted"
    ERROR = "error"


def _combine(results: list[tuple[str | None, str]]) -> tuple[str | None, str]:
    """Combine one row's per-PDF results into ``(text_or_None, status)``.

    A row can have more than one PDF rendition; their texts are concatenated.
    The combined status is ``ok`` if any PDF yielded text, otherwise ``error``
    > ``encrypted`` > ``empty`` in that order of informativeness.
    """
    texts = [text for text, _ in results if text]
    if texts:
        return PAGE_SEPARATOR.join(texts), PdfTextStatus.OK.value
    statuses = [status for _, status in results]
    for candidate in (PdfTextStatus.ERROR.value, PdfTextStatus.ENCRYPTED.value, PdfTextStatus.EMPTY.value):
        if candidate in statuses:
            return None, candidate
    return None, PdfTextStatus.EMPTY.value
