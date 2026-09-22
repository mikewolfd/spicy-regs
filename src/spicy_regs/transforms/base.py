"""Base class for record-stream transforms.

A ``Transform`` sits between a Reader and a Writer in the record-stream
vocabulary (``Reader.iter_records() -> Transform.apply(...) -> Writer.write(...)``)
and maps, filters, or enriches records lazily, one at a time, so it never
buffers the whole dataset. Bulk, whole-dataset operations — deduplicating by
key, partitioning, building summaries — are deliberately *not* Transforms:
they live in the columnar bulk modules of :mod:`spicy_regs.transforms`.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator


class Transform(ABC):
    """Maps a stream of records to another stream of records."""

    @abstractmethod
    def apply(self, records: Iterable[dict]) -> Iterator[dict]: ...
