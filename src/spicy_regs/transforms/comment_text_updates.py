"""Stream shared enrichment results into bounded, text-only Parquet updates."""

from collections.abc import Callable, Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.enrich_pdf import TEXT_UPDATES
from spicy_regs.sources.derived_text import DERIVED_STATUS
from spicy_regs.transforms.derived_text_pool import TextResult

UPDATES_SCHEMA = pa.schema([("comment_id", pa.string()), *((name, pa.string()) for name in TEXT_UPDATES.values())])
FLUSH_CHARACTERS = 64 * 1024 * 1024


def text_update_stats() -> dict[str, int]:
    """Shared accounting: missing means a valid empty read; failed means unknown."""
    return dict.fromkeys(("selected", "derived", "missing", "failed", "failed_dockets"), 0)


def write_text_updates(
    results: Iterable[TextResult], path: Path, *, observe: Callable[[TextResult], None] | None = None,
) -> dict[str, int]:
    """Write completed fills only; failures stay distinct from successful empty reads."""
    stats = text_update_stats()
    refused: set[tuple[str, str]] = set()
    rows: list[tuple[str, str, str, str]] = []
    characters = 0
    with pq.ParquetWriter(path, UPDATES_SCHEMA, compression="zstd") as writer:
        def flush() -> None:
            if rows:
                writer.write_table(pa.Table.from_arrays(list(zip(*rows, strict=True)), schema=UPDATES_SCHEMA))
                rows.clear()

        try:
            for result in results:
                if observe is not None:
                    observe(result)
                if result.status == "skipped":
                    continue
                stats["selected"] += 1
                stats[result.status] += 1
                if result.error is not None and result.error.phase == "listing":
                    refused.add((result.record["agency_code"], result.record["docket_id"]))
                if result.fill is not None:
                    rows.append((result.record["comment_id"], result.fill.text, DERIVED_STATUS, result.fill.provenance))
                    characters += len(result.fill.text)
                    if characters >= FLUSH_CHARACTERS:
                        flush()
                        characters = 0
        finally:
            flush()
    stats["failed_dockets"] = len(refused)
    return stats
