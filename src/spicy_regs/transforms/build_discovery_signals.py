"""Transform: build the discovery *spike* signal rollup.

Of the four feed discovery signals, only **spike** scans ``documents.parquet``
in the browser (surge / closing / discussed ride ``comments_index`` /
``feed_summary``), so this bakes the per-agency spike into
``discovery_signals.parquet`` to stop the feed scanning the documents file on
every load. Spike = agencies whose document output in the last 30 days is ≥ 2×
their prior-year monthly mean, requiring ≥ 24 documents in the prior year to
avoid tiny-denominator noise; it is CURRENT_TIMESTAMP-relative and recomputed
every run, and the full qualifying set is baked (no LIMIT) so the UI applies
its own top-N. Future publisher dates do not contribute, windows use UTC
instants with explicit source offsets preserved, and the file's metadata
records the evaluation time, timezone and parent digest.
"""

from hashlib import file_digest
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


def build_discovery_signals(output_dir: Path) -> Path:
    """Build ``discovery_signals.parquet`` (the per-agency spike signal)."""
    import duckdb

    documents_file = output_dir / "documents.parquet"
    if not documents_file.exists():
        raise FileNotFoundError(f"documents.parquet not found in {output_dir}")

    logger.info("Building discovery signals (spike) via DuckDB...")

    out_file = output_dir / "discovery_signals.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute("SET TimeZone='UTC'")
    con.execute(f"SET temp_directory='{spill_dir}'")
    clock = con.execute("SELECT CURRENT_TIMESTAMP::VARCHAR, current_setting('TimeZone')").fetchone()
    assert clock is not None
    as_of, timezone = clock
    with documents_file.open("rb") as source:
        parent_digest = file_digest(source, "sha256").hexdigest()
    metadata = {
        "spicy_regs.discovery_signals.as_of": as_of,
        "spicy_regs.discovery_signals.timezone": timezone,
        "spicy_regs.discovery_signals.date_policy": "source offsets preserved; offset-free values use UTC",
        "spicy_regs.input.documents.sha256": f"sha256:{parent_digest}",
        "spicy_regs.input.documents.rows": str(pq.read_metadata(documents_file).num_rows),
    }

    query = """
    COPY (
        WITH per AS (
            SELECT agency_code,
              COUNT(*) FILTER (
                WHERE TRY_CAST(posted_date AS TIMESTAMPTZ) >= $as_of::TIMESTAMPTZ - INTERVAL '30' DAY
              ) AS recent_30d,
              COUNT(*) FILTER (
                WHERE TRY_CAST(posted_date AS TIMESTAMPTZ) >= $as_of::TIMESTAMPTZ - INTERVAL '13' MONTH
                  AND TRY_CAST(posted_date AS TIMESTAMPTZ) <  $as_of::TIMESTAMPTZ - INTERVAL '1' MONTH
              ) AS prior_yr
            FROM read_parquet($documents)
            WHERE TRY_CAST(posted_date AS TIMESTAMPTZ) >= $as_of::TIMESTAMPTZ - INTERVAL '13' MONTH
              AND TRY_CAST(posted_date AS TIMESTAMPTZ) <= $as_of::TIMESTAMPTZ
              AND agency_code IS NOT NULL
            GROUP BY 1
        )
        SELECT agency_code,
               recent_30d,
               (prior_yr / 12.0) AS baseline,
               (recent_30d / NULLIF(prior_yr / 12.0, 0)) AS ratio
        FROM per
        WHERE prior_yr >= 24
          AND (recent_30d / NULLIF(prior_yr / 12.0, 0)) >= 2.0
        ORDER BY ratio DESC
    ) TO $output (FORMAT PARQUET, COMPRESSION ZSTD, KV_METADATA $metadata);
    """
    con.execute(
        query, {"as_of": as_of, "documents": str(documents_file), "output": str(out_file), "metadata": metadata}
    )
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Discovery signals (spike): {:,} rows", rows)

    return out_file
