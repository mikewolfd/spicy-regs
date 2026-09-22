"""Transform: build the per-agency monthly document-volume rollup.

Split out of the former monolithic ``build_agency_rollups`` so it can be
materialized by its own decoupled rollup pipeline. Produces
``agency_monthly_volume.parquet`` — per-agency monthly document counts typed by
``document_type``, serving both the directory activity sparkline and the profile
activity panel.
Unusable dates, including year zero, are omitted from calendar buckets. The
Parquet metadata retains omitted counts and the exact parent-file digest.
"""

from hashlib import file_digest
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from loguru import logger


def build_agency_monthly_volume(output_dir: Path) -> Path:
    """Build ``agency_monthly_volume.parquet`` — per-agency monthly document
    count by ``document_type``.

    Emits a stable-schema empty artifact when ``documents.parquet`` isn't present
    yet, so the frontend can always read it.
    """
    import duckdb

    documents_file = output_dir / "documents.parquet"
    volume_file = output_dir / "agency_monthly_volume.parquet"

    if documents_file.exists():
        logger.info("Building agency monthly volume via DuckDB...")

        spill_dir = output_dir / ".duckdb_tmp"
        spill_dir.mkdir(exist_ok=True)

        con = duckdb.connect()
        con.execute("SET memory_limit='4GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute(f"SET temp_directory='{spill_dir}'")
        counts = con.execute(
            """
            SELECT COUNT(*),
                COUNT(*) FILTER (WHERE posted_date IS NULL),
                COUNT(*) FILTER (WHERE posted_date IS NOT NULL AND TRY_CAST(posted_date AS DATE) IS NULL),
                COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM TRY_CAST(posted_date AS DATE)) = 0)
            FROM read_parquet($documents)
            """,
            {"documents": str(documents_file)},
        ).fetchone()
        assert counts is not None
        input_rows, missing_dates, invalid_dates, year_zero_dates = counts
        omitted = missing_dates + invalid_dates + year_zero_dates
        with documents_file.open("rb") as source:
            parent_digest = file_digest(source, "sha256").hexdigest()
        metadata = {
            "spicy_regs.input.documents.sha256": f"sha256:{parent_digest}",
            "spicy_regs.input.documents.rows": str(input_rows),
            "spicy_regs.agency_monthly_volume.included_rows": str(input_rows - omitted),
            "spicy_regs.agency_monthly_volume.omitted_rows": str(omitted),
            "spicy_regs.agency_monthly_volume.missing_dates": str(missing_dates),
            "spicy_regs.agency_monthly_volume.invalid_dates": str(invalid_dates),
            "spicy_regs.agency_monthly_volume.year_zero_dates": str(year_zero_dates),
        }

        volume_query = """
        COPY (
            SELECT
                agency_code,
                EXTRACT(YEAR FROM TRY_CAST(posted_date AS DATE)) AS year,
                EXTRACT(MONTH FROM TRY_CAST(posted_date AS DATE)) AS month,
                document_type,
                COUNT(*) AS document_count
            FROM read_parquet($documents)
            WHERE posted_date IS NOT NULL
              AND TRY_CAST(posted_date AS DATE) IS NOT NULL
              AND EXTRACT(YEAR FROM TRY_CAST(posted_date AS DATE)) <> 0
            GROUP BY agency_code, year, month, document_type
            ORDER BY agency_code, year, month
        ) TO $output (FORMAT PARQUET, COMPRESSION ZSTD, KV_METADATA $metadata);
        """
        con.execute(volume_query, {"documents": str(documents_file), "output": str(volume_file), "metadata": metadata})
        con.close()
    else:
        # No documents yet — still emit the artifact with a stable schema so
        # the frontend can always read it.
        pl.DataFrame(
            [],
            schema={
                "agency_code": pl.Utf8,
                "year": pl.Int64,
                "month": pl.Int64,
                "document_type": pl.Utf8,
                "document_count": pl.Int64,
            },
        ).write_parquet(volume_file, compression="zstd")

    volume_rows = pq.ParquetFile(volume_file).metadata.num_rows
    logger.info("Agency monthly volume: {:,} rows", volume_rows)

    return volume_file
