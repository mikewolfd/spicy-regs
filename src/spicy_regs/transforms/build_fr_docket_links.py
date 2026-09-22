"""Transform: build ``fr_docket_links.parquet``, the Federal Register ↔ docket link table.

Explodes each FR document's ``docket_ids_json`` array into one row per
(docket_id, document_number, publication_date) carrying the display columns the
docket page needs, sorted by ``docket_id`` so ``WHERE docket_id = ?`` prunes row
groups instead of scanning; it replaces the ``docket_ids_json LIKE '%"<id>"%'``
full-scan the docket page used to run on every load. Reading the published
``federal_register.parquet`` as a base input, the explosion preserves the exact
matching semantics of the old ``LIKE``, including the quirk where a few array
elements join two IDs.
"""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


def build_fr_docket_links(output_dir: Path) -> Path:
    """Build ``fr_docket_links.parquet`` (exploded FR→docket links + display cols)."""
    import duckdb

    fr_file = output_dir / "federal_register.parquet"
    if not fr_file.exists():
        raise FileNotFoundError(f"federal_register.parquet not found in {output_dir}")

    logger.info("Building FR docket links via DuckDB...")

    out_file = output_dir / "fr_docket_links.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # Carries the columns the docket page's FR section renders (normalizeFRRow
    # rebuilds `docket_ids` from docket_ids_json, so keep that column). Sorted by
    # docket_id with a small row-group size so per-docket lookups prune.
    query = f"""
    COPY (
        SELECT
            link.docket_id AS docket_id,
            fr.document_number,
            fr.title,
            fr.abstract,
            fr.document_type,
            fr.subtype,
            fr.publication_date,
            fr.effective_on,
            fr.comments_close_on,
            fr.signing_date,
            fr.agency_slugs,
            fr.docket_ids_json,
            fr.regulation_id_numbers_json,
            fr.html_url,
            fr.pdf_url,
            fr.executive_order_number
        FROM read_parquet('{fr_file}') fr,
             UNNEST(CAST(json_extract(fr.docket_ids_json, '$') AS VARCHAR[])) AS link(docket_id)
        WHERE fr.docket_ids_json IS NOT NULL
          AND link.docket_id IS NOT NULL
          AND TRIM(link.docket_id) <> ''
        ORDER BY docket_id, fr.publication_date DESC
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """
    con.execute(query)
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FR docket links: {:,} rows", rows)

    return out_file
