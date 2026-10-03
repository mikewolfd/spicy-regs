"""Postings Regulations.gov removed: the documents and comments a derived count leaves out.

``documents.publisher_status`` is ``removed`` where the publisher answered 404 or 410 for a document its docket's
listing no longer names (``pipelines.docket_reconcile``). The document stays held in ``documents``, and its comments
in ``comments`` and ``comments_index``; the derived counts leave out both, so a posting the publisher moved is not
counted twice. Before the first reconcile publishes, ``documents`` has no such column and nothing is left out.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.public_url import comments_source

REMOVED = "removed"


def _quoted(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _literals(values: set[str]) -> str:
    """Constant SQL strings, so the scan prunes row groups by the column's statistics."""
    return ", ".join(f"'{_quoted(value)}'" for value in sorted(values))


def counted_documents(documents_file: Path) -> str:
    """A relation of the held documents a derived count keeps: every one but a removed one."""
    source = f"read_parquet('{_quoted(documents_file)}')"
    if "publisher_status" not in pq.ParquetFile(documents_file).schema_arrow.names:
        return source
    return f"(SELECT * FROM {source} WHERE publisher_status IS DISTINCT FROM '{REMOVED}')"


def removed_comments(con: duckdb.DuckDBPyConnection, documents_file: Path, output_dir: Path) -> str:
    """Name a temp table of the comments held on removed documents, counted by ``agency_code`` and ``docket_id``.

    It reads no comments when nothing is removed or no documents are held yet. Otherwise it reads three columns of
    the comments export, only in the removed documents' agencies: the export is written agency by agency (3,762 of
    its 3,844 row groups held one agency on 2026-10-03), so FNA's comments on one document took 1.0 s over the
    public URL against 51 s for the whole column. A comment filed under an agency other than its document's is
    missed: 11 of 26,387,876 held comments naming a held document, on 2 documents (receipt
    round6/impl-C1/comment_agency_vs_document_agency.out).
    """
    removed = []
    if documents_file.exists() and "publisher_status" in pq.ParquetFile(documents_file).schema_arrow.names:
        removed = con.execute(
            "SELECT DISTINCT agency_code, document_id FROM read_parquet(?) WHERE publisher_status = ?",
            [str(documents_file), REMOVED],
        ).fetchall()
    con.execute("CREATE OR REPLACE TEMP TABLE removed_comments (agency_code VARCHAR, docket_id VARCHAR, n BIGINT)")
    if removed:
        comments = comments_source(output_dir)
        if comments.startswith("https://"):
            load_public_http(con)
        agencies = {agency for agency, _ in removed}
        in_agencies = f"agency_code IN ({_literals(agencies)}) AND" if None not in agencies else ""
        con.execute(
            f"""INSERT INTO removed_comments
                SELECT agency_code, trim(docket_id, '"'), count(*) FROM read_parquet('{_quoted(comments)}')
                WHERE {in_agencies} comment_on_document_id IN ({_literals({d for _, d in removed})})
                GROUP BY ALL"""
        )
    return "removed_comments"
