"""Build the FEC committee reference table using one shared row mapping.

The fixed all-VARCHAR columns describe committees, not transactions or proven
organization affiliations. Candidate IDs and cycles remain JSON strings.

The default builder requires a completed unfiltered SpicyDocs traversal, merges
it with the prior R2 table and prefers fresh whole rows for matching committee
IDs. Prior-only rows remain observed coverage; the result is not a frozen
publisher snapshot. Raw capture evidence is retained separately, and failures
leave any previous output intact.

Call ``write_fec_committee_rows`` for an explicit retained-record slice. Its caller
owns input verification, provenance and coverage. That path makes no HTTP request
or publication and does not merge an independently moving prior table.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.fec_committees import FecCommitteesReader
from spicy_regs.transforms.parquet_rows import write_rows

OUTPUT = "fec_committees.parquet"

COLUMNS = (
    "committee_id",
    "name",
    "committee_type",
    "committee_type_full",
    "designation",
    "designation_full",
    "party",
    "party_full",
    "state",
    "treasurer_name",
    "organization_type_full",
    "filing_frequency",
    "first_file_date",
    "last_file_date",
    "cycles_json",
    "candidate_ids_json",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _json_array(value: object) -> str:
    """Serialize a list-ish field to a JSON string, defaulting to ``[]``."""
    return json.dumps(value if isinstance(value, list) else [])


def _shape(doc: dict) -> dict:
    """Map one raw OpenFEC committee onto the published column shape."""
    return {
        "committee_id": doc.get("committee_id"),
        "name": doc.get("name"),
        "committee_type": doc.get("committee_type"),
        "committee_type_full": doc.get("committee_type_full"),
        "designation": doc.get("designation"),
        "designation_full": doc.get("designation_full"),
        "party": doc.get("party"),
        "party_full": doc.get("party_full"),
        "state": doc.get("state"),
        "treasurer_name": doc.get("treasurer_name"),
        "organization_type_full": doc.get("organization_type_full"),
        "filing_frequency": doc.get("filing_frequency"),
        "first_file_date": doc.get("first_file_date"),
        "last_file_date": doc.get("last_file_date"),
        "cycles_json": _json_array(doc.get("cycles")),
        "candidate_ids_json": _json_array(doc.get("candidate_ids")),
    }


def write_fec_committee_rows(records: Iterable[dict], destination: Path, *, batch_size: int = 2_000) -> Path:
    """Map selected raw OpenFEC rows; replace the output only after full consumption.

    Memory grows with one batch of source rows. Use the fixed Arrow schema
    directly: the ontology writer's scalar coercion would change type handling.
    This preserves source order and duplicates; the default builder deduplicates
    after combining its fresh rows with the prior table.
    """
    return write_rows((_shape(doc) for doc in records), destination, _SCHEMA, batch_size=batch_size)


def build_fec_committees(output_dir: Path, *, capture_dir: Path | None = None) -> Path:
    """Merge a completed walk; retain evidence under ``FEC_CAPTURE_DIR`` or locally.

    An explicit ``capture_dir`` wins over the environment setting. Otherwise the
    default is ``output_dir/.fec-captures``; callers should select durable storage
    when their build directory is temporary.
    """
    import duckdb

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_fec_prior.parquet"

    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("FEC committees: merging against prior table {}", prior_file)
    else:
        logger.info("FEC committees: no prior table found — clean build")

    reader = FecCommitteesReader(
        capture_dir=capture_dir or Path(os.environ.get("FEC_CAPTURE_DIR", output_dir / ".fec-captures"))
    )
    # Closing also handles a row-shaping/Arrow failure while acquisition is
    # suspended at a yield, leaving its run marked incomplete and HTTP closed.
    with closing(reader.iter_records()) as records:
        new_file = write_fec_committee_rows(records, output_dir / "_fec_new.parquet")
    logger.info("FEC committees: fetched {:,} committees this run", pq.ParquetFile(new_file).metadata.num_rows)

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    cols = ", ".join(COLUMNS)
    prior_sql = str(prior_file).replace("'", "''")
    new_sql = str(new_file).replace("'", "''")
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_sql}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_sql}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_sql}')"

    with TemporaryDirectory(dir=output_dir) as temporary, duckdb.connect() as con:
        staged = Path(temporary) / OUTPUT
        staged_sql = str(staged).replace("'", "''")
        con.execute("SET memory_limit='4GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute("SET temp_directory=?", [str(spill_dir)])
        con.execute(f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY committee_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE committee_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY committee_id
        ) TO '{staged_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """)
        staged.replace(out_file)

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FEC committees: {:,} rows", total)
    return out_file
