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
import time
from collections.abc import Generator, Iterable
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.transforms.table_merge import merge_local_prior

if TYPE_CHECKING:
    import httpx

API_BASE = "https://api.open.fec.gov/v1"
PER_PAGE = 100
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "FEC_API_KEY", "REGULATIONS_GOV_API_KEY")
_MAX_PAGES = 5_000
_PROGRESS_EVERY = 5_000
# OpenFEC states this key's quota as X-RateLimit-Limit: 60 per rolling minute
# (2026-09-25); runs 36049323173 and 36180816422 paced at 0.25 s got exactly 60
# pages per clock minute, then HTTP 429. SpicyDocs through 0.33.2 does not pace to the
# header, so start requests 1.1 s apart: about 55 a minute, under the window.
_MIN_INTERVAL = 1.1
# ~900 pages at that pace take ~17 minutes; the rollup job is killed at 30
# (_rollup.yml). Stop at 25 so an overrun leaves an incomplete attempt, not a
# killed job, and nothing is published.
_DEADLINE_SECONDS = 25 * 60

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


def _resolve_api_key() -> str | None:
    """Resolve the repository's shared api.data.gov key without logging it."""
    return next((os.environ[var] for var in API_KEY_ENV_VARS if os.environ.get(var)), None)


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


def iter_fec_committee_records(
    *,
    per_page: int = PER_PAGE,
    max_pages: int | None = None,
    api_key: str | None = None,
    capture_dir: Path | None = None,
    transport: httpx.BaseTransport | None = None,
    min_interval: float = _MIN_INTERVAL,
) -> Generator[dict, None, None]:
    """Yield native committee metadata from one SpicyDocs ``FecClient`` traversal.

    SpicyDocs owns pagination, HTTP retries, header-only credentials and exact raw
    response captures. This adapter selects the complete committee registry, checks
    that identifiers advance without duplication, and retains a per-run page index.
    Only normal iterator exhaustion completes an acquisition. Missing credentials,
    page bounds, changed exact counts, failed requests and a walk still unfinished
    at the run deadline refuse the run; each attempt restarts from page one.

    Set ``FEC_CAPTURE_DIR`` or pass ``capture_dir`` to keep raw evidence outside the
    build directory. Each attempt gets its own directory; incomplete attempts remain
    marked incomplete. Source traversal is not a frozen publisher snapshot.
    """
    if type(per_page) is not int or per_page <= 0:
        raise ValueError("per_page must be a positive integer")
    if max_pages is not None and (type(max_pages) is not int or max_pages <= 0):
        raise ValueError("max_pages must be a positive integer")
    per_page = min(per_page, PER_PAGE)
    bound = _MAX_PAGES if max_pages is None else min(max_pages, _MAX_PAGES)
    api_key = api_key if api_key is not None else _resolve_api_key()
    if not api_key:
        raise ValueError("FEC committees require an API key; no complete acquisition was attempted")
    try:
        from spicy_docs.sources.fec.client import FecClient
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "FEC committees require spicy-regs[source-readers]; run `uv sync --frozen`."
            ) from None
        raise

    def records() -> Generator[dict, None, None]:
        capture_path = Path(capture_dir or os.environ.get("FEC_CAPTURE_DIR", ".fec-captures"))
        capture_path.mkdir(parents=True, exist_ok=True)
        run_path = Path(mkdtemp(prefix="committees-", dir=capture_path))
        state = {
            "status": "incomplete",
            "source": API_BASE + "/committees/",
            "params": {"sort": "committee_id", "per_page": per_page, "page": 1},
            "scope": "unfiltered source traversal; no frozen publisher snapshot",
            "max_pages": bound,
            "pages": 0,
            "records": 0,
            "declared_exact_count": None,
        }
        (run_path / "run.json").write_text(json.dumps(state, indent=2) + "\n")
        previous_id = None
        deadline = time.monotonic() + _DEADLINE_SECONDS
        try:
            with (
                FecClient(
                    store=run_path / "blobs",
                    api_key=api_key,
                    # Includes room for the provider's three attempts and redirects;
                    # exhausting either request or page budget refuses acquisition.
                    max_requests=bound * 12,
                    min_interval=min_interval,
                    transport=transport,
                ) as client,
                (run_path / "pages.jsonl").open("w") as index,
            ):
                for page in client.api(
                    "/v1/committees/",
                    params={"sort": "committee_id", "per_page": per_page, "page": 1},
                    max_pages=bound,
                ):
                    state["pages"] += 1
                    index.write(
                        json.dumps(
                            {
                                key: page[key]
                                for key in (
                                    "request_url",
                                    "resolved_url",
                                    "observed_at",
                                    "media_type",
                                    "via",
                                    "evidence",
                                    "next_url",
                                )
                            }
                            | {"records": len(page["records"])}
                        )
                        + "\n"
                    )
                    index.flush()
                    pagination = page["pagination"]
                    if pagination.get("is_count_exact") is True:
                        count = pagination.get("count")
                        if type(count) is not int or count < 0:
                            raise ValueError("FEC exact committee count must be a nonnegative integer")
                        if state["declared_exact_count"] not in (None, count):
                            raise ValueError("FEC exact committee count changed during traversal")
                        state["declared_exact_count"] = count
                    for observation in page["records"]:
                        record = observation["metadata"]
                        if not isinstance(record, dict):
                            raise ValueError("FEC committee record must be an object")
                        committee_id = record.get("committee_id")
                        if not isinstance(committee_id, str) or not committee_id:
                            raise ValueError("FEC committee record omitted its identifier")
                        if previous_id is not None and committee_id <= previous_id:
                            raise ValueError("FEC committee identifiers repeated or ceased increasing")
                        previous_id = committee_id
                        state["records"] += 1
                        if state["records"] % _PROGRESS_EVERY == 0:
                            logger.info("FEC committees: {:,} observed so far", state["records"])
                        yield record
                    if page["next_url"] is not None and time.monotonic() > deadline:
                        raise TimeoutError(
                            f"FEC committees traversal passed its {_DEADLINE_SECONDS // 60}-minute deadline "
                            f"after {state['pages']:,} pages; the attempt is incomplete"
                        )
                if state["declared_exact_count"] not in (None, state["records"]):
                    raise ValueError("FEC observed committees disagree with the declared exact count")
                state["status"] = "complete"
        finally:
            (run_path / "run.json").write_text(json.dumps(state, indent=2) + "\n")
        logger.info("FEC committees: {:,} observed; evidence at {}", state["records"], run_path)

    return records()


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

    records = iter_fec_committee_records(
        capture_dir=capture_dir or Path(os.environ.get("FEC_CAPTURE_DIR", output_dir / ".fec-captures"))
    )
    # Closing also handles a row-shaping/Arrow failure while acquisition is
    # suspended at a yield, leaving its run marked incomplete and HTTP closed.
    with closing(records) as source:
        new_file = write_fec_committee_rows(source, output_dir / "_fec_new.parquet")
    logger.info("FEC committees: fetched {:,} committees this run", pq.ParquetFile(new_file).metadata.num_rows)

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    with TemporaryDirectory(dir=output_dir) as temporary, duckdb.connect() as con:
        staged = Path(temporary) / OUTPUT
        con.execute("SET memory_limit='4GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute("SET temp_directory=?", [str(spill_dir)])
        merge_local_prior(
            con,
            columns=COLUMNS,
            identity="committee_id",
            order_by="committee_id",
            prior_file=prior_file if have_prior else None,
            new_file=new_file,
            out_file=staged,
        )
        staged.replace(out_file)

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FEC committees: {:,} rows", total)
    return out_file
