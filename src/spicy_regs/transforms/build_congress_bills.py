"""Transform: build ``congress_bills.parquet`` from the Congress.gov REST API.

Produces a 10-column all-VARCHAR schema keyed on ``bill_id`` (e.g.
``118-hr-1234``) from the ``/bill`` list payload alone — no per-bill detail
fetches — the legislative record complementary to the regulations.gov
``dockets``/``documents`` view.

**Incremental.** Best-effort download the prior table; fetch only bills updated
since its max ``update_date`` (minus a short overlap) through at most
:data:`MAX_WINDOW_DAYS` later, with the bounds sent to the API as
``fromDateTime``/``toDateTime``; dedup the union on ``bill_id``, preferring the
fresh row. No prior table means a full backfill, and chunks can be driven
explicitly with ``CONGRESS_SINCE``/``CONGRESS_UNTIL``. The window is capped
because a run publishes nothing until its walk finishes, so one unbounded
catch-up is all-or-nothing: the first 510-day attempt fetched 190,000 of
238,197 bills, hit the job timeout and persisted nothing, and capping lets each
run publish and advance the watermark so a deep backfill converges.

**Pooled, not warn-and-publish.** A table this repo asked for and did not
fully receive must never look like a completed run, and a walk that agrees
with its declared count proves only the row count: the list shifts while it is
read. :class:`~spicy_regs.sources.congress_bills.CongressBillsReader` pools
whole walks of the window by bill until one is clean or they hold exactly the
declared number of bills; a walk whose count moves mid-walk (a bill edited past
``toDateTime`` while the window is read) is spent and pooling restarts after
it. A query that does not settle within spicy-docs' pass bound, or any other
refusal, propagates and publishes nothing.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import CongressBillsReader
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

OUTPUT = "congress_bills.parquet"
NAME = "congress_bills"

# Re-scan this many days before the last stored update_date on each run, so bills
# updated after our previous run's cutoff are picked up.
OVERLAP_DAYS = 3

# Largest span one run will fetch. Sized off measured throughput (~3,050
# bills/min against this API) and the ~467 bills/day average across the 510-day
# freeze: ~90 days is ~42k bills, roughly 14 minutes a walk — inside the job
# timeout even when the pooled read spends every walk spicy-docs allows (four,
# about an hour) or a stretch runs several times denser than average.
MAX_WINDOW_DAYS = 90

# The published schema: 10 columns, all VARCHAR, in a fixed order. ``bill_id`` is
# the primary / dedup key.
#
# ``policy_area`` is intentionally omitted — the ``/bill`` list endpoint doesn't
# return it (it's a detail-endpoint-only field); it could be added later via a
# per-bill detail enrichment pass.
COLUMNS = (
    "bill_id",
    "congress",
    "bill_type",
    "bill_number",
    "title",
    "origin_chamber",
    "latest_action_date",
    "latest_action_text",
    "update_date",
    "url",
)


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (congress/number come as ints.)"""
    if value is None:
        return None
    return str(value)


def _bill_id(doc: dict) -> str | None:
    """Build ``{congress}-{type}-{number}`` (e.g. ``118-hr-1234``), or None."""
    congress = doc.get("congress")
    bill_type = doc.get("type")
    number = doc.get("number")
    if congress is None or not bill_type or number is None:
        return None
    return f"{congress}-{str(bill_type).lower()}-{number}"


def _shape(doc: dict) -> dict:
    """Map one raw Congress.gov bill onto the published column shape."""
    latest_action = doc.get("latestAction") or {}
    return {
        "bill_id": _bill_id(doc),
        "congress": _s(doc.get("congress")),
        "bill_type": (str(doc["type"]).lower() if doc.get("type") else None),
        "bill_number": _s(doc.get("number")),
        "title": doc.get("title"),
        "origin_chamber": doc.get("originChamber"),
        "latest_action_date": latest_action.get("actionDate"),
        "latest_action_text": latest_action.get("text"),
        "update_date": doc.get("updateDate"),
        "url": doc.get("url"),
        # The list route's url is the API resource, not the congress.gov page BILLSTATUS names.
        "url_source": "congress_api_list" if doc.get("url") else None,
    }


def _bounded_until(since: date | None, until: date | None, *, today: date | None = None) -> date | None:
    """Return a deterministic end date no more than one catch-up window ahead."""
    if since is None:
        return until
    if until is not None and until < since:
        raise ValueError(f"Congress until date {until} precedes since date {since}")
    return min(until or today or date.today(), since + timedelta(days=MAX_WINDOW_DAYS))


def _prior_max_update_date(prior_file: Path) -> date | None:
    """Largest ``update_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(update_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_congress_bills(output_dir: Path, *, since: date | None = None, until: date | None = None) -> Path:
    """Build ``congress_bills.parquet`` (incremental merge with the prior table)."""
    # 1. Pull the prior table (best effort — absence just means full backfill). Downloaded
    # to table_merge's own scratch path so merge_table() below reuses it in place instead
    # of downloading it again.
    prior_file = prior_scratch_path(output_dir, NAME)
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("Congress bills: merging against prior table {}", prior_file)
    else:
        logger.info("Congress bills: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_update_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    until = _bounded_until(since, until)
    logger.info(
        "Congress bills: fetching bills updated {} through {}",
        since or "the beginning",
        until or "now",
    )

    # 3. Fetch + shape the freshly fetched rows; the reader pools the window's walks.
    rows = [_shape(doc) for doc in CongressBillsReader(since=since, until=until).iter_records()]
    logger.info("Congress bills: fetched {:,} bills this run", len(rows))

    # 4. Merge through the contract, not through COLUMNS. This walk fills the
    # frozen ten; the bill family fills all forty-eight. Publishing at this
    # writer's width would rewrite the file 10 columns wide and delete the
    # other thirty-eight from every row — a ~0.97 byte ratio the R2 shrink
    # guard (0.5) does not notice. merge_contract_table publishes the
    # contract's shape and, for this table alone, merges column-wise so a NULL
    # here never overwrites a value the family put there. prior_present carries
    # what step 1 already found, so a cold start doesn't retry the same failed
    # download.
    return merge_contract_table(output_dir, NAME, rows, prior_present=have_prior)
