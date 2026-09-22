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

**Refuse-and-retry replaces warn-and-publish.** The spicy-docs reader this
module fetches through (see :mod:`spicy_regs.sources.congress_bills`) raises
``PagedJsonSourceError`` and publishes nothing the moment its declared and
observed counts disagree, at any point in the walk — a window this repo asked
for and did not fully receive must never look like a completed run. But a
nightly window closing at "now" is walked over minutes, and a bill edited
mid-walk moves its ``updateDate`` past ``toDateTime`` and shrinks the declared
count out from under a request in flight, which is a transient publisher-side
race rather than a truncated walk. :func:`_fetch_bills` therefore re-asks the
identical window up to :data:`FETCH_ATTEMPTS` times with a pause between
attempts — but only when the refusal's context names both a ``declaredCount``
and an ``observedCount`` (the drift shape); a permanent refusal (malformed
JSON, a bad date parameter, a 404) propagates on the first attempt, and a drift
that survives every attempt propagates too, so a table never walked in full is
never published.
"""

from __future__ import annotations

import time
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
# freeze: ~90 days is ~42k bills, roughly 14 minutes — comfortably inside the
# job timeout even if a stretch runs several times denser than average.
MAX_WINDOW_DAYS = 90

# How many times _fetch_bills asks the same window for before giving up and
# propagating the reader's refusal. Three total attempts: the first is the
# ordinary case, the second absorbs a single mid-walk publisher edit (the
# expected shape of the drift below), and the third is headroom for an
# unlucky repeat rather than a promise every drift resolves in one retry.
FETCH_ATTEMPTS = 3

# Paused between attempts so the publisher's own state (and this repo's
# window, which is wall-clock "now"-relative on every run) has a moment to
# settle rather than re-asking an in-flux window immediately. Short relative
# to one walk (~14 minutes at MAX_WINDOW_DAYS) and to a nightly job's budget.
RETRY_PAUSE_SECONDS = 30.0

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
    }


def _bounded_until(since: date | None, until: date | None, *, today: date | None = None) -> date | None:
    """Return a deterministic end date no more than one catch-up window ahead."""
    if since is None:
        return until
    if until is not None and until < since:
        raise ValueError(f"Congress until date {until} precedes since date {since}")
    return min(until or today or date.today(), since + timedelta(days=MAX_WINDOW_DAYS))


def _fetch_bills(since: date | None, until: date | None) -> list[dict]:
    """Fetch + shape one window's bills, retrying a mid-walk publisher drift.

    ``CongressBillsReader.iter_records()`` refuses — raises
    ``PagedJsonSourceError`` — the moment the publisher's declared and
    observed counts disagree; that refusal is correct and this function never
    weakens it. What it adds is the retry *policy*: a fresh reader asks the
    identical ``since``/``until`` window again, up to :data:`FETCH_ATTEMPTS`
    times total with :data:`RETRY_PAUSE_SECONDS` between attempts, before the
    refusal propagates. Each attempt starts a brand-new walk from scratch —
    a partially-yielded generator cannot be resumed, and a refused walk's
    partial rows are exactly the truncated-table shape this repo refuses to
    publish, so they are discarded, not kept.

    Retried only when the refusal actually looks like a drift: its
    ``paged_json_acquisition`` context names both a ``declaredCount`` and an
    ``observedCount`` — the shape of "the declared count changed mid-walk" and
    "declared and observed disagree at the end", the two ways a publisher's
    state can move out from under a request already in flight. Every other
    ``PagedJsonSourceError`` — malformed JSON, a bad date parameter, a 404 —
    is permanent: retrying it would only burn ``FETCH_ATTEMPTS`` pauses under
    a misleading "walk refused, retrying" log line before failing the same
    way it would have on the first attempt, so it propagates immediately.
    """
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        reader = CongressBillsReader(since=since, until=until)
        try:
            return [_shape(doc) for doc in reader.iter_records()]
        except Exception as error:
            # Deferred so a keyless run — CongressBillsReader.iter_records()
            # returns before ever importing spicy-docs — never needs the
            # optional source-readers extra either, matching the reader's own
            # lazy-import discipline. By the time a PagedJsonSourceError can
            # actually occur, the reader has already imported this module
            # itself in order to raise it.
            from spicy_docs.reading.paged_json import PagedJsonSourceError

            if not isinstance(error, PagedJsonSourceError):
                raise
            context = getattr(error, "paged_json_acquisition", None) or {}
            declared, observed = context.get("declaredCount"), context.get("observedCount")
            if declared is None or observed is None:
                raise
            logger.warning(
                "Congress bills: walk refused on attempt {}/{} (declared={}, observed={}): {}",
                attempt,
                FETCH_ATTEMPTS,
                declared,
                observed,
                error,
            )
            if attempt == FETCH_ATTEMPTS:
                raise
            time.sleep(RETRY_PAUSE_SECONDS)
    raise AssertionError("unreachable")  # the loop above always returns or raises


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

    # 3. Fetch + shape the freshly fetched rows, retrying a mid-walk drift.
    rows = _fetch_bills(since, until)
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
