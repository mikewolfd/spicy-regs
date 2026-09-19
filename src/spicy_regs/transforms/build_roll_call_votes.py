"""Transform: build ``roll_call_votes.parquet`` and ``member_votes.parquet``.

Two steps, from two different publishers, because neither alone is enough:

1. **Linkage, from two publisher statements.** The Congress.gov ``house-vote``
   list route states which bill a House roll call was about, and a bill's own
   actions carry ``recordedVotes`` entries naming the roll calls they settled.
   ``house_vote_references`` reads the first off the listing; the second
   arrives with BILLSTATUS, so the ``bill-family`` rollup — which already holds
   every bill's actions — publishes them as ``bill_vote_references`` and this
   transform reads that table at merge time. ``index_vote_references`` settles
   the cases where two references claim the same roll call, so a bill linkage
   carries the rule that produced it and a count of what it beat.

   The recorded-vote references are indexed **first**, so they win a
   disagreement: a recorded vote sits on the bill's own action, which *is* the
   join, while the listing's ``legislationType``/``legislationNumber`` is a
   statement made about the roll call from outside it
   (``spicy_docs.interpretation.vote_matching``'s own reasoning). A losing
   reference is counted on the row it disagreed with, never dropped.
2. **Counts and positions.** The tallies and every member's position come from
   the House Clerk's own EVS XML through ``spicy_docs.sources.congress.votes``,
   one keyless request per roll call. BillTrax published zeroes in these four
   columns; they are real here or they are NULL.

**Scope is House only, and what is fetched is the House half of the union.**
The fetch set is every House roll call in the index — the listing's own walk,
plus any House roll call a bill's action names that the listing has not
indexed yet. It is a superset of the listing, never a subset, so the coverage
claim this table makes ("every House roll call the publisher's index names,
under the per-run cap") stays true, and a vote that arrives on a bill's action
first is published a cron early rather than missed. Such a row is not partial:
the Clerk file is addressable from the roll-call key alone, so it carries the
same tally and member positions as any other, or it is counted as refused.

The Senate half of that union is **not** fetched, and the asymmetry is the
point. For the House the references can only add to a complete enumeration.
For the Senate there is no enumeration at all — ``listing.py`` has no Senate
index route, and the LIS menu is not a reader this repository has — so the
Senate rows would *be* whatever the scoped bills happened to reference: 6 of
the 34 roll calls in the measured sample, a biased sample that would read as a
Senate vote table. Publishing that would state a coverage this has not got, so
no Senate row is published at all, and the Senate references sit in the index
unused until a Senate index route lands. They cost nothing.

**Why the references are read rather than re-derived.** Reaching the same six
fields inside this rollup would mean re-acquiring every scoped bill's
BILLSTATUS — up to ~52 MB of bulk zip per run, or thousands of per-bill API
calls — to recompute what the family already parsed an hour earlier. The read
is the cheaper and the more consistent of the two, and it is the one the
rollup contract permits (``pipelines/rollups/base.py``): ``bill_vote_references``
is an *ingest* rollup's published output, with no upstream dependency inside
this repository, so reading it is an ordering preference the crons already
honour rather than a race. It is read best-effort and is not declared in
``inputs``: with no family run yet published, the listing's own linkage still
fills every House row this rollup can establish.

**Incremental.** A roll call already published with a real tally has had its
Clerk file read, and that file does not change once the vote is recorded, so it
is not fetched again — a steady-state run costs the index walk plus the
overlap, not one request per roll call in the Congress. Two honest limits on
that:

* The contract has no column for the publisher's ``updateDate``, so a
  correction to an already-published roll call cannot be detected by comparing
  it. :data:`OVERLAP_VOTES` stands in for that comparison: the newest few
  roll calls are re-read every run whether or not they are held, which is the
  same shape as ``build_congress_bills``' day overlap.
* The index walk itself is **not** short-circuited. The ``house-vote`` route
  declares ``sort_honored=False``, so the publisher's order is not guaranteed
  monotonic in roll number, and stopping on a page of already-held votes could
  silently drop roll calls sitting later in an unordered listing. The walk is a
  handful of 250-row pages per session; the per-roll-call Clerk fetch is the
  cost worth avoiding, and that is the one avoided.

``MAX_VOTES_PER_RUN`` still bounds the Clerk walk: a run publishes nothing
until it finishes, so an unbounded first pass over several Congresses would
time out and persist nothing, the failure mode ``build_congress_bills``
documents. Votes are acquired newest first, so a bounded run advances from the
present and the next run resumes on ground this one did not reach.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from loguru import logger
from spicy_docs.interpretation.vote_matching import (
    VOTE_CHAMBERS,
    VoteKey,
    VoteMatchError,
    VoteReference,
    house_vote_references,
    index_vote_references,
    match_votes,
)
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.congress_activity_tables import shape_member_vote, shape_roll_call_vote
from spicy_docs.sources.congress.bill_status import BillIdentity, BillSourceError
from spicy_docs.sources.congress.listing import (
    LIST_ROUTES,
    MAX_LIMIT,
    CongressListingReader,
    list_route_url,
)
from spicy_docs.sources.congress.votes import VoteAcquirer, VoteBudget, VoteLocator, VoteSourceError

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.build_bill_family import VOTE_REFERENCES_TABLE
from spicy_regs.transforms.congress_scope import congresses_from_env, sessions_of
from spicy_regs.transforms.congress_walk import ListingSource
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    from spicy_docs.sources.congress.votes import Chamber


class VoteSource(Protocol):
    """What this transform needs of a roll-call acquirer."""

    def acquire(self, locator: Any, *, crosswalk: Any = ...) -> Any: ...


LIST_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: One request per roll call, paced at two a second against the Clerk.
VOTE_BUDGET = VoteBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,  # the acquirer's own ceiling for a roll-call file
    timeout_seconds=60.0,
    min_request_interval_seconds=0.5,
)

MAX_PAGES = 40

#: ~700 House roll calls a session, so this covers a full Congress in one run
#: (~12 minutes at the pacing above) and bounds a multi-Congress backfill.
MAX_VOTES_PER_RUN = 1_500

#: The newest roll calls are re-read every run even when already published,
#: because the contract carries no publisher ``updateDate`` to compare and a
#: late correction would otherwise never be picked up.
OVERLAP_VOTES = 25

NAME = "roll_call_votes"
OUTPUT = "roll_call_votes.parquet"

#: The columns the reference table's rows are read back through. Each is a
#: field of the ``VoteReference`` the family wrote, so nothing is re-derived.
REFERENCE_COLUMNS = ("bill_id", "chamber", "congress", "session", "roll_number", "action_index", "url", "date")


def _bill_identity(bill_id: str) -> BillIdentity:
    """``119-hr-6028`` back to the identity the reference named, refusing anything else.

    The key is written by ``schemas.tables.bill_id`` and read back here, which
    is the one place this repository parses it. A key that does not split into
    three parts is a row this rollup cannot use, and saying so beats matching a
    vote to a bill that was never named.
    """
    parts = bill_id.split("-")
    if len(parts) != 3:
        raise BillSourceError(f"bill_id {bill_id!r} is not congress-type-number")
    congress, bill_type, number = parts
    return BillIdentity(congress=int(congress), bill_type=bill_type, number=int(number))


def _recorded_vote_references(
    output_dir: Path, congresses: Sequence[int], download_prior: Callable[[str, Path], bool]
) -> tuple[VoteReference, ...]:
    """The family's published references for the Congresses in scope, or ``()`` when none are published.

    Scoped in DuckDB rather than read whole: the table grows one row per
    recorded vote per action across every Congress the family has run, and this
    rollup only ever matches the ones its own listing walk reached.

    ``ORDER BY`` makes the first-wins tie-break a stated rule rather than the
    scan's accident. ``index_vote_references`` keeps the first reference it
    sees for a vote, so when one roll call is named by two of a bill's actions
    — the ordinary case, since the publisher records a passage vote on both the
    passage action and the motion to reconsider — the winner is the lowest
    ``action_index``, which is the earlier action. Ordering by the identity
    columns makes that reproducible across runs.
    """
    path = published_table(output_dir, VOTE_REFERENCES_TABLE, download_prior)
    if path is None:
        logger.info(
            "Roll-call votes: no published {} table — the listing's own linkage is the only one this run",
            VOTE_REFERENCES_TABLE,
        )
        return ()
    import duckdb

    columns = ", ".join(REFERENCE_COLUMNS)
    rows = duckdb.sql(
        f"SELECT {columns} FROM read_parquet('{path}') "
        "WHERE congress IN (SELECT UNNEST(?)) "
        "ORDER BY congress, chamber, session, roll_number, action_index, bill_id",
        params=[[str(congress) for congress in sorted(congresses)]],
    ).fetchall()
    path.unlink(missing_ok=True)

    references: list[VoteReference] = []
    refused = 0
    for bill_id, chamber, congress, session, roll_number, action_index, url, date in rows:
        try:
            references.append(
                VoteReference(
                    vote=VoteKey(
                        congress=int(congress), chamber=str(chamber), session=int(session), roll_number=int(roll_number)
                    ),
                    bill=_bill_identity(str(bill_id)),
                    rule="bill_action_recorded_vote",
                    url=None if url is None else str(url),
                    date=None if date is None else str(date),
                    action_index=None if action_index is None else int(action_index),
                )
            )
        except (ValueError, TypeError, VoteMatchError, BillSourceError) as error:
            refused += 1
            logger.warning("Roll-call votes: reference row skipped: {}", error)
    if refused:
        logger.warning("Roll-call votes: {:,} published reference row(s) unreadable", refused)
    logger.info("Roll-call votes: {:,} recorded-vote references from the bill family", len(references))
    return tuple(references)


def _held_votes(prior_file: Path) -> set[tuple[str, ...]]:
    """Roll calls already published *with a tally*, keyed as the contract keys them.

    A row without a tally is a linkage-only row — the reference landed but the
    Clerk file did not — so it is not "held" and must be tried again.
    """
    if not prior_file.exists():
        return set()
    import duckdb

    rows = duckdb.sql(
        f"SELECT congress, chamber, session, roll_number FROM read_parquet('{prior_file}') WHERE yea IS NOT NULL"
    ).fetchall()
    return {tuple(str(part) for part in row) for row in rows}


def build_roll_call_votes(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    acquirer: VoteSource | None = None,
    max_votes: int = MAX_VOTES_PER_RUN,
    overlap: int = OVERLAP_VOTES,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, Path]:
    """Build ``roll_call_votes.parquet`` and ``member_votes.parquet`` (House)."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Roll-call votes need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = CongressListingReader(budget=LIST_BUDGET, api_key=api_key)
    acquirer = acquirer or VoteAcquirer(budget=VOTE_BUDGET)

    # 1. Linkage: what the publisher says each House roll call was about.
    congresses = congresses_from_env()
    route = LIST_ROUTES["house-vote"]
    records: list[object] = []
    for congress in congresses:
        before = len(records)
        for session in sessions_of(congress):
            url = list_route_url(route, congress=congress, session=session, limit=MAX_LIMIT)
            for page in reader.records(route, url, max_pages=MAX_PAGES):
                records.extend(page.records)
        logger.info("Roll-call votes: Congress {} — {:,} listed House votes", congress, len(records) - before)

    # The bill's own action is the stronger statement, so it is indexed first
    # and wins a disagreement; the listing's is indexed after it.
    index = index_vote_references(
        (
            *_recorded_vote_references(output_dir, congresses, download_prior),
            *house_vote_references(records),
        )
    )
    # Per vote, not per run: the contract's sentence is "how many later
    # references disagreed with the one that won", which is a fact about one
    # roll call. A single run-wide number stamped on every row would say that
    # every roll call was contested whenever any one of them was.
    conflicts: Counter[VoteKey] = Counter(held.vote for held, _ in index.conflicts)
    if conflicts:
        logger.warning(
            "Roll-call votes: {:,} roll call(s) claimed by two references — {:,} disagreements in total",
            len(conflicts),
            sum(conflicts.values()),
        )
    by_rule = Counter(reference.rule for reference in index.by_vote.values())
    logger.info("Roll-call votes: {:,} roll calls indexed by rule — {}", len(index.by_vote), dict(by_rule))

    # 2. Counts and positions, newest first, bounded, and skipping what is held.
    prior_file = published_table(output_dir, NAME, download_prior)
    have_prior = prior_file is not None
    held = _held_votes(prior_file) if prior_file is not None else set()

    # Only the chamber this rollup publishes: a Senate reference is a linkage
    # for a roll call no index route here can enumerate (see the docstring).
    ordered = sorted(
        (key for key in index.by_vote if key.chamber == "house"),
        key=lambda k: (k.congress, k.session, k.roll_number),
        reverse=True,
    )
    # The newest few are always re-read; the rest only if not already captured.
    keys = [
        key
        for position, key in enumerate(ordered)
        if position < overlap
        or (str(key.congress), str(key.chamber), str(key.session), str(key.roll_number)) not in held
    ]
    if held:
        logger.info(
            "Roll-call votes: {:,} of {:,} listed roll calls already published — fetching {:,}"
            " (the newest {} are re-read for corrections)",
            len(ordered) - len(keys) + min(overlap, len(ordered)),
            len(ordered),
            len(keys),
            overlap,
        )
    if len(keys) > max_votes:
        logger.warning("Roll-call votes: {:,} roll calls to fetch, taking the newest {:,}", len(keys), max_votes)
        keys = keys[:max_votes]
    matches = {match.vote: match for match in match_votes(keys, index)}

    vote_rows: list[dict] = []
    member_rows: list[dict] = []
    refused = 0
    for key in keys:
        if key.chamber not in VOTE_CHAMBERS:
            # Only "house" and "senate" address a locator; the index cannot
            # produce another today, and a third would be a new publisher.
            logger.warning("Roll-call votes: skipping unknown chamber {!r}", key.chamber)
            continue
        locator = VoteLocator(
            chamber=cast("Chamber", key.chamber),
            congress=key.congress,
            session=key.session,
            roll_number=key.roll_number,
        )
        try:
            vote = acquirer.acquire(locator).vote
        except VoteSourceError as error:
            # A roll call the Clerk will not serve is counted, never dropped
            # silently and never published with a fabricated tally.
            refused += 1
            logger.warning("Roll-call votes: {} refused: {}", locator.url(), error)
            continue
        reference = index.by_vote.get(key)
        vote_rows.append(
            shape_roll_call_vote(
                vote,
                match=matches.get(key),
                tally=vote.tallies,
                member_vote_count=len(vote.member_votes),
                action_index=None if reference is None else reference.action_index,
                conflict_count=conflicts.get(key, 0),
            )
        )
        for member in vote.member_votes:
            member_rows.append(shape_member_vote(member, vote=vote))

    logger.info(
        "Roll-call votes: {:,} roll calls, {:,} member positions, {:,} refused",
        len(vote_rows),
        len(member_rows),
        refused,
    )
    return (
        merge_contract_table(output_dir, NAME, vote_rows, prior_present=have_prior),
        merge_contract_table(output_dir, "member_votes", member_rows),
    )
