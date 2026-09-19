"""Transform: build ``roll_call_votes.parquet`` and ``member_votes.parquet``.

Two steps, from two different publishers, because neither alone is enough:

1. **Linkage.** The Congress.gov ``house-vote`` list route states which bill a
   House roll call was about. ``house_vote_references`` reads that off the
   record and ``index_vote_references`` settles the cases where two references
   claim the same roll call, so a bill linkage carries the rule that produced
   it and a count of what it beat.
2. **Counts and positions.** The tallies and every member's position come from
   the House Clerk's own EVS XML through ``spicy_docs.sources.congress.votes``,
   one keyless request per roll call. BillTrax published zeroes in these four
   columns; they are real here or they are NULL.

**Scope.** House only. The Senate's roll calls live on the Senate LIS menu,
which ``listing.py`` has no route for, so no Senate row is published rather
than a Senate row with a NULL tally. The linkage half also has a second source
the design names — the ``recordedVotes`` entries on a bill's actions, which
reach Senate votes — but those arrive with BILLSTATUS, and a rollup may not
read another rollup's output, so wiring that would mean re-acquiring every
bill's status here. The coverage statement says so.

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

from pathlib import Path
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast

from loguru import logger
from spicy_docs.interpretation.vote_matching import (
    VOTE_CHAMBERS,
    house_vote_references,
    index_vote_references,
    match_votes,
)
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.congress_activity_tables import shape_member_vote, shape_roll_call_vote
from spicy_docs.sources.congress.listing import (
    LIST_ROUTES,
    MAX_LIMIT,
    CongressListingReader,
    list_route_url,
)
from spicy_docs.sources.congress.votes import VoteAcquirer, VoteBudget, VoteLocator, VoteSourceError

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env, sessions_of
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

if TYPE_CHECKING:
    from spicy_docs.sources.congress.votes import Chamber


class ListingSource(Protocol):
    """What this transform needs of a Congress.gov listing reader.

    Structural, for the same reason the bill family's acquirer seams are: a
    hermetic test serves fixture pages, and naming the concrete reader here
    would make that untypeable.
    """

    def records(self, route: Any, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


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
    route = LIST_ROUTES["house-vote"]
    records: list[object] = []
    for congress in congresses_from_env():
        before = len(records)
        for session in sessions_of(congress):
            url = list_route_url(route, congress=congress, session=session, limit=MAX_LIMIT)
            for page in reader.records(route, url, max_pages=MAX_PAGES):
                records.extend(page.records)
        logger.info("Roll-call votes: Congress {} — {:,} listed House votes", congress, len(records) - before)

    index = index_vote_references(house_vote_references(records))
    conflict_count = len(index.conflicts)
    if conflict_count:
        logger.warning("Roll-call votes: {} roll call(s) claimed by two references", conflict_count)

    # 2. Counts and positions, newest first, bounded, and skipping what is held.
    prior_file = prior_scratch_path(output_dir, NAME)
    have_prior = prior_file.exists() or download_prior(OUTPUT, prior_file)
    held = _held_votes(prior_file) if have_prior else set()

    ordered = sorted(index.by_vote, key=lambda k: (k.congress, k.session, k.roll_number), reverse=True)
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
                conflict_count=conflict_count,
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
