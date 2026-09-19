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

``MAX_VOTES_PER_RUN`` bounds the Clerk walk: a run publishes nothing until it
finishes, so an unbounded first pass over several Congresses would time out and
persist nothing, the failure mode ``build_congress_bills`` already documents.
Votes are acquired newest first, so a bounded run advances from the present.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

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

from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env, sessions_of
from spicy_regs.transforms.table_merge import merge_contract_table

if TYPE_CHECKING:
    from spicy_docs.sources.congress.votes import Chamber

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


def build_roll_call_votes(
    output_dir: Path,
    *,
    reader: CongressListingReader | None = None,
    acquirer: VoteAcquirer | None = None,
    max_votes: int = MAX_VOTES_PER_RUN,
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
        for session in sessions_of(congress):
            url = list_route_url(route, congress=congress, session=session, limit=MAX_LIMIT)
            for page in reader.records(route, url, max_pages=MAX_PAGES):
                records.extend(page.records)
        logger.info("Roll-call votes: Congress {} — {:,} listed House votes", congress, len(records))

    index = index_vote_references(house_vote_references(records))
    conflict_count = len(index.conflicts)
    if conflict_count:
        logger.warning("Roll-call votes: {} roll call(s) claimed by two references", conflict_count)

    # 2. Counts and positions, newest first, bounded.
    keys = sorted(index.by_vote, key=lambda k: (k.congress, k.session, k.roll_number), reverse=True)
    if len(keys) > max_votes:
        logger.warning("Roll-call votes: {:,} roll calls in scope, acquiring the newest {:,}", len(keys), max_votes)
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
        merge_contract_table(output_dir, "roll_call_votes", vote_rows),
        merge_contract_table(output_dir, "member_votes", member_rows),
    )
