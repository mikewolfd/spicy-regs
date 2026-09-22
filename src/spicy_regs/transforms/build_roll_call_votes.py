"""Build roll-call and member-vote tables from complete source enumerations.

House listing identities and Senate LIS menus determine which votes to acquire.
Bill references enrich those votes independently: an unlinked procedural vote
still has its own tally and member positions. House action references may add
keys before the listing catches up; Senate references alone never establish a
complete Senate selection.

Acquisition is bounded by MAX_VOTES_PER_RUN. Unseen keys precede held correction
refreshes so a small cap cannot stall backfill. OVERLAP_VOTES applies separately
to each chamber; all keys retain the publisher's chamber namespace. A successful
reacquisition replaces that roll call's complete member roster. Failed, capped
or unselected roll calls retain their prior observations.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
import json
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
    read_house_vote_key,
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
from spicy_docs.sources.congress.votes import (
    VoteAcquirer,
    VoteBudget,
    VoteLocator,
    VoteSourceError,
    locator_from_menu_entry,
)

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

    def list_senate_votes(self, congress: int, session: int) -> Any: ...


LIST_BUDGET = PagedJsonBudget(
    max_requests=5,  # measured retry bound: see fork-execution-2026-09-21/retry-measurement-2026-09-22
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Each publisher request is paced at two a second.
VOTE_BUDGET = VoteBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,  # the acquirer's own ceiling for a roll-call file
    timeout_seconds=60.0,
    min_request_interval_seconds=0.5,
)

MAX_PAGES = 40

#: Bound per-run source requests; larger selections resume from prior outputs.
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
    numeric ``action_index`` (the publisher’s earlier list position), even
    though the published column is VARCHAR.
    Unreadable indices still reach the refusal below. Ordering by the identity
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
        "ORDER BY congress, chamber, session, roll_number, "
        "TRY_CAST(action_index AS BIGINT) NULLS LAST, bill_id",
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
    """Captured roll calls: ordinary tallies or complete native candidate totals.

    Legacy rows have no tally kind and keep the existing non-NULL yea rule.
    Candidate elections have no yea/nay total; their explicit kind, native
    count map and reconciled member count distinguish capture from linkage.
    """
    if not prior_file.exists():
        return set()
    import duckdb

    relation = duckdb.read_parquet(str(prior_file))
    columns = set(relation.columns)
    ordinary = "yea IS NOT NULL"
    if "tally_kind" in columns:
        ordinary += " AND (tally_kind IS NULL OR tally_kind = 'positions')"
    rows = relation.filter(ordinary).project("congress, chamber, session, roll_number").fetchall()
    held = {tuple(str(part) for part in row) for row in rows}
    candidate_columns = {
        "tally_kind",
        "tallies_json",
        "member_vote_count",
        "source_url",
        "nay",
        "present",
        "not_voting",
    }
    if not candidate_columns.issubset(columns):
        return held
    candidates = (
        relation.filter(
            "tally_kind = 'candidates' AND yea IS NULL AND nay IS NULL AND present IS NULL AND not_voting IS NULL"
        )
        .project("congress, chamber, session, roll_number, tallies_json, member_vote_count, source_url")
        .fetchall()
    )
    for *parts, raw_tallies, raw_count, source_url in candidates:
        try:
            tallies = json.loads(raw_tallies)
            member_count = int(raw_count)
        except (TypeError, ValueError):
            continue
        if (
            source_url
            and member_count > 0
            and isinstance(tallies, dict)
            and tallies
            and all(
                isinstance(name, str) and name.strip() and type(count) is int and count >= 0
                for name, count in tallies.items()
            )
            and sum(tallies.values()) == member_count
        ):
            held.add(tuple(str(part) for part in parts))
    return held


def build_roll_call_votes(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    acquirer: VoteSource | None = None,
    max_votes: int = MAX_VOTES_PER_RUN,
    overlap: int = OVERLAP_VOTES,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, Path]:
    """Build both chambers; optional bill links never restrict native selection.

    Menu/listing failures propagate before either output is written. Unseen
    votes take priority over correction refreshes, with overlap per chamber.
    """
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
    listed_keys: set[VoteKey] = set()
    for congress in congresses:
        before = len(records)
        for session in sessions_of(congress):
            url = list_route_url(route, congress=congress, session=session, limit=MAX_LIMIT)
            for page in reader.records(route, url, max_pages=MAX_PAGES):
                records.extend(page.records)
            # The owner reader proves menu identity and refuses empty/failed
            # responses; a refusal cannot establish a zero-vote session.
            menu = acquirer.list_senate_votes(congress, session).menu
            for entry in menu.votes:
                locator = locator_from_menu_entry(menu, entry)
                listed_keys.add(VoteKey(locator.congress, locator.chamber, locator.session, locator.roll_number))
        logger.info("Roll-call votes: Congress {} — {:,} listed House votes", congress, len(records) - before)

    listed_keys.update(read_house_vote_key(record) for record in records)
    listing_references: list[VoteReference] = []
    for record in records:
        try:
            listing_references.extend(house_vote_references((record,)))
        except (VoteMatchError, BillSourceError) as error:
            # The identity was validated independently. A malformed optional
            # bill reference must not discard that source vote.
            logger.warning(
                "Roll-call votes: optional House bill reference refused for {}: {}",
                read_house_vote_key(record),
                error,
            )

    # The bill's own action is the stronger statement, so it is indexed first
    # and wins a disagreement; the listing's is indexed after it.
    index = index_vote_references(
        (
            *_recorded_vote_references(output_dir, congresses, download_prior),
            *listing_references,
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

    # House action references can precede the House listing. Senate scope
    # comes from its own menu, never from a bill-only sample.
    ordered = sorted(
        listed_keys | {key for key in index.by_vote if key.chamber == "house"},
        key=lambda k: (k.congress, k.session, k.roll_number, k.chamber),
        reverse=True,
    )
    fresh_keys: list[VoteKey] = []
    refresh_keys: list[VoteKey] = []
    chamber_positions: Counter[str] = Counter()
    for key in ordered:
        position = chamber_positions[key.chamber]
        chamber_positions[key.chamber] += 1
        identity = (str(key.congress), str(key.chamber), str(key.session), str(key.roll_number))
        if identity not in held:
            fresh_keys.append(key)
        elif position < overlap:
            refresh_keys.append(key)
    keys = fresh_keys + refresh_keys
    if held:
        logger.info(
            "Roll-call votes: {:,} of {:,} listed roll calls already published — fetching {:,}"
            " (up to {} per chamber are re-read for corrections)",
            len(ordered) - len(fresh_keys),
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
        merge_contract_table(output_dir, NAME, vote_rows, prior_present=have_prior, download_prior=download_prior),
        merge_contract_table(
            output_dir,
            "member_votes",
            member_rows,
            download_prior=download_prior,
            replace_parents=("vote_id", {str(row["vote_id"]) for row in vote_rows}),
        ),
    )
