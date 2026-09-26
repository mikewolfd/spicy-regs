"""Build roll-call and member-vote tables from complete source enumerations.

Each chamber's own session index determines which votes to acquire: the
Clerk's EVS pages for the House, the LIS vote menu for the Senate. Both hosts
are keyless, so the rollup needs no credential. Two statements link a roll
call to a bill, and ``match_rule`` names the one that did: the bill family's
recorded references (the bill's own action records the vote) and the vote
file's own statement of its measure (spicy-docs ``read_vote_file_statement``:
the Clerk's legis-num, the Senate's document or amended document). The bill's
action wins; where two bills' actions record one vote, the one the file names
wins, since positions in two different bills' action lists do not compare; with
no recorded reference the file links alone. An unlinked procedural vote still
has its own tally and member positions. House action references may add keys
before an index catches up; Senate references alone never establish a complete
Senate selection.

A held row is relinked from its own columns, never refetched: ``legis_num`` for
the House, ``documents_json``/``amendments_json`` for the Senate. A House row
published before ``legis_num`` existed is not held until it carries one, so
each is read again once.

Congress.gov's ``house-vote`` listing was retired as a second linkage source
(2026-09-26): over the 115th-119th it listed exactly the Clerk index's 5,079
House roll calls, won no published link and changed no conflict count, and
linked only 115-1-527 and -528 (H.R. 3354), which the Clerk files state as
their own ``legis-num``. Receipt:
``~/Work/corpora/fork-execution-2026-09-21/votes-backfill-2026-09-26/keyless/``.

Acquisition is bounded by MAX_VOTES_PER_RUN. Unseen keys precede held correction
refreshes so a small cap cannot stall backfill. OVERLAP_VOTES applies separately
to each chamber of a sitting Congress; a closed Congress's held roll calls are
never re-read, so a backfill reads each once and the scheduled scope never
reaches them. All keys retain the publisher's chamber namespace. A successful
reacquisition replaces that roll call's complete member roster. Failed, capped
or unselected roll calls retain their prior observations; a held one published
before ``vote_day`` existed gains it from its own ``vote_date`` before the merge.
A held roll call's derived bill link changes only when the bill family's
recorded references name it; without that input its published link stands.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from loguru import logger
from spicy_docs.interpretation.vote_matching import (
    VOTE_CHAMBERS,
    VoteKey,
    VoteMatch,
    VoteMatchError,
    VoteReference,
    index_vote_references,
    read_vote_file_statement,
)
from spicy_docs.schemas.congress_activity_tables import shape_member_vote, shape_roll_call_vote
from spicy_docs.sources.congress.bill_status import BillIdentity, BillSourceError
from spicy_docs.sources.congress.votes import (
    VoteAcquirer,
    VoteBudget,
    VoteLocator,
    VoteSourceError,
    locator_from_index_entry,
    locator_from_menu_entry,
    vote_day,
)

from spicy_regs.sources import r2
from spicy_regs.transforms.build_bill_family import VOTE_REFERENCES_TABLE
from spicy_regs.transforms.congress_scope import congresses_from_env, default_congresses, sessions_of
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    import pyarrow as pa
    from spicy_docs.sources.congress.votes import Chamber

    from spicy_regs.source_evidence import CaptureEvidence


class VoteSource(Protocol):
    """What this transform needs of a roll-call acquirer."""

    def acquire(self, locator: Any, *, crosswalk: Any = ...) -> Any: ...

    def list_house_votes(self, congress: int, session: int) -> Any: ...

    def list_senate_votes(self, congress: int, session: int) -> Any: ...


#: Each publisher request is paced at two a second.
VOTE_BUDGET = VoteBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,  # the acquirer's own ceiling for a roll-call file
    timeout_seconds=60.0,
    min_request_interval_seconds=0.5,
)

#: Bound per-run source requests; larger selections resume from prior outputs.
MAX_VOTES_PER_RUN = 1_500

#: The newest roll calls of a sitting Congress are re-read every run even when
#: already published, because the contract carries no publisher ``updateDate``
#: to compare and a late correction would otherwise never be picked up. A
#: closed Congress (outside ``default_congresses``) is read once.
OVERLAP_VOTES = 25

NAME = "roll_call_votes"
OUTPUT = "roll_call_votes.parquet"

#: The columns the reference table's rows are read back through. Each is a
#: field of the ``VoteReference`` the family wrote, so nothing is re-derived.
REFERENCE_COLUMNS = ("bill_id", "chamber", "congress", "session", "roll_number", "action_index", "url", "date")
#: v4: the vote file's own statement chooses among disagreeing recorded
#: references and links alone where none exists; v3 used recorded references
#: only, v2 indexed Congress.gov's listing after them.
LINK_RULE_VERSION = "recorded-vote-then-vote-file-v4"
LINK_COLUMNS = ("bill_id", "match_rule", "match_action_index", "match_url", "conflict_count")
IDENTITY_COLUMNS = ("congress", "chamber", "session", "roll_number")
#: The row's own statement of its measure, read by spicy-docs ``read_vote_file_statement``.
STATEMENT_COLUMNS = ("source_url", "legis_num", "documents_json", "amendments_json")
#: The bill family's reference: the bill's own action names the roll call.
RECORDED_RULE = "bill_action_recorded_vote"


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
    output_dir: Path, congresses: Sequence[int], download_prior: Callable[[str, Path], bool],
    *, evidence: CaptureEvidence | None = None,
) -> tuple[VoteReference, ...] | None:
    """The family's published references for the Congresses in scope, or ``None`` when none are published.

    ``None`` and ``()`` differ on purpose: an unpublished input says nothing
    about any vote, so no held vote is relinked, while a published one that
    names no vote in scope gives no vote a new link. Neither clears one: a
    recorded link the input no longer names is kept and counted as
    ``unresolved_prior_preserved``, because the bill family's scope can narrow
    without the publisher retracting the action that recorded the vote.

    Scoped in DuckDB rather than read whole: the table grows one row per
    recorded vote per action across every Congress the family has run, and this
    rollup only ever matches the Congresses in its own scope.

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
        if evidence is not None:
            evidence.event("vote-reference-input", available=False, rule_version=LINK_RULE_VERSION)
        logger.info(
            "Roll-call votes: no published {} table — no roll call is linked to a bill this run",
            VOTE_REFERENCES_TABLE,
        )
        return None
    import duckdb

    if evidence is not None:
        with path.open("rb") as stream:
            digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        evidence.event(
            "vote-reference-input", available=True, sha256=digest,
            generation=evidence.published_input(f"{VOTE_REFERENCES_TABLE}.parquet", sha256=digest),
            rule_version=LINK_RULE_VERSION, congresses=list(congresses),
        )

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
                    rule=RECORDED_RULE,
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
    A House row without ``legis_num`` (published before the column) is not
    held: its file is read once more so its own statement can link it.
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
    candidates = (
        relation.filter(
            "tally_kind = 'candidates' AND yea IS NULL AND nay IS NULL AND present IS NULL AND not_voting IS NULL"
        )
        .project("congress, chamber, session, roll_number, tallies_json, member_vote_count, source_url")
        .fetchall()
        if candidate_columns.issubset(columns)
        else ()
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
    unstated = relation.filter("chamber = 'house'" + (" AND legis_num IS NULL" if "legis_num" in columns else ""))
    held -= {tuple(str(part) for part in row) for row in unstated.project(", ".join(IDENTITY_COLUMNS)).fetchall()}
    return held


def _link_columns(
    key: VoteKey, recorded: Sequence[VoteReference], stated: VoteReference | None
) -> dict[str, str | None]:
    """One roll call's link columns: its bill's own action, chosen by its file where actions disagree, else its file.

    ``recorded`` keeps the family's action order; the reference naming the
    bill the file states moves first, since a lower action index in a different
    bill's list says nothing about which bill the vote was on. The file's own
    reference follows, so it links only a vote no action records and otherwise
    counts in ``conflict_count`` when it disagrees. O(references to this vote).
    """
    ordered = sorted(recorded, key=lambda reference: stated is None or reference.bill != stated.bill)
    index = index_vote_references((*ordered, *(() if stated is None else (stated,))))
    winner = index.by_vote.get(key)
    match = (
        VoteMatch(key, None, "unmatched")
        if winner is None
        else VoteMatch(key, winner.bill, winner.rule, winner.url, winner.date)
    )
    shaped = shape_roll_call_vote(
        key, match=match, action_index=None if winner is None else winner.action_index,
        conflict_count=len(index.conflicts),
    )
    return {name: shaped[name] for name in LINK_COLUMNS}


def _stated_reference(row: Mapping[str, object]) -> VoteReference | None:
    """The bill the row's own file names, from its native columns; ``None`` when it names none."""
    return read_vote_file_statement(row).reference


def _backfill_vote_day(table: pa.Table, held: set[tuple[str, ...]]) -> tuple[pa.Table, int]:
    """Fill a NULL ``vote_day`` on each held prior roll call from its own ``vote_date``; return the table and the count.

    Published roll calls are re-read only within ``OVERLAP_VOTES``, so a row
    published before the contract gained ``vote_day`` would otherwise keep
    NULL indefinitely, and NULL would mean "no printed date", "linkage only"
    and "published too early" at once. A held row's ``vote_date`` is the
    chamber's printed literal, read by the same spicy-docs ``vote_day`` the
    contract shaper uses for a fresh row. A linkage-only row is not held and is
    left alone; a held row whose date that function refuses (a stored UTC
    instant) stays NULL and is counted. The table is returned unchanged when
    nothing was filled.
    """
    import pyarrow as pa

    names = table.column_names
    days = table.column("vote_day").to_pylist() if "vote_day" in names else [None] * table.num_rows
    identities = zip(*(table.column(c).to_pylist() for c in IDENTITY_COLUMNS))
    filled = refused = 0
    for row, (identity, literal) in enumerate(zip(identities, table.column("vote_date").to_pylist())):
        if days[row] is not None or tuple(str(part) for part in identity) not in held:
            continue
        try:
            days[row] = vote_day(identity[1], literal)
        except VoteSourceError:
            refused += 1
            continue
        filled += days[row] is not None
    if refused:
        logger.warning(
            "Roll-call votes: vote_day left NULL on {:,} held roll call(s) whose vote_date it refuses", refused
        )
    logger.info("Roll-call votes: vote_day backfilled on {:,} prior roll call(s)", filled)
    if not filled:
        return table, 0
    column = pa.array(days, type=pa.string())
    if "vote_day" in names:
        return table.set_column(names.index("vote_day"), "vote_day", column), filled
    return table.append_column("vote_day", column), filled


def _relink_held_votes(
    table: pa.Table, held: set[tuple[str, ...]], recorded_by_vote: Mapping[VoteKey, Sequence[VoteReference]],
    recorded: frozenset[VoteKey] | None, congresses: Sequence[int], *, evidence: CaptureEvidence | None = None,
) -> tuple[pa.Table, int, dict[str, dict]]:
    """Refresh held votes' derived links without their files; return the table, count and held links.

    A held vote in scope is relinked when the bill family's recorded
    references name it, or when its own columns state a bill and its published
    link is not a recorded one; the link is then exactly what a fresh
    acquisition would publish (``_link_columns``). A recorded link the input
    no longer names is kept, as is every link when the input is unpublished
    (``recorded is None``) and the row states nothing: an absent optional input
    or an unresolved vote is not deletion evidence. Only identity, statement
    and link columns are read into Python and only link columns are replaced;
    native fields and member rows never pass through here. O(prior rows).
    """
    import pyarrow as pa

    scope = {str(congress) for congress in congresses}
    names = table.column_names
    vote_ids = table.column("vote_id").to_pylist()
    links = {name: table.column(name).to_pylist() if name in names else [None] * table.num_rows for name in LINK_COLUMNS}
    statements = {
        name: table.column(name).to_pylist() if name in names else [None] * table.num_rows
        for name in STATEMENT_COLUMNS
    }
    relinked, unchanged, unresolved = [], 0, []
    in_scope = 0
    for row, parts in enumerate(zip(*(table.column(name).to_pylist() for name in IDENTITY_COLUMNS))):
        identity = tuple(str(part) for part in parts)
        if identity not in held or identity[0] not in scope:
            continue
        in_scope += 1
        key = VoteKey(int(identity[0]), identity[1], int(identity[2]), int(identity[3]))
        references = recorded_by_vote.get(key, ())
        stated = _stated_reference(
            dict(zip(IDENTITY_COLUMNS, identity)) | {name: statements[name][row] for name in STATEMENT_COLUMNS}
        )
        previous = {name: links[name][row] for name in LINK_COLUMNS}
        if not references and (stated is None or previous["match_rule"] == RECORDED_RULE):
            unresolved.append(vote_ids[row])
            continue
        current = _link_columns(key, references, stated)
        if current == previous:
            unchanged += 1
            continue
        relinked.append({"vote_id": vote_ids[row], "previous": previous, **current})
        for name in LINK_COLUMNS:
            links[name][row] = current[name]
    logger.info(
        "Roll-call votes: refreshed derived links on {:,} of {:,} held roll calls ({}); {:,} kept as published",
        len(relinked), in_scope, "no recorded-vote input" if recorded is None else "recorded-vote input read",
        len(unresolved),
    )
    if evidence is not None:
        evidence.event(
            "held-vote-linkage", rule_version=LINK_RULE_VERSION, input_available=recorded is not None,
            held_in_scope=in_scope, relinked=relinked, unchanged=unchanged, unresolved_prior_preserved=unresolved,
        )
    held_links = {
        vote_ids[row]: {name: links[name][row] for name in LINK_COLUMNS}
        for row, parts in enumerate(zip(*(table.column(name).to_pylist() for name in IDENTITY_COLUMNS)))
        if tuple(str(part) for part in parts) in held
    }
    if relinked:
        # All native columns retain their existing Arrow type and values.
        for name in LINK_COLUMNS:
            column = pa.array(links[name], type=pa.string())
            position = table.schema.get_field_index(name)
            table = table.set_column(position, name, column) if position >= 0 else table.append_column(name, column)
    return table, len(relinked), held_links


def _repair_held_votes(
    prior_file: Path, held: set[tuple[str, ...]], recorded_by_vote: Mapping[VoteKey, Sequence[VoteReference]],
    recorded: frozenset[VoteKey] | None, congresses: Sequence[int], *, evidence: CaptureEvidence | None = None,
) -> dict[str, dict]:
    """Backfill ``vote_day`` and refresh held links in one read and at most one rewrite of ``prior_file``.

    Rewrites only when a row changed, so the merge that follows reads the
    repaired rows and an unchanged prior keeps its bytes.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(prior_file)
    table, filled = _backfill_vote_day(table, held)
    table, relinked, held_links = _relink_held_votes(
        table, held, recorded_by_vote, recorded, congresses, evidence=evidence
    )
    if filled or relinked:
        staged = prior_file.with_name(f".{prior_file.name}.partial")
        pq.write_table(table, staged, compression="zstd")
        staged.replace(prior_file)
    return held_links


def build_roll_call_votes(
    output_dir: Path,
    *,
    acquirer: VoteSource | None = None,
    max_votes: int = MAX_VOTES_PER_RUN,
    overlap: int = OVERLAP_VOTES,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
    open_congresses: Collection[int] | None = None,
) -> tuple[Path, Path]:
    """Build both chambers; optional bill links never restrict native selection.

    Index and menu failures propagate before either output is written.
    Unseen votes take priority over correction refreshes, with overlap per
    chamber for ``open_congresses`` only (default: ``default_congresses()``,
    the sitting Congress and, just after a boundary, the outgoing one).
    """
    acquirer = acquirer or VoteAcquirer(
        budget=VOTE_BUDGET,
        transport=None if evidence is None else evidence.transport(stage="vote-source", max_bytes=VOTE_BUDGET.max_bytes),
    )

    # 1. Population from each chamber's own session index.
    congresses = congresses_from_env()
    listed_keys: set[VoteKey] = set()
    withheld: list[tuple[VoteKey, str]] = []
    for congress in congresses:
        indexed = len(listed_keys)
        for session in sessions_of(congress):
            # The owner reader proves each index's identity and refuses an
            # empty, failed or gapped one; a refusal cannot establish a
            # zero-vote session.
            index = acquirer.list_house_votes(congress, session).index
            listed_keys.update(locator_from_index_entry(index, entry).as_vote_key() for entry in index.votes)
            menu = acquirer.list_senate_votes(congress, session).menu
            for entry in menu.votes:
                key = locator_from_menu_entry(menu, entry).as_vote_key()
                if entry.data_available:
                    listed_keys.add(key)
                else:
                    withheld.append((key, entry.title))
        logger.info("Roll-call votes: Congress {} — {:,} roll calls in the chambers' indexes", congress, len(listed_keys) - indexed)
    # A vote the Senate's own menu says it holds no data for (116-2-216, a
    # secret session) is not read: its file is served, but as 0-0 with every
    # senator "Not Voting", which the menu says is not the vote's record.
    for key, statement in withheld:
        logger.warning("Roll-call votes: Senate {}-{}-{} withheld by its menu: {}", key.congress, key.session, key.roll_number, statement)
        if evidence is not None:
            evidence.event(
                "vote-withheld", congress=key.congress, chamber=key.chamber, session=key.session,
                roll_number=key.roll_number, statement=statement,
            )

    # Linkage: the bill's own action names the roll call; the vote file's own
    # statement is read per row below, from the file or the held row.
    recorded_references = _recorded_vote_references(output_dir, congresses, download_prior, evidence=evidence)
    recorded = None if recorded_references is None else frozenset(r.vote for r in recorded_references)
    recorded_by_vote: dict[VoteKey, list[VoteReference]] = defaultdict(list)
    for reference in recorded_references or ():
        recorded_by_vote[reference.vote].append(reference)
    contested = sum(1 for references in recorded_by_vote.values() if len({r.bill for r in references}) > 1)
    logger.info(
        "Roll-call votes: {:,} roll calls recorded by a bill's action, {:,} by two bills' actions",
        len(recorded_by_vote), contested,
    )

    # 2. Counts and positions, newest first, bounded, and skipping what is held.
    prior_file = published_table(output_dir, NAME, download_prior)
    have_prior = prior_file is not None
    held: set[tuple[str, ...]] = set()
    held_links: dict[str, dict] = {}
    if prior_file is not None:
        held = _held_votes(prior_file)
        held_links = _repair_held_votes(prior_file, held, recorded_by_vote, recorded, congresses, evidence=evidence)

    # House action references can precede the Clerk's index. Senate scope
    # comes from its own menu, never from a bill-only sample.
    ordered = sorted(
        listed_keys | {key for key in recorded_by_vote if key.chamber == "house"},
        key=lambda k: (k.congress, k.session, k.roll_number, k.chamber),
        reverse=True,
    )
    sitting = set(default_congresses() if open_congresses is None else open_congresses)
    fresh_keys: list[VoteKey] = []
    refresh_keys: list[VoteKey] = []
    chamber_positions: Counter[str] = Counter()
    for key in ordered:
        identity = (str(key.congress), str(key.chamber), str(key.session), str(key.roll_number))
        if identity not in held:
            fresh_keys.append(key)
        if key.congress not in sitting:
            continue
        if identity in held and chamber_positions[key.chamber] < overlap:
            refresh_keys.append(key)
        chamber_positions[key.chamber] += 1
    keys = fresh_keys + refresh_keys
    if held:
        logger.info(
            "Roll-call votes: {:,} of {:,} listed roll calls already published — fetching {:,}"
            " (up to {} per chamber of a sitting Congress are re-read for corrections)",
            len(ordered) - len(fresh_keys),
            len(ordered),
            len(keys),
            overlap,
        )
    if len(keys) > max_votes:
        logger.warning("Roll-call votes: {:,} roll calls to fetch, taking the newest {:,}", len(keys), max_votes)
        keys = keys[:max_votes]

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
            if evidence is not None:
                evidence.refusal(error, stage="vote-source")
            # A roll call the Clerk will not serve is counted, never dropped
            # silently and never published with a fabricated tally.
            refused += 1
            logger.warning("Roll-call votes: {} refused: {}", locator.url(), error)
            continue
        row = shape_roll_call_vote(vote, tally=vote.tallies, member_vote_count=len(vote.member_votes))
        row.update(_link_columns(key, recorded_by_vote.get(key, ()), _stated_reference(row)))
        # A re-read held vote keeps a recorded link the input no longer names:
        # silence never replaces an established link.
        published = held_links.get(str(row["vote_id"]))
        if published is not None and key not in (recorded or ()) and published["match_rule"] == RECORDED_RULE:
            row.update(published)
        vote_rows.append(row)
        for member in vote.member_votes:
            member_rows.append(shape_member_vote(member, vote=vote))

    logger.info(
        "Roll-call votes: {:,} roll calls, {:,} member positions, {:,} refused, {:,} withheld by the Senate's menu",
        len(vote_rows),
        len(member_rows),
        refused,
        len(withheld),
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
