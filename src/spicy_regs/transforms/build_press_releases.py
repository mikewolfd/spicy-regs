"""Transform: build ``press_releases.parquet``.

One row per item in one capture of an appropriations committee press-release
feed, through ``spicy_docs.sources.congress.press_releases`` (House and Senate).

The identity is ``release_id = sha256(chamber \\x1f link)``, computed by the
contract's own ``press_release_id`` rather than here: BillTrax keyed these on a
255-byte prefix of the source URL, which collides on feeds whose links share a
long prefix. The full ``link`` stays its own column.

A feed is a rotating window — the publisher drops older items off the end — so
this table is append-only by nature: the merge keeps every release ever
captured and prefers the fresh copy of one seen again.

**The bill linkage.** ``bill_id`` and the three match provenance columns are
filled by ``spicy_docs.interpretation.release_matching``: one pattern is
compiled per published bill, and each release's title, then its description
text, is searched for a mention. The bills come from the published
``congress_bills`` table, read once at merge time through the same best-effort
download every prior table goes through. That is a read of another rollup's
published output, which the rollup contract (``pipelines/rollups/base.py``)
allows for an *ingest* table — it has no upstream dependency inside this
repository, so reading it is an ordering preference the crons already honour,
not a race.

**Each release is scoped by its own publication date, not by a run-wide
Congress.** A feed is a rotating window roughly two months deep, so through
every January it still carries December's releases, which name the *previous*
Congress's bills. Scoping the whole run to one Congress — the current one, or
an env override — would leave those releases unmatchable for as long as they
stay in the window, and worse: the run would still publish them with
``match_rule = unmatched``, overwriting the correct ``bill_id`` a December run
had already published, because this table merges row-wise and a fresh NULL
wins. So the Congress is computed per release from ``pub_date``
(``congress_scope.current_congress``, which knows a Congress convenes on 3
January), the bills are read for exactly the Congresses the run's releases fall
in, and each release is matched only against its own.

**Three row states, deliberately distinct.** A ``bill_id`` with a
``bill_number_in_*`` rule is a match. ``match_rule = unmatched`` means the
matcher ran against a real set of published bills and none was named. An
all-NULL ``match_rule`` means no matching pass could run for that release —
no ``congress_bills`` table was published, or none was published for that
release's Congress, or the release carries no readable ``pub_date`` to scope
by. The third must never be published as the second: asserting "no bill names
this" on the strength of an empty bill list is a false measurement, and one
that erases a true one.

Keyless: both feeds are public RSS; the bills table is read from R2.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Sequence
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from spicy_docs.interpretation.release_matching import (
    BillPattern,
    Release,
    ReleaseMatch,
    compile_bill_patterns,
    match_releases,
)
from spicy_docs.schemas.congress_activity_tables import press_release_id, shape_press_release
from spicy_docs.sources.congress.bill_status import BillIdentity
from spicy_docs.sources.congress.press_releases import (
    PRESS_RELEASE_FEEDS,
    PressRelease,
    PressReleaseAcquirer,
    PressReleaseBudget,
)

from spicy_regs.sources import r2
from spicy_regs.transforms.congress_scope import current_congress
from spicy_regs.transforms.table_merge import merge_contract_table, published_table


class PressReleaseSource(Protocol):
    """What this transform needs of a press-release acquirer.

    Structural, for the same reason the bill family's and the committee
    reports' acquirer seams are: a hermetic test serves the captured feed
    bytes, and naming the concrete acquirer here would make that stub
    untypeable.
    """

    def acquire_press_releases(self, feed: Any) -> Any: ...


BUDGET = PressReleaseBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=1.0,
)

#: The published table the bill identities are read from.
BILLS_TABLE = "congress_bills"


def release_congress(release: PressRelease) -> int | None:
    """The Congress sitting when this release was published, or ``None`` when it states no date.

    ``current_congress`` is the same arithmetic the rest of the pipeline scopes
    by, including the rule that a Congress convenes on 3 January, so a release
    published on 1 January still belongs to the outgoing one. A release whose
    ``pub_date`` did not parse cannot be scoped at all, and gets NULL match
    columns rather than being matched against a Congress guessed for it.
    """
    instant = release.pub_date_instant
    return None if instant is None else current_congress(instant.date())


def _bill_patterns(
    output_dir: Path, congresses: Collection[int], download_prior: Callable[[str, Path], bool]
) -> dict[int, tuple[BillPattern, ...]]:
    """``congress -> one compiled pattern per published bill of it``, for the Congresses asked for.

    A Congress with no published bills is **absent from the result**, never
    present with an empty tuple: an empty pattern set matches nothing, and
    reporting that as ``unmatched`` would state a measurement no bill list
    backed — and overwrite the ``bill_id`` an earlier run correctly published,
    since this table merges row-wise. Absent instead leaves the columns NULL,
    which is the state that says "no pass ran".

    Read in DuckDB down to the three identity columns for the Congresses asked
    for rather than materialised whole: the table runs to tens of thousands of
    rows across Congresses, and the matcher's cost is releases times bills, so
    scoping it is what keeps a run bounded. ``ORDER BY`` makes the tie-break a
    stated rule rather than the scan's accident: ``match_releases`` takes the
    first pattern that hits, so two bills whose patterns could both match one
    release are resolved by ascending Congress, then type, then number. The
    scratch file is removed afterwards, the way ``merge_table`` removes its
    own, so it is not mistaken for an output.
    """
    if not congresses:
        return {}
    path = published_table(output_dir, BILLS_TABLE, download_prior)
    if path is None:
        logger.warning("Press releases: no published {} table — no bill matching this run", BILLS_TABLE)
        return {}
    import duckdb

    rows = duckdb.sql(
        f"SELECT congress, bill_type, bill_number FROM read_parquet('{path}') "
        "WHERE congress IN (SELECT UNNEST(?)) "
        "ORDER BY congress, bill_type, bill_number",
        params=[[str(congress) for congress in sorted(congresses)]],
    ).fetchall()
    path.unlink(missing_ok=True)

    bills: dict[int, list[BillIdentity]] = {}
    for congress, kind, number in rows:
        bills.setdefault(int(congress), []).append(
            BillIdentity(congress=int(congress), bill_type=str(kind), number=int(number))
        )
    for congress in sorted(set(congresses) - set(bills)):
        logger.warning(
            "Press releases: no published bills for Congress {} — its releases keep NULL match columns", congress
        )
    logger.info(
        "Press releases: {:,} published bills to match against, by Congress — {}",
        sum(len(group) for group in bills.values()),
        {congress: len(group) for congress, group in sorted(bills.items())},
    )
    return {congress: compile_bill_patterns(group) for congress, group in bills.items()}


def _matches(releases: Iterable[PressRelease], patterns: Sequence[BillPattern]) -> dict[str, ReleaseMatch]:
    """``release_id`` -> the match the interpretation seam made, title first, then the description text."""
    candidates = [
        Release(
            release_id=press_release_id(release.chamber, release.link),
            title=release.title,
            excerpt=release.description_text,
        )
        for release in releases
    ]
    return {match.release_id: match for match in match_releases(candidates, patterns)}


def build_press_releases(
    output_dir: Path,
    *,
    acquirer: PressReleaseSource | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> Path:
    """Build ``press_releases.parquet`` from both chambers' feeds.

    **Both feeds are captured before the bills table is asked for.** A feed is
    a rotating window: an item the publisher drops is gone, and this table is
    the only place it survives. Downloading a linkage input first would put an
    R2 refusal between the run and a capture it can never retake, so the
    perishable read happens first and the linkage — which is re-derivable on
    the next run — happens second.
    """
    acquirer = acquirer or PressReleaseAcquirer(budget=BUDGET)

    captures = []
    for chamber, feed in PRESS_RELEASE_FEEDS.items():
        acquisition = acquirer.acquire_press_releases(feed)
        channel = acquisition.channel
        logger.info("Press releases: {} feed — {:,} items", chamber, len(channel.releases))
        captures.append((feed, channel, acquisition.capture.observed_at))

    # The Congresses the run's own releases fall in, from their publication
    # dates — not a run-wide scope, which a rotating window straddles.
    scope = {
        congress
        for _, channel, _ in captures
        for congress in (release_congress(release) for release in channel.releases)
        if congress is not None
    }
    patterns = _bill_patterns(output_dir, scope, download_prior)

    rows: list[dict] = []
    matched = undated = unscoped = 0
    for feed, channel, observed_at in captures:
        # Grouped by Congress so each release is matched only against the bills
        # of its own, and `compile_bill_patterns` is still called once per
        # Congress rather than once per release.
        grouped: dict[int, list[PressRelease]] = {}
        for release in channel.releases:
            congress = release_congress(release)
            if congress is None:
                undated += 1
            elif congress not in patterns:
                unscoped += 1
            else:
                grouped.setdefault(congress, []).append(release)

        matches: dict[str, ReleaseMatch] = {}
        for congress, group in grouped.items():
            matches.update(_matches(group, patterns[congress]))

        for release in channel.releases:
            match = matches.get(press_release_id(release.chamber, release.link))
            if match is not None and match.bill is not None:
                matched += 1
            rows.append(
                shape_press_release(
                    release,
                    channel,
                    feed=feed,
                    observed_at=observed_at,
                    match=match,
                )
            )

    logger.info("Press releases: {:,} rows this run, {:,} naming a published bill", len(rows), matched)
    if undated or unscoped:
        # Both keep NULL match columns rather than a false `unmatched`.
        logger.info(
            "Press releases: {:,} release(s) state no readable pub_date and {:,} fall in a Congress with no "
            "published bills — no matching pass ran for either, so their match columns stay NULL",
            undated,
            unscoped,
        )
    return merge_contract_table(output_dir, "press_releases", rows, download_prior=download_prior)
