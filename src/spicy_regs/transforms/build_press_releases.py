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
compiled per bill in the Congresses in scope, and each release's title, then
its description text, is searched for a mention. The bills come from the
published ``congress_bills`` table, read once at merge time through the same
best-effort download every prior table goes through. That is a read of
another rollup's published output, which the rollup contract
(``pipelines/rollups/base.py``) allows for an *ingest* table — it has no
upstream dependency inside this repository, so reading it is an ordering
preference the crons already honour, not a race — and it is tolerated absent:
with no bills published yet, no matching pass runs and every match column is
NULL, which is distinct from ``match_rule = unmatched``, the value a release
gets when the pass ran and nothing in it named a bill.

Keyless: both feeds are public RSS; the bills table is read from R2.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
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
from spicy_regs.transforms.congress_scope import congresses_from_env
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


def _bill_patterns(
    output_dir: Path, congresses: Sequence[int], download_prior: Callable[[str, Path], bool]
) -> tuple[BillPattern, ...] | None:
    """One compiled pattern per published bill in scope, or ``None`` when no bills are published.

    Read in DuckDB down to the three identity columns for the Congresses in
    scope rather than materialised whole: the table runs to tens of thousands
    of rows across Congresses, and the matcher's cost is releases times bills,
    so scoping it is what keeps a run bounded. The scratch file is removed
    afterwards, the way ``merge_table`` removes its own, so it is not mistaken
    for an output.
    """
    path = published_table(output_dir, BILLS_TABLE, download_prior)
    if path is None:
        logger.warning("Press releases: no published {} table — no bill matching this run", BILLS_TABLE)
        return None
    import duckdb

    rows = duckdb.sql(
        f"SELECT congress, bill_type, bill_number FROM read_parquet('{path}') WHERE congress IN (SELECT UNNEST(?))",
        params=[[str(congress) for congress in congresses]],
    ).fetchall()
    path.unlink(missing_ok=True)
    bills = [
        BillIdentity(congress=int(congress), bill_type=str(kind), number=int(number)) for congress, kind, number in rows
    ]
    logger.info("Press releases: {:,} published bills in Congress(es) {} to match against", len(bills), congresses)
    return compile_bill_patterns(bills)


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
    """Build ``press_releases.parquet`` from both chambers' feeds."""
    acquirer = acquirer or PressReleaseAcquirer(budget=BUDGET)
    patterns = _bill_patterns(output_dir, congresses_from_env(), download_prior)

    rows: list[dict] = []
    matched = 0
    for chamber, feed in PRESS_RELEASE_FEEDS.items():
        acquisition = acquirer.acquire_press_releases(feed)
        channel = acquisition.channel
        observed_at = acquisition.capture.observed_at
        logger.info("Press releases: {} feed — {:,} items", chamber, len(channel.releases))
        matches = _matches(channel.releases, patterns) if patterns is not None else {}
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
    return merge_contract_table(output_dir, "press_releases", rows, download_prior=download_prior)
