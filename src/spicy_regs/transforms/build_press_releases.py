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

``bill_id`` and the match provenance columns are left unset: matching a release
to a bill is an interpretation seam this transform does not yet drive, and the
contract preserves the columns as NULL rather than inventing a linkage.

Keyless: both feeds are public RSS.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from spicy_docs.schemas.congress_activity_tables import shape_press_release
from spicy_docs.sources.congress.press_releases import (
    PRESS_RELEASE_FEEDS,
    PressReleaseAcquirer,
    PressReleaseBudget,
)

from spicy_regs.transforms.table_merge import merge_contract_table

BUDGET = PressReleaseBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=1.0,
)


def build_press_releases(output_dir: Path, *, acquirer: PressReleaseAcquirer | None = None) -> Path:
    """Build ``press_releases.parquet`` from both chambers' feeds."""
    acquirer = acquirer or PressReleaseAcquirer(budget=BUDGET)

    rows: list[dict] = []
    for chamber, feed in PRESS_RELEASE_FEEDS.items():
        acquisition = acquirer.acquire_press_releases(feed)
        channel = acquisition.channel
        observed_at = acquisition.capture.observed_at
        logger.info("Press releases: {} feed — {:,} items", chamber, len(channel.releases))
        for release in channel.releases:
            rows.append(
                shape_press_release(
                    release,
                    channel,
                    feed=feed,
                    observed_at=observed_at,
                )
            )

    logger.info("Press releases: {:,} rows this run", len(rows))
    return merge_contract_table(output_dir, "press_releases", rows)
