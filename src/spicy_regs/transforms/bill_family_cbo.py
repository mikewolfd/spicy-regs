"""``cbo_cost_estimates`` from both of its routes: each bill's BILLSTATUS and CBO's own per-Congress feed.

BILLSTATUS states no CBO estimate for the 112th and 113th Congresses, and elsewhere lacks a few that CBO's feed
lists (spicy-docs 0.52.0, "CBO's own feed is read for every Congress"). So the bill family reads the keyless feed
of every Congress it is scoped to, once a run, shapes its items through spicy-docs'
``build_cbo_feed_cost_estimates``, and merges them with the BILLSTATUS rows through ``merge_cbo_cost_estimates``,
which keeps the BILLSTATUS row wherever both routes state one ``(bill_id, publication_id)``.

Three facts come from this host, because spicy-docs reads no table:

* ``law_bills``, ``laws.law_id`` to ``laws.bill_id`` from the published ``laws`` table, lets a title leading
  with a public law name that law's bill (``title_bill_id``, and ``found_by`` ``title_law``). It is read
  best-effort, like the ``laws`` join on ``congress_bills``: without it those titles stay unmapped.
* A feed row's report citations are its bill's own BILLSTATUS ``<committeeReports>``: this run's parsed status
  where the run read the bill's folder, else the citations the bill's published rows carry, re-shaped from their
  ``citation`` strings (a round trip that reproduces all 12,732 live rows, 2026-09-29). A bill neither names
  publishes NULL there, as the contract says.
* Precedence against rows this run did not rebuild: a feed row is merged with the prior BILLSTATUS rows of the
  bills this run did not re-read, so a prior BILLSTATUS row still wins its identity.

The replacement scope is ``(bill_id, source)``: a re-read bill replaces its prior BILLSTATUS rows, and a Congress
whose feed was read replaces every prior feed row of that Congress, so an item CBO withdraws or re-files leaves.
A Congress whose feed could not be read keeps its prior feed rows. Cost: one small request per scoped Congress
(the twelve feeds are 0.36-0.59 MB, 2026-09-28) and one pass over the prior table's rows of those Congresses.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger
from spicy_docs.interpretation.bill_family import FamilyRefusal, build_cbo_feed_cost_estimates
from spicy_docs.schemas.cost_estimate_tables import (
    CBO_COST_ESTIMATES,
    CBO_FEED,
    ESTIMATE_SOURCES,
    merge_cbo_cost_estimates,
)
from spicy_docs.sources.cbo import CboAcquirer, CboBudget, CboSourceError
from spicy_docs.sources.congress.bill_status import BillIdentity
from spicy_docs.transport.credentials import scrub_credential

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: One feed request a Congress; each measured feed is under 0.6 MB, so 8 MB bounds a feed with room to grow.
CBO_FEED_BUDGET = CboBudget(max_requests=1, max_bytes=8 * 1024 * 1024, timeout_seconds=60.0,
                            min_request_interval_seconds=1.0)

#: The feed exists from the 108th Congress on, the same floor as BILLSTATUS bulk.
CBO_FEED_FLOOR = 108


def law_bills_from(path: Path | None) -> dict[str, str] | None:
    """``laws.law_id`` -> ``laws.bill_id`` from the published ``laws`` table, or ``None`` without a readable one.

    Best-effort, as the ``laws`` join on ``congress_bills`` is: an absent or unreadable table leaves a title that
    leads with a public law unmapped rather than failing the run.
    """
    if path is None:
        return None
    import duckdb

    try:
        rows = duckdb.sql(
            f"SELECT law_id, bill_id FROM read_parquet('{path}') WHERE law_id IS NOT NULL AND bill_id IS NOT NULL"
        ).fetchall()
    except duckdb.Error as error:
        logger.warning("Bill family: laws unreadable, so no CBO title reads through a law: {}", error)
        return None
    return dict(rows)


def bill_identity(bill: str) -> BillIdentity:
    """A ``bill_id`` (``119-hr-1``) as the identity spicy-docs keys it by."""
    congress, bill_type, number = bill.split("-")
    return BillIdentity(int(congress), bill_type, int(number))


def prior_citations(path: Path | None, congresses: Collection[int]) -> dict[BillIdentity, tuple[str, ...]]:
    """Each bill's report citations as its published rows state them, for the bills of ``congresses``.

    The rows carry the bill's own BILLSTATUS citations, shaped; the ``citation`` strings re-shape to the same
    JSON. A row with NULL citations names nothing, so the bill is left for its next status read.
    """
    if path is None or not congresses:
        return {}
    import duckdb

    wanted = ", ".join(f"'{congress}'" for congress in sorted(congresses))
    found: dict[BillIdentity, tuple[str, ...]] = {}
    for bill, cited in duckdb.sql(
        f"SELECT bill_id, any_value(report_citations_json) FROM read_parquet('{path}') "
        f"WHERE congress IN ({wanted}) AND report_citations_json IS NOT NULL GROUP BY bill_id"
    ).fetchall():
        found[bill_identity(bill)] = tuple(entry["citation"] for entry in json.loads(cited))
    return found


@dataclass(frozen=True, slots=True)
class FeedRead:
    """What this run read from CBO's feeds: the shaped rows, the refusals, and the Congresses read whole."""

    rows: tuple[dict[str, Any], ...]
    refusals: tuple[FamilyRefusal, ...]
    congresses: frozenset[int]


def read_cbo_feeds(
    congresses: Iterable[int],
    *,
    citations: Mapping[BillIdentity, Sequence[str]],
    law_bills: Mapping[str, str] | None,
    acquirer: Any = None,
    evidence: CaptureEvidence | None = None,
) -> FeedRead:
    """Fetch and shape each Congress's feed once; a feed that cannot be read is logged and keeps its prior rows."""
    scoped = sorted(congress for congress in set(congresses) if congress >= CBO_FEED_FLOOR)
    if not scoped:
        return FeedRead((), (), frozenset())
    if acquirer is None:
        acquirer = CboAcquirer(
            budget=CboBudget(
                max_requests=len(scoped),
                max_bytes=CBO_FEED_BUDGET.max_bytes,
                timeout_seconds=CBO_FEED_BUDGET.timeout_seconds,
                min_request_interval_seconds=CBO_FEED_BUDGET.min_request_interval_seconds,
            ),
            transport=None if evidence is None else evidence.transport(stage="cbo-feed",
                                                                       max_bytes=CBO_FEED_BUDGET.max_bytes),
        )
    rows: list[dict[str, Any]] = []
    refusals: list[FamilyRefusal] = []
    read: set[int] = set()
    for congress in scoped:
        try:
            feed = acquirer.acquire_per_congress_feed(congress).feed
        except (CboSourceError, OSError) as error:
            if evidence is not None:
                evidence.refusal(error, stage="cbo-feed")
            logger.warning("Bill family: CBO's {} feed unread, so its prior feed rows stand: {}", congress,
                           scrub_credential(str(error), ""))
            continue
        tables = build_cbo_feed_cost_estimates(feed, congress, report_citations=citations, law_bills=law_bills)
        rows.extend(tables.cbo_cost_estimates)
        refusals.extend(tables.refusals)
        read.add(congress)
        logger.info("Bill family: CBO's {} feed — {:,} items, {:,} rows, {:,} refused", congress, len(feed.items),
                    len(tables.cbo_cost_estimates), len(tables.refusals))
    return FeedRead(tuple(rows), tuple(refusals), frozenset(read))


type CostEstimateScope = tuple[tuple[str, str], set[tuple[str, str]]]


def cost_estimate_publication(
    billstatus: Sequence[Mapping[str, Any]],
    feed: FeedRead,
    *,
    reread: Collection[str],
    prior: Path | None,
) -> tuple[list[Mapping[str, Any]], CostEstimateScope]:
    """This run's ``cbo_cost_estimates`` rows and the ``(bill_id, source)`` scope they replace.

    ``billstatus`` are the rows this run's status reads shaped, ``reread`` the bills whose BILLSTATUS outcome was
    listed (their prior BILLSTATUS rows are replaced). A feed row survives only where ``merge_cbo_cost_estimates``
    keeps it over this run's BILLSTATUS rows and the prior BILLSTATUS rows of every bill not re-read.
    """
    held: list[dict[str, Any]] = []
    prior_feed_bills: set[str] = set()
    if prior is not None and feed.congresses:
        import duckdb

        wanted = ", ".join(f"'{congress}'" for congress in sorted(feed.congresses))
        relation = duckdb.sql(f"SELECT * FROM read_parquet('{prior}') WHERE congress IN ({wanted})")
        for row in relation.to_arrow_table().to_pylist():
            if row["source"] == CBO_FEED:
                prior_feed_bills.add(row["bill_id"])
            elif row["bill_id"] not in reread:
                held.append(row)
    fresh = [dict(row) for row in billstatus]
    feed_rows = list(feed.rows)
    kept = {CBO_COST_ESTIMATES.key(row): row for row in merge_cbo_cost_estimates([*fresh, *held, *feed_rows])}
    added = [row for row in feed_rows if kept[CBO_COST_ESTIMATES.key(row)] is row]
    if feed_rows:
        logger.info("Bill family: {:,} of {:,} CBO feed rows state an estimate no BILLSTATUS row states",
                    len(added), len(feed_rows))
    scope = {(bill, source) for bill in reread for source in ESTIMATE_SOURCES if source != CBO_FEED}
    scope |= {(bill, CBO_FEED) for bill in prior_feed_bills}
    return [*fresh, *added], (("bill_id", "source"), scope)


__all__ = [
    "CBO_FEED_BUDGET",
    "CBO_FEED_FLOOR",
    "FeedRead",
    "bill_identity",
    "cost_estimate_publication",
    "law_bills_from",
    "prior_citations",
    "read_cbo_feeds",
]
