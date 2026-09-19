"""Transform: the five Congress.gov index tables, from one incremental walk.

``house_communications``, ``committee_meetings``, ``record_issues``,
``treaties`` and ``nominations`` (spicy-docs ``schemas/congress_index_tables.py``,
gaps A5, A7 and A10 of ``docs/research/closing-the-gaps-2026-09-19.md``) are
each one row per record of one Congress.gov list route, four of them completed
by that record's own detail route. The five differ only in their address, their
shaper and the column that says a detail was read, so one walk
(:func:`build_index_table`) serves five :class:`IndexSpec` entries rather than
five copies of the same loop. Each spec is published by its own rollup
(``pipelines/rollups/congress_index.py``) so a refusal on one route fails one
table's run and not four others', the isolation ``pipelines/rollups/base.py``
gives as the reason rollups are separate at all.

**Every run walks the whole list.** None of the five list routes honours
``sort`` (measured, spicy-docs ``docs/sources/listings.md``), so there is no
newest-first page to stop on, and a walk cut short would silently drop rows
sitting later in an unordered listing -- the reason the roll-call index is not
short-circuited either. A date window is not used: it is measured honoured on
``committee-meeting`` and ``treaty`` only, measured *ignored* on the daily
Record, and an unmeasured default on the other two, and a window that filtered
on some field other than ``updateDate`` would hide a changed row from the
resume below. The list is cheap (4,975 communications are 20 pages of 250) and
the reader refuses an incomplete or inconsistent walk, so a walk that returns
*is* the declared count; the per-unit line in the run log states declared,
walked and pages, and a repeated record across a page boundary is counted
rather than published twice (measured on the bill route, A11).

**Details are what is skipped, and what is capped.** The published table is
the resume state, because the contract already distinguishes the two cases
that matter: a list-only row has every detail-only column NULL, and a row
whose detail was read has ``[]`` where the detail states none. So
``detail_marker`` -- one detail-only column, NULL exactly when no detail was
read -- and ``update_date`` together decide each listed record:

* not held: queued; beyond the cap it is published list-only *now*, so the
  index is complete after the first run and its NULL marker queues it next;
* held at the same stamp with the marker set: no request;
* held with a NULL marker (an earlier refusal, or list-only under the cap):
  queued, which is how every previously unsuccessful row is retried;
* held at an older stamp: queued; not reached or refused, the prior row
  stands and its stale stamp queues it again, because publishing a list-only
  replacement would erase a detail already held.

One queue per table, newest ``updateDate`` first, at most
:data:`MAX_DETAILS_PER_RUN` requests. A ``401``/``403`` aborts the run. A
``404``, a malformed page, a transport failure after the reader's retries, a
type the detail route's vocabulary refuses, or a detail naming a different
record than the one asked for is that row's refusal: counted, logged scrubbed,
never a row with invented values. The identity is read off the shaper's own
output for both the list row and the detail, so the key is spelled in exactly
one place, the wheel.

``nominations`` has no detail route; every list row is complete and is
published every run, fresh row winning, the way ``amendments`` publishes its
window. A partitioned treaty (a non-empty ``suffix``) has no detail route in
``LIST_ROUTES`` either, so it is published list-only and never queued.

The RIN on a communication is ``interpretation.communication_rin``'s finding
over the detail's ``reportNature``, computed at shape time; a list-only row
carries NULL in all three RIN columns, since the rule was not run on it.

Needs an api.data.gov key, resolved the way every Congress.gov consumer here
resolves it and sent only as a header by the spicy-docs reader.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from loguru import logger
from spicy_docs.interpretation.communication_rin import rin_from_report_nature
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.congress_index_tables import (
    shape_committee_meeting,
    shape_house_communication,
    shape_nomination,
    shape_record_issue,
    shape_treaty,
)
from spicy_docs.schemas.tables import Row, TableContractError
from spicy_docs.sources.congress.listing import LIST_ROUTES, MAX_LIMIT, CongressListRoute, list_route_url
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key, listing_reader
from spicy_regs.transforms.congress_scope import congresses_from_env, record_volumes
from spicy_regs.transforms.congress_walk import ListingSource
from spicy_regs.transforms.table_merge import merge_contract_table, published_table


#: Pages of ``MAX_LIMIT`` per list unit. The largest unit measured is
#: ``house-communication/119`` at 4,975 (20 pages); an older Congress with
#: twice that still fits, and reaching the bound is the reader's refusal, not
#: a quiet stop.
MAX_PAGES = 100

#: Detail requests per run, per table. At the reader's 0.2 s pacing this is
#: under four minutes; a cold start on the 119th's communications converges in
#: five runs, and every run after that pays for the day's changes only.
MAX_DETAILS_PER_RUN = 1_000

#: What one detail request can refuse with, short of a credential refusal,
#: which propagates: the reader's own refusals (``404``/``410`` included), a
#: transport failure after its retries, a shaper refusing the record, and the
#: URL builder refusing a value the route's vocabulary does not know.
_REFUSALS = (PagedJsonSourceError, httpx.HTTPError, ConnectionError, TableContractError, ValueError, TypeError)


@dataclass(frozen=True, slots=True)
class IndexSpec:
    """One index table: its routes, its shaper, and how a list row addresses its detail."""

    table: str
    list_route: CongressListRoute
    #: ``(label, url)`` per list unit for the scoped Congresses.
    units: Callable[[Sequence[int]], Sequence[tuple[str, str]]]
    shape: Callable[[Mapping[str, Any], Mapping[str, Any] | None], Row]
    detail_route: CongressListRoute | None = None
    #: A detail-only column, NULL exactly when no detail was read.
    detail_marker: str | None = None
    #: ``list_route_url`` keywords for the detail from the shaped list row, or
    #: ``None`` when the row has no addressable detail.
    detail_query: Callable[[Row], Mapping[str, Any] | None] | None = None


def _per_congress(route: CongressListRoute) -> Callable[[Sequence[int]], Sequence[tuple[str, str]]]:
    return lambda congresses: [
        (f"{route.name}/{congress}", list_route_url(route, congress=congress, limit=MAX_LIMIT))
        for congress in congresses
    ]


def _record_units(congresses: Sequence[int]) -> Sequence[tuple[str, str]]:
    route = LIST_ROUTES["daily-congressional-record"]
    return [
        (f"{route.name}/{volume}", list_route_url(route, volume=volume, limit=MAX_LIMIT))
        for congress in congresses
        for volume in record_volumes(congress)
    ]


def _shape_communication(listed: Mapping[str, Any], detail: Mapping[str, Any] | None) -> Row:
    rin = None if detail is None else rin_from_report_nature(detail.get("reportNature"))
    return shape_house_communication(listed, detail, rin=rin)


def _communication_query(row: Row) -> Mapping[str, Any]:
    return {
        "congress": int(str(row["congress"])),
        "communication_type": row["communication_type"],
        "number": int(str(row["number"])),
    }


def _meeting_query(row: Row) -> Mapping[str, Any]:
    return {"congress": int(str(row["congress"])), "chamber": row["chamber"], "event_id": int(str(row["event_id"]))}


def _issue_query(row: Row) -> Mapping[str, Any]:
    return {"volume": int(str(row["volume"])), "issue": int(str(row["issue"]))}


def _treaty_query(row: Row) -> Mapping[str, Any] | None:
    # ``treaty/{congress}/{number}`` addresses an unpartitioned treaty only;
    # the publisher's suffixed form has no route in LIST_ROUTES.
    if row["suffix"]:
        return None
    return {"congress": int(str(row["congress_received"])), "number": int(str(row["number"]))}


INDEX_SPECS: Mapping[str, IndexSpec] = {
    "house_communications": IndexSpec(
        "house_communications",
        LIST_ROUTES["house-communication"],
        _per_congress(LIST_ROUTES["house-communication"]),
        _shape_communication,
        detail_route=LIST_ROUTES["house-communication-detail"],
        detail_marker="committees_json",
        detail_query=_communication_query,
    ),
    "committee_meetings": IndexSpec(
        "committee_meetings",
        LIST_ROUTES["committee-meeting"],
        _per_congress(LIST_ROUTES["committee-meeting"]),
        shape_committee_meeting,
        detail_route=LIST_ROUTES["committee-meeting-detail"],
        detail_marker="committees_json",
        detail_query=_meeting_query,
    ),
    "record_issues": IndexSpec(
        "record_issues",
        LIST_ROUTES["daily-congressional-record"],
        _record_units,
        shape_record_issue,
        detail_route=LIST_ROUTES["daily-congressional-record-detail"],
        detail_marker="sections_json",
        detail_query=_issue_query,
    ),
    "treaties": IndexSpec(
        "treaties",
        LIST_ROUTES["treaty"],
        _per_congress(LIST_ROUTES["treaty"]),
        shape_treaty,
        detail_route=LIST_ROUTES["treaty-detail"],
        detail_marker="titles_json",
        detail_query=_treaty_query,
    ),
    "nominations": IndexSpec(
        "nominations",
        LIST_ROUTES["nomination"],
        _per_congress(LIST_ROUTES["nomination"]),
        lambda listed, _detail: shape_nomination(listed),
    ),
}


@dataclass(frozen=True, slots=True)
class _Held:
    """What the published table says about one identity: its stamp, and whether a detail was read."""

    stamp: str | None
    read: bool


def _held_rows(prior: Path | None, spec: IndexSpec, identity: tuple[str, ...], version: str) -> dict[tuple, _Held]:
    if prior is None or spec.detail_marker is None:
        return {}
    import duckdb

    published = {str(row[0]) for row in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{prior}')").fetchall()}
    if spec.detail_marker in published:
        read = f"{spec.detail_marker} IS NOT NULL"
    else:
        # A prior without the marker column (a renamed contract column, or a
        # table published before it) cannot say a detail was read, so none was:
        # every row is re-read rather than the run refusing on the column.
        read = "FALSE"
        logger.warning("{}: prior table lacks {!r}; every row reads as unread", spec.table, spec.detail_marker)
    columns = ", ".join((*identity, version, read))
    rows = duckdb.sql(f"SELECT {columns} FROM read_parquet('{prior}')").fetchall()
    held = {tuple(row[: len(identity)]): _Held(row[len(identity)], bool(row[len(identity) + 1])) for row in rows}
    logger.info(
        "{}: {:,} rows published, {:,} with a detail read",
        spec.table,
        len(held),
        sum(1 for state in held.values() if state.read),
    )
    return held


@dataclass(slots=True)
class _Listed:
    """One listed record, shaped list-only, and its key."""

    key: tuple[str, ...]
    record: Mapping[str, Any]
    row: Row


def _walk(
    reader: ListingSource, spec: IndexSpec, congresses: Sequence[int], identity: tuple[str, ...]
) -> list[_Listed]:
    """Every record the scoped units list, keyed by the shaper's identity; repeats and unkeyable records counted."""
    listed: dict[tuple[str, ...], _Listed] = {}
    for label, url in spec.units(congresses):
        declared: int | None = None
        walked = pages = repeated = unkeyable = 0
        for page in reader.records(spec.list_route, url, max_pages=MAX_PAGES):
            pages += 1
            if declared is None and page.declared_count is not None:
                declared = int(page.declared_count)
            for record in page.records:
                walked += 1
                try:
                    row = spec.shape(record, None)
                except TableContractError as error:
                    unkeyable += 1
                    logger.warning("{}: {} list record refused by the shaper: {}", spec.table, label, error)
                    continue
                parts = [row[column] for column in identity]
                if any(part is None for part in parts):
                    unkeyable += 1
                    logger.warning("{}: {} list record names no keyable row", spec.table, label)
                    continue
                key = tuple(str(part) for part in parts)
                if key in listed:
                    repeated += 1
                listed[key] = _Listed(key, record, row)
        logger.info(
            "{}: {} — declared {}, walked {:,} on {} page(s); {:,} repeated, {:,} unkeyable",
            spec.table,
            label,
            "?" if declared is None else f"{declared:,}",
            walked,
            pages,
            repeated,
            unkeyable,
        )
        if repeated and declared is not None:
            # Declared equal to walked is a row count, not an identity count:
            # each repeat displaced a record this walk never delivered. The
            # next whole walk delivers it, which is why the list is never
            # short-circuited.
            logger.info(
                "{}: {} — {:,} distinct records delivered of {:,} declared; the {:,} not delivered"
                " are recovered by the next whole walk",
                spec.table,
                label,
                declared - repeated,
                declared,
                repeated,
            )
    return list(listed.values())


def _detail(
    reader: ListingSource, spec: IndexSpec, entry: _Listed, query: Mapping[str, Any], identity: tuple[str, ...]
) -> Mapping[str, Any]:
    """The one detail record for ``entry``, proven to name the same identity the list row did.

    Raises one of :data:`_REFUSALS` for anything short of that, and lets a
    credential refusal through untouched.
    """
    assert spec.detail_route is not None
    url = list_route_url(spec.detail_route, limit=1, **query)
    page = next(iter(reader.records(spec.detail_route, url, max_pages=1)))
    if len(page.records) != 1:
        raise PagedJsonSourceError(f"{spec.detail_route.name} answered {len(page.records)} records, not one")
    detail = page.records[0]
    # Shaped here on its own, and again by the caller with the list row: this
    # pass reads the identity the *detail* states, through the same shaper
    # that keyed the list row, so the comparison is key-spelling to
    # key-spelling; the caller's pass is the row that is published. The cost
    # is one dict build per detail request, nothing against the request.
    stated = spec.shape(detail, detail)
    if tuple(str(stated[column]) for column in identity) != entry.key:
        raise PagedJsonSourceError(f"{spec.detail_route.name} identity differs from the requested record")
    return detail


def build_index_table(
    output_dir: Path,
    spec: IndexSpec,
    *,
    reader: ListingSource | None = None,
    congresses: Sequence[int] | None = None,
    max_details: int = MAX_DETAILS_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> Path:
    """Walk ``spec``'s list units, read the details the published table lacks, and merge."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"{spec.table} needs an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = listing_reader(api_key)
    contract = TABLE_CONTRACTS[spec.table]
    identity, version = contract.identity, contract.version_column
    assert version is not None, f"{spec.table} has no version column to resume on"
    congresses = tuple(congresses) if congresses is not None else congresses_from_env()

    prior = published_table(output_dir, spec.table, download_prior)
    held = _held_rows(prior, spec, identity, version)
    listed = _walk(reader, spec, congresses, identity)

    rows: list[Row] = []
    queue: list[tuple[_Listed, Mapping[str, Any]]] = []
    outcomes: Counter[str] = Counter()

    def refuse(entry: _Listed, state: _Held | None, error: Exception) -> None:
        """One row's refusal: counted, logged scrubbed, and a new record still indexed list-only."""
        outcomes["refused"] += 1
        logger.warning("{}: {} refused: {}", spec.table, "-".join(entry.key), scrub_credential(str(error), ""))
        if state is None:
            rows.append(entry.row)

    for entry in listed:
        if spec.detail_route is None:
            rows.append(entry.row)
            continue
        state = held.get(entry.key)
        stamp = entry.row[version]
        if state is not None and state.read and state.stamp == stamp:
            outcomes["held"] += 1
            continue
        assert spec.detail_query is not None
        try:
            # Built once here, under the same refusal rule as the request it
            # addresses: a publisher value the builder cannot spell (a
            # non-numeric number, say) is this row's refusal, not the run's.
            query = spec.detail_query(entry.row)
        except (ValueError, TypeError) as error:
            refuse(entry, state, error)
            continue
        if query is None:
            # No route addresses this detail; the list row is the whole fact.
            outcomes["list_only"] += 1
            if state is None or state.stamp != stamp:
                rows.append(entry.row)
            continue
        queue.append((entry, query))

    # Newest first, so a bounded run advances from the present; the identity
    # breaks ties so a re-run over the same listing asks in the same order.
    queue.sort(key=lambda item: (item[0].row[version] or "", item[0].key), reverse=True)
    if len(queue) > max_details:
        logger.warning("{}: {:,} details wanted, taking the newest {:,}", spec.table, len(queue), max_details)
    for position, (entry, query) in enumerate(queue):
        state = held.get(entry.key)
        if position >= max_details:
            # Not reached this run: a new record is indexed list-only so the
            # next run finds it by its NULL marker; a held one keeps its row.
            if state is None:
                rows.append(entry.row)
                outcomes["deferred"] += 1
            else:
                outcomes["stale"] += 1
            continue
        try:
            detail = _detail(reader, spec, entry, query, identity)
        except CredentialRefusedError:
            raise
        except _REFUSALS as error:
            refuse(entry, state, error)
            continue
        rows.append(spec.shape(entry.record, detail))
        outcomes["read"] += 1

    logger.info("{}: {:,} listed; {}", spec.table, len(listed), dict(outcomes) or "every row complete from the list")
    return merge_contract_table(output_dir, spec.table, rows, prior_present=prior is not None)


def build_house_communications(output_dir: Path, **kwargs: Any) -> Path:
    return build_index_table(output_dir, INDEX_SPECS["house_communications"], **kwargs)


def build_committee_meetings(output_dir: Path, **kwargs: Any) -> Path:
    return build_index_table(output_dir, INDEX_SPECS["committee_meetings"], **kwargs)


def build_record_issues(output_dir: Path, **kwargs: Any) -> Path:
    return build_index_table(output_dir, INDEX_SPECS["record_issues"], **kwargs)


def build_treaties(output_dir: Path, **kwargs: Any) -> Path:
    return build_index_table(output_dir, INDEX_SPECS["treaties"], **kwargs)


def build_nominations(output_dir: Path, **kwargs: Any) -> Path:
    return build_index_table(output_dir, INDEX_SPECS["nominations"], **kwargs)
