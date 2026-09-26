"""Congress.gov index tables — house communications, committee meetings, record issues, treaties and nominations —
built with bounded detail reads and own-output resume keyed on each table's marker column.

House communications retain the publisher route on historical rows. The
Congressional Record reconstruction is a separate, deferred acquisition. A table
that keeps an earlier Congress in scope lists it only from the day before its
held rows' newest stamp, so that Congress costs its changes, not a re-walk. A
held detail that no retained response backs is read once more, after each
run's own queue and under the same cap, until the journal's remainder is empty.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence, Set
from dataclasses import dataclass
from datetime import date, timedelta
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
from spicy_docs.sources.congress.listing import (
    LIST_ROUTES,
    MAX_LIMIT,
    CongressListRoute,
    list_route_url,
    utc_day_window,
)
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key, listing_reader
from spicy_regs.transforms.congress_scope import congresses_from_env, current_congress, record_volumes
from spicy_regs.transforms.congress_walk import ListingSource, PooledListingSource
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence


#: Pages per complete pass of a list unit; pooled passes vary the page size.
#: The largest unit measured is
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
    #: ``(label, url)`` per list unit for the scoped Congresses, each listed
    #: from its ``fromDateTime`` in the mapping when it has one.
    units: Callable[[Sequence[int], Mapping[int, str]], Sequence[tuple[str, str]]]
    shape: Callable[[Mapping[str, Any], Mapping[str, Any] | None], Row]
    detail_route: CongressListRoute | None = None
    #: A detail-only column, NULL exactly when no detail was read.
    detail_marker: str | None = None
    #: ``list_route_url`` keywords for the detail from the shaped list row, or
    #: ``None`` when the row has no addressable detail.
    detail_query: Callable[[Row], Mapping[str, Any] | None] | None = None
    #: Earlier Congresses the default scope keeps, listed through an update
    #: window (:func:`_windows`). A route whose records keep changing after
    #: their Congress ends needs them; its ``units`` must honor the window.
    trailing_congresses: int = 0


def _per_congress(route: CongressListRoute) -> Callable[[Sequence[int], Mapping[int, str]], Sequence[tuple[str, str]]]:
    return lambda congresses, windows: [
        (
            f"{route.name}/{congress}",
            list_route_url(route, congress=congress, limit=MAX_LIMIT, from_datetime=windows.get(congress)),
        )
        for congress in congresses
    ]


def _record_units(congresses: Sequence[int], _windows: Mapping[int, str]) -> Sequence[tuple[str, str]]:
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
        # A meeting's record is updated when GovInfo prints its transcript, up
        # to two years on: in 2026 through 2026-09-26, 204 of the 118th's
        # 3,326 meetings changed and 5 of the 117th's (receipt
        # join-gaps-2026-09-26/f/). hearing_transcripts joins on event_id, and
        # every transcript it could not join was a 118th meeting.
        trailing_congresses=1,
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


def _unevidenced(held: Mapping[tuple, _Held], table: str, evidence: CaptureEvidence | None) -> set[tuple]:
    """Held rows whose detail no retained response backs, to be read once more under evidence.

    A prior whose evidence journal states the table's remainder hands it on;
    any other prior backs none of its held details: a legacy one
    (``inputs=[]``), or one retained before the remainder was journaled. So
    the set starts as every held detail and only shrinks as runs read it. A
    run without evidence has nothing to back and re-reads nothing.
    """
    if evidence is None:
        return set()
    read = {key for key, state in held.items() if state.read}
    selection = evidence.inherited_event("congress-index-selection", table=table)
    stated = None if selection is None else selection.get("unevidenced")
    return read if stated is None else read & {tuple(key) for key in stated}


def _windows(
    held: Mapping[tuple, _Held], congresses: Sequence[int], today: date | None = None, unevidenced: Set[tuple] = frozenset()
) -> dict[int, str]:
    """``fromDateTime`` per earlier Congress with held rows: the day before its newest stamp.

    The current Congress is walked whole. An earlier one is listed
    from the day before its newest held stamp, or before its oldest row whose
    detail is unread or unevidenced, so a row deferred by the detail cap is
    listed again until it is read. A Congress with nothing held is walked whole
    once. The key's first part is its Congress.
    """
    current = current_congress(today)
    earliest: dict[int, str] = {}
    newest: dict[int, str] = {}
    for key, state in held.items():
        if not state.stamp or not str(key[0]).isdecimal() or int(key[0]) >= current:
            continue
        congress = int(key[0])
        newest[congress] = max(newest.get(congress, state.stamp), state.stamp)
        if not state.read or key in unevidenced:
            earliest[congress] = min(earliest.get(congress, state.stamp), state.stamp)
    windows = {}
    for congress in congresses:
        if congress in newest:
            stamp = earliest.get(congress, newest[congress])
            opens, _ = utc_day_window(date.fromisoformat(stamp[:10]) - timedelta(days=1), None)
            if opens:
                windows[congress] = opens
    return windows


@dataclass(slots=True)
class _Listed:
    """One listed record, shaped list-only, and its key."""

    key: tuple[str, ...]
    record: Mapping[str, Any]
    row: Row


def _walk(
    reader: PooledListingSource,
    spec: IndexSpec,
    congresses: Sequence[int],
    identity: tuple[str, ...],
    windows: Mapping[int, str],
) -> list[_Listed]:
    """Settle each scoped list by identity before reading details or writing output.

    Equal row counts can conceal repeated and missing identities at page
    boundaries. The source reader varies page sizes over bounded whole walks
    and refuses a pool that cannot reach the declared total.
    """

    def entry(record: Mapping[str, Any]) -> _Listed:
        row = spec.shape(record, None)
        parts = [row[column] for column in identity]
        if any(part is None for part in parts):
            raise PagedJsonSourceError(f"{spec.table}: list record names no keyable row")
        return _Listed(tuple(str(part) for part in parts), record, row)

    listed: dict[tuple[str, ...], _Listed] = {}
    for label, url in spec.units(congresses, windows):
        pooled = reader.pooled(
            spec.list_route,
            url,
            # JSON preserves the complete tuple, including a treaty's valid
            # empty suffix; it is not a missing identity component.
            key=lambda record: json.dumps(entry(record).key),
            version=itemgetter("updateDate"),
            max_pages=MAX_PAGES,
        )
        for record in pooled.records:
            current = entry(record)
            listed[current.key] = current
        logger.info(
            "{}: {} — {:,} distinct records, {:,} declared, in {} walk(s)",
            spec.table,
            label,
            len(pooled.records),
            pooled.declared,
            pooled.passes,
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
    reader: PooledListingSource | None = None,
    congresses: Sequence[int] | None = None,
    max_details: int = MAX_DETAILS_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
) -> Path:
    """Walk ``spec``'s list units, read the details the published table lacks, and merge."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"{spec.table} needs an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = listing_reader(api_key, evidence=evidence)
    contract = TABLE_CONTRACTS[spec.table]
    identity, version = contract.identity, contract.version_column
    assert version is not None, f"{spec.table} has no version column to resume on"
    if congresses is None:
        congresses = congresses_from_env(trailing=spec.trailing_congresses)
    congresses = tuple(congresses)

    prior = published_table(output_dir, spec.table, download_prior)
    held = _held_rows(prior, spec, identity, version)
    unevidenced = _unevidenced(held, spec.table, evidence)
    windows = _windows(held, congresses, unevidenced=unevidenced) if spec.trailing_congresses else {}
    listed = _walk(reader, spec, congresses, identity, windows)

    rows: list[Row] = []
    # Each wanted detail, and whether it is only a re-read for evidence of a row already held.
    queue: list[tuple[_Listed, Mapping[str, Any], bool]] = []
    outcomes: Counter[str] = Counter()

    def refuse(entry: _Listed, state: _Held | None, error: Exception) -> None:
        """One row's refusal: counted, logged scrubbed, and a new record still indexed list-only."""
        outcomes["refused"] += 1
        if evidence is not None:
            evidence.refusal(error, stage=f"{spec.table}-detail")
        logger.warning("{}: {} refused: {}", spec.table, "-".join(entry.key), scrub_credential(str(error), ""))
        if state is None:
            rows.append(entry.row)

    for entry in listed:
        if spec.detail_route is None:
            rows.append(entry.row)
            continue
        state = held.get(entry.key)
        stamp = entry.row[version]
        reread = state is not None and state.read and state.stamp == stamp
        if reread and entry.key not in unevidenced:
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
            unevidenced.discard(entry.key)
            if state is None or state.stamp != stamp:
                rows.append(entry.row)
            continue
        queue.append((entry, query, reread))

    # New, changed and unread records before re-reads for evidence, each tier
    # newest first, so a bounded run advances from the present; the identity
    # breaks ties so a re-run over the same listing asks in the same order.
    queue.sort(key=lambda item: (not item[2], item[0].row[version] or "", item[0].key), reverse=True)
    if len(queue) > max_details:
        logger.warning("{}: {:,} details wanted, taking the newest {:,}", spec.table, len(queue), max_details)
    for position, (entry, query, reread) in enumerate(queue):
        state = held.get(entry.key)
        if position >= max_details:
            # Not reached this run: a new record is indexed list-only so the
            # next run finds it by its NULL marker; a held one keeps its row,
            # and an unevidenced one stays in the journaled remainder.
            if state is None:
                rows.append(entry.row)
                outcomes["deferred"] += 1
            else:
                outcomes["unevidenced" if reread else "stale"] += 1
            continue
        try:
            detail = _detail(reader, spec, entry, query, identity)
        except CredentialRefusedError:
            raise
        except _REFUSALS as error:
            refuse(entry, state, error)
            continue
        rows.append(spec.shape(entry.record, detail))
        outcomes["reread" if reread else "read"] += 1
        unevidenced.discard(entry.key)

    logger.info("{}: {:,} listed; {}", spec.table, len(listed), dict(outcomes) or "every row complete from the list")
    if evidence is not None:
        # ``unevidenced`` is the remainder the next run inherits (``_unevidenced``),
        # including a held row the publisher no longer lists, which no run can re-read.
        evidence.event("congress-index-selection", table=spec.table, congresses=list(congresses),
                       windows={str(congress): start for congress, start in windows.items()},
                       max_details=max_details, listed=len(listed), outcomes=dict(outcomes),
                       unevidenced=sorted(list(key) for key in unevidenced))
    output = merge_contract_table(
        output_dir, spec.table, rows, download_prior=download_prior, prior_present=prior is not None
    )
    if spec.table == "house_communications":
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
        from spicy_docs.schemas.congress_index_tables import COMMUNICATION_SOURCE_ROUTES

        table = pq.read_table(output)
        index = table.schema.get_field_index("source_route")
        table = table.set_column(
            index, "source_route", pc.fill_null(table["source_route"], COMMUNICATION_SOURCE_ROUTES[0])
        )
        temporary = output.with_suffix(".tmp.parquet")
        pq.write_table(table, temporary, compression="zstd")
        temporary.replace(output)
    return output


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
