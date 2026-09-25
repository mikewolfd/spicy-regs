"""Transform: build ``laws``, ``law_code_sections`` and ``table3_records``.

Three tables from three publishers in one pass, because the second and third
are addressed by what the first enumerates. **``laws``**: the Congress.gov
``law/{congress}`` list route walked whole per scoped Congress (it lists
private laws too when ``law_type`` is omitted), one row per
``laws[]`` entry, each joined to the GovInfo PLAW USLM file whose ``<meta>``
states the Statutes at Large citation (one keyless request per law, under the
per-run cap). ``shape_law`` refuses a meta that states another law, so a
citation never lands on the wrong row; a validated file that states no
``NNN Stat. NNN`` (the private laws) is ``captured_partial``, its literal
``citableAs`` kept and its Statutes fields NULL.
**``law_code_sections``**: the OLRC per-Congress classification table, read in
the publisher's public-law order alone (the index links the current Congress's
session tables only) — the two orders hold the same rows, but the contract
keys a row on its position in one page, so the code-order twin would collide.
**``table3_records``**: Table III's own chain, per Congress, walked by
spicy-docs' ``iter_table3_chain``. Every served act page names the next act
the table holds (``Table3Page.next_act``: 119-4 names 119-12), so the walk asks
only the acts the chain names, under this rollup's cap and deadline. Table III
lags enactment and holds no page for an act that classified nothing; OLRC
answers such an act with a connection dropped inside its site menu, which is
why the walk never probes a number the chain does not name.

**Incremental.** The list is re-walked whole every run; the PLAW is what is
not re-read. A law already published ``captured`` or ``captured_partial`` under
the current :data:`~spicy_docs.schemas.law_tables.USLM_READER_VERSION` with the
same ``update_date`` is left standing; any other is asked for again, newest
first, under :data:`MAX_USLM_PER_RUN`. Every attempt publishes a truthful
outcome (``unavailable`` only for a ``404``/``410`` from the exact locator),
except that a failed attempt never replaces a validated prior row; a law the
cap does not reach keeps its prior row, or gets its list row with
``not_requested``. A classification page read this run replaces every prior
row for its Congress and session. Table III keeps one checkpoint per act
(reader rule, capture digest, shaped-row digest): a held act whose checkpoint
is missing or from an older rule is read again before any chain walks, and a
successful read replaces that act's whole row set, an empty one included,
while a failed read keeps its rows and stays retryable. A ``401``/``403`` from
any of the three publishers aborts the run. Needs an api.data.gov key for the
list route; the PLAW and OLRC routes are keyless.
"""

from __future__ import annotations

import re
import time
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, cast

import httpx
from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.law_tables import USLM_READER_VERSION, shape_law, shape_law_code_section, shape_table3_record
from spicy_docs.schemas.tables import TableContractError, digest, json_column
from spicy_docs.sources.congress.listing import LIST_ROUTES, MAX_LIMIT, CongressListingReader, list_route_url
from spicy_docs.sources.govinfo.uslm import PublicLawSelection, UslmIdentityError, UslmSourceError
from spicy_docs.sources.govinfo.uslm_acquisition import (
    UslmAcquirer,
    UslmAcquisitionBudget,
    UslmSourceUnavailableError,
)
from spicy_docs.sources.uscode import Table3Page, UsCodeSourceError, iter_table3_chain
from spicy_docs.sources.uscode.acquisition import UsCodeAcquirer, UsCodeAcquisitionBudget
from spicy_docs.sources.uscode.table3 import TABLE3_READER_VERSION
from spicy_docs.transport.captured import attached_capture
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env
from spicy_regs.transforms.congress_walk import ListingSource, PerRunCap, walk_route
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence


class LawTextSource(Protocol):
    """What this transform needs of the PLAW USLM acquirer."""

    def acquire_public_law(self, selection: Any, *, max_bytes: int | None = ...) -> Any: ...


class OlrcSource(Protocol):
    """What this transform needs of the OLRC acquirer: the classification index and tables, and Table III."""

    def acquire_classification_index(self, *, max_bytes: int | None = ...) -> Any: ...

    def acquire_classification_table(
        self, congress: int, session: int, *, order: Any = ..., max_bytes: int | None = ..., max_rows: int = ...
    ) -> Any: ...

    def acquire_table3_act(self, key: str, *, max_bytes: int | None = ..., max_rows: int = ...) -> Any: ...


LIST_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: One request per law, paced; a PLAW file is tens of KB to a few MB
#: (the acquirer's own default allowance is 32 MB).
USLM_BUDGET = UslmAcquisitionBudget(
    max_requests=4,
    max_bytes=32 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.5,
)

#: OLRC pages are generated per request and can be slow; the largest read
#: here is a session table at ~115 KB.
OLRC_BUDGET = UsCodeAcquisitionBudget(
    max_requests=4,
    max_bytes=8 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=1.0,
)

#: Pages of ``MAX_LIMIT`` per Congress; a Congress enacts a few hundred laws.
MAX_PAGES = 20

#: PLAW files asked for per run. A Congress's laws fit in one run at the
#: pacing above (~2.5 minutes for 300); the cap bounds a multi-Congress
#: backfill so a run publishes before it times out.
MAX_USLM_PER_RUN = 300

#: Table III pages asked for per run, oldest unread act first.
MAX_TABLE3_PER_RUN = 300

NAME = "laws"
CODE_SECTIONS = "law_code_sections"
TABLE3 = "table3_records"
CAPTURED = "captured"
#: The outcomes whose USLM identity was validated; a failed later attempt never replaces one.
VALIDATED = ("captured", "captured_partial")
PUBLIC = "public"
#: The one classification order read; see the module docstring.
TABLE_ORDER = "public-law"


class HeldLaw(NamedTuple):
    """The prior table's state for one ``law_id``: its list ``update_date`` and USLM outcome."""

    update_date: str | None
    uslm_outcome: str | None
    reader_version: str | None = None


#: Consecutive Table III failures that end the walk for the run: the publisher
#: is not answering. A failure is a chain act that did not serve (a transport
#: failure the acquirer has already retried, or a page the reader refuses); it
#: ends its own Congress's chain, since only the page names the next act, so
#: the count runs across Congresses and a page read resets it.
TABLE3_STOP_AFTER = 3

#: Wall-clock seconds the Table III walk may start requests in. The laws job
#: times out at 60 minutes and publishes nothing if it does, so this leaves
#: room for the rest: the other legs took 30 seconds on 2026-09-24 (a cold
#: PLAW leg of 300 files takes about 2.5 minutes), and one act can overrun the
#: deadline by its four attempts at the 120-second timeout plus backoff, about
#: 8.5 minutes. That totals under 35 minutes.
TABLE3_DEADLINE_SECONDS = 20 * 60

#: A public-law key as a Table III page states it, en dash and all.
_PUBLIC_LAW_KEY = re.compile(r"(\d+)[-\u2013](\d+)")


def olrc_acquirer(
    transport: httpx.BaseTransport | None = None, *, budget: UsCodeAcquisitionBudget = OLRC_BUDGET
) -> UsCodeAcquirer:
    """The OLRC acquirer the rollup reads with; the arguments are for tests and measurements."""
    return UsCodeAcquirer(budget=budget, transport=transport)


def _transport_error(error: BaseException) -> str:
    return scrub_credential(str(error), "")


def _held_laws(prior_file: Path | None, congresses: tuple[int, ...]) -> dict[str, HeldLaw]:
    """``law_id`` -> what is published for it, for the scoped Congresses."""
    if prior_file is None:
        return {}
    import duckdb

    columns = {row[0] for row in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{prior_file}')").fetchall()}
    version = "uslm_reader_version" if "uslm_reader_version" in columns else "NULL"
    rows = duckdb.sql(
        f"SELECT law_id, update_date, uslm_outcome, {version} FROM read_parquet('{prior_file}') WHERE congress IN (SELECT UNNEST(?))",
        params=[[str(c) for c in congresses]],
    ).fetchall()
    return {str(law_id): HeldLaw(update_date, outcome, version) for law_id, update_date, outcome, version in rows}


def _held_public_laws(prior_file: Path | None, congresses: tuple[int, ...]) -> set[tuple[int, int]]:
    """``(congress, number)`` of every public law already published for the scoped Congresses."""
    if prior_file is None:
        return set()
    import duckdb

    rows = duckdb.sql(
        f"SELECT congress, number FROM read_parquet('{prior_file}') WHERE law_type = ? AND congress IN (SELECT UNNEST(?))",
        params=[PUBLIC, [str(c) for c in congresses]],
    ).fetchall()
    return {(int(congress), int(number)) for congress, number in rows}


def _held_acts(prior_file: Path | None) -> set[str]:
    if prior_file is None:
        return set()
    import duckdb

    return {str(row[0]) for row in duckdb.sql(f"SELECT DISTINCT act_key FROM read_parquet('{prior_file}')").fetchall()}


def _list_laws(
    reader: ListingSource, congresses: tuple[int, ...]
) -> list[tuple[Mapping[str, Any], Mapping[str, Any], dict]]:
    """Every ``(record, laws[] entry, plain row)`` the route lists, newest law first.

    The plain row — the shaper with no USLM — is the key and the version the
    incremental rule reads; an entry the contract refuses is logged and
    skipped rather than published with a hole in its key.
    """
    route = LIST_ROUTES["law"]
    listed: list[tuple[Mapping[str, Any], Mapping[str, Any], dict]] = []
    for congress in congresses:
        walk = walk_route(
            reader,
            route,
            list_route_url(route, congress=congress, limit=MAX_LIMIT),
            max_pages=MAX_PAGES,
            label=f"Laws: Congress {congress}",
        )
        for record in walk.records:
            entries = record.get("laws")
            for law in entries if isinstance(entries, list) else ():
                if not isinstance(law, Mapping):
                    continue
                try:
                    plain = shape_law(record, law)
                except TableContractError as error:
                    logger.warning("Laws: list entry {!r} refused by the contract: {}", law, error)
                    continue
                listed.append((record, law, plain))
    listed.sort(key=lambda item: (int(item[2]["congress"]), int(item[2]["number"])), reverse=True)
    return listed


def _uslm_row(
    uslm: LawTextSource, record: Mapping[str, Any], law: Mapping[str, Any], plain: dict,
    evidence: CaptureEvidence | None = None,
) -> dict:
    """The law's row with its PLAW leg: every attempted read states a truthful outcome.

    ``unavailable`` is only a ``404``/``410`` from the exact PLAW locator. A
    ``200`` body the reader refuses is ``captured_refused``, with the reason
    saying whether it stated another law (``native_identity_refused``) or was
    not the requested native USLM at all (``native_shape_refused``, e.g. an
    HTML page); its digest is kept, its unvalidated metadata is not. Any other
    failure is ``request_failed``. A ``401``/``403`` propagates and aborts the
    run. The caller decides whether a failed attempt may replace a prior row.
    """
    selection = PublicLawSelection(int(plain["congress"]), plain["law_type"], int(plain["number"]))
    stage = "uslm:" + plain["law_id"]
    try:
        acquired = uslm.acquire_public_law(selection)
    except UslmSourceUnavailableError as error:
        if evidence:
            evidence.refusal(error, stage=stage)
        return shape_law(record, law, uslm_outcome="unavailable", uslm_reason="source_unavailable")
    except (UslmSourceError, httpx.HTTPError, ConnectionError) as error:
        if evidence:
            evidence.refusal(error, stage=stage)
        capture = attached_capture(error)
        refused = capture is not None and capture.status_code == 200
        reason = "native_identity_refused" if isinstance(error, UslmIdentityError) else "native_shape_refused"
        return shape_law(
            record, law, uslm_outcome="captured_refused" if refused else "request_failed",
            uslm_reason=reason if refused else "request_failed",
            uslm_sha256=capture.sha256 if refused else None,
            uslm_observed_at=capture.observed_at if refused else None,
        )
    if evidence:
        evidence.capture(acquired.capture, stage=stage)
    try:
        return shape_law(
            record, law, uslm=acquired.metadata, uslm_sha256=acquired.capture.sha256,
            uslm_observed_at=acquired.capture.observed_at, uslm_outcome=CAPTURED,
        )
    except TableContractError as error:
        if evidence:
            evidence.refusal(error, stage=stage)
        return shape_law(
            record, law, uslm_sha256=acquired.capture.sha256,
            uslm_observed_at=acquired.capture.observed_at,
            uslm_outcome="captured_refused", uslm_reason="contract_refused",
        )


def _law_rows(
    listed: list[tuple[Mapping[str, Any], Mapping[str, Any], dict]],
    held: Mapping[str, HeldLaw],
    uslm: LawTextSource,
    cap: PerRunCap,
    evidence: CaptureEvidence | None = None,
) -> list[dict]:
    rows: list[dict] = []
    outcomes: Counter[str] = Counter()
    unchanged = deferred = 0
    for record, law, plain in listed:
        prior = held.get(plain["law_id"])
        # A validated read under the current rule stands until the list row moves; ``captured_partial`` is
        # the whole truth about a law whose USLM states no Statutes citation, not a read to retry every run.
        if (prior is not None and prior.uslm_outcome in VALIDATED
                and prior.reader_version == USLM_READER_VERSION and prior.update_date == plain["update_date"]):
            unchanged += 1
            continue
        row = _uslm_row(uslm, record, law, plain, evidence) if cap.take() else None
        if row is not None and evidence:
            evidence.event("law-read", law_id=plain["law_id"], outcome=row["uslm_outcome"],
                           reason=row["uslm_reason"], reader_version=USLM_READER_VERSION)
        if (row is not None and prior is not None and prior.uslm_outcome in VALIDATED
                and row["uslm_outcome"] not in VALIDATED):
            # A failed new attempt must not erase an earlier validated body.
            deferred += 1
            continue
        if row is None:
            if prior is not None:
                # The prior row stands until a run reaches this law again.
                deferred += 1
                continue
            row = plain
        outcomes[row["uslm_outcome"]] += 1
        rows.append(row)
    logger.info(
        "Laws: {:,} listed — {:,} rows this run by uslm_outcome {}, {:,} already captured and unchanged, {:,} held over",
        len(listed),
        len(rows),
        dict(outcomes),
        unchanged,
        deferred,
    )
    return rows


def _classification_rows(olrc: OlrcSource, congresses: tuple[int, ...],
                         evidence: CaptureEvidence | None = None, evaluated: set | None = None) -> list[dict]:
    """Every line of each public-law-order session table the index links for a scoped Congress."""
    try:
        acquired_index = olrc.acquire_classification_index()
        if evidence:
            evidence.capture(acquired_index.capture, stage="classification:index")
        index = acquired_index.result
    except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
        if evidence:
            evidence.refusal(error, stage="classification:index")
        logger.warning(
            "Laws: classification index not established — no session table read this run: {}", _transport_error(error)
        )
        return []
    linked = [link for link in index.tables if link.order == TABLE_ORDER and link.congress in congresses]
    unlinked = sorted(set(congresses) - {link.congress for link in linked})
    if unlinked:
        logger.info("Laws: the classification index links no table for Congress {}", unlinked)
    rows: list[dict] = []
    for link in linked:
        try:
            acquired = olrc.acquire_classification_table(link.congress, link.session, order=TABLE_ORDER)
        except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
            if evidence:
                evidence.refusal(error, stage="classification:" + link.href)
            logger.warning("Laws: classification table {} not established: {}", link.href, _transport_error(error))
            continue
        table = acquired.result
        if evidence:
            evidence.capture(acquired.capture, stage="classification:" + link.href)
        if evaluated is not None:
            evaluated.add((str(table.congress), str(table.session)))
        observed_at = acquired.capture.observed_at
        rows.extend(shape_law_code_section(record, table=table, observed_at=observed_at) for record in table.records)
        logger.info(
            "Laws: classification table {} — {:,} rows, stated {!r}, prepared {}",
            link.href,
            len(table.records),
            table.stated_laws,
            table.prepared_date,
        )
    return rows


def _public_law(key: str | None) -> tuple[int, int] | None:
    """``(congress, number)`` of a public-law key such as ``119-4`` or ``119–4``; ``None`` for anything else."""
    match = _PUBLIC_LAW_KEY.fullmatch((key or "").strip())
    return (int(match[1]), int(match[2])) if match else None


def _table3_current(state: Mapping[str, Any]) -> bool:
    """Only a complete, capture-bound checkpoint can skip a reader-version repair."""
    return (state.get("reader_version") == TABLE3_READER_VERSION and state.get("outcome") == "complete"
            and isinstance(state.get("sha256"), str)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", state["sha256"]) is not None
            and isinstance(state.get("observed_at"), str) and bool(state["observed_at"]))


class _WalkStopped(Exception):
    """Raised from the walk's ``acquire`` to end the whole Table III walk: the per-run cap or the deadline."""


def _table3_rows(
    olrc: OlrcSource,
    acts: set[tuple[int, int]],
    held: set[str],
    cap: PerRunCap,
    *,
    deadline_seconds: float = TABLE3_DEADLINE_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    checkpoints: dict[str, dict] | None = None,
    evaluated: set[str] | None = None,
    evidence: CaptureEvidence | None = None,
) -> list[dict]:
    """Re-read stale held acts, then follow each scoped Congress's Table III chain, newest Congress first.

    ``iter_table3_chain`` walks one Congress and owns where a chain ends: a
    page that names no next public law, one in another Congress, one that does
    not follow, one the laws table does not state (``within``), or one past the
    release point the page states (the lag). Before any chain, every held act
    whose checkpoint is missing, from an older reader rule or not bound to a
    capture is read again (``checkpoints``); the old forward frontier alone
    could never repair an earlier act. Each chain then starts at the Congress's
    highest held act, asked again only for the act its page names. A Congress
    with no held act starts where the previous Congress's highest held page
    says the chain continues, and otherwise at the lowest act the laws table
    states, unless that seed page's release point shows Table III holds none of
    this Congress yet. An act is published when it was not held or its shaped
    rows changed (:func:`_rows_digest`; the page bytes embed a per-request
    session id, so their digest changes every read). The per-run cap and the
    deadline are spent in ``acquire``, once per act (its retries included),
    raising to end the walk. A failed act ends its Congress's chain for the
    run, and :data:`TABLE3_STOP_AFTER` failures in a row, stale rereads
    included, end the walk.
    """
    started = clock()
    rows: list[dict] = []
    read: list[str] = []
    failed: list[str] = []
    asked: list[str] = []
    stated: dict[int, set[int]] = {}
    for congress, number in acts:
        stated.setdefault(congress, set()).add(number)
    within = {f"{congress}-{number}" for congress, number in acts}
    held_numbers: dict[int, list[int]] = {}
    for act in filter(None, map(_public_law, held)):
        held_numbers.setdefault(act[0], []).append(act[1])

    cached: dict[str, Any] = {}
    failures: dict[str, Exception] = {}
    consecutive = 0

    def acquire(key: str) -> Any:
        if key in failures:
            raise failures[key]
        if key in cached:
            return cached[key]
        if clock() - started >= deadline_seconds:
            raise _WalkStopped(f"reached its {deadline_seconds:,}-second deadline before {key}")
        if not cap.take():
            raise _WalkStopped(f"reached its per-run cap before {key}")
        asked.append(key)
        try:
            acquired = olrc.acquire_table3_act(key)
        except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
            failures[key] = error
            if evidence:
                evidence.refusal(error, stage="table3:" + key)
            raise
        if evidence:
            evidence.capture(acquired.capture, stage="table3:" + key)
        cached[key] = acquired
        return acquired

    def fail(key: str) -> bool:
        """Record a failed act; ``True`` once :data:`TABLE3_STOP_AFTER` failures in a row are reached."""
        nonlocal consecutive
        if key not in failed:
            failed.append(key)
        consecutive += 1
        return consecutive >= TABLE3_STOP_AFTER

    def materialize(key: str, acquired: Any, page_rows: list[dict] | None = None) -> None:
        if key in cached_rows:
            return
        page = cast(Table3Page, acquired.result)
        page_rows = page_rows if page_rows is not None else _page_rows(page, acquired.capture.observed_at)
        cached_rows[key] = page_rows
        rows.extend(page_rows)
        read.append(key)
        if evaluated is not None:
            evaluated.add(key)
        if checkpoints is not None:
            checkpoints[key] = {"act_key": key, "reader_version": TABLE3_READER_VERSION,
                                "sha256": acquired.capture.sha256, "records_sha256": _rows_digest(page_rows),
                                "observed_at": acquired.capture.observed_at, "next_act": page.next_act,
                                "release_point": page.release_point, "outcome": "complete"}
        if evidence:
            evidence.event("table3-read", act_key=key, rows=len(page_rows),
                           reader_version=TABLE3_READER_VERSION, sha256=acquired.capture.sha256)

    cached_rows: dict[str, list[dict]] = {}

    # Stale held acts first: successful checkpoints shrink this queue across
    # bounded runs; a failed read keeps the act's rows and its old or missing
    # rule, so it stays retryable, and is tried again after never-tried acts.
    # Failures here end only this queue: acts that keep failing must not stop
    # the forward walk from ever discovering a new act, and in a real outage
    # the walk's own first failure stops it one request later.
    if checkpoints is not None:
        acts_by_key = {key: act for key in held if (act := _public_law(key)) is not None and act[0] in stated}
        stale = sorted((key for key in acts_by_key if not _table3_current(checkpoints.get(key, {}))),
                       key=lambda key: (checkpoints.get(key, {}).get("last_attempt", ""), acts_by_key[key]))
        for key in stale:
            try:
                materialize(key, acquire(key))
            except _WalkStopped as stop:
                logger.warning("Laws: Table III walk {}", stop)
                return _table3_summary(rows, read, failed)
            except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
                logger.warning("Laws: Table III stale act {} not re-read: {}", key, _transport_error(error))
                checkpoints[key] = {**checkpoints.get(key, {}), "act_key": key,
                                    "last_attempt": datetime.now(UTC).isoformat(), "outcome": "refused"}
                if fail(key):
                    logger.warning("Laws: Table III stale re-reads stop after {} failures in a row", consecutive)
                    break
                continue
            consecutive = 0

    for congress in sorted(stated, reverse=True):
        if congress in held_numbers:
            start_key = f"{congress}-{max(held_numbers[congress])}"
        else:
            try:
                start_key = _cold_start(congress, stated[congress], held_numbers, within, acquire)
            except _WalkStopped as stop:
                logger.warning("Laws: Table III walk {}", stop)
                return _table3_summary(rows, read, failed)
            except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
                seed = f"{congress - 1}-{max(held_numbers[congress - 1])}"
                logger.warning("Laws: Table III seed {} for Congress {} not read: {}", seed, congress,
                               _transport_error(error))
                if fail(seed):
                    logger.warning("Laws: Table III walk stops after {} failures in a row", consecutive)
                    return _table3_summary(rows, read, failed)
                start_key = f"{congress}-{min(stated[congress])}"
            if start_key is None:
                continue
        first_ask = len(asked)
        # Never binds: every act after the start is a distinct act the laws table states.
        chain = iter_table3_chain(acquire, start_key, max_acts=len(stated[congress]) + 1, within=within)
        named: tuple[str | None, str | None] = (None, None)  # the last page's next act and release point
        while True:
            try:
                acquired = next(chain)
            except StopIteration as end:
                logger.info(
                    "Laws: Table III chain for Congress {} ends at {}: its page {} (next {!r}, release point {!r})",
                    congress,
                    asked[-1] if asked else start_key,
                    end.value,
                    *named,
                )
                break
            except _WalkStopped as stop:
                logger.warning("Laws: Table III walk {}", stop)
                return _table3_summary(rows, read, failed)
            except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
                # The walker refuses a start it cannot read before asking for anything.
                key = asked[-1] if len(asked) > first_ask else start_key
                logger.warning(
                    "Laws: Table III chain for Congress {} stops at {}: {}", congress, key, _transport_error(error)
                )
                if fail(key):
                    logger.warning("Laws: Table III walk stops after {} failures in a row", consecutive)
                    return _table3_summary(rows, read, failed)
                break
            consecutive = 0
            page = cast(Table3Page, acquired.result)
            key = page.key  # acquire is acquire_table3_act
            named = (page.next_act, page.release_point)
            if key in cached_rows:
                continue
            if key not in held:
                materialize(key, acquired)
            elif checkpoints is not None:
                page_rows = _page_rows(page, acquired.capture.observed_at)
                if checkpoints.get(key, {}).get("records_sha256") != _rows_digest(page_rows):
                    materialize(key, acquired, page_rows)
    return _table3_summary(rows, read, failed)


def _cold_start(
    congress: int,
    numbers: set[int],
    held_numbers: Mapping[int, list[int]],
    within: set[str],
    acquire: Callable[[str], Any],
) -> str | None:
    """Where a Congress with no held act starts, or ``None`` when Table III holds none of it yet.

    The previous Congress's highest held page is the only seed: its ``next_act``
    is the publisher's own link. When it names an act of this Congress the laws
    table states, the chain starts there. When it names none but its release
    point is still in an earlier Congress, Table III holds nothing of this one
    yet and nothing is asked. Otherwise, or with no seed, the chain starts at
    the lowest act the laws table states, as a cold walk always has; a failure
    there ends only this Congress's chain.
    """
    lowest = f"{congress}-{min(numbers)}"
    if congress - 1 not in held_numbers:
        logger.info("Laws: Table III for Congress {} starts cold at {}", congress, lowest)
        return lowest
    seed = f"{congress - 1}-{max(held_numbers[congress - 1])}"
    page = cast(Table3Page, acquire(seed).result)
    following, release = _public_law(page.next_act), _public_law(page.release_point)
    if following is not None and following[0] == congress and f"{following[0]}-{following[1]}" in within:
        logger.info("Laws: Table III for Congress {} starts at {}, the act {} names", congress, page.next_act, seed)
        return f"{following[0]}-{following[1]}"
    if release is not None and release[0] < congress:
        logger.info("Laws: Table III for Congress {} not yet released ({} states release point {})",
                    congress, seed, page.release_point)
        return None
    logger.info("Laws: Table III for Congress {} starts at {}; seed {} names {!r}", congress, lowest, seed,
                page.next_act)
    return lowest


def _page_rows(page: Table3Page, observed_at: str) -> list[dict]:
    """One act page's ``table3_records`` rows, sequenced in native order."""
    return [shape_table3_record(record, page=page, seq=seq, observed_at=observed_at)
            for seq, record in enumerate(page.records)]


def _rows_digest(rows: list[dict]) -> str | None:
    """A digest of an act's shaped rows without their observation time: equal digests publish nothing new."""
    return digest(json_column([{name: value for name, value in row.items() if name != "observed_at"} for row in rows]))


def _table3_summary(rows: list[dict], read: list[str], failed: list[str]) -> list[dict]:
    logger.info(
        "Laws: Table III — {:,} acts read {}, {:,} failed {}, {:,} rows",
        len(read),
        read,
        len(failed),
        failed,
        len(rows),
    )
    return rows


def build_laws(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    uslm: LawTextSource | None = None,
    olrc: OlrcSource | None = None,
    max_uslm: int = MAX_USLM_PER_RUN,
    max_table3: int = MAX_TABLE3_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
) -> tuple[Path, Path, Path]:
    """Build ``laws.parquet``, ``law_code_sections.parquet`` and ``table3_records.parquet``."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Laws need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        if evidence:
            evidence.credential = api_key
        reader = CongressListingReader(budget=LIST_BUDGET, api_key=api_key,
                                       transport=evidence.transport(stage="laws:list", max_bytes=LIST_BUDGET.max_page_bytes)
                                       if evidence else None)
    uslm = uslm or UslmAcquirer(budget=USLM_BUDGET)
    olrc = olrc or olrc_acquirer()

    congresses = congresses_from_env()
    priors = {name: published_table(output_dir, name, download_prior) for name in (NAME, CODE_SECTIONS, TABLE3)}

    # 1. The enumeration, whole, then the PLAW leg under its cap.
    if evidence:
        evidence.event("selection", congresses=congresses, max_uslm=max_uslm, max_table3=max_table3,
                       uslm_reader_version=USLM_READER_VERSION, table3_reader_version=TABLE3_READER_VERSION)
    listed = _list_laws(reader, congresses)
    if evidence:
        evidence.event("law-list-complete", law_ids=[plain["law_id"] for _, _, plain in listed])
    law_rows = _law_rows(listed, _held_laws(priors[NAME], congresses), uslm, PerRunCap(max_uslm, "Laws: PLAW files"), evidence)

    # 2. The per-Congress classification tables the index links.
    sessions: set[tuple[str, str]] = set()
    section_rows = _classification_rows(olrc, congresses, evidence, sessions)

    # 3. Table III, along its own chain through the public laws the route listed or the prior holds.
    acts = _held_public_laws(priors[NAME], congresses) | {
        (int(plain["congress"]), int(plain["number"])) for _, _, plain in listed if plain["law_type"] == PUBLIC
    }
    checkpoints = {r["act_key"]: r for r in read_checkpoints(priors[TABLE3], "laws-table3")
                   if isinstance(r.get("act_key"), str)}
    evaluated: set[str] = set()
    held = _held_acts(priors[TABLE3]) | set(checkpoints)
    table3_rows = _table3_rows(olrc, acts, held, PerRunCap(max_table3, "Laws: Table III pages"),
                               checkpoints=checkpoints, evaluated=evaluated, evidence=evidence)

    return (
        merge_contract_table(output_dir, NAME, law_rows, prior_present=priors[NAME] is not None),
        merge_contract_table(output_dir, CODE_SECTIONS, section_rows, prior_present=priors[CODE_SECTIONS] is not None,
                             replace_parents=(("congress", "session"), sessions)),
        merge_contract_table(output_dir, TABLE3, table3_rows, prior_present=priors[TABLE3] is not None,
                             replace_parents=("act_key", evaluated),
                             parquet_metadata=checkpoint_metadata(priors[TABLE3], "laws-table3", checkpoints.values())),
    )
