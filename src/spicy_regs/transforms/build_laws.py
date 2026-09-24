"""Transform: build ``laws``, ``law_code_sections`` and ``table3_records``.

Three tables from three publishers in one pass, because the second and third
are addressed by what the first enumerates. **``laws``**: the Congress.gov
``law/{congress}`` list route walked whole per scoped Congress (it lists
private laws too when ``law_type`` is omitted), one row per
``laws[]`` entry, each joined to the GovInfo PLAW USLM file whose ``<meta>``
states the Statutes at Large citation (one keyless request per law, under the
per-run cap); ``shape_law`` refuses a meta that states another law and a
captured file whose ``citableAs`` names no ``NNN Stat. NNN``, so a citation
never lands on the wrong row and ``captured`` always carries one.
**``law_code_sections``**: the OLRC per-Congress classification table, read in
the publisher's public-law order alone (the index links the current Congress's
session tables only) — the two orders hold the same rows, but the contract
keys a row on its position in one page, so the code-order twin would collide.
**``table3_records``**: Table III's own chain, per Congress. Every served
act page names the next act the table holds (``Table3Page.next_act``: 119-4
names 119-12), so the walk asks only the acts the chain names, from the
Congress's highest held act on, under its own cap and deadline. Table III lags
enactment and holds no page for an act that classified nothing; OLRC answers
such an act with a connection dropped inside its site menu, which is why the
walk never probes a number the chain does not name.

**Incremental.** The list is re-walked whole every run; the PLAW is what is
not re-read. A law already published with ``uslm_outcome = captured`` and the
same ``update_date`` is left standing; one whose list row moved, or whose
outcome is ``unavailable`` (only a ``404``/``410`` from the exact PLAW locator
— a transport failure or refused body is logged, never published as absence)
or ``not_requested``, is asked for again, newest first, under
:data:`MAX_USLM_PER_RUN`. When the cap or a failure stops the ask, a law not
yet published gets its list row with ``not_requested`` and one already
published keeps its prior row. A classification page read this run replaces
every prior row for its Congress and session (:func:`retire_prior_rows`),
while a Table III page is published once and never retired (its
``release_point`` says how current it was); the highest held act's page is
asked again each run only for the next act it names. A ``401``/``403``
from any of the three publishers aborts the run. Needs an api.data.gov key for
the list route; the PLAW and OLRC routes are keyless.
"""

from __future__ import annotations

import re
import time
from collections import Counter
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, NamedTuple, Protocol

import httpx
from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.law_tables import shape_law, shape_law_code_section, shape_table3_record
from spicy_docs.schemas.tables import TableContractError
from spicy_docs.sources.congress.listing import LIST_ROUTES, MAX_LIMIT, CongressListingReader, list_route_url
from spicy_docs.sources.govinfo.uslm import PublicLawSelection, UslmSourceError
from spicy_docs.sources.govinfo.uslm_acquisition import (
    UslmAcquirer,
    UslmAcquisitionBudget,
    UslmSourceUnavailableError,
)
from spicy_docs.sources.uscode import UsCodeSourceError
from spicy_docs.sources.uscode.acquisition import UsCodeAcquirer, UsCodeAcquisitionBudget
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env
from spicy_regs.transforms.congress_walk import ListingSource, PerRunCap, walk_route
from spicy_regs.transforms.table_merge import merge_contract_table, published_table, retire_prior_rows


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
PUBLIC = "public"
#: The one classification order read; see the module docstring.
TABLE_ORDER = "public-law"


class HeldLaw(NamedTuple):
    """The prior table's state for one ``law_id``: its list ``update_date`` and USLM outcome."""

    update_date: str | None
    uslm_outcome: str | None


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

    rows = duckdb.sql(
        f"SELECT law_id, update_date, uslm_outcome FROM read_parquet('{prior_file}') WHERE congress IN (SELECT UNNEST(?))",
        params=[[str(c) for c in congresses]],
    ).fetchall()
    return {str(law_id): HeldLaw(update_date, outcome) for law_id, update_date, outcome in rows}


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


def _uslm_row(uslm: LawTextSource, record: Mapping[str, Any], law: Mapping[str, Any], plain: dict) -> dict | None:
    """The row with its PLAW leg made: ``captured`` or ``unavailable``, or ``None`` when nothing was established.

    A ``401``/``403`` propagates and aborts the run. A transport failure, a
    refused body, or a meta the contract refuses is ``None``: the caller
    decides whether the list row is published without the leg or the prior
    row stands.
    """
    selection = PublicLawSelection(int(plain["congress"]), plain["law_type"], int(plain["number"]))
    try:
        acquired = uslm.acquire_public_law(selection)
    except UslmSourceUnavailableError:
        return shape_law(record, law, uslm_outcome="unavailable")
    except (UslmSourceError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Laws: PLAW {} not established: {}", selection.file_name, _transport_error(error))
        return None
    try:
        return shape_law(
            record,
            law,
            uslm=acquired.metadata,
            uslm_sha256=acquired.capture.sha256,
            uslm_observed_at=acquired.capture.observed_at,
            uslm_outcome=CAPTURED,
        )
    except TableContractError as error:
        logger.warning("Laws: PLAW {} captured but refused by the contract: {}", selection.file_name, error)
        return None


def _law_rows(
    listed: list[tuple[Mapping[str, Any], Mapping[str, Any], dict]],
    held: Mapping[str, HeldLaw],
    uslm: LawTextSource,
    cap: PerRunCap,
) -> list[dict]:
    rows: list[dict] = []
    outcomes: Counter[str] = Counter()
    unchanged = deferred = 0
    for record, law, plain in listed:
        prior = held.get(plain["law_id"])
        if prior is not None and prior.uslm_outcome == CAPTURED and prior.update_date == plain["update_date"]:
            unchanged += 1
            continue
        row = _uslm_row(uslm, record, law, plain) if cap.take() else None
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


def _classification_rows(olrc: OlrcSource, congresses: tuple[int, ...], prior_file: Path | None) -> list[dict]:
    """Every line of each public-law-order session table the index links for a scoped Congress."""
    try:
        index = olrc.acquire_classification_index().result
    except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
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
            logger.warning("Laws: classification table {} not established: {}", link.href, _transport_error(error))
            continue
        table = acquired.result
        if prior_file is not None:
            retire_prior_rows(prior_file, congress=str(table.congress), session=str(table.session))
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


# TODO(spicy-docs): expose this chain walk in spicy-docs (`iter_table3_acts`; the bulk-zip
# streamer holds that name today) and amend `docs/sources/uscode.md:129-136` with the early-drop
# window. An act the table does not hold answers 200, then the connection drops inside the site
# menu, before the content div a served page opens 27,199-27,947 bytes in. The bytes are a prefix
# of a served page, and the transport discards and retries them, so they are neither retained nor
# distinguishable from a served page dropped early.
def _public_law(key: str | None) -> tuple[int, int] | None:
    """``(congress, number)`` of a public-law key such as ``119-4`` or ``119–4``; ``None`` for anything else."""
    match = _PUBLIC_LAW_KEY.fullmatch((key or "").strip())
    return (int(match[1]), int(match[2])) if match else None


def _chain_end(page: Any, congress: int, number: int, stated: set[int]) -> tuple[int | None, str]:
    """The next act number the page names for this Congress's chain, or ``None`` and why the chain ends here."""
    following = _public_law(page.next_act)
    if following is None:
        return None, f"names no next public law ({page.next_act!r})"
    if following[0] != congress:
        return None, f"names {page.next_act}, in another Congress"
    if following[1] <= number:
        return None, f"names {page.next_act}, which does not follow it"
    if following[1] not in stated:
        return None, f"names {page.next_act}, which the laws table does not state"
    current = _public_law(page.release_point)
    if current is not None and following > current:
        return None, f"names {page.next_act}, past the release point {page.release_point} it states"
    return following[1], ""


def _table3_rows(
    olrc: OlrcSource,
    acts: set[tuple[int, int]],
    held: set[str],
    cap: PerRunCap,
    *,
    deadline_seconds: float = TABLE3_DEADLINE_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> list[dict]:
    """Follow each scoped Congress's Table III chain from its highest held act, newest Congress first.

    The walk asks the Congress's highest held act again (only for the next act
    its page names; its rows are not published again) or, cold, the lowest act
    the laws table states. It then asks each next act a page names and
    publishes that act's rows. The chain ends at a page that names no next act,
    or names one in another Congress, one the laws table does not state, or one
    past the release point the page states (the lag). A chain act that fails
    ends its Congress's chain for the run, and :data:`TABLE3_STOP_AFTER`
    failures in a row end the walk, as do the per-run cap and the deadline.
    """
    started = clock()
    rows: list[dict] = []
    read: list[str] = []
    failed: list[str] = []
    stated: dict[int, set[int]] = {}
    for congress, number in acts:
        stated.setdefault(congress, set()).add(number)
    held_numbers: dict[int, list[int]] = {}
    for act in filter(None, map(_public_law, held)):
        held_numbers.setdefault(act[0], []).append(act[1])
    consecutive = 0
    for congress in sorted(stated, reverse=True):
        number = max(held_numbers[congress]) if congress in held_numbers else min(stated[congress])
        while True:
            key = f"{congress}-{number}"
            if clock() - started >= deadline_seconds:
                logger.warning("Laws: Table III walk reached its {:,}-second deadline before {}", deadline_seconds, key)
                return _table3_summary(rows, read, failed)
            if not cap.take():
                return _table3_summary(rows, read, failed)
            try:
                acquired = olrc.acquire_table3_act(key)
            except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
                failed.append(key)
                consecutive += 1
                logger.warning(
                    "Laws: Table III chain for Congress {} stops at {}: {}", congress, key, _transport_error(error)
                )
                if consecutive >= TABLE3_STOP_AFTER:
                    logger.warning("Laws: Table III walk stops after {} failures in a row", consecutive)
                    return _table3_summary(rows, read, failed)
                break
            consecutive = 0
            page = acquired.result
            if key not in held:
                observed_at = acquired.capture.observed_at
                rows.extend(
                    shape_table3_record(record, page=page, seq=seq, observed_at=observed_at)
                    for seq, record in enumerate(page.records)
                )
                read.append(key)
            following, reason = _chain_end(page, congress, number, stated[congress])
            if following is None:
                logger.info("Laws: Table III chain for Congress {} ends at {}: its page {}", congress, key, reason)
                break
            number = following
    return _table3_summary(rows, read, failed)


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
) -> tuple[Path, Path, Path]:
    """Build ``laws.parquet``, ``law_code_sections.parquet`` and ``table3_records.parquet``."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Laws need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = CongressListingReader(budget=LIST_BUDGET, api_key=api_key)
    uslm = uslm or UslmAcquirer(budget=USLM_BUDGET)
    olrc = olrc or olrc_acquirer()

    congresses = congresses_from_env()
    priors = {name: published_table(output_dir, name, download_prior) for name in (NAME, CODE_SECTIONS, TABLE3)}

    # 1. The enumeration, whole, then the PLAW leg under its cap.
    listed = _list_laws(reader, congresses)
    law_rows = _law_rows(listed, _held_laws(priors[NAME], congresses), uslm, PerRunCap(max_uslm, "Laws: PLAW files"))

    # 2. The per-Congress classification tables the index links.
    section_rows = _classification_rows(olrc, congresses, priors[CODE_SECTIONS])

    # 3. Table III, along its own chain through the public laws the route listed or the prior holds.
    acts = _held_public_laws(priors[NAME], congresses) | {
        (int(plain["congress"]), int(plain["number"])) for _, _, plain in listed if plain["law_type"] == PUBLIC
    }
    table3_rows = _table3_rows(olrc, acts, _held_acts(priors[TABLE3]), PerRunCap(max_table3, "Laws: Table III pages"))

    return (
        merge_contract_table(output_dir, NAME, law_rows, prior_present=priors[NAME] is not None),
        merge_contract_table(output_dir, CODE_SECTIONS, section_rows, prior_present=priors[CODE_SECTIONS] is not None),
        merge_contract_table(output_dir, TABLE3, table3_rows, prior_present=priors[TABLE3] is not None),
    )
