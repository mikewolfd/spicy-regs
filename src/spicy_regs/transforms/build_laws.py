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
**``table3_records``**: OLRC's Table III bulk file, the whole table in one
15 MB zip, fetched once a run through spicy-docs' ``acquire_table3_bulk``. Every
public law of a Congress the laws table holds or the run scopes is derived from
it, so a Congress keeps its rows current after it leaves the laws scope, while
Table III catches up months behind enactment. The file splits an act's
records about ten to an ``<act>`` fragment; an act's rows are its fragments'
records in the file's own order, each shaped with its own fragment's Congress,
date and volume.
The file spells those its own way (``119``, ``2025-01-29``, ``139``, ``119-4``)
where a page prints ``119th Cong.``, ``Jan. 29, 2025``, ``139 Stat.`` and
``119–4``; the classifications agree on all 2,981 rows the pages published,
and the file also holds two acts the page walk never reached (receipt
``fork-execution-2026-09-21/table3-bulk-2026-09-26/``). Being the whole table,
it states absence too: a held act it no longer lists loses its rows.

**Incremental.** The list is re-walked whole every run; the PLAW is what is
not re-read. A law already published ``captured`` or ``captured_partial`` under
the current :data:`~spicy_docs.schemas.law_tables.USLM_READER_VERSION` with the
same ``update_date`` is left standing; any other is asked for again, newest
first, under :data:`MAX_USLM_PER_RUN`. Every attempt publishes a truthful
outcome (``unavailable`` only for a ``404``/``410`` from the exact locator),
except that a failed attempt never replaces a validated prior row; a law the
cap does not reach keeps its prior row, or gets its list row with
``not_requested``. A classification page read this run replaces every prior
row for its Congress and session. Table III keeps one checkpoint per Congress:
the derivation rule, the bulk member's digest and release point, and each act's
shaped-row digest. OLRC states no ``Last-Modified``, ``ETag`` or
``Content-Length`` for the zip, on ``GET`` or ``HEAD``, so only its bytes can
say it is unchanged: while every scoped Congress holds a checkpoint for this
member, release point and rule, nothing is derived. Otherwise every scoped act
is derived again, and only an act whose rows changed, or which the file no
longer lists, is published, its whole row set replaced. A failed read
publishes nothing and keeps every checkpoint. A ``401``/``403`` from any of the
three publishers aborts the run. Needs an api.data.gov key for the list route;
the PLAW and OLRC routes are keyless.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Mapping
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

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
from spicy_docs.sources.uscode import Table3Page, Table3Record, UsCodeSourceError, iter_table3_acts
from spicy_docs.sources.uscode.acquisition import UsCodeAcquirer, UsCodeAcquisitionBudget
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
    """What this transform needs of the OLRC acquirer: the classification index and tables, and Table III's bulk file."""

    def acquire_classification_index(self, *, max_bytes: int | None = ...) -> Any: ...

    def acquire_classification_table(
        self, congress: int, session: int, *, order: Any = ..., max_bytes: int | None = ..., max_rows: int = ...
    ) -> Any: ...

    def acquire_table3_bulk(self, *, release_point: str | None = ..., max_bytes: int | None = ...) -> Any: ...


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

#: The largest OLRC read here is Table III's bulk zip: 14,966,992 bytes in 22
#: seconds and one request on 2026-09-26, where a session table is ~115 KB.
#: The timeout bounds each read, not a whole download, so a slow zip that keeps
#: arriving is not cut. 1.5 seconds between request starts is the pace
#: spicy-docs' U.S. Code guide asks of this publisher.
OLRC_BUDGET = UsCodeAcquisitionBudget(
    max_requests=4,
    max_bytes=32 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=1.5,
)

#: Pages of ``MAX_LIMIT`` per Congress; a Congress enacts a few hundred laws.
MAX_PAGES = 20

#: PLAW files asked for per run. A Congress's laws fit in one run at the
#: pacing above (~2.5 minutes for 300); the cap bounds a multi-Congress
#: backfill so a run publishes before it times out.
MAX_USLM_PER_RUN = 300

NAME = "laws"
CODE_SECTIONS = "law_code_sections"
TABLE3 = "table3_records"
CAPTURED = "captured"
#: The outcomes whose USLM identity was validated; a failed later attempt never replaces one.
VALIDATED = ("captured", "captured_partial")
#: The one classification order read; see the module docstring.
TABLE_ORDER = "public-law"


class HeldLaw(NamedTuple):
    """The prior table's state for one ``law_id``: its list ``update_date`` and USLM outcome."""

    update_date: str | None
    uslm_outcome: str | None
    reader_version: str | None = None


#: The rule ``table3_records`` rows are derived under, with the spicy-docs
#: release whose bulk reader and shaper derive them: either moving derives the
#: next bulk read again. That costs no request, since the zip is fetched every
#: run anyway, and publishes only the acts whose rows it changes; a package
#: version in a re-fetch token, by contrast, costs a request per item.
TABLE3_RULE = f"table3-bulk-v1+spicy-docs={version('spicy-docs')}"

#: A public-law key as Table III states it: ``119-4`` in the bulk, ``119–4`` on a page.
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


def _held_congresses(prior_file: Path | None) -> set[int]:
    """Every Congress the published laws table holds, in or out of the run's scope."""
    if prior_file is None:
        return set()
    import duckdb

    rows = duckdb.sql(f"SELECT DISTINCT congress FROM read_parquet('{prior_file}')").fetchall()
    return {int(congress) for (congress,) in rows if str(congress).isdigit()}


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


def _table3_current(checkpoint: Mapping[str, Any] | None, bulk: Any) -> bool:
    """Whether a Congress's checkpoint was derived from this bulk member and release point under the current rule.

    The release point is in the member's name, not its bytes, so an unchanged
    member under a new release point is still a new statement of currency.
    """
    return (checkpoint is not None and checkpoint.get("rule") == TABLE3_RULE
            and checkpoint.get("member_sha256") == bulk.member_sha256
            and checkpoint.get("release_point") == bulk.release_point and isinstance(checkpoint.get("acts"), dict))


def _bulk_rows(
    body: bytes, congresses: set[int], *, release_point: str, observed_at: str
) -> dict[int, dict[str, list[dict]]]:
    """Each scoped public law's ``table3_records`` rows from the bulk zip, by Congress and act.

    The file splits an act's records about ten to an ``<act>`` fragment, and each
    fragment states its own Congress, date and volume: 87-845 prints one record
    in volume 76 and the rest in 76A. So each record is shaped with its own
    fragment standing in for the page (``Table3Page`` with no chain links), and
    ``seq`` counts records through the act in the file's order, as a page
    numbers its rows. The file's own ``sequence`` cannot: it repeats within 70
    acts, restarts in 87-845's second volume and runs out of order in 5.
    A record's volume is its fragment's where the record states a page, as a
    page's Statutes link carries it. Pre-1957 chapters
    (``1955-08-01:360``) are not public laws and are left out, as before.
    """
    acts: dict[int, dict[str, list[dict]]] = {}
    for fragment in iter_table3_acts(body):
        law = _public_law(fragment.search_key)
        if law is None or law[0] not in congresses:
            continue
        context = Table3Page("table3-bulk", fragment.search_key, fragment.num, release_point, fragment.congress,
                             fragment.statutes_at_large_volume, fragment.date, None, None, (),
                             ("release-point:member-name",))
        rows = acts.setdefault(law[0], {}).setdefault(fragment.search_key, [])
        for record in fragment.records:
            volume = fragment.statutes_at_large_volume if record.statutes_at_large_page else None
            stated = Table3Record(record.act_section, volume, record.statutes_at_large_page, record.usc_title,
                                  record.usc_section, record.usc_status)
            rows.append(shape_table3_record(stated, page=context, seq=len(rows), observed_at=observed_at))
    return acts


def _table3_rows(
    olrc: OlrcSource,
    congresses: set[int],
    held: set[str],
    checkpoints: dict[str, dict],
    evaluated: set[str],
    evidence: CaptureEvidence | None = None,
) -> list[dict]:
    """Every scoped act's rows from one read of the Table III bulk file; only changed or vanished acts are published.

    One request a run, retained as evidence. ``checkpoints`` maps a Congress to
    what was last derived for it: rule, member digest, release point and each
    act's shaped-row digest. While every scoped Congress's checkpoint names this
    member and release point under the current rule, nothing is derived. Otherwise each scoped act
    is derived and compared with its digest (a held act with none is published),
    and a held or checkpointed act the file no longer lists is ``evaluated``
    with no rows, which removes them. A failed read changes nothing.
    """
    try:
        acquired = olrc.acquire_table3_bulk()
    except (UsCodeSourceError, httpx.HTTPError, ConnectionError) as error:
        if evidence:
            evidence.refusal(error, stage="table3:bulk")
        logger.warning("Laws: Table III bulk not established — every row stands: {}", _transport_error(error))
        return []
    if evidence:
        evidence.capture(acquired.capture, stage="table3:bulk")
    bulk = acquired.result
    scope = sorted(congresses)
    if all(_table3_current(checkpoints.get(str(congress)), bulk) for congress in scope):
        logger.info("Laws: Table III bulk unchanged at release point {} ({}) for Congresses {}; nothing derived",
                    bulk.release_point, bulk.member_sha256, scope)
        if evidence:
            evidence.event("table3-bulk", release_point=bulk.release_point, member_sha256=bulk.member_sha256,
                           rule=TABLE3_RULE, congresses=scope, derived=False)
        return []
    derived = _bulk_rows(acquired.capture.body, congresses, release_point=bulk.release_point,
                         observed_at=acquired.capture.observed_at)
    rows: list[dict] = []
    published: list[str] = []
    removed: list[str] = []
    for congress in scope:
        prior = (checkpoints.get(str(congress)) or {}).get("acts") or {}
        acts = derived.get(congress, {})
        digests = {key: _rows_digest(act_rows) for key, act_rows in acts.items()}
        for key, act_digest in digests.items():
            if prior.get(key) != act_digest:
                published.append(key)
                rows.extend(acts[key])
        stated = {key for key in held | set(prior) if (act := _public_law(key)) is not None and act[0] == congress}
        removed.extend(sorted(stated - set(digests)))
        checkpoints[str(congress)] = {"congress": str(congress), "rule": TABLE3_RULE,
                                      "member_sha256": bulk.member_sha256, "sha256": acquired.capture.sha256,
                                      "release_point": bulk.release_point, "observed_at": acquired.capture.observed_at,
                                      "acts": digests}
    evaluated.update(published, removed)
    if removed:
        logger.warning("Laws: Table III no longer lists {}; their rows are removed", removed)
    logger.info(
        "Laws: Table III bulk at release point {} — {:,} acts derived for Congresses {}, {:,} published {}, {:,} rows",
        bulk.release_point, sum(map(len, derived.values())), scope, len(published), published, len(rows),
    )
    if evidence:
        evidence.event("table3-bulk", release_point=bulk.release_point, member_sha256=bulk.member_sha256,
                       rule=TABLE3_RULE, congresses=scope, derived=True,
                       acts={str(congress): sorted(acts) for congress, acts in derived.items()},
                       published=published, removed=removed, rows=len(rows))
    return rows


def _rows_digest(rows: list[dict]) -> str | None:
    """A digest of an act's shaped rows without their observation time: equal digests publish nothing new."""
    return digest(json_column([{name: value for name, value in row.items() if name != "observed_at"} for row in rows]))


def build_laws(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    uslm: LawTextSource | None = None,
    olrc: OlrcSource | None = None,
    max_uslm: int = MAX_USLM_PER_RUN,
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
        evidence.event("selection", congresses=congresses, max_uslm=max_uslm,
                       uslm_reader_version=USLM_READER_VERSION, table3_rule=TABLE3_RULE)
    listed = _list_laws(reader, congresses)
    if evidence:
        evidence.event("law-list-complete", law_ids=[plain["law_id"] for _, _, plain in listed])
    law_rows = _law_rows(listed, _held_laws(priors[NAME], congresses), uslm, PerRunCap(max_uslm, "Laws: PLAW files"), evidence)

    # 2. The per-Congress classification tables the index links.
    sessions: set[tuple[str, str]] = set()
    section_rows = _classification_rows(olrc, congresses, evidence, sessions)

    # 3. Table III, from its bulk file, for every Congress the laws table holds as well as the scoped ones.
    checkpoints = {r["congress"]: r for r in read_checkpoints(priors[TABLE3], "laws-table3")
                   if isinstance(r.get("congress"), str) and isinstance(r.get("acts"), dict)}
    evaluated: set[str] = set()
    table3_rows = _table3_rows(olrc, set(congresses) | _held_congresses(priors[NAME]), _held_acts(priors[TABLE3]),
                               checkpoints, evaluated, evidence)

    return (
        merge_contract_table(output_dir, NAME, law_rows, prior_present=priors[NAME] is not None),
        merge_contract_table(output_dir, CODE_SECTIONS, section_rows, prior_present=priors[CODE_SECTIONS] is not None,
                             replace_parents=(("congress", "session"), sessions)),
        merge_contract_table(output_dir, TABLE3, table3_rows, prior_present=priors[TABLE3] is not None,
                             replace_parents=("act_key", evaluated),
                             parquet_metadata=checkpoint_metadata(priors[TABLE3], "laws-table3", checkpoints.values())),
    )
