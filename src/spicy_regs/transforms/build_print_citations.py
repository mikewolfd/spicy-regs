"""Build activity reports, budget volumes, bill actions and citations in one pass.

SpicyDocs owns selection, rendition preference, interpretation and row shapes.
This host owns the issue-date window, caps, prior-output resume and publication.
The whole published window is enumerated each run so unsuccessful packages
remain eligible; unchanged captured packages cost no body requests. Collections
share the package cap round-robin so neither starves on a cold start.

Print families use the package's PDF-first order because their columns state
pages. Bodies are read whole; read_depth reports the publisher's stated extent
against pages actually extracted. The two keyless chamber rosters supply the
committee vocabulary. A roster failure leaves its names unresolved; access
refusals abort. Senate expenditure granules use a separate acquisition pass.
"""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
from loguru import logger
from spicy_docs.extraction.body_text import BodyText, body_text
from spicy_docs.interpretation.bill_actions import find_bill_actions
from spicy_docs.interpretation.citations import (
    CITATION_RULE_SET_VERSION,
    CITATION_RULES_BY_NAME,
    committee_vocabulary,
    find_citations,
)
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.schemas.budget_volume_tables import (
    BUDGET_VOLUME,
    budget_index_stated_keys,
    shape_budget_volume,
)
from spicy_docs.schemas.bill_action_tables import shape_bill_committee_action
from spicy_docs.schemas.document_citation_tables import (
    GOVINFO_PACKAGE,
    document_provenance,
    index_stated_keys,
    shape_activity_report,
    shape_document_citation,
)
from spicy_docs.sources.congress.committee_rosters import (
    CommitteeRosterAcquirer,
    CommitteeRosterBudget,
    CommitteeRosterError,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget, GovInfoFormatNotOfferedError
from spicy_docs.sources.govinfo.bodies import MEASURED_BUDGET_PARTS, PRINT_BODY_PREFERENCE, parse_package_id
from spicy_docs.sources.govinfo.activity_reports import ACTIVITY_REPORT_RULE_VERSION, is_activity_report, names_activity
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, published_url
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import current_congress
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

#: The four tables this transform publishes, in the order ``build`` returns
#: them. ``document_citations`` is last because it is fed by both families.
ACTIVITY_REPORTS = "house_activity_reports"
BUDGET_VOLUMES = "budget_volumes"
BILL_ACTIONS = "bill_committee_actions"
CITATIONS = "document_citations"

#: The two collections, with the package-id prefix each row must actually
#: carry. The prefix check is not redundant with the request: a collection
#: walk serves neighbouring-collection rows (3 of 3,000 CRPT-scoped ids,
#: spicy-docs 2026-09-19).
CRPT = "CRPT"
BUDGET = "BUDGET"

#: The issue-date floor a cold run walks from. **This is a scope, not a
#: coverage claim.** GovInfo's CRPT and CDOC collections reach 1817, and
#: whether the activity-report title rule finds anything before the 118th is
#: unmeasured — spicy-docs' own register calls one early-window ``published``
#: walk "the cheapest unanswered question in the document", and no CRPT
#: artifact outside the 118th and 119th is retained anywhere in that corpus.
#: 2023-01-01 reaches the 118th Congress's activity reports (issued January
#: 2025) and the FY2026/FY2027 budget volumes. Widen it with
#: ``PRINT_CITATIONS_SINCE`` and the walk goes back as far as it is pointed;
#: the answer is then a measurement someone made rather than a default.
DEFAULT_ISSUE_FLOOR = "2023-01-01"

#: Packages fetched per run, across both collections. Two keyed requests each
#: (summary and MODS; the body is keyless), so 40 packages is ~80 keyed plus a
#: handful of list pages — comfortably inside the share of the 4,000-per-hour
#: GovInfo ceiling left by the 3,680 worst hour D1 measured across the existing
#: rollups, and this rollup's cron shares an hour with none of them. It is a
#: catch-up bound, not a steady-state one: both families together publish
#: fewer than 30 packages a Congress.
MAX_PACKAGES_PER_RUN = 40

#: List pages per collection walk. At 1,000 rows a page this clears every
#: window either collection can produce (291 CDOC rows and 71 CRPT rows in the
#: measured windows) with room for a decade of backfill.
MAX_PAGES = 20

DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Three requests per package (summary, MODS, body), paced. ``max_body_bytes``
#: is 24 MiB because the Budget Appendix is 19.4 MB — the largest body either
#: family publishes — and a volume above the bound is refused whole rather than
#: read short, then retried next run.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,
    max_metadata_bytes=8 * 1024 * 1024,
    timeout_seconds=300.0,
    min_request_interval_seconds=0.34,
)

#: The two keyless roster files. ``max_bytes`` matches what
#: ``build_committee_rosters`` allows the larger of the two.
ROSTER_BUDGET = CommitteeRosterBudget(
    max_requests=6,
    max_bytes=4 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: The committee whose print this is, for ``find_bill_actions``. It uses the
#: chamber for one thing only: a hearing or a markup is the committee's own
#: act, so its action code follows the actor rather than the measure. Every
#: package this transform reads for actions is a **House** committee's activity
#: report — the title rule and the ``hrpt`` package type both say so.
ACTIVITY_REPORT_CHAMBER = "house"

#: What one package's acquisition and reading can refuse with, short of a
#: credential refusal, which propagates and aborts the run.
_PACKAGE_REFUSALS = (
    PagedJsonSourceError,
    httpx.HTTPError,
    ConnectionError,
    ValueError,
    TypeError,
    KeyError,
    OSError,
)


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer.

    ``prefer`` is in the signature because this transform passes one — see
    :data:`PRINT_BODY_PREFERENCE`, the one place it names a rendition.
    """

    def acquire(self, package_id: str, *, prefer: Sequence[str] = ..., max_bytes: int | None = ...) -> Any: ...


class RosterSource(Protocol):
    """What this transform needs of the chamber roster acquirer."""

    def acquire_house(self, *, congress: int, session: int | None = ..., max_bytes: int | None = ...) -> Any: ...

    def acquire_senate(self, *, max_bytes: int | None = ...) -> Any: ...


def _issue_floor() -> str:
    """The issue date a run walks from. ``PRINT_CITATIONS_SINCE`` overrides."""
    raw = os.environ.get("PRINT_CITATIONS_SINCE", "").strip()
    return raw or DEFAULT_ISSUE_FLOOR


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _held_packages(prior_file: Path | None) -> dict[str, str | None]:
    """``package_id`` -> published ``last_modified``, for skipping unchanged packages."""
    if prior_file is None:
        return {}
    import duckdb

    return dict(duckdb.sql(f"SELECT package_id, last_modified FROM read_parquet('{prior_file}')").fetchall())


def _is_budget_volume(package_id: str, title: str) -> bool:
    # The title says nothing useful here — "Appendix", "Mid-Session Review" —
    # so the id's own prefix is the whole rule, checked against the row rather
    # than assumed from the request.
    return package_id.startswith(f"{BUDGET}-")


def _listed(
    reader: PackageDiscoverySource,
    collection: str,
    since: str,
    keep: Callable[[str, str], bool],
) -> list[tuple[str, str | None]]:
    """``(package_id, last_modified)`` for every row of one collection window this run accepts.

    The whole window, every run — see the module docstring. A row whose id the
    grammar refuses is dropped by name and counted, never fetched.
    """
    accepted: list[tuple[str, str | None]] = []
    walked = refused_id = activity_named = 0
    url = published_url(since, _today(), collections=[collection], page_size=1000)
    for page in reader.packages(url, max_pages=MAX_PAGES):
        for record in page.records:
            walked += 1
            package_id = str(record.get("packageId") or "")
            title = str(record.get("title") or "")
            activity_named += int(names_activity(title))
            if not keep(package_id, title):
                continue
            try:
                parse_package_id(package_id)
            except ValueError as error:
                # A neighbouring collection's id inside this collection's walk.
                refused_id += 1
                logger.warning("{}: {} is not a {} package id: {}", collection, package_id, collection, error)
                continue
            accepted.append((package_id, record.get("lastModified")))
    logger.info(
        "{}: {:,} rows walked since {}, {:,} accepted, {:,} refused by id grammar",
        collection,
        walked,
        since,
        len(accepted),
        refused_id,
    )
    if collection == CRPT:
        logger.info("CRPT title rule {}: {} titles name activity, {} accepted",
                    ACTIVITY_REPORT_RULE_VERSION, activity_named, len(accepted))
    return accepted


def _roster_vocabulary(rosters: RosterSource, congress: int) -> tuple[tuple[str, str], ...]:
    """The committee names ``find_citations`` resolves against, from both chamber files.

    A file that refuses leaves its chamber out of the vocabulary, which makes
    ``committees_unresolved`` larger and invents nothing. Both routes are
    keyless, so neither refusal is a credential refusal.
    """
    house: list[Any] = []
    senate: list[Any] = []
    for label, call, into in (
        ("House", lambda: rosters.acquire_house(congress=congress), house),
        ("Senate", rosters.acquire_senate, senate),
    ):
        try:
            into.append(call().roster)
        except CredentialRefusedError:
            raise
        except (CommitteeRosterError, httpx.HTTPError, ConnectionError) as error:
            logger.warning(
                "Print citations: {} roster not established, its committees stay unresolved: {}",
                label,
                scrub_credential(str(error), ""),
            )
    vocabulary = committee_vocabulary(house=house, senate=senate)
    logger.info("Print citations: committee vocabulary holds {:,} names", len(vocabulary))
    return vocabulary


def _read_body(acquirer: PackageBodySource, package_id: str) -> tuple[Any, BodyText]:
    """One package's fetched body and the one text derivation for its rendition.

    No extractor argument, for the reason ``build_committee_reports`` states:
    ``body_text``'s default is the pipeline ``gpo_normalize`` was derived on,
    and the GPO gutter-numbered layout only comes through on PyMuPDF's line
    adjacency. Table detection stays off — nothing published here is a ruled
    cell, and it would cost three to eight times the per-page time.
    """
    package = acquirer.acquire(package_id, prefer=PRINT_BODY_PREFERENCE)
    return package, body_text(package)


#: The two collections this pass walks, with the table each fills and the rule
#: that says a listing row belongs to it. Named once so the enumeration, the
#: scheduling and the row lists cannot disagree about which families exist.
_FAMILIES: tuple[tuple[str, str, str, Callable[[str, str], bool]], ...] = (
    (CRPT, ACTIVITY_REPORTS, "activity", is_activity_report),
    (BUDGET, BUDGET_VOLUMES, "budget", _is_budget_volume),
)


def _schedule(outstanding: Mapping[str, Sequence[str]], cap: int) -> list[tuple[str, str]]:
    """Round-robin the collections' outstanding packages into one capped list.

    **Why not simply walk one collection and then the other.** The cap is
    shared, so walking CRPT first spends all of it on CRPT whenever CRPT has
    more outstanding packages than the cap — and then the run publishes an
    *empty* ``budget_volumes``. That is what the first measured run did
    (2026-09-20: 40 activity reports, 0 budget volumes), and an empty contract
    table is worse than a partial one, because a consumer cannot tell "this
    family published nothing" from "this family has nothing". The second run
    filled it, so the outcome converged, but a cold deploy should not have to
    run twice to publish a table at all.

    Round-robin fixes that without touching the cap: the same total is
    fetched, both families make progress every run, and a family with more
    outstanding work keeps the slack once the other is exhausted. With 41
    activity reports and 23 budget volumes against a cap of 40 this schedules
    20 and 20; the next run takes the remaining 21 and 3.

    ``O(cap)`` in time, and the order within a collection is the publisher's
    own listing order, untouched.
    """
    queues = {collection: list(packages) for collection, packages in outstanding.items() if packages}
    scheduled: list[tuple[str, str]] = []
    while queues and len(scheduled) < cap:
        for collection in list(queues):
            if len(scheduled) >= cap:
                break
            scheduled.append((collection, queues[collection].pop(0)))
            if not queues[collection]:
                del queues[collection]
    remaining = sum(len(packages) for packages in outstanding.values()) - len(scheduled)
    if remaining:
        # The windows were enumerated whole; only the fetch stopped. The next
        # run enumerates them again and picks up what this one left.
        logger.warning(
            "Print citations: per-run cap of {:,} packages reached — {:,} outstanding packages are retried next run",
            cap,
            remaining,
        )
    logger.info(
        "Print citations: scheduled {}",
        dict(Counter(collection for collection, _ in scheduled)) or "nothing — every package is published",
    )
    return scheduled


def build_print_citations(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: PackageBodySource | None = None,
    rosters: RosterSource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, Path, Path, Path]:
    """Build the two document tables, the bill-action table and the shared citation link table."""
    if reader is None or acquirer is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Print citations need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = reader or GovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key)
        acquirer = acquirer or GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)
    rosters = rosters or CommitteeRosterAcquirer(budget=ROSTER_BUDGET)

    # All four priors are asked for once, up front. The two document tables
    # need theirs for the held-package check anyway; the two derived tables ask
    # only so ``merge_contract_table`` is told whether a prior exists and does
    # not repeat a download that already returned 404.
    priors = {
        name: published_table(output_dir, name, download_prior)
        for name in (ACTIVITY_REPORTS, BUDGET_VOLUMES, BILL_ACTIONS, CITATIONS)
    }
    have_prior = {name: path is not None for name, path in priors.items()}
    held = {name: _held_packages(priors[name]) for name in (ACTIVITY_REPORTS, BUDGET_VOLUMES)}
    since = _issue_floor()
    logger.info("BUDGET: {} measured parts admitted by the package grammar", len(MEASURED_BUDGET_PARTS))

    vocabulary = _roster_vocabulary(rosters, current_congress())

    activity_rows: list[dict] = []
    budget_rows: list[dict] = []
    action_rows: list[dict] = []
    citation_rows: list[dict] = []
    refusals: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    unchanged = fetched = capped = root_not_offered = 0
    pages_read = 0

    # Enumerate both windows first, drop what is already published, then
    # interleave — see :func:`_schedule` for why the order is not "CRPT, then
    # whatever is left".
    outstanding: dict[str, list[str]] = {}
    for collection, table, _label, keep in _FAMILIES:
        already = held[table]
        listed = _listed(reader, collection, since, keep)
        outstanding[collection] = [
            package_id
            for package_id, last_modified in listed
            # Published already and unmodified since; the publisher's own
            # last_modified is the comparison, not a guess from the date.
            if not (package_id in already and already[package_id] == last_modified)
        ]
        unchanged += len(listed) - len(outstanding[collection])

    scheduled = _schedule(outstanding, max_packages)
    rows_for = {CRPT: activity_rows, BUDGET: budget_rows}

    for collection, package_id in scheduled:
        rows = rows_for[collection]
        try:
            package, derived = _read_body(acquirer, package_id)
        except CredentialRefusedError:
            raise
        except GovInfoFormatNotOfferedError as error:
            if collection != BUDGET:
                raise
            root_not_offered += 1
            logger.info("BUDGET: {} publisher offers no preferred package-root body: {}; no granule attempted",
                        package_id, scrub_credential(str(error), ""))
            continue
        except _PACKAGE_REFUSALS as error:
            # Counted by reason, not just counted: forty refusals sharing
            # one reason is a defect here, and forty different ones are the
            # publisher.
            reason = type(error).__name__
            refusals[reason] += 1
            logger.warning("{}: {} refused ({}): {}", collection, package_id, reason, scrub_credential(str(error), ""))
            continue
        fetched += 1

        identity = package.identity
        findings = find_citations(
            derived.text,
            pages=derived.pages,
            # A BUDGET package states no Congress at all, so a printed bill
            # key stays congress-free and unresolved rather than being
            # stamped with a Congress nothing established.
            congress=getattr(identity, "congress", None),
            committees=vocabulary if collection == CRPT else (),
        )
        for finding in findings:
            kinds[finding.kind] += 1

        if collection == CRPT:
            stated = index_stated_keys(package.mods)
            document_kind = GOVINFO_PACKAGE
            rows.append(
                shape_activity_report(
                    package.summary, package.mods, derived, findings, rule_set_version=CITATION_RULE_SET_VERSION
                )
            )
        else:
            # Bills are dropped from the comparison, not answered `false`:
            # a budget volume has no Congress, so no comparison against the
            # MODS's `119-hr-7806` is possible from the print's `HR7806`.
            stated = budget_index_stated_keys(package.mods)
            document_kind = BUDGET_VOLUME
            rows.append(
                shape_budget_volume(
                    package.summary, package.mods, derived, findings, rule_set_version=CITATION_RULE_SET_VERSION
                )
            )

        provenance = document_provenance(derived, document_key=package_id, document_kind=document_kind)
        citation_rows.extend(shape_document_citation(f, provenance, stated_by_index=stated) for f in findings)

        if collection == CRPT:
            reading = find_bill_actions(derived.text, findings, committee_chamber=ACTIVITY_REPORT_CHAMBER)
            action_rows.extend(
                shape_bill_committee_action(
                    action,
                    provenance,
                    citation_rule_version=CITATION_RULES_BY_NAME["bill_number"].version,
                )
                for action in reading.findings
            )

        depth = len(derived.pages) if derived.pages is not None else 0
        pages_read += depth
        stated_pages = package.summary.pages
        if stated_pages is not None and str(stated_pages).isdecimal() and depth < int(stated_pages):
            capped += 1

    logger.info(
        "Print citations: {:,} activity reports, {:,} budget volumes, {:,} citation rows, {:,} action rows"
        " from {:,} packages and {:,} pages; {:,} already held, {:,} refused",
        len(activity_rows),
        len(budget_rows),
        len(citation_rows),
        len(action_rows),
        fetched,
        pages_read,
        unchanged,
        sum(refusals.values()),
    )
    logger.info("BUDGET: {} publisher package-root format answers (not failures)", root_not_offered)
    if capped:
        # Every count on a capped document is a floor. Nothing here caps pages,
        # so this can only mean the body the publisher served was shorter than
        # the extent it states — which is the publisher's disagreement with
        # itself and worth seeing.
        logger.warning("Print citations: {:,} packages read fewer pages than the publisher states", capped)
    if refusals:
        logger.info("Print citations: refusals by reason — {}", dict(refusals))
    if kinds:
        # Which citation kinds the prints actually stated. The contract has no
        # column for the per-run distribution, so this is where it is said.
        logger.info("Print citations: citation findings by kind — {}", dict(kinds))

    return (
        merge_contract_table(
            output_dir,
            ACTIVITY_REPORTS,
            activity_rows,
            download_prior=download_prior,
            prior_present=have_prior[ACTIVITY_REPORTS],
        ),
        merge_contract_table(
            output_dir,
            BUDGET_VOLUMES,
            budget_rows,
            download_prior=download_prior,
            prior_present=have_prior[BUDGET_VOLUMES],
        ),
        merge_contract_table(
            output_dir,
            BILL_ACTIONS,
            action_rows,
            download_prior=download_prior,
            prior_present=have_prior[BILL_ACTIONS],
        ),
        merge_contract_table(
            output_dir,
            CITATIONS,
            citation_rows,
            download_prior=download_prior,
            prior_present=have_prior[CITATIONS],
        ),
    )
