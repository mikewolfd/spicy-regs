"""Transform: build ``house_activity_reports``, ``budget_volumes``, ``bill_committee_actions`` and ``document_citations``.

Four tables from two GovInfo collections, in one pass, for the same reason
``build_committee_reports`` builds three: they share an acquirer and its pacing
budget, and everything published here is read out of a document body that is
only in hand during the pass that fetched it.

**Why these four and not some other grouping.** The spicy-docs research splits
the PDF-only families by *acquisition pass*, and this is the pass that is one
keyed GovInfo body fetch per package:

* **CRPT** activity reports fill ``house_activity_reports``, one row per
  captured package. Their text then produces two derived tables —
  ``document_citations`` (every cited key with the span it was read at) and
  ``bill_committee_actions`` (what the print says *happened* to a bill it
  names). Both come from the same two rule sets run over the same characters;
  splitting them into separate rollups would fetch and extract the same
  1,249-page corpus twice for no gain.
* **BUDGET** volumes fill ``budget_volumes`` and contribute to the same
  ``document_citations``. A budget citation is a link-table row with
  ``document_kind`` ``budget_volume``, not a second table
  (spicy-docs ``docs/decisions.md``, "A citation is keyed on where it was read").
  They are here rather than in their own rollup because they are the same
  acquirer, the same key, the same budget and the same shared output.

``senate_expenditures`` is deliberately **not** here: its pass is PyMuPDF table
detection over 1,259-to-3,018-page volumes, which costs 26.5–84.6 ms a page
against 9.8 ms without table detection (spicy-docs
``docs/research/pdf-yield-mods-recheck-2026-09-20.md``), and its bodies are
granule files rather than package ones. Different acquisition, different cost
class, its own rollup.

**``document_citations`` has exactly one owner, and it is this transform.** The
contract is a shared link table over every document family, keyed
``(document_key, text_sha256, cite_kind, target_key, span_start)``.
``document_kind`` is what keeps the two families apart inside it —
``govinfo_package`` for an activity report, ``budget_volume`` for a budget
volume, both spicy-docs' own constants, never spelled here. A third family
would join by being added to this pass or by a decision to give the table a
second writer, which the repository's one-writer rule
(``tests/test_hosted_rollups.py``) makes a decision rather than an accident.

**The row shapes are spicy-docs'.** ``shape_activity_report``,
``shape_budget_volume``, ``shape_document_citation`` and
``shape_bill_committee_action`` build every row; ``find_citations`` and
``find_bill_actions`` find everything in them; ``document_provenance`` takes
the text digest. Nothing about a column, an identity or a rule is restated
here. What this module owns is the acquisition, the enumeration and the
pacing.

**Enumeration: the whole issue-date window, every run.** Both families are
small — 15 activity reports in one quarter of CRPT and 18 budget volumes in
twenty months, measured 2026-09-20 — so walking the window whole costs a
handful of list pages and buys the property a watermark cannot give: a package
refused this run is enumerated again next run and retried, which is
AGENTS.md's "retry every previously unsuccessful row on resume". A
last-modified watermark would advance past a refused package as soon as any
*other* package published, and the refusal would never be seen again. What is
skipped is only a package already published with the same ``last_modified``,
so a steady-state run fetches nothing and a revised volume is re-read.

**The ``published`` route, not ``collections``.** ``/published/{start}/{end}``
selects by issue date; ``/collections/{code}/{modified}`` selects by
last-modified and would be the cheaper resume primitive. It is not used
because it does not answer only the collection asked for: spicy-docs measured
``/collections/BUDGET/{lastModified}`` serving SERIALSET rows among BUDGET
rows on 2026-09-20. So the issued-date route is walked and **every row's own
``packageId`` is checked against the grammar** rather than trusted from the
request — which is also why a CRPT walk that meets an ``ERP-`` or ``GPO-``
neighbour (3 of 3,000 CRPT-scoped ids, 2026-09-19) drops it by name.

**Which CRPT packages are activity reports is a title rule, and it is stated
here because it is not published.** ``ACTIVITY_REPORT_TITLE`` is spicy-docs'
rule from ``tools/analysis/pdf_family_rollup.py``, which the wheel does not
ship — ``tools/`` is not packaged — so this is the one selection rule in this
module that is a copy rather than an import, and it is pinned by
``tests/test_print_citations.py`` against both the titles it must match and
the measured class it must not. It matches a *phrase* and not the word
``activit``: a bare word match took in "DIRECTING THE SECRETARY ... RELATING
TO ... ACTIVITIES" twice in the first eight matches spicy-docs measured on
2026-09-20. Measured live here 2026-09-20 on the ``published`` CRPT window
2025-01-01..2025-03-31: 71 packages walked, 15 matched.

**The committee roster vocabulary is two keyless requests a run.**
``find_citations`` settles a printed committee name against the vocabulary
``committee_vocabulary`` builds from the Clerk's ``MemberData.xml`` and the
Senate ``cvc`` file — the same two files ``build_committee_rosters`` already
reads, through the same acquirer, so this is a reuse and not a second reader.
Without it a printed committee name stays unresolved rather than being
guessed, so a roster file that refuses degrades the vocabulary and is logged;
it does not fail the run. ``committees_unresolved`` on the published row is a
floor for that reason and not a defect count: the Senate ``cvc`` file states
only the committees its listed senators sit on.

**Pages are read whole, and that is the measurement that matters.** The
research read at most 60 pages a document, and the re-check showed what that
cost: ``BUDGET-2027-APP`` names 29 public laws read to 60 pages and 490 read
to all 1,340, "a 4-percent sample of a 1,340-page volume reported as the
volume". So nothing here caps pages; ``pages_read``, ``stated_page_count`` and
``pages_capped`` are filled by spicy-docs' own ``read_depth`` from the fetch
that happened, and the per-run bound is a *package* count. Uncapped reading
costs 9.8 ms a page with table detection off (18,119 pages in 176.8 s), so the
whole corpus is a minute of extraction, not an hour.

Needs an api.data.gov key: GovInfo's ``published``, summary and MODS routes are
keyed. A ``401``/``403`` aborts the run rather than being counted as a bad row.
"""

from __future__ import annotations

import os
import re
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
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
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.sources.govinfo.bodies import parse_package_id
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

#: spicy-docs' own selection rule for "this CRPT package is a committee
#: activity report", copied because ``tools/`` is not in the wheel. The phrase,
#: never the bare word — see the module docstring.
ACTIVITY_REPORT_TITLE = re.compile(
    r"(?i)activit(?:y|ies)\b[^.]{0,80}\bcommittee\b"
    r"|\bcommittee\b[^.]{0,80}\bactivit(?:y|ies)\b"
    r"|\bactivity report\b"
    r"|\breport on activities\b"
)

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

#: **The one place this transform names a rendition, against the acquirer's
#: sealed default, and the reason is measured.** ``BODY_PREFERENCE`` puts HTML
#: ahead of PDF, which is right for ``committee_reports`` — a CRPT ``htm`` body
#: *is* GPO's text inside a ``<pre>`` wrapper — and wrong for these two
#: families, for three reasons:
#:
#: 1. **Every number in both contracts' prose is a PDF measurement.**
#:    ``body_derivation`` is ``pdf-extraction-gpo-normalized`` in every
#:    spicy-docs fixture of these families, and the GPO normalizer's own gate
#:    fired on 8 of 8 activity reports and on none of the other 63 documents in
#:    the PDF-family census. It exists for this layout.
#: 2. **Four published columns are meaningless on an unpaginated rendition.**
#:    ``pages_read``, ``stated_page_count`` and ``pages_capped`` on both
#:    document tables, and ``evidence_page`` on every citation row, all state
#:    a page. No GovInfo ``htm`` body of any collection carries a page
#:    boundary, so reading HTML publishes four NULL columns and calls it a row.
#: 3. **HTML refuses outright on a quarter of this family.** Measured here
#:    2026-09-20, first run, retained as
#:    ``receipts/rollups-pdf-families-2026-09-20/requests/print-citations-attempt-1-html.json``:
#:    under the sealed default, **10 of 41** activity reports refused with
#:    ``MarkupReadError: HTML markup exceeds the supported nesting depth``, and
#:    the 31 that were read published **0 page attributions across 29,308
#:    citation rows**. Under this order the same window reads as PDF.
#:
#: PDF is first and the sealed order follows it, so a package that offers no
#: PDF still yields a body rather than being refused for want of one.
PRINT_BODY_PREFERENCE: tuple[str, ...] = ("pdf", "xml", "uslm", "htm", "txt")

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


def _is_activity_report(package_id: str, title: str) -> bool:
    return package_id.startswith(f"{CRPT}-") and ACTIVITY_REPORT_TITLE.search(title) is not None


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
    walked = refused_id = 0
    url = published_url(since, _today(), collections=[collection], page_size=1000)
    for page in reader.packages(url, max_pages=MAX_PAGES):
        for record in page.records:
            walked += 1
            package_id = str(record.get("packageId") or "")
            if not keep(package_id, str(record.get("title") or "")):
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

    priors = {name: published_table(output_dir, name, download_prior) for name in (ACTIVITY_REPORTS, BUDGET_VOLUMES)}
    have_prior = {name: path is not None for name, path in priors.items()}
    held = {name: _held_packages(path) for name, path in priors.items()}
    since = _issue_floor()

    vocabulary = _roster_vocabulary(rosters, current_congress())

    activity_rows: list[dict] = []
    budget_rows: list[dict] = []
    action_rows: list[dict] = []
    citation_rows: list[dict] = []
    refusals: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    unchanged = fetched = capped = 0
    pages_read = 0

    for collection, table, rows, keep in (
        (CRPT, ACTIVITY_REPORTS, activity_rows, _is_activity_report),
        (BUDGET, BUDGET_VOLUMES, budget_rows, _is_budget_volume),
    ):
        already = held[table]
        for package_id, last_modified in _listed(reader, collection, since, keep):
            if fetched >= max_packages:
                # The window was enumerated whole; only the fetch stopped. The
                # next run enumerates it again and picks up where this left off.
                logger.warning(
                    "Print citations: per-run cap of {:,} packages reached in {} — the rest of the window"
                    " is retried next run",
                    max_packages,
                    collection,
                )
                break
            if package_id in already and already[package_id] == last_modified:
                # Published already and unmodified since; the publisher's own
                # last_modified is the comparison, not a guess from the date.
                unchanged += 1
                continue
            try:
                package, derived = _read_body(acquirer, package_id)
            except CredentialRefusedError:
                raise
            except _PACKAGE_REFUSALS as error:
                # Counted by reason, not just counted: forty refusals sharing
                # one reason is a defect here, and forty different ones are the
                # publisher.
                reason = type(error).__name__
                refusals[reason] += 1
                logger.warning(
                    "{}: {} refused ({}): {}", collection, package_id, reason, scrub_credential(str(error), "")
                )
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
        merge_contract_table(output_dir, BILL_ACTIONS, action_rows, download_prior=download_prior),
        merge_contract_table(output_dir, CITATIONS, citation_rows, download_prior=download_prior),
    )
