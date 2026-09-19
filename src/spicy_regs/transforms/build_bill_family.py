"""Transform: the bill family — thirteen tables from one acquisition pass.

``spicy_docs.interpretation.bill_family.build_bill_family`` does the composing:
given one bill's BILLSTATUS and its captured printings, it returns twelve row
tuples (bills, actions, committees, publisher summaries, versions, sections,
the three diff tables, the two model tables, diff summaries) plus the refusals.
This transform acquires the input, folds the per-bill results, derives the
thirteenth table (``public_activity_events``) by comparing the run against the
previously published one, and merges each through the one shared helper. It
re-derives no rule: every column is shaped in spicy-docs.

**Acquisition, and why it is bounded.** Bills come from the BILLSTATUS bulk
archive — one keyless zip per ``(congress, bill_type)``, so the whole scope
costs eight requests. Printings are the expensive half: each is a GovInfo
package (summary, MODS, body — three keyed requests), and a Congress has tens
of thousands. A rollup publishes nothing until it finishes, so an unbounded
first pass would time out and persist nothing, exactly the failure
``build_congress_bills`` documents. ``MAX_VERSION_FETCHES`` bounds the
printings; the status-derived tables still cover every bill in scope, and the
coverage statement says which is which.

**The model seams.** ``classify`` and ``summarize`` are wired only when a
Gemini key is in the environment; without one they are ``None`` and the three
model tables come back empty, which is what a keyless CI run does. Nothing
about a bill changes when they are off.

**Refusals are counted.** A printing with no parsed document, a pair that
cannot be diffed, a row whose identity has a null part — ``build_bill_family``
returns each as a ``FamilyRefusal``, and this logs them by table. They are
never dropped silently and never published as a row with invented values.
"""

from __future__ import annotations

import functools
import os
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from spicy_docs.interpretation.bill_family import (
    BillFamilyCapture,
    BillFamilyTables,
    BillVersionCapture,
    EngineStamp,
    installed_engine_stamp,
)
from spicy_docs.interpretation.bill_family import build_bill_family as build_family
from spicy_docs.interpretation.bill_summaries import summarize_bill, summarize_diff
from spicy_docs.interpretation.section_classification import classify_sections
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.activity_events import activity_events, snapshot_from_rows
from spicy_docs.sources.congress.bill_status import bill_package_id_from_url
from spicy_docs.sources.congress.bill_tree import engine_available, parse_bill_tree
from spicy_docs.sources.congress.bill_versions import (
    bill_version_package_id,
    choose_format,
    version_slug,
)
from spicy_docs.sources.congress.bulk_status import BulkStatusAcquirer, BulkStatusBudget
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import bill_types_from_env, congresses_from_env
from spicy_regs.transforms.model_call import DEFAULT_MODEL, model_call, resolve_gemini_key
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

#: The DeltaTrack commit the vendored wheel was built from. ``installed_engine_stamp``
#: reads a revision out of ``direct_url.json``, which only a git install writes;
#: this repository vendors wheels on purpose, so the stamp it returns has an
#: empty revision. The commit is a fact of the vendoring, recorded in
#: ``vendor/README.md`` beside the wheel's SHA-256, and is supplied here rather
#: than left blank — ``section_diffs.engine_revision`` is the column that says
#: which engine produced a diff.
DELTATRACK_REVISION = "c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6"

#: One zip per (congress, bill_type); the archive bounds live on the budget.
BULK_BUDGET = BulkStatusBudget(
    max_requests=4,
    max_bytes=256 * 1024 * 1024,
    timeout_seconds=300.0,
    min_request_interval_seconds=1.0,
)

#: Three requests per printing (summary, MODS, body), paced at ~3/s.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,  # MAX_EVIDENCE_BYTES
    max_metadata_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.34,
)

#: ~3 requests each at ~3/s: 600 printings is ~10 minutes.
MAX_VERSION_FETCHES = 600

#: Formats worth parsing, in preference order. XML gives a section tree; text
#: gives a body; PDF is left to the enrichment path that already exists.
BODY_PREFERENCE = ("xml", "txt")

#: The three tables ``public_activity_events`` compares between runs.
SNAPSHOT_TABLES = ("congress_bills", "bill_versions", "bill_summaries")

#: The twelve ``BillFamilyTables`` row tuples, by the contract each fills.
FAMILY_TABLES: tuple[tuple[str, str], ...] = (
    ("congress_bills", "bills"),
    ("bill_actions", "bill_actions"),
    ("bill_committees", "bill_committees"),
    ("bill_publisher_summaries", "bill_publisher_summaries"),
    ("bill_versions", "bill_versions"),
    ("bill_sections", "bill_sections"),
    ("section_diffs", "section_diffs"),
    ("section_diff_items", "section_diff_items"),
    ("financial_changes", "financial_changes"),
    ("section_classifications", "section_classifications"),
    ("bill_summaries", "bill_summaries"),
    ("diff_summaries", "diff_summaries"),
)


class BulkStatusSource(Protocol):
    """What this transform needs of a BILLSTATUS archive acquirer.

    Structural on purpose: the real ``BulkStatusAcquirer`` satisfies it, and so
    does the fixture-backed stub in ``tests/test_bill_family.py``. Naming the
    concrete class here would make the hermetic test unrepresentable.
    """

    def acquire(self, congress: int, bill_type: str) -> Any: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer."""

    def acquire(self, package_id: str, *, prefer: Sequence[str] = ..., max_bytes: int | None = ...) -> Any: ...


def engine_stamp() -> EngineStamp:
    """The installed engine's stamp, with the vendored commit filled in."""
    stamp = installed_engine_stamp()
    return stamp if stamp.revision else EngineStamp(stamp.name, stamp.version, DELTATRACK_REVISION)


def _version_captures(status: Any, acquirer: PackageBodySource | None, budget: list[int]) -> list[BillVersionCapture]:
    """One :class:`BillVersionCapture` per printing, fetching bodies while budget remains.

    A printing whose body is not fetched still gets a capture — the publisher's
    own facts about it (type, date, offered formats) are real and belong in
    ``bill_versions``; what is missing is the body, and the NULL columns say so.
    """
    captures: list[BillVersionCapture] = []
    for version in status.text_versions:
        if not version.type:
            continue
        version_code = version_slug(version.type)
        chosen = choose_format(version.formats, prefer=BODY_PREFERENCE)
        package_id = version.package_id
        if package_id is None and chosen is not None:
            package_id = bill_package_id_from_url(status.identity, chosen.url)
        if package_id is None:
            package_id = bill_version_package_id(status.identity, version_code)

        body = None
        document = None
        source = "congress"
        if acquirer is not None and chosen is not None and budget[0] > 0:
            budget[0] -= 1
            try:
                package = acquirer.acquire(package_id, prefer=BODY_PREFERENCE)
            except Exception as error:  # noqa: BLE001 — one printing's refusal is not the bill's
                logger.warning("Bill family: {} {} body refused: {}", package_id, version_code, error)
            else:
                body = package.body_capture
                source = "govinfo"
                if package.format == "xml" and engine_available():
                    try:
                        document = parse_bill_tree(body.body, version=version_code)
                    except Exception as error:  # noqa: BLE001 — an unparsed printing is a NULL tree
                        logger.warning("Bill family: {} tree refused: {}", package_id, error)

        captures.append(
            BillVersionCapture(
                version=version,
                version_code=version_code,
                source=source,
                package_id=package_id,
                chosen_format=chosen,
                body=body,
                document=document,
            )
        )
    return captures


def _prior_snapshot(output_dir: Path, download_prior: Callable[[str, Path], bool] = r2.download) -> Any:
    """The previously published bills/versions/summaries, as an event snapshot.

    Downloaded through ``merge_table``'s own scratch path, so the merge that
    follows reuses each file in place instead of fetching it a second time.
    A table that has never been published contributes nothing, which makes the
    first run's events all ``*_added`` — correct, and stated in the coverage.
    """
    import duckdb

    rows: dict[str, list[dict]] = {}
    for name in SNAPSHOT_TABLES:
        path = prior_scratch_path(output_dir, name)
        if not (path.exists() or download_prior(f"{name}.parquet", path)):
            rows[name] = []
            continue
        columns = TABLE_CONTRACTS[name].columns
        present = {str(r[0]) for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
        select = ", ".join(c if c in present else f"CAST(NULL AS VARCHAR) AS {c}" for c in columns)
        table = duckdb.sql(f"SELECT {select} FROM read_parquet('{path}')").arrow()
        rows[name] = table.to_pylist()
        logger.info("Bill family: prior {} has {:,} rows", name, len(rows[name]))
    return snapshot_from_rows(
        bills=rows["congress_bills"],
        bill_versions=rows["bill_versions"],
        bill_summaries=rows["bill_summaries"],
    )


def build_bill_family(
    output_dir: Path,
    *,
    bulk_acquirer: BulkStatusSource | None = None,
    body_acquirer: PackageBodySource | None = None,
    max_version_fetches: int = MAX_VERSION_FETCHES,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, ...]:
    """Build all thirteen bill-family tables; returns one path per table."""
    congresses = congresses_from_env()
    bill_types = bill_types_from_env()
    logger.info("Bill family: Congresses {}, bill types {}", congresses, bill_types)

    bulk_acquirer = bulk_acquirer or BulkStatusAcquirer(budget=BULK_BUDGET)
    if body_acquirer is None:
        api_key = _resolve_api_key()
        if api_key:
            body_acquirer = GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)
        else:
            logger.warning(
                "Bill family: no api.data.gov key ({}) — publishing status-derived tables only",
                ", ".join(API_KEY_ENV_VARS),
            )

    # The model seams, wired only when a key is present.
    gemini_key = resolve_gemini_key()
    classify = summarize = summarize_diff_call = None
    if gemini_key:
        from spicy_docs.extraction.gemini import GeminiClient

        model = os.environ.get("GEMINI_MODEL", DEFAULT_MODEL)
        call = model_call(GeminiClient(api_key=gemini_key))
        classify = functools.partial(classify_sections, call=call, model=model)
        summarize = functools.partial(summarize_bill, call=call, model=model)
        summarize_diff_call = functools.partial(summarize_diff, call=call, model=model)
        logger.info("Bill family: model seams wired ({})", model)
    else:
        logger.info("Bill family: no Gemini key — section_classifications, bill_summaries, diff_summaries stay empty")

    stamp = engine_stamp()
    logger.info("Bill family: engine {} {} @ {}", stamp.name, stamp.version, stamp.revision or "(unknown)")

    # 1. Acquire and build, one bill at a time.
    remaining = [max_version_fetches]
    families: list[BillFamilyTables] = []
    refusals: Counter[str] = Counter()
    bills = skipped = 0
    for congress in congresses:
        for bill_type in bill_types:
            acquisition = bulk_acquirer.acquire(congress, bill_type)
            archive = acquisition.archive
            observed_at = acquisition.capture.observed_at
            logger.info(
                "Bill family: {} {} — {:,} parsed, {:,} refused by the reader",
                congress,
                bill_type,
                archive.parsed_count,
                archive.refused_count,
            )
            skipped += archive.refused_count
            for member in archive.members:
                if member.status is None:
                    continue
                capture = BillFamilyCapture(
                    status=member.status,
                    versions=tuple(_version_captures(member.status, body_acquirer, remaining)),
                    observed_at=observed_at,
                )
                tables = build_family(
                    capture,
                    engine=stamp,
                    classify=classify,
                    summarize=summarize,
                    summarize_diff=summarize_diff_call,
                )
                families.append(tables)
                for refusal in tables.refusals:
                    refusals[refusal.table] += 1
                bills += 1

    folded = BillFamilyTables.concat(families)
    logger.info("Bill family: {:,} bills, {:,} printings fetched", bills, max_version_fetches - remaining[0])
    if skipped:
        logger.warning("Bill family: {:,} archive entries the reader refused", skipped)
    if refusals:
        logger.warning("Bill family: refusals by table — {}", dict(refusals))

    # 2. The thirteenth table: what changed since the last published run.
    prior = _prior_snapshot(output_dir, download_prior)
    current = snapshot_from_rows(
        bills=folded.bills,
        bill_versions=folded.bill_versions,
        bill_summaries=folded.bill_summaries,
    )
    detected_at = _detected_at()
    events = activity_events(prior, current, detected_at=detected_at)
    logger.info("Bill family: {:,} activity events", len(events))

    # 3. Publish. Every table goes through the one merge helper.
    paths = [
        merge_contract_table(output_dir, contract, getattr(folded, attr), download_prior=download_prior)
        for contract, attr in FAMILY_TABLES
    ]
    paths.append(merge_contract_table(output_dir, "public_activity_events", events, download_prior=download_prior))
    return tuple(paths)


def _detected_at() -> str:
    """The run instant every event this run detects is stamped with."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
