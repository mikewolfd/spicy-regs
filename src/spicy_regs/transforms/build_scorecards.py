"""Replace complete publisher editions; preserve every failed or unobserved scope."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import urljoin, urlsplit
from uuid import UUID, uuid4
from os import getenv

import httpx
import pyarrow.parquet as pq
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.scorecards.registry import REGISTRY, load_registry, select_sources
from spicy_regs.scorecards.acquisition import MAX_BYTES, MAX_REQUESTS, ScorecardTransportError, validate_limits
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.sources import r2
from spicy_regs.scorecards.etl import (
    read_family,
    read_indexed_family,
    write_family,
    verified_receipt_download,
    source_failure_receipts,
    POLICIES,
)
from spicy_regs.etl_receipts import write_dataset

TABLE_NAMES = (
    "scorecard_publishers",
    "scorecards",
    "scorecard_snapshots",
    "scorecard_methodologies",
    "scorecard_metrics",
    "scorecard_items",
    "scorecard_metric_items",
    "scorecard_metric_components",
    "scorecard_members",
    "scorecard_member_ratings",
    "scorecard_member_item_results",
)
OUTPUTS = tuple(name + ".parquet" for name in TABLE_NAMES if name != "scorecard_snapshots")


class NoScorecardsDue(RuntimeError):
    """Explicit successful no-op; do not create a new generation."""


class ScorecardRefreshError(RuntimeError):
    """No accepted scopes, or an integrity failure; no candidate generation."""


def installed_provider():
    """Import only the installed package; never reach into a sibling checkout."""
    from spicy_docs.schemas.scorecard_tables import SCORECARD_TABLES
    from spicy_docs.sources.scorecards import get_adapter
    from spicy_docs.sources.scorecards.common import ScorecardContext, ScorecardSourceError, validate_scorecard_bundle

    return SimpleNamespace(
        contracts=SCORECARD_TABLES,
        get_adapter=get_adapter,
        context=ScorecardContext,
        validate=validate_scorecard_bundle,
        refusal=ScorecardSourceError,
    )


def _fatal(error: BaseException) -> None:
    """An owner may wrap transport errors; integrity and credentials still abort."""
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        if isinstance(current, (SourceEvidenceError, CredentialRefusedError, ScorecardTransportError)):
            raise current
        seen.add(id(current))
        current = current.__cause__ or current.__context__


@contextmanager
def bounded_fetch(source, *, max_bytes=MAX_BYTES, max_requests=MAX_REQUESTS):
    """Bound exact HTTP payloads in memory, outside all workflow upload trees."""
    validate_limits(max_bytes, max_requests)
    count = 0
    with httpx.Client(
        timeout=httpx.Timeout(30, connect=10),
        follow_redirects=False,
        headers={"Accept-Encoding": "identity", "User-Agent": "SpicyRegs/scorecards"},
    ) as client:

        def request(url, *, method="GET", content=None, request_headers=None):
            nonlocal count
            if method not in {"GET", "POST"} or (content is not None) != (method == "POST"):
                raise ScorecardRefreshError("Publisher request method and body disagree")
            requested, current = url, url
            host = urlsplit(url).hostname
            # The CPAC publisher application explicitly configures this public
            # GraphQL service over HTTP. The reader retains that source choice.
            schemes = (
                {"http", "https"}
                if source.publisher_id == "cpac" and host == "production.data.conservative.org"
                else {"https"}
            )
            started = time.monotonic()
            for _ in range(6):
                parsed = urlsplit(current)
                if (
                    parsed.scheme not in schemes
                    or parsed.hostname not in {host, "www." + str(host), str(host).removeprefix("www.")}
                    or parsed.username
                    or parsed.password
                    or parsed.port
                    or (request_headers and parsed.hostname != host)
                ):
                    raise ScorecardRefreshError("Publisher redirect leaves the selected host")
                if count >= max_requests:
                    raise ScorecardRefreshError("Scorecard HTTP request budget exhausted")
                count += 1
                observed = datetime.now(UTC).isoformat()
                raw = bytearray()
                try:
                    headers = {"Content-Type": "application/json"} if method == "POST" else {}
                    if request_headers:
                        headers.update(request_headers)
                    if source.publisher_id == "cpac" and host == "production.data.conservative.org":
                        # Public role declarations used by the publisher app,
                        # not credentials or an administrative role.
                        headers.update({"X-Hasura-Role": "anonymous", "X-Hasura-User-Id": "-1"})
                    with client.stream(method, current, content=content, headers=headers) as response:
                        for chunk in response.iter_raw():
                            if len(raw) + len(chunk) > max_bytes or time.monotonic() - started > 90:
                                raise ScorecardRefreshError("Source HTTP response exceeds its acquisition bound")
                            raw.extend(chunk)
                        capture = CapturedBodyResponse(
                            requested,
                            str(response.url),
                            response.status_code,
                            response.headers.get("content-type"),
                            observed,
                            bytes(raw),
                            content_encoding=response.headers.get("content-encoding", "identity"),
                            method=method,
                            request_body=content,
                        )
                        if response.is_redirect:
                            source.capture(capture, stage="redirect")
                            if method == "POST":
                                raise ScorecardRefreshError("Publisher POST redirects require an explicit source rule")
                            location = response.headers.get("location")
                            if not location:
                                raise ScorecardRefreshError("Publisher redirect has no location")
                            current = urljoin(current, location)
                            continue
                        return capture
                except BaseException as error:
                    source.event(
                        "capture-incomplete",
                        stage="http",
                        error_type=type(error).__name__,
                        response_complete=False,
                        body_retained=False,
                        requested_url=current,
                        bytes_received=len(raw),
                    )
                    raise
            raise ScorecardRefreshError("Publisher redirect limit exceeded")

        from spicy_regs.scorecards.acquisition import RequestFetcher

        yield RequestFetcher(request)


def _prior_tables(
    output_dir, evidence, contracts, download_prior, receipt_public_url=None, download_prior_receipts=None
):
    """Verify legacy input explicitly or reconstruct a native generation with its receipts."""
    snapshot = evidence.read_snapshot or {"families": {}}
    prior = snapshot["families"].get("scorecards")
    if prior is None:
        return {name: [] for name in contracts}, {name: False for name in contracts}, None
    native = "etlReceipts" in prior
    expected = set(OUTPUTS) if native else {name + ".parquet" for name in TABLE_NAMES}
    if set(prior["tables"]) != expected:
        raise ScorecardRefreshError("Prior scorecard family does not contain the complete frozen table set")
    directory = output_dir / (".scorecard-prior-" + uuid4().hex)
    directory.mkdir()
    result = {}
    receipt = None
    for key in sorted(expected):
        path = directory / key
        if not download_prior(key, path):
            raise ScorecardRefreshError("A managed prior scorecard table is unavailable")
        pin = prior["tables"][key]
        with path.open("rb") as stream:
            digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        if pin.get("sha256") != digest or pin.get("byteSize") != path.stat().st_size:
            raise ScorecardRefreshError("Managed prior scorecard table differs from its immutable pin")
        if not native:
            result[path.stem] = pq.ParquetFile(path).read().to_pylist()
    if native:
        if download_prior_receipts is not None and not download_prior_receipts(directory / "etl_receipts.parquet"):
            raise ScorecardRefreshError("Pinned prior receipts are unavailable")
        receipt = verified_receipt_download(
            snapshot, directory / "etl_receipts.parquet", public_url=receipt_public_url or getenv("R2_PUBLIC_URL")
        )
        result = read_indexed_family(directory, TABLE_NAMES, prior, receipt_path=receipt)
    return result, {name: True for name in contracts}, receipt


def _accept(result, edition, source, receipts, provider, parser_version):
    provider.validate(result.tables)
    if set(result.tables) != set(TABLE_NAMES):
        raise ScorecardRefreshError("Provider changed the frozen scorecard table set")
    if result.scorecard_id != edition.scorecard_id or not result.completeness_rule:
        raise provider.refusal("Acquisition identity or completeness witness differs from selected edition")
    rows = result.tables
    if len(rows["scorecards"]) != 1 or len(rows["scorecard_snapshots"]) != 1:
        raise provider.refusal("Acquisition must contain exactly one whole edition")
    card, snapshot = rows["scorecards"][0], rows["scorecard_snapshots"][0]
    if (
        card["scorecard_id"] != result.scorecard_id
        or card["publisher_id"] != source.publisher_id
        or snapshot["snapshot_id"] != result.snapshot_id
        or snapshot["evidence_policy"] != source.evidence_policy
        or snapshot["completeness_rule"] != result.completeness_rule
        or snapshot["parser_version"] != parser_version
        or (edition.chamber is not None and card["chamber_scope_text"] != edition.chamber)
        or any(
            row.get("scorecard_id") != result.scorecard_id
            for name, group in rows.items()
            if name != "scorecard_publishers"
            for row in group
        )
        or any(row["publisher_id"] != source.publisher_id for row in rows["scorecard_publishers"])
    ):
        raise provider.refusal("Acquisition escaped its selected publisher or edition scope")
    if source.evidence_policy == "metadata_only":
        try:
            suffix = result.snapshot_id.removeprefix("scorecard-snapshot:")
            opaque = UUID(suffix)
            if result.snapshot_id != "scorecard-snapshot:" + str(opaque) or opaque.version != 4:
                raise ValueError("Not an opaque observation ID")
        except (ValueError, AttributeError) as error:
            raise SourceEvidenceError("Metadata-only snapshot must use an opaque UUID4 observation identity") from error
    selected = json.loads(snapshot["capture_ids_json"])
    if (
        set(result.capture_ids) != set(selected)
        or len(result.capture_ids) != len(set(result.capture_ids))
        or not set(selected) <= receipts.keys()
    ):
        raise SourceEvidenceError("Edition capture selection differs from observed evidence")
    for capture_id in selected:
        receipt = receipts[capture_id]
        if (
            receipt["status_code"] != 200
            or receipt["response_complete"] is not True
            or receipt["byte_size"] <= 0
            or receipt["evidence_policy"] != source.evidence_policy
        ):
            raise provider.refusal("Selected edition capture is not a complete successful source body")
    for group in rows.values():
        for row in group:
            receipt = receipts.get(row["capture_id"])
            if receipt is None or row["source_url"] != receipt["resolved_url"]:
                raise SourceEvidenceError("Source row locator differs from observed capture metadata")
    if not rows["scorecard_members"]:
        raise provider.refusal("An empty member result cannot establish a complete scorecard")
    declared = json.loads(snapshot["source_declared_counts_json"] or "{}")
    parsed = json.loads(snapshot["parsed_counts_json"] or "{}")
    if not isinstance(declared, dict) or not isinstance(parsed, dict):
        raise provider.refusal("Invalid source count witness")
    for name in TABLE_NAMES:
        if name in parsed and (type(parsed[name]) is not int or parsed[name] != len(rows[name])):
            raise provider.refusal("Parser row count witness differs from source rows")
        if name in declared and declared[name] != len(rows[name]):
            raise provider.refusal("Source-declared table count differs from parsed rows")


def build_scorecards(
    output_dir: Path,
    *,
    evidence: CaptureEvidence,
    registry: Path = REGISTRY,
    publishers=(),
    editions=(),
    historical_backfill=False,
    force=False,
    provider=None,
    pdf_extractor=None,
    retain_extraction=None,
    fetch_factory: Callable = bounded_fetch,
    download_prior: Callable = r2.download,
    receipt_public_url: str | None = None,
    download_prior_receipts: Callable | None = None,
    receipt_generation_id: str | None = None,
    now: datetime | None = None,
    max_bytes: int = MAX_BYTES,
    max_requests: int = MAX_REQUESTS,
    validate_acquisitions: Callable | None = None,
    validate_readback: Callable | None = None,
) -> tuple[Path, ...]:
    """Acquire explicit selected scopes and construct one complete source family.

    A PDF host injects the existing SpicyDocs extractor and private observation
    retention together. These objects never enter public evidence or table rows.
    The host owns the model choice and private storage outside publication trees.
    A synchronous readback validator may inspect the already receipt-checked
    prior and persisted current rows before success; rows are not reused across
    a later filesystem or generation admission boundary.
    """
    validate_limits(max_bytes, max_requests)
    if (pdf_extractor is None) != (retain_extraction is None):
        raise ScorecardRefreshError("PDF extraction requires both an extractor and private observation retention")
    now = now or datetime.now(UTC)
    sources = select_sources(load_registry(registry), publishers=publishers, historical_backfill=historical_backfill)
    due = []
    for source in sources:
        prior = evidence.inherited_event("scorecard-source-attempt", publisher_id=source.publisher_id)
        if source.due(now, prior.get("observed_at") if prior else None, force=force):
            due.append(source)
    if not due:
        evidence.event("scorecard-no-op", reason_code="no_sources_due")
        raise NoScorecardsDue("No enabled qualified scorecard sources are due")
    provider = provider or installed_provider()
    if set(provider.contracts) != set(TABLE_NAMES):
        raise ScorecardRefreshError("Installed provider does not expose the frozen scorecard tables")
    output_dir.mkdir(parents=True, exist_ok=True)
    prior_rows, present, prior_receipts = _prior_tables(
        output_dir, evidence, provider.contracts, download_prior, receipt_public_url, download_prior_receipts
    )
    if any(present.values()):
        provider.validate(prior_rows)
    accepted, failed, selected_editions = {}, [], set()
    failed_attempts = []
    generation_id = receipt_generation_id or "scorecard-build:" + uuid4().hex
    requested = set(editions)
    for source in due:
        adapter = provider.get_adapter(source.adapter)
        scope = evidence.for_source(
            source.publisher_id,
            source.evidence_policy,
            parser_version=adapter.parser_version,
            policy_decision_id=source.policy_decision_id,
        )
        receipts = {}

        def record(response, *, stage):
            receipt = scope.capture(response, stage=stage)
            receipts[receipt["capture_id"]] = receipt
            return receipt

        evidence.event(
            "scorecard-source-attempt",
            publisher_id=source.publisher_id,
            observed_at=now.isoformat(),
            qualification_id=source.qualification_id,
            historical_backfill=historical_backfill,
        )
        try:
            fetch_manager = (
                bounded_fetch(scope, max_bytes=max_bytes, max_requests=max_requests)
                if fetch_factory is bounded_fetch
                else fetch_factory(scope)
            )
            with fetch_manager as fetch:
                context = provider.context(
                    fetch=fetch,
                    capture=record,
                    evidence_policy=source.evidence_policy,
                    max_bytes=max_bytes,
                    max_requests=max_requests,
                    pdf_extractor=pdf_extractor,
                    retain_extraction=retain_extraction,
                )
                listed = tuple(adapter.list_scorecards(context))
                if len({e.scorecard_id for e in listed}) != len(listed):
                    raise provider.refusal("Publisher listing repeats edition identities")
                if not listed:
                    raise provider.refusal("An empty listing does not establish source deletion")
                selected = [
                    e
                    for e in listed
                    if (historical_backfill or e.is_current)
                    and (not requested or e.scorecard_id in requested or e.edition_id in requested)
                ]
                for edition in selected:
                    selected_editions.update({edition.scorecard_id, edition.edition_id} & requested)
                    if edition.publisher_id != source.publisher_id:
                        raise provider.refusal("Publisher listing escaped its selected identity")
                    try:
                        edition_adapter = adapter.for_edition(edition) if hasattr(adapter, "for_edition") else adapter
                        if edition_adapter is not adapter:
                            scope.event(
                                "scorecard-edition-reader",
                                scorecard_id=edition.scorecard_id,
                                reader_parser_version=edition_adapter.parser_version,
                            )
                        acquired = edition_adapter.acquire_scorecard(edition, context)
                        _accept(acquired, edition, source, receipts, provider, edition_adapter.parser_version)
                        if evidence.retention_failure:
                            raise SourceEvidenceError("Source evidence retention failed")
                        if acquired.scorecard_id in accepted:
                            raise ScorecardRefreshError("One run cannot replace an edition more than once")
                        accepted[acquired.scorecard_id] = acquired
                        evidence.event(
                            "scorecard-scope-success",
                            publisher_id=source.publisher_id,
                            scorecard_id=acquired.scorecard_id,
                            snapshot_id=acquired.snapshot_id,
                            observed_at=now.isoformat(),
                        )
                    except (provider.refusal, ScorecardRefreshError, httpx.HTTPError) as error:
                        _fatal(error)
                        failed_attempts.append(
                            dict(
                                publisher_id=source.publisher_id,
                                scorecard_id=edition.scorecard_id,
                                stage="edition",
                                error_type=type(error).__name__,
                            )
                        )
                        scope.refusal(error, stage="edition")
                        failed.append(edition.scorecard_id)
                        evidence.event(
                            "scorecard-scope-failure",
                            publisher_id=source.publisher_id,
                            scorecard_id=edition.scorecard_id,
                            reason_code="acquisition_refused",
                        )
        except (provider.refusal, ScorecardRefreshError, httpx.HTTPError) as error:
            _fatal(error)
            failed_attempts.append(
                dict(publisher_id=source.publisher_id, stage="listing", error_type=type(error).__name__)
            )
            scope.refusal(error, stage="listing")
            failed.append(source.publisher_id + ":listing")
    if requested - selected_editions:
        failed.extend(sorted(requested - selected_editions))
        evidence.event("scorecard-selection-missing", edition_ids=sorted(requested - selected_editions))
    if evidence.retention_failure:
        raise SourceEvidenceError("Source evidence retention failed")
    if validate_acquisitions is not None:
        validate_acquisitions()
    attempt_receipts = source_failure_receipts(failed_attempts, generation_id=generation_id, registry=registry)
    if not accepted:
        write_dataset(
            [],
            output_dir / (".scorecard-failures-" + uuid4().hex),
            POLICIES["scorecard_snapshots"],
            failures=attempt_receipts,
        )
        evidence.event("scorecard-all-failed", failed_scopes=failed)
        raise ScorecardRefreshError("No complete scorecard edition was acquired; prior generation preserved")
    fresh = {name: [] for name in TABLE_NAMES}
    publisher_rows = {}
    for result in accepted.values():
        for name, rows in result.tables.items():
            if name == "scorecard_publishers":
                for row in rows:
                    # Publisher observations are independent of edition snapshots; latest observed row wins.
                    old = publisher_rows.get(row["publisher_id"])
                    if old is None or (row.get("observed_at") or "") > (old.get("observed_at") or ""):
                        publisher_rows[row["publisher_id"]] = row
            else:
                fresh[name].extend(rows)
    fresh["scorecard_publishers"] = list(publisher_rows.values())
    merged = {}
    for name, contract in provider.contracts.items():
        scope_column = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
        replaced = set(publisher_rows) if name == "scorecard_publishers" else set(accepted)
        merged[name] = [row for row in prior_rows[name] if row[scope_column] not in replaced] + fresh[name]
        current_keys = {contract.key(row) for row in fresh[name]}
        removed = [
            list(contract.key(row))
            for row in prior_rows[name]
            if row[scope_column] in replaced and contract.key(row) not in current_keys
        ]
        if removed:
            evidence.event(
                "rows-retired",
                table=name,
                key=list(contract.identity),
                rows=removed,
                reason="complete-scorecard-replacement",
            )
    provider.validate(merged)  # Duplicates/orphans fail before the merge helper can deduplicate them.
    paths = write_family(
        output_dir,
        merged,
        generation_id=generation_id,
        attempt_failures=attempt_receipts,
        prior_receipts=prior_receipts,
    )
    readback = read_family(output_dir, TABLE_NAMES)
    provider.validate(readback)
    if validate_readback is not None:
        validate_readback(prior_rows, readback)
    evidence.event(
        "scorecard-refresh",
        accepted_scopes=sorted(accepted),
        failed_scopes=failed,
        coverage="complete replacement result; requested source failures remain explicit",
    )
    return tuple(paths)
