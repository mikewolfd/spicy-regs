"""Build and acquire multi-cycle FEC relationship, contribution, and outside-spending profiles.

This module is a downstream caller of maintained FEC source readers and financial
mappers. ``build`` executes strictly offline from retained originals in
``CentralRawArchiveStore``, streaming all five output tables through bounded Arrow
batches. ``acquire`` performs bounded OpenFEC API and docquery.fec.gov acquisition
using a caller-supplied or CI-provided api.data.gov key (never a local ``.env``
fallback) and pre-verifies each admitted scope against the exact domain transforms.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_docs.sources.fec.catalog import official_url
from spicy_docs.sources.fec.filings import filing_encoding, filing_records_from_stream
from spicy_docs.sources.fec.metadata import parse_api
from spicy_docs.sources.fec.retained import page_request
from spicy_regs.contract_types import described_schema
from spicy_regs.env_values import first_env
from spicy_regs.generations import build_generation
from spicy_regs.native_types import extra_field_checker
from spicy_regs.pipelines.raw_releases import inherit_read_index
from spicy_regs.runtime_bounds import checkpoint
from spicy_regs.source_evidence import CaptureEvidence, local_path_name, without_local_paths
from spicy_regs.transforms.build_fec_financial_profiles import json_bytes
from spicy_regs.transforms.fec_committee_profiles import (
    SCHEMA as COMMITTEE_SCHEMA,
    TABLE as COMMITTEES,
    iter_committee_profiles,
)
from spicy_regs.transforms.fec_contribution_profiles import (
    DECISIONS as CONTRIBUTION_DECISIONS,
    PROFILES as CONTRIBUTION_PROFILES,
    SCHEMAS as CONTRIBUTION_SCHEMAS,
    contribution_profiles,
)
from spicy_regs.transforms.fec_outside_profiles import (
    PROFILES as OUTSIDE_PROFILES,
    SCHEMAS as OUTSIDE_SCHEMAS,
    SPENDERS as OUTSIDE_SPENDERS,
    outside_profiles,
)
from spicy_regs.transforms.fec_query import CollectionSelection

FAMILY = "fec-research-profiles"
SCHEMAS = {COMMITTEES: COMMITTEE_SCHEMA, **CONTRIBUTION_SCHEMAS, **OUTSIDE_SCHEMAS}

SELECTION_FORMAT = "fec-research-profiles-selection"
SELECTION_VERSION = 2

#: Bounded original sizes (16 MiB API JSON pages; 128 MiB electronic Form 3/3P/3X .fec filings).
MAX_API_PAGE_BYTES = 16 * 1024**2
MAX_ORIGINAL_BYTES = 128 * 1024**2
#: Bounded page limits per query and committee census traversal.
MAX_AGGREGATE_PAGES = 100
MAX_COMMITTEE_PAGES = 5_000
#: Default streaming Parquet write batch size.
WRITE_BATCH_SIZE = 2_000

DEFAULT_CYCLES: tuple[int, ...] = (2020, 2022, 2024, 2026)
DEFAULT_COMMITTEE_CYCLES: tuple[int, ...] = (2024, 2026)
DEFAULT_CONTRIBUTION_FORMS: tuple[str, ...] = ("house-senate", "presidential", "pac-party")
REPORT_ENTITY_FORMS: dict[str, str] = {
    "house-senate": "F3",
    "presidential": "F3P",
    "pac-party": "F3X",
}
COMMITTEE_POPULATIONS = frozenset({"complete_traversal", "bounded_page_prefix", "sampled_pages"})
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "FEC_API_KEY", "REGULATIONS_GOV_API_KEY")
_REQUIRED_REPORT_MONEY_FIELDS = (
    "individual_itemized_contributions_period",
    "individual_unitemized_contributions_period",
    "total_individual_contributions_period",
    "refunded_individual_contributions_period",
    "other_political_committee_contributions_period",
    "refunded_other_political_committee_contributions_period",
)
_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")
_CANDIDATE_RE = re.compile(r"[HPS][A-Z0-9]{8}")
_COMMITTEE_RE = re.compile(r"C[0-9]{8}")
_FILING_RE = re.compile(r"[0-9]{1,12}")
_MIN_CYCLE = 1978
_MAX_CYCLE = 2030


def _resolve_api_key() -> str | None:
    """Resolve the shared api.data.gov key from process environment without logging it."""
    return first_env(API_KEY_ENV_VARS)


class _TableWriter:
    """Bounded streaming Parquet writer that replaces ``destination`` atomically on commit."""

    def __init__(self, destination: Path, schema: pa.Schema, *, batch_size: int = WRITE_BATCH_SIZE):
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self.destination = destination
        self.schema = schema
        self.batch_size = batch_size
        self.row_count = 0
        self._check_fields = extra_field_checker(schema)
        self._scratch = TemporaryDirectory(dir=destination.parent, prefix=".fec-profile-write-")
        self._temporary = Path(self._scratch.name) / destination.name
        self._writer: pq.ParquetWriter | None = pq.ParquetWriter(self._temporary, schema, compression="zstd")
        self._buffer: list[dict[str, Any]] = []
        self._committed = False

    def _flush(self) -> None:
        if not self._buffer:
            return
        assert self._writer is not None
        self._writer.write_table(pa.Table.from_pylist(self._buffer, schema=self.schema))
        self._buffer.clear()

    def write(self, rows: Iterable[dict[str, Any]]) -> int:
        written = 0
        for row in rows:
            self._check_fields(row)
            self._buffer.append(row)
            written += 1
            self.row_count += 1
            if len(self._buffer) >= self.batch_size:
                self._flush()
                checkpoint()
        return written

    def commit(self) -> Path:
        if self._committed:
            return self.destination
        self._flush()
        assert self._writer is not None
        self._writer.close()
        self._writer = None
        self._temporary.replace(self.destination)
        self._committed = True
        self._scratch.cleanup()
        return self.destination

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if not self._committed:
            self._scratch.cleanup()


def _capture_pin(capture: Mapping[str, Any], *, label: str = "capture") -> tuple[str, int]:
    if not isinstance(capture, Mapping):
        raise ValueError(f"FEC profile {label} must be an object")
    digest = capture.get("response_sha256") or capture.get("sha256")
    size = capture["byte_size"] if "byte_size" in capture else capture.get("bytes")
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise ValueError(f"FEC profile {label} is missing a valid response_sha256/sha256 digest")
    if type(size) is not int or size <= 0:
        raise ValueError(f"FEC profile {label} is missing a positive byte_size/bytes count")
    return digest, size


def _expected_query_pairs(params: Mapping[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for key, value in params.items():
        if isinstance(value, (list, tuple)):
            for item in value:
                pairs.append((key, str(item).lower() if isinstance(item, bool) else str(item)))
        else:
            pairs.append((key, str(value).lower() if isinstance(value, bool) else str(value)))
    return sorted(pairs)


def _read(evidence: CaptureEvidence, capture: Mapping[str, Any], stage: str, *, max_bytes: int = MAX_API_PAGE_BYTES) -> bytes:
    checkpoint()
    digest, size = _capture_pin(capture, label=stage)
    if size > max_bytes:
        raise ValueError("FEC profile input exceeds its original byte bound")
    with evidence.store.open(digest) as stream:
        body = stream.read(size + 1)
    # CentralRawArchiveStore.open verifies raw digest and size at exhaustion.
    if len(body) != size or "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        raise ValueError("FEC profile original differs from its selected digest/size")
    evidence.retain_bytes(
        body,
        stage=stage,
        raw_source=True,
        source_url=capture["request_url"],
        original_location=capture["request_url"],
        observed_at=capture.get("observed_at"),
    )
    return body


def _retain_archive_original(
    evidence: CaptureEvidence, capture: Mapping[str, Any], stage: str, *, max_bytes: int = MAX_ORIGINAL_BYTES
) -> tuple[str, int]:
    """Verify and retain an archived original without holding large filings twice in memory."""
    checkpoint()
    digest, size = _capture_pin(capture, label=stage)
    if size > max_bytes:
        raise ValueError("FEC profile input exceeds its original byte bound")
    described = evidence.store.describe_blob(digest, size)
    if described["encoding"] == "raw":
        evidence.retain_file(
            evidence.store.root / described["object"],
            stage=stage,
            raw_source=True,
            source_url=capture["request_url"],
            original_location=capture["request_url"],
            observed_at=capture.get("observed_at"),
        )
    elif size <= MAX_API_PAGE_BYTES:
        _read(evidence, capture, stage, max_bytes=max_bytes)
    else:
        with TemporaryDirectory(dir=evidence.directory, prefix=".fec-filing-spool-") as scratch:
            spooled = Path(scratch) / "original.fec"
            with evidence.store.open(digest) as src, spooled.open("wb") as dst:
                while chunk := src.read(1024 * 1024):
                    checkpoint()
                    dst.write(chunk)
            evidence.retain_file(
                spooled,
                stage=stage,
                raw_source=True,
                source_url=capture["request_url"],
                original_location=capture["request_url"],
                observed_at=capture.get("observed_at"),
            )
    return digest, size


def _buffered_stream(raw_stream: Any) -> Any:
    """Ensure ``readline`` is available on archive streams for streaming filing parsing."""
    if hasattr(raw_stream, "readline"):
        return raw_stream
    return io.BufferedReader(raw_stream)


def api_records(evidence: CaptureEvidence, query: Mapping[str, Any], *, max_pages: int = MAX_AGGREGATE_PAGES) -> list[dict[str, Any]]:
    """Check exact retained pages of one declared publisher API query."""
    records: list[dict[str, Any]] = []
    count: int | None = None
    pages_raw = query.get("pages")
    if not isinstance(pages_raw, list) or not 1 <= len(pages_raw) <= max_pages:
        raise ValueError("FEC profile query exceeds its bounded page population")
    pages: list[Any] = pages_raw
    expected_pairs = _expected_query_pairs(query["params"])
    for index, capture in enumerate(pages, 1):
        checkpoint()
        if not isinstance(capture, Mapping):
            raise ValueError("FEC profile page descriptor must be an object")
        capture_map: Mapping[str, Any] = capture
        pairs, page = page_request(capture_map["request_url"], endpoint="https://api.open.fec.gov" + query["path"])
        if page != index or sorted((k, v) for k, v in pairs if k != "page") != expected_pairs:
            raise ValueError("FEC profile page differs from its declared request scope")
        native = parse_api(_read(evidence, capture_map, query["name"], max_bytes=MAX_API_PAGE_BYTES))
        pagination = native.get("pagination", {})
        if (
            pagination.get("page") != index
            or pagination.get("is_count_exact") is not True
            or type(pagination.get("count")) is not int
            or pagination["count"] < 0
            or pagination.get("per_page") != query["params"]["per_page"]
            or max(1, pagination.get("pages", 0)) != len(pages)
        ):
            raise ValueError("FEC profile query has incomplete or uncertain pagination")
        if count is not None and count != pagination["count"]:
            raise ValueError("FEC profile count changed during its observed traversal")
        count = pagination["count"]
        rows = native.get("results")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("FEC profile API lacks a successful results list")
        records.extend(rows)
    if len(records) != count:
        raise ValueError("FEC profile query does not reconcile to the declared native count")
    return records


def iter_filing_rows(
    evidence: CaptureEvidence, capture: Mapping[str, Any], *, retain: bool = True
) -> Iterator[dict[str, Any]]:
    """Stream native FEC positional records using verified archive reads and bounded memory.

    A historical original with unknown acquisition time remains unknown. It is
    a raw-reader input, not an invented timestamp for a sealed source release.
    """
    if retain:
        digest, _ = _retain_archive_original(
            evidence, capture, "selected-contribution-filing", max_bytes=MAX_ORIGINAL_BYTES
        )
    else:
        digest, size = _capture_pin(capture, label="selected-contribution-filing")
        if size > MAX_ORIGINAL_BYTES:
            raise ValueError("FEC profile input exceeds its original byte bound")
        evidence.store.describe_blob(digest, size)
    with evidence.store.open(digest) as stream:
        encoding = filing_encoding(stream)
    collection = "filing-" + str(capture["filing_id"])
    with evidence.store.open(digest) as raw_stream:
        stream = _buffered_stream(raw_stream)
        found_header = False
        for ordinal, record in enumerate(filing_records_from_stream(stream, sha256=digest, encoding=encoding)):
            if ordinal % 512 == 0:
                checkpoint()
            if ordinal == 0:
                version = record.get("format_version")
                if not isinstance(version, str) or not version.strip():
                    raise ValueError("Selected contribution cohort requires a native filing format version")
                found_header = True
            source_id = f"{digest}/original/{ordinal:020d}"
            locator = {
                **record.get("source", {}),
                "collection_id": collection,
                "source_record_id": source_id,
                "member": None,
                "ordinal": ordinal,
            }
            yield dict(
                collection_id=collection,
                source_record_id=source_id,
                source_sha256=digest,
                source_locator_json=json_bytes(locator).decode(),
                metadata_json=json_bytes(record).decode(),
            )
        if not found_header:
            raise ValueError("Selected contribution filing is empty")


def filing_rows(evidence: CaptureEvidence, capture: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Materialize ``iter_filing_rows`` for callers and tests expecting a row list."""
    return list(iter_filing_rows(evidence, capture))


def normalize_selection(selection: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize v1 (pilot) or v2 (multi-cycle) selection payloads."""
    if not isinstance(selection, Mapping):
        raise ValueError("FEC research profile selection must be a JSON object")
    v1_outside = {"outside", "outside_candidate", "outside_cycle"}
    has_v1_outside = bool(v1_outside & set(selection))
    has_v2_outside = "outside_scopes" in selection
    has_v1_contrib = "contribution" in selection
    has_v2_contrib = "contribution_scopes" in selection
    if has_v1_outside and has_v2_outside:
        raise ValueError("Selection cannot mix legacy outside keys with outside_scopes")
    if has_v1_contrib and has_v2_contrib:
        raise ValueError("Selection cannot mix legacy contribution key with contribution_scopes")

    is_v2 = has_v2_outside or has_v2_contrib or "version" in selection or "format" in selection
    if is_v2:
        if selection.get("format") != SELECTION_FORMAT or selection.get("version") != SELECTION_VERSION:
            raise ValueError(f"Unsupported FEC selection format/version (expected {SELECTION_FORMAT} v{SELECTION_VERSION})")
        if not has_v2_outside or not has_v2_contrib or "committees" not in selection:
            raise ValueError("Selection v2 requires committees, outside_scopes, and contribution_scopes")
        outside_scopes = selection["outside_scopes"]
        contribution_scopes = selection["contribution_scopes"]
        committee_population = selection.get("committee_population", "sampled_pages")
    else:
        required = {"committees", *v1_outside, "contribution"}
        missing = sorted(required - set(selection))
        if missing:
            raise ValueError(f"Selection is missing required keys: {', '.join(missing)}")
        outside_scopes = [
            {
                "candidate_id": selection["outside_candidate"],
                "cycle": selection["outside_cycle"],
                "queries": selection["outside"],
            }
        ]
        contribution_scopes = [selection["contribution"]]
        committee_population = "sampled_pages"

    committees = selection["committees"]
    if not isinstance(committees, list) or not 1 <= len(committees) <= MAX_COMMITTEE_PAGES:
        raise ValueError("Selection requires a bounded non-empty committees page list")
    if committee_population not in COMMITTEE_POPULATIONS:
        raise ValueError(f"Unsupported committee_population {committee_population!r}")

    if not isinstance(outside_scopes, list) or not outside_scopes:
        raise ValueError("Selection requires at least one outside-spending scope")
    seen_outside: set[tuple[str, int]] = set()
    for scope in outside_scopes:
        if not isinstance(scope, Mapping) or not {"candidate_id", "cycle", "queries"} <= set(scope):
            raise ValueError("Outside-spending scope requires candidate_id, cycle, and queries")
        cid, cycle = scope["candidate_id"], scope["cycle"]
        if not isinstance(cid, str) or not _CANDIDATE_RE.fullmatch(cid) or type(cycle) is not int or cycle % 2:
            raise ValueError("Outside-spending scope requires a valid candidate_id and even cycle")
        if (cid, cycle) in seen_outside:
            raise ValueError(f"Duplicate outside-spending scope ({cid}, {cycle})")
        seen_outside.add((cid, cycle))
        if not isinstance(scope["queries"], list) or not scope["queries"]:
            raise ValueError("Outside-spending scope requires non-empty queries")

    if not isinstance(contribution_scopes, list) or not contribution_scopes:
        raise ValueError("Selection requires at least one contribution scope")
    seen_filings: set[str] = set()
    for scope in contribution_scopes:
        if not isinstance(scope, Mapping) or not {"original", "queries", "cycle"} <= set(scope):
            raise ValueError("Contribution scope requires original, queries, and cycle")
        original = scope["original"]
        if not isinstance(original, Mapping) or not original.get("filing_id"):
            raise ValueError("Contribution scope original requires filing_id")
        filing_id = str(original["filing_id"])
        if not _FILING_RE.fullmatch(filing_id):
            raise ValueError(f"Invalid contribution filing_id: {filing_id!r}")
        if filing_id in seen_filings:
            raise ValueError(f"Duplicate contribution filing_id: {filing_id}")
        seen_filings.add(filing_id)
        cycle = scope["cycle"]
        if type(cycle) is not int or cycle < _MIN_CYCLE or cycle % 2:
            raise ValueError("Contribution scope requires an even cycle")
        candidate_id = scope.get("candidate_id")
        if candidate_id is not None and (not isinstance(candidate_id, str) or not _CANDIDATE_RE.fullmatch(candidate_id)):
            raise ValueError(f"Invalid contribution candidate_id: {candidate_id!r}")
        if not isinstance(scope["queries"], list) or not scope["queries"]:
            raise ValueError("Contribution scope requires non-empty queries")

    return {
        "committees": committees,
        "committee_population": committee_population,
        "outside_scopes": outside_scopes,
        "contribution_scopes": contribution_scopes,
    }


def iter_selection_pins(selection: Mapping[str, Any]) -> list[tuple[str, str, int | None]]:
    """Return ``(label, digest, byte_size)`` for every pinned original in a v1 or v2 selection."""
    normalized = normalize_selection(selection)

    def _entry(item: Any, label: str) -> tuple[str, str, int | None]:
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} entry must be a JSON object")
        digest = item.get("response_sha256") or item.get("sha256")
        if not isinstance(digest, str) or not digest.strip():
            raise ValueError(f"{label} entry is missing response_sha256/sha256")
        raw_size = item["byte_size"] if "byte_size" in item else item.get("bytes")
        byte_size = raw_size if type(raw_size) is int else None
        return label, digest, byte_size

    pinned: list[tuple[str, str, int | None]] = []
    for item in normalized["committees"]:
        pinned.append(_entry(item, "committees"))
    for scope in normalized["outside_scopes"]:
        cid, cycle = scope["candidate_id"], scope["cycle"]
        for query in scope["queries"]:
            for page in query["pages"]:
                pinned.append(_entry(page, f"outside[{cid}:{cycle}/{query.get('name', '?')}]"))
    for scope in normalized["contribution_scopes"]:
        filing_id = scope["original"].get("filing_id", "?")
        pinned.append(_entry(scope["original"], f"contribution[{filing_id}].original"))
        for query in scope["queries"]:
            for page in query["pages"]:
                pinned.append(_entry(page, f"contribution[{filing_id}].queries[{query.get('name', '?')}]"))
    return pinned


def _select_matching_report(reports: Sequence[dict[str, Any]], filing_id: str) -> dict[str, Any]:
    """Select the unambiguous effective report matching ``filing_id``."""
    if len(reports) == 1:
        return reports[0]
    filing_int = int(filing_id)
    matched = [r for r in reports if not isinstance(r.get("file_number"), bool) and r.get("file_number") == filing_int]
    if len(matched) != 1:
        raise ValueError("Select exactly one effective report observation matching filing_id")
    chosen = matched[0]
    period_key = (
        chosen.get("report_year"),
        chosen.get("report_type"),
        chosen.get("coverage_start_date"),
        chosen.get("coverage_end_date"),
    )
    conflicting = [
        r
        for r in reports
        if r is not chosen
        and r.get("most_recent") is True
        and (
            r.get("report_year"),
            r.get("report_type"),
            r.get("coverage_start_date"),
            r.get("coverage_end_date"),
        )
        == period_key
    ]
    if conflicting:
        raise ValueError("Multiple most_recent reports conflict for the selected period")
    return chosen


def build(
    selection: Mapping[str, Any],
    output_dir: Path | str,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
):
    """Build all five FEC research profile Parquet tables strictly offline in bounded memory."""
    normalized = normalize_selection(selection)
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh FEC profile output directory")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    writers: dict[str, _TableWriter] = {}
    try:
        snapshot = inherit_read_index(evidence, read_index)
        selection_pin = evidence.retain_bytes(
            json_bytes(without_local_paths(dict(selection))), stage="fec-profile-selection"
        )["sha256"]
        writers = {table: _TableWriter(output_dir / f"{table}.parquet", schema) for table, schema in SCHEMAS.items()}

        # Stage 1: Committee disclosures (streamed 1 page at a time).
        committees = normalized["committees"]
        committee_population = normalized["committee_population"]
        seen_committee_pages: set[tuple[str, str]] = set()
        expected_committee_count: int | None = None
        committee_rows_seen = 0
        for index, capture in enumerate(committees, 1):
            checkpoint()
            digest, _ = _capture_pin(capture, label="committees")
            page_key = (digest, capture["request_url"])
            if page_key in seen_committee_pages:
                raise ValueError("Duplicate committee page in selection")
            seen_committee_pages.add(page_key)
            _pairs, page = page_request(
                capture["request_url"],
                endpoint="https://api.open.fec.gov/v1/committees/",
                order_by="committee_id",
            )
            native = parse_api(_read(evidence, capture, "committee-disclosure-page", max_bytes=MAX_API_PAGE_BYTES))
            pagination = native.get("pagination", {})
            rows = native.get("results")
            if (
                not isinstance(rows, list)
                or len(rows) != capture["expected_results"]
                or pagination.get("page") != page
            ):
                raise ValueError("Selected committee page does not match its retained population")
            if committee_population == "complete_traversal":
                if (
                    page != index
                    or pagination.get("is_count_exact") is not True
                    or type(pagination.get("count")) is not int
                    or pagination["count"] < 0
                    or max(1, pagination.get("pages", 0)) != len(committees)
                ):
                    raise ValueError("Complete committee traversal has incomplete or non-contiguous pagination")
                if expected_committee_count is not None and expected_committee_count != pagination["count"]:
                    raise ValueError("Committee traversal count changed across pages")
                expected_committee_count = pagination["count"]
            committee_rows_seen += writers[COMMITTEES].write(iter_committee_profiles(rows, capture))
        if committee_population == "complete_traversal" and committee_rows_seen != expected_committee_count:
            raise ValueError("Committee traversal row count differs from declared exact count")

        # Stage 2: Outside spending (streamed 1 candidate-cycle scope at a time).
        for scope in normalized["outside_scopes"]:
            checkpoint()
            queries = scope["queries"]
            outside = {query["name"]: api_records(evidence, query) for query in queries}
            if "by-spender" not in outside or "totals" not in outside:
                raise ValueError("Outside-spending scope requires both by-spender and totals queries")
            qualified = outside_profiles(
                outside["by-spender"],
                outside["totals"],
                candidate_id=scope["candidate_id"],
                cycle=scope["cycle"],
                evidence=queries,
            )
            writers[OUTSIDE_PROFILES].write(qualified[OUTSIDE_PROFILES])
            writers[OUTSIDE_SPENDERS].write(qualified[OUTSIDE_SPENDERS])

        # Stage 3: Contributions (streamed 1 filing scope at a time).
        for scope in normalized["contribution_scopes"]:
            checkpoint()
            context = {query["name"]: api_records(evidence, query) for query in scope["queries"]}
            if "reports" not in context or "committee-history" not in context:
                raise ValueError("Contribution scope requires reports and committee-history queries")
            if len(context["committee-history"]) != 1:
                raise ValueError("Select exactly one committee history observation")
            capture = scope["original"]
            filing_id = str(capture["filing_id"])
            report = _select_matching_report(context["reports"], filing_id)
            candidate_id = scope.get("candidate_id")
            if candidate_id is not None:
                if "candidate-search" in context:
                    candidates = context["candidate-search"]
                elif "candidate" in context:
                    candidates = context["candidate"]
                else:
                    raise ValueError("Candidate-committee contribution scope requires candidate or candidate-search query")
            else:
                candidates = context.get("candidate-search") or context.get("candidate") or []
            digest, _ = _capture_pin(capture, label=f"contribution[{filing_id}].original")
            mapped_selection = CollectionSelection(
                "filing-" + filing_id,
                digest,
                digest,
                "official-fec",
                scope["cycle"],
                "snapshot",
                selection_pin,
            )
            qualified = contribution_profiles(
                iter_filing_rows(evidence, capture),
                mapped_selection,
                report=report,
                filing_id=filing_id,
                source_url=capture["request_url"],
                captured_at=capture.get("observed_at"),
                candidate_id=candidate_id,
                committee_history=context["committee-history"][0],
                candidates=candidates,
                evidence=scope["queries"],
            )
            writers[CONTRIBUTION_PROFILES].write(qualified[CONTRIBUTION_PROFILES])
            writers[CONTRIBUTION_DECISIONS].write(qualified[CONTRIBUTION_DECISIONS])

        paths = [writers[table].commit() for table in SCHEMAS]
        row_counts = {table: writers[table].row_count for table in SCHEMAS}
        summary = {
            "family": FAMILY,
            "committee_population": committee_population,
            "committee_pages": len(committees),
            "outside_scopes": len(normalized["outside_scopes"]),
            "contribution_scopes": len(normalized["contribution_scopes"]),
            "rows": row_counts,
        }
        evidence.event("fec-research-profiles-built", **summary)
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=paths,
            expected_keys=[p.name for p in paths],
            schemas={n: described_schema(s) for n, s in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        for writer in writers.values():
            writer.close()
        evidence.finish(error)
        raise
    evidence.finish()
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return artifact, evidence.artifact_dir


def _validate_cycles(cycles: Sequence[int], *, allow_empty: bool = False) -> tuple[int, ...]:
    if not cycles:
        if allow_empty:
            return ()
        raise ValueError("At least one two-year FEC cycle is required")
    seen: list[int] = []
    for cycle in cycles:
        if type(cycle) is not int or cycle < _MIN_CYCLE or cycle > _MAX_CYCLE or cycle % 2:
            raise ValueError(f"Invalid FEC two-year cycle: {cycle!r}")
        if cycle in seen:
            raise ValueError(f"Duplicate FEC cycle: {cycle}")
        seen.append(cycle)
    return tuple(sorted(seen))


def _load_reused_committee_pages(
    source_path: Path, evidence: CaptureEvidence, *, max_pages: int = 0
) -> tuple[list[dict[str, Any]], str]:
    """Load and verify retained ``/v1/committees/`` pages from a selection JSON or ``pages.jsonl``."""
    source_path = Path(source_path)
    if not source_path.is_file():
        raise ValueError(f"Reused committee page inventory does not exist: {source_path}")
    entries: list[dict[str, Any]] = []
    population = "sampled_pages"
    if source_path.suffix == ".jsonl":
        blobs_root = source_path.parent / "blobs"
        for line in source_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            ev = row["evidence"]
            digest = ev["sha256"]
            size = ev["bytes"]
            try:
                evidence.store.describe_blob(digest, size)
            except FileNotFoundError:
                blob_file = blobs_root / ev["blob_path"]
                if not blob_file.is_file():
                    raise ValueError(f"Reused committee blob missing from archive and capture dir: {digest}") from None
                evidence.retain_file(
                    blob_file,
                    stage="committee-disclosure-page-reuse",
                    raw_source=True,
                    source_url=row["request_url"],
                    original_location=row["request_url"],
                    observed_at=row.get("observed_at"),
                )
            entries.append(
                {
                    "response_sha256": digest,
                    "byte_size": size,
                    "request_url": row["request_url"],
                    "observed_at": row.get("observed_at"),
                    "expected_results": row["records"],
                }
            )
    else:
        prior = json.loads(source_path.read_text(encoding="utf-8"))
        if not isinstance(prior, Mapping) or not isinstance(prior.get("committees"), list):
            raise ValueError("Reused selection file lacks a committees page list")
        population = str(prior.get("committee_population", "sampled_pages"))
        for item in prior["committees"]:
            digest, size = _capture_pin(item, label="reused-committees")
            evidence.store.describe_blob(digest, size)
            entries.append(
                {
                    "response_sha256": digest,
                    "byte_size": size,
                    "request_url": item["request_url"],
                    "observed_at": item.get("observed_at"),
                    "expected_results": item["expected_results"],
                }
            )
    if max_pages > 0 and len(entries) > max_pages:
        entries = entries[:max_pages]
        population = "bounded_page_prefix"
    if not entries:
        raise ValueError("Reused committee page inventory is empty")
    return entries, population


def _select_balanced_candidates(totals_rows: Sequence[Mapping[str, Any]], limit: int) -> list[str]:
    """Choose up to ``limit`` candidate IDs balanced across Presidential (P), Senate (S), and House (H)."""
    if limit <= 0:
        return []
    totals_by_candidate: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for row in totals_rows:
        cid = row.get("candidate_id")
        if not isinstance(cid, str) or not _CANDIDATE_RE.fullmatch(cid):
            continue
        amount = row.get("total")
        if isinstance(amount, bool) or not isinstance(amount, (int, str, Decimal)):
            continue
        try:
            totals_by_candidate[cid] += abs(Decimal(str(amount)))
        except Exception:
            continue
    by_chamber: dict[str, list[str]] = {"P": [], "S": [], "H": []}
    for cid, _ in sorted(totals_by_candidate.items(), key=lambda item: (-item[1], item[0])):
        by_chamber[cid[0]].append(cid)
    selected: list[str] = []
    # Round-robin across chambers so Presidential, Senate, and competitive House all enter the cohort.
    while len(selected) < limit and any(by_chamber.values()):
        for chamber in ("P", "S", "H"):
            if by_chamber[chamber] and len(selected) < limit:
                selected.append(by_chamber[chamber].pop(0))
    return selected


def _report_money_present(report: Mapping[str, Any]) -> bool:
    for field in _REQUIRED_REPORT_MONEY_FIELDS:
        value = report.get(field)
        if value is None or isinstance(value, bool) or not isinstance(value, (int, str, Decimal)):
            return False
    return True


def _rank_candidate_reports(reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Filter to eligible e-filed effective reports, ordered for broad committee coverage."""
    eligible: list[dict[str, Any]] = []
    seen_committees: set[str] = set()
    for report in reports:
        cid = report.get("committee_id")
        fec_url = report.get("fec_url")
        file_num = report.get("file_number")
        if (
            not isinstance(cid, str)
            or not _COMMITTEE_RE.fullmatch(cid)
            or cid in seen_committees
            or report.get("means_filed") != "e-file"
            or report.get("most_recent") is not True
            or report.get("is_amended") is not False
            or isinstance(file_num, bool)
            or not isinstance(file_num, int)
            or file_num <= 0
            or not isinstance(fec_url, str)
            or not fec_url.startswith("https://docquery.fec.gov/")
            or not fec_url.endswith(".fec")
            or not _report_money_present(report)
        ):
            continue
        try:
            official_url(fec_url)
        except ValueError:
            continue
        seen_committees.add(cid)
        eligible.append(dict(report))

    def _sort_key(item: dict[str, Any]) -> tuple[int, Decimal, str]:
        ind = abs(Decimal(str(item["individual_itemized_contributions_period"])))
        pac = abs(Decimal(str(item["other_political_committee_contributions_period"])))
        has_activity = 1 if (ind > 0 or pac > 0) else 0
        return (-has_activity, -(ind + pac), str(item["committee_id"]))

    eligible.sort(key=_sort_key)
    return eligible


def _distribute_counts(total: int, buckets: Sequence[str]) -> dict[str, int]:
    """Split ``total`` evenly across ``buckets`` (earlier buckets receive any remainder)."""
    if not buckets or total <= 0:
        return {bucket: 0 for bucket in buckets}
    base, rem = divmod(total, len(buckets))
    return {bucket: base + (1 if idx < rem else 0) for idx, bucket in enumerate(buckets)}


def acquire(
    output_dir: Path | str,
    *,
    cycles: Sequence[int] = DEFAULT_CYCLES,
    committee_cycles: Sequence[int] | None = DEFAULT_COMMITTEE_CYCLES,
    max_committee_pages: int = 0,
    reuse_committees_from: Path | None = None,
    outside_candidates_per_cycle: int = 25,
    outside_candidates: Sequence[tuple[str, int]] = (),
    contribution_reports_per_cycle: int = 8,
    contribution_forms: Sequence[str] = DEFAULT_CONTRIBUTION_FORMS,
    contribution_filings: Sequence[tuple[str, str, int, str | None]] = (),
    max_requests: int = 850,
    max_filing_bytes: int = MAX_ORIGINAL_BYTES,
    discovery_pages: int = 2,
    archive_root: Path | None = None,
    api_key: str | None = None,
    min_interval: float = 1.1,
    transport=None,
) -> dict[str, Any]:
    """Acquire bounded multi-cycle FEC originals into ``CentralRawArchiveStore`` and emit a v2 selection.

    OpenFEC API access uses ``api_key`` (or CI environment ``API_GOV`` / ``DATA_GOV_API_KEY``)
    strictly via ``X-Api-Key`` header on ``api.open.fec.gov``. Every admitted outside-spending
    and contribution scope is pre-verified against the exact domain transforms before being
    written to the selection manifest; auto-discovered candidates that fail reconciliation or
    version/form gates are recorded explicitly in ``acquisition_summary``.
    """
    from spicy_docs.sources.fec.client import FecClient

    selected_cycles = _validate_cycles(cycles)
    selected_committee_cycles = (
        () if committee_cycles is None else _validate_cycles(committee_cycles, allow_empty=True)
    )
    if type(max_committee_pages) is not int or not 0 <= max_committee_pages <= MAX_COMMITTEE_PAGES:
        raise ValueError(f"max_committee_pages must be between 0 and {MAX_COMMITTEE_PAGES}")
    if type(outside_candidates_per_cycle) is not int or outside_candidates_per_cycle < 0:
        raise ValueError("outside_candidates_per_cycle must be a non-negative integer")
    if type(contribution_reports_per_cycle) is not int or contribution_reports_per_cycle < 0:
        raise ValueError("contribution_reports_per_cycle must be a non-negative integer")
    if type(max_requests) is not int or max_requests < 1:
        raise ValueError("max_requests must be a positive integer")
    if type(max_filing_bytes) is not int or not 1 <= max_filing_bytes <= MAX_ORIGINAL_BYTES:
        raise ValueError(f"max_filing_bytes must be between 1 and {MAX_ORIGINAL_BYTES}")
    if type(discovery_pages) is not int or not 1 <= discovery_pages <= 20:
        raise ValueError("discovery_pages must be between 1 and 20")
    forms = tuple(dict.fromkeys(contribution_forms))
    if not forms or any(form not in REPORT_ENTITY_FORMS for form in forms):
        raise ValueError(f"contribution_forms must be chosen from {sorted(REPORT_ENTITY_FORMS)}")

    resolved_key = api_key if api_key is not None else _resolve_api_key()
    if not resolved_key:
        raise ValueError(
            "OpenFEC acquisition requires an API key in DATA_GOV_API_KEY or API_GOV "
            "(configured in GitHub Actions secrets; never local .env)"
        )

    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh FEC profile acquisition directory")

    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    evidence.credential = resolved_key

    skipped_outside: list[dict[str, Any]] = []
    skipped_contributions: list[dict[str, Any]] = []
    outside_scopes: list[dict[str, Any]] = []
    contribution_scopes: list[dict[str, Any]] = []
    committee_pages: list[dict[str, Any]] = []
    committee_population = "sampled_pages"
    committee_available_pages: int | None = None
    committee_available_count: int | None = None

    try:
        with (
            TemporaryDirectory(dir=output_dir, prefix="fec-acquire-blobs-") as scratch_dir,
            FecClient(
                store=Path(scratch_dir),
                api_key=resolved_key,
                max_requests=max_requests,
                min_interval=min_interval,
                transport=transport,
            ) as client,
        ):
            scratch_root = Path(scratch_dir)

            def _retain_api_page(page: Mapping[str, Any], *, stage: str) -> dict[str, Any]:
                ev = page["evidence"]
                blob_file = scratch_root / ev["blob_path"]
                evidence.retain_file(
                    blob_file,
                    stage=stage,
                    raw_source=True,
                    source_url=page["request_url"],
                    original_location=page["request_url"],
                    observed_at=page["observed_at"],
                )
                blob_file.unlink(missing_ok=True)
                return {
                    "request_url": page["request_url"],
                    "observed_at": page["observed_at"],
                    "response_sha256": ev["sha256"],
                    "byte_size": ev["bytes"],
                }

            def _fetch_bounded_prefix(
                path: str, params: Mapping[str, Any], *, page_limit: int, stage: str
            ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
                """Fetch at most ``page_limit`` pages without triggering FecClient's terminal bound error."""
                descriptors: list[dict[str, Any]] = []
                rows: list[dict[str, Any]] = []
                last_pagination: dict[str, Any] = {}
                stream = client.api(path, params=params, max_pages=page_limit + 1)
                try:
                    for page in stream:
                        checkpoint()
                        descriptors.append(_retain_api_page(page, stage=stage))
                        last_pagination = page.get("pagination") or {}
                        rows.extend(item["metadata"] for item in page["records"])
                        if len(descriptors) >= page_limit:
                            break
                finally:
                    close = getattr(stream, "close", None)
                    if callable(close):
                        close()
                return descriptors, rows, last_pagination

            def _fetch_complete_query(
                name: str, path: str, params: Mapping[str, Any], *, stage: str, max_pages: int = MAX_AGGREGATE_PAGES
            ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
                pages: list[dict[str, Any]] = []
                rows: list[dict[str, Any]] = []
                for page in client.api(path, params=params, max_pages=max_pages):
                    checkpoint()
                    pages.append(_retain_api_page(page, stage=stage))
                    rows.extend(item["metadata"] for item in page["records"])
                descriptor = {"name": name, "path": path, "params": dict(params), "pages": pages}
                return descriptor, rows

            # Phase 1: Outside-spending scopes (Presidential, Senate, and competitive House across cycles).
            outside_targets: list[tuple[str, int, bool]] = []
            seen_outside_targets: set[tuple[str, int]] = set()
            for cid, cycle in outside_candidates:
                if not _CANDIDATE_RE.fullmatch(cid) or type(cycle) is not int or cycle % 2:
                    raise ValueError(f"Invalid outside_candidates entry: {(cid, cycle)!r}")
                if (cid, cycle) not in seen_outside_targets:
                    seen_outside_targets.add((cid, cycle))
                    outside_targets.append((cid, cycle, True))
            if outside_candidates_per_cycle > 0:
                for cycle in selected_cycles:
                    checkpoint()
                    _, discovered_totals, _ = _fetch_bounded_prefix(
                        "/v1/schedules/schedule_e/totals/by_candidate/",
                        {"cycle": cycle, "sort": "-total", "per_page": 100},
                        page_limit=discovery_pages,
                        stage=f"discover-outside-{cycle}",
                    )
                    for cid in _select_balanced_candidates(discovered_totals, outside_candidates_per_cycle):
                        if (cid, cycle) not in seen_outside_targets:
                            seen_outside_targets.add((cid, cycle))
                            outside_targets.append((cid, cycle, False))

            for cid, cycle, explicit in outside_targets:
                checkpoint()
                try:
                    by_spender_q, by_spender_rows = _fetch_complete_query(
                        "by-spender",
                        "/v1/schedules/schedule_e/by_candidate/",
                        {"candidate_id": cid, "cycle": cycle, "per_page": 100, "sort": "-total"},
                        stage="acquire-outside-by-spender",
                    )
                    totals_q, totals_rows = _fetch_complete_query(
                        "totals",
                        "/v1/schedules/schedule_e/totals/by_candidate/",
                        {"candidate_id": cid, "cycle": cycle, "per_page": 100},
                        stage="acquire-outside-totals",
                    )
                    queries = [by_spender_q, totals_q]
                    qualified = outside_profiles(
                        by_spender_rows,
                        totals_rows,
                        candidate_id=cid,
                        cycle=cycle,
                        evidence=queries,
                    )
                    if not qualified[OUTSIDE_PROFILES]:
                        raise ValueError("Outside-spending scope returned zero reconciled profiles")
                    outside_scopes.append({"candidate_id": cid, "cycle": cycle, "queries": queries})
                except Exception as exc:
                    if explicit:
                        raise
                    skipped_outside.append(
                        {"candidate_id": cid, "cycle": cycle, "reason": local_path_name(str(exc))}
                    )

            # Phase 2: Contribution scopes (Form 3 / 3P / 3X official effective reports).
            admitted_filings: set[str] = set()

            def _qualify_and_admit_report(
                committee_id: str,
                filing_id: str,
                cycle: int,
                *,
                report_year: int | None = None,
                report_type: str | None = None,
                explicit_candidate_id: str | None = None,
                expects_candidate: bool = True,
                explicit: bool = False,
            ) -> bool:
                if filing_id in admitted_filings:
                    return False
                try:
                    report_params: dict[str, Any] = {
                        "is_amended": False,
                        "most_recent": True,
                        "per_page": 100,
                    }
                    if report_year is not None and report_type is not None:
                        report_params["year"] = report_year
                        report_params["report_type"] = report_type
                    else:
                        report_params["cycle"] = cycle
                    reports_q, reports_rows = _fetch_complete_query(
                        "reports",
                        f"/v1/committee/{committee_id}/reports/",
                        report_params,
                        stage="acquire-contribution-reports",
                    )
                    report = _select_matching_report(reports_rows, filing_id)
                    history_q, history_rows = _fetch_complete_query(
                        "committee-history",
                        f"/v1/committee/{committee_id}/history/{cycle}/",
                        {"per_page": 100},
                        stage="acquire-contribution-history",
                    )
                    if len(history_rows) != 1:
                        raise ValueError("Committee history did not return exactly one cycle record")
                    history = history_rows[0]
                    queries = [reports_q, history_q]
                    cand_rows: list[dict[str, Any]] = []
                    chosen_candidate: str | None = explicit_candidate_id
                    if expects_candidate:
                        linked = [c for c in history.get("candidate_ids") or [] if isinstance(c, str) and _CANDIDATE_RE.fullmatch(c)]
                        if chosen_candidate is not None:
                            cand_q, cand_rows = _fetch_complete_query(
                                "candidate",
                                "/v1/candidates/",
                                {"candidate_id": chosen_candidate, "per_page": 100},
                                stage="acquire-contribution-candidate",
                            )
                            queries.append(cand_q)
                        else:
                            if not linked:
                                raise ValueError("Candidate committee history lacks candidate_ids")
                            cand_q, cand_rows = _fetch_complete_query(
                                "candidate",
                                "/v1/candidates/",
                                {"candidate_id": linked, "per_page": 100},
                                stage="acquire-contribution-candidate",
                            )
                            queries.append(cand_q)
                            cov_start = str(report["coverage_start_date"])[:10]
                            matching_cands = [
                                row["candidate_id"]
                                for row in cand_rows
                                if row.get("candidate_id") in linked
                                and (row.get("first_file_date") or "9999") <= cov_start
                                and report["cycle"] in (row.get("cycles") or [])
                            ]
                            if len(matching_cands) != 1:
                                raise ValueError("Could not resolve one unambiguous pre-report candidate")
                            chosen_candidate = matching_cands[0]
                    else:
                        chosen_candidate = None

                    fec_url = report.get("fec_url")
                    if not isinstance(fec_url, str) or not fec_url:
                        raise ValueError("Selected official report lacks fec_url")
                    downloaded = client.download(fec_url, max_bytes=max_filing_bytes)
                    downloaded_at = (downloaded.get("response") or {}).get("observed_at")
                    blob_file = scratch_root / downloaded["blob_path"]
                    evidence.retain_file(
                        blob_file,
                        stage="acquire-contribution-filing",
                        raw_source=True,
                        source_url=fec_url,
                        original_location=fec_url,
                        observed_at=downloaded_at,
                    )
                    blob_file.unlink(missing_ok=True)
                    original_capture = {
                        "response_sha256": downloaded["sha256"],
                        "byte_size": downloaded["bytes"],
                        "request_url": fec_url,
                        "observed_at": downloaded_at,
                        "filing_id": filing_id,
                    }
                    # Pre-qualify against exact domain reconciliation before admitting to selection.
                    mapped_selection = CollectionSelection(
                        "filing-" + filing_id,
                        original_capture["response_sha256"],
                        original_capture["response_sha256"],
                        "official-fec",
                        cycle,
                        "snapshot",
                        original_capture["response_sha256"],
                    )
                    contribution_profiles(
                        iter_filing_rows(evidence, original_capture, retain=False),
                        mapped_selection,
                        report=report,
                        filing_id=filing_id,
                        source_url=fec_url,
                        captured_at=original_capture["observed_at"],
                        candidate_id=chosen_candidate,
                        committee_history=history,
                        candidates=cand_rows,
                        evidence=queries,
                    )
                    contribution_scopes.append(
                        {
                            "original": original_capture,
                            "queries": queries,
                            "cycle": cycle,
                            "candidate_id": chosen_candidate,
                        }
                    )
                    admitted_filings.add(filing_id)
                    return True
                except Exception as exc:
                    if explicit:
                        raise
                    skipped_contributions.append(
                        {
                            "committee_id": committee_id,
                            "filing_id": filing_id,
                            "cycle": cycle,
                            "reason": local_path_name(str(exc)),
                        }
                    )
                    return False

            for committee_id, filing_id, cycle, candidate_id in contribution_filings:
                checkpoint()
                _qualify_and_admit_report(
                    committee_id,
                    filing_id,
                    cycle,
                    explicit_candidate_id=candidate_id,
                    expects_candidate=candidate_id is not None,
                    explicit=True,
                )

            if contribution_reports_per_cycle > 0:
                per_form = _distribute_counts(contribution_reports_per_cycle, forms)
                for cycle in selected_cycles:
                    for entity_type in forms:
                        target = per_form[entity_type]
                        if target <= 0:
                            continue
                        checkpoint()
                        _, discovered_reports, _ = _fetch_bounded_prefix(
                            f"/v1/reports/{entity_type}/",
                            {"cycle": cycle, "is_amended": False, "most_recent": True, "per_page": 100},
                            page_limit=discovery_pages,
                            stage=f"discover-reports-{entity_type}-{cycle}",
                        )
                        candidates = _rank_candidate_reports(discovered_reports)
                        admitted_here = 0
                        attempts = 0
                        max_attempts = max(target * 3, target + 4)
                        expects_candidate = entity_type != "pac-party"
                        for candidate in candidates:
                            if admitted_here >= target or attempts >= max_attempts:
                                break
                            attempts += 1
                            ok = _qualify_and_admit_report(
                                candidate["committee_id"],
                                str(candidate["file_number"]),
                                cycle,
                                report_year=candidate.get("report_year"),
                                report_type=candidate.get("report_type"),
                                expects_candidate=expects_candidate,
                                explicit=False,
                            )
                            if ok:
                                admitted_here += 1

            # Phase 3: Committee relationship profiles (reuse retained pages or walk /v1/committees/).
            if reuse_committees_from is not None:
                committee_pages, committee_population = _load_reused_committee_pages(
                    reuse_committees_from, evidence, max_pages=max_committee_pages
                )
            else:
                committee_params: dict[str, Any] = {"sort": "committee_id", "per_page": 100}
                if selected_committee_cycles:
                    committee_params["cycle"] = list(selected_committee_cycles)
                if max_committee_pages > 0:
                    descriptors: list[dict[str, Any]] = []
                    last_pag: dict[str, Any] = {}
                    stream = client.api(
                        "/v1/committees/", params=committee_params, max_pages=max_committee_pages + 1
                    )
                    try:
                        for page in stream:
                            checkpoint()
                            desc = _retain_api_page(page, stage="acquire-committees")
                            desc["expected_results"] = len(page["records"])
                            descriptors.append(desc)
                            last_pag = page.get("pagination") or {}
                            if len(descriptors) >= max_committee_pages:
                                break
                    finally:
                        close = getattr(stream, "close", None)
                        if callable(close):
                            close()
                    committee_pages = descriptors
                    committee_available_pages = last_pag.get("pages")
                    committee_available_count = last_pag.get("count")
                    if (
                        last_pag.get("is_count_exact") is True
                        and isinstance(committee_available_pages, int)
                        and len(committee_pages) == max(1, committee_available_pages)
                    ):
                        committee_population = "complete_traversal"
                    else:
                        committee_population = "bounded_page_prefix"
                else:
                    descriptors = []
                    last_pag = {}
                    for page in client.api("/v1/committees/", params=committee_params, max_pages=MAX_COMMITTEE_PAGES):
                        checkpoint()
                        desc = _retain_api_page(page, stage="acquire-committees")
                        desc["expected_results"] = len(page["records"])
                        descriptors.append(desc)
                        last_pag = page.get("pagination") or {}
                    committee_pages = descriptors
                    committee_available_pages = last_pag.get("pages")
                    committee_available_count = last_pag.get("count")
                    committee_population = "complete_traversal"

            requests_used = client.http.request_count

        if not committee_pages:
            raise ValueError("Acquisition produced zero committee pages")
        if not outside_scopes:
            raise ValueError("Acquisition produced zero qualified outside-spending scopes")
        if not contribution_scopes:
            raise ValueError("Acquisition produced zero qualified contribution scopes")

        cycles_label = ",".join(str(c) for c in selected_cycles)
        comm_label = ",".join(str(c) for c in selected_committee_cycles) if selected_committee_cycles else "all-cycles"
        selection: dict[str, Any] = {
            "format": SELECTION_FORMAT,
            "version": SELECTION_VERSION,
            "scope": (
                f"Multi-cycle FEC research profiles (cycles {cycles_label}; committees {comm_label} "
                f"[{committee_population}, {len(committee_pages)} pages]; "
                f"{len(outside_scopes)} outside-spending candidate-cycles; "
                f"{len(contribution_scopes)} reconciled Form 3/3P/3X reports)."
            ),
            "cycles": list(selected_cycles),
            "committee_cycles": list(selected_committee_cycles),
            "committee_population": committee_population,
            "committees": committee_pages,
            "outside_scopes": outside_scopes,
            "contribution_scopes": contribution_scopes,
            "acquisition_summary": {
                "requests_used": requests_used,
                "max_requests": max_requests,
                "committee_pages": len(committee_pages),
                "committee_available_pages": committee_available_pages,
                "committee_available_count": committee_available_count,
                "outside_scopes_admitted": len(outside_scopes),
                "outside_scopes_skipped": len(skipped_outside),
                "skipped_outside_scopes": skipped_outside,
                "contribution_scopes_admitted": len(contribution_scopes),
                "contribution_scopes_skipped": len(skipped_contributions),
                "skipped_contribution_scopes": skipped_contributions,
            },
        }
        evidence.event(
            "fec-research-profiles-acquired",
            cycles=list(selected_cycles),
            committee_population=committee_population,
            committee_pages=len(committee_pages),
            outside_scopes=len(outside_scopes),
            contribution_scopes=len(contribution_scopes),
            requests_used=requests_used,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection


def _parse_csv_cycles(raw: str) -> list[int]:
    items = [part.strip() for part in raw.split(",") if part.strip()]
    result: list[int] = []
    for item in items:
        if not re.fullmatch(r"\d{4}", item):
            raise ValueError(f"Invalid cycle value: {item!r}")
        result.append(int(item))
    return result


def _parse_outside_candidate(raw: str) -> tuple[str, int]:
    if ":" not in raw:
        raise ValueError("Outside candidate must use CANDIDATE_ID:CYCLE syntax (e.g. S6OH00163:2024)")
    cid, cycle_text = raw.split(":", 1)
    if not _CANDIDATE_RE.fullmatch(cid) or not re.fullmatch(r"\d{4}", cycle_text):
        raise ValueError(f"Invalid outside candidate specification: {raw!r}")
    return cid, int(cycle_text)


def _parse_contribution_filing(raw: str) -> tuple[str, str, int, str | None]:
    parts = raw.split(":")
    if len(parts) not in (3, 4):
        raise ValueError("Contribution filing must use COMMITTEE_ID:FILING_ID:CYCLE[:CANDIDATE_ID]")
    cid, fid, cycle_text = parts[0], parts[1], parts[2]
    cand = parts[3] if len(parts) == 4 and parts[3] else None
    if not _COMMITTEE_RE.fullmatch(cid) or not _FILING_RE.fullmatch(fid) or not re.fullmatch(r"\d{4}", cycle_text):
        raise ValueError(f"Invalid contribution filing specification: {raw!r}")
    if cand is not None and not _CANDIDATE_RE.fullmatch(cand):
        raise ValueError(f"Invalid candidate ID in contribution filing: {raw!r}")
    return cid, fid, int(cycle_text), cand


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Use `acquire --help` to acquire multi-cycle FEC originals into CentralRawArchiveStore.",
    )
    parser.add_argument("--selection", type=Path, required=True, help="Path to a retained v1 or v2 selection JSON")
    parser.add_argument("--output-dir", type=Path, required=True, help="Fresh directory for build output and generation")
    parser.add_argument("--archive-root", type=Path, help="Optional CentralRawArchiveStore root override")
    parser.add_argument("--read-index", type=Path, help="Optional published index snapshot saved by `spicy-regs mirror`")
    return parser


def _acquire_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="build-fec-research-profiles acquire",
        description="Acquire bounded multi-cycle FEC research profile originals via OpenFEC and docquery.fec.gov.",
    )
    parser.add_argument("--output-dir", type=Path, required=True, help="Fresh directory for acquisition evidence")
    parser.add_argument("--selection-out", type=Path, required=True, help="Path to write the generated v2 selection JSON")
    parser.add_argument("--archive-root", type=Path, help="Optional CentralRawArchiveStore root override")
    parser.add_argument(
        "--cycles",
        default=",".join(str(c) for c in DEFAULT_CYCLES),
        help="Comma-separated two-year cycles for outside spending and contributions (default: 2020,2022,2024,2026)",
    )
    parser.add_argument(
        "--committee-cycles",
        default=",".join(str(c) for c in DEFAULT_COMMITTEE_CYCLES),
        help="Comma-separated cycles for /v1/committees/ census, or 'all' for unfiltered full registry",
    )
    parser.add_argument(
        "--max-committee-pages",
        type=int,
        default=0,
        help="Bounded committee page cap (0 = complete traversal up to 5000 pages)",
    )
    parser.add_argument(
        "--reuse-committees-from",
        type=Path,
        help="Optional prior selection.json or build_fec_committees pages.jsonl to reuse retained committee pages",
    )
    parser.add_argument(
        "--outside-candidates-per-cycle",
        type=int,
        default=25,
        help="Auto-discovered outside-spending candidates per cycle balanced across P/S/H (default: 25)",
    )
    parser.add_argument(
        "--outside-candidate",
        action="append",
        default=[],
        help="Explicit CANDIDATE_ID:CYCLE pair to include (repeatable)",
    )
    parser.add_argument(
        "--contribution-reports-per-cycle",
        type=int,
        default=8,
        help="Auto-discovered reconciled contribution reports per cycle across selected forms (default: 8)",
    )
    parser.add_argument(
        "--contribution-forms",
        default=",".join(DEFAULT_CONTRIBUTION_FORMS),
        help="Comma-separated OpenFEC report entity types: house-senate,presidential,pac-party",
    )
    parser.add_argument(
        "--contribution-filing",
        action="append",
        default=[],
        help="Explicit COMMITTEE_ID:FILING_ID:CYCLE[:CANDIDATE_ID] report to include (repeatable)",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=850,
        help="Bounded HTTP request budget across OpenFEC and docquery downloads (default: 850)",
    )
    parser.add_argument(
        "--max-filing-bytes",
        type=int,
        default=MAX_ORIGINAL_BYTES,
        help="Per-filing byte bound for downloaded .fec originals (default: 128 MiB)",
    )
    parser.add_argument(
        "--discovery-pages",
        type=int,
        default=2,
        help="Discovery pages per cycle/entity endpoint (1-20, default: 2)",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=1.1,
        help="Minimum interval in seconds between requests (default: 1.1s for 60 req/min OpenFEC quota)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if raw_argv and raw_argv[0] == "acquire":
        args = _acquire_parser().parse_args(raw_argv[1:])
        cycles = _parse_csv_cycles(args.cycles)
        comm_raw = args.committee_cycles.strip()
        committee_cycles: Sequence[int] | None = (
            None if comm_raw.lower() in {"", "all"} else _parse_csv_cycles(comm_raw)
        )
        forms = [item.strip() for item in args.contribution_forms.split(",") if item.strip()]
        selection = acquire(
            args.output_dir,
            cycles=cycles,
            committee_cycles=committee_cycles,
            max_committee_pages=args.max_committee_pages,
            reuse_committees_from=args.reuse_committees_from,
            outside_candidates_per_cycle=args.outside_candidates_per_cycle,
            outside_candidates=[_parse_outside_candidate(item) for item in args.outside_candidate],
            contribution_reports_per_cycle=args.contribution_reports_per_cycle,
            contribution_forms=forms,
            contribution_filings=[_parse_contribution_filing(item) for item in args.contribution_filing],
            max_requests=args.max_requests,
            max_filing_bytes=args.max_filing_bytes,
            discovery_pages=args.discovery_pages,
            archive_root=args.archive_root,
            min_interval=args.min_interval,
        )
        args.selection_out.parent.mkdir(parents=True, exist_ok=True)
        args.selection_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(
            json.dumps(
                {
                    "selection": str(args.selection_out),
                    "committee_population": selection["committee_population"],
                    "committee_pages": len(selection["committees"]),
                    "outside_scopes": len(selection["outside_scopes"]),
                    "contribution_scopes": len(selection["contribution_scopes"]),
                    "requests_used": selection["acquisition_summary"]["requests_used"],
                }
            )
        )
        return

    if raw_argv and raw_argv[0] == "build":
        raw_argv = raw_argv[1:]
    args = _build_parser().parse_args(raw_argv)
    artifact, evidence = build(
        json.loads(args.selection.read_text(encoding="utf-8")),
        args.output_dir,
        archive_root=args.archive_root,
        read_index=args.read_index,
    )
    print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "evidence": str(evidence)}))


if __name__ == "__main__":
    main()
