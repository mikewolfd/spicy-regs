"""Build ``federal_register_document_bodies.parquet`` for EO 12866 significant Rules & Proposed Rules.

Declared v1 population: every dated Federal Register document where
``document_type IN ('Rule', 'Proposed Rule') AND significant = true AND publication_date >= since``
(default ``since = 2021-01-01``; lowerable in future waves with zero schema change).

Explicit exclusions:
- ``Notice`` documents
- ``Presidential Document`` documents (Executive Orders are already published offline in
  ``executive_order_observations``)
- non-significant or unflagged Rules & Proposed Rules (``significant IS NOT TRUE``)
- pre-``since`` significant rules

Storage and L1-05 provenance model (mirrors ``build_executive_orders.py``):
- Derived searchable ``body_text`` and canonical L1-05 provenance live in
  ``federal_register_document_bodies.parquet``, keyed by ``(document_number, publication_date)``.
- Immutable raw publisher XML / GovInfo HTML bytes are retained once before parsing in
  ``CaptureEvidence`` (uploaded to R2 ``source-evidence/blobs/sha256/<hex>``), never duplicated
  as raw XML inside Parquet.
- Refused responses and missing canonical locators remain explicit rows
  (``body_status = 'refused' | 'missing_locator'``, ``body_failure_reason`` populated,
  ``body_text = NULL``) and are never silently dropped.
- Prior ``body_status == 'acquired'`` rows are carried forward without re-fetching their bodies,
  and optional ``max_new`` batching emits prior rows plus up to ``max_new`` newly attempted rows
  so row count and Parquet byte size grow monotonically across resumable CI bootstrap batches.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.build_federal_register import FR_EPOCH
from spicy_regs.transforms.table_merge import published_prior

FAMILY = "federal-register-bodies"
TABLE = "federal_register_document_bodies"
OUTPUT = TABLE + ".parquet"

#: Default v1 publication-date floor; parameterized so future expansion lowers it with zero schema change.
DEFAULT_SINCE = date(2021, 1, 1)

#: Declared v1 Federal Register document types included in this body family.
INCLUDED_DOCUMENT_TYPES: tuple[str, ...] = ("Rule", "Proposed Rule")

#: Expected publisher XML root element for each included Federal Register ``document_type``.
EXPECTED_XML_ROOTS: Mapping[str, str] = {
    "Rule": "RULE",
    "Proposed Rule": "PRORULE",
}

#: CaptureEvidence stage name for raw body responses (XML / GovInfo HTML).
BODY_STAGE = "federal-register-body"

#: Polite inter-request interval (seconds) matching ``executive_orders`` acquisition limits.
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.4

#: Parquet key-value metadata key holding the declared population scope and batch completion state.
COVERAGE_KEY = "spicy_regs.federal_register_bodies.coverage"

SCHEMA = pa.schema(
    [
        pa.field("document_number", pa.string(), nullable=False),
        pa.field("publication_date", pa.date32(), nullable=False),
        pa.field("document_type", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=True),
        pa.field("significant", pa.bool_(), nullable=False),
        pa.field("source_record_id", pa.string(), nullable=False),
        pa.field("source_sha256", pa.string(), nullable=True),
        pa.field("body_status", pa.string(), nullable=False),
        pa.field("body_format", pa.string(), nullable=True),
        pa.field("body_url", pa.string(), nullable=True),
        pa.field("body_sha256", pa.string(), nullable=True),
        pa.field("text_sha256", pa.string(), nullable=True),
        pa.field("body_observed_at", pa.string(), nullable=True),
        pa.field("text_derivation", pa.string(), nullable=True),
        pa.field("text_derivation_version", pa.string(), nullable=True),
        pa.field("body_failure_reason", pa.string(), nullable=True),
        pa.field("body_text", pa.string(), nullable=True),
    ]
)

_METADATA_COLUMNS: tuple[str, ...] = (
    "document_number",
    "publication_date",
    "document_type",
    "title",
    "significant",
    "source_record_id",
    "source_sha256",
)

_BODY_COLUMNS: tuple[str, ...] = (
    "body_status",
    "body_format",
    "body_url",
    "body_sha256",
    "text_sha256",
    "body_observed_at",
    "text_derivation",
    "text_derivation_version",
    "body_failure_reason",
    "body_text",
)


def _json_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _coerce_since(value: date | str) -> date:
    if type(value) is date:
        parsed = value
    elif isinstance(value, str):
        raw = value.strip()
        try:
            parsed = date.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"since must be YYYY-MM-DD, got {value!r}") from exc
        if parsed.isoformat() != raw:
            raise ValueError(f"since must be YYYY-MM-DD, got {value!r}")
    else:
        raise ValueError(f"since must be a date or YYYY-MM-DD string, got {value!r}")
    if parsed < FR_EPOCH:
        raise ValueError(f"since must be on or after {FR_EPOCH.isoformat()}, got {parsed.isoformat()}")
    return parsed


def _validate_max_new(max_new: int | None) -> int | None:
    if max_new is None:
        return None
    if isinstance(max_new, bool) or not isinstance(max_new, int) or max_new < 1:
        raise ValueError(f"max_new must be a positive integer or None, got {max_new!r}")
    return max_new


def _validate_interval(seconds: float) -> float:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ValueError(f"min_request_interval_seconds must be a non-negative finite number, got {seconds!r}")
    value = float(seconds)
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"min_request_interval_seconds must be a non-negative finite number, got {seconds!r}")
    return value


def _parse_canonical_date(value: Any, *, label: str = "publication_date") -> date:
    if type(value) is date:
        return value
    if isinstance(value, str):
        raw = value.strip()
        try:
            parsed = date.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{label} must be canonical YYYY-MM-DD, got {value!r}") from exc
        if parsed.isoformat() != raw:
            raise ValueError(f"{label} must be canonical YYYY-MM-DD, got {value!r}")
        return parsed
    raise ValueError(f"{label} must be canonical YYYY-MM-DD, got {value!r}")


def _metadata_pin(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Deterministic metadata pin used to detect whether a carried row's metadata columns changed."""
    return tuple(row.get(col) for col in _METADATA_COLUMNS)


def _empty_body_values(*, status: str, failure_reason: str | None) -> dict[str, Any]:
    return {
        "body_status": status,
        "body_format": None,
        "body_url": None,
        "body_sha256": None,
        "text_sha256": None,
        "body_observed_at": None,
        "text_derivation": None,
        "text_derivation_version": None,
        "body_failure_reason": failure_reason,
        "body_text": None,
    }


def _file_source_pin(fr_path: Path) -> dict[str, str]:
    with fr_path.open("rb") as stream:
        digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "logicalId": "urn:spicy-regs:family:federal-register",
        "artifactDigest": digest,
    }


def _load_candidates(fr_path: Path, *, since: date) -> list[dict[str, Any]]:
    """Select the declared v1 population from ``federal_register.parquet`` in newest-first order."""
    import duckdb
    from spicy_docs.sources.federal_register.native import federal_register_source_record_id

    if not fr_path.exists():
        raise FileNotFoundError(f"federal_register.parquet is required to select body population: {fr_path}")

    quoted = str(fr_path).replace("'", "''")
    with duckdb.connect() as con:
        available = {
            str(row[0]).lower(): str(row[0])
            for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{quoted}')").fetchall()
        }
        required = ("document_number", "publication_date", "document_type", "significant")
        missing = [col for col in required if col not in available]
        if missing:
            raise ValueError(f"federal_register.parquet lacks required columns: {missing}")

        def col_expr(name: str) -> str:
            actual = available.get(name)
            return f'"{actual.replace(chr(34), chr(34) * 2)}"' if actual is not None else "NULL"

        type_placeholders = ", ".join("?" for _ in INCLUDED_DOCUMENT_TYPES)
        rows = con.execute(
            f"""
            SELECT
                {col_expr("document_number")} AS document_number,
                {col_expr("publication_date")} AS publication_date,
                {col_expr("document_type")} AS document_type,
                {col_expr("title")} AS title,
                {col_expr("significant")} AS significant,
                {col_expr("source_sha256")} AS source_sha256,
                {col_expr("body_html_url")} AS body_html_url
            FROM read_parquet('{quoted}')
            WHERE {col_expr("document_type")} IN ({type_placeholders})
              AND lower(trim(COALESCE(CAST({col_expr("significant")} AS VARCHAR), ''))) IN ('true', '1')
              AND TRY_CAST({col_expr("publication_date")} AS DATE) >= CAST(? AS DATE)
            ORDER BY TRY_CAST({col_expr("publication_date")} AS DATE) DESC, {col_expr("document_number")} ASC
            """,
            [*INCLUDED_DOCUMENT_TYPES, since.isoformat()],
        ).fetchall()

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, date]] = set()
    for doc_number, pub_raw, doc_type, title, _significant, source_sha256, body_html_url in rows:
        if not isinstance(doc_number, str) or not doc_number.strip():
            raise ValueError(f"Federal Register candidate lacks document_number: {doc_number!r}")
        pub_date = _parse_canonical_date(pub_raw)
        key = (doc_number, pub_date)
        if key in seen:
            raise ValueError(f"federal_register.parquet repeats dated key: {key!r}")
        seen.add(key)
        pub_iso = pub_date.isoformat()
        source_record_id = federal_register_source_record_id(
            {"document_number": doc_number, "publication_date": pub_iso}
        )
        candidates.append(
            {
                "document_number": doc_number,
                "publication_date": pub_date,
                "publication_date_iso": pub_iso,
                "document_type": str(doc_type),
                "title": str(title) if title is not None else None,
                "significant": True,
                "source_record_id": source_record_id,
                "source_sha256": str(source_sha256) if source_sha256 is not None else None,
                "body_html_url": str(body_html_url) if body_html_url is not None else None,
            }
        )
    return candidates


def _load_prior(prior_path: Path | None) -> dict[tuple[str, date], dict[str, Any]]:
    """Load prior published body rows keyed by ``(document_number, publication_date)``."""
    if prior_path is None or not prior_path.exists():
        return {}
    rows = pq.read_table(prior_path).to_pylist()
    held: dict[tuple[str, date], dict[str, Any]] = {}
    for raw in rows:
        doc_number = raw.get("document_number")
        if not isinstance(doc_number, str) or not doc_number.strip():
            raise ValueError("Published federal_register_document_bodies row lacks document_number")
        pub_date = _parse_canonical_date(raw.get("publication_date"))
        key = (doc_number, pub_date)
        if key in held:
            raise ValueError(f"Published federal_register_document_bodies repeats dated key: {key!r}")
        row = {field.name: raw.get(field.name) for field in SCHEMA}
        row["publication_date"] = pub_date
        held[key] = row
    return held


def _has_valid_locator(candidate: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Validate canonical body locators via SpicyDocs without guessing unlocated paths."""
    from spicy_docs.sources.federal_register.body_sources import (
        FederalRegisterBodySourceError,
        body_source_locators,
    )

    record = {
        "document_number": candidate["document_number"],
        "publication_date": candidate["publication_date_iso"],
        "body_html_url": candidate.get("body_html_url"),
    }
    try:
        body_source_locators(record)
    except FederalRegisterBodySourceError as exc:
        return False, str(exc)
    return True, None


def _extract_acquired_body(result: Any, *, document_type: str) -> dict[str, Any]:
    """Derive searchable text and L1-05 provenance from a validated SpicyDocs body acquisition."""
    from spicy_docs.extraction.body_text import body_text
    from spicy_docs.extraction.derivation import BODY_TEXT_DERIVATION_VERSION

    acquisition = result.acquisition
    if acquisition is None:
        return _empty_body_values(status="refused", failure_reason="acquisition missing from acquired result")

    capture = acquisition.body
    if acquisition.format == "xml":
        expected_root = EXPECTED_XML_ROOTS.get(document_type)
        actual_root = getattr(acquisition.identity, "document_type", None)
        if expected_root is not None and actual_root != expected_root:
            return _empty_body_values(
                status="refused",
                failure_reason=(
                    f"Publisher XML root {actual_root!r} differs from declared document_type {document_type!r}"
                ),
            )

    media_type = (capture.content_type or "").split(";", 1)[0].strip().lower()
    rendition = (
        "htm"
        if acquisition.format == "html" or (acquisition.format == "txt" and media_type == "text/html")
        else acquisition.format
    )
    try:
        derived = body_text(
            SimpleNamespace(
                format=rendition,
                body=SimpleNamespace(media_type=media_type or None, byte_size=capture.byte_size),
                body_capture=capture,
            )
        )
    except ValueError as exc:
        return _empty_body_values(status="refused", failure_reason=f"body_text derivation failed: {exc}")

    if not derived.text.strip():
        return _empty_body_values(
            status="refused",
            failure_reason="Validated rendition produced no searchable text",
        )

    return {
        "body_status": "acquired",
        "body_format": acquisition.format,
        "body_url": capture.resolved_url,
        "body_sha256": capture.sha256,
        "text_sha256": "sha256:" + hashlib.sha256(derived.text.encode("utf-8")).hexdigest(),
        "body_observed_at": capture.observed_at,
        "text_derivation": derived.derivation,
        "text_derivation_version": BODY_TEXT_DERIVATION_VERSION,
        "body_failure_reason": None,
        "body_text": derived.text,
    }


def _failure_reason_from_manifest(manifest: Mapping[str, Any]) -> str:
    failure = manifest.get("failure")
    if isinstance(failure, Mapping):
        reason = str(failure.get("reason") or "").strip()
        error_type = str(failure.get("type") or "").strip()
        if error_type and reason:
            return f"{error_type}: {reason}"
        if reason:
            return reason
        if error_type:
            return error_type
    return "source_failed"


def _is_carried_acquired(prior_row: Mapping[str, Any]) -> bool:
    return (
        prior_row.get("body_status") == "acquired"
        and isinstance(prior_row.get("body_text"), str)
        and bool(prior_row["body_text"].strip())
        and isinstance(prior_row.get("body_sha256"), str)
        and isinstance(prior_row.get("text_sha256"), str)
    )


def build_federal_register_bodies(
    output_dir: Path,
    *,
    fr_path: Path | None = None,
    since: date | str = DEFAULT_SINCE,
    max_new: int | None = None,
    retry_failed: bool = False,
    min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
    prior_path: Path | None = None,
    download_prior: Callable[[str, Path], bool] | None = None,
    source_pin: Mapping[str, str] | None = None,
    transport: Any = None,
    evidence: Any = None,
    raise_if_unchanged: bool = False,
) -> Path:
    """Build or incrementally extend ``federal_register_document_bodies.parquet``.

    Prior ``body_status == 'acquired'`` rows are carried forward without network calls; metadata
    updates (such as backfilled ``source_sha256``) refresh in place while preserving body digests
    and observation timestamps. Unattempted candidates beyond ``max_new`` are deferred to the next
    batch so published row counts and Parquet sizes grow monotonically.
    """
    from spicy_docs.reading.refusals import attach_refused_response
    from spicy_docs.sources.federal_register.body_acquisition import (
        FederalRegisterBodyAcquirer,
        FederalRegisterBodyBudget,
    )
    from spicy_docs.sources.federal_register.body_captures import capture_body
    from spicy_regs.pipelines.rollups.base import Unchanged
    from spicy_regs.source_evidence import without_local_paths

    since_date = _coerce_since(since)
    batch_limit = _validate_max_new(max_new)
    interval = _validate_interval(min_request_interval_seconds)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    resolved_fr_path = Path(fr_path) if fr_path is not None else output_dir / "federal_register.parquet"
    out_file = output_dir / OUTPUT

    prior_scratch = output_dir / "_fr_bodies_prior.parquet"
    downloaded_prior = False
    resolved_prior: Path | None = None
    if prior_path is not None:
        resolved_prior = Path(prior_path)
    else:
        prior_downloader = published_prior if download_prior is None else download_prior
        downloaded_prior = bool(prior_downloader(OUTPUT, prior_scratch))
        if downloaded_prior and prior_scratch.exists():
            resolved_prior = prior_scratch
        elif out_file.exists():
            resolved_prior = out_file

    try:
        prior_by_key = _load_prior(resolved_prior)
        have_prior = bool(prior_by_key) or (resolved_prior is not None and resolved_prior.exists())
        candidates = _load_candidates(resolved_fr_path, since=since_date)
        candidate_keys = {(c["document_number"], c["publication_date"]) for c in candidates}

        carried_rows: list[dict[str, Any]] = []
        to_acquire: list[dict[str, Any]] = []
        metadata_updated = 0

        for candidate in candidates:
            key = (candidate["document_number"], candidate["publication_date"])
            held = prior_by_key.get(key)
            meta = {col: candidate[col] for col in _METADATA_COLUMNS}

            if held is not None and _is_carried_acquired(held):
                row = {**meta, **{col: held[col] for col in _BODY_COLUMNS}}
                if _metadata_pin(row) != _metadata_pin(held):
                    metadata_updated += 1
                carried_rows.append(row)
                continue

            if held is not None and held.get("body_status") == "missing_locator":
                valid_now, _ = _has_valid_locator(candidate)
                if not valid_now:
                    row = {**meta, **{col: held[col] for col in _BODY_COLUMNS}}
                    if _metadata_pin(row) != _metadata_pin(held):
                        metadata_updated += 1
                    carried_rows.append(row)
                    continue

            if held is not None and held.get("body_status") == "refused" and not retry_failed:
                row = {**meta, **{col: held[col] for col in _BODY_COLUMNS}}
                if _metadata_pin(row) != _metadata_pin(held):
                    metadata_updated += 1
                carried_rows.append(row)
                continue

            to_acquire.append(candidate)

        # Preserve prior rows outside the current candidate filter so a narrower dispatch never shrinks the table.
        preserved_outside = 0
        for key, held in prior_by_key.items():
            if key not in candidate_keys:
                carried_rows.append(dict(held))
                preserved_outside += 1

        attempt = to_acquire[:batch_limit] if batch_limit is not None else to_acquire
        deferred_count = max(0, len(to_acquire) - len(attempt))

        if raise_if_unchanged and have_prior and not attempt and metadata_updated == 0:
            raise Unchanged(
                f"all {len(prior_by_key)} federal_register_document_bodies rows are current "
                f"(since={since_date.isoformat()})"
            )

        acquired_new = 0
        refused_new = 0
        missing_locator_new = 0
        attempted_rows: list[dict[str, Any]] = []

        if attempt:
            pin = dict(source_pin) if source_pin is not None else _file_source_pin(resolved_fr_path)
            budget = FederalRegisterBodyBudget(
                max_requests=4,
                max_body_bytes=8 * 1024 * 1024,
                max_mods_bytes=16 * 1024 * 1024,
                timeout_seconds=30.0,
                min_request_interval_seconds=interval,
            )
            active_transport = (
                evidence.transport(
                    transport,
                    stage=BODY_STAGE,
                    max_bytes=max(budget.max_body_bytes, budget.max_mods_bytes),
                )
                if evidence is not None
                else transport
            )
            with (
                TemporaryDirectory(dir=output_dir, prefix=".fr-bodies-captures-") as scratch_dir,
                FederalRegisterBodyAcquirer(budget=budget, transport=active_transport) as acquirer,
            ):
                scratch_root = Path(scratch_dir)
                for candidate in attempt:
                    meta = {col: candidate[col] for col in _METADATA_COLUMNS}
                    identity = candidate["source_record_id"]
                    valid_locator, locator_error = _has_valid_locator(candidate)
                    if not valid_locator:
                        missing_locator_new += 1
                        if evidence is not None:
                            evidence.event(
                                "federal-register-body-missing-locator",
                                source_record_id=identity,
                                document_number=candidate["document_number"],
                                publication_date=candidate["publication_date_iso"],
                                reason=locator_error,
                            )
                        attempted_rows.append(
                            {**meta, **_empty_body_values(status="missing_locator", failure_reason=locator_error)}
                        )
                        continue

                    record = {
                        "document_number": candidate["document_number"],
                        "publication_date": candidate["publication_date_iso"],
                        "body_html_url": candidate["body_html_url"],
                    }
                    result = capture_body(
                        scratch_root / identity / "result.json",
                        record=record,
                        source_pin=pin,
                        acquirer=acquirer,
                        format="prefer-xml",
                    )
                    if evidence is not None:
                        sanitized_manifest = without_local_paths(result.manifest)
                        evidence.retain_bytes(
                            _json_value(sanitized_manifest).encode("utf-8"),
                            stage="federal-register-body-result",
                            source_record_id=identity,
                        )

                    if result.outcome == "acquired":
                        body_values = _extract_acquired_body(result, document_type=candidate["document_type"])
                        if body_values["body_status"] == "acquired":
                            acquired_new += 1
                        else:
                            refused_new += 1
                        attempted_rows.append({**meta, **body_values})
                    else:
                        refused_new += 1
                        if evidence is not None and result.refused_response is not None:
                            refusal_error = ValueError("Retained federal-register body acquisition failed")
                            attach_refused_response(refusal_error, result.refused_response)
                            evidence.refusal(refusal_error, stage=BODY_STAGE)
                        reason = _failure_reason_from_manifest(result.manifest)
                        attempted_rows.append(
                            {**meta, **_empty_body_values(status="refused", failure_reason=reason)}
                        )

        all_rows = carried_rows + attempted_rows
        final_keys = {(row["document_number"], row["publication_date"]) for row in all_rows}
        if len(final_keys) != len(all_rows):
            raise ValueError("federal_register_document_bodies merge produced duplicate (document_number, publication_date)")

        all_rows.sort(key=lambda row: row["document_number"])
        all_rows.sort(key=lambda row: row["publication_date"], reverse=True)

        coverage = _json_value(
            {
                "since": since_date.isoformat(),
                "documentTypes": list(INCLUDED_DOCUMENT_TYPES),
                "significantOnly": True,
                "declaredPopulationCount": len(candidates),
                "publishedRowCount": len(all_rows),
                "deferredCount": deferred_count,
                "maxNew": batch_limit,
                "complete": deferred_count == 0,
            }
        )
        written = write_rows(all_rows, out_file, SCHEMA.with_metadata({COVERAGE_KEY: coverage}), batch_size=64)

        summary = {
            "since": since_date.isoformat(),
            "declared_population": len(candidates),
            "prior_rows": len(prior_by_key),
            "carried_rows": len(carried_rows),
            "preserved_outside_window": preserved_outside,
            "metadata_updated": metadata_updated,
            "attempted_rows": len(attempt),
            "acquired_new": acquired_new,
            "refused_new": refused_new,
            "missing_locator_new": missing_locator_new,
            "deferred_rows": deferred_count,
            "output_rows": len(all_rows),
            "max_new": batch_limit,
        }
        if evidence is not None:
            evidence.event("federal-register-bodies-build", **summary)
        logger.info(
            "FR bodies: {output_rows:,} rows ({carried_rows:,} carried, {acquired_new:,} acquired, "
            "{refused_new:,} refused, {missing_locator_new:,} missing_locator, {deferred_rows:,} deferred)",
            **summary,
        )
        return written
    finally:
        if downloaded_prior:
            prior_scratch.unlink(missing_ok=True)
