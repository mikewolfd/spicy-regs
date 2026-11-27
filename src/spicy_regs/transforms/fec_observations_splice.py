"""Receipts-free streaming splice for retained ``fec-observations`` (ss-42zk / ss-42nr).

Rather than cold-replaying manifest ``5c0e5639`` (whose 135 legacy ``release_path``
directories no longer exist on disk) or re-parsing 80.4M unchanged rows for a
3-collection encoding fix, this module:

1. Verifies the pinned manifest and corrects ONLY the explicitly declared
   cp1252 positional filings after confirming ``spicy_docs.sources.fec.filings.filing_encoding``
   decides ``"cp1252"`` on the raw bytes from ``CentralRawArchiveStore(create=False)``.
2. Re-parses ONLY those target collections inside ``verified_archive_session()``
   using ``held_original()`` (single read + single hash per archive).
3. Streams verified prior ``fec-observations`` Parquets (``4cb0dffa`` subject
   schema or full ``RECORD_SCHEMA``) in bounded 4,096-row Arrow batches, passing
   untouched batches through without Python dict materialization and splicing
   only the declared target collections.
4. Proves bidirectional ``EXCEPT ALL`` parity on every untouched collection across
   all three tables and passes ``refuse_c1_controls()`` with zero matches
   (including JSON-escaped ``\\u0080``–``\\u009f``).
5. Seals a receipts-free ``fec-observations`` generation via
   ``generations.build_generation(..., receipt_path=None, owned_output_root=build)``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import resource
import shutil
import sys
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spicy_regs.generations import build_generation
from spicy_regs.transforms.build_fec_observations import (
    COLLECTION_SCHEMA,
    OUTPUTS,
    RECORD_SCHEMA,
    _named_fields,
    _original,
    _profile,
    _relationships,
    _shape,
)
from spicy_regs.transforms.fec_identity_context_fields import normalize_record
from spicy_regs.transforms.fec_identity_receipts import dataset_policy
from spicy_regs.transforms.fec_native_subjects import refuse_c1_controls
from spicy_regs.transforms.fec_query import _digest, _json
from spicy_regs.transforms.fec_relationships import SCHEMA as RELATIONSHIP_SCHEMA

SOURCE_MANIFEST_SHA256 = "sha256:5c0e5639738cc897a1d7fb6066f80fec78ab5e4a7386095d8a38bc06317c180e"
CP1252_SPLICE_COLLECTION_IDS: tuple[str, ...] = (
    "retained-filing-archive-e5604236d54b-0090",  # 1981885.fec in 20260608.zip
    "retained-filing-2178afb37e366d54",  # 1998706.fec
    "retained-filing-b6e6dffafef1febe",  # 1998705.fec
)
# Two DOJ press-release disposition caller contexts in 4cb0dffa fec_collections
# carry literal cp1252 U+0092 (0x92) apostrophes ("Judge Bell\x92s"). They are
# NEVER silently rewritten; callers may opt into explicit provenance-bound repair
# via ``collection_context_repairs`` or ``build_doj_c1_context_repairs()``.
KNOWN_DOJ_C1_CONTEXT_COLLECTIONS: dict[str, str] = {
    "research-capture-27549c221ab03f7ccfbd88ce-0000": (
        "sha256:1f8c5a8dcd41f1e22f495fa14b765059d6d4a59fc9ef97274e79700c53ffd71a"
    ),
    "research-capture-d046d177f3390f09ab4b6ed1-0000": (
        "sha256:5d930cfe400274d0dd43007fab78f657b43cf4a6cac15d08a4687d45f9a5db01"
    ),
}
SPLICE_BATCH_ROWS = 4096
DEFAULT_DUCKDB_MEMORY_LIMIT = "2GiB"
DEFAULT_DUCKDB_TEMP_LIMIT = "2GiB"


@dataclass(frozen=True)
class CollectionContextRepair:
    """Explicit, provenance-bound repair for a disposition caller context containing C1 controls."""

    collection_id: str
    prior_context_sha256: str
    replacement_text: str | None = None
    cp1252_c1_decode: bool = True
    reason: str = "Repair literal cp1252 C1 punctuation in retained callerContext while preserving original pin."


@dataclass(frozen=True)
class ManifestCorrectionResult:
    """Pinned diff between the source manifest and the cp1252-corrected manifest."""

    source_manifest_sha256: str
    corrected_manifest_sha256: str
    corrected_manifest_bytes: bytes
    collection_diffs: tuple[dict[str, Any], ...]
    corrected_collections: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class SpliceTelemetry:
    """Measured runtime and resource envelope for one receipts-free observations splice."""

    wall_seconds: float
    peak_rss_bytes: int
    peak_disk_bytes: int
    prior_rows: dict[str, int]
    spliced_rows: dict[str, int]
    changed_collections: tuple[str, ...]
    untouched_collections_count: int
    zip_spool_opens: int
    c1_control_matches: int


@dataclass(frozen=True)
class ObservationSpliceResult:
    """Sealed receipts-free ``fec-observations`` candidate plus parity proof and telemetry."""

    generation_dir: Path
    artifact_digest: str
    logical_id: str
    manifest_diff: dict[str, Any]
    parity: dict[str, Any]
    telemetry: SpliceTelemetry
    receipt_path: Path
    corrected_manifest_path: Path


def _peak_rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux reports KiB.
    return int(usage if sys.platform == "darwin" else usage * 1024)


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink())


def _sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _open_store_digest(store: Any, sha256_value: str) -> bytes:
    """Read raw bytes from ``CentralRawArchiveStore`` or any compatible blob store."""
    digest = _digest(sha256_value)
    bare = digest.removeprefix("sha256:")
    try:
        ctx = store.open(bare)
    except (FileNotFoundError, KeyError, ValueError):
        ctx = store.open(digest)
    with ctx as stream:
        raw = stream.read()
    if _sha256_bytes(raw) != digest:
        raise ValueError(f"Raw archive bytes differ from pinned digest: {digest}")
    return raw


def _duckdb_config(
    memory_limit: str, temp_limit: str, threads: int, temp_dir: Path
) -> dict[str, str | bool | int | float | list[str]]:
    return {
        "threads": threads,
        "memory_limit": memory_limit,
        "max_temp_directory_size": temp_limit,
        "temp_directory": str(temp_dir),
    }


def _pc_call(name: str, *args: Any, **kwargs: Any) -> Any:
    return getattr(pc, name)(*args, **kwargs)


def _sql_in_list(values: Sequence[str]) -> str:
    escaped = ["'" + v.replace("'", "''") + "'" for v in values]
    return "(" + ", ".join(escaped) + ")"


def build_doj_c1_context_repairs() -> tuple[CollectionContextRepair, ...]:
    """Explicit repairs for the 2 known DOJ press-release C1 apostrophe rows in live ``4cb0dffa``."""
    return tuple(
        CollectionContextRepair(
            collection_id=cid,
            prior_context_sha256=sha,
            cp1252_c1_decode=True,
            reason=(
                "Replace literal U+0092 (cp1252 right single quotation mark misdecoded as latin-1) "
                "in retained third-party DOJ press-release callerContext teaser while preserving pin.sha256."
            ),
        )
        for cid, sha in sorted(KNOWN_DOJ_C1_CONTEXT_COLLECTIONS.items())
    )


def _repair_c1_in_json_value(value: Any) -> tuple[Any, int]:
    """Decode literal U+0080..U+009F cp1252 codepoints inside nested JSON strings."""
    if isinstance(value, str):
        count = 0
        out_chars: list[str] = []
        for ch in value:
            code = ord(ch)
            if 0x80 <= code <= 0x9F:
                decoded = bytes([code]).decode("cp1252")
                out_chars.append(decoded)
                count += 1
            else:
                out_chars.append(ch)
        return "".join(out_chars), count
    if isinstance(value, list):
        total = 0
        out_list = []
        for item in value:
            fixed, n = _repair_c1_in_json_value(item)
            out_list.append(fixed)
            total += n
        return out_list, total
    if isinstance(value, dict):
        total = 0
        out_dict = {}
        for k, v in value.items():
            fixed_k, nk = _repair_c1_in_json_value(k)
            fixed_v, nv = _repair_c1_in_json_value(v)
            out_dict[fixed_k] = fixed_v
            total += nk + nv
        return out_dict, total
    return value, 0


def correct_manifest_encodings(
    manifest_bytes: bytes,
    *,
    store: Any,
    target_collection_ids: Sequence[str] = CP1252_SPLICE_COLLECTION_IDS,
    expected_manifest_sha256: str | None = None,
) -> ManifestCorrectionResult:
    """Verify target positional filings against raw bytes and switch only their encoding to ``cp1252``.

    Every target collection must exist in the manifest with ``profile == "positional"``
    and ``scope.format == "fec"``, and ``spicy_docs.sources.fec.filings.filing_encoding``
    must decide ``"cp1252"`` on the verified bytes read from ``store``.
    """
    from spicy_docs.sources.fec.delimited import held_original, selected_stream, verified_archive_session
    from spicy_docs.sources.fec.filings import filing_encoding
    from spicy_docs.sources.fec.row_profile import FEC_POSITIONAL_ROWS_PROFILE

    source_sha = _sha256_bytes(manifest_bytes)
    if expected_manifest_sha256 is not None and _digest(expected_manifest_sha256) != source_sha:
        raise ValueError(
            f"Source manifest digest differs from expected pin: expected {expected_manifest_sha256}, got {source_sha}"
        )
    parsed = json.loads(manifest_bytes)
    if (
        not isinstance(parsed, dict)
        or set(parsed) != {"version", "collections"}
        or type(parsed["version"]) is not int
        or parsed["version"] != 1
        or not isinstance(parsed["collections"], list)
        or not parsed["collections"]
    ):
        raise ValueError("Expected version-1 FEC manifest with nonempty collections")

    target_order = tuple(dict.fromkeys(target_collection_ids))
    if not target_order:
        raise ValueError("At least one target collection_id is required for cp1252 splice")

    corrected_manifest = deepcopy(parsed)
    by_cid: dict[str, dict[str, Any]] = {}
    for item in corrected_manifest["collections"]:
        cid = item.get("collection_id")
        if not isinstance(cid, str) or not cid or cid in by_cid:
            raise ValueError(f"Duplicate or invalid collection_id in manifest: {cid!r}")
        by_cid[cid] = item

    missing = [cid for cid in target_order if cid not in by_cid]
    if missing:
        raise ValueError(f"Target splice collections absent from manifest: {missing}")

    diffs: list[dict[str, Any]] = []
    corrected_items: list[dict[str, Any]] = []

    # Sort target checks by outer capture sha256 so contiguous ZIP members reuse one spool.
    check_order = sorted(
        target_order,
        key=lambda cid: (
            str((by_cid[cid].get("scope") or {}).get("capture", {}).get("responseSha256", "")),
            cid,
        ),
    )
    diff_by_cid: dict[str, dict[str, Any]] = {}
    with verified_archive_session():
        for cid in check_order:
            item = by_cid[cid]
            if item.get("profile") != "positional" or "scope" not in item:
                raise ValueError(f"Target splice collection must be scope-mode positional: {cid}")
            scope = FEC_POSITIONAL_ROWS_PROFILE.validate_query_scope(item["scope"])
            if scope["format"] != "fec":
                raise ValueError(f"Target cp1252 splice collection must use format='fec': {cid}")
            capture = scope["capture"]
            original = held_original(store, capture["responseSha256"], capture["byteSize"])
            with selected_stream(
                original,
                capture=capture,
                member=scope["member"],
                max_members=scope["max_members"],
                max_decoded_bytes=scope["max_decoded_bytes"],
            ) as (decoded, _member, member_sha):
                decided = filing_encoding(decoded)
            if decided != "cp1252":
                raise ValueError(
                    f"Raw bytes for {cid} decided {decided!r}, expected 'cp1252'; refusing encoding override"
                )
            prior_encoding = scope["encoding"]
            item["scope"] = {**scope, "encoding": "cp1252"}
            FEC_POSITIONAL_ROWS_PROFILE.validate_query_scope(item["scope"])
            diff_by_cid[cid] = {
                "collection_id": cid,
                "prior_encoding": prior_encoding,
                "new_encoding": "cp1252",
                "encoding_basis": (
                    "spicy_docs.sources.fec.filings.filing_encoding: strict UTF-8 refused, strict cp1252 verified"
                ),
                "source_sha256": capture["responseSha256"],
                "decoded_member_sha256": member_sha,
                "member": scope["member"],
                "byte_size": capture["byteSize"],
                "request_url": capture["requestUrl"],
            }

    for cid in target_order:
        diffs.append(diff_by_cid[cid])
        corrected_items.append(deepcopy(by_cid[cid]))

    corrected_bytes = (_json(corrected_manifest) + "\n").encode("utf-8")
    corrected_sha = _sha256_bytes(corrected_bytes)
    return ManifestCorrectionResult(
        source_manifest_sha256=source_sha,
        corrected_manifest_sha256=corrected_sha,
        corrected_manifest_bytes=corrected_bytes,
        collection_diffs=tuple(diffs),
        corrected_collections=tuple(corrected_items),
    )


def reparse_spliced_collections(
    corrected_collections: Sequence[dict[str, Any]],
    *,
    store: Any,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], int]:
    """Re-parse ONLY the declared corrected positional collections via ``verified_archive_session()``.

    Returns ``(records_by_cid, collection_row_by_cid, relationships_by_cid, unique_archive_opens)``.
    """
    from spicy_docs.sources.fec.delimited import verified_archive_session

    records_by_cid: dict[str, list[dict[str, Any]]] = {}
    collection_row_by_cid: dict[str, dict[str, Any]] = {}
    relationships_by_cid: dict[str, list[dict[str, Any]]] = {}
    opened_archives: set[tuple[str, int]] = set()
    headers: dict[str, Any] = {}

    # Order by outer archive digest so members of one ZIP reuse a single held spool.
    ordered = sorted(
        corrected_collections,
        key=lambda item: (
            str((item.get("scope") or {}).get("capture", {}).get("responseSha256", "")),
            item["collection_id"],
        ),
    )
    with verified_archive_session():
        for item in ordered:
            cid = item["collection_id"]
            if item.get("profile") != "positional" or "scope" not in item:
                raise ValueError(f"Spliced reparse supports scope-mode positional collections only: {cid}")
            capture = item["scope"]["capture"]
            opened_archives.add((capture["responseSha256"], capture["byteSize"]))
            profile, _ = _profile(item["profile"])
            records_iter, outcome = _original(item, store, profile)
            rec_rows: list[dict[str, Any]] = []
            rel_rows: list[dict[str, Any]] = []
            version = None
            for wrapped in records_iter:
                row, locator = _shape(item, wrapped, outcome)
                native = wrapped["record"]["record"]
                if native.get("kind") == "header":
                    version = native["format_version"]
                mapped = _named_fields(item, wrapped, row, locator, headers, None)
                rec_rows.append(row)
                for relationship in _relationships(item, wrapped, row, locator, version, mapped):
                    rel_rows.append(relationship)
            if len(rec_rows) != outcome["publishedRecordCount"]:
                raise ValueError(f"Reparsed row count differs from outcome for {cid}")
            collection_row_by_cid[cid] = {
                "collection_id": cid,
                "source_family": item["source_family"],
                "profile": item["profile"],
                "source_system_id": profile.source_system_id,
                "source_state_scope": outcome["sourceStateScope"],
                "record_count": len(rec_rows),
                "relationship_count": len(rel_rows),
                "record_outcome": outcome["recordOutcome"],
                "requested_scope_json": _json(outcome["requestedScope"]),
                "coverage_limits_json": _json(outcome["acquisitionPolicy"].get("coverageLimits", [])),
                "collection_outcome_json": _json(outcome),
                "artifact_sha256": item.get("artifact_sha256"),
            }
            records_by_cid[cid] = rec_rows
            relationships_by_cid[cid] = rel_rows

    return records_by_cid, collection_row_by_cid, relationships_by_cid, len(opened_archives)


def _apply_context_repair(row: dict[str, Any], repair: CollectionContextRepair) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply an explicit provenance-bound C1 repair to one ``fec_collections`` row."""
    outcome_raw = row.get("collection_outcome_json")
    if not isinstance(outcome_raw, str):
        raise ValueError(f"Context repair requires collection_outcome_json for {repair.collection_id}")
    outcome = json.loads(outcome_raw)
    caller_ctx = (outcome.get("receiverDisposition") or {}).get("callerContext")
    pin = (caller_ctx or {}).get("pin") if isinstance(caller_ctx, dict) else None
    actual_sha = pin.get("sha256") if isinstance(pin, dict) else None
    if actual_sha != _digest(repair.prior_context_sha256):
        raise ValueError(
            f"Context repair pin mismatch for {repair.collection_id}: "
            f"expected {repair.prior_context_sha256}, found {actual_sha!r}"
        )
    if repair.replacement_text is not None:
        repaired_outcome = json.loads(repair.replacement_text)
        replacements = -1
    elif repair.cp1252_c1_decode:
        repaired_outcome, replacements = _repair_c1_in_json_value(outcome)
        if replacements == 0:
            raise ValueError(f"Declared C1 context repair found zero C1 characters in {repair.collection_id}")
        repaired_outcome["c1ContextRepair"] = {
            "priorContextSha256": actual_sha,
            "encodingBasis": "cp1252-c1-punctuation-normalization",
            "replacedCharacters": replacements,
            "reason": repair.reason,
        }
    else:
        raise ValueError(f"Context repair for {repair.collection_id} requires replacement_text or cp1252_c1_decode=True")
    updated = {**row, "collection_outcome_json": _json(repaired_outcome)}
    proof = {
        "collection_id": repair.collection_id,
        "prior_context_sha256": actual_sha,
        "replaced_characters": replacements,
        "reason": repair.reason,
    }
    return updated, proof


def _prior_ordinals_by_collection(
    records_path: Path,
    target_cids: Sequence[str],
    *,
    con: duckdb.DuckDBPyConnection,
) -> dict[str, list[int]]:
    in_list = _sql_in_list(target_cids)
    rows = con.execute(
        f"SELECT collection_id, observation_ordinal FROM read_parquet(?, hive_partitioning=false) "
        f"WHERE collection_id IN {in_list} ORDER BY collection_id, observation_ordinal",
        [str(records_path)],
    ).fetchall()
    result: dict[str, list[int]] = defaultdict(list)
    for cid, ordinal in rows:
        result[str(cid)].append(int(ordinal))
    return dict(result)


def _splice_source_records(
    prior_path: Path,
    dest_path: Path,
    records_by_cid: Mapping[str, list[dict[str, Any]]],
    *,
    con: duckdb.DuckDBPyConnection,
    batch_size: int,
    check_resources: Callable[[], None],
) -> tuple[int, int]:
    """Stream ``fec_source_records.parquet`` replacing only declared ``records_by_cid``."""
    prior_schema = pq.read_schema(prior_path)
    subject_schema = dataset_policy("fec_source_records").subject_schema
    if prior_schema.equals(subject_schema, check_metadata=False):
        schema_mode = "subject"
    elif prior_schema.equals(RECORD_SCHEMA, check_metadata=False):
        schema_mode = "full"
    else:
        raise ValueError(f"Unsupported prior fec_source_records schema: {prior_schema.names}")

    target_cids = tuple(sorted(records_by_cid))
    target_set = frozenset(target_cids)
    target_array = pa.array(target_cids, type=pa.string())

    # Pre-fetch existing observation_ordinals for the changed collections so spliced rows
    # retain exact positional ordinals even when a collection straddles an Arrow batch boundary.
    old_ordinals: dict[str, list[int]] = {}
    same_cardinality = True
    if schema_mode == "subject":
        old_ordinals = _prior_ordinals_by_collection(prior_path, target_cids, con=con)
        missing_in_prior = [cid for cid in target_cids if cid not in old_ordinals]
        if missing_in_prior:
            raise ValueError(f"Target splice collections absent from prior fec_source_records: {missing_in_prior}")
        same_cardinality = all(len(old_ordinals[cid]) == len(records_by_cid[cid]) for cid in target_cids)

    prepared_by_cid: dict[str, list[dict[str, Any]]] = {}
    if schema_mode == "full":
        for cid, rows in records_by_cid.items():
            prepared_by_cid[cid] = [{name: r.get(name) for name in prior_schema.names} for r in rows]
    elif same_cardinality:
        for cid, rows in records_by_cid.items():
            ords = old_ordinals[cid]
            prepared_by_cid[cid] = [
                {name: normalize_record("fec_source_records", r, observation_ordinal=ord_val)[name] for name in prior_schema.names}
                for r, ord_val in zip(rows, ords, strict=True)
            ]

    seen_counts: dict[str, int] = defaultdict(int)
    emitted_cids: set[str] = set()
    prior_total = 0
    spliced_total = 0
    next_ordinal = 0

    with pq.ParquetFile(prior_path) as source, pq.ParquetWriter(dest_path, prior_schema, compression="zstd") as writer:
        for batch in source.iter_batches(batch_size=batch_size, use_threads=False):
            check_resources()
            prior_total += batch.num_rows
            cid_col = batch.column("collection_id")
            has_target = bool(_pc_call("any", _pc_call("is_in", cid_col, value_set=target_array)).as_py())
            if not has_target and (schema_mode == "full" or same_cardinality):
                writer.write_batch(batch)
                spliced_total += batch.num_rows
                continue

            out_rows: list[dict[str, Any]] = []
            for row in batch.to_pylist():
                cid = row["collection_id"]
                if cid not in target_set:
                    if schema_mode == "subject" and not same_cardinality:
                        row = {**row, "observation_ordinal": next_ordinal}
                        next_ordinal += 1
                    out_rows.append(row)
                    continue

                idx = seen_counts[cid]
                seen_counts[cid] = idx + 1
                if schema_mode == "full" or same_cardinality:
                    if idx == 0:
                        out_rows.extend(prepared_by_cid[cid])
                        emitted_cids.add(cid)
                else:
                    if idx == 0:
                        for raw_row in records_by_cid[cid]:
                            norm = normalize_record("fec_source_records", raw_row, observation_ordinal=next_ordinal)
                            out_rows.append({name: norm[name] for name in prior_schema.names})
                            next_ordinal += 1
                        emitted_cids.add(cid)

            if out_rows:
                writer.write_table(pa.Table.from_pylist(out_rows, schema=prior_schema))
                spliced_total += len(out_rows)

    missing_seen = sorted(target_set - emitted_cids)
    if missing_seen:
        raise ValueError(f"Target splice collections absent from prior fec_source_records: {missing_seen}")
    return prior_total, spliced_total


def _splice_collections(
    prior_path: Path,
    dest_path: Path,
    collection_row_by_cid: Mapping[str, dict[str, Any]],
    context_repairs: Mapping[str, CollectionContextRepair],
    *,
    batch_size: int,
    check_resources: Callable[[], None],
) -> tuple[int, int, list[dict[str, Any]]]:
    """Stream ``fec_collections.parquet`` replacing declared cp1252 and context-repair rows."""
    prior_schema = pq.read_schema(prior_path)
    expected_schema = dataset_policy("fec_collections").subject_schema
    if not prior_schema.equals(expected_schema, check_metadata=False) and not prior_schema.equals(
        COLLECTION_SCHEMA, check_metadata=False
    ):
        raise ValueError(f"Unsupported prior fec_collections schema: {prior_schema.names}")

    overlap = set(collection_row_by_cid) & set(context_repairs)
    if overlap:
        raise ValueError(f"Collections cannot be both cp1252-reparsed and context-repaired: {sorted(overlap)}")

    normalized_new = {
        cid: {name: normalize_record("fec_collections", row)[name] for name in prior_schema.names}
        for cid, row in collection_row_by_cid.items()
    }
    seen_reparsed: set[str] = set()
    seen_repaired: set[str] = set()
    repair_proofs: list[dict[str, Any]] = []
    prior_total = spliced_total = 0

    with pq.ParquetFile(prior_path) as source, pq.ParquetWriter(dest_path, prior_schema, compression="zstd") as writer:
        for batch in source.iter_batches(batch_size=batch_size, use_threads=False):
            check_resources()
            prior_total += batch.num_rows
            out_rows: list[dict[str, Any]] = []
            for row in batch.to_pylist():
                cid = row["collection_id"]
                if cid in normalized_new:
                    out_rows.append(normalized_new[cid])
                    seen_reparsed.add(cid)
                elif cid in context_repairs:
                    updated, proof = _apply_context_repair(row, context_repairs[cid])
                    out_rows.append(updated)
                    repair_proofs.append(proof)
                    seen_repaired.add(cid)
                else:
                    out_rows.append(row)
            if out_rows:
                writer.write_table(pa.Table.from_pylist(out_rows, schema=prior_schema))
                spliced_total += len(out_rows)

    missing_reparsed = sorted(set(normalized_new) - seen_reparsed)
    if missing_reparsed:
        raise ValueError(f"Target splice collections absent from prior fec_collections: {missing_reparsed}")
    missing_repaired = sorted(set(context_repairs) - seen_repaired)
    if missing_repaired:
        raise ValueError(f"Context-repair collections absent from prior fec_collections: {missing_repaired}")
    return prior_total, spliced_total, repair_proofs


def _splice_relationships(
    prior_path: Path,
    dest_path: Path,
    relationships_by_cid: Mapping[str, list[dict[str, Any]]],
    *,
    con: duckdb.DuckDBPyConnection,
    batch_size: int,
    check_resources: Callable[[], None],
) -> tuple[int, int, bool]:
    """Copy or stream-splice ``fec_relationships.parquet`` preserving untouched collections."""
    prior_schema = pq.read_schema(prior_path)
    subject_schema = dataset_policy("fec_relationships").subject_schema
    if prior_schema.equals(subject_schema, check_metadata=False):
        schema_mode = "subject"
    elif prior_schema.equals(RELATIONSHIP_SCHEMA, check_metadata=False):
        schema_mode = "full"
    else:
        raise ValueError(f"Unsupported prior fec_relationships schema: {prior_schema.names}")

    target_cids = tuple(sorted(relationships_by_cid))
    in_list = _sql_in_list(target_cids)
    has_cid_col = "collection_id" in prior_schema.names
    cid_expr = "collection_id" if has_cid_col else "json_extract_string(source_locator_json, '$.collection_id')"
    [(prior_hits,)] = con.execute(
        f"SELECT count(*) FROM read_parquet(?, hive_partitioning=false) WHERE {cid_expr} IN {in_list}",
        [str(prior_path)],
    ).fetchall()
    new_total = sum(len(v) for v in relationships_by_cid.values())
    prior_total = pq.read_metadata(prior_path).num_rows
    if prior_hits == 0 and new_total == 0:
        shutil.copyfile(prior_path, dest_path)
        return prior_total, prior_total, True

    target_set = frozenset(target_cids)
    target_array = pa.array(target_cids, type=pa.string())
    prepared_new: list[dict[str, Any]] = []
    for cid in target_cids:
        for row in relationships_by_cid[cid]:
            if schema_mode == "subject":
                norm = normalize_record("fec_relationships", row)
                prepared_new.append({name: norm[name] for name in prior_schema.names})
            else:
                prepared_new.append({name: row.get(name) for name in prior_schema.names})

    spliced_total = 0
    with pq.ParquetFile(prior_path) as source, pq.ParquetWriter(dest_path, prior_schema, compression="zstd") as writer:
        for batch in source.iter_batches(batch_size=batch_size, use_threads=False):
            check_resources()
            if has_cid_col:
                cid_col = batch.column("collection_id")
                mask = _pc_call("is_in", cid_col, value_set=target_array)
                if not bool(_pc_call("any", mask).as_py()):
                    writer.write_batch(batch)
                    spliced_total += batch.num_rows
                    continue
                kept = batch.filter(_pc_call("invert", mask))
                if kept.num_rows:
                    writer.write_batch(kept)
                    spliced_total += kept.num_rows
            else:
                kept_rows = [
                    r
                    for r in batch.to_pylist()
                    if (json.loads(r["source_locator_json"]).get("collection_id") if r.get("source_locator_json") else None)
                    not in target_set
                ]
                if kept_rows:
                    writer.write_table(pa.Table.from_pylist(kept_rows, schema=prior_schema))
                    spliced_total += len(kept_rows)
        if prepared_new:
            for offset in range(0, len(prepared_new), batch_size):
                check_resources()
                chunk = prepared_new[offset : offset + batch_size]
                writer.write_table(pa.Table.from_pylist(chunk, schema=prior_schema))
                spliced_total += len(chunk)
    return prior_total, spliced_total, False


def verify_splice_parity(
    *,
    prior_dir: Path,
    spliced_dir: Path,
    changed_collection_ids: Sequence[str],
    cp1252_collection_ids: Sequence[str] = (),
    memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    threads: int = 4,
) -> dict[str, Any]:
    """Prove bidirectional ``EXCEPT ALL`` equality on every untouched collection across all 3 tables."""
    changed = tuple(sorted(set(changed_collection_ids)))
    if not changed:
        raise ValueError("Parity verification requires explicit changed_collection_ids")
    in_list = _sql_in_list(changed)
    summary: dict[str, Any] = {"changed_collections": list(changed), "tables": {}}

    with TemporaryDirectory(prefix="fec-splice-parity-") as temp_root:
        cfg = _duckdb_config(memory_limit, temp_limit, threads, Path(temp_root))
        with duckdb.connect(config=cfg) as con:
            for filename in OUTPUTS:
                prior_path = str(prior_dir / filename)
                spliced_path = str(spliced_dir / filename)
                cols = set(pq.read_schema(prior_path).names)
                if "collection_id" in cols:
                    where = f"WHERE collection_id NOT IN {in_list}"
                else:
                    where = (
                        f"WHERE COALESCE(json_extract_string(source_locator_json, '$.collection_id'), '') "
                        f"NOT IN {in_list}"
                    )
                [(prior_untouched,)] = con.execute(
                    f"SELECT count(*) FROM read_parquet(?, hive_partitioning=false) {where}",
                    [prior_path],
                ).fetchall()
                [(spliced_untouched,)] = con.execute(
                    f"SELECT count(*) FROM read_parquet(?, hive_partitioning=false) {where}",
                    [spliced_path],
                ).fetchall()
                [(prior_only,)] = con.execute(
                    f"SELECT count(*) FROM ("
                    f"SELECT * FROM read_parquet(?, hive_partitioning=false) {where} "
                    f"EXCEPT ALL "
                    f"SELECT * FROM read_parquet(?, hive_partitioning=false) {where})",
                    [prior_path, spliced_path],
                ).fetchall()
                [(spliced_only,)] = con.execute(
                    f"SELECT count(*) FROM ("
                    f"SELECT * FROM read_parquet(?, hive_partitioning=false) {where} "
                    f"EXCEPT ALL "
                    f"SELECT * FROM read_parquet(?, hive_partitioning=false) {where})",
                    [spliced_path, prior_path],
                ).fetchall()
                if prior_untouched != spliced_untouched or prior_only != 0 or spliced_only != 0:
                    raise ValueError(
                        f"Untouched collection parity failed for {filename}: "
                        f"prior_untouched={prior_untouched}, spliced_untouched={spliced_untouched}, "
                        f"prior_only={prior_only}, spliced_only={spliced_only}"
                    )
                summary["tables"][filename] = {
                    "prior_untouched_rows": prior_untouched,
                    "spliced_untouched_rows": spliced_untouched,
                    "prior_only_rows": prior_only,
                    "spliced_only_rows": spliced_only,
                }

            [(untouched_collections,)] = con.execute(
                f"SELECT count(DISTINCT collection_id) FROM read_parquet(?, hive_partitioning=false) "
                f"WHERE collection_id NOT IN {in_list}",
                [str(spliced_dir / "fec_collections.parquet")],
            ).fetchall()
            summary["untouched_collections_count"] = int(untouched_collections)

            # Confirm every cp1252-spliced collection states encoding='cp1252' and agrees with record counts.
            if cp1252_collection_ids:
                cp_in = _sql_in_list(tuple(sorted(set(cp1252_collection_ids))))
                col_rows = con.execute(
                    f"SELECT collection_id, record_count, requested_scope_json "
                    f"FROM read_parquet(?, hive_partitioning=false) WHERE collection_id IN {cp_in}",
                    [str(spliced_dir / "fec_collections.parquet")],
                ).fetchall()
                rec_counts = dict(
                    con.execute(
                        f"SELECT collection_id, count(*) FROM read_parquet(?, hive_partitioning=false) "
                        f"WHERE collection_id IN {cp_in} GROUP BY collection_id",
                        [str(spliced_dir / "fec_source_records.parquet")],
                    ).fetchall()
                )
                if len(col_rows) != len(set(cp1252_collection_ids)):
                    raise ValueError("Spliced fec_collections is missing one or more cp1252 target collections")
                for cid, declared_count, scope_raw in col_rows:
                    scope = json.loads(scope_raw)
                    if scope.get("encoding") != "cp1252":
                        raise ValueError(f"Spliced collection {cid} did not record encoding='cp1252'")
                    if int(declared_count) != int(rec_counts.get(cid, -1)):
                        raise ValueError(
                            f"Spliced record_count mismatch for {cid}: "
                            f"fec_collections={declared_count}, fec_source_records={rec_counts.get(cid)}"
                        )
    return summary


def splice_fec_observations(
    *,
    prior_dir: Path,
    output_dir: Path,
    store: Any,
    manifest_bytes: bytes | None = None,
    manifest_sha256: str | None = SOURCE_MANIFEST_SHA256,
    target_collection_ids: Sequence[str] = CP1252_SPLICE_COLLECTION_IDS,
    collection_context_repairs: Sequence[CollectionContextRepair] = (),
    read_snapshot: Mapping[str, Any] | None = None,
    inputs: Sequence[Mapping[str, Any]] = (),
    batch_size: int = SPLICE_BATCH_ROWS,
    duckdb_memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    duckdb_temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    c1_threads: int = 6,
    check_resources: Callable[[], None] = lambda: None,
) -> ObservationSpliceResult:
    """Splice cp1252-corrected positional filings into verified prior ``fec-observations`` and seal receipts-free.

    Outputs under ``output_dir``:
    - ``build/generation/``: receipts-free sealed ``fec-observations`` generation
      (passes ``admit_generation_for_publication()`` with zero ``etlReceipts``).
    - ``corrected_manifest.json``: canonical cp1252-corrected manifest bytes.
    - ``splice_receipt.json``: manifest diff, parity proof, and measured telemetry.
    """
    started = monotonic()
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")
    prior_dir = Path(prior_dir).resolve()
    # Resolve output_dir so owned_output_root has no symlink ancestors (e.g., macOS /var -> /private/var).
    output_dir = Path(output_dir).resolve()
    for name in OUTPUTS:
        candidate = prior_dir / name
        if candidate.is_symlink() or not candidate.is_file():
            raise ValueError(f"Prior fec-observations input missing or not a regular file: {candidate}")

    if manifest_bytes is None:
        if manifest_sha256 is None:
            raise ValueError("Either manifest_bytes or manifest_sha256 is required")
        manifest_bytes = _open_store_digest(store, manifest_sha256)

    correction = correct_manifest_encodings(
        manifest_bytes,
        store=store,
        target_collection_ids=target_collection_ids,
        expected_manifest_sha256=manifest_sha256,
    )
    records_by_cid, collection_row_by_cid, relationships_by_cid, zip_spool_opens = reparse_spliced_collections(
        correction.corrected_collections,
        store=store,
    )

    repair_map = {item.collection_id: item for item in collection_context_repairs}
    if len(repair_map) != len(collection_context_repairs):
        raise ValueError("Duplicate collection_id in collection_context_repairs")

    build_root = output_dir / "build"
    tables_dir = build_root / "tables"
    generation_dir = build_root / "generation"
    if generation_dir.exists():
        raise FileExistsError(f"Target generation directory already exists: {generation_dir}")
    tables_dir.mkdir(parents=True, exist_ok=True)

    prior_rows: dict[str, int] = {}
    spliced_rows: dict[str, int] = {}
    peak_disk = 0

    with ExitStack() as stack:
        temp_root = Path(stack.enter_context(TemporaryDirectory(prefix="fec-splice-duckdb-")))
        cfg = _duckdb_config(duckdb_memory_limit, duckdb_temp_limit, max(1, min(c1_threads, 4)), temp_root)
        con = stack.enter_context(duckdb.connect(config=cfg))

        p_rec, s_rec = _splice_source_records(
            prior_dir / "fec_source_records.parquet",
            tables_dir / "fec_source_records.parquet",
            records_by_cid,
            con=con,
            batch_size=batch_size,
            check_resources=check_resources,
        )
        prior_rows["fec_source_records.parquet"] = p_rec
        spliced_rows["fec_source_records.parquet"] = s_rec

        p_col, s_col, context_repair_proofs = _splice_collections(
            prior_dir / "fec_collections.parquet",
            tables_dir / "fec_collections.parquet",
            collection_row_by_cid,
            repair_map,
            batch_size=batch_size,
            check_resources=check_resources,
        )
        prior_rows["fec_collections.parquet"] = p_col
        spliced_rows["fec_collections.parquet"] = s_col

        p_rel, s_rel, rel_copied_unchanged = _splice_relationships(
            prior_dir / "fec_relationships.parquet",
            tables_dir / "fec_relationships.parquet",
            relationships_by_cid,
            con=con,
            batch_size=batch_size,
            check_resources=check_resources,
        )
        prior_rows["fec_relationships.parquet"] = p_rel
        spliced_rows["fec_relationships.parquet"] = s_rel

    peak_disk = max(peak_disk, _dir_bytes(output_dir))

    declared_changed = tuple(sorted(set(records_by_cid) | set(repair_map)))
    parity = verify_splice_parity(
        prior_dir=prior_dir,
        spliced_dir=tables_dir,
        changed_collection_ids=declared_changed,
        cp1252_collection_ids=tuple(records_by_cid),
        memory_limit=duckdb_memory_limit,
        temp_limit=duckdb_temp_limit,
        threads=max(1, min(c1_threads, 4)),
    )
    parity["relationships_byte_copied_unchanged"] = rel_copied_unchanged

    staged_files = [tables_dir / name for name in OUTPUTS]
    refuse_c1_controls(staged_files, check_resources=check_resources, threads=c1_threads)

    artifact = build_generation(
        generation_dir,
        family="fec-observations",
        files=staged_files,
        expected_keys=OUTPUTS,
        read_snapshot=read_snapshot,
        inputs=inputs,
        receipt_path=None,
        owned_output_root=build_root,
    )
    if tables_dir.exists() and not any(tables_dir.iterdir()):
        tables_dir.rmdir()

    corrected_manifest_path = output_dir / "corrected_manifest.json"
    corrected_manifest_path.write_bytes(correction.corrected_manifest_bytes)

    peak_disk = max(peak_disk, _dir_bytes(output_dir))
    telemetry = SpliceTelemetry(
        wall_seconds=round(monotonic() - started, 3),
        peak_rss_bytes=_peak_rss_bytes(),
        peak_disk_bytes=peak_disk,
        prior_rows=prior_rows,
        spliced_rows=spliced_rows,
        changed_collections=declared_changed,
        untouched_collections_count=int(parity["untouched_collections_count"]),
        zip_spool_opens=zip_spool_opens,
        c1_control_matches=0,
    )
    manifest_diff = {
        "source_manifest_sha256": correction.source_manifest_sha256,
        "corrected_manifest_sha256": correction.corrected_manifest_sha256,
        "cp1252_collection_diffs": list(correction.collection_diffs),
        "collection_context_repairs": context_repair_proofs,
    }
    receipt_payload = {
        "format": "fec-observations-splice-receipt/1",
        "artifact_digest": artifact.pin.artifact_digest,
        "logical_id": artifact.pin.logical_id,
        "receipts_free": True,
        "manifest_diff": manifest_diff,
        "parity": parity,
        "telemetry": asdict(telemetry),
    }
    receipt_path = output_dir / "splice_receipt.json"
    receipt_path.write_text(json.dumps(receipt_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return ObservationSpliceResult(
        generation_dir=generation_dir,
        artifact_digest=artifact.pin.artifact_digest,
        logical_id=artifact.pin.logical_id,
        manifest_diff=manifest_diff,
        parity=parity,
        telemetry=telemetry,
        receipt_path=receipt_path,
        corrected_manifest_path=corrected_manifest_path,
    )
