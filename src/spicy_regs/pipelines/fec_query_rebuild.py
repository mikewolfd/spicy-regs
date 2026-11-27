"""Three-phase receipts-free FEC rebuild CLI (ss-42zk / ss-42nr).

Exposes cleanly separated, restartable phases designed for a bounded local disk
and memory envelope (default pre-flight floor >= 25 GiB free, hard abort < 20 GiB
free, DuckDB memory/spill caps at 2 GiB, zero-copy ``owned_output_root`` sealing):

- ``splice-observations``: Splice the 3 cp1252 positional filings into verified
  prior ``fec-observations`` Parquets and seal receipts-free.
- ``build-query``: Stream all 50 ``fec_typed_schemas.json`` tables with
  table-level checkpoint/resume, C1-zero verification, and receipts-free seal.
- ``verify-parity``: Verify receipts-free admission, C1-zero, and bidirectional
  value parity under ``id_mapping.parquet`` one table at a time.
"""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import shutil
from typing import Annotated, Any

from cyclopts import App, Parameter

from spicy_regs.transforms.fec_observations_splice import (
    CP1252_SPLICE_COLLECTION_IDS,
    DEFAULT_DUCKDB_MEMORY_LIMIT,
    DEFAULT_DUCKDB_TEMP_LIMIT,
    SOURCE_MANIFEST_SHA256,
    SPLICE_BATCH_ROWS,
    build_doj_c1_context_repairs,
    splice_fec_observations,
)
from spicy_regs.transforms.fec_query_builder import (
    DEFAULT_BATCH_ROWS,
    build_fec_query_generation,
    verify_fec_query_parity,
)

GIB = 1024**3
DEFAULT_MIN_FREE_GIB = 25.0
DEFAULT_ABORT_FREE_GIB = 20.0
DEFAULT_SCOPE_PATH = Path(__file__).resolve().parents[1] / "fec_query_scope.json"

app = App(name="fec-query-rebuild", help=__doc__)


def check_disk_headroom(
    target_path: Path,
    *,
    min_free_gib: float = DEFAULT_MIN_FREE_GIB,
    abort_free_gib: float = DEFAULT_ABORT_FREE_GIB,
) -> dict[str, Any]:
    """Assert sufficient free filesystem space before launching a heavy FEC phase."""
    probe = Path(target_path).resolve()
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    free_gib = usage.free / GIB
    total_gib = usage.total / GIB
    if free_gib < abort_free_gib:
        raise RuntimeError(
            f"Pre-flight disk guard aborted: {free_gib:.2f} GiB free on {probe} "
            f"is below hard floor {abort_free_gib:.1f} GiB"
        )
    if free_gib < min_free_gib:
        raise RuntimeError(
            f"Pre-flight disk guard refused launch: {free_gib:.2f} GiB free on {probe} "
            f"is below recommended floor {min_free_gib:.1f} GiB"
        )
    return {
        "probe_path": str(probe),
        "free_gib": round(free_gib, 3),
        "total_gib": round(total_gib, 3),
        "min_free_gib": min_free_gib,
        "abort_free_gib": abort_free_gib,
    }


def _open_raw_store(raw_root: Path | None):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    if raw_root is None:
        return CentralRawArchiveStore(create=False)
    return CentralRawArchiveStore(root=Path(raw_root), create=False)


def _safe_cleanup_staging(path: Path, *, raw_root: Path | None = None) -> bool:
    """Remove an explicitly staged prior download directory; refuses raw archive roots."""
    target = Path(path).resolve()
    if raw_root is not None:
        resolved_raw = Path(raw_root).resolve()
        if target == resolved_raw or resolved_raw.is_relative_to(target) or target.is_relative_to(resolved_raw):
            raise ValueError(f"Refusing to clean up path overlapping raw archive root: {target}")
    if "corpora" in target.parts and "raw" in target.parts:
        raise ValueError(f"Refusing to clean up shared corpora raw path: {target}")
    if target.is_dir():
        shutil.rmtree(target)
        return True
    if target.is_file():
        target.unlink()
        return True
    return False


@app.command(name="splice-observations")
def splice_observations_cmd(
    *,
    prior_dir: Path,
    output_dir: Path,
    raw_root: Path | None = None,
    manifest_path: Path | None = None,
    manifest_sha256: str = SOURCE_MANIFEST_SHA256,
    target_collections: Annotated[tuple[str, ...], Parameter(consume_multiple=True)] = CP1252_SPLICE_COLLECTION_IDS,
    repair_doj_c1_contexts: bool = False,
    min_free_gib: float = DEFAULT_MIN_FREE_GIB,
    abort_free_gib: float = DEFAULT_ABORT_FREE_GIB,
    duckdb_memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    duckdb_temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    batch_size: int = SPLICE_BATCH_ROWS,
    threads: int = 6,
    cleanup_prior: bool = False,
) -> dict:
    """Phase 1: Splice cp1252-corrected positional filings into prior ``fec-observations`` and seal receipts-free."""
    disk = check_disk_headroom(output_dir, min_free_gib=min_free_gib, abort_free_gib=abort_free_gib)
    store = _open_raw_store(raw_root)
    manifest_bytes = manifest_path.read_bytes() if manifest_path is not None else None
    repairs = build_doj_c1_context_repairs() if repair_doj_c1_contexts else ()
    result = splice_fec_observations(
        prior_dir=prior_dir,
        output_dir=output_dir,
        store=store,
        manifest_bytes=manifest_bytes,
        manifest_sha256=None if manifest_path is not None else manifest_sha256,
        target_collection_ids=target_collections,
        collection_context_repairs=repairs,
        batch_size=batch_size,
        duckdb_memory_limit=duckdb_memory_limit,
        duckdb_temp_limit=duckdb_temp_limit,
        c1_threads=threads,
    )
    cleaned = _safe_cleanup_staging(prior_dir, raw_root=raw_root) if cleanup_prior else False
    summary = {
        "phase": "splice-observations",
        "disk_preflight": disk,
        "artifact_digest": result.artifact_digest,
        "generation_dir": str(result.generation_dir),
        "receipt_path": str(result.receipt_path),
        "corrected_manifest_path": str(result.corrected_manifest_path),
        "prior_cleaned": cleaned,
        "telemetry": asdict(result.telemetry),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


@app.command(name="build-query")
def build_query_cmd(
    *,
    observations_dir: Path,
    output_dir: Path,
    source_generation_pin: str,
    selection_evidence_sha256: str,
    raw_root: Path | None = None,
    scope_file: Path = DEFAULT_SCOPE_PATH,
    full_source_records: Path | None = None,
    individual_snapshot_proof_path: Path | None = None,
    retained_mur_manifest: Path | None = None,
    discover_retained_murs: bool = True,
    include_rebuilt_tables: bool = True,
    verify_rebuilt_expected_rows: bool = True,
    min_free_gib: float = DEFAULT_MIN_FREE_GIB,
    abort_free_gib: float = DEFAULT_ABORT_FREE_GIB,
    duckdb_memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    duckdb_temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    threads: int = 6,
    resume: bool = True,
) -> dict:
    """Phase 2: Stream all 50 ``fec-query`` tables with checkpoints, C1-zero check, and receipts-free seal."""
    import hashlib

    disk = check_disk_headroom(output_dir, min_free_gib=min_free_gib, abort_free_gib=abort_free_gib)
    scope = json.loads(Path(scope_file).read_text(encoding="utf-8"))
    store = _open_raw_store(raw_root)
    indiv_proof: tuple[bytes, str] | None = None
    if individual_snapshot_proof_path is not None:
        raw = individual_snapshot_proof_path.read_bytes()
        indiv_proof = (raw, "sha256:" + hashlib.sha256(raw).hexdigest())

    mur_captures: list[dict[str, Any]] | None
    if retained_mur_manifest is not None:
        descriptors = json.loads(Path(retained_mur_manifest).read_text(encoding="utf-8"))
        if not isinstance(descriptors, list):
            raise ValueError("retained_mur_manifest must be a JSON array of MUR capture descriptors")
        mur_captures = []
        for entry in descriptors:
            sha = str(entry["source_sha256"])
            bare = sha.removeprefix("sha256:")
            with store.open(bare) as stream:
                payload_bytes = stream.read()
            mur_captures.append({**entry, "payload_bytes": payload_bytes})
    elif not discover_retained_murs:
        mur_captures = []
    else:
        mur_captures = None

    result = build_fec_query_generation(
        observations_dir=observations_dir,
        output_dir=output_dir,
        source_generation_pin=source_generation_pin,
        selection_evidence_sha256=selection_evidence_sha256,
        namespace_evidence=scope.get("namespace_evidence", {}),
        store=store,
        full_source_records_path=full_source_records,
        individual_snapshot_proof=indiv_proof,
        retained_mur_captures=mur_captures,
        include_rebuilt_tables=include_rebuilt_tables,
        verify_rebuilt_expected_rows=verify_rebuilt_expected_rows,
        batch_rows=batch_rows,
        duckdb_memory_limit=duckdb_memory_limit,
        duckdb_temp_limit=duckdb_temp_limit,
        c1_threads=threads,
        resume=resume,
    )
    summary = {
        "phase": "build-query",
        "disk_preflight": disk,
        "artifact_digest": result.artifact_digest,
        "generation_dir": str(result.generation_dir),
        "id_mapping_path": str(result.id_mapping_path),
        "receipt_path": str(result.receipt_path),
        "table_count": len(result.table_rows),
        "telemetry": asdict(result.telemetry),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


@app.command(name="verify-parity")
def verify_parity_cmd(
    *,
    prior_dir: Path,
    candidate_dir: Path,
    id_mapping: Path,
    output_report: Path,
    tables: Annotated[tuple[str, ...], Parameter(consume_multiple=True)] = (),
    declared_changed_collections: Annotated[tuple[str, ...], Parameter(consume_multiple=True)] = CP1252_SPLICE_COLLECTION_IDS,
    declared_repaired_tables: Annotated[tuple[str, ...], Parameter(consume_multiple=True)] = (),
    duckdb_memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
    duckdb_temp_limit: str = DEFAULT_DUCKDB_TEMP_LIMIT,
    threads: int = 4,
    cleanup_prior: bool = False,
) -> dict:
    """Phase 3: Verify receipts-free admission, C1-zero, and bidirectional value parity under ``id_mapping.parquet``."""
    report = verify_fec_query_parity(
        prior_dir=prior_dir,
        candidate_dir=candidate_dir,
        id_mapping_path=id_mapping,
        tables=tables or None,
        declared_changed_collections=declared_changed_collections,
        declared_repaired_tables=declared_repaired_tables,
        memory_limit=duckdb_memory_limit,
        temp_limit=duckdb_temp_limit,
        threads=threads,
        admit_candidate=True,
    )
    output_report = Path(output_report)
    output_report.parent.mkdir(parents=True, exist_ok=True)
    output_report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    cleaned = _safe_cleanup_staging(prior_dir) if cleanup_prior else False
    summary = {
        "phase": "verify-parity",
        "output_report": str(output_report),
        "tables_checked": len(report["tables"]),
        "c1_control_matches": report["c1_control_matches"],
        "prior_cleaned": cleaned,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


if __name__ == "__main__":
    app()
