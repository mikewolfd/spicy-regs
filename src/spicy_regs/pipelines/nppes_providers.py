"""Build a pinned CMS NPPES V2 provider observation generation for local publication."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_nppes_providers import (
    FAMILY,
    SCHEMA,
    TABLE,
    build_observations_multi,
)

if TYPE_CHECKING:
    from spicy_docs.sources.nppes import NppesCsv


def _asset_kind(local_path: Path, source_url: str) -> str:
    suffix = Path(source_url.split("?", 1)[0]).suffix.lower() or local_path.suffix.lower()
    if suffix == ".zip":
        return "zip"
    if suffix == ".csv":
        return "csv"
    return "binary"


def build(
    csv_inputs: Iterable[NppesCsv],
    output_dir: Path,
    *,
    raw_files: Iterable[tuple[Path, str, str]],
    release_metadata: Mapping[str, Any] | None = None,
):
    """Write ``nppes_provider_observations.parquet`` and package an admitted generation.

    ``raw_files`` is a required non-empty iterable of ``(local_path, source_url, stage)``
    tuples for the exact publisher ZIP archives or CSV originals retained with
    ``raw_source=True``. Every ``csv_inputs`` item's ``source_url`` must match a
    retained ``raw_files`` entry, and retained originals are declared through
    ``evidence.declare_used_inputs`` so input-proof resolution verifies every byte.
    Only publisher URLs, logical names, and content digests are journaled—never local
    machine paths.
    """
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh NPPES provider generation directory")
    inputs = tuple(csv_inputs)
    if not inputs:
        raise ValueError("NPPES generation build requires at least one pinned NppesCsv input")
    raw_list = tuple((Path(path), source_url, stage) for path, source_url, stage in raw_files)
    if not raw_list:
        raise ValueError("NPPES generation build requires non-empty raw_files pinning retained originals")
    raw_urls = {source_url for _, source_url, _ in raw_list}
    if any(not url for url in raw_urls) or len(raw_urls) != len(raw_list):
        raise ValueError("NPPES raw_files must provide unique non-empty publisher source URLs")
    unretained = sorted({item.source_url for item in inputs} - raw_urls)
    if unretained:
        raise ValueError(
            f"NPPES csv_inputs cite source_url(s) not retained in raw_files: {', '.join(unretained)}"
        )

    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        retained_pins: list[dict[str, Any]] = []
        used_inputs: list[dict[str, Any]] = []
        for local_path, source_url, stage in raw_list:
            retained = evidence.retain_file(
                local_path,
                stage=stage,
                raw_source=True,
                source_url=source_url,
                original_location=source_url,
                source_id=source_url,
            )
            retained_pins.append(
                {
                    "stage": stage,
                    "source_url": source_url,
                    "sha256": retained["sha256"],
                    "byte_size": retained["byte_size"],
                }
            )
            used_inputs.append(
                {
                    "location": source_url,
                    "sha256": retained["sha256"],
                    "bytes": retained["byte_size"],
                    "asset_kind": _asset_kind(local_path, source_url),
                }
            )
        evidence.declare_used_inputs(used_inputs, scope_id=f"{FAMILY}:inputs", complete=True)
        table = build_observations_multi(inputs, output_dir / f"{TABLE}.parquet")
        evidence.event(
            "nppes-v2-selection",
            inputs=[
                {
                    "kind": item.kind,
                    "scope": item.scope,
                    "source_url": item.source_url,
                    "member_name": item.member_name,
                    "sha256": item.sha256,
                    "byte_size": item.byte_size,
                    "scanned_rows": item.scanned_rows,
                    "selected_rows": len(item.rows),
                }
                for item in inputs
            ],
            retained_originals=retained_pins,
            release_metadata=dict(release_metadata or {}),
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=(table,),
            expected_keys=(table.name,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir
