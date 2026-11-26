"""Build sector comparisons from pinned FEC profiles and lobbying-lane facts."""

import argparse
import hashlib
from importlib.resources import files
import json
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.fec_research_profiles import _read
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_fec_financial_profiles import json_bytes
from spicy_regs.transforms.sector_comparisons import SCHEMAS, ISSUES, compare, issue_views

FAMILY = "sector-comparisons"
SELECTION_RESOURCE = "sector_comparisons_selection.json"
MAX_INPUT_BYTES = {
    "fec_financial_profiles": 256 * 1024**2,
    "lobbying_quarterly_profiles": 256 * 1024**2,
    "lobbying_activities": 1024 * 1024**2,
}
INPUT_COLUMNS = {
    "fec_financial_profiles": ("profile_id entity_kind entity_id period_start period_end selection_status "
                               "coverage_status receipts receipts_status disbursements disbursements_status").split(),
    "lobbying_quarterly_profiles": ("profile_id registrant_id client_id period_start period_end selection_status income income_status "
                                    "expenses expenses_status selected_filing_uuid selected_source_url").split(),
    "lobbying_activities": ["filing_uuid", "general_issue_code"],
}


def load_rollup_selection() -> dict:
    """Return the curated NAICS-2022 sector mapping, captures, and window scope."""
    return json.loads(files("spicy_regs.transforms").joinpath(SELECTION_RESOURCE).read_text())


def _check_input_file(path: Path, name: str, expected_hash: str | None = None) -> Path:
    if name not in INPUT_COLUMNS:
        raise ValueError("Unsupported sector prerequisite table")
    if path.stat().st_size > MAX_INPUT_BYTES[name]:
        raise ValueError("Sector prerequisite exceeds its bounded input size")
    if expected_hash is not None:
        with path.open("rb") as stream:
            digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != expected_hash:
            raise ValueError("Sector prerequisite table differs from its selected bytes")
    return path


def _verified_input_path(directory, name, expected_generation, expected_hash) -> Path:
    directory = Path(directory)
    admitted = verify_generation(directory)
    if admitted.pin.artifact_digest != expected_generation:
        raise ValueError("Sector prerequisite generation differs from its selected pin")
    return _check_input_file(directory / (name + ".parquet"), name, expected_hash)


def _read_table_from_path(path: Path, name: str, expected_hash: str | None = None) -> list[dict]:
    _check_input_file(path, name, expected_hash)
    # The accepted FEC profiles carry large complete witness JSON strings. The
    # comparison already pins their admitted generation; decoding those witnesses
    # again adds no input fact and dominated the initial memory measurement.
    return pq.read_table(path, columns=INPUT_COLUMNS[name]).to_pylist()


def _table(directory, name, expected_generation, expected_hash):
    path = _verified_input_path(directory, name, expected_generation, expected_hash)
    return _read_table_from_path(path, name)


def _iter_activities_from_path(path: Path, *, expected_hash: str | None = None, batch_size: int = 65_536):
    """Stream projected lobbying activity columns in bounded batches without materializing the full table."""
    _check_input_file(path, "lobbying_activities", expected_hash)
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=INPUT_COLUMNS["lobbying_activities"]):
        filing_uuids = batch.column("filing_uuid").to_pylist()
        issue_codes = batch.column("general_issue_code").to_pylist()
        for filing_uuid, issue_code in zip(filing_uuids, issue_codes, strict=True):
            yield {"filing_uuid": filing_uuid, "general_issue_code": issue_code}


def _iter_activities(directory, name, expected_generation, expected_hash, *, batch_size: int = 65_536):
    """Stream projected lobbying activity columns in bounded batches without materializing the full table."""
    path = _verified_input_path(directory, name, expected_generation, expected_hash)
    return _iter_activities_from_path(path, batch_size=batch_size)


def _retain_selection_evidence(evidence, *, scope: dict, mapping: dict, classification_captures: list[dict]) -> None:
    for name, capture in enumerate(classification_captures):
        _read(evidence, capture, f"sector-primary-{name}")
    evidence.retain_bytes(json_bytes({"scope": scope, "mapping": mapping}), stage="sector-selection")


def _write_comparison_tables(output_dir: Path, *, fec, lobbying, activities, mapping: dict, scope: dict) -> tuple[Path, ...]:
    fec_filtered = [
        r for r in fec
        if r["entity_kind"] == "committee"
        and r["period_start"] is not None
        and r["period_end"] is not None
        and r["period_start"].isoformat() == scope["period_start"]
        and r["period_end"].isoformat() == scope["period_end"]
    ]
    tables = compare(fec_filtered, lobbying, mapping=mapping, scope=scope)
    tables[ISSUES] = issue_views(lobbying, activities, source_generation=scope["lda"]["generation"])
    paths = []
    for name, schema in SCHEMAS.items():
        path = output_dir / (name + ".parquet")
        write_rows(tables[name], path, schema=schema)
        paths.append(path)
    return tuple(paths)


def build_tables_from_paths(
    evidence,
    *,
    scope: dict,
    mapping: dict,
    classification_captures: list[dict],
    fec_path: Path,
    lda_path: Path,
    lda_activities_path: Path,
    output_dir: Path,
    fec_expected_hash: str | None = None,
    lda_expected_hash: str | None = None,
    lda_activities_expected_hash: str | None = None,
) -> tuple[Path, ...]:
    """Verify primed table paths, retain selection/capture evidence, and write the three sector Parquet tables."""
    _retain_selection_evidence(evidence, scope=scope, mapping=mapping, classification_captures=classification_captures)
    fec = _read_table_from_path(fec_path, "fec_financial_profiles", fec_expected_hash)
    lobbying = _read_table_from_path(lda_path, "lobbying_quarterly_profiles", lda_expected_hash)
    activities = _iter_activities_from_path(lda_activities_path, expected_hash=lda_activities_expected_hash)
    return _write_comparison_tables(output_dir, fec=fec, lobbying=lobbying, activities=activities, mapping=mapping, scope=scope)


def build(selection, output_dir):
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh sector-comparison output directory")
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        scope = selection["scope"]
        for source in ("fec", "lda"):
            config = selection["inputs"][source]
            if (scope[source]["generation"] != config["expected_generation"]
                    or scope[source]["table_sha256"] != config["expected_hash"]):
                raise ValueError("Declared sector source scope differs from its prerequisite input pins")
        _retain_selection_evidence(
            evidence,
            scope=scope,
            mapping=selection["mapping"],
            classification_captures=selection["classification_captures"],
        )
        # Source originals are retained by their owning lanes. Pin the admitted
        # prerequisite roots here, without copying their tables into evidence.
        for source, config in selection["inputs"].items():
            evidence.retain_control(Path(config["directory"]) / "artifact.json", stage="sector-parent-" + source)
        fec = _table(**selection["inputs"]["fec"])
        lobbying = _table(**selection["inputs"]["lda"])
        activities = _iter_activities(**selection["inputs"]["lda_activities"])
        paths = _write_comparison_tables(
            output_dir,
            fec=fec,
            lobbying=lobbying,
            activities=activities,
            mapping=selection["mapping"],
            scope=scope,
        )
        artifact = build_generation(output_dir / "generation", family=FAMILY, files=paths,
            expected_keys=[p.name for p in paths], schemas={n: described_schema(s) for n, s in SCHEMAS.items()},
            inputs=evidence.inputs(), owned_output_root=output_dir)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(json.loads(args.selection.read_text()), args.output_dir)
    print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "evidence": str(evidence)}))


if __name__ == "__main__":
    main()
