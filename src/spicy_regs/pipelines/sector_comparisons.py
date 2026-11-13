"""Build sector comparisons from pinned FEC profiles and lobbying-lane facts."""

import argparse
import hashlib
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
INPUT_COLUMNS = {
    "fec_financial_profiles": ("profile_id entity_kind entity_id period_start period_end selection_status "
                               "coverage_status receipts receipts_status disbursements disbursements_status").split(),
    "lobbying_quarterly_profiles": ("profile_id registrant_id client_id period_start period_end selection_status income income_status "
                                    "expenses expenses_status selected_filing_uuid selected_source_url").split(),
    "lobbying_activities": ["filing_uuid", "general_issue_code"],
}


def _table(directory, name, expected_generation, expected_hash):
    directory = Path(directory)
    admitted = verify_generation(directory)
    if admitted.pin.artifact_digest != expected_generation:
        raise ValueError("Sector prerequisite generation differs from its selected pin")
    path = directory / (name + ".parquet")
    if path.stat().st_size > 64 * 1024**2:
        raise ValueError("Sector prerequisite exceeds its bounded input size")
    with path.open("rb") as stream:
        digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != expected_hash:
        raise ValueError("Sector prerequisite table differs from its selected bytes")
    # The accepted FEC profiles carry large complete witness JSON strings. The
    # comparison already pins their admitted generation; decoding those witnesses
    # again adds no input fact and dominated the initial memory measurement.
    if name not in INPUT_COLUMNS:
        raise ValueError("Unsupported sector prerequisite table")
    return pq.read_table(path, columns=INPUT_COLUMNS[name]).to_pylist()


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
        for name, capture in enumerate(selection["classification_captures"]):
            _read(evidence, capture, f"sector-primary-{name}")
        # Source originals are retained by their owning lanes. Pin the admitted
        # prerequisite roots here, without copying their tables into evidence.
        for source, config in selection["inputs"].items():
            evidence.retain_control(Path(config["directory"]) / "artifact.json", stage="sector-parent-" + source)
        evidence.retain_bytes(json_bytes({"scope": scope, "mapping": selection["mapping"]}), stage="sector-selection")
        fec = _table(**selection["inputs"]["fec"])
        fec = [r for r in fec if r["entity_kind"] == "committee" and r["period_start"] is not None and
               r["period_end"] is not None and r["period_start"].isoformat() == scope["period_start"] and
               r["period_end"].isoformat() == scope["period_end"]]
        lobbying = _table(**selection["inputs"]["lda"])
        activities = _table(**selection["inputs"]["lda_activities"])
        tables = compare(fec, lobbying, mapping=selection["mapping"], scope=scope)
        tables[ISSUES] = issue_views(lobbying, activities, source_generation=scope["lda"]["generation"])
        paths = []
        for name, schema in SCHEMAS.items():
            path = output_dir / (name + ".parquet")
            write_rows(tables[name], path, schema=schema)
            paths.append(path)
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
