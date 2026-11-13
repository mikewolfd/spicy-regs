"""Build an explicit quarterly comparison from one pinned native LDA table; no source fetch or publication."""

import argparse
import json
from pathlib import Path

from rulespec_artifacts import LocalMemberSource, admit_artifact, iter_member_descriptors

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_lobbying_quarterly_profiles import OUTPUT, SCHEMA, TABLE, build_profiles, json_bytes

FAMILY = "lobbying-quarterly-profiles"


def source_pin(selection, source):
    """Bind the chosen table digest to its admitted source root, without copying sibling tables."""
    source = Path(source)
    if source.name != "lobbying_filings.parquet":
        raise ValueError("Select lobbying_filings.parquet inside its source generation directory")
    members = LocalMemberSource(source.parent)
    artifact = admit_artifact(members)
    if artifact.root["spec"]["family"] != "lobbying-filings" or artifact.pin.artifact_digest != selection["source_table_generation"]:
        raise ValueError("Quarterly source generation differs from the selected lobbying-filings root")
    descriptor = next((item for item in iter_member_descriptors(artifact, members) if item.object_key == source.name), None)
    if descriptor is None or descriptor.sha256 != selection["source_table_sha256"]:
        raise ValueError("Quarterly table SHA-256 differs from its admitted source member")
    return artifact


def build(selection, source, output_dir):
    output_dir = Path(output_dir).resolve()
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh quarterly profile build directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        artifact = source_pin(selection, source)
        evidence.retain_bytes(json_bytes(artifact.root), stage="quarterly-source-root")
        evidence.retain_bytes(json_bytes(selection), stage="quarterly-selection")
        summary = build_profiles(selection, Path(source), output_dir / OUTPUT)
        evidence.event("selected-native-table", stage="quarterly-input", table="lobbying_filings",
                       artifact_digest=selection["source_table_generation"], sha256=selection["source_table_sha256"])
        # selected_inputs describes publisher originals, not derived tables. Keep
        # this input's admitted root and verified member pins without relabeling
        # it as an original or claiming publisher-wide input closure.
        evidence.retain_bytes(json_bytes(summary), stage="quarterly-reconciliation")
        artifact = build_generation(
            output_dir / "generation", family=FAMILY, files=(output_dir / OUTPUT,), expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)}, inputs=evidence.inputs(), owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir, summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True, help="The exact native lobbying_filings.parquet named by selection")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    artifact, evidence, summary = build(json.loads(args.selection.read_text()), args.source, args.output_dir)
    print(json.dumps({"generation": artifact.pin.artifact_digest, "evidence": str(evidence), "population": summary}))


if __name__ == "__main__":
    main()
