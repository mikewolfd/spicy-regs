"""Build a selected hospital cost-report cohort from one retained original."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_hospital_cost_reports import FAMILY, MEASURES, SCHEMAS, build_hospital_cost_reports


def build(selection, output_dir: Path):
    from spicy_docs.sources.hospital_cost_reports import PARSER_REVISION, open_hospital_archive

    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh hospital build output directory")
    output_dir.mkdir(parents=True)
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        # Pin caller scope without publishing its local original path.
        scope = {key: selection[key] for key in ("sha256", "byte_size", "form", "year", "source_url")}
        evidence.event("hospital-selection", selection=scope, parser_revision=PARSER_REVISION,
                       measures=selection["measures"], coverage="All rows of one pinned CMS highest-status extract")
        with open_hospital_archive(Path(selection["original"]), **{key: scope[key] for key in ("sha256", "byte_size", "form", "year")}) as reader:
            saved = evidence.retain_file(Path(selection["original"]), stage="hospital-native-original", raw_source=True,
                                         original_location=scope["source_url"], source_url=scope["source_url"],
                                         form=scope["form"], cohort_year=scope["year"])
            if saved["sha256"] != scope["sha256"] or saved["byte_size"] != scope["byte_size"]:
                raise ValueError("Hospital retained input differs from selection")
            evidence.event("hospital-archive-inventory", inventory=reader.inventory)
            files, counts = build_hospital_cost_reports(reader, output_dir / "tables", measures=selection["measures"])
        evidence.event("hospital-completion", row_counts=counts, all_selected_members_consumed=True)
        artifact = build_generation(
            output_dir / "generation", family=FAMILY, files=files,
            expected_keys=[path.name for path in files],
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(), owned_output_root=output_dir,
        )
        evidence.finish()
        return {"pin": artifact.pin.as_dict(), "evidence": str(evidence.artifact_dir), "row_counts": counts}
    except BaseException as error:
        evidence.finish(error)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    selection = json.loads(args.selection.read_text())
    if "measures" not in selection:
        selection["measures"] = list(MEASURES)
    print(json.dumps(build(selection, args.output_dir), indent=2))


if __name__ == "__main__":
    main()
