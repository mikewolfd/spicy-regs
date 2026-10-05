"""Prepare current native members from the reviewed complete roster captures.

This explicit build preserves the captured published prior as lineage, rather
than converting old rows into new source facts. It performs no publication.
"""

import argparse
from pathlib import Path
import json

from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
from spicy_regs.sources.member_rosters import SELECTION


class CompleteRosterMembers(MembersRollup):
    """Only this explicit preparation command selects the complete rebuild."""

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return self.rebuild_complete_rosters(output_dir)


def prepare(work: Path):
    if work.exists():
        raise ValueError("Current member preparation requires a new work directory")
    pipeline = CompleteRosterMembers(output_dir=work, skip_upload=True)
    pipeline.run()
    (generation,) = (work / "generations").iterdir()
    artifact = verify_generation(generation)
    reader = SelectedPriors(work / "readback", root=work, public_url="")
    restored = {}
    for name in ("members", "member_terms", "member_party_affiliations"):
        path = reader.get(name)
        if path is None:
            raise ValueError(f"Complete member generation cannot restore {name}")
        restored[name] = str(path.resolve())
    if pipeline.source_evidence is None:
        raise ValueError("Complete member preparation must retain source evidence")
    result = {
        "format": "spicy-regs-current-members-preparation",
        "version": 1,
        "generation": str(generation.resolve()),
        "generationPin": artifact.pin.as_dict(),
        "sourceRosterSelection": json.loads(SELECTION.read_bytes()),
        "restoredProcessingInputs": restored,
        "evidence": str(pipeline.source_evidence.artifact_dir.resolve()),
        "capturedPrior": artifact.root["spec"]["readSnapshot"],
        "published": False,
    }
    (work / "members-prepared.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.work), indent=2))
