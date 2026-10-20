"""Build an ordinary Treasury generation from explicit local source releases."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_treasury_fiscal import SCHEMAS, build_treasury_fiscal

MAX_REPORT_BYTES = 3 * 1024 * 1024


def build(readers, output_dir: Path, *, debt_dates, report_dates, reports):
    """Retain admitted pages and the explicit report controls through the existing owner."""
    from spicy_docs.sources.treasury_fiscal import MAX_BYTES

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each Treasury generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, "treasury-fiscal")
    try:
        qualifications = {}
        for report_date, control in reports.items():
            with Path(control["path"]).open("rb") as stream:
                body = stream.read(MAX_REPORT_BYTES + 1)
            if len(body) > MAX_REPORT_BYTES or "sha256:" + hashlib.sha256(body).hexdigest() != control["sha256"]:
                raise ValueError("Treasury report control differs from its explicit byte bound or digest")
            evidence.retain_bytes(body, stage="treasury-qualified-report", report_date=report_date.isoformat(),
                                  source_url=control["url"], expected_sha256=control["sha256"])
            qualifications[report_date] = {"url": control["url"], "sha256": control["sha256"]}
        files = build_treasury_fiscal(
            readers, output_dir, debt_dates=debt_dates, report_dates=report_dates, qualifications=qualifications,
        )
        retained = set()
        for reader in readers:
            evidence.event("treasury-source-release", source_logical_id=reader.pin.logical_id,
                           source_artifact_digest=reader.pin.artifact_digest)
            for entry in reader.iter_record_evidence():
                ref = entry["evidenceBlobRef"]
                if ref not in retained:
                    evidence.retain_bytes(reader.read_evidence(ref, max_bytes=MAX_BYTES),
                                          stage="treasury-source-page", evidence_blob_ref=ref)
                    retained.add(ref)
        artifact = build_generation(
            output_dir / "generation", family="treasury-fiscal", files=files,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", required=True, type=Path, help="Explicit source pins, dates and report controls")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.sources.treasury_fiscal_profile import TREASURY_FISCAL_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    selection = json.loads(args.selection.read_text())
    readers = [SourceNativeReleaseReader(
        LocalMemberSource(item["release"]), blob_source=LocalSourceNativeBlobStore(item["blobs"]),
        profile=TREASURY_FISCAL_PROFILE,
        accepted_verifier_implementation_ids=frozenset({item["verifierImplementationId"]}),
        expected_pin=ArtifactPin(item["logicalId"], item["artifactDigest"]),
    ) for item in selection["sources"]]
    artifact, evidence = build(
        readers, args.output_dir,
        debt_dates=tuple(map(date.fromisoformat, selection["debt_dates"])),
        report_dates=tuple(map(date.fromisoformat, selection["report_dates"])),
        reports={date.fromisoformat(key): value for key, value in selection["reports"].items()},
    )
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest}))


if __name__ == "__main__":
    main()
