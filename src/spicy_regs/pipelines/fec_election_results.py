"""Build one local Federal Elections 2022 Senate generation from retained originals."""

import argparse
import hashlib
import inspect
import json
from pathlib import Path

from spicy_docs.sources.fec.election_results import workbook_rows
from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.raw_releases import raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.fec_election_results import SCHEMAS, interpret

FAMILY = "fec-election-results"


def build(capture, output_dir):
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh election-result output directory")
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        # Coordinated source branches can be tested before the coordinator cuts
        # a shared package. Retain the actual source code identity separately
        # from installed distribution metadata; never call that a package release.
        from spicy_docs.sources.scorecards import workbooks

        reader_code = {}
        for module in (inspect.getmodule(workbook_rows), workbooks):
            reader_code[module.__name__] = "sha256:" + hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        evidence.retain_bytes(json.dumps(reader_code, sort_keys=True).encode(), stage="election-reader-code-identities")
        original = raw_original(evidence.store, capture["response_sha256"], capture["byte_size"])
        saved = evidence.retain_file(original, stage="fec-results-workbook", raw_source=True,
            source_url=capture["request_url"], original_location=capture["request_url"],
            observed_at=capture["observed_at"])
        if saved["sha256"] != capture["response_sha256"]:
            raise ValueError("Election original digest changed")
        if capture["byte_size"] > 8 * 1024**2:
            raise ValueError("Election workbook exceeds its reviewed size bound")
        rows = workbook_rows(original.read_bytes(), sha256=capture["response_sha256"], cycle=2022)
        tables = interpret(rows, capture)
        files = []
        for name, schema in SCHEMAS.items():
            path = output_dir / (name + ".parquet")
            write_rows(tables[name], path, schema=schema)
            files.append(path)
        artifact = build_generation(output_dir / "generation", family=FAMILY, files=files,
            expected_keys=[p.name for p in files], schemas={n: described_schema(s) for n, s in SCHEMAS.items()},
            inputs=evidence.inputs(), owned_output_root=output_dir)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(json.loads(args.capture.read_text()), args.output_dir)
    print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "evidence": str(evidence)}))


if __name__ == "__main__":
    main()
