"""Build one multi-cycle official FEC election results generation from retained originals."""

import argparse
import hashlib
import inspect
import json
from pathlib import Path
from typing import Any

from spicy_docs.sources.fec.election_results import workbook_rows
from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.raw_releases import raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.fec_election_results import (
    NATIVE,
    RESULTS,
    SCHEMAS,
    interpret,
    reconcile_general_contests,
)

FAMILY = "fec-election-results"
MAX_WORKBOOK_BYTES = 16 * 1024**2


def _normalize_editions(selection_or_capture: dict[str, Any], *, default_cycle: int = 2022) -> list[dict[str, Any]]:
    if "editions" in selection_or_capture:
        raw_editions = selection_or_capture["editions"]
        if not isinstance(raw_editions, list) or not raw_editions:
            raise ValueError("Election selection must provide a non-empty editions list")
        editions = [dict(entry) for entry in raw_editions]
    else:
        entry = dict(selection_or_capture)
        entry.setdefault("cycle", default_cycle)
        editions = [entry]

    seen_cycles: set[int] = set()
    seen_digests: set[str] = set()
    for entry in editions:
        cycle = entry.get("cycle")
        if type(cycle) is not int:
            raise ValueError(f"Election edition requires an integer cycle, got {cycle!r}")
        if cycle in seen_cycles:
            raise ValueError(f"Duplicate election cycle in selection: {cycle}")
        digest = entry.get("response_sha256")
        if not isinstance(digest, str) or not digest.startswith("sha256:"):
            raise ValueError(f"Election edition {cycle} requires a sha256 response_sha256")
        if digest in seen_digests:
            raise ValueError(f"Duplicate workbook digest in selection: {digest}")
        seen_cycles.add(cycle)
        seen_digests.add(digest)
    return sorted(editions, key=lambda e: e["cycle"])


def build(selection_or_capture: dict[str, Any], output_dir: str | Path, *, cycle: int = 2022):
    """Build one receipts-free generation from one capture or a multi-edition selection manifest."""
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh election-result output directory")
    editions = _normalize_editions(selection_or_capture, default_cycle=cycle)
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        # Coordinated source branches can be tested before the coordinator cuts
        # a shared package. Retain the actual source code identity separately
        # from installed distribution metadata; never call that a package release.
        from spicy_docs.sources.scorecards import workbooks

        reader_code = {}
        for module in (inspect.getmodule(workbook_rows), workbooks):
            if module is None or module.__file__ is None:
                raise ValueError("Election reader source module identity is unavailable")
            reader_code[module.__name__] = "sha256:" + hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        evidence.retain_bytes(json.dumps(reader_code, sort_keys=True).encode(), stage="election-reader-code-identities")

        all_native: list[dict[str, Any]] = []
        all_results: list[dict[str, Any]] = []
        for capture in editions:
            edition_cycle = capture["cycle"]
            original = raw_original(evidence.store, capture["response_sha256"], capture["byte_size"])
            saved = evidence.retain_file(
                original,
                stage=f"fec-results-workbook-{edition_cycle}",
                raw_source=True,
                source_url=capture["request_url"],
                original_location=capture["request_url"],
                observed_at=capture["observed_at"],
            )
            if saved["sha256"] != capture["response_sha256"]:
                raise ValueError("Election original digest changed")
            if capture["byte_size"] > MAX_WORKBOOK_BYTES:
                raise ValueError(f"Election workbook {edition_cycle} exceeds its reviewed size bound")
            rows = workbook_rows(original.read_bytes(), sha256=capture["response_sha256"], cycle=edition_cycle)
            tables = interpret(rows, capture)
            all_native.extend(tables[NATIVE])
            all_results.extend(tables[RESULTS])

        reconciliation = reconcile_general_contests(all_results)
        evidence.retain_bytes(
            json.dumps(
                {
                    "cycles": [e["cycle"] for e in editions],
                    "native_rows": len(all_native),
                    "result_rows": len(all_results),
                    **reconciliation,
                },
                sort_keys=True,
            ).encode(),
            stage="fec-results-reconciliation",
        )

        combined = {NATIVE: all_native, RESULTS: all_results}
        files = []
        for name, schema in SCHEMAS.items():
            path = output_dir / (name + ".parquet")
            write_rows(combined[name], path, schema=schema)
            files.append(path)
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=files,
            expected_keys=[p.name for p in files],
            schemas={n: described_schema(s) for n, s in SCHEMAS.items()},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--selection", type=Path, help="Multi-edition JSON manifest with an 'editions' list.")
    group.add_argument("--capture", type=Path, help="Single-edition capture JSON (backward-compatible).")
    parser.add_argument("--cycle", type=int, default=2022, help="Default cycle when using --capture without cycle.")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest_path = args.selection or args.capture
    artifact, evidence = build(json.loads(manifest_path.read_text()), args.output_dir, cycle=args.cycle)
    print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "evidence": str(evidence)}))


if __name__ == "__main__":
    main()
