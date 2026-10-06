"""Run the actual website court bridge once, using its reviewed 900-second bound."""
from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

SOURCE = "07915382a7a76c284cd50f70d3e8f3a0d2ea8c4f"
SITE = "6ea9dec7cd26c2b5c2b19c80df5d53a48e2ad1ce"
PRODUCER = "6b6e39059f4b0129df1810da34834bc3568f7947"
GENERATION = "61ef48c6dbb34bb09353c2409fb2daf2"
ARTIFACT = "sha256:731ab961d8825f1f099581d70f4100fd3fb1a3d1fbc3d1aa3ecc85a75553f8f6"
IMPLEMENTATION = "sha256:36bedaf57c9f3730143b61e728bb8ff546bca3d46a21d3944e7edc5bd2d59928"
ROWS = 10798347
WITNESSES = [{"rows": ROWS, "witnesses": [{"body_version": None, "locator": None,
    "sha256": "sha256:361d1f3b8d28f391fd2c4cfd10367f40d42437f73412a460d4ea458cce855602",
    "source_id": "generations/court-opinions/f7cc67cc03bf7ef1a7d976d6eb74bf2654f7212b66ad785c633331a4cf377d41/court_opinions.parquet",
    "source_uri": None}]}]


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "site", "handoff", "evidence"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    for name, pin in (("source", SOURCE), ("site", SITE)):
        checkout = getattr(args, name).resolve()
        if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip() != pin:
            raise ValueError("Exact consumer checkout differs: " + name)
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=checkout, text=True):
            raise ValueError("Consumer checkout is dirty: " + name)
    scripts = args.site.resolve() / "scripts"
    sys.path.insert(0, str(scripts))
    import additional_native_coverage as adapter
    bridge = scripts / "restore_additional_coverage.py"
    os.environ.update(SPICYGOV_ADDITIONAL_COVERAGE_BRIDGE=str(bridge),
                      SPICYGOV_ADDITIONAL_COVERAGE_PYTHON=str(args.source.resolve() / ".venv/bin/python"))
    if sha(bridge) != "6de7be6139b8f47049e10496f94706c2b4b4c162df954bf040de19b7081bc685":
        raise ValueError("Maintained full bridge changed")
    manifest_path = Path(__file__).with_name("court-handoff.json")
    if sha(manifest_path) != "6059f0f50f15d502e3ed86f55423103e7ea517063042aedbac742e80cbdec1b0":
        raise ValueError("Sealed download manifest changed")
    manifest = json.loads(manifest_path.read_text())
    root = args.handoff.resolve() / "native-subset/court-opinions"
    artifact = json.loads((root / "build/generation/artifact.json").read_text())
    conversion = json.loads((root / "conversion.json").read_text())
    if (artifact["artifactDigest"] != ARTIFACT or conversion["source"]["checkout"] != PRODUCER
            or conversion["source"]["main"] != PRODUCER
            or conversion["generation"]["artifactDigest"] != ARTIFACT
            or conversion["generation"]["receiptGenerationId"] != GENERATION):
        raise ValueError("Original seal or producer differs")
    adapter.bridge(["--verify-artifacts"], [artifact])
    schema = adapter.bridge(["--schema", "court_opinions"])
    if schema["implementationSha256"] != IMPLEMENTATION:
        raise ValueError("Actual reader implementation differs from reviewed preflight")
    pins = {member["name"]: member for member in manifest["members"]}
    def member(name):
        relative = "court-opinions/build/generation/" + name
        pin = pins[relative]
        return {"path": str(root / "build/generation" / name),
                "sha256": "sha256:" + pin["sha256"], "byteSize": pin["bytes"]}
    args.evidence.mkdir(parents=True, exist_ok=False)
    request = {"dataset": "court_opinions", "generationId": GENERATION,
               "subjects": [member("court_opinions.parquet")], "receipts": member("etl_receipts.parquet"),
               "destination": str(args.evidence.resolve().parent / "private-restored")}
    (args.evidence / "request.json").write_text(json.dumps(request, indent=2) + "\n")
    report = {"observedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "status": "FAIL", "sourceRevision": SOURCE, "websiteRevision": SITE,
              "producerRevision": PRODUCER, "artifactDigest": ARTIFACT,
              "generationId": GENERATION, "expectedRows": ROWS, "timeoutSeconds": 900,
              "downloadsInsideTimer": False, "schemaPreflight": schema,
              "runnerSha256": sha(Path(__file__)), "helperSha256": sha(Path(__file__).with_name("download_court_handoff.py")),
              "manifestSha256": sha(manifest_path), "bridgeSha256": sha(bridge),
              "adapterSha256": sha(scripts / "additional_native_coverage.py"),
              "runtime": {name: importlib.metadata.version(name) for name in ("duckdb", "pyarrow", "spicy-docs", "rulespec-artifacts")},
              "scope": "Complete input hashing, generation checks, admission, replay, accepted witnesses, counts and final output hash within the actual adapter bound. No capture, conversion or publication."}
    wrapper = Path(__file__).with_name("instrument_bridge.py").resolve()
    if sha(wrapper) != "468256bf63640256a07be58f3ba66e4868462270204af4fce5d4b885a084826b":
        raise ValueError("Reviewed instrumentation differs")
    stages = args.evidence.resolve() / "stages"
    os.environ.update(SPICYGOV_ADDITIONAL_COVERAGE_BRIDGE=str(wrapper),
                      SPICYGOV_COURT_STAGE_SOURCE=str(args.source.resolve()),
                      SPICYGOV_COURT_STAGE_SITE=str(args.site.resolve()),
                      SPICYGOV_COURT_STAGE_DIR=str(stages))
    report.update(instrumentationSha256=sha(wrapper),
                  instrumentationScope="Transparent wrappers around the actual maintained calls; full adapter timer includes instrumentation and all bridge checks",
                  privateInventoryScope="Small file sizes and available Parquet footers only, captured after the timed call; private payloads excluded from upload")
    started = time.monotonic()
    try:
        result = adapter.bridge(request=request)
        elapsed = time.monotonic() - started
        (args.evidence / "stdout.json").write_text(json.dumps(result, indent=2) + "\n")
        if (elapsed >= 900 or result["rows"] != ROWS or result["generationId"] != GENERATION
                or result["dataset"] != "court_opinions" or result["implementationSha256"] != IMPLEMENTATION
                or result["selection"] != {k: request[k] for k in ("subjects", "receipts")}
                or result["acceptedWitnessGroups"] != WITNESSES
                or result["processingSchema"] != schema["processingSchema"]
                or len(result["processingMembers"]) != 1
                or result["processingMembers"][0]["byteSize"] <= 0
                or not result["processingMembers"][0]["sha256"].startswith("sha256:")):
            raise ValueError("Complete bridge result differs from sealed candidate or bound")
        outcome = json.loads((stages / "instrumentation-outcome.json").read_text())
        identity = json.loads((stages / "instrumentation-identity.json").read_text())
        events = [json.loads(line) for line in (stages / "stages.jsonl").read_text().splitlines()]
        started_events = [e for e in events if e["status"] == "started"]
        terminal_events = [e for e in events if e["status"] in {"completed", "failed", "closed"}]
        started_calls = {e["call"] for e in started_events}
        terminal_calls = {e["call"] for e in terminal_events}
        required_phases = {"inputs.checked_member", "processing.select_receipts",
                           "processing.restore_processing_input", "witnesses.select_receipts",
                           "witnesses.read_attempts_and_caller_grouping", "files.file_hash",
                           "admission._check_receipts", "admission._check_subjects", "bridge.main"}
        completed_phases = {e["phase"] for e in terminal_events if e["status"] == "completed"}
        phases_by_call = {e["call"]: e["phase"] for e in started_events}
        complete_stage_log = (started_calls == terminal_calls
            and len(started_calls) == len(started_events) == len(terminal_events)
            and all(phases_by_call[e["call"]] == e["phase"] for e in terminal_events)
            and required_phases.issubset(completed_phases)
            and len([e for e in terminal_events if e["phase"] == "bridge.main" and e["status"] == "completed"]) == 1)
        if (outcome["status"] != "COMPLETE_WITH_CHECKS" or outcome["instrumentationFailures"]
                or identity["implementationSha256"] != IMPLEMENTATION
                or identity["instrumentationSha256"] != sha(wrapper)
                or identity["sourceRevision"] != SOURCE or identity["websiteRevision"] != SITE
                or not complete_stage_log):
            raise ValueError("Complete instrumentation evidence differs")
        report.update(status="PASS", restoration=result, instrumentationOutcome=outcome)
    except Exception as error:
        report.update(errorType=type(error).__name__, error=str(error))
        for attribute, filename in (("stdout", "stdout.json"), ("stderr", "stderr.log")):
            value = getattr(error, attribute, None)
            if value is not None:
                (args.evidence / filename).write_text(value.decode(errors="replace") if isinstance(value, bytes) else value)
    finally:
        report["seconds"] = time.monotonic() - started
        import pyarrow.parquet as pq
        inventory = []
        private = args.evidence.resolve().parent / "private-restored"
        if private.exists():
            for path in sorted(private.rglob("*")):
                if not path.is_file():
                    continue
                item = {"relativePath": str(path.relative_to(private)), "bytes": path.stat().st_size,
                        "modifiedUnixSeconds": path.stat().st_mtime}
                try:
                    metadata = pq.read_metadata(path)
                    item.update(footerReadable=True, rows=metadata.num_rows, rowGroups=metadata.num_row_groups)
                except Exception as error:
                    item.update(footerReadable=False, footerErrorType=type(error).__name__)
                inventory.append(item)
        (args.evidence / "private-file-inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        report["childResources"] = {"maxRssObserved": usage.ru_maxrss, "maxRssUnit": "KiB",
                                  "userSeconds": usage.ru_utime, "systemSeconds": usage.ru_stime,
                                  "scope": "Cumulative children including small git and schema preflight calls"}
        (args.evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({k: report[k] for k in ("status", "seconds", "timeoutSeconds")}), flush=True)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
