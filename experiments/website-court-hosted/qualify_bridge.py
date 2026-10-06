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

SOURCE = "ee2de4a1c42a04970bcc87065ad3b7ffecd3662f"
SITE = "7553c697014cc3e897119f54174587529c13c2bf"
PRODUCER = "6b6e39059f4b0129df1810da34834bc3568f7947"
GENERATION = "61ef48c6dbb34bb09353c2409fb2daf2"
ARTIFACT = "sha256:731ab961d8825f1f099581d70f4100fd3fb1a3d1fbc3d1aa3ecc85a75553f8f6"
IMPLEMENTATION = "sha256:48b5e613f87bd8c53039387bb0072c351632620a6f80bfa90c3fb35b1840fce1"
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
        report.update(status="PASS", restoration=result)
    except Exception as error:
        report.update(errorType=type(error).__name__, error=str(error))
        for attribute, filename in (("stdout", "stdout.json"), ("stderr", "stderr.log")):
            value = getattr(error, attribute, None)
            if value is not None:
                (args.evidence / filename).write_text(value.decode(errors="replace") if isinstance(value, bytes) else value)
    finally:
        report["seconds"] = time.monotonic() - started
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        report["childResources"] = {"maxRssObserved": usage.ru_maxrss, "maxRssUnit": "KiB",
                                  "userSeconds": usage.ru_utime, "systemSeconds": usage.ru_stime,
                                  "scope": "Cumulative children including small git and schema preflight calls"}
        (args.evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({k: report[k] for k in ("status", "seconds", "timeoutSeconds")}), flush=True)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
