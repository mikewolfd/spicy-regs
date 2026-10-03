"""Exercise actual LCV replacement and stale-write refusal using local storage."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from integrate_lcv import local_test_store
from qualify_lcv import digest, write
from spicy_regs.generations import verify_generation
from rulespec_artifacts import LocalMemberSource, admit_artifact
from spicy_regs.source_evidence import SourceEvidenceError, _verify_receipts, verify_evidence
from spicy_regs.sources import publication


def check(source_report, integration_dir, output):
    report = json.loads(source_report.read_bytes())
    directory = source_report.parent
    store = local_test_store()
    retained_store = integration_dir / "local-object-store"
    store.objects = {
        str(p.relative_to(retained_store)): p.read_bytes() for p in retained_store.rglob("*") if p.is_file()
    }
    initial = publication.parse_index(store.objects[publication.INDEX_V2_KEY])
    original_source = initial["families"]["scorecards"]
    pinned_bytes = {k: v for k, v in store.objects.items() if k.startswith(original_source["prefix"] + "/")}
    replacement_dir = directory / "replacement" / "generation"
    replacement = verify_generation(replacement_dir)
    evidence = list((directory / "replacement" / "source-evidence").glob("*/artifact"))
    assert len(evidence) == 1
    published = publication.publish_generation(
        replacement_dir,
        client=store,
        bucket="local-qualification",
        prior_index=initial,
        evidence_directories=tuple(evidence),
    )
    assert published["families"]["scorecards"]["artifactDigest"] == replacement.pin.artifact_digest
    assert published["families"]["scorecard-analysis"] == initial["families"]["scorecard-analysis"]
    assert set(original_source["tables"]) == set(published["families"]["scorecards"]["tables"])
    assert pinned_bytes == {key: store.objects[key] for key in pinned_bytes}
    pointer_before = store.objects[publication.INDEX_V2_KEY]
    try:
        publication.publish_generation(
            replacement_dir,
            client=store,
            bucket="local-qualification",
            prior_index=initial,
            evidence_directories=tuple(evidence),
        )
    except publication.PublicationError:
        assert store.objects[publication.INDEX_V2_KEY] == pointer_before
    else:
        raise AssertionError("A stale prior unexpectedly overwrote the selected generation")
    failure_evidence = []
    for case in report["failure_preservation"]:
        artifacts = list((directory / ("failure-" + case["case"]) / "source-evidence").glob("*/artifact"))
        assert len(artifacts) == 1
        source = LocalMemberSource(artifacts[0])
        artifact = admit_artifact(source)
        _verify_receipts(source, artifact)
        try:
            verify_evidence(artifacts[0])
        except SourceEvidenceError:
            pass
        else:
            raise AssertionError("Failed evidence unexpectedly qualified a generation")
        assert artifact.root["spec"]["outcome"] == "failed"
        assert not any(path.is_file() for path in (artifacts[0] / "blobs").rglob("*"))
        failure_evidence.append({"case": case["case"], "pin": artifact.pin.as_dict(), "raw_blobs": 0})
    result = {
        "status": "actual_local_replacement_and_stale_refusal_passed",
        "remote_writes": 0,
        "source_report_sha256": digest(source_report),
        "prior": report["source_generation"],
        "replacement": replacement.pin.as_dict(),
        "table_count": len(original_source["tables"]),
        "prior_generation_bytes_unchanged": True,
        "stale_writer_pointer_unchanged": True,
        "independent_analysis_family_unchanged": True,
        "failed_capture_evidence": failure_evidence,
    }
    write(output, result)
    print(json.dumps({key: result[key] for key in ("status", "table_count", "stale_writer_pointer_unchanged")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-report", "integration-dir", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    check(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
