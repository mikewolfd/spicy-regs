"""Hosted dispatch refuses a changed whole prior before starting a conversion."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from scripts.native_conversion_invocation import check_prepared_prior, entry_digest, record_invocation


def prior():
    return {"artifactDigest": "sha256:" + "a" * 64, "publishedAt": "2026-10-03T00:00:00Z",
            "tables": {"dockets.parquet": {"rows": 3, "byteSize": 40, "sha256": "b" * 64}}}


def run(tmp_path, selected, **overrides):
    args = {"family": "dockets", "expected_entry": entry_digest(prior()), "publish": False,
            "source_run": "", "bucket": "", "output": tmp_path / "invocation",
            "public_url": "https://public.example", "read_index": lambda _: {"families": {"dockets": selected}}}
    args.update(overrides)
    return record_invocation(**args)


def test_preparation_records_full_entry_and_has_no_publication_target(tmp_path):
    result = run(tmp_path, prior())
    assert result["entry"] == prior()
    assert not result["publicationRequested"] and result["expectedBucket"] is None
    assert json.loads((tmp_path / "invocation/invocation.json").read_text()) == result


@pytest.mark.parametrize("change", ["timestamp", "rows", "member", "native", "missing"])
def test_changed_full_entry_refuses_without_writing_invocation(tmp_path, change):
    entry = deepcopy(prior())
    if change == "timestamp":
        entry["publishedAt"] = "2026-10-04T00:00:00Z"
    elif change == "rows":
        entry["tables"]["dockets.parquet"]["rows"] = 4
    elif change == "member":
        entry["tables"]["dockets.parquet"]["sha256"] = "c" * 64
    elif change == "native":
        entry["etlReceipts"] = {"rows": 3}
    else:
        entry = None
    with pytest.raises(ValueError, match="prior entry changed"):
        run(tmp_path, entry)
    assert not (tmp_path / "invocation").exists()


@pytest.mark.parametrize("overrides", [
    {"family": "../../other"}, {"expected_entry": "a" * 64}, {"publish": True},
    {"source_run": "123"}, {"publish": True, "bucket": "named", "source_run": "../123"},
    {"public_url": "http://public.example"},
])
def test_invalid_dispatch_refuses_before_any_public_metadata_read(tmp_path, overrides):
    def unexpected(_):
        pytest.fail("Invalid dispatch must refuse before reading published data")
    with pytest.raises(ValueError):
        run(tmp_path, prior(), read_index=unexpected, **overrides)


def test_explicit_prior_run_is_recorded_without_rebuilding(tmp_path):
    result = run(tmp_path, prior(), publish=True, bucket="explicit-production", source_run="12345")
    assert result["sourceRunId"] == "12345"
    assert result["publicationRequested"] and result["expectedBucket"] == "explicit-production"


def test_already_native_family_refuses_even_with_its_matching_digest(tmp_path):
    entry = prior()
    entry["etlReceipts"] = {"rows": 3}
    with pytest.raises(ValueError, match="already native"):
        run(tmp_path, entry, expected_entry=entry_digest(entry))
    assert not (tmp_path / "invocation").exists()


def test_timestamp_only_change_after_preflight_refuses_the_later_capture(tmp_path):
    invocation = run(tmp_path, prior())
    changed = prior()
    changed["publishedAt"] = "2026-10-04T00:00:00Z"
    receipt = tmp_path / "conversion.json"
    receipt.write_text(json.dumps({"family": "dockets", "captured": {"entry": changed}}))
    before = receipt.read_bytes()
    with pytest.raises(ValueError, match="different complete prior"):
        check_prepared_prior(family="dockets", expected_entry=invocation["expectedEntrySha256"],
                             receipt_path=receipt)
    assert receipt.read_bytes() == before


@pytest.mark.parametrize("mode", ["prepare", "download"])
@pytest.mark.parametrize("change", ["same", "timestamp", "family", "missing_capture"])
def test_actual_workflow_step_checks_both_artifact_paths_before_publication(tmp_path, mode, change):
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.safe_load((root / ".github/workflows/convert-native-family.yml").read_text())
    steps = workflow["jobs"]["convert"]["steps"]
    check_index = next(i for i, step in enumerate(steps)
                       if step.get("name") == "Check the prepared artifact captured the exact requested prior")
    check = steps[check_index]
    assert "if" not in check  # Both a new preparation and a downloaded artifact must pass this step.
    assert next(i for i, step in enumerate(steps) if step.get("uses") == "actions/download-artifact@v4") < check_index
    assert next(i for i, step in enumerate(steps) if "prepare.log" in step.get("run", "")) < check_index
    assert next(i for i, step in enumerate(steps) if "--publish-prepared" in step.get("run", "")) > check_index
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "native_conversion_invocation.py").symlink_to(root / "scripts/native_conversion_invocation.py")
    uv = tmp_path / "uv"
    uv.write_text('#!/bin/bash\nset -euo pipefail\nshift 3\nexec "$TEST_PYTHON" "$@"\n')
    uv.chmod(0o755)
    entry = prior()
    candidate = {"family": "dockets", "captured": {"entry": entry}}
    if change == "timestamp":
        entry["publishedAt"] = "2026-10-04T00:00:00Z"
    elif change == "family":
        candidate["family"] = "documents"
    elif change == "missing_capture":
        candidate.pop("captured")
    receipt = tmp_path / "output/native/dockets/conversion.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps(candidate))
    before = receipt.read_bytes()
    env = os.environ | {"PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
                        "TEST_PYTHON": sys.executable, "GITHUB_WORKSPACE": str(tmp_path),
                        "FAMILY": "dockets", "ENTRY_SHA256": entry_digest(prior()),
                        "SOURCE_RUN_ID": "123" if mode == "download" else ""}
    result = subprocess.run(["bash", "-c", check["run"]], cwd=tmp_path, env=env,
                            capture_output=True, text=True)
    assert (result.returncode == 0) == (change == "same"), result.stderr
    assert (tmp_path / "output/invocation/prepared-prior.json").exists() == (change == "same")
    assert receipt.read_bytes() == before
