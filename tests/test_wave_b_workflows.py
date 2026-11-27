"""Validation tests for the Wave B manual-dispatch refresh workflows (ss-wjvp AC #3)."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

import pytest
import yaml

from spicy_regs import local_publication
from spicy_regs.source_evidence import CaptureEvidence

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
HOUSE_WORKFLOW = WORKFLOWS / "refresh-house-disclosures.yml"
FEC_WORKFLOW = WORKFLOWS / "refresh-fec-research-profiles.yml"
HOUSE_FIXTURES = Path(__file__).parent / "fixtures" / "house_disclosures"
FEC_FIXTURES = Path(__file__).parent / "fixtures" / "fec-research-profiles"


def _load_workflow(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(doc: dict) -> dict:
    # PyYAML 1.1 parses unquoted top-level key `on:` as boolean True.
    return doc.get("on", doc.get(True, {}))


def _steps_by_name(doc: dict) -> dict[str, dict]:
    return {step["name"]: step for step in doc["jobs"]["run"]["steps"] if "name" in step}


def _help_flags(argv: list[str]) -> set[str]:
    result = subprocess.run(argv, cwd=REPO_ROOT, capture_output=True, text=True, check=True, timeout=30)
    return set(re.findall(r"--[a-z][a-z0-9-]*", result.stdout))


def _clean_env(extra: dict[str, str]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in {"PYTEST_CURRENT_TEST", "SPICYGOV_RAW_ROOT"}}
    env.update(extra)
    return env


@pytest.mark.parametrize(
    ("path", "expected_group", "expected_family"),
    [
        (HOUSE_WORKFLOW, "refresh-house-disclosures", "house-disclosures"),
        (FEC_WORKFLOW, "refresh-fec-research-profiles", "fec-research-profiles"),
    ],
    ids=["house-disclosures", "fec-research-profiles"],
)
def test_wave_b_workflows_have_valid_structure_permissions_concurrency_and_safe_defaults(
    path: Path, expected_group: str, expected_family: str
):
    assert path.is_file(), f"missing {path}"
    raw_text = path.read_text(encoding="utf-8")
    doc = yaml.safe_load(raw_text)
    assert isinstance(doc, dict)
    assert isinstance(doc.get("name"), str) and doc["name"].strip()

    triggers = _triggers(doc)
    assert set(triggers) == {"workflow_dispatch"}, "Wave B refresh workflows must be manual-dispatch only"
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["skip_upload"]["type"] == "boolean"
    assert inputs["skip_upload"]["default"] is True

    assert doc["permissions"] == {"contents": "read"}
    assert doc["concurrency"] == {"group": expected_group, "cancel-in-progress": False}

    # Neither family has a run-rollup-* console script; both stay in LOCAL_FAMILIES.
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]
    assert f"run-rollup-{expected_family}" not in scripts
    assert expected_family in local_publication.LOCAL_FAMILIES
    assert "run-rollup-" not in raw_text
    assert "_rollup.yml" not in raw_text

    # Env hygiene: never hardcode shrink overrides or paid-route tokens.
    assert "R2_ALLOW_SHRINK=" not in raw_text
    assert "R2_MIN_SIZE_RATIO=" not in raw_text
    assert "secrets.ZYTE_TOKEN" not in raw_text
    assert "secrets.FIRECRAWL_API_KEY" not in raw_text
    assert "--senate-paid-ladder" not in raw_text

    # Every run step must pass bash syntax validation and keep temp files under $RUNNER_TEMP.
    job = doc["jobs"]["run"]
    assert job["runs-on"] == "ubuntu-latest"
    assert isinstance(job["timeout-minutes"], int) and 0 < job["timeout-minutes"] <= 120
    steps = _steps_by_name(doc)
    for name, step in steps.items():
        if "run" in step:
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)
            if name not in {"Install pinned dependencies", "Retain invocation scope"}:
                assert 'TMPDIR="$RUNNER_TEMP"' in step["run"], f"{path.name}:{name} must pin TMPDIR to $RUNNER_TEMP"

    # Publish step is strictly gated on skip_upload == false and sets explicit User-Agent.
    publish = steps["Publish candidate generation"]
    assert publish["if"] == "inputs.skip_upload == false"
    assert "WORKFLOW_USER_AGENT" in publish["env"]
    assert '-H "User-Agent: $WORKFLOW_USER_AGENT"' in publish["run"]
    assert "unset SPICYGOV_RAW_ROOT R2_ALLOW_SHRINK R2_MIN_SIZE_RATIO" in publish["run"]
    assert f"--family {expected_family}" in publish["run"]
    assert "publish-local-generation" in publish["run"]

    # R2 write credentials are scoped only to the guarded publish step.
    for name, step in steps.items():
        if name != "Publish candidate generation":
            step_env = step.get("env", {})
            assert "R2_ACCESS_KEY_ID" not in step_env
            assert "R2_SECRET_ACCESS_KEY" not in step_env


def test_workflow_cli_flags_match_live_help_parsers():
    """Every CLI command and flag referenced by the workflows must exist in the real parsers."""
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]
    assert "build-fec-research-profiles" in scripts
    assert "publish-local-generation" in scripts
    assert "spicy-regs" in scripts

    house_base = [sys.executable, "-m", "spicy_regs.pipelines.house_disclosures"]
    acquire_index_flags = _help_flags([*house_base, "acquire-index", "--help"])
    acquire_indexes_flags = _help_flags([*house_base, "acquire-indexes", "--help"])
    acquire_flags = _help_flags([*house_base, "acquire", "--help"])
    house_build_flags = _help_flags([*house_base, "build", "--help"])
    fec_build_flags = _help_flags([sys.executable, "-m", "spicy_regs.pipelines.fec_research_profiles", "--help"])
    publish_flags = _help_flags([sys.executable, "-m", "spicy_regs.local_publication", "--help"])
    mirror_flags = _help_flags([sys.executable, "-m", "spicy_regs.cli", "mirror", "--help"])

    assert {"--year", "--output-dir", "--archive-root", "--captures-out"} <= acquire_index_flags
    assert {"--year", "--max-requests", "--output-dir", "--archive-root", "--captures-out"} <= acquire_indexes_flags
    assert {"--selection", "--doc-id", "--max-requests", "--output-dir", "--archive-root", "--captures-out"} <= acquire_flags
    assert {"--selection", "--output-dir", "--archive-root", "--read-index"} <= house_build_flags
    assert {"--selection", "--output-dir"} <= fec_build_flags
    assert {"--generation", "--family", "--evidence", "--expect-prior", "--output"} <= publish_flags
    assert {"--family", "--min-free-gib"} <= mirror_flags


@pytest.mark.parametrize(
    ("env_overrides", "expected_error"),
    [
        ({"HOUSE_YEARS": ""}, "years is required"),
        ({"HOUSE_YEARS": "1999"}, "out of supported range"),
        ({"HOUSE_YEARS": "2025,2025"}, "Duplicate House index year"),
        ({"HOUSE_YEARS": "2025", "HOUSE_DOC_IDS": "20030591", "HOUSE_MAX_PTRS": "2"}, "not both"),
        ({"HOUSE_YEARS": "2025", "HOUSE_MAX_REQUESTS": "0"}, "Invalid max_requests"),
        ({"HOUSE_YEARS": "2024,2025", "HOUSE_MAX_REQUESTS": "1"}, "exceeding max_requests=1"),
        ({"HOUSE_YEARS": "2025", "HOUSE_MAX_PTRS": "3", "HOUSE_MAX_REQUESTS": "2"}, "exceeds max_requests"),
        (
            {"HOUSE_YEARS": "2025", "HOUSE_DOC_IDS": "20030591,20030594,8220747", "HOUSE_MAX_REQUESTS": "2"},
            "exceeding max_requests=2",
        ),
        ({"HOUSE_YEARS": "2025", "HOUSE_EXPECT_PRIOR": "not-hex"}, "expect_prior"),
    ],
)
def test_house_workflow_validation_fails_closed_on_invalid_inputs(tmp_path: Path, env_overrides, expected_error):
    step = _steps_by_name(_load_workflow(HOUSE_WORKFLOW))["Validate inputs"]
    github_output = tmp_path / "github_output.txt"
    github_output.write_text("", encoding="utf-8")
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(tmp_path.resolve()),
                "GITHUB_OUTPUT": str(github_output),
                "HOUSE_YEARS": "2025",
                "HOUSE_DOC_IDS": "",
                "HOUSE_MAX_PTRS": "0",
                "HOUSE_MAX_REQUESTS": "16",
                "HOUSE_EXPECT_PRIOR": "",
                **env_overrides,
            }
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert expected_error in (result.stderr + result.stdout)


@pytest.mark.parametrize(
    ("years_csv", "doc_ids_csv", "skip_upload", "expected_subcommands"),
    [
        ("2025", "", "true", [["acquire-index", "--year", "2025"]]),
        ("2024,2025", "", "true", [["acquire-indexes", "--year", "2024", "--year", "2025", "--max-requests", "16"]]),
        (
            "2025",
            "20030591,2025:20030594",
            "false",
            [
                ["acquire-index", "--year", "2025"],
                ["acquire", "--doc-id", "20030591", "--doc-id", "2025:20030594", "--max-requests", "16"],
            ],
        ),
    ],
    ids=["single-year-dry", "multi-year-dry", "single-year-explicit-ptrs-publish"],
)
def test_house_acquire_step_constructs_exact_quoted_cli_invocations(
    tmp_path: Path, years_csv: str, doc_ids_csv: str, skip_upload: str, expected_subcommands: list[list[str]]
):
    step = _steps_by_name(_load_workflow(HOUSE_WORKFLOW))["Acquire House disclosure index and selected PTRs"]
    work_dir = tmp_path.resolve() / "house-disclosures"
    work_dir.mkdir()
    (work_dir / "selection.json").write_text('{"format":"house-disclosures-captures","version":1}\n', encoding="utf-8")
    log = tmp_path / "uv-calls.jsonl"
    stub = (
        f'uv() {{ python3 -c "import json, sys; open({str(log)!r}, \'a\').write(json.dumps(sys.argv[1:]) + \'\\n\')" "$@"; }}\n'
    )
    subprocess.run(
        ["bash", "-c", stub + step["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(tmp_path.resolve()),
                "WORK_DIR": str(work_dir),
                "YEARS_CSV": years_csv,
                "DOC_IDS_CSV": doc_ids_csv,
                "MAX_PTRS": "0",
                "MAX_REQUESTS": "16",
                "SKIP_UPLOAD": skip_upload,
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(calls) == len(expected_subcommands)
    for actual, expected_tokens in zip(calls, expected_subcommands, strict=True):
        assert actual[:5] == ["run", "--frozen", "python", "-m", "spicy_regs.pipelines.house_disclosures"]
        for token in expected_tokens:
            assert token in actual
        if skip_upload == "true":
            assert "--archive-root" in actual
        else:
            assert "--archive-root" not in actual


def test_house_build_and_validate_step_runs_end_to_end_on_retained_fixture(tmp_path: Path):
    """Execute the actual workflow Build and validate step against real checked-in House fixtures."""
    runner_temp = tmp_path.resolve() / "runner-temp"
    work_dir = runner_temp / "house-disclosures"
    raw_root = work_dir / "raw-archive"
    workspace = tmp_path.resolve() / "workspace"
    work_dir.mkdir(parents=True)
    workspace.mkdir()

    manifest = json.loads((HOUSE_FIXTURES / "manifest.json").read_text(encoding="utf-8"))
    evidence = CaptureEvidence(tmp_path / "seed-inputs", "house-disclosures", archive_root=raw_root)
    for name, source in manifest.items():
        evidence.retain_file(
            HOUSE_FIXTURES / name, stage="original-test-input", raw_source=True, original_location=source["url"]
        )
    evidence.finish()

    captures = {
        "format": "house-disclosures-captures",
        "version": 1,
        "scope": "Retained fixture verification for refresh-house-disclosures workflow.",
        "index": {**manifest["2025FD.zip"], "year": 2025},
        "reports": [{**manifest[doc + ".pdf"], "doc_id": doc, "year": 2025} for doc in ("20030591", "20030594")],
    }
    (work_dir / "captures.json").write_text(json.dumps(captures), encoding="utf-8")
    github_output = tmp_path / "github_output.txt"
    github_output.write_text("", encoding="utf-8")

    step = _steps_by_name(_load_workflow(HOUSE_WORKFLOW))["Build and validate candidate generation"]
    subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=workspace,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "WORK_DIR": str(work_dir),
                "READ_INDEX": "",
                "SKIP_UPLOAD": "true",
                "GITHUB_OUTPUT": str(github_output),
                "UV_PROJECT": str(REPO_ROOT),
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    validation = json.loads((workspace / "output" / "candidate-validation.json").read_text(encoding="utf-8"))
    assert validation["status"] == "candidate_validated"
    assert validation["family"] == "house-disclosures"
    assert validation["rows"]["house_disclosure_filings"] == 2935
    assert validation["rows"]["house_disclosure_reports"] == 2
    assert validation["skip_upload"] is True
    assert "artifact_digest=sha256:" in github_output.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("selection_path", "archive_root", "expected_error"),
    [
        ("", "", "selection_path is required"),
        ("selection.json", "", "archive_root is required"),
        ("/nonexistent/selection.json", "/nonexistent/archive", "selection_path does not exist"),
    ],
    ids=["missing-both", "missing-archive-root", "nonexistent-selection"],
)
def test_fec_research_profiles_workflow_fails_closed_when_retained_inputs_absent(
    tmp_path: Path, selection_path: str, archive_root: str, expected_error: str
):
    step = _steps_by_name(_load_workflow(FEC_WORKFLOW))["Validate required retained inputs"]
    github_output = tmp_path / "github_output.txt"
    github_output.write_text("", encoding="utf-8")
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(tmp_path.resolve()),
                "GITHUB_OUTPUT": str(github_output),
                "SELECTION_PATH": selection_path,
                "ARCHIVE_ROOT": archive_root,
                "EXPECT_PRIOR": "",
                "SKIP_UPLOAD": "true",
            }
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert expected_error in (result.stderr + result.stdout)


def test_fec_research_profiles_workflow_validates_and_builds_end_to_end_on_retained_fixture(tmp_path: Path):
    """Execute both Validate and Build steps against retained FEC research profile originals."""
    import hashlib

    runner_temp = tmp_path.resolve() / "runner-temp"
    runner_temp.mkdir()
    raw_root = tmp_path.resolve() / "retained-raw"
    workspace = tmp_path.resolve() / "workspace"
    workspace.mkdir()

    selection = json.loads((FEC_FIXTURES / "selection.json").read_text(encoding="utf-8"))
    evidence = CaptureEvidence(tmp_path / "seed-fec", "fec-research-profiles", archive_root=raw_root)

    # Retain a valid 1-record committee page plus the 6 real checked-in outside/contribution originals.
    committee_url = "https://api.open.fec.gov/v1/committees/?cycle=2024&cycle=2026&sort=committee_id&per_page=100&page=1"
    committee_body = json.dumps(
        {
            "pagination": {"page": 1, "pages": 1, "per_page": 100, "count": 1, "is_count_exact": True},
            "results": [{"committee_id": "C00000001", "name": "Fixture Committee", "sponsor_candidate_ids": []}],
        }
    ).encode("utf-8")
    committee_sha = "sha256:" + hashlib.sha256(committee_body).hexdigest()
    evidence.retain_bytes(
        committee_body,
        stage="fec-fixture",
        raw_source=True,
        source_url=committee_url,
        original_location=committee_url,
        observed_at="2026-09-12T16:57:29.402394+00:00",
    )
    selection["committees"] = [
        {
            "response_sha256": committee_sha,
            "byte_size": len(committee_body),
            "request_url": committee_url,
            "observed_at": "2026-09-12T16:57:29.402394+00:00",
            "expected_results": 1,
        }
    ]

    fixtures_to_retain: list[tuple[str, str]] = []
    for query in selection["outside"]:
        for page in query["pages"]:
            fixtures_to_retain.append((page["sha256"], page["request_url"]))
    orig = selection["contribution"]["original"]
    fixtures_to_retain.append((orig["response_sha256"], orig["request_url"]))
    for query in selection["contribution"]["queries"]:
        for page in query["pages"]:
            fixtures_to_retain.append((page["sha256"], page["request_url"]))
    for digest, url in fixtures_to_retain:
        blob_file = FEC_FIXTURES / digest.removeprefix("sha256:")
        evidence.retain_file(blob_file, stage="fec-fixture", raw_source=True, source_url=url, original_location=url)
    evidence.finish()

    selection_file = tmp_path / "selection.json"
    selection_file.write_text(json.dumps(selection), encoding="utf-8")

    steps = _steps_by_name(_load_workflow(FEC_WORKFLOW))
    validate_out = tmp_path / "validate_out.txt"
    validate_out.write_text("", encoding="utf-8")

    # 1. Missing pinned originals in an empty archive fail closed during validation.
    empty_archive = tmp_path.resolve() / "empty-raw"
    empty_archive.mkdir()
    missing_res = subprocess.run(
        ["bash", "-c", steps["Validate required retained inputs"]["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_OUTPUT": str(validate_out),
                "SELECTION_PATH": str(selection_file),
                "ARCHIVE_ROOT": str(empty_archive),
                "EXPECT_PRIOR": "",
                "SKIP_UPLOAD": "true",
            }
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert missing_res.returncode != 0
    assert "pinned original(s) missing from archive_root" in (missing_res.stderr + missing_res.stdout)

    # 2. Attempting skip_upload=false against a scratch archive must fail closed before building.
    refused = subprocess.run(
        ["bash", "-c", steps["Validate required retained inputs"]["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_OUTPUT": str(validate_out),
                "SELECTION_PATH": str(selection_file),
                "ARCHIVE_ROOT": str(raw_root),
                "EXPECT_PRIOR": "",
                "SKIP_UPLOAD": "false",
            }
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert refused.returncode != 0
    assert "must match the shared raw archive" in (refused.stderr + refused.stdout)

    # 3. Dry-run validation succeeds and emits work_dir / selection_path / archive_root.
    subprocess.run(
        ["bash", "-c", steps["Validate required retained inputs"]["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_OUTPUT": str(validate_out),
                "SELECTION_PATH": str(selection_file),
                "ARCHIVE_ROOT": str(raw_root),
                "EXPECT_PRIOR": "",
                "SKIP_UPLOAD": "true",
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    outputs = dict(line.split("=", 1) for line in validate_out.read_text(encoding="utf-8").splitlines() if "=" in line)

    # 4. Actual workflow Build and validate step builds all 5 tables and verifies schemas & admission.
    build_out = tmp_path / "build_out.txt"
    build_out.write_text("", encoding="utf-8")
    subprocess.run(
        ["bash", "-c", steps["Build and validate candidate generation"]["run"]],
        cwd=workspace,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "WORK_DIR": outputs["work_dir"],
                "SELECTION_PATH": outputs["selection_path"],
                "ARCHIVE_ROOT": outputs["archive_root"],
                "SKIP_UPLOAD": "true",
                "GITHUB_OUTPUT": str(build_out),
                "UV_PROJECT": str(REPO_ROOT),
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    validation = json.loads((workspace / "output" / "candidate-validation.json").read_text(encoding="utf-8"))
    assert validation["status"] == "candidate_validated"
    assert validation["family"] == "fec-research-profiles"
    assert validation["tables"] == [
        "fec_committee_relationship_profiles.parquet",
        "fec_contribution_decisions.parquet",
        "fec_contribution_profiles.parquet",
        "fec_outside_spending_profiles.parquet",
        "fec_outside_spending_spenders.parquet",
    ]
    assert validation["rows"]["fec_committee_relationship_profiles.parquet"] == 1
    assert validation["rows"]["fec_contribution_decisions.parquet"] == 404
    assert validation["rows"]["fec_outside_spending_spenders.parquet"] == 73
    assert validation["skip_upload"] is True
    assert "artifact_digest=sha256:" in build_out.read_text(encoding="utf-8")


def test_house_acquire_step_max_ptrs_branch_selects_bounded_native_ptrs(tmp_path: Path):
    """Exercise the MAX_PTRS > 0 branch against the real retained 2025FD.zip index fixture."""
    runner_temp = tmp_path.resolve() / "runner-temp"
    work_dir = runner_temp / "house-disclosures"
    raw_root = work_dir / "raw-archive"
    work_dir.mkdir(parents=True)

    manifest = json.loads((HOUSE_FIXTURES / "manifest.json").read_text(encoding="utf-8"))
    evidence = CaptureEvidence(tmp_path / "seed-idx", "house-disclosures", archive_root=raw_root)
    evidence.retain_file(
        HOUSE_FIXTURES / "2025FD.zip",
        stage="original-test-input",
        raw_source=True,
        original_location=manifest["2025FD.zip"]["url"],
    )
    evidence.finish()

    selection = {
        "format": "house-disclosures-captures",
        "version": 1,
        "scope": "Retained 2025 index fixture for MAX_PTRS branch test.",
        "index": {**manifest["2025FD.zip"], "year": 2025},
        "reports": [],
    }
    (work_dir / "selection.json").write_text(json.dumps(selection), encoding="utf-8")

    step = _steps_by_name(_load_workflow(HOUSE_WORKFLOW))["Acquire House disclosure index and selected PTRs"]
    log = tmp_path / "uv-max-ptrs-calls.jsonl"
    # Execute the real inline Python selector (`uv run --frozen python - ...`) while stubbing network acquire CLIs.
    stub = (
        "uv() {\n"
        '  if [ "${3:-}" = "python" ] && [ "${4:-}" = "-" ]; then\n'
        '    command uv "$@"\n'
        "    return $?\n"
        "  fi\n"
        f'  python3 -c "import json, sys; open({str(log)!r}, \'a\').write(json.dumps(sys.argv[1:]) + \'\\n\')" "$@"\n'
        "}\n"
    )
    subprocess.run(
        ["bash", "-c", stub + step["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "WORK_DIR": str(work_dir),
                "YEARS_CSV": "2025",
                "DOC_IDS_CSV": "",
                "MAX_PTRS": "3",
                "MAX_REQUESTS": "8",
                "SKIP_UPLOAD": "true",
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(calls) == 2
    assert calls[0][5] == "acquire-index"
    assert calls[1][5] == "acquire"
    selected_docs = [calls[1][idx + 1] for idx, token in enumerate(calls[1]) if token == "--doc-id"]
    assert len(selected_docs) == 3
    assert selected_docs == sorted(selected_docs)
    assert all(re.fullmatch(r"2025:[A-Za-z0-9_-]+", item) for item in selected_docs)
    assert "--max-requests" in calls[1] and calls[1][calls[1].index("--max-requests") + 1] == "8"


def test_fec_research_profiles_validation_handles_response_sha256_only_pages_and_missing_digests(tmp_path: Path):
    """Regression: outside/contribution pages using response_sha256 without sha256 validate and build cleanly."""
    import hashlib

    runner_temp = tmp_path.resolve() / "runner-temp"
    runner_temp.mkdir()
    raw_root = tmp_path.resolve() / "retained-raw"
    workspace = tmp_path.resolve() / "workspace"
    workspace.mkdir()

    selection = json.loads((FEC_FIXTURES / "selection.json").read_text(encoding="utf-8"))
    evidence = CaptureEvidence(tmp_path / "seed-fec-resp", "fec-research-profiles", archive_root=raw_root)

    committee_url = "https://api.open.fec.gov/v1/committees/?cycle=2024&cycle=2026&sort=committee_id&per_page=100&page=1"
    committee_body = json.dumps(
        {
            "pagination": {"page": 1, "pages": 1, "per_page": 100, "count": 1, "is_count_exact": True},
            "results": [{"committee_id": "C00000001", "name": "Fixture Committee", "sponsor_candidate_ids": []}],
        }
    ).encode("utf-8")
    committee_sha = "sha256:" + hashlib.sha256(committee_body).hexdigest()
    evidence.retain_bytes(
        committee_body,
        stage="fec-fixture",
        raw_source=True,
        source_url=committee_url,
        original_location=committee_url,
        observed_at="2026-09-12T16:57:29.402394+00:00",
    )
    selection["committees"] = [
        {
            "response_sha256": committee_sha,
            "byte_size": len(committee_body),
            "request_url": committee_url,
            "observed_at": "2026-09-12T16:57:29.402394+00:00",
            "expected_results": 1,
        }
    ]

    # Convert every outside & contribution query page to response_sha256/byte_size ONLY (no sha256/bytes keys).
    orig = selection["contribution"]["original"]
    orig_file = FEC_FIXTURES / orig["response_sha256"].removeprefix("sha256:")
    evidence.retain_file(
        orig_file,
        stage="fec-fixture",
        raw_source=True,
        source_url=orig["request_url"],
        original_location=orig["request_url"],
    )
    for query in [*selection["outside"], *selection["contribution"]["queries"]]:
        for page in query["pages"]:
            digest = page.pop("sha256")
            size = page.pop("bytes")
            page["response_sha256"] = digest
            page["byte_size"] = size
            blob_file = FEC_FIXTURES / digest.removeprefix("sha256:")
            evidence.retain_file(
                blob_file,
                stage="fec-fixture",
                raw_source=True,
                source_url=page["request_url"],
                original_location=page["request_url"],
            )
    evidence.finish()

    selection_file = tmp_path / "selection-response-sha256.json"
    selection_file.write_text(json.dumps(selection), encoding="utf-8")

    steps = _steps_by_name(_load_workflow(FEC_WORKFLOW))
    validate_out = tmp_path / "validate_out.txt"
    validate_out.write_text("", encoding="utf-8")

    # 1. response_sha256-only pages pass validation without KeyError('sha256') and build end-to-end.
    subprocess.run(
        ["bash", "-c", steps["Validate required retained inputs"]["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_OUTPUT": str(validate_out),
                "SELECTION_PATH": str(selection_file),
                "ARCHIVE_ROOT": str(raw_root),
                "EXPECT_PRIOR": "",
                "SKIP_UPLOAD": "true",
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    outputs = dict(line.split("=", 1) for line in validate_out.read_text(encoding="utf-8").splitlines() if "=" in line)
    build_out = tmp_path / "build_out.txt"
    build_out.write_text("", encoding="utf-8")
    subprocess.run(
        ["bash", "-c", steps["Build and validate candidate generation"]["run"]],
        cwd=workspace,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "WORK_DIR": outputs["work_dir"],
                "SELECTION_PATH": outputs["selection_path"],
                "ARCHIVE_ROOT": outputs["archive_root"],
                "SKIP_UPLOAD": "true",
                "GITHUB_OUTPUT": str(build_out),
                "UV_PROJECT": str(REPO_ROOT),
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    validation = json.loads((workspace / "output" / "candidate-validation.json").read_text(encoding="utf-8"))
    assert validation["status"] == "candidate_validated"
    assert validation["rows"]["fec_contribution_decisions.parquet"] == 404

    # 2. A page missing both response_sha256 and sha256 fails closed cleanly without KeyError.
    broken = json.loads(selection_file.read_text(encoding="utf-8"))
    broken["outside"][0]["pages"][0].pop("response_sha256")
    broken_file = tmp_path / "selection-missing-digest.json"
    broken_file.write_text(json.dumps(broken), encoding="utf-8")
    missing_digest = subprocess.run(
        ["bash", "-c", steps["Validate required retained inputs"]["run"]],
        cwd=REPO_ROOT,
        env=_clean_env(
            {
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_OUTPUT": str(validate_out),
                "SELECTION_PATH": str(broken_file),
                "ARCHIVE_ROOT": str(raw_root),
                "EXPECT_PRIOR": "",
                "SKIP_UPLOAD": "true",
            }
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert missing_digest.returncode != 0
    assert "missing response_sha256/sha256" in (missing_digest.stderr + missing_digest.stdout)
    assert "KeyError" not in (missing_digest.stderr + missing_digest.stdout)
