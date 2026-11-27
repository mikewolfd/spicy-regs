"""Tests for audited dockets placeholder RIN repair and its manual-dispatch workflow (ss-eyhu)."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs.pipelines.regulatory_publication import finish_dataset
from spicy_regs.pipelines.repair_regulations import DOCKETS_RIN_RECEIPT, repair_docket_rins
from spicy_regs.schemas import DOCKET
from spicy_regs.selected_generations import SelectedInputs
from spicy_regs.sources import publication
from spicy_regs.transforms.regulations_receipts import policy


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "repair-dockets-rins.yml"


def _docket_row(docket_id: str, rin: str | None, *, agency: str = "EPA", title: str | None = None) -> dict[str, str | None]:
    return {
        "docket_id": docket_id,
        "agency_code": agency,
        "title": title if title is not None else f"Title for {docket_id}",
        "docket_type": "Rulemaking",
        "modify_date": "2026-10-01T12:00:00Z",
        "abstract": f"Abstract for {docket_id}",
        "rin": rin,
    }


def _write_source_dockets(path: Path, rows: list[dict[str, str | None]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(column, pa.string()) for column in DOCKET.schema])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def _seed_local_dockets(root: Path, rows: list[dict[str, str | None]]) -> None:
    source = _write_source_dockets(root / ".seed" / "dockets.parquet", rows)
    finish_dataset(root, "dockets", source, publish=False)


def test_repair_docket_rins_nullifies_placeholders_and_preserves_valid_and_historical_rins(tmp_path):
    rows = [
        _docket_row("BIS-2023-0005", "Not Assigned", agency="BIS"),
        _docket_row("CFPB-2022-0070", "0000-AA00", agency="CFPB"),
        _docket_row("OSM-2023-0001", "0000-AA00", agency="OSM"),
        _docket_row("EPA-BLANK-0001", ""),
        _docket_row("EPA-SPACE-0001", "   \t "),
        _docket_row("EPA-CASE-0001", "  not   assigned  "),
        _docket_row("EPA-XXXX-0001", "1234-XXXX"),
        _docket_row("EPA-NULL-0001", None),
        _docket_row("EPA-CANON-0001", "2060-AV12"),
        _docket_row("EPA-TRIM-0001", " 2060-AV13 "),
        _docket_row("NOAA-XSER-0001", "0648-XA529", agency="NOAA"),
        _docket_row("NCUA-HIST-0001", "3133-AEOO", agency="NCUA"),
        _docket_row("AMS-5CHR-0001", "0581-AD345", agency="AMS"),
        _docket_row("FRS-SPACED-0001", "7100 AE-62", agency="FRS"),
        _docket_row("DOT-MULTI-0001", "2125-AF66, 2132-AB21", agency="DOT"),
        _docket_row("NOAA-SEMI-0001", "0648-BB09; 0648-BE03", agency="NOAA"),
        _docket_row("OPM-XSER-0001", "0660-XC023", agency="OPM"),
    ]
    _seed_local_dockets(tmp_path, rows)

    receipt = repair_docket_rins(tmp_path, publish=False, public_url="")

    assert receipt["format"] == "spicy-regs-dockets-rin-repair"
    assert receipt["table"] == "dockets"
    assert receipt["policy_version"] == "regulations-native-v1"
    assert receipt["mode"] == "dry-run"
    assert receipt["published"] is False
    assert receipt["total_rows"] == len(rows)
    assert receipt["changed_rows"] == 8
    assert receipt["nullified_rows"] == 7
    assert receipt["trimmed_rows"] == 1
    assert receipt["unchanged_non_null_rows"] == 8
    assert receipt["remaining_placeholder_rins"] == 0
    assert receipt["non_rin_columns_unchanged"] is True
    assert receipt["acquisition_manifest_changed"] is False
    assert json.loads((tmp_path / DOCKETS_RIN_RECEIPT).read_text()) == receipt

    repaired_table = pq.read_table(tmp_path / "dockets.parquet")
    assert repaired_table.schema.equals(policy("dockets").subject_schema)
    assert repaired_table.num_rows == len(rows)
    by_id = {row["docket_id"]: row for row in repaired_table.to_pylist()}

    # Every placeholder / blank value became NULL.
    for docket_id in (
        "BIS-2023-0005",
        "CFPB-2022-0070",
        "OSM-2023-0001",
        "EPA-BLANK-0001",
        "EPA-SPACE-0001",
        "EPA-CASE-0001",
        "EPA-XXXX-0001",
        "EPA-NULL-0001",
    ):
        assert by_id[docket_id]["rin"] is None

    # Every valid and historical RIN is preserved (with edge whitespace trimmed).
    assert by_id["EPA-CANON-0001"]["rin"] == "2060-AV12"
    assert by_id["EPA-TRIM-0001"]["rin"] == "2060-AV13"
    assert by_id["NOAA-XSER-0001"]["rin"] == "0648-XA529"
    assert by_id["NCUA-HIST-0001"]["rin"] == "3133-AEOO"
    assert by_id["AMS-5CHR-0001"]["rin"] == "0581-AD345"
    assert by_id["FRS-SPACED-0001"]["rin"] == "7100 AE-62"
    assert by_id["DOT-MULTI-0001"]["rin"] == "2125-AF66, 2132-AB21"
    assert by_id["NOAA-SEMI-0001"]["rin"] == "0648-BB09; 0648-BE03"
    assert by_id["OPM-XSER-0001"]["rin"] == "0660-XC023"

    # All 6 non-rin columns stay identical for every row.
    for original in rows:
        actual = by_id[original["docket_id"]]
        for column in ("agency_code", "title", "docket_type", "modify_date", "abstract"):
            assert actual[column] == original[column]

    # Re-running is idempotent: zero changes.
    second = repair_docket_rins(tmp_path, publish=False, public_url="")
    assert second["changed_rows"] == 0
    assert second["nullified_rows"] == 0
    assert second["total_rows"] == len(rows)


def test_repair_docket_rins_remote_dry_run_publish_expect_prior_and_idempotency(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    publisher = tmp_path / "publisher"
    pub_inputs = SelectedInputs(publisher, publisher / "selected")
    initial_rows = [
        _docket_row("BIS-2023-0005", "Not Assigned", agency="BIS"),
        _docket_row("CFPB-2022-0070", "0000-AA00", agency="CFPB"),
        _docket_row("EPA-CANON-0001", "2060-AV12", agency="EPA"),
    ]
    finish_dataset(
        publisher,
        "dockets",
        _write_source_dockets(publisher / "dockets.parquet", initial_rows),
        publish=True,
        inputs=pub_inputs,
    )
    index_before = remote.objects[publication.INDEX_V2_KEY]
    prior_digest = publication.parse_index(index_before)["families"]["dockets"]["artifactDigest"]

    # Dry run reads live remote dockets, repairs locally, and never mutates R2.
    dry_dir = tmp_path / "dry-run"
    dry_receipt = repair_docket_rins(dry_dir, publish=False)
    assert dry_receipt["published"] is False
    assert dry_receipt["mode"] == "dry-run"
    assert dry_receipt["prior_digest"] == prior_digest
    assert dry_receipt["published_digest"] is None
    assert dry_receipt["changed_rows"] == 2
    assert remote.objects[publication.INDEX_V2_KEY] == index_before

    # Mismatched or malformed expect_prior refuses before writing.
    with pytest.raises(ValueError, match="expect_prior"):
        repair_docket_rins(tmp_path / "bad-hex", publish=False, expect_prior="not-hex!")
    with pytest.raises(ValueError, match="not the expected"):
        repair_docket_rins(tmp_path / "wrong-prior", publish=False, expect_prior="0" * 12)
    assert remote.objects[publication.INDEX_V2_KEY] == index_before

    # Publish with matching expect_prior prefix advances the live dockets pointer cleanly.
    live_dir = tmp_path / "live-run"
    live_receipt = repair_docket_rins(live_dir, publish=True, expect_prior=prior_digest[:19])
    assert live_receipt["published"] is True
    assert live_receipt["mode"] == "publish"
    assert live_receipt["prior_digest"] == prior_digest
    assert live_receipt["published_digest"] is not None
    assert live_receipt["published_digest"] != prior_digest
    assert remote.objects[publication.INDEX_V2_KEY] != index_before
    published_index = publication.parse_index(remote.objects[publication.INDEX_V2_KEY])
    assert published_index["families"]["dockets"]["artifactDigest"] == live_receipt["published_digest"]

    # Subsequent publish on the already-clean live generation is a verified no-op without a redundant R2 commit.
    index_after_publish = remote.objects[publication.INDEX_V2_KEY]
    rerun = repair_docket_rins(tmp_path / "rerun", publish=True)
    assert rerun["changed_rows"] == 0
    assert rerun["published"] is False
    assert rerun["skipped_reason"] == "already_clean"
    assert remote.objects[publication.INDEX_V2_KEY] == index_after_publish


def test_repair_docket_rins_refuses_publish_without_remote_index_or_on_concurrent_change(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install

    _seed_local_dockets(tmp_path / "local", [_docket_row("BIS-2023-0005", "Not Assigned")])
    with pytest.raises(ValueError, match="captured remote publication index"):
        repair_docket_rins(tmp_path / "local", publish=True, public_url="")

    remote = install(monkeypatch)
    publisher = tmp_path / "publisher"
    pub_inputs = SelectedInputs(publisher, publisher / "selected")
    finish_dataset(
        publisher,
        "dockets",
        _write_source_dockets(publisher / "v1.parquet", [_docket_row("BIS-2023-0005", "Not Assigned")]),
        publish=True,
        inputs=pub_inputs,
    )
    stale_root = tmp_path / "stale"
    stale_inputs = SelectedInputs(stale_root, stale_root / "selected")
    # Intervening live publish advances dockets before repair_docket_rins commits.
    finish_dataset(
        publisher,
        "dockets",
        _write_source_dockets(
            publisher / "v2.parquet",
            [_docket_row("BIS-2023-0005", "Not Assigned"), _docket_row("EPA-NEW-0001", "2060-AV12")],
        ),
        publish=True,
        inputs=pub_inputs,
    )
    index_before = remote.objects[publication.INDEX_V2_KEY]
    with pytest.raises((ValueError, RuntimeError), match="changed|concurrent|stale|Conflict"):
        repair_docket_rins(stale_root, publish=True, inputs=stale_inputs)
    assert remote.objects[publication.INDEX_V2_KEY] == index_before


def test_repair_regulations_cli_exposes_repair_docket_rins_and_keeps_default_help():
    root_help = subprocess.run(
        [sys.executable, "-m", "spicy_regs.pipelines.repair_regulations", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert "repair-docket-rins" in root_help.stdout
    assert "--table" in root_help.stdout

    sub_help = subprocess.run(
        [sys.executable, "-m", "spicy_regs.pipelines.repair_regulations", "repair-docket-rins", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    flags = set(re.findall(r"--[a-z][a-z0-9-]*", sub_help.stdout))
    assert {"--output-dir", "--skip-upload", "--public-url", "--expect-prior"} <= flags


def test_repair_dockets_rins_workflow_guards_permissions_inputs_and_shell_safety(tmp_path):
    raw_text = WORKFLOW_PATH.read_text(encoding="utf-8")
    doc = yaml.safe_load(raw_text)
    triggers = doc.get("on", doc.get(True, {}))
    assert set(triggers) == {"workflow_dispatch"}

    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["skip_upload"]["type"] == "boolean"
    assert inputs["skip_upload"]["default"] is True
    assert inputs["expect_prior"]["type"] == "string"
    assert inputs["expect_prior"]["default"] == ""

    assert doc["permissions"] == {"contents": "read"}
    assert doc["concurrency"] == {"group": "repair-dockets-rins", "cancel-in-progress": False}
    assert not WORKFLOW_PATH.name.startswith("rollup-")
    assert "run-rollup-" not in raw_text
    assert "_rollup.yml" not in raw_text
    assert "R2_ALLOW_SHRINK=" not in raw_text
    assert "R2_MIN_SIZE_RATIO=" not in raw_text
    assert "secrets.ZYTE_TOKEN" not in raw_text
    assert "secrets.FIRECRAWL_API_KEY" not in raw_text

    steps = {step["name"]: step for step in doc["jobs"]["repair"]["steps"] if "name" in step}
    for name, step in steps.items():
        if "run" in step:
            assert "${{ inputs." not in step["run"], f"{name} interpolates raw inputs.* in run:"
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)
            if name != "Install pinned dependencies":
                assert 'TMPDIR="$RUNNER_TEMP"' in step["run"]
                assert "unset SPICYGOV_RAW_ROOT R2_ALLOW_SHRINK R2_MIN_SIZE_RATIO" in step["run"]

    dry_step = steps["Repair dockets RINs (dry run)"]
    publish_step = steps["Repair and publish dockets RINs"]
    assert dry_step["if"] == "inputs.skip_upload != false"
    assert publish_step["if"] == "inputs.skip_upload == false"
    assert "R2_ACCESS_KEY_ID" not in dry_step.get("env", {})
    assert "R2_SECRET_ACCESS_KEY" not in dry_step.get("env", {})
    assert "R2_ACCESS_KEY_ID" in publish_step["env"]
    assert "R2_SECRET_ACCESS_KEY" in publish_step["env"]
    assert '-H "User-Agent: $WORKFLOW_USER_AGENT"' in dry_step["run"]
    assert '-H "User-Agent: $WORKFLOW_USER_AGENT"' in publish_step["run"]
    assert "--skip-upload" in dry_step["run"]
    assert "--no-skip-upload" in publish_step["run"]

    # Validate inputs step rejects malformed expect_prior cleanly.
    validate = steps["Validate inputs"]
    env = {k: v for k, v in os.environ.items() if k not in {"PYTEST_CURRENT_TEST", "SPICYGOV_RAW_ROOT"}}
    env.update({"RUNNER_TEMP": str(tmp_path.resolve()), "EXPECT_PRIOR": "not-valid-hex", "SKIP_UPLOAD": "true"})
    bad = subprocess.run(["bash", "-c", validate["run"]], cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=30)
    assert bad.returncode != 0
    assert "expect_prior" in (bad.stderr + bad.stdout)
