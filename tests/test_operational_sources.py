"""Future H adapter uses the actual maintained builder and independent auditor; D adapter uses cfr_native_audit."""

import json
import subprocess
import sys

import pytest

from spicy_regs import cfr_native_audit as audit
from spicy_regs import operational_sources as adapter
from spicy_regs.operational_runner import census, file_pin, read_json
from spicy_regs.sources import uscode_editions as builder, uscode_editions_audit as auditor
from tests.operational_helpers import ENV, REPO, cfr_capture
from tests.test_cfr_native_audit import BODY, TREE
from tests.test_uscode_editions_audit import fixture


def test_annual_adapter_full_fixture_build_and_independent_audit(tmp_path):
    candidate, selection, _ = fixture(tmp_path, year=1994, build_candidate=False)
    pins = builder._code_pins()
    generation = builder._identity("uscode-editions-public-v2", builder._digest(selection.read_bytes()), pins)
    config = {
        "selection": file_pin(selection),
        "original_input": file_pin(tmp_path / "original.zip"),
        "expected_reader_pins": pins,
        "expected_generation": generation,
        "annual_year": 1994,
    }
    adapter.annual(config, "h-build", candidate, None)
    actual = auditor.audit(candidate, selection, annual_year=1994)
    config["conservation"] = {
        "expected_rows": actual["rows"],
        "expected_content_categories": actual["content_categories"],
        "expected_complete_members": actual["complete_original_members"],
    }
    before = census(candidate)
    output = tmp_path / "audit.json"
    adapter.annual(config, "h-audit", candidate, output)
    assert json.loads(output.read_text())["verdict"] == "APPROVE"
    assert census(candidate) == before
    config["expected_reader_pins"] = {"changed": True}
    with pytest.raises(ValueError, match="recipe"):
        adapter.annual(config, "h-audit", candidate, tmp_path / "should-not-exist.json")


def test_annual_adapter_refuses_changed_selection_and_extra_candidate_files(tmp_path):
    candidate, selection, _ = fixture(tmp_path, year=1994, build_candidate=False)
    pins = builder._code_pins()
    generation = builder._identity("uscode-editions-public-v2", builder._digest(selection.read_bytes()), pins)
    config = {
        "selection": file_pin(selection),
        "original_input": file_pin(tmp_path / "original.zip"),
        "expected_reader_pins": pins,
        "expected_generation": generation,
        "annual_year": 1994,
    }
    adapter.annual(config, "h-build", candidate, None)
    actual = auditor.audit(candidate, selection, annual_year=1994)
    config["conservation"] = {
        "expected_rows": actual["rows"],
        "expected_content_categories": actual["content_categories"],
        "expected_complete_members": actual["complete_original_members"],
    }
    (candidate / "extra.txt").write_text("not part of the closed build")
    with pytest.raises(ValueError, match="extra/missing"):
        adapter.annual(config, "h-audit", candidate, tmp_path / "audit.json")
    (candidate / "extra.txt").unlink()
    selection.write_text(selection.read_text() + " ")
    with pytest.raises(ValueError, match="File pin differs"):
        adapter.annual(config, "h-audit", candidate, tmp_path / "changed.json")


def prepare(tmp_path, kind, preparation, inventory, *extra):
    output = tmp_path / "new-request.json"
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "spicy_regs.operational_runner",
            "prepare",
            kind,
            "--preparation",
            str(preparation),
            "--runtime-inventory",
            str(inventory),
            "--python",
            sys.executable,
            "--target",
            str(REPO / "src"),
            "--run-root",
            str(tmp_path / "future-run"),
            "--output",
            str(output),
            *extra,
        ],
        cwd=REPO,
        env=ENV,
        capture_output=True,
        text=True,
    )
    return result, output


def historical_h(tmp_path):
    selection = tmp_path / "selection.json"
    selection.write_text("{}")
    original = tmp_path / "never-opened-original.zip"
    original.write_bytes(b"x" * 123)
    original.chmod(0)  # It can be stat-ed, so a read anywhere in preparation would fail.
    historical = tmp_path / "historical-request.json"
    value = {
        "format": "H-maintained-annual1994-source-replay-request-v1",
        "selection": file_pin(selection),
        "original_input": {"path": str(original), "bytes": 123, "sha256": "0" * 64, "year": 1994},
        "metadata_pins": [],
        "expected_reader_pins": {},
        "expected_generation": "fixture",
        "conservation": {},
        "resource_request": {
            "wall_seconds": 10,
            "sample_interval_seconds": 0.1,
            "process_tree_rss_bytes": 1024**3,
            "owned_output_scratch_bytes": 1024**2,
            "admission_free_bytes": 1,
            "stop_free_bytes": 1,
        },
    }
    historical.write_text(json.dumps(value))
    return historical, value


def test_preparation_pins_the_whole_target_package_without_reading_source(tmp_path):
    historical, value = historical_h(tmp_path)
    inventory = tmp_path / "runtime.json"
    inventory.write_text(json.dumps({"files": []}))
    result, output = prepare(tmp_path, "h-annual", historical, inventory)
    assert result.returncode == 0, result.stdout + result.stderr
    prepared = json.loads(output.read_text())
    assert prepared["commands"]["build"]["args"][0] == "h-build"
    assert prepared["commands"]["audit"]["args"][0] == "h-audit"
    assert prepared["offline_single_process"] is True
    package = REPO / "src/spicy_regs"
    expected = {str(path) for path in package.rglob("*") if path.is_file() and "__pycache__" not in path.parts}
    assert {pin["path"] for pin in prepared["code"]} == expected
    assert not (tmp_path / "future-run").exists()
    assert historical.read_text() == json.dumps(value)


PREPARE_PROBE = """
import json, sys
from pathlib import Path
from types import SimpleNamespace
from spicy_regs import operational_runner as r
from spicy_regs.operational_prepare import prepare_request
pseudo, preparation, inventory, run_root, output = map(Path, sys.argv[1:6])
r.KERNEL_ROOTS = (pseudo,)
args = SimpleNamespace(kind='h-annual', preparation=preparation, runtime_inventory=inventory, python=sys.executable,
                       target=Path(sys.argv[6]), run_root=run_root, output=output)
print(json.dumps([pin['path'] for pin in prepare_request(args)['runtime']]))
"""


def test_preparation_leaves_kernel_state_out_of_an_observed_inventory(tmp_path):
    historical, _ = historical_h(tmp_path)
    pseudo, real = tmp_path / "pseudo", tmp_path / "real.txt"
    pseudo.mkdir()
    (pseudo / "stat").write_text("ticks 1\n")
    real.write_text("stable runtime file")
    inventory = tmp_path / "runtime.json"
    inventory.write_text(json.dumps({"files": [file_pin(pseudo / "stat"), file_pin(real)]}))
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            PREPARE_PROBE,
            *map(str, (pseudo, historical, inventory, tmp_path / "future-run", tmp_path / "new-request.json")),
            str(REPO / "src"),
        ],
        cwd=REPO,
        env=ENV,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    runtime = json.loads(result.stdout.splitlines()[-1])
    assert str(real) in runtime and str(pseudo / "stat") not in runtime


def test_preparation_refuses_missing_inventory_pin_files_and_writes_nothing(tmp_path):
    historical, _ = historical_h(tmp_path)
    inventory = tmp_path / "runtime.json"
    gone = tmp_path / "clean-room/venv/lib/gone.py"
    inventory.write_text(json.dumps({"files": [{"path": str(gone), "bytes": 1, "sha256": "0" * 64}]}))
    result, output = prepare(tmp_path, "h-annual", historical, inventory)
    assert result.returncode == 2 and "Traceback" not in result.stderr
    assert "1 of " in result.stderr and "runtime pin file(s) missing" in result.stderr
    assert "produce a new runtime inventory for --python" in result.stderr and str(gone) in result.stderr
    assert not output.exists() and not output.with_name("new-request-source.json").exists()


def test_prepare_d_cfr_translation(tmp_path):
    config = cfr_capture(tmp_path / "capture", lambda i: b'<CFRDOC><X a="1">%d<X/>tail</X></CFRDOC>' % i)
    capture = tmp_path / "capture-config.json"
    capture.write_text(json.dumps(config))
    limits = {
        "start_free_bytes": 1,
        "stop_free_bytes": 1,
        "rss_bytes": 1024**3,
        "wall_seconds": 120,
        "aggregate_bytes": 33554432,
        "scratch_bytes": 8388608,
        "output_bytes": 16777216,
        "evidence_bytes": 8388608,
        "interval_seconds": 0.25,
    }
    historical = tmp_path / "preflight-config.json"
    historical.write_text(json.dumps({"phase": "preflight", "capture_config": file_pin(capture), "limits": limits}))
    inventory = tmp_path / "runtime.json"
    inventory.write_text(json.dumps({"files": []}))
    result, output = prepare(tmp_path, "d-cfr", historical, inventory)
    assert result.returncode == 0, result.stdout + result.stderr
    prepared = json.loads(output.read_text())
    assert prepared["limits"] == {
        "start_free_bytes": 1,
        "stop_free_bytes": 1,
        "rss_bytes": 1024**3,
        "wall_seconds": 120,
        "interval_seconds": 0.25,
        "output_bytes": 33554432,
        "scratch_bytes": 8388608,
        "data_bytes": 16777216,
        "evidence_bytes": 8388608,
    }
    assert [prepared["commands"][name]["args"][0] for name in ("build", "audit")] == ["d-preflight", "d-semantic"]
    inputs = {pin["path"] for pin in prepared["inputs"]}
    wanted = [item["path"] for item in config["custody_acceptance"]]
    wanted += [spec["stored"]["path"] for spec in config["bodies"].values()]
    wanted += [context["capture"]["path"] for context in config["contexts"]]
    assert set(wanted) | {str(capture)} <= inputs and len(wanted) == 14
    source = read_json(output.with_name("new-request-source.json"))
    assert set(source) == {"capture_config"}  # No historical preflight acceptance or supervisor input.
    refused, _ = prepare(tmp_path, "d-cfr", historical, inventory, "--preflight-acceptance", str(historical))
    assert refused.returncode == 2 and "unrecognized arguments" in refused.stderr


def test_prepare_d_refuses_a_capture_config_that_differs_from_its_pin(tmp_path):
    config = cfr_capture(tmp_path / "capture", lambda i: b'<CFRDOC><X a="1">%d<X/>tail</X></CFRDOC>' % i)
    capture = tmp_path / "capture-config.json"
    capture.write_text(json.dumps(config))
    historical = tmp_path / "preflight-config.json"
    historical.write_text(
        json.dumps(
            {
                "phase": "preflight",
                "capture_config": file_pin(capture),
                "limits": {
                    "start_free_bytes": 1,
                    "stop_free_bytes": 1,
                    "rss_bytes": 1024**3,
                    "wall_seconds": 120,
                    "aggregate_bytes": 1,
                    "scratch_bytes": 1,
                    "output_bytes": 1,
                    "evidence_bytes": 1,
                    "interval_seconds": 0.25,
                },
            }
        )
    )
    capture.write_text(json.dumps({**config, "contexts": config["contexts"][:1]}))  # Changed after it was pinned.
    inventory = tmp_path / "runtime.json"
    inventory.write_text(json.dumps({"files": []}))
    result, output = prepare(tmp_path, "d-cfr", historical, inventory)
    assert result.returncode == 2 and "File pin differs" in result.stderr and "Traceback" not in result.stderr
    assert not output.exists() and not output.with_name("new-request-source.json").exists()


def test_cfr_adapter_phases_with_fake_backend(tmp_path, monkeypatch):
    class Backend:
        @staticmethod
        def read(body, context, url):
            return TREE, {"year": context["year"]}

    monkeypatch.setattr(audit, "maintained_backend", lambda: Backend)
    config = cfr_capture(tmp_path / "capture", lambda i: BODY + b"<!-- %d -->" % i)
    capture = tmp_path / "capture-config.json"
    capture.write_text(json.dumps(config))
    adapter_config = {"capture_config": file_pin(capture)}
    candidate, output = tmp_path / "candidate", tmp_path / "audit.json"
    with pytest.raises(FileNotFoundError):  # The semantic phase needs this run's own sealed preflight first.
        adapter.cfr(adapter_config, "d-semantic", candidate, output)
    adapter.cfr(adapter_config, "d-preflight", candidate, None)
    assert read_json(candidate / "preflight.json") == {
        "state": "tiny-maintained-preflight-passed",
        "capture_config": adapter_config["capture_config"],
    }
    adapter.cfr(adapter_config, "d-semantic", candidate, output)
    result = json.loads(output.read_text())
    assert result["status"] == "native-observations-and-comparisons-complete" and len(result["observations"]) == 6
    output.unlink()
    (candidate / "preflight.json").write_text(
        json.dumps({"state": "tiny-maintained-preflight-passed", "capture_config": {}})
    )
    with pytest.raises(ValueError, match="candidate selection differs"):
        adapter.cfr(adapter_config, "d-semantic", candidate, output)
    assert not output.exists()
    capture.write_text(capture.read_text() + " ")
    with pytest.raises(ValueError, match="File pin differs"):
        adapter.cfr(adapter_config, "d-preflight", tmp_path / "other", None)
