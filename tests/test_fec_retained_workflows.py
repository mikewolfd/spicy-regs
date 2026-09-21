"""Explicit retained inputs reach the existing rollup without changing source facts."""

import hashlib
import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path
from zipfile import ZipFile, is_zipfile

import pyarrow.parquet as pq
import pytest
import yaml

from scripts.prepare_fec_retained_inputs import prepare_inputs
from spicy_regs.pipelines.rollups.fec_observations import FecObservationsRollup
from spicy_regs.transforms.build_fec_observations import build_fec_observations

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).parent / "fixtures/fec-retained-inputs"


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _archive(tmp_path, selection=FIXTURE, *, extra=None):
    archive = tmp_path / "selection.tar.gz"
    with tarfile.open(archive, "w:gz") as writer:
        for path in sorted(selection.rglob("*")):
            writer.add(path, arcname=path.relative_to(selection).as_posix(), recursive=False)
        if extra is not None:
            writer.addfile(extra, io.BytesIO(b"x") if extra.isfile() else None)
    return archive


def _prepare(tmp_path, archive, **overrides):
    options = {
        "archive_sha256": _digest(archive),
        "manifest_sha256": _digest(FIXTURE / "manifest.json"),
        "output_dir": tmp_path / "inputs",
        "audit_dir": tmp_path / "audit",
    }
    options.update(overrides)
    return prepare_inputs(archive, **options)


def test_real_release_and_capture_transfer_build_complete_generation(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    archive = _archive(tmp_path)
    manifest = _prepare(tmp_path, archive)
    FecObservationsRollup(manifest=manifest, output_dir=tmp_path / "output").run()
    generations = list((tmp_path / "output/generations").iterdir())
    assert len(generations) == 1
    generation = generations[0]
    assert {p.name for p in generation.iterdir()} == {
        "artifact.json",
        "members.json",
        "fec_source_records.parquet",
        "fec_collections.parquet",
        "fec_relationships.parquet",
    }
    records = pq.read_table(generation / "fec_source_records.parquet").to_pylist()
    collections = pq.read_table(generation / "fec_collections.parquet").to_pylist()
    expected = _load_fixture_rows(tmp_path)
    assert records == expected
    evidence = next(path for path in (FIXTURE / "release-blobs/sha256").iterdir() if is_zipfile(path))
    with ZipFile(evidence) as retained:
        raw = retained.read("response.json")
        capture = json.loads(retained.read("manifest.json"))
    candidate = next(row for row in records if row["collection_id"] == "candidate-mixed-candidates")
    assert json.loads(candidate["metadata_json"]) == json.loads(raw)["results"][0]
    assert candidate["source_sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert candidate["observed_at"] == capture["observedAt"]
    assert {row["collection_id"] for row in collections} == {"candidate-mixed-candidates", "bulk-ccl-header"}
    header = next(row for row in records if row["collection_id"] == "bulk-ccl-header")
    assert json.loads(header["metadata_json"])["fields"] == [
        "CAND_ID",
        "CAND_ELECTION_YR",
        "FEC_ELECTION_YR",
        "CMTE_ID",
        "CMTE_TP",
        "CMTE_DSGN",
        "LINKAGE_ID",
    ]
    assert (tmp_path / "audit/source-manifest.json").read_bytes() == (FIXTURE / "source-manifest.json").read_bytes()
    assert (tmp_path / "audit/manifest.json").read_bytes() == (FIXTURE / "manifest.json").read_bytes()
    receipt = json.loads((tmp_path / "audit/transfer.json").read_text())
    assert receipt["status"] == "verified-transfer"
    assert receipt["manifest_sha256"] == _digest(manifest)
    assert "observed_at" not in receipt  # transfer time cannot replace source observations
    assert len(json.loads((tmp_path / "audit/relocations.json").read_text())) == 3


def _load_fixture_rows(tmp_path):
    outputs = build_fec_observations(FIXTURE / "manifest.json", tmp_path / "direct")
    return pq.read_table(outputs[0]).to_pylist()


@pytest.mark.parametrize("kind", ["archive_pin", "manifest_pin", "archive_size", "expanded_size", "existing"])
def test_bad_pins_or_limits_refuse_install_and_retain_receipt(tmp_path, kind):
    archive = _archive(tmp_path)
    options = {
        "archive_pin": {"archive_sha256": "0" * 64},
        "manifest_pin": {"manifest_sha256": "0" * 64},
        "archive_size": {"max_archive_bytes": archive.stat().st_size - 1},
        "expanded_size": {"max_extracted_bytes": 1},
        "existing": {},
    }[kind]
    if kind == "existing":
        (tmp_path / "inputs").mkdir()
        (tmp_path / "inputs/keep").write_text("prior")
    with pytest.raises((ValueError, FileExistsError)):
        _prepare(tmp_path, archive, **options)
    assert not list(tmp_path.glob(".fec-inputs-*"))
    assert json.loads((tmp_path / "audit/transfer.json").read_text())["status"] == "refused"
    if kind == "existing":
        assert (tmp_path / "inputs/keep").read_text() == "prior"
    else:
        assert not (tmp_path / "inputs").exists()


@pytest.mark.parametrize(
    "name,kind",
    [
        ("../escape", "file"),
        ("/absolute", "file"),
        ("manifest.json", "file"),
        ("link", "link"),
        ("hardlink", "hardlink"),
    ],
)
def test_archive_paths_and_links_cannot_escape_or_overwrite(tmp_path, name, kind):
    member = tarfile.TarInfo(name)
    member.size = 1
    if kind in {"link", "hardlink"}:
        member.type = tarfile.SYMTYPE if kind == "link" else tarfile.LNKTYPE
        member.linkname = "../escape"
    archive = _archive(tmp_path, extra=member)
    with pytest.raises(ValueError):
        _prepare(tmp_path, archive)
    assert not (tmp_path / "inputs").exists()
    assert not (tmp_path / "escape").exists()


@pytest.mark.parametrize(
    "mutation", ["absolute", "parent", "source_time", "source_digest", "missing_blob", "wrong_release_pin"]
)
def test_path_only_relocation_and_existing_source_verification(tmp_path, mutation):
    selection = tmp_path / "selected"
    shutil.copytree(FIXTURE, selection)
    portable = json.loads((selection / "manifest.json").read_bytes())
    if mutation == "absolute":
        portable["collections"][0]["blob_root"] = str(FIXTURE / "release-blobs")
    elif mutation == "parent":
        portable["collections"][0]["blob_root"] = "../release-blobs"
    elif mutation in {"source_time", "source_digest"}:
        field, value = (
            ("observedAt", "2026-09-22T00:00:00Z")
            if mutation == "source_time"
            else ("responseSha256", "sha256:" + "0" * 64)
        )
        portable["collections"][1]["scope"]["capture"][field] = value
    elif mutation == "wrong_release_pin":
        portable["collections"][0]["artifact_sha256"] = "sha256:" + "0" * 64
        source = json.loads((selection / "source-manifest.json").read_bytes())
        source["collections"][0]["artifact_sha256"] = "sha256:" + "0" * 64
        (selection / "source-manifest.json").write_text(json.dumps(source))
    else:
        next((selection / "capture-blobs/sha256").iterdir()).unlink()
    (selection / "manifest.json").write_text(json.dumps(portable))
    archive = _archive(tmp_path, selection)
    if mutation in {"missing_blob", "wrong_release_pin"}:
        manifest = _prepare(tmp_path, archive, manifest_sha256=_digest(selection / "manifest.json"))
        with pytest.raises((ValueError, OSError)):
            build_fec_observations(manifest, tmp_path / "output")
        assert not (tmp_path / "output").exists()
    else:
        with pytest.raises(ValueError):
            _prepare(tmp_path, archive, manifest_sha256=_digest(selection / "manifest.json"))
        assert not (tmp_path / "inputs").exists()


def test_workflows_pin_inputs_delegate_and_retain_successful_build_only_outputs():
    workflows = ROOT / ".github/workflows"
    shared = yaml.safe_load((workflows / "_rollup.yml").read_text())
    declared = shared[True]["workflow_call"]["inputs"]
    for name in ("fec-source-catalog", "fec-observations"):
        caller = yaml.safe_load((workflows / f"rollup-{name}.yml").read_text())
        assert caller[True]["workflow_dispatch"]["inputs"]["skip_upload"]["default"] is True
        assert caller["concurrency"]["cancel-in-progress"] is False
        assert caller["jobs"]["run"]["uses"] == "./.github/workflows/_rollup.yml"
        assert set(caller["jobs"]["run"]["with"]) <= set(declared)
    steps = {step["name"]: step for step in shared["jobs"]["rollup"]["steps"]}
    preparation = steps["Prepare explicit retained FEC inputs"]
    assert preparation["if"] == "inputs.command == 'build-fec-observations'"
    assert "--archive-sha256" in preparation["run"] and "--manifest-sha256" in preparation["run"]
    assert "--max-filesize" in preparation["run"]
    run = steps["Run rollup"]
    assert 'ARGS+=(--manifest "$RUNNER_TEMP/fec-retained-inputs/manifest.json")' in run["run"]
    assert steps["Retain invocation and transferred-input audit"]["if"] == "always()"
    assert steps["Retain build-only outputs and failures"]["if"] == "failure() || (success() && inputs.skip_upload)"
    for step in steps.values():
        if "run" in step:
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)
