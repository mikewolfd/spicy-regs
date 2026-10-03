"""Build the scorecard reader overlay on the consumer's recorded provider baseline.

Run with Python 3.12 and uv available. The two archive builds must be byte
identical. The checkout's unrelated changes and package version are never read.
The explicit overlay includes the requested existing Docling/Ovis extraction
infrastructure and Vote Smart crosswalk retention. This produces a local
candidate, not a package-registry release.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile

BUILD_UV = "0.11.21"
BASELINE = "69964fe27c6ddc437e817041a3c5cca57a02723c"
REGISTRY = "src/spicy_docs/schemas/__init__.py"
OBSERVATION_TESTS = (
    "test_provider_tables_preserve_unknown_empty_and_merged_cell_metadata",
    "test_duplicate_native_text_geometry_keeps_distinct_observed_styles",
    "test_regional_insertion_preserves_native_column_order",
)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def overlay(repo: Path) -> dict[str, bytes]:
    paths = [repo / "src/spicy_docs/schemas/scorecard_tables.py", repo / "docs/sources/scorecards.md"]
    # The user explicitly requested these existing extraction capabilities.
    # Name each adopted path rather than sweep the dirty provider checkout.
    paths += [
        repo / path
        for path in (
            "src/spicy_docs/sources/legislators.py",
            "src/spicy_docs/schemas/legislator_tables.py",
            "docs/sources/legislators.md",
            "docs/sources/scorecards-govtrack-discovery.md",
            "docs/sources/scorecards-api-readers.md",
            "docs/sources/scorecards-hrc.md",
            "docs/tables.md",
            "docs/README.md",
            "tests/test_legislators.py",
            "tests/test_table_contracts.py",
            "src/spicy_docs/extraction/__init__.py",
            "src/spicy_docs/extraction/api.py",
            "src/spicy_docs/extraction/docling.py",
            "src/spicy_docs/extraction/docling_assets.py",
            "src/spicy_docs/extraction/docling_markdown.py",
            "src/spicy_docs/extraction/gemini.py",
            "src/spicy_docs/extraction/model.py",
            "src/spicy_docs/extraction/ocr.py",
            "docs/extraction/pdf-extraction-api.md",
        )
    ]
    paths += sorted((repo / "tests/extraction").glob("test_*.py"))
    paths += sorted(path for path in (repo / "tests/fixtures/ocr_conversion").rglob("*") if path.is_file())
    paths += sorted(path for path in (repo / "tests/fixtures/legislators").rglob("*") if path.is_file())
    paths += sorted((repo / "src/spicy_docs/sources/scorecards").rglob("*.py"))
    paths += sorted((repo / "tests").glob("test_scorecards*.py"))
    paths += sorted(path for path in (repo / "tests/fixtures/scorecards").rglob("*") if path.is_file())
    if not any(path.name.startswith("test_scorecards") for path in paths):
        raise ValueError("Scorecard parser tests must exist before packaging")
    files = {str(path.relative_to(repo)): path.read_bytes() for path in paths}
    # This mixed test module also exercises a newer reconstruction.evidence
    # implementation, which this extraction overlay deliberately does not adopt.
    # Select the extraction and baseline-compatible cases explicitly rather than
    # importing unrelated runtime changes or marking failing cases skipped.
    observation_path = "tests/extraction/test_observations.py"
    observations = files[observation_path].decode()
    functions = {node.name: node for node in ast.parse(observations).body if isinstance(node, ast.FunctionDef)}
    if set(OBSERVATION_TESTS) - functions.keys():
        raise ValueError("Selected observation tests changed; review the extraction overlay")
    selected_tests: list[str] = []
    for name in OBSERVATION_TESTS:
        segment = ast.get_source_segment(observations, functions[name])
        if segment is None:
            raise ValueError(f"Cannot locate selected observation test: {name}")
        selected_tests.append(segment)
    files[observation_path] = (
        '"""Adopted extraction behavior and baseline-compatible native evidence checks."""\n\n'
        "from spicy_docs.extraction import (\n"
        "    Box,\n    Observation,\n    PageContent,\n    PageResult,\n    Raster,\n"
        "    Recognition,\n    TableCell,\n    TableObservation,\n    TextBlock,\n)\n"
        "from spicy_docs.reconstruction.evidence import evidence_from_pages\n\n\n"
        + "\n\n\n".join(selected_tests)
        + "\n"
    ).encode()
    extraction_guide = "docs/extraction/pdf-extraction-api.md"
    current_guide = files[extraction_guide].decode()
    reconstruction_claim = (
        "Reconstruction can consume these shared\n"
        "blocks without a PyMuPDF dictionary; font/style data stays unknown where the\n"
        "provider does not state it. Native PyMuPDF observations retain their style reader."
    )
    if current_guide.count(reconstruction_claim) != 1:
        raise ValueError("Review reconstruction scope in the adopted extraction guide")
    files[extraction_guide] = current_guide.replace(
        reconstruction_claim,
        "This local package retains baseline reconstruction, which\n"
        "requires native PyMuPDF evidence. Structured page outcomes remain available\n"
        "through retained observations; general reconstruction support is not adopted.",
        1,
    ).encode()
    # These shared files also differ from the baseline for unrelated FEC work.
    # Add only scorecard sections to their baseline versions.
    test_path = "tests/test_table_contracts.py"
    current_tests = files[test_path].decode()
    baseline_tests = subprocess.check_output(["git", "show", f"{BASELINE}:{test_path}"], cwd=repo).decode()
    helper = current_tests.split("def _scorecard_cases()", 1)[1].split("def all_cases()", 1)[0]
    additions = {
        "from spicy_docs.schemas.regulations import DOCUMENT, RECORD_TYPES\n": "from spicy_docs.schemas.regulations import DOCUMENT, RECORD_TYPES\n"
        "from spicy_docs.schemas.scorecard_tables import SCORECARD_TABLES\n",
        "def all_cases()": "def _scorecard_cases()" + helper + "def all_cases()",
        "        + _gao_recommendation_cases()\n": "        + _gao_recommendation_cases()\n        + _scorecard_cases()\n",
        "FILLED_BY: dict[str, tuple[str, ...]] = {\n": "FILLED_BY: dict[str, tuple[str, ...]] = {\n"
        '    **{name: ("schemas/scorecard_tables.py", "sources/scorecards/common.py") for name in SCORECARD_TABLES},\n',
    }
    for marker, replacement in additions.items():
        if baseline_tests.count(marker) != 1:
            raise ValueError("Baseline table tests differ from the recorded overlay recipe")
        baseline_tests = baseline_tests.replace(marker, replacement, 1)
    files[test_path] = baseline_tests.encode()
    table_path = "docs/tables.md"
    rows = [line for line in files[table_path].decode().splitlines(keepends=True) if line.startswith("| `scorecard")]
    baseline_tables = subprocess.check_output(["git", "show", f"{BASELINE}:{table_path}"], cwd=repo).decode()
    anchor = next(
        line for line in baseline_tables.splitlines(keepends=True) if line.startswith("| `member_party_affiliations`")
    )
    files[table_path] = baseline_tables.replace(anchor, anchor + "".join(rows), 1).encode()
    index_path = "docs/README.md"
    guides = [line for line in files[index_path].decode().splitlines(keepends=True) if "(sources/scorecards" in line]
    baseline_index = subprocess.check_output(["git", "show", f"{BASELINE}:{index_path}"], cwd=repo).decode()
    anchor = next(
        line for line in baseline_index.splitlines(keepends=True) if line.startswith("- [Legislators crosswalk]")
    )
    files[index_path] = baseline_index.replace(anchor, anchor + "".join(guides), 1).encode()
    raw = subprocess.check_output(["git", "show", f"{BASELINE}:{REGISTRY}"], cwd=repo).decode()
    marker = "_REGISTERED: tuple[TableContract, ...] = ("
    if raw.count(marker) != 1 or "SCORECARD_TABLES" in raw:
        raise ValueError("Provider baseline registry differs from the recorded recipe")
    raw = raw.replace(
        marker,
        "from spicy_docs.schemas.scorecard_tables import SCORECARD_TABLES\n\n"
        + marker
        + "\n    *SCORECARD_TABLES.values(),",
        1,
    )
    files[REGISTRY] = raw.encode()
    metadata = subprocess.check_output(["git", "show", f"{BASELINE}:pyproject.toml"], cwd=repo).decode()
    prior_extra = 'pdf-docling = ["docling>=2,<3"]'
    if metadata.count(prior_extra) != 1:
        raise ValueError("Provider baseline Docling extra differs from the recorded recipe")
    build_requirement = 'requires = ["uv_build<0.12"]'
    if metadata.count(build_requirement) != 1:
        raise ValueError("Provider baseline build backend differs from the recorded recipe")
    files["pyproject.toml"] = (
        metadata.replace(prior_extra, 'pdf-docling = ["docling[ocrmac,rapidocr]==2.131.0"]', 1)
        .replace(build_requirement, f'requires = ["uv_build=={BUILD_UV}"]', 1)
        .encode()
    )
    return files


def build(repo: Path, directory: Path, files: dict[str, bytes], version: str) -> Path:
    directory.mkdir()
    source = directory / "source"
    source.mkdir()
    archive = subprocess.check_output(["git", "archive", BASELINE], cwd=repo)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(source, filter="data")
    for name, body in files.items():
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    metadata = source / "pyproject.toml"
    raw, count = re.subn(r'^version = "[^"]+"$', f'version = "{version}"', metadata.read_text(), count=1, flags=re.M)
    if count != 1:
        raise ValueError("Expected one baseline project version")
    metadata.write_text(raw)
    subprocess.run(["uv", "build", "--wheel", "--out-dir", str(directory / "dist")], cwd=source, check=True)
    wheels = list((directory / "dist").glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("Expected one candidate wheel")
    return wheels[0]


def compare_packages(baseline_wheel: Path, candidate: Path, files: dict[str, bytes]) -> list[str]:
    def package(path):
        with zipfile.ZipFile(path) as wheel:
            return {
                name: wheel.read(name)
                for name in wheel.namelist()
                if name.startswith("spicy_docs/") and not name.endswith("/")
            }

    before, after = package(baseline_wheel), package(candidate)
    changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
    allowed = {name.removeprefix("src/") for name in files if name.startswith("src/")}
    if set(changed) - allowed:
        raise ValueError(f"Candidate changes unrelated package files: {set(changed) - allowed}")
    if set(before) - set(after):
        raise ValueError("Candidate removed baseline package files")
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", type=Path, required=True)
    parser.add_argument("--baseline-wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    uv_version = subprocess.check_output(["uv", "--version"], text=True).strip()
    if uv_version.split()[:2] != ["uv", BUILD_UV] or sys.version_info[:2] != (3, 12):
        raise ValueError(f"Recipe requires uv {BUILD_UV} and Python 3.12")
    repo, output = args.provider.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    files = overlay(repo)
    hashes = {name: sha(raw) for name, raw in sorted(files.items())}
    digest = sha(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode())
    version = "0.53.0+scorecards." + digest[:12]
    first = build(repo, output / "first", files, version)
    second = build(repo, output / "second", files, version)
    if first.read_bytes() != second.read_bytes():
        raise ValueError("Independent builds are not byte-identical")
    changed = compare_packages(args.baseline_wheel, first, files)
    shutil.copyfile(first, output / first.name)
    receipt = {
        "build_environment": {"uv": uv_version, "python": sys.version, "backend": "uv_build==" + BUILD_UV},
        "baseline_commit": BASELINE,
        "baseline_wheel_sha256": sha(args.baseline_wheel.read_bytes()),
        "version": version,
        "overlay_sha256": digest,
        "overlay_files": hashes,
        "observation_test_scope": {
            "included": list(OBSERVATION_TESTS),
            "unadopted_runtime": "spicy_docs.reconstruction.evidence general non-native observations",
            "reason": "The explicit overlay adopts extraction only; baseline reconstruction remains unchanged.",
        },
        "wheel": first.name,
        "wheel_sha256": sha(first.read_bytes()),
        "wheel_bytes": first.stat().st_size,
        "repeat_build_identical": True,
        "changed_package_files": changed,
        "scope": "Local candidate only. Runtime and consumer qualification are separate.",
    }
    (output / "wheel.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
