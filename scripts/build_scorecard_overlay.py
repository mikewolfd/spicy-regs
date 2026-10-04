"""Overlay scorecard modules onto the consumer's actual pinned provider wheel.

Preserve every unrelated runtime and dependency declaration byte-for-byte. Build
the deterministic wheel twice and record input/module/output pins. This avoids
rebuilding an obsolete provider baseline during concurrent repository work.
"""

import argparse
import base64
import csv
from hashlib import sha256
from io import StringIO
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def build(baseline: Path, provider: Path, output: Path):
    with ZipFile(baseline) as archive:
        files = {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}
    overlays = {
        "spicy_docs/sources/scorecards/" + path.name: path.read_bytes()
        for path in sorted((provider / "src/spicy_docs/sources/scorecards").glob("*.py"))
    }
    if not overlays or "spicy_docs/sources/scorecards/__init__.py" not in overlays:
        raise ValueError("Scorecard overlay does not contain the publisher registry")
    inputs = {name: sha256(raw).hexdigest() for name, raw in sorted(overlays.items())}
    baseline_sha = sha256(baseline.read_bytes()).hexdigest()
    digest = sha256(json.dumps({"baseline": baseline_sha, "modules": inputs}, sort_keys=True).encode()).hexdigest()
    version = "0.53.0+scorecards." + digest[:12]
    metadata_paths = [name for name in files if name.endswith(".dist-info/METADATA")]
    if len(metadata_paths) != 1:
        raise ValueError("Baseline must contain one installed distribution")
    old_info = metadata_paths[0].rsplit("/", 1)[0]
    new_info = "spicy_docs-" + version + ".dist-info"
    metadata = files[old_info + "/METADATA"].decode()
    previous = [line for line in metadata.splitlines() if line.startswith("Version: ")]
    if len(previous) != 1 or "Name: spicy-docs\n" not in metadata:
        raise ValueError("Baseline is not the expected provider distribution")
    metadata = metadata.replace(previous[0] + "\n", "Version: " + version + "\n", 1)
    result = {
        name.replace(old_info + "/", new_info + "/", 1): raw
        for name, raw in files.items()
        if name != old_info + "/RECORD"
    }
    result[new_info + "/METADATA"] = metadata.encode()
    result.update(overlays)
    for name, raw in files.items():
        if name.startswith("spicy_docs/") and name not in overlays and result.get(name) != raw:
            raise ValueError("Scorecard overlay changed an unrelated runtime file")
    record = StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, raw in sorted(result.items()):
        encoded = base64.urlsafe_b64encode(sha256(raw).digest()).decode().rstrip("=")
        writer.writerow((name, "sha256=" + encoded, len(raw)))
    writer.writerow((new_info + "/RECORD", "", ""))
    result[new_info + "/RECORD"] = record.getvalue().encode()
    wheel_name = "spicy_docs-" + version + "-py3-none-any.whl"
    output.mkdir(parents=True, exist_ok=False)
    paths = []
    for index in (1, 2):
        path = output / (str(index) + ".whl")
        with ZipFile(path, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
            for name, raw in sorted(result.items()):
                info = ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
                info.compress_type = ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, raw, compress_type=ZIP_DEFLATED, compresslevel=9)
        paths.append(path)
    if paths[0].read_bytes() != paths[1].read_bytes():
        raise ValueError("Repeated scorecard wheel builds differ")
    wheel = output / wheel_name
    wheel.write_bytes(paths[0].read_bytes())
    receipt = {
        "version": version,
        "baseline_wheel": baseline.name,
        "baseline_sha256": baseline_sha,
        "overlay_sha256": digest,
        "modules": inputs,
        "wheel": wheel_name,
        "wheel_sha256": sha256(wheel.read_bytes()).hexdigest(),
        "repeat_build_identical": True,
        "unrelated_runtime_and_dependency_metadata_preserved": True,
        "changed_runtime_files": [name for name, raw in overlays.items() if files.get(name) != raw],
        "scope": "Local candidate; tests and source qualification required before adoption",
    }
    (output / "wheel.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-wheel", type=Path, required=True)
    parser.add_argument("--provider", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.baseline_wheel, args.provider, args.output), indent=2))


if __name__ == "__main__":
    main()
