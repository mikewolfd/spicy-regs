"""A scorecard package update preserves the consumer's unrelated provider code."""

import base64
import csv
from hashlib import sha256
import importlib.util
from io import StringIO
from pathlib import Path
from zipfile import ZipFile

import pytest


SPEC = importlib.util.spec_from_file_location(
    "scorecard_overlay", Path(__file__).resolve().parents[1] / "scripts/build_scorecard_overlay.py"
)
assert SPEC is not None and SPEC.loader is not None
overlay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(overlay)


@pytest.mark.parametrize("baseline_version", ["0.53.0", "0.54.0+etl.reviewed"])
def test_repeated_build_preserves_runtime_dependencies_and_valid_record(tmp_path, baseline_version):
    baseline_info = "spicy_docs-" + baseline_version + ".dist-info"
    original = {
        "spicy_docs/etl/new_runtime.py": b"CURRENT_RUNTIME = True\n",
        "spicy_docs/transport/zyte.py": b"CURRENT_TRANSPORT = True\n",
        "spicy_docs/sources/scorecards/__init__.py": b"OLD = True\n",
        baseline_info + "/METADATA": (
            "Metadata-Version: 2.4\nName: spicy-docs\nVersion: " + baseline_version + "\nRequires-Dist: preserved==3\n"
        ).encode(),
        baseline_info + "/WHEEL": b"Wheel-Version: 1.0\nTag: py3-none-any\n",
        baseline_info + "/RECORD": b"old record\n",
    }
    baseline = tmp_path / "baseline.whl"
    with ZipFile(baseline, "w") as archive:
        for name, body in original.items():
            archive.writestr(name, body)
    provider = tmp_path / "provider"
    modules = provider / "src/spicy_docs/sources/scorecards"
    modules.mkdir(parents=True)
    (modules / "__init__.py").write_bytes(b"NEW = True\n")
    (modules / "publisher.py").write_bytes(b"PUBLISHER = True\n")
    receipt = overlay.build(baseline, provider, tmp_path / "output")
    receipt2 = overlay.build(baseline, provider, tmp_path / "output2")
    assert receipt["version"].startswith(baseline_version.split("+", 1)[0] + "+scorecards.")
    assert receipt == receipt2
    wheel = tmp_path / "output" / receipt["wheel"]
    assert wheel.read_bytes() == (tmp_path / "output2" / receipt2["wheel"]).read_bytes()
    with ZipFile(wheel) as archive:
        assert archive.read("spicy_docs/etl/new_runtime.py") == original["spicy_docs/etl/new_runtime.py"]
        assert archive.read("spicy_docs/transport/zyte.py") == original["spicy_docs/transport/zyte.py"]
        assert archive.read("spicy_docs/sources/scorecards/__init__.py") == b"NEW = True\n"
        assert archive.read("spicy_docs/sources/scorecards/publisher.py") == b"PUBLISHER = True\n"
        metadata = archive.read("spicy_docs-" + receipt["version"] + ".dist-info/METADATA")
        assert (
            metadata.replace(receipt["version"].encode(), baseline_version.encode())
            == original[baseline_info + "/METADATA"]
        )
        record_name = "spicy_docs-" + receipt["version"] + ".dist-info/RECORD"
        records = list(csv.reader(StringIO(archive.read(record_name).decode())))
        assert {row[0] for row in records} == set(archive.namelist())
        for name, digest, size in records:
            if name == record_name:
                assert (digest, size) == ("", "")
                continue
            body = archive.read(name)
            encoded = base64.urlsafe_b64encode(sha256(body).digest()).decode().rstrip("=")
            assert digest == "sha256=" + encoded
            assert int(size) == len(body)
