"""Execute the exact Worker/image startup owner against tiny real receipt checks."""

from copy import deepcopy
import json
from pathlib import Path
import runpy

import pytest

from spicy_regs import fec_release
from spicy_regs.runtime_bounds import current_bound
from spicy_regs.sources import publication
from tests.test_fec_release import fixture

STARTUP = Path(__file__).parents[1] / "deploy/cloudflare/start_reader.py"


@pytest.mark.parametrize(
    "change", [None, "code", "packages", "image", "receipt_bytes", "unavailable", "deadline"]
)
def test_exact_startup_refuses_before_exec_and_allows_matching_rollback(tmp_path, monkeypatch, change):
    _, _, receipt, consumer = fixture(tmp_path)
    raw = json.dumps(receipt).encode()
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", fec_release.sha256(raw))
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://fixture.invalid")
    monkeypatch.setenv("PORT", "9090")
    measure = fec_release.runtime_consumer
    actual = deepcopy(consumer)
    if change == "code":
        (tmp_path / "installed/consumer.py").write_text("VERSION = 2\n")
    elif change == "packages":
        actual["package_versions"]["duckdb"] = "different"
    elif change == "image":
        monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", fec_release.sha256(b"other"))

    def measured(image_digest):
        observed = measure(image_digest, package_root=tmp_path / "installed", packages=("duckdb",))
        if change == "packages":
            observed["package_versions"] = actual["package_versions"]
        return observed

    monkeypatch.setattr(fec_release, "runtime_consumer", measured)
    reads = []

    def read(url, **kwargs):
        reads.append(url)
        assert kwargs == {"allow_missing": False, "limit": fec_release.LIMIT}
        assert current_bound() is not None
        if change == "unavailable":
            raise OSError("private-token-must-not-escape")
        if change == "deadline":
            current_bound().cancel()
        return raw + b"changed" if change == "receipt_bytes" else raw

    monkeypatch.setattr(publication, "_bounded_get", read)
    launched = []
    monkeypatch.setattr("os.execvp", lambda executable, argv: launched.append((executable, argv)))
    if change:
        with pytest.raises(SystemExit, match="Reader release refused") as error:
            runpy.run_path(str(STARTUP), run_name="__main__")
        assert "private-token" not in str(error.value)
        assert not launched
    else:
        runpy.run_path(str(STARTUP), run_name="__main__")
        assert launched == [(
            "uvicorn", [
                "uvicorn", "spicy_regs.mcp_server:build_app", "--factory",
                "--host", "0.0.0.0", "--port", "9090",
            ],
        )]
    assert len(reads) == 1  # No automatic mismatch or transport retry.
    assert current_bound() is None
    if change == "code":
        # Selecting the compatible retained code/receipt works without bypassing checks.
        (tmp_path / "installed/consumer.py").write_text("VERSION = 1\n")
        runpy.run_path(str(STARTUP), run_name="__main__")
        assert len(launched) == 1


@pytest.mark.parametrize("partial", [None, "SPICY_REGS_FEC_RELEASE_SHA256", "SPICY_REGS_CONSUMER_IMAGE_DIGEST"])
def test_unqualified_fork_requires_both_pins_absent(monkeypatch, partial):
    for key in ("SPICY_REGS_FEC_RELEASE_SHA256", "SPICY_REGS_CONSUMER_IMAGE_DIGEST", "PORT"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://fixture.invalid")
    if partial:
        monkeypatch.setenv(partial, "invalid")
    monkeypatch.setattr(publication, "_bounded_get", lambda *a, **k: pytest.fail("Unexpected network"))
    launched = []
    monkeypatch.setattr("os.execvp", lambda *args: launched.append(args))
    if partial:
        with pytest.raises(SystemExit, match="Reader release refused"):
            runpy.run_path(str(STARTUP), run_name="__main__")
        assert not launched
    else:
        runpy.run_path(str(STARTUP), run_name="__main__")
        assert launched[0][1][-1] == "8080"
