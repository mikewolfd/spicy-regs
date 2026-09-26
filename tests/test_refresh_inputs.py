"""A refresh must finish against the same public base versions it started with."""

import json
import sys

import httpx
import pytest

from scripts import check_refresh_inputs as script


def test_changed_base_version_fails_refresh_verification(tmp_path, monkeypatch):
    etag = "original"
    client = httpx.Client

    def respond(request):
        return httpx.Response(200, headers={"etag": etag, "content-length": "100"})

    monkeypatch.setattr(script.httpx, "Client", lambda **kwargs: client(
        transport=httpx.MockTransport(respond), **kwargs))
    monkeypatch.setattr(script.publication, "load_index", lambda base_url: {"families": {}})
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://fork.example")
    receipt = tmp_path / "inputs.json"
    monkeypatch.setattr(sys, "argv", ["refresh", "capture", "--receipt", str(receipt)])
    assert script.main() == 0
    assert set(json.loads(receipt.read_text())["objects"]) == set(script.BASE_KEYS)
    monkeypatch.setattr(sys, "argv", ["refresh", "verify", "--receipt", str(receipt)])
    assert script.main() == 0
    etag = "replacement"
    with pytest.raises(RuntimeError, match="base versions changed"):
        script.main()


def test_changed_base_family_fails_refresh_verification(tmp_path, monkeypatch):
    """Dependents read the managed dockets/documents families, so their pins are part of the receipt."""
    client = httpx.Client
    monkeypatch.setattr(script.httpx, "Client", lambda **kwargs: client(
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"etag": "same", "content-length": "100"})), **kwargs))
    families = {"dockets": {"artifactDigest": "sha256:" + "a" * 64}, "other": {"artifactDigest": "sha256:" + "b" * 64}}
    monkeypatch.setattr(script.publication, "load_index", lambda base_url: {"families": families})
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://fork.example")
    receipt = tmp_path / "inputs.json"
    monkeypatch.setattr(sys, "argv", ["refresh", "capture", "--receipt", str(receipt)])
    assert script.main() == 0
    objects = json.loads(receipt.read_text())["objects"]
    assert set(objects) == set(script.BASE_KEYS) | {"family:dockets"}
    families["dockets"] = {"artifactDigest": "sha256:" + "c" * 64}
    monkeypatch.setattr(sys, "argv", ["refresh", "verify", "--receipt", str(receipt)])
    with pytest.raises(RuntimeError, match="base versions changed"):
        script.main()
