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
