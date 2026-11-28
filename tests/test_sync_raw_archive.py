"""Exercise the CLI against a real local archive and a stubbed S3 API."""

import base64
import gzip
import hashlib
import json

from botocore.stub import Stubber
import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from scripts import sync_raw_archive as sync

ORIGINAL = b"exact source bytes"


def _capture(store: CentralRawArchiveStore, *, wrapped: bool = False) -> tuple[str, str]:
    digest = hashlib.sha256(ORIGINAL).hexdigest()
    key = f"objects/sha256/{digest[:2]}/{digest}"
    if wrapped:
        key += ".gz"
        path = store.root / key
        path.parent.mkdir(parents=True)
        path.write_bytes(gzip.compress(ORIGINAL, mtime=0))
    else:
        store.put_blob(digest, len(ORIGINAL), (ORIGINAL,))
    receipt = store.record_receipt(
        files=[{"sha256": digest, "bytes": len(ORIGINAL)}],
        receipt={"source": "fixture"},
    )
    return key, receipt["object"]


@pytest.fixture
def archive(monkeypatch, tmp_path):
    store = CentralRawArchiveStore(tmp_path / "raw")
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(store.root))
    original, receipt = _capture(store)
    return store, original, receipt


@pytest.fixture
def r2(monkeypatch):
    monkeypatch.setenv("R2_ENDPOINT", sync.VERIFIED_ENDPOINT)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    client = sync._r2_client()
    monkeypatch.setattr(sync, "_r2_client", lambda: client)
    with Stubber(client) as stub:
        yield stub
        stub.assert_no_pending_responses()


def _params(key):
    return {"Bucket": sync.VERIFIED_BUCKET, "Key": sync.PREFIX + key}


def _head(stub, key, size=None, *, status=404):
    if size is None:
        stub.add_client_error("head_object", str(status), http_status_code=status, expected_params=_params(key))
    else:
        stub.add_response("head_object", {"ContentLength": size}, _params(key))


def _put(stub, key, payload, *, conflict=False):
    md5 = hashlib.md5(payload, usedforsecurity=False).digest()
    params = {
        **_params(key), "Body": payload, "IfNoneMatch": "*", "ContentLength": len(payload),
        "ContentMD5": base64.b64encode(md5).decode(), "ContentType": "application/octet-stream",
        "CacheControl": "public, max-age=31536000, immutable",
    }
    if conflict:
        stub.add_client_error("put_object", "PreconditionFailed", http_status_code=412, expected_params=params)
    else:
        stub.add_response("put_object", {"ETag": f'"{md5.hex()}"'}, params)


@pytest.mark.parametrize("flags", [[], ["--dry-run"]])
def test_dry_run_never_puts(archive, r2, capsys, flags):
    store, original, receipt = archive
    _head(r2, original)
    _head(r2, receipt)

    assert sync.main(flags) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "ok"
    assert report["raw_root"] == str(store.root)
    assert report["dry_run"] is True
    for kind in ("objects", "acquisitions"):
        assert report[kind]["missing_at_preflight"] == 1
        assert report[kind]["ensured_after_conditional_write"] == 0


@pytest.mark.parametrize("flag,wrapped", [("--execute", False), ("--no-dry-run", True)])
def test_execute_creates_missing_original_then_receipt(r2, tmp_path, capsys, flag, wrapped):
    store = CentralRawArchiveStore(tmp_path / "explicit-root")
    original, receipt = _capture(store, wrapped=wrapped)
    _head(r2, original)
    _head(r2, receipt)
    for key in (original, receipt):
        payload = (store.root / key).read_bytes()
        _put(r2, key, payload)
        _head(r2, key, len(payload))

    assert sync.main([flag, "--raw-root", str(store.root), "--receipt-key", receipt]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["dry_run"] is False
    for kind in ("objects", "acquisitions"):
        assert report[kind]["ensured_after_conditional_write"] == 1


def test_existing_size_match_skips_writes(archive, r2, capsys):
    store, original, receipt = archive
    for key in (original, receipt):
        _head(r2, key, (store.root / key).stat().st_size)

    assert sync.main(["--execute"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["objects"]["skipped_size_match"] == report["acquisitions"]["skipped_size_match"] == 1
    assert report["objects"]["missing_at_preflight"] == report["acquisitions"]["missing_at_preflight"] == 0


@pytest.mark.parametrize("kind", ["objects", "acquisitions"])
def test_size_mismatch_aborts_preflight_without_writing(archive, r2, capsys, kind):
    store, original, receipt = archive
    key = original if kind == "objects" else receipt
    if kind == "acquisitions":
        _head(r2, original)  # A missing original must not be written before the refusal.
    _head(r2, key, (store.root / key).stat().st_size + 1)

    assert sync.main(["--execute"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "failed"
    assert "remote size" in report["error"]
    assert key in report["error"]


@pytest.mark.parametrize("status", [403, 429, 500])
def test_head_errors_are_not_absence(archive, r2, capsys, status):
    _, original, _ = archive
    _head(r2, original, status=status)

    assert sync.main(["--execute"]) == 1
    assert str(status) in json.loads(capsys.readouterr().out)["error"]


def test_tampered_original_with_same_size_is_refused(archive, r2, capsys):
    store, original, receipt = archive
    (store.root / original).write_bytes(b"x" * len(ORIGINAL))
    _head(r2, original)
    _head(r2, receipt)

    assert sync.main(["--execute"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "failed"
    assert original in report["error"]
    assert "integrity verification" in report["error"]


def test_tampered_receipt_refused_before_any_request(archive, r2, capsys):
    store, _, receipt = archive
    (store.root / receipt).write_bytes(b"{}")

    assert sync.main(["--execute"]) == 1
    assert "receipt differs from its immutable identity" in json.loads(capsys.readouterr().out)["error"]


def test_race_never_overwrites_and_rechecks_size(archive, r2, capsys):
    _, original, receipt = archive
    _head(r2, original)
    _head(r2, receipt)
    _put(r2, original, ORIGINAL, conflict=True)
    _head(r2, original, len(ORIGINAL) + 1)

    assert sync.main(["--execute"]) == 1
    assert "remote size" in json.loads(capsys.readouterr().out)["error"]


def test_client_has_explicit_user_agent(r2):
    assert sync._r2_client().meta.config.user_agent_extra == sync.USER_AGENT


def test_unverified_destination_is_refused(archive, monkeypatch, capsys):
    monkeypatch.setenv("R2_ENDPOINT", "https://invalid.example")
    assert sync.main(["--execute"]) == 1
    assert "verified spicy-regs deployment" in json.loads(capsys.readouterr().out)["error"]
