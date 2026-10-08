"""Local reuse requires exact publication pins and leaves shared evidence unchanged."""

from hashlib import sha256
from types import SimpleNamespace

import pytest

from spicy_regs.scorecards.inputs import stage_member


def member(body=b"verified bytes"):
    return SimpleNamespace(key="input.parquet", path="generations/source/input.parquet",
                           sha256="sha256:" + sha256(body).hexdigest(), byte_size=len(body))


def refuse_fetch(*args):
    raise AssertionError("Matching local evidence must avoid network acquisition")


def test_exact_local_member_is_reused_without_copying_or_mutating(tmp_path):
    source = tmp_path / "retained" / member().path
    source.parent.mkdir(parents=True)
    source.write_bytes(b"verified bytes")
    staged = stage_member(member(), tmp_path / "owned/input.parquet",
                          local_inputs=[tmp_path / "retained"], fetch=refuse_fetch)
    assert staged.is_symlink() and staged.resolve() == source
    assert source.read_bytes() == b"verified bytes"
    assert stage_member(member(), staged, fetch=refuse_fetch) == staged


def test_wrong_local_generation_is_skipped_before_bounded_download(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / member().key).write_bytes(b"wrong bytes!!!")
    calls = []

    def download(pin, target):
        calls.append(pin)
        target.write_bytes(b"verified bytes")
        return True

    staged = stage_member(member(), tmp_path / "owned/input.parquet", local_inputs=[cache], fetch=download)
    assert calls == [member()] and not staged.is_symlink()
    assert (cache / member().key).read_bytes() == b"wrong bytes!!!"


@pytest.mark.parametrize("body", [b"short", b"different byte"])
def test_corrupt_staged_input_is_refused_without_overwriting(tmp_path, body):
    target = tmp_path / "input.parquet"
    target.write_bytes(body)
    with pytest.raises(ValueError, match="immutable pin"):
        stage_member(member(), target, fetch=refuse_fetch)
    assert target.read_bytes() == body


def test_download_must_match_both_hash_and_size(tmp_path):
    def download(pin, target):
        target.write_bytes(b"different byte")
        return True
    with pytest.raises(ValueError, match="differs"):
        stage_member(member(), tmp_path / "input.parquet", fetch=download)


@pytest.mark.parametrize("locator", ["/tmp/elsewhere", "../elsewhere"])
def test_local_lookup_cannot_escape_selected_root(tmp_path, locator):
    pin = member()
    pin.path = locator
    with pytest.raises(ValueError, match="retained root"):
        stage_member(pin, tmp_path / "owned/input.parquet", local_inputs=[tmp_path], fetch=refuse_fetch)
