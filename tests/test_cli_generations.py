"""Real local batch selection with hermetic remote bytes and failure injection."""

import argparse
import builtins
import hashlib
import io
import json
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

import httpx
import polars as pl
import pytest

from spicy_regs import cli
from spicy_regs.sources import publication


def _remote(monkeypatch, label="old", *, legacy=False):
    bodies = {}
    tables = {}
    for name in ("a", "b"):
        buffer = io.BytesIO()
        pl.DataFrame({"id": [f"{label}-{name}"], "title": [f"{label} title"]}).write_parquet(buffer)
        body = buffer.getvalue()
        bodies[name] = body
        tables[f"{name}.parquet"] = {
            "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "byteSize": len(body),
            "rows": 1,
            "columns": [["id", "VARCHAR"], ["title", "VARCHAR"]],
        }
    index = publication.empty_index()
    digest = hashlib.sha256(label.encode()).hexdigest()
    index["families"]["pair"] = {
        "prefix": f"generations/pair/{digest}",
        "logicalId": "urn:test:pair",
        "artifactDigest": "sha256:" + digest,
        "tables": tables,
    }
    index = publication.parse_index(json.dumps(index).encode())
    loads, requests = [], []

    def load(_):
        loads.append(True)
        return deepcopy(index)

    @contextmanager
    def stream(method, url, **kwargs):
        requests.append(url)
        name = url.rsplit("/", 1)[-1].removesuffix(".parquet")
        if name in {"a", "b"}:
            assert url == f"{cli.PUBLIC_URL}/{index['families']['pair']['prefix']}/{name}.parquet"
        yield httpx.Response(200, content=bodies[name], request=httpx.Request(method, url))

    if legacy:
        bodies["dockets"] = bodies["a"]
    monkeypatch.setattr(publication, "load_index", load)
    monkeypatch.setattr(cli.httpx, "stream", stream)
    return index, bodies, loads, requests


def _download(output, *names):
    cli.cmd_download(argparse.Namespace(output_dir=output, types=list(names), force=False))
    return (output / "current").resolve(strict=True)


def test_complete_managed_batch_records_one_snapshot_and_switches_together(tmp_path, monkeypatch):
    index, _, loads, requests = _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    assert len(loads) == 1 and len(requests) == 2
    metadata = json.loads((old / "download.json").read_text())
    assert metadata["publication"] == index
    assert metadata["status"] == "complete"
    assert set(metadata["selected"]) == {"a", "b"}
    assert not (tmp_path / "a.parquet").exists()
    _remote(monkeypatch, "new")
    new = _download(tmp_path, "a", "b")
    assert new != old
    assert pl.read_parquet(old / "a.parquet")["id"].to_list() == ["old-a"]
    assert pl.read_parquet(new / "a.parquet")["id"].to_list() == ["new-a"]
    assert pl.read_parquet(new / "b.parquet")["id"].to_list() == ["new-b"]


@pytest.mark.parametrize("failure", ["hash", "http"])
def test_member_failure_retains_prior_current_and_incomplete_evidence(tmp_path, monkeypatch, failure):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    _, bodies, _, _ = _remote(monkeypatch, "new")
    if failure == "hash":
        bodies["b"] = b"corrupted"
    else:
        real_stream = cli.httpx.stream

        @contextmanager
        def fail(method, url, **kwargs):
            if url.endswith("/b.parquet"):
                raise httpx.ConnectError("interrupted")
            with real_stream(method, url, **kwargs) as response:
                yield response

        monkeypatch.setattr(cli.httpx, "stream", fail)
    with pytest.raises(RuntimeError, match="Download incomplete: b"):
        _download(tmp_path, "a", "b")
    assert (tmp_path / "current").resolve() == old
    failed = next(path for path in (tmp_path / "download-runs").iterdir() if path != old)
    assert (failed / "a.parquet").exists()
    assert not (failed / "b.parquet").exists()
    assert json.loads((failed / "download.json").read_text())["status"] == "incomplete"


def test_pointer_failure_preserves_old_current(tmp_path, monkeypatch):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    _remote(monkeypatch, "new")
    replace = Path.replace

    def fail_current(self, target):
        if Path(target).name == "current":
            raise OSError("cannot switch current")
        return replace(self, target)

    monkeypatch.setattr(Path, "replace", fail_current)
    with pytest.raises(OSError, match="cannot switch"):
        _download(tmp_path, "a", "b")
    assert (tmp_path / "current").resolve() == old
    assert not list(tmp_path.glob(".current-*"))


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_current_does_not_replace_existing_user_path(tmp_path, monkeypatch, kind):
    _remote(monkeypatch)
    current = tmp_path / "current"
    if kind == "directory":
        current.mkdir()
        saved = current / "user-file"
    else:
        saved = current
    saved.write_text("preserve me")
    with pytest.raises(RuntimeError, match="preserving existing path"):
        _download(tmp_path, "a", "b")
    assert saved.read_text() == "preserve me"
    assert not current.is_symlink()


@pytest.mark.parametrize("damage", ["missing-member", "incomplete-metadata"])
def test_invalid_current_does_not_fall_back_to_stale_root(tmp_path, monkeypatch, damage):
    _remote(monkeypatch)
    batch = _download(tmp_path, "a", "b")
    (tmp_path / "a.parquet").write_bytes((batch / "a.parquet").read_bytes())
    if damage == "missing-member":
        (batch / "a.parquet").unlink()
    else:
        metadata = json.loads((batch / "download.json").read_text())
        metadata["status"] = "incomplete"
        (batch / "download.json").write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError, match="Current download"):
        cli._local_files(tmp_path)


def test_mixed_batch_labels_legacy_and_readers_prefer_selected_files(tmp_path, monkeypatch, capsys):
    _remote(monkeypatch, legacy=True)
    # Conflicting root files remain available as legacy only when unselected.
    pl.DataFrame({"id": ["stale-root"], "title": ["stale"]}).write_parquet(tmp_path / "a.parquet")
    pl.DataFrame({"id": ["standalone"], "title": ["legacy"]}).write_parquet(tmp_path / "other.parquet")
    batch = _download(tmp_path, "a", "b", "dockets")
    metadata = json.loads((batch / "download.json").read_text())
    assert metadata["selected"]["dockets"] == {"key": "dockets.parquet", "status": "legacy-unversioned"}
    args = argparse.Namespace(output_dir=tmp_path, data_type="a", agency=None, n=1, query="old", limit=5)
    cli.cmd_stats(args)
    cli.cmd_sample(args)
    cli.cmd_search(args)
    output = capsys.readouterr().out
    assert "legacy-unversioned" in output and "managed" in output
    assert "old-a" in output and "old-b" in output and "stale-root" not in output
    assert "OTHER" in output


def test_reader_holds_one_local_batch_if_current_switches_during_operation(tmp_path, monkeypatch, capsys):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    _remote(monkeypatch, "new")
    new = _download(tmp_path, "a", "b")
    (tmp_path / "current").unlink()
    (tmp_path / "current").symlink_to(old)
    read = pl.read_parquet
    visited = []

    def switch_after_first(path, **kwargs):
        visited.append(Path(path))
        if len(visited) == 1:
            (tmp_path / "current").unlink()
            (tmp_path / "current").symlink_to(new)
        return read(path, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", switch_after_first)
    cli.cmd_search(argparse.Namespace(output_dir=tmp_path, query="old", limit=5))
    assert visited == [old / "a.parquet", old / "b.parquet"]
    assert "old-b" in capsys.readouterr().out


def test_legacy_only_download_keeps_root_behavior(tmp_path, monkeypatch, capsys):
    _, bodies, _, _ = _remote(monkeypatch, legacy=True)
    cli.cmd_download(argparse.Namespace(output_dir=tmp_path, types=["dockets"], force=False))
    assert (tmp_path / "dockets.parquet").read_bytes() == bodies["dockets"]
    assert not (tmp_path / "current").exists()
    assert "legacy-unversioned" in capsys.readouterr().out


@pytest.mark.parametrize("name", ["../other", "a/b", "a.parquet", "A", "", "a?b"])
def test_table_names_refuse_paths(name):
    with pytest.raises(argparse.ArgumentTypeError):
        cli._table_name(name)


@pytest.mark.parametrize("command", ["download", "sample"])
def test_parser_accepts_rollup_names(monkeypatch, command):
    seen = []
    monkeypatch.setattr(cli, f"cmd_{command}", lambda args: seen.append(args))
    monkeypatch.setattr(
        "sys.argv", ["spicy-regs", command, *(["--types"] if command == "download" else []), "bill_actions"]
    )
    cli.main()
    assert (seen[0].types if command == "download" else [seen[0].data_type]) == ["bill_actions"]


def test_managed_download_does_not_import_optional_source_readers_or_mcp(tmp_path, monkeypatch):
    _remote(monkeypatch)
    original = builtins.__import__

    def base_install_only(name, *args, **kwargs):
        if name.split(".")[0] in {"spicy_docs", "rulespec_artifacts"} or name == "spicy_regs.mcp_server":
            raise ImportError(f"Optional module unavailable: {name}")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", base_install_only)
    _download(tmp_path, "a", "b")
