"""Accepted receipt reuse preserves custody checks without opening originals."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog import maintenance, raw_archive
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.storage import connect
from tests.source_catalog.test_content_reviews import decision, review
from tests.source_catalog.test_input_refresh import file_rows
from tests.source_catalog.test_raw_archive import base_archive


@pytest.fixture
def accepted(tmp_path):
    root = tmp_path / "raw"
    base_archive(root)
    body = b"No meeting!!"
    sha = hashlib.sha256(body).hexdigest()
    store = CentralRawArchiveStore(root)
    store.put_blob(sha, len(body), [body])
    # Two accepted receipts name the same original; custody work must deduplicate.
    for run in ("one", "two"):
        store.record_receipt(files=[{"sha256": sha, "bytes": len(body), "http_status": 200}],
                             receipt={"run_id": run})
    store.record_receipt(files=[], receipt={"content_review": review(decision("sha256:" + sha))})
    config = tmp_path / "registry.json"
    config.write_text("{}")
    index = tmp_path / "base.sqlite"
    report = build(root, index, config=config)
    return root, sha, dict(index=index, output=tmp_path / "candidate.sqlite", config=config,
                          expected_generation=report["generation"], include_acquisitions=True)


def observe_original_access(monkeypatch, root):
    opens, stats = [], []
    original_open = os.open
    original_stat = Path.lstat
    object_names = {path.name for path in root.glob("objects/sha256/*/*")}

    def opened(path, *args, **kwargs):
        descriptor = original_open(path, *args, **kwargs)
        if Path(path).name in object_names:
            opens.append(str(path))
        return descriptor

    def stated(path, *args, **kwargs):
        if path.is_relative_to(root / "objects/sha256") and path.name not in {"sha256"}:
            stats.append(str(path.relative_to(root)))
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", opened)
    # The wrapper forwards dir_fd unchanged; retain that capability marker for
    # the maintained descriptor-relative reader's platform check.
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {opened})
    monkeypatch.setattr(Path, "lstat", stated)
    return opens, stats


def test_reuses_review_associations_and_deduplicates_original_custody(accepted, monkeypatch):
    root, sha, args = accepted
    # Model an older selected index whose review metadata remains indexed but
    # whose per-file association must be reapplied after content interpretation.
    with connect(args["index"]) as db:
        for ident, data in db.execute("SELECT id,data FROM files WHERE sha256=?", ("sha256:" + sha,)):
            facts = json.loads(data)
            facts.pop("content_review")
            db.execute("UPDATE files SET status='held',data=? WHERE id=?", (json.dumps(facts), ident))
    before = args["index"].read_bytes()
    opens, stats = observe_original_access(monkeypatch, root)
    result = maintenance.refresh_metadata(**args)
    assert args["index"].read_bytes() == before
    assert result["inputRefresh"]["newReceipts"] == 0
    assert opens == []
    # contained_file checks the final component, then lstat obtains its size.
    original = f"objects/sha256/{sha[:2]}/{sha}"
    assert stats.count(original) == 2
    assert {path for path in stats if len(Path(path).parts) == 4} == {original}
    fresh = root.parent / "fresh.sqlite"
    build(root, fresh, config=args["config"])
    assert file_rows(args["output"]) == file_rows(fresh)


def test_reuse_survives_a_fresh_process(accepted):
    root, _sha, args = accepted
    program = """
import json, os, sys
from pathlib import Path
from spicy_regs.source_catalog.maintenance import refresh_metadata
args = json.loads(sys.argv[1])
root = Path(args['index']).parent / 'raw'
object_names = {path.name for path in root.glob('objects/sha256/*/*')}
opened = []
original = os.open
def track(path, *args, **kwargs):
    descriptor = original(path, *args, **kwargs)
    if Path(path).name in object_names:
        opened.append(str(path))
    return descriptor
os.open = track
os.supports_dir_fd = os.supports_dir_fd | {track}
for key in ('index', 'output', 'config'):
    args[key] = Path(args[key])
result = refresh_metadata(**args)
print(json.dumps({'generation': result['generation'], 'opened': opened}))
"""
    first = subprocess.run([sys.executable, "-c", program, json.dumps(args, default=str)],
                           check=True, capture_output=True, text=True, env=os.environ)
    result = json.loads(first.stdout)
    assert result["opened"] == []
    second_args = {**args, "index": args["output"], "output": root.parent / "restart.sqlite",
                   "expected_generation": result["generation"]}
    second = subprocess.run([sys.executable, "-c", program, json.dumps(second_args, default=str)],
                            check=True, capture_output=True, text=True, env=os.environ)
    assert json.loads(second.stdout)["opened"] == []
    assert file_rows(args["output"]) == file_rows(second_args["output"])


@pytest.mark.parametrize("mutation", ["receipt", "removed-receipt", "original", "removed-original", "symlink"])
def test_changed_or_missing_custody_refuses_without_candidate(accepted, mutation):
    root, sha, args = accepted
    receipt = next(root.glob("acquisitions/sha256/*/*"))
    original = root / f"objects/sha256/{sha[:2]}/{sha}"
    if mutation == "receipt":
        receipt.write_bytes(receipt.read_bytes() + b" ")
    elif mutation == "removed-receipt":
        receipt.unlink()
    elif mutation == "original":
        original.write_bytes(b"changed stored size")
    elif mutation == "removed-original":
        original.unlink()
    else:
        outside = root.parent / "outside"
        original.rename(outside)
        original.symlink_to(outside)
    before = args["index"].read_bytes()
    with pytest.raises((ValueError, FileNotFoundError)):
        maintenance.refresh_metadata(**args)
    assert args["index"].read_bytes() == before
    assert not args["output"].exists()


def test_late_receipt_mutation_keeps_the_base(accepted, monkeypatch):
    root, _sha, args = accepted
    prior = raw_archive.RawImporter.reuse_receipts

    def mutate(self, pins):
        result = prior(self, pins)
        receipt = next(root.glob("acquisitions/sha256/*/*"))
        receipt.write_bytes(receipt.read_bytes() + b" ")
        return result

    monkeypatch.setattr(raw_archive.RawImporter, "reuse_receipts", mutate)
    with pytest.raises(RuntimeError, match="Input changed"):
        maintenance.refresh_metadata(**args)
    assert not args["output"].exists()


def test_new_receipt_preserves_the_normal_reader_path(accepted, monkeypatch):
    root, _sha, args = accepted
    CentralRawArchiveStore(root).record_receipt(files=[], receipt={"run_id": "new"})
    opens, _stats = observe_original_access(monkeypatch, root)
    result = maintenance.refresh_metadata(**args)
    assert result["inputRefresh"]["newReceipts"] == 1
    # The maintained reader opens the member for descriptor validation and
    # again for its stream, for each of the two receipt references.
    assert len(opens) == 4
