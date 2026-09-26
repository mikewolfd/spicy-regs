"""A derived generation records the exact parents it read (audit finding D4).

Managed parents name their family and generation and reuse the verified
descriptor digest; bare parents record the digest of the bytes read; inputs
read in place over HTTP record their storage version and must not change while
the rollup builds. The verifier holds every managed parent to the index the
generation captured.
"""

import hashlib
import json
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import publication as pub, r2
from tests.generation_fakes import Store


def _table(path: Path, value: str) -> Path:
    pq.write_table(pa.table({"id": [value]}), path, store_schema=False)
    return path


class _Base(RollupPipeline):
    name: ClassVar[str] = "base"
    output: ClassVar[str] = "base.parquet"

    def build(self, output_dir: Path) -> Path:
        return _table(output_dir / self.output, "base")


class _Derived(RollupPipeline):
    name: ClassVar[str] = "derived"
    inputs: ClassVar[tuple[str, ...]] = ("base.parquet", "bare.parquet")
    remote_inputs: ClassVar[tuple[str, ...]] = ("remote.parquet",)
    output: ClassVar[str] = "derived.parquet"

    def build(self, output_dir: Path) -> Path:
        return _table(output_dir / self.output, "derived")



def _serve(monkeypatch, store: Store, versions: list[dict]) -> bytes:
    """Serve the managed member and a bare object; report ``versions`` for the remote input in turn."""
    bare = pa.BufferOutputStream()
    pq.write_table(pa.table({"id": ["bare"]}), bare)
    bare_bytes = bare.getvalue().to_pybytes()

    def download(remote_key, local_path):
        if remote_key == "bare.parquet":
            local_path.write_bytes(bare_bytes)
            return True
        location, _ = pub.table_location(pub.parse_index(store.objects[pub.INDEX_KEY]), remote_key)
        local_path.write_bytes(store.objects[location])
        return True

    monkeypatch.setattr(r2, "download", download)
    monkeypatch.setattr(r2, "public_object_version", lambda url: versions.pop(0))
    return bare_bytes


def _parents(store: Store, family: str) -> dict:
    entry = pub.parse_index(store.objects[pub.INDEX_KEY])["families"][family]
    root = json.loads(store.objects[f"{entry['prefix']}/artifact.json"])
    return root["spec"]["parents"]


def test_a_derived_generation_records_each_parent_it_read(tmp_path, monkeypatch, remote):
    _Base(output_dir=tmp_path / "base", skip_upload=False).run()
    base = pub.parse_index(remote.objects[pub.INDEX_KEY])["families"]["base"]
    bare = _serve(monkeypatch, remote, [{"etag": '"v1"', "bytes": 10}, {"etag": '"v1"', "bytes": 10}])

    _Derived(output_dir=tmp_path / "derived", skip_upload=False).run()

    assert _parents(remote, "derived") == {
        "base.parquet": {"sha256": base["tables"]["base.parquet"]["sha256"],
                         "byteSize": base["tables"]["base.parquet"]["byteSize"],
                         "family": "base", "artifactDigest": base["artifactDigest"]},
        "bare.parquet": {"sha256": "sha256:" + hashlib.sha256(bare).hexdigest(), "byteSize": len(bare)},
        "remote.parquet": {"etag": '"v1"', "byteSize": 10},
    }


def test_a_remote_input_that_changes_during_the_build_is_refused(tmp_path, monkeypatch, remote):
    _Base(output_dir=tmp_path / "base", skip_upload=False).run()
    before = dict(remote.objects)
    _serve(monkeypatch, remote, [{"etag": '"v1"', "bytes": 10}, {"etag": '"v2"', "bytes": 11}])
    with pytest.raises(pub.PublicationError, match="remote input changed"):
        _Derived(output_dir=tmp_path / "derived", skip_upload=False).run()
    assert remote.objects == before


@pytest.mark.parametrize("parent", [
    {"sha256": "sha256:" + "0" * 64, "byteSize": 1, "family": "base", "artifactDigest": "sha256:" + "1" * 64},
    {"sha256": "sha256:" + "0" * 64, "byteSize": 1, "family": "base"},
    {"sha256": "sha256:" + "0" * 64, "etag": '"v1"', "byteSize": 1},
    {"etag": "", "byteSize": 1},
    {"sha256": "sha256:" + "0" * 64, "byteSize": -1},
])
def test_the_verifier_refuses_a_parent_it_cannot_bind(tmp_path, parent):
    with pytest.raises(ValueError, match="[Pp]arent"):
        build_generation(tmp_path / "artifact", family="derived", files=[_table(tmp_path / "derived.parquet", "d")],
                         expected_keys=["derived.parquet"], read_snapshot=pub.empty_index(),
                         parents={"base.parquet": parent})
