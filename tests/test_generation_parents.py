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
        location = pub.single_member(pub.parse_index(store.objects[pub.INDEX_KEY]), remote_key).path
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


def test_a_managed_input_that_changes_during_the_build_is_refused(tmp_path, monkeypatch, remote):
    _Base(output_dir=tmp_path / "base", skip_upload=False).run()
    snapshot = pub.parse_index(remote.objects[pub.INDEX_V2_KEY])
    _serve(monkeypatch, remote, [{"etag": '"v1"', "bytes": 10}, {"etag": '"v1"', "bytes": 10}])
    build = _Derived.build

    def change_parent(self, output_dir):
        result = build(self, output_dir)
        _Base(output_dir=tmp_path / "new-base", skip_upload=False).run()
        return result

    monkeypatch.setattr(_Derived, "build", change_parent)
    with pytest.raises(pub.PublicationError, match="Managed parent changed"):
        _Derived(output_dir=tmp_path / "derived", skip_upload=False).run()
    current = pub.parse_index(remote.objects[pub.INDEX_V2_KEY])
    assert current["families"]["base"] != snapshot["families"]["base"]
    assert "derived" not in current["families"]


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


# --- one rule: parents are the tables of other families the build read --------


class _Self(RollupPipeline):
    """A rollup that reads its own published prior, as an incremental one does."""

    name: ClassVar[str] = "base"
    inputs: ClassVar[tuple[str, ...]] = ("base.parquet",)
    output: ClassVar[str] = "base.parquet"

    def build(self, output_dir: Path) -> Path:
        assert (output_dir / self.output).exists(), "the prior is primed"
        (output_dir / "next").mkdir()
        return _table(output_dir / "next" / self.output, "base-2")


class _SoftReader(RollupPipeline):
    """A rollup whose build reads another family's table best-effort, outside its declared inputs."""

    name: ClassVar[str] = "reader"
    output: ClassVar[str] = "reader.parquet"
    soft: ClassVar[str] = "base"

    def build(self, output_dir: Path) -> Path:
        from spicy_regs.transforms.table_merge import published_table

        published_table(output_dir, self.soft, r2.download)
        return _table(output_dir / self.output, "reader")


def _fetch_from(store: Store, monkeypatch) -> None:
    """Serve pinned members from ``store`` through the real download helper (only the HTTP transfer is faked)."""
    def fetch_member(base_url, member, local_path, label=None, **_):
        if member.path not in store.objects:
            return False
        local_path.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch_member)


def test_a_generation_refuses_its_own_family_as_a_parent(tmp_path, remote):
    _Base(output_dir=tmp_path / "base", skip_upload=False).run()
    index = pub.parse_index(remote.objects[pub.INDEX_KEY])
    entry = index["families"]["base"]
    own = {"sha256": entry["tables"]["base.parquet"]["sha256"], "byteSize": entry["tables"]["base.parquet"]["byteSize"],
           "family": "base", "artifactDigest": entry["artifactDigest"]}
    with pytest.raises(ValueError, match="own prior"):
        build_generation(tmp_path / "artifact", family="base", files=[_table(tmp_path / "base.parquet", "b")],
                         expected_keys=["base.parquet"], read_snapshot=index, parents={"base.parquet": own})


def test_the_audit_still_admits_a_published_root_whose_parent_is_its_own_family(tmp_path, remote):
    """gao-reports a7e7c05e, d5e6f3ac and print-citations c8e49dbb were published so; roots are immutable."""
    from rulespec_artifacts import LocalMemberSource, describe_member

    from spicy_regs.generations import _table_info, _write_generation_metadata, verify_generation

    _Base(output_dir=tmp_path / "base", skip_upload=False).run()
    index = pub.parse_index(remote.objects[pub.INDEX_KEY])
    entry = index["families"]["base"]
    directory = tmp_path / "historical"
    directory.mkdir()
    _table(directory / "base.parquet", "b")
    info = _table_info(directory / "base.parquet")
    _write_generation_metadata(
        directory, family="base", tables={"base.parquet": info}, read_snapshot=index,
        members=[describe_member(LocalMemberSource(directory), object_key="base.parquet", role="table",
                                 media_type="application/vnd.apache.parquet", record_count=info["rows"])],
        parents={"base.parquet": {"sha256": entry["tables"]["base.parquet"]["sha256"],
                                  "byteSize": entry["tables"]["base.parquet"]["byteSize"],
                                  "family": "base", "artifactDigest": entry["artifactDigest"]}})
    assert verify_generation(directory).root["spec"]["parents"]["base.parquet"]["family"] == "base"


def test_a_rollup_reading_its_own_prior_records_no_parent(tmp_path, monkeypatch, remote):
    _Base(output_dir=tmp_path / "first", skip_upload=False).run()
    _fetch_from(remote, monkeypatch)
    _Self(output_dir=tmp_path / "second", skip_upload=False).run()
    entry = pub.parse_index(remote.objects[pub.INDEX_KEY])["families"]["base"]
    assert "parents" not in json.loads(remote.objects[f"{entry['prefix']}/artifact.json"])["spec"]


def test_a_soft_read_of_another_family_is_recorded_at_the_bytes_read(tmp_path, monkeypatch, remote):
    _Base(output_dir=tmp_path / "base", skip_upload=False).run()
    base = pub.parse_index(remote.objects[pub.INDEX_KEY])["families"]["base"]
    _fetch_from(remote, monkeypatch)
    _SoftReader(output_dir=tmp_path / "reader", skip_upload=False).run()
    assert _parents(remote, "reader") == {"base.parquet": {
        "sha256": base["tables"]["base.parquet"]["sha256"], "byteSize": base["tables"]["base.parquet"]["byteSize"],
        "family": "base", "artifactDigest": base["artifactDigest"]}}


def test_an_absent_soft_input_records_nothing(tmp_path, monkeypatch, remote):
    _fetch_from(remote, monkeypatch)
    _SoftReader(output_dir=tmp_path / "reader", skip_upload=False).run()
    entry = pub.parse_index(remote.objects[pub.INDEX_KEY])["families"]["reader"]
    assert "parents" not in json.loads(remote.objects[f"{entry['prefix']}/artifact.json"])["spec"]
