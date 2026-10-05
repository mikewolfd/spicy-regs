"""Index writer qualifies exact pins and never rewrites the receipt member."""
import duckdb
import pyarrow.parquet as pq
import pytest
from typing import Any
from spicy_regs.receipt_key_index_writer import build_key_index
from spicy_regs.receipt_key_index import KEY, lookup_receipts
from spicy_regs.sources.publication import file_identity
from tests.test_fec_receipt_adapter import fixture


def test_small_members_skip_and_forced_build_is_exact(tmp_path):
    _, receipts, _ = fixture(tmp_path)
    before = receipts.read_bytes()
    assert build_key_index(receipts) is None
    assert not (tmp_path / KEY).exists()
    key = tmp_path / 'sidecar' / KEY
    descriptor = build_key_index(receipts, key, force=True)
    assert descriptor is not None
    identity = file_identity(receipts)
    receipt = {"sha256": identity["sha256"], "byteSize": identity["bytes"],
               "rows": pq.ParquetFile(receipts).metadata.num_rows}
    original = pq.read_table(receipts).to_pylist()[0]
    with duckdb.connect() as con:
        assert lookup_receipts(con, str(receipts), str(key), descriptor, receipt, dataset="fec_receipts",
                               record_ids=[original["record_id"]]) == [[original]]
    assert receipts.read_bytes() == before
    with pytest.raises(ValueError, match="cannot replace"):
        build_key_index(receipts, receipts, force=True)


def test_local_generation_creates_index_only_above_bound(tmp_path, monkeypatch):
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.sources.publication import publish_generation, empty_index
    from tests.generation_fakes import Store
    subject, receipts, _ = fixture(tmp_path)
    monkeypatch.setattr("spicy_regs.receipt_key_index_writer.THRESHOLD", 0)
    before = receipts.read_bytes()
    generation = tmp_path / 'generation'
    artifact = build_generation(generation, family="fec-receipts", files=[subject], expected_keys=[subject.name],
                                receipt_path=receipts, receipt_policies=[installed_policies()["fec_receipts"]],
                                receipt_generation_id="new-publisher")
    assert verify_generation(generation).pin == artifact.pin
    specification = artifact.root["spec"]["etlReceipts"]
    assert specification["keyIndex"]["key"] == KEY
    assert set(artifact.root["spec"]["tables"]) == {subject.name}
    assert (generation / 'etl_receipts.parquet').read_bytes() == before
    index = publish_generation(generation, client=Store(), bucket="test", prior_index=empty_index())
    assert index["families"]["fec-receipts"]["etlReceipts"]["keyIndex"] == specification["keyIndex"]


def test_remote_indexed_generation_admits_and_publishes_exact_sidecar(tmp_path, monkeypatch):
    from spicy_regs import remote_generations as remote
    from spicy_regs.generations import _table_info
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.sources.publication import empty_index
    from spicy_regs.sources.remote_parquet import StoredParquet
    from tests.test_remote_generations import RemoteStore
    subject, receipts, _ = fixture(tmp_path)
    descriptor = build_key_index(receipts, force=True)
    assert descriptor is not None
    sidecar = receipts.with_name(KEY)
    store = RemoteStore()
    members = []
    schemas = {}
    for path in (subject, receipts, sidecar):
        key = 'stage/indexed/' + path.name
        raw = path.read_bytes()
        store.objects[key] = raw
        identity = file_identity(path)
        members.append(StoredParquet(key, len(raw), identity['sha256'],
                                     store.get_object(Bucket='test', Key=key)['ETag'],
                                     pq.ParquetFile(path).metadata.num_rows))
        if path != sidecar:
            schemas[path.stem] = _table_info(path)['columns']
    directory = tmp_path / 'remote-generation'
    arguments: dict[str, Any] = dict(client=store, bucket='test', staging_prefix='stage/indexed', members=members,
                     family='fec-indexed', expected_keys=[p.name for p in (subject, receipts, sidecar)], schemas=schemas,
                     receipt_policies=[installed_policies()['fec_receipts']], receipt_generation_id='selected-publisher')
    with pytest.raises(ValueError, match='exact key index declaration'):
        remote.prepare_remote_generation(tmp_path / 'undeclared', **arguments)
    monkeypatch.setattr("spicy_regs.receipt_key_index.THRESHOLD", 0)
    without_index: dict[str, Any] = {**arguments, "members": members[:2], "expected_keys": [subject.name, receipts.name]}
    with pytest.raises(ValueError, match='above the bound require a key index'):
        remote.prepare_remote_generation(tmp_path / 'over-bound', **without_index)
    artifact = remote.prepare_remote_generation(directory, receipt_key_index=descriptor, **arguments)
    assert set(artifact.root['spec']['tables']) == {subject.name}
    assert {p.name for p in directory.iterdir()} == {'artifact.json', 'members.json'}
    index = remote.publish_remote_generation(directory, client=store, bucket='test', staging_prefix='stage/indexed',
                                             members=members, prior_index=empty_index())
    assert index['families']['fec-indexed']['etlReceipts']['keyIndex'] == descriptor


def test_failed_index_verification_preserves_existing_sidecar_and_cleans_scratch(tmp_path, monkeypatch):
    _, receipts, _ = fixture(tmp_path)
    original = receipts.read_bytes()
    target = tmp_path / 'index-output' / KEY
    target.parent.mkdir()
    target.write_bytes(b'existing-sidecar')
    def refused(*args, **kwargs):
        raise ValueError('injected positional map failure')
    monkeypatch.setattr('spicy_regs.receipt_key_index_writer.verify_key_index', refused)
    with pytest.raises(ValueError, match='injected positional'):
        build_key_index(receipts, target, force=True)
    assert target.read_bytes() == b'existing-sidecar'
    assert receipts.read_bytes() == original
    assert sorted(p.name for p in target.parent.iterdir()) == [KEY]
