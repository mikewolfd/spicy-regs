"""Explicit builder qualification; generation writer activation is excluded."""
import duckdb
import pyarrow.parquet as pq
import pytest
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
