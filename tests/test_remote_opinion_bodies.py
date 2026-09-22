"""A real retained opinion uses identical local and remote row semantics."""

from datetime import date
from hashlib import sha256
from io import BytesIO
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import build_court_opinion_bodies as local_builder
from spicy_regs.transforms.build_court_opinion_bodies import stage_court_opinion_bodies_remote
from tests.test_remote_parquet import Store

FIXTURE = Path(__file__).parent / "fixtures/courtlistener_bulk/opinion-380204.csv.bz2"
PIN = "sha256:62cbca0ba7f3fb6c047d5e3448dbf48aad089b26bfceeaadd009d77a7fab44ab"


def stage(store, path=FIXTURE, pin=PIN):
    return stage_court_opinion_bodies_remote(
        client=store, bucket="fork", key="staging/opinions/body.parquet", local_file=path,
        source_sha256=pin, dump_date=date(2026, 6, 30), max_output_bytes=1024 * 1024,
    )


def test_native_opinion_remote_table_equals_existing_local_builder(tmp_path, monkeypatch):
    import importlib
    module = importlib.import_module("spicy_regs.transforms.build_court_opinion_bodies")
    monkeypatch.setattr(module, "check_headroom", lambda *args, **kwargs: None)
    store = Store()
    result = stage(store)
    local = local_builder(tmp_path, local_file=FIXTURE, dump_date=date(2026, 6, 30), rebuild=True)
    remote = pq.read_table(BytesIO(store.objects[result.key]))
    assert remote.equals(pq.read_table(local), check_metadata=True)
    assert result.rows == 1 and remote.num_columns == 25
    row = remote.to_pylist()[0]
    assert row["opinion_id"] == "380204"
    assert sha256(row["html"].encode()).hexdigest() == "716ae706f3d0aad0c2bc5fd5fa8f5a447e1f9d434fcc32bd007fe21c38084e8b"
    assert {p.name for p in tmp_path.iterdir()} == {"court_opinion_bodies.parquet"}


def test_source_pin_mismatch_refuses_before_any_remote_upload():
    store = Store()
    with pytest.raises(ValueError, match="differs from its pin"):
        stage(store, pin="sha256:" + "0" * 64)
    assert not store.uploads and not store.objects and not store.aborts


def test_changed_original_aborts_before_remote_completion(tmp_path, monkeypatch):
    import importlib
    module = importlib.import_module("spicy_regs.transforms.build_court_opinion_bodies")
    path = tmp_path / "source.bz2"
    path.write_bytes(FIXTURE.read_bytes())
    shape = module._shape

    def change_source(row, **kwargs):
        result = shape(row, **kwargs)
        path.write_bytes(path.read_bytes() + b"changed")
        return result

    monkeypatch.setattr(module, "_shape", change_source)
    store = Store()
    with pytest.raises(ValueError, match="changed or was not fully consumed"):
        stage(store, path)
    assert not store.objects and not store.uploads and store.aborts


def test_remote_batches_bound_rows_and_preserve_complete_tail():
    import importlib
    module = importlib.import_module("spicy_regs.transforms.build_court_opinion_bodies")
    from spicy_regs.sources.remote_parquet import write_remote_parquet

    rows = [{"id": str(i), "plain_text": None if i % 2 else ""} for i in range(4_001)]
    store = Store()
    result = write_remote_parquet(
        client=store, bucket="fork", key="staging/rows.parquet", schema=module._SCHEMA,
        batches=module._remote_opinion_batches(iter(rows), dump_date=date(2026, 6, 30)),
        max_bytes=1024 * 1024,
    )
    file = pq.ParquetFile(BytesIO(store.objects[result.key]))
    assert [file.metadata.row_group(i).num_rows for i in range(file.num_row_groups)] == [2_000, 2_000, 1]
    restored = file.read().to_pylist()
    assert restored == [module._shape(row, dump_date=date(2026, 6, 30)) for row in rows]


def test_remote_batches_count_utf8_and_keep_single_large_record(monkeypatch):
    import importlib
    module = importlib.import_module("spicy_regs.transforms.build_court_opinion_bodies")
    when = date(2026, 6, 30)
    row = {"id": "1", "plain_text": "😀" * 20, "html": ""}
    shaped = module._shape(row, dump_date=when)
    encoded = sum(len(value.encode()) for value in shaped.values() if value is not None)
    # One fits exactly, two do not. Character counts would incorrectly admit two.
    monkeypatch.setattr(module, "REMOTE_BATCH_TEXT_BYTES", encoded)
    large = {"id": "2", "plain_text": "😀" * 100, "html": None}
    rows = [row, row, large, row]
    batches = list(module._remote_opinion_batches(iter(rows), dump_date=when))
    assert [batch.num_rows for batch in batches] == [1, 1, 1, 1]
    assert [row for batch in batches for row in batch.to_pylist()] == [
        module._shape(row, dump_date=when) for row in rows
    ]
