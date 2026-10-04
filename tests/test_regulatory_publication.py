"""Live scheduler publication uses native rows and selected shared receipts."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.regulatory_publication import (
    finish_checkpoints,
    finish_dataset,
    restore_checkpoint,
    restore_dataset,
)
from spicy_regs.schemas import DOCKET


def test_checkpoint_family_preserves_other_retry_set_and_retires_empty(tmp_path):
    failed = {
        "agency": "EPA",
        "record_type": "dockets",
        "key": "raw/EPA/D/docket/D.json",
        "status": "unreadable",
        "reason": "truncated",
        "attempted_at": "2026-10-03",
        "attempts": 2,
    }
    pending = {
        "agency_code": "EPA",
        "docket_id": "D",
        "comment_id": "C",
        "posted_date": None,
        "phase": "fetch",
        "reason": "timeout",
        "source_json": "{}",
        "rule_version": "v1",
        "attempted_at": "2026-10-03",
        "attempts": 1,
    }
    finish_checkpoints(tmp_path, {"failed_keys": [failed], "pending_comment_text": [pending]}, publish=False)
    assert restore_checkpoint(tmp_path, "failed_keys") == [failed]
    assert restore_checkpoint(tmp_path, "pending_comment_text") == [pending]
    finish_checkpoints(tmp_path, {"failed_keys": [], "pending_comment_text": [pending]}, publish=False)
    assert restore_checkpoint(tmp_path, "failed_keys") == []
    assert restore_checkpoint(tmp_path, "pending_comment_text") == [pending]
    assert not (tmp_path / "failed_keys.parquet").exists()
    assert not (tmp_path / "pending_comment_text.parquet").exists()


def test_native_dataset_roundtrip_and_missing_receipt_refusal(tmp_path):
    work = tmp_path / ".processing"
    work.mkdir()
    source = work / "dockets.parquet"
    row = {name: None for name in DOCKET.schema}
    row.update(docket_id="D", agency_code="EPA", title="Decision", rin="Not Assigned")
    pq.write_table(
        pa.Table.from_pylist([row], schema=pa.schema([(name, pa.string()) for name in DOCKET.schema])), source
    )
    finish_dataset(tmp_path, "dockets", source, publish=False)
    native = pq.read_table(tmp_path / "dockets.parquet")
    from spicy_regs.transforms.regulations_receipts import policy

    assert native.schema.equals(policy("dockets").subject_schema)
    assert restore_dataset(tmp_path, "dockets", work / "restored.parquet")
    assert pq.read_table(work / "restored.parquet").to_pylist() == [row]
    selected = json.loads((tmp_path / ".native-state/dockets.json").read_text())
    (Path(selected["directory"]) / "etl_receipts.parquet").unlink()
    with pytest.raises((ValueError, OSError)):
        restore_dataset(tmp_path, "dockets", work / "restored.parquet")
