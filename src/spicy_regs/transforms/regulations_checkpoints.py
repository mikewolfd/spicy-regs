"""Regulatory retry checkpoints as processing-only shared ETL receipts."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, read_attempts, write_dataset, select_receipts

CHECKPOINT_FIELDS = {
    "pending_comment_text": (
        "agency_code",
        "docket_id",
        "comment_id",
        "posted_date",
        "phase",
        "reason",
        "source_json",
        "rule_version",
        "attempted_at",
        "attempts",
    ),
    "failed_keys": ("agency", "record_type", "key", "status", "reason", "attempted_at", "attempts"),
}
CHECKPOINT_KEYS = {"pending_comment_text": "comment_id", "failed_keys": "key"}


def checkpoint_policy(name):
    return DatasetPolicy(
        name,
        pa.schema([]),
        (),
        (*CHECKPOINT_FIELDS[name], "checkpoint_empty"),
        policy_version="regulations-checkpoint-v1",
        receipt_only=True,
    )


def write_checkpoint(name: str, rows, destination: Path, context: ReceiptContext):
    """Preserve active retries or an explicit empty checkpoint, without subjects."""
    declared = checkpoint_policy(name)

    def records():
        seen = set()
        for ordinal, row in enumerate(rows):
            if set(row) - set(CHECKPOINT_FIELDS[name]):
                raise ValueError(f"{name}: unclassified checkpoint fields")
            identity = row.get(CHECKPOINT_KEYS[name])
            if not isinstance(identity, str) or not identity or identity in seen:
                raise ValueError(f"{name}: checkpoint key is missing or repeated")
            seen.add(identity)
            yield {**row, "checkpoint_empty": False}, replace(context, attempt_id=f"{context.attempt_id}:{ordinal}")
        if not seen:
            yield {"checkpoint_empty": True}, replace(context, attempt_id=f"{context.attempt_id}:empty")

    subject, receipt = write_dataset(records(), destination, declared)
    assert subject is None
    return receipt


def read_checkpoint(name: str, receipt: Path, *, generation_id: str):
    """Restore one selected active retry set; an empty marker retires old failures."""
    rows = []
    seen = set()
    empty = False
    with TemporaryDirectory(prefix="checkpoint-read-") as temporary:
        scoped = select_receipts(receipt, Path(temporary) / "receipts.parquet", dataset=name)
        attempts = list(read_attempts([scoped], checkpoint_policy(name), generation_id=generation_id))
    for attempt in attempts:
        if attempt["outcome"] != "observed":
            raise ValueError("Checkpoint contains an unqualified attempt")
        values = dict(attempt["processing_fields"])
        marker = values.pop("checkpoint_empty", None)
        if marker is True:
            if values or empty:
                raise ValueError("Malformed or repeated empty checkpoint")
            empty = True
            continue
        identity = values.get(CHECKPOINT_KEYS[name])
        if marker is not False or not isinstance(identity, str) or not identity or identity in seen:
            raise ValueError("Malformed or repeated checkpoint identity")
        seen.add(identity)
        rows.append(values)
    if empty and rows or not empty and not rows:
        raise ValueError("Checkpoint is ambiguous or has no empty marker")
    return rows
