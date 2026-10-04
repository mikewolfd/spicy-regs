"""Unresolved-key observations, restored from R2 across agency/type subsets."""

from collections.abc import Iterable, Mapping
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
from loguru import logger
from spicy_docs.schemas.base import RecordType as SourceRecordType
from spicy_docs.sources.mirrulations import KeyOutcome
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.schemas import RecordType


UNRESOLVED_SCHEMA = pa.schema(
    [
        ("agency", pa.string()),
        ("record_type", pa.string()),
        ("key", pa.string()),
        ("status", pa.string()),
        ("reason", pa.string()),
        ("attempted_at", pa.string()),
        ("attempts", pa.int64()),
    ]
)


def source_record_type(record_type: RecordType) -> SourceRecordType:
    """Supply the source path while keeping this host's schema and extractor."""
    return SourceRecordType(**asdict(record_type))


class UnresolvedKeys:
    """Resume unsuccessful reads first, including legacy failed-key diagnostics.

    A legacy parse failure may also be in the old processed manifest. Supplying
    it through ``unresolved_keys`` bypasses that membership check and repairs
    the old reader's false coverage. Attempts before this format are unknown.
    """

    def __init__(self, output_dir: Path, *, index=None) -> None:
        from spicy_regs.pipelines.regulatory_publication import restore_checkpoint

        self.path = output_dir / ".processing" / "failed_keys.parquet"
        self.rows = {row["key"]: row for row in restore_checkpoint(output_dir, "failed_keys", index=index)}

    @classmethod
    def from_receipts(cls, receipt: Path, *, generation_id: str, output_dir: Path):
        """Restore selected failed-key observations without reading an old pointer."""
        from spicy_regs.transforms.regulations_checkpoints import read_checkpoint

        instance = cls.__new__(cls)
        instance.path = output_dir / ".processing" / "failed_keys.parquet"
        instance.rows = {
            row["key"]: row for row in read_checkpoint("failed_keys", receipt, generation_id=generation_id)
        }
        return instance

    def save_receipts(self, destination: Path, context) -> Path:
        """Retain the current unresolved-key set as processing-only receipts."""
        from spicy_regs.transforms.regulations_checkpoints import write_checkpoint

        return write_checkpoint("failed_keys", self.rows.values(), destination, context)

    def for_reader(self, agency: str, record_type: RecordType | SourceRecordType) -> list[KeyOutcome]:
        return [
            KeyOutcome(**{k: row[k] for k in KeyOutcome.__dataclass_fields__})
            for row in self.rows.values()
            if (row["agency"], row["record_type"]) == (agency, record_type.name)
        ]

    def update(self, successful: Iterable[str], outcomes: Mapping[tuple[str, str], Iterable[KeyOutcome]]) -> None:
        for key in successful:
            self.rows.pop(key, None)
        for (agency, record_type), observations in outcomes.items():
            for observation in observations:
                row = asdict(observation)
                row["reason"] = scrub_credential(row["reason"], "")
                self.rows[observation.key] = {"agency": agency, "record_type": record_type, **row}
        from spicy_regs.pipelines.regulatory_publication import finish_checkpoints, restore_checkpoint

        root = self.path.parent.parent
        finish_checkpoints(
            root,
            {
                "failed_keys": list(self.rows.values()),
                "pending_comment_text": restore_checkpoint(root, "pending_comment_text"),
            },
            publish=False,
        )
        logger.info("Retained {} unresolved keys; none manifested as coverage", len(self.rows))
