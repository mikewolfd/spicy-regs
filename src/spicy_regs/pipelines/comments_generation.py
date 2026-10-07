"""Seal the comments mirror and its aggregate index with exact ETL receipts."""
from dataclasses import asdict
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow.parquet as pq

from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, write_dataset
from spicy_regs.generations import build_generation
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.regulations_receipts import policy


def build_comments_generation(output_dir, result, snapshot):
    """Require the catalog's selected pair; derive index receipts from that body."""
    metadata = json.loads(result["generation"].read_text())
    if metadata.get("dataset") != "comments" or not metadata.get("generation_id"):
        raise ValueError("Comments export has no selected receipt generation")
    generation_id = metadata["generation_id"]
    comments = result["comments"]
    if metadata.get("snapshot") != asdict(snapshot):
        raise ValueError("Comments receipts name a different catalog snapshot")
    index_policy = policy("comments_index")
    witness = {"source_id": "comments", "source_uri": None,
               "sha256": file_identity(comments)["sha256"],
               "locator": json.dumps(asdict(snapshot), sort_keys=True), "body_version": generation_id}
    with TemporaryDirectory(prefix="comments-index-receipts-", dir=output_dir) as temporary:
        temporary = Path(temporary)
        def records():
            ordinal = 0
            with pq.ParquetFile(result["index"]) as source:
                for batch in source.iter_batches(batch_size=2000):
                    for row in batch.to_pylist():
                        yield row, ReceiptContext(generation_id, str(ordinal), "comments-index/1", [witness])
                        ordinal += 1
        index_subject, index_receipts = write_dataset(records(), temporary / "index", index_policy)
        assert index_subject is not None
        shared = combine_receipts([result["receipts"], index_receipts], temporary / "etl_receipts.parquet")
        directory = output_dir / ".comments-generations" / uuid4().hex
        build_generation(directory, family="comments", files=[comments, index_subject],
                         expected_keys=("comments.parquet", "comments_index.parquet"),
                         parents={"catalog_comments": {"sha256": witness["sha256"], "byteSize": comments.stat().st_size}},
                         receipt_path=shared, receipt_policies=[policy("comments"), index_policy],
                         receipt_generation_id=generation_id, adopt_owned_receipt=True)
    return directory
