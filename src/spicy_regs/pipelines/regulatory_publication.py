"""Native regulatory file generations and generation-bound retry checkpoints."""

from __future__ import annotations

import json
import hashlib
import os
import shutil

import pyarrow.parquet as pq
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from spicy_regs.etl_receipts import ReceiptContext, combine_receipts
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.sources import publication, r2
from spicy_regs.transforms.regulations_checkpoints import checkpoint_policy, read_checkpoint, write_checkpoint
from spicy_regs.transforms.regulations_receipts import ReceiptInput, build_local_generation, materialize_internal


def _pointer(root: Path, dataset: str) -> Path:
    return root / ".native-state" / (dataset + ".json")


def _local(root: Path, dataset: str):
    pointer = _pointer(root, dataset)
    if not pointer.exists():
        return None
    value = json.loads(pointer.read_text())
    directory = Path(value["directory"])
    artifact = verify_generation(directory)
    if artifact.pin.artifact_digest != value["digest"]:
        raise ValueError("Selected local regulatory generation changed")
    return directory, artifact.root["spec"]["etlReceipts"]["generationId"]


def _remember(root: Path, dataset: str, directory: Path, artifact):
    pointer = _pointer(root, dataset)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    temporary = pointer.with_suffix(".tmp")
    temporary.write_text(json.dumps({"directory": str(directory.resolve()), "digest": artifact.pin.artifact_digest}))
    temporary.replace(pointer)


def restore_dataset(root: Path, dataset: str, destination: Path, *, index=None) -> bool:
    selected = _local(root, dataset)
    held = root / (dataset + ".parquet")
    if selected is None and held.exists():
        if "record_id" in pq.read_schema(held).names:
            raise ValueError(f"{dataset}: native local rows have no selected receipt generation")
        finish_dataset(root, dataset, held, publish=False, index=index)
        selected = _local(root, dataset)
    if selected:
        directory, generation = selected
        materialize_internal(
            ReceiptInput(
                dataset, (directory / (dataset + ".parquet"),), directory / "etl_receipts.parquet", generation
            ),
            destination,
        )
        return True
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    with TemporaryDirectory(dir=root, prefix=".selected-") as temp:
        return SelectedPriors(Path(temp), index=index).download(dataset + ".parquet", destination)


def finish_dataset(root: Path, dataset: str, source: Path, *, publish: bool, index=None):
    public_url = os.getenv("R2_PUBLIC_URL")
    index = (
        index
        if index is not None
        else (publication.current_index(public_url) if public_url else publication.empty_index())
    )
    owner = publication.table_owner(index, dataset + ".parquet")
    family = owner[0] if owner else dataset.replace("_", "-")
    directory = root / "generations" / uuid4().hex
    pair = source.parent / ".catalog-pairs" / dataset
    if (pair / "generation.json").exists():
        from spicy_regs.transforms.regulations_receipts import policy
        from spicy_regs.native_types import described_schema

        selected = json.loads((pair / "generation.json").read_text())
        if selected["dataset"] != dataset:
            raise ValueError("Catalog export identifies a different dataset")
        declared = policy(dataset)
        artifact = build_generation(
            directory,
            family=family,
            files=[pair / (dataset + ".parquet")],
            expected_keys=[dataset + ".parquet"],
            schemas={dataset: described_schema(declared.subject_schema)},
            publication_status="complete-family",
            receipt_path=pair / "etl_receipts.parquet",
            receipt_policies=[declared],
            receipt_generation_id=selected["generation_id"],
        )
    else:
        artifact = build_local_generation(
            {dataset: source}, directory, generation_id=uuid4().hex, family=family, publication_status="complete-family"
        )
    for batch in pq.ParquetFile(directory / "etl_receipts.parquet").iter_batches(columns=["outcome"]):
        if any(value in {"refused", "error", "rejected"} for value in batch.column(0).to_pylist()):
            raise ValueError(f"{dataset}: native conversion refused; checkpoint remains unadvanced")
    if publish:
        r2.require_credentials("Regulatory generation publication")
        publication.publish_generation(
            directory, client=r2.get_r2_client(), bucket=os.getenv("R2_BUCKET_NAME", "spicy-regs"), prior_index=index
        )
    _remember(root, dataset, directory, artifact)
    shutil.copyfile(directory / (dataset + ".parquet"), root / (dataset + ".parquet"))
    return directory / (dataset + ".parquet")


def restore_checkpoint(root: Path, dataset: str, *, index=None) -> list[dict]:
    selected = _local(root, dataset)
    if selected:
        directory, generation = selected
        return read_checkpoint(dataset, directory / "etl_receipts.parquet", generation_id=generation)
    public_url = os.getenv("R2_PUBLIC_URL")
    if not public_url:
        return []
    index = index if index is not None else publication.current_index(public_url)
    owners = [f for f in index["families"].values() if dataset in f.get("etlReceipts", {}).get("datasets", ())]
    if not owners:
        if publication.table_owner(index, dataset + ".parquet"):
            raise ValueError(f"{dataset}: selected checkpoint must be migrated to shared receipts")
        return []
    if len(owners) != 1:
        raise ValueError("Ambiguous regulatory checkpoint owner")
    members = publication.receipt_members(index, dataset=dataset)
    if len(members) != 1:
        raise ValueError("Ambiguous regulatory checkpoint member")
    with TemporaryDirectory(dir=root, prefix=".checkpoint-") as temp:
        path = Path(temp) / "etl_receipts.parquet"
        member = members[0]
        if not publication.fetch_member(public_url, member, path, member.path):
            raise ValueError("Selected regulatory checkpoint unavailable")
        return read_checkpoint(dataset, path, generation_id=owners[0]["etlReceipts"]["generationId"])


def finish_checkpoints(root: Path, checkpoints: dict[str, list[dict]], *, publish: bool, witnesses=(), index=None):
    """Commit complete retry sets together, including explicit empty retirement."""
    root.mkdir(parents=True, exist_ok=True)
    generation = uuid4().hex
    directory = root / "generations" / generation
    with TemporaryDirectory(dir=root, prefix=".checkpoints-") as temp:
        work = Path(temp)
        evidence = root / ".checkpoint-evidence" / (generation + ".json")
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(checkpoints, sort_keys=True, separators=(",", ":")))
        witness = {
            "source_id": "regulatory-retry-state",
            "source_uri": str(evidence.resolve()),
            "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "locator": None,
            "body_version": generation,
        }
        context = ReceiptContext(generation, "checkpoint", "spicy-regs:regulatory-retries-v1", [witness, *witnesses])
        shards = [write_checkpoint(name, rows, work / name, context) for name, rows in checkpoints.items()]
        receipt = combine_receipts(shards, work / "etl_receipts.parquet")
        artifact = build_generation(
            directory,
            family="regulatory-checkpoints",
            files=[],
            expected_keys=[],
            schemas={},
            publication_status="complete-family",
            receipt_path=receipt,
            receipt_policies=[checkpoint_policy(name) for name in checkpoints],
            receipt_generation_id=generation,
        )
    if publish:
        public_url = os.getenv("R2_PUBLIC_URL")
        if not public_url:
            raise ValueError("Checkpoint publication requires R2_PUBLIC_URL")
        index = index if index is not None else publication.current_index(public_url)
        publication.publish_generation(
            directory,
            client=r2.get_r2_client(),
            bucket=os.getenv("R2_BUCKET_NAME", "spicy-regs"),
            prior_index=index,
            receipt_only_tables=frozenset(name + ".parquet" for name in checkpoints),
        )
    for name in checkpoints:
        _remember(root, name, directory, artifact)
    return directory


def migrate_checkpoints(root: Path, sources: dict[str, Path], *, publish: bool = False):
    """Explicitly migrate selected held retry files; never use them as a read fallback."""
    from dataclasses import asdict
    from spicy_docs.sources.mirrulations import KeyOutcome
    from spicy_regs.schemas import RECORD_TYPES

    if set(sources) != {"failed_keys", "pending_comment_text"}:
        raise ValueError("Select both retry datasets for their coordinated migration")
    checkpoints = {}
    for name, source in sources.items():
        rows = pq.read_table(source).to_pylist()
        if name == "failed_keys":
            for ordinal, row in enumerate(rows):
                if "status" not in row:
                    key = row["key"]
                    record_type = next(
                        rt.name for rt in RECORD_TYPES.values() if rt.path_pattern and rt.path_pattern in key
                    )
                    rows[ordinal] = {
                        "agency": key.split("/")[1],
                        "record_type": record_type,
                        **asdict(
                            KeyOutcome(
                                key,
                                "transport" if row["kind"] == "transient" else "unreadable",
                                "legacy diagnostic; prior attempts unknown",
                                row["run_at"],
                                0,
                            )
                        ),
                    }
        checkpoints[name] = rows
    witnesses = [
        {
            "source_id": name,
            "source_uri": str(path.resolve()),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "locator": None,
            "body_version": None,
        }
        for name, path in sources.items()
    ]
    return finish_checkpoints(root, checkpoints, publish=publish, witnesses=witnesses)
