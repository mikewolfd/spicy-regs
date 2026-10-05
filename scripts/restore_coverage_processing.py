"""Restore private coverage inputs with the maintained selected-prior reader.

This local bridge never publishes files or reconstructs inputs from IDs. Its
caller supplies immutable selected subjects, receipts and generation identity.
"""

from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
import pyarrow.parquet as pq
import inspect
from spicy_regs.legislative_receipts import policy as legislative_policy
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, dataset_policy
from spicy_regs.selected_generations import SelectedDataset, remember_selection


def file_hash(path):
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def implementation_identity():
    root = Path(inspect.getfile(legislative_policy)).parent
    files = [
        Path(__file__),
        *(
            root / name
            for name in (
                "etl_receipts.py",
                "legislative_receipts.py",
                "legislative_documents.py",
                "legislative_document_fields.json",
                "congress_receipts.py",
                "congress_subjects.py",
                "selected_generations.py",
                "native_types.py",
                "pipelines/rollups/subject_receipts.py",
            )
        ),
    ]
    digest = hashlib.sha256()
    for file in files:
        data = file.read_bytes()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return "sha256:" + digest.hexdigest()


def checked_member(value):
    if not isinstance(value, dict) or set(value) != {"path", "sha256", "byteSize"}:
        raise ValueError("Coverage input needs an exact local file identity")
    path = Path(value["path"])
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise ValueError("Coverage input must be an existing absolute regular file")
    if type(value["byteSize"]) is not int or path.stat().st_size != value["byteSize"]:
        raise ValueError("Coverage input byte size changed")
    with path.open("rb") as source:
        digest = "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()
    if digest != value["sha256"]:
        raise ValueError("Coverage input SHA-256 changed")
    return path


def restore(request):
    if not isinstance(request, dict) or set(request) != {
        "dataset",
        "generationId",
        "subjects",
        "receipts",
        "destination",
    }:
        raise ValueError("Unsupported coverage restoration request")
    dataset = request["dataset"]
    selected_policy = dataset_policy(dataset)
    if not isinstance(request["generationId"], str) or not request["generationId"]:
        raise ValueError("Missing coverage receipt generation")
    if not isinstance(request["subjects"], list):
        raise ValueError("Coverage subjects must be explicit")
    subjects = tuple(checked_member(member) for member in request["subjects"])
    receipts = checked_member(request["receipts"])
    with pq.ParquetFile(receipts) as source:
        if not any(
            request["generationId"] in batch.column(0).to_pylist()
            for batch in source.iter_batches(columns=["generation_id"], batch_size=2000)
        ):
            raise ValueError("Selected coverage generation has no receipt context")
    destination = Path(request["destination"])
    if not destination.is_absolute() or destination.exists():
        raise ValueError("Coverage destination must be a new absolute directory")
    destination.mkdir(parents=True)
    remember_selection(destination, [SelectedDataset(dataset, subjects, receipts, request["generationId"])])
    selected = SelectedPriors(destination / "selected", root=destination, public_url="")
    restored = selected.get(dataset)
    if restored is None:
        raise ValueError("Selected coverage input is absent")
    members = sorted(restored.rglob("*.parquet")) if restored.is_dir() else [restored]
    schema = None
    rows = 0
    for member in members:
        with pq.ParquetFile(member) as source:
            current = described_schema(source.schema_arrow)
            if schema is not None and schema != current:
                raise ValueError("Restored member schemas differ")
            schema = current
            rows += source.metadata.num_rows
    return {
        "format": "spicygov-private-coverage-input",
        "version": 1,
        "dataset": dataset,
        "generationId": request["generationId"],
        "nativeSchema": [] if selected_policy.receipt_only else described_schema(selected_policy.subject_schema),
        "selection": {"subjects": request["subjects"], "receipts": request["receipts"]},
        "processingSchema": schema,
        "rows": rows,
        "urls": [str(path) for path in members],
        "receiptOnly": selected_policy.receipt_only,
        "policyVersion": selected_policy.policy_version,
        "policy": selected_policy.descriptor(),
        "processingMembers": [
            {"path": str(path), "sha256": file_hash(path), "byteSize": path.stat().st_size} for path in members
        ],
        "implementationSha256": implementation_identity(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema")
    args = parser.parse_args()
    if args.schema:
        selected_policy = dataset_policy(args.schema)
        print(
            json.dumps(
                {
                    "nativeSchema": []
                    if selected_policy.receipt_only
                    else described_schema(selected_policy.subject_schema),
                    "receiptOnly": selected_policy.receipt_only,
                    "policyVersion": selected_policy.policy_version,
                    "policy": selected_policy.descriptor(),
                    "implementationSha256": implementation_identity(),
                }
            )
        )
    else:
        print(json.dumps(restore(json.load(sys.stdin))))


if __name__ == "__main__":
    main()
