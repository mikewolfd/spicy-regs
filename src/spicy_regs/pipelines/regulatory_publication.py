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
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication, r2
from spicy_regs.selected_generations import SelectedInputs, SelectedDataset, remember_selection
from spicy_regs.transforms.regulations_checkpoints import checkpoint_policy, read_checkpoint, write_checkpoint
from spicy_regs.transforms.regulations_receipts import ReceiptInput, build_local_generation, materialize_internal


def restore_dataset(root: Path, dataset: str, destination: Path, *, index=None, inputs=None) -> bool:
    """Restore only an explicitly selected native generation, never a loose file."""
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=root, prefix='.selected-') as temporary:
        inputs = inputs or SelectedInputs(root, temporary, index=index, public_url=None if index is not None else '')
        selected = inputs.select(dataset)
        if selected is None:
            return False
        materialize_internal(ReceiptInput(dataset, selected.subjects, selected.receipts, selected.generation_id),
                             destination)
        return True


def _publish(directory, *, inputs):
    if inputs is None or inputs.index is None:
        raise ValueError('Publication requires the captured native input selection used by this build')
    r2.require_credentials('Regulatory generation publication')
    return publication.publish_generation(
        directory, client=r2.get_r2_client(), bucket=os.getenv('R2_BUCKET_NAME', 'spicy-regs'),
        prior_index=inputs.index)


def _remember(root, datasets, directory, generation, *, inputs=None, published=None):
    selections = []
    for dataset in datasets:
        subject = directory / (dataset + '.parquet')
        selections.append(SelectedDataset(dataset, (subject,) if subject.exists() else (),
                                           directory / 'etl_receipts.parquet', generation))
    remember_selection(root, selections)
    if inputs is not None:
        inputs.cache.update({selected.dataset: selected for selected in selections})
        if published is not None:
            from copy import deepcopy
            inputs.index = deepcopy(inputs.index)
            for dataset in datasets:
                # Advance only families this write committed, preserving the
                # prior selections actually read for all other datasets.
                owner = next((name for name, entry in published['families'].items()
                              if dataset in entry.get('etlReceipts', {}).get('datasets', ())), None)
                if owner is None:
                    raise ValueError('Published dataset is absent from its receipt family')
                inputs.index['families'][owner] = published['families'][owner]


def finish_dataset(root: Path, dataset: str, source: Path, *, publish: bool, inputs=None):
    if publish and (inputs is None or inputs.index is None):
        raise ValueError('Publication requires the captured native input selection used by this build')
    index = inputs.index if inputs is not None and inputs.index is not None else publication.empty_index()
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
        with source.open('rb') as actual, (pair / (dataset + '.parquet')).open('rb') as paired:
            if hashlib.file_digest(actual, 'sha256').digest() != hashlib.file_digest(paired, 'sha256').digest():
                raise ValueError('Catalog export source differs from its paired native subject')
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
        selected_inputs = inputs or SelectedInputs(root, root / '.selected-write' / uuid4().hex, public_url='')
        prior = selected_inputs.select(dataset)
        if prior is not None:
            from spicy_regs.transforms.regulations_receipts import read_internal
            for _ in read_internal(ReceiptInput(dataset, prior.subjects, prior.receipts, prior.generation_id)):
                pass
        artifact = build_local_generation(
            {dataset: source}, directory, generation_id=uuid4().hex, family=family, publication_status="complete-family",
            prior_receipts={dataset: [prior.receipts]} if prior is not None else {}
        )
    if not (pair / 'generation.json').exists():
        # A current source conversion must be complete. Historical attempts in
        # an already qualified catalog pair remain evidence, not a veto.
        for batch in pq.ParquetFile(directory / 'etl_receipts.parquet').iter_batches(columns=['outcome']):
            if any(value in {'refused', 'error'} for value in batch.column(0).to_pylist()):
                raise ValueError(f'{dataset}: native conversion refused; checkpoint remains unadvanced')
    published = _publish(directory, inputs=inputs) if publish else None
    _remember(root, [dataset], directory, artifact.root['spec']['etlReceipts']['generationId'],
              inputs=inputs, published=published)
    shutil.copyfile(directory / (dataset + ".parquet"), root / (dataset + ".parquet"))
    return directory / (dataset + ".parquet")


def restore_checkpoint(root: Path, dataset: str, *, index=None, inputs=None) -> list[dict]:
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=root, prefix='.checkpoint-') as temporary:
        inputs = inputs or SelectedInputs(root, temporary, index=index, public_url=None if index is not None else '')
        selected = inputs.select(dataset)
        if selected is None:
            return []
        if selected.subjects:
            raise ValueError('Checkpoint selection contains subject files')
        return read_checkpoint(dataset, selected.receipts, generation_id=selected.generation_id)


def finish_checkpoints(root: Path, checkpoints: dict[str, list[dict]], *, publish: bool, witnesses=(), inputs=None):
    """Commit complete retry sets together, including explicit empty retirement."""
    if publish and (inputs is None or inputs.index is None):
        raise ValueError('Publication requires the captured native input selection used by this build')
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
        build_generation(
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
    published = _publish(directory, inputs=inputs) if publish else None
    _remember(root, list(checkpoints), directory, generation, inputs=inputs, published=published)
    return directory
