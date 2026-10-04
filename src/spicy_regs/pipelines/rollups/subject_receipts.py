"""Scheduled subject writers with exact, selected-generation processing priors.

Only private working files restore processing fields. The returned outputs are
native subjects and one generation-bound receipt member; no public mirrors.
"""

from __future__ import annotations

import inspect
import json
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.congress_subjects import INPUT_COLUMNS as CONGRESS
from spicy_regs.congress_receipts import (
    ACQUISITION_POLICY,
    CongressInput,
    _build_events,
    policy as congress_policy,
    restore_processing_input,
    write_congress_dataset,
)
from spicy_regs.etl_receipts import combine_receipts, select_receipts, validate_receipt_bundle
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import (
    FILE_POLICY,
    FILE_STATES,
    migrate_outputs,
    policy as legislative_policy,
    restore_prior,
)
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.regulations_shape import LEGACY_COLUMNS as REGULATIONS
from spicy_regs.transforms.fec_identity_receipts import dataset_policy as fec_policy, read_identity_rows
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY as FEC
from spicy_regs.transforms.regulations_receipts import (
    ReceiptInput,
    materialize_internal,
    policy as regulations_policy,
    write_held_dataset,
)


def dataset_policy(name):
    if name in CONGRESS:
        return congress_policy(name)
    if name in field_registry():
        return legislative_policy(name)
    if name in REGULATIONS:
        return regulations_policy(name)
    if name in FEC:
        return fec_policy(name)
    raise ValueError(f"No coordinated processing reader for {name}")


class SelectedPriors:
    """Capture one publication index and fetch only its pinned subject/receipt pairs."""

    def __init__(self, directory, *, index=None, public_url=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.public_url = public_url if public_url is not None else os.getenv("R2_PUBLIC_URL")
        self.index = (
            index
            if index is not None
            else (publication.current_index(self.public_url) if self.public_url else publication.empty_index())
        )
        self.restored = {}
        self.selections = {}

    def get(self, dataset):
        if dataset in self.restored:
            return self.restored[dataset]
        selected_policy = dataset_policy(dataset)
        owner = publication.table_owner(self.index, dataset + ".parquet")
        receipt_owners = [
            f for f in self.index["families"].values() if dataset in f.get("etlReceipts", {}).get("datasets", ())
        ]
        if not receipt_owners and owner is None:
            self.restored[dataset] = None
            return None
        if not isinstance(self.public_url, str):
            raise ValueError("Selected remote inputs require their public base URL")
        directory = self.directory / dataset
        directory.mkdir()
        subjects = []
        for member in publication.table_members(self.index, dataset + ".parquet") if owner else ():
            target = directory / "selected" / member.key
            target.parent.mkdir(parents=True, exist_ok=True)
            if not publication.fetch_member(self.public_url, member, target, member.path):
                raise ValueError(f"{dataset}: selected subject unavailable")
            subjects.append(target)
        if not receipt_owners:
            subjects, receipt_path, generation = self._migrate_legacy(dataset, directory, subjects, owner)
        else:
            receipts = publication.receipt_members(self.index, dataset=dataset)
            if len(receipts) != 1 or len(receipt_owners) != 1:
                raise ValueError(f"{dataset}: ambiguous selected receipt generation")
            receipt = receipts[0]
            receipt_path = directory / "etl_receipts.parquet"
            if not publication.fetch_member(self.public_url, receipt, receipt_path, receipt.path):
                raise ValueError(f"{dataset}: selected receipts unavailable")
            generation = receipt_owners[0]["etlReceipts"]["generationId"]
        scoped = select_receipts(receipt_path, directory / "scoped.parquet", dataset=dataset)
        validate_receipt_bundle({dataset: subjects}, [scoped], [selected_policy], generation_id=generation)
        self.selections[dataset] = (subjects, receipt_path, generation)
        output = directory / "processing" / (dataset + ".parquet")
        if dataset in CONGRESS:
            output.parent.mkdir(parents=True, exist_ok=True)
            if len(subjects) > 1:
                raise ValueError("Congress processing input unexpectedly partitioned")
            restore_processing_input(
                subjects[0] if subjects else None, receipt_path, output, dataset=dataset, generation_id=generation
            )
        elif dataset in field_registry():
            files = select_receipts(receipt_path, directory / "file-receipts.parquet", dataset=FILE_STATES)
            bundle = directory / "legislative"
            bundle.mkdir()
            combine_receipts([scoped, files], bundle / "etl_receipts.parquet")
            relative = []
            for i, subject in enumerate(subjects):
                target = bundle / f"subject-{i}.parquet"
                shutil.copyfile(subject, target)
                relative.append(target.name)
            manifest = dict(
                generation_id=generation,
                datasets=[dataset],
                subjects={dataset: relative},
                receipt_file="etl_receipts.parquet",
                unowned_outputs=[],
                refused_rows={},
            )
            (bundle / "legislative-bundle.json").write_text(json.dumps(manifest))
            output = restore_prior(bundle, directory / "processing")[dataset]
        elif dataset == "fec_committees":
            if subjects and subjects[0].parent != receipt_path.parent:
                shutil.copyfile(subjects[0], receipt_path.parent / (dataset + ".parquet"))
            from spicy_regs.transforms.build_fec_committees import COLUMNS, _SCHEMA
            from spicy_regs.transforms.parquet_rows import write_rows

            def original_rows():
                for row in read_identity_rows(receipt_path.parent, dataset, generation_id=generation):
                    original = row["conversion_inputs"]
                    yield {name: original.get(name, row.get(name)) for name in COLUMNS}

            output.parent.mkdir(parents=True, exist_ok=True)
            write_rows(original_rows(), output, _SCHEMA)
        elif dataset in FEC:
            raise ValueError(f"No processing reconstruction declared for {dataset}")
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            materialize_internal(ReceiptInput(dataset, tuple(subjects), receipt_path, generation), output)
        self.restored[dataset] = output
        return output

    def _migrate_legacy(self, dataset, directory, sources, owner):
        """Qualify pinned old-generation bytes once, then use the ordinary receipt reader."""
        from hashlib import sha256
        from uuid import uuid4

        generation = "prior-migration-" + uuid4().hex
        witnesses = [
            {
                "source_id": dataset + ":selected-generation",
                "source_uri": str(path.resolve()),
                "sha256": "sha256:" + sha256(path.read_bytes()).hexdigest(),
                "locator": None,
                "body_version": owner[1].get("artifactDigest"),
            }
            for path in sources
        ]
        destination = directory / "migrated"
        if dataset in field_registry():
            source = (
                directory / "selected" / dataset
                if len(sources) > 1 or sources[0].parent.name.startswith("congress=")
                else sources[0]
            )
            manifest = migrate_outputs([source], destination, generation_id=generation, source_witnesses=witnesses)
            if manifest["unowned_outputs"] or any(manifest["refused_rows"].values()):
                raise ValueError(f"{dataset}: selected legacy input conversion refused")
            subjects = [destination / member for member in manifest["subjects"].get(dataset, ())]
            receipt = destination / "etl_receipts.parquet"
        elif dataset in CONGRESS:
            if len(sources) != 1:
                raise ValueError("Congress legacy input unexpectedly partitioned")
            subject, receipt = write_congress_dataset(
                sources[0], destination, dataset=dataset, generation_id=generation, witnesses=witnesses
            )
            subjects = [] if subject is None else [subject]
        elif dataset in REGULATIONS:
            if len(sources) != 1:
                raise ValueError("Regulatory legacy input unexpectedly partitioned")
            subject, receipt = write_held_dataset(
                dataset, sources[0], destination, generation_id=generation, witnesses=witnesses
            )
            subjects = [subject]
        else:
            from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter

            with IdentityReceiptWriter(destination, generation_id=generation, tables=[dataset]) as writer:
                for path, witness in zip(sources, witnesses, strict=True):
                    for batch in pq.ParquetFile(path).iter_batches():
                        for row in batch.to_pylist():
                            writer.emit(dataset, row, input_witness=witness)
            subjects = [] if fec_policy(dataset).receipt_only else [destination / (dataset + ".parquet")]
            receipt = destination / "etl_receipts.parquet"
        for batch in pq.ParquetFile(receipt).iter_batches(columns=["outcome"]):
            if any(value in {"refused", "error"} for value in batch.column(0).to_pylist()):
                raise ValueError(f"{dataset}: selected legacy input conversion refused")
        return subjects, receipt, generation

    def download(self, key, target):
        source = self.get(key.removesuffix(".parquet"))
        if source is None:
            return False
        if source.is_dir():
            raise ValueError("Partitioned prior requested as one file")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return True

    def download_members(self, key, target):
        source = self.get(key.removesuffix(".parquet"))
        if source is None:
            return ()
        members = sorted(source.rglob("*.parquet")) if source.is_dir() else [source]
        result = []
        for member in members:
            relative = member.relative_to(source) if source.is_dir() else Path(member.name)
            destination = target / source.name / relative if source.is_dir() else target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(member, destination)
            result.append(destination)
        return tuple(result)


class SubjectReceiptRollup(RollupPipeline):
    """Congress, legislative and regulatory producers sharing a complete family."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        keys = cls.outputs or (cls.output,)
        cls.source_outputs = keys
        if any(not isinstance(key, str) for key in keys):
            raise ValueError("Receipt rollup requires explicit output names")
        names = tuple(str(key).removesuffix(".parquet") for key in keys)
        policies = [dataset_policy(name) for name in names]
        if any(name in field_registry() for name in names):
            policies.append(FILE_POLICY)
        if any(name in CONGRESS for name in names):
            policies.append(ACQUISITION_POLICY)
        cls.receipt_policies = tuple(policies)
        cls.receipt_only_tables = tuple(
            p.dataset + ".parquet" for p in policies if p.receipt_only and p.dataset in names
        )
        visible = tuple(key for key in keys if key not in cls.receipt_only_tables)
        if cls.outputs:
            cls.outputs = visible
        elif visible:
            cls.output = visible[0]

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}

    def build_receipts(self, output_dir, builder, **kwargs):
        from uuid import uuid4

        private = output_dir / ".builds" / uuid4().hex
        private.mkdir(parents=True)
        prior = SelectedPriors(private / "selected-priors")
        work = private / "source-output"
        work.mkdir()
        for key in self.inputs:
            if not prior.download(key, work / key):
                raise ValueError(f"Required selected processing input unavailable: {key}")
        parameters = inspect.signature(builder).parameters
        if "download_prior" in parameters:
            kwargs["download_prior"] = prior.download
        if "download_members" in parameters:
            kwargs["download_members"] = prior.download_members
        try:
            built = builder(work, **kwargs)
        except Exception as error:
            if any(p.dataset == ACQUISITION_POLICY.dataset for p in self.receipt_policies):
                _build_events(output_dir, self.receipt_generation_id, builder.__name__, self.source_evidence, error)
            raise
        paths = built if isinstance(built, tuple) else (built,)
        observed = {path.name if path.is_dir() else path.stem for path in paths}
        if observed != {key.removesuffix(".parquet") for key in self.source_outputs}:
            raise ValueError("Producer outputs differ from the complete classified family")
        receipts, subjects, legislative = [], [], []
        for path in paths:
            dataset = path.name if path.is_dir() else path.stem
            if dataset in field_registry():
                legislative.append(path)
                continue
            destination = private / "datasets" / dataset
            if dataset in CONGRESS:
                selection = prior.selections.get(dataset)
                held = (
                    None
                    if selection is None
                    else CongressInput(selection[0][0] if selection[0] else None, selection[1], selection[2])
                )
                subject, receipt = write_congress_dataset(
                    path, destination, dataset=dataset, generation_id=self.receipt_generation_id, prior=held
                )
            else:
                subject, receipt = write_held_dataset(
                    dataset, path, destination, generation_id=self.receipt_generation_id
                )
            receipts.append(receipt)
            if subject is not None:
                target = output_dir / subject.name
                shutil.copyfile(subject, target)
                subjects.append(target)
        if legislative:
            bundle = private / "legislative-bundle"
            manifest = migrate_outputs(legislative, bundle, generation_id=self.receipt_generation_id)
            if manifest["unowned_outputs"] or any(manifest["refused_rows"].values()):
                raise ValueError("Legislative output conversion refused")
            receipts.append(bundle / "etl_receipts.parquet")
            for name, members in manifest["subjects"].items():
                if legislative_policy(name).receipt_only:
                    continue
                if (bundle / name).is_dir():
                    target = output_dir / name
                    if target.exists():
                        previous = private / "previous" / name
                        previous.parent.mkdir(parents=True, exist_ok=True)
                        target.rename(previous)
                    shutil.copytree(bundle / name, target)
                else:
                    target = output_dir / (name + ".parquet")
                    shutil.copyfile(bundle / members[0], target)
                subjects.append(target)
        if any(p.dataset == ACQUISITION_POLICY.dataset for p in self.receipt_policies):
            receipts.append(
                _build_events(output_dir, self.receipt_generation_id, builder.__name__, self.source_evidence)
            )
        combined = combine_receipts(receipts, output_dir / "etl_receipts.parquet")
        for batch in pq.ParquetFile(combined).iter_batches(columns=["outcome"]):
            if any(value in {"refused", "error"} for value in batch.column(0).to_pylist()):
                raise ValueError("Subject conversion refused; retained receipts require review")
        by_dataset = {p.dataset: [] for p in self.receipt_policies}
        for path in subjects:
            by_dataset[path.name if path.is_dir() else path.stem] = (
                sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
            )
        validate_receipt_bundle(by_dataset, [combined], self.receipt_policies, generation_id=self.receipt_generation_id)
        return tuple(subjects) if self.outputs else subjects[0]
