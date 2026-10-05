"""Build native Congress subjects with generation-bound receipts and exact resume reads.

The source builders retain their acquisition, safety, merge and interpretation
rules. This entry point supplies explicit prior selections, reconstructs private
working inputs from matching receipts, and splits every owned output before
admission. Every incremental input must contain selected native subjects and
matching receipts. There is no legacy import path.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import congress_bulk, etl_bulk
from spicy_regs.congress_subjects import IDENTITIES, INPUT_COLUMNS, RECEIPT_ONLY, map_record, subject_schema
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ReceiptContext,
    combine_receipts,
    carry_receipt_history,
    failure_receipt,
    observation_receipt,
    read_attempts,
    read_with_receipts,
    select_receipts,
    split_record,
    validate_receipt_bundle,
    _unpack,
)
from spicy_regs.transforms.parquet_rows import write_rows

PROCESSOR = "congress-subjects/1"
RECEIPT_FIELDS = ("source_fields", "source_metadata", "source_schema", "entry_kind")
ACQUISITION_POLICY = DatasetPolicy(
    "congress_acquisition",
    pa.schema([]),
    (),
    ("capture_event", "build_event"),
    policy_version=PROCESSOR,
    receipt_only=True,
)


def policy(dataset: str) -> DatasetPolicy:
    return DatasetPolicy(
        dataset,
        subject_schema(dataset),
        IDENTITIES.get(dataset, ()),
        RECEIPT_FIELDS,
        policy_version="congress-subjects/1",
        receipt_only=dataset in RECEIPT_ONLY,
    )


def _admit(subjects, receipts, policies, *, generation_id, bulk=True):
    """Use bulk proof where eligible; the exact row validator decides every fallback."""
    if bulk:
        try:
            return etl_bulk.validate_bundle(subjects, receipts, policies, generation_id=generation_id)
        except etl_bulk.NotBulkEligible:
            pass
    return validate_receipt_bundle(subjects, receipts, policies, generation_id=generation_id, bulk=False)


def _rows(path: Path):
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _digest(path: Path) -> str:
    with path.open("rb") as source:
        return "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()


def _build_events(directory: Path, generation_id: str, builder: str, evidence, error=None):
    """Copy the already-scrubbed source journal into shared receipt observations.

    This is a receipt dataset label, never another subject table. Refusals,
    captured inputs, parser versions and incomplete responses keep their exact
    journal objects. A top-level failure still raises after this local record.
    """
    event = {
        "builder": builder,
        "outcome": "error" if error else "built",
        "error_type": type(error).__name__ if error else None,
    }
    invocation = directory / "build-event.json"
    invocation.write_text(json.dumps(event, sort_keys=True) + "\n")
    journal = directory / "source-journal.jsonl"
    if evidence is not None:
        shutil.copyfile(evidence.artifact_dir / "journal.jsonl", journal)

    def records():
        files = [(invocation, "build_event")]
        if journal.exists():
            files.append((journal, "capture_event"))
        for path, field_name in files:
            digest = _digest(path)
            with path.open() as stream:
                for line, raw in enumerate(stream):
                    context = ReceiptContext(
                        generation_id,
                        f"{field_name}:{line}",
                        PROCESSOR,
                        [
                            {
                                "source_id": field_name,
                                "source_uri": str(path.resolve()),
                                "sha256": digest,
                                "locator": f"line:{line + 1}",
                                "body_version": None,
                            }
                        ],
                    )
                    fields = {field_name: json.loads(raw)}
                    if field_name == "build_event" and error is not None:
                        yield failure_receipt(ACQUISITION_POLICY, context, outcome="error", raw_fields=fields)
                    else:
                        yield observation_receipt(ACQUISITION_POLICY, context, processing_fields=fields)

    return write_rows(records(), directory / "acquisition-receipts.parquet", RECEIPT_SCHEMA)


def congress_attempt(dataset: str, raw: Mapping, context: ReceiptContext) -> tuple[dict | None, dict]:
    """The reference attempt for one raw Congress row, shared with qualification."""
    selected = policy(dataset)
    try:
        mapped = map_record(dataset, raw)
        processing = {"source_fields": mapped.source_fields, "entry_kind": "row"}
        if mapped.subject is None and not selected.receipt_only:
            return None, failure_receipt(selected, replace(context, diagnostics={"reason": "no_domain_subject"}),
                                         outcome="rejected", raw_fields=processing)
        return split_record(selected, (mapped.subject or {}) | processing, context)
    except (ValueError, TypeError, OverflowError, pa.ArrowException) as error:
        return None, failure_receipt(selected, replace(context, diagnostics={"reason": "conversion_refused",
                                     "error_type": type(error).__name__}), outcome="refused",
                                     raw_fields={"source_fields": raw, "entry_kind": "row"})


def write_congress_dataset(
    source: Path,
    directory: Path,
    *,
    dataset: str,
    generation_id: str,
    witnesses: Sequence[Mapping[str, Any]] = (),
    prior: CongressInput | None = None,
    bulk: bool = True,
) -> tuple[Path | None, Path]:
    """Retain one complete shaped output, including rejected rows and its exact footer.

    The source file remains in place. Its digest and row ordinal identify the
    conversion input; callers can additionally retain ordered native capture
    witnesses. The first witness never asserts that a shaped file is an original
    publisher response.

    A bulk-eligible dataset is written in one scan. Both paths create fresh
    attempts, then carry exact unchanged receipt occurrences from the selected
    prior. Changed accepted records name only their direct predecessor.
    ``bulk=False`` keeps the row writer available as the reference.
    """
    from rulespec_artifacts import publish_directory_no_replace

    if directory.exists():
        raise FileExistsError(directory)
    selected = policy(dataset)
    digest = _digest(source)
    directory.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".congress-", dir=directory.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        subjects = None if selected.receipt_only else stage / (dataset + ".parquet")
        receipts = stage / "etl_receipts.parquet"
        base_witness = {
            "source_id": "shaped-observation:" + dataset,
            "source_uri": str(source.resolve()),
            "sha256": digest,
            "body_version": None,
        }
        footer = dict(pq.read_schema(source).metadata or {})
        in_bulk = bulk and subjects is not None and congress_bulk.eligible(dataset, pq.read_schema(source))
        prior_paths = []
        if prior is not None:
            scoped = select_receipts(prior.receipts, Path(temporary) / "prior.parquet", dataset=dataset)
            _admit({dataset: prior.subjects}, [scoped], [selected], generation_id=prior.generation_id, bulk=bulk)
            prior_paths.append(scoped)

        def context(ordinal, diagnostics=None, subject=None, processing=None):
            current = ReceiptContext(
                generation_id,
                f"{dataset}:{digest}:{ordinal}",
                PROCESSOR,
                [{**base_witness, "locator": str(ordinal)}, *witnesses],
                diagnostics or {},
            )
            return current

        def attempt(ordinal, raw):
            return congress_attempt(dataset, raw, context(ordinal))

        metadata = {
            "source_metadata": list(footer.items()),
            "source_schema": pq.read_schema(source).serialize().to_pybytes(),
            "entry_kind": "table_metadata",
        }
        first = observation_receipt(selected, context("metadata", processing=metadata), processing_fields=metadata)
        # One scan for a flat dataset; the row loop below is its reference.
        if in_bulk:
            assert subjects is not None
            try:
                congress_bulk.write_bundle(
                    source, subjects, receipts, dataset=dataset, policy=selected, generation_id=generation_id,
                    processor=PROCESSOR, digest=digest, first_receipts=[first], witnesses=witnesses,
                    row_receipt=lambda text, ordinal: attempt(ordinal, _unpack(json.loads(text))["source_fields"]),
                )
            except (etl_bulk.duckdb.Error, pa.ArrowInvalid):
                # The same staged row reader establishes the exact first source
                # decoding error. Count/order guard failures still abort directly.
                in_bulk = False
        if not in_bulk:
            with pq.ParquetWriter(receipts, RECEIPT_SCHEMA, compression="zstd") as rw:
                sw = None if subjects is None else pq.ParquetWriter(subjects, selected.subject_schema, compression="zstd")
                try:
                    receipt_batch, subject_batch = [first], []
                    for ordinal, raw in enumerate(_rows(source)):
                        subject, receipt = attempt(ordinal, raw)
                        if subject is not None:
                            subject_batch.append(subject)
                        receipt_batch.append(receipt)
                        if len(receipt_batch) >= 2000:
                            if subject_batch:
                                assert sw is not None
                                sw.write_table(pa.Table.from_pylist(subject_batch, schema=selected.subject_schema))
                                subject_batch.clear()
                            rw.write_table(pa.Table.from_pylist(receipt_batch, schema=RECEIPT_SCHEMA))
                            receipt_batch.clear()
                    if subject_batch:
                        assert sw is not None
                        sw.write_table(pa.Table.from_pylist(subject_batch, schema=selected.subject_schema))
                    if receipt_batch:
                        rw.write_table(pa.Table.from_pylist(receipt_batch, schema=RECEIPT_SCHEMA))
                finally:
                    if sw is not None:
                        sw.close()
        if prior_paths:
            carried = carry_receipt_history(receipts, prior_paths, stage / "carried-receipts.parquet")
            carried.replace(receipts)
        _admit(
            {dataset: [] if subjects is None else [subjects]}, [receipts], [selected], generation_id=generation_id, bulk=bulk
        )
        publish_directory_no_replace(stage, directory)
    return (None if subjects is None else directory / subjects.name, directory / receipts.name)


def restore_processing_input(
    subject: Path | tuple[Path, ...] | None,
    receipts: Path,
    destination: Path,
    *,
    dataset: str,
    generation_id: str,
    bulk: bool = True,
) -> Path:
    """Restore retained original source fields and schema after exact receipt admission.

    Rejected conversion attempts remain available for retry. Technical tables
    and processing events remain receipt-only in the public generation. Footer
    metadata includes bill-family completion scopes and per-reader refusal state.

    A dataset ``congress_bulk.eligible`` accepts is decoded in one scan after the
    one admission. ``bulk=False`` is the row readers for any dataset: the
    reference a bulk restore is checked against.
    """
    selected = policy(dataset)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".congress-read-", dir=destination.parent) as temp:
        scoped = select_receipts(receipts, Path(temp) / "receipts.parquet", dataset=dataset)
        paths = () if subject is None else ((subject,) if isinstance(subject, Path) else tuple(subject))
        _admit({dataset: paths}, [scoped], [selected], generation_id=generation_id, bulk=bulk)

        def source_fields(data, diagnostic):
            kept = data.get("entry_kind") == "row" and diagnostic.get("reason") != "conversion_refused"
            return data["source_fields"] if kept else None

        def retained_schema(metadata_rows):
            if not metadata_rows or any(row != metadata_rows[0] for row in metadata_rows[1:]):
                raise ValueError("Congress input requires consistent retained source footers")
            return pa.ipc.read_schema(pa.BufferReader(metadata_rows[0]["source_schema"]))

        if bulk and congress_bulk.eligible(dataset):
            # The bundle is admitted above. One scan finds its footers and one decodes its rows; each reader
            # below admits the whole bundle again before it yields anything.
            footers = (_unpack(json.loads(text)) for text in congress_bulk.footer_candidates(scoped, dataset=dataset))
            schema = retained_schema([data for data in footers if data.get("entry_kind") == "table_metadata"])
            if congress_bulk.eligible(dataset, schema):
                congress_bulk.restore_input(
                    scoped, destination, dataset=dataset, schema=schema,
                    row_fields=lambda processing, diagnostic: source_fields(
                        _unpack(json.loads(processing)), _unpack(json.loads(diagnostic))
                    ),
                )
                return destination
        # Exercise the shared exact matching reader before reconstructing original source rows.
        for _ in read_with_receipts(paths, [scoped], selected, generation_id=generation_id):
            pass
        schema = retained_schema([
            receipt["processing_fields"]
            for receipt in read_attempts([scoped], selected, generation_id=generation_id)
            if receipt["processing_fields"].get("entry_kind") == "table_metadata"
        ])

        # Source shapers use strings. Preserve discovered source columns and
        # absent columns by reading the exact retained input dictionaries.
        def original_rows():
            for receipt in read_attempts([scoped], selected, generation_id=generation_id):
                fields = source_fields(receipt["processing_fields"], receipt["diagnostics"])
                if fields is not None:
                    yield fields

        return write_rows(original_rows(), destination, schema)


@dataclass(frozen=True)
class CongressInput:
    """One admitted native table, including every selected partition member."""

    source: Path | tuple[Path, ...] | None
    receipts: Path
    generation_id: str

    @property
    def subjects(self) -> tuple[Path, ...]:
        return () if self.source is None else ((self.source,) if isinstance(self.source, Path) else tuple(self.source))

    def materialize(self, dataset: str, destination: Path) -> Path:
        if not self.receipts or not self.generation_id:
            raise ValueError("Native Congress input requires selected generation receipts")
        return restore_processing_input(
            self.subjects, self.receipts, destination, dataset=dataset, generation_id=self.generation_id
        )


@dataclass
class CongressBuild:
    """A local build selection passed to an existing Congress producer.

    Empty inputs explicitly mean cold start. No unselected remote prior is read.
    Cross-family inputs must be supplied by their owner in an explicitly admitted
    private working shape until the integration reader registry is available.
    """

    generation_id: str
    inputs: Mapping[str, CongressInput] = field(default_factory=dict)
    witnesses: Sequence[Mapping[str, Any]] = ()
    receipt_path: Path | None = field(default=None, init=False)
    policies: tuple[DatasetPolicy, ...] = field(default=(), init=False)
    unclassified_outputs: tuple[Path, ...] = field(default=(), init=False)
    source_evidence: Any = field(default=None, init=False)
    completed: bool = field(default=False, init=False)

    def run(self, builder: Callable, output_dir: Path, **kwargs):
        import inspect

        self.completed = False
        if (
            not self.generation_id
            or self.generation_id in {".", ".."}
            or Path(self.generation_id).name != self.generation_id
        ):
            raise ValueError("A local build needs a plain generation identity")
        directory = output_dir / self.generation_id
        directory.mkdir(parents=True, exist_ok=False)
        work = directory / "processing"
        work.mkdir()
        for name, selected in self.inputs.items():
            selected.materialize(name, work / (name + ".parquet"))

        def download(key: str, destination: Path) -> bool:
            name = Path(key).stem
            selected = self.inputs.get(name)
            if selected is None:
                return False
            selected.materialize(name, destination)
            return True

        parameters = inspect.signature(builder).parameters
        options = dict(kwargs)
        if "download_prior" in parameters:
            options["download_prior"] = download
        if "download_members" in parameters:

            def download_members(key, destination):
                name = Path(key).stem
                selected = self.inputs.get(name)
                if selected is None:
                    return ()
                destination.mkdir(parents=True, exist_ok=True)
                return (selected.materialize(name, destination / key),)

            options["download_members"] = download_members
        self.source_evidence = options.get("evidence")
        try:
            built = builder(work, **options)
        except Exception as error:
            events = _build_events(
                directory,
                self.generation_id,
                getattr(builder, "__name__", type(builder).__name__),
                self.source_evidence,
                error,
            )
            self.receipt_path = combine_receipts([events], directory / "etl_receipts.parquet")
            self.policies = (ACQUISITION_POLICY,)
            raise
        paths = built if isinstance(built, tuple) else (built,)
        subjects, receipts, policies, untouched = [], [], [], []
        for source in paths:
            dataset = source.stem
            if dataset not in INPUT_COLUMNS:
                untouched.append(source)
                continue
            subject, receipt = write_congress_dataset(
                source,
                directory / "datasets" / dataset,
                dataset=dataset,
                generation_id=self.generation_id,
                witnesses=self.witnesses,
                prior=self.inputs.get(dataset),
            )
            if subject is not None:
                subjects.append(subject)
            receipts.append(receipt)
            policies.append(policy(dataset))
        if not receipts:
            raise ValueError("Builder emitted no owned Congress datasets")
        receipts.append(
            _build_events(
                directory,
                self.generation_id,
                getattr(builder, "__name__", type(builder).__name__),
                self.source_evidence,
            )
        )
        policies.append(ACQUISITION_POLICY)
        self.receipt_path = combine_receipts(receipts, directory / "etl_receipts.parquet")
        self.policies = tuple(policies)
        self.unclassified_outputs = tuple(untouched)
        validate_receipt_bundle(
            {p.dataset: [s for s in subjects if s.stem == p.dataset] for p in policies},
            [self.receipt_path],
            policies,
            generation_id=self.generation_id,
        )
        # Other family outputs remain exact; complete-family admission below
        # refuses them until their owner supplies policies and receipts.
        result = tuple(subjects) + tuple(untouched)
        self.completed = True
        return result if isinstance(built, tuple) else result[0]

    def admit(self, directory: Path, *, family: str, subjects: Sequence[Path]):
        from spicy_regs.generations import build_generation

        if self.unclassified_outputs:
            raise ValueError("Complete family awaits policies and receipts for sibling outputs")
        if not self.completed or self.receipt_path is None:
            raise ValueError("No completed Congress build to admit")
        if any(r["outcome"] == "refused" for r in _rows(self.receipt_path)):
            raise ValueError("Conversion refusals require review before complete generation admission")
        return build_generation(
            directory,
            family=family,
            files=subjects,
            expected_keys=[path.name for path in subjects],
            receipt_path=self.receipt_path,
            receipt_policies=self.policies,
            receipt_generation_id=self.generation_id,
            inputs=self.source_evidence.inputs() if self.source_evidence is not None else (),
        )
