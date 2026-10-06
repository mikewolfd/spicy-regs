"""Write legislative subjects and shared receipts; restore exact inputs for ETL.

The original qualified builders keep their acquisition, replacement, financial,
and retry checks. ``build_with_receipts`` supplies verified prior observations
to those checks and migrates their actual outputs before generation admission.
"""

from __future__ import annotations

import hashlib
import inspect
import itertools
import json
import shutil
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    ReceiptLineage,
    combine_receipts,
    failure_receipt,
    read_attempts,
    read_with_receipts,
    select_receipts,
    split_record,
    validate_receipt_bundle,
    write_dataset,
)
from spicy_regs.legislative_documents import (
    LegislativeShapeError,
    field_registry,
    map_subject,
    recorded_subject,
    subject_schema,
)
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.navigation_read_outcomes import POLICIES as OUTCOME_POLICIES, READ_TABLES, checkpoint_rows, file_rows


RAW = "raw_source_row"
FILE_STATES = "legislative_document_file_states"
FILE_POLICY = DatasetPolicy(FILE_STATES, pa.schema([]), (), ("file_state",), receipt_only=True)


def policy(dataset: str) -> DatasetPolicy:
    spec = field_registry()[dataset]
    return DatasetPolicy(
        dataset,
        subject_schema(dataset),
        () if spec["processing_only"] else tuple(spec["identity_fields"]),
        (RAW,),
        policy_version="legislative-documents/2" if dataset in {"native_legal_references", "native_legal_reference_reads"} else "legislative-documents/1",
        receipt_only=spec["processing_only"],
    )


def input_policy(dataset: str, subjects: Sequence[Path]) -> DatasetPolicy:
    """Read only an exact current or declared earlier subject shape.

    An earlier native read scope had no subject files. Its unchanged observed
    receipts remain readable, then the next writer creates a main scope row.
    """
    from spicy_regs.etl_receipts import selected_subject_policy

    current = policy(dataset)
    return selected_subject_policy(current, subjects)


def _processor(dataset: str) -> str:
    if dataset in {"native_legal_references", "native_legal_reference_reads"}:
        return "legislative-documents/3"
    # The remaining field policies stay /1: receipt ownership is unchanged.
    # Version the changed court-key mapping separately in each new attempt.
    return "legislative-documents/2" if dataset == "document_citations" else "legislative-documents/1"


def mapped_record(dataset: str, row: Mapping[str, Any]) -> dict:
    return (map_subject(dataset, row) or {}) | {RAW: dict(row)}


def split_source_row(dataset: str, row: Mapping[str, Any], context: ReceiptContext) -> tuple[dict | None, dict]:
    """A refused native conversion retains the entire exact input and its witness."""
    try:
        mapped = mapped_record(dataset, row)
    except LegislativeShapeError as error:
        from dataclasses import replace

        failed_context = replace(context, diagnostics=dict(context.diagnostics) | {"conversion_error": str(error)})
        return None, failure_receipt(policy(dataset), failed_context, outcome="refused", raw_fields={RAW: dict(row)})
    return split_record(policy(dataset), mapped, context)


def _rows(path: Path) -> Iterable[dict]:
    # Do not infer Hive partition values: the stored source spelling is evidence.
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _members(path: Path) -> list[Path]:
    return sorted(path.rglob("*.parquet")) if path.is_dir() else [path]


def write_legislative_outputs(
    outputs: Sequence[Path],
    destination: Path,
    *,
    generation_id: str,
    source_witnesses: Sequence[Mapping] = (),
    prior_receipts: Sequence[Path] = (),
    file_outcomes_table: str | None = None,
) -> dict:
    """Create a new local bundle from explicit producer outputs, preserving partitions.

    Non-owned outputs are returned as dependency paths, never reclassified.
    Source files remain untouched and must remain retained with the build.
    """
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "generation_id": generation_id,
        "subjects": {},
        "datasets": [],
        "source_outputs": {},
        "unowned_outputs": [],
        "refused_rows": {},
        "receipt_file": "etl_receipts.parquet",
        "outcome_subjects": {},
    }
    with TemporaryDirectory(prefix=".legislative-", dir=destination.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        receipt_shards, states = [], []
        for output in outputs:
            output = Path(output)
            dataset = output.name if output.is_dir() else output.stem
            if dataset not in field_registry():
                manifest["unowned_outputs"].append(str(output.resolve()))
                continue
            if dataset in manifest["datasets"]:
                raise ValueError(f"Repeated dataset output: {dataset}")
            manifest["datasets"].append(dataset)
            manifest["subjects"][dataset] = []
            manifest["source_outputs"][dataset] = str(output.resolve())
            manifest["refused_rows"][dataset] = 0
            members = _members(output)
            if output.is_dir() and not members:
                # An empty partitioned table has no physical member. Its receipt
                # records successful emptiness, without inventing a partition key.
                from spicy_regs.etl_receipts import exact_json

                state = {"dataset": dataset, "relative_path": None, "partitioned": True, "rows": 0}
                witness = {
                    "source_id": dataset,
                    "source_uri": None,
                    "sha256": "sha256:" + hashlib.sha256(exact_json(state).encode()).hexdigest(),
                    "locator": "receipt.values.file_state",
                    "body_version": None,
                }
                states.append(
                    (
                        {"file_state": state},
                        ReceiptContext(generation_id, f"{dataset}/empty", _processor(dataset), [witness]),
                    )
                )
                (stage / dataset).mkdir()
            for member_index, member in enumerate(members):
                digest = _sha(member)
                relative = (
                    str(member.relative_to(output))
                    if output.is_dir() and member.is_relative_to(output)
                    else member.name
                )
                retained_member = destination / member.relative_to(stage) if member.is_relative_to(stage) else member
                witness = {
                    "source_id": dataset,
                    "source_uri": retained_member.resolve().as_uri(),
                    "sha256": digest,
                    "locator": None,
                    "body_version": None,
                }
                member_context = ReceiptContext(
                    generation_id,
                    f"{dataset}/member/{member_index}",
                    _processor(dataset),
                    [witness, *source_witnesses],
                )
                with pq.ParquetFile(member) as source:
                    file_state = {
                        "dataset": dataset,
                        "relative_path": relative,
                        "partitioned": output.is_dir(),
                        "schema": source.schema_arrow.serialize().to_pybytes(),
                        "metadata": list((source.metadata.metadata or {}).items()),
                        "sha256": digest,
                        "source_path": str(retained_member.resolve()),
                        "rows": source.metadata.num_rows,
                    }
                states.append(({"file_state": file_state}, member_context))
                failures = []

                def records():
                    for row_index, row in enumerate(_rows(member)):
                        context = ReceiptContext(
                            generation_id,
                            f"{dataset}/{member_index}/{row_index}",
                            _processor(dataset),
                            [dict(witness, locator=f"row:{row_index}"), *source_witnesses],
                        )
                        try:
                            mapped = mapped_record(dataset, row)
                        except LegislativeShapeError as error:
                            from dataclasses import replace

                            failed = replace(context, diagnostics={"conversion_error": str(error)})
                            failures.append(
                                failure_receipt(policy(dataset), failed, outcome="refused", raw_fields={RAW: row})
                            )
                            manifest["refused_rows"][dataset] += 1
                            continue
                        yield (
                            mapped,
                            lineage.inherit(context, policy(dataset), mapped)
                            if not policy(dataset).receipt_only
                            else lineage.inherit_processing(context, mapped),
                        )

                shard_dir = stage / ".shards" / dataset / str(member_index)
                with ReceiptLineage(prior_receipts, dataset=dataset) as lineage:
                    subject, receipts = write_dataset(records(), shard_dir, policy(dataset), failures=failures)
                receipt_shards.append(receipts)
                if subject is not None:
                    final_relative = f"{dataset}/{relative}" if output.is_dir() else f"{dataset}.parquet"
                    target = stage / final_relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(subject, target)
                    manifest["subjects"][dataset].append(final_relative)
            if dataset in READ_TABLES:
                name = READ_TABLES[dataset]
                checkpoint_digest = _sha(output)
                def recorded_checkpoints():
                    for ordinal, (_, row, raw) in enumerate(checkpoint_rows(dataset, output, generation_id=generation_id)):
                        yield row | {"recorded_event": raw}, ReceiptContext(generation_id,
                            f"{dataset}/checkpoint/{ordinal}", "navigation-read-outcomes/1",
                            [{"source_id": dataset, "source_uri": None, "sha256": checkpoint_digest,
                              "locator": f"row:{ordinal}", "body_version": None}])
                subject, receipts = write_dataset(recorded_checkpoints(), stage / ".outcomes" / name,
                                                   OUTCOME_POLICIES[name])
                if subject is None:
                    raise ValueError(f"Main read-outcome policy has no subject: {name}")
                shutil.copyfile(subject, stage / f"{name}.parquet")
                receipt_shards.append(receipts)
                manifest["outcome_subjects"][name] = [f"{name}.parquet"]
        if not manifest["datasets"]:
            raise ValueError("No owned legislative producer outputs selected")
        _, state_receipts = write_dataset(states, stage / ".shards" / FILE_STATES, FILE_POLICY)
        receipt_shards.append(state_receipts)
        if file_outcomes_table is not None:
            selected_policy = OUTCOME_POLICIES[file_outcomes_table]
            mapped = manifest["subjects"] | manifest["outcome_subjects"]
            subject, receipts = write_dataset(
                ((row | {"recorded_event": raw}, context) for row, raw, context in
                 file_rows(states, mapped, stage, generation_id=generation_id)),
                stage / ".outcomes" / file_outcomes_table, selected_policy)
            if subject is None:
                raise ValueError(f"Main file-outcome policy has no subject: {file_outcomes_table}")
            shutil.copyfile(subject, stage / f"{file_outcomes_table}.parquet")
            receipt_shards.append(receipts)
            manifest["outcome_subjects"][file_outcomes_table] = [f"{file_outcomes_table}.parquet"]
        combine_receipts(receipt_shards, stage / manifest["receipt_file"])
        validate_receipt_bundle(
            {name: [stage / f for f in paths] for name, paths in
             (manifest["subjects"] | manifest["outcome_subjects"]).items()} | {FILE_STATES: []},
            [stage / manifest["receipt_file"]],
            [policy(n) for n in manifest["datasets"]] + [FILE_POLICY] +
            [OUTCOME_POLICIES[n] for n in manifest["outcome_subjects"]],
            generation_id=generation_id,
        )
        # Staging shards are private duplicates; the admitted bundle has one shared receipt member.
        shutil.rmtree(stage / ".shards")
        if (stage / ".outcomes").exists():
            shutil.rmtree(stage / ".outcomes")
        (stage / "legislative-bundle.json").write_text(json.dumps(manifest, indent=2) + "\n")
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(stage, destination)
    return manifest


def _processing_rows(receipts: Path, dataset_policy: DatasetPolicy, generation_id: str) -> Iterable[dict]:
    """Read receipt-only source observations through the shared verified API."""
    validate_receipt_bundle({dataset_policy.dataset: []}, [receipts], [dataset_policy], generation_id=generation_id)
    for receipt in read_attempts(
        [receipts], dataset_policy, generation_id=generation_id, outcomes=frozenset({"observed"})
    ):
        yield receipt["processing_fields"]


def restore_prior(bundle: Path, destination: Path) -> dict[str, Path]:
    """Rebuild exact source-owner inputs before any incremental/financial checks.

    Missing, wrong-generation, duplicate or changed receipts refuse. Conversion
    refusals do not qualify a prior: rereading their retained inputs is required.
    """
    bundle = Path(bundle)
    manifest = json.loads((bundle / "legislative-bundle.json").read_text())
    if any(manifest["refused_rows"].values()):
        raise ValueError("A bundle with conversion refusals cannot qualify an incremental prior")
    destination.mkdir(parents=True, exist_ok=False)
    generation = manifest["generation_id"]
    with TemporaryDirectory(prefix=".receipt-select-", dir=destination) as temp:
        selected = Path(temp)
        receipts = bundle / manifest["receipt_file"]
        selected_policies = {n: input_policy(n, [bundle / p for p in manifest["subjects"][n]])
                             for n in manifest["datasets"]}
        validate_receipt_bundle(
            {n: [bundle / p for p in paths] for n, paths in
             (manifest["subjects"] | manifest.get("outcome_subjects", {})).items()} | {FILE_STATES: []},
            [receipts],
            list(selected_policies.values()) + [FILE_POLICY] +
            [OUTCOME_POLICIES[n] for n in manifest.get("outcome_subjects", {})],
            generation_id=generation,
        )
        file_receipts = select_receipts(receipts, selected / "files.parquet", dataset=FILE_STATES)
        file_states = [r["file_state"] for r in _processing_rows(file_receipts, FILE_POLICY, generation)]
        outputs = {}
        for dataset in manifest["datasets"]:
            dataset_policy = selected_policies[dataset]
            dataset_receipts = select_receipts(receipts, selected / f"{dataset}.parquet", dataset=dataset)
            states = [s for s in file_states if s["dataset"] == dataset]
            if not states or len({s["relative_path"] for s in states}) != len(states):
                raise ValueError("Missing or duplicate retained source file state")

            # Receipt inputs retain the exact source spelling, including native
            # null versus absent JSON properties. File-state receipts preserve
            # processing footer metadata even for successful empty tables.
            def source_rows():
                if dataset_policy.receipt_only:
                    seen = set()
                    for record in _processing_rows(dataset_receipts, dataset_policy, generation):
                        raw = record[RAW]
                        map_subject(dataset, raw)
                        identity = tuple(raw[k] for k in field_registry()[dataset]["identity_fields"])
                        if identity in seen:
                            raise ValueError("Ambiguous selected processing scope")
                        seen.add(identity)
                        yield raw
                else:
                    for record in read_with_receipts(
                        [bundle / p for p in manifest["subjects"][dataset]],
                        [dataset_receipts],
                        dataset_policy,
                        generation_id=generation,
                    ):
                        raw = record[RAW]
                        if recorded_subject(dataset, raw, record) != {k: v for k, v in record.items() if k != RAW}:
                            raise ValueError("Retained conversion input disagrees with its native subject")
                        yield raw

            expected = iter(source_rows())
            for state in states:
                if state["relative_path"] is None:
                    if len(states) != 1 or not state["partitioned"] or state["rows"] != 0:
                        raise ValueError("Invalid empty partitioned source state")
                    target = destination / dataset
                    target.mkdir(parents=True)
                    outputs[dataset] = target
                    continue
                relative = (
                    Path(dataset) / state["relative_path"] if state["partitioned"] else Path(f"{dataset}.parquet")
                )
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("Invalid retained member path")
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                schema = pa.ipc.read_schema(pa.BufferReader(state["schema"]))
                write_rows(itertools.islice(expected, state["rows"]), target, schema)
                if pq.read_metadata(target).num_rows != state["rows"]:
                    raise ValueError("Missing raw inputs for retained source file")
                outputs[dataset] = destination / dataset if state["partitioned"] else target
            if next(expected, None) is not None:
                raise ValueError("Extra prior receipt inputs without retained source rows")
        return outputs


def build_with_receipts(
    builder: Callable[..., Path | tuple[Path, ...]],
    source_directory: Path,
    destination: Path,
    *,
    generation_id: str,
    prior_bundle: Path | None = None,
    builder_kwargs: Mapping[str, Any] | None = None,
    source_witnesses: Sequence[Mapping] = (),
) -> dict:
    """Run an actual family producer with verified priors, then split its outputs.

    Callers supply retained/injected source readers for local-only builds.
    Download fallback is deliberately disabled: a missing pinned prior refuses
    source qualification through the producer's existing initial-build behavior.
    """
    source_directory.mkdir(parents=True, exist_ok=False)
    kwargs = dict(builder_kwargs or {})
    if {"download_prior", "download_members"} & set(kwargs):
        raise ValueError("Receipt builds select priors through prior_bundle only")
    if prior_bundle is not None:
        prior_manifest = json.loads((prior_bundle / "legislative-bundle.json").read_text())
        if prior_manifest["unowned_outputs"]:
            raise ValueError(
                "Mixed-owner producer requires the integrated family prior; owned-only rows cannot qualify it"
            )
    priors = {} if prior_bundle is None else restore_prior(prior_bundle, source_directory / ".verified-prior")

    def download_prior(key: str, target: Path) -> bool:
        name = key.removesuffix(".parquet")
        if name not in priors:
            return False
        source = priors[name]
        if source.is_dir():
            raise ValueError("Partitioned prior requested as a single file")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return True

    def download_members(key: str, target: Path) -> list[Path]:
        source = priors.get(key.removesuffix(".parquet"))
        if source is None:
            return []
        target.mkdir(parents=True, exist_ok=True)
        result = []
        for member in _members(source):
            relative = member.relative_to(source) if source.is_dir() else Path(member.name)
            output = target / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(member, output)
            result.append(output)
        return result

    if "download_members" in inspect.signature(builder).parameters:
        kwargs["download_members"] = download_members
    built = builder(source_directory, download_prior=download_prior, **kwargs)
    return write_legislative_outputs(
        built if isinstance(built, tuple) else (built,),
        destination,
        generation_id=generation_id,
        source_witnesses=source_witnesses,
        prior_receipts=() if prior_bundle is None else (prior_bundle / "etl_receipts.parquet",),
    )


def admit_bundle(bundle: Path, destination: Path, *, family: str = "legislative-documents"):
    """Qualify a local scoped generation through the shared admission path.

    Global publication combines other owners' policies and regenerates serving
    declarations. This artifact records local-partial status explicitly.
    """
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.native_types import described_schema

    manifest = json.loads((bundle / "legislative-bundle.json").read_text())
    files, partitions = [], {}
    for dataset, members in manifest["subjects"].items():
        if policy(dataset).receipt_only:
            continue
        if members and members[0].startswith(dataset + "/"):
            files.append(bundle / dataset)
            partitions[dataset + ".parquet"] = tuple(p.split("=", 1)[0] for p in Path(members[0]).parts[1:-1])
        else:
            files.append(bundle / f"{dataset}.parquet")
    for members in manifest.get("outcome_subjects", {}).values():
        files.extend(bundle / path for path in members)
    policies = ([policy(n) for n in manifest["datasets"]] + [FILE_POLICY] +
                [OUTCOME_POLICIES[n] for n in manifest.get("outcome_subjects", {})])
    artifact = build_generation(
        destination,
        family=family,
        files=files,
        expected_keys=tuple(p.dataset + ".parquet" for p in policies if not p.receipt_only),
        schemas={p.dataset: described_schema(p.subject_schema) for p in policies if not p.receipt_only},
        partitioned=partitions,
        publication_status="local-partial",
        receipt_path=bundle / manifest["receipt_file"],
        receipt_policies=policies,
        receipt_generation_id=manifest["generation_id"],
    )
    verify_generation(destination, expected_pin=artifact.pin)
    return artifact
