"""Government-source adoption of the shared ETL receipt writer and internal reader.

Legacy source shapers and precedence rules run on their literal inputs. Public
outputs are replaced only after the typed subjects and receipts validate. An
incremental read of a migrated subject requires its matching selected receipts.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import shutil
import sqlite3
from contextvars import ContextVar
from dataclasses import replace
from functools import wraps
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ReceiptContext,
    combine_receipts,
    failure_receipt,
    observation_receipt,
    rebind_receipt,
    read_with_receipts,
    select_receipts,
    subject_identity,
    validate_receipt_bundle,
    write_dataset,
)
from spicy_regs.sources import publication
from spicy_regs.transforms.government_source_shapes import (
    IDENTITY_FIELDS,
    LEGACY_COLUMNS,
    SUBJECT_SCHEMAS,
    GovernmentShapeError,
    map_subject,
)
from spicy_regs.transforms.parquet_rows import write_rows

#: A dataset whose subject columns changed after it first published states its own version; the rest keep the
#: family's. Columns returned from the receipt to the subject table on 2026-10-05: gao_decisions
#: ``b_numbers_truncated``; gao_recommendations ``first_seen`` and ``last_seen``.
POLICY_VERSIONS = {"gao_decisions": "government-sources/2", "gao_recommendations": "government-sources/2"}
POLICIES = {
    dataset: DatasetPolicy(
        dataset,
        schema,
        tuple(IDENTITY_FIELDS[dataset]),
        ("raw_record",),
        policy_version=POLICY_VERSIONS.get(dataset, "government-sources/1"),
        nullable_identity_fields=("entity_eft_indicator",) if dataset == "sam_entities" else (),
    )
    for dataset, schema in SUBJECT_SCHEMAS.items()
}


def _without(dataset: str, *columns: str) -> DatasetPolicy:
    """The dataset's ``government-sources/1`` policy: today's subject schema less the columns returned since."""
    schema = POLICIES[dataset].subject_schema
    for column in columns:
        schema = schema.remove(schema.get_field_index(column))
    return replace(POLICIES[dataset], subject_schema=schema, policy_version="government-sources/1")


#: Exact policies a published prior may still carry. A prior is read under one only to restore its receipts' whole
#: original rows; every write uses POLICIES. On 2026-10-04 gao-reports published gao_decisions without
#: ``b_numbers_truncated`` and gao-recommendations published without ``first_seen`` and ``last_seen``.
EARLIER_POLICIES = {
    "gao_decisions": (_without("gao_decisions", "b_numbers_truncated"),),
    "gao_recommendations": (_without("gao_recommendations", "first_seen", "last_seen"),),
}
_ACTIVE: ContextVar[bool] = ContextVar("government_receipt_build", default=False)
_INHERITED: ContextVar[dict[str, Path] | None] = ContextVar("government_prior_receipts", default=None)
# This is local build metadata for forwarding the shared API's admission arguments,
# not another receipt format. The receipts themselves use etl_receipts.RECEIPT_SCHEMA.
BUILD_METADATA = ".government-etl-build.json"


def _rows(path: Path):
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _legacy_schema(dataset: str) -> pa.Schema:
    return pa.schema(
        [
            (
                c,
                pa.int64()
                if c in ("recommendation_count", "matters_for_congress_count", "page_count")
                else pa.string(),
            )
            for c in LEGACY_COLUMNS[dataset]
        ]
    )


def _native(path: Path, dataset: str) -> bool:
    schema = pq.read_schema(path)
    return (
        schema.metadata is not None
        and schema.metadata.get(b"government_etl_policy") == b"1"
        or (
            not schema.equals(_legacy_schema(dataset), check_metadata=False)
            and schema.equals(SUBJECT_SCHEMAS[dataset], check_metadata=False)
        )
    )


def _prior_policy(dataset: str, path: Path) -> DatasetPolicy:
    """The earlier policy whose exact subject schema the prior was written under, else the current one."""
    schema = pq.read_schema(path)
    return next(
        (p for p in EARLIER_POLICIES.get(dataset, ()) if schema.equals(p.subject_schema, check_metadata=False)),
        POLICIES[dataset],
    )


def internal_prior(
    dataset: str, path: Path, *, receipt_path: Path | None = None, generation_id: str | None = None
) -> Path:
    """Restore literal processing input from a validated native subject/receipt pair.

    Explicit local receipt selection takes precedence. Otherwise a build metadata
    file must pin the subject bytes, or the captured publication index must pin
    them. Missing, changed, wrong-generation or ambiguous receipts always refuse.
    Legacy source-shaped inputs remain explicitly distinguishable by schema.
    """
    if not _native(path, dataset):
        if unknown := set(pq.read_schema(path).names) - set(LEGACY_COLUMNS[dataset]):
            raise GovernmentShapeError(f"{dataset}: unclassified prior fields {sorted(unknown)}")
        return path
    scratch = path.parent / f".{path.name}.receipts.parquet"
    selected = path.parent / f".{path.name}.selected-receipts.parquet"
    restored = path.parent / f".{path.name}.processing.parquet"
    try:
        if receipt_path is None:
            local = path.parent / BUILD_METADATA
            metadata = json.loads(local.read_text()) if local.exists() else None
            if metadata and metadata.get("subjects", {}).get(dataset, {}).get("sha256") == _digest(path):
                receipt_path = Path(metadata["receipt_path"])
                if _digest(receipt_path) != metadata["receipt_sha256"]:
                    raise ValueError("Local receipt bytes differ from the selected build")
                generation_id = metadata["generation_id"]
            else:
                public_url = os.getenv("R2_PUBLIC_URL")
                if not public_url:
                    raise ValueError(f"{dataset}: native prior requires explicitly selected receipts")
                index = publication.current_index(public_url)
                descriptor = publication.table_descriptor(index, dataset + ".parquet")
                if descriptor is None or descriptor.get("sha256") != _digest(path):
                    raise ValueError("Local native prior differs from the selected publication")
                members = publication.receipt_members(index, dataset=dataset)
                if len(members) != 1:
                    raise ValueError("Expected one selected family receipt member")
                if not publication.fetch_member(public_url, members[0], scratch, dataset + " receipts"):
                    raise ValueError("Selected receipt member is missing")
                receipt_path = scratch
                owner = publication.table_owner(index, dataset + ".parquet")
                assert owner is not None
                generation_id = owner[1]["etlReceipts"]["generationId"]
        if generation_id is None:
            raise ValueError("An explicit receipt generation identity is required")
        select_receipts(receipt_path, selected, dataset=dataset)
        inherited = _INHERITED.get()
        if inherited is not None:
            inherited[dataset] = selected
        restored_rows = (
            row["raw_record"]
            for row in read_with_receipts([path], [selected], _prior_policy(dataset, path), generation_id=generation_id)
        )
        write_rows(restored_rows, restored, _legacy_schema(dataset))
        return restored
    finally:
        scratch.unlink(missing_ok=True)
        if _INHERITED.get() is None:
            selected.unlink(missing_ok=True)


def migrate_outputs(
    paths: tuple[Path, ...], *, generation_id: str | None = None, datasets: tuple[str, ...] | None = None, evidence=None
) -> dict[str, Any]:
    """Convert every family output and retain the exact pre-conversion Parquet.

    A malformed record receives a refused receipt and no subject. The writer
    still validates the complete subject/receipt pairing before replacing any
    caller-visible file. Pre-conversion inputs stay under .government-inputs.
    """
    if not paths:
        raise ValueError("No government outputs")
    if len({path.parent for path in paths}) != 1:
        raise ValueError("Government family outputs must share a build directory")
    datasets = datasets or tuple(path.stem for path in paths)
    if len(datasets) != len(paths) or len(set(datasets)) != len(datasets):
        raise ValueError("Outputs require distinct declared dataset identities")
    directory = paths[0].parent
    generation_id = generation_id or uuid4().hex
    work = directory / ".government-etl" / generation_id
    if work.exists():
        raise FileExistsError(work)
    work.mkdir(parents=True)
    inputs = directory / ".government-inputs" / generation_id
    inputs.mkdir(parents=True)
    inherited = sqlite3.connect(work / "prior-witnesses.sqlite")
    try:
        inherited.execute(
            "CREATE TABLE witnesses (dataset TEXT, record_id TEXT, witnesses TEXT, receipt_id TEXT, generation_id TEXT, PRIMARY KEY(dataset,record_id))"
        )
        for dataset, prior_receipts in (_INHERITED.get() or {}).items():
            inherited.executemany(
                "INSERT INTO witnesses VALUES (?,?,?,?,?)",
                (
                    (dataset, row["record_id"], json.dumps(row["witnesses"]), row["receipt_id"], row["generation_id"])
                    for row in _rows(prior_receipts)
                    if row["outcome"] == "accepted"
                ),
            )
        inherited.commit()
        bundles = []
        policies = []
        counts = {}
        for path, dataset in zip(paths, datasets, strict=True):
            policy = POLICIES[dataset]
            policies.append(policy)
            retained = inputs / path.name
            # Preserve, do not reclassify, the exact bytes the existing producer wrote.
            shutil.copyfile(path, retained)
            digest = _digest(retained)
            if evidence is not None:
                evidence.retain_file(retained, stage="government-producer-output", dataset=dataset)
            failure_path = work / f"{dataset}.failures.parquet"
            failure_writer = pq.ParquetWriter(failure_path, RECEIPT_SCHEMA, compression="zstd")
            failure_batch = []
            prior_attempts = (_INHERITED.get() or {}).get(dataset)
            if prior_attempts is not None:
                for attempt in _rows(prior_attempts):
                    if attempt["outcome"] != "accepted":
                        failure_batch.append(rebind_receipt(attempt, generation_id=generation_id))
                        if len(failure_batch) >= 2000:
                            failure_writer.write_table(pa.Table.from_pylist(failure_batch, schema=RECEIPT_SCHEMA))
                            failure_batch.clear()
            attempted = accepted = refused_count = 0

            def records():
                nonlocal attempted, accepted, refused_count
                for ordinal, raw in enumerate(_rows(retained)):
                    attempted += 1
                    witnesses = [
                        {
                            "source_id": "spicy-regs:producer-output:" + dataset,
                            "source_uri": str(retained),
                            "sha256": digest,
                            "locator": f"row:{ordinal}",
                            "body_version": None,
                        }
                    ]
                    # Carry the prior selected receipt's ordered witnesses across
                    # incremental merges. These are merge inputs, not claims that
                    # every fresh field came from the prior source.
                    mapping_error = None
                    old = None
                    try:
                        subject = map_subject(dataset, raw)
                        record_id = subject_identity(policy, subject)[0]
                        old = inherited.execute(
                            "SELECT witnesses, receipt_id, generation_id FROM witnesses WHERE dataset=? AND record_id=?",
                            (dataset, record_id),
                        ).fetchone()
                        if old:
                            witnesses = json.loads(old[0]) + witnesses
                    except (GovernmentShapeError, ValueError, TypeError) as error:
                        mapping_error = error  # Retain the complete failed conversion below.
                    # The explicit page digest remains a second source witness. It is
                    # not replaced with the current run's time on carried USAspending rows.
                    if raw.get("source_capture_sha256") and (
                        not isinstance(raw["source_capture_sha256"], str)
                        or not re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", raw["source_capture_sha256"])
                    ):
                        mapping_error = GovernmentShapeError("Invalid retained source capture digest")
                    elif raw.get("source_capture_sha256"):
                        witnesses.append(
                            {
                                "source_id": "usaspending:ranking-page",
                                "source_uri": None,
                                "sha256": raw["source_capture_sha256"],
                                "locator": None,
                                "body_version": None,
                            }
                        )
                    diagnostics = {"source_qualification": "producer-output; original capture coverage unchanged"}
                    if old:
                        diagnostics["prior_receipt_id"] = old[1]
                        diagnostics["prior_generation_id"] = old[2]
                    context = ReceiptContext(
                        generation_id, f"{dataset}:{ordinal}", policy.policy_version, witnesses, diagnostics
                    )
                    try:
                        if mapping_error is not None:
                            raise mapping_error
                    except (GovernmentShapeError, ValueError, TypeError) as error:
                        refused = ReceiptContext(
                            generation_id,
                            f"{dataset}:{ordinal}",
                            policy.policy_version,
                            witnesses,
                            {**diagnostics, "error_type": type(error).__name__, "message": str(error)},
                        )
                        refused_count += 1
                        failure_batch.append(
                            failure_receipt(policy, refused, outcome="refused", raw_fields={"raw_record": raw})
                        )
                        if len(failure_batch) >= 2000:
                            failure_writer.write_table(pa.Table.from_pylist(failure_batch, schema=RECEIPT_SCHEMA))
                            failure_batch.clear()
                        continue
                    accepted += 1
                    yield {**subject, "raw_record": raw}, context
                if attempted == 0:
                    failure_batch.append(
                        observation_receipt(
                            policy,
                            ReceiptContext(
                                generation_id,
                                dataset + ":empty",
                                policy.policy_version,
                                [
                                    {
                                        "source_id": "spicy-regs:producer-output:" + dataset,
                                        "source_uri": str(retained),
                                        "sha256": digest,
                                        "locator": None,
                                        "body_version": None,
                                    }
                                ],
                                {"subject_rows": 0},
                            ),
                            processing_fields={"raw_record": None},
                        )
                    )
                if failure_batch:
                    failure_writer.write_table(pa.Table.from_pylist(failure_batch, schema=RECEIPT_SCHEMA))
                failure_writer.close()

            try:
                subject_path, receipts = write_dataset(records(), work / dataset, policy, failures=_rows(failure_path))
            finally:
                failure_writer.close()
            assert subject_path is not None
            bundles.append((path, subject_path, receipts))
            counts[dataset] = {"inputs": attempted, "subjects": accepted, "refused": refused_count}
    finally:
        inherited.close()
    combined = combine_receipts([bundle[2] for bundle in bundles], work / "etl_receipts.parquet")
    validate_receipt_bundle(
        {dataset: [bundle[1]] for dataset, bundle in zip(datasets, bundles, strict=True)},
        [combined],
        policies,
        generation_id=generation_id,
    )
    # A refused conversion is inspectable but never silently replaces a complete
    # producer output with a smaller subject table. Explicit qualification decides
    # whether to admit such a candidate; ordinary ingest refuses the replacement.
    if any(value["refused"] for value in counts.values()):
        (work / "refusal-summary.json").write_text(json.dumps(counts, indent=2) + "\n")
        raise GovernmentShapeError(f"Government conversion refused inputs; preserved candidate at {work}")
    for original, subject, _ in bundles:
        staged = original.with_name("." + original.name + ".native")
        shutil.copyfile(subject, staged)
        staged.replace(original)
    receipt_path = directory / "etl_receipts.parquet"
    shutil.copyfile(combined, receipt_path)
    metadata = {
        "generation_id": generation_id,
        "receipt_path": str(receipt_path.resolve()),
        "receipt_sha256": _digest(receipt_path),
        "subjects": {
            dataset: {"path": str(p.resolve()), "sha256": _digest(p)}
            for dataset, p in zip(datasets, paths, strict=True)
        },
        "counts": counts,
    }
    (directory / BUILD_METADATA).write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def generation_receipt_args(paths: tuple[Path, ...]) -> dict[str, Any]:
    """Arguments forwarded unchanged to the shared generation writer."""
    directory = paths[0].parent
    metadata = json.loads((directory / BUILD_METADATA).read_text())
    for path in paths:
        if metadata["subjects"][path.stem]["sha256"] != _digest(path):
            raise ValueError("Government subject changed after receipt validation")
    receipts = Path(metadata["receipt_path"])
    if _digest(receipts) != metadata["receipt_sha256"]:
        raise ValueError("Government receipts changed after validation")
    return {
        "receipt_path": receipts,
        "receipt_policies": [POLICIES[path.stem] for path in paths],
        "receipt_generation_id": metadata["generation_id"],
    }


def receipt_builder(function=None, *, output_argument: str | None = None, dataset: str | None = None):
    """Adopt actual producers; nested family builders share the outer conversion."""

    def decorate(builder):
        signature = inspect.signature(builder)

        @wraps(builder)
        def build(*args, **kwargs):
            if _ACTIVE.get():
                return builder(*args, **kwargs)
            generation_id = kwargs.pop("receipt_generation_id", None)
            bound = signature.bind(*args, **kwargs)
            owner = bound.arguments.get("self")
            generation_id = generation_id or getattr(owner, "receipt_generation_id", None)
            evidence = bound.arguments.get("evidence") or getattr(owner, "source_evidence", None)
            token = _ACTIVE.set(True)
            inherited_token = _INHERITED.set({})
            try:
                result = builder(*args, **kwargs)
                paths = (
                    (Path(bound.arguments[output_argument]),)
                    if output_argument
                    else result
                    if isinstance(result, tuple)
                    else (result,)
                )
                migrate_outputs(
                    tuple(Path(path) for path in paths),
                    generation_id=generation_id,
                    datasets=(dataset,) if dataset else None,
                    evidence=evidence,
                )
                return result
            finally:
                _ACTIVE.reset(token)
                _INHERITED.reset(inherited_token)

        return build

    return decorate(function) if function is not None else decorate
