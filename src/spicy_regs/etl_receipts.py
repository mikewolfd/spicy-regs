"""Split explicitly classified records and verify their generation-bound ETL evidence.

Source acquisition stays in SpicyDocs. Family mappers own domain decisions and
native conversion; this module refuses unknown fields and ambiguous joins.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

ParquetInput = Path | Callable[[], AbstractContextManager[BinaryIO]]

RECEIPT_KEY = "etl_receipts.parquet"
WITNESS_TYPE = pa.struct(
    [
        ("source_id", pa.string()),
        ("source_uri", pa.string()),
        ("sha256", pa.string()),
        ("locator", pa.string()),
        ("body_version", pa.string()),
    ]
)
RECEIPT_SCHEMA = pa.schema(
    [
        ("receipt_id", pa.string()),
        ("dataset", pa.string()),
        ("policy_version", pa.string()),
        ("generation_id", pa.string()),
        ("record_id", pa.string()),
        ("subject_version", pa.string()),
        ("identity_json", pa.string()),
        ("attempt_id", pa.string()),
        ("outcome", pa.string()),
        ("processor", pa.string()),
        ("witnesses", pa.list_(WITNESS_TYPE)),
        ("processing_json", pa.string()),
        ("diagnostic_json", pa.string()),
    ]
)
OUTCOMES = frozenset({"accepted", "rejected", "refused", "error", "observed"})
_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")


def _pack(value: Any) -> Any:
    # Tagged pairs for every value avoid collisions with publisher dictionaries.
    if value is None:
        return ["null", None]
    if type(value) in (bool, int, str):
        return [type(value).__name__, value]
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, bytes):
        return ["bytes", base64.b64encode(value).decode()]
    if isinstance(value, float) and math.isfinite(value):
        return ["float", value.hex()]
    if isinstance(value, Mapping) and all(isinstance(k, str) for k in value):
        return ["dict", [[k, _pack(v)] for k, v in sorted(value.items())]]
    if isinstance(value, (list, tuple)):
        return ["list", [_pack(v) for v in value]]
    raise ValueError(f"Unsupported exact receipt value: {type(value).__name__}")


def _unpack(value: Any) -> Any:
    tag, body = value
    if tag in {"null", "bool", "int", "str"}:
        return body
    if tag == "dict":
        return {k: _unpack(v) for k, v in body}
    if tag == "list":
        return [_unpack(v) for v in body]
    return {
        "decimal": Decimal,
        "datetime": datetime.fromisoformat,
        "date": date.fromisoformat,
        "bytes": base64.b64decode,
        "float": float.fromhex,
    }[tag](body)


def exact_json(value: Any) -> str:
    """Lossless, deterministic encoding for receipt evidence, never subject data."""
    return json.dumps(_pack(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def decode_exact_json(value: str) -> Any:
    """Read the existing lossless evidence encoding and require its canonical form."""
    decoded = _unpack(json.loads(value))
    if exact_json(decoded) != value:
        raise ValueError("Evidence differs from its canonical lossless encoding")
    return decoded


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(exact_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class DatasetPolicy:
    """A complete classification of one mapper's output; no field is discarded.

    ``receipt_only`` supports processing tables and failed-attempt datasets.
    Null identity components must be individually authorized; the complete tuple
    still identifies one row and duplicate identities are always refused.
    """

    dataset: str
    subject_schema: pa.Schema
    identity_fields: tuple[str, ...]
    receipt_fields: tuple[str, ...]
    policy_version: str = "1"
    receipt_only: bool = False
    nullable_identity_fields: tuple[str, ...] = ()

    def __post_init__(self):
        names = self.subject_schema.names
        if (
            not _NAME.fullmatch(self.dataset)
            or not self.policy_version
            or len({name.casefold() for name in names}) != len(names)
            or any(not name or "\0" in name for name in names)
            or len(set(self.receipt_fields)) != len(self.receipt_fields)
            or set(names) & set(self.receipt_fields)
            or len(set(self.identity_fields)) != len(self.identity_fields)
            or not set(self.identity_fields) <= set(names)
            or not set(self.nullable_identity_fields) <= set(self.identity_fields)
            or self.receipt_only != (not names)
            or (not self.receipt_only and not self.identity_fields)
        ):
            raise ValueError(f"Invalid explicit field policy: {self.dataset}")

    @property
    def input_fields(self) -> frozenset[str]:
        return frozenset((*self.subject_schema.names, *self.receipt_fields))

    def check_fields(self, row: Mapping) -> None:
        if extra := set(row) - self.input_fields:
            raise ValueError(f"{self.dataset}: unclassified fields {sorted(extra)}")

    def descriptor(self) -> dict:
        return {
            "dataset": self.dataset,
            "policy_version": self.policy_version,
            "subject_schema": base64.b64encode(self.subject_schema.serialize()).decode(),
            "identity_fields": list(self.identity_fields),
            "receipt_fields": list(self.receipt_fields),
            "receipt_only": self.receipt_only,
            "nullable_identity_fields": list(self.nullable_identity_fields),
        }

    @classmethod
    def from_descriptor(cls, value: Mapping) -> DatasetPolicy:
        if set(value) != {
            "dataset",
            "policy_version",
            "subject_schema",
            "identity_fields",
            "receipt_fields",
            "receipt_only",
            "nullable_identity_fields",
        }:
            raise ValueError("Invalid stored dataset field policy")
        args = dict(value)
        args["subject_schema"] = pa.ipc.read_schema(
            pa.BufferReader(base64.b64decode(args["subject_schema"], validate=True))
        )
        for name in ("identity_fields", "receipt_fields", "nullable_identity_fields"):
            args[name] = tuple(args[name])
        return cls(**args)


@dataclass(frozen=True)
class ReceiptContext:
    generation_id: str
    attempt_id: str
    processor: str
    witnesses: Sequence[Mapping[str, Any]]
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not all(isinstance(v, str) and v for v in (self.generation_id, self.attempt_id, self.processor)):
            raise ValueError("Receipt needs generation, attempt and processor identities")
        if not self.witnesses:
            raise ValueError("Receipt needs at least one input witness")
        for witness in self.witnesses:
            _native(witness, WITNESS_TYPE, "witness")
            if not witness.get("source_id") or not any(witness.get(k) for k in ("sha256", "body_version")):
                raise ValueError("Witness needs source identity and a content digest or pinned body version")
            digest = witness.get("sha256")
            if digest is not None and not re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", digest):
                raise ValueError("Invalid witness SHA-256")


def _native(value: Any, dtype: pa.DataType, label: str) -> None:
    """Check before Arrow can drop extra struct keys or coerce money/integers."""
    if value is None:
        return
    if pa.types.is_struct(dtype):
        if not isinstance(value, Mapping) or set(value) - {f.name for f in dtype}:
            raise ValueError(f"{label}: unexpected struct fields")
        for item in dtype:
            if value.get(item.name) is None and not item.nullable:
                raise ValueError(f"{label}.{item.name}: null is not allowed")
            _native(value.get(item.name), item.type, f"{label}.{item.name}")
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        if not isinstance(value, list):
            raise ValueError(f"{label}: expected native list")
        for item in value:
            if item is None and not dtype.value_field.nullable:
                raise ValueError(f"{label}: null list item is not allowed")
            _native(item, dtype.value_type, label)
    elif pa.types.is_decimal(dtype):
        if not isinstance(value, Decimal):
            raise ValueError(f"{label}: exact decimal required")
    elif pa.types.is_integer(dtype):
        if type(value) is not int:
            raise ValueError(f"{label}: native integer required")
    elif pa.types.is_boolean(dtype):
        if type(value) is not bool:
            raise ValueError(f"{label}: native boolean required")
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        if not isinstance(value, str):
            raise ValueError(f"{label}: native string required")
    elif pa.types.is_floating(dtype):
        if type(value) is not float or not math.isfinite(value):
            raise ValueError(f"{label}: finite native float required")


def _subject(policy: DatasetPolicy, row: Mapping) -> dict:
    missing = set(policy.subject_schema.names) - set(row)
    if missing:
        raise ValueError(f"{policy.dataset}: missing subject fields {sorted(missing)}")
    values = {f.name: row[f.name] for f in policy.subject_schema}
    for item in policy.subject_schema:
        if values[item.name] is None and not item.nullable:
            raise ValueError(f"{item.name}: null is not allowed")
        _native(values[item.name], item.type, item.name)
    return pa.Table.from_pylist([values], schema=policy.subject_schema).to_pylist()[0]


def subject_identity(policy: DatasetPolicy, subject: Mapping) -> tuple[str, str, str]:
    """Record hash, content version and exact natural key for a normalized subject."""
    identity = [[name, subject[name]] for name in policy.identity_fields]
    if any(v is None and k not in policy.nullable_identity_fields for k, v in identity):
        raise ValueError(f"{policy.dataset}: null record identity")
    return (_digest([policy.dataset, identity]), _digest([policy.dataset, subject]), exact_json(identity))


def _receipt(policy, context, *, subject, processing, outcome, identity=None):
    if outcome not in OUTCOMES or (subject is not None) != (outcome == "accepted"):
        raise ValueError("Receipt outcome and subject presence disagree")
    record_id, version, identity_json = subject_identity(policy, subject) if subject is not None else (None, None, None)
    if identity is not None:
        if set(identity) != set(policy.identity_fields):
            raise ValueError("Failed attempt identity differs from the declared key")
        record_id, _, identity_json = subject_identity(policy, identity)
    receipt = {
        "dataset": policy.dataset,
        "policy_version": policy.policy_version,
        "generation_id": context.generation_id,
        "record_id": record_id,
        "subject_version": version,
        "identity_json": identity_json,
        "attempt_id": context.attempt_id,
        "outcome": outcome,
        "processor": context.processor,
        "witnesses": pa.array([context.witnesses], type=pa.list_(WITNESS_TYPE)).to_pylist()[0],
        "processing_json": exact_json(processing),
        "diagnostic_json": exact_json(context.diagnostics),
    }
    return {"receipt_id": _digest(receipt), **receipt}


def split_record(policy: DatasetPolicy, row: Mapping, context: ReceiptContext) -> tuple[dict | None, dict]:
    """Split one explicitly classified, already mapped record without silently dropping fields."""
    policy.check_fields(row)
    subject = None if policy.receipt_only else _subject(policy, row)
    processing = {key: row[key] for key in policy.receipt_fields if key in row}
    return subject, _receipt(
        policy,
        context,
        subject=subject,
        processing=processing,
        outcome="observed" if policy.receipt_only else "accepted",
    )


def failure_receipt(
    policy: DatasetPolicy,
    context: ReceiptContext,
    *,
    outcome: str,
    raw_fields: Mapping,
    identity: Mapping | None = None,
) -> dict:
    """Retain a failed conversion/refusal without fabricating a subject row."""
    if outcome not in {"error", "rejected", "refused"}:
        raise ValueError("Failure receipt needs a failure outcome")
    policy.check_fields(raw_fields)
    return _receipt(policy, context, subject=None, processing=raw_fields, outcome=outcome, identity=identity)


def write_dataset(
    records: Iterable[tuple[Mapping, ReceiptContext]],
    directory: Path,
    policy: DatasetPolicy,
    *,
    failures: Iterable[dict] = (),
    batch_size: int = 2000,
) -> tuple[Path | None, Path]:
    """Atomically expose a new local directory with subject data and receipts.

    Accepts mappings from Arrow batches or Polars ``iter_rows(named=True)``.
    Existing directories are never replaced. Combine dataset receipts into one
    shared member with ``combine_receipts`` before generation admission.
    """
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be positive")
    directory = Path(directory)
    if directory.exists():
        raise FileExistsError(directory)
    directory.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=directory.parent, prefix=".etl-") as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        subject_path = stage / f"{policy.dataset}.parquet"
        receipt_path = stage / RECEIPT_KEY
        from contextlib import ExitStack

        with ExitStack() as stack:
            subject_writer = (
                None
                if policy.receipt_only
                else stack.enter_context(pq.ParquetWriter(subject_path, policy.subject_schema, compression="zstd"))
            )
            receipt_writer = stack.enter_context(pq.ParquetWriter(receipt_path, RECEIPT_SCHEMA, compression="zstd"))
            subjects, receipts = [], []

            def flush():
                if subjects:
                    assert subject_writer is not None
                    subject_writer.write_table(pa.Table.from_pylist(subjects, schema=policy.subject_schema))
                    subjects.clear()
                if receipts:
                    receipt_writer.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA))
                    receipts.clear()

            for row, context in records:
                subject, receipt = split_record(policy, row, context)
                if subject is not None:
                    subjects.append(subject)
                receipts.append(receipt)
                if len(receipts) >= batch_size:
                    flush()
            for receipt in failures:
                receipts.append(receipt)
                if len(receipts) >= batch_size:
                    flush()
            flush()
        validate_receipt_bundle(
            {policy.dataset: [] if policy.receipt_only else [subject_path]}, [receipt_path], [policy]
        )
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(stage, directory)
    return (None if policy.receipt_only else directory / subject_path.name, directory / RECEIPT_KEY)


def combine_receipts(paths: Sequence[Path], destination: Path) -> Path:
    """Combine dataset shards, keeping every row and witness, with the shared schema."""
    from spicy_regs.transforms.parquet_rows import write_rows

    return write_rows((row for path in paths for row in _rows(path)), destination, RECEIPT_SCHEMA)


@contextmanager
def _parquet(path):
    # Pinned remote sources supply a seekable context-managed opener.
    if callable(path):
        with path() as stream, pq.ParquetFile(stream) as parquet:
            yield parquet
    else:
        with pq.ParquetFile(path) as parquet:
            yield parquet


def _rows(path):
    with _parquet(path) as parquet:
        for batch in parquet.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _load_receipts(connection, receipt_paths, policies, generation_id):
    connection.execute(
        "CREATE TABLE receipts (dataset TEXT, record_id TEXT, version TEXT, identity_json TEXT, "
        "receipt_id TEXT UNIQUE, outcome TEXT, processing TEXT, used INTEGER DEFAULT 0)"
    )
    connection.execute("CREATE UNIQUE INDEX accepted_identity ON receipts(dataset, record_id) WHERE outcome='accepted'")
    generations = set()
    for path in receipt_paths:
        with _parquet(path) as parquet:
            if not parquet.schema_arrow.equals(RECEIPT_SCHEMA):
                raise ValueError("Receipt schema differs from the shared schema")
        for receipt in _rows(path):
            policy = policies.get(receipt["dataset"])
            if policy is None or policy.policy_version != receipt["policy_version"]:
                raise ValueError("Receipt has no matching dataset policy")
            if _digest({k: v for k, v in receipt.items() if k != "receipt_id"}) != receipt["receipt_id"]:
                raise ValueError("Receipt digest differs from its contents")
            ReceiptContext(
                receipt["generation_id"],
                receipt["attempt_id"],
                receipt["processor"],
                receipt["witnesses"],
                _unpack(json.loads(receipt["diagnostic_json"])),
            )
            generations.add(receipt["generation_id"])
            if len(generations) > 1 or (generation_id is not None and generations != {generation_id}):
                raise ValueError("Receipts mix or differ from the selected generation")
            accepted = receipt["outcome"] == "accepted"
            if (
                receipt["outcome"] not in OUTCOMES
                or accepted
                and policy.receipt_only
                or accepted != (receipt["subject_version"] is not None)
                or accepted
                and (not receipt["record_id"] or not receipt["identity_json"])
            ):
                raise ValueError("Invalid receipt outcome or subject identity")
            processing = _unpack(json.loads(receipt["processing_json"]))
            allowed = (
                policy.input_fields
                if receipt["outcome"] in {"error", "rejected", "refused"}
                else set(policy.receipt_fields)
            )
            if not isinstance(processing, dict) or set(processing) - allowed:
                raise ValueError("Receipt contains unclassified processing fields")
            try:
                connection.execute(
                    "INSERT INTO receipts VALUES (?,?,?,?,?,?,?,0)",
                    [
                        receipt[k]
                        for k in (
                            "dataset",
                            "record_id",
                            "subject_version",
                            "identity_json",
                            "receipt_id",
                            "outcome",
                            "processing_json",
                        )
                    ],
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Duplicate or ambiguous receipt join") from exc


def _match_subjects(connection, subjects, policies):
    for dataset, paths in subjects.items():
        policy = policies[dataset]
        if policy.receipt_only and paths:
            raise ValueError("Processing-only dataset cannot publish a subject table")
        for path in paths:
            with _parquet(path) as parquet:
                if not parquet.schema_arrow.equals(policy.subject_schema):
                    raise ValueError(f"Subject schema differs from policy: {dataset}")
            for row in _rows(path):
                row = _subject(policy, row)
                record_id, version, identity = subject_identity(policy, row)
                found = connection.execute(
                    "SELECT receipt_id, processing, used FROM receipts WHERE dataset=? "
                    "AND record_id=? AND version=? AND identity_json=? AND outcome='accepted'",
                    [dataset, record_id, version, identity],
                ).fetchone()
                if found is None or found[2]:
                    raise ValueError(f"Missing, ambiguous or reused subject receipt: {dataset}")
                connection.execute("UPDATE receipts SET used=1 WHERE receipt_id=?", [found[0]])
                yield row, _unpack(json.loads(found[1]))
    if connection.execute("SELECT 1 FROM receipts WHERE outcome='accepted' AND used=0 LIMIT 1").fetchone():
        raise ValueError("Accepted receipt has no matching subject")


def validate_receipt_bundle(
    subjects: Mapping[str, Sequence[ParquetInput]],
    receipt_paths: Sequence[ParquetInput],
    policies: Sequence[DatasetPolicy],
    *,
    generation_id: str | None = None,
) -> None:
    """Check native schemas and exact one-to-one accepted joins using a disk index.

    Includes failures with no subjects. A generation may contain no records;
    callers still bind its explicit generation id in artifact metadata.
    """
    registered = {p.dataset: p for p in policies}
    if len(registered) != len(policies) or set(subjects) != set(registered) or not receipt_paths:
        raise ValueError("Bundle datasets differ from explicit policies or receipts are missing")
    with TemporaryDirectory(prefix="etl-joins-") as temp, sqlite3.connect(str(Path(temp) / "joins.db")) as con:
        _load_receipts(con, receipt_paths, registered, generation_id)
        for _ in _match_subjects(con, subjects, registered):
            pass


def read_with_receipts(
    subject_paths: Sequence[ParquetInput],
    receipt_paths: Sequence[ParquetInput],
    policy: DatasetPolicy,
    *,
    generation_id: str,
) -> Iterable[dict]:
    """Reconstruct internal processing columns after complete validation; no legacy fallback.

    Receipt inputs here must be scoped to this dataset. Use ``select_receipts``
    when reading the shared member; unrelated datasets never weaken admission.
    Only accepted subjects are returned. Processing-only checkpoints, successful
    empty reads and failed attempts are available through ``read_attempts``.
    """
    with TemporaryDirectory(prefix="etl-read-") as temp, sqlite3.connect(str(Path(temp) / "joins.db")) as con:
        _load_receipts(con, receipt_paths, {policy.dataset: policy}, generation_id)
        for _ in _match_subjects(con, {policy.dataset: subject_paths}, {policy.dataset: policy}):
            pass
        con.execute("UPDATE receipts SET used=0")
        for subject, processing in _match_subjects(con, {policy.dataset: subject_paths}, {policy.dataset: policy}):
            yield subject | processing


def select_receipts(path: Path, destination: Path, *, dataset: str) -> Path:
    """Select a dataset's receipt rows without losing failed attempts."""
    from spicy_regs.transforms.parquet_rows import write_rows

    return write_rows((row for row in _rows(path) if row["dataset"] == dataset), destination, RECEIPT_SCHEMA)


def rebind_receipt(receipt: Mapping, *, generation_id: str) -> dict:
    """Carry exact prior processing evidence into a new selected generation.

    Does not reinterpret an old observation as a new acquisition. Original
    witnesses, processor, attempt and values stay intact; diagnostic lineage
    records the prior receipt and generation. Admission still checks the row.
    """
    if not isinstance(generation_id, str) or not generation_id:
        raise ValueError("A carried receipt needs the new generation identity")
    if set(receipt) != set(RECEIPT_SCHEMA.names):
        raise ValueError("Carried receipt fields differ from shared schema")
    if _digest({k: v for k, v in receipt.items() if k != "receipt_id"}) != receipt["receipt_id"]:
        raise ValueError("Carried receipt digest differs from its contents")
    if generation_id == receipt["generation_id"]:
        return dict(receipt)
    diagnostics = _retain_history(
        receipt,
        {
            **_unpack(json.loads(receipt["diagnostic_json"])),
            "carried_from": {"receipt_id": receipt["receipt_id"], "generation_id": receipt["generation_id"]},
        },
    )
    updated = {**receipt, "generation_id": generation_id, "diagnostic_json": exact_json(diagnostics)}
    updated["receipt_id"] = _digest({k: v for k, v in updated.items() if k != "receipt_id"})
    return updated


def observation_receipt(policy: DatasetPolicy, context: ReceiptContext, *, processing_fields: Mapping) -> dict:
    """Retain a successful source read/checkpoint that emitted no subject row."""
    if set(processing_fields) - set(policy.receipt_fields):
        raise ValueError("Observation contains unclassified processing fields")
    return _receipt(policy, context, subject=None, processing=processing_fields, outcome="observed")


def read_attempts(
    receipt_paths: Sequence[Path], policy: DatasetPolicy, *, generation_id: str, outcomes: frozenset[str] | None = None
) -> Iterable[dict]:
    """Read decoded processing evidence including successful-empty and failed reads.

    Inputs must be scoped to the dataset and come from an admitted generation;
    this checks receipts, while generation admission proves subject joins.
    """
    if outcomes is not None and not outcomes <= OUTCOMES:
        raise ValueError("Unknown attempt outcome")
    with TemporaryDirectory(prefix="etl-attempts-") as temp, sqlite3.connect(str(Path(temp) / "joins.db")) as con:
        _load_receipts(con, receipt_paths, {policy.dataset: policy}, generation_id)
        for path in receipt_paths:
            for receipt in _rows(path):
                if outcomes is None or receipt["outcome"] in outcomes:
                    yield {
                        **receipt,
                        "processing_fields": _unpack(json.loads(receipt["processing_json"])),
                        "diagnostics": _unpack(json.loads(receipt["diagnostic_json"])),
                    }


def _retain_history(prior: Mapping, diagnostics: Mapping) -> dict:
    """Retain source values once by digest, and prior receipt identities as references."""
    body = {k: v for k, v in prior.items() if k != "receipt_id"}
    if set(prior) != set(RECEIPT_SCHEMA.names) or _digest(body) != prior["receipt_id"]:
        raise ValueError("Prior receipt digest differs from its contents")
    previous = _unpack(json.loads(prior["diagnostic_json"]))
    retained = dict(diagnostics.get("retained_processing", {}))
    retained.update(previous.get("retained_processing", {}))
    processing = _unpack(json.loads(prior["processing_json"]))
    digest = _digest(processing)
    retained[digest] = processing
    if any(_digest(value) != key for key, value in retained.items()):
        raise ValueError("Retained processing digest differs")
    references = [
        *diagnostics.get("prior_receipts", ()),
        *previous.get("prior_receipts", ()),
        {
            "receipt_id": prior["receipt_id"],
            "generation_id": prior["generation_id"],
            "processing_sha256": digest,
            "processor": prior["processor"],
            "attempt_id": prior["attempt_id"],
            "outcome": prior["outcome"],
            "diagnostics": {k: v for k, v in previous.items() if k not in {"prior_receipts", "retained_processing"}},
        },
    ]
    references = list({(r["generation_id"], r["receipt_id"]): r for r in references}.values())
    return {**diagnostics, "prior_receipts": references, "retained_processing": retained}


def inherit_receipt(context: ReceiptContext, prior: Mapping | None) -> ReceiptContext:
    """Preserve witnesses, prior receipt identities and exact source/processing values.

    Digest-keyed processing payloads are flat and deduplicated. Receipt history
    stores identities, not recursively serialized prior receipts.
    """
    if prior is None:
        return context
    from dataclasses import replace

    retained = _retain_history(prior, context.diagnostics)
    digest = _digest(_unpack(json.loads(prior["processing_json"])))
    reference = {
        "source_id": "prior-etl-processing",
        "source_uri": "receipt-processing:" + digest,
        "sha256": digest,
        "locator": "/",
        "body_version": prior["receipt_id"],
    }
    return replace(context, witnesses=[*prior["witnesses"], reference, *context.witnesses], diagnostics=retained)


def resolve_receipt_witness(receipt: Mapping, witness: Mapping) -> bytes:
    """Resolve current or retained source/processing values and verify their digest."""
    body = {k: v for k, v in receipt.items() if k != "receipt_id"}
    if _digest(body) != receipt["receipt_id"]:
        raise ValueError("Retained receipt digest differs")
    processing = _unpack(json.loads(receipt["processing_json"]))
    retained = _unpack(json.loads(receipt["diagnostic_json"])).get("retained_processing", {})
    if any(_digest(value) != key for key, value in retained.items()):
        raise ValueError("Retained processing digest differs")
    values = {_digest(processing): processing, **retained}
    uri = witness.get("source_uri") or ""
    if uri.startswith("receipt-processing:"):
        key = uri.removeprefix("receipt-processing:")
        if key in values and key.removeprefix("sha256:") == str(witness.get("sha256", "")).removeprefix("sha256:"):
            return exact_json(values[key]).encode()
    if str(witness.get("locator", "")).startswith("receipt.values."):
        field = witness["locator"].removeprefix("receipt.values.").split(" ", 1)[0]
        for current in values.values():
            if field in current:
                encoded = exact_json(current[field]).encode()
                if hashlib.sha256(encoded).hexdigest() == str(witness.get("sha256", "")).removeprefix("sha256:"):
                    return encoded
    raise ValueError("Receipt witness has no retained payload")


class ReceiptLineage:
    """Disk-backed accepted-prior lookup shared by family writers."""

    def __init__(self, paths, *, dataset):
        self._temporary = TemporaryDirectory(prefix="receipt-lineage-")
        self.connection = sqlite3.connect(str(Path(self._temporary.name) / "prior.db"))
        self.connection.execute("CREATE TABLE prior (record_id TEXT PRIMARY KEY, receipt TEXT)")
        self.connection.execute(
            "CREATE TABLE observations (processing TEXT, receipt_id TEXT PRIMARY KEY, receipt TEXT)"
        )
        for path in paths:
            for receipt in _rows(path):
                if receipt["dataset"] == dataset and receipt["outcome"] == "observed":
                    self.connection.execute(
                        "INSERT OR IGNORE INTO observations VALUES (?, ?, ?)",
                        [receipt["processing_json"], receipt["receipt_id"], exact_json(receipt)],
                    )
                if receipt["dataset"] == dataset and receipt["outcome"] == "accepted":
                    encoded = exact_json(receipt)
                    existing = self.connection.execute(
                        "SELECT receipt FROM prior WHERE record_id=?", [receipt["record_id"]]
                    ).fetchone()
                    if existing is not None and existing[0] != encoded:
                        raise ValueError("Conflicting selected prior receipts")
                    self.connection.execute(
                        "INSERT OR IGNORE INTO prior VALUES (?, ?)", [receipt["record_id"], encoded]
                    )

    def inherit(self, context, policy, subject):
        identity = subject_identity(policy, subject)[0]
        row = self.connection.execute("SELECT receipt FROM prior WHERE record_id=?", [identity]).fetchone()
        return inherit_receipt(context, None if row is None else _unpack(json.loads(row[0])))

    def inherit_processing(self, context, processing):
        rows = self.connection.execute("SELECT receipt FROM observations WHERE processing=?", [exact_json(processing)])
        for row in rows:
            context = inherit_receipt(context, _unpack(json.loads(row[0])))
        return context

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()
        self._temporary.cleanup()


def retire_receipt(receipt: Mapping, *, generation_id: str, reason: str) -> dict:
    """Retain a removed subject's evidence as an observed historical attempt."""
    context = inherit_receipt(
        ReceiptContext(
            generation_id,
            receipt["attempt_id"] + ":retired",
            receipt["processor"],
            receipt["witnesses"],
            {"retired_reason": reason},
        ),
        receipt,
    )
    result = dict(
        receipt,
        generation_id=generation_id,
        outcome="observed",
        subject_version=None,
        attempt_id=context.attempt_id,
        witnesses=list(context.witnesses),
        diagnostic_json=exact_json(context.diagnostics),
    )
    result["receipt_id"] = _digest({k: v for k, v in result.items() if k != "receipt_id"})
    return result
