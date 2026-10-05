"""Regulations-family writers and qualified internal reads using shared receipts.

The local producer seals native subject files and the one
shared receipt member together. It never updates a remote pointer or catalog.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from hashlib import file_digest, sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_SCHEMA,
    combine_receipts,
    failure_receipt,
    read_with_receipts,
    observation_receipt,
    read_attempts,
    exact_json,
    select_receipts,
    write_dataset,
)
from spicy_regs.transforms.regulations_shape import (
    IDENTITIES,
    SOURCE_COLUMNS,
    RECEIPT_COLUMNS,
    TYPES,
    shape_record,
    subject_schema,
)

NULLABLE_IDENTITIES = {
    "agency_lifecycle_stats": ("agency_code",),
    "agency_monthly_volume": ("agency_code", "document_type"),
    "comments_index": ("agency_code", "docket_id", "year", "month"),
}


@cache
def policy(dataset: str) -> DatasetPolicy:
    return DatasetPolicy(
        dataset,
        subject_schema(dataset),
        IDENTITIES[dataset],
        (*RECEIPT_COLUMNS[dataset], "raw_conversion_inputs", "input_metadata", "raw_source_record"),
        policy_version="regulations-native-v1",
        nullable_identity_fields=NULLABLE_IDENTITIES.get(dataset, ()),
    )


@dataclass(frozen=True)
class ReceiptInput:
    """One selected dataset, its exact generation and its shared receipt member."""

    dataset: str
    subjects: tuple[Path, ...]
    receipts: Path
    generation_id: str

    def __post_init__(self):
        policy(self.dataset)
        if not self.subjects or not self.generation_id:
            raise ValueError("Dataset subjects and selected generation are required")


def _qualified_rows(selected: ReceiptInput) -> Iterable[dict]:
    """Validate the whole selected dataset before reconstructing processing input.

    Absence, ambiguity, a different subject version or a different generation
    refuses; no public projection can substitute for missing retry evidence.
    """
    with TemporaryDirectory(prefix="regulations-receipt-read-") as temporary:
        scoped = select_receipts(selected.receipts, Path(temporary) / "receipts.parquet", dataset=selected.dataset)
        # Complete input consistency validation precedes any processor yield.
        # This stays bounded and catches a bad later row before an external fetch.
        for row in read_with_receipts(
            selected.subjects, [scoped], policy(selected.dataset), generation_id=selected.generation_id
        ):
            _processor_input(selected.dataset, row)
        yield from read_with_receipts(
            selected.subjects, [scoped], policy(selected.dataset), generation_id=selected.generation_id
        )


def _processor_input(dataset, row):
    value = row.get("raw_conversion_inputs")
    if not isinstance(value, Mapping):
        raise ValueError(f"{dataset}: exact retained processor input is required")
    shaped = shape_record(dataset, value)
    schema = subject_schema(dataset)
    reproduced = pa.Table.from_pylist([shaped], schema=schema).to_pylist()[0]
    selected = {name: row.get(name) for name in schema.names}
    if exact_json(reproduced) != exact_json(selected):
        raise ValueError(f"{dataset}: retained processor input differs from selected native subject")
    if any(exact_json(shaped.get(name)) != exact_json(row.get(name)) for name in RECEIPT_COLUMNS[dataset]):
        raise ValueError(f"{dataset}: retained processor input differs from selected processing evidence")
    return dict(value)


def read_internal(selected: ReceiptInput) -> Iterable[dict]:
    """Read exact retained processor inputs after validating the selected dataset."""
    for row in _qualified_rows(selected):
        yield _processor_input(selected.dataset, row)


def materialize_internal(selected: ReceiptInput, destination: Path) -> Path:
    """Bounded exact retained processor inputs, with qualified file metadata.

    File-level placement/aggregation metadata is restored only when every row's
    receipt agrees. This keeps CFR incremental placement from becoming a blind
    full re-read after the split, and never promotes one row's marker to a file.
    """
    from spicy_regs.transforms.parquet_rows import write_rows

    rows = iter(_qualified_rows(selected))
    first = next(rows, None)
    metadata = first.get("input_metadata", {}) if first is not None else {}
    if first is None:
        with TemporaryDirectory(prefix="regulations-empty-metadata-") as temporary:
            scoped = select_receipts(selected.receipts, Path(temporary) / "receipts.parquet", dataset=selected.dataset)
            observations = list(
                read_attempts(
                    [scoped],
                    policy(selected.dataset),
                    generation_id=selected.generation_id,
                    outcomes=frozenset({"observed"}),
                )
            )
            held = [
                a["processing_fields"]["input_metadata"]
                for a in observations
                if a["diagnostics"].get("kind") == "input_file_metadata"
            ]
            if held and any(value != held[0] for value in held):
                raise ValueError("Empty input metadata differs across selected receipts")
            metadata = held[0] if held else {}
    schema = pa.schema([(name, TYPES[t]) for name, t in SOURCE_COLUMNS[selected.dataset]])
    if metadata:
        schema = schema.with_metadata(metadata)

    def restored():
        if first is not None:
            yield _processor_input(selected.dataset, first)
        for row in rows:
            if row.get("input_metadata", {}) != metadata:
                raise ValueError("Input metadata differs across selected receipts")
            yield _processor_input(selected.dataset, row)

    return write_rows(restored(), destination, schema)


def _shape_regulations_attempt(dataset: str, row: Mapping, context: ReceiptContext, *,
                            project: Callable[[Mapping], Mapping] | None = None,
                            input_metadata: Mapping | None = None):
    """Shape one exact source attempt using the row writer's refusal and diagnostic rules."""
    declared = policy(dataset)
    try:
        shaped = shape_record(dataset, project(row) if project is not None else row)
        if project is not None:
            shaped["raw_source_record"] = dict(row)
        if any(shaped.get(key) is None for key in declared.identity_fields
               if key not in declared.nullable_identity_fields):
            raise ValueError(f"{dataset}: missing required subject identity")
    except (ValueError, TypeError, pa.ArrowException) as error:
        failed = ReceiptContext(context.generation_id, context.attempt_id, context.processor, context.witnesses,
                                {**context.diagnostics, "error_type": type(error).__name__, "error": str(error)})
        return None, failure_receipt(declared, failed, outcome="refused", raw_fields={
            "raw_conversion_inputs": dict(row), "input_metadata": dict(input_metadata or {})})
    shaped["input_metadata"] = dict(input_metadata or {})
    return shaped, None


def map_regulations_attempt(dataset: str, row: Mapping, context: ReceiptContext, *,
                            project: Callable[[Mapping], Mapping] | None = None,
                            input_metadata: Mapping | None = None):
    """One row attempt for bounded bulk fallbacks, with the exact writer classification."""
    from spicy_regs.etl_receipts import split_record
    shaped, failure = _shape_regulations_attempt(dataset, row, context, project=project, input_metadata=input_metadata)
    return (None, failure) if shaped is None else split_record(policy(dataset), shaped, context)


def write_records(
    dataset: str,
    records: Iterable[tuple[Mapping, ReceiptContext]],
    destination: Path,
    *,
    input_metadata: Mapping | None = None,
    project: Callable[[Mapping], Mapping] | None = None,
    observation_context: ReceiptContext | None = None,
    prior_receipts: Sequence[Path] = (),
) -> tuple[Path, Path]:
    """Write one dataset and preserve malformed-input attempts without subjects.

    Unknown or invalid source values remain in a refused receipt with their full
    source row. The exception class and reason are diagnostic fields. Successful
    rows require a stable subject identity and exactly one matching receipt.
    """
    declared = policy(dataset)
    with TemporaryDirectory(prefix="regulations-refusals-") as temp:
        refusal_path = Path(temp) / "refused.parquet"
        writer = pq.ParquetWriter(refusal_path, RECEIPT_SCHEMA, compression="zstd")
        refused = []

        def flush():
            if refused:
                writer.write_table(pa.Table.from_pylist(refused, schema=RECEIPT_SCHEMA))
                refused.clear()

        def mapped():
            try:
                for row, context in records:
                    shaped, receipt = _shape_regulations_attempt(dataset, row, context, project=project,
                                                               input_metadata=input_metadata)
                    if shaped is None:
                        refused.append(receipt)
                        if len(refused) >= 2000:
                            flush()
                        continue
                    yield shaped, context
            finally:
                flush()
                writer.close()

        def failures():
            if observation_context is not None:
                yield observation_receipt(
                    declared, observation_context, processing_fields={"input_metadata": dict(input_metadata or {})}
                )
            for batch in pq.ParquetFile(refusal_path).iter_batches(batch_size=2000):
                yield from batch.to_pylist()

        try:
            subject, receipts = write_dataset(mapped(), destination, declared, failures=failures(), prior_receipts=prior_receipts)
        finally:
            writer.close()
        assert subject is not None
        return subject, receipts


def write_held_dataset(
    dataset: str,
    source: Path,
    destination: Path,
    *,
    generation_id: str,
    processor: str = "spicy-regs:regulations-native-v1",
    witnesses: Sequence[Mapping] = (),
    include_source_witness: bool = True,
    prior_receipts: Sequence[Path] = (),
) -> tuple[Path, Path]:
    """Convert retained rows with witnesses to their exact receipt-held input.

    Existing source metadata (placement rules, omitted dates, evaluation clock)
    goes into each receipt. Neither source bytes nor their metadata are deleted.
    """
    parquet = pq.ParquetFile(source)
    metadata = dict(parquet.schema_arrow.metadata or {})
    # The exact input metadata is receipt data, including byte-valued keys.
    metadata = {key.decode("utf-8"): value for key, value in metadata.items()}

    def records():
        ordinal = 0
        for batch in parquet.iter_batches(batch_size=2000):
            for row in batch.to_pylist():
                witness = {
                    "source_id": f"retained:{dataset}",
                    "source_uri": None,
                    "sha256": sha256(exact_json(row).encode()).hexdigest(),
                    "locator": "receipt.values.raw_conversion_inputs (canonical exact_json)",
                    "body_version": None,
                }
                context = ReceiptContext(
                    generation_id,
                    f"{dataset}:row:{ordinal}",
                    processor,
                    ([witness] if include_source_witness else []) + list(witnesses),
                    {"output_row_ordinal": ordinal} if not include_source_witness else {},
                )
                yield row, context
                ordinal += 1

    input_witness = {
        "source_id": f"retained:{dataset}",
        "source_uri": None,
        "sha256": sha256(exact_json(metadata).encode()).hexdigest(),
        "locator": "receipt.values.input_metadata (canonical exact_json)",
        "body_version": None,
    }
    observed = ReceiptContext(
        generation_id,
        f"{dataset}:input-file",
        processor,
        ([input_witness] if include_source_witness else []) + list(witnesses),
        {"kind": "input_file_metadata", "rows": parquet.metadata.num_rows},
    )
    return write_records(dataset, records(), destination, input_metadata=metadata, observation_context=observed,
                         prior_receipts=prior_receipts)


def build_local_generation(
    sources: Mapping[str, Path],
    destination: Path,
    *,
    generation_id: str,
    family: str = "regulations",
    publication_status: str = "local-partial",
    witnesses: Sequence[Mapping] = (),
    include_source_witness: bool = True,
    prior_receipts: Mapping[str, Sequence[Path]] | None = None,
):
    """Replay explicitly selected held datasets and admit their complete row joins.

    Local-partial is deliberate: retained input alone does not prove complete
    source coverage. Publishers must explicitly declare and qualify a full family.
    """
    from spicy_regs.generations import build_generation

    if not sources or not set(sources) <= set(SOURCE_COLUMNS):
        raise ValueError("Explicit assigned regulations datasets are required")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-native-", dir=destination.parent) as temp:
        work = Path(temp)
        subjects = []
        receipts = []
        for name, source in sources.items():
            subject, receipt = write_held_dataset(
                name,
                source,
                work / name,
                generation_id=generation_id,
                witnesses=witnesses,
                include_source_witness=include_source_witness,
                prior_receipts=(prior_receipts or {}).get(name, ()),
            )
            subjects.append(subject)
            receipts.append(receipt)
        shared = combine_receipts(receipts, work / "etl_receipts.parquet")
        artifact = build_generation(
            work / "generation",
            family=family,
            files=subjects,
            expected_keys=[p.name for p in subjects],
            publication_status=publication_status,
            receipt_path=shared,
            receipt_policies=[policy(n) for n in sources],
            receipt_generation_id=generation_id,
        )
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(work / "generation", destination)
        return artifact


def build_from_receipts(
    inputs: Sequence[ReceiptInput],
    destination: Path,
    *,
    generation_id: str,
    outputs: Sequence[str],
    builder: Callable[[Path], object],
    family: str = "regulations",
):
    """Run an existing producer over receipt-qualified internal inputs, then seal outputs.

    This is an explicit producer entry, not a serving-time cleanup. Failed joins
    refuse before the builder runs. The builder must perform local work only.
    """
    if len({i.dataset for i in inputs}) != len(inputs):
        raise ValueError("Repeated input dataset")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-producer-", dir=destination.parent) as temp:
        work = Path(temp)
        for selected in inputs:
            materialize_internal(selected, work / f"{selected.dataset}.parquet")
        builder(work)
        sources = {name: work / f"{name}.parquet" for name in outputs}
        if any(not p.is_file() for p in sources.values()):
            raise ValueError("Producer did not complete every declared output")
        witnesses = []
        for selected in inputs:
            for source in (*selected.subjects, selected.receipts):
                with source.open("rb") as body:
                    digest = file_digest(body, "sha256").hexdigest()
                witnesses.append(
                    {
                        "source_id": f"{selected.dataset}:{source.name}",
                        "source_uri": str(source),
                        "sha256": digest,
                        "locator": None,
                        "body_version": selected.generation_id,
                    }
                )
        return build_local_generation(
            sources,
            destination,
            generation_id=generation_id,
            family=family,
            witnesses=witnesses,
            include_source_witness=False,
            prior_receipts={name: [i.receipts for i in inputs if i.dataset == name] for name in outputs},
        )


def write_source_records(dataset: str, records: Iterable[tuple[Mapping, ReceiptContext]], destination: Path):
    """Native Regulations.gov source/staging writer for base and attribute rows.

    The caller supplies each captured raw record and its pinned context. Source
    validation and projection refusals are retained. The original raw record is
    processing evidence, and declared empty/null attachment lists stay distinct.
    This is local staging; a catalog switch still needs paired transactional writes.
    """
    import json
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.transforms.regulations_attributes import _projection_and_digest

    def source_attributes(payload):
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
            raise ValueError("Source data must be an object")
        data = payload["data"]
        attrs = data.get("attributes")
        if attrs is not None and not isinstance(attrs, Mapping):
            raise ValueError("Source attributes must be an object or null")
        return data, attrs or {}

    if dataset in ("docket_attributes", "document_attributes", "comment_attributes"):
        project, _ = _projection_and_digest(dataset)

        def mapper(payload):
            data, attrs = source_attributes(payload)
            return project(data.get("id"), attrs)
    elif dataset in RECORD_TYPES:
        extract = RECORD_TYPES[dataset].extract

        def formats(value):
            if value is None:
                return None
            if not isinstance(value, list):
                raise ValueError("Native fileFormats must be a list or null")
            if any(f is not None and not isinstance(f, Mapping) for f in value):
                raise ValueError("Source file format must be an object or null")
            return [
                None if f is None else {"url": f.get("fileUrl"), "format": f.get("format"), "size": f.get("size")}
                for f in value
            ]

        def mapper(payload):
            data, attrs = source_attributes(payload)
            # The old compact attachment shaper discards null/empty entries.
            # Shape scalar fields with an empty attachment input, then supply
            # the literal attachment structure below, preserving its positions.
            prepared = {**payload, "data": {**data, "attributes": dict(attrs)}}
            if dataset == "documents":
                prepared["data"]["attributes"]["fileFormats"] = []
            elif dataset == "comments":
                prepared["included"] = []
            row = extract(prepared)
            if dataset == "documents":
                if "fileFormats" in attrs:
                    row["attachments_json"] = json.dumps(formats(attrs["fileFormats"]))
                if "additionalRins" in attrs:
                    row["additional_rins"] = json.dumps(attrs["additionalRins"])
            elif dataset == "comments" and "included" in payload:
                included = payload["included"]
                if included is not None and not isinstance(included, list):
                    raise ValueError("Native included attachment records must be a list or null")
                attachments = []
                for item in included or ():
                    if not isinstance(item, Mapping):
                        raise ValueError("Invalid included record")
                    if item.get("type") != "attachments":
                        continue
                    attributes = item.get("attributes") or {}
                    attachments.append(
                        {
                            "attachment_id": item.get("id"),
                            "title": attributes.get("title"),
                            "formats": formats(attributes.get("fileFormats")),
                            "restrict_reason": attributes.get("restrictReason"),
                            "restrict_reason_type": attributes.get("restrictReasonType"),
                        }
                    )
                row["attachments_json"] = json.dumps(None if included is None else attachments)
            return row
    else:
        raise ValueError("Source staging supports only Regulations.gov base and attribute datasets")
    return write_records(dataset, records, destination, project=mapper)


def build_native_rollup(name: str, inputs: Sequence[ReceiptInput], destination: Path, *, generation_id: str):
    """Run an existing local aggregate/link producer over qualified native inputs."""
    from spicy_regs.transforms.build_agency_stats import build_agency_stats
    from spicy_regs.transforms.build_agency_monthly_volume import build_agency_monthly_volume
    from spicy_regs.transforms.build_discovery_signals import build_discovery_signals
    from spicy_regs.transforms.build_feed_summary import build_feed_summary
    from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links

    def comment_index(work):
        import duckdb
        from spicy_regs.sources.iceberg import _build_comments_index
        from spicy_regs.schemas.regulations import COMMENT

        with duckdb.connect() as connection:
            connection.from_parquet(str(work / "comments.parquet")).create_view("native_comments_input")
            return _build_comments_index(connection, COMMENT, work, source_sql="SELECT * FROM native_comments_input")

    operations = {
        "comments_index": (comment_index, {"comments"}, {"comments"}),
        "agency_stats": (build_agency_stats, {"dockets"}, {"dockets", "documents", "comments", "comments_index"}),
        "agency_monthly_volume": (build_agency_monthly_volume, {"documents"}, {"documents"}),
        "discovery_signals": (build_discovery_signals, {"documents"}, {"documents"}),
        "feed_summary": (build_feed_summary, {"dockets"}, {"dockets", "documents", "comments", "comments_index"}),
        "fr_docket_links": (build_fr_docket_links, {"federal_register"}, {"federal_register"}),
    }
    operation, required, allowed = operations[name]
    present = {i.dataset for i in inputs}
    if not required <= present <= allowed:
        raise ValueError("Rollup inputs differ from the explicitly supported datasets")
    return build_from_receipts(
        inputs,
        destination,
        generation_id=generation_id,
        outputs=[name],
        builder=operation,
        family=name.replace("_", "-"),
    )


def merge_native_staging(
    staged: Sequence[ReceiptInput],
    destination: Path,
    *,
    generation_id: str,
    prior: ReceiptInput | None = None,
    source_correction: bool = False,
):
    """Merge qualified base-table staging with typed columns and retained witnesses.

    The existing source-recency/correction checks run in an isolated directory.
    All input generations remain pinned witnesses, including losing copies.
    """
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.transforms.merge_staging_files import merge_staging_files

    if not staged:
        raise ValueError("Native staging requires at least one selected input")
    dataset = staged[0].dataset
    if (
        dataset not in RECORD_TYPES
        or any(s.dataset != dataset for s in staged)
        or prior is not None
        and prior.dataset != dataset
    ):
        raise ValueError("Native staging inputs must name one Regulations.gov base dataset")
    record_type = RECORD_TYPES[dataset]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-staging-", dir=destination.parent) as temp:
        work = Path(temp)
        parts = work / "staging" / dataset
        parts.mkdir(parents=True)
        for i, selected in enumerate(staged):
            materialize_internal(selected, parts / f"{i}.parquet")
        if prior is not None:
            materialize_internal(prior, work / f"{dataset}.parquet")
        merge_staging_files(
            work / "staging",
            work,
            [dataset],
            {dataset: record_type.schema},
            {dataset: record_type.dedup_key},
            source_correction=source_correction,
        )
        pins = []
        for selected in [*staged, *([prior] if prior is not None else [])]:
            for source in (*selected.subjects, selected.receipts):
                with source.open("rb") as body:
                    digest = file_digest(body, "sha256").hexdigest()
                pins.append(
                    {
                        "source_id": f"{dataset}:{source.name}",
                        "source_uri": str(source),
                        "sha256": digest,
                        "locator": None,
                        "body_version": selected.generation_id,
                    }
                )
        return build_local_generation(
            {dataset: work / f"{dataset}.parquet"},
            destination,
            generation_id=generation_id,
            family=dataset,
            witnesses=pins,
            include_source_witness=False,
            prior_receipts={dataset: [prior.receipts] if prior is not None else []},
        )
