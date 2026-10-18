"""The maintained base-family converter prepares once and publishes those exact bytes."""

from pathlib import Path

import duckdb
import pytest

from spicy_regs import native_conversion as conversion, regulations_bulk
from spicy_regs.sources import publication
from spicy_regs.transforms import regulations_receipts
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS
from tests.duckdb_faults import fail_streams_after
from tests.test_native_conversion import (
    BASE, BUCKET, MAIN, STATE, WHEEL, bucket as _bucket, convert, publish_old,
)

bucket = _bucket


def rows(dataset):
    identity = "docket_id" if dataset == "dockets" else "document_id"
    result = []
    for number, title in ((7, "literal  repeated  text"), (2, "")):
        value = dict.fromkeys(name for name, _ in SOURCE_COLUMNS[dataset])
        value.update({identity: f"A-{number}", "agency_code": "A", "title": title})
        result.append(value)
    return result


@pytest.mark.parametrize("dataset", ["dockets", "documents"])
@pytest.mark.parametrize("source_shape", ["current", "retained", "reordered"])
def test_regulatory_preparation_batches_and_publishes_exact_artifact_once(tmp_path, monkeypatch, bucket, dataset, source_shape):
    original_rows = rows(dataset)
    if source_shape == "retained" and dataset == "documents":
        original_rows = [{key: value for key, value in row.items()
                          if key not in {"publisher_status", "removed_observed_at"}} for row in original_rows]
    elif source_shape == "reordered":
        identity = "docket_id" if dataset == "dockets" else "document_id"
        original_rows = [{key: row[key] for key in ("title", identity)} for row in original_rows]
    old = publish_old(bucket, monkeypatch, tmp_path, dataset, {dataset: original_rows})
    calls = []
    writer = regulations_bulk.write_held_dataset

    def actual_writer(*args, **kwargs):
        calls.append(args[0])
        return writer(*args, **kwargs)

    monkeypatch.setattr(regulations_bulk, "write_held_dataset", actual_writer)
    work = tmp_path / "prepared"
    prepared = convert(dataset, work)
    assert calls == [dataset]
    assert prepared["published"] is None
    assert prepared["tables"][dataset]["retained_rows"] == len(original_rows)
    generation = Path(prepared["generation"]["directory"])
    sealed = {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError("Prepared publication must reuse the full validated generation")

    monkeypatch.setattr(conversion, "_convert_regulatory_base", no_writer)
    published = conversion.publish_prepared(
        work / conversion.RECEIPT, allowed=[dataset], expected_main=MAIN, expected_spicy_docs=WHEEL,
        expect_bucket=BUCKET, state=lambda _: dict(STATE),
    )
    assert published["generation"] == prepared["generation"]
    assert published["read_back"]["anonymous_read_rows"][dataset] == len(original_rows)
    assert {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()} == sealed
    assert calls == [dataset]
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"][dataset] == old


def test_regulatory_converter_refuses_other_family_members_before_writing(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "dockets", {"documents": rows("documents")})
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="one maintained unsplit output"):
        convert("dockets", tmp_path / "prepared", publish=True)
    assert bucket.writes == written


def test_regulatory_converter_refuses_duplicate_base_identity_without_publication(tmp_path, monkeypatch, bucket):
    value = rows("dockets")[0]
    publish_old(bucket, monkeypatch, tmp_path, "dockets", {"dockets": [value, value]})
    written = list(bucket.writes)
    with pytest.raises((ValueError, RuntimeError), match="(repeat|duplicate|Duplicate)"):
        convert("dockets", tmp_path / "prepared", publish=True)
    assert bucket.writes == written


@pytest.mark.parametrize("change", ["order", "value", "metadata"])
def test_regulatory_preparation_refuses_processing_changes_before_publication(tmp_path, monkeypatch, bucket, change):
    from spicy_regs.transforms import regulations_receipts

    publish_old(bucket, monkeypatch, tmp_path, "dockets", {"dockets": rows("dockets")})
    written = list(bucket.writes)
    intercepted = []
    if change == "metadata":
        from spicy_regs.conversion_reads import _FamilyReader
        read = _FamilyReader.read_subjects

        def changed_metadata(reader, dataset):
            for row in read(reader, dataset):
                intercepted.append(change)
                yield row | {"input_metadata": {b"changed": b"yes"}}

        monkeypatch.setattr(_FamilyReader, "read_subjects", changed_metadata)
        reason = "metadata"
    else:
        processing = regulations_receipts._processor_inputs

        def changed_processing(dataset, admitted_rows):
            restored = list(processing(dataset, admitted_rows))
            intercepted.append(change)
            if change == "order":
                restored.reverse()
            else:
                restored[0]["title"] = "changed"
            yield from restored

        monkeypatch.setattr(regulations_receipts, "_processor_inputs", changed_processing)
        reason = "values, order or repetitions"
    with pytest.raises(conversion.ConversionRefused, match=f"dockets does not restore.*{reason}"):
        convert("dockets", tmp_path / "prepared", publish=True)
    assert intercepted and set(intercepted) == {change}
    assert bucket.writes == written


@pytest.mark.parametrize("failure, code, report", [
    (duckdb.InvalidInputException("injected"), 1, "REFUSED: dockets: "),
    (duckdb.OutOfMemoryException("injected"), 2, "FAILED: BulkValidationError: "),
], ids=["invalid-input", "operational"])
def test_a_regulatory_bulk_failure_is_reported_by_its_kind_and_never_redone_by_row(
        tmp_path, monkeypatch, bucket, capsys, failure, code, report):
    """Invalid input refuses (exit 1) and an operational failure does not (exit 2); nothing is rewritten by row."""
    publish_old(bucket, monkeypatch, tmp_path, "dockets", {"dockets": rows("dockets")})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    written = list(bucket.writes)

    def row_path(*args, **kwargs):
        pytest.fail("a bulk failure was retried by row")

    monkeypatch.setattr(regulations_receipts, "write_records", row_path)
    monkeypatch.setattr(regulations_bulk, "_BATCH", 1)
    delivered = fail_streams_after(monkeypatch, failure)
    arguments = [
        "dockets", "--allow", "dockets", "--expect-main", MAIN, "--expect-spicy-docs", WHEEL, "--remote", "fork",
        "--work", str(tmp_path / "work"), "--publish", "--expect-bucket", BUCKET,
    ]
    assert conversion.main(arguments) == code
    error = capsys.readouterr().err
    assert report in error and "Bulk dockets write failed; no row fallback: " in error
    assert f"STATE: {conversion.NOTHING_PUBLISHED}" in error
    assert delivered == [1] and bucket.writes == written


@pytest.mark.parametrize("dataset,identity", [("docket_attributes", "docket_id"), ("document_attributes", "document_id")])
def test_attribute_families_convert_from_their_typed_published_shape(tmp_path, monkeypatch, bucket, dataset, identity):
    """Nightly etl-new restores each attribute table and refuses one that carries no receipts (since 2026-10-04)."""
    import json
    from datetime import date, datetime, timezone

    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation
    from spicy_regs.transforms.regulations_receipts import TYPES

    family = dataset.replace("_", "-")
    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[dataset]])
    # One non-null value of every declared kind, so a value that changed in conversion would be seen.
    samples = {
        "BIGINT": lambda n: n, "INTEGER": lambda n: n, "DOUBLE": lambda n: n + 0.5, "BOOLEAN": lambda n: n % 2 == 1,
        "DATE": lambda n: date(2026, 1, n),
        "TIMESTAMP WITH TIME ZONE": lambda n: datetime(2026, 1, n, 12, 30, tzinfo=timezone.utc),
        "VARCHAR[]": lambda n: [f"k{n}", f"j{n}"], "VARCHAR": lambda n: f"v{n}",
    }
    original = [{name: json.dumps([{"name": f"n{number}", "label": "L", "tooltip": "T"}]) if name.endswith("_json") else samples[kind](number)
                 for name, kind in SOURCE_COLUMNS[dataset]} | {identity: f"A-{number}"}
                for number in (1, 2)]
    path = tmp_path / "old" / f"{dataset}.parquet"
    path.parent.mkdir()
    pq.write_table(pa.Table.from_pylist(original, schema=schema), path)
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(tmp_path / "generation", family=family, files=[path], expected_keys=[path.name])
        publication.publish_generation(tmp_path / "generation", client=bucket, bucket=BUCKET,
                                       prior_index=publication.current_index(BASE))
    receipt = convert(family, tmp_path / "work", publish=True)
    assert receipt["read_back"]["anonymous_read_rows"][dataset] == 2
    assert dataset in publication.current_index(BASE)["families"][family]["etlReceipts"]["datasets"]
