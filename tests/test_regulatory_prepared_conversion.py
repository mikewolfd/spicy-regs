"""The maintained base-family converter prepares once and publishes those exact bytes."""

from pathlib import Path

import pytest

from spicy_regs import native_conversion as conversion, regulations_bulk
from spicy_regs.sources import publication
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS
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
