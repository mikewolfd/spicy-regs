"""Actual maintained vote producers prepare once and reuse the qualified artifact."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import native_conversion as conversion
from spicy_regs.congress_subjects import INPUT_COLUMNS
from spicy_regs.pipelines.rollups import subject_receipts
from spicy_regs.sources import publication
from tests.test_native_conversion import (
    BASE, BUCKET, MAIN, STATE, WHEEL, bucket as _bucket, convert, publish_old,
)

bucket = _bucket


def rows(dataset):
    values = []
    for number in (7, 2):
        value: dict[str, str | None] = dict.fromkeys(INPUT_COLUMNS[dataset])
        value.update(vote_id=f"119-house-1-{number}", chamber="house")
        if dataset == "member_vote_terms":
            value.update(member_key=f"bioguide:X{number}", bioguide_id=f"X{number}", vote_day="2025-01-23",
                         term_match="half_open", term_index="0", term_start="2025-01-03", term_end="2027-01-03")
        else:
            value.update(congress="119", session="1")
            if dataset == "member_votes":
                value.update(member_key=f"bioguide:X{number}", bioguide_id=f"X{number}",
                             member_name="literal  repeated  name", position="Yea")
            else:
                value.update(tallies_json='{"Yea": 2}', roll_number=str(number))
        values.append(value)
    return values


@pytest.mark.parametrize("family,datasets", [
    ("roll-call-votes", ("roll_call_votes", "member_votes")),
    ("member-vote-terms", ("member_vote_terms",)),
])
def test_maintained_vote_preparation_and_publication_reuse(tmp_path, monkeypatch, bucket, family, datasets):
    original = {dataset: rows(dataset) for dataset in datasets}
    old = publish_old(bucket, monkeypatch, tmp_path, family, original)
    calls, writer = [], subject_receipts.write_congress_dataset

    def actual_writer(*args, **kwargs):
        calls.append((kwargs["dataset"], kwargs.get("bulk", False)))
        return writer(*args, **kwargs)

    monkeypatch.setattr(subject_receipts, "write_congress_dataset", actual_writer)
    work = tmp_path / "prepared"
    prepared = convert(family, work)
    assert calls == [(dataset, True) for dataset in datasets]
    generation = Path(prepared["generation"]["directory"])
    sealed = {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError("Prepared publication must reuse the admitted vote generation")

    monkeypatch.setattr(conversion, "_convert_rollup", no_writer)
    published = conversion.publish_prepared(
        work / conversion.RECEIPT, allowed=[family], expected_main=MAIN, expected_spicy_docs=WHEEL,
        expect_bucket=BUCKET, state=lambda _: dict(STATE),
    )
    assert published["generation"] == prepared["generation"]
    assert published["read_back"]["anonymous_read_rows"] == {name: len(values) for name, values in original.items()}
    assert {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()} == sealed
    assert calls == [(dataset, True) for dataset in datasets]
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"][family] == old


@pytest.mark.parametrize("change", ["order", "value", "metadata"])
def test_vote_preparation_refuses_changed_restored_processing(tmp_path, monkeypatch, bucket, change):
    old = publish_old(bucket, monkeypatch, tmp_path, "member-vote-terms", {"member_vote_terms": rows("member_vote_terms")})
    written = list(bucket.writes)
    from spicy_regs.conversion_reads import _FamilyReader
    compare = _FamilyReader.compare_original
    intercepted = []

    def changed_processing(reader, dataset, original, **kwargs):
        intercepted.append(change)
        table = pq.read_table(original)
        if change == "order":
            table = table.take(pa.array([1, 0]))
        elif change == "value":
            index = table.schema.get_field_index("vote_day")
            table = table.set_column(index, table.schema.field(index), pa.array(["2025-01-24", "2025-01-23"]))
        else:
            table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"changed": b"yes"})
        # Match the maintained native-conversion corruption controls: change
        # real source bytes, then execute the actual streamed comparison.
        pq.write_table(table, original)
        return compare(reader, dataset, original, **kwargs)

    monkeypatch.setattr(_FamilyReader, "compare_original", changed_processing)
    reason = "schema or metadata" if change == "metadata" else "values, order or repetitions"
    with pytest.raises(conversion.ConversionRefused, match=f"member_vote_terms does not restore.*{reason}") as refusal:
        convert("member-vote-terms", tmp_path / "prepared", publish=True)
    assert intercepted == [change]
    assert refusal.value.state == conversion.NOTHING_PUBLISHED
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"]["member-vote-terms"] == old
