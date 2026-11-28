"""Plain-Parquet scorecard family persistence, readback verification, and policy admission."""

from copy import deepcopy
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.receipt_switch import receipt_files
from spicy_regs.scorecards.etl import (
    POLICIES,
    SOURCE_NAMES,
    admitted_read_policies,
    generation_schemas,
    read_family,
    verify_family_readback,
    write_family,
)
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import combine, edition, evidence, provider, run, tables


def test_native_generation_keeps_domain_subjects_without_receipts(tmp_path):
    before = tables(edition("2025"))
    directory = tmp_path / "candidate"
    files = write_family(directory, before)
    assert read_family(directory, SOURCE_NAMES, include_omitted=True) == {
        name: [
            {k: (None if name == "scorecard_publishers" and k == "observed_at" else v) for k, v in row.items()}
            for row in rows
        ]
        for name, rows in before.items()
    }
    assert not list(directory.glob(".scorecard-etl-*"))
    assert receipt_files(directory) == []
    assert (directory / "scorecard_snapshots.parquet").exists()
    for path in files:
        names = pq.read_schema(path).names
        assert {"capture_id", "source_url", "source_path"} <= set(names)
    target = tmp_path / "generation"
    artifact = build_generation(
        target,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        schemas=generation_schemas(SOURCE_NAMES),
    )
    admitted = verify_generation(target)
    assert admitted.pin == artifact.pin
    assert "etlReceipts" not in admitted.root["spec"]


def test_conversion_refusal_does_not_replace_prior_family(tmp_path):
    directory = tmp_path / "candidate"
    write_family(directory, tables(edition("2025")))
    previous = {path.name: path.read_bytes() for path in directory.iterdir()}
    bad = tables(edition("2026"))
    bad["scorecard_member_ratings"][0]["value_number"] = "NaN"
    with pytest.raises(ValueError, match="conversion refused"):
        write_family(directory, bad)
    assert {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()} == previous
    assert not list(directory.glob(".scorecard-etl-*"))


def test_exact_readback_visits_shuffled_rows_without_materializing(tmp_path, monkeypatch):
    before = combine(tables(edition("2025")), tables(edition("2024")))
    directory = tmp_path / "candidate"
    write_family(directory, {name: list(reversed(rows)) for name, rows in before.items()})
    original = pq.ParquetFile

    class StreamedOnly:
        def __init__(self, *args, **kwargs):
            self._inner = original(*args, **kwargs)

        def __getattr__(self, item):
            return getattr(self._inner, item)

        def __enter__(self):
            self._inner.__enter__()
            return self

        def __exit__(self, *args):
            return self._inner.__exit__(*args)

        def read(self, *args, **kwargs):
            pytest.fail("Readback must stream batches, never eagerly call ParquetFile.read()")

    monkeypatch.setattr(pq, "ParquetFile", StreamedOnly)
    counts = verify_family_readback(directory, before)
    assert counts == {name: len(rows) for name, rows in before.items()}
    assert counts["scorecard_snapshots"] == 2


@pytest.mark.parametrize("change", ["capture", "literal", "null", "type", "missing", "additional", "duplicate"])
def test_exact_readback_refuses_source_changes(tmp_path, change):
    before = tables(edition("2025"))
    directory = tmp_path / "candidate"
    write_family(directory, before)
    expected = deepcopy(before)
    rows = expected["scorecard_member_ratings"]
    if change == "capture":
        rows[0]["capture_id"] = "a different original capture"
    elif change == "literal":
        rows[0]["value_text"] += " "
    elif change == "null":
        rows[0]["rank_text"] = "" if rows[0]["rank_text"] is None else None
    elif change == "type":
        rows[0]["value_number"] = 80
    elif change == "missing":
        rows.pop()
    elif change == "additional":
        rows.append(dict(rows[0], publisher_member_key="absent-member"))
    else:
        rows.append(deepcopy(rows[0]))
    with pytest.raises(ValueError):
        verify_family_readback(directory, expected)


def test_duplicate_source_identity_refuses(tmp_path):
    before = tables(edition("2025"))
    before["scorecard_snapshots"].append(deepcopy(before["scorecard_snapshots"][0]))
    with pytest.raises(ValueError, match="Duplicate or ambiguous"):
        write_family(tmp_path / "candidate", before)


@pytest.mark.parametrize("corruption", ["schema", "missing"])
def test_corrupt_or_missing_parquet_refuses_read(tmp_path, corruption):
    directory = tmp_path / "candidate"
    write_family(directory, tables(edition("2025")))
    if corruption == "schema":
        path = directory / "scorecards.parquet"
        pq.write_table(pq.read_table(path).drop(["title"]), path)
    else:
        (directory / "scorecards.parquet").unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        read_family(directory, SOURCE_NAMES)


def test_plain_prior_preserves_unselected_scope_without_receipts(tmp_path, monkeypatch):
    old = combine(tables(edition("2025")), tables(edition("2024")))
    first = tmp_path / "first"
    files = write_family(first, old)
    generation = tmp_path / "generation"
    build_generation(
        generation,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        schemas=generation_schemas(SOURCE_NAMES),
    )
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    second = tmp_path / "second"
    second.mkdir()
    e = evidence(second)
    e.read_snapshot = index

    def download(key, target):
        return fetch(None, pub.single_member(index, key), target)

    selected = edition("2025")
    run(second, provider([selected], {selected.scorecard_id: tables}), e, download_prior=download)
    assert receipt_files(second) == []
    actual = read_family(second / "build", SOURCE_NAMES, include_omitted=True)
    for name, rows in actual.items():
        if name != "scorecard_publishers":
            assert [r for r in rows if r["scorecard_id"] == "lcv:2024"] == [
                r for r in old[name] if r["scorecard_id"] == "lcv:2024"
            ]


def test_conversion_failure_leaves_candidate_directory_clean(tmp_path):
    raw = tables(edition("2025"))
    raw["scorecard_member_ratings"][0]["value_number"] = "NaN"
    with pytest.raises(ValueError, match="conversion refused"):
        write_family(tmp_path / "candidate", raw)
    assert not list((tmp_path / "candidate").glob("*.parquet"))
    assert receipt_files(tmp_path / "candidate") == []


@pytest.mark.parametrize("late_error", [False, True])
def test_streamed_multibatch_family_handles_batches_and_late_error(tmp_path, late_error):
    base = tables(edition("2025"))["scorecard_member_ratings"][0]

    def source_rows():
        for index in range(4001):
            yield dict(base, publisher_member_key=f"member-{index}")
        if late_error:
            yield dict(base, publisher_member_key="invalid-member", value_number="1e2")

    candidate = tmp_path / "candidate"
    raw = {"scorecard_member_ratings": source_rows()}
    if late_error:
        with pytest.raises(ValueError, match="conversion refused"):
            write_family(candidate, raw)
        assert not list(candidate.glob("*.parquet"))
    else:
        write_family(candidate, raw)
        assert read_family(candidate, ("scorecard_member_ratings",)) == {
            "scorecard_member_ratings": [dict(base, publisher_member_key=f"member-{index}") for index in range(4001)]
        }


def test_installed_scorecard_policy_accepts_receiptless_and_refuses_receipts(tmp_path):
    from spicy_regs.etl_policy_registry import installed_policies, require_registered_receipts

    installed = installed_policies()
    assert all(installed[name].descriptor() == pol.descriptor() for name, pol in POLICIES.items())
    files = write_family(tmp_path / "candidate", tables(edition("2025")))
    artifact = build_generation(
        tmp_path / "accepted",
        family="scorecards",
        files=files,
        expected_keys=[p.name for p in files],
        schemas=generation_schemas(SOURCE_NAMES),
    )
    assert "etlReceipts" not in artifact.root["spec"]
    with pytest.raises(ValueError, match="Receipts are retired"):
        require_registered_receipts(
            {"scorecards.parquet": {}}, {"policies": [POLICIES["scorecards"].descriptor()]}
        )


@pytest.mark.parametrize("change", ["version", "schema", "classification", "duplicate"])
def test_historical_policy_selection_refuses_unknown_declarations(change):
    descriptors = [p.descriptor() for name, p in POLICIES.items() if name in SOURCE_NAMES]
    rating = next(p for p in descriptors if p["dataset"] == "scorecard_member_ratings")
    if change == "version":
        rating["policy_version"] = "unreviewed"
    elif change == "schema":
        schema = POLICIES["scorecard_member_ratings"].subject_schema
        schema = schema.set(
            schema.get_field_index("value_number"), schema.field("value_number").with_type(pa.decimal128(38, 20))
        )
        rating.update(replace(POLICIES["scorecard_member_ratings"], subject_schema=schema).descriptor())
    elif change == "classification":
        rating["receipt_fields"].remove("raw_source")
    else:
        descriptors[-1] = descriptors[0]
    with pytest.raises(ValueError, match="supported exact"):
        admitted_read_policies(SOURCE_NAMES, descriptors=descriptors)


@pytest.mark.parametrize("change", ["schema", "classification", "identity"])
def test_shared_historical_admission_requires_exact_current_descriptor(change):
    from spicy_regs.policy_migrations import earlier_policies

    current = POLICIES["scorecard_member_ratings"]
    if change == "schema":
        field = current.subject_schema.field("value_number")
        changed = replace(
            current,
            subject_schema=current.subject_schema.set(
                current.subject_schema.get_field_index("value_number"), field.with_type(pa.decimal128(38, 20))
            ),
        )
    elif change == "classification":
        changed = replace(current, receipt_fields=tuple(f for f in current.receipt_fields if f != "raw_source"))
    else:
        changed = replace(current, identity_fields=tuple(reversed(current.identity_fields)))
    assert earlier_policies(current) and earlier_policies(changed) == ()
