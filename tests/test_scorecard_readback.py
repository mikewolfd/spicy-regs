"""Reuse fully verified scorecard rows synchronously; corrupt or late reads refuse."""

from copy import deepcopy
import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family, write_family
from spicy_regs.scorecards.retained import check_preserved
from spicy_regs.sources import publication as pub
from spicy_regs.transforms import build_scorecards as transform
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import combine, edition, evidence, journal, prior, provider, run, tables


def native_prior(tmp_path, monkeypatch, selected_evidence):
    old = combine(tables(edition("2024")), tables(edition("2025")))
    directory = tmp_path / "old"
    files = write_family(directory, old)
    generation = tmp_path / "old-generation"
    build_generation(generation, family="scorecards", files=files, expected_keys=tuple(p.name for p in files),
                     **generation_options(directory, SOURCE_NAMES))
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())
    selected_evidence.read_snapshot = index

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    return old, lambda key, target: fetch(None, pub.single_member(index, key), target), lambda target: fetch(
        None, pub.receipt_members(index, dataset="scorecards")[0], target
    )


@pytest.mark.parametrize("native", [False, True])
def test_readback_validator_gets_exact_prior_and_persisted_rows_after_validation(tmp_path, monkeypatch, native):
    selected = edition("2025")
    e = evidence(tmp_path)
    options = {}
    if native:
        old, download, receipt_download = native_prior(tmp_path, monkeypatch, e)
        options["download_prior_receipts"] = receipt_download
        # Native admission must use the receipt reader, never eager raw subject
        # materialization. The wrapper still permits every real streamed read.
        original = pq.ParquetFile

        class StreamedOnly:
            def __init__(self, *args, **kwargs):
                self.value = original(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(self.value, name)

            def __enter__(self):
                self.value.__enter__()
                return self

            def __exit__(self, *args):
                return self.value.__exit__(*args)

            def read(self, *args, **kwargs):
                pytest.fail("Native subject eagerly materialized instead of receipt-checked reconstruction")

        monkeypatch.setattr(pq, "ParquetFile", StreamedOnly)
    else:
        old = combine(tables(edition("2024")), tables(edition("2025")))
        download = prior(e, old, tmp_path)
    p = provider([selected], {selected.scorecard_id: tables})
    validated = []
    original_validate = p.validate

    def validate(rows):
        original_validate(rows)
        validated.append(rows)

    p.validate = validate
    calls = []

    def assess(previous, current):
        assert validated[-1] is current
        assert previous == old
        assert current == read_family(tmp_path / "build", SOURCE_NAMES)
        assert not any(row["event"] == "scorecard-refresh" for row in journal(e))
        counts = check_preserved(previous, current, scorecard_ids={selected.scorecard_id}, publisher_ids={"lcv"})
        assert counts["scorecard_member_ratings"] == 2
        calls.append((previous, current))

    run(tmp_path, p, e, download_prior=download, validate_readback=assess, **options)
    assert len(calls) == 1
    assert any(row["event"] == "scorecard-refresh" for row in journal(e))


def test_readback_validator_failure_aborts_before_success(tmp_path):
    selected = edition("2025")
    e = evidence(tmp_path)
    calls = []

    def reject(previous, current):
        calls.append((previous, current))
        raise ValueError("Preservation assessment refused")

    with pytest.raises(ValueError, match="assessment refused"):
        run(tmp_path, provider([selected], {selected.scorecard_id: tables}), e,
            download_prior=lambda *args: False, validate_readback=reject)
    assert len(calls) == 1
    assert not any(row["event"] == "scorecard-refresh" for row in journal(e))


@pytest.mark.parametrize("corruption", ["subject", "receipt", "generation"])
def test_bad_persisted_family_never_reaches_readback_validator(tmp_path, monkeypatch, corruption):
    original_write = transform.write_family

    def corrupt(directory, *args, **kwargs):
        result = original_write(directory, *args, **kwargs)
        if corruption == "generation":
            state = directory / "scorecard-etl-build.json"
            value = json.loads(state.read_text())
            value["generation_id"] = "unselected-generation"
            state.write_text(json.dumps(value))
        else:
            path = directory / ("scorecards.parquet" if corruption == "subject" else "etl_receipts.parquet")
            arrow = pq.read_table(path)
            rows = arrow.to_pylist()
            rows[0]["title" if corruption == "subject" else "processor"] = "source corruption"
            pq.write_table(pa.Table.from_pylist(rows, schema=arrow.schema), path)
        return result

    monkeypatch.setattr(transform, "write_family", corrupt)
    called = []
    selected = edition("2025")
    e = evidence(tmp_path)
    with pytest.raises(ValueError):
        run(tmp_path, provider([selected], {selected.scorecard_id: tables}), e,
            download_prior=lambda *args: False, validate_readback=lambda *rows: called.append(rows))
    assert called == []
    assert not any(row["event"] == "scorecard-refresh" for row in journal(e))


def test_prior_pin_corruption_refuses_before_callback(tmp_path, monkeypatch):
    e = evidence(tmp_path)
    _, download, receipts = native_prior(tmp_path, monkeypatch, e)

    def changed(key, target):
        download(key, target)
        if key == "scorecards.parquet":
            target.write_bytes(target.read_bytes() + b"source drift")
        return True

    selected = edition("2025")
    called = []
    with pytest.raises(transform.ScorecardRefreshError, match="immutable pin"):
        run(tmp_path, provider([selected], {selected.scorecard_id: tables}), e,
            download_prior=changed, download_prior_receipts=receipts,
            validate_readback=lambda *rows: called.append(rows))
    assert called == []


def preparation_fixture(tmp_path, monkeypatch, *, fault=None):
    from hashlib import sha256
    from tests.test_scorecard_prepare import PREPARE

    selected = edition("2025")
    previous = combine(tables(edition("2024")), tables(selected))
    current = combine(
        {name: [row for row in rows if name == "scorecard_publishers" or row["scorecard_id"] == "lcv:2024"]
         for name, rows in previous.items()}, tables(selected)
    )
    fake = SimpleNamespace(entries={selected.scorecard_id: dict(edition_object=selected)},
                           reference_publishers={}, complete=lambda: None)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(dict(schema_version="1", entries=[dict(adapter="lcv", edition=dict(publisher_id="lcv"))])))
    index = tmp_path / "index.json"
    index.write_text(json.dumps(pub.empty_index()))
    monkeypatch.setattr(PREPARE, "RetainedScorecardBatch", lambda *args, **kwargs: fake)
    monkeypatch.setattr(PREPARE, "installed_provider", lambda: SimpleNamespace(get_adapter=lambda name: None))
    monkeypatch.setattr(PREPARE, "generation_options", lambda *args: {})
    calls = []

    def build(directory, *, validate_readback, validate_acquisitions, **options):
        validate_acquisitions()
        # The real retained batch populates these observations during acquire,
        # rather than at construction. Read them only at the verified boundary.
        fake.reference_publishers["lcv"] = current["scorecard_publishers"][0]
        altered = deepcopy(current)
        if fault == "unselected-scope":
            altered["scorecard_member_ratings"][0]["value_text"] = "changed prior row"
        elif fault == "publisher":
            altered["scorecard_publishers"][0]["name"] = "not the qualified publisher"
        if fault != "missing-assessment":
            validate_readback(previous, altered)
        return ()

    def generation(*args, **kwargs):
        calls.append("generation")
        raise RuntimeError("verified assessment reached copied-generation boundary")

    monkeypatch.setattr(PREPARE, "build_scorecards", build)
    monkeypatch.setattr(PREPARE, "build_generation", generation)
    return PREPARE, SimpleNamespace(output=tmp_path / "candidate", private_observations=tmp_path / "private",
        plan=plan_path, plan_sha256=sha256(plan_path.read_bytes()).hexdigest(), index=index,
        corpus=tmp_path, public_url="https://data.example"), calls


def test_preparation_assesses_verified_rows_before_separate_generation_admission(tmp_path, monkeypatch):
    prepare, args, calls = preparation_fixture(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="copied-generation boundary"):
        prepare.prepare(args)
    assert calls == ["generation"]


@pytest.mark.parametrize("fault", ["unselected-scope", "publisher", "missing-assessment"])
def test_preparation_preservation_reference_and_missing_assessment_refuse_before_generation(tmp_path, monkeypatch, fault):
    prepare, args, calls = preparation_fixture(tmp_path, monkeypatch, fault=fault)
    with pytest.raises(ValueError):
        prepare.prepare(args)
    assert calls == []


def test_complete_preparation_uses_late_publisher_reference_and_real_generation_checks(tmp_path, monkeypatch):
    from hashlib import sha256
    from tests.test_scorecard_prepare import PREPARE
    from spicy_regs.generations import verify_generation

    selected = edition("2025")
    p = provider([selected], {selected.scorecard_id: tables})
    adapter = p.get_adapter("lcv")

    class Batch:
        parser_version = adapter.parser_version
        entries = {selected.scorecard_id: dict(edition_object=selected)}

        def __init__(self):
            self.reference_publishers = {}
            self.acquired = False

        def list_scorecards(self, context):
            return adapter.list_scorecards(context)

        def acquire_scorecard(self, edition, context):
            result = adapter.acquire_scorecard(edition, context)
            self.reference_publishers["lcv"] = result.tables["scorecard_publishers"][0]
            self.acquired = True
            return result

        def complete(self):
            assert self.acquired

    batch = Batch()
    monkeypatch.setattr(PREPARE, "RetainedScorecardBatch", lambda *args, **kwargs: batch)
    monkeypatch.setattr(PREPARE, "installed_provider", lambda: p)
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps(dict(schema_version="1", entries=[dict(adapter="lcv", edition=dict(publisher_id="lcv"))])))
    index = tmp_path / "index.json"
    index.write_text(json.dumps(pub.empty_index()))
    args = SimpleNamespace(output=tmp_path / "candidate", private_observations=tmp_path / "private",
        plan=plan, plan_sha256=sha256(plan.read_bytes()).hexdigest(), index=index,
        corpus=tmp_path, public_url="https://offline.example")
    report = PREPARE.prepare(args)
    assert report["status"] == "prepared_not_published"
    assert report["counts"]["scorecard_members"] == 2
    assert report["counts"]["scorecard_member_ratings"] == 2
    assert report["publisher_or_model_bodies_in_public_evidence"] == 0
    assert report["source_network_requests"] == 0
    assert all(count == 0 for count in report["preserved_row_counts"].values())
    generation = verify_generation(args.output / "generation")
    assert generation.pin.as_dict() == report["generation"]
