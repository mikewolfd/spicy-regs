"""Whole-edition replacement under success, failure and immutable prior pins."""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs.pipelines.rollups.scorecards import ScorecardsRollup
from spicy_regs.scorecards.etl import SOURCE_NAMES, read_family
from spicy_regs.scorecards.registry import (
    CADENCES, CADENCE_TOLERANCES, LIVE_QUALIFICATIONS, REGISTRY, RegistryError, load_registry, select_sources,
)
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.transforms.build_scorecards import NoScorecardsDue, ScorecardRefreshError, build_scorecards
from tests.test_source_evidence import capture, journal

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/scorecards"
SCHEMA = json.loads((FIXTURES / "proposed_schema.json").read_text())
SPEC = importlib.util.spec_from_file_location("scorecard_gate", FIXTURES / "validate_gate.py")
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
NOW = datetime(2026, 10, 3, tzinfo=UTC)


class Refused(ValueError):
    pass


def edition(key, publisher="lcv", current=True):
    return SimpleNamespace(
        scorecard_id=publisher + ":" + key, publisher_id=publisher, edition_id=key, chamber="both", is_current=current
    )


def tables(edition, capture_id="fixture-capture", members=2, items=True):
    result = {t["name"]: [] for t in SCHEMA["tables"]}
    definitions = {t["name"]: t for t in SCHEMA["tables"]}
    snap = "snapshot:" + uuid4().hex

    def add(table_name, **values):
        row = dict.fromkeys(definitions[table_name]["fields"])
        defaults = dict(
            scorecard_id=edition.scorecard_id,
            snapshot_id=snap,
            capture_id=capture_id,
            source_url="https://source.test/body",
            source_path="synthetic fixture",
        )
        row.update({k: v for k, v in defaults.items() if k in row})
        row.update(values)
        result[table_name].append(row)

    add(
        "scorecard_publishers",
        publisher_id=edition.publisher_id,
        name="Synthetic publisher",
        observed_at=NOW.isoformat(),
    )
    add("scorecards", publisher_id=edition.publisher_id, chamber_scope_text="both")
    add(
        "scorecard_snapshots",
        capture_ids_json=json.dumps([capture_id], separators=(",", ":")),
        capture_roles_json=json.dumps([dict(capture_id=capture_id, role="primary", field_groups=["all"])], separators=(",", ":")),
        parser_version="test-v1",
        identity_rule_version="test-v1",
        rendition_selection_rule="one synthetic rendition",
        completeness_rule="Synthetic complete member table",
        completeness_status="complete",
        evidence_policy="hash_only",
    )
    add("scorecard_metrics", metric_id="annual", name="Annual")
    if items:
        add("scorecard_items", item_id="item", title="Literal source item")
    for index in range(members):
        key = str(index)
        add("scorecard_members", publisher_member_key=key, member_name="Synthetic " + key)
        add("scorecard_member_ratings", publisher_member_key=key, metric_id="annual", value_text="A+")
        if items:
            add(
                "scorecard_member_item_results",
                publisher_member_key=key,
                item_id="item",
                result_id="cell",
                result_text="✓",
            )
    return result


def provider(listed, outcomes):
    def validate(rows):
        try:
            GATE.validate_bundle(SCHEMA, rows)
        except ValueError as error:
            raise Refused(str(error)) from error

    contracts = {}
    for table in SCHEMA["tables"]:
        key = tuple(table["key"])
        contracts[table["name"]] = SimpleNamespace(
            columns=tuple(table["fields"]), identity=key, key=lambda row, key=key: tuple(row[k] for k in key)
        )

    class Adapter:
        parser_version = "test-v1"

        def list_scorecards(self, context):
            return listed

        def acquire_scorecard(self, selected, context):
            value = outcomes[selected.scorecard_id]
            if isinstance(value, Exception):
                raise value
            receipt = context.capture(capture(b"PRIVATE SOURCE PAYLOAD"), stage="test")
            rows = value(selected, receipt["capture_id"])
            return SimpleNamespace(
                scorecard_id=selected.scorecard_id,
                snapshot_id=rows["scorecard_snapshots"][0]["snapshot_id"],
                tables=rows,
                capture_ids=(receipt["capture_id"],),
                completeness_rule="Synthetic complete member table",
            )

    return SimpleNamespace(
        contracts=contracts,
        get_adapter=lambda name: Adapter(),
        validate=validate,
        context=lambda **kwargs: SimpleNamespace(**kwargs),
        refusal=Refused,
    )


@contextmanager
def no_fetch(scope):
    yield lambda url: pytest.fail("Synthetic provider must not fetch")


def registry(tmp_path, *, historical=False):
    source = deepcopy(yaml.safe_load(REGISTRY.read_text())["sources"][0])
    source.update(enabled=True, qualification_id="synthetic-qualified-v1", historical_backfill=historical)
    path = tmp_path / "sources.yaml"
    path.write_text(yaml.safe_dump(dict(version=1, sources=[source])))
    return path


def test_registry_publisher_mapping_comes_from_installed_readers(tmp_path):
    from spicy_docs.sources.scorecards import ADAPTER_PUBLISHERS

    sources = load_registry()
    assert {source.publisher_id for source in sources} == set(ADAPTER_PUBLISHERS.values())
    assert all(ADAPTER_PUBLISHERS[source.adapter] == source.publisher_id for source in sources)
    c4ip = next(source for source in sources if source.publisher_id == "c4ip")
    assert c4ip.adapter == "c4ip_api"
    path = registry(tmp_path)
    document = yaml.safe_load(path.read_text())
    document["sources"][0]["adapter"] = "c4ip_api"
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(RegistryError, match="configuration"):
        load_registry(path)


def evidence(tmp_path):
    return CaptureEvidence(tmp_path / "output", "scorecards")


def prior(evidence, rows, tmp_path):
    files, descriptors = {}, {}
    for name, values in rows.items():
        schema = pa.schema([(t, pa.string()) for t in next(d["fields"] for d in SCHEMA["tables"] if d["name"] == name)])
        path = tmp_path / (name + ".parquet")
        pq.write_table(pa.Table.from_pylist(values, schema=schema), path)
        files[path.name] = path
        descriptors[path.name] = dict(
            sha256="sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(), byteSize=path.stat().st_size
        )
    evidence.read_snapshot = dict(families=dict(scorecards=dict(tables=descriptors)))

    def download(key, into):
        into.write_bytes(files[key].read_bytes())
        return True

    return download


def combine(*bundles):
    result = {name: [] for name in bundles[0]}
    for bundle in bundles:
        for name, rows in bundle.items():
            if name != "scorecard_publishers":
                result[name].extend(rows)
    result["scorecard_publishers"] = bundles[0]["scorecard_publishers"]
    return result


def run(tmp_path, provider, evidence, **kwargs):
    return build_scorecards(
        tmp_path / "build",
        evidence=evidence,
        registry=registry(tmp_path),
        provider=provider,
        fetch_factory=no_fetch,
        now=NOW,
        **kwargs,
    )


def test_pdf_host_injection_stays_outside_public_evidence(tmp_path):
    selected = edition("2025")
    p = provider([selected], {selected.scorecard_id: tables})
    extractor = object()

    def retain(*args, **kwargs):
        return {"extraction_id": str(uuid4())}

    contexts = []

    def context(**kwargs):
        contexts.append(kwargs)
        return SimpleNamespace(**kwargs)

    p.context = context
    e = evidence(tmp_path)
    run(tmp_path, p, e, pdf_extractor=extractor, retain_extraction=retain, download_prior=lambda *args: False)
    assert contexts[0]["pdf_extractor"] is extractor
    assert contexts[0]["retain_extraction"] is retain
    public = json.dumps(journal(e))
    assert "pdf_extractor" not in public
    assert "retain_extraction" not in public
    assert "PRIVATE SOURCE PAYLOAD" not in public


def test_edition_reader_records_its_own_parser_version(tmp_path):
    selected = edition("2025")
    p = provider([selected], {selected.scorecard_id: tables})
    reader = p.get_adapter("lcv")
    original = reader.acquire_scorecard

    def acquire(selected, context):
        result = original(selected, context)
        result.tables["scorecard_snapshots"][0]["parser_version"] = "edition-v2"
        return result

    reader.acquire_scorecard = acquire
    reader.parser_version = "edition-v2"
    p.get_adapter = lambda name: SimpleNamespace(
        parser_version="publisher-selector-v1",
        list_scorecards=lambda context: [selected],
        for_edition=lambda edition: reader,
    )
    e = evidence(tmp_path)
    run(tmp_path, p, e, download_prior=lambda *args: False)
    captures = [row for row in journal(e) if row["event"] == "capture"]
    assert captures and {row["parser_version"] for row in captures} == {"publisher-selector-v1"}
    selected_reader = next(row for row in journal(e) if row["event"] == "scorecard-edition-reader")
    assert selected_reader["reader_parser_version"] == "edition-v2"
    assert selected_reader["scorecard_id"] == selected.scorecard_id
    current = read_family(tmp_path / "build", SOURCE_NAMES, include_omitted=True)
    assert current["scorecard_snapshots"][0]["parser_version"] == "edition-v2"


@pytest.mark.parametrize("options", [{"pdf_extractor": object()}, {"retain_extraction": lambda: None}])
def test_pdf_host_requires_extraction_and_private_retention_together(tmp_path, options):
    with pytest.raises(ScorecardRefreshError, match="both an extractor and private observation retention"):
        build_scorecards(tmp_path / "build", evidence=evidence(tmp_path), **options)


@pytest.mark.parametrize("failure", ["404", "500", "empty", "layout drift", "parse failure"])
def test_mixed_success_replaces_whole_scope_and_retains_failed_edition(tmp_path, failure):
    a, b = edition("2025"), edition("2024")
    old_a, old_b = tables(a), tables(b)
    old = combine(old_a, old_b)
    e = evidence(tmp_path)
    download = prior(e, old, tmp_path)
    p = provider(
        [a, b],
        {
            a.scorecard_id: lambda edition, cap: tables(edition, cap, members=1, items=False),
            b.scorecard_id: Refused(failure),
        },
    )
    paths = run(tmp_path, p, e, download_prior=download)
    result = read_family(tmp_path / "build", SOURCE_NAMES, include_omitted=True)
    for name, rows in result.items():
        if name == "scorecard_publishers":
            continue
        assert [r for r in rows if r["scorecard_id"] == b.scorecard_id] == old_b[name]
    assert not [r for r in result["scorecard_items"] if r["scorecard_id"] == a.scorecard_id]
    assert len(result["scorecard_member_ratings"]) == 3
    assert len(paths) == len(SCHEMA["tables"])
    assert any(p.stem == "scorecard_snapshots" for p in paths)
    assert any(r["event"] == "rows-retired" and r["rows"] for r in journal(e))
    assert journal(e)[-1]["failed_scopes"] == [b.scorecard_id]


def test_absent_historical_edition_is_additive_not_deleted(tmp_path):
    a, b = edition("2025"), edition("2005")
    e = evidence(tmp_path)
    old_b = tables(b)
    download = prior(e, combine(tables(a), old_b), tmp_path)
    run(tmp_path, provider([a], {a.scorecard_id: tables}), e, download_prior=download)
    result = read_family(tmp_path / "build", SOURCE_NAMES, include_omitted=True)["scorecards"]
    assert next(r for r in result if r["scorecard_id"] == b.scorecard_id) == old_b["scorecards"][0]


@pytest.mark.parametrize("managed", [False, True])
def test_all_failed_never_writes_a_new_table_generation(tmp_path, managed):
    a = edition("2025")
    e = evidence(tmp_path)
    kwargs = {"download_prior": prior(e, tables(a), tmp_path)} if managed else {}
    with pytest.raises(ScorecardRefreshError, match="No complete"):
        run(tmp_path, provider([a], {a.scorecard_id: Refused("PRIVATE SOURCE PAYLOAD")}), e, **kwargs)
    assert not list((tmp_path / "build").glob("score*.parquet"))
    e.finish(ScorecardRefreshError("PRIVATE SOURCE PAYLOAD"))
    assert all(b"PRIVATE SOURCE PAYLOAD" not in p.read_bytes() for p in e.directory.rglob("*") if p.is_file())


def test_disabled_registry_is_explicit_noop_without_importing_provider(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    path = registry(tmp_path)
    document = yaml.safe_load(path.read_text())
    document["sources"][0]["enabled"] = False
    path.write_text(yaml.safe_dump(document))
    pipeline = ScorecardsRollup(output_dir=tmp_path, registry=path)
    pipeline.run()
    assert pipeline.no_op
    assert not (tmp_path / "generations").exists()
    assert pipeline.source_evidence is not None
    assert pipeline.source_evidence.artifact is not None
    assert pipeline.source_evidence.artifact.root["spec"]["outcome"] == "no-op"


def test_registry_rejects_disabled_selectors_and_full_without_rights(tmp_path):
    sources = load_registry()
    assert all(s.evidence_policy == "hash_only" for s in sources)
    with pytest.raises(RegistryError, match="disabled"):
        select_sources(sources, publishers=["afl_cio"])
    path = registry(tmp_path)
    document = yaml.safe_load(path.read_text())
    document["sources"][0]["evidence_policy"] = "full"
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(RegistryError, match="redistribution"):
        load_registry(path)


def test_no_due_source_does_not_fake_a_successful_refresh(tmp_path, monkeypatch):
    e = evidence(tmp_path)
    monkeypatch.setattr(e, "inherited_event", lambda *args, **kwargs: dict(observed_at=NOW.isoformat()))
    with pytest.raises(NoScorecardsDue):
        run(tmp_path, None, e)
    assert not (tmp_path / "build").exists()
    s = load_registry(registry(tmp_path))[0]
    assert s.due(NOW + timedelta(days=8), NOW.isoformat())


@pytest.mark.parametrize("fault", ["duplicate", "wrong_chamber", "wrong_count", "empty"])
def test_incomplete_or_invalid_edition_is_refused_before_merge(tmp_path, fault):
    a = edition("2025")

    def invalid(edition, cap):
        rows = tables(edition, cap)
        if fault == "duplicate":
            rows["scorecard_members"].append(deepcopy(rows["scorecard_members"][0]))
        elif fault == "wrong_chamber":
            rows["scorecards"][0]["chamber_scope_text"] = "House"
        elif fault == "wrong_count":
            rows["scorecard_snapshots"][0]["source_declared_counts_json"] = '{"scorecard_members":3}'
        else:
            rows = tables(edition, cap, members=0)
        return rows

    with pytest.raises(ScorecardRefreshError, match="No complete"):
        run(tmp_path, provider([a], {a.scorecard_id: invalid}), evidence(tmp_path))
    assert not list((tmp_path / "build").glob("*.parquet"))


@pytest.mark.parametrize("kind", ["absent", "wrong_digest"])
def test_managed_prior_failure_never_uses_stale_local_cache(tmp_path, kind):
    a = edition("2025")
    e = evidence(tmp_path)
    real = prior(e, tables(a), tmp_path)
    build = tmp_path / "build"
    build.mkdir()
    (build / "_scorecards_prior.parquet").write_bytes(b"stale-local")

    def download(key, into):
        if kind == "absent":
            return False
        result = real(key, into)
        into.write_bytes(b"changed")
        return result

    with pytest.raises(ScorecardRefreshError, match="managed prior|Managed prior"):
        run(tmp_path, provider([a], {a.scorecard_id: tables}), e, download_prior=download)
    assert not list(build.glob("score*.parquet"))


def test_evidence_integrity_error_is_not_a_skippable_publisher_failure(tmp_path):
    a = edition("2025")
    with pytest.raises(SourceEvidenceError, match="journal failed"):
        run(tmp_path, provider([a], {a.scorecard_id: SourceEvidenceError("journal failed")}), evidence(tmp_path))


def test_history_requires_explicit_enablement_and_selector(tmp_path):
    a = edition("2024", current=False)
    e = evidence(tmp_path)
    with pytest.raises(ScorecardRefreshError, match="No complete"):
        run(tmp_path, provider([a], {a.scorecard_id: tables}), e)
    with pytest.raises(RegistryError, match="Historical"):
        select_sources(load_registry(registry(tmp_path)), historical_backfill=True)


def test_complete_local_generation_binds_every_table_and_one_evidence_artifact(tmp_path, monkeypatch):
    import spicy_regs.data_dictionary as dictionary
    from spicy_regs.contract_types import described_schema
    from spicy_regs.generations import verify_generation
    from spicy_regs.scorecards.etl import POLICIES, SOURCE_NAMES
    from spicy_regs.source_evidence import verify_evidence

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(
        dictionary,
        "expected_schemas",
        lambda: {
            name: described_schema(POLICIES[name].subject_schema)
            for name in SOURCE_NAMES
            if not POLICIES[name].receipt_only
        },
    )
    a = edition("2025")
    pipeline = ScorecardsRollup(
        output_dir=tmp_path / "output",
        registry=registry(tmp_path),
        provider=provider([a], {a.scorecard_id: tables}),
        fetch_factory=no_fetch,
    )
    pipeline.run()
    generations = list((tmp_path / "output/generations").iterdir())
    assert len(generations) == 1
    admitted = verify_generation(generations[0])
    assert set(admitted.root["spec"]["tables"]) == {
        table["name"] + ".parquet" for table in SCHEMA["tables"]
    }
    assert "etlReceipts" not in admitted.root["spec"]
    assert admitted.root["spec"]["family"] == "scorecards"
    assert len(admitted.root["inputs"]) == 1
    assert admitted.root["inputs"][0]["role"] == "source-evidence"
    assert pipeline.source_evidence is not None
    verify_evidence(pipeline.source_evidence.artifact_dir)
    assert all(b"PRIVATE SOURCE PAYLOAD" not in p.read_bytes() for p in (tmp_path / "output").rglob("*") if p.is_file())


def test_live_registry_matches_complete_pinned_reader_qualifications():
    from spicy_docs.sources.scorecards import get_adapter

    proof = json.loads(LIVE_QUALIFICATIONS.read_text())
    selected = select_sources(load_registry())
    by_id = {row["publisher_id"]: row for row in proof["scopes"]}
    assert set(by_id) == {source.publisher_id for source in selected} == {"lcv", "afscme"}
    for source in selected:
        row = by_id[source.publisher_id]
        adapter = get_adapter(source.adapter)
        spec = importlib.util.find_spec(adapter.__name__)
        assert spec is not None and spec.origin is not None
        assert hashlib.sha256(Path(spec.origin).read_bytes()).hexdigest() == row["reader_sha256"]
        assert adapter.parser_version == row["parser_version"]
        assert source.qualification_id == row["qualification_id"]
        assert source.evidence_policy == row["evidence_policy"] == "hash_only"
        assert row["completeness_status"] == "complete"
        assert row["edition"]["is_current"] is True
        assert row["parsed_counts"]["scorecards"] == 1
        assert row["parsed_counts"]["scorecard_members"] > 0
        assert row["parsed_counts"]["scorecard_member_ratings"] > 0


def test_weekly_workflow_preserves_manual_build_only_and_quotes_selectors():
    path = ROOT / ".github/workflows/rollup-scorecards.yml"
    raw = path.read_text()
    document = yaml.safe_load(raw)
    triggers = document.get("on", document.get(True))
    assert set(triggers) == {"workflow_dispatch", "schedule"}
    assert triggers["schedule"] == [{"cron": "15 6 * * 1"}]
    assert triggers["workflow_dispatch"]["inputs"]["skip_upload"]["default"] is True
    assert document["concurrency"] == {"group": "scorecards-publication", "cancel-in-progress": False}
    for step in document["jobs"]["run"]["steps"]:
        assert "${{ inputs." not in step.get("run", "")
    build_step = next(step for step in document["jobs"]["run"]["steps"] if "Build or publish" in step.get("name", ""))
    assert (
        build_step["env"]["SCORECARD_SKIP_UPLOAD"]
        == "${{ github.event_name == 'schedule' && 'false' || inputs.skip_upload }}"
    )
    assert '"${ARGS[@]}"' in raw
    assert "RUNNER_TEMP" not in raw  # Private source spools are never uploaded.


@pytest.mark.parametrize("status", [401, 403])
def test_credential_http_refusal_aborts_before_next_publisher(tmp_path, status):
    from spicy_docs.transport.credentials import CredentialRefusedError

    path = registry(tmp_path)
    document = yaml.safe_load(path.read_text())
    second = deepcopy(document["sources"][0])
    second.update(publisher_id="nea", adapter="nea")
    document["sources"].append(second)
    path.write_text(yaml.safe_dump(document))
    selected = []
    p = provider([], {})

    class Adapter:
        parser_version = "test-v1"

        def __init__(self, publisher):
            self.publisher = publisher

        def list_scorecards(self, context):
            return [edition("2025", self.publisher)]

        def acquire_scorecard(self, edition, context):
            context.capture(replace(capture(b"PRIVATE SOURCE PAYLOAD"), status_code=status), stage="refused")
            pytest.fail("Credential refusal must not return")

    def get_adapter(name):
        selected.append(name)
        return Adapter(name)

    p.get_adapter = get_adapter
    e = evidence(tmp_path)
    with pytest.raises(CredentialRefusedError):
        build_scorecards(tmp_path / "build", evidence=e, registry=path, provider=p, fetch_factory=no_fetch, now=NOW)
    assert selected == ["lcv"]
    assert not list((tmp_path / "build").glob("score*.parquet"))
    assert all(b"PRIVATE SOURCE PAYLOAD" not in f.read_bytes() for f in e.directory.rglob("*") if f.is_file())


def test_cadence_due_absorbs_cron_jitter_while_blocking_same_day_reruns():
    assert set(CADENCE_TOLERANCES) == set(CADENCES)
    for name, cadence in CADENCES.items():
        assert timedelta(0) <= CADENCE_TOLERANCES[name] < cadence

    weekly = next(source for source in load_registry() if source.publisher_id == "lcv")
    assert weekly.cadence == "weekly"

    # Matches live runs 43818223394 (2026-11-16T06:39:23Z) -> 45125140288 (2026-11-23T06:35:48Z, 3m35s early).
    monday_one = datetime(2026, 11, 16, 6, 39, 23, tzinfo=UTC)
    monday_two_early = datetime(2026, 11, 23, 6, 35, 48, tzinfo=UTC)
    assert weekly.due(monday_two_early, monday_one.isoformat())
    assert weekly.due(monday_one + timedelta(days=7) - timedelta(hours=2), monday_one.isoformat())
    assert weekly.due(monday_one + timedelta(days=7) - CADENCE_TOLERANCES["weekly"], monday_one.isoformat())
    assert not weekly.due(
        monday_one + timedelta(days=7) - CADENCE_TOLERANCES["weekly"] - timedelta(seconds=1),
        monday_one.isoformat(),
    )

    # Accidental same-day reruns and day-before triggers remain blocked unless forced.
    assert not weekly.due(monday_one, monday_one.isoformat())
    assert not weekly.due(monday_one + timedelta(hours=2), monday_one.isoformat())
    assert not weekly.due(monday_one + timedelta(hours=12), monday_one.isoformat())
    assert not weekly.due(monday_one + timedelta(days=6), monday_one.isoformat())
    assert weekly.due(monday_one + timedelta(hours=2), monday_one.isoformat(), force=True)
    assert not replace(weekly, enabled=False).due(monday_one + timedelta(days=8), monday_one.isoformat(), force=True)

    # Other supported periodic cadences also absorb bounded jitter while blocking same-day reruns.
    for cadence_name in ("daily", "monthly"):
        source = replace(weekly, cadence=cadence_name)
        threshold = CADENCES[cadence_name] - CADENCE_TOLERANCES[cadence_name]
        assert source.due(monday_one + threshold, monday_one.isoformat())
        assert not source.due(monday_one + threshold - timedelta(seconds=1), monday_one.isoformat())
        assert not source.due(monday_one + timedelta(hours=2), monday_one.isoformat())


def test_consecutive_scheduled_monday_runs_acquire_due_publishers_and_block_same_day_rerun(tmp_path, monkeypatch):
    document = yaml.safe_load(REGISTRY.read_text())
    selected_sources = []
    for publisher_id in ("lcv", "afscme"):
        entry = deepcopy(next(row for row in document["sources"] if row["publisher_id"] == publisher_id))
        entry.update(enabled=True, qualification_id=f"synthetic-qualified-{publisher_id}-v1")
        selected_sources.append(entry)
    reg_path = tmp_path / "sources.yaml"
    reg_path.write_text(yaml.safe_dump(dict(version=1, sources=selected_sources)))

    editions_by_publisher = {
        "lcv": edition("2025", publisher="lcv"),
        "afscme": edition("2025-2026", publisher="afscme"),
    }
    base_provider = provider([], {})

    def make_provider():
        acquired_publishers = []

        class PublisherAdapter:
            parser_version = "test-v1"

            def __init__(self, publisher_id):
                self.publisher_id = publisher_id

            def list_scorecards(self, context):
                return [editions_by_publisher[self.publisher_id]]

            def acquire_scorecard(self, selected, context):
                acquired_publishers.append(self.publisher_id)
                receipt = context.capture(capture(b"PRIVATE SOURCE PAYLOAD"), stage="test")
                rows = tables(selected, receipt["capture_id"])
                return SimpleNamespace(
                    scorecard_id=selected.scorecard_id,
                    snapshot_id=rows["scorecard_snapshots"][0]["snapshot_id"],
                    tables=rows,
                    capture_ids=(receipt["capture_id"],),
                    completeness_rule="Synthetic complete member table",
                )

        return SimpleNamespace(
            contracts=base_provider.contracts,
            get_adapter=lambda name: PublisherAdapter(name),
            validate=base_provider.validate,
            context=base_provider.context,
            refusal=base_provider.refusal,
        ), acquired_publishers

    def attach_prior_journal(target_evidence, prior_events):
        def inherited_event(event_name, **match):
            found = [
                row
                for row in prior_events
                if row.get("event") == event_name and all(row.get(k) == v for k, v in match.items())
            ]
            return found[-1] if found else None

        monkeypatch.setattr(target_evidence, "inherited_event", inherited_event)

    monday_one = datetime(2026, 11, 16, 6, 39, 23, tzinfo=UTC)
    run_one_evidence = CaptureEvidence(tmp_path / "run1", "scorecards")
    p1, acquired_one = make_provider()
    build_scorecards(
        tmp_path / "run1/build",
        evidence=run_one_evidence,
        registry=reg_path,
        provider=p1,
        fetch_factory=no_fetch,
        download_prior=lambda *args: False,
        now=monday_one,
    )
    assert acquired_one == ["lcv", "afscme"]
    run_one_journal = journal(run_one_evidence)

    # Next Monday arrives 3m35s early (routine GitHub Actions cron jitter); both publishers are still due.
    monday_two = datetime(2026, 11, 23, 6, 35, 48, tzinfo=UTC)
    run_two_evidence = CaptureEvidence(tmp_path / "run2", "scorecards")
    attach_prior_journal(run_two_evidence, run_one_journal)
    p2, acquired_two = make_provider()
    build_scorecards(
        tmp_path / "run2/build",
        evidence=run_two_evidence,
        registry=reg_path,
        provider=p2,
        fetch_factory=no_fetch,
        download_prior=lambda *args: False,
        now=monday_two,
    )
    assert acquired_two == ["lcv", "afscme"]
    run_two_journal = journal(run_two_evidence)

    # Accidental same-day rerun 2h later is blocked without --force.
    same_day_rerun = monday_two + timedelta(hours=2)
    run_three_evidence = CaptureEvidence(tmp_path / "run3", "scorecards")
    attach_prior_journal(run_three_evidence, run_two_journal)
    p3, acquired_three = make_provider()
    with pytest.raises(NoScorecardsDue):
        build_scorecards(
            tmp_path / "run3/build",
            evidence=run_three_evidence,
            registry=reg_path,
            provider=p3,
            fetch_factory=no_fetch,
            download_prior=lambda *args: False,
            now=same_day_rerun,
        )
    assert acquired_three == []
    assert not (tmp_path / "run3/build").exists()
    assert any(
        row["event"] == "scorecard-no-op" and row["reason_code"] == "no_sources_due"
        for row in journal(run_three_evidence)
    )


def test_weekly_workflow_skips_analysis_on_scheduled_noop_and_records_outcome(tmp_path, monkeypatch):
    import spicy_regs.data_dictionary as dictionary
    from spicy_regs.contract_types import described_schema
    from spicy_regs.scorecards.etl import POLICIES

    workflow = yaml.safe_load((ROOT / ".github/workflows/rollup-scorecards.yml").read_text())
    run_job = workflow["jobs"]["run"]
    assert run_job["outputs"] == {"outcome": "${{ steps.outcome.outputs.outcome }}"}
    outcome_step = next(
        step for step in run_job["steps"] if step.get("name") == "Record scorecard refresh outcome"
    )
    assert outcome_step["id"] == "outcome"

    analysis_job = workflow["jobs"]["analysis"]
    assert analysis_job["needs"] == "run"
    assert analysis_job["if"] == (
        "(github.event_name == 'schedule' && needs.run.outputs.outcome == 'build-complete') "
        "|| (github.event_name == 'workflow_dispatch' && inputs.skip_upload == false)"
    )

    def run_outcome_step(work_dir: Path) -> subprocess.CompletedProcess[str]:
        github_output = work_dir / "github_output.txt"
        github_output.write_text("", encoding="utf-8")
        return subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", outcome_step["run"]],
            cwd=work_dir,
            env={**os.environ, "GITHUB_OUTPUT": str(github_output)},
            capture_output=True,
            text=True,
            timeout=10,
        )

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    # 1. No-op run emits outcome=no-op so scheduled analysis is skipped.
    noop_work = tmp_path / "noop"
    noop_work.mkdir()
    noop_registry = registry(noop_work)
    noop_doc = yaml.safe_load(noop_registry.read_text())
    noop_doc["sources"][0]["enabled"] = False
    noop_registry.write_text(yaml.safe_dump(noop_doc))
    noop_pipeline = ScorecardsRollup(output_dir=noop_work / "output", registry=noop_registry)
    noop_pipeline.run()
    assert noop_pipeline.no_op
    noop_result = run_outcome_step(noop_work)
    assert noop_result.returncode == 0, noop_result.stderr
    assert (noop_work / "github_output.txt").read_text(encoding="utf-8").splitlines() == ["outcome=no-op"]

    # 2. Successful build run emits outcome=build-complete so scheduled analysis runs.
    monkeypatch.setattr(
        dictionary,
        "expected_schemas",
        lambda: {
            name: described_schema(POLICIES[name].subject_schema)
            for name in SOURCE_NAMES
            if not POLICIES[name].receipt_only
        },
    )
    built_work = tmp_path / "built"
    built_work.mkdir()
    a = edition("2025")
    built_pipeline = ScorecardsRollup(
        output_dir=built_work / "output",
        registry=registry(built_work),
        provider=provider([a], {a.scorecard_id: tables}),
        fetch_factory=no_fetch,
    )
    built_pipeline.run()
    assert not built_pipeline.no_op
    built_result = run_outcome_step(built_work)
    assert built_result.returncode == 0, built_result.stderr
    assert (built_work / "github_output.txt").read_text(encoding="utf-8").splitlines() == ["outcome=build-complete"]

    # 3. Missing evidence fails closed rather than silently gating.
    empty_work = tmp_path / "empty"
    (empty_work / "output").mkdir(parents=True)
    empty_result = run_outcome_step(empty_work)
    assert empty_result.returncode != 0
