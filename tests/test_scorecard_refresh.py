"""Whole-edition replacement under success, failure and immutable prior pins."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs.pipelines.rollups.scorecards import ScorecardsRollup
from spicy_regs.scorecards.etl import SOURCE_NAMES, read_family
from spicy_regs.scorecards.registry import REGISTRY, RegistryError, load_registry, select_sources
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.transforms.build_scorecards import NoScorecardsDue, ScorecardRefreshError, build_scorecards
from tests.test_source_evidence import capture, journal

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "docs/research/scorecards/proposed_schema.json").read_text())
SPEC = importlib.util.spec_from_file_location(
    "research_gate", ROOT / "docs/research/scorecards/work/gate/validate_gate.py"
)
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
        capture_ids_json=json.dumps([capture_id]),
        capture_roles_json=json.dumps([dict(capture_id=capture_id, role="primary", field_groups=["all"])]),
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
    result = read_family(tmp_path / "build", SOURCE_NAMES)
    for name, rows in result.items():
        if name == "scorecard_publishers":
            continue
        assert [r for r in rows if r["scorecard_id"] == b.scorecard_id] == old_b[name]
    assert not [r for r in result["scorecard_items"] if r["scorecard_id"] == a.scorecard_id]
    assert len(result["scorecard_member_ratings"]) == 3
    assert len(paths) == len(SCHEMA["tables"]) - 1
    assert not any(p.stem == "scorecard_snapshots" for p in paths)
    assert any(r["event"] == "rows-retired" and r["rows"] for r in journal(e))
    assert journal(e)[-1]["failed_scopes"] == [b.scorecard_id]


def test_absent_historical_edition_is_additive_not_deleted(tmp_path):
    a, b = edition("2025"), edition("2005")
    e = evidence(tmp_path)
    old_b = tables(b)
    download = prior(e, combine(tables(a), old_b), tmp_path)
    run(tmp_path, provider([a], {a.scorecard_id: tables}), e, download_prior=download)
    result = read_family(tmp_path / "build", SOURCE_NAMES)["scorecards"]
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


def test_disabled_default_is_explicit_noop_without_importing_provider(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    pipeline = ScorecardsRollup(output_dir=tmp_path)
    pipeline.run()
    assert pipeline.no_op
    assert not (tmp_path / "generations").exists()
    assert pipeline.source_evidence is not None
    assert pipeline.source_evidence.artifact is not None
    assert pipeline.source_evidence.artifact.root["spec"]["outcome"] == "no-op"


def test_registry_rejects_disabled_selectors_and_full_without_rights(tmp_path):
    sources = load_registry()
    assert all(not s.enabled and s.evidence_policy == "hash_only" for s in sources)
    with pytest.raises(RegistryError, match="disabled"):
        select_sources(sources, publishers=["lcv"])
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
            for name in SOURCE_NAMES if not POLICIES[name].receipt_only
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
        table["name"] + ".parquet" for table in SCHEMA["tables"] if table["name"] != "scorecard_snapshots"
    }
    assert admitted.root["spec"]["etlReceipts"]["key"] == "etl_receipts.parquet"
    assert admitted.root["spec"]["family"] == "scorecards"
    assert len(admitted.root["inputs"]) == 1
    assert admitted.root["inputs"][0]["role"] == "source-evidence"
    assert pipeline.source_evidence is not None
    verify_evidence(pipeline.source_evidence.artifact_dir)
    assert all(b"PRIVATE SOURCE PAYLOAD" not in p.read_bytes() for p in (tmp_path / "output").rglob("*") if p.is_file())


def test_manual_workflow_has_no_schedule_and_quotes_selectors():
    path = ROOT / ".github/workflows/rollup-scorecards.yml"
    raw = path.read_text()
    document = yaml.safe_load(raw)
    triggers = document.get("on", document.get(True))
    assert set(triggers) == {"workflow_dispatch"}
    assert triggers["workflow_dispatch"]["inputs"]["skip_upload"]["default"] is True
    assert document["concurrency"] == {"group": "scorecards-publication", "cancel-in-progress": False}
    for step in document["jobs"]["run"]["steps"]:
        assert "${{ inputs." not in step.get("run", "")
    assert '"${ARGS[@]}"' in raw
    assert "RUNNER_TEMP" not in raw  # Private source spools are never uploaded.


@pytest.mark.parametrize("status", [401, 403])
def test_credential_http_refusal_aborts_before_next_publisher(tmp_path, status):
    from dataclasses import replace
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
