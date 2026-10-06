"""Retained meeting replay verifies observations and leaves unresolved history explicit."""
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import (
    ArtifactInput, LocalMemberSource, Producer, build_artifact_root, canonical_json_bytes,
    describe_member, write_member_manifest,
)
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.congress_index_tables import shape_committee_meeting
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.congress_receipts import write_congress_dataset
from spicy_regs.pipelines.rollups.subject_receipts import dataset_policy
from spicy_regs.generations import build_generation, implementation_id
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.transforms.build_congress_index import INDEX_SPECS, _Held, _unevidenced
from spicy_regs.transforms.meeting_reference_replay import FIELDS, IDENTITY, replay_meeting_references

FIXTURE = Path(__file__).parent / "fixtures/congress_index/committee-meeting-119-senate-338774.json"


def detail():
    value = json.loads(FIXTURE.read_text())["committeeMeeting"]
    value["relatedItems"] = {
        "nominations":[{"congress":119, "number":42, "part":"00", "url":"https://api.congress.gov/v3/nomination/119/42"},
                       {"congress":119, "number":42, "part":"00", "url":"https://api.congress.gov/v3/nomination/119/42"},
                       {"congress":119, "number":42, "part":"01", "url":"https://api.congress.gov/v3/nomination/119/42/01"}],
        "treaties":[],
    }
    return value


def source_files(root, details, *, legacy=False, declare_bodies=True):
    evidence = CaptureEvidence(root / "capture", "committee-meetings")
    for value in details:
        body = json.dumps({"committeeMeeting":value}).encode()
        url = "https://api.congress.gov/v3/committee-meeting/119/senate/338774?format=json&limit=1"
        evidence.capture(CapturedBodyResponse(url, url, 200, "application/json", "2026-09-01T00:00:00Z", body),
                         stage="congress-listing-response")
    artifact = evidence.seal(outcome="build-complete")
    if legacy:
        journal = evidence.artifact_dir / "journal.jsonl"
        events = [json.loads(line) for line in journal.read_bytes().splitlines()]
        for event in events:
            if event["event"] == "capture":
                for name in ("body_retained", "blob_member", "response_complete", "capture_id", "evidence_policy"):
                    event.pop(name, None)
        journal.write_text("".join(json.dumps(event) + "\n" for event in events))
        source = LocalMemberSource(evidence.artifact_dir)
        members = [describe_member(source, object_key=key, role="evidence", media_type="application/octet-stream")
                   for key in sorted(source.keys()) if key not in ("members.json", "artifact.json")
                   and (declare_bodies or not key.startswith("blobs/"))]
        with (evidence.artifact_dir / "members.json").open("wb") as stream:
            manifest = write_member_manifest(stream, scope_kind="global", scope_id="committee-meetings",
                                             object_key="members.json", members=members)
        implementation = implementation_id()
        rebuilt = build_artifact_root(kind=artifact.root["kind"], spec={"family":"committee-meetings", "outcome":"build-complete"},
                                      producer=Producer("spicy-regs", implementation, "urn:test", "1", implementation), manifests=[manifest])
        (evidence.artifact_dir / "artifact.json").write_bytes(canonical_json_bytes(rebuilt))
        artifact = SimpleNamespace(root=rebuilt)
    return evidence, artifact.root


def setup(root, captures=None, *, legacy=False, stored=None, declare_bodies=True):
    original = shape_committee_meeting(detail(), detail())
    original.update(dict.fromkeys(FIELDS))
    if stored:
        original.update(stored)
    source = root / "committee_meetings.parquet"
    schema = pa.schema([(name, pa.string()) for name in TABLE_CONTRACTS["committee_meetings"].columns])
    pq.write_table(pa.Table.from_pylist([original], schema=schema), source)
    evidence, source_root = source_files(root, captures or [detail()], legacy=legacy, declare_bodies=declare_bodies)
    subject, receipts = write_congress_dataset(source, root / "native", dataset="committee_meetings", generation_id="selected")
    assert subject is not None
    artifact = build_generation(root / "generation", family="committee-meetings", files=[subject],
                                expected_keys=[source.name], receipt_path=receipts,
                                receipt_policies=[dataset_policy("committee_meetings")], receipt_generation_id="selected",
                                inputs=[ArtifactInput("source-evidence", source_root["logicalId"], source_root["artifactDigest"])])
    prefix = "generations/committee-meetings/" + artifact.root["artifactDigest"][7:]
    evidence_prefix = "source-evidence/" + source_root["artifactDigest"][7:]
    files = {prefix + "/" + path.name:path for path in (root / "generation").iterdir() if path.is_file()}
    for path in evidence.artifact_dir.rglob("*"):
        if path.is_file():
            key = str(path.relative_to(evidence.artifact_dir))
            files[evidence_prefix + "/" + key] = path
            if key.startswith("blobs/"):
                files["source-evidence/" + key] = path
    replay = CaptureEvidence(root / "replay", "committee-meetings")
    replay.read_snapshot = {"families":{"committee-meetings":{
        "prefix":prefix, "artifactDigest":artifact.root["artifactDigest"], "logicalId":artifact.root["logicalId"]}}}
    return source, replay, files, tuple(original[name] for name in IDENTITY)


def run(source, evidence, files, key):
    return replay_meeting_references(source, {key}, evidence, base_url="https://test.invalid",
                                     fetch=lambda key:files[key].read_bytes())


def events(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]


def test_already_read_meetings_under_falsely_advanced_002_need_003_once():
    spec = INDEX_SPECS["committee_meetings"]
    key = ("119", "senate", "338774")
    held = {key:_Held("2026-09-01T00:00:00Z", True, False)}
    evidence = SimpleNamespace(inherited_event=lambda *args, **kw:{"shape_version":"lists=related-nominations-treaties-002", "unevidenced":[]})
    assert _unevidenced(held, spec, cast(CaptureEvidence, evidence)) == {key}
    evidence.inherited_event = lambda *args, **kw:{"shape_version":spec.shape_version, "unevidenced":[]}
    assert _unevidenced(held, spec, cast(CaptureEvidence, evidence)) == set()
    evidence.inherited_event = lambda *args, **kw:{"shape_version":spec.shape_version, "unevidenced":[list(key)]}
    assert _unevidenced(held, spec, cast(CaptureEvidence, evidence)) == {key}
    assert _unevidenced(held, replace(spec, reread_on_shape_change=False),
                         cast(CaptureEvidence, SimpleNamespace(inherited_event=lambda *a, **k:{"shape_version":"old", "unevidenced":[]}))) == set()


@pytest.mark.parametrize("legacy", [False, True])
def test_exact_captured_response_restores_repeats_partition_and_stated_empty(tmp_path, legacy):
    source, evidence, files, key = setup(tmp_path, [detail(), detail()], legacy=legacy)
    before = pq.read_table(source).to_pylist()[0]
    resolved, ambiguous = run(source, evidence, files, key)
    assert resolved == {key} and ambiguous == set()
    after = pq.read_table(source).to_pylist()[0]
    assert {name:value for name,value in before.items() if name not in FIELDS} == {name:value for name,value in after.items() if name not in FIELDS}
    references = json.loads(after[FIELDS[0]])
    assert [reference["part"] for reference in references] == ["00", "00", "01"]
    assert references[0] == references[1]
    assert after[FIELDS[1]] == "[]"
    replay = next(event for event in events(evidence) if event["event"] == "committee-meeting-reference-replay")
    assert len(replay["variants"][0]["witnesses"]) == 2
    assert not any(event["event"] == "capture" for event in events(evidence))


@pytest.mark.parametrize("change", ["identity", "version", "source-fact", "unread", "unknown-version"])
def test_other_versions_identities_source_facts_and_unread_rows_stay_unavailable(tmp_path, change):
    altered = detail()
    stored = None
    if change == "identity":
        altered["eventId"] = 338775
    elif change == "version":
        altered["updateDate"] = "1900-01-01T00:00:00Z"
    elif change == "source-fact":
        altered["title"] = "another source passage"
    elif change == "unread":
        stored = {"detail_read":"false"}
    else:
        stored = {"update_date":None}
    source, evidence, files, key = setup(tmp_path, [altered], stored=stored)
    before = source.read_bytes()
    assert run(source, evidence, files, key) == (set(), set())
    assert source.read_bytes() == before
    assert events(evidence)[-1]["outcome"] == "unavailable"


def test_conflicting_qualified_responses_do_not_choose_latest(tmp_path):
    first, second = detail(), detail()
    second["relatedItems"]["nominations"] = []
    source, evidence, files, key = setup(tmp_path, [first, second])
    before = source.read_bytes()
    assert run(source, evidence, files, key) == (set(), {key})
    assert source.read_bytes() == before
    assert events(evidence)[-1]["outcome"] == "ambiguous"
    assert len(events(evidence)[-1]["variants"]) == 2


@pytest.mark.parametrize("member", ["body", "members.json", "journal.jsonl"])
def test_changed_declared_bytes_refuse_without_rewriting_prior(tmp_path, member):
    source, evidence, files, key = setup(tmp_path, legacy=True)
    before = source.read_bytes()
    path = next(value for name,value in files.items() if name.startswith("source-evidence/blobs/")) if member == "body" else next(value for name,value in files.items() if name.startswith("source-evidence/") and name.endswith("/" + member))
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(SourceEvidenceError, match="declared bytes"):
        run(source, evidence, files, key)
    assert source.read_bytes() == before


@pytest.mark.parametrize("conflict", [False, True])
def test_meeting_builder_replays_without_detail_requests_and_holds_ambiguity(tmp_path, monkeypatch, conflict):
    from spicy_regs.sources import publication
    from spicy_regs.transforms.build_congress_index import build_index_table
    from tests.test_congress_index import Listing
    from tests.test_incremental_rollups import no_download, seed

    captures = [detail()]
    if conflict:
        second = detail()
        second["relatedItems"]["nominations"] = []
        captures.append(second)
    source, evidence, files, key = setup(tmp_path, captures)
    work = tmp_path / "run"
    work.mkdir()
    seed(work, "committee_meetings", pq.read_table(source).to_pylist())
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    monkeypatch.setattr(publication, "_bounded_get", lambda url, **kwargs:files[url.removeprefix("https://test.invalid/")].read_bytes())
    reader = Listing(lambda record:record["eventId"] == "338774")
    out = build_index_table(work, INDEX_SPECS["committee_meetings"], reader=reader, congresses=[119],
                            max_details=1000, download_prior=no_download, evidence=evidence)
    assert reader.details == []
    selection = next(event for event in events(evidence) if event["event"] == "congress-index-selection")
    assert selection["shape_version"] == INDEX_SPECS["committee_meetings"].shape_version
    row = pq.read_table(out).to_pylist()[0]
    if conflict:
        assert selection["unevidenced"] == [list(key)]
        assert selection["outcomes"] == {"ambiguous_replay":1}
        assert row["nomination_references_json"] is None
    else:
        assert selection["unevidenced"] == []
        assert selection["outcomes"] == {"held":1}
        assert [reference["part"] for reference in json.loads(row["nomination_references_json"])] == ["00", "00", "01"]


def test_old_detail_marker_remains_read_without_inventing_new_read_state(tmp_path):
    source, evidence, files, key = setup(tmp_path, legacy=True, stored={"detail_read":None})
    assert run(source, evidence, files, key) == ({key}, set())
    row = pq.read_table(source).to_pylist()[0]
    assert row["detail_read"] is None
    assert json.loads(row["nomination_references_json"])[0]["part"] == "00"


@pytest.mark.parametrize("bound", ["MAX_HISTORY", "MAX_CAPTURES", "MAX_BODY_BYTES"])
def test_replay_limits_keep_all_rows_unresolved_for_bounded_api_fallback(tmp_path, monkeypatch, bound):
    from spicy_regs.transforms import meeting_reference_replay

    source, evidence, files, key = setup(tmp_path)
    before = source.read_bytes()
    monkeypatch.setattr(meeting_reference_replay, bound, 0)
    assert run(source, evidence, files, key) == (set(), set())
    assert source.read_bytes() == before
    assert events(evidence)[-1]["outcome"] == "unavailable"
    assert events(evidence)[-1]["refusals"]


def test_legacy_capture_digest_cannot_qualify_an_undeclared_blob(tmp_path):
    source, evidence, files, key = setup(tmp_path, legacy=True, declare_bodies=False)
    before = source.read_bytes()
    with pytest.raises(SourceEvidenceError, match="not declared"):
        run(source, evidence, files, key)
    assert source.read_bytes() == before


def test_replay_never_overwrites_an_existing_conflicting_reference(tmp_path):
    source, evidence, files, key = setup(tmp_path, stored={"nomination_references_json":"[]"})
    before = source.read_bytes()
    assert run(source, evidence, files, key) == (set(), {key})
    assert source.read_bytes() == before
    assert events(evidence)[-1]["outcome"] == "ambiguous"
