"""Qualification failures cannot be mistaken for full source-only recovery."""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
import shutil
import socket

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.qualify_source_catalog_f import RetainedArtifact, assert_pin, compare_rows, pin, replay, selection


def test_full_rows_and_duplicate_multiplicity():
    left = [{"id": "same", "value": None}, {"id": "same", "value": "x"}]
    assert compare_rows(left, list(reversed(left)))["all_values_and_multiplicity_equal"]
    assert not compare_rows(left, list(reversed(left)))["ordered_rows_equal"]
    assert not compare_rows(left, [left[0], left[0]])["all_values_and_multiplicity_equal"]
    assert not compare_rows(left, [{"id": "same", "value": ""}, left[1]])["all_values_and_multiplicity_equal"]


def test_input_pin_rejects_same_size_changed_bytes(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"original")
    expected = pin(source)
    source.write_bytes(b"modified")
    with pytest.raises(ValueError, match="Changed selected input"):
        assert_pin(source, expected)


def _artifact(tmp_path, body):
    digest = "sha256:" + "a" * 64
    directory = tmp_path / "source-evidence" / digest[7:]
    directory.mkdir(parents=True)
    sha = hashlib.sha256(body).hexdigest()
    member = {"objectKey": "journal.jsonl", "sha256": "sha256:" + sha, "byteSize": len(body)}
    (directory / "artifact.json").write_text(json.dumps({"memberManifests": [{"objectKey": "members.json"}]}))
    (directory / "members.json").write_text(json.dumps({"members": [member]}))
    blob = tmp_path / "source-evidence/blobs/sha256" / (sha + ".gz")
    blob.parent.mkdir(parents=True)
    blob.write_bytes(gzip.compress(body))
    return RetainedArtifact(tmp_path, digest), blob


def test_compressed_original_is_checked_after_decoding(tmp_path):
    body = b'{"event":"capture"}\n'
    source, blob = _artifact(tmp_path, body)
    assert source.journal() == [{"event": "capture"}]
    blob.write_bytes(gzip.compress(body.replace(b"capture", b"changed")))
    with pytest.raises(ValueError, match="differs from declared bytes"):
        source.journal()


def test_read_budget_and_decompression_bound(tmp_path):
    source, blob = _artifact(tmp_path, b"small")
    source.remaining = 1
    with pytest.raises(ValueError, match="budget exceeded"):
        source.open("journal.jsonl")
    blob.write_bytes(gzip.compress(b"larger than declaration"))
    with pytest.raises(ValueError, match="differs from declared bytes"):
        source.open("journal.jsonl")


def test_gao_first_seen_closed_history_survives_native_receipts(tmp_path):
    from spicy_docs.schemas.gao_recommendation_tables import GAO_RECOMMENDATIONS, shape_gao_recommendation
    from spicy_docs.sources.gao.recommendations import parse_recommendations_export
    from spicy_regs.etl_receipts import read_with_receipts
    from spicy_regs.transforms.build_gao_recommendations import fold
    from spicy_regs.transforms.government_receipts import POLICIES, migrate_outputs

    fixture = Path(__file__).parent / "fixtures/gao_recommendations/open-recs-2026-09-28-excerpt.csv"
    export = parse_recommendations_export(fixture.read_bytes())
    fresh = [shape_gao_recommendation(rec, status_as_of=export.status_as_of, as_of=export.as_of)
             for rec in export.recommendations]
    prior = [{**row, "first_seen": "2026-09-20"} for row in fresh]
    history, retirement = fold(prior, fresh[1:])
    assert retirement["retired"] == 1
    assert all(row["first_seen"] == "2026-09-20" for row in history)
    assert history[-1]["listed_open"] == "false"
    candidate = tmp_path / "gao_recommendations.parquet"
    schema = pa.schema([(column, pa.string()) for column in GAO_RECOMMENDATIONS.columns])
    pq.write_table(pa.Table.from_pylist(history, schema=schema), candidate)
    migration = migrate_outputs((candidate,))
    restored = [row["raw_record"] for row in read_with_receipts(
        [candidate], [Path(migration["receipt_path"])], POLICIES["gao_recommendations"],
        generation_id=migration["generation_id"])]
    assert compare_rows(history, restored)["all_values_and_multiplicity_equal"]


def _retained_fixture_corpus(tmp_path, *, mismatch=False):
    """Use actual maintained producer artifacts, not invented manifest shapes."""
    from spicy_docs.schemas.gao_recommendation_tables import GAO_RECOMMENDATIONS, shape_gao_recommendation
    from spicy_docs.sources.gao.recommendations import parse_recommendations_export
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_regs.generations import build_generation
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.government_receipts import generation_receipt_args, migrate_outputs

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    body = (Path(__file__).parent / "fixtures/gao_recommendations/open-recs-2026-09-28-excerpt.csv").read_bytes()
    export = parse_recommendations_export(body)
    rows = [shape_gao_recommendation(rec, status_as_of=export.status_as_of, as_of=export.as_of)
            for rec in export.recommendations]
    if mismatch:
        rows[0]["comments"] = "A differing retained producer value"
    output = tmp_path / "producer"
    output.mkdir()
    table = output / "gao_recommendations.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in GAO_RECOMMENDATIONS.columns])), table)
    evidence = CaptureEvidence(output, "gao-recommendations")
    evidence.capture(CapturedBodyResponse("https://source.test/export", "https://source.test/export", 200,
                                         "text/csv", "2026-09-28T23:05:00Z", body),
                     stage="gao-recommendations-export")
    evidence.event("fold", stage="gao-recommendations-export", prior_rows=0)
    migrate_outputs((table,), evidence=evidence)
    artifact = evidence.seal(outcome="build-complete")
    source_dir = corpus / "source-evidence" / artifact.pin.artifact_digest[7:]
    source_dir.mkdir(parents=True)
    for key in ("artifact.json", "members.json"):
        shutil.copyfile(evidence.artifact_dir / key, source_dir / key)
    blobs = corpus / "source-evidence/blobs/sha256"
    blobs.mkdir(parents=True)
    for member in json.loads((source_dir / "members.json").read_text())["members"]:
        shutil.copyfile(evidence.artifact_dir / member["objectKey"], blobs / member["sha256"][7:])
    generation_dir = tmp_path / "generation"
    generation = build_generation(generation_dir, family="gao-recommendations", files=[table],
                                  expected_keys=[table.name], inputs=evidence.inputs(),
                                  **generation_receipt_args((table,)))
    target = corpus / "lineage/generations/gao-recommendations" / generation.pin.artifact_digest[7:]
    target.mkdir(parents=True)
    shutil.copyfile(generation_dir / "artifact.json", target / "artifact.json")
    (corpus / "catalog.json").write_text(json.dumps({"families": {"gao-recommendations": {
        "current_generation": generation.pin.artifact_digest,
        "source_artifacts": [artifact.pin.artifact_digest]}}}))
    manifest = tmp_path / "selection.json"
    manifest.write_text(json.dumps(selection(corpus)))
    return manifest, corpus, rows


def test_complete_offline_replay_and_refuse_successful_output_overwrite(tmp_path, monkeypatch):
    manifest, _, expected = _retained_fixture_corpus(tmp_path)
    def refuse_network(*args, **kwargs):
        raise AssertionError("Unexpected network")
    monkeypatch.setattr(socket.socket, "connect", refuse_network)
    output = tmp_path / "candidate"
    result = replay(manifest, output)
    assert result["state"] == "qualified"
    assert result["final_comparison"]["expected_rows"] == len(expected)
    assert result["native_receipt_roundtrip"]["all_values_and_multiplicity_equal"]
    before = {str(p): pin(p) for p in output.rglob("*") if p.is_file()}
    with pytest.raises(FileExistsError):
        replay(manifest, output)
    assert {str(p): pin(p) for p in output.rglob("*") if p.is_file()} == before


def test_changed_capture_retains_terminal_error_without_success(tmp_path):
    manifest, corpus, _ = _retained_fixture_corpus(tmp_path)
    selected = json.loads(manifest.read_text())
    capture = selected["captures"][0]["export"]
    blob = corpus / "source-evidence/blobs/sha256" / capture["sha256"][7:]
    blob.write_bytes(b"changed")
    output = tmp_path / "failed"
    with pytest.raises(Exception):
        replay(manifest, output)
    terminal = json.loads((output / "qualification.json").read_text())
    assert terminal["state"] == "error"
    assert terminal["production_actions"] == []


def test_modified_selection_refuses_before_candidate_creation(tmp_path):
    manifest, _, _ = _retained_fixture_corpus(tmp_path)
    selected = json.loads(manifest.read_text())
    selected["captures"][0]["fold"]["prior_rows"] = 99
    manifest.write_text(json.dumps(selected))
    output = tmp_path / "candidate"
    with pytest.raises(ValueError, match="exact canonical journals"):
        replay(manifest, output)
    assert not output.exists()


def test_equal_ids_with_changed_field_do_not_qualify_recovery(tmp_path):
    manifest, _, _ = _retained_fixture_corpus(tmp_path, mismatch=True)
    output = tmp_path / "mismatch"
    result = replay(manifest, output)
    assert result["state"] == "failed-source-only-recovery"
    assert result["final_comparison"]["expected_rows"] == result["final_comparison"]["actual_rows"]
    assert result["final_comparison"]["missing_row_occurrences"] == 1
    assert result["final_comparison"]["added_row_occurrences"] == 1
    assert result["native_receipt_roundtrip"]["all_values_and_multiplicity_equal"]


def test_final_catalog_drift_retains_terminal_error(tmp_path, monkeypatch):
    from spicy_regs.transforms import government_receipts
    manifest, corpus, _ = _retained_fixture_corpus(tmp_path)
    original = government_receipts.migrate_outputs
    def changed_after_write(*args, **kwargs):
        result = original(*args, **kwargs)
        (corpus / "catalog.json").write_text("{}")
        return result
    monkeypatch.setattr(government_receipts, "migrate_outputs", changed_after_write)
    output = tmp_path / "drift"
    with pytest.raises(ValueError, match="Changed selected input"):
        replay(manifest, output)
    assert json.loads((output / "qualification.json").read_text())["state"] == "error"


def _court_fixture(tmp_path, pages):
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_regs.source_evidence import CaptureEvidence
    from scripts.qualify_source_catalog_f import court_selection
    corpus, producer = tmp_path / 'corpus', tmp_path / 'producer'
    corpus.mkdir()
    producer.mkdir()
    evidence = CaptureEvidence(producer, 'courtlistener')
    for group in pages:
        evidence.event('selection', stage='courtlistener', url=group[0][0], max_records=None)
        for url, value in group:
            evidence.capture(CapturedBodyResponse(url, url, 200, 'application/json',
                '2026-10-09T18:42:37Z', json.dumps(value).encode()), stage='courtlistener-response')
    artifact = evidence.seal(outcome='build-complete')
    directory = corpus / 'source-evidence' / artifact.pin.artifact_digest[7:]
    directory.mkdir(parents=True)
    for key in ('artifact.json', 'members.json'):
        shutil.copyfile(evidence.artifact_dir / key, directory / key)
    blobs = corpus / 'source-evidence/blobs/sha256'
    blobs.mkdir(parents=True)
    for member in json.loads((directory / 'members.json').read_text())['members']:
        shutil.copyfile(evidence.artifact_dir / member['objectKey'], blobs / member['sha256'][7:])
    manifest = tmp_path / 'court-selection.json'
    manifest.write_text(json.dumps(court_selection(corpus, artifact.pin.artifact_digest)))
    return manifest, corpus


COURT_URL = 'https://www.courtlistener.com/api/rest/v4/search/?type=r&order_by=dateFiled+asc&q=test'


def _court_page(rows, *, next_url=None, count=None):
    return {'results': rows, 'count': len(rows) if count is None else count, 'next': next_url}


def test_court_delta_occurrences_empty_searches_and_refusals(tmp_path, monkeypatch):
    from scripts.qualify_source_catalog_f import court_replay
    row = {'docket_id': 1, 'caseName': 'Case', 'party': ['b', None, 'a'], 'blocked': False,
           'meta': {'score': {'bm25': 0.123456789}}}
    malformed = {'docket_id': 2, 'party': {'bad': 'array'}}
    third = {'docket_id': 3, 'party': [], 'blocked': True}
    manifest, _ = _court_fixture(tmp_path, [[(COURT_URL, _court_page([row, malformed, third]))],
        [(COURT_URL, _court_page([row]))], [(COURT_URL, _court_page([]))], [(COURT_URL, _court_page([]))]])
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Network called'))
    output = tmp_path / 'court-candidate'
    result = court_replay(manifest, output)
    assert result['state'] == 'qualified'
    assert result['capture_occurrences'] == 4
    assert result['groups'][0]['accepted'] == 2
    assert result['groups'][0]['refused'] == 1
    assert sum(r['source_occurrences'] for r in result['groups']) == 4
    assert result['groups'][2]['complete_empty_search']
    assert result['groups'][3]['complete_empty_search']
    index = json.loads((output / 'group-000/occurrences.json').read_text())
    assert [r['outcome'] for r in index] == ['accepted', 'refused', 'accepted']
    assert [r['attempt_id'] for r in index] == [f'court_dockets:delta:{i}' for i in range(3)]
    before = {str(p): pin(p) for p in output.rglob('*') if p.is_file()}
    with pytest.raises(FileExistsError):
        court_replay(manifest, output)
    assert before == {str(p): pin(p) for p in output.rglob('*') if p.is_file()}


@pytest.mark.parametrize('rows', [[{'docket_id': True}], [{'docket_id': 0}], [{}],
                                 [{'docket_id': 1}, {'docket_id': 1}]])
def test_court_delta_invalid_or_repeated_identity_fails(tmp_path, rows):
    from scripts.qualify_source_catalog_f import court_replay
    manifest, _ = _court_fixture(tmp_path, [[(COURT_URL, _court_page(rows))]])
    output = tmp_path / 'candidate'
    with pytest.raises(ValueError, match='docket identity'):
        court_replay(manifest, output)
    assert json.loads((output / 'qualification.json').read_text())['state'] == 'error'


def test_court_delta_complete_multipage_and_cross_page_identity(tmp_path):
    from scripts.qualify_source_catalog_f import court_replay
    next_url = COURT_URL + '&cursor=next'
    manifest, _ = _court_fixture(tmp_path, [[
        (COURT_URL, _court_page([{'docket_id': 1}], next_url=next_url, count=2)),
        (next_url, _court_page([{'docket_id': 2}], count=2))]])
    assert court_replay(manifest, tmp_path / 'candidate')['state'] == 'qualified'
    other = tmp_path / 'other'
    other.mkdir()
    manifest, _ = _court_fixture(other, [[
        (COURT_URL, _court_page([{'docket_id': 1}], next_url=next_url, count=2)),
        (next_url, _court_page([{'docket_id': 1}], count=2))]])
    with pytest.raises(ValueError, match='docket identity'):
        court_replay(manifest, other / 'candidate')


@pytest.mark.parametrize('kind', ['missing', 'extra', 'count', 'cursor', 'bytes', 'manifest', 'empty'])
def test_court_delta_drift_and_incomplete_capture_fail(tmp_path, kind):
    from scripts.qualify_source_catalog_f import court_replay
    next_url = COURT_URL + '&cursor=next'
    pages = [[(COURT_URL, _court_page([{'docket_id': 1}]))]]
    if kind == 'missing':
        pages[0][0] = (COURT_URL, _court_page([{'docket_id': 1}], next_url=next_url, count=2))
    elif kind == 'extra':
        pages[0].append((next_url, _court_page([])))
    elif kind == 'count':
        pages[0][0] = (COURT_URL, _court_page([{'docket_id': 1}], count=2))
    elif kind == 'empty':
        pages[0][0] = (COURT_URL, _court_page([], next_url=next_url, count=0))
    elif kind == 'cursor':
        pages[0][0] = (COURT_URL, _court_page([{'docket_id': 1}], next_url=next_url, count=2))
        pages[0].append((COURT_URL + '&cursor=wrong', _court_page([{'docket_id': 2}], count=2)))
    manifest, corpus = _court_fixture(tmp_path, pages)
    if kind == 'bytes':
        item = json.loads(manifest.read_text())['groups'][0]['captures'][0]['event']
        (corpus / 'source-evidence/blobs/sha256' / item['sha256'][7:]).write_bytes(b'changed')
    elif kind == 'manifest':
        data = json.loads(manifest.read_text())
        data['groups'][0]['selection']['max_records'] = 1
        manifest.write_text(json.dumps(data))
    with pytest.raises(Exception):
        court_replay(manifest, tmp_path / 'candidate')
    if (tmp_path / 'candidate').exists():
        assert json.loads((tmp_path / 'candidate/qualification.json').read_text())['state'] == 'error'


def test_court_delta_final_body_drift_and_code_drift_fail(tmp_path, monkeypatch):
    from scripts import qualify_source_catalog_f as runner
    from spicy_regs import court_receipts
    manifest, corpus = _court_fixture(tmp_path, [[(COURT_URL, _court_page([{'docket_id': 1}]))]])
    original = court_receipts.write_court_rows
    capture = json.loads(manifest.read_text())['groups'][0]['captures'][0]['event']
    def drift_after_write(*args, **kwargs):
        value = original(*args, **kwargs)
        (corpus / 'source-evidence/blobs/sha256' / capture['sha256'][7:]).write_bytes(b'changed')
        return value
    monkeypatch.setattr(court_receipts, 'write_court_rows', drift_after_write)
    output = tmp_path / 'candidate'
    with pytest.raises(Exception):
        runner.court_replay(manifest, output)
    assert json.loads((output / 'qualification.json').read_text())['state'] == 'error'
    monkeypatch.setattr(runner, 'court_code_pins', lambda: {'changed': True})
    with pytest.raises(ValueError, match='selection/code differs'):
        runner.court_replay(manifest, tmp_path / 'code-drift')
    assert not (tmp_path / 'code-drift').exists()
