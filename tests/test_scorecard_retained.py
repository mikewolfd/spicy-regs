"""Qualified replay refuses changed readers, source facts and unselected rows."""

from dataclasses import asdict
from hashlib import sha256
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from spicy_docs.sources.scorecards import ScorecardContext, ScorecardEdition

from spicy_regs.scorecards import retained
from spicy_regs.scorecards.replay import ScorecardReplayError

URL = "https://source.example/grades"
EDITION = ScorecardEdition("lcv", "synthetic", URL, "Synthetic edition")


def rows(value="94%"):
    return {
        "scorecards": [{"scorecard_id": EDITION.scorecard_id, "publisher_id": "lcv"}],
        "scorecard_publishers": [{"publisher_id": "lcv", "observed_at": "2026-10-04T00:00:00Z"}],
        "ratings": [{"scorecard_id": EDITION.scorecard_id, "capture_id": "original", "value_text": value}],
    }


def batch(tmp_path, monkeypatch, *, reference_value="94%", **changes):
    (tmp_path / "reader.py").write_bytes(b"SYNTHETIC_READER = True\n")
    (tmp_path / "body").write_bytes(b"94%")
    manifest = [
        dict(
            requested_url=URL,
            resolved_url=URL,
            http_status=200,
            content_type="text/plain",
            observed_at="2026-10-04T00:00:00Z",
            byte_size=3,
            sha256=sha256(b"94%").hexdigest(),
            body_file="body",
        )
    ]
    (tmp_path / "captures.json").write_text(json.dumps(manifest))
    (tmp_path / "reference.json").write_text(json.dumps(rows(reference_value)))

    def acquire(edition, context):
        source = context.read(URL, stage="scores", hosts=("source.example",))
        return SimpleNamespace(tables=rows(source.body.decode()))

    adapter = SimpleNamespace(__file__=str(tmp_path / "reader.py"), parser_version="test/1", acquire_scorecard=acquire)
    monkeypatch.setattr(retained, "get_adapter", lambda name: adapter)
    entry = dict(
        adapter="lcv",
        edition=asdict(EDITION),
        captures_root=".",
        reader_sha256=sha256((tmp_path / "reader.py").read_bytes()).hexdigest(),
        manifest_sha256=sha256((tmp_path / "captures.json").read_bytes()).hexdigest(),
        reference_file="reference.json",
        reference_sha256=sha256((tmp_path / "reference.json").read_bytes()).hexdigest(),
    )
    entry.update(changes)
    return retained.RetainedScorecardBatch(tmp_path, [entry])


def context(reader):
    return ScorecardContext(reader, lambda response, **kwargs: {"capture_id": str(uuid4())})


def test_installed_reader_replays_and_reconciles_exact_native_value(tmp_path, monkeypatch):
    selected = batch(tmp_path, monkeypatch)
    ctx = context(selected)
    assert selected.list_scorecards(ctx) == (EDITION,)
    result = selected.for_edition(EDITION).acquire_scorecard(EDITION, ctx)
    assert result.tables["ratings"][0]["value_text"] == "94%"
    selected.complete()


def test_changed_source_reference_refuses_instead_of_accepting_parser_counts(tmp_path, monkeypatch):
    selected = batch(tmp_path, monkeypatch, reference_value="93%")
    selected.list_scorecards(context(selected))
    with pytest.raises(ScorecardReplayError, match="source reference"):
        selected.for_edition(EDITION).acquire_scorecard(EDITION, context(selected))
    assert selected.checked == []


@pytest.mark.parametrize("changes", [{"reader_sha256": "0" * 64}, {"captures_root": "../outside"}])
def test_reader_or_private_input_path_change_refuses(tmp_path, monkeypatch, changes):
    with pytest.raises(ScorecardReplayError):
        batch(tmp_path, monkeypatch, **changes)


def test_unread_qualified_scope_is_incomplete(tmp_path, monkeypatch):
    selected = batch(tmp_path, monkeypatch)
    with pytest.raises(ScorecardReplayError, match="Not every"):
        selected.complete()


def test_named_private_assets_preserve_two_chambers_and_refuse_one_changed_input(tmp_path):
    (tmp_path / "house.json").write_bytes(b'{"source_score": "94%"}')
    (tmp_path / "senate.json").write_bytes(b'{"source_score": "A+"}')
    entry = dict(
        reader_inputs=dict(
            qualified_items={
                chamber: dict(file=name, sha256=sha256((tmp_path / name).read_bytes()).hexdigest())
                for chamber, name in (("house", "house.json"), ("senate", "senate.json"))
            }
        )
    )
    selected = retained.qualified_reader_inputs(entry, lambda name: tmp_path / name)
    assert selected["qualified_items"]["house"] == b'{"source_score": "94%"}'
    assert selected["qualified_items"]["senate"] == b'{"source_score": "A+"}'
    (tmp_path / "house.json").write_bytes(b'{"source_score": "93%"}')
    with pytest.raises(ScorecardReplayError, match="pin"):
        retained.qualified_reader_inputs(entry, lambda name: tmp_path / name)


def test_named_inputs_cannot_replace_private_retention_callback(tmp_path):
    with pytest.raises(ScorecardReplayError, match="callbacks"):
        retained.qualified_reader_inputs(
            dict(reader_inputs={"retain_observations": {"file": "body", "sha256": "x"}}), lambda name: tmp_path / name
        )


def test_unselected_rows_require_full_evidence_identity_preservation():
    prior = {
        "scorecard_publishers": [{"publisher_id": "hrc", "capture_id": "old"}],
        "ratings": [{"scorecard_id": "hrc:118", "capture_id": "old", "value_text": "N/A"}],
    }
    current = json.loads(json.dumps(prior))
    assert retained.check_preserved(prior, current, scorecard_ids={"lcv:2025"}, publisher_ids={"lcv"}) == {
        "scorecard_publishers": 1,
        "ratings": 1,
    }
    current["ratings"][0]["capture_id"] = "new"
    with pytest.raises(ScorecardReplayError, match="Unselected"):
        retained.check_preserved(prior, current, scorecard_ids={"lcv:2025"}, publisher_ids={"lcv"})


def test_observation_locator_normalization_preserves_literal_grade():
    first = {"source_path": "PDF/page[1]/extraction[" + str(uuid4()) + "]", "value_text": "N/A"}
    second = {**first, "source_path": "PDF/page[1]/extraction[" + str(uuid4()) + "]"}
    assert retained.canonical([first], observation_ids=True) == retained.canonical([second], observation_ids=True)
    second["value_text"] = "NA"
    assert retained.canonical([first], observation_ids=True) != retained.canonical([second], observation_ids=True)


@pytest.mark.parametrize("suffix", [";heading=Score", "/heading[Score]", ""])
def test_semantic_observation_id_normalization_preserves_page_and_field(suffix):
    first = {"source_path": "pdf:page=6;observation=" + str(uuid4()) + suffix, "value_text": str(uuid4())}
    second = {**first, "source_path": "pdf:page=6;observation=" + str(uuid4()) + suffix}
    assert retained.canonical([first], observation_ids=True) == retained.canonical([second], observation_ids=True)
    second["source_path"] = second["source_path"].replace("page=6", "page=7")
    assert retained.canonical([first], observation_ids=True) != retained.canonical([second], observation_ids=True)
    second = {**first, "value_text": str(uuid4())}
    assert retained.canonical([first], observation_ids=True) != retained.canonical([second], observation_ids=True)
