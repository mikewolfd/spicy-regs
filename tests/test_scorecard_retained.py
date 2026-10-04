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


def batch(tmp_path, monkeypatch, *, reference_value="94%", named_reader=None, retain_observations=None, **changes):
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
    if named_reader is not None:
        setattr(adapter, named_reader.__name__, named_reader)
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
    return retained.RetainedScorecardBatch(tmp_path, [entry], retain_observations=retain_observations)


def context(reader):
    return ScorecardContext(reader, lambda response, **kwargs: {"capture_id": str(uuid4())})


def test_installed_reader_replays_and_reconciles_exact_native_value(tmp_path, monkeypatch):
    selected = batch(tmp_path, monkeypatch)
    ctx = context(selected)
    assert selected.list_scorecards(ctx) == (EDITION,)
    result = selected.for_edition(EDITION).acquire_scorecard(EDITION, ctx)
    assert result.tables["ratings"][0]["value_text"] == "94%"
    selected.complete()


class NativeAPIReader:
    parser_version = "native-api/test"

    def acquire_scorecard(self, edition, context):
        source = context.read(URL, stage="scores", hosts=("source.example",))
        return SimpleNamespace(tables=rows(source.body.decode()))


@pytest.mark.parametrize("reference_value", ["94%", "93%"])
def test_named_native_reader_uses_source_without_semantic_assets(tmp_path, monkeypatch, reference_value):
    selected = batch(
        tmp_path,
        monkeypatch,
        named_reader=NativeAPIReader,
        reader_class="NativeAPIReader",
        reference_value=reference_value,
    )
    ctx = context(selected)
    selected.list_scorecards(ctx)
    reader = selected.for_edition(EDITION)
    assert reader.parser_version == "native-api/test"
    if reference_value == "93%":
        with pytest.raises(ScorecardReplayError, match="source reference"):
            reader.acquire_scorecard(EDITION, ctx)
        assert selected.checked == []
    else:
        assert reader.acquire_scorecard(EDITION, ctx).tables["ratings"][0]["value_text"] == "94%"
        selected.complete()


@pytest.mark.parametrize(
    "assets",
    [
        {"observations_file": "missing.json", "observations_sha256": "0" * 64},
        {"reader_inputs": {"qualified_observations": {"file": "missing.json", "sha256": "0" * 64}}},
    ],
)
def test_named_semantic_reader_still_requires_private_retention(tmp_path, monkeypatch, assets):
    with pytest.raises(ScorecardReplayError, match="private observation retention"):
        batch(tmp_path, monkeypatch, named_reader=NativeAPIReader, reader_class="NativeAPIReader", **assets)


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


def capture_paths(prefix):
    selected = [prefix + "-catalog", prefix + "-detail"]
    return {
        "scorecard_snapshots": [{"scorecard_id": "source:edition", "capture_ids_json": json.dumps(selected)}],
        "scorecard_items": [
            {
                "scorecard_id": "source:edition",
                "source_url": "https://source.example/catalog",
                "source_path": "$[0]",
                "position_source_path": "capture:" + selected[1] + "#$.stance",
                "references_json": json.dumps(
                    [{"source_path": "capture:" + selected[0] + "#$.citation", "citation_text": "H.R. 1"}]
                ),
                "value_text": "94%",
            }
        ],
    }


def test_capture_locator_ids_change_without_changing_selected_document_or_native_facts():
    retained.check_reference(capture_paths("fresh"), capture_paths("retained"))


@pytest.mark.parametrize("change", ["target", "order", "unknown", "field", "native", "native_capture_text"])
def test_capture_locator_comparison_preserves_attribution_and_literal_values(change):
    actual, reference = capture_paths("fresh"), capture_paths("retained")
    row = actual["scorecard_items"][0]
    if change == "target":
        row["position_source_path"] = "capture:fresh-catalog#$.stance"
    elif change == "order":
        actual["scorecard_snapshots"][0]["capture_ids_json"] = json.dumps(["fresh-detail", "fresh-catalog"])
    elif change == "unknown":
        row["position_source_path"] = "capture:unselected#$.stance"
    elif change == "field":
        row["position_source_path"] = "capture:fresh-detail#$.different_stance"
    elif change == "native":
        row["value_text"] = "93%"
    else:
        row["value_text"] = "capture:fresh-detail#$.stance"
        reference["scorecard_items"][0]["value_text"] = "capture:retained-detail#$.stance"
    with pytest.raises(ScorecardReplayError, match="source reference"):
        retained.check_reference(actual, reference)


def test_unknown_locator_refuses_even_when_both_inputs_repeat_the_same_unknown_token():
    actual, reference = capture_paths("fresh"), capture_paths("retained")
    for tables in (actual, reference):
        tables["scorecard_items"][0]["position_source_path"] = "capture:unselected#$.stance"
    with pytest.raises(ScorecardReplayError, match="source reference"):
        retained.check_reference(actual, reference)


def test_locator_cannot_cross_into_another_snapshot_even_when_both_inputs_agree():
    actual, reference = capture_paths("fresh"), capture_paths("retained")
    for tables, prefix in ((actual, "fresh"), (reference, "retained")):
        tables["scorecard_snapshots"].append(
            {"scorecard_id": "other:edition", "capture_ids_json": json.dumps([prefix + "-other"])}
        )
        tables["scorecard_items"][0]["position_source_path"] = "capture:" + prefix + "-other#$.stance"
    with pytest.raises(ScorecardReplayError, match="source reference"):
        retained.check_reference(actual, reference)


@pytest.mark.parametrize("change", [None, "action_field", "capture_target"])
def test_compound_member_action_locator_preserves_native_field_and_document(change):
    actual, reference = capture_paths("fresh"), capture_paths("retained")
    for tables, prefix in ((actual, "fresh"), (reference, "retained")):
        tables["scorecard_items"][0]["source_path"] = (
            "$[0].factors[0];action_source=capture:" + prefix + "-detail#$.positions[3]"
        )
    path = actual["scorecard_items"][0]["source_path"]
    if change == "action_field":
        actual["scorecard_items"][0]["source_path"] = path.replace("positions[3]", "positions[4]")
    elif change == "capture_target":
        actual["scorecard_items"][0]["source_path"] = path.replace("fresh-detail", "fresh-catalog")
    if change:
        with pytest.raises(ScorecardReplayError, match="source reference"):
            retained.check_reference(actual, reference)
    else:
        retained.check_reference(actual, reference)


def test_nested_native_citation_text_is_not_capture_locator_normalized():
    actual, reference = capture_paths("fresh"), capture_paths("retained")
    for tables, prefix in ((actual, "fresh"), (reference, "retained")):
        refs = json.loads(tables["scorecard_items"][0]["references_json"])
        refs[0]["citation_text"] = "capture:" + prefix + "-detail#literal-publisher-citation"
        tables["scorecard_items"][0]["references_json"] = json.dumps(refs)
    with pytest.raises(ScorecardReplayError, match="source reference"):
        retained.check_reference(actual, reference)


def test_duplicate_capture_selection_refuses_even_when_both_references_agree():
    actual, reference = capture_paths("fresh"), capture_paths("retained")
    for tables, prefix in ((actual, "fresh"), (reference, "retained")):
        tables["scorecard_snapshots"][0]["capture_ids_json"] = json.dumps([prefix + "-detail"] * 2)
    with pytest.raises(ScorecardReplayError, match="capture selection is malformed"):
        retained.check_reference(actual, reference)
