"""A native input refusal must happen before any potentially large downloads."""

from copy import deepcopy
import sys

import pytest

from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecard_analysis import (
    SOURCE_TABLES, OFFICIAL_TABLES, COMPARISON_INPUTS, analysis_input_entries,
)
from tests.test_scorecard_analysis import _inputs, _serve


def fixture_owner(index, key):
    selected = publication.table_owner(index, key)
    assert selected is not None
    owner = selected[1]
    assert isinstance(owner, dict)
    return owner


@pytest.mark.parametrize("name", (*SOURCE_TABLES, *OFFICIAL_TABLES, *COMPARISON_INPUTS))
def test_missing_native_input_refuses_before_any_subject_or_receipt_download(tmp_path, monkeypatch, name):
    store, index = _inputs(tmp_path)
    owner = fixture_owner(index, name + ".parquet")
    owner["etlReceipts"]["datasets"].remove(name)
    calls = _serve(monkeypatch, store)
    output = tmp_path / "refused"
    output.mkdir()
    rollup = ScorecardAnalysisRollup()
    with pytest.raises(publication.PublicationError, match="requires native receipts for:.*" + name):
        rollup._prime(output, index)
    assert calls == []
    assert list(output.iterdir()) == []
    assert not hasattr(rollup, "_scorecard_input_paths")


def test_mixed_scorecard_generations_refuse_before_any_download(tmp_path, monkeypatch):
    store, index = _inputs(tmp_path)
    source = index["families"]["scorecards"]
    split = deepcopy(source)
    split["tables"] = {"scorecard_items.parquet": source["tables"].pop("scorecard_items.parquet")}
    split["artifactDigest"] = "sha256:" + "1" * 64
    index["families"]["different-scorecard-generation"] = split
    calls = _serve(monkeypatch, store)
    with pytest.raises(publication.PublicationError, match="source inputs must share one generation"):
        ScorecardAnalysisRollup()._prime(tmp_path, index)
    assert calls == []


def test_preflight_requires_a_selected_receipt_generation(tmp_path):
    _, index = _inputs(tmp_path)
    fixture_owner(index, "members.parquet")["etlReceipts"]["generationId"] = ""
    with pytest.raises(publication.PublicationError, match="selected native receipt generation: members"):
        analysis_input_entries(index)


def test_preparation_script_uses_same_preflight_before_object_download(tmp_path, monkeypatch):
    from spicy_regs.scorecards.operations import analysis as module
    _, index = _inputs(tmp_path)
    fixture_owner(index, "members.parquet").pop("etlReceipts")
    client = object()
    monkeypatch.setattr(module.r2, "get_r2_client", lambda: client)
    monkeypatch.setattr(module.publication, "_stored_index", lambda *args: (index, None))
    monkeypatch.setattr(module, "load_dotenv", lambda *args: None)
    monkeypatch.setattr(sys, "argv", ["prepare_analysis.py", "--output", str(tmp_path / "refused")])
    with pytest.raises(publication.PublicationError, match="requires native receipts for: members, member_terms"):
        module.main()
    assert list((tmp_path / "refused").iterdir()) == []


def captured_native_index():
    """Metadata-only selection: no source build, download or invented admission proof."""
    index = publication.empty_index()
    index["version"] = 2
    sources = (*SOURCE_TABLES, *COMPARISON_INPUTS[:3])
    groups = {"scorecards": sources} | {name: (name,) for name in (*OFFICIAL_TABLES, *COMPARISON_INPUTS[3:])}
    for ordinal, (family, names) in enumerate(groups.items()):
        digest = f"{ordinal:064x}"
        member = {"sha256": "sha256:" + digest, "byteSize": 1, "rows": 0, "columns": [["value", "VARCHAR"]]}
        index["families"][family] = {
            "prefix": f"generations/{family}/{digest}",
            "artifactDigest": "sha256:" + digest,
            "logicalId": "urn:test:" + family,
            "tables": {name + ".parquet": dict(member) for name in names},
            "etlReceipts": {
                "key": "etl_receipts.parquet",
                **member,
                "generationId": "selected",
                "datasets": list(names),
            },
        }
    return index


def split_receipt_declaration(index, dataset):
    receipt = fixture_owner(index, dataset + ".parquet")["etlReceipts"]
    member = {key: receipt.pop(key) for key in ("key", "sha256", "byteSize")}
    receipt.update(version=2, members=[{**member, "key": f"etl_receipts/{i}.parquet", "rows": 0} for i in range(2)])


@pytest.mark.parametrize("dataset", ["roll_call_votes", "member_votes"])
def test_split_votes_require_the_actual_projected_native_route(dataset):
    import json
    from spicy_regs.scorecards.analysis_inputs import projected_native_official_inputs
    from spicy_regs.scorecards.operations import analysis

    assert analysis.projected_native_official_inputs is projected_native_official_inputs
    index = captured_native_index()
    split_receipt_declaration(index, dataset)
    index = publication.parse_index(json.dumps(index).encode())
    entries = analysis_input_entries(index)
    assert projected_native_official_inputs(entries)
    assert len(publication.receipt_members(index, dataset=dataset)) == 2
    # Explicit conversion of another projected official input disables that route.
    fixture_owner(index, "congress_bills.parquet").pop("etlReceipts")
    with pytest.raises(publication.PublicationError, match="one selected native receipt generation: " + dataset):
        analysis_input_entries(index, convert_published_official_inputs=True)


def test_split_source_and_nonprojected_official_receipts_still_refuse():
    for dataset in ("scorecards", "members", "bill_cosponsors"):
        index = captured_native_index()
        split_receipt_declaration(index, dataset)
        with pytest.raises(publication.PublicationError, match="one selected native receipt generation"):
            analysis_input_entries(index)
