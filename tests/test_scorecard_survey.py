"""Research status must not outrun its evidence or hide unsupported shapes."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("scorecard_survey", ROOT / "scripts/build_scorecard_survey.py")
assert SPEC is not None and SPEC.loader is not None
survey = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(survey)


@pytest.fixture
def data():
    return {name: json.loads((survey.DEFAULT_DIRECTORY / name).read_text()) for name in survey.INPUTS}


def test_research_inputs_and_reports_are_consistent():
    outputs = survey.generate(survey.DEFAULT_DIRECTORY)
    for name, content in outputs.items():
        assert (survey.DEFAULT_DIRECTORY / name).read_text() == content


@pytest.mark.parametrize("status", ["verified", "profiled"])
def test_error_response_cannot_verify_a_source(data, status):
    source = next(s for s in data["scorecard_source_catalog.json"]["sources"] if s["publisher_id"] == "nfib")
    source["discovery_status"] = status
    source["last_verified_at"] = "2026-10-03T00:00:00+00:00"
    with pytest.raises(ValueError, match="No successful original-publisher capture"):
        survey.validate(data)


def test_unsupported_profile_requires_a_named_disposition(data):
    profile = next(p for p in data["shape_profiles.json"]["profiles"] if p["schema_fit"] == "unsupported")
    profile["unsupported_shapes"] = []
    with pytest.raises(ValueError, match="Unsupported disposition needs a reason"):
        survey.validate(data)


def test_literal_sample_requires_a_capture_in_its_profile(data):
    profile = next(p for p in data["shape_profiles.json"]["profiles"] if p["literal_examples"])
    profile["literal_examples"][0]["capture_id"] = "missing-capture"
    with pytest.raises(ValueError, match="Unbound sample"):
        survey.validate(data)


def test_duplicate_publisher_series_is_refused(data):
    sources = data["scorecard_source_catalog.json"]["sources"]
    sources.append(deepcopy(sources[0]))
    with pytest.raises(ValueError, match="duplicate source_id"):
        survey.validate(data)


def test_complete_http_read_does_not_establish_a_complete_scorecard(data):
    capture = next(c for c in data["capture_receipts.json"]["captures"] if c["capture_complete"])
    capture["source_snapshot_complete"] = True
    with pytest.raises(ValueError, match="Unqualified complete snapshot"):
        survey.validate(data)


def test_research_cannot_promote_a_production_adapter(data):
    data["scorecard_source_catalog.json"]["sources"][0]["discovery_status"] = "supported"
    with pytest.raises(ValueError, match="cannot establish production support"):
        survey.validate(data)


def test_row_example_cannot_invent_a_numeric_literal(data):
    case = next(c for c in data["schema_examples.json"]["cases"] if c["profile_id"] == "humane_world_action:2025")
    case["rows"]["scorecard_member_ratings"][0]["value_text"] = "100"
    with pytest.raises(ValueError, match="Changed literal source value"):
        survey.validate(data)


def test_row_example_cannot_lose_its_member(data):
    case = next(c for c in data["schema_examples.json"]["cases"] if c["profile_id"] == "ntu:2024")
    case["rows"]["scorecard_members"] = []
    with pytest.raises(ValueError, match="Orphan example reference"):
        survey.validate(data)


def test_every_profile_requires_rows_or_an_unsupported_disposition(data):
    data["schema_examples.json"]["cases"].pop()
    with pytest.raises(ValueError, match="Every profile needs an example"):
        survey.validate(data)


def test_reproduction_limit_does_not_make_source_model_unsupported(data):
    profile = next(p for p in data["shape_profiles.json"]["profiles"] if p["profile_id"] == "ntu:2024")
    assert profile["schema_fit"] == "fits_proposal"
    assert profile["reproduction_readiness"] == "not_qualified"
    assert "U03" in profile["unsupported_shapes"]
    survey.validate(data)


def test_readiness_dimensions_cannot_disagree(data):
    data["shape_profiles.json"]["profiles"][0]["source_model_fit"] = "unsupported"
    with pytest.raises(ValueError, match="Source-model disposition drift"):
        survey.validate(data)


def test_schema_cannot_freeze_without_accepted_gate(data):
    data["proposed_schema.json"]["frozen"] = True
    data["work/gate/freeze_receipt.json"]["accepted"] = False
    with pytest.raises(ValueError, match="requires an accepted gate receipt"):
        survey.validate(data)
