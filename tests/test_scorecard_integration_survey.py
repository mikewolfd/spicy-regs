"""Discovery findings guide work without establishing source qualification."""

from copy import deepcopy
from hashlib import sha256
import importlib.util
import json
from pathlib import Path

import pytest


SPEC = importlib.util.spec_from_file_location(
    "scorecard_integrations", Path(__file__).resolve().parents[1] / "scripts/build_scorecard_integrations.py"
)
assert SPEC is not None and SPEC.loader is not None
integrations = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(integrations)


@pytest.fixture
def directory(tmp_path):
    source = dict(
        publisher_name="Example publisher",
        task_id="example-task",
        support_status="unverified",
        original_source_urls=["https://publisher.example"],
        next_action="Find a source",
    )
    documents = {
        "adapter_inventory.json": dict(publishers={"example": source}),
        "publisher_api_inventory.json": dict(
            publishers=[
                dict(publisher_id="example", discovery_status="unverified", endpoints=[]),
            ]
        ),
        "integration_qualifications.json": dict(observed_at="2026-10-05T00:00:00Z", editions=[], readers={}),
        "publisher_survey_20261005.json": dict(
            format_version="scorecard-publisher-survey-aggregate/1",
            observed_on="2026-10-05",
            summary=dict(surveyed_publishers=1, newly_qualified_editions=0),
            publishers=[
                dict(
                    publisher_id="example",
                    task_id="example-task",
                    assessment=dict(category="reader_candidate", finding="Original table recovered"),
                    survey=dict(
                        next_step="Qualify the retained original table",
                        newest_recovered="2025",
                        latest_publication="unknown",
                        latest_offered="2025",
                    ),
                )
            ],
        ),
    }
    for name, value in documents.items():
        (tmp_path / name).write_text(json.dumps(value))
    return tmp_path


def test_survey_lead_updates_work_queue_without_promoting_support(directory):
    report, markdown = integrations.generate(directory)
    row = report["publishers"][0]
    assert row["state"] == "unverified"
    assert row["reader"] is None and row["qualified_editions"] == []
    assert row["original_source_urls"] == ["https://publisher.example"]
    assert row["next_action"] == "Qualify the retained original table"
    assert row["survey_finding"]["assessment"]["finding"] == "Original table recovered"
    assert row["survey_finding"]["latest_publication"] == "unknown"
    assert "publisher_survey_20261005.md" in markdown
    assert (
        report["input_pins"]["publisher_survey_20261005.json"]
        == sha256((directory / "publisher_survey_20261005.json").read_bytes()).hexdigest()
    )


def test_discovery_does_not_replace_implemented_reader_next_step(directory):
    path = directory / "integration_qualifications.json"
    data = json.loads(path.read_bytes())
    data["readers"]["example"] = "example_reader"
    path.write_text(json.dumps(data))
    report, _ = integrations.generate(directory)
    row = report["publishers"][0]
    assert row["state"] == "reader_implemented"
    assert row["next_action"] == "Qualify original source through the implemented reader"
    assert row["survey_finding"]["next_step"] == "Qualify the retained original table"


@pytest.mark.parametrize("change", ["duplicate", "unknown", "qualified", "unsupported_format"])
def test_invalid_discovery_aggregate_is_refused(directory, change):
    path = directory / "publisher_survey_20261005.json"
    data = json.loads(path.read_bytes())
    if change == "duplicate":
        data["publishers"].append(deepcopy(data["publishers"][0]))
        data["summary"]["surveyed_publishers"] = 2
    elif change == "unknown":
        data["publishers"][0]["publisher_id"] = "unregistered"
    elif change == "qualified":
        data["summary"]["newly_qualified_editions"] = 1
    else:
        data["format_version"] = "unknown/1"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="Survey"):
        integrations.generate(directory)


def test_older_inventory_without_survey_still_generates(directory):
    (directory / "publisher_survey_20261005.json").unlink()
    report, _ = integrations.generate(directory)
    assert report["publishers"][0]["next_action"] == "Find a source"
    assert report["publishers"][0]["survey_finding"] is None


def implementation_document(directory, result="implemented"):
    document = {
        "format_version": "scorecard-publisher-implementation-aggregate/1",
        "observed_on": "2026-10-05",
        "summary": {"assigned_publishers": 1, "newly_published_editions": 0},
        "publishers": [
            {
                "publisher_id": "example",
                "task_result": result,
                "integration_state": "independent_reader_review_pending",
                "period_fields": {"newest_recovered": "2025"},
                "next_missing_item": "Independently compare the exact original action table",
                "source_document": {"path": "/private/implementation.json", "sha256": "a" * 64},
                "source_manifest": {"path": "/private/handoff.json", "sha256": "b" * 64},
                "group": "example-group",
            }
        ],
    }
    path = directory / "publisher_implementation_20261005.json"
    path.write_text(json.dumps(document))
    return path


def test_completed_implementation_guides_queue_without_qualifying_it(directory):
    path = implementation_document(directory)
    report, markdown = integrations.generate(directory)
    row = report["publishers"][0]
    assert row["state"] == "unverified"
    assert row["reader"] is None and row["qualified_editions"] == []
    assert row["implementation_finding"]["task_result"] == "implemented"
    assert row["next_action"] == "Independently compare the exact original action table"
    assert report["input_pins"][path.name] == sha256(path.read_bytes()).hexdigest()
    assert "publisher_implementation_20261005.md" in markdown


def test_task_metadata_preserves_qualified_reader_priority(directory):
    implementation_document(directory, "original-unrecovered")
    path = directory / "integration_qualifications.json"
    data = json.loads(path.read_bytes())
    data["readers"]["example"] = "example_reader"
    path.write_text(json.dumps(data))
    report, _ = integrations.generate(directory)
    row = report["publishers"][0]
    assert row["state"] == "reader_implemented"
    assert row["next_action"] == "Qualify original source through the implemented reader"
    assert row["implementation_finding"]["task_result"] == "original-unrecovered"


@pytest.mark.parametrize("change", ["duplicate", "unknown", "published", "unknown_status", "wrong_format"])
def test_invalid_implementation_metadata_is_refused(directory, change):
    path = implementation_document(directory)
    data = json.loads(path.read_text())
    if change == "duplicate":
        data["publishers"].append(deepcopy(data["publishers"][0]))
        data["summary"]["assigned_publishers"] = 2
    elif change == "unknown":
        data["publishers"][0]["publisher_id"] = "unknown"
    elif change == "published":
        data["summary"]["newly_published_editions"] = 1
    elif change == "unknown_status":
        data["publishers"][0]["task_result"] = "production_supported"
    else:
        data["format_version"] = "unknown/1"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="Implementation"):
        integrations.generate(directory)
