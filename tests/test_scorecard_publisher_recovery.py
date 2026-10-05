"""Recovery aggregation refuses incomplete coverage and altered source metadata."""

from copy import deepcopy
from hashlib import sha256
import importlib.util
import json
from pathlib import Path

import pytest


SPEC = importlib.util.spec_from_file_location(
    "publisher_survey", Path(__file__).resolve().parents[1] / "scripts/build_scorecard_publisher_survey.py"
)
assert SPEC is not None and SPEC.loader is not None
survey = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(survey)


@pytest.fixture
def recovery(tmp_path):
    def pin(name, value):
        path = tmp_path / name
        path.write_text(json.dumps(value))
        return {"path": str(path), "sha256": sha256(path.read_bytes()).hexdigest()}

    original = pin(
        "result.json", {"task_result": "original-recovered", "periods": {"latest": "2008", "unknown": ["2009 onward"]}}
    )
    report = pin("report.md", "Original recovered; latest overall unknown")
    source = pin("input.json", {"publisher_id": "example"})
    dispatch = pin(
        "dispatch.json",
        {
            "tasks": [
                {
                    "publisher_id": "example",
                    "threadId": "task",
                    "input_path": source["path"],
                    "input_sha256": source["sha256"],
                }
            ],
            "final_reports": [
                {
                    "publisher_id": "example",
                    "threadId": "task",
                    "latest_turn_status": "completed",
                    "result": {**original, "path": "result.json"},
                    "report": {**report, "path": "report.md"},
                }
            ],
        },
    )
    return {
        "format_version": "scorecard-publisher-recovery-aggregate/1",
        "observed_on": "2026-10-05",
        "inputs": {"dispatch": dispatch},
        "expected_publishers": ["example"],
        "summary": {"assigned_publishers": 1, "newly_qualified_editions": 0, "newly_published_editions": 0},
        "publishers": [
            {
                "publisher_id": "example",
                "publisher_name": "Example",
                "source_input": source,
                "result": original,
                "report": report,
                "reported_research_result": {"task_result": "original-recovered"},
                "finding": "2008 original recovered; later range unknown",
                "next_action": "Qualify the original",
                "recovery": {"periods": {"latest": "2008", "unknown": ["2009 onward"]}},
                "source_field_names": {"recovery.periods": "/periods"},
            }
        ],
    }


def test_recovery_preserves_literal_periods_and_remains_research(recovery):
    checked = survey.verify_recovery(recovery)
    assert checked["publishers"][0]["recovery"]["periods"]["unknown"] == ["2009 onward"]
    assert "qualifies and publishes no editions" in survey.render_recovery(checked)


def test_api_recovery_verifies_retained_responses_without_promoting_them(recovery, tmp_path):
    recovery["format_version"] = "scorecard-publisher-recovery-api/1"
    recovery["summary"]["reviewed_publishers"] = recovery["summary"].pop("assigned_publishers")
    entry = recovery["publishers"][0]
    entry["source_result"] = entry.pop("result")
    entry["source_report"] = entry.pop("report")
    body = tmp_path / "response.body"
    body.write_bytes(b"<html>Original article; rating API unverified</html>")
    entry["endpoints"] = [
        {
            "evidence": {"metadata": entry["source_result"], "locator": ""},
            "independent_inspection": {
                "status": "retained_raw_read",
                "body_path": str(body),
                "sha256": sha256(body.read_bytes()).hexdigest(),
            },
        }
    ]
    survey.verify_recovery(recovery)
    body.write_bytes(b"changed")
    with pytest.raises(ValueError, match="Input pin differs"):
        survey.verify_recovery(recovery)


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "duplicate",
        "changed_pin",
        "changed_literal",
        "missing_result",
        "wrong_prefix",
        "unmapped",
        "promoted",
    ],
)
def test_recovery_refuses_missing_or_changed_authority(recovery, change):
    entry = recovery["publishers"][0]
    if change == "missing":
        recovery["publishers"] = []
    elif change == "duplicate":
        recovery["publishers"].append(deepcopy(entry))
    elif change == "changed_pin":
        Path(entry["result"]["path"]).write_text("{}")
    elif change == "changed_literal":
        entry["recovery"]["periods"]["latest"] = "2025"
    elif change == "missing_result":
        entry["reported_research_result"] = {}
    elif change == "wrong_prefix":
        entry["periods"] = deepcopy(entry["recovery"]["periods"])
        entry["source_field_names"] = {"periods": "/periods"}
        entry["recovery"]["periods"]["latest"] = "2025"
    elif change == "unmapped":
        entry["recovery"]["unverified"] = "invented metadata"
    else:
        recovery["summary"]["newly_qualified_editions"] = 1
    with pytest.raises(ValueError):
        survey.verify_recovery(recovery)
