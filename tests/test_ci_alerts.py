"""Offline unit tests for .github/scripts/ci_alerts.py and .github/workflows/ci-alerts.yml."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest
import yaml

REPO_ROOT = Path(__file__).parents[1]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
SCRIPT_PATH = REPO_ROOT / ".github" / "scripts" / "ci_alerts.py"


def _load_ci_alerts() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ci_alerts", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ci_alerts = _load_ci_alerts()


class _FakeGH:
    """In-memory fake for GitHub CLI issue, label, run, and workflow queries."""

    def __init__(
        self,
        *,
        issues: list[dict] | None = None,
        labels: list[str] | None = None,
        jobs: list[dict] | None = None,
        run_history: dict[str, list[dict]] | None = None,
        workflow_states: list[dict] | None = None,
    ) -> None:
        self.issues = list(issues or [])
        self.labels = list(labels or [])
        self.jobs = list(jobs or [])
        self.run_history = dict(run_history or {})
        self.workflow_states = list(
            workflow_states
            or [{"name": name, "state": "active"} for name, _ in ci_alerts.CORE_WORKFLOW_SCHEDULES]
        )
        self.calls: list[tuple[tuple[str, ...], str | None]] = []

    def __call__(self, *args: str, stdin: str | None = None) -> str:
        self.calls.append((args, stdin))
        cmd = args[:2]
        if cmd == ("issue", "list"):
            return json.dumps([item for item in self.issues if item.get("state", "open") == "open"])
        if cmd == ("issue", "create"):
            title = args[args.index("--title") + 1]
            number = max((item["number"] for item in self.issues), default=0) + 1
            self.issues.append({"number": number, "title": title, "state": "open", "comments": [stdin or ""]})
            return ""
        if cmd == ("issue", "comment"):
            number = int(args[2])
            for item in self.issues:
                if item["number"] == number:
                    item.setdefault("comments", []).append(stdin or "")
            return ""
        if cmd == ("issue", "close"):
            number = int(args[2])
            comment = args[args.index("--comment") + 1]
            for item in self.issues:
                if item["number"] == number:
                    item["state"] = "closed"
                    item.setdefault("comments", []).append(comment)
            return ""
        if cmd == ("label", "list"):
            return json.dumps([{"name": name} for name in self.labels])
        if cmd == ("label", "create"):
            self.labels.append(args[2])
            return ""
        if cmd == ("run", "view"):
            return json.dumps({"jobs": self.jobs})
        if cmd == ("run", "list"):
            workflow = args[args.index("--workflow") + 1]
            status = args[args.index("--status") + 1] if "--status" in args else None
            limit = int(args[args.index("--limit") + 1]) if "--limit" in args else 20
            runs = list(self.run_history.get(workflow, []))
            if status == "success":
                runs = [run for run in runs if run.get("conclusion") == "success"]
            elif status == "completed":
                runs = [run for run in runs if run.get("status", "completed") == "completed"]
            return json.dumps(runs[:limit])
        if cmd == ("workflow", "list"):
            return json.dumps(self.workflow_states)
        raise AssertionError(f"Unexpected gh command: {args!r}")


def _workflow_event(
    tmp_path: Path,
    *,
    name: str = "Rollup — bill family",
    run_id: int = 101,
    conclusion: str = "failure",
    head_branch: str = "main",
) -> Path:
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps(
            {
                "workflow_run": {
                    "id": run_id,
                    "run_attempt": 1,
                    "name": name,
                    "conclusion": conclusion,
                    "event": "schedule",
                    "head_branch": head_branch,
                    "head_sha": "abcdef1234567890",
                    "html_url": f"https://github.com/mikewolfd/spicy-regs/actions/runs/{run_id}",
                }
            }
        )
    )
    return event_path


def test_ci_alerts_workflow_references_exact_existing_workflow_names() -> None:
    ci_alerts_doc = yaml.safe_load((WORKFLOW_DIR / "ci-alerts.yml").read_text())
    on_block = ci_alerts_doc[True] if True in ci_alerts_doc else ci_alerts_doc["on"]
    watched = on_block["workflow_run"]["workflows"]
    assert len(watched) == len(set(watched))
    assert len(watched) <= 50

    declared_names = {
        yaml.safe_load(path.read_text())["name"]: path.name
        for path in sorted(WORKFLOW_DIR.glob("*.yml"))
        if path.name != "ci-alerts.yml"
    }
    missing = [name for name in watched if name not in declared_names]
    assert missing == []

    required_core = {
        "Rollup — bill family",
        "Rollup — laws",
        "Refresh congressional votes and terms",
        "Rollup — roll_call_votes",
        "Rollup — amendments",
        "Rollup — nominations",
        "Rollup — committee_reports",
        "Rollup — executive_order_observations",
        "Check published table freshness",
    }
    assert required_core <= set(watched)
    assert ci_alerts_doc["permissions"] == {"contents": "read", "issues": "write", "actions": "read"}
    assert "cron" in on_block["schedule"][0]
    for job_name in ("failure", "recovered", "freshness"):
        runs = "\n".join(step.get("run", "") for step in ci_alerts_doc["jobs"][job_name]["steps"])
        assert f"python3 -I .github/scripts/ci_alerts.py {job_name}" in runs


def test_failure_opens_issue_then_updates_existing_on_consecutive_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeGH(
        jobs=[
            {"name": "run", "conclusion": "failure", "steps": [{"name": "Run rollup", "conclusion": "failure"}]},
        ],
        run_history={
            "Rollup — bill family": [
                {"databaseId": 100, "conclusion": "success", "url": "https://ex/100", "updatedAt": "2026-11-23T02:00:00Z"},
            ]
        },
    )
    monkeypatch.setattr(ci_alerts, "gh", fake)
    monkeypatch.setattr(ci_alerts.Path, "read_text", Path.read_text)

    first = _workflow_event(tmp_path, name="Rollup — bill family", run_id=101, conclusion="failure")
    env = {"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(first)}
    ci_alerts.main(["failure"], env=env)

    assert len(fake.issues) == 1
    issue = fake.issues[0]
    assert issue["title"] == "Rollup — bill family"
    assert ci_alerts.LABEL in fake.labels
    assert "(consecutive failures: 1)" in issue["comments"][0]
    assert "Failed job: run (step: Run rollup)" in issue["comments"][0]
    assert (REPO_ROOT / ci_alerts.RUNBOOK).is_file()

    # Second consecutive failure updates the same issue (with streak=2) instead of opening a duplicate.
    fake.run_history["Rollup — bill family"] = [
        {"databaseId": 102, "conclusion": "timed_out", "url": "https://ex/102", "updatedAt": "2026-11-24T02:00:00Z"},
        {"databaseId": 101, "conclusion": "failure", "url": "https://ex/101", "updatedAt": "2026-11-23T02:00:00Z"},
        {"databaseId": 100, "conclusion": "success", "url": "https://ex/100", "updatedAt": "2026-11-22T02:00:00Z"},
    ]
    second = _workflow_event(tmp_path, name="Rollup — bill family", run_id=102, conclusion="timed_out")
    ci_alerts.main(["failure"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(second)})

    assert len(fake.issues) == 1
    assert len(issue["comments"]) == 2
    assert "(consecutive failures: 2)" in issue["comments"][1]

    # Secondary failure in another workflow still opens its own dedicated issue.
    fake.run_history["Rollup — laws"] = []
    other = _workflow_event(tmp_path, name="Rollup — laws", run_id=201, conclusion="failure")
    ci_alerts.main(["failure"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(other)})
    assert [i["title"] for i in fake.issues] == ["Rollup — bill family", "Rollup — laws"]

    # Recovery closes only the recovered workflow's open issue.
    rec = _workflow_event(tmp_path, name="Rollup — bill family", run_id=103, conclusion="success")
    ci_alerts.main(["recovered"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(rec)})
    assert fake.issues[0]["state"] == "closed"
    assert "Recovered:" in fake.issues[0]["comments"][-1]
    assert fake.issues[1]["state"] == "open"


def test_min_consecutive_waits_for_threshold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeGH(
        jobs=[{"name": "run", "conclusion": "failure", "steps": []}],
        run_history={
            "Rollup — laws": [
                {"databaseId": 10, "conclusion": "success"},
            ]
        },
    )
    monkeypatch.setattr(ci_alerts, "gh", fake)
    first = _workflow_event(tmp_path, name="Rollup — laws", run_id=11)
    env = {"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(first)}

    ci_alerts.main(["failure", "--min-consecutive", "2"], env=env)
    assert fake.issues == []

    fake.run_history["Rollup — laws"] = [
        {"databaseId": 12, "conclusion": "failure"},
        {"databaseId": 11, "conclusion": "failure"},
        {"databaseId": 10, "conclusion": "success"},
    ]
    second = _workflow_event(tmp_path, name="Rollup — laws", run_id=12)
    ci_alerts.main(["failure", "--min-consecutive", "2"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(second)})
    assert len(fake.issues) == 1
    assert "(consecutive failures: 2)" in fake.issues[0]["comments"][0]


def test_freshness_alerts_when_daily_workflow_stale_and_closes_on_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 11, 24, 14, 37, tzinfo=timezone.utc)
    recent = (now - timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    weekday_ok = (now - timedelta(hours=70)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stale_ts = (now - timedelta(hours=35)).strftime("%Y-%m-%dT%H:%M:%SZ")

    history = {
        workflow: [{"url": f"https://ex/{idx}", "updatedAt": recent, "conclusion": "success"}]
        for idx, (workflow, _) in enumerate(ci_alerts.CORE_WORKFLOW_SCHEDULES)
    }
    history["Rollup — executive_order_observations"] = [
        {"url": "https://ex/eo", "updatedAt": weekday_ok, "conclusion": "success"}
    ]
    history["Rollup — bill family"] = [
        {"url": "https://ex/bf", "updatedAt": stale_ts, "conclusion": "failure"}
    ]
    fake = _FakeGH(
        run_history=history,
        workflow_states=[
            {"name": name, "state": "disabled_manually" if name == "Rollup — committee_reports" else "active"}
            for name, _ in ci_alerts.CORE_WORKFLOW_SCHEDULES
        ],
    )
    monkeypatch.setattr(ci_alerts, "gh", fake)

    ci_alerts.main(["freshness"], env={"GH_REPO": "mikewolfd/spicy-regs"}, now=now)
    assert len(fake.issues) == 1
    assert fake.issues[0]["title"] == ci_alerts.STALE_TITLE
    body = fake.issues[0]["comments"][0]
    assert 'Skipped "Rollup — committee_reports"' in body
    assert '35.0 hours ago' in body

    # Once bill family completes freshly, freshness closes the open stale issue.
    fake.run_history["Rollup — bill family"] = [
        {"url": "https://ex/bf2", "updatedAt": recent, "conclusion": "success"}
    ]
    ci_alerts.main(["freshness"], env={"GH_REPO": "mikewolfd/spicy-regs"}, now=now)
    assert fake.issues[0]["state"] == "closed"
    assert "Fresh again." in fake.issues[0]["comments"][-1]
