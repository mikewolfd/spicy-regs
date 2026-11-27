"""Offline unit tests for .github/scripts/ci_alerts.py and .github/workflows/ci-alerts.yml."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
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
        jobs_by_run: dict[int, list[dict]] | None = None,
        run_history: dict[str, list[dict]] | None = None,
        workflow_states: list[dict] | None = None,
    ) -> None:
        self.issues = list(issues or [])
        self.labels = list(labels or [])
        self.jobs = list(jobs or [])
        self.jobs_by_run = dict(jobs_by_run or {})
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
            run_id = int(args[2])
            return json.dumps({"jobs": self.jobs_by_run.get(run_id, self.jobs)})
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
        "Check comments freshness",
        "Check published table freshness",
        "ETL (new pipeline)",
        "Publish comments mirror",
        "Materialize — rulemaking join surface",
        "Rollup — bill family",
        "Rollup — laws",
        "Refresh congressional votes and terms",
        "Rollup — roll_call_votes",
        "Rollup — amendments",
        "Rollup — nominations",
        "Rollup — committee_reports",
        "Rollup — executive_order_observations",
    }
    assert required_core <= set(watched)
    core_schedule_names = {name for name, _ in ci_alerts.CORE_WORKFLOW_SCHEDULES}
    assert {
        "Check published table freshness",
        "Check comments freshness",
        "ETL (new pipeline)",
        "Publish comments mirror",
        "Materialize — rulemaking join surface",
    } <= core_schedule_names
    assert core_schedule_names <= set(watched)
    for workflow_name, providers in ci_alerts.PUBLISHING_PROVIDERS.items():
        assert workflow_name in core_schedule_names
        for provider_wf, required_jobs in providers:
            assert provider_wf in declared_names
            assert required_jobs
    for workflow_name in ci_alerts.RECOVERY_REQUIRED_JOBS:
        assert workflow_name in watched
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


def _full_etl_publish_jobs() -> list[dict]:
    return [
        {"name": "setup", "conclusion": "success"},
        {"name": "etl-new", "conclusion": "success"},
        {"name": "refresh-consumers / mirror / publish", "conclusion": "success"},
        {"name": "refresh-consumers / rulemaking / materialize", "conclusion": "success"},
        {"name": "refresh-consumers / verify", "conclusion": "success"},
    ]


def _skipped_etl_retry_jobs() -> list[dict]:
    return [
        {"name": "setup", "conclusion": "success"},
        {"name": "etl-new", "conclusion": "skipped"},
        {"name": "refresh-consumers", "conclusion": "skipped"},
    ]


def _fresh_core_history(now: datetime) -> tuple[dict[str, list[dict]], dict[int, list[dict]]]:
    recent = (now - timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    weekday_ok = (now - timedelta(hours=70)).strftime("%Y-%m-%dT%H:%M:%SZ")
    history: dict[str, list[dict]] = {}
    for idx, (workflow, _) in enumerate(ci_alerts.CORE_WORKFLOW_SCHEDULES, start=1000):
        if workflow in {"Publish comments mirror", "Materialize — rulemaking join surface"}:
            # Normal day: no standalone manual dispatch needed when ETL ran both via workflow_call.
            history[workflow] = []
        else:
            history[workflow] = [{"databaseId": idx, "url": f"https://ex/{idx}", "updatedAt": recent, "conclusion": "success"}]
    history["Rollup — executive_order_observations"] = [
        {"databaseId": 2001, "url": "https://ex/eo", "updatedAt": weekday_ok, "conclusion": "success"}
    ]
    etl_run_id = history["ETL (new pipeline)"][0]["databaseId"]
    jobs_by_run = {etl_run_id: _full_etl_publish_jobs()}
    return history, jobs_by_run


def test_freshness_alerts_when_daily_workflow_stale_and_closes_on_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 11, 24, 14, 37, tzinfo=timezone.utc)
    recent = (now - timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stale_ts = (now - timedelta(hours=35)).strftime("%Y-%m-%dT%H:%M:%SZ")

    history, jobs_by_run = _fresh_core_history(now)
    history["Rollup — bill family"] = [
        {"databaseId": 3001, "url": "https://ex/bf", "updatedAt": stale_ts, "conclusion": "failure"}
    ]
    fake = _FakeGH(
        run_history=history,
        jobs_by_run=jobs_by_run,
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
    assert 'via "ETL (new pipeline)"' in body

    # Once bill family completes freshly, freshness closes the open stale issue (and comments/rulemaking via ETL stay fresh).
    fake.run_history["Rollup — bill family"] = [
        {"databaseId": 3002, "url": "https://ex/bf2", "updatedAt": recent, "conclusion": "success"}
    ]
    ci_alerts.main(["freshness"], env={"GH_REPO": "mikewolfd/spicy-regs"}, now=now)
    assert fake.issues[0]["state"] == "closed"
    assert "Fresh again." in fake.issues[0]["comments"][-1]


def test_freshness_flags_green_runs_that_skipped_publishing_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 11, 26, 14, 37, tzinfo=timezone.utc)
    recent_skip = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stale_pub = (now - timedelta(hours=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
    fresh_pub = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    history, jobs_by_run = _fresh_core_history(now)
    # ETL has a green run 2h ago that skipped publishing, and its last real publish was 40h ago.
    history["ETL (new pipeline)"] = [
        {"databaseId": 5002, "url": "https://ex/etl-skip", "updatedAt": recent_skip, "conclusion": "success"},
        {"databaseId": 5001, "url": "https://ex/etl-old", "updatedAt": stale_pub, "conclusion": "success"},
    ]
    # Publish comments mirror also has a green dry-run (skip_upload=true -> verify skipped) 2h ago.
    history["Publish comments mirror"] = [
        {"databaseId": 5003, "url": "https://ex/mirror-dry", "updatedAt": recent_skip, "conclusion": "success"},
    ]
    jobs_by_run[5002] = _skipped_etl_retry_jobs()
    jobs_by_run[5001] = _full_etl_publish_jobs()
    jobs_by_run[5003] = [
        {"name": "refresh / mirror / publish", "conclusion": "success"},
        {"name": "refresh / verify", "conclusion": "skipped"},
    ]

    fake = _FakeGH(run_history=history, jobs_by_run=jobs_by_run)
    monkeypatch.setattr(ci_alerts, "gh", fake)

    ci_alerts.main(["freshness"], env={"GH_REPO": "mikewolfd/spicy-regs"}, now=now)
    assert len(fake.issues) == 1
    assert fake.issues[0]["title"] == ci_alerts.STALE_TITLE
    body = fake.issues[0]["comments"][0]
    assert "40.0 hours ago" in body
    assert 'newer green "ETL (new pipeline)" run https://ex/etl-skip skipped publishing' in body
    assert 'newer green "Publish comments mirror" run https://ex/mirror-dry skipped publishing' in body

    # Once a real ETL publishing run succeeds, all three (ETL, comments mirror, rulemaking) recover and close the alert.
    fake.run_history["ETL (new pipeline)"].insert(
        0, {"databaseId": 5004, "url": "https://ex/etl-fresh", "updatedAt": fresh_pub, "conclusion": "success"}
    )
    fake.jobs_by_run[5004] = _full_etl_publish_jobs()
    ci_alerts.main(["freshness"], env={"GH_REPO": "mikewolfd/spicy-regs"}, now=now)
    assert fake.issues[0]["state"] == "closed"
    assert "Fresh again." in fake.issues[0]["comments"][-1]


def test_green_run_that_skipped_publishing_does_not_close_open_alert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fake = _FakeGH(
        issues=[
            {"number": 42, "title": "ETL (new pipeline)", "state": "open", "comments": ["initial"]},
            {"number": 43, "title": "Publish explorer metadata", "state": "open", "comments": ["initial"]},
        ],
        jobs_by_run={
            6001: _skipped_etl_retry_jobs(),
            6002: _full_etl_publish_jobs(),
            6003: [{"name": "preflight", "conclusion": "success"}, {"name": "publish", "conclusion": "skipped"}],
            6004: [{"name": "preflight", "conclusion": "success"}, {"name": "publish", "conclusion": "success"}],
        },
    )
    monkeypatch.setattr(ci_alerts, "gh", fake)

    # Skipped ETL retry must NOT close #42.
    ev_skip = _workflow_event(tmp_path, name="ETL (new pipeline)", run_id=6001, conclusion="success")
    ci_alerts.main(["recovered"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(ev_skip)})
    assert fake.issues[0]["state"] == "open"
    assert "Keeping #42 open" in capsys.readouterr().out

    # Real ETL publishing run closes #42.
    ev_ok = _workflow_event(tmp_path, name="ETL (new pipeline)", run_id=6002, conclusion="success")
    ci_alerts.main(["recovered"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(ev_ok)})
    assert fake.issues[0]["state"] == "closed"

    # Preflight-skipped explorer metadata run must NOT close #43.
    ev_exp_skip = _workflow_event(tmp_path, name="Publish explorer metadata", run_id=6003, conclusion="success")
    ci_alerts.main(["recovered"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(ev_exp_skip)})
    assert fake.issues[1]["state"] == "open"
    assert "Keeping #43 open" in capsys.readouterr().out

    # Real explorer metadata publish job closes #43.
    ev_exp_ok = _workflow_event(tmp_path, name="Publish explorer metadata", run_id=6004, conclusion="success")
    ci_alerts.main(["recovered"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(ev_exp_ok)})
    assert fake.issues[1]["state"] == "closed"


def test_check_rollup_freshness_stagnant_pointer_failure_alerts_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeGH(
        jobs_by_run={
            7001: [
                {
                    "name": "check",
                    "conclusion": "failure",
                    "steps": [
                        {"name": "Restore row-count history", "conclusion": "success"},
                        {"name": "Check published tables", "conclusion": "failure"},
                        {"name": "Save row-count history", "conclusion": "success"},
                    ],
                }
            ]
        },
        run_history={
            "Check published table freshness": [
                {"databaseId": 7000, "conclusion": "success", "url": "https://ex/7000", "updatedAt": "2026-11-25T23:55:00Z"},
            ]
        },
    )
    monkeypatch.setattr(ci_alerts, "gh", fake)

    fail_ev = _workflow_event(tmp_path, name="Check published table freshness", run_id=7001, conclusion="failure")
    ci_alerts.main(["failure"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(fail_ev)})
    assert len(fake.issues) == 1
    assert fake.issues[0]["title"] == "Check published table freshness"
    assert "Failed job: check (step: Check published tables)" in fake.issues[0]["comments"][0]

    rec_ev = _workflow_event(tmp_path, name="Check published table freshness", run_id=7002, conclusion="success")
    ci_alerts.main(["recovered"], env={"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(rec_ev)})
    assert fake.issues[0]["state"] == "closed"
    assert "Recovered:" in fake.issues[0]["comments"][-1]


@pytest.mark.parametrize("mode", ["failure", "recovered", "freshness"])
def test_gh_disabled_issues_fails_loudly_with_error_annotation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, mode: str
) -> None:
    def _disabled_run(cmd, *args, **kwargs):
        if cmd[:3] == ["gh", "workflow", "list"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")
        if cmd[:3] == ["gh", "run", "list"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")
        if cmd[:3] == ["gh", "run", "view"]:
            return subprocess.CompletedProcess(cmd, 0, stdout='{"jobs": []}', stderr="")
        raise subprocess.CalledProcessError(
            1,
            cmd,
            stderr="the 'mikewolfd/spicy-regs' repository has disabled issues\n",
        )

    monkeypatch.setattr(ci_alerts.subprocess, "run", _disabled_run)
    ev = _workflow_event(tmp_path, name="Rollup — laws", run_id=301, conclusion="failure" if mode == "failure" else "success")
    env = {"GH_REPO": "mikewolfd/spicy-regs", "GITHUB_EVENT_PATH": str(ev)}
    with pytest.raises(SystemExit) as excinfo:
        ci_alerts.main([mode], env=env, now=datetime(2026, 11, 26, 14, 37, tzinfo=timezone.utc))
    assert excinfo.value.code == 1
    assert isinstance(excinfo.value.__cause__, ci_alerts.IssuesDisabledError)
    captured = capsys.readouterr()
    assert "::error title=CI alerts delivery failed::" in captured.out
    assert "GitHub Issues are disabled on mikewolfd/spicy-regs" in captured.out
    assert "has disabled issues" in captured.err
