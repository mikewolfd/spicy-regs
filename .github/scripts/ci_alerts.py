#!/usr/bin/env python3
"""Keep one GitHub issue per unattended rollup or freshness workflow alert.

  failure    a watched workflow failed on main: compute consecutive failures and open its
             ci-failure issue or append a streak update comment on the open issue
  recovered  it succeeded on main again: comment on its open issue and close it
  freshness  verify core scheduled rollups and table freshness ran within budget; open or
             update "Nightly rollups are stale" when overdue, close once fresh

failure and recovered read the workflow_run payload at GITHUB_EVENT_PATH. gh reads
GH_TOKEN and GH_REPO. Standard library only, so the job runs cleanly under python3 -I.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

LABEL = "ci-failure"
STALE_TITLE = "Nightly rollups are stale"
STALE_AFTER = timedelta(hours=30)
WEEKDAY_STALE_AFTER = timedelta(hours=80)
PASSED = frozenset({"success", "skipped", "neutral"})
RUNBOOK = "docs/publication-status.md"

CORE_WORKFLOW_SCHEDULES: tuple[tuple[str, timedelta], ...] = (
    ("Check published table freshness", STALE_AFTER),
    ("Check comments freshness", STALE_AFTER),
    ("ETL (new pipeline)", STALE_AFTER),
    ("Publish comments mirror", STALE_AFTER),
    ("Materialize — rulemaking join surface", STALE_AFTER),
    ("Rollup — bill family", STALE_AFTER),
    ("Rollup — laws", STALE_AFTER),
    ("Refresh congressional votes and terms", STALE_AFTER),
    ("Rollup — amendments", STALE_AFTER),
    ("Rollup — nominations", STALE_AFTER),
    ("Rollup — committee_reports", STALE_AFTER),
    ("Rollup — executive_order_observations", WEEKDAY_STALE_AFTER),
)

#: Workflows whose runs can conclude ``success`` while skipping publishing (e.g. an 18:25 retry
#: no-op or a ``skip_upload=true`` dispatch) or whose daily execution runs inside a parent
#: workflow via ``workflow_call``. Each entry lists ``(provider_workflow, required_jobs)`` pairs
#: where every required job must conclude ``success`` (not ``skipped``) to count as published.
PUBLISHING_PROVIDERS: Mapping[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "ETL (new pipeline)": (
        ("ETL (new pipeline)", ("etl-new", "refresh-consumers / verify")),
    ),
    "Publish comments mirror": (
        ("Publish comments mirror", ("refresh / mirror / publish", "refresh / verify")),
        ("ETL (new pipeline)", ("refresh-consumers / mirror / publish", "refresh-consumers / verify")),
    ),
    "Materialize — rulemaking join surface": (
        ("Materialize — rulemaking join surface", ("materialize",)),
        ("ETL (new pipeline)", ("refresh-consumers / rulemaking / materialize",)),
    ),
}

#: Required publishing jobs that must conclude ``success`` before a green ``workflow_run``
#: closes an open ``ci-failure`` alert for the workflow.
RECOVERY_REQUIRED_JOBS: Mapping[str, tuple[str, ...]] = {
    "ETL (new pipeline)": ("etl-new", "refresh-consumers / verify"),
    "Publish comments mirror": ("refresh / mirror / publish", "refresh / verify"),
    "Materialize — rulemaking join surface": ("materialize",),
    "Publish explorer metadata": ("publish",),
}

PROVIDER_RUN_SCAN_LIMIT = 10


class IssuesDisabledError(RuntimeError):
    """Raised when the target repository has GitHub Issues disabled."""


def gh(*args: str, stdin: str | None = None) -> str:
    """Run gh; its errors reach the job log and fail the job."""
    try:
        return subprocess.run(
            ["gh", *args],
            input=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        if exc.stderr:
            sys.stderr.write(exc.stderr)
            if "has disabled issues" in exc.stderr:
                raise IssuesDisabledError(exc.stderr.strip()) from exc
        raise


def open_issue(title: str) -> int | None:
    """Return the issue number of the open ci-failure issue with exact ``title``."""
    # The list endpoint, not search: a just-created issue is listed at once.
    issues = json.loads(
        gh(
            "issue",
            "list",
            "--state",
            "open",
            "--label",
            LABEL,
            "--limit",
            "200",
            "--json",
            "number,title",
        )
    )
    return next((issue["number"] for issue in issues if issue["title"] == title), None)


def report(title: str, body: str, *, env: Mapping[str, str] = os.environ) -> None:
    """Comment on the open issue with ``title``, or create the issue if none is open."""
    repository = f"{env.get('GITHUB_SERVER_URL', 'https://github.com')}/{env['GH_REPO']}"
    body += f"\nWhat to do: {repository}/blob/main/{RUNBOOK}\n"
    number = open_issue(title)
    if number is not None:
        gh("issue", "comment", str(number), "--body-file", "-", stdin=body)
        return
    labels = json.loads(gh("label", "list", "--limit", "200", "--json", "name"))
    if LABEL not in {label["name"] for label in labels}:
        gh(
            "label",
            "create",
            LABEL,
            "--force",
            "--color",
            "B60205",
            "--description",
            "An unattended workflow failed on main or went stale",
        )
    gh("issue", "create", "--title", title, "--label", LABEL, "--body-file", "-", stdin=body)


def run_jobs(run_id: int | str, attempt: int | str | None = None) -> list[dict]:
    """Return jobs for a workflow run."""
    cmd = ["run", "view", str(run_id)]
    if attempt is not None:
        cmd.extend(["--attempt", str(attempt)])
    cmd.extend(["--json", "jobs"])
    return json.loads(gh(*cmd))["jobs"]


def _jobs_succeeded(jobs: list[dict], required_jobs: tuple[str, ...]) -> tuple[bool, list[str]]:
    """Return ``(ok, non_success_reasons)`` requiring every named job to conclude ``success``."""
    conclusions = {job["name"]: job.get("conclusion") for job in jobs}
    missing = [
        f"{name}={conclusions.get(name) or 'missing'}"
        for name in required_jobs
        if conclusions.get(name) != "success"
    ]
    return (not missing, missing)


def recent_completed_runs(workflow: str, branch: str = "main", *, limit: int = 20) -> list[dict]:
    """Return recent completed runs on ``branch`` sorted newest-first."""
    return json.loads(
        gh(
            "run",
            "list",
            "--workflow",
            workflow,
            "--branch",
            branch,
            "--status",
            "completed",
            "--limit",
            str(limit),
            "--json",
            "databaseId,conclusion,url,updatedAt,attempt",
        )
    )


def consecutive_failures(workflow: str, branch: str, current_run: dict) -> int:
    """Count consecutive non-passing runs ending at ``current_run`` on ``branch``."""
    runs = recent_completed_runs(workflow, branch=branch)
    current_id = current_run.get("id")
    seen: list[dict] = []
    if current_id is not None and not any(item.get("databaseId") == current_id for item in runs):
        seen.append(
            {
                "databaseId": current_id,
                "conclusion": current_run.get("conclusion"),
            }
        )
    seen.extend(runs)
    streak = 0
    for item in seen:
        conclusion = item.get("conclusion")
        if conclusion in PASSED or conclusion == "cancelled":
            break
        streak += 1
    return max(streak, 1)


def failure(run: dict, *, min_consecutive: int = 1, env: Mapping[str, str] = os.environ) -> None:
    """Open or update the ci-failure issue for a failed/timed-out workflow_run."""
    branch = run.get("head_branch") or "main"
    streak = consecutive_failures(run["name"], branch, run)
    if streak < min_consecutive:
        print(
            f"{run['name']} failed on {branch} ({streak}/{min_consecutive} consecutive); waiting before opening issue."
        )
        return
    jobs = run_jobs(run["id"], run.get("run_attempt"))
    lines = [
        f"**{run['name']}** concluded `{run['conclusion']}` on `{branch}` (consecutive failures: {streak}).",
        "",
        f"- Run: {run['html_url']}",
        f"- Event: `{run['event']}`",
        f"- Head SHA: `{run['head_sha']}`",
    ]
    failed = [job for job in jobs if job["conclusion"] not in PASSED]
    for job in failed:
        steps = [
            step["name"]
            for step in job.get("steps", [])
            if step.get("conclusion") and step["conclusion"] not in PASSED
        ]
        lines.append(f"- Failed job: {job['name']}" + (f" (step: {'; '.join(steps)})" if steps else ""))
    if not failed:
        lines.append("- No job reported a failure; open the run for details.")
    report(run["name"], "\n".join(lines) + "\n", env=env)


def recovered(run: dict) -> None:
    """Close the workflow's open ci-failure issue once a real publishing/check run succeeds on main."""
    number = open_issue(run["name"])
    if number is None:
        print(f"No open {LABEL} issue for {run['name']}.")
        return
    required = RECOVERY_REQUIRED_JOBS.get(run["name"])
    if required is not None:
        jobs = run_jobs(run["id"], run.get("run_attempt"))
        ok, missing = _jobs_succeeded(jobs, required)
        if not ok:
            print(
                f"Keeping #{number} open for {run['name']}: {run['html_url']} concluded success "
                f"but skipped publishing ({', '.join(missing)})."
            )
            return
    branch = run.get("head_branch") or "main"
    gh(
        "issue",
        "close",
        str(number),
        "--comment",
        f"Recovered: {run['html_url']} succeeded on {branch} at `{run['head_sha']}`.",
    )


def workflow_states() -> dict[str, str]:
    """Return GitHub workflow states keyed by workflow name."""
    entries = json.loads(gh("workflow", "list", "--all", "--limit", "200", "--json", "name,state"))
    return {entry["name"]: entry["state"] for entry in entries}


def newest(workflow: str, status: str) -> dict | None:
    """Return the most recent ``status`` run on main for ``workflow``."""
    runs = json.loads(
        gh(
            "run",
            "list",
            "--workflow",
            workflow,
            "--branch",
            "main",
            "--status",
            status,
            "--limit",
            "1",
            "--json",
            "url,updatedAt,conclusion",
        )
    )
    return runs[0] if runs else None


def _parse_timestamp(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _evaluate_publishing_workflow(
    workflow: str,
    max_age: timedelta,
    now: datetime,
    *,
    runs_cache: dict[str, list[dict]],
    jobs_cache: dict[tuple[int, int | None], list[dict]],
) -> tuple[bool, str]:
    """Check whether any provider workflow ran its required publishing jobs within ``max_age``."""
    best_publish: tuple[datetime, str, dict] | None = None
    skipped_green: tuple[datetime, str, dict, list[str]] | None = None
    for provider_wf, required_jobs in PUBLISHING_PROVIDERS[workflow]:
        runs = runs_cache.get(provider_wf)
        if runs is None:
            runs = recent_completed_runs(provider_wf, branch="main", limit=PROVIDER_RUN_SCAN_LIMIT)
            runs_cache[provider_wf] = runs
        for run in runs:
            if run.get("conclusion") == "cancelled":
                continue
            finished = _parse_timestamp(run["updatedAt"])
            if best_publish is not None and finished <= best_publish[0]:
                break
            run_id = run.get("databaseId")
            attempt = run.get("attempt")
            if run_id is None:
                continue
            cache_key = (int(run_id), int(attempt) if attempt is not None else None)
            jobs = jobs_cache.get(cache_key)
            if jobs is None:
                jobs = run_jobs(run_id, attempt)
                jobs_cache[cache_key] = jobs
            ok, missing = _jobs_succeeded(jobs, required_jobs)
            if ok:
                best_publish = (finished, provider_wf, run)
                break
            if run.get("conclusion") == "success" and (skipped_green is None or finished > skipped_green[0]):
                skipped_green = (finished, provider_wf, run, missing)
    if best_publish is None:
        if skipped_green is not None:
            _, s_wf, s_run, s_missing = skipped_green
            return (
                True,
                f'- Newest publishing "{workflow}" run on main: none found '
                f'(latest green "{s_wf}" run {s_run["url"]} skipped publishing: {", ".join(s_missing)})',
            )
        return True, f'- Newest publishing "{workflow}" run on main: none found'
    finished, p_wf, p_run = best_publish
    age = now - finished
    is_overdue = age > max_age
    via = "" if p_wf == workflow else f' via "{p_wf}"'
    note = ""
    if is_overdue and skipped_green is not None and skipped_green[0] > finished:
        _, s_wf, s_run, s_missing = skipped_green
        note = f'; newer green "{s_wf}" run {s_run["url"]} skipped publishing ({", ".join(s_missing)})'
    return (
        is_overdue,
        f'- Newest publishing "{workflow}" run on main{via}: {p_run["url"]} ({p_run["conclusion"]}), '
        f'finished {p_run["updatedAt"]}, {age.total_seconds() / 3600:.1f} hours ago '
        f'(limit {max_age.total_seconds() / 3600:.0f}h){note}',
    )


def freshness(now: datetime, *, disabled: str = "", env: Mapping[str, str] = os.environ) -> None:
    """Alert if any active core scheduled rollup or freshness check went silent past its window."""
    if disabled == "true":
        print("CI alerts freshness check is switched off; skipping.")
        return
    states = workflow_states()
    runs_cache: dict[str, list[dict]] = {}
    jobs_cache: dict[tuple[int, int | None], list[dict]] = {}
    lines: list[str] = []
    stale = False
    for workflow, max_age in CORE_WORKFLOW_SCHEDULES:
        state = states.get(workflow)
        if state is not None and state != "active":
            lines.append(f'- Skipped "{workflow}" (`{state}`)')
            continue
        if workflow in PUBLISHING_PROVIDERS:
            is_overdue, line = _evaluate_publishing_workflow(
                workflow,
                max_age,
                now,
                runs_cache=runs_cache,
                jobs_cache=jobs_cache,
            )
            stale |= is_overdue
            lines.append(line)
            continue
        recent = recent_completed_runs(workflow, branch="main", limit=5)
        run = next((item for item in recent if item.get("conclusion") not in {"skipped", "cancelled"}), None)
        if run is None:
            stale = True
            skipped = next((item for item in recent if item.get("conclusion") == "skipped"), None)
            if skipped is not None:
                lines.append(
                    f'- Newest completed "{workflow}" run on main: none executed '
                    f'(latest run {skipped["url"]} was `skipped` at {skipped["updatedAt"]})'
                )
            else:
                lines.append(f'- Newest completed "{workflow}" run on main: none found')
            continue
        finished = _parse_timestamp(run["updatedAt"])
        age = now - finished
        is_overdue = age > max_age
        stale |= is_overdue
        lines.append(
            f'- Newest completed "{workflow}" run on main: {run["url"]} ({run["conclusion"]}), '
            f'finished {run["updatedAt"]}, {age.total_seconds() / 3600:.1f} hours ago '
            f'(limit {max_age.total_seconds() / 3600:.0f}h)'
        )
    checked = f"Checked {now:%Y-%m-%dT%H:%M}Z."
    if stale:
        report(STALE_TITLE, "\n".join([checked, "", *lines]) + "\n", env=env)
        return
    number = open_issue(STALE_TITLE)
    if number is None:
        print("\n".join(["Scheduled rollups are running on schedule. " + checked, *lines]))
        return
    gh("issue", "close", str(number), "--comment", "\n".join(["Fresh again. " + checked, "", *lines]))


def main(
    argv: list[str] | None = None,
    env: Mapping[str, str] = os.environ,
    now: datetime | None = None,
) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("check", choices=["failure", "recovered", "freshness"])
    parser.add_argument(
        "--min-consecutive",
        type=int,
        default=int(env.get("CI_ALERTS_MIN_CONSECUTIVE", "1")),
        help="Minimum consecutive failures before opening/updating an issue (default 1)",
    )
    args = parser.parse_args(argv)
    try:
        if args.check == "freshness":
            freshness(now or datetime.now(timezone.utc), disabled=env.get("CI_ALERTS_DISABLED", ""), env=env)
            return
        run = json.loads(Path(env["GITHUB_EVENT_PATH"]).read_text())["workflow_run"]
        if args.check == "failure":
            failure(run, min_consecutive=args.min_consecutive, env=env)
        else:
            recovered(run)
    except IssuesDisabledError as exc:
        repo = env.get("GH_REPO", "target repository")
        msg = (
            f"Cannot deliver CI alert: GitHub Issues are disabled on {repo}. "
            "Enable repository Issues or configure an active alert destination."
        )
        print(f"::error title=CI alerts delivery failed::{msg}", flush=True)
        print(msg, file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
