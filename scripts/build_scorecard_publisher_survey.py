"""Aggregate pinned publisher surveys without changing qualification or publication.

Task reports use several field names. Normalize their metadata explicitly, retain
the original reports privately, and generate a dated, evidence-linked work queue.
No source acquisition, parser invocation, or production status changes occur here.
"""

import argparse
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path


FIELDS = {
    "survey_status": ("status", "survey_state", "survey_status"),
    "latest_publication": ("latest_global", "latest_publication", "latest_publication_status"),
    "latest_offered": ("newest_offered", "current_latest_offered", "latest_offered_in_inspected_surfaces"),
    "newest_recovered": ("newest_recovered", "newest_recovered_edition", "newest_recovered_rendition"),
    "identity_findings": ("identity_findings", "identity"),
    "year_intervals": ("year_intervals", "known_unknown_year_intervals", "known_intervals", "known_year_intervals"),
    "unknown_year_intervals": ("unknown_intervals", "unknown_year_intervals"),
    "shapes": ("shape", "shapes", "rating_member_item_methodology_shape", "rating_member_item_methodology_shapes"),
    "completeness": ("completeness",),
    "rights_access": ("rights", "rights_access", "rights_access_constraints"),
    "api_endpoints": ("api_endpoints",),
    "api_status": ("api_status", "rating_api_status"),
    "shared_mechanics": ("shared_mechanics",),
    "reusable_modules": ("reusable_module_candidates",),
    "publisher_specific_logic": ("publisher_semantics", "publisher_specific_logic"),
    "next_step": ("next_step", "next_integration_step", "next_concrete_integration_step"),
    "inspection_limits": ("inspection_limits", "inspection_limit_reason"),
    "evidence": ("evidence",),
}


def pinned(record):
    path = Path(record["path"])
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != record["sha256"]:
        raise ValueError(f"Input pin differs: {path}")
    return raw


def document(record):
    return json.loads(pinned(record))


def pointer(value, path):
    """Read an exact source JSON pointer, including list indices."""
    if path == "":
        return value
    if not path.startswith("/"):
        raise ValueError("Recovery source pointer must start at the document root")
    for raw in path[1:].split("/"):
        key = raw.replace("~1", "/").replace("~0", "~")
        value = value[int(key)] if isinstance(value, list) else value[key]
    return value


def verify_recovery(result):
    """Verify reviewed metadata against every original completed task and pinned file."""
    api = result.get("format_version") == "scorecard-publisher-recovery-api/1"
    if result.get("format_version") not in {
        "scorecard-publisher-recovery-aggregate/1",
        "scorecard-publisher-recovery-api/1",
    }:
        raise ValueError("Unknown recovery format")
    inputs = result["inputs"]
    dispatch = document(inputs["dispatch"])
    for name, record in inputs.items():
        if name != "dispatch":
            pinned(record)
    tasks = {row["publisher_id"]: row for row in dispatch["tasks"]}
    final = {row["publisher_id"]: row for row in dispatch["final_reports"]}
    entries = {row["publisher_id"]: row for row in result["publishers"]}
    expected = result["expected_publishers"]
    summary = result["summary"]
    if (
        len(tasks) != len(dispatch["tasks"])
        or len(final) != len(dispatch["final_reports"])
        or len(entries) != len(result["publishers"])
        or len(set(expected)) != len(expected)
        or set(expected) != set(tasks)
        or set(tasks) != set(final)
        or set(final) != set(entries)
        or summary.get("reviewed_publishers" if api else "assigned_publishers") != len(entries)
        or summary.get("newly_qualified_editions") != 0
        or summary.get("newly_published_editions") != 0
    ):
        raise ValueError("Recovery coverage or stage differs from completed dispatch")
    directory = Path(inputs["dispatch"]["path"]).parent
    evidence_documents = {}
    response_pins = set()
    for identifier, entry in entries.items():
        task, terminal = tasks[identifier], final[identifier]
        if terminal["latest_turn_status"] != "completed" or terminal["threadId"] != task["threadId"]:
            raise ValueError(f"Recovery task is not completed: {identifier}")
        for field in ("result", "report"):
            selected, original = entry[f"source_{field}" if api else field], terminal[field]
            if (
                Path(selected["path"]).resolve() != (directory / original["path"]).resolve()
                or selected["sha256"] != original["sha256"]
            ):
                raise ValueError(f"Recovery output identity differs: {identifier}/{field}")
            pinned(selected)
        selected = entry["source_input"]
        if (
            Path(selected["path"]).resolve() != Path(task["input_path"]).resolve()
            or selected["sha256"] != task["input_sha256"]
        ):
            raise ValueError(f"Recovery input identity differs: {identifier}")
        pinned(selected)
        original = document(entry["source_result" if api else "result"])
        if not api:
            reported = {
                field: original[field]
                for field in ("task_result", "recovery_status", "research_status", "assessment")
                if field in original
            }
            if not reported and "status" in original:
                reported = {"status": original["status"]}
            if entry.get("reported_research_result") != reported:
                raise ValueError(f"Recovery research result changed: {identifier}")
            if any(not target.startswith("recovery.") for target in entry["source_field_names"]):
                raise ValueError(f"Recovery pointer leaves its source metadata: {identifier}")
        for target, source in entry.get("source_field_names", {}).items():
            selected = entry
            for field in target.split("."):
                selected = selected[field]
            if selected != pointer(original, source):
                raise ValueError(f"Recovery source field changed: {identifier}/{target}")
        if not api and set(entry["recovery"]) != {
            field.removeprefix("recovery.") for field in entry["source_field_names"]
        }:
            raise ValueError(f"Recovery field lacks an exact source pointer: {identifier}")
        for field in ("finding", "next_action"):
            if not isinstance(entry.get(field), str) or not entry[field].strip():
                raise ValueError(f"Recovery needs a concrete {field}: {identifier}")
        if api:
            for endpoint in entry["endpoints"]:
                for evidence in [endpoint.get("evidence"), *endpoint.get("additional_evidence", [])]:
                    if not evidence:
                        continue
                    metadata = evidence["metadata"]
                    key = (metadata["path"], metadata["sha256"])
                    if key not in evidence_documents:
                        raw = pinned(metadata)
                        evidence_documents[key] = (
                            json.loads(raw) if key[0].endswith(".json") else raw.decode().splitlines()
                        )
                    if key[0].endswith(".json"):
                        pointer(evidence_documents[key], evidence["locator"])
                    elif not evidence["locator"].startswith("line ") or not 0 < int(evidence["locator"][5:]) <= len(
                        evidence_documents[key]
                    ):
                        raise ValueError(f"Recovery report locator differs: {identifier}")
                inspected = endpoint.get("independent_inspection", {})
                if inspected.get("status") == "retained_raw_read":
                    key = (inspected["body_path"], inspected["sha256"])
                    if key not in response_pins:
                        pinned({"path": key[0], "sha256": key[1]})
                        response_pins.add(key)
    return result


def render_recovery(result):
    """Render reviewed findings; literal source details remain in the structured input."""
    api = result["format_version"] == "scorecard-publisher-recovery-api/1"
    title = "Publisher recovery API review" if api else "Publisher scorecard recovery rollup"
    filename = "publisher_recovery_api_20261005.json" if api else "publisher_recovery_20261005.json"
    lines = [
        f"# {title} — {result['observed_on']}",
        "",
        "Generated from reviewed metadata after verifying complete dispatch coverage and pinned task inputs, results and reports.",
        "",
        "All assigned publisher findings are retained. Recovered originals, partial data, archive leads and API observations remain research evidence; this rollup qualifies and publishes no editions.",
        "",
        f"The [structured inventory]({filename}) retains original result labels, source metadata, access and rights limits, exact hashes and private file locators. Original bodies and member-level observations remain private.",
        "",
        "| Publisher | Finding | Next action |",
        "|---|---|---|",
    ]
    for entry in result["publishers"]:
        report = entry["source_report" if api else "report"]["path"]
        lines.append(
            f"| [{cell(entry['publisher_name'])}]({report}) (`{entry['publisher_id']}`) | {cell(entry['finding'])} | {cell(entry['next_action'])} |"
        )
    if result.get("shared_patterns"):
        lines.extend(["", "## Shared implementation patterns", ""])
        lines.extend("- " + cell(pattern) for pattern in result["shared_patterns"])
    lines.extend(
        [
            "",
            "## Reproduce and verify",
            "",
            "Run `uv run --frozen python scripts/build_scorecard_publisher_survey.py --recovery "
            + (
                "docs/research/scorecards/work/integration/publisher_recovery_api_20261005.json"
                if api
                else "docs/research/scorecards/work/integration/publisher_recovery_20261005.json"
            )
            + " --check` from SpicyRegs. Missing inputs, changed hashes, incomplete task coverage and changed literal metadata refuse verification.",
            "",
            "Use the [current catalog](../../catalog_report.md), [current API inventory](publisher_api_current.md) and [integration queue](integration_progress.md) for current work. Dated discovery findings remain available independently.",
            "",
        ]
    )
    return "\n".join(lines)


def cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def aggregate(manifest):
    dispatch = document(manifest["dispatch"])
    statuses = document(manifest["task_status"])
    verification = document(manifest["input_verification"])
    if verification["status"] != "passed":
        raise ValueError("Retained input verification has not passed")
    tasks = {x["key"]: x for x in statuses["tasks"]}
    expected_groups = {g["key"]: g for g in dispatch["groups"]}
    if len(tasks) != len(statuses["tasks"]) or set(tasks) != set(expected_groups):
        raise ValueError("Task status coverage differs from dispatch")
    if {g["key"] for g in manifest["groups"]} != set(expected_groups):
        raise ValueError("Survey input coverage differs from dispatch")
    publishers, originals = [], []
    for group in manifest["groups"]:
        key = group["key"]
        task = tasks[key]
        if task["threadId"] != expected_groups[key]["thread_id"] or not task["turns"]:
            raise ValueError(f"Task identity or terminal status missing: {key}")
        if any(t["status"] != "completed" or t["error"] is not None for t in task["turns"]):
            raise ValueError(f"Survey task has not completed successfully: {key}")
        raw = document(group["survey"])
        pinned(group["report"])
        if raw["format_version"] != "scorecard-publisher-survey/1":
            raise ValueError(f"Unknown survey format: {key}")
        entries = raw["publishers"]
        ids = [e["publisher_id"] for e in entries]
        if len(set(ids)) != len(ids) or set(ids) != set(expected_groups[key]["ids"]):
            raise ValueError(f"Publisher coverage differs from dispatch: {key}")
        originals.append({"group": key, "source": group["survey"], "document": raw})
        for entry in entries:
            identifier = entry["publisher_id"]
            editorial = manifest["assessments"][identifier]
            name = entry.get("publisher_name") or entry["identity"]["name"]
            normalized = {}
            source_fields = {}
            for target, aliases in FIELDS.items():
                found = next((k for k in aliases if k in entry), None)
                normalized[target] = entry[found] if found else None
                source_fields[target] = found
            publishers.append(
                {
                    "publisher_id": identifier,
                    "publisher_name": name,
                    "group": key,
                    "task_id": next(
                        p["task_id"] for p in expected_groups[key]["publishers"] if p["publisher_id"] == identifier
                    ),
                    "assessment": editorial,
                    "survey": normalized,
                    "source_field_names": source_fields,
                    "source_document": group["survey"],
                    "source_report": group["report"],
                }
            )
    ids = [p["publisher_id"] for p in publishers]
    if len(ids) != len(set(ids)) or set(ids) != set(manifest["assessments"]):
        raise ValueError("Aggregate contains duplicate publishers or incomplete assessments")
    if set(ids) & set(dispatch["underway_publishers"]):
        raise ValueError("Survey duplicates publishers already assigned to other work")
    publishers.sort(key=lambda p: (p["group"], p["publisher_id"]))
    result = {
        "format_version": "scorecard-publisher-survey-aggregate/1",
        "observed_on": manifest["observed_on"],
        "scope": "Completed bounded surveys; metadata and next steps only. No edition qualified or published by this aggregation.",
        "summary": {
            "completed_tasks": len(tasks),
            "surveyed_publishers": len(publishers),
            "newly_qualified_editions": 0,
            "already_underway_publishers": dispatch["underway_publishers"],
            "assessment_counts": dict(sorted(Counter(p["assessment"]["category"] for p in publishers).items())),
        },
        "inputs": {k: manifest[k] for k in ("dispatch", "task_status", "input_verification", "groups")},
        "priorities": manifest["priorities"],
        "shared_patterns": manifest["shared_patterns"],
        "schema_findings": manifest["schema_findings"],
        "publishers": publishers,
    }
    return result, {"format_version": "scorecard-publisher-survey-original-results/1", "groups": originals}


def render(result):
    summary = result["summary"]
    by_id = {p["publisher_id"]: p for p in result["publishers"]}
    lines = [
        "# Remaining publisher survey — " + result["observed_on"],
        "",
        "Generated from pinned, completed task outputs. Do not edit this report directly.",
        "",
        f"Completed {summary['completed_tasks']} tasks covering {summary['surveyed_publishers']} distinct assigned publishers. "
        "The survey produced source candidates and catalog evidence; no new edition was qualified or published.",
        "",
        "See [structured inventory](publisher_survey_20261005.json) for original URLs, response hashes, "
        "edition uncertainty, source shapes, access, rights, completeness, and task provenance. "
        "Original task reports and raw inputs remain private at their pinned paths.",
        "",
        "The separately assigned work is excluded: "
        + ", ".join(f"`{x}`" for x in summary["already_underway_publishers"])
        + ".",
        "",
        "## Recommended integration order",
        "",
        "These priorities are implementation judgments based on the task evidence, not support status.",
        "",
        "| Order | Publisher | Source and remaining work |",
        "|---|---|---|",
    ]
    for rank, identifier in enumerate(result["priorities"], 1):
        p = by_id[identifier]
        lines.append(
            f"| {rank} | {cell(p['publisher_name'])} | {cell(p['assessment']['finding'])} {cell(p['survey']['next_step'])} |"
        )
    lines.extend(["", "## Shared patterns", "", "| Pattern | Proven reuse and boundary |", "|---|---|"])
    for pattern in result["shared_patterns"]:
        lines.append(f"| {cell(pattern['name'])} | {cell(pattern['finding'])} |")
    lines.extend(["", "## Schema and identity findings", ""])
    lines.extend("- " + x for x in result["schema_findings"])
    lines.extend(
        [
            "",
            "## All surveyed publishers",
            "",
            "Each next step comes from its pinned task report. Unknown coverage remains unknown.",
            "",
        ]
    )
    for group in result["inputs"]["groups"]:
        lines.extend(["### " + group["key"], "", "| Publisher | Finding | Next step |", "|---|---|---|"])
        for p in result["publishers"]:
            if p["group"] == group["key"]:
                lines.append(
                    f"| {cell(p['publisher_name'])} | {cell(p['assessment']['finding'])} | {cell(p['survey']['next_step'])} |"
                )
        lines.append("")
    lines.extend(
        [
            "## Reproduce and verify",
            "",
            "Run `uv run python scripts/build_scorecard_publisher_survey.py --check` from SpicyRegs. "
            "The manifest pins all task reports, the dispatch, terminal task statuses and the retained-input verification receipt. "
            "Missing or changed inputs refuse regeneration. `--private-output PATH` retains every original task JSON document without field normalization.",
            "",
            "The authoritative qualification ledger and publication registry remain unchanged. "
            "Public access does not establish redistribution rights. Keep original bodies private and retain failed requests. "
            "A failed request, empty search or old catalog date does not establish retirement. "
            "Liberty Lobby has a dated publisher closure notice; its last scorecard edition remains unknown.",
            "",
        ]
    )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    directory = Path(__file__).resolve().parents[1] / "docs/research/scorecards/work/integration"
    parser.add_argument("--manifest", type=Path, default=directory / "publisher_survey_manifest_20261005.json")
    parser.add_argument("--output-directory", type=Path, default=directory)
    parser.add_argument("--private-output", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--recovery",
        type=Path,
        action="append",
        help="Verify a reviewed recovery JSON and generate its Markdown; repeat for the API review",
    )
    args = parser.parse_args()
    if args.recovery:
        for path in args.recovery:
            result = verify_recovery(json.loads(path.read_bytes()))
            content = render_recovery(result)
            destination = path.with_suffix(".md")
            if args.check:
                if not destination.exists() or destination.read_text() != content:
                    raise ValueError(f"Generated report differs: {destination}")
            else:
                destination.write_text(content)
            print(json.dumps(result["summary"]))
        return
    result, originals = aggregate(json.loads(args.manifest.read_text()))
    outputs = {
        "publisher_survey_20261005.json": json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        "publisher_survey_20261005.md": render(result),
    }
    for filename, content in outputs.items():
        destination = args.output_directory / filename
        if args.check:
            if destination.read_text() != content:
                raise ValueError(f"Generated report differs: {destination}")
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content)
    if args.private_output:
        args.private_output.write_text(json.dumps(originals, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(result["summary"]))


if __name__ == "__main__":
    main()
