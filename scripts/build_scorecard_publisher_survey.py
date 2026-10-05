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
    args = parser.parse_args()
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
