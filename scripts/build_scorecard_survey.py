"""Validate the research census and generate its coverage and shape reports offline."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import re

DEFAULT_DIRECTORY = Path(__file__).resolve().parents[1] / "docs/research/scorecards"
INPUTS = (
    "scorecard_source_catalog.json",
    "shape_profiles.json",
    "capture_receipts.json",
    "schema_breakers.json",
    "proposed_schema.json",
    "schema_examples.json",
    "discovery_log.json",
    "work/samples/sample_profiles.json",
    "work/samples/row_examples.json",
    "work/samples/measurements.json",
    "work/gate/qualification.json",
    "work/gate/freeze_receipt.json",
)
STATUSES = {"discovered", "verified", "profiled", "supported", "blocked", "retired"}
RECOVERY_FILE = "work/integration/publisher_recovery_20261005.json"


def unique(rows: list[dict], key: str) -> dict:
    result = {}
    for row in rows:
        value = row[key]
        if not value or value in result:
            raise ValueError(f"Missing or duplicate {key}: {value!r}")
        result[value] = row
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate(data: dict) -> None:
    """Refuse unsupported evidence/status claims before generating any reports."""
    sources = unique(data["scorecard_source_catalog.json"]["sources"], "source_id")
    profiles = unique(data["shape_profiles.json"]["profiles"], "profile_id")
    captures = unique(data["capture_receipts.json"]["captures"], "capture_id")
    breakers = unique(data["schema_breakers.json"]["unsupported_shapes"], "id")
    tables = unique(data["proposed_schema.json"]["tables"], "name")
    schema = data["proposed_schema.json"]
    recovery = data.get(RECOVERY_FILE)
    if recovery:
        entries = unique(recovery["publishers"], "publisher_id")
        linked = {s["publisher_id"] for s in sources.values() if s.get("latest_recovery")}
        require(linked == set(entries), "Recovery catalog links must cover every assigned publisher")
        require(
            recovery.get("format_version") == "scorecard-publisher-recovery-aggregate/1"
            and recovery["summary"].get("newly_qualified_editions") == 0
            and recovery["summary"].get("newly_published_editions") == 0,
            "Recovery cannot establish source qualification or publication",
        )
        for source in sources.values():
            reference = source.get("latest_recovery")
            if reference:
                entry = entries[source["publisher_id"]]
                require(reference.get("source_file") == RECOVERY_FILE, "Recovery catalog source differs")
                require(reference.get("observed_on") == recovery["observed_on"], "Recovery catalog date differs")
                for field in ("finding", "next_action", "reported_research_result", "result", "report"):
                    require(
                        reference.get(field) == entry.get(field),
                        f"Recovery catalog field differs: {source['publisher_id']}/{field}",
                    )
    else:
        require(not any(s.get("latest_recovery") for s in sources.values()), "Recovery catalog source is missing")
    require(type(schema["frozen"]) is bool, "Schema freeze status must be explicit")
    if schema["frozen"]:
        receipt = data["work/gate/freeze_receipt.json"]
        require(receipt.get("accepted") is True, "Frozen schema requires an accepted gate receipt")
        digest = hashlib.sha256(json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        require(receipt.get("schema_semantic_sha256") == digest, "Frozen schema differs from accepted gate")
        spec = importlib.util.spec_from_file_location(
            "scorecard_gate", DEFAULT_DIRECTORY / "work/gate/validate_gate.py"
        )
        if spec is None or spec.loader is None:
            raise ValueError("Missing schema gate validator")
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        result = gate.validate_gate(schema, data["work/gate/qualification.json"])
        require(result["allows"] is True, "Frozen schema has unaccepted requirements or invalid fixtures")
    for capture in captures.values():
        key = capture["capture_id"]
        require(capture["evidence_policy"] == "hash_only", f"Unexpected research retention: {key}")
        require(capture["body_retained_publicly"] is False, f"Public source bytes: {key}")
        require(capture["source_snapshot_complete"] is False, f"Unqualified complete snapshot: {key}")
        if capture["capture_complete"]:
            require(bool(re.fullmatch(r"[0-9a-f]{64}", capture["sha256"] or "")), f"Missing hash: {key}")
            require(isinstance(capture["byte_size"], int) and capture["byte_size"] >= 0, f"Missing size: {key}")
            require(isinstance(capture["http_status"], int), f"Missing response status: {key}")
    for source in sources.values():
        key, status = source["source_id"], source["discovery_status"]
        require(status in STATUSES, f"Unknown discovery status: {key}")
        require(status != "supported", f"Research data cannot establish production support: {key}")
        require(bool(source["discovered_via"]), f"No discovery evidence: {key}")
        ids = source["verification_capture_ids"]
        require(set(ids) <= captures.keys(), f"Unknown verification capture: {key}")
        if status == "retired":
            outcome = source.get("latest_discovery_outcome", {})
            require(
                outcome.get("outcome") == "retired_confirmed"
                and bool(outcome.get("capture_ids"))
                and set(outcome["capture_ids"]) <= set(ids)
                and any(
                    captures[k]["capture_complete"]
                    and captures[k]["http_status"] == 200
                    and captures[k].get("review_status") == "original_publisher_retirement_notice"
                    for k in outcome["capture_ids"]
                ),
                f"Retirement needs an original publisher closure notice: {key}",
            )
        if status in {"verified", "profiled"}:
            require(bool(source["scorecard_index_url"] and source["last_verified_at"]), f"Missing verification: {key}")
            require(
                any(
                    captures[k]["http_status"] == 200
                    and captures[k]["capture_complete"]
                    and captures[k]["review_status"] != "discovery_only"
                    for k in ids
                ),
                f"No successful original-publisher capture: {key}",
            )
        if status == "profiled":
            require(any(p["source_id"] == key for p in profiles.values()), f"No shape profile: {key}")
    for profile in profiles.values():
        key = profile["profile_id"]
        require(profile["source_id"] in sources, f"Unknown profile source: {key}")
        require(profile["publisher_id"] == sources[profile["source_id"]]["publisher_id"], f"Wrong publisher: {key}")
        require(bool(profile["capture_ids"]), f"No profile evidence: {key}")
        for capture_id in profile["capture_ids"]:
            require(capture_id in captures, f"Unknown profile capture: {key}")
            capture = captures[capture_id]
            require(capture["http_status"] == 200 and capture["capture_complete"], f"Failed profile input: {key}")
        require(profile["schema_fit"] in {"fits_proposal", "unsupported"}, f"Unclassified profile: {key}")
        require(profile["source_model_fit"] == profile["schema_fit"], f"Source-model disposition drift: {key}")
        require(bool(profile["acquisition_readiness"]), f"Missing acquisition disposition: {key}")
        require(bool(profile["reproduction_readiness"]), f"Missing reproduction disposition: {key}")
        require(set(profile["unsupported_shapes"]) <= breakers.keys(), f"Unknown unsupported shape: {key}")
        require(
            profile["schema_fit"] != "unsupported" or bool(profile["unsupported_shapes"]),
            f"Unsupported disposition needs a reason: {key}",
        )
        require(profile["complete_source_snapshot_established"] is False, f"Unqualified snapshot: {key}")
        require(set(profile["features"].values()) <= {"yes", "no", "unknown", "partial"}, f"Invalid feature: {key}")
        for sample in profile["literal_examples"]:
            require(sample["capture_id"] in profile["capture_ids"], f"Unbound sample: {key}")
            require(bool(sample["locator"]) and isinstance(sample["value_text"], str), f"Invalid literal sample: {key}")
    for case in data["schema_breakers.json"]["hunt"]:
        require(set(case["profiles"]) <= profiles.keys(), f"Unknown breaker profile: {case['shape']}")
    for table in tables.values():
        require(set(table["key"]) <= set(table["fields"]), f"Missing key field: {table['name']}")
        require(len(table["fields"]) == len(set(table["fields"])), f"Duplicate schema field: {table['name']}")
    validate_examples(data, profiles, tables)
    samples = unique(data["work/samples/sample_profiles.json"]["profiles"], "sample_id")
    for key, sample in samples.items():
        require(sample["production_qualified"] is False, f"Unqualified production sample: {key}")
        require(sample["complete_edition_claim"] is False, f"Research scope is not an accepted edition: {key}")
        require(set(sample["capture_ids"]) <= captures.keys(), f"Unknown scope capture: {key}")
        require(bool(sample["scope"] and sample["limits"]), f"Missing sample boundary: {key}")
    for example in data["work/samples/row_examples.json"]["examples"]:
        require(example["sample_id"] in samples, "Unknown sample example")
        require(example["capture_id"] in samples[example["sample_id"]]["capture_ids"], "Unbound scope example")
        require(example["production_ready"] is False, "Research example is not a production parser")


def validate_examples(data: dict, profiles: dict, tables: dict) -> None:
    """Check bounded logical examples without treating them as accepted snapshots."""
    examples = data["schema_examples.json"]
    require(examples["schema_version"] == data["proposed_schema.json"]["version"], "Example schema version drift")
    cases = unique(examples["cases"], "profile_id")
    require(cases.keys() == profiles.keys(), "Every profile needs an example or explicit unsupported disposition")
    references = {
        "scorecards": [("scorecard_publishers", ("publisher_id",))],
        "scorecard_metrics": [("scorecard_methodologies", ("scorecard_id", "methodology_id"))],
        "scorecard_metric_items": [
            ("scorecard_metrics", ("scorecard_id", "metric_id")),
            ("scorecard_items", ("scorecard_id", "item_id")),
        ],
        "scorecard_member_ratings": [
            ("scorecard_metrics", ("scorecard_id", "metric_id")),
            ("scorecard_members", ("scorecard_id", "publisher_member_key")),
        ],
        "scorecard_member_item_results": [
            ("scorecard_items", ("scorecard_id", "item_id")),
            ("scorecard_metric_items", ("scorecard_id", "metric_id", "item_id", "participation_id")),
            ("scorecard_members", ("scorecard_id", "publisher_member_key")),
        ],
    }
    common = set(data["proposed_schema.json"]["common_source_fields"])
    for pid, case in cases.items():
        require(case["production_ready"] is False, f"Unqualified production example: {pid}")
        rows, profile = case["rows"], profiles[pid]
        require("scorecard_snapshots" not in rows, f"Research examples cannot assert accepted snapshots: {pid}")
        if case["kind"] == "unsupported_without_cell_fixture":
            require(not rows and not profile["literal_examples"], f"Unmapped source observations: {pid}")
            require(profile["schema_fit"] == "unsupported" and case["unsupported_shapes"], f"No disposition: {pid}")
            continue
        require(case["kind"] == "bounded_row_fragments" and bool(rows), f"Unknown example kind: {pid}")
        for name, records in rows.items():
            require(name in tables, f"Unknown example table: {name}")
            keys = set()
            for row in records:
                require(set(row) <= set(tables[name]["fields"]) | common, f"Unknown example field: {pid}/{name}")
                key = tuple(row.get(k) for k in tables[name]["key"])
                require(None not in key and key not in keys, f"Missing or duplicate example key: {pid}/{name}")
                keys.add(key)
                if name != "scorecard_publishers":
                    require(row.get("capture_id") in profile["capture_ids"], f"Unbound example capture: {pid}/{name}")
                    require(bool(row.get("source_path")), f"Missing example locator: {pid}/{name}")
                if name not in {"scorecards", "scorecard_publishers"}:
                    require(
                        any(r["scorecard_id"] == row["scorecard_id"] for r in rows.get("scorecards", [])),
                        f"Orphan example edition: {pid}/{name}",
                    )
                for target, fields in references.get(name, []):
                    if all(row.get(f) is not None for f in fields):
                        require(
                            any(all(r.get(f) == row[f] for f in fields) for r in rows.get(target, [])),
                            f"Orphan example reference: {pid}/{name} -> {target}",
                        )
                if name == "scorecard_metric_components":
                    for field in ("parent_metric_id", "component_metric_id"):
                        require(
                            any(
                                r["scorecard_id"] == row["scorecard_id"] and r["metric_id"] == row[field]
                                for r in rows.get("scorecard_metrics", [])
                            ),
                            f"Orphan component metric: {pid}/{field}",
                        )
                if name == "scorecard_member_item_results":
                    require(
                        (row.get("metric_id") is None) == (row.get("participation_id") is None),
                        f"Partial metric context: {pid}",
                    )
        bindings = case["sample_bindings"]
        require(
            sorted(b["sample_index"] for b in bindings) == list(range(len(profile["literal_examples"]))),
            f"Every literal sample needs exactly one row binding: {pid}",
        )
        for binding in bindings:
            sample = profile["literal_examples"][binding["sample_index"]]
            row = rows[binding["table"]][binding["row_index"]]
            require(row[binding["field"]] == sample["value_text"], f"Changed literal source value: {pid}")
            require(row["capture_id"] == sample["capture_id"], f"Changed literal source evidence: {pid}")


def cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def generate(directory: Path) -> dict[str, str]:
    raw = {name: (directory / name).read_bytes() for name in INPUTS}
    if (directory / RECOVERY_FILE).exists():
        raw[RECOVERY_FILE] = (directory / RECOVERY_FILE).read_bytes()
    data = {name: json.loads(value) for name, value in raw.items()}
    validate(data)
    catalog = data["scorecard_source_catalog.json"]
    sources = catalog["sources"]
    for source in sources:
        if source.get("latest_recovery"):
            require(
                source["latest_recovery"].get("source_sha256") == hashlib.sha256(raw[RECOVERY_FILE]).hexdigest(),
                f"Recovery catalog pin differs: {source['publisher_id']}",
            )
    profiles = data["shape_profiles.json"]["profiles"]
    captures = data["capture_receipts.json"]["captures"]
    summary = {
        "surveyed_at": catalog["surveyed_at"],
        "input_sha256": {name: hashlib.sha256(value).hexdigest() for name, value in raw.items()},
        "catalog_series_candidates": len(sources),
        "distinct_publisher_candidates": len({s["publisher_id"] for s in sources}),
        "status_counts": {status: sum(s["discovery_status"] == status for s in sources) for status in sorted(STATUSES)},
        "verified_or_profiled_series": sum(s["discovery_status"] in {"verified", "profiled"} for s in sources),
        "profiled_publishers": len({p["publisher_id"] for p in profiles}),
        "sample_profiles": len(profiles),
        "profile_depths": dict(sorted(Counter(p["profile_depth"] for p in profiles).items())),
        "schema_dispositions": dict(sorted(Counter(p["schema_fit"] for p in profiles).items())),
        "acquisition_dispositions": dict(sorted(Counter(p["acquisition_readiness"] for p in profiles).items())),
        "reproduction_dispositions": dict(sorted(Counter(p["reproduction_readiness"] for p in profiles).items())),
        "captured_responses": sum(c["capture_complete"] for c in captures),
        "capture_attempts": len(captures),
        "complete_scorecard_snapshots": 0,
        "research_sample_scopes": len(data["work/samples/sample_profiles.json"]["profiles"]),
        "schema_frozen": data["proposed_schema.json"]["frozen"],
        "production_supported": 0,
        "all_sampled_profiles_classified": True,
        "bounded_row_example_cases": sum(
            c["kind"] == "bounded_row_fragments" for c in data["schema_examples.json"]["cases"]
        ),
        "all_discovered_series_profiled": all(s["discovery_status"] == "profiled" for s in sources),
        "limitation": "Candidate publishers are not confirmed federal scorecard coverage. Response EOF is not scorecard completeness. Classification validates research metadata, not parser or publication qualification.",
    }
    intro = "Generated by `uv run --frozen python scripts/build_scorecard_survey.py`. Edit the JSON inputs.\n\n"
    report = [
        "# Scorecard census\n\n",
        intro,
        f"Observed through {catalog['surveyed_at']}.\n\n",
        "Counts and exact input hashes: [coverage_summary.json](coverage_summary.json). Candidates include unverified historical leads and mixed election guides.\n\n",
        "Latest completed investigations appear in the [recovery rollup](work/integration/publisher_recovery_20261005.md). Their original-source, partial-data and archive findings guide further work; qualification remains in the separate integration ledger.\n\n",
        "| Publisher / series | Status | Original location | Scope note | Latest recovery |\n| --- | --- | --- | --- | --- |\n",
    ]
    for source in sources:
        url = source["scorecard_index_url"]
        location = f"[Publisher]({url})" if url else "Discovery lead only"
        recovery_note = source.get("latest_recovery", {}).get("finding")
        recovery_note = (
            f"[{cell(recovery_note)}](work/integration/publisher_recovery_20261005.md)"
            if recovery_note
            else "Not assigned to recovery review"
        )
        report.append(
            f"| {cell(source['publisher_name'])} / {cell(source['scorecard_name'])} | {source['discovery_status']} | {location} | {cell(source['notes'])} | {recovery_note} |\n"
        )
    features = sorted({feature for p in profiles for feature in p["features"]})
    matrix = [
        "# Sampled scorecard shapes\n\n",
        intro,
        "Each column covers the named sample only. Y = observed or explicitly publisher-stated; ? = unknown; N = explicitly absent; partial = partially observed. A PDF history observation never proves PDF-only history. Offered downloads are not verified exports.\n\n",
    ]
    marks = {"yes": "Y", "no": "N", "unknown": "?", "partial": "partial"}
    for start in range(0, len(profiles), 4):
        group = profiles[start : start + 4]
        matrix.append("| Capability | " + " | ".join(f"`{p['profile_id']}`" for p in group) + " |\n")
        matrix.append("| --- | " + " | ".join("---" for _ in group) + " |\n")
        for feature in features:
            matrix.append(
                f"| {feature.replace('_', ' ')} | "
                + " | ".join(marks[p["features"].get(feature, "unknown")] for p in group)
                + " |\n"
            )
        matrix.append("\n")
    matrix.append(
        "Source evidence, edition scope and limits: [shape_profiles.json](shape_profiles.json). Unsupported dispositions: [schema_fit.md](schema_fit.md).\n"
    )
    fit = [
        "# Sample disposition against the proposal\n\n",
        intro,
        "A fit means the proposed source tables preserve the sampled facts. Unsupported entries name the unavailable or unqualified part and the facts that can still be preserved. No entry establishes a complete parser, a reproduced score, or production support.\n\n",
        "[Bounded logical row examples](schema_examples.json) bind literal observations to proposed fields and check keys and references. Historical profiles without checked cells have explicit unsupported dispositions. These examples do not establish full source-defined snapshots or freeze physical types.\n\n",
        "| Sample | Source model | Acquisition | Reproduction | Limits | Evidence and reasoning |\n| --- | --- | --- | --- | --- | --- |\n",
    ]
    for p in profiles:
        fit.append(
            f"| `{p['profile_id']}` | {p['schema_fit']} | {cell(p['acquisition_readiness'])} | {cell(p['reproduction_readiness'])} | {', '.join(p['unsupported_shapes']) or 'None identified in sample'} | {cell(p['notes'])} |\n"
        )
    fit.append("\n## Unsupported parts\n\n")
    for issue in data["schema_breakers.json"]["unsupported_shapes"]:
        fit.append(
            f"### {issue['id']}: {issue['shape']}\n\nPreserve: {issue['preserve']}\n\nUnsupported: {issue['unsupported']}\n\nNext evidence: {issue['next_evidence']}\n\n"
        )
    return {
        "coverage_summary.json": json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        "catalog_report.md": "".join(report),
        "shape_matrix.md": "".join(matrix),
        "schema_fit.md": "".join(fit).rstrip() + "\n",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--check", action="store_true", help="Fail if generated reports differ; write nothing")
    args = parser.parse_args()
    outputs = generate(args.directory)
    if args.check:
        stale = [
            name
            for name, text in outputs.items()
            if not (args.directory / name).is_file() or (args.directory / name).read_text() != text
        ]
        if stale:
            parser.exit(1, "Stale scorecard research outputs: " + ", ".join(stale) + "\n")
    else:
        for name, text in outputs.items():
            (args.directory / name).write_text(text)
    print("Scorecard research references, dispositions and generated reports are valid.")


if __name__ == "__main__":
    main()
