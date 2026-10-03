"""Check research schema bundles without acquiring, publishing, or freezing data.

The schema is read from proposed_schema.json. Qualification records acceptance
separately from structural fixture validation; a valid fixture is not a complete
publisher acquisition or a production-ready parser.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

DEFAULT_SCHEMA = Path(__file__).resolve().parents[2] / "proposed_schema.json"
DEFAULT_QUALIFICATION = Path(__file__).with_name("qualification.json")
HANDOFFS = ("census", "samples", "operations")
REQUIREMENTS = (
    "all_profiles_dispositioned",
    "representative_rows_qualified",
    "selected_scope_boundaries_proven",
    "identity_type_relationship_rules_settled",
    "initial_source_selection",
    "independent_readback",
)
RESULT_KEY = ["scorecard_id", "item_id", "publisher_member_key", "result_id"]
COMMON = {"snapshot_id", "capture_id", "source_url", "source_path"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _reference(child, parent, columns, optional=False):
    return tuple(child), parent, tuple(columns), optional


def validate_schema(schema: dict) -> dict[str, dict]:
    """Check explicit fields/types and identity-targeted references; return tables."""
    require(isinstance(schema, dict), "Schema must be an object")
    require(isinstance(schema.get("tables"), list), "Schema tables must be a list")
    tables = {}
    for table in schema["tables"]:
        require(isinstance(table, dict), "Table definition must be an object")
        name, fields, key = table.get("name"), table.get("fields"), table.get("key")
        require(nonempty(name) and name not in tables, "Missing or duplicate table name")
        require(isinstance(fields, list) and all(map(nonempty, fields)), f"{name}: invalid fields")
        require(len(set(fields)) == len(fields), f"{name}: duplicate field")
        require(isinstance(key, list) and key and all(map(nonempty, key)), f"{name}: invalid identity")
        require(len(set(key)) == len(key) and set(key) <= set(fields), f"{name}: invalid identity columns")
        require(table.get("types") == dict.fromkeys(fields, "VARCHAR"), f"{name}: all fields must declare VARCHAR")
        require(isinstance(table.get("references"), list), f"{name}: references must be explicit")
        if name != "scorecard_publishers":
            require(COMMON <= set(fields), f"{name}: common source fields must be physical columns")
        else:
            require(
                {"capture_id", "source_url", "source_path"} <= set(fields),
                f"{name}: publisher observations need physical provenance columns",
            )
        tables[name] = table
    expected_names = {
        "scorecard_publishers",
        "scorecards",
        "scorecard_snapshots",
        "scorecard_methodologies",
        "scorecard_metrics",
        "scorecard_items",
        "scorecard_metric_items",
        "scorecard_metric_components",
        "scorecard_members",
        "scorecard_member_ratings",
        "scorecard_member_item_results",
    }
    require(set(tables) == expected_names, "Research V1 table set differs from the reviewed model")
    require(tables["scorecard_member_item_results"]["key"] == RESULT_KEY, "Member-item results need a metric-free key")
    require(
        {"publisher_position_text", "position_basis", "position_source_path"}
        <= set(tables["scorecard_items"]["fields"]),
        "Items need source target, basis and locator",
    )
    for name, table in tables.items():
        signatures = set()
        for reference in table["references"]:
            require(isinstance(reference, dict), f"{name}: invalid reference")
            require(
                set(reference) == {"child_columns", "parent_table", "parent_columns", "optional"},
                f"{name}: reference fields differ",
            )
            child, parent, columns = (reference[k] for k in ("child_columns", "parent_table", "parent_columns"))
            require(
                isinstance(child, list) and child and set(child) <= set(table["fields"]),
                f"{name}: invalid child columns",
            )
            require(
                parent in tables and columns == tables[parent]["key"], f"{name}: reference must target parent identity"
            )
            require(
                len(child) == len(columns) and type(reference["optional"]) is bool, f"{name}: invalid reference shape"
            )
            signature = _reference(child, parent, columns, reference["optional"])
            require(signature not in signatures, f"{name}: duplicate reference")
            signatures.add(signature)
        required = set()
        if name not in {"scorecard_publishers", "scorecards"}:
            required.add(_reference(["scorecard_id"], "scorecards", ["scorecard_id"]))
        if name not in {"scorecard_publishers", "scorecard_snapshots"}:
            required.add(_reference(["snapshot_id"], "scorecard_snapshots", ["snapshot_id"]))
        if name == "scorecards":
            required.add(_reference(["publisher_id"], "scorecard_publishers", ["publisher_id"]))
        for column, parent in (("publisher_member_key", "scorecard_members"), ("item_id", "scorecard_items")):
            if column in table["fields"] and name != parent:
                required.add(_reference(["scorecard_id", column], parent, ["scorecard_id", column]))
        if "metric_id" in table["fields"] and name not in {"scorecard_metrics", "scorecard_member_item_results"}:
            required.add(_reference(["scorecard_id", "metric_id"], "scorecard_metrics", ["scorecard_id", "metric_id"]))
        if "methodology_id" in table["fields"] and name != "scorecard_methodologies":
            required.add(
                _reference(
                    ["scorecard_id", "methodology_id"],
                    "scorecard_methodologies",
                    ["scorecard_id", "methodology_id"],
                    True,
                )
            )
        if name == "scorecard_metric_components":
            for column in ("parent_metric_id", "component_metric_id"):
                required.add(_reference(["scorecard_id", column], "scorecard_metrics", ["scorecard_id", "metric_id"]))
        if name == "scorecard_member_item_results":
            columns = ["scorecard_id", "metric_id", "item_id", "participation_id"]
            required.add(_reference(columns, "scorecard_metric_items", columns, True))
        require(required <= signatures, f"{name}: required identity reference missing or incorrectly optional")
    return tables


def _reject_constant(value):
    raise ValueError(f"Non-finite JSON constant: {value}")


def _json(value: str | None, label: str):
    if value is None:
        return None
    try:
        return json.loads(value, parse_constant=_reject_constant)
    except (ValueError, TypeError) as error:
        raise ValueError(f"{label}: invalid JSON") from error


def _occurrences(value: object, field: str, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise ValueError(f"{label}: ordered array required")
    mandatory = {"occurrence_id", "source_path", "kind", "period_text" if field == "periods_json" else "citation_text"}
    contexts = (
        {"year_text", "congress_text", "session_text", "chamber_text"}
        if field == "periods_json"
        else {
            "congress_text",
            "chamber_text",
            "session_text",
            "roll_number_text",
            "bill_citation_text",
            "amendment_citation_text",
        }
    )
    seen = set()
    for occurrence in value:
        if not isinstance(occurrence, dict) or not mandatory <= set(occurrence) <= mandatory | contexts:
            raise ValueError(f"{label}: occurrence fields differ")
        occurrence = dict(occurrence)
        require(
            all(v is None or isinstance(v, str) for v in occurrence.values()),
            f"{label}: occurrence values must be text",
        )
        key = occurrence["occurrence_id"]
        require(nonempty(key) and key not in seen, f"{label}: missing or duplicate occurrence_id")
        require(nonempty(occurrence["kind"]), f"{label}: missing occurrence kind")
        text_field = "period_text" if field == "periods_json" else "citation_text"
        require(
            nonempty(occurrence[text_field]) and nonempty(occurrence["source_path"]),
            f"{label}: occurrence needs source text and locator",
        )
        if field == "periods_json":
            require(occurrence["kind"] in {"explicit", "relative"}, f"{label}: unknown period kind")
        seen.add(key)


def _capture_set(snapshot: dict) -> set[str]:
    value = _json(snapshot["capture_ids_json"], "snapshot capture_ids_json")
    require(isinstance(value, list) and value and all(map(nonempty, value)), "Snapshot needs capture IDs")
    require(len(set(value)) == len(value), "Snapshot repeats a capture ID")
    captures = set(value)
    roles = _json(snapshot["capture_roles_json"], "snapshot capture_roles_json")
    require(isinstance(roles, list) and roles, "Snapshot needs capture roles")
    seen, groups, primary = set(), set(), Counter()
    for role in roles:
        require(
            isinstance(role, dict) and set(role) == {"capture_id", "role", "field_groups"}, "Capture role fields differ"
        )
        require(role["capture_id"] in captures and nonempty(role["role"]), "Capture role names unknown capture or role")
        require(
            isinstance(role["field_groups"], list) and all(map(nonempty, role["field_groups"])), "Invalid field groups"
        )
        require(len(set(role["field_groups"])) == len(role["field_groups"]), "Repeated capture field group")
        seen.add(role["capture_id"])
        groups.update(role["field_groups"])
        if role["role"] == "primary":
            primary.update(role["field_groups"])
    require(seen == captures, "Every snapshot capture needs a role")
    require(
        bool(groups) and all(primary[group] == 1 for group in groups),
        "Each field group needs exactly one primary rendition",
    )
    for field in ("parser_version", "identity_rule_version", "rendition_selection_rule", "completeness_rule"):
        require(nonempty(snapshot[field]), f"Snapshot needs {field}")
    require(snapshot["completeness_status"] == "complete", "Accepted fixture snapshot must state complete")
    require(snapshot["evidence_policy"] in {"full", "hash_only", "metadata_only"}, "Unknown evidence policy")
    return captures


def _acyclic(rows: list[dict]) -> None:
    adjacency = defaultdict(set)
    for row in rows:
        adjacency[(row["scorecard_id"], row["parent_metric_id"])].add((row["scorecard_id"], row["component_metric_id"]))
    active, finished = set(), set()

    def visit(node):
        require(node not in active, "Metric component cycle")
        if node in finished:
            return
        active.add(node)
        for child in adjacency.get(node, ()):
            visit(child)
        active.remove(node)
        finished.add(node)

    for node in list(adjacency):
        visit(node)


def validate_bundle(
    schema: dict, rows: dict, snapshot_captures: dict | None = None, literal_assertions: list[dict] | None = None
) -> dict:
    """Validate complete schema fixtures without modifying or normalizing values."""
    tables = validate_schema(schema)
    require(
        isinstance(rows, dict) and set(rows) == set(tables), "Bundle must declare every table, including empty tables"
    )
    indexes = {}
    for name, table in tables.items():
        require(isinstance(rows[name], list), f"{name}: rows must be a list")
        index = indexes[name] = {}
        for position, row in enumerate(rows[name]):
            label = f"{name} row {position}"
            require(isinstance(row, dict) and set(row) == set(table["fields"]), f"{label}: exact fields required")
            require(
                all(v is None or isinstance(v, str) for v in row.values()), f"{label}: values must be string or null"
            )
            for field in ("capture_id", "source_url", "source_path"):
                require(nonempty(row[field]), f"{label}: source observation needs {field}")
            key = tuple(row[c] for c in table["key"])
            require(all(map(nonempty, key)) and key not in index, f"{label}: null, empty or duplicate identity")
            index[key] = row
            for field, value in row.items():
                if field.endswith("_json"):
                    decoded = _json(value, f"{label} {field}")
                    if field in {"periods_json", "references_json"}:
                        _occurrences(decoded, field, label + " " + field)
            if name == "scorecard_member_item_results":
                metric, participation = row["metric_id"], row["participation_id"]
                require(
                    (metric is None and participation is None) or (nonempty(metric) and nonempty(participation)),
                    f"{label}: partial metric participation",
                )
            if name == "scorecard_items" and row["publisher_position_text"] is not None:
                require(
                    nonempty(row["position_basis"]) and nonempty(row["position_source_path"]),
                    f"{label}: item target needs basis and locator",
                )
    for name, table in tables.items():
        for row in rows[name]:
            for ref in table["references"]:
                key = tuple(row[c] for c in ref["child_columns"])
                if any(value is None for value in key) and ref["optional"]:
                    continue
                require(key in indexes[ref["parent_table"]], f"{name}: orphan reference to {ref['parent_table']}")
    editions = indexes["scorecards"]
    snapshots = indexes["scorecard_snapshots"]
    by_edition = Counter(row["scorecard_id"] for row in rows["scorecard_snapshots"])
    require(
        set(by_edition) == {key[0] for key in editions} and all(n == 1 for n in by_edition.values()),
        "Exactly one accepted snapshot is required per edition",
    )
    captures = {key[0]: _capture_set(row) for key, row in snapshots.items()}
    if snapshot_captures is not None:
        require(
            isinstance(snapshot_captures, dict) and set(snapshot_captures) == set(captures),
            "External snapshot capture set differs",
        )
        for key, value in snapshot_captures.items():
            require(
                isinstance(value, list) and all(map(nonempty, value)) and len(set(value)) == len(value),
                "Invalid external capture list",
            )
            require(set(value) == captures[key], "External snapshot capture membership differs")
    for name, table in tables.items():
        if name == "scorecard_publishers":
            continue
        for row in rows[name]:
            edition = editions[(row["scorecard_id"],)]
            snapshot = snapshots[(row["snapshot_id"],)]
            require(
                edition["snapshot_id"] == row["snapshot_id"] and snapshot["scorecard_id"] == row["scorecard_id"],
                f"{name}: fact snapshot differs from selected edition snapshot",
            )
            require(row["capture_id"] in captures[row["snapshot_id"]], f"{name}: capture is not in selected snapshot")
            for ref in table["references"]:
                key = tuple(row[c] for c in ref["child_columns"])
                if any(value is None for value in key) and ref["optional"]:
                    continue
                parent = indexes[ref["parent_table"]][key]
                if "snapshot_id" in parent:
                    require(parent["snapshot_id"] == row["snapshot_id"], f"{name}: reference crosses snapshots")
    _acyclic(rows["scorecard_metric_components"])
    for assertion in literal_assertions or []:
        require(
            isinstance(assertion, dict) and set(assertion) == {"table", "key", "field", "expected"},
            "Literal assertion fields differ",
        )
        name, field = assertion["table"], assertion["field"]
        require(
            name in tables and field in tables[name]["fields"] and isinstance(assertion["key"], list),
            "Invalid literal assertion address",
        )
        row = indexes[name].get(tuple(assertion["key"]))
        require(row is not None and row[field] == assertion["expected"], "Literal source assertion differs")
    return {"table_rows": {name: len(value) for name, value in rows.items()}, "editions": len(editions)}


def validate_gate(schema: dict, qualification: dict | None = None) -> dict:
    """Report readiness from explicit acceptance; never grant publication authority."""
    errors, pending, checked = [], [], []
    try:
        validate_schema(schema)
    except ValueError as error:
        errors.append(str(error))
    if not isinstance(qualification, dict):
        pending.append("qualification record absent")
        qualification = {}
    handoffs = qualification.get("handoffs", {})
    if not isinstance(handoffs, dict):
        handoffs = {}
    for name in HANDOFFS:
        handoff = handoffs.get(name)
        if not isinstance(handoff, dict) or handoff.get("accepted") is not True:
            pending.append(f"handoff not accepted: {name}")
    requirements = qualification.get("requirements", [])
    accepted, seen = set(), set()
    if not isinstance(requirements, list):
        requirements = []
        errors.append("requirements must be a list")
    for requirement in requirements:
        if not isinstance(requirement, dict) or not nonempty(requirement.get("id")):
            errors.append("invalid requirement")
            continue
        key = requirement["id"]
        if key in seen:
            errors.append(f"duplicate requirement: {key}")
        seen.add(key)
        evidence = requirement.get("evidence")
        if (
            requirement.get("accepted") is True
            and isinstance(evidence, list)
            and evidence
            and all(map(nonempty, evidence))
        ):
            accepted.add(key)
    pending.extend(f"requirement not accepted with evidence: {key}" for key in REQUIREMENTS if key not in accepted)
    bundles = qualification.get("bundles", [])
    if not isinstance(bundles, list):
        errors.append("bundles must be a list")
        bundles = []
    if not bundles:
        pending.append("no representative schema fixtures")
    seen = set()
    for bundle in bundles:
        try:
            require(isinstance(bundle, dict) and nonempty(bundle.get("sample_id")), "Fixture needs sample_id")
            key = bundle["sample_id"]
            require(key not in seen, "Duplicate sample_id")
            seen.add(key)
            require(
                bundle.get("fixture_only") is True and nonempty(bundle.get("fixture_scope")),
                "Research fixture must be labeled fixture_only with a bounded scope",
            )
            require("snapshot_captures" in bundle, "Fixture needs independent snapshot capture membership")
            result = validate_bundle(
                schema, bundle.get("tables"), bundle["snapshot_captures"], bundle.get("literal_assertions")
            )
            require(result["editions"] > 0, "Representative fixture must contain an edition")
            checked.append({"sample_id": key, **result})
        except (ValueError, TypeError) as error:
            errors.append(
                f"Fixture {bundle.get('sample_id', '<unknown>') if isinstance(bundle, dict) else '<unknown>'}: {error}"
            )
    return {
        "allows": not errors and not pending,
        "allows_production_publication": False,
        "scope": "Research schema qualification only; no acquisition completeness or parser support is inferred.",
        "schema_version": schema.get("version"),
        "errors": errors,
        "pending": pending,
        "checked_fixtures": checked,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--qualification", type=Path, default=DEFAULT_QUALIFICATION)
    args = parser.parse_args(argv)
    schema = json.loads(args.schema.read_text())
    qualification = json.loads(args.qualification.read_text()) if args.qualification.exists() else None
    report = validate_gate(schema, qualification)
    print(json.dumps(report, indent=2))
    return 0 if report["allows"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
