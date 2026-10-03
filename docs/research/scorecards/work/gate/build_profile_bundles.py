"""Lift bounded source examples into complete-schema research fixtures.

The generated snapshots close only the listed fixture rows. They do not assert
complete publisher acquisitions, parser qualification, or publication readiness.
No source facts are added: omitted physical fields become null, while snapshot
metadata and fixture-local identities make the sampled relationships testable.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path

from validate_gate import validate_bundle, validate_schema


GATE_DIR = Path(__file__).resolve().parent
SURVEY_DIR = GATE_DIR.parents[1]
DEFAULT_OUTPUT = GATE_DIR / "profile_bundles.json"
SOURCE_FIELDS = ("capture_id", "source_url", "source_path")


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def encode(value: object) -> str:
    """Stable JSON serialization for VARCHAR JSON columns and fixture metadata."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def lift_row(name: str, source: dict, fields: list[str]) -> dict:
    unexpected = set(source) - set(fields)
    if unexpected:
        raise ValueError(f"{name}: example has fields outside the schema: {sorted(unexpected)}")
    row = dict.fromkeys(fields)
    for field, value in source.items():
        if field.endswith("_json") and isinstance(value, (list, dict)):
            # Preserve structured source identifiers as JSON text, not new facts.
            value = encode(value)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{name}.{field}: expected source text or null")
        row[field] = value
    return row


def build_bundle(case: dict, profile: dict, captures: dict, schema: dict, tables: dict) -> dict:
    profile_id = profile["profile_id"]
    snapshot_id = f"fixture:{profile_id}:snapshot-v1"
    rows = {name: [] for name in tables}
    for name, examples in case["rows"].items():
        rows[name] = [lift_row(name, row, tables[name]["fields"]) for row in examples]
    if len(rows["scorecards"]) != 1 or rows["scorecard_snapshots"]:
        raise ValueError(f"{profile_id}: expected one bounded edition and no existing snapshot")
    edition = rows["scorecards"][0]
    source = {field: edition[field] for field in SOURCE_FIELDS}
    if not all(isinstance(value, str) and value for value in source.values()):
        raise ValueError(f"{profile_id}: edition lacks a concrete source observation")

    # Publisher identity rows may omit observation fields in bounded examples.
    # Use their edition's actual source attribution, not an invented homepage.
    for publisher in rows["scorecard_publishers"]:
        for field in SOURCE_FIELDS:
            if publisher[field] is None:
                publisher[field] = source[field]
    for name, facts in rows.items():
        if name == "scorecard_publishers":
            continue
        for fact in facts:
            if fact["snapshot_id"] is not None:
                raise ValueError(f"{profile_id}: bounded example already has snapshot identity")
            fact["snapshot_id"] = snapshot_id

    capture_ids = list(profile["capture_ids"])
    if len(capture_ids) != len(set(capture_ids)) or not capture_ids:
        raise ValueError(f"{profile_id}: invalid profile capture set")
    for capture_id in capture_ids:
        if capture_id not in captures:
            raise ValueError(f"{profile_id}: unknown capture {capture_id}")
    snapshot = dict.fromkeys(tables["scorecard_snapshots"]["fields"])
    snapshot.update(
        snapshot_id=snapshot_id,
        scorecard_id=edition["scorecard_id"],
        observed_at=captures[source["capture_id"]]["observed_at"],
        capture_ids_json=encode(capture_ids),
        parser_version="research-v1",
        completeness_status="complete",
        completeness_rule=(
            "Fixture closure only: every row in this bounded canonical example is represented, "
            "with physical fields and internal references checked. No complete publisher scope is asserted."
        ),
        source_declared_counts_json=None,
        evidence_policy="hash_only",
        rendition_selection_rule=(
            "Each table/key observation keeps its canonical example capture as primary. "
            "Unused profile captures are corroboration only and supply no additional fixture facts."
        ),
        identity_rule_version="research-v1",
        **source,
    )
    rows["scorecard_snapshots"].append(snapshot)
    snapshot["parsed_counts_json"] = encode(
        {"scope": "bounded_research_fixture_rows", "tables": {name: len(facts) for name, facts in rows.items()}}
    )
    groups = defaultdict(list)
    for name, facts in rows.items():
        for fact in facts:
            capture_id = fact["capture_id"]
            if capture_id not in capture_ids:
                raise ValueError(f"{profile_id}: {name} capture is outside the profile set")
            key = [fact[field] for field in tables[name]["key"]]
            groups[capture_id].append(f"{name}:{encode(key)}")
    snapshot["capture_roles_json"] = encode(
        [
            {
                "capture_id": capture_id,
                "role": "primary" if groups[capture_id] else "corroboration",
                "field_groups": groups[capture_id],
            }
            for capture_id in capture_ids
        ]
    )

    assertions = []
    for binding in case["sample_bindings"]:
        name, field = binding["table"], binding["field"]
        row = rows[name][binding["row_index"]]
        literal = profile["literal_examples"][binding["sample_index"]]
        if row[field] != literal["value_text"] or row["capture_id"] != literal["capture_id"]:
            raise ValueError(f"{profile_id}: canonical literal binding differs from its source sample")
        assertions.append(
            {
                "table": name,
                "key": [row[column] for column in tables[name]["key"]],
                "field": field,
                "expected": literal["value_text"],
            }
        )
    bundle = {
        "sample_id": f"profile:{profile_id}",
        "profile_id": profile_id,
        "fixture_only": True,
        "fixture_scope": (
            "Only the bounded rows in schema_examples.json for this profile. "
            "Complete means fixture closure; it does not mean a complete source-defined scorecard, "
            "acquisition, production parser, or publication scope."
        ),
        "source_example_kind": case["kind"],
        "source_example_limits": case["limits"],
        "tables": rows,
        "snapshot_captures": {snapshot_id: capture_ids},
        "literal_assertions": assertions,
    }
    validate_bundle(schema, rows, bundle["snapshot_captures"], assertions)
    return bundle


def build() -> dict:
    schema = load(SURVEY_DIR / "proposed_schema.json")
    tables = validate_schema(schema)
    cases = load(SURVEY_DIR / "schema_examples.json")["cases"]
    profiles = {row["profile_id"]: row for row in load(SURVEY_DIR / "shape_profiles.json")["profiles"]}
    captures = {row["capture_id"]: row for row in load(SURVEY_DIR / "capture_receipts.json")["captures"]}
    bundles, skipped = [], []
    for case in cases:
        if case["kind"] == "unsupported_without_cell_fixture":
            skipped.append(case["profile_id"])
            continue
        if case["kind"] != "bounded_row_fragments":
            raise ValueError(f"Unknown example kind: {case['kind']}")
        bundles.append(build_bundle(case, profiles[case["profile_id"]], captures, schema, tables))
    if len({bundle["profile_id"] for bundle in bundles}) != len(bundles):
        raise ValueError("Duplicate profile fixture")
    return {
        "schema_version": schema["version"],
        "fixture_only": True,
        "scope": "Complete-schema fixtures for bounded canonical research examples only.",
        "skipped_unsupported_profiles": skipped,
        "bundles": bundles,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true", help="Validate and require byte-identical checked-in output")
    args = parser.parse_args(argv)
    data = build()
    expected = json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    if args.check:
        if not args.output.exists() or args.output.read_text() != expected:
            raise SystemExit("Profile fixtures differ; regenerate before accepting this gate evidence")
    else:
        args.output.write_text(expected)
    print(
        f"Validated {len(data['bundles'])} bounded profile fixtures; skipped {len(data['skipped_unsupported_profiles'])} explicit deferrals"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
