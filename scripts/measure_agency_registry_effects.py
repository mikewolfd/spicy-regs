"""Measure the batch-1 agency-registry candidates under spicy-regs' own FR resolution rule.

This is spicy-regs code: it imports ``spicy_regs.ontology.agencies`` (the rule
``agency_code_for_fr_agencies`` over its vendored REF-038 projection) and runs in spicy-regs'
environment, so it lives here and never in RefSpec (REF-024). RefSpec's batch-1 tool reads only
its JSON output, by digest, and refuses one from another spicy-regs commit, projection, module, or
version of this script -- the output records this file's own sha256 for that check.

Every Federal Register row of the rulemaking replay is classified by the rule (resolved /
unresolved / ambiguous / joint), and each docket-less proceeding by its one FR document.
Each candidate is then "adopted" by feeding the module a projection with the rows adoption
would add; the module's own reverse lookup and parent chains decide the outcome:

* an identity bridge FR X sameEntityAs org T gives FR X every code selecting T (reverse
  lookups follow bridges), with FR X's roster parent as its parent_org;
* a succession event gives each original every code selecting any of its results (the
  forward lookup returns a set; the exactly-one rule decides), with the original's parent.

Per candidate: rows and proceedings gained (no code -> code), lost (code -> none) and
changed (one code -> another, broken down by code pair such as "HHS→CMS"), alone and on top
of every other batch-1 candidate; each split is also measured once per result on its own.
The output is deterministic: same inputs, same bytes.

Run (from the spicy-regs checkout's environment):
  uv run --frozen --project <spicy-regs> python scripts/measure_agency_registry_effects.py \\
    --spicy-regs <spicy-regs> --replay-root <replay> --candidates <candidates.json> --output <rule-effects.json>
where <candidates.json> is the RefSpec tool's stdout without --rule-effects (or its committed
JSON; both carry the same candidates_digest).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from spicy_regs.ontology import agencies

FR = "urn:ref:federal-register-agency:"
DELTA_FIELDS = (
    "fr_rows_gained",
    "fr_rows_lost",
    "fr_rows_changed",
    "docketless_gained",
    "docketless_lost",
    "docketless_changed",
)

Key = frozenset  # frozenset[tuple[int, int | None]]: an FR row's (agency id, parent id) set


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return "sha256:" + hashlib.file_digest(handle, "sha256").hexdigest()


def parse(value: object) -> list:
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return value if isinstance(value, list) else []


def is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def row_key(entries: Iterable[Any]) -> Key:
    """Exactly the (id, parent_id) pairs agency_code_for_fr_agencies reads; malformed entries skipped."""
    return frozenset(
        (e["id"], e.get("parent_id") if is_int(e.get("parent_id")) else None)
        for e in entries
        if isinstance(e, dict) and is_int(e.get("id"))
    )


def entries_of(key: Key) -> list[dict]:
    return [{"id": i, "parent_id": p} for i, p in sorted(key, key=lambda pair: (pair[0], pair[1] is None, pair[1]))]


BASE_ROWS = agencies.projection_rows()


def use_rows(rows: tuple[Mapping[str, Any], ...]) -> None:
    """Point the module at a projection; its own cached reverse lookup is rebuilt from these rows."""
    setattr(agencies, "projection_rows", lambda: rows)  # a measurement-only substitution, never a spicy-regs path
    agencies._projection.cache_clear()


def codes(keys: Iterable[Key]) -> dict[Key, str | None]:
    return {key: agencies.agency_code_for_fr_agencies(entries_of(key)) for key in keys}


def category(key: Key) -> str:
    """The four categories, read from the module's own projection state and its function."""
    projection = agencies._projection()
    ids = {i for i, _ in key}
    coded = {i: c for i in ids if (c := projection.code_by_fr_id.get(i)) is not None}
    specific = [
        i for i in coded if not any(f"{FR}{i}" in projection.ancestors_by_org[f"{FR}{o}"] for o in coded if o != i)
    ]
    if len(specific) != 1:
        return "unresolved" if not coded else "ambiguous"
    return "resolved" if agencies.agency_code_for_fr_agencies(entries_of(key)) is not None else "joint_unrelated"


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--spicy-regs", type=Path, required=True, help="the spicy-regs checkout this environment runs")
    parser.add_argument(
        "--replay-root", type=Path, required=True, help="replay directory with federal_register and proceedings parquet"
    )
    parser.add_argument("--candidates", type=Path, required=True, help="the RefSpec tool's candidates JSON")
    parser.add_argument("--output", type=Path, required=True, help="where to write the rule-effects JSON")
    args = parser.parse_args()
    report = json.loads(args.candidates.read_text())

    fr_counts: Counter[Key] = Counter()
    key_by_row_id: dict[str, Key] = {}
    fr_file = pq.ParquetFile(args.replay_root / "federal_register.parquet")
    for batch in fr_file.iter_batches(
        batch_size=50_000, columns=["document_number", "publication_date", "agencies_json"]
    ):
        for number, date, agencies_json in zip(
            batch.column("document_number").to_pylist(),
            batch.column("publication_date").to_pylist(),
            batch.column("agencies_json").to_pylist(),
            strict=True,
        ):
            key = row_key(parse(agencies_json))
            fr_counts[key] += 1
            key_by_row_id[f"{number}@{date}"] = key
    docketless: Counter[Key] = Counter()
    docketless_total = unmatched = 0
    for batch in pq.ParquetFile(args.replay_root / "proceedings.parquet").iter_batches(
        batch_size=50_000, columns=["docket_ids_json", "fr_document_ids_json"]
    ):
        for dockets, fr_ids in zip(
            batch.column("docket_ids_json").to_pylist(), batch.column("fr_document_ids_json").to_pylist(), strict=True
        ):
            if parse(dockets):
                continue
            docketless_total += 1
            ids = [value for value in parse(fr_ids) if isinstance(value, str)]
            if len(ids) != 1:
                raise SystemExit(
                    f"a docket-less proceeding names {len(ids)} FR documents; the per-document reading needs one"
                )
            if ids[0] in key_by_row_id:
                docketless[key_by_row_id[ids[0]]] += 1
            else:
                unmatched += 1

    keys = sorted(fr_counts, key=lambda key: sorted(key, key=lambda pair: (pair[0], pair[1] is None, pair[1])))
    use_rows(BASE_ROWS)
    base = codes(keys)
    base_categories: Counter[str] = Counter()
    base_docketless_categories: Counter[str] = Counter()
    base_category: dict[Key, str] = {}
    for key in keys:
        cat = base_category[key] = category(key)
        base_categories[cat] += fr_counts[key]
        base_docketless_categories[cat] += docketless[key]
        if (cat == "resolved") != (base[key] is not None):
            raise SystemExit("the category reading disagrees with agency_code_for_fr_agencies")

    identity = [row for row in report["candidates"] if row["relation"] == "sameEntityAs"]
    withdrawn = report["non_emissions"]
    rows_by_id = {row["candidate_id"]: row for row in report["candidates"]}
    events = report["events"]

    def parent_org(record: Mapping) -> str | None:
        return record["parent"]["resource_iri"] if record["parent"] else None

    def bridge_rows(candidate: Mapping, rows: tuple[Mapping, ...]) -> list[dict]:
        target = candidate["target"]["resource_iri"]
        return [
            {
                "org": candidate["source"]["resource_iri"],
                "source_value": row["source_value"],
                "parent_org": parent_org(candidate["source"]),
            }
            for row in rows
            if row["org"] == target
        ]

    def event_rows(event: Mapping, rows: tuple[Mapping, ...]) -> list[dict]:
        results = {rows_by_id[row_id]["target"]["resource_iri"] for row_id in event["rows"]}
        found = sorted({row["source_value"] for row in rows if row["org"] in results})
        return [
            {"org": original["resource_iri"], "source_value": code, "parent_org": parent_org(original)}
            for original in event["originals"]
            for code in found
        ]

    def projection_for(bridges: list[Mapping], chosen_events: list[Mapping]) -> tuple[Mapping, ...]:
        rows = tuple(BASE_ROWS)
        rows += tuple(row for candidate in bridges for row in bridge_rows(candidate, rows))
        return rows + tuple(row for event in chosen_events for row in event_rows(event, rows))

    def scenario(bridges: list[Mapping], chosen_events: list[Mapping]) -> dict[Key, str | None]:
        use_rows(projection_for(bridges, chosen_events))
        return codes(keys)

    def delta(after: Mapping[Key, str | None], before: Mapping[Key, str | None] = base) -> dict:
        out: Counter[str] = Counter()
        rows_moved: Counter[str] = Counter()
        docketless_moved: Counter[str] = Counter()
        for key in keys:
            b, a = before[key], after[key]
            if b == a:
                continue
            kind = "gained" if b is None else "lost" if a is None else "changed"
            out[f"fr_rows_{kind}"] += fr_counts[key]
            out[f"docketless_{kind}"] += docketless[key]
            if kind == "changed":
                rows_moved[f"{b}→{a}"] += fr_counts[key]
                if docketless[key]:
                    docketless_moved[f"{b}→{a}"] += docketless[key]
        result: dict = {name: out[name] for name in DELTA_FIELDS}
        result["fr_rows_rerouted"] = dict(sorted(rows_moved.items()))
        result["docketless_rerouted"] = dict(sorted(docketless_moved.items()))
        return result

    everything = scenario(identity, events)
    effects = {}
    for candidate in identity:
        others = [c for c in identity if c is not candidate]
        effects[candidate["candidate_id"]] = {
            "alone": delta(scenario([candidate], [])),
            "on_top": delta(everything, scenario(others, events)),
        }
    split_variants = {}
    for event in events:
        others = [e for e in events if e is not event]
        effects[event["event_id"]] = {
            "alone": delta(scenario([], [event])),
            "on_top": delta(everything, scenario(identity, others)),
        }
        if len(event["rows"]) > 1:
            split_variants[event["event_id"]] = {
                row_id: delta(scenario([], [{**event, "rows": [row_id]}])) for row_id in event["rows"]
            }
    withdrawn_reference = {item["candidate_id"]: delta(scenario([item], [])) for item in withdrawn}

    # The first brief's "31,648 of 41,937": every eCFR org found by slug equality (the
    # ten eCFR bridges plus the withdrawn IBWC row), over the rule's unresolved rows.
    slug_found = [c for c in identity if c["target"]["release_key"].startswith("ecfr-")] + list(withdrawn)
    slug_codes = scenario(slug_found, [])
    slug_ids = {c["source"]["id"] for c in slug_found}
    reconciliation = {
        "slug_found_ecfr_orgs": len(slug_found),
        "rows_gained_from_unresolved": sum(
            fr_counts[key] for key in keys if base_category[key] == "unresolved" and slug_codes[key] is not None
        ),
        "rows_gained_any_category": sum(
            fr_counts[key] for key in keys if base[key] is None and slug_codes[key] is not None
        ),
        "unresolved_rows_naming_a_slug_found_fr_id": sum(
            fr_counts[key] for key in keys if base_category[key] == "unresolved" and {i for i, _ in key} & slug_ids
        ),
        "baseline_unresolved_rows": base_categories["unresolved"],
    }

    use_rows(projection_for(identity, events))
    after_categories: Counter[str] = Counter()
    after_docketless_categories: Counter[str] = Counter()
    for key in keys:
        cat = category(key)
        after_categories[cat] += fr_counts[key]
        after_docketless_categories[cat] += docketless[key]
    use_rows(BASE_ROWS)

    commit = subprocess.run(
        ["git", "-C", str(args.spicy_regs), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    result = {
        "rule": "spicy_regs.ontology.agencies.agency_code_for_fr_agencies",
        "spicy_regs_commit": commit,
        "spicy_regs_agencies_module_sha256": sha256(Path(agencies.__file__)),
        "measurement_script_sha256": sha256(Path(__file__)),
        "vendored_projection_sha256": "sha256:" + agencies.AGENCY_PROJECTION_SHA256,
        "replay": {
            "federal_register_sha256": sha256(args.replay_root / "federal_register.parquet"),
            "proceedings_sha256": sha256(args.replay_root / "proceedings.parquet"),
        },
        "refspec_candidates_digest": report["candidates_digest"],
        "baseline": {
            "federal_register_rows": dict(sorted(base_categories.items())),
            "federal_register_row_total": sum(fr_counts.values()),
            "distinct_agency_sets": len(keys),
            "docketless_proceedings": docketless_total,
            "docketless_unmatched_fr_document": unmatched,
            "docketless_by_category": dict(sorted(base_docketless_categories.items())),
            "docketless_not_resolved": sum(v for k, v in base_docketless_categories.items() if k != "resolved"),
        },
        "all_batch1_adopted": {
            "federal_register_rows": dict(sorted(after_categories.items())),
            "docketless_by_category": dict(sorted(after_docketless_categories.items())),
            "delta": delta(everything),
        },
        "effects": effects,
        "split_variants": split_variants,
        "withdrawn_reference": withdrawn_reference,
        "first_brief_31648_of_41937": reconciliation,
    }
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
