"""Build locally through MembersRollup, then compare its tables to retained raw JSON.

Run from spicy-regs with the installed provider. --build requires a new private
output directory and performs the existing two complete-roster acquisitions.
Without --build, qualification reads retained bytes only. No publication occurs.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from hashlib import sha256
from importlib.metadata import version
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from spicy_docs.schemas.legislator_tables import MEMBERS, MEMBER_PARTY_AFFILIATIONS, MEMBER_TERMS
from spicy_docs.sources.legislators import LEGISLATORS_CURRENT_URL, LEGISLATORS_HISTORICAL_URL
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.source_evidence import verify_evidence

PROVIDER = "0.53.0+scorecards.d030db4a182c"
TABLES = (MEMBERS, MEMBER_TERMS, MEMBER_PARTY_AFFILIATIONS)
URLS = {"current": LEGISLATORS_CURRENT_URL, "historical": LEGISLATORS_HISTORICAL_URL}
ID_FIELDS = {
    "bioguide_id": "bioguide",
    "lis_id": "lis",
    "icpsr_id": "icpsr",
    "govtrack_id": "govtrack",
    "votesmart_id": "votesmart",
    "opensecrets_id": "opensecrets",
    "wikidata_id": "wikidata",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def text(value):
    return None if value is None else str(value)


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def equal(actual, expected, location: str) -> None:
    require(actual == expected, f"Raw source/output mismatch: {location}")


def json_field(row, column: str, source: dict, field: str, location: str) -> None:
    if field not in source:
        equal(row[column], None, location)
    else:
        require(isinstance(row[column], str), f"Present source JSON must remain explicit: {location}")
        equal(json.loads(row[column]), source[field], location)


def qualify(output: Path) -> dict:
    generations = list((output / "generations").iterdir())
    require(len(generations) == 1, "Select one isolated local generation")
    generation = generations[0]
    artifact = verify_generation(generation)
    require(artifact.root["spec"]["family"] == "members", "Wrong generation family")
    require(artifact.root["spec"]["packages"]["spicy-docs"] == PROVIDER, "Wrong build provider")
    require(not artifact.root["spec"]["readSnapshot"]["families"], "Qualification expects a clean local rebuild")
    evidence_paths = list(output.glob("source-evidence/*/artifact"))
    require(len(evidence_paths) == 1, "Expected one retained evidence artifact")
    evidence_path = evidence_paths[0]
    evidence = verify_evidence(evidence_path)
    equal(
        artifact.root["inputs"],
        [
            {
                "role": "source-evidence",
                "logicalId": evidence.pin.logical_id,
                "artifactDigest": evidence.pin.artifact_digest,
            }
        ],
        "generation evidence pin",
    )
    events = [json.loads(line) for line in (evidence_path / "journal.jsonl").read_text().splitlines()]
    require(not any(row["event"] in {"refusal", "capture-incomplete"} for row in events), "Source refusal present")
    captures = [row for row in events if row["event"] == "capture"]
    equal(
        Counter(row["stage"] for row in captures),
        Counter({"current": 1, "current:used": 1, "historical": 1, "historical:used": 1}),
        "capture stages",
    )

    rows, indexed = {}, {}
    for table in TABLES:
        path = generation / f"{table.name}.parquet"
        rows[table.name] = pq.read_table(path).to_pylist()
        values = {}
        for row in rows[table.name]:
            table.checked(row)
            key = tuple(row[column] for column in table.identity)
            require(all(value is not None for value in key), f"Null identity: {table.name}")
            require(key not in values, f"Duplicate identity: {table.name}")
            values[key] = row
        indexed[table.name] = values

    counts = Counter()
    coverage = {}
    diagnostics = {"missing": defaultdict(list), "collisions": {}}
    crosswalks = defaultdict(lambda: defaultdict(set))
    observed_keys = {table.name: set() for table in TABLES}
    source_reports = []
    examples = []
    for roster, url in URLS.items():
        receipt = next(row for row in captures if row["stage"] == roster)
        used = next(row for row in captures if row["stage"] == roster + ":used")
        for field in ("sha256", "observed_at", "requested_url", "resolved_url", "byte_size"):
            equal(receipt[field], used[field], f"{roster}: used capture")
        equal(receipt["requested_url"], url, f"{roster}: route")
        require(
            receipt["response_complete"] and receipt["body_retained"] and receipt["status_code"] == 200,
            "Incomplete roster capture",
        )
        equal(receipt["evidence_policy"], "full", "community-source evidence policy")
        source_path = evidence_path / receipt["blob_member"]
        body = source_path.read_bytes()
        equal("sha256:" + sha256(body).hexdigest(), receipt["sha256"], "raw body hash")
        equal(len(body), receipt["byte_size"], "raw body byte count")
        # Direct JSON decoding is independent of the provider's record parser
        # and shapers. Every mapped literal is compared below.
        source = json.loads(body)
        require(isinstance(source, list) and bool(source), "Roster is not a complete nonempty JSON array")
        coverage[roster] = Counter()
        raw_seen = set()
        for position, raw in enumerate(source):
            bioguide = raw["id"]["bioguide"]
            require(bioguide not in raw_seen, "Duplicate source Bioguide")
            raw_seen.add(bioguide)
            key = (bioguide,)
            require(key not in observed_keys["members"], "Overlapping current/historical member identity")
            row = indexed["members"][key]
            observed_keys["members"].add(key)
            location = f"{roster}/{position}"
            for column, field in ID_FIELDS.items():
                equal(row[column], text(raw["id"].get(field)), location + "/id/" + field)
                if row[column] is None:
                    diagnostics["missing"][column].append(bioguide)
                else:
                    coverage[roster][column] += 1
                    crosswalks[column][row[column]].add(bioguide)
            equal(json.loads(row["fec_ids_json"]), raw["id"].get("fec", []), location + "/id/fec")
            for value in raw["id"].get("fec", []):
                crosswalks["fec_id"][value].add(bioguide)
            for column, container, field in (
                ("other_names_json", raw, "other_names"),
                ("bioguide_previous_json", raw["id"], "bioguide_previous"),
            ):
                json_field(row, column, container, field, location + "/" + field)
                if field in container:
                    coverage[roster][column] += 1
            for value in raw["id"].get("bioguide_previous", []) or []:
                crosswalks["bioguide_id"][value].add(bioguide)
            for field in ("first", "last"):
                equal(row["name_" + field], raw["name"][field], location + "/name/" + field)
            equal(row["roster"], roster, location + "/roster")
            equal(row["observed_at"], receipt["observed_at"], location + "/observed_at")
            equal(row["term_count"], str(len(raw["terms"])), location + "/terms count")
            equal(row["first_term_start"], raw["terms"][0].get("start"), location + "/first_term_start")
            equal(row["last_term_end"], raw["terms"][-1].get("end"), location + "/last_term_end")
            for field in ("type", "state", "party", "district"):
                equal(row["current_term_" + field], text(raw["terms"][-1].get(field)), location + "/latest/" + field)
            counts["members"] += 1
            if raw.get("other_names") or raw["id"].get("bioguide_previous"):
                examples.append(
                    {
                        "roster": roster,
                        "source_path": f"/{position}",
                        "bioguide_id": bioguide,
                        "other_names": raw.get("other_names"),
                        "bioguide_previous": raw["id"].get("bioguide_previous"),
                    }
                )
            for term_index, term in enumerate(raw["terms"]):
                term_key = (bioguide, str(term_index))
                term_row = indexed["member_terms"][term_key]
                observed_keys["member_terms"].add(term_key)
                for field in ("type", "start", "end", "state", "party", "district"):
                    equal(term_row["term_" + field], text(term.get(field)), location + f"/terms/{term_index}/{field}")
                equal(term_row["observed_at"], receipt["observed_at"], location + "/term observed_at")
                affiliations = term.get("party_affiliations")
                state = (
                    "absent"
                    if "party_affiliations" not in term
                    else "null"
                    if affiliations is None
                    else "populated"
                    if affiliations
                    else "empty"
                )
                equal(term_row["party_affiliations_state"], state, location + "/affiliation state")
                coverage[roster]["affiliations_state:" + state] += 1
                counts["member_terms"] += 1
                for index, affiliation in enumerate(affiliations or []):
                    affiliation_key = (bioguide, receipt["sha256"], str(term_index), str(index))
                    found = indexed["member_party_affiliations"][affiliation_key]
                    observed_keys["member_party_affiliations"].add(affiliation_key)
                    equal(json.loads(found["source_json"]), affiliation, location + "/affiliation source_json")
                    equal(
                        found["source_path"],
                        f"/{position}/terms/{term_index}/party_affiliations/{index}",
                        location + "/affiliation pointer",
                    )
                    for column, field in (
                        ("party", "party"),
                        ("affiliation_start", "start"),
                        ("affiliation_end", "end"),
                    ):
                        equal(found[column], affiliation.get(field), location + "/affiliation " + field)
                    equal(found["term_party"], term.get("party"), location + "/affiliation term_party")
                    equal(found["observed_at"], receipt["observed_at"], location + "/affiliation observed_at")
                    counts["member_party_affiliations"] += 1
        source_reports.append(
            {
                field: receipt[field]
                for field in (
                    "requested_url",
                    "resolved_url",
                    "observed_at",
                    "sha256",
                    "byte_size",
                    "status_code",
                    "content_type",
                    "response_complete",
                    "evidence_policy",
                    "capture_id",
                )
            }
            | {"roster": roster, "source_body_path": str(source_path), "records": len(source)}
        )

    for table in TABLES:
        equal(observed_keys[table.name], set(indexed[table.name]), table.name + " complete source membership")
        equal(counts[table.name], len(rows[table.name]), table.name + " source count")
    for scheme, identifiers in crosswalks.items():
        diagnostics["collisions"][scheme] = {
            key: sorted(values) for key, values in identifiers.items() if len(values) > 1
        }
    write_json(output / "identity_diagnostics.json", diagnostics)
    write_json(output / "historical_identity_raw_examples.json", examples)
    manifest = json.loads((generation / "members.json").read_text())
    pins = {
        row["objectKey"].removesuffix(".parquet"): {
            "sha256": row["sha256"],
            "byteSize": row["byteSize"],
            "family": "members",
            "artifactDigest": artifact.pin.artifact_digest,
        }
        for row in manifest["members"]
    }
    paths = {name: str(generation / (name + ".parquet")) for name in pins}
    write_json(output / "analysis_inputs.json", {"paths": paths, "input_pins": pins})
    report = {
        "status": "qualified-local",
        "publication": False,
        "ticket": "ss-kvli",
        "provider": PROVIDER,
        "generation_path": str(generation),
        "generation_pin": {"logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest},
        "evidence_path": str(evidence_path),
        "evidence_pin": {"logicalId": evidence.pin.logical_id, "artifactDigest": evidence.pin.artifact_digest},
        "source_selection": "Fresh existing community current/historical JSON routes; no prior merge or extra congressional feed.",
        "capture_accounting": "One GET per roster; acquired and used journal receipts refer to the same retained body.",
        "completeness": "Both bounded HTTP bodies reached EOF and parsed as complete arrays; every member, term and nested affiliation reconciles to the outputs.",
        "source_declared_totals": None,
        "source_reports": source_reports,
        "rows": dict(counts),
        "field_coverage": coverage,
        "null_identity_fields": {field: len(diagnostics["missing"].get(field, [])) for field in ID_FIELDS},
        "identifier_collisions": diagnostics["collisions"],
        "input_pins": pins,
        "paths": paths,
        "checks": [
            "generation and evidence admission",
            "all-VARCHAR schemas and unique non-null primary keys",
            "all retained raw JSON members and terms equal output literals",
            "historical JSON absence/null/order preserved",
            "every affiliation source JSON, literal dates, ordinal and pointer preserved",
            "exact source membership and foreign keys",
        ],
        "limitations": [
            "Observed community crosswalk completeness is not a claim that the source lists every historical person or identifier.",
            "Unstated IDs remain NULL; identity collisions remain explicit and are not deduplicated.",
            "The upstream JSON routes do not expose a repository commit pin; retained body hashes bind the actual observations.",
        ],
        "qualification_script_sha256": "sha256:" + sha256(Path(__file__).read_bytes()).hexdigest(),
        "supplemental_artifacts": {
            name: {
                "sha256": "sha256:" + sha256((output / name).read_bytes()).hexdigest(),
                "byteSize": (output / name).stat().st_size,
            }
            for name in ("identity_diagnostics.json", "historical_identity_raw_examples.json", "analysis_inputs.json")
        },
    }
    write_json(output / "qualification.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--report-copy", type=Path)
    args = parser.parse_args()
    if args.build:
        require(version("spicy-docs") == PROVIDER, "Build with the pinned installed provider; do not sync environments")
        require(not os.environ.get("R2_PUBLIC_URL"), "Local rebuild requires R2_PUBLIC_URL unset")
        require(not args.output_dir.exists(), "Build requires a new private output directory")
        MembersRollup(output_dir=args.output_dir, skip_upload=True).run()
    report = qualify(args.output_dir)
    if args.report_copy:
        write_json(args.report_copy, report)
    print(json.dumps({"status": report["status"], "rows": report["rows"], "generation_pin": report["generation_pin"]}))


if __name__ == "__main__":
    main()
