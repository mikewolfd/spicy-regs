"""Generate an exhaustive integration work queue from pinned discovery and qualifications.

Discovery remains evidence at its recorded date. Edition qualification advances
only the stated scope; it never promotes an entire publisher archive implicitly.
"""

import argparse
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any


SURVEY_FILE = "publisher_survey_20261005.json"


def survey_findings(document, inventory):
    """Select discovery metadata; no finding establishes qualification or support."""
    entries = document.get("publishers", [])
    identifiers = [entry["publisher_id"] for entry in entries]
    summary = document.get("summary", {})
    if (
        document.get("format_version") != "scorecard-publisher-survey-aggregate/1"
        or summary.get("newly_qualified_editions") != 0
        or summary.get("surveyed_publishers") != len(entries)
        or len(set(identifiers)) != len(identifiers)
        or not set(identifiers) <= set(inventory)
    ):
        raise ValueError("Survey must describe distinct known publishers and discovery only")
    findings = {}
    for entry in entries:
        survey = entry["survey"]
        if not isinstance(survey.get("next_step"), str) or not survey["next_step"].strip():
            raise ValueError("Survey needs a concrete discovery or integration next step")
        findings[entry["publisher_id"]] = {
            "observed_on": document["observed_on"],
            "source_file": SURVEY_FILE,
            "task_id": entry["task_id"],
            "assessment": entry["assessment"],
            **{
                name: survey.get(name)
                for name in (
                    "latest_publication",
                    "latest_offered",
                    "newest_recovered",
                    "year_intervals",
                    "unknown_year_intervals",
                    "inspection_limits",
                    "next_step",
                )
            },
        }
    return findings


def pinned_document(directory, relative, pin):
    path = (directory / relative).resolve()
    if Path(relative).is_absolute() or not path.is_relative_to(directory.resolve()):
        raise ValueError("Qualification receipt leaves the integration directory")
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != pin:
        raise ValueError("Qualification receipt differs from its recorded pin")
    return json.loads(raw)


def nested(value, path):
    for key in path:
        value = value[key]
    return value


def read_receipt(directory, record):
    receipt = pinned_document(directory, record["receipt"], record["receipt_sha256"])
    if receipt.get("format_version") == "scorecard-integration-published-observation/2":
        source = pinned_document(directory, receipt["source_qualification"], receipt["source_qualification_sha256"])
        if source.get("format_version") != "scorecard-integration-qualified-scope/1":
            raise ValueError("Publication needs an immutable source qualification, not another publication")
        qualification = read_receipt(
            directory,
            dict(
                publisher_id=record["publisher_id"],
                scorecard_id=record["scorecard_id"],
                state="qualified",
                receipt=receipt["source_qualification"],
                receipt_sha256=receipt["source_qualification_sha256"],
            ),
        )
        readback = pinned_document(directory, receipt["public_readback"], receipt["public_readback_sha256"])
        scope = readback.get("scopes", {}).get(record["scorecard_id"])
        if (
            record["state"] != "published"
            or qualification.get("format_version") != "scorecard-integration-qualified-scope/1"
            or receipt.get("publisher_id") != record["publisher_id"]
            or receipt.get("scorecard_id") != record["scorecard_id"]
            or receipt.get("counts") != qualification["counts"]
            or receipt.get("parser_version") != qualification["parser_version"]
            or readback.get("format_version") != "scorecard-family-publication-readback/1"
            or readback.get("status") != "passed"
            or readback.get("source_generation") != receipt.get("observed_source_generation")
            or scope
            != dict(
                publisher_id=record["publisher_id"],
                parser_version=qualification["parser_version"],
                counts=qualification["counts"],
            )
            or readback.get("public_member_pins_verified") is not True
            or readback.get("hosted_scope_counts_verified") is not True
        ):
            raise ValueError("Publication observation differs from its qualified complete public scope")
        return receipt
    if receipt.get("format_version") == "scorecard-integration-published-observation/1":
        qualification = pinned_document(
            directory, receipt["source_qualification"], receipt["source_qualification_sha256"]
        )
        readback = pinned_document(directory, receipt["public_readback"], receipt["public_readback_sha256"])
        counts = nested(qualification, receipt["qualification_counts_path"])
        if (
            record["state"] != "published"
            or receipt.get("publisher_id") != record["publisher_id"]
            or receipt.get("scorecard_id") != record["scorecard_id"]
            or receipt.get("parser_version") != qualification["parser_version"]
            or readback.get("status")
            not in {"independent_public_readback_passed", "independent_public_rating_readback_passed"}
            or counts != receipt.get("counts")
            or not isinstance(counts, dict)
            or not counts
            or any(type(n) is not int or n < 0 for n in counts.values())
            or qualification.get("status") == "refused"
            or qualification.get("error_type")
            or nested(readback, receipt["readback_rating_path"]) != counts["scorecard_member_ratings"]
            or readback["source_generation"] != receipt["observed_source_generation"]
        ):
            raise ValueError("Publication observation differs from its qualified public readback")
        return receipt
    if (
        receipt.get("format_version") != "scorecard-integration-qualified-scope/1"
        or receipt.get("qualified") is not True
        or receipt.get("publisher_id") != record["publisher_id"]
        or receipt.get("scorecard_id") != record["scorecard_id"]
        or record["state"] == "published"
        or not receipt.get("parser_version")
        or not receipt.get("completeness_rule")
        or not isinstance(receipt.get("counts"), dict)
        or not receipt["counts"]
        or any(type(n) is not int or n < 0 for n in receipt["counts"].values())
        or any(
            not re.fullmatch(r"[0-9a-f]{64}", receipt.get(k, ""))
            for k in (
                "reader_sha256",
                "capture_manifest_sha256",
                "native_reference_sha256",
                "source_qualification_sha256",
            )
        )
    ):
        raise ValueError("Receipt does not prove the named complete source scope")
    return receipt


def generate(directory: Path):
    names = ("adapter_inventory.json", "publisher_api_inventory.json", "integration_qualifications.json")
    raw = {name: (directory / name).read_bytes() for name in names}
    if (directory / SURVEY_FILE).exists():
        raw[SURVEY_FILE] = (directory / SURVEY_FILE).read_bytes()
    data = {name: json.loads(body) for name, body in raw.items()}
    inventory = data[names[0]]["publishers"]
    apis = {row["publisher_id"]: row for row in data[names[1]]["publishers"]}
    ledger = data[names[2]]
    if set(inventory) != set(apis):
        raise ValueError("Publisher inventories differ; reconcile candidates before generating coverage")
    findings = survey_findings(data[SURVEY_FILE], inventory) if SURVEY_FILE in data else {}
    qualifications = {}
    scopes = set()
    for record in ledger["editions"]:
        publisher = record["publisher_id"]
        if publisher not in inventory or record["state"] not in {"qualified", "published"}:
            raise ValueError("Qualification must name a known publisher and explicit completed stage")
        if record["scorecard_id"] in scopes:
            raise ValueError("Qualification ledger repeats an edition")
        scopes.add(record["scorecard_id"])
        read_receipt(directory, record)
        qualifications.setdefault(publisher, []).append(record)
    newer_apis = {}
    for observed in ledger.get("api_observations", []):
        if observed["publisher_id"] not in inventory:
            raise ValueError("API observation names an unknown publisher")
        receipt = read_receipt(directory, observed)
        if not observed.get("endpoints") or observed["scorecard_id"] not in scopes:
            raise ValueError("New API observation requires a qualified source scope")
        for endpoint in observed["endpoints"]:
            if endpoint["method"] not in {"GET", "POST"} or not endpoint["url_template"].startswith(
                ("https://", "http://")
            ):
                raise ValueError("Unknown API request shape")
            newer_apis.setdefault(observed["publisher_id"], []).append(
                {
                    **endpoint,
                    "qualification_receipt": observed["receipt"],
                    "parser_version": receipt["parser_version"],
                }
            )
    readers = ledger["readers"]
    rows: list[dict[str, Any]] = []
    for publisher, source in inventory.items():
        editions = qualifications.get(publisher, [])
        state = (
            "published_scope"
            if any(e["state"] == "published" for e in editions)
            else (
                "qualified_scope"
                if editions
                else "reader_implemented"
                if publisher in readers
                else source["support_status"]
            )
        )
        endpoints = [e for e in apis[publisher]["endpoints"] if e["discovery_status"] == "verified_api"]
        routes = [
            {"method": e["method"], "url_template": e["url_template"], "purpose": e["purpose"]} for e in endpoints
        ]
        routes.extend(newer_apis.get(publisher, []))
        rows.append(
            {
                "publisher_id": publisher,
                "publisher_name": source["publisher_name"],
                "task_id": source["task_id"],
                "state": state,
                "reader": readers.get(publisher),
                "qualified_editions": editions,
                "verified_api_endpoints": routes,
                "discovery_api_status": apis[publisher]["discovery_status"],
                "discovery_support_status": source["support_status"],
                "original_source_urls": source["original_source_urls"],
                "survey_finding": findings.get(publisher),
                "next_action": "Integrate remaining available editions and renditions"
                if editions
                else "Qualify original source through the implemented reader"
                if publisher in readers
                else findings[publisher]["next_step"]
                if publisher in findings
                else source.get("next_action")
                or (
                    "Implement source-specific reader using verified endpoints"
                    if endpoints and publisher != "nra_pvf"
                    else "Acquire and profile original source; implement explicit reader"
                ),
            }
        )
    report = {
        "dataset": "scorecard_integration_progress",
        "schema_version": "1",
        "observed_at": ledger["observed_at"],
        "input_pins": {name: sha256(body).hexdigest() for name, body in raw.items()},
        "scope": "Every publisher candidate in both inventories; all available editions and renditions remain in scope",
        "states": dict(sorted(Counter(r["state"] for r in rows).items())),
        "publishers": rows,
        "validation": {"candidate_sets_equal": True, "qualification_receipt_pins_match": True},
        "boundary": "Qualified source scope, installed package, publication and scheduled refresh are separate stages",
    }
    markdown = [
        "# Scorecard integration progress",
        "",
        f"Generated from pinned inputs at {ledger['observed_at']}.",
        "",
        "Edition receipts supersede the older discovery matrix only for their named scopes. The queue retains every candidate and every remaining rendition.",
        "",
        "| Publisher | Stage | Qualified editions | Reader | Next action | Latest survey |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        editions = ", ".join(f"[{e['scorecard_id']}]({e['receipt']})" for e in r["qualified_editions"]) or "Pending"
        finding = r["survey_finding"]
        survey_note = (
            f"[{finding['assessment']['finding'].replace('|', r'\|')}]({SURVEY_FILE.replace('.json', '.md')})"
            if finding
            else "Pending"
        )
        markdown.append(
            f"| {r['publisher_name']} (`{r['publisher_id']}`) | `{r['state']}` | {editions} | `{r['reader'] or 'pending'}` | {r['next_action'].replace('|', r'\|')} | {survey_note} |"
        )
    markdown.extend(
        [
            "",
            "See [observed API routes](publisher_api_routes.md) for rating JSON, JSON containing HTML, embedded JSON and navigation-only distinctions.",
            "Survey findings guide remaining work. Qualification and publication stages come only from their separate receipts.",
        ]
    )
    return report, "\n".join(markdown) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("docs/research/scorecards/work/integration"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    report, markdown = generate(args.directory)
    outputs = {
        "integration_progress.json": json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        "integration_progress.md": markdown,
    }
    for name, body in outputs.items():
        path = args.directory / name
        if args.check:
            if not path.exists() or path.read_text() != body:
                raise ValueError("Generated integration progress is stale; rerun this command without --check")
        else:
            path.write_text(body)
    print(json.dumps({"states": report["states"], "validation": report["validation"]}))


if __name__ == "__main__":
    main()
