"""Select source-matching immutable member records; refuse incomplete HRC coverage.

This research artifact is not a production table or publication. Selection is at
an explicitly checked record boundary; generated records themselves are never
edited, and every rejected observation remains retained.
"""

from __future__ import annotations

import argparse
import json
import pickle
from hashlib import sha256
from pathlib import Path

from jsonschema import Draft202012Validator
from house_source_validation import compare, source_inventory
from probe_gemini import SOURCE_SHA256
from spicy_docs.sources.scorecards.hrc_outcomes import HOUSE_ITEMS, RATING_PERIODS, outcome_schema, validate_outcome

CAMPAIGNS = (
    "gemini-v3-house-bands-page10",
    "gemini-v3-house-bands-page19-pilot",
    "gemini-v3-house-bands-page26-pilot",
    "gemini-v3-house-bands-remaining",
    "gemini-v3-house-bands-page19-rest",
    "gemini-v3-house-single-retries-pass1",
    "gemini-v3-house-schiff-marker-retry",
    "gemini-v3-house-single-retries-pass2",
    "gemini-v3-house-uncertain-band-retry",
)


def keyed(records, field, expected):
    keys = [r[field] for r in records]
    return len(keys) == len(expected) and set(keys) == set(expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = (args.corpus / "118-pdf.body").read_bytes()
    inventory = source_inventory(source)
    reference_path = args.corpus / "original-house-cell-readback-300dpi/manual-original-cell-reference.json"
    reference = json.loads(reference_path.read_text())
    if reference["source_sha256"] != SOURCE_SHA256:
        raise ValueError("manual source reference mismatch")
    manual = {
        (p["physical_page"], c["member_name"], c["item_key"]): c["value"]
        for p in reference["pages"]
        for c in p["cells"]
    }
    paths = [args.corpus / "gemini-page10-v3-alabama-paired-shared/outcome.json"]
    paths += [p for campaign in CAMPAIGNS for p in sorted((args.corpus / campaign).glob("*/outcome.json"))]
    selected = {}
    refused = []
    vacancy = None
    for path in paths:
        if not path.exists():
            continue
        parent = path.parent
        outcome = json.loads(path.read_text())
        settings = json.loads((parent / "settings.json").read_text())
        if settings["source_sha256"] != SOURCE_SHA256 or settings["schema_version"] != "hrc-118-source-facts/3":
            raise ValueError("observation source/schema mismatch")
        number = settings["physical_page"]
        page = next(p for p in inventory["pages"] if p["physical_page"] == number)
        scoped_rows = settings.get("source_rows")
        if scoped_rows is None:
            if parent.name != "gemini-page10-v3-alabama-paired-shared":
                raise ValueError("missing explicitly reviewed observation scope")
            scoped_rows = list(range(1, 8))
        Draft202012Validator(outcome_schema(number)).validate(outcome)
        with (parent / "observation.pickle").open("rb") as stream:
            observation = pickle.load(stream)  # trusted locally retained shared-backend evidence
        if observation.raw["outcome"] != outcome:
            raise ValueError("decoded model outcome changed after retention")
        attribution = {
            "outcome_path": str(path.relative_to(args.corpus)),
            "outcome_sha256": sha256(path.read_bytes()).hexdigest(),
            "observation_sha256": sha256((parent / "observation.pickle").read_bytes()).hexdigest(),
            "settings_sha256": sha256((parent / "settings.json").read_bytes()).hexdigest(),
            "physical_page": number,
        }
        names = [r["member_name_text"] for r in outcome["members"]]
        if outcome["uncertainties"] or len(names) != len(set(names)):
            refused.append(
                {**attribution, "scope": "whole observation", "reason": "source uncertainty or duplicate identity"}
            )
            continue
        for index, member in enumerate(outcome["members"]):
            matches = [
                r for r in page["rows"] if r["name"] == member["member_name_text"] and r["source_row"] in scoped_rows
            ]
            report = compare(page, {"members": [member]}, manual)
            valid = (
                len(matches) == 1
                and keyed(member["item_results"], "item_key", HOUSE_ITEMS)
                and keyed(member["ratings"], "period_text", RATING_PERIODS)
                and not report["differences"]
                and not report["unclassified_cells"]
                and report["counts"]["ratings"] == 3
                and report["counts"]["classified_cells"] + report["counts"]["manual_image_cells"] == 41
            )
            if not valid:
                refused.append(
                    {
                        **attribution,
                        "scope": f"/members/{index}",
                        "reason": "not every field and key matched original source",
                        "differences": report["differences"],
                    }
                )
                continue
            key = (number, matches[0]["source_row"])
            record = {
                "source_row": key[1],
                "record": member,
                "evidence": {**attribution, "json_pointer": f"/members/{index}"},
            }
            if key in selected and selected[key]["record"] != member:
                raise ValueError("independently source-matching observations disagree")
            selected.setdefault(key, record)
        for index, record in enumerate(outcome["vacant_seats"]):
            valid = (
                number == 19
                and 3 in scoped_rows
                and record["label_text"] == "Vacant"
                and record["state_text"] == "NEW JERSEY"
                and record["district_text"] == "9"
                and record["footnote_markers"] == ["11"]
                and keyed(record["item_results"], "item_key", HOUSE_ITEMS)
                and keyed(record["ratings"], "period_text", RATING_PERIODS)
                and all(r["result_text"] is None for r in record["item_results"])
                and all(r["value_text"] == "N/A" for r in record["ratings"])
            )
            if not valid:
                refused.append(
                    {
                        **attribution,
                        "scope": f"/vacant_seats/{index}",
                        "reason": "vacancy differs from reviewed source row",
                    }
                )
                continue
            value = {
                "source_row": 3,
                "record": record,
                "evidence": {**attribution, "json_pointer": f"/vacant_seats/{index}"},
            }
            if vacancy and vacancy["record"] != record:
                raise ValueError("vacancy observations disagree")
            vacancy = vacancy or value
    missing = []
    page_validation_failures = []
    pages = []
    for page in inventory["pages"]:
        number = page["physical_page"]
        expected = [r for r in page["rows"] if r["name"] != "Vacant"]
        missing += [
            {"physical_page": number, "source_row": r["source_row"], "member_name": r["name"]}
            for r in expected
            if (number, r["source_row"]) not in selected
        ]
        records = [selected[(number, r["source_row"])] for r in expected if (number, r["source_row"]) in selected]
        members = [r["record"] for r in records]
        vacancies = [vacancy["record"]] if number == 19 and vacancy else []
        if len(records) == len(expected):
            try:
                validate_outcome(
                    number,
                    {
                        "members": members,
                        "vacant_seats": vacancies,
                        "notes": [],
                        "scoring_notes": [],
                        "result_definitions": [],
                        "uncertainties": [],
                    },
                    member_grid_only=True,
                )
            except ValueError as error:
                page_validation_failures.append({"physical_page": number, "reason": str(error)})
        pages.append(
            {"physical_page": number, "members": records, "vacant_seats": [vacancy] if number == 19 and vacancy else []}
        )
    if vacancy is None:
        missing.append({"physical_page": 19, "source_row": 3, "label": "Vacant"})
    result = {
        "source_sha256": SOURCE_SHA256,
        "manual_reference_sha256": sha256(reference_path.read_bytes()).hexdigest(),
        "status": "complete_source_checked_House_member_candidate"
        if not missing and not page_validation_failures
        else "incomplete_refused_candidate",
        "full_edition_qualified": False,
        "production_reader_qualified": False,
        "scope": "House named member records and separate vacancy only; notes/items/legends selected separately",
        "counts": {
            "named_members": len(selected),
            "ratings": len(selected) * 3,
            "item_results": len(selected) * 41,
            "vacancies": int(vacancy is not None),
        },
        "missing_source_rows": missing,
        "page_validation_failures": page_validation_failures,
        "pages": pages,
        "rejected_record_observations": refused,
    }
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ("status", "counts", "missing_source_rows", "page_validation_failures")}))
    if missing or page_validation_failures:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
