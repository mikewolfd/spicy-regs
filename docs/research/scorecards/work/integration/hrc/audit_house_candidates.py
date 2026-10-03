"""Compare immutable HRC observations with original PDF spans and image readback."""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from pathlib import Path

from house_source_validation import compare, source_inventory
from probe_gemini import SOURCE_SHA256
from spicy_docs.sources.scorecards.hrc_outcomes import HOUSE_ITEMS, RATING_PERIODS


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--reference", type=Path, required=True)
    p.add_argument("--campaigns", type=Path, nargs="+", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    inventory = source_inventory(args.source.read_bytes())
    reference = json.loads(args.reference.read_text())
    if reference["source_sha256"] != SOURCE_SHA256:
        raise ValueError("manual reference belongs to another source")
    manual = {
        (p["physical_page"], c["member_name"], c["item_key"]): c["value"]
        for p in reference["pages"]
        for c in p["cells"]
    }
    reports = []
    for campaign in args.campaigns:
        for outcome_path in sorted(campaign.glob("*/outcome.json")):
            settings = json.loads((outcome_path.parent / "settings.json").read_text())
            if settings["source_sha256"] != SOURCE_SHA256:
                raise ValueError("observation belongs to another source")
            page = next(p for p in inventory["pages"] if p["physical_page"] == settings["physical_page"])
            outcome = json.loads(outcome_path.read_text())
            report = compare(page, outcome, manual)
            report.update(
                {
                    "physical_page": settings["physical_page"],
                    "source_rows": settings["source_rows"],
                    "path": str(outcome_path),
                    "outcome_sha256": sha256(outcome_path.read_bytes()).hexdigest(),
                }
            )
            expected_rows = [
                r for r in page["rows"] if r["source_row"] in settings["source_rows"] and r["name"] != "Vacant"
            ]
            observed_names = [m["member_name_text"] for m in outcome["members"]]
            missing = [r["name"] for r in expected_rows if r["name"] not in observed_names]
            extra = [n for n in observed_names if n not in {r["name"] for r in expected_rows}]
            coverage_problems = []
            bad_names = {d["member"] for d in report["differences"]} | set(missing)
            if len(observed_names) != len(set(observed_names)):
                coverage_problems.append("duplicate_member_identity")
                bad_names.update(observed_names)
            for member in outcome["members"]:
                item_keys = [r["item_key"] for r in member["item_results"]]
                period_keys = [r["period_text"] for r in member["ratings"]]
                if len(item_keys) != len(HOUSE_ITEMS) or set(item_keys) != set(HOUSE_ITEMS):
                    coverage_problems.append("incomplete_or_duplicate_item_keys")
                    bad_names.add(member["member_name_text"])
                if len(period_keys) != len(RATING_PERIODS) or set(period_keys) != set(RATING_PERIODS):
                    coverage_problems.append("incomplete_or_duplicate_rating_periods")
                    bad_names.add(member["member_name_text"])
            report["coverage_problems"] = sorted(set(coverage_problems))
            report["missing_names"] = missing
            report["unexpected_names"] = extra
            report["retry_source_rows"] = [r["source_row"] for r in expected_rows if r["name"] in bad_names]
            report["source_uncertainties"] = outcome["uncertainties"]
            report["status"] = (
                "refused_candidate"
                if report["differences"]
                or missing
                or extra
                or outcome["uncertainties"]
                or report["unclassified_cells"]
                or coverage_problems
                else "all_named_member_fields_match_source_checks"
            )
            reports.append(report)
    result = {
        "source_sha256": SOURCE_SHA256,
        "manual_reference_sha256": sha256(args.reference.read_bytes()).hexdigest(),
        "method": "Original native spans plus separately recorded direct original-cell image readback. No model output used as reference. Status concerns bounded named-member records, not edition completeness or all source notes.",
        "reports": reports,
        "full_edition_qualified": False,
    }
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                "observations": len(reports),
                "source_checked_pass": sum(
                    r["status"] == "all_named_member_fields_match_source_checks" for r in reports
                ),
                "refused": sum(r["status"] == "refused_candidate" for r in reports),
            }
        )
    )


if __name__ == "__main__":
    main()
