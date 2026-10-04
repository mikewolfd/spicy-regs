"""Independently compare candidate Parquet values with the immutable HRC asset."""

from __future__ import annotations

import argparse
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path


from prepare_hrc import canonical, check_public_evidence
from spicy_regs.scorecards.etl import read_source_generation
from spicy_regs.generations import verify_generation

CARD = "hrc:118-final"


def value_readback(asset, tables):
    counts = {}
    normalizations = []
    for table, group, field in (
        ("scorecard_member_ratings", "ratings", "value_text"),
        ("scorecard_member_item_results", "item_results", "result_text"),
    ):
        expected = {
            f"/members/{i}/record/{group}/{j}": record
            for i, member in enumerate(asset["members"])
            for j, record in enumerate(member["record"][group])
        }
        observed = set()
        for row in tables[table]:
            pointer = row["source_path"].rsplit(";", 1)[-1]
            if pointer not in expected or pointer in observed:
                raise ValueError("Candidate contains an unknown or duplicate source value locator")
            observed.add(pointer)
            source_value = expected[pointer][field]
            value = "N/A" if group == "ratings" and source_value == "NA" else source_value
            if row[field] != value:
                raise ValueError("Candidate differs from the raw source value and its permitted rating mapping")
            if group == "ratings" and value == "N/A" and row["value_number"] is not None:
                raise ValueError("An unavailable rating acquired a numeric value")
            if source_value != value:
                normalizations.append(
                    {
                        "source_pointer": pointer,
                        "publisher_member_key": row["publisher_member_key"],
                        "metric_id": row["metric_id"],
                        "raw_value": source_value,
                        "native_value": value,
                    }
                )
        if observed != set(expected):
            raise ValueError("Candidate omits source rating or result values")
        counts[table] = len(observed)
    return {"checked_values": counts, "normalization_count": len(normalizations), "normalizations": normalizations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--asset", type=Path, required=True)
    parser.add_argument("--prior-generation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Preserve prior readbacks; choose a new receipt path")
    prepared = json.loads(args.preparation.read_bytes())
    artifact = verify_generation(Path(prepared["generation_directory"]))
    prior_artifact = verify_generation(args.prior_generation)
    if artifact.pin.as_dict() != prepared["generation"]:
        raise ValueError("Generation differs from preparation receipt")
    if prior_artifact.pin.artifact_digest != prepared["prior_generation"]:
        raise ValueError("Wrong prior source generation")
    asset_bytes = args.asset.read_bytes()
    asset_hash = "sha256:" + sha256(asset_bytes).hexdigest()
    if asset_hash != prepared["qualified_asset_sha256"]:
        raise ValueError("Readback asset differs from prepared input")
    tables = {}
    preserved = {}
    current_rows = read_source_generation(Path(prepared["generation_directory"]))
    prior_rows = read_source_generation(args.prior_generation)
    for name, rows in current_rows.items():
        field, scope = ("publisher_id", "hrc") if name == "scorecard_publishers" else ("scorecard_id", CARD)
        kept = [row for row in rows if row[field] != scope]
        old = prior_rows[name]
        old = [row for row in old if row[field] != scope]
        if canonical(old) != canonical(kept):
            raise ValueError("A prior source value or observation identifier changed")
        tables[name] = [row for row in rows if row[field] == scope]
        preserved[name] = len(kept)
    result = value_readback(json.loads(asset_bytes), tables)
    check_public_evidence(Path(prepared["evidence_directory"]))
    report = {
        "status": "local_candidate_independent_readback_passed_not_published",
        "generation": artifact.pin.as_dict(),
        "candidate_receipt_sha256": sha256(args.preparation.read_bytes()).hexdigest(),
        "raw_qualified_asset_sha256": asset_hash,
        "rating_normalization_rule": "Exact standalone NA to N/A in rating value_text only; raw asset unchanged",
        **result,
        "hrc_counts": {name: len(rows) for name, rows in tables.items()},
        "native_na_remaining": sum(row["value_text"] == "NA" for row in tables["scorecard_member_ratings"]),
        "na_ratings": sum(row["value_text"] == "N/A" for row in tables["scorecard_member_ratings"]),
        "result_tokens": dict(Counter(row["result_text"] for row in tables["scorecard_member_item_results"])),
        "prior_generation": prior_artifact.pin.as_dict(),
        "independent_exact_preserved_row_counts": preserved,
        "all_prior_source_values_and_observation_identifiers_preserved": True,
        "remote_writes": 0,
    }
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
