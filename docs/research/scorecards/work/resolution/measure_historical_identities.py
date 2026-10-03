"""Measure retained commit-pinned crosswalk inputs without acquisition or publication."""

from __future__ import annotations

import argparse
from collections import defaultdict
from hashlib import sha256
import json
from pathlib import Path

import yaml

from spicy_docs.schemas.legislator_tables import MEMBERS, shape_member
from spicy_docs.sources.legislators import MAX_HISTORICAL_BYTES, parse_legislators

COMMIT = "d5af3d2d2490f8c6532c9490b1f47a76cacca699"
PINS = {
    "legislators-current.yaml": "bd32e62b1c89e9585e4ec49df5676563510e9435d779c061ff50139833f73a49",
    "legislators-historical.yaml": "9641615f336b3113d6f89152e3441128cbdc3f61ccf87c65da3a015076c78c91",
}


def measure(folder: Path) -> dict:
    reports = []
    ids = defaultdict(set)
    for name, digest in PINS.items():
        body = (folder / name).read_bytes()
        if sha256(body).hexdigest() != digest:
            raise ValueError(f"Pinned source changed: {name}")
        original = yaml.load(body, Loader=yaml.CSafeLoader)
        # The existing reader consumes JSON. YAML scalar dates are spelled as
        # ISO text, the pinned dataset's JSON representation, for offline proof.
        json_body = json.dumps(original, default=str).encode()
        parsed = parse_legislators(json_body, max_bytes=MAX_HISTORICAL_BYTES)
        observed = []
        for raw, record in zip(original, parsed.records, strict=True):
            row = shape_member(record, roster=name.split("-")[1].split(".")[0], observed_at="2026-10-03T00:00:00Z")
            MEMBERS.checked(row)
            ids[record.bioguide].add(record.bioguide)
            for value in raw["id"].get("bioguide_previous", []):
                ids[value].add(record.bioguide)
            if "other_names" in raw or "bioguide_previous" in raw["id"]:
                assert json.loads(row["other_names_json"] or "null") == raw.get("other_names")
                assert json.loads(row["bioguide_previous_json"] or "null") == raw["id"].get("bioguide_previous")
                observed.append(
                    {
                        "bioguide_id": record.bioguide,
                        "other_names": raw.get("other_names"),
                        "bioguide_previous": raw["id"].get("bioguide_previous"),
                    }
                )
        reports.append(
            {
                "file": name,
                "sha256": digest,
                "records_parsed_and_shaped": len(parsed.records),
                "records_with_other_names": sum("other_names" in row for row in original),
                "records_with_previous_bioguide": sum("bioguide_previous" in row["id"] for row in original),
                "bounded_identity_observations": observed,
            }
        )
    return {
        "upstream_commit": COMMIT,
        "upstream_dictionary": f"https://github.com/unitedstates/congress-legislators/blob/{COMMIT}/README.md",
        "measurement": "Offline source parsing and member row shaping from retained pinned YAML converted to JSON.",
        "acquisition": False,
        "publication": False,
        "files": reports,
        "cross_person_current_or_previous_id_collisions": {
            key: sorted(values) for key, values in ids.items() if len(values) > 1
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(measure(args.input_dir), indent=2, ensure_ascii=False) + "\n")
