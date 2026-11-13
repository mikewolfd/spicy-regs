"""Build an offline NPPES excerpt table and left join caller-supplied identifiers.

Run with coordinated SpicyDocs and RefSpec on PYTHONPATH (see docs/tables/nppes_provider_observations.md).
The source pins and member identity are mandatory; this example never fetches.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from importlib import import_module
from pathlib import Path

import duckdb
import pyarrow as pa
from spicy_docs.sources.nppes import read_csv_cohort
from spicy_regs.transforms.build_nppes_providers import build_observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--header", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--bytes", type=int, required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--member", required=True)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # This optional example uses the identifier owner without making RefSpec a
    # mandatory SpicyRegs dependency or copying its algorithm into this package.
    try:
        validate_npi = import_module("refspec.registry.nppes_npi_identifiers").validate_npi
    except ModuleNotFoundError:
        parser.error("Add RefSpec/src to PYTHONPATH to use the maintained NPI validator")
    requests = json.loads(args.requests.read_text())
    cohort = tuple(dict.fromkeys(validate_npi(r["identifier"]) for r in requests if r["identifier_kind"] == "NPI"))
    columns = tuple(next(csv.reader(io.StringIO(args.header.read_text()))))
    with args.csv.open("rb") as stream:
        data = read_csv_cohort(
            stream,
            kind="main",
            scope="excerpt",
            source_url=args.source_url,
            member_name=args.member,
            expected_sha256=args.sha256,
            expected_bytes=args.bytes,
            columns=columns,
            cohort=cohort,
            validate_npi=validate_npi,
            max_scanned_rows=1000,
            max_selected_rows=1000,
        )
    path = build_observations(data, args.output)
    with duckdb.connect() as db:
        db.register("requests", pa.Table.from_pylist(requests))
        rows = db.execute(
            """
            SELECT r.request_id, r.identifier_kind, r.identifier,
                   p.npi IS NOT NULL AS matched, p.entity_type_code,
                   p.last_name, p.first_name, p.source_member, p.record_number,
                   p.source_sha256, p.scope
            FROM requests r LEFT JOIN read_parquet(?) p
              ON r.identifier_kind = 'NPI' AND r.identifier = p.npi
            ORDER BY r.request_id
        """,
            [str(path)],
        ).fetchall()
        names = [c[0] for c in db.description]
    print(
        json.dumps(
            {
                "product": "weekly-changes-excerpt",
                "current_directory": False,
                "scanned_rows": data.scanned_rows,
                "rows": [dict(zip(names, r, strict=True)) for r in rows],
                "meaning": "NPI registration alone does not establish licensure, participation or affiliation.",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
