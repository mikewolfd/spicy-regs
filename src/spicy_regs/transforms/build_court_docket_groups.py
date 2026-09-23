"""Build ``court_docket_groups.parquet``: same-case groups over the published ``court_dockets`` selection.

CourtListener leaves its own ``parent_docket_id`` blank in every row of the bulk
editions, so a parent is inferred from PACER's mechanics: one main case created
first (the lowest PACER case id) plus per-defendant sub-dockets that share its
visible docket number — the publisher's documented "doppeldocket" problem (FLP
wiki, CourtListener issue #2185).

Rule version 1, over one native bulk edition:

* Candidate groups are the native records sharing a published row's
  ``(court_id, docket_number)``; only records in the published selection are
  members, so every parent resolves in ``court_dockets``.
* A group needs at least two members, a single caption (case-insensitive) and
  at least one numeric PACER case id. Several captions mean docket-number
  reuse, and no PACER id leaves nothing to order; both stay ungrouped.
* The parent is the member with the numerically lowest PACER case id (ties by
  ``cl_docket_id``). The ids are strings in the source; comparing them as
  strings would rank ``"100000"`` below ``"99999"``.
* The tier is ``doppeldocket`` when the PACER ids span at most five, otherwise
  ``refiled``.

Membership reads one native edition as a filtered single-table scan with
Python-side set logic, the same pattern the enrichment used after a DuckDB
parallel-join anomaly.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from loguru import logger

RULE_VERSION = "1"
DOPPELDOCKET_MAX_SPREAD = 5

SCHEMA = pa.schema([
    ("cl_docket_id", pa.string()),
    ("parent_cl_docket_id", pa.string()),
    ("confidence_tier", pa.string()),
    ("group_size", pa.int64()),
    ("edition", pa.string()),
    ("rule_version", pa.string()),
])

_NATIVE_COLUMNS = ["id", "court_id", "docket_number", "pacer_case_id", "case_name"]


def group_members(members: list[dict]) -> tuple[str, str] | None:
    """Return (parent id, tier) for one candidate group of published members, or None to leave it ungrouped."""
    if len(members) < 2:
        return None
    if len({(member["case_name"] or "").lower() for member in members}) > 1:
        return None
    pacers = sorted((int(member["pacer_case_id"]), member["id"]) for member in members
                    if (member["pacer_case_id"] or "").isdigit())
    if not pacers:
        return None
    spread = pacers[-1][0] - pacers[0][0]
    return pacers[0][1], "doppeldocket" if spread <= DOPPELDOCKET_MAX_SPREAD else "refiled"


def build_court_docket_groups(output_dir: Path, *, dockets_file: Path, native_file: Path, edition: str) -> Path:
    """Write the side table for the published ``dockets_file`` from one native bulk edition."""
    published = pq.read_table(dockets_file, columns=["cl_docket_id", "court_id", "docket_number"]).to_pylist()
    published_ids = {row["cl_docket_id"] for row in published}
    keys = {(row["court_id"], row["docket_number"]) for row in published if row["docket_number"]}
    courts = sorted({court for court, _ in keys})

    candidates: dict[tuple[str, str], list[dict]] = defaultdict(list)
    scanned = 0
    dataset = ds.dataset(native_file, format="parquet")
    for batch in dataset.to_batches(columns=_NATIVE_COLUMNS, filter=ds.field("court_id").isin(courts)):
        scanned += batch.num_rows
        for row in batch.to_pylist():
            key = (row["court_id"], row["docket_number"])
            if key in keys and row["id"] in published_ids:
                candidates[key].append(row)

    rows, stats = [], {"groups": 0, "doppeldocket": 0, "refiled": 0, "single": 0, "ungrouped": 0}
    for _, members in sorted(candidates.items()):
        decision = group_members(members)
        if decision is None:
            stats["single" if len(members) < 2 else "ungrouped"] += 1
            continue
        parent, tier = decision
        stats["groups"] += 1
        stats[tier] += 1
        rows.extend({"cl_docket_id": member["id"], "parent_cl_docket_id": parent, "confidence_tier": tier,
                     "group_size": len(members), "edition": edition, "rule_version": RULE_VERSION}
                    for member in members)
    rows.sort(key=lambda row: row["cl_docket_id"])
    if {row["parent_cl_docket_id"] for row in rows} - published_ids:
        raise RuntimeError("a group parent is missing from the published court_dockets selection")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "court_docket_groups.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), out_file, compression="zstd")
    receipt = {"edition": edition, "rule_version": RULE_VERSION, "dockets_file": str(dockets_file),
               "native_file": str(native_file), "published_rows": len(published), "native_rows_scanned": scanned,
               "grouped_rows": len(rows), **stats}
    (output_dir / "court_docket_groups.parquet.receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    logger.info("Court docket groups: {:,} groups over {:,} rows ({:,} doppeldocket, {:,} refiled)",
                stats["groups"], len(rows), stats["doppeldocket"], stats["refiled"])
    return out_file
