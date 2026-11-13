"""Interpret the Senate's administration spans without inventing term splits.

Total means regular plus pocket vetoes. Overridden is a subset of regular
vetoes, not an additional category to add. The publisher's footnotes define
disputed exclusions; modern bill rows never supply missing historical counts.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

ADMINISTRATIONS = "historical_veto_administrations"
SNAPSHOTS = "historical_veto_snapshots"
_PROVENANCE = [
    ("snapshot_id", pa.string()),
    ("source_url", pa.string()),
    ("observed_at", pa.string()),
]
_COUNTS = [(name + "_vetoes", pa.int64()) for name in ("regular", "pocket", "total", "overridden")]
ADMINISTRATION_SCHEMA = pa.schema(
    [
        *_PROVENANCE,
        ("president", pa.string()),
        ("administration_label_raw", pa.string()),
        ("years_raw", pa.string()),
        ("coinciding_congresses_raw", pa.string()),
        *_COUNTS,
        ("footnotes_json", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_row_json", pa.string()),
    ]
)
SNAPSHOT_SCHEMA = pa.schema(
    [
        *_PROVENANCE,
        ("title", pa.string()),
        ("scope", pa.string()),
        ("administration_rows", pa.int64()),
        *_COUNTS,
        ("headers_json", pa.string()),
        ("footnotes_json", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_total_json", pa.string()),
    ]
)
SCHEMAS = {ADMINISTRATIONS: ADMINISTRATION_SCHEMA, SNAPSHOTS: SNAPSHOT_SCHEMA}
SCOPE = (
    "Senate Summary of Bills Vetoed, 1789 to Present, as captured; rows are publisher administration spans, "
    "which may cover several terms. Current administration counts are provisional at observation time. "
    "Total = regular + pocket; overridden is a subset of regular. Disputed exclusions follow retained footnotes. "
    "No per-bill population, per-term subdivision, line-item cancellations or detail-document coverage is inferred."
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _counts(row):
    values = [cell["text"].strip() for cell in row["cells"][2:]]
    if any(not re.fullmatch(r"[0-9]+", value) for value in values):
        raise ValueError("Veto counts must be explicit nonnegative integer cells; blank is not zero")
    regular, pocket, total, overridden = map(int, values)
    if regular + pocket != total or overridden > regular:
        raise ValueError("Veto categories do not reconcile under Senate definitions")
    return dict(zip((name for name, _ in _COUNTS), (regular, pocket, total, overridden), strict=True))


def interpret_table(table):
    """Validate every native row before any destination table is replaced."""
    provenance = {
        "snapshot_id": table["source_sha256"],
        "source_url": table["source_url"],
        "observed_at": table["observed_at"],
    }
    rows, identities = [], set()
    for raw in table["rows"]:
        cells = raw["cells"]
        label = " ".join(cells[0]["text"].split())
        match = re.fullmatch(r"(.+?)\s*\((\d{4}(?:-\d{2,4})?)\)(.*)", label)
        if not match or match[3].replace(" ", "") != "".join(raw["footnote_refs"]):
            raise ValueError("Unsupported administration label or unassociated footnote marker")
        identity = (match[1].strip(), match[2])
        if identity in identities:
            raise ValueError("Duplicate administration span in one snapshot")
        identities.add(identity)
        if not cells[1]["text"].strip():
            raise ValueError("Administration has no publisher-stated coinciding Congresses")
        rows.append(
            {
                **provenance,
                "president": identity[0],
                "administration_label_raw": cells[0]["text"],
                "years_raw": identity[1],
                "coinciding_congresses_raw": cells[1]["text"],
                **_counts(raw),
                "footnotes_json": _json({ref: table["footnotes"][ref] for ref in raw["footnote_refs"]}),
                "source_locator_json": _json({key: raw[key] for key in ("locator", "byte_start", "byte_end")}),
                "raw_row_json": _json(raw),
            }
        )
    total = table["total"]
    if " ".join(total["cells"][0]["text"].split()) != "Total" + "".join(total["footnote_refs"]):
        raise ValueError("Summary footer is not the publisher's Total row")
    if total["cells"][1]["text"].strip():
        raise ValueError("Unexpected Congress scope on summary total")
    counts = _counts(total)
    if not rows or any(sum(row[name] for row in rows) != value for name, value in counts.items()):
        raise ValueError("Administration population differs from the publisher's category totals")
    snapshot = {
        **provenance,
        "title": table["title"],
        "scope": SCOPE,
        "administration_rows": len(rows),
        **counts,
        "headers_json": _json(table["headers"]),
        "footnotes_json": _json(table["footnotes"]),
        "source_locator_json": _json(table["table"]),
        "raw_total_json": _json(total),
    }
    return rows, [snapshot]


def build_historical_vetoes(table, output_dir: Path):
    populations = interpret_table(table)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return tuple(
        write_rows(rows, output_dir / (name + ".parquet"), schema)
        for (name, schema), rows in zip(SCHEMAS.items(), populations, strict=True)
    )
