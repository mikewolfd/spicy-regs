"""Resolve publisher identities against one verified set of congressional tables."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from spicy_regs.scorecards.etl import read_family, read_source_inputs, write_family

SOURCE_TABLES = ("scorecards", "scorecard_members", "scorecard_items")
OFFICIAL_TABLES = ("members", "member_terms", "congress_bills", "amendments", "roll_call_votes")
INPUTS = tuple(f"{name}.parquet" for name in (*SOURCE_TABLES, *OFFICIAL_TABLES))
OUTPUTS = ("scorecard_member_links.parquet", "scorecard_item_links.parquet")


def _read(paths: Sequence[Path], columns: Sequence[str] | None = None) -> list[dict]:
    if not paths or any(not path.is_file() for path in paths):
        raise FileNotFoundError("Every analysis input needs its verified Parquet members")
    # ParquetFile avoids inventing hive partition fields from a source path.
    rows = []
    for path in paths:
        reader = pq.ParquetFile(path)
        missing = sorted(set(columns or ()) - set(reader.schema_arrow.names))
        if missing:
            raise ValueError(f"Analysis input {path.name} lacks required columns: {', '.join(missing)}")
        for batch in reader.iter_batches(columns=list(columns) if columns else None):
            rows.extend(batch.to_pylist())
    return rows


def _checked_rows(rows: list[dict], columns: Sequence[str], key: Sequence[str]) -> None:
    seen = set()
    for row in rows:
        if set(row) != set(columns) or any(value is not None and not isinstance(value, str) for value in row.values()):
            raise ValueError("Analysis rows must match the declared string/null schema")
        identity = tuple(row[column] for column in key)
        if any(not value for value in identity) or identity in seen:
            raise ValueError("Analysis rows have an empty or duplicate identity")
        seen.add(identity)


def build_scorecard_analysis(
    output_dir: Path,
    *,
    input_pins: Mapping[str, dict],
    input_paths: Mapping[str, Sequence[Path]],
    member_overrides: Sequence[dict] = (),
    source_receipt_path: Path | None = None,
    source_generation_id: str | None = None,
    official_receipts: Mapping[str, tuple[Path, str]] | None = None,
    receipt_generation_id: str | None = None,
) -> tuple[Path, Path]:
    """Resolve already verified inputs; the rollup owns download and generation pins.

    Logical table names key both mappings. A published table may have multiple
    Parquet members, all drawn from the same captured publication index. Missing
    files refuse the build; they never become an empty congressional table.
    """
    from spicy_regs.scorecards.resolution import (
        ITEM_LINK_COLUMNS,
        MEMBER_LINK_COLUMNS,
        OFFICIAL_COLUMNS,
        RULE_VERSION,
        resolve_scorecard_links,
    )

    expected = set((*SOURCE_TABLES, *OFFICIAL_TABLES))
    if set(input_pins) != expected or set(input_paths) != expected:
        raise ValueError("Scorecard analysis requires every source and official input pin")
    if source_receipt_path is None or not source_generation_id:
        raise ValueError("Scorecard inputs require their selected native receipts and generation")
    source = read_source_inputs(
        {name: input_paths[name] for name in SOURCE_TABLES}, source_receipt_path, generation_id=source_generation_id
    )
    if not source["scorecards"]:
        raise ValueError("An empty scorecard corpus cannot qualify an analysis generation")
    if official_receipts is None or set(official_receipts) != set(OFFICIAL_TABLES):
        raise ValueError("Every official analysis input requires its selected native receipts")
    from spicy_regs.congress_receipts import CongressInput

    official = {}
    with TemporaryDirectory(prefix="scorecard-official-inputs-") as temporary:
        for name in OFFICIAL_TABLES:
            receipts, generation_id = official_receipts[name]
            selected = CongressInput(tuple(input_paths[name]), receipts, generation_id)
            path = selected.materialize(name, Path(temporary) / (name + ".parquet"))
            official[name] = _read([path], OFFICIAL_COLUMNS[name])
    results = resolve_scorecard_links(source, official, input_pins, member_overrides=member_overrides)
    definitions = (
        ("scorecard_member_links", MEMBER_LINK_COLUMNS, ("scorecard_id", "publisher_member_key")),
        ("scorecard_item_links", ITEM_LINK_COLUMNS, ("scorecard_id", "item_id", "reference_id")),
    )
    if set(results) != {name for name, _, _ in definitions}:
        raise ValueError("Resolver output differs from the complete analysis family")
    for name, columns, key in definitions:
        _checked_rows(results[name], columns, key)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = write_family(output_dir, results, generation_id=receipt_generation_id)
    read_family(output_dir, tuple(results))  # Persisted one-to-one linkage and refusal readback.
    receipt = {
        "rule_version": RULE_VERSION,
        "input_pins": dict(input_pins),
        "counts": {name: len(rows) for name, rows in results.items()},
        "resolution_statuses": {
            name: dict(sorted(Counter(row["resolution_status"] for row in rows).items()))
            for name, rows in results.items()
        },
        "scope": "Exact identity links for the stated inputs; published publisher ratings remain unchanged.",
    }
    (output_dir / "scorecard-analysis-qualification.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return paths[0], paths[1]
