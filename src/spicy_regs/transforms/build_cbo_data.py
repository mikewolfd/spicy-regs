"""Build schema-validated Parquet tables for all 13 US-CBO/cbo-data baseline datasets.

Converts commit-pinned ``US-CBO/cbo-data`` CSV vintages and ``schema.json``
metadata (acquired via ``spicy_docs.sources.cbo_data``) into 13 dataset Parquet
tables plus ``cbo_data_snapshots`` provenance rows. Preserves ``vintage``,
``publication_id``, series/unit metadata, and every source row in order
(including datasets where CBO publishes duplicate ``(date, variable)`` keys).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

if TYPE_CHECKING:
    from spicy_docs.sources.cbo_data import CboCsvRow, CboDataCatalog, CboDataSnapshot, CboDatasetSchema

SNAPSHOTS_TABLE = "cbo_data_snapshots"

DATASET_TO_TABLE: dict[str, str] = {
    "historical_budget": "cbo_historical_budget",
    "ten_year_budget": "cbo_ten_year_budget",
    "long_term_budget": "cbo_long_term_budget",
    "trust_fund": "cbo_trust_fund",
    "spending_detail": "cbo_spending_detail",
    "revenue_detail": "cbo_revenue_detail",
    "tax_parameters": "cbo_tax_parameters",
    "automatic_stabilizers": "cbo_automatic_stabilizers",
    "historical_economic": "cbo_historical_economic",
    "economic_projections": "cbo_economic_projections",
    "long_term_economic": "cbo_long_term_economic",
    "potential_gdp": "cbo_potential_gdp",
    "demographic": "cbo_demographic",
}
TABLE_TO_DATASET: dict[str, str] = {table: dataset for dataset, table in DATASET_TO_TABLE.items()}
BUDGET_TABLES: tuple[str, ...] = tuple(
    DATASET_TO_TABLE[ds]
    for ds in (
        "historical_budget",
        "ten_year_budget",
        "long_term_budget",
        "trust_fund",
        "spending_detail",
        "revenue_detail",
        "tax_parameters",
        "automatic_stabilizers",
    )
)
ECONOMIC_TABLES: tuple[str, ...] = tuple(
    DATASET_TO_TABLE[ds]
    for ds in (
        "historical_economic",
        "economic_projections",
        "long_term_economic",
        "potential_gdp",
        "demographic",
    )
)


def _long_schema(*extra_fields: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema(
        [
            ("source_record_id", pa.string()),
            ("commit_sha", pa.string()),
            ("publication_id", pa.string()),
            ("vintage", pa.string()),
            ("file_type", pa.string()),
            ("date", pa.string()),
            ("variable", pa.string()),
            ("value", pa.float64()),
            ("value_raw", pa.string()),
            *extra_fields,
            ("unit", pa.string()),
            ("category", pa.string()),
            ("series_description", pa.string()),
            ("metadata_json", pa.string()),
            ("source_file", pa.string()),
            ("source_row_index", pa.int64()),
            ("source_sha256", pa.string()),
            ("schema_sha256", pa.string()),
        ]
    )


_PLAIN_LONG_SCHEMA = _long_schema()
_ECONOMIC_PROJECTIONS_SCHEMA = _long_schema(("estimate_type", pa.string()))
_REVENUE_DETAIL_SCHEMA = _long_schema(
    ("estimate_type", pa.string()),
    ("section", pa.string()),
    ("variable_note", pa.string()),
)
_LONG_TERM_ECONOMIC_SCHEMA = _long_schema(
    ("sex", pa.string()),
    ("age_group", pa.string()),
)

_SPENDING_DETAIL_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("commit_sha", pa.string()),
        ("publication_id", pa.string()),
        ("vintage", pa.string()),
        ("file_type", pa.string()),
        ("date", pa.string()),
        ("tin", pa.string()),
        ("title", pa.string()),
        ("disc_or_mand", pa.string()),
        ("category", pa.string()),
        ("agency", pa.string()),
        ("bureau", pa.string()),
        ("function_code", pa.string()),
        ("subfunction_code", pa.string()),
        ("off_budget", pa.string()),
        ("budget_authority", pa.float64()),
        ("outlays", pa.float64()),
        ("budget_authority_raw", pa.string()),
        ("outlays_raw", pa.string()),
        ("unit", pa.string()),
        ("source_file", pa.string()),
        ("source_row_index", pa.int64()),
        ("source_sha256", pa.string()),
        ("schema_sha256", pa.string()),
    ]
)

_DEMOGRAPHIC_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("commit_sha", pa.string()),
        ("publication_id", pa.string()),
        ("vintage", pa.string()),
        ("domain", pa.string()),
        ("year", pa.int32()),
        ("age", pa.string()),
        ("sex", pa.string()),
        ("place_of_birth", pa.string()),
        ("immigration_status", pa.string()),
        ("migration_flow", pa.string()),
        ("marital_status", pa.string()),
        ("measure", pa.string()),
        ("value", pa.float64()),
        ("value_raw", pa.string()),
        ("unit", pa.string()),
        ("domain_description", pa.string()),
        ("source_file", pa.string()),
        ("source_row_index", pa.int64()),
        ("source_sha256", pa.string()),
        ("schema_sha256", pa.string()),
    ]
)

_SNAPSHOTS_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("vintage", pa.string()),
        ("file_type", pa.string()),
        ("commit_sha", pa.string()),
        ("publication_id", pa.string()),
        ("title", pa.string()),
        ("source_url", pa.string()),
        ("source_file", pa.string()),
        ("row_count", pa.int64()),
        ("source_sha256", pa.string()),
        ("schema_sha256", pa.string()),
        ("catalog_sha256", pa.string()),
        ("observed_at", pa.string()),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {
    "cbo_historical_budget": _PLAIN_LONG_SCHEMA,
    "cbo_ten_year_budget": _PLAIN_LONG_SCHEMA,
    "cbo_long_term_budget": _PLAIN_LONG_SCHEMA,
    "cbo_trust_fund": _PLAIN_LONG_SCHEMA,
    "cbo_spending_detail": _SPENDING_DETAIL_SCHEMA,
    "cbo_revenue_detail": _REVENUE_DETAIL_SCHEMA,
    "cbo_tax_parameters": _PLAIN_LONG_SCHEMA,
    "cbo_automatic_stabilizers": _PLAIN_LONG_SCHEMA,
    "cbo_historical_economic": _PLAIN_LONG_SCHEMA,
    "cbo_economic_projections": _ECONOMIC_PROJECTIONS_SCHEMA,
    "cbo_long_term_economic": _LONG_TERM_ECONOMIC_SCHEMA,
    "cbo_potential_gdp": _PLAIN_LONG_SCHEMA,
    "cbo_demographic": _DEMOGRAPHIC_SCHEMA,
    SNAPSHOTS_TABLE: _SNAPSHOTS_SCHEMA,
}


def _variable_metadata_json(var_meta: Any) -> str:
    extra = json.loads(var_meta.extra_json) if var_meta.extra_json else {}
    payload: dict[str, Any] = {}
    if var_meta.aggregation is not None:
        payload["aggregation"] = var_meta.aggregation
    if var_meta.source_frequency is not None:
        payload["source_frequency"] = var_meta.source_frequency
    if extra:
        payload["extra"] = extra
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def shape_row(row: CboCsvRow, *, commit_sha: str, schema: CboDatasetSchema) -> dict[str, Any]:
    """Map one validated ``CboCsvRow`` onto its target Arrow table row."""
    record_id = f"{row.dataset}:{row.vintage}:{row.file_type}:{row.source_row_index}"
    cells = row.cells
    base = {
        "source_record_id": record_id,
        "commit_sha": commit_sha,
        "publication_id": row.publication_id,
        "vintage": row.vintage,
        "source_file": row.source_file,
        "source_row_index": row.source_row_index,
        "source_sha256": row.source_sha256,
        "schema_sha256": schema.input_sha256,
    }
    if row.dataset == "spending_detail":
        return {
            **base,
            "file_type": row.file_type,
            "date": cells["date"],
            "tin": cells["tin"],
            "title": cells["title"],
            "disc_or_mand": cells["disc_or_mand"],
            "category": cells["category"],
            "agency": cells["agency"],
            "bureau": cells["bureau"],
            "function_code": cells["function_code"],
            "subfunction_code": cells["subfunction_code"],
            "off_budget": cells["off_budget"],
            "budget_authority": cells["budget_authority_num"],
            "outlays": cells["outlays_num"],
            "budget_authority_raw": cells["budget_authority"],
            "outlays_raw": cells["outlays"],
            "unit": "Millions of dollars",
        }
    if row.dataset == "demographic":
        return {
            **base,
            "domain": row.file_type,
            "year": int(cells["year"]),
            "age": cells["age"],
            "sex": cells.get("sex"),
            "place_of_birth": cells.get("place_of_birth"),
            "immigration_status": cells.get("immigration_status"),
            "migration_flow": cells.get("migration_flow"),
            "marital_status": cells.get("marital_status"),
            "measure": cells["measure"],
            "value": cells["value_num"],
            "value_raw": cells["value_raw"],
            "unit": cells["unit"],
            "domain_description": cells["domain_description"],
        }

    var_name = cells["variable"]
    var_meta = schema.variables[var_name]
    shaped: dict[str, Any] = {
        **base,
        "file_type": row.file_type,
        "date": cells["date"],
        "variable": var_name,
        "value": cells["value_num"],
        "value_raw": cells["value"],
        "unit": var_meta.unit,
        "category": var_meta.category,
        "series_description": var_meta.description,
        "metadata_json": _variable_metadata_json(var_meta),
    }
    if row.dataset == "economic_projections":
        shaped["estimate_type"] = cells["estimate_type"]
    elif row.dataset == "revenue_detail":
        shaped["estimate_type"] = cells["estimate_type"]
        shaped["section"] = cells.get("section")
        shaped["variable_note"] = var_meta.variable_note
    elif row.dataset == "long_term_economic":
        shaped["sex"] = cells.get("sex")
        shaped["age_group"] = cells.get("age_group")
    return shaped


def build_cbo_data_streaming(
    *,
    commit: str,
    catalog: CboDataCatalog,
    schemas: Mapping[str, CboDatasetSchema],
    iter_distribution_rows: Callable[[str], tuple[Iterable[CboCsvRow], str, str]],
    output_dir: Path,
    batch_size: int = 10_000,
) -> tuple[Path, ...]:
    """Stream all 13 datasets and ``cbo_data_snapshots`` to Parquet files in ``output_dir``."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    by_id = catalog.by_identifier()
    missing = [ds for ds in DATASET_TO_TABLE if ds not in by_id or ds not in schemas]
    if missing:
        raise ValueError(f"cbo-data build missing required datasets/schemas: {', '.join(missing)}")

    written: list[Path] = []
    snapshot_rows: list[dict[str, Any]] = []

    for dataset, table_name in DATASET_TO_TABLE.items():
        entry = by_id[dataset]
        schema = schemas[dataset]

        def _dataset_rows() -> Iterable[dict[str, Any]]:
            for dist in entry.distributions:
                rows_iter, source_sha256, observed_at = iter_distribution_rows(dist.download_url)
                row_count = 0
                for row in rows_iter:
                    row_count += 1
                    yield shape_row(row, commit_sha=commit, schema=schema)
                snapshot_rows.append(
                    {
                        "dataset": dataset,
                        "vintage": dist.vintage,
                        "file_type": dist.file_type,
                        "commit_sha": commit,
                        "publication_id": entry.publication_id,
                        "title": schema.title,
                        "source_url": schema.source_url,
                        "source_file": dist.download_url,
                        "row_count": row_count,
                        "source_sha256": source_sha256,
                        "schema_sha256": schema.input_sha256,
                        "catalog_sha256": catalog.input_sha256,
                        "observed_at": observed_at,
                    }
                )

        written.append(
            write_rows(_dataset_rows(), output_dir / f"{table_name}.parquet", SCHEMAS[table_name], batch_size=batch_size)
        )

    written.append(
        write_rows(snapshot_rows, output_dir / f"{SNAPSHOTS_TABLE}.parquet", _SNAPSHOTS_SCHEMA, batch_size=batch_size)
    )
    return tuple(written)


def build_cbo_data(snapshot: CboDataSnapshot, output_dir: Path, *, batch_size: int = 10_000) -> tuple[Path, ...]:
    """Convert an in-memory ``CboDataSnapshot`` into all 14 Parquet tables."""

    def _loader(download_url: str):
        rows = snapshot.distributions[download_url]
        cap = snapshot.distribution_captures[download_url]
        return rows, cap.sha256, cap.observed_at

    return build_cbo_data_streaming(
        commit=snapshot.commit,
        catalog=snapshot.catalog,
        schemas=snapshot.schemas,
        iter_distribution_rows=_loader,
        output_dir=output_dir,
        batch_size=batch_size,
    )
