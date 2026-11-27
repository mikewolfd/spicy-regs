"""Normalize and reconcile CBO projection accuracy tables from US-CBO/eval-projections.

Builds five schema-validated Parquet tables from a commit-pinned capture:
- ``cbo_projection_errors``: realized projections vs. actuals, legislative
  adjustments, and forecast errors across horizons 1-11.
- ``cbo_projection_error_summary_stats``: CBO horizon summary statistics
  (mean error, MAE, RMSE, central two-thirds spread).
- ``cbo_eval_actuals``: historical actual budget components, nominal GDP, and
  percent-of-GDP shares (preserving Fannie Mae/Freddie Mac and student loan
  forgiveness rows excluded from error tables).
- ``cbo_eval_baselines``: full input baseline projections across all 117
  vintages (including future fiscal years through 2036).
- ``cbo_eval_baseline_changes``: legislative, economic, and technical baseline
  revisions.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

PROJECTION_ERRORS = "cbo_projection_errors"
SUMMARY_STATS = "cbo_projection_error_summary_stats"
ACTUALS = "cbo_eval_actuals"
BASELINES = "cbo_eval_baselines"
BASELINE_CHANGES = "cbo_eval_baseline_changes"

REPOSITORY = "US-CBO/eval-projections"
RAW_BASE = f"https://raw.githubusercontent.com/{REPOSITORY}"

# Subcategories excluded from CBO's projection_errors analysis:
# Fannie Mae/Freddie Mac (explicit filter in eval-projections merge/scale) and
# Student Loan Foregiveness (CBO spelling; absent from baselines.csv).
EXCLUDED_ACTUAL_SUBCATEGORIES = frozenset({"Fannie Freddie", "Student Loan Foregiveness"})

_PROVENANCE = [
    ("source_commit", pa.string()),
    ("source_url", pa.string()),
    ("source_sha256", pa.string()),
    ("observed_at", pa.string()),
]

PROJECTION_ERRORS_SCHEMA = pa.schema(
    [
        ("component", pa.string()),
        ("category", pa.string()),
        ("subcategory", pa.string()),
        ("baseline_date", pa.date32()),
        ("spring_flag", pa.bool_()),
        ("winter_flag", pa.bool_()),
        ("projected_fiscal_year", pa.int64()),
        ("projected_year_number", pa.int64()),
        ("projected_value_billions", pa.float64()),
        ("actual_value_billions", pa.float64()),
        ("gdp_billions", pa.float64()),
        ("legislative_change_billions", pa.float64()),
        ("legislative_deficit_change_billions", pa.float64()),
        ("adjusted_projection_billions", pa.float64()),
        ("projection_error_billions", pa.float64()),
        ("projection_error_pct_actual", pa.float64()),
        ("leg_change_pct_gdp", pa.float64()),
        ("projection_error_pct_gdp", pa.float64()),
        *_PROVENANCE,
    ]
)

SUMMARY_STATS_SCHEMA = pa.schema(
    [
        ("component", pa.string()),
        ("category", pa.string()),
        ("subcategory", pa.string()),
        ("projected_year_number", pa.int64()),
        ("projection_year_range", pa.string()),
        ("number_of_projections", pa.int64()),
        ("error_basis", pa.string()),
        ("average_error_pct", pa.float64()),
        ("average_absolute_error_pct", pa.float64()),
        ("rmse_pct", pa.float64()),
        ("two_thirds_spread_pct", pa.float64()),
        *_PROVENANCE,
    ]
)

ACTUALS_SCHEMA = pa.schema(
    [
        ("component", pa.string()),
        ("category", pa.string()),
        ("subcategory", pa.string()),
        ("fiscal_year", pa.int64()),
        ("actual_value_billions", pa.float64()),
        ("gdp_billions", pa.float64()),
        ("actuals_pct_gdp", pa.float64()),
        ("excluded_from_error_analysis", pa.bool_()),
        ("source_commit", pa.string()),
        ("source_url", pa.string()),
        ("actual_gdp_sha256", pa.string()),
        ("source_sha256", pa.string()),
        ("observed_at", pa.string()),
    ]
)

BASELINES_SCHEMA = pa.schema(
    [
        ("component", pa.string()),
        ("category", pa.string()),
        ("subcategory", pa.string()),
        ("baseline_date", pa.date32()),
        ("spring_flag", pa.bool_()),
        ("winter_flag", pa.bool_()),
        ("projected_fiscal_year", pa.int64()),
        ("projected_year_number", pa.int64()),
        ("projected_value_billions", pa.float64()),
        *_PROVENANCE,
    ]
)

BASELINE_CHANGES_SCHEMA = pa.schema(
    [
        ("component", pa.string()),
        ("category", pa.string()),
        ("subcategory", pa.string()),
        ("changes_baseline_date", pa.date32()),
        ("change_category", pa.string()),
        ("projected_fiscal_year", pa.int64()),
        ("change_value_billions", pa.float64()),
        *_PROVENANCE,
    ]
)

SCHEMAS = {
    PROJECTION_ERRORS: PROJECTION_ERRORS_SCHEMA,
    SUMMARY_STATS: SUMMARY_STATS_SCHEMA,
    ACTUALS: ACTUALS_SCHEMA,
    BASELINES: BASELINES_SCHEMA,
    BASELINE_CHANGES: BASELINE_CHANGES_SCHEMA,
}


def _attr(obj: Any, name: str) -> Any:
    if isinstance(obj, Mapping):
        return obj[name]
    return getattr(obj, name)


def _source_url(commit: str, rel_path: str) -> str:
    return f"{RAW_BASE}/{commit}/{rel_path}"


def _quantile(sorted_vals: list[float], q: float) -> float:
    """Linear-interpolation quantile matching pandas Series.quantile(q)."""
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _check_close(actual: float, expected: float, tol: float, label: str) -> None:
    if not (math.isfinite(actual) and math.isfinite(expected)) or abs(actual - expected) > tol:
        raise ValueError(f"{label} failed reconciliation: got {actual}, expected {expected} (tol={tol})")


def interpret_bundle(bundle: Any, *, require_full_reconciliation: bool = True) -> dict[str, list[dict[str, Any]]]:
    """Validate, reconcile, and normalize all 5 tables from a parsed EvalProjectionsBundle."""
    commit: str = _attr(bundle, "commit")
    observed_at: str = _attr(bundle, "observed_at")
    file_sha256: Mapping[str, str] = _attr(bundle, "file_sha256")

    gdp_by_fy: dict[int, float] = {}
    for row in _attr(bundle, "actual_gdp"):
        fy = int(_attr(row, "fiscal_year"))
        if fy in gdp_by_fy:
            raise ValueError(f"Duplicate fiscal_year in actual_GDP.csv: {fy}")
        gdp_by_fy[fy] = float(_attr(row, "gdp"))

    # 1. cbo_eval_actuals
    actuals_sha = file_sha256["input_data/actuals.csv"]
    gdp_sha = file_sha256["input_data/actual_GDP.csv"]
    actuals_url = _source_url(commit, "input_data/actuals.csv")
    actuals_rows: list[dict[str, Any]] = []
    actuals_keys: set[tuple[str, str, str, int]] = set()
    actuals_pct_map: dict[tuple[str, str, str, int], float] = {}
    for row in _attr(bundle, "actuals"):
        comp = _attr(row, "component")
        cat = _attr(row, "category")
        subcat = _attr(row, "subcategory")
        fy = int(_attr(row, "fiscal_year"))
        key = (comp, cat, subcat, fy)
        if key in actuals_keys:
            raise ValueError(f"Duplicate primary key in actuals: {key}")
        actuals_keys.add(key)
        if fy not in gdp_by_fy:
            raise ValueError(f"actuals row {key} references missing fiscal_year {fy} in actual_GDP.csv")
        val = float(_attr(row, "actual_value"))
        gdp = gdp_by_fy[fy]
        pct = val / gdp * 100.0
        actuals_pct_map[key] = pct
        actuals_rows.append(
            {
                "component": comp,
                "category": cat,
                "subcategory": subcat,
                "fiscal_year": fy,
                "actual_value_billions": val,
                "gdp_billions": gdp,
                "actuals_pct_gdp": pct,
                "excluded_from_error_analysis": subcat in EXCLUDED_ACTUAL_SUBCATEGORIES,
                "source_commit": commit,
                "source_url": actuals_url,
                "actual_gdp_sha256": gdp_sha,
                "source_sha256": actuals_sha,
                "observed_at": observed_at,
            }
        )

    for row in _attr(bundle, "actuals_pct_gdp"):
        key = (
            _attr(row, "component"),
            _attr(row, "category"),
            _attr(row, "subcategory"),
            int(_attr(row, "fiscal_year")),
        )
        if key not in actuals_pct_map:
            raise ValueError(f"actuals_pct_GDP row {key} missing from input_data/actuals.csv")
        if key[2] == "Fannie Freddie":
            raise ValueError(f"Unexpected Fannie Freddie row in actuals_pct_GDP output: {key}")
        _check_close(float(_attr(row, "actuals_pct_gdp")), actuals_pct_map[key], 0.06, f"actuals_pct_GDP {key}")

    # 2. cbo_eval_baselines
    baselines_sha = file_sha256["input_data/baselines.csv"]
    baselines_url = _source_url(commit, "input_data/baselines.csv")
    baseline_rows: list[dict[str, Any]] = []
    baseline_keys: set[tuple[str, str, str, date, int]] = set()
    for row in _attr(bundle, "baselines"):
        bdate = date.fromisoformat(_attr(row, "baseline_date"))
        key = (
            _attr(row, "component"),
            _attr(row, "category"),
            _attr(row, "subcategory"),
            bdate,
            int(_attr(row, "projected_fiscal_year")),
        )
        if key in baseline_keys:
            raise ValueError(f"Duplicate primary key in baselines: {key}")
        baseline_keys.add(key)
        baseline_rows.append(
            {
                "component": key[0],
                "category": key[1],
                "subcategory": key[2],
                "baseline_date": bdate,
                "spring_flag": bool(_attr(row, "spring_flag")),
                "winter_flag": bool(_attr(row, "winter_flag")),
                "projected_fiscal_year": key[4],
                "projected_year_number": int(_attr(row, "projected_year_number")),
                "projected_value_billions": float(_attr(row, "value")),
                "source_commit": commit,
                "source_url": baselines_url,
                "source_sha256": baselines_sha,
                "observed_at": observed_at,
            }
        )

    # 3. cbo_eval_baseline_changes
    changes_sha = file_sha256["input_data/baseline_changes.csv"]
    changes_url = _source_url(commit, "input_data/baseline_changes.csv")
    change_rows: list[dict[str, Any]] = []
    change_keys: set[tuple[str, str, str, date, str, int]] = set()
    for row in _attr(bundle, "baseline_changes"):
        cdate = date.fromisoformat(_attr(row, "changes_baseline_date"))
        key = (
            _attr(row, "component"),
            _attr(row, "category"),
            _attr(row, "subcategory"),
            cdate,
            _attr(row, "change_category"),
            int(_attr(row, "projected_fiscal_year")),
        )
        if key in change_keys:
            raise ValueError(f"Duplicate primary key in baseline_changes: {key}")
        change_keys.add(key)
        raw_val = _attr(row, "value")
        change_rows.append(
            {
                "component": key[0],
                "category": key[1],
                "subcategory": key[2],
                "changes_baseline_date": cdate,
                "change_category": key[4],
                "projected_fiscal_year": key[5],
                "change_value_billions": None if raw_val is None else float(raw_val),
                "source_commit": commit,
                "source_url": changes_url,
                "source_sha256": changes_sha,
                "observed_at": observed_at,
            }
        )

    # 4. cbo_projection_errors + error math reconciliation
    error_rows: list[dict[str, Any]] = []
    error_keys: set[tuple[str, str, str, date, int]] = set()
    stat_groups: dict[tuple[str, str, str, int], list[tuple[int, float]]] = defaultdict(list)
    for row in _attr(bundle, "projection_errors"):
        comp = _attr(row, "component")
        cat = _attr(row, "category")
        subcat = _attr(row, "subcategory")
        bdate = date.fromisoformat(_attr(row, "baseline_date"))
        pfy = int(_attr(row, "projected_fiscal_year"))
        horizon = int(_attr(row, "projected_year_number"))
        key = (comp, cat, subcat, bdate, pfy)
        if key in error_keys:
            raise ValueError(f"Duplicate primary key in projection_errors: {key}")
        error_keys.add(key)

        val = float(_attr(row, "value"))
        act = float(_attr(row, "actual_value"))
        gdp = float(_attr(row, "gdp"))
        leg = float(_attr(row, "legislative_change"))
        leg_def_raw = _attr(row, "legislative_deficit_change")
        leg_def = None if leg_def_raw is None else float(leg_def_raw)
        adj = float(_attr(row, "adjusted_projection"))
        err = float(_attr(row, "projection_error"))
        pct_act_raw = _attr(row, "projection_error_pct_actual")
        pct_act = None if pct_act_raw is None else float(pct_act_raw)
        leg_pct_gdp = float(_attr(row, "leg_change_pct_gdp"))
        err_pct_gdp = float(_attr(row, "projection_error_pct_gdp"))

        _check_close(adj, val + leg, 0.002, f"adjusted_projection {key}")
        expected_err = (adj - act) * (-1.0 if comp == "deficit" else 1.0)
        _check_close(err, expected_err, 0.002, f"projection_error {key}")
        _check_close(leg_pct_gdp, leg / gdp * 100.0, 0.002, f"leg_change_pct_GDP {key}")
        _check_close(err_pct_gdp, err / gdp * 100.0, 0.002, f"projection_error_pct_GDP {key}")
        if comp in ("outlay", "revenue"):
            if pct_act is None:
                raise ValueError(f"Missing projection_error_pct_actual for {key}")
            _check_close(pct_act, err / act * 100.0, 0.002, f"projection_error_pct_actual {key}")
        elif pct_act is not None:
            raise ValueError(f"Unexpected projection_error_pct_actual for {key}")

        rel_err_path = f"output_data/{comp}_projection_errors.csv"
        spring_flag = bool(_attr(row, "spring_flag"))
        winter_flag = bool(_attr(row, "winter_flag"))
        error_rows.append(
            {
                "component": comp,
                "category": cat,
                "subcategory": subcat,
                "baseline_date": bdate,
                "spring_flag": spring_flag,
                "winter_flag": winter_flag,
                "projected_fiscal_year": pfy,
                "projected_year_number": horizon,
                "projected_value_billions": val,
                "actual_value_billions": act,
                "gdp_billions": gdp,
                "legislative_change_billions": leg,
                "legislative_deficit_change_billions": leg_def,
                "adjusted_projection_billions": adj,
                "projection_error_billions": err,
                "projection_error_pct_actual": pct_act,
                "leg_change_pct_gdp": leg_pct_gdp,
                "projection_error_pct_gdp": err_pct_gdp,
                "source_commit": commit,
                "source_url": _source_url(commit, rel_err_path),
                "source_sha256": file_sha256[rel_err_path],
                "observed_at": observed_at,
            }
        )
        # Revenue summary stats are computed over Winter baselines only; all others over Spring baselines.
        if comp != "revenue" or winter_flag:
            metric = err_pct_gdp if comp in ("deficit", "debt") else pct_act
            assert metric is not None
            stat_groups[(comp, cat, subcat, horizon)].append((pfy, metric))

    # 5. cbo_projection_error_summary_stats + summary reconciliation
    stat_rows: list[dict[str, Any]] = []
    stat_keys: set[tuple[str, str, str, int]] = set()
    for row in _attr(bundle, "summary_stats"):
        comp = _attr(row, "component")
        cat = _attr(row, "category")
        subcat = _attr(row, "subcategory")
        horizon = int(_attr(row, "projected_year_number"))
        key = (comp, cat, subcat, horizon)
        if key in stat_keys:
            raise ValueError(f"Duplicate primary key in summary_stats: {key}")
        stat_keys.add(key)
        year_range = _attr(row, "projection_year_range")
        n_proj = int(_attr(row, "number_of_projections"))
        avg_err = float(_attr(row, "average_error"))
        avg_abs = float(_attr(row, "average_absolute_error"))
        rmse = float(_attr(row, "rmse"))
        spread = float(_attr(row, "two_thirds_spread"))

        group = stat_groups.get(key, [])
        if require_full_reconciliation or len(group) == n_proj:
            if len(group) != n_proj:
                raise ValueError(f"summary_stats {key} count {n_proj} != observed projection rows {len(group)}")
            years = [yr for yr, _ in group]
            if f"{min(years)}-{max(years)}" != year_range:
                raise ValueError(f"summary_stats {key} year_range {year_range!r} != {min(years)}-{max(years)}")
            vals = sorted(v for _, v in group)
            _check_close(avg_err, sum(vals) / n_proj, 0.06, f"average_error {key}")
            _check_close(avg_abs, sum(abs(v) for v in vals) / n_proj, 0.06, f"average_absolute_error {key}")
            _check_close(rmse, math.sqrt(sum(v * v for v in vals) / n_proj), 0.06, f"RMSE {key}")
            _check_close(
                spread, _quantile(vals, 5.0 / 6.0) - _quantile(vals, 1.0 / 6.0), 0.06, f"two_thirds_spread {key}"
            )

        rel_stat_path = f"output_data/{comp}_projection_errors_summary_stats.csv"
        stat_rows.append(
            {
                "component": comp,
                "category": cat,
                "subcategory": subcat,
                "projected_year_number": horizon,
                "projection_year_range": year_range,
                "number_of_projections": n_proj,
                "error_basis": "pct_gdp" if comp in ("deficit", "debt") else "pct_actual",
                "average_error_pct": avg_err,
                "average_absolute_error_pct": avg_abs,
                "rmse_pct": rmse,
                "two_thirds_spread_pct": spread,
                "source_commit": commit,
                "source_url": _source_url(commit, rel_stat_path),
                "source_sha256": file_sha256[rel_stat_path],
                "observed_at": observed_at,
            }
        )

    return {
        PROJECTION_ERRORS: error_rows,
        SUMMARY_STATS: stat_rows,
        ACTUALS: actuals_rows,
        BASELINES: baseline_rows,
        BASELINE_CHANGES: change_rows,
    }


def build_cbo_eval_projections(
    bundle: Any, output_dir: Path, *, require_full_reconciliation: bool = True
) -> tuple[Path, ...]:
    """Validate, reconcile, and write all 5 Parquet tables for one commit-pinned capture."""
    populations = interpret_bundle(bundle, require_full_reconciliation=require_full_reconciliation)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return tuple(
        write_rows(populations[name], output_dir / f"{name}.parquet", schema) for name, schema in SCHEMAS.items()
    )
