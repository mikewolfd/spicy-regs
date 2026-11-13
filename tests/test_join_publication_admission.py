"""New local declarations never stand in for live join measurements."""
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import check_table_joins as live
from spicy_regs import table_joins


def declared(kind="unmeasured", keys=0):
    return table_joins.Join("child", ("id",), "parent", ("id",), keys, 0, kind)


def test_absent_new_join_reports_no_measurement_and_cannot_record_a_baseline(tmp_path, monkeypatch):
    monkeypatch.setattr(live, "_unpublished", lambda con, urls: True)
    with duckdb.connect() as con:
        result = live.measure(con, declared(), live.TableURLs("https://example.test", {}), allow_unpublished=True)
    assert result["status"] == "NOT_PUBLISHED"
    assert result["keys"] is result["missing"] is result["resolved_pct"] is None
    assert "pending" in live._line(result)
    baseline = tmp_path / "baselines.json"
    baseline.write_text('{"results": []}')
    with pytest.raises(ValueError, match="no baseline"):
        live.record_baselines([declared()], [result], tmp_path / "receipt.json", "2026-11-13", baseline)
    assert json.loads(baseline.read_text()) == {"results": []}


@pytest.mark.parametrize("kind,keys,selected,allowed", [
    ("unmeasured", 0, {}, False),  # default live check remains strict
    ("complete", 1, {}, True),  # prior measurement cannot disappear
    ("unmeasured", 0, {"child": ["missing-selected.parquet"]}, True),
])
def test_existing_or_selected_inputs_are_not_excused(tmp_path, monkeypatch, kind, keys, selected, allowed):
    monkeypatch.setattr(live, "_unpublished", lambda con, urls: True)
    with duckdb.connect() as con, pytest.raises(duckdb.IOException):
        live.measure(con, declared(kind, keys), live.TableURLs(str(tmp_path), selected), allow_unpublished=allowed)


def test_published_new_rows_still_need_a_baseline(tmp_path):
    selected = {}
    for name in ("child", "parent"):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({"id": ["one"]}), path)
        selected[name] = [str(path)]
    with duckdb.connect() as con:
        result = live.measure(con, declared(), live.TableURLs(str(tmp_path), selected), allow_unpublished=True)
    assert result["status"] == "UNBASELINED"
    assert result["keys"] == 1 and result["status"] in live.FAILING


def test_source_failure_is_not_unpublished(monkeypatch):
    def failed(*args):
        raise duckdb.HTTPException("503 unavailable")
    monkeypatch.setattr(live, "_unpublished", failed)
    with duckdb.connect() as con, pytest.raises(duckdb.HTTPException):
        live.measure(con, declared(), live.TableURLs("https://example.test", {}), allow_unpublished=True)


@pytest.mark.parametrize("previously_declared", ["same", "changed-parent", "new"])
def test_cli_opt_in_applies_only_to_new_identity(tmp_path, monkeypatch, previously_declared):
    join = declared()
    previous = {"format": table_joins.RECORD_FORMAT, "version": 1,
                "joins": [] if previously_declared == "new" else [dict(
                    table_joins.record(join), reason="earlier", parent="older" if previously_declared == "changed-parent" else "parent")]}
    monkeypatch.setattr(table_joins, "JOINS", (join,))
    monkeypatch.setattr(live, "previous_record", lambda ref: previous)
    monkeypatch.setattr(live, "connect", duckdb.connect)
    monkeypatch.setattr(live, "table_urls", lambda base: live.TableURLs(base, {}))
    selected = []

    def check(con, joins, urls, *, unpublished_new):
        selected.extend(unpublished_new)
        return []

    monkeypatch.setattr(live, "check", check)
    assert live.main(["--changed-since", "a" * 40, "--allow-unpublished-new",
                      "--index-url", "https://example.test"]) == 0
    assert selected == ([join.name] if previously_declared == "new" else [])
