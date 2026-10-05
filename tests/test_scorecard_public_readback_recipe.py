"""Public source defaults and analysis lineage/status readback refuse mismatches."""

from copy import deepcopy
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import exact_json

DEPLOYMENT = Path(__file__).resolve().parents[1] / "docs/research/scorecards/work/integration/deployment"
sys.path.insert(0, str(DEPLOYMENT))
spec = spec_from_file_location("tested_scorecard_readback_recipe", DEPLOYMENT / "readback_candidate.py")
assert spec is not None and spec.loader is not None
recipe = module_from_spec(spec)
spec.loader.exec_module(recipe)
sys.path.pop(0)


def test_source_remains_the_default_and_uses_original_counts():
    args = SimpleNamespace()
    prepared = {"counts": {"scorecard_member_ratings": 2, "scorecard_snapshots": 1}}
    assert recipe.selected_family(args) == "scorecards"
    assert recipe.qualified_counts(prepared, "scorecards") is prepared["counts"]


@pytest.mark.parametrize("fault", [None, "table-count", "footer", "receipt-generation", "snapshots"])
def test_original_source_public_file_path_still_checks_native_rows_and_snapshot_receipts(tmp_path, monkeypatch, fault):
    from hashlib import sha256

    source = tmp_path / "originals"
    source.mkdir()
    subject = source / "scorecard_member_ratings.parquet"
    receipts = source / "etl_receipts.parquet"
    pq.write_table(pa.table({"value_text": ["N/A", "0"]}), subject)
    pq.write_table(
        pa.table(
            {
                "dataset": ["scorecard_snapshots", "scorecard_snapshots"],
                "generation_id": ["wrong" if fault == "receipt-generation" else "selected"] * 2,
                "outcome": ["observed", "error" if fault == "snapshots" else "observed"],
            }
        ),
        receipts,
    )

    def descriptor(path, columns):
        return {
            "sha256": "sha256:" + sha256(path.read_bytes()).hexdigest(),
            "byteSize": path.stat().st_size,
            "rows": 2,
            "columns": columns,
        }

    digest = "sha256:" + "a" * 64
    family = {
        "artifactDigest": digest,
        "prefix": "generations/scorecards/" + "a" * 64,
        "tables": {subject.name: descriptor(subject, [["value_text", "VARCHAR"]])},
        "etlReceipts": {
            **descriptor(receipts, []),
            "key": "etl_receipts.parquet",
            "generationId": "selected",
            "datasets": ["scorecards"],
        },
    }
    if fault == "footer":
        family["tables"][subject.name]["rows"] = 3
    index = {"families": {"scorecards": family}}
    monkeypatch.setattr(recipe.publication, "load_index", lambda url: index)

    def fetch(url, member, path, **kwargs):
        raw = (source / path.name).read_bytes()
        assert member.sha256 == "sha256:" + sha256(raw).hexdigest() and member.byte_size == len(raw)
        path.write_bytes(raw)
        return True

    monkeypatch.setattr(recipe.publication, "fetch_member", fetch)
    output = tmp_path / "readback"
    output.mkdir()
    args = SimpleNamespace(output=output, public_url="https://example.invalid")
    prepared = {
        "generation": {"artifactDigest": digest},
        "counts": {"scorecard_member_ratings": 1 if fault == "table-count" else 2, "scorecard_snapshots": 2},
    }
    if fault is None:
        connection, actual = recipe.public_files(args, prepared, {"family_entry": family})
        assert actual is family
        assert recipe.local_rows(connection, "SELECT value_text FROM scorecard_member_ratings ORDER BY value_text") == [
            {"value_text": "0"},
            {"value_text": "N/A"},
        ]
        assert json.loads((output / "public-files.json").read_bytes())["family"] == "scorecards"
    else:
        with pytest.raises(ValueError):
            recipe.public_files(args, prepared, {"family_entry": family})


@pytest.mark.parametrize(
    "counts",
    [
        {},
        {"scorecard_member_links": 1},
        {"scorecard_member_links": 1.0, "scorecard_item_links": 1},
        {"scorecard_member_links": True, "scorecard_item_links": 1},
        {"scorecard_member_links": -1, "scorecard_item_links": 1},
    ],
)
def test_analysis_requires_its_exact_qualified_populations(counts):
    with pytest.raises(ValueError, match="counts"):
        recipe.qualified_counts({"qualification": {"counts": counts}}, "scorecard-analysis")


def parent_fixture():
    digest = "sha256:" + "a" * 64
    source = {
        "artifactDigest": digest,
        "tables": {"scorecards.parquet": {"sha256": "sha256:" + "b" * 64, "byteSize": 10}},
    }
    index = {"families": {"scorecards": source}}
    pin = recipe.publication.table_pin(index, "scorecards.parquet")
    prepared = {"qualification": {"input_pins": {"scorecards": pin}}}
    root = {"spec": {"parents": {"scorecards.parquet": pin}, "readSnapshot": deepcopy(index)}}
    return root, prepared, index


def test_analysis_parent_matches_preparation_captured_index_and_current_source():
    root, prepared, index = parent_fixture()
    assert recipe.analysis_parents(root, prepared, index) == root["spec"]["parents"]


@pytest.mark.parametrize("target", ["parent", "captured", "current", "extra"])
def test_changed_parent_or_mixed_source_generation_refuses(target):
    root, prepared, index = parent_fixture()
    if target == "parent":
        root["spec"]["parents"]["scorecards.parquet"] = {}
    elif target == "captured":
        root["spec"]["readSnapshot"]["families"]["scorecards"]["artifactDigest"] = "sha256:" + "c" * 64
    elif target == "current":
        index["families"]["scorecards"]["artifactDigest"] = "sha256:" + "c" * 64
    else:
        root["spec"]["parents"]["unqualified.parquet"] = {}
    with pytest.raises(ValueError, match="parent"):
        recipe.analysis_parents(root, prepared, index)


def query_fixture():
    connection = duckdb.connect()
    body = {
        "columns": ["value"],
        "rows": [["N/A"]],
        "truncated": False,
        "publication": {
            "source": {"family": "scorecards", "artifact_digest": "source"},
            "analysis": {"family": "scorecard-analysis", "artifact_digest": "links"},
        },
    }
    return connection, body, {"scorecards": "source", "scorecard-analysis": "links"}


def test_mixed_family_query_uses_each_own_pin_and_preserves_native_cells():
    connection, body, pins = query_fixture()
    recipe.check_query(body, connection, "SELECT 'N/A' AS value", pins, set(pins))
    assert body["rows"] == [["N/A"]]


@pytest.mark.parametrize(
    "fault", ["analysis-pin", "source-pin", "missing", "truncated", "cell-truncated", "unknown", "value"]
)
def test_query_refuses_stale_or_missing_pin_truncation_or_changed_cell(fault):
    connection, body, pins = query_fixture()
    if fault == "analysis-pin":
        body["publication"]["analysis"]["artifact_digest"] = "source"
    elif fault == "source-pin":
        body["publication"]["source"]["artifact_digest"] = "links"
    elif fault == "missing":
        body["publication"].pop("analysis")
    elif fault == "truncated":
        body["truncated"] = True
    elif fault == "cell-truncated":
        body["truncated_cells"] = 1
    elif fault == "unknown":
        body["publication"]["unknown"] = {"family": "unknown", "artifact_digest": None}
    else:
        body["rows"] = [["0"]]
    with pytest.raises(ValueError):
        recipe.check_query(body, connection, "SELECT 'N/A' AS value", pins, set(pins))


@pytest.mark.parametrize("fault", [None, "sha256", "rows", "generation_id", "datasets", "missing", "unknown"])
def test_shared_receipt_view_checks_each_nested_generation_byte_and_dataset_pin(fault):
    connection, body, pins = query_fixture()
    expected = {
        name: dict(
            artifact_digest=digest,
            generation_id=name + ":generation",
            sha256=name + ":bytes",
            rows=1,
            datasets=[name + ":dataset"],
        )
        for name, digest in pins.items()
    }
    actual = deepcopy(expected)
    if fault in {"sha256", "rows", "generation_id", "datasets"}:
        actual["scorecard-analysis"][fault] = "different"
    elif fault == "missing":
        actual.pop("scorecard-analysis")
    elif fault == "unknown":
        actual["unknown"] = {}
    body["publication"] = {"etl_receipts": {"status": "managed_receipts", "families": actual}}
    if fault is None:
        recipe.check_query(body, connection, "SELECT 'N/A' AS value", pins, set(pins), expected)
    else:
        with pytest.raises(ValueError):
            recipe.check_query(body, connection, "SELECT 'N/A' AS value", pins, set(pins), expected)


def receipt_fixture(tmp_path, monkeypatch, *, fault=None):
    pin = {
        "sha256": "sha256:" + "a" * 64,
        "artifactDigest": "sha256:" + "b" * 64,
        "family": "scorecards",
        "byteSize": 10,
    }
    states = {"scorecard_member_links": {"resolved": 1, "ambiguous": 1}, "scorecard_item_links": {"unresolved": 1}}
    prepared = {
        "qualification": {
            "counts": {"scorecard_member_links": 2, "scorecard_item_links": 1},
            "input_pins": {"scorecards": pin},
            "resolution_statuses": states,
        }
    }
    rows: list[dict[str, Any]] = []
    for name, statuses in states.items():
        for ordinal, status in enumerate(statuses):
            identity = [
                ["scorecard_id", "publisher:edition"],
                ["publisher_member_key" if "member" in name else "item_id", str(ordinal)],
            ]
            processing = {"input_pins_json": json.dumps({"scorecards": pin}), "resolution_status": status}
            witnesses = [{"source_id": "scorecards", "sha256": pin["sha256"], "body_version": pin["artifactDigest"]}]
            rows.append(
                {
                    "dataset": name,
                    "identity_json": exact_json(identity),
                    "processing_json": exact_json(processing),
                    "witnesses": witnesses,
                }
            )
    if fault == "row-parent":
        processing = {"input_pins_json": "{}", "resolution_status": "resolved"}
        rows[0]["processing_json"] = exact_json(processing)
    elif fault == "witness":
        rows[0]["witnesses"][0]["body_version"] = "wrong"
    elif fault == "missing-row":
        rows.pop()
    elif fault == "status":
        prepared["qualification"]["resolution_statuses"]["scorecard_member_links"] = {"resolved": 2}
    path = tmp_path / "receipts.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    calls = []
    monkeypatch.setattr(recipe, "admitted_read_policies", lambda *a, **kw: {n: n for n in recipe.LINK_NAMES})

    def validate(*args, **kwargs):
        calls.append((args, kwargs))
        if fault == "admission":
            raise ValueError("Shared receipt admission refused")

    monkeypatch.setattr(recipe, "validate_receipt_bundle", validate)
    paths = {n: tmp_path / (n + ".parquet") for n in recipe.LINK_NAMES}
    paths["etl_receipts"] = path
    family = {"etlReceipts": {"generationId": "selected-generation"}}
    root = {"spec": {"etlReceipts": {"policies": []}}}
    return paths, family, root, prepared, calls


def test_all_nonaccepted_outcomes_remain_in_qualified_total_and_edition_counts(tmp_path, monkeypatch):
    paths, family, root, prepared, calls = receipt_fixture(tmp_path, monkeypatch)
    result = recipe.verify_analysis_receipts(paths, family, root, prepared)
    assert result["counts"] == prepared["qualification"]["counts"]
    assert len(result["per_edition"]) == 3
    assert len(calls) == 1 and calls[0][1] == {"generation_id": "selected-generation"}


@pytest.mark.parametrize("fault", ["row-parent", "witness", "missing-row", "status", "admission"])
def test_full_receipt_population_parent_witness_and_admission_refusals(tmp_path, monkeypatch, fault):
    paths, family, root, prepared, _ = receipt_fixture(tmp_path, monkeypatch, fault=fault)
    with pytest.raises(ValueError):
        recipe.verify_analysis_receipts(paths, family, root, prepared)


def test_sql_outcomes_select_tagged_fields_by_name_and_generation_without_positional_guessing():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE etl_receipts(dataset VARCHAR,outcome VARCHAR,generation_id VARCHAR,identity_json VARCHAR,processing_json VARCHAR)"
    )
    identity = exact_json([["publisher_member_key", "x"], ["scorecard_id", "p:e"]])
    processing = exact_json({"resolution_status": "unresolved", "other": "literal"})
    for generation in ["chosen'period", "other"]:
        connection.execute(
            "INSERT INTO etl_receipts VALUES (?,?,?,?,?)",
            ["scorecard_member_links", "observed", generation, identity, processing],
        )
    sql = recipe.analysis_outcomes_sql("chosen'period")
    assert recipe.local_rows(connection, sql) == [
        {
            "dataset": "scorecard_member_links",
            "outcome": "observed",
            "scorecard_id": "p:e",
            "publisher_member_key": "x",
            "item_id": None,
            "resolution_status": "unresolved",
        }
    ]
