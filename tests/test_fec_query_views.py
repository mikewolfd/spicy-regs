"""Schema/dependency binding of trusted view declarations, without activation."""

import json
from copy import deepcopy
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any

import duckdb
import pytest

from spicy_regs import fec_release
from spicy_regs.mcp_server import _tables_named
from spicy_regs.relationship_views.core import quoted
from spicy_regs.relationship_views.fec_query_views import fec_query_views
from spicy_regs.relationship_views.sql_views import install_sql_views

REPO = Path(__file__).resolve().parents[1]
SOURCE = "sha256:" + "a" * 64  # synthetic test pin, never a production release


@pytest.fixture
def installed(tmp_path):
    schema = json.loads((REPO / "data_dictionary/fec_typed_schemas.json").read_text())["tables"]
    root = tmp_path / "spicy_regs"
    for name in [
        "relationship_views/fec_query_views.py",
        "relationship_views/fec_typed.py",
        "relationship_views/fec_filing_associations.py",
        "relationship_views/fec_financial_meaning.py",
        "fec_financial_rules.py",
        "fec_versions.py",
    ]:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / "src/spicy_regs" / name, path)
    (root / "table_metadata.json").write_text(
        json.dumps(
            {
                name: {"columns": [{"column_name": n, "column_type": t} for n, t in item["columns"]]}
                for name, item in schema.items()
            }
        )
    )
    return root, {name: dict(item["columns"]) for name, item in schema.items()}


def factory(root, **kwargs):
    return fec_query_views(
        source_generation_pin=SOURCE,
        population="synthetic selected observations",
        as_of="test-only",
        namespace_evidence={},
        package_root=root,
        **kwargs,
    )


def test_all_views_bind_with_exact_declared_tables_and_columns(installed):
    root, schemas = installed
    specs = factory(root)
    assert len({s.view.name for s in specs}) == len(specs)
    assert all(s.evidence_generations == {"fec-observations": SOURCE} for s in specs)
    for spec in specs:
        with duckdb.connect(config={"threads": 1, "memory_limit": "128MB", "max_temp_directory_size": "0B"}) as con:
            for table, columns in spec.view.required.items():
                declaration = ",".join(quoted(name) + " " + schemas[table][name] for name in columns)
                con.execute("CREATE TABLE " + quoted(table) + " (" + declaration + ")")
            query = spec.view.query({})
            assert _tables_named(con, query) == set(spec.view.required), spec.view.name
            status = install_sql_views(con, spec.view.required, [spec.view])
            assert status[spec.view.name]["status"] == "available"
            assert con.sql("SELECT count(*) FROM " + quoted(spec.view.name)).fetchone() == (0,)


def test_specs_capture_scope_and_literal_witnesses_and_installed_identities(installed):
    root, _ = installed
    proofs = {"fec-bulk-individual-contributions": "sha256:" + "b" * 64}
    specs = fec_query_views(
        source_generation_pin=SOURCE,
        population="selected population",
        as_of="explicit as-of",
        namespace_evidence=proofs,
        package_root=root,
    )
    proofs.clear()
    config = fec_release.capture_configuration(
        specs, receipt_digest=None, image_digest=None, base_url="unused", consumer={}
    )
    assert config["receipt_sha256"] is None and config["receipt"] is None
    assert config["receipt_error"] == "deployment_receipt_digest_missing"
    assert all(item["error"] is None for item in config["views"].values())
    association = config["views"]["fec_receipts_native_filing_associations"]
    assert "sha256:" + "b" * 64 in association["sql"]
    for spec in specs:
        entry = config["views"][spec.view.name]
        assert entry["population"] == "selected population" and entry["as_of"] == "explicit as-of"
        assert all(entry["interpretation"][group] for group in ("policies", "dictionaries", "definitions"))
        assert all(Path(path).is_file() for group in spec.identities.values() for path in group.values())
    assert "spicy_regs.transforms" not in str(config["views"]["fec_receipts_source_analysis_decision"]["sql"])


def test_installed_serving_selection_preserves_all_historical_sql_and_dependencies():
    from spicy_regs import mcp_server
    from spicy_regs.fec_receipt_adapter import qualified_views

    scope = mcp_server.FEC_QUERY_SCOPE
    # Accepted joint descriptor 08a4e8fdf4bcf6b9daeb50640e9f13665c3f93735a8e35014535ddc6207a8082
    # preserves the historical witnesses while selecting the compatible observations representation.
    historical = "sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0"
    selected = "sha256:4cb0dffa91c278eb4e7a6048e266a5e4389748666428a82bc58c35ee3fe77f2f"
    assert scope["source_generation_pin"] == historical
    assert scope["selected_observations_pin"] == selected
    prior_scope = {key: value for key, value in scope.items() if key != "selected_observations_pin"}
    for actual, legacy in ((mcp_server.FEC_QUALIFIED_VIEWS, False), (mcp_server.FEC_LEGACY_QUALIFIED_VIEWS, True)):
        prior = qualified_views(prior_scope, legacy_associations=legacy)
        assert [s.view.name for s in actual] == [s.view.name for s in prior]
        for current, previous in zip(actual, prior, strict=True):
            assert current.view.query({}) == previous.view.query({})
            assert current.view.required == previous.view.required
            assert current.population == previous.population and current.as_of == previous.as_of
            assert current.evidence_generations == {"fec-observations": selected}
            assert previous.evidence_generations == {"fec-observations": historical}
        evidence_sql = next(s.view.query({}) for s in actual if s.view.name == "fec_account_transfers_evidence")
        assert historical in evidence_sql and selected not in evidence_sql


@pytest.mark.parametrize("fault", ["old_selection", "next_selection", "old_receipt", "query_changed", "not_retained"])
def test_separate_serving_pin_requires_exact_receipt_and_selected_pair(installed, tmp_path, fault):
    from tests.test_fec_release import capture, digest

    root, schemas = installed
    selected = digest("compatible-observations")
    spec = next(s for s in factory(root, selected_observations_pin=selected, legacy_associations=True)
                if s.view.name == "fec_account_transfers_evidence")
    index = {"families": {
        "fec-observations": {"artifactDigest": selected, "prefix": "generations/fec-observations/" + selected[7:],
                             "tables": {}},
        "fec-query": {"artifactDigest": digest("unchanged-query"), "tables": {
            name + ".parquet": {"sha256": digest(name), "byteSize": 100, "rows": 1,
                                 "columns": list(map(list, schemas[name].items()))}
            for name in spec.view.required}},
    }}
    consumer = {"image_digest": digest("image"), "code_sha256": digest("code"), "package_versions": {"duckdb": "test"}}
    state = fec_release.capture_configuration((spec,), receipt_digest=None, image_digest=None, base_url="unused",
                                              consumer=consumer)["views"][spec.view.name]
    dependencies = {name: fec_release.captured_table(index, name) for name in spec.view.required}
    receipt = {
        "format": fec_release.FORMAT, "version": fec_release.VERSION, "consumer": consumer,
        "output_membership": deepcopy(dependencies),
        "recovery": {"source_archive_sha256": digest("archive"), "rollback_receipts": [], "retained_generations": {
            "fec-query": [digest("unchanged-query")], "fec-observations": [SOURCE, selected]}},
        "views": {spec.view.name: {"dependencies": dependencies, "acceptance_receipts": [digest("accepted-pair")],
            **{key: state[key] for key in ("sql_sha256", "interpretation", "population", "as_of", "evidence_generations")}}},
    }
    config = capture(tmp_path, (spec,), receipt, consumer)
    assert fec_release.check_view(spec, config, index, dependencies)["status"] == "compatible"
    if fault in {"old_selection", "next_selection"}:
        pin = SOURCE if fault == "old_selection" else digest("unaccepted-observations")
        index["families"]["fec-observations"].update(artifactDigest=pin, prefix="generations/fec-observations/" + pin[7:])
        reason = "exact_evidence_generation_mismatch"
    elif fault == "old_receipt":
        receipt["views"][spec.view.name]["evidence_generations"] = {"fec-observations": SOURCE}
        reason = "application_and_receipt_evidence_generations_differ"
    elif fault == "query_changed":
        index["families"]["fec-query"]["artifactDigest"] = digest("unaccepted-query")
        reason = "exact_table_pin_mismatch"
    else:
        receipt["recovery"]["retained_generations"]["fec-observations"] = [SOURCE]
        reason = "required_evidence_generation_not_retained"
    config = capture(tmp_path, (spec,), receipt, consumer)
    result = fec_release.check_view(spec, config, index, dependencies)
    assert result["status"] == "disabled"
    assert reason in {issue["reason"] for issue in result["reasons"]}


@pytest.mark.parametrize(
    "change",
    [
        {"source_generation_pin": "bad"},
        {"selected_observations_pin": "bad"},
        {"selected_observations_pin": ""},
        {"population": ""},
        {"as_of": None},
        {"namespace_evidence": {"fec-bulk-individual-contributions": "bad"}},
    ],
)
def test_missing_explicit_scope_refuses(installed, change):
    root, _ = installed
    arguments: dict[str, Any] = dict(
        source_generation_pin=SOURCE, population="test", as_of="test", namespace_evidence={}, package_root=root
    )
    with pytest.raises(ValueError):
        fec_query_views(**{**arguments, **change})


def test_missing_source_column_disables_only_dependent_view(installed):
    root, schemas = installed
    spec = next(s for s in factory(root) if s.view.name == "fec_receipts_source_analysis_decision")
    table, required = next(iter(spec.view.required.items()))
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE "
            + quoted(table)
            + " ("
            + ",".join(quoted(n) + " " + schemas[table][n] for n in required if n != "amount")
            + ")"
        )
        result = install_sql_views(con, [table], [spec.view])
        assert result[spec.view.name]["status"] == "unsupported"
        assert table + ".amount" in result[spec.view.name]["reason"]


def test_runtime_import_does_not_load_arrow_source_processors_or_metadata_generator():
    code = "import sys; from spicy_regs.relationship_views.fec_query_views import fec_query_views; assert not any(n == 'pyarrow' or n == 'spicy_regs.data_dictionary' or n.startswith(('spicy_docs', 'spicy_regs.transforms')) for n in sys.modules)"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
