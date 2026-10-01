"""Schema/dependency binding of trusted view declarations, without activation."""

import json
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
        "transforms/fec_research_context.py",
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


@pytest.mark.parametrize(
    "change",
    [
        {"source_generation_pin": "bad"},
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
