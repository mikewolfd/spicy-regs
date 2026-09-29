"""New serving registrations follow owning builders without asserting publication."""

from pathlib import Path
import tomllib

from spicy_regs.data_dictionary import MCP_QUERYABLE, expected_schemas
from spicy_regs.relationship_views import SQL_RELATIONSHIP_VIEWS, install_relationship_views
from spicy_regs.relationship_views.fcc_native import FCC_NATIVE_VIEWS
from spicy_regs.relationship_views.lifecycle_dates import LIFECYCLE_DATE_VIEWS
from spicy_regs.transforms.native_legal_references import CONTRACTS
from spicy_regs.transforms.build_fcc_ecfs import FILING_COLUMNS
import duckdb


def test_native_and_fcc_schemas_follow_builders():
    schemas = expected_schemas()
    for contract in CONTRACTS:
        assert schemas[contract.name] == [(column, "VARCHAR") for column in contract.columns]
        assert contract.name in MCP_QUERYABLE
    assert schemas["fcc_filings"] == [(column, "VARCHAR") for column in FILING_COLUMNS]
    assert ("docket_source_ordinal", "BIGINT") in schemas["fr_docket_links"]


def test_new_views_registered_but_missing_inputs_unavailable():
    registered = {view.name for view in SQL_RELATIONSHIP_VIEWS}
    names = {view.name for view in (*FCC_NATIVE_VIEWS, *LIFECYCLE_DATE_VIEWS)}
    assert names <= registered
    with duckdb.connect() as con:
        states = install_relationship_views(con, [])
    assert all(states[name]["status"] == "unavailable" for name in names)


def test_retained_rollup_cli_entries():
    scripts = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())["project"]["scripts"]
    for stem in ("native_legal_references", "held_citations"):
        assert scripts["run-rollup-" + stem.replace("_", "-")] == f"spicy_regs.pipelines.rollups.{stem}:app"
