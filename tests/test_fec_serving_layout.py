"""Frozen legacy FEC remains readable while native inputs keep strict receipt gates."""

from dataclasses import replace
import json

import duckdb
import pyarrow as pa
import pytest

from spicy_regs import fec_release, mcp_server
from spicy_regs.fec_receipt_adapter import ReceiptAdapter, selected_qualified_views
from spicy_regs.relationship_views.fec_filing_associations import financial_number_association_sql, financial_header_association_sql
from spicy_regs.relationship_views.sql_views import SQLView
from tests.test_fec_filing_associations import EVIDENCE, GEN, NAMESPACE, filing, financial, headers
from spicy_regs.transforms.fec_filing_associations import ASSOCIATION_SCHEMA
from tests.test_fec_release import fixture, capture
from tests.test_mcp_server import _tool_data


def test_compatible_legacy_release_installs_and_answers_through_mcp(tmp_path, monkeypatch):
    specs, index, receipt, consumer = fixture(tmp_path)
    config = capture(tmp_path, specs, receipt, consumer)
    with duckdb.connect() as con:
        con.execute("CREATE TABLE _spicy_publication(snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
        for table in ("fec_receipts", "fec_reports", "members"):
            con.execute(f"CREATE TABLE {table}(id INTEGER)")
            con.execute(f"INSERT INTO {table} VALUES (1)")
        monkeypatch.setattr(mcp_server, "FEC_QUALIFIED_VIEWS", specs)
        monkeypatch.setattr(mcp_server, "FEC_LEGACY_QUALIFIED_VIEWS", specs)
        monkeypatch.setattr(mcp_server, "_fec_release_configuration", lambda *args: config)
        monkeypatch.setattr(mcp_server, "TABLES", ("fec_receipts", "fec_reports", "members"))
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        mcp_server._install_relationship_views(con)
        mcp_server._apply_security_settings(con)
        app = mcp_server.build_server()
        result = _tool_data(app, "query_sql", {"sql": "SELECT * FROM fec_test_money_members"})
        assert result["rows"] == [[1]]
        assert result["publication"]["fec_test_money_members"]["release_compatibility"]["status"] == "compatible"
        assert not any(name.startswith("_spicy_fec_processing_") for name, in con.execute("SHOW TABLES").fetchall())


@pytest.mark.parametrize("marker", ["local", "published", "malformed"])
def test_native_selection_never_falls_back_to_legacy_when_receipts_are_missing(marker):
    spec = SQLView("test", {"fec_receipts": ("id",), "fec_filings": ("id",)},
                   lambda _: "SELECT id FROM fec_receipts", "test", ("id",))
    index, local = {"families": {}}, {}
    if marker == "local":
        local["fec_receipts"] = {"receipts": "/missing", "subjects": [], "generation_id": "native"}
    else:
        index["families"]["fec-query"] = {
            "tables": {"fec_receipts.parquet": {}},
            "etlReceipts": {"datasets": ["fec_receipts"] if marker == "published" else []},
        }
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, index, "unused", local_native=local)
        with pytest.raises(ValueError, match="require selected native receipts"):
            adapter.prepare_qualified(spec, spec.query({}))
        assert con.execute("SHOW TABLES").fetchall() == []


@pytest.mark.parametrize("legacy,version,resolved", [
    (True, "fec-identity-observations/1", True),
    (True, "fec-identity-observations/2", False),
    (False, "fec-identity-observations/2", True),
    (False, "fec-identity-observations/1", False),
])
def test_filing_mapping_matches_only_the_selected_layout(legacy, version, resolved):
    rows = [financial()]
    target = {**filing(), "mapping_version": version}
    with duckdb.connect() as con:
        con.register("fec_receipts", pa.Table.from_pylist(rows))
        con.register("fec_filings", pa.Table.from_pylist([target]))
        sql = financial_number_association_sql("fec_receipts", GEN, {NAMESPACE: EVIDENCE},
                                              columns=tuple(rows[0]), legacy=legacy)
        result = con.sql(sql).to_arrow_table().to_pylist()[0]
    assert (result["association_status"] == "resolved_native_filing_key") == resolved
    assert result["policy_version"] == "fec-retained-filing-association/" + ("1" if legacy else "2")
    assert (result["filing_key"] is not None) == resolved


def test_selected_layout_changes_registered_sql_policy_before_binding(tmp_path):
    specs, _, _, _ = fixture(tmp_path)
    legacy = tuple(replace(spec, mapping_version="old/1") for spec in specs)
    old_index = {"families": {}}
    assert selected_qualified_views(specs, legacy, old_index) == legacy
    native = {"families": {"fec-query": {
        "tables": {"fec_receipts.parquet": {}}, "etlReceipts": {"datasets": []},
    }}}
    selected = selected_qualified_views(specs, legacy, native)
    assert selected[0] is specs[0] and selected[1] is legacy[1]
    before = fec_release.capture_configuration(legacy, receipt_digest=None, image_digest=None,
                                              base_url="unused", consumer={})
    after = fec_release.capture_configuration(selected, receipt_digest=None, image_digest=None,
                                             base_url="unused", consumer={})
    assert before["views"][specs[0].view.name]["interpretation"]["mapping_version"] == "old/1"
    assert after["views"][specs[0].view.name]["interpretation"]["mapping_version"] == specs[0].mapping_version


@pytest.mark.parametrize("legacy,version,resolved", [
    (True, "fec-retained-filing-association/1", True),
    (True, "fec-retained-filing-association/2", False),
    (False, "fec-retained-filing-association/2", True),
    (False, "fec-retained-filing-association/1", False),
])
def test_header_policy_matches_only_the_selected_layout(legacy, version, resolved):
    association = {**headers()[0], "policy_version": version}
    observation = {**financial(), "filing_header_record_id": association["header_record_id"],
                   "filing_header_locator_json": association["header_locator_json"]}
    with duckdb.connect() as con:
        con.register("fec_receipts", pa.Table.from_pylist([observation]))
        con.register("fec_filing_header_associations", pa.Table.from_pylist([association], schema=ASSOCIATION_SCHEMA))
        result = con.sql(financial_header_association_sql("fec_receipts", GEN, legacy=legacy)).to_arrow_table().to_pylist()[0]
    assert (result["association_status"] == "resolved_native_filing_key") == resolved
    assert result["association_policy_version"] == "fec-retained-filing-association/" + ("1" if legacy else "2")
    assert (result["filing_key"] is not None) == resolved


@pytest.mark.parametrize("native", [False, True])
def test_filing_reference_resolution_uses_the_selected_legacy_or_native_schema(native):
    """The pinned legacy filing-link footer lacks only the new receipt field."""
    import base64
    from spicy_regs.fec_receipt_adapter import qualified_views, processing_declarations
    from spicy_regs.relationship_views.sql_views import install_sql_views

    scope = dict(source_generation_pin=GEN, population="retained fixture", as_of="fixture", namespace_evidence={})
    native_specs = qualified_views(scope)
    legacy_specs = qualified_views(scope, legacy_associations=True)
    index = {"families": {"fec-query": {
        "tables": {"fec_filing_links.parquet": {}, "fec_filings.parquet": {}},
        **({"etlReceipts": {"datasets": ["fec_filing_links", "fec_filings"]}} if native else {}),
    }}}
    spec = next(s for s in selected_qualified_views(native_specs, legacy_specs, index)
                if s.view.name == "fec_filing_reference_resolution")
    declarations = processing_declarations()
    links_schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declarations["fec_filing_links"]["arrow_schema"])))
    links_schema = pa.schema([field for field in links_schema if field.name != "source_representation_role"])
    links = [dict(record_id="link", filing_key="source-key", source_filing_record_id="source",
                  target_filing_key="target-key", target_resolution_status="pending", relation_type="reported_reference")]
    filings_schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declarations["fec_filings"]["arrow_schema"])))
    filings = [dict(record_id=record, filing_key=key, source_authority="fec", source_namespace="filings",
                    report_number="1", native_filer_id="C00000001", form_type="F3")
               for record, key in [("source", "source-key"), ("target-a", "target-key"), ("target-b", "target-key")]]
    with duckdb.connect() as con:
        con.register("fec_filing_links", pa.Table.from_pylist(links, schema=links_schema))
        con.register("fec_filings", pa.Table.from_pylist(filings, schema=filings_schema))
        result = install_sql_views(con, ["fec_filing_links", "fec_filings"], [spec.view])[spec.view.name]
        if native:
            assert result["status"] == "unsupported"
            assert "fec_filing_links.source_representation_role" in result["reason"]
        else:
            assert result["status"] == "available", result
            row = con.sql("SELECT * FROM fec_filing_reference_resolution").to_arrow_table().to_pylist()[0]
            assert row["record_id"] == "link" and row["relation_type"] == "reported_reference"
            assert row["target_resolution_status"] == "resolved_native_filing_key"
            assert row["target_observation_count"] == 2
            assert "source_representation_role" not in row
        assert spec.view.query({}) == next(s for s in native_specs if s.view.name == spec.view.name).view.query({})


@pytest.mark.parametrize("missing", ["source_filing_record_id", "filing_key", "target_filing_key", "target_resolution_status"])
def test_legacy_filing_reference_refuses_missing_resolution_inputs(missing):
    from spicy_regs.fec_receipt_adapter import qualified_views, processing_declarations
    from spicy_regs.relationship_views.sql_views import install_sql_views

    spec = next(s for s in qualified_views(dict(source_generation_pin=GEN, population="fixture", as_of="fixture",
                                               namespace_evidence={}), legacy_associations=True)
                if s.view.name == "fec_filing_reference_resolution")
    declarations = processing_declarations()
    with duckdb.connect() as con:
        for table in spec.view.required:
            columns = declarations[table]["columns"]
            con.execute(f"CREATE TABLE {table}(" + ",".join(f'"{n}" {t}' for n, t in columns
                        if n != "source_representation_role" and not (table == "fec_filing_links" and n == missing)) + ")")
        result = install_sql_views(con, list(spec.view.required), [spec.view])[spec.view.name]
        assert result["status"] == "unsupported"
        assert f"fec_filing_links.{missing}" in result["reason"]
        assert con.execute("SELECT count(*) FROM information_schema.views WHERE table_name=?", [spec.view.name]).fetchone() == (0,)
