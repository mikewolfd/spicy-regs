"""Installed FEC registry without any fabricated production release or image pin."""

from collections import Counter
from importlib.resources import files
import json
from pathlib import Path

import duckdb
import pytest

from spicy_regs import fec_release, mcp_server
from tests.test_mcp_server import _tool_data


def test_installed_registry_uses_retained_application_scope():
    scope = json.loads(files("spicy_regs").joinpath("fec_query_scope.json").read_text())
    specs = mcp_server.FEC_QUALIFIED_VIEWS
    assert len(specs) == len({s.view.name for s in specs}) == 72
    assert set(scope) == {"source_generation_pin", "namespace_evidence", "population", "as_of"}
    assert scope["source_generation_pin"] == "sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0"
    assert len(scope["namespace_evidence"]) == 6
    assert all(s.population == scope["population"] and s.as_of == scope["as_of"] for s in specs)
    association = next(s for s in specs if s.view.name == "fec_receipts_native_filing_associations")
    for pin in scope["namespace_evidence"].values():
        assert pin in association.view.query({})


def test_missing_release_disables_actual_registry_but_keeps_raw_and_unrelated_views(tmp_path, monkeypatch):
    for name in ("SPICY_REGS_FEC_RELEASE_SHA256", "SPICY_REGS_FEC_RELEASE_FILE", "SPICY_REGS_CONSUMER_IMAGE_DIGEST"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MB", "max_temp_directory_size": "0B"}) as con:
        # Minimal local raw rows exercise availability, not source qualification.
        con.execute("CREATE TABLE fec_receipts(record_id VARCHAR, amount DECIMAL(38,9))")
        con.execute("INSERT INTO fec_receipts VALUES ('raw-example', 12.5)")
        con.execute("CREATE TABLE sam_entities(uei VARCHAR, entity_eft_indicator VARCHAR)")
        con.execute("INSERT INTO sam_entities VALUES ('ABCDEFGHIJKL', NULL)")
        mcp_server._install_relationship_views(con)
        mcp_server._apply_security_settings(con)
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        discovery = _tool_data(server, "list_sources", {})
        release = discovery["fec_release"]
        assert release["receipt_sha256"] is None
        assert release["consumer"]["image_digest"] is None
        assert len(release["views"]) == 72
        for state in release["views"].values():
            assert state["status"] == "disabled"
            assert any(reason["reason"] == "deployment_receipt_digest_missing" for reason in state["reasons"])
        name = "fec_receipts_source_analysis_decision"
        described = _tool_data(server, "describe_table", {"table": name})
        assert not described["available"]
        assert "deployment_receipt_digest_missing" in described["relationship"]["reason"]
        with pytest.raises(Exception, match="disabled.*deployment_receipt_digest_missing"):
            _tool_data(server, "query_sql", {"sql": f"SELECT status, value FROM {name}"})
        raw = _tool_data(server, "query_sql", {"sql": "SELECT record_id FROM fec_receipts"})
        assert raw["rows"] == [{"record_id": "raw-example"}]
        assert "release_compatibility" not in raw["publication"]["fec_receipts"]
        unrelated = _tool_data(server, "query_sql", {"sql": "SELECT uei, entity_eft_indicator FROM sam_uei_registrations"})
        assert unrelated["rows"] == [{"uei": "ABCDEFGHIJKL", "entity_eft_indicator": None}]


def test_interpretation_file_digests_are_shared_only_within_each_capture(monkeypatch):
    specs = mcp_server.FEC_QUALIFIED_VIEWS
    original = fec_release._file_digest
    calls = Counter()

    def counted(path):
        calls[path] += 1
        return original(path)

    monkeypatch.setattr(fec_release, "_file_digest", counted)
    config = fec_release.capture_configuration(specs, receipt_digest=None, image_digest=None, base_url="unused", consumer={})
    expected = {Path(p).resolve() for s in specs for group in s.identities.values() for p in group.values()}
    assert set(calls) == expected and set(calls.values()) == {1}
    assert all(state["error"] is None for state in config["views"].values())
    fec_release.capture_configuration(specs, receipt_digest=None, image_digest=None, base_url="unused", consumer={})
    assert set(calls.values()) == {2}
