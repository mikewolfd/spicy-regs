"""Exact reviewed lookups through MCP need no source table connection."""

import asyncio
import json
import shutil

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from spicy_regs import mcp_server, vocabulary_mapping
from spicy_regs.ontology import agencies
from tests.test_mcp_server import _tool_data


def test_real_lookup_namespaces_and_temporal_abstention(monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: pytest.fail("Lookup opened source tables"))
    server = mcp_server.build_server()
    tool = next(t for t in asyncio.run(server.list_tools()) if t.name == "lookup_agency")
    assert set(tool.input_schema["required"]) == {"namespace", "identifier"}
    assert tool.input_schema["properties"]["identifier"]["maxLength"] == 256

    def lookup(namespace, identifier, **kw):
        return _tool_data(server, "lookup_agency", {"namespace": namespace, "identifier": identifier, **kw})

    assert lookup("regulations.gov:agency", "OPM")["candidates"][0]["org"] == "urn:ref:federal-register-agency:406"
    assert lookup("federal_register_agency", "406")["status"] == "reviewed_mapping"
    assert lookup("federal_register_agency", "406")["registry_evidence"]["organization"]["short_name"] == "OPM"
    assert lookup("regulations.gov:agency", "Office of Personnel Management")["status"] == "unmatched"
    assert lookup("regulations.gov:agency", "ARCTICGAS")["abstentions"]
    assert lookup("regulations.gov:agency", "OPM", on_date="1990-01-01")["status"] == "temporal_scope_unqualified"
    # View 1.4 reviewed alias lookup through MCP without opening source tables.
    blm = lookup("name", "Bureau of Land Management")
    assert blm["status"] == "reviewed_alias"
    assert blm["registry_evidence"]["organization"]["fr_id"] == 275
    assert blm["publication"]["schema_version"] == "1.4"
    # High-alias agency (DoD, 33 aliases) stays compact and well below REPLY_CHARS.
    dod = lookup("federal_register_agency", "103")
    assert len(dod["registry_evidence"]["aliases"]) == 33
    assert len(json.dumps(dod, separators=(",", ":"))) < mcp_server.REPLY_CHARS
    # Round 6 + View 1.4: another namespace is refused naming all three closed namespaces.
    with pytest.raises(ToolError, match="'regulations.gov:agency', 'federal_register_agency' or 'name'"):
        lookup("gao_topic", "OPM")
    with pytest.raises(ToolError):
        lookup("regulations.gov:agency", "OPM", on_date="2026-02-30")


@pytest.mark.parametrize(
    "filename",
    [
        "view-manifest.json",
        "tables/agency-projection-unresolved.parquet",
        "tables/agency-registry-events.parquet",
        "tables/agency-registry-aliases.parquet",
        "tables/agency-registry-organizations.parquet",
    ],
)
def test_registry_lookup_refuses_tampered_owner_artifacts(tmp_path, monkeypatch, filename):
    root = tmp_path / "registry"
    shutil.copytree(str(agencies.AGENCY_REGISTRY_VIEW_PATH), root)
    file = root / filename
    data = bytearray(file.read_bytes())
    data[len(data) // 2] ^= 1
    file.write_bytes(data)
    monkeypatch.setattr(agencies, "AGENCY_REGISTRY_VIEW_PATH", root)
    with pytest.raises(ValueError, match="differs|not RefSpec"):
        vocabulary_mapping.lookup_agency("federal_register_agency", "559")
