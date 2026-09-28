"""Exact reviewed lookups through MCP need no source table connection."""

import asyncio
from pathlib import Path
import shutil

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from spicy_regs import mcp_server, vocabulary_mapping
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
    assert lookup("regulations.gov:agency", "Office of Personnel Management")["status"] == "unmatched"
    assert lookup("regulations.gov:agency", "ARCTICGAS")["abstentions"]
    assert lookup("regulations.gov:agency", "OPM", on_date="1990-01-01")["status"] == "temporal_scope_unqualified"
    assert lookup("gao_topic", "OPM")["status"] == "unsupported_namespace"
    with pytest.raises(ToolError):
        lookup("regulations.gov:agency", "OPM", on_date="2026-02-30")


@pytest.mark.parametrize("filename", ["view-manifest.json", "agency-projection-unresolved.parquet"])
def test_lookup_refuses_tampered_vendored_evidence(tmp_path, monkeypatch, filename):
    root = tmp_path / "reference/refspec"
    root.mkdir(parents=True)
    original = Path(__file__).parents[1] / "src/spicy_regs/reference/refspec"
    for source in original.iterdir():
        if source.is_file():
            shutil.copyfile(source, root / source.name)
    file = root / filename
    data = bytearray(file.read_bytes())
    data[len(data) // 2] ^= 1
    file.write_bytes(data)
    monkeypatch.setattr(vocabulary_mapping, "files", lambda _: tmp_path)
    with pytest.raises(ValueError, match="differs|not RefSpec"):
        vocabulary_mapping.lookup_agency("regulations.gov:agency", "ARCTICGAS")


@pytest.mark.parametrize("filename", ["view-manifest.json", "tables/agency-registry-events.parquet"])
def test_registry_lookup_refuses_tampered_owner_artifacts(tmp_path, monkeypatch, filename):
    from spicy_regs.ontology import agencies

    root = tmp_path / "registry"
    shutil.copytree(str(agencies.AGENCY_REGISTRY_VIEW_PATH), root)
    file = root / filename
    data = bytearray(file.read_bytes())
    data[len(data) // 2] ^= 1
    file.write_bytes(data)
    monkeypatch.setattr(agencies, "AGENCY_REGISTRY_VIEW_PATH", root)
    with pytest.raises(ValueError, match="differs|not RefSpec"):
        vocabulary_mapping.lookup_agency("federal_register_agency", "559")
