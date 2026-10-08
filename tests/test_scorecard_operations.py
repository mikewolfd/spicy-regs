"""Maintained commands stay importable and live activation stays qualification-bound."""

import json
from dataclasses import replace

import pytest
from spicy_docs.sources.scorecards import ScorecardEdition

from spicy_regs.scorecards import registry
from spicy_regs.scorecards.operations import cli, inspect


@pytest.mark.parametrize("command", cli.COMMANDS)
def test_phase_help_requires_no_workspace_scripts_or_source_requests(command, capsys):
    with pytest.raises(SystemExit) as result:
        cli.main([command, "--help"])
    assert result.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_every_reader_is_inspected_without_acquisition():
    from spicy_docs.sources.scorecards import ADAPTER_PUBLISHERS
    result = inspect.inspect_readers()
    assert {row["adapter"] for row in result["readers"]} == set(ADAPTER_PUBLISHERS)
    assert all(row["explicit_interfaces"] and row["parser_version"] and len(row["reader_sha256"]) == 64
               for row in result["readers"])


def test_live_edition_cannot_outgrow_its_qualified_scope():
    source = next(source for source in registry.load_registry() if source.enabled)
    edition = ScorecardEdition(**registry.live_qualification(source)["edition"])
    registry.check_live_edition(source, edition)
    with pytest.raises(registry.RegistryError, match="qualified source scope"):
        registry.check_live_edition(source, replace(edition, edition_id="unqualified-new-edition"))


@pytest.mark.parametrize("change", ["version", "reader", "policy", "missing", "format"])
def test_live_source_drift_refuses_before_acquisition(tmp_path, monkeypatch, change):
    source = next(source for source in registry.load_registry() if source.enabled)
    document = json.loads(registry.LIVE_QUALIFICATIONS.read_text())
    row = next(row for row in document["scopes"] if row["publisher_id"] == source.publisher_id)
    if change == "version":
        row["parser_version"] = "different/1"
    elif change == "reader":
        row["reader_sha256"] = "0" * 64
    elif change == "policy":
        row["evidence_policy"] = "full"
    elif change == "missing":
        document["scopes"] = []
    else:
        document["format_version"] = "unknown"
    path = tmp_path / "qualification.json"
    path.write_text(json.dumps(document))
    monkeypatch.setattr(registry, "LIVE_QUALIFICATIONS", path)
    with pytest.raises(registry.RegistryError):
        registry.load_registry()
