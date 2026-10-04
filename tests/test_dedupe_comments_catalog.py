"""The maintained native duplicate audit never mutates or migrates a catalog."""
import sys
from unittest.mock import Mock
import pytest
from scripts import dedupe_comments_catalog as script


@pytest.mark.parametrize(('duplicates', 'expected'), [([], 0), ([('EPA', 3, 2)], 1)])
def test_native_audit_outcome(monkeypatch, duplicates, expected):
    connection = Mock()
    monkeypatch.setattr(sys, 'argv', ['audit'])
    monkeypatch.setattr(script.iceberg, 'is_configured', lambda: True)
    monkeypatch.setattr(script.iceberg, '_connect', lambda: connection)
    monkeypatch.setattr(script.iceberg, 'audit_duplicates', lambda *_: duplicates)
    assert script.main() == expected
    connection.close.assert_called_once()
    connection.execute.assert_not_called()


def test_no_rewrite_option(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['audit', '--apply'])
    with pytest.raises(SystemExit) as result:
        script.main()
    assert result.value.code == 2
