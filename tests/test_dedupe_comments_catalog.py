"""Scheduled audits fail visibly; explicit recovery precedes the clean shortcut."""

import sys
from unittest.mock import Mock

import pytest

from scripts import dedupe_comments_catalog as script


@pytest.mark.parametrize(
    ("pending", "apply", "duplicates", "expected", "repairs"),
    [(False, False, [], 0, 0),
     (False, False, [("EPA", 3, 2)], 1, 0),
     (True, False, [], 1, 0),
     (True, True, [], 0, 1)],
)
def test_audit_and_recovery_outcomes(monkeypatch, pending, apply, duplicates, expected, repairs):
    con = Mock()
    repair = Mock(return_value=(3, 2))
    audit = Mock(return_value=duplicates)
    monkeypatch.setattr(sys, "argv", ["dedupe"] + (["--apply"] if apply else []))
    monkeypatch.setattr(script.iceberg, "is_configured", lambda: True)
    monkeypatch.setattr(script.iceberg, "_connect", lambda: con)
    monkeypatch.setattr(script.iceberg, "dedupe_recovery_pending", lambda *_: pending)
    monkeypatch.setattr(script.iceberg, "audit_duplicates", audit)
    monkeypatch.setattr(script.iceberg, "dedupe_table", repair)
    assert script.main() == expected
    assert repair.call_count == repairs
    con.close.assert_called_once()
    if pending and not apply:
        audit.assert_not_called()
