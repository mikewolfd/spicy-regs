"""Bulk dispatch retains a separately callable row oracle and exact fallback refusal."""
from tests.test_etl_receipts import policy, row, context
import pytest
from spicy_regs import etl_receipts, etl_bulk


def test_default_dispatch_and_explicit_row_oracle(tmp_path, policy, row, context, monkeypatch):
    subject, receipt = etl_receipts.write_dataset([(row, context)], tmp_path / 'bundle', policy)
    calls = []
    actual = etl_bulk.validate_bundle
    def checked(*args, **kwargs):
        calls.append(True)
        return actual(*args, **kwargs)
    monkeypatch.setattr(etl_bulk, 'validate_bundle', checked)
    arguments = ({policy.dataset: [subject]}, [receipt], [policy])
    etl_receipts.validate_receipt_bundle(*arguments, generation_id='new-publisher')
    assert calls == [True]
    etl_receipts.validate_receipt_bundle(*arguments, generation_id='new-publisher', bulk=False)
    assert calls == [True]


def test_only_ineligible_sql_falls_back_and_preserves_row_error(tmp_path, policy, row, context, monkeypatch):
    subject, receipt = etl_receipts.write_dataset([(row, context)], tmp_path / 'bundle', policy)
    def ineligible(*args, **kwargs):
        raise etl_bulk.NotBulkEligible('unsupported exact spelling')
    monkeypatch.setattr(etl_bulk, 'validate_bundle', ineligible)
    arguments = ({policy.dataset: [subject]}, [receipt], [policy])
    etl_receipts.validate_receipt_bundle(*arguments, generation_id='new-publisher')
    subject.unlink()
    with pytest.raises(Exception) as actual:
        etl_receipts.validate_receipt_bundle(*arguments, generation_id='new-publisher')
    with pytest.raises(Exception) as oracle:
        etl_receipts.validate_receipt_bundle(*arguments, generation_id='new-publisher', bulk=False)
    assert type(actual.value) is type(oracle.value)
    assert str(actual.value) == str(oracle.value)
    def refused(*args, **kwargs):
        raise ValueError('qualified refusal must propagate')
    monkeypatch.setattr(etl_bulk, 'validate_bundle', refused)
    with pytest.raises(ValueError, match='qualified refusal must propagate'):
        etl_receipts.validate_receipt_bundle(*arguments, generation_id='new-publisher')
