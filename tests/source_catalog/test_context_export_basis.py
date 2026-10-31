"""Only the trusted publication-context rule receives its public observation marker."""
import pytest

from spicy_regs.source_catalog.file_inventory import native_row
from spicy_regs.source_catalog.raw_mapping import SourceMapper


class NoReceiptReferences:
    def indexed(self, ref):
        return None

    def original(self, digest):
        return None


def exported(periods):
    return native_row({'id': 'alias', 'name': 'source', 'status': 'held', 'sha256': None, 'bytes': 0},
                      {'mapping': {'periods': periods}}, dataset_id='family:federal-register',
                      publisher=None, inventory_kind='active', references=NoReceiptReferences())


@pytest.mark.parametrize(('origin', 'kind', 'rule', 'qualified'), [
    ('publication-context', 'publication-date', 'exact-original-unique-published-date-context/1', True),
    ('receipt', 'publication-date', 'exact-original-unique-published-date-context/1', False),
    ('publication-context', 'signing-date', 'exact-original-unique-published-date-context/1', False),
    ('publication-context', 'publication-date', 'unverified-date', False),
])
def test_export_marker_requires_typed_context_kind_and_rule(origin, kind, rule, qualified):
    row = exported([{'kind': kind, 'label': '2025-07-11', 'origin': origin, 'rule': rule,
                     'basis': 'Independent metadata date'}])
    assert row['period_basis'] == ('Observed in pinned publication context' if qualified else 'Independent metadata date')


def test_untrusted_receipt_period_cannot_invent_observed_context_origin_or_basis():
    mapping = SourceMapper({}).map_source({'dataset_id': 'family:federal-register', 'receipt_context': {
        'period': {'kind': 'publication-date', 'label': '2030-01-01', 'basis': 'Observed in pinned publication context',
                   'origin': 'publication-context', 'rule': 'exact-original-unique-published-date-context/1'}}})
    assert mapping['periods'] and all('origin' not in period and 'rule' not in period for period in mapping['periods'])
    assert exported(mapping['periods'])['period_basis'] != 'Observed in pinned publication context'
