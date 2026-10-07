"""Resolver source snapshots remain distinct from current business entities."""
from copy import deepcopy

import pytest

from spicy_regs.explorer_navigation import declarations, published_navigation, target_keys
from spicy_regs.navigation_measurements import MeasurementCache
from spicy_regs.scorecards.subject_shapes import IDENTITIES
from spicy_regs.table_joins import JOINS, RETIRED_PROCESSING_JOINS, SOURCE_JOINS
from tests.test_remaining_main_navigation import selected


@pytest.mark.parametrize(('source', 'entity', 'business_key'), [
    ('scorecard_member_links', 'scorecard_members', 'publisher_member_key'),
    ('scorecard_item_links', 'scorecard_items', 'item_id'),
])
@pytest.mark.parametrize('copies', [1, 2])
def test_resolver_snapshot_is_edition_qualified_in_both_directions(tmp_path, source, entity, business_key, copies):
    name = 'recorded_snapshot_' + source + '_scorecard_snapshots'
    spec = next(s for s in declarations(RETIRED_PROCESSING_JOINS) if s['id'] == name)
    target = spec['targets'][0]
    assert target['columns'] == ['snapshot_id', 'scorecard_id']
    rows = [
        {'scorecard_id': 's1', business_key: 'a', 'source_snapshot_id': 'snapshot1', 'resolution_status': 'unresolved'},
        {'scorecard_id': 's1', business_key: 'b', 'source_snapshot_id': 'snapshot1', 'resolution_status': 'resolved'},
        {'scorecard_id': 's2', business_key: 'c', 'source_snapshot_id': 'snapshot1', 'resolution_status': 'resolved'},
        {'scorecard_id': 's1', business_key: 'd', 'source_snapshot_id': None, 'resolution_status': 'unresolved'},
    ]
    expected_identity = ('scorecard_id', business_key)
    if source == 'scorecard_item_links':
        expected_identity += ('reference_id',)
        for ordinal, row in enumerate(rows):
            row['reference_id'] = f'reference-{ordinal}'
        # A second reference to the same item remains a separate resolver observation.
        rows.append({**rows[0], 'reference_id': 'reference-repeated-item'})
    matched = 3 if source == 'scorecard_item_links' else 2
    index, paths = selected(tmp_path, [(source, rows, None),
        ('scorecard_snapshots', [{'snapshot_id': 'snapshot1', 'scorecard_id': 's1'}] * copies, None)])
    proof = MeasurementCache(tmp_path/'cache').measure(index, paths, spec, source_identity=IDENTITIES[source])
    assert (proof['eligible'], proof['matched'], proof['missing'], proof['ambiguous']) == (matched + 1, matched, 1, matched if copies == 2 else 0)
    assert proof['unsupportedReferences'] == 1
    assert proof['sourceIdentity']['qualified']
    assert proof['distinctMatchedSourceRecords'] == matched
    assert proof['reverse']['matchedTargetRows'] == copies
    assert proof['reverse']['maximumPhysicalSourceRowsPerTarget'] == matched
    assert proof['reverse']['maximumDistinctSourceRecordsPerTarget'] == matched
    assert target_keys(target, {}, {**rows[0], 'scorecard_id': None}) is None
    # The unresolved official target still has a recorded source association.
    assert target_keys(target, {}, rows[0]) == ['snapshot1', 's1']
    original = next(j for j in SOURCE_JOINS if j.child == source and j.parent == 'scorecard_snapshots')
    assert (original.child_columns, original.parent_columns, original.kind) == (('source_snapshot_id',), ('snapshot_id',), 'unmeasured')
    # Business keys and the pre-existing current-entity route retain their grain.
    assert IDENTITIES[source] == expected_identity
    assert any(j.child == source and j.parent == entity and j.child_columns == ('scorecard_id', business_key)
               and j.parent_columns == ('scorecard_id', business_key) for j in JOINS)
    assert not any(j.child == source and j.parent == 'scorecard_snapshots' for j in JOINS)


@pytest.mark.parametrize('source', ['scorecard_member_links', 'scorecard_item_links'])
@pytest.mark.parametrize(('table', 'field'), [
    ('source', 'source_snapshot_id'), ('source', 'scorecard_id'),
    ('target', 'snapshot_id'), ('target', 'scorecard_id'),
])
def test_old_main_schema_cannot_borrow_snapshot_context_from_receipts(source, table, field):
    spec = next(s for s in declarations(RETIRED_PROCESSING_JOINS)
                if s['id'] == 'recorded_snapshot_' + source + '_scorecard_snapshots')
    schemas = {source: [('source_snapshot_id', 'VARCHAR'), ('scorecard_id', 'VARCHAR')],
               'scorecard_snapshots': [('snapshot_id', 'VARCHAR'), ('scorecard_id', 'VARCHAR')]}
    owner = source if table == 'source' else 'scorecard_snapshots'
    schemas[owner] = [c for c in schemas[owner] if c[0] != field]
    spec = deepcopy(spec)
    spec['receiptFields'] = ['source_snapshot_id', 'snapshot_id', 'scorecard_id']
    target = published_navigation([spec], schemas)[0]['targets'][0]
    assert not target['directions']['forward']['available']
    assert not target['directions']['reverse']['available']
    assert not target['completeKey']
