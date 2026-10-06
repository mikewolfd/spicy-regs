"""Native array navigation preserves repeated and unsupported source positions."""
import duckdb
import pytest

from spicy_regs.relationship_views.regulations_native import ARRAYS, native_regulations_views


@pytest.mark.parametrize('definition', ARRAYS, ids=lambda definition: definition[0])
def test_native_occurrences_keep_independent_ordinals_and_pair_keys(definition):
    name, table, keys, field, kind, _value = definition
    value = '1234-AB56' if kind == 'rin' else 'https://example.test/document' if kind == 'offered_url' else 'key'
    typ = 'VARCHAR[]'
    valid, invalid = value, ''
    if field in ('attachments', 'attachment_records', 'agencies'):
        child = 'url' if field == 'attachments' else 'attachment_id' if field == 'attachment_records' else 'id'
        typ = f'STRUCT({child} VARCHAR)[]'
        valid, invalid = {child: value}, {child: None}
    with duckdb.connect() as con:
        con.execute('CREATE TABLE ' + table + '(' + ','.join(k + ' VARCHAR' for k in keys) + ',' + field + ' ' + typ + ')')
        placeholders = ','.join('?' for _ in (*keys, field))
        for identity, values in [('populated', [valid, valid, None, invalid]), ('empty', []), ('null', None)]:
            con.execute('INSERT INTO ' + table + ' VALUES(' + placeholders + ')', [identity] * len(keys) + [values])
        specs = {spec.name: spec for spec in native_regulations_views([table])}
        occurrence = specs[name + '_occurrences']
        pairs = specs[name + '_pairs']
        con.execute('CREATE VIEW occurrences AS ' + occurrence.query({}))
        con.execute('CREATE VIEW pairs AS ' + pairs.query({}))
        assert con.execute('SELECT source_ordinal,target_key FROM occurrences ORDER BY source_ordinal').fetchall() == [
            (0, value), (1, value), (2, None), (3, None)]
        assert con.execute('SELECT target_key FROM pairs').fetchall() == [(value,)]
        assert con.execute('SELECT count(*) FROM occurrences').fetchone()[0] == 4
        assert con.execute('SELECT count(*) FROM ' + table).fetchone()[0] == 3
        plan = con.execute('EXPLAIN SELECT * FROM pairs').fetchone()[1]
        assert 'DELIM_JOIN' not in plan
