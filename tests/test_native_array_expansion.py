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
        assert con.execute('SELECT count(*) FROM occurrences').fetchone() == (4,)
        assert con.execute('SELECT count(*) FROM ' + table).fetchone() == (3,)
        planned = con.execute('EXPLAIN SELECT * FROM pairs').fetchone()
        assert planned is not None
        plan = planned[1]
        assert 'DELIM_JOIN' not in plan


def test_json_array_expansion_keeps_json_each_values_without_delimiter_join():
    from spicy_regs.relationship_views.core import ArrayRelationship, _selects

    spec = ArrayRelationship('references', 'records', ('record_id',), 'refs', 'example',
                             'CAST(e.value AS VARCHAR)', 'e.type IS NOT NULL', 'Fixture references.')
    values = [None, 'broken', 'null', '{}', '[]',
              '[null,"a","a",true,false,0,-2,1.20,1e3,{"id":"x"},[1,2]]',
              '[  { "id" : "x" }, "escaped\\nvalue" ]']
    with duckdb.connect() as con:
        con.execute('CREATE TABLE records(record_id VARCHAR, refs VARCHAR)')
        for ordinal, value in enumerate(values):
            con.execute('INSERT INTO records VALUES(?,?)', [str(ordinal), value])
        occurrences, pairs, states = _selects(spec, None)
        con.execute('CREATE VIEW references_occurrences AS ' + occurrences)
        con.execute('CREATE VIEW references_pairs AS ' + pairs)
        con.execute('CREATE VIEW references_states AS ' + states)
        expected = con.execute("""SELECT s.record_id, CAST(e.key AS BIGINT), CAST(e.value AS VARCHAR),
                                      '/refs/' || e.key,
                                      CASE WHEN e.type='NULL' THEN 'null_element' ELSE 'valid' END
                                 FROM records s,json_each(CASE WHEN json_type(try_cast(refs AS JSON))='ARRAY'
                                      THEN try_cast(refs AS JSON) ELSE '[]'::JSON END) e
                                 ORDER BY s.record_id, CAST(e.key AS BIGINT)""").fetchall()
        actual = con.execute('SELECT record_id,source_ordinal,raw_value_json,source_pointer,parsing_status '
                             'FROM references_occurrences ORDER BY record_id,source_ordinal').fetchall()
        assert actual == expected
        assert dict(con.execute('SELECT record_id,field_state FROM references_states').fetchall()) == {
            '0': 'sql_null', '1': 'malformed_json', '2': 'json_null', '3': 'unsupported_shape',
            '4': 'empty_array', '5': 'populated_array', '6': 'populated_array'}
        for name in ('references_occurrences', 'references_pairs'):
            planned = con.execute('EXPLAIN SELECT * FROM ' + name).fetchone()
            assert planned is not None
            assert 'DELIM_JOIN' not in planned[1]


def test_lobbying_native_expansion_preserves_duplicate_and_null_positions():
    from spicy_regs.relationship_views.lobbying_native import contacted_entities
    with duckdb.connect() as con:
        con.execute('CREATE TABLE lobbying_activities(filing_uuid VARCHAR,activity_index BIGINT,'
                    'government_entities STRUCT(id VARCHAR,name VARCHAR)[])')
        con.execute('INSERT INTO lobbying_activities VALUES(?,?,?)', ['filing', 0, [
            {'id': 'entity', 'name': 'Name'}, {'id': 'entity', 'name': 'Name'}, None, {'id': None, 'name': 'Unknown'}]])
        con.execute('INSERT INTO lobbying_activities VALUES(?,?,?)', ['empty', 0, []])
        con.execute('CREATE VIEW candidate AS ' + contacted_entities({}))
        assert con.execute('SELECT filing_uuid,activity_index,source_ordinal,target_key,native_label '
                           'FROM candidate ORDER BY source_ordinal').fetchall() == [
            ('filing', 0, 0, 'entity', 'Name'), ('filing', 0, 1, 'entity', 'Name'),
            ('filing', 0, 2, None, None), ('filing', 0, 3, None, 'Unknown')]
        plan = con.execute('EXPLAIN SELECT * FROM candidate').fetchone()
        assert plan is not None and 'DELIM_JOIN' not in plan[1]
