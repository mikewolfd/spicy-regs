"""Court endpoint joins retain all outcomes without quadratic equality plans."""
import re

import duckdb
import pytest

from spicy_regs.relationship_views.courts import COURT_VIEWS


@pytest.mark.parametrize('spec', COURT_VIEWS, ids=lambda spec: spec.name)
def test_guarded_endpoint_equality_preserves_full_rows_and_uses_hash_join(spec):
    with duckdb.connect(config={'threads': 1, 'memory_limit': '128MiB'}) as con:
        source = next(iter(spec.required))
        for table, columns in spec.required.items():
            con.execute('CREATE TABLE ' + table + '(' + ','.join(c + ' VARCHAR' for c in columns) + ')')
            values = ['1', '2', '3', '001', 'bad', None, '1'] if table == source else ['1', '3', '3']
            for value in values:
                con.execute('INSERT INTO ' + table + ' VALUES(' + ','.join('?' for _ in columns) + ')',
                            [value] * len(columns))
        candidate = spec.query({})
        baseline = re.sub(r"ON CASE WHEN (regexp_full_match\(s\.([a-z_]+), '\[1-9\]\[0-9\]\*'\)) THEN s\.\2 ELSE NULL END=t\.([a-z_]+)",
                          r'ON \1 AND s.\2=t.\3', candidate)
        assert baseline != candidate
        con.execute('CREATE VIEW baseline AS ' + baseline)
        con.execute('CREATE VIEW candidate AS ' + candidate)
        assert con.execute('''SELECT count(*) FROM (
            (SELECT * FROM baseline EXCEPT ALL SELECT * FROM candidate)
            UNION ALL (SELECT * FROM candidate EXCEPT ALL SELECT * FROM baseline))''').fetchone()[0] == 0
        roles = 2 if spec.name in ('court_opinion_citation_endpoints', 'court_parenthetical_endpoints') else 1
        assert dict(con.execute('SELECT target_status,count(*) FROM candidate GROUP BY 1').fetchall()) == {
            'found': 2 * roles, 'ambiguous': roles, 'missing': roles, 'unsupported': 3 * roles}
        plan = con.execute('EXPLAIN SELECT * FROM candidate').fetchone()[1]
        assert 'HASH_JOIN' in plan
        assert 'BLOCKWISE_NL_JOIN' not in plan
