"""An empty-field audit states which published representation it actually checks."""
import duckdb
import pytest
from tests.test_chaos_r4_dictionary_live import _empty_claim_condition


@pytest.mark.parametrize('name,typ,empty,nonempty', [
    ('authority_refs', 'VARCHAR[]', '[]::VARCHAR[]', "['5 U.S.C. 301']"),
    ('authority_refs_json', 'VARCHAR', "'[]'", "'[\"5 U.S.C. 301\"]'"),
])
def test_proceedings_empty_claim_checks_native_or_literal_historical_field(name, typ, empty, nonempty):
    condition, observed = _empty_claim_condition('proceedings', 'authority_refs', '`[]`', {name: typ})
    assert observed == name
    with duckdb.connect() as con:
        con.execute(f'CREATE TABLE proceedings ("{name}" {typ})')
        con.execute(f'INSERT INTO proceedings VALUES ({empty}), ({nonempty}), (NULL)')
        assert con.execute(f'SELECT count(*) FROM proceedings WHERE {condition}').fetchone() == (2,)


def test_empty_claim_refuses_undeclared_missing_field():
    with pytest.raises(ValueError, match='neither the documented field'):
        _empty_claim_condition('proceedings', 'authority_refs', '`[]`', {'unrelated': 'VARCHAR'})
