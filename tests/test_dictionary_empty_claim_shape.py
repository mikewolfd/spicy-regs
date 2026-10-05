"""An empty-column claim applies to both declared shapes and still rejects changed data."""
import duckdb
import pytest

from tests.test_chaos_r4_dictionary_live import _empty_condition


@pytest.mark.parametrize("native", [False, True])
def test_empty_authorities_claim_uses_the_maintained_published_field(native):
    with duckdb.connect() as con:
        name, kind, empty, populated = ("authority_refs", "VARCHAR[]", "[]", "['5 USC 1']") if native else (
            "authority_refs_json", "VARCHAR", "'[]'", "'[\"5 USC 1\"]'")
        con.execute(f"CREATE TABLE sample ({name} {kind})")
        con.execute(f"INSERT INTO sample VALUES ({empty})")
        condition = _empty_condition(con, "sample", "proceedings", "authority_refs", "`[]`")
        assert con.execute(f"SELECT count(*) FROM sample WHERE {condition}").fetchone() == (0,)
        con.execute(f"INSERT INTO sample VALUES ({populated}), (NULL)")
        assert con.execute(f"SELECT count(*) FROM sample WHERE {condition}").fetchone() == (2,)


def test_missing_claim_column_without_a_declared_mapping_refuses():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE sample (guessed_authorities VARCHAR)")
        with pytest.raises(ValueError, match="no published column or declared source field"):
            _empty_condition(con, "sample", "proceedings", "authority_refs", "`[]`")
