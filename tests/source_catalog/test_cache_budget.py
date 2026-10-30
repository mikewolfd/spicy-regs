"""Catalog readers and writers share one bounded SQLite page-cache policy."""
import pytest

from spicy_regs.source_catalog.storage import connect, schema


def test_readers_and_writers_use_the_same_budget(tmp_path):
    path = tmp_path / 'catalog.sqlite'
    with connect(path) as db:
        schema(db)
        assert db.execute('PRAGMA cache_size').fetchone()[0] == -256 * 1024
        db.execute("INSERT INTO metadata VALUES('test','123')")
    with connect(path, readonly=True, scope='all') as db:
        assert db.execute('PRAGMA cache_size').fetchone()[0] == -256 * 1024
        assert db.execute("SELECT value FROM metadata WHERE key='test'").fetchone()[0] == '123'
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    with connect(path, readonly=True, scope='all', cache_mib=2) as db:
        assert db.execute('PRAGMA cache_size').fetchone()[0] == -2 * 1024


@pytest.mark.parametrize('budget', [0, -1, True, '256', 1.5])
def test_invalid_budget_does_not_create_a_database(tmp_path, budget):
    path = tmp_path / 'absent.sqlite'
    with pytest.raises(ValueError, match='positive integer'):
        connect(path, cache_mib=budget)
    assert not path.exists()
