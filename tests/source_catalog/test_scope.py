"""Current-use selection preserves searchable custody and shared originals."""

import json

import pytest

from spicy_regs.source_catalog.scope import install_scope
from spicy_regs.source_catalog.storage import connect, schema


@pytest.fixture
def scoped(tmp_path):
    path = tmp_path / "inventory.sqlite"
    db = connect(path)
    schema(db)
    db.execute("CREATE TABLE raw_periods(file_id TEXT, kind TEXT, label TEXT, basis TEXT)")
    for cid in ("current", "unused", "new-unreviewed"):
        db.execute("INSERT INTO collections VALUES(?,?,?)", (cid, cid, "{}"))
    shared = "sha256:" + "a" * 64
    refused = "sha256:" + "b" * 64
    for sha in (shared, refused):
        db.execute("INSERT INTO objects VALUES(?,10)", (sha,))
    for ident, cid, status, sha in [
        ("current-source", "current", "held", shared),
        ("unused-alias", "unused", "held", shared),
        ("failed", "current", "refused-source", shared),
        ("search-page", "current", "held", refused),
        ("new-source", "new-unreviewed", "held", shared),
        ("schema", "current", "supporting-evidence", shared),
    ]:
        db.execute("INSERT INTO files VALUES(?,?,NULL,?,?,?,10,?,?)",
                   (ident, cid, ident, status, sha, "raw/" + ident, "{}"))
        db.execute("INSERT INTO raw_periods VALUES(?,'year','2026','native')", (ident,))
    db.execute("INSERT INTO tables VALUES('published','current','current','{}')")
    policy = {
        "format": "spicy-source-scope", "version": 1,
        "collections": [
            {"id": "current", "disposition": "active", "reason": "Current published consumer."},
            {"id": "unused", "disposition": "retained", "reason": "Saved for possible future use."},
        ],
        "excluded_objects": [{"sha256": refused, "reason": "Search page, not the advertised data."}],
    }
    install_scope(db, policy)
    db.commit()
    db.close()
    return path, policy


def test_default_active_and_retained_are_disjoint_and_complete(scoped):
    path, _ = scoped
    sets = {}
    for scope in ("active", "retained", "all"):
        with connect(path, readonly=True, scope=scope) as db:
            sets[scope] = {row[0] for row in db.execute("SELECT id FROM files")}
            periods = {row[0] for row in db.execute("SELECT file_id FROM raw_periods")}
            assert periods == sets[scope]
    assert sets["active"] == {"current-source"}
    assert sets["retained"] == {"unused-alias", "failed", "search-page", "new-source", "schema"}
    assert not sets["active"] & sets["retained"]
    assert sets["active"] | sets["retained"] == sets["all"]
    with connect(path, readonly=True) as db:
        assert [row[0] for row in db.execute("SELECT id FROM collections")] == ["current"]
        assert db.execute("SELECT count(*) FROM main.files").fetchone()[0] == 6
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_scope_cannot_hide_a_current_publication(scoped):
    path, policy = scoped
    with connect(path) as db:
        policy["collections"][0]["disposition"] = "retained"
        before = list(db.execute("SELECT * FROM catalog_scope_collections"))
        with pytest.raises(ValueError, match="omits a published collection"):
            install_scope(db, policy)
        assert list(db.execute("SELECT * FROM catalog_scope_collections")) == before


def test_bad_exclusions_do_not_replace_previous_selection(scoped):
    path, policy = scoped
    with connect(path) as db:
        before = db.execute("SELECT value FROM metadata WHERE key='source_scope'").fetchone()[0]
        policy["excluded_objects"] *= 2
        with pytest.raises(ValueError, match="Duplicate excluded"):
            install_scope(db, policy)
        assert db.execute("SELECT value FROM metadata WHERE key='source_scope'").fetchone()[0] == before
        assert json.loads(before)["collections"][0]["disposition"] == "active"


def test_legacy_catalog_has_no_invented_retained_scope(tmp_path):
    path = tmp_path / "legacy.sqlite"
    db = connect(path)
    schema(db)
    db.close()
    with connect(path, readonly=True) as db:
        assert db.execute("SELECT count(*) FROM collections").fetchone()[0] == 0
    with pytest.raises(ValueError, match="no reviewed source scope"):
        connect(path, readonly=True, scope="retained")


def test_scoped_exclusion_preserves_valid_uses_of_same_original(scoped):
    path, policy = scoped
    policy["version"] = 2
    policy["collections"][1]["disposition"] = "active"
    policy["collections"].append({
        "id": "new-unreviewed", "disposition": "active", "reason": "Reviewed shared use.",
    })
    policy["excluded_objects"].append({
        "sha256": "sha256:" + "a" * 64,
        "collection_id": "unused",
        "reason": "Native document identifies a different dataset; correction receipt retained.",
    })
    with connect(path, scope="all") as db:
        before = [tuple(row) for row in db.execute("SELECT * FROM files ORDER BY id")]
        install_scope(db, policy)
        db.commit()
    selected = {}
    for scope in ("active", "retained", "all"):
        with connect(path, readonly=True, scope=scope) as db:
            selected[scope] = {row[0] for row in db.execute("SELECT id FROM files")}
            assert {row[0] for row in db.execute("SELECT file_id FROM raw_periods")} == selected[scope]
    assert selected["active"] == {"current-source", "new-source"}
    assert "unused-alias" in selected["retained"]
    assert selected["active"].isdisjoint(selected["retained"])
    assert selected["active"] | selected["retained"] == selected["all"]
    with connect(path, readonly=True, scope="all") as db:
        assert [tuple(row) for row in db.execute("SELECT * FROM files ORDER BY id")] == before


@pytest.mark.parametrize("collection_id", [None, "", 7, "not-declared"])
def test_invalid_scoped_exclusion_preserves_policy(scoped, collection_id):
    path, policy = scoped
    policy["version"] = 2
    policy["excluded_objects"][0]["collection_id"] = collection_id
    with connect(path, scope="all") as db:
        before = [tuple(row) for row in db.execute("SELECT * FROM catalog_scope_exclusions")]
        with pytest.raises(ValueError, match="Invalid excluded source collection"):
            install_scope(db, policy)
        assert [tuple(row) for row in db.execute("SELECT * FROM catalog_scope_exclusions")] == before


def test_scoped_exclusion_requires_explicit_policy_version(scoped):
    path, policy = scoped
    policy["excluded_objects"][0]["collection_id"] = "current"
    with connect(path, scope="all") as db:
        with pytest.raises(ValueError, match="Invalid excluded source collection"):
            install_scope(db, policy)


def test_existing_two_column_scope_retains_hash_wide_behavior(scoped):
    path, policy = scoped
    with connect(path, scope="all") as db:
        db.execute("DROP TABLE catalog_scope_exclusions")
        db.execute("CREATE TABLE catalog_scope_exclusions(sha256 TEXT PRIMARY KEY, reason TEXT NOT NULL)")
        db.execute("INSERT INTO catalog_scope_exclusions VALUES(?,?)", (
            policy["excluded_objects"][0]["sha256"], policy["excluded_objects"][0]["reason"],
        ))
        db.commit()
    with connect(path, readonly=True) as db:
        assert {row[0] for row in db.execute("SELECT id FROM files")} == {"current-source"}
    with connect(path, scope="all") as db:
        install_scope(db, policy)
        db.commit()
    with connect(path, readonly=True) as db:
        assert {row[0] for row in db.execute("SELECT id FROM files")} == {"current-source"}


def test_unclassified_retained_source_is_searchable_without_active_use(scoped):
    path, _ = scoped
    with connect(path, scope="all") as db:
        db.execute("UPDATE main.files SET status='unclassified-source' WHERE id='current-source'")
        db.commit()
    with connect(path, readonly=True, scope="active") as db:
        assert db.execute("SELECT id FROM files WHERE id='current-source'").fetchone() is None
    with connect(path, readonly=True, scope="retained") as db:
        assert db.execute("SELECT status FROM files WHERE id='current-source'").fetchone()[0] == "unclassified-source"
