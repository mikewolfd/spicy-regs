"""Global export totals count held objects once across eligible file aliases."""

import json

import pytest

from spicy_regs.source_catalog import export
from spicy_regs.source_catalog.storage import connect, schema


@pytest.mark.parametrize("eligible", [False, True])
def test_global_object_totals_preserve_alias_null_and_value_semantics(tmp_path, eligible):
    index = tmp_path / "index.sqlite"
    db = connect(index)
    schema(db)
    db.executemany(
        "INSERT INTO metadata VALUES (?,?)",
        [("generation", json.dumps("sha256:" + "a" * 64)), ("generatedAt", json.dumps("2026-01-01"))],
    )
    db.executemany("INSERT INTO collections VALUES (?,?,?)", [("a", "A", "{}"), ("b", "B", "{}")])
    db.executemany(
        "INSERT INTO objects VALUES (?,?)",
        [("shared", 11), ("zero", 0), ("large", 2**40), ("orphan", 99), (None, 88)]
        + [(status, 77) for status in ("missing-bytes", "receipt-only", "known-not-acquired", "external-by-choice")],
    )
    aliases = [
        ("a", "shared", "missing-bytes", 900),
        ("b", None, "verified", 1000),
        *[("a", status, status, 77) for status in
          ("missing-bytes", "receipt-only", "known-not-acquired", "external-by-choice")],
    ]
    if eligible:
        aliases += [
            ("a", "shared", "verified", 900),
            ("a", "shared", "verified", None),
            ("b", "shared", "held-not-integrated", 900),
            ("a", "zero", "verified", 1),
            ("b", "large", "verified", 1),
        ]
    db.executemany(
        "INSERT INTO files(id,collection_id,name,status,sha256,bytes,data) VALUES (?,?,?,?,?,?,?)",
        [(str(i), cid, "alias-" + str(i), status, sha, size, "{}")
         for i, (cid, sha, status, size) in enumerate(aliases)],
    )
    db.commit()
    assert not db.execute("PRAGMA foreign_key_check").fetchall()
    db.close()

    output = tmp_path / "public"
    manifest = export(index, output, page_size=1)
    assert manifest["totals"]["objects"] == (3 if eligible else 0)
    assert manifest["totals"]["decodedBytes"] == (11 + 2**40 if eligible else 0)
    assert manifest["totals"]["aliases"] == len(aliases)
    assert json.loads((output / "manifest.json").read_text())["totals"] == manifest["totals"]
