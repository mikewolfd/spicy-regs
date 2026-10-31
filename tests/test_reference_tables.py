"""RefSpec's vendored U.S. Code tables: pinned bytes, documented pins, and SQL views a locked connection can read."""

import hashlib
import re
import shutil
from importlib.resources import files

import duckdb
import pytest

from spicy_regs.relationship_views import reference
from spicy_regs.relationship_views.reference import REFERENCE_TABLES, REFERENCE_VIEWS, reference_paths
from spicy_regs.relationship_views.sql_views import install_sql_views

ROOT = files("spicy_regs").joinpath("reference/refspec")


def _readme_rows() -> dict[str, tuple[str, int, int, str]]:
    rows = re.findall(r"^\| `(\w+)` \| `([^`]+)` \| ([\d,]+) \| ([\d,]+) \| `([0-9a-f]{64})` \|$",
                      ROOT.joinpath("README.md").read_text(), re.MULTILINE)
    return {name: (path, int(size.replace(",", "")), int(count.replace(",", "")), sha)
            for name, path, size, count, sha in rows}


def test_bytes_pins_and_readme_agree():
    documented = _readme_rows()
    assert set(documented) == set(REFERENCE_TABLES)
    for name, (relative, sha256, _columns) in REFERENCE_TABLES.items():
        body = ROOT.joinpath(relative).read_bytes()
        assert documented[name][0] == relative and documented[name][3] == sha256
        assert (len(body), hashlib.sha256(body).hexdigest()) == (documented[name][1], sha256)


def test_each_view_reads_its_rows_through_a_locked_connection():
    with duckdb.connect() as con:
        installed = install_sql_views(con, (), REFERENCE_VIEWS)
        assert {name: entry["status"] for name, entry in installed.items()} == dict.fromkeys(REFERENCE_TABLES,
                                                                                             "available")
        con.execute("SET allowed_paths=?", [list(reference_paths())])
        con.execute("SET enable_external_access=false")
        counts = {name: con.execute(f'SELECT count(*) FROM "{name}"').fetchall()[0][0] for name in REFERENCE_TABLES}
    assert counts == {name: row[2] for name, row in _readme_rows().items()}


def test_other_bytes_are_refused(tmp_path, monkeypatch):
    shutil.copytree(str(ROOT.joinpath("usc-section-oracle")), tmp_path / "usc-section-oracle")
    shutil.copytree(str(ROOT.joinpath("usc-act-index")), tmp_path / "usc-act-index")
    (tmp_path / "usc-act-index/usc-act-sections.parquet").write_bytes(b"PAR1 tampered")
    monkeypatch.setattr(reference, "_path", lambda relative: str(tmp_path / relative))
    reference_paths.cache_clear()
    try:
        with pytest.raises(ValueError, match="usc_act_sections: vendored RefSpec bytes differ"):
            reference_paths()
    finally:
        reference_paths.cache_clear()
