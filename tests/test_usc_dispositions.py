"""Tests for vendored U.S. Code disposition tables across all 8 recodification families / 9 tables (ss-iorw)."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from spicy_regs.ontology.usc_dispositions import (
    DISPOSITION_RULE,
    NOT_IN_TABLE_CAVEATS,
    RECODIFICATIONS,
    RECODIFICATIONS_NOT_PINNED,
    STATUSES,
    VENDORED_DISPOSITION_DIR,
    VERDICTS,
    Disposition,
    DispositionRow,
    Successor,
    UscDispositionTables,
    load_disposition_tables,
)

README_PATH = Path(__file__).resolve().parents[1] / "src/spicy_regs/reference/refspec/README.md"


def test_all_nine_vendored_tables_match_pins_and_readme() -> None:
    assert DISPOSITION_RULE == "usc-disposition-tables/2"
    assert len(RECODIFICATIONS) == 9
    assert RECODIFICATIONS_NOT_PINNED == {}
    readme = README_PATH.read_text(encoding="utf-8")
    for rec in RECODIFICATIONS:
        path = VENDORED_DISPOSITION_DIR / rec.table
        assert path.is_file(), f"missing vendored table {rec.table}"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert f"sha256:{digest}" == rec.digest
        assert digest in readme
        assert rec.table in readme


def test_tampered_disposition_table_refuses_loudly(tmp_path: Path) -> None:
    shutil.copytree(VENDORED_DISPOSITION_DIR, tmp_path / "tables")
    (tmp_path / "tables" / "usc-2017-title34-disposition.parquet").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="pinned disposition table drifted: usc-2017-title34-disposition.parquet"):
        UscDispositionTables.from_directory(tmp_path / "tables")


def test_duplicate_recodification_name_refuses() -> None:
    dup = (RECODIFICATIONS[0], RECODIFICATIONS[0])
    with pytest.raises(ValueError, match="duplicate recodification name"):
        UscDispositionTables.from_directory(VENDORED_DISPOSITION_DIR, recodifications=dup)


def test_disposition_invariants() -> None:
    with pytest.raises(ValueError, match="undeclared verdict"):
        Disposition(49, "1432", (), "bogus", "title-49-1994", (), (), None)
    with pytest.raises(ValueError, match="absence from a table"):
        Disposition(49, "99999", (), "not-in-table", "title-49-1994", (), (), None)
    with pytest.raises(ValueError, match="successor cannot carry"):
        Successor(49, "101", None, "repealed")
    with pytest.raises(ValueError, match="names a successor exactly when"):
        DispositionRow("1", None, None, None, "restated", "1", "101", 1, "left")
    assert set(STATUSES) == {"restated", "restated-as-note", "repealed", "eliminated", "see-reference"}
    assert set(VERDICTS) == {
        "exists-as-recodified",
        "repealed-no-successor",
        "eliminated-no-successor",
        "stated-without-successor",
        "not-in-table",
        "no-table-for-title",
    }


def test_title_10_ropma_specimens() -> None:
    tables = load_disposition_tables()
    s593 = tables.disposition(10, "593")
    assert s593.verdict == "exists-as-recodified"
    assert s593.recodification == "title-10-1994"
    assert [(s.title, s.section) for s in s593.successors] == [(10, "12203")]

    assert [(s.title, s.section) for s in tables.disposition(10, "261", "a").successors] == [(10, "10101")]
    assert [(s.title, s.section) for s in tables.disposition(10, "261", "b").successors] == [(10, "10213")]
    assert {s.section for s in tables.disposition(10, "1004").successors} == {"12644", "10206"}
    assert tables.disposition(10, "1003").verdict == "repealed-no-successor"
    assert tables.disposition(10, "3359").verdict == "stated-without-successor"
    assert [(s.title, s.section, s.subsection) for s in tables.disposition(10, "3385").successors] == [
        (10, "14308", "(f)")
    ]
    missing = tables.disposition(10, "3040")
    assert missing.verdict == "not-in-table"
    assert missing.caveats == NOT_IN_TABLE_CAVEATS
    assert tables.disposition(10, "593", appendix=True).verdict == "no-table-for-title"


def test_title_31_and_title_41_specimens() -> None:
    tables = load_disposition_tables()
    t31 = tables.disposition(31, "483a")
    assert t31.recodification == "title-31-1982"
    assert [(s.title, s.section) for s in t31.successors] == [(31, "9701")]
    assert tables.disposition(31, "483a", appendix=True).verdict == "no-table-for-title"

    t41_46 = tables.disposition(41, "46", "a")
    assert t41_46.recodification == "title-41-2011"
    assert [(s.title, s.section) for s in t41_46.successors] == [(41, "8502")]
    assert t41_46.caveats == ("olrc_hrn_divergence:printed_41:8502_body_hrn_also_cites_41:8501(2)",)

    assert [(s.title, s.section) for s in tables.disposition(41, "351").successors] == [
        (41, "6702"),
        (41, "6703"),
        (41, "6704"),
    ]
    assert [(s.title, s.section) for s in tables.disposition(41, "351", "(a)(2)").successors] == [(41, "6703")]
    assert tables.disposition(41, "20a").verdict == "repealed-no-successor"


def test_title_34_editorial_reclassification_and_multi_former_title() -> None:
    tables = load_disposition_tables()
    meta = tables.recodification_metadata("title-34-2017")
    assert meta is not None and meta.is_editorial_reclassification is True
    assert meta.former_titles == (42, 18, 28)

    leaa = tables.disposition(42, "3711")
    assert leaa.recodification == "title-34-2017"
    assert [(s.title, s.section, s.status) for s in leaa.successors] == [
        (34, "10101", "restated"),
        (34, "10101", "restated-as-note"),
    ]
    assert tables.disposition(42, "3711", appendix=True).verdict == "no-table-for-title"
    assert tables.disposition(18, "922").recodification == "title-34-2017"
    assert tables.disposition(28, "509").recodification == "title-34-2017"
    assert tables.disposition(42, "3797dd-1").verdict == "eliminated-no-successor"

    typo = tables.disposition(42, "3797ee-1")
    assert [(s.title, s.section) for s in typo.successors] == [(34, "16902")]
    assert "olrc_front_matter_print_typo:printed_34:16902_body_codification_34:10692" in typo.caveats


def test_title_46_and_title_49_appendix_vs_positivelaw() -> None:
    tables = load_disposition_tables()
    t46_170 = tables.disposition(46, "170")
    assert [(s.title, s.section, s.subsection) for s in t46_170.successors] == [
        (46, "3306", "(a)(5)"),
        (46, "2106", None),
    ]
    shipping = tables.disposition(46, "1701", section_end="1705", appendix=True)
    assert shipping.covers == ("1701", "1702", "1703", "1704", "1705")
    assert {"40101", "40102", "40301"} <= {s.section for s in shipping.successors}
    assert [(s.title, s.section) for s in tables.disposition(46, "491", "a", appendix=True).successors] == [
        (46, "30102"),
        (46, "30103"),
    ]

    # Title 49 Appendix: 1-to-many successors preserved; subsection narrows; Rep./See-reference distinguished.
    s1432 = tables.disposition(49, "1432", appendix=True)
    assert len(s1432.successors) == 4
    assert {s.section for s in s1432.successors} == {"44701", "44702", "44706", "44914"}
    s1432_d = tables.disposition(49, "1432", "d", appendix=True)
    assert s1432_d.subsection_resolved is True
    assert [(s.title, s.section) for s in s1432_d.successors] == [(49, "44914")]

    s1604_h = tables.disposition(49, "1604", "h", appendix=True)
    assert s1604_h.verdict == "repealed-no-successor"
    assert s1604_h.subsection_resolved is True
    assert tables.disposition(49, "1432", "z", appendix=True).subsection_resolved is False
    assert tables.disposition(49, "1655", "(a)(4)", appendix=True).verdict == "stated-without-successor"

    # Title 49 positive-law companion resolves main-body 49:3102 -> 31502, and NEVER leaks on appendix=True!
    assert tables.disposition(49, "3102").recodification == "title-49-1994-positivelaw"
    assert [(s.title, s.section) for s in tables.disposition(49, "3102").successors] == [(49, "31502")]
    app_3102 = tables.disposition(49, "3102", appendix=True)
    assert app_3102.verdict == "not-in-table"
    assert app_3102.recodification == "title-49-1994"

    # Title 50 / 50 App. has no pinned table -> no-table-for-title.
    assert tables.disposition(50, "541", appendix=True).verdict == "no-table-for-title"
    assert tables.disposition(50, "541").verdict == "no-table-for-title"


def test_title_51_and_title_54_specimens() -> None:
    tables = load_disposition_tables()
    assert [(s.title, s.section) for s in tables.disposition(15, "5601").successors] == [(51, "60101")]
    assert [(s.title, s.section) for s in tables.disposition(42, "2451").successors] == [(51, "20102")]
    assert tables.disposition(42, "16654", "(a)(1)").verdict == "eliminated-no-successor"

    assert {s.section for s in tables.disposition(16, "1").successors} == {"100101", "100301", "100302"}
    assert [(s.title, s.section) for s in tables.disposition(16, "470f").successors] == [(54, "306108")]
    span_double = tables.disposition(16, "19aa", section_end="19gg")
    assert span_double.covers == ("19aa", "19bb", "19cc", "19dd", "19ee", "19ff", "19gg")
    assert tables.disposition(16, "1a-9", section_end="1a-11").covers == ("1a-9", "1a-10", "1a-11")


def test_usc_dispositions_loads_without_pyarrow() -> None:
    code = """
import importlib.abc, sys
class BlockPyArrow(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'pyarrow':
            raise AssertionError('pyarrow imported')
        return None
sys.meta_path.insert(0, BlockPyArrow())
from spicy_regs.ontology.usc_dispositions import load_disposition_tables
assert load_disposition_tables().disposition(49, '1432', 'd', appendix=True).successors[0].section == '44914'
"""
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stdout + res.stderr
