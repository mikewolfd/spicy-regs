"""Tests for composite enclosing source identity (ss-njjd) and CERCLA / Civil Rights Act resolution."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from spicy_regs.ontology.act_resolution import (
    ENCLOSING_SOURCE_IDENTITY_RULE,
    REVIEWED_ACT_ALIASES,
    VENDORED_ACT_INDEX_DIR,
    ActIndex,
    ActRelativeCitation,
    Classification,
    PopularNameRecord,
    SourceCreditIndex,
    act_resolution_pins,
    find_act_relative_occurrences,
    load_act_index,
    load_source_credit_index,
    resolve_act_relative_citation,
)

README_PATH = Path(__file__).resolve().parents[1] / "src/spicy_regs/reference/refspec/README.md"


def test_vendored_act_index_and_source_credits_match_pins_and_readme() -> None:
    assert ENCLOSING_SOURCE_IDENTITY_RULE == "composite-law-division-enclosing-section-v1"
    readme = README_PATH.read_text(encoding="utf-8")
    pins = act_resolution_pins()
    assert set(pins) == {
        "usc-popular-names.parquet",
        "usc-act-sections.parquet",
        "quarantine.parquet",
        "usc-source-credits.parquet",
    }
    for filename, expected_pin in pins.items():
        path = VENDORED_ACT_INDEX_DIR / filename
        assert path.is_file(), f"missing {filename}"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert f"sha256:{digest}" == expected_pin
        assert digest in readme
        assert filename in readme


def test_tampered_act_parquet_refuses_loudly(tmp_path: Path) -> None:
    shutil.copytree(VENDORED_ACT_INDEX_DIR, tmp_path / "act")
    credits_file = tmp_path / "act" / "usc-source-credits.parquet"
    credits_file.chmod(0o644)
    credits_file.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="pinned act artifact drifted: usc-source-credits.parquet"):
        SourceCreditIndex.from_directory(tmp_path / "act")

    pop_file = tmp_path / "act" / "usc-popular-names.parquet"
    pop_file.chmod(0o644)
    pop_file.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="pinned act artifact drifted: usc-popular-names.parquet"):
        ActIndex.from_directory(tmp_path / "act")


def test_small_business_act_enclosing_section_negative_control_and_alignment() -> None:
    """``85-536 § 2[3]``: bare § 3 MUST refuse (never ``12-371``); aligned ``(85-536, 2, 3)`` resolves to ``15-632``."""
    index = load_act_index()
    credits = load_source_credit_index()

    for phrase in ("section 3 of the Small Business Act", "Small Business Act section 3"):
        occs = find_act_relative_occurrences(phrase, act_names=index.act_names)
        assert len(occs) == 1, phrase
        resolution = resolve_act_relative_citation(occs[0].citation, index=index, source_credits=credits)
        assert resolution.iri is None
        assert resolution.unresolved_reason == "act_section_ambiguous"
        assert resolution.source_credit_status == "not_consulted"
        assert resolution.table3_key == "85-536"

    sec4 = resolve_act_relative_citation(
        ActRelativeCitation("Small Business Act", "small business act", "4"),
        index=index,
        source_credits=credits,
    )
    assert sec4.iri is None
    assert sec4.unresolved_reason == "act_section_ambiguous"

    aligned = resolve_act_relative_citation(
        ActRelativeCitation("Small Business Act", "small business act", "3", enclosing_section="2"),
        index=index,
        source_credits=credits,
    )
    assert aligned.iri == "urn:rkaf:us:usc:15:632"
    assert (aligned.usc_title, aligned.usc_section) == ("15", "632")
    assert aligned.enclosing_section == "2"
    assert aligned.answered_by == "table3"


def test_enclosing_source_identity_composite_join_and_conflict_refusal() -> None:
    sba_index = ActIndex(
        table3_key_by_name={"small business act": "85-536"},
        enclosing_section_by_name={"small business act": "2"},
        classifications={
            "85-536": {
                "2[2]": (Classification("15", "631", None, 384),),
                "2[3]": (Classification("15", "632", None, 384),),
                "3": (Classification("12", "371", None, 396),),
            }
        },
    )
    sba_credits = SourceCreditIndex.from_rows(
        [
            ("85-536", None, "2", "3", "15", "632", "72", "384"),
            ("85-536", "A", "3", "12", "371", "72", "396"),
        ]
    )
    resolved = resolve_act_relative_citation(
        ActRelativeCitation("Small Business Act", "small business act", "3"),
        index=sba_index,
        source_credits=sba_credits,
    )
    assert resolved.iri == "urn:rkaf:us:usc:15:632"
    assert resolved.answered_by == "both"
    assert resolved.enclosing_section == "2"
    assert (resolved.statutes_at_large_volume, resolved.statutes_at_large_page) == ("72", "384")

    conflict = resolve_act_relative_citation(
        ActRelativeCitation("Small Business Act", "small business act", "3", enclosing_section="5"),
        index=sba_index,
        source_credits=sba_credits,
    )
    assert conflict.iri is None
    assert conflict.unresolved_reason == "act_section_ambiguous"
    assert conflict.source_credit_status == "not_consulted"

    multi = ActIndex(
        table3_key_by_name={"example act": "85-536"},
        name_candidates={
            "example act": (
                PopularNameRecord(name="Example Act", content_type="cite", table3_key="85-536", enclosing_section="2"),
                PopularNameRecord(name="Example Act", content_type="cite", table3_key="85-536", enclosing_section="9"),
            )
        },
        classifications=sba_index.classifications,
    )
    ambig = resolve_act_relative_citation(
        ActRelativeCitation("Example Act", "example act", "3"), index=multi, source_credits=sba_credits
    )
    assert ambig.iri is None and ambig.unresolved_reason == "act_name_ambiguous"
    assert len(ambig.candidate_resolutions) == 2

    narrowed = resolve_act_relative_citation(
        ActRelativeCitation("Example Act", "example act", "3", enclosing_section="2"),
        index=multi,
        source_credits=sba_credits,
    )
    assert narrowed.iri == "urn:rkaf:us:usc:15:632"


def test_cercla_section_107a_resolves_to_42_9607() -> None:
    index = load_act_index()
    credits = load_source_credit_index()
    occs = find_act_relative_occurrences("section 107(a) of CERCLA", act_names=index.act_names)
    assert len(occs) == 1
    assert occs[0].pinpoint == ("a",)

    for cit in (occs[0].citation, ActRelativeCitation("CERCLA", "cercla", "107(a)")):
        res = resolve_act_relative_citation(cit, index=index, source_credits=credits)
        assert res.iri == "urn:rkaf:us:usc:42:9607"
        assert (res.usc_title, res.usc_section) == ("42", "9607")
        assert res.table3_key == "96-510"
        assert res.answered_by == "table3"

    assert "cercla" in REVIEWED_ACT_ALIASES
    assert (
        resolve_act_relative_citation(
            ActRelativeCitation("CERCLA", "cercla", "107(a)"), index=ActIndex()
        ).unresolved_reason
        == "act_not_in_index"
    )


def test_civil_rights_act_of_1964_resolves_and_bare_name_refuses() -> None:
    index = load_act_index()
    credits = load_source_credit_index()
    for phrase in ("section 706 of the Civil Rights Act of 1964", "Civil Rights Act of 1964 section 706"):
        occs = find_act_relative_occurrences(phrase, act_names=index.act_names)
        assert len(occs) == 1, phrase
        res = resolve_act_relative_citation(occs[0].citation, index=index, source_credits=credits)
        assert res.iri == "urn:rkaf:us:usc:42:2000e-5"
        assert (res.usc_title, res.usc_section) == ("42", "2000e-5")
        assert res.table3_key == "88-352"
        assert res.answered_by == "table3"

    assert find_act_relative_occurrences("Civil Rights Act section 706", act_names=index.act_names) == ()
    bare = resolve_act_relative_citation(
        ActRelativeCitation("Civil Rights Act", "civil rights act", "706"),
        index=index,
        source_credits=credits,
    )
    assert bare.iri is None
    assert bare.unresolved_reason == "act_not_in_index"


def test_act_resolution_loads_without_pyarrow() -> None:
    code = """
import importlib.abc, sys
class BlockPyArrow(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'pyarrow':
            raise AssertionError('pyarrow imported')
        return None
sys.meta_path.insert(0, BlockPyArrow())
from spicy_regs.ontology.act_resolution import ActRelativeCitation, load_act_index, load_source_credit_index, resolve_act_relative_citation
res = resolve_act_relative_citation(ActRelativeCitation('CERCLA', 'cercla', '107(a)'), index=load_act_index(), source_credits=load_source_credit_index())
assert res.iri == 'urn:rkaf:us:usc:42:9607'
"""
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stdout + res.stderr
