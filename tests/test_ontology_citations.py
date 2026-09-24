"""The identifier readers the rulemaking tables use.

The CFR reader takes only the Federal Register's structured objects; a string is
no citation, so no phantom title or part can be read from prose. The RIN key and
the Regulations.gov identifier stay syntax-only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from spicy_regs.ontology.citations import (
    CfrCitation,
    canonical_cfr_iri,
    normalize_regsgov_identifier,
    normalize_rin,
    parse_cfr_citation,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"title": 40, "part": 60}, [CfrCitation("40", "60")]),
        ({"title": "40", "part": "60", "section": "1"}, [CfrCitation("40", "60", "1")]),
        ({"title": "040", "part": " 60 ", "section": "5375A(a)(1)"}, [CfrCitation("40", "60", "5375a")]),
        ({"title": 40}, []),
        ({"title": "forty", "part": 60}, []),
        # Strings are prose, and prose is no longer read here: the Register states
        # every reference as an object, and the removed grammar minted citations from
        # years and street numbers ("11555 Rockville Pike").
        ("40 CFR 60", []),
        ("40-60.1", []),
        (None, []),
    ],
)
def test_parse_cfr_reads_only_the_registers_objects(raw, expected):
    assert parse_cfr_citation(raw) == expected


def test_a_cfr_citation_spells_its_compact_key():
    assert CfrCitation("40", "60").cfr_ref == "40-60"
    assert CfrCitation("40", "60", "5375a").cfr_ref == "40-60.5375a"


def test_rulespec_cfr_identifier_expansion():
    assert canonical_cfr_iri("40", "60") == "urn:rkaf:us:cfr:40:60"
    assert canonical_cfr_iri("40", "60", "1") == "urn:rkaf:us:cfr:40:60.1"
    assert canonical_cfr_iri("40", "60", "5375A(a)(1)") == "urn:rkaf:us:cfr:40:60.5375a"


@pytest.mark.parametrize(
    ("title", "part", "section"),
    [("40", "60", "appendix-a"), ("", "60", None), ("40", "sixty", None)],
)
def test_invalid_cfr_components_are_rejected(title, part, section):
    with pytest.raises(ValueError):
        canonical_cfr_iri(title, part, section)


def test_regulations_gov_identifier_normalization_matches_repaired_grammar():
    assert normalize_regsgov_identifier(" epa-hq-oar-2021-0317 ") == "EPA-HQ-OAR-2021-0317"
    assert normalize_regsgov_identifier("epa_frdoc_0001") == "EPA_FRDOC_0001"
    assert normalize_regsgov_identifier("EPA") == "EPA"
    assert normalize_regsgov_identifier("Sequence No. 1") is None
    assert normalize_regsgov_identifier("Docket No. SSA-2010-0037") is None
    assert normalize_regsgov_identifier(None) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2060-AV16", "2060-AV16"),
        ("2060-av16", "2060-AV16"),
        ("  2060-av16  ", "2060-AV16"),
        ("2060AV16", None),
        ("2060-A16", None),
        ("2060-AV1", None),
        ("RIN 2060-AV16", None),
        # The wider detection shapes admit only damage and placeholders as keys.
        ("1625-AAOO", None),
        ("2060-XXXX", None),
        ("0648-A110", None),
        ("", None),
        (None, None),
    ],
)
def test_rin_normalization(value, expected):
    assert normalize_rin(value) == expected


def test_the_rin_grammar_has_one_definition():
    """A grammar copied into five modules drifts; a grammar imported cannot.

    The RIN shape was restated in four transforms and one discovery tool, each
    beside its own private ``_rin`` wrapper, so a correction to any one of them
    left the other four saying something different about the same identifier.
    """
    restating = sorted(
        str(path.relative_to(REPO_ROOT))
        for directory in ("src", "tools")
        for path in (REPO_ROOT / directory).rglob("*.py")
        if r"\d{4}-[A-Z]{2}\d{2}" in path.read_text()
    )

    assert restating == ["src/spicy_regs/ontology/citations.py"]
