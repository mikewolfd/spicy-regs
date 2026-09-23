"""Hermetic tests for the GovInfo CFR section ingest (no network).

Covers the pieces with real logic: the api.data.gov key resolution fallback
chain, the reader's keyless refusal + default edition window, the
raw-granule → published-schema mapping (``_shape``), which reads the granule
ID grammar + list-level fields (no per-granule ``/summary`` call), and the
placement of section granules under their annual volume's PART headings
(``place_sections``). No live network calls are made.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from loguru import logger

from spicy_regs.sources.cfr_sections import API_KEY_ENV_VARS, CfrSectionsError, CfrSectionsReader, _resolve_api_key
from spicy_regs.transforms.build_cfr_sections import COLUMNS, _cfr_ref, _shape, annual_volume, place_sections

ANCESTRY = Path(__file__).parent / "fixtures" / "cfr" / "ancestry"

# A CONTENT/section granule with real list-level fields + package stamps, using
# the real GovInfo ID grammar. Note: no cfrTitle/cfrPart/cfrSection fields exist
# on list-level granules — everything is parsed from the IDs.
_SECTION_GRANULE = {
    "granuleId": "CFR-2024-title40-vol1-sec1-1",
    "granuleClass": "CONTENT",
    "title": "Definitions.",
    "dateIssued": "2024-07-01",
    "granuleLink": "https://api.govinfo.gov/packages/CFR-2024-title40-vol1/granules/CFR-2024-title40-vol1-sec1-1/summary",
    "_package_id": "CFR-2024-title40-vol1",
    "_package_last_modified": "2026-07-16T20:57:39Z",
    "_package_title": "Protection of Environment",
}

# A part-level CONTENT granule (part token present, no section).
_PART_GRANULE = {
    "granuleId": "CFR-2024-title48-vol5-part700",
    "granuleClass": "CONTENT",
    "title": "Reserved",
    "_package_id": "CFR-2024-title48-vol5",
    "_package_last_modified": "2026-01-02T00:00:00Z",
}

# A chapter appendix granule (part token present via app parent, section absent).
_APPENDIX_GRANULE = {
    "granuleId": "CFR-2024-title48-vol1-part3-app1",
    "granuleClass": "APPENDIX",
    "title": "Cost Accounting Standards.",
    "_package_id": "CFR-2024-title48-vol1",
}

# A NODE granule (chapter) — no part, no section.
_NODE_GRANULE = {
    "granuleId": "CFR-2024-title48-vol5-chap7",
    "granuleClass": "NODE",
    "title": "AGENCY FOR INTERNATIONAL DEVELOPMENT",
    "_package_id": "CFR-2024-title48-vol5",
}


# -- API key resolution -------------------------------------------------------


def _clear_key_env(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_resolve_api_key_none_when_unset(monkeypatch):
    _clear_key_env(monkeypatch)
    assert _resolve_api_key() is None


def test_resolve_api_key_prefers_data_gov(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "primary")
    monkeypatch.setenv("GOVINFO_API_KEY", "secondary")
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "tertiary")
    assert _resolve_api_key() == "primary"


def test_resolve_api_key_fallback_chain(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "tertiary")
    assert _resolve_api_key() == "tertiary"
    monkeypatch.setenv("GOVINFO_API_KEY", "secondary")
    assert _resolve_api_key() == "secondary"


def test_resolve_api_key_ignores_blank(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "   ")
    monkeypatch.setenv("GOVINFO_API_KEY", "real")
    assert _resolve_api_key() == "real"


def test_reader_keyless_refuses(monkeypatch):
    """No configured credential must not become a successful empty selection."""
    _clear_key_env(monkeypatch)
    reader = CfrSectionsReader()
    with pytest.raises(CfrSectionsError, match="requires an api.data.gov key"):
        list(reader.iter_records())


def test_reader_default_edition_window():
    """Default window is last-year..this-year (CFR editions are annual)."""
    reader = CfrSectionsReader(api_key="x")
    this_year = dt.date.today().year
    assert reader.until_year == this_year
    assert reader.since_year == this_year - 1


# -- raw granule -> published schema mapping ----------------------------------


def test_shape_produces_exact_schema():
    row = _shape(_SECTION_GRANULE)
    assert set(row) == set(COLUMNS)


def test_shape_parses_section_granule():
    row = _shape(_SECTION_GRANULE)
    assert row["granule_id"] == "CFR-2024-title40-vol1-sec1-1"
    assert row["package_id"] == "CFR-2024-title40-vol1"
    # CFR title number parsed from the id, not the (heading) ``title`` field.
    assert row["title"] == "40"
    assert row["edition_year"] == "2024"
    # The list has no native ancestry; the token stays until the volume places it.
    assert row["part"] is None
    assert row["section"] == "1-1"
    assert row["cfr_ref"] is None
    assert row["heading"] == "Definitions."
    assert row["structure_level"] == "CONTENT"
    # last_modified comes from the enclosing package stamp.
    assert row["last_modified"] == "2026-07-16T20:57:39Z"
    # Canonical govinfo details URL built from package + granule ids.
    assert row["url"] == "https://www.govinfo.gov/app/details/CFR-2024-title40-vol1/CFR-2024-title40-vol1-sec1-1"


def test_shape_parses_part_granule():
    row = _shape(_PART_GRANULE)
    assert row["title"] == "48"
    assert row["part"] == "700"
    assert row["section"] is None
    assert row["cfr_ref"] == "48-700"
    assert row["structure_level"] == "CONTENT"
    assert row["edition_year"] == "2024"


@pytest.mark.parametrize(
    ("granule_id", "part", "cfr_ref"),
    [
        ("CFR-2026-title14-vol5-part1203a", "1203a", "14-1203a"),
        ("CFR-2026-title14-vol5-part1203b", "1203b", "14-1203b"),
        ("CFR-2026-title7-vol1-part15a-subpartA", "15a", "7-15a"),
        ("CFR-2026-title7-vol1-part15a-toc-id785", "15a", "7-15a"),
    ],
)
def test_shape_preserves_lettered_part_tokens(granule_id, part, cfr_ref):
    # Literal IDs from the 2026-09-21 pinned public cfr_sections table. The raw
    # table and full 336-row counterexample set are retained outside the repo:
    # receipts/data-validation-sprint-2026-09-21/cfr-fix-review.json.
    # Only the listed part token is under test; no source hierarchy is inferred.
    row = _shape({"granuleId": granule_id})
    assert row["part"] == part
    assert row["cfr_ref"] == cfr_ref
    assert row["section"] is None


def test_shape_parses_appendix_granule():
    row = _shape(_APPENDIX_GRANULE)
    assert row["title"] == "48"
    assert row["part"] == "3"
    assert row["section"] is None
    assert row["cfr_ref"] == "48-3"
    assert row["structure_level"] == "APPENDIX"


def test_shape_node_granule_has_no_part_or_section():
    row = _shape(_NODE_GRANULE)
    assert row["title"] == "48"
    assert row["part"] is None
    assert row["section"] is None
    # No part -> no cfr_ref.
    assert row["cfr_ref"] is None
    assert row["structure_level"] == "NODE"


def test_shape_edition_year_falls_back_to_date_issued():
    row = _shape({"granuleId": "weird-id", "dateIssued": "2019-03-04"})
    assert row["edition_year"] == "2019"


def test_shape_cfr_ref_full_citation():
    # A section granule that also carries a part token yields a full title-part.section ref.
    row = _shape(
        {
            "granuleId": "CFR-2024-title21-vol1-part1-sec5",
            "granuleClass": "SECTION",
            "_package_id": "CFR-2024-title21-vol1",
        }
    )
    assert row["title"] == "21"
    assert row["part"] == "1"
    assert row["section"] == "5"
    assert row["cfr_ref"] == "21-1.5"


def test_cfr_ref_degrades_gracefully():
    # Full citation, part-only, then null when title or part is missing.
    assert _cfr_ref("40", "60", "1") == "40-60.1"
    assert _cfr_ref("40", "60", None) == "40-60"
    assert _cfr_ref("40", None, None) is None
    assert _cfr_ref(None, None, None) is None


def test_shape_handles_missing_fields():
    row = _shape({"granuleId": "x"})
    assert row["granule_id"] == "x"
    assert row["package_id"] is None
    assert row["cfr_ref"] is None
    assert row["title"] is None
    assert row["part"] is None
    assert row["section"] is None
    assert row["heading"] is None
    # No package id -> no canonical url.
    assert row["url"] is None


# A section granule whose part carries a letter suffix (5 CFR 5b is a real part).
_LETTERED_PART_GRANULE = {
    "granuleId": "CFR-2025-title5-vol3-sec5b-11",
    "granuleClass": "CONTENT",
    "title": "Safeguarding personal information.",
    "_package_id": "CFR-2025-title5-vol3",
}

# A malformed publisher id: the word "Sec" repeats and no part is recoverable.
_MALFORMED_SECTION_GRANULE = {
    "granuleId": "CFR-2025-title14-vol4-secSec-1-1",
    "granuleClass": "CONTENT",
    "title": "Applicability.",
    "_package_id": "CFR-2025-title14-vol4",
}


def test_shape_requires_ancestry_even_for_a_lettered_section_prefix():
    row = _shape(_LETTERED_PART_GRANULE)
    assert row["part"] is None
    assert row["section"] == "5b-11"
    assert row["cfr_ref"] is None


def test_shape_leaves_a_malformed_section_id_unsplit():
    """The id yields no part, so part stays null until the volume places it (Part 241)."""
    row = _shape(_MALFORMED_SECTION_GRANULE)
    assert row["part"] is None
    assert row["section"] == "Sec-1-1"
    assert row["cfr_ref"] is None


# -- section placement from the annual volume XML -----------------------------


def _place(package_id: str, *suffixes: str) -> tuple[dict[str, dict], list[str]]:
    """Place ``{package_id}-{suffix}`` granules against the package's retained volume excerpt.

    Returns the placed rows keyed by suffix and the warnings logged while placing.
    """
    rows = [_shape({"granuleId": f"{package_id}-{suffix}", "_package_id": package_id}) for suffix in suffixes]
    volume = annual_volume(package_id)
    assert volume is not None
    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING", format="{message}")
    try:
        placed = place_sections(rows, (ANCESTRY / f"{package_id}.xml").read_bytes(), volume)
    finally:
        logger.remove(sink)
    return {row["granule_id"].removeprefix(f"{package_id}-"): row for row in placed}, warnings


@pytest.mark.parametrize(
    ("package_id", "suffix", "part", "section", "cfr_ref"),
    [
        # Title 43 numbers sections by subpart: § 1601.0-1 is in Part 1600 and
        # cites as printed (ruling 5), not as the nonexistent Part 1601.
        ("CFR-2025-title43-vol2", "sec1601-0-1", "1600", "1601.0-1", "43-1601.0-1"),
        # Title 41's compound part keeps its hyphen; the id's token splits it at 50.
        ("CFR-2025-title41-vol1", "sec50-201-1", "50-201", "1", "41-50-201.1"),
        # Title 14 Part 241 prints 19-8.1: part 241, never 19, and no citation.
        ("CFR-2025-title14-vol4", "sec19-8-1", "241", "19-8.1", None),
        # An unprefixed number in the same part has a part but no citation.
        ("CFR-2025-title14-vol4", "secSec-1-1", "241", "Sec.1-1", None),
        # Ranges (a §§ em-dash span and a single-§ hyphen span) cite nothing.
        ("CFR-2025-title41-vol1", "sec51-10-104-51-10-109", "51-10", "104—51-10.109", None),
        ("CFR-2025-title26-vol6", "sec1-404a-4-1-404a-7", "1", "404(a)-4-1.404(a)-7", None),
        # A parenthesized citation does not join the Federal Register key yet.
        ("CFR-2025-title26-vol6", "sec1-401k-1", "1", "401(k)-1", None),
        # A publisher typo (§ 206.253 under PART 1206) keeps the heading's part.
        ("CFR-2025-title30-vol3", "sec206-253", "1206", "206.253", None),
        # GovInfo's -id duplicate reaches the canonical copy of § 849.504.
        ("CFR-2025-title48-vol5", "sec849-504-id915", "849", "504", "48-849.504"),
        ("CFR-2025-title48-vol5", "sec849-504", "849", "504", "48-849.504"),
    ],
)
def test_section_granules_take_the_volume_part_heading(package_id, suffix, part, section, cfr_ref):
    placed, _warnings = _place(package_id, suffix)
    row = placed[suffix]
    assert (row["part"], row["section"], row["cfr_ref"]) == (part, section, cfr_ref)


def test_non_section_and_unscanned_granules_keep_the_identifier_path():
    placed, _warnings = _place(
        "CFR-2025-title41-vol1",
        "part50",  # a NODE: Title 41's compound part stays cut at the hyphen (documented limit)
        "part50-201-toc-id5",
        "sec50-201-1-app1",  # a section-token appendix the scan does not hold
    )
    assert (placed["part50"]["part"], placed["part50"]["cfr_ref"]) == ("50", "41-50")
    assert placed["part50-201-toc-id5"]["part"] == "50"
    appendix = placed["sec50-201-1-app1"]
    assert (appendix["part"], appendix["section"], appendix["cfr_ref"]) == (None, "50-201-1-app1", None)


def test_placement_keeps_every_non_placement_column():
    package_id = "CFR-2025-title43-vol2"
    granule = {
        "granuleId": f"{package_id}-sec1601-0-1",
        "_package_id": package_id,
        "title": "Purpose.",
        "granuleClass": "CONTENT",
        "lastModified": "2026-07-30T12:33:07Z",
    }
    shaped = _shape(granule)
    volume = annual_volume(package_id)
    assert volume is not None
    [placed] = place_sections([shaped], (ANCESTRY / f"{package_id}.xml").read_bytes(), volume)
    changed = {column for column in COLUMNS if placed[column] != shaped[column]}
    assert changed == {"part", "section", "cfr_ref"}


def test_a_volume_the_validator_refuses_is_still_placed_and_the_refusal_logged():
    """CFR-2025-title34-vol4 prints TITLENUM twice (Title 34 and reserved Title 35)."""
    placed, warnings = _place("CFR-2025-title34-vol4", "sec681-1")
    assert (placed["sec681-1"]["part"], placed["sec681-1"]["cfr_ref"]) == ("681", "34-681.1")
    assert len(warnings) == 1
    assert "CFR-2025-title34-vol4" in warnings[0] and "TITLENUM" in warnings[0]


def test_a_volume_without_sections_places_nothing():
    """CFR-2025-title40-vol9 prints only appendices; its scan is empty, so rows keep their identifier values."""
    placed, warnings = _place("CFR-2025-title40-vol9", "sec60-1", "part60-appA-1")
    assert (placed["sec60-1"]["part"], placed["sec60-1"]["section"]) == (None, "60-1")
    assert placed["part60-appA-1"]["part"] == "60"
    assert len(warnings) == 1 and "lacks source section content" in warnings[0]


@pytest.mark.parametrize(
    ("package_id", "volume"),
    [
        ("CFR-2025-title14-vol4", (2025, 14, 4)),
        ("CFR-2026-title14-vol0", (2026, 14, 0)),
        ("GPO-CFR-INDEX-2025", None),
        (None, None),
    ],
)
def test_annual_volume_reads_only_volume_package_ids(package_id, volume):
    selection = annual_volume(package_id)
    assert (None if selection is None else (selection.year, selection.title, selection.volume)) == volume
