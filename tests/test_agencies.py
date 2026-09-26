"""The REF-038 agency lookup: reverse codes for FR agency ids, one code per FR row."""

from __future__ import annotations

import hashlib
from collections import Counter
from importlib.resources import files

import pytest

from spicy_regs.ontology import agencies

# FR agency ids looked up from the replay's federal_register.parquet agencies_json and
# RefSpec's vendored projection; each names the agency it belongs to.
EPA_ID = 145  # Environmental Protection Agency
DOT_ID = 492  # Transportation Department, the parent of the FAA entry below
FAA_ID = 159  # Federal Aviation Administration
NSF_ID = 366  # National Science Foundation
DOE_ID = 136  # Energy Department; DOE projects onto an eCFR org, not an FR one
FERC_ID = 167  # Federal Energy Regulatory Commission, Energy Department's child
USDA_ID = 12  # Agriculture Department, GIPSA's grandparent below
GIPSA_ID = 218  # Grain Inspection, Packers and Stockyards Administration, under AMS (9)
ED_ID = 126  # Education Department
LABOR_ID = 271  # Labor Department; DOL projects onto an eCFR org, not an FR one
CNCS_ID = 91  # Corporation for National and Community Service
HHS_ID = 221  # Health and Human Services Department
HCFA_ID = 559  # Health Care Finance Administration, HHS's unresolved child (parent_id 221)
JUSTICE_ID = 268  # Justice Department
INS_ID = 232  # Immigration and Naturalization Service, Justice's unresolved child (parent_id 268)

#: RefSpec dev19's published ambiguous pairs: both codes of each pair project onto one org.
DEV19_AMBIGUOUS_PAIRS = (("FR", "OFR"), ("FPPO", "OFPP"), ("ACHP", "HPAC"), ("CDFI", "CDFIF"), ("CNCS", "CORP"))


def test_fr_agency_code_resolves_epa():
    assert agencies.fr_agency_code(EPA_ID) == "EPA"


def test_parent_department_yields_to_its_most_specific_agency():
    entries = [
        {"id": DOT_ID, "name": "Transportation Department"},
        {"id": FAA_ID, "name": "Federal Aviation Administration"},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "FAA"


def test_two_unrelated_agencies_resolve_to_none():
    entries = [
        {"id": EPA_ID, "name": "Environmental Protection Agency"},
        {"id": NSF_ID, "name": "National Science Foundation"},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) is None


def test_unknown_fr_agency_id_resolves_to_none():
    assert agencies.fr_agency_code(999_999) is None


def test_doe_energy_department_is_the_documented_gap():
    entries = [{"id": DOE_ID, "name": "Energy Department"}]
    assert agencies.fr_agency_code(DOE_ID) is None
    assert agencies.agency_code_for_fr_agencies(entries) is None


def test_several_codes_projecting_onto_one_org_resolve_to_none():
    assert agencies.fr_agency_code(CNCS_ID) is None  # CNCS and CORP both project onto it


def test_tampered_projection_bytes_are_refused(tmp_path, monkeypatch):
    tampered = tmp_path / "agency-projection.parquet"
    data = bytearray(agencies.AGENCY_PROJECTION_PATH.read_bytes())
    data[len(data) // 2] ^= 1
    tampered.write_bytes(data)
    monkeypatch.setattr(agencies, "AGENCY_PROJECTION_PATH", tampered)
    with pytest.raises(ValueError, match="is not RefSpec's file"):
        agencies.projection_rows()


def test_malformed_entries_are_skipped():
    # The str element is out of contract on purpose: FR rows carry junk entries too.
    entries = [{"name": "An agency without an id"}, {"id": "not-an-integer"}, "not-a-dict", {"id": EPA_ID}]
    assert agencies.agency_code_for_fr_agencies(entries) == "EPA"  # ty: ignore[invalid-argument-type]


def test_unresolved_parent_of_the_chosen_agency_is_accepted():
    entries = [
        {"id": DOE_ID, "name": "Energy Department"},
        {"id": FERC_ID, "name": "Federal Energy Regulatory Commission"},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "FERC"


def test_ancestor_two_levels_up_collapses_to_the_most_specific():
    entries = [
        {"id": USDA_ID, "name": "Agriculture Department"},
        {"id": GIPSA_ID, "name": "Grain Inspection, Packers and Stockyards Administration"},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "GIPSA"


def test_unresolved_unrelated_agency_makes_a_joint_document():
    entries = [
        {"id": ED_ID, "name": "Education Department"},
        {"id": LABOR_ID, "name": "Labor Department"},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) is None


def test_unresolved_child_of_the_chosen_agency_is_the_departments_code():
    entries = [
        {"id": HHS_ID, "name": "Health and Human Services Department"},
        {"id": HCFA_ID, "name": "Health Care Finance Administration", "parent_id": HHS_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "HHS"


def test_justice_with_ins_is_the_justice_departments_code():
    entries = [
        {"id": JUSTICE_ID, "name": "Justice Department"},
        {"id": INS_ID, "name": "Immigration and Naturalization Service", "parent_id": JUSTICE_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "DOJ"


def test_vendored_companions_match_the_readmes_pinned_digests():
    base = files("spicy_regs").joinpath("reference/refspec")
    unresolved = base.joinpath("agency-projection-unresolved.parquet").read_bytes()
    manifest = base.joinpath("view-manifest.json").read_bytes()
    assert hashlib.sha256(unresolved).hexdigest() == agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256
    assert hashlib.sha256(manifest).hexdigest() == agencies.VIEW_MANIFEST_SHA256


def test_refspec_dev19_ambiguity_agrees_with_the_reverse_lookup():
    rows = agencies.projection_rows()
    org_by_code = {row["source_value"]: row["org"] for row in rows}
    pair_targets = {org_by_code[first] for first, _ in DEV19_AMBIGUOUS_PAIRS}
    for first, second in DEV19_AMBIGUOUS_PAIRS:
        assert org_by_code[first] == org_by_code[second], (first, second)
    fr_targets = {org for org in pair_targets if org.startswith("urn:ref:federal-register-agency:")}
    # Every pair whose target is an FR-agency org gives None, and no other FR org carries several codes.
    for org in fr_targets:
        assert agencies.fr_agency_code(int(org.rsplit(":", 1)[1])) is None
    counts = Counter(row["org"] for row in rows if row["org"].startswith("urn:ref:federal-register-agency:"))
    assert {org for org, n in counts.items() if n > 1} == fr_targets
