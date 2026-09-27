"""The agency lookup: REF-038's projection with the registry view's bridges and successions, one code per FR row."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from collections import Counter
from importlib.resources import files
from pathlib import Path

import pytest

from spicy_regs.ontology import agencies

# FR agency ids looked up from the replay's federal_register.parquet agencies_json, RefSpec's
# vendored projection and its registry view; each names the agency it belongs to.
EPA_ID = 145  # Environmental Protection Agency
DOT_ID = 492  # Transportation Department, the parent of the FAA entry below
FAA_ID = 159  # Federal Aviation Administration
NSF_ID = 366  # National Science Foundation
DOE_ID = 136  # Energy Department; DOE selects an eCFR org, which bridge same:fr136 joins
LABOR_ID = 271  # Labor Department; DOL likewise, through same:fr271
FERC_ID = 167  # Federal Energy Regulatory Commission, Energy Department's child
USDA_ID = 12  # Agriculture Department, GIPSA's grandparent below
GIPSA_ID = 218  # Grain Inspection, Packers and Stockyards Administration, under AMS (9)
COMMERCE_ID = 54  # Commerce Department
BEA_ID = 118  # Economic Analysis Bureau, Commerce's child; same:fr118 joins the eCFR's, which EAB selects
EXPORT_ADMIN_ID = 150  # Export Administration Bureau, renamed Industry and Security Bureau (BIS) 2002-04-18
CNCS_ID = 91  # Corporation for National and Community Service
HHS_ID = 221  # Health and Human Services Department
HCFA_ID = 559  # Health Care Finance Administration, renamed CMS 2001-06-29 (parent_id 221)
TREASURY_ID = 497  # Treasury Department
IIO_ID = 259  # International Investment Office, renamed Investment Security Office (603) 2008-11-21
FISCAL_SERVICE_ID = 196  # Bureau of the Fiscal Service, Treasury's child no code selects
POSTAL_RATE_ID = 564  # Postal Rate Commission, redesignated Postal Regulatory Commission (409) 2006-12-20
JUSTICE_ID = 268  # Justice Department
INS_ID = 232  # Immigration and Naturalization Service, split to USCIS, CBP and ICE 2003-03-01
USIA_ID = 510  # United States Information Agency, split to State and the BBG 1999-10-01
CUSTOMS_ID = 96  # Customs Service, split to CBP and ICE 2003-03-01
ICC_ID = 543  # Interstate Commerce Commission, split to the STB and Transportation 1996-01-01
MINES_ID = 290  # Mines Bureau, an Interior child no code selects
INTERIOR_ID = 253  # Interior Department
EXIM_ID = 151  # Export-Import Bank: the owner refused its bridge (a non-emission)
UDALL_ID = 296  # Udall Foundation: its bridge is held (a non-emission)
IBWC_ID = 255  # International Boundary and Water Commission: its bridge was withdrawn

#: RefSpec dev19's published ambiguous pairs: both codes of each pair project onto one org.
DEV19_AMBIGUOUS_PAIRS = (("FR", "OFR"), ("FPPO", "OFPP"), ("ACHP", "HPAC"), ("CDFI", "CDFIF"), ("CNCS", "CORP"))
REGISTRY_TABLES = {
    "bridges": agencies.AGENCY_REGISTRY_BRIDGES_SHA256,
    "events": agencies.AGENCY_REGISTRY_EVENTS_SHA256,
    "non-emissions": agencies.AGENCY_REGISTRY_NON_EMISSIONS_SHA256,
}
FR = "urn:ref:federal-register-agency:"


def event(original: int, result: int) -> dict:
    """One synthetic view event row, (event, result), in the view's shape."""
    return {"originals": [f"{FR}{original}"], "result": f"{FR}{result}"}


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


@pytest.mark.parametrize("table", sorted(REGISTRY_TABLES))
def test_tampered_registry_bytes_are_refused(tmp_path, monkeypatch, table):
    view = tmp_path / "agency-registry-view"
    shutil.copytree(Path(str(agencies.AGENCY_REGISTRY_VIEW_PATH)), view)
    tampered = view / f"tables/agency-registry-{table}.parquet"
    data = bytearray(tampered.read_bytes())
    data[len(data) // 2] ^= 1
    tampered.write_bytes(data)
    monkeypatch.setattr(agencies, "AGENCY_REGISTRY_VIEW_PATH", view)
    with pytest.raises(ValueError, match="is not RefSpec's file"):
        agencies.registry_rows(table)


def test_malformed_entries_are_skipped():
    # The str element is out of contract on purpose: FR rows carry junk entries too.
    entries = [{"name": "An agency without an id"}, {"id": "not-an-integer"}, "not-a-dict", {"id": EPA_ID}]
    assert agencies.agency_code_for_fr_agencies(entries) == "EPA"  # ty: ignore[invalid-argument-type]


def test_unresolved_ancestor_of_the_chosen_agency_is_accepted(monkeypatch):
    # REF-038 alone, before the registry bridged the Energy Department: no code selected it.
    ref038 = agencies._build_projection(agencies.projection_rows(), (), ())
    monkeypatch.setattr(agencies, "_projection", lambda: ref038)
    entries = [
        {"id": DOE_ID, "name": "Energy Department"},
        {"id": FERC_ID, "name": "Federal Energy Regulatory Commission"},
    ]
    assert agencies.fr_agency_code(DOE_ID) is None
    assert agencies.agency_code_for_fr_agencies(entries) == "FERC"


def test_ancestor_two_levels_up_collapses_to_the_most_specific():
    entries = [
        {"id": USDA_ID, "name": "Agriculture Department"},
        {"id": GIPSA_ID, "name": "Grain Inspection, Packers and Stockyards Administration"},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "GIPSA"


def test_unresolved_unrelated_agency_makes_a_joint_document():
    entries = [
        {"id": EPA_ID, "name": "Environmental Protection Agency"},
        {"id": MINES_ID, "name": "Mines Bureau", "parent_id": INTERIOR_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) is None


def test_unresolved_child_of_the_chosen_agency_is_the_departments_code():
    entries = [
        {"id": TREASURY_ID, "name": "Treasury Department"},
        {"id": FISCAL_SERVICE_ID, "name": "Bureau of the Fiscal Service", "parent_id": TREASURY_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "TREAS"


def test_justice_with_ins_is_the_justice_departments_code():
    # INS split three ways, so no one code selects it: it stays Justice's unresolved bureau.
    entries = [
        {"id": JUSTICE_ID, "name": "Justice Department"},
        {"id": INS_ID, "name": "Immigration and Naturalization Service", "parent_id": JUSTICE_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(entries) == "DOJ"


def test_a_bridge_gains_its_counterparts_code():
    assert agencies.fr_agency_code(DOE_ID) == "DOE"
    assert agencies.fr_agency_code(LABOR_ID) == "DOL"
    assert agencies.agency_code_for_fr_agencies([{"id": DOE_ID, "name": "Energy Department"}]) == "DOE"


def test_a_bridge_carries_its_subjects_roster_parent():
    # No parent_id on the entries: the chain Commerce -> Economic Analysis Bureau is the bridge's.
    entries = [{"id": COMMERCE_ID, "name": "Commerce Department"}, {"id": BEA_ID, "name": "Economic Analysis Bureau"}]
    assert agencies.agency_code_for_fr_agencies(entries) == "EAB"


def test_a_bridge_onto_an_already_coded_agency_adds_codes():
    # The refused Ex-Im bridge's shape: USEIB selects FR 151 and EIB the eCFR bank, so bridged
    # FR 151 would carry both and resolve to neither (the cost the owner refused).
    (exim,) = [row for row in agencies.registry_rows("non-emissions") if row["subject"] == f"{FR}{EXIM_ID}"]
    bridge = {
        "candidate_id": exim["item_id"],
        "subject": exim["subject"],
        "subject_parent": None,
        "object": exim["object"],
    }
    ref038 = agencies._build_projection(agencies.projection_rows(), (), ())
    bridged = agencies._build_projection(agencies.projection_rows(), [bridge], ())
    assert ref038.code_by_fr_id[EXIM_ID] == "USEIB"
    assert EXIM_ID not in bridged.code_by_fr_id


def test_an_entrys_parent_id_never_overrides_a_stated_chain(monkeypatch):
    rows = [
        {"org": f"{FR}900020", "source_value": "PARENT", "parent_org": None},
        {"org": f"{FR}900021", "source_value": "CHILD", "parent_org": f"{FR}900020"},
    ]
    projection = agencies._build_projection(rows, (), ())
    monkeypatch.setattr(agencies, "_projection", lambda: projection)
    assert agencies.agency_code_for_fr_agencies([{"id": 900020}, {"id": 900021, "parent_id": 900099}]) == "CHILD"


def test_a_bridge_subject_keeps_its_sealed_parent_whatever_its_entry_says():
    # The bridge seals the Economic Analysis Bureau under Commerce; a row naming HHS as its parent moves nothing.
    commerce_row = [{"id": COMMERCE_ID}, {"id": BEA_ID, "parent_id": HHS_ID}]
    hhs_row = [{"id": HHS_ID}, {"id": BEA_ID, "parent_id": HHS_ID}]
    assert agencies.agency_code_for_fr_agencies(commerce_row) == "EAB"
    assert agencies.agency_code_for_fr_agencies(hhs_row) is None


def test_an_originals_chain_from_its_entry_reaches_its_grandparents(monkeypatch):
    # 900033 was renamed 900032. Its entry names 900031 as parent, whose stated parent is 900030.
    rows = [
        {"org": f"{FR}900030", "source_value": "GRAND", "parent_org": None},
        {"org": f"{FR}900031", "source_value": "MIDDLE", "parent_org": f"{FR}900030"},
        {"org": f"{FR}900032", "source_value": "SUCCESSOR", "parent_org": None},
    ]
    projection = agencies._build_projection(rows, (), [event(900033, 900032)])
    monkeypatch.setattr(agencies, "_projection", lambda: projection)
    entries = [{"id": 900030}, {"id": 900033, "parent_id": 900031}]
    assert agencies.agency_code_for_fr_agencies(entries) == "SUCCESSOR"


def test_an_original_without_a_parent_id_stands_apart_from_its_department(monkeypatch):
    # With no parent_id, HCFA (CMS) and HHS are two unrelated coded agencies: a joint document.
    entries = [{"id": HHS_ID, "name": "Health and Human Services Department"}, {"id": HCFA_ID}]
    assert agencies.agency_code_for_fr_agencies(entries) is None
    # The same answer as REF-038 alone, where HCFA was an unresolved agency with no parent_id.
    ref038 = agencies._build_projection(agencies.projection_rows(), (), ())
    monkeypatch.setattr(agencies, "_projection", lambda: ref038)
    assert agencies.agency_code_for_fr_agencies(entries) is None


def test_an_originals_differing_parent_id_is_taken_as_the_row_states():
    # A row naming Commerce as HCFA's parent puts HCFA under Commerce, and so away from HHS.
    assert (
        agencies.agency_code_for_fr_agencies([{"id": COMMERCE_ID}, {"id": HCFA_ID, "parent_id": COMMERCE_ID}]) == "CMS"
    )
    assert agencies.agency_code_for_fr_agencies([{"id": HHS_ID}, {"id": HCFA_ID, "parent_id": COMMERCE_ID}]) is None


def test_a_rename_reroutes_to_the_successors_code():
    assert agencies.fr_agency_code(HCFA_ID) == "CMS"
    hcfa = [
        {"id": HHS_ID, "name": "Health and Human Services Department"},
        {"id": HCFA_ID, "name": "Health Care Finance Administration", "parent_id": HHS_ID},
    ]
    export_administration = [
        {"id": COMMERCE_ID, "name": "Commerce Department"},
        {"id": EXPORT_ADMIN_ID, "name": "Export Administration Bureau", "parent_id": COMMERCE_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(hcfa) == "CMS"  # HHS under REF-038 alone
    assert agencies.agency_code_for_fr_agencies(export_administration) == "BIS"  # DOC under REF-038 alone


def test_a_succession_reaches_its_successors_bridged_code():
    # Neither successor is selected by a code in REF-038; each is reached only through its bridge.
    iio = [
        {"id": TREASURY_ID, "name": "Treasury Department"},
        {"id": IIO_ID, "name": "International Investment Office", "parent_id": TREASURY_ID},
    ]
    assert agencies.agency_code_for_fr_agencies(iio) == "IIO"
    assert agencies.fr_agency_code(POSTAL_RATE_ID) == "PRC"


def test_a_split_to_several_codes_gives_none():
    for split in (INS_ID, USIA_ID, CUSTOMS_ID, ICC_ID):
        assert agencies.fr_agency_code(split) is None, split
    assert (
        agencies.agency_code_for_fr_agencies([{"id": INS_ID, "name": "Immigration and Naturalization Service"}]) is None
    )


def test_a_split_resolves_only_when_every_successor_agrees():
    # 900010, 900011 and 900012 are coded; 900013 is not. Each original splits two ways.
    rows = [
        {"org": f"{FR}900010", "source_value": "SAME", "parent_org": None},
        {"org": f"{FR}900011", "source_value": "SAME", "parent_org": None},
        {"org": f"{FR}900012", "source_value": "OTHER", "parent_org": None},
    ]
    events = [
        *(event(900001, result) for result in (900010, 900011)),  # both SAME
        *(event(900002, result) for result in (900010, 900012)),  # SAME and OTHER: ambiguous
        *(event(900003, result) for result in (900010, 900013)),  # SAME and an uncoded successor
    ]
    codes = agencies._build_projection(rows, (), events).code_by_fr_id
    assert codes.get(900001) == "SAME"
    assert codes.get(900002) is None
    assert codes.get(900003) is None, "an uncoded successor is unknown, not absent"


def test_a_split_is_read_even_where_it_gives_none():
    # Each original also carries a code of its own, which it keeps only if its split went unread.
    rows = [
        {"org": f"{FR}900001", "source_value": "OWN1", "parent_org": None},
        {"org": f"{FR}900002", "source_value": "OWN2", "parent_org": None},
        {"org": f"{FR}900010", "source_value": "TEN", "parent_org": None},
        {"org": f"{FR}900011", "source_value": "ELEVEN", "parent_org": None},
    ]
    events = [
        *(event(900001, result) for result in (900010, 900011)),  # ambiguous
        *(event(900002, result) for result in (900010, 900013)),  # unknown
    ]
    codes = agencies._build_projection(rows, (), events).code_by_fr_id
    assert (codes.get(900001), codes.get(900002)) == (None, None)


def test_the_views_splits_are_read():
    # Were every result of each vendored split selected by one code, each original would take it.
    events = agencies.registry_rows("events")
    results = {}
    for row in events:
        results.setdefault(row["event_id"], set()).add(row["result"])
    splits = {event_id: orgs for event_id, orgs in results.items() if len(orgs) > 1}
    assert set(splits) == {"event:fr232", "event:fr510", "event:fr543", "event:fr96"}
    rows = [
        {"org": org, "source_value": f"ONE:{event_id}", "parent_org": None}
        for event_id, orgs in splits.items()
        if event_id != "event:fr96"  # Customs' results are two of INS's
        for org in orgs
    ]
    codes = agencies._build_projection(rows, (), events).code_by_fr_id
    assert codes.get(INS_ID) == "ONE:event:fr232"
    assert codes.get(USIA_ID) == "ONE:event:fr510"
    assert codes.get(ICC_ID) == "ONE:event:fr543"
    assert codes.get(CUSTOMS_ID) == "ONE:event:fr232"


def test_a_chain_resolves_to_its_end():
    # 900001 was renamed 900002, which was renamed 900003; codes select 900002 and 900003.
    rows = [
        {"org": f"{FR}900002", "source_value": "MIDDLE", "parent_org": None},
        {"org": f"{FR}900003", "source_value": "END", "parent_org": None},
    ]
    projection = agencies._build_projection(rows, (), [event(900001, 900002), event(900002, 900003)])
    assert projection.code_by_fr_id[900001] == "END"
    assert 900002 not in projection.code_by_fr_id  # MIDDLE and END both select it


def test_a_chain_deeper_than_the_recursion_limit_resolves():
    depth = sys.getrecursionlimit() * 5
    rows = [{"org": f"{FR}{depth}", "source_value": "END", "parent_org": None}]
    projection = agencies._build_projection(rows, (), [event(i, i + 1) for i in range(depth)])
    assert projection.code_by_fr_id[0] == "END"


def test_a_cycle_is_refused():
    rows = [{"org": f"{FR}900003", "source_value": "END", "parent_org": None}]
    events = [event(900001, 900002), event(900002, 900001), event(900002, 900003)]
    with pytest.raises(ValueError, match="cycle"):
        agencies._build_projection(rows, (), events)


def test_a_bridge_stating_another_parent_is_refused():
    rows = [{"org": f"{FR}900001", "source_value": "ONE", "parent_org": f"{FR}900002"}]
    bridge = {
        "candidate_id": "same:fr900001",
        "subject": f"{FR}900001",
        "subject_parent": f"{FR}900003",
        "object": "urn:ref:ecfr-agency:one",
    }
    with pytest.raises(ValueError, match="another parent"):
        agencies._build_projection(rows, [bridge], ())


def test_non_emissions_add_nothing():
    items = {row["item_id"] for row in agencies.registry_rows("non-emissions")}
    assert items == {
        "no-fr-bridge:regs:CISA",
        "same:fr151:ecfr:export-import-bank",
        "same:fr255:ecfr:international-boundary-and-water-commission-united-states-and-mexico",
        "same:fr296:fh:300000070",
    }
    assert agencies.fr_agency_code(EXIM_ID) == "USEIB"  # the refused bridge would add EIB, leaving none
    assert agencies.fr_agency_code(UDALL_ID) is None
    assert agencies.fr_agency_code(IBWC_ID) is None


def test_vendored_companions_match_the_readmes_pinned_digests():
    base = files("spicy_regs").joinpath("reference/refspec")
    unresolved = base.joinpath("agency-projection-unresolved.parquet").read_bytes()
    manifest = base.joinpath("view-manifest.json").read_bytes()
    assert hashlib.sha256(unresolved).hexdigest() == agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256
    assert hashlib.sha256(manifest).hexdigest() == agencies.VIEW_MANIFEST_SHA256


def test_registry_manifest_is_the_pin_and_names_each_vendored_table():
    data = agencies.AGENCY_REGISTRY_VIEW_PATH.joinpath("view-manifest.json").read_bytes()
    assert hashlib.sha256(data).hexdigest() == agencies.AGENCY_REGISTRY_MANIFEST_SHA256
    members = {member["path"]: member for member in json.loads(data)["members"]}
    assert set(members) == {f"tables/agency-registry-{table}.parquet" for table in REGISTRY_TABLES}
    for table, sha256 in REGISTRY_TABLES.items():
        member = members[f"tables/agency-registry-{table}.parquet"]
        assert member["sha256"] == f"sha256:{sha256}"
        assert member["rowCount"] == len(agencies.registry_rows(table))


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
