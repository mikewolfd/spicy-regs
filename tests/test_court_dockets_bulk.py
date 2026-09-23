"""Hermetic tests for the bulk-docket mapping (no network, no big files)."""
from __future__ import annotations

from spicy_regs.transforms.build_court_dockets_bulk import (
    COLUMNS,
    is_apa_nature,
    shape_bulk_docket,
)

_COURTS = {
    "dcd": {"full_name": "District Court, District of Columbia", "citation_string": "D.D.C."},
    "orctapp": {"full_name": "Court of Appeals of Oregon", "citation_string": "Or. Ct. App."},
}


def _native_row(**overrides) -> dict:
    row = {
        "id": "73613631",
        "case_name": "Todd v. United States",
        "case_name_full": "",
        "court_id": "dcd",
        "docket_number": "1:26-cv-02460",
        "date_filed": "2026-07-14 00:00:00-07",
        "date_terminated": None,
        "date_argued": None,
        "nature_of_suit": "899 Other Statutes: Administrative Procedures Act/Review or Appeal of Agency Decision",
        "cause": "05:551 Administrative Procedure Act",
        "jurisdiction_type": "U.S. Government Defendant",
        "jury_demand": "None",
        "assigned_to_str": "Christopher Reid Cooper",
        "referred_to_str": "",
        "pacer_case_id": "294455",
        "date_created": "2014-10-30 05:45:33.413835+00",
        "slug": "todd-v-united-states",
        "blocked": "f",
    }
    row.update(overrides)
    return row


def test_maps_every_host_column_and_nothing_else():
    row = shape_bulk_docket(_native_row(), _COURTS)
    assert set(row) == set(COLUMNS)


def test_direct_fields_and_court_join():
    row = shape_bulk_docket(_native_row(), _COURTS)
    assert row["cl_docket_id"] == "73613631"
    assert row["case_name"] == "Todd v. United States"
    assert row["court_id"] == "dcd"
    assert row["court"] == "District Court, District of Columbia"
    assert row["court_citation_string"] == "D.D.C."
    assert row["docket_number"] == "1:26-cv-02460"
    assert row["pacer_case_id"] == "294455"
    assert row["nature_of_suit"].startswith("899")


def test_empty_string_becomes_null_where_the_host_spells_absence_as_null():
    row = shape_bulk_docket(_native_row(), _COURTS)
    assert row["case_name_full"] is None
    assert row["referred_to"] is None
    assert row["date_terminated"] is None
    assert row["date_argued"] is None


def test_date_prefix_and_created_at_normalization():
    row = shape_bulk_docket(_native_row(), _COURTS)
    assert row["date_filed"] == "2026-07-14"
    assert row["date_created"] == "2014-10-30T05:45:33.413835+00"


def test_relationship_columns_are_explicitly_unknown_not_empty():
    row = shape_bulk_docket(_native_row(), _COURTS)
    assert row["parties_json"] is None
    assert row["attorneys_json"] is None
    assert row["firms_json"] is None


def test_absolute_url_reconstructed_from_id_and_slug():
    row = shape_bulk_docket(_native_row(), _COURTS)
    assert row["absolute_url"] == "https://www.courtlistener.com/docket/73613631/todd-v-united-states/"
    bare = shape_bulk_docket(_native_row(slug=""), _COURTS)
    assert bare["absolute_url"] == "https://www.courtlistener.com/docket/73613631/"


def test_missing_court_keeps_court_id_but_null_names():
    row = shape_bulk_docket(_native_row(court_id="nope"), _COURTS)
    assert row["court_id"] == "nope"
    assert row["court"] is None
    assert row["court_citation_string"] is None


def test_null_vs_empty_preserved_for_non_nullable_native_values():
    row = shape_bulk_docket(_native_row(date_filed=None), _COURTS)
    assert row["date_filed"] is None


def test_apa_classification_covers_all_three_publisher_spellings():
    assert is_apa_nature("899 Other Statutes: Administrative Procedures Act/Review or Appeal of Agency Decision")
    assert is_apa_nature("899 Admin Proc Act/Rvw Ag Dec (Fed Qst.)")
    assert is_apa_nature("Administrative Procedure Act/Review or Appeal of Agency Decision")
    assert is_apa_nature("Other Statutes:  Administrative Procedures Act/ Review or Appeal of Agency Decision")
    assert is_apa_nature("2899 Other Statutes - APA Review/Appeal")
    assert is_apa_nature("3899 Adminstrative Review Act")
    assert is_apa_nature("2899 Admin Proc Act/Review")


def test_apa_classification_rejects_non_apa_natures():
    assert not is_apa_nature("")
    assert not is_apa_nature(None)
    assert not is_apa_nature("230 Rent, lease, ejectment")
    assert not is_apa_nature("510 Prisoner petitions - vacate sentence")
    assert not is_apa_nature("890 Other statutory actions")
    assert not is_apa_nature("2899 Other Statutes")  # plain, no APA wording
    assert not is_apa_nature("3899 Other Statutes")
