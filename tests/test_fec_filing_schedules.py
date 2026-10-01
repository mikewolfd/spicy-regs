"""Meaningful source-format differences in the retained filing tables."""

from datetime import date
from decimal import Decimal
import json

import pytest

from spicy_regs.transforms.fec_filing_financial import load_layout, map_filing_financial
from spicy_regs.transforms.fec_filing_schedules import filing_mapping_for
from tests.test_fec_filing_financial import SELECTION, header, native, source


def mapped(version, form, fields):
    mapping = filing_mapping_for(version, form)
    native_header = header(version)
    if version.startswith("P"):
        native_header = source(
            dict(kind="header", format_version=version, fields=["HDR", version, "Data Capture System"]), 0
        )
    elif version == "1.02":
        native_header = source(
            dict(kind="header", format_version=version, fields=["/* Header", "FEC_Ver_# = 1.02", "/* End Header"]), 0
        )
    return map_filing_financial(
        source(native(mapping, form, fields), 1),
        SELECTION,
        mapping,
        layout=load_layout(mapping),
        header_row=native_header,
    )[0]


def test_combined_donor_names_and_two_address_roles_survive():
    row = mapped(
        "1.02",
        "SA11A1",
        {
            "2": "SMITH, JANE",
            "3": "1 MAIN ST",
            "12": "7500",
            "13": "20000229",
            "14": "125.25",
            "17": "C00000002",
            "18": "DONOR COMMITTEE",
            "19": "2 SECOND ST",
        },
    )
    assert row["contributor_name"] == "SMITH, JANE" and "contributor_last_name" not in row
    assert row["contributor_street1"] == "1 MAIN ST"
    assert row["donor_committee_street1"] == "2 SECOND ST" and row["donor_committee_id"] == "C00000002"
    assert row["amount"] == Decimal("125.25") and row["reported_aggregate_amount"] == 7500
    assert row["transaction_date"] == date(2000, 2, 29)


def test_disbursement_dates_and_original_reference_codes_keep_their_meaning():
    row = mapped("5.1", "SB17", {"13": "20040229", "14": "-0.99", "29": "A", "36": "20040301"})
    assert row["amount"] == Decimal("-.99") and row["transaction_date"] == date(2004, 2, 29)
    assert row["communication_date"] == date(2004, 3, 1) and row["source_amendment_code"] == "A"
    assert row["current_record_status"] == "unqualified" and row["correction_operation"] == "none"
    row = mapped("2.00", "SA11A1", {"33": "000123", "34": "ORIG", "35": "SUPR"})
    assert row["transaction_id"] == "000123" and row["source_original_transaction_id"] == "ORIG"
    assert row["source_supr_transaction_id"] == "SUPR" and row["filing_key"] is None


def test_guarantor_amount_uses_the_source_version_position():
    row = mapped("8.1", "SC2/12", {"3": "LOAN01", "4": "JONES", "16": "135.75"})
    assert row["guarantor_last_name"] == "JONES" and row["guaranteed_amount"] == Decimal("135.75")
    assert row["back_reference_transaction_id"] == "LOAN01" and row["loan_link_status"] == "unresolved"


def test_paper_inaugural_data_keeps_amounts_and_image_number_without_pdf_work():
    row = mapped("P3.3", "F132", {"13": "20170120", "14": "1000.01", "15": "2000.02", "18": "17000001234"})
    assert row["amount"] == Decimal("1000.01") and row["reported_aggregate_amount"] == Decimal("2000.02")
    assert row["transaction_date"] == date(2017, 1, 20) and row["image_number"] == "17000001234"
    assert row["source_namespace"] == "fec-paper-filing-schedule" and row["declared_format_version"] == "P3.3"


def test_padded_paper_money_stays_exact_with_raw_spelling_and_conversion_state():
    row = mapped("P3.3", "F132", {"14": "1000000.00 "})
    assert row["amount"] == Decimal("1000000.00") and row["amount_raw"] == "1000000.00 "
    assert row["amount_status"] == "exact_after_ascii_padding"
    assert "amount" not in json.loads(row["mapping_reason_json"])["field_refusals"]
    row = mapped("P3.3", "F132", {"14": "1,000.00 "})
    assert row["amount"] is None and row["amount_status"] == "unsupported_spelling"


def test_debt_stock_and_period_activity_do_not_become_one_transaction():
    row = mapped("3.00", "SD10", {"10": "900", "11": "50", "12": "20", "13": "930"})
    assert [row[n] for n in ("opening_balance", "incurred_in_period", "paid_in_period", "closing_balance")] == [
        900,
        50,
        20,
        930,
    ]
    assert "amount" not in row


def test_noncanonical_header_spelling_is_retained_only_when_explicitly_selected():
    row = mapped("8.5 ", "SA11AI", {"20": "12"})
    assert row["declared_format_version"] == "8.5 " and row["amount"] == 12
    for version in (" 8.5", "8.500", "p3.3", "2.01"):
        assert filing_mapping_for(version, "SA11AI") is None


@pytest.mark.parametrize("record_type", ["SA3L", "SB3L", "SASI1", "SBSI2", "TEXT", "F3XN"])
def test_other_financial_meanings_do_not_become_ordinary_transactions(record_type):
    assert filing_mapping_for("8.5", record_type) is None


def test_source_version_mismatch_and_invalid_values_are_visible():
    mapping = filing_mapping_for("1.02", "SA11A1")
    with pytest.raises(ValueError, match="exact version"):
        map_filing_financial(
            source(native(mapping, "SA11A1"), 1),
            SELECTION,
            mapping,
            layout=load_layout(mapping),
            header_row=header("8.5"),
        )
    row = mapped("1.02", "SA11A1", {"13": "20010229", "14": "1e6"})
    assert row["amount"] is None and row["transaction_date"] is None
    assert row["amount_raw"] == "1e6" and row["transaction_date_raw"] == "20010229"
    assert set(json.loads(row["mapping_reason_json"])["field_refusals"]) == {"amount", "transaction_date"}
