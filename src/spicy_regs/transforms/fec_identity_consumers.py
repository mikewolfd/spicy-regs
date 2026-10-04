"""Receipt-qualified inputs to the existing financial and filing safeguards."""

from .fec_identity_receipts import read_identity_rows


def quality_notice_effect_with_receipts(financial_row, *, directory, notice_id, generation_id):
    from .fec_financial_policy import quality_notice_effect

    notices = [
        row
        for row in read_identity_rows(directory, "fec_quality_notices", generation_id=generation_id)
        if row["record_id"] == notice_id
    ]
    if len(notices) != 1:
        raise ValueError("Quality notice requires exactly one selected receipt-backed observation")
    return quality_notice_effect(financial_row, notices[0])


def associate_filing_numbers_with_receipts(
    rows, *, directory, generation_id, table, source_generation_pin, namespace_evidence
):
    from .fec_filing_associations import associate_filing_numbers

    return associate_filing_numbers(
        rows,
        table=table,
        filings=read_identity_rows(directory, "fec_filings", generation_id=generation_id),
        source_generation_pin=source_generation_pin,
        namespace_evidence=namespace_evidence,
    )
