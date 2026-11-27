"""One effective, source-reconciled Form 3 / 3P / 3X contribution population.

Native decoding, field positions, amounts and membership decisions reuse their
existing owners. This named calculation never qualifies all retained FEC rows.
"""

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, localcontext
import json

import pyarrow as pa

from spicy_regs.runtime_bounds import checkpoint
from spicy_regs.transforms.build_fec_financial_profiles import json_bytes
from spicy_regs.transforms.fec_financial_selection import FinancialRecord, membership_pin, select_scope
from spicy_regs.transforms.fec_filing_financial import load_layout, map_filing_financial
from spicy_regs.transforms.fec_filing_schedules import filing_mapping_for
from spicy_regs.transforms.fec_filing_forms import filing_cover_mapping_for, map_filing_cover
from spicy_regs.transforms.fec_query import exact_amount

POLICY = "fec-selected-report-contributions/1"
PROFILES = "fec_contribution_profiles"
DECISIONS = "fec_contribution_decisions"
SHARED = ("profile_id committee_id committee_name candidate_id candidate_association_basis filing_id source_sha256 "
          "source_url captured_at period_start period_end source_evidence_json policy_version coverage_note").split()
SCHEMAS = {
    PROFILES: pa.schema([(n, pa.string()) for n in SHARED + (
        "contribution_family gross_itemized_receipts negative_receipt_adjustments signed_itemized_receipts "
        "itemized_refunds net_itemized_flow unitemized_receipts net_reported_family amount_basis breakdown_json"
    ).split()] + [("source_cycle", pa.int32()), ("included_records", pa.int32()), ("memo_records", pa.int32())]),
    DECISIONS: pa.schema([(n, pa.string()) for n in SHARED + (
        "record_id contribution_family disposition disposition_reason amount donor_id donor_label employer "
        "occupation native_entity_type election_code memo_field_status transaction_date_scope native_values_json source_locator_json"
    ).split()] + [("source_cycle", pa.int32())]),
}


@dataclass(frozen=True)
class _FormSpec:
    individual_receipts: frozenset[str]
    individual_refunds: frozenset[str]
    pac_receipts: frozenset[str]
    pac_refunds: frozenset[str]
    requires_candidate: bool

    @property
    def schedule_types(self) -> frozenset[str]:
        return self.individual_receipts | self.individual_refunds | self.pac_receipts | self.pac_refunds

    @property
    def pac_types(self) -> frozenset[str]:
        return self.pac_receipts | self.pac_refunds


# Exact FEC Form 3 / 3P / 3X Schedule A & B contribution and refund line mappings.
SUPPORTED_FORMS: dict[str, _FormSpec] = {
    "F3": _FormSpec(
        individual_receipts=frozenset({"SA11AI"}),
        individual_refunds=frozenset({"SB20A"}),
        pac_receipts=frozenset({"SA11C"}),
        pac_refunds=frozenset({"SB20C"}),
        requires_candidate=True,
    ),
    "F3P": _FormSpec(
        individual_receipts=frozenset({"SA17A", "SA17AI"}),
        individual_refunds=frozenset({"SB28A"}),
        pac_receipts=frozenset({"SA17C"}),
        pac_refunds=frozenset({"SB28C"}),
        requires_candidate=True,
    ),
    "F3X": _FormSpec(
        individual_receipts=frozenset({"SA11AI"}),
        individual_refunds=frozenset({"SB28A"}),
        pac_receipts=frozenset({"SA11C"}),
        pac_refunds=frozenset({"SB28C"}),
        requires_candidate=False,
    ),
}


def _money(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("Selected contribution report requires exact source money")
    number, status = exact_amount(str(value))
    if status != "exact":
        raise ValueError("Selected contribution report has missing/refused money")
    return number


def _sum(values):
    with localcontext() as context:
        context.prec = 50
        return sum(values, Decimal(0))


def _amendment_tail(report) -> list[str]:
    chain = report.get("amendment_chain")
    if not isinstance(chain, list) or not chain:
        return []
    last = chain[-1]
    if isinstance(last, bool) or not isinstance(last, (str, int)):
        return []
    return [str(last)]


def candidate_for_report(candidate_id, committee_history, candidates, report, *, requires_candidate: bool = True):
    """Require explicit native linkage and an observation predating the report.

    The current committee can name candidates for different offices. Source
    first/last filing dates are evidence of observation, never authorization dates.
    A date-specific authorization request still needs the original statements.
    For Form 3X non-candidate committees, ``candidate_id`` must be ``None`` and
    no candidate linkage is asserted.
    """
    if not requires_candidate:
        if candidate_id is not None:
            raise ValueError("Non-candidate Form 3X contribution report cannot assert a candidate association")
        if committee_history.get("candidate_ids"):
            raise ValueError("Form 3X committee history unexpectedly lists candidate IDs")
        return "not_applicable_form_3x_non_candidate_committee; no candidate authorization asserted"
    if not isinstance(candidate_id, str) or candidate_id not in committee_history.get("candidate_ids", []):
        raise ValueError("Selected candidate lacks source committee linkage")
    selected = [row for row in candidates if row.get("candidate_id") == candidate_id]
    if len(selected) != 1 or (selected[0].get("first_file_date") or "9999") > report["coverage_start_date"][:10]:
        raise ValueError("Current candidate observation does not establish the selected report-period association")
    if report["cycle"] not in selected[0].get("cycles", []):
        raise ValueError("Candidate association is outside the selected cycle")
    return "native_committee_cycle_link_and_candidate_observed_before_report; daily authorization dates not asserted"


def contribution_profiles(rows, selection, *, report, filing_id, source_url, captured_at,
                          candidate_id, committee_history, candidates, evidence):
    """Qualify recipient-side receipts/refunds from one source-selected report.

    Publisher most_recent and amendment_chain bind the retained filing. Memo
    rows stay visible and excluded. Negative receipts are signed adjustments,
    distinct from Schedule B refunds. Employer labels never identify donors.
    Zero-PAC or zero-individual effective reports are admitted only when the
    official report's matching period totals reconcile exactly to 0.
    """
    filing_text = str(filing_id)
    most_recent_num = report.get("most_recent_file_number")
    if (report.get("most_recent") is not True or report.get("is_amended") is not False
            or isinstance(report.get("file_number"), bool)
            or report.get("file_number") != int(filing_text)
            or isinstance(most_recent_num, bool)
            or most_recent_num is None
            or most_recent_num != Decimal(filing_text)
            or _amendment_tail(report) != [filing_text]):
        raise ValueError("Retained filing is not the publisher-selected effective report")
    if report.get("fec_url") != source_url or report.get("committee_id") != committee_history.get("committee_id"):
        raise ValueError("Selected report identity differs from retained filing or committee history")
    row_iter = iter(rows)
    header = next(row_iter, None)
    cover_row = next(row_iter, None)
    if header is None or cover_row is None:
        raise ValueError("Selected contribution filing lacks header and cover records")
    header_meta = json.loads(header["metadata_json"])
    declared_version = header_meta.get("format_version")
    if not isinstance(declared_version, str) or not declared_version:
        raise ValueError("Selected contribution filing lacks a native format version")
    native_cover = json.loads(cover_row["metadata_json"])
    cover_mapping = filing_cover_mapping_for(declared_version, native_cover.get("record_type"))
    if cover_mapping is None or cover_mapping.form not in SUPPORTED_FORMS:
        raise ValueError("Selected contribution population requires a reviewed Form 3, 3P, or 3X cover")
    spec = SUPPORTED_FORMS[cover_mapping.form]
    association = candidate_for_report(
        candidate_id, committee_history, candidates, report, requires_candidate=spec.requires_candidate
    )
    cover, _ = map_filing_cover(cover_row, selection, cover_mapping, layout=load_layout(cover_mapping), header_row=header)
    if (cover["reporting_committee_id"] != report["committee_id"]
            or cover["period_start"] is None or cover["period_end"] is None
            or cover["period_start"].isoformat() != report["coverage_start_date"][:10]
            or cover["period_end"].isoformat() != report["coverage_end_date"][:10]
            or cover["report_code"] != report["report_type"]):
        raise ValueError("Retained filing cover differs from the matching official report identity or coverage")
    mapped = []
    layouts = {}
    schedule_types = spec.schedule_types
    for idx, row in enumerate(row_iter):
        if idx % 512 == 0:
            checkpoint()
        native = json.loads(row["metadata_json"])
        kind = native.get("record_type")
        if kind not in schedule_types:
            continue
        mapping = filing_mapping_for(declared_version, kind)
        if mapping is None:
            raise ValueError(f"Unsupported schedule layout for version {declared_version!r} and record type {kind!r}")
        if mapping not in layouts:
            layouts[mapping] = load_layout(mapping)
        values, _ = map_filing_financial(row, selection, mapping, layout=layouts[mapping], header_row=header)
        if values.get("reporting_committee_id") != report["committee_id"] or values.get("transaction_date_status") != "exact":
            raise ValueError("Contribution record differs from selected committee or has an unresolved date")
        if values.get("amount_status") not in {"exact", "exact_after_ascii_padding"}:
            raise ValueError("Selected contribution observation has no exact amount")
        if values.get("memo_indicator") not in {None, "", "X"}:
            raise ValueError("Unresolved contribution memo indicator")
        mapped.append(values)
    # Source selection qualifies membership only. It does not alter the existing
    # observation-level current_financial_total_qualified policy.
    if any(not r.get("transaction_id") for r in mapped):
        raise ValueError("Contribution observation lacks its native transaction ID")
    records = tuple(FinancialRecord(r["record_id"], str(r.get("transaction_id")), r["amount"],
        selection.source_sha256, filing_text) for r in mapped)
    selected = select_scope(filing_text, records, (), base_membership_sha256=membership_pin(records),
                           base_evidence_sha256=selection.source_sha256, base_complete=True)
    selected.require_members()
    by_family: dict[str, list[dict]] = defaultdict(list)
    for row in mapped:
        by_family["pac" if row["form_type"] in spec.pac_types else "individual"].append(row)
    capture_note = (
        "Capture time of the historical original remains unknown."
        if captured_at is None
        else "Original capture time recorded in captured_at."
    )
    result = {PROFILES: [], DECISIONS: []}
    for family in ("individual", "pac"):
        members = by_family.get(family, [])
        if family == "pac" and any(r["form_type"] in spec.pac_receipts and r.get("entity_type") != "PAC" for r in members):
            raise ValueError("Other-committee receipt is not source-classified as a PAC")
        profile_id = selection.source_sha256 + "/" + filing_text + "/" + family
        common = dict(profile_id=profile_id, committee_id=report["committee_id"],
            committee_name=report["committee_name"], candidate_id=candidate_id, candidate_association_basis=association,
            filing_id=filing_text, source_sha256=selection.source_sha256, source_url=source_url, captured_at=captured_at,
            period_start=report["coverage_start_date"][:10], period_end=report["coverage_end_date"][:10],
            source_cycle=report["cycle"], source_evidence_json=json_bytes(evidence).decode(), policy_version=POLICY,
            coverage_note="Complete selected report, not a complete election cycle. Recipient-side observations only. "
                "Native optional trailing-field omissions remain distinct from empty fields. Reported labels are not "
                "resolved people; no unique-donor or employer-donation claim (subject to 52 U.S.C. 30111(a)(4) "
                "sale-or-use restrictions). Current committee name is separate "
                "from the retained filing title. Coverage dates describe the report; earlier transaction dates "
                f"remain included and explicitly marked. {capture_note}")
        receipts, refunds, memos = [], [], []
        breakdown = defaultdict(lambda: {"amount": Decimal(0), "records": 0})
        for row in members:
            memo = row.get("memo_indicator") == "X"
            refund = row["form_type"].startswith("SB")
            if memo:
                memos.append(row)
            elif refund:
                if row["amount"] < 0:
                    raise ValueError("Negative refund requires source-specific qualification")
                refunds.append(row["amount"])
            else:
                receipts.append(row["amount"])
            # Keep each native observation, including repeated donor labels,
            # negative amounts, memo conduit representations and missing fields.
            donor_id = row.get("donor_committee_id") if family == "pac" else None
            donor_label = row.get("contributor_organization") or ", ".join(
                v for v in (row.get("contributor_last_name"), row.get("contributor_first_name")) if v) or None
            key = donor_id if family == "pac" else row.get("contributor_state")
            if not memo and not refund:
                with localcontext() as context:
                    context.prec = 50
                    breakdown[key]["amount"] += row["amount"]
                breakdown[key]["records"] += 1
            result[DECISIONS].append({**common, "record_id": row["record_id"], "contribution_family": family,
                "disposition": "excluded" if memo else "included",
                "disposition_reason": "source_memo_representation" if memo else "itemized_refund" if refund else
                                      "negative_receipt_adjustment" if row["amount"] < 0 else "recipient_receipt",
                "amount": format(row["amount"], "f"), "donor_id": donor_id, "donor_label": donor_label,
                "employer": row.get("employer"), "occupation": row.get("occupation"),
                "native_entity_type": row.get("entity_type"), "election_code": row.get("election_code"),
                "memo_field_status": "missing" if row.get("memo_indicator") is None else
                                     "empty" if row["memo_indicator"] == "" else "reported",
                "transaction_date_scope": "within_report_coverage" if common["period_start"] <=
                    row["transaction_date"].isoformat() <= common["period_end"] else "outside_report_coverage",
                "native_values_json": json_bytes(row).decode(), "source_locator_json": row["source_locator_json"]})
        signed, refunded = _sum(receipts), _sum(refunds)
        expected_field = "individual_itemized_contributions_period" if family == "individual" else "other_political_committee_contributions_period"
        refund_field = "refunded_individual_contributions_period" if family == "individual" else "refunded_other_political_committee_contributions_period"
        if signed != _money(report[expected_field]) or refunded != _money(report[refund_field]):
            raise ValueError("Contribution breakdown does not reconcile to the scope-matched official report")
        unitemized = _money(report["individual_unitemized_contributions_period"]) if family == "individual" else Decimal(0)
        net = _sum((signed, unitemized, -refunded))
        if family == "individual" and signed + unitemized != _money(report["total_individual_contributions_period"]):
            raise ValueError("Individual itemized/unitemized report measures do not reconcile")
        result[PROFILES].append({**common, "contribution_family": family,
            "gross_itemized_receipts": format(_sum(v for v in receipts if v >= 0), "f"),
            "negative_receipt_adjustments": format(_sum(v for v in receipts if v < 0), "f"),
            "signed_itemized_receipts": format(signed, "f"), "itemized_refunds": format(refunded, "f"),
            "net_itemized_flow": format(_sum((signed, -refunded)), "f"), "unitemized_receipts": format(unitemized, "f"),
            "net_reported_family": format(net, "f"), "amount_basis": "effective_as_filed_recipient_report_reconciled_to_official_report",
            "breakdown_json": json_bytes([{"source_donor_id" if family == "pac" else "reported_state": k,
                                           **v} for k, v in sorted(breakdown.items(), key=lambda x: x[0] or "")]).decode(),
            "included_records": len(members) - len(memos), "memo_records": len(memos)})
    return result
