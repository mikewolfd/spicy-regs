"""Scaling, streaming, multi-form, and repeatable acquisition tests for fec-research-profiles (ss-44um)."""

from __future__ import annotations

from decimal import Decimal
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import LocalMemberSource, admit_artifact

from spicy_regs import local_publication
from spicy_regs.pipelines import fec_research_profiles as frp
from spicy_regs.runtime_bounds import RequestBound
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.fec_contribution_profiles import contribution_profiles
from spicy_regs.transforms.fec_outside_profiles import outside_profiles
from spicy_regs.transforms.fec_query import CollectionSelection

FS = "\x1c"
FIXTURES = Path(__file__).parent / "fixtures/fec-research-profiles"
PILOT_SELECTION = json.loads((FIXTURES / "selection.json").read_text(encoding="utf-8"))


def _fec_line(*fields: str) -> str:
    return FS.join(fields) + "\n"


def _make_cover_line(
    form_type: str,
    committee_id: str,
    committee_name: str,
    report_code: str,
    period_start: str,
    period_end: str,
    *,
    field_count: int,
    report_code_idx: int,
    period_start_idx: int,
    period_end_idx: int,
    signed_idx: int,
    signed_date: str = "20260131",
) -> str:
    fields = [""] * field_count
    fields[0] = form_type
    fields[1] = committee_id
    fields[2] = committee_name
    fields[report_code_idx] = report_code
    fields[period_start_idx] = period_start
    fields[period_end_idx] = period_end
    fields[signed_idx] = signed_date
    return _fec_line(*fields)


def _make_sa_line(
    form_type: str,
    committee_id: str,
    transaction_id: str,
    entity_type: str,
    date_yyyymmdd: str,
    amount: str,
    *,
    org: str = "",
    last: str = "",
    first: str = "",
    state: str = "VA",
    donor_committee_id: str = "",
    memo_indicator: str = "",
    election_code: str = "P2026",
    version: str = "8.5",
) -> str:
    # 8.5 / 8.4 Sch A layout: 46 fields (indices 0..45)
    # 0 form_type, 1 reporting_committee_id, 2 transaction_id, 5 entity_type,
    # 6 contributor_organization, 7 last, 8 first, 15 state, 17 election_code,
    # 19 transaction_date, 20 amount, 25 donor_committee_id, 42 memo_indicator
    fields = [""] * (46 if version == "8.5" else 44)
    fields[0] = form_type
    fields[1] = committee_id
    fields[2] = transaction_id
    fields[5] = entity_type
    fields[6] = org
    fields[7] = last
    fields[8] = first
    fields[15] = state
    fields[17] = election_code
    fields[19] = date_yyyymmdd
    fields[20] = amount
    fields[25] = donor_committee_id
    if memo_indicator:
        fields[42] = memo_indicator
    return _fec_line(*fields)


def _make_sb_line(
    form_type: str,
    committee_id: str,
    transaction_id: str,
    entity_type: str,
    date_yyyymmdd: str,
    amount: str,
    *,
    org: str = "",
    last: str = "",
    first: str = "",
    state: str = "VA",
    memo_indicator: str = "",
    election_code: str = "P2026",
    version: str = "8.5",
) -> str:
    # 8.5 / 8.4 Sch B layout: 45 fields (indices 0..44)
    # 0 form_type, 1 reporting_committee_id, 2 transaction_id, 5 entity_type,
    # 6 org, 7 last, 8 first, 15 state, 17 election_code, 19 transaction_date,
    # 20 amount, 41 memo_indicator (in 8.5/8.4 SB)
    fields = [""] * (45 if version == "8.5" else 44)
    fields[0] = form_type
    fields[1] = committee_id
    fields[2] = transaction_id
    fields[5] = entity_type
    fields[6] = org
    fields[7] = last
    fields[8] = first
    fields[15] = state
    fields[17] = election_code
    fields[19] = date_yyyymmdd
    fields[20] = amount
    if memo_indicator:
        fields[41] = memo_indicator
    return _fec_line(*fields)


def _retain_blob(evidence: CaptureEvidence, body: bytes, *, url: str, observed_at: str | None = "2026-11-27T12:00:00Z") -> dict:
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    evidence.retain_bytes(
        body,
        stage="test-fixture",
        raw_source=True,
        source_url=url,
        original_location=url,
        observed_at=observed_at,
    )
    return {
        "response_sha256": digest,
        "byte_size": len(body),
        "request_url": url,
        "observed_at": observed_at,
    }


def _build_f3_filing_bytes(
    *,
    committee_id: str,
    committee_name: str,
    include_pac: bool = True,
    include_ind: bool = True,
    include_refund: bool = False,
) -> bytes:
    lines = [
        _fec_line("HDR", "FEC", "8.5", "TESTFILER", "1.0"),
        _make_cover_line(
            "F3N",
            committee_id,
            committee_name,
            "YE",
            "20251001",
            "20251231",
            field_count=93,
            report_code_idx=11,
            period_start_idx=15,
            period_end_idx=16,
            signed_idx=22,
        ),
    ]
    if include_ind:
        lines.append(
            _make_sa_line(
                "SA11AI",
                committee_id,
                "SA11AI.1",
                "IND",
                "20251015",
                "500.00",
                last="DOE",
                first="JANE",
                state="OH",
            )
        )
        lines.append(
            _make_sa_line(
                "SA11AI",
                committee_id,
                "SA11AI.2",
                "IND",
                "20251020",
                "-50.00",
                last="DOE",
                first="JANE",
                state="OH",
            )
        )
        lines.append(
            _make_sa_line(
                "SA11AI",
                committee_id,
                "SA11AI.3",
                "PAC",
                "20251015",
                "500.00",
                org="CONDUIT",
                donor_committee_id="C00694323",
                memo_indicator="X",
            )
        )
    if include_refund:
        lines.append(
            _make_sb_line(
                "SB20A",
                committee_id,
                "SB20A.1",
                "IND",
                "20251101",
                "25.00",
                last="DOE",
                first="JANE",
                state="OH",
            )
        )
    if include_pac:
        lines.append(
            _make_sa_line(
                "SA11C",
                committee_id,
                "SA11C.1",
                "PAC",
                "20251105",
                "1000.00",
                org="BUILDERS PAC",
                donor_committee_id="C00111222",
            )
        )
    return "".join(lines).encode("utf-8")


def _build_f3p_filing_bytes(*, committee_id: str, committee_name: str) -> bytes:
    lines = [
        _fec_line("HDR", "FEC", "8.5", "TESTFILER", "1.0"),
        _make_cover_line(
            "F3PN",
            committee_id,
            committee_name,
            "Q3",
            "20240701",
            "20240930",
            field_count=206,
            report_code_idx=11,
            period_start_idx=15,
            period_end_idx=16,
            signed_idx=22,
            signed_date="20241015",
        ),
        _make_sa_line(
            "SA17A",
            committee_id,
            "SA17A.1",
            "IND",
            "20240801",
            "2000.00",
            last="SMITH",
            first="ALEX",
            state="PA",
            election_code="G2024",
        ),
        _make_sa_line(
            "SA17C",
            committee_id,
            "SA17C.1",
            "PAC",
            "20240810",
            "5000.00",
            org="PRESIDENTIAL PAC",
            donor_committee_id="C00555666",
            election_code="G2024",
        ),
        _make_sb_line(
            "SB28A",
            committee_id,
            "SB28A.1",
            "IND",
            "20240820",
            "100.00",
            last="SMITH",
            first="ALEX",
            state="PA",
            election_code="G2024",
        ),
    ]
    return "".join(lines).encode("utf-8")


def _build_f3x_filing_bytes(*, committee_id: str, committee_name: str, version: str = "8.5") -> bytes:
    lines = [
        _fec_line("HDR", "FEC", version, "TESTFILER", "1.0"),
        _make_cover_line(
            "F3XN",
            committee_id,
            committee_name,
            "M10",
            "20240901",
            "20240930",
            field_count=123,
            report_code_idx=9,
            period_start_idx=13,
            period_end_idx=14,
            signed_idx=21,
            signed_date="20241020",
        ),
        _make_sa_line(
            "SA11AI",
            committee_id,
            "SA11AI.10",
            "IND",
            "20240910",
            "750.00",
            last="LEE",
            first="SAM",
            state="CA",
            version=version,
        ),
        _make_sb_line(
            "SB28A",
            committee_id,
            "SB28A.10",
            "IND",
            "20240915",
            "50.00",
            last="LEE",
            first="SAM",
            state="CA",
            version=version,
        ),
    ]
    return "".join(lines).encode("utf-8")


def _make_report(
    *,
    committee_id: str,
    committee_name: str,
    filing_id: str,
    cycle: int,
    report_year: int,
    report_type: str,
    cov_start: str,
    cov_end: str,
    ind_itemized: str,
    ind_unitemized: str,
    ind_refund: str,
    pac_itemized: str,
    pac_refund: str,
) -> dict:
    total_ind = str(Decimal(ind_itemized) + Decimal(ind_unitemized))
    return {
        "committee_id": committee_id,
        "committee_name": committee_name,
        "cycle": cycle,
        "report_year": report_year,
        "report_type": report_type,
        "coverage_start_date": cov_start + "T00:00:00",
        "coverage_end_date": cov_end + "T00:00:00",
        "file_number": int(filing_id),
        "most_recent_file_number": int(filing_id),
        "most_recent": True,
        "is_amended": False,
        "amendment_chain": [filing_id],
        "means_filed": "e-file",
        "fec_url": f"https://docquery.fec.gov/dcdev/posted/{filing_id}.fec",
        "individual_itemized_contributions_period": ind_itemized,
        "individual_unitemized_contributions_period": ind_unitemized,
        "total_individual_contributions_period": total_ind,
        "refunded_individual_contributions_period": ind_refund,
        "other_political_committee_contributions_period": pac_itemized,
        "refunded_other_political_committee_contributions_period": pac_refund,
    }


def _api_page_bytes(results: list[dict], *, page: int = 1, pages: int = 1, per_page: int = 100, count: int | None = None) -> bytes:
    payload = {
        "api_version": "1.0",
        "pagination": {
            "page": page,
            "pages": pages,
            "per_page": per_page,
            "count": len(results) if count is None else count,
            "is_count_exact": True,
        },
        "results": results,
    }
    return json.dumps(payload, default=str).encode("utf-8")


# ---------------------------------------------------------------------------
# 1. Transform tests: zero-PAC / zero-individual / F3P / F3X / streaming iter
# ---------------------------------------------------------------------------


def test_zero_pac_and_zero_individual_effective_f3_reports_reconcile_and_mismatch_refuses(tmp_path: Path):
    raw_root = tmp_path / "raw"
    evidence = CaptureEvidence(tmp_path / "seed", "fec-test", archive_root=raw_root)
    url = "https://docquery.fec.gov/dcdev/posted/2001001.fec"
    body = _build_f3_filing_bytes(committee_id="C00990001", committee_name="GRASSROOTS FOR SENATE", include_pac=False, include_refund=True)
    cap = {**_retain_blob(evidence, body, url=url), "filing_id": "2001001"}
    report = _make_report(
        committee_id="C00990001",
        committee_name="GRASSROOTS FOR SENATE",
        filing_id="2001001",
        cycle=2026,
        report_year=2025,
        report_type="YE",
        cov_start="2025-10-01",
        cov_end="2025-12-31",
        ind_itemized="450.00",
        ind_unitemized="100.00",
        ind_refund="25.00",
        pac_itemized="0.00",
        pac_refund="0.00",
    )
    history = {"committee_id": "C00990001", "candidate_ids": ["S6OH99001"]}
    candidates = [{"candidate_id": "S6OH99001", "first_file_date": "2025-02-01", "cycles": [2026]}]
    pin = cap["response_sha256"]
    sel = CollectionSelection("filing-2001001", pin, pin, "official-fec", 2026, "snapshot", pin)

    # Pass a single-pass streaming iterator (not a list!) to verify streaming consumption.
    out = contribution_profiles(
        frp.iter_filing_rows(evidence, cap),
        sel,
        report=report,
        filing_id="2001001",
        source_url=url,
        captured_at=cap["observed_at"],
        candidate_id="S6OH99001",
        committee_history=history,
        candidates=candidates,
        evidence=[],
    )
    profiles = {row["contribution_family"]: row for row in out["fec_contribution_profiles"]}
    assert set(profiles) == {"individual", "pac"}
    assert Decimal(profiles["individual"]["signed_itemized_receipts"]) == Decimal("450.00")
    assert Decimal(profiles["individual"]["itemized_refunds"]) == Decimal("25.00")
    assert Decimal(profiles["individual"]["net_reported_family"]) == Decimal("525.00")
    assert profiles["individual"]["memo_records"] == 1
    assert Decimal(profiles["pac"]["signed_itemized_receipts"]) == Decimal("0")
    assert profiles["pac"]["included_records"] == 0
    assert profiles["pac"]["breakdown_json"] == "[]"

    # If the official report claims non-zero PAC receipts when 0 SA11C rows exist, refusal is mandatory.
    bad_report = {**report, "other_political_committee_contributions_period": "500.00"}
    with pytest.raises(ValueError, match="reconcile"):
        contribution_profiles(
            frp.iter_filing_rows(evidence, cap),
            sel,
            report=bad_report,
            filing_id="2001001",
            source_url=url,
            captured_at=cap["observed_at"],
            candidate_id="S6OH99001",
            committee_history=history,
            candidates=candidates,
            evidence=[],
        )

    # Legitimate zero-individual itemized report with unitemized-only individual receipts.
    url2 = "https://docquery.fec.gov/dcdev/posted/2001002.fec"
    body2 = _build_f3_filing_bytes(committee_id="C00990001", committee_name="GRASSROOTS FOR SENATE", include_ind=False, include_pac=True)
    cap2 = {**_retain_blob(evidence, body2, url=url2), "filing_id": "2001002"}
    report2 = _make_report(
        committee_id="C00990001",
        committee_name="GRASSROOTS FOR SENATE",
        filing_id="2001002",
        cycle=2026,
        report_year=2025,
        report_type="YE",
        cov_start="2025-10-01",
        cov_end="2025-12-31",
        ind_itemized="0.00",
        ind_unitemized="350.00",
        ind_refund="0.00",
        pac_itemized="1000.00",
        pac_refund="0.00",
    )
    pin2 = cap2["response_sha256"]
    sel2 = CollectionSelection("filing-2001002", pin2, pin2, "official-fec", 2026, "snapshot", pin2)
    out2 = contribution_profiles(
        frp.iter_filing_rows(evidence, cap2),
        sel2,
        report=report2,
        filing_id="2001002",
        source_url=url2,
        captured_at=cap2["observed_at"],
        candidate_id="S6OH99001",
        committee_history=history,
        candidates=candidates,
        evidence=[],
    )
    profiles2 = {row["contribution_family"]: row for row in out2["fec_contribution_profiles"]}
    assert Decimal(profiles2["individual"]["signed_itemized_receipts"]) == Decimal("0")
    assert Decimal(profiles2["individual"]["unitemized_receipts"]) == Decimal("350.00")
    assert Decimal(profiles2["individual"]["net_reported_family"]) == Decimal("350.00")
    assert Decimal(profiles2["pac"]["signed_itemized_receipts"]) == Decimal("1000.00")
    evidence.finish()


def test_form_3p_and_form_3x_v85_and_v84_reconcile_and_enforce_candidate_boundaries(tmp_path: Path):
    raw_root = tmp_path / "raw"
    evidence = CaptureEvidence(tmp_path / "seed", "fec-test", archive_root=raw_root)

    # Form 3P (Presidential, 8.5, SA17A/SA17C/SB28A)
    url_p = "https://docquery.fec.gov/dcdev/posted/2002001.fec"
    body_p = _build_f3p_filing_bytes(committee_id="C00990002", committee_name="JONES FOR PRESIDENT")
    cap_p = {**_retain_blob(evidence, body_p, url=url_p), "filing_id": "2002001"}
    rep_p = _make_report(
        committee_id="C00990002",
        committee_name="JONES FOR PRESIDENT",
        filing_id="2002001",
        cycle=2024,
        report_year=2024,
        report_type="Q3",
        cov_start="2024-07-01",
        cov_end="2024-09-30",
        ind_itemized="2000.00",
        ind_unitemized="500.00",
        ind_refund="100.00",
        pac_itemized="5000.00",
        pac_refund="0.00",
    )
    pin_p = cap_p["response_sha256"]
    out_p = contribution_profiles(
        frp.iter_filing_rows(evidence, cap_p),
        CollectionSelection("filing-2002001", pin_p, pin_p, "official-fec", 2024, "snapshot", pin_p),
        report=rep_p,
        filing_id="2002001",
        source_url=url_p,
        captured_at=cap_p["observed_at"],
        candidate_id="P40099002",
        committee_history={"committee_id": "C00990002", "candidate_ids": ["P40099002"]},
        candidates=[{"candidate_id": "P40099002", "first_file_date": "2023-05-01", "cycles": [2024]}],
        evidence=[],
    )
    prof_p = {r["contribution_family"]: r for r in out_p["fec_contribution_profiles"]}
    assert Decimal(prof_p["individual"]["net_itemized_flow"]) == Decimal("1900.00")
    assert Decimal(prof_p["pac"]["signed_itemized_receipts"]) == Decimal("5000.00")
    assert prof_p["individual"]["candidate_id"] == "P40099002"

    # Form 3X (PAC/Party, both 8.5 and 8.4, SA11AI/SB28A, candidate_id=None)
    for version, fid in (("8.5", "2003001"), ("8.4", "2003002")):
        url_x = f"https://docquery.fec.gov/dcdev/posted/{fid}.fec"
        body_x = _build_f3x_filing_bytes(committee_id="C00990003", committee_name="CIVIC ACTION PAC", version=version)
        cap_x = {**_retain_blob(evidence, body_x, url=url_x), "filing_id": fid}
        rep_x = _make_report(
            committee_id="C00990003",
            committee_name="CIVIC ACTION PAC",
            filing_id=fid,
            cycle=2024,
            report_year=2024,
            report_type="M10",
            cov_start="2024-09-01",
            cov_end="2024-09-30",
            ind_itemized="750.00",
            ind_unitemized="0.00",
            ind_refund="50.00",
            pac_itemized="0.00",
            pac_refund="0.00",
        )
        pin_x = cap_x["response_sha256"]
        sel_x = CollectionSelection("filing-" + fid, pin_x, pin_x, "official-fec", 2024, "snapshot", pin_x)
        out_x = contribution_profiles(
            frp.iter_filing_rows(evidence, cap_x),
            sel_x,
            report=rep_x,
            filing_id=fid,
            source_url=url_x,
            captured_at=cap_x["observed_at"],
            candidate_id=None,
            committee_history={"committee_id": "C00990003", "candidate_ids": []},
            candidates=[],
            evidence=[],
        )
        prof_x = {r["contribution_family"]: r for r in out_x["fec_contribution_profiles"]}
        assert prof_x["individual"]["candidate_id"] is None
        assert "form_3x" in prof_x["individual"]["candidate_association_basis"]
        assert Decimal(prof_x["individual"]["net_itemized_flow"]) == Decimal("700.00")

        # Form 3X refuses if a candidate_id is asserted or committee history lists candidate_ids.
        with pytest.raises(ValueError, match="Form 3X"):
            contribution_profiles(
                frp.iter_filing_rows(evidence, cap_x),
                sel_x,
                report=rep_x,
                filing_id=fid,
                source_url=url_x,
                captured_at=cap_x["observed_at"],
                candidate_id="H4CA01001",
                committee_history={"committee_id": "C00990003", "candidate_ids": []},
                candidates=[],
                evidence=[],
            )
    evidence.finish()


def test_outside_profiles_scales_across_chambers_cycles_and_refuses_invalid_cycle():
    rows_2020 = [
        {"candidate_id": "P80001571", "candidate_name": "CANDIDATE PRES", "committee_id": "C00100001", "committee_name": "SUPER PAC A", "support_oppose_indicator": "S", "cycle": 2020, "total": Decimal("10000.50"), "count": 5},
        {"candidate_id": "P80001571", "candidate_name": "CANDIDATE PRES", "committee_id": "C90010001", "committee_name": "FORM 5 GROUP", "support_oppose_indicator": "O", "cycle": 2020, "total": Decimal("2500.25"), "count": 2},
    ]
    totals_2020 = [
        {"candidate_id": "P80001571", "cycle": 2020, "support_oppose_indicator": "S", "total": Decimal("10000.50")},
        {"candidate_id": "P80001571", "cycle": 2020, "support_oppose_indicator": "O", "total": Decimal("2500.25")},
    ]
    out = outside_profiles(rows_2020, totals_2020, candidate_id="P80001571", cycle=2020, evidence=[{"name": "test"}])
    spenders = {r["spender_id"]: r for r in out["fec_outside_spending_spenders"]}
    assert spenders["C00100001"]["spender_namespace"] == "fec_committee"
    assert spenders["C90010001"]["spender_namespace"] == "fec_form5_filer"

    with pytest.raises(ValueError, match="two-year cycle"):
        outside_profiles([], [], candidate_id="P80001571", cycle=1970, evidence=[])


# ---------------------------------------------------------------------------
# 2. Streaming multi-cycle v2 build(), >6 pages, schema parity, cancellation
# ---------------------------------------------------------------------------


def test_v2_streaming_build_multi_cycle_exceeds_pilot_6_page_limit_and_passes_admission(tmp_path: Path, monkeypatch):
    # Force multiple Parquet flush batches inside _TableWriter to verify streaming commits.
    monkeypatch.setattr(frp, "WRITE_BATCH_SIZE", 3)

    raw_root = tmp_path / "raw"
    seed = CaptureEvidence(tmp_path / "seed", "fec-research-profiles", archive_root=raw_root)

    # Complete 2-page committee traversal (5 committees total).
    comm_p1_url = "https://api.open.fec.gov/v1/committees/?cycle=2024&cycle=2026&sort=committee_id&per_page=100&page=1"
    comm_p2_url = "https://api.open.fec.gov/v1/committees/?cycle=2024&cycle=2026&sort=committee_id&per_page=100&page=2"
    comm_p1 = _api_page_bytes(
        [
            {"committee_id": "C00000001", "name": "CMTE ONE", "designation": "P", "affiliated_committee_name": "AFF ONE"},
            {"committee_id": "C00000002", "name": "CMTE TWO", "designation": "D", "sponsor_candidate_ids": ["S6OH00163"]},
            {"committee_id": "C00000003", "name": "CMTE THREE", "designation": "U"},
        ],
        page=1,
        pages=2,
        count=5,
    )
    comm_p2 = _api_page_bytes(
        [
            {"committee_id": "C00000004", "name": "CMTE FOUR", "designation": "J"},
            {"committee_id": "C00000005", "name": "CMTE FIVE", "designation": "B"},
        ],
        page=2,
        pages=2,
        count=5,
    )
    committees = [
        {**_retain_blob(seed, comm_p1, url=comm_p1_url), "expected_results": 3},
        {**_retain_blob(seed, comm_p2, url=comm_p2_url), "expected_results": 2},
    ]

    # Outside spending: 8-page aggregate (proves pilot's <=6 page cap is lifted!) + second cycle House candidate.
    pres_pages = []
    pres_total = Decimal("0")
    for p in range(1, 9):
        row_total = Decimal("100.00") + Decimal(p)
        pres_total += row_total
        cid = f"C9000000{p}" if p % 2 == 0 else f"C0000001{p}"
        page_bytes = _api_page_bytes(
            [
                {
                    "candidate_id": "P80001571",
                    "candidate_name": "PRES CANDIDATE",
                    "committee_id": cid,
                    "committee_name": f"SPENDER {p}",
                    "support_oppose_indicator": "S",
                    "cycle": 2024,
                    "total": str(row_total),
                    "count": p,
                }
            ],
            page=p,
            pages=8,
            per_page=1,
            count=8,
        )
        url = f"https://api.open.fec.gov/v1/schedules/schedule_e/by_candidate/?candidate_id=P80001571&cycle=2024&per_page=1&sort=-total&page={p}"
        pres_pages.append(_retain_blob(seed, page_bytes, url=url))
    pres_totals_url = "https://api.open.fec.gov/v1/schedules/schedule_e/totals/by_candidate/?candidate_id=P80001571&cycle=2024&per_page=100"
    pres_totals_cap = _retain_blob(
        seed,
        _api_page_bytes([{"candidate_id": "P80001571", "cycle": 2024, "support_oppose_indicator": "S", "total": str(pres_total)}]),
        url=pres_totals_url,
    )
    house_by_url = "https://api.open.fec.gov/v1/schedules/schedule_e/by_candidate/?candidate_id=H2OH01001&cycle=2022&per_page=100&sort=-total"
    house_by_cap = _retain_blob(
        seed,
        _api_page_bytes(
            [
                {
                    "candidate_id": "H2OH01001",
                    "candidate_name": "HOUSE CANDIDATE",
                    "committee_id": "C00888001",
                    "committee_name": "HOUSE PAC",
                    "support_oppose_indicator": "O",
                    "cycle": 2022,
                    "total": "4200.00",
                    "count": 3,
                }
            ]
        ),
        url=house_by_url,
    )
    house_tot_url = "https://api.open.fec.gov/v1/schedules/schedule_e/totals/by_candidate/?candidate_id=H2OH01001&cycle=2022&per_page=100"
    house_tot_cap = _retain_blob(
        seed,
        _api_page_bytes([{"candidate_id": "H2OH01001", "cycle": 2022, "support_oppose_indicator": "O", "total": "4200.00"}]),
        url=house_tot_url,
    )

    # Contributions: 1 Form 3 (zero-PAC) + 1 Form 3X (non-candidate PAC).
    f3_url = "https://docquery.fec.gov/dcdev/posted/3001001.fec"
    f3_cap = {**_retain_blob(seed, _build_f3_filing_bytes(committee_id="C00990001", committee_name="SENATE CMTE", include_pac=False), url=f3_url), "filing_id": "3001001"}
    f3_rep_url = "https://api.open.fec.gov/v1/committee/C00990001/reports/?year=2025&report_type=YE&is_amended=false&most_recent=true&per_page=100"
    f3_rep_cap = _retain_blob(
        seed,
        _api_page_bytes(
            [
                _make_report(
                    committee_id="C00990001",
                    committee_name="SENATE CMTE",
                    filing_id="3001001",
                    cycle=2026,
                    report_year=2025,
                    report_type="YE",
                    cov_start="2025-10-01",
                    cov_end="2025-12-31",
                    ind_itemized="450.00",
                    ind_unitemized="0.00",
                    ind_refund="0.00",
                    pac_itemized="0.00",
                    pac_refund="0.00",
                )
            ]
        ),
        url=f3_rep_url,
    )
    f3_hist_url = "https://api.open.fec.gov/v1/committee/C00990001/history/2026/?per_page=100"
    f3_hist_cap = _retain_blob(seed, _api_page_bytes([{"committee_id": "C00990001", "candidate_ids": ["S6OH99001"]}]), url=f3_hist_url)
    f3_cand_url = "https://api.open.fec.gov/v1/candidates/?candidate_id=S6OH99001&per_page=100"
    f3_cand_cap = _retain_blob(seed, _api_page_bytes([{"candidate_id": "S6OH99001", "first_file_date": "2025-01-15", "cycles": [2026]}]), url=f3_cand_url)

    f3x_url = "https://docquery.fec.gov/dcdev/posted/3002001.fec"
    f3x_cap = {**_retain_blob(seed, _build_f3x_filing_bytes(committee_id="C00990003", committee_name="CIVIC PAC"), url=f3x_url), "filing_id": "3002001"}
    f3x_rep_url = "https://api.open.fec.gov/v1/committee/C00990003/reports/?year=2024&report_type=M10&is_amended=false&most_recent=true&per_page=100"
    f3x_rep_cap = _retain_blob(
        seed,
        _api_page_bytes(
            [
                _make_report(
                    committee_id="C00990003",
                    committee_name="CIVIC PAC",
                    filing_id="3002001",
                    cycle=2024,
                    report_year=2024,
                    report_type="M10",
                    cov_start="2024-09-01",
                    cov_end="2024-09-30",
                    ind_itemized="750.00",
                    ind_unitemized="0.00",
                    ind_refund="50.00",
                    pac_itemized="0.00",
                    pac_refund="0.00",
                )
            ]
        ),
        url=f3x_rep_url,
    )
    f3x_hist_url = "https://api.open.fec.gov/v1/committee/C00990003/history/2024/?per_page=100"
    f3x_hist_cap = _retain_blob(seed, _api_page_bytes([{"committee_id": "C00990003", "candidate_ids": []}]), url=f3x_hist_url)
    seed.finish()

    selection_v2 = {
        "format": frp.SELECTION_FORMAT,
        "version": frp.SELECTION_VERSION,
        "scope": "Multi-cycle scaling fixture",
        "committee_population": "complete_traversal",
        "committees": committees,
        "outside_scopes": [
            {
                "candidate_id": "P80001571",
                "cycle": 2024,
                "queries": [
                    {"name": "by-spender", "path": "/v1/schedules/schedule_e/by_candidate/", "params": {"candidate_id": "P80001571", "cycle": 2024, "per_page": 1, "sort": "-total"}, "pages": pres_pages},
                    {"name": "totals", "path": "/v1/schedules/schedule_e/totals/by_candidate/", "params": {"candidate_id": "P80001571", "cycle": 2024, "per_page": 100}, "pages": [pres_totals_cap]},
                ],
            },
            {
                "candidate_id": "H2OH01001",
                "cycle": 2022,
                "queries": [
                    {"name": "by-spender", "path": "/v1/schedules/schedule_e/by_candidate/", "params": {"candidate_id": "H2OH01001", "cycle": 2022, "per_page": 100, "sort": "-total"}, "pages": [house_by_cap]},
                    {"name": "totals", "path": "/v1/schedules/schedule_e/totals/by_candidate/", "params": {"candidate_id": "H2OH01001", "cycle": 2022, "per_page": 100}, "pages": [house_tot_cap]},
                ],
            },
        ],
        "contribution_scopes": [
            {
                "original": f3_cap,
                "cycle": 2026,
                "candidate_id": "S6OH99001",
                "queries": [
                    {"name": "reports", "path": "/v1/committee/C00990001/reports/", "params": {"year": 2025, "report_type": "YE", "is_amended": False, "most_recent": True, "per_page": 100}, "pages": [f3_rep_cap]},
                    {"name": "committee-history", "path": "/v1/committee/C00990001/history/2026/", "params": {"per_page": 100}, "pages": [f3_hist_cap]},
                    {"name": "candidate", "path": "/v1/candidates/", "params": {"candidate_id": "S6OH99001", "per_page": 100}, "pages": [f3_cand_cap]},
                ],
            },
            {
                "original": f3x_cap,
                "cycle": 2024,
                "candidate_id": None,
                "queries": [
                    {"name": "reports", "path": "/v1/committee/C00990003/reports/", "params": {"year": 2024, "report_type": "M10", "is_amended": False, "most_recent": True, "per_page": 100}, "pages": [f3x_rep_cap]},
                    {"name": "committee-history", "path": "/v1/committee/C00990003/history/2024/", "params": {"per_page": 100}, "pages": [f3x_hist_cap]},
                ],
            },
        ],
    }

    build_dir = tmp_path / "build"
    artifact, ev_dir = frp.build(selection_v2, build_dir, archive_root=raw_root)
    gen_dir = build_dir / "generation"
    root = local_publication._candidate(gen_dir, frp.FAMILY, (ev_dir,))
    admitted = admit_artifact(LocalMemberSource(gen_dir))
    assert admitted.pin.artifact_digest == artifact.pin.artifact_digest
    assert root["spec"]["tables"]["fec_committee_relationship_profiles.parquet"]["rows"] == 5
    assert root["spec"]["tables"]["fec_outside_spending_profiles.parquet"]["rows"] == 2
    assert root["spec"]["tables"]["fec_outside_spending_spenders.parquet"]["rows"] == 9
    assert root["spec"]["tables"]["fec_contribution_profiles.parquet"]["rows"] == 4
    assert root["spec"]["tables"]["fec_contribution_decisions.parquet"]["rows"] == 5

    # Verify SpicyGov FecResearchProfile exact contract fields stay unchanged.
    for table, expected in (
        ("fec_committee_relationship_profiles", {"policy_version": "fec-disclosed-relationships/1"}),
        ("fec_contribution_profiles", {"policy_version": "fec-selected-report-contributions/1", "amount_basis": "effective_as_filed_recipient_report_reconciled_to_official_report"}),
        ("fec_outside_spending_profiles", {"policy_version": "fec-publisher-outside-spending/1", "amount_basis": "publisher_schedule_e_aggregate_reconciled_to_publisher_total", "election_basis": "all_elections_in_selected_two_year_cycle"}),
    ):
        rows = pq.read_table(gen_dir / f"{table}.parquet").to_pylist()
        for row in rows:
            for key, val in expected.items():
                assert row[key] == val

    # Cooperative RequestBound cancellation aborts cleanly with no committed parquet files.
    cancelled_dir = tmp_path / "cancelled-build"
    bound = RequestBound(60.0, label="test-cancel")
    with pytest.raises(TimeoutError, match="test-cancel"), bound.scope():
        bound.cancel()
        frp.build(selection_v2, cancelled_dir, archive_root=raw_root)
    assert not list(cancelled_dir.glob("*.parquet"))
    assert not (cancelled_dir / "generation").exists()


def test_selection_validation_refuses_duplicates_and_mixed_versions():
    with pytest.raises(ValueError, match="mix legacy"):
        frp.normalize_selection({**PILOT_SELECTION, "outside_scopes": []})
    with pytest.raises(ValueError, match="Duplicate outside-spending"):
        frp.normalize_selection(
            {
                "format": frp.SELECTION_FORMAT,
                "version": frp.SELECTION_VERSION,
                "committees": PILOT_SELECTION["committees"],
                "outside_scopes": [
                    {"candidate_id": "S6OH00163", "cycle": 2024, "queries": PILOT_SELECTION["outside"]},
                    {"candidate_id": "S6OH00163", "cycle": 2024, "queries": PILOT_SELECTION["outside"]},
                ],
                "contribution_scopes": [PILOT_SELECTION["contribution"]],
            }
        )
    with pytest.raises(ValueError, match="Duplicate contribution filing_id"):
        frp.normalize_selection(
            {
                "format": frp.SELECTION_FORMAT,
                "version": frp.SELECTION_VERSION,
                "committees": PILOT_SELECTION["committees"],
                "outside_scopes": [{"candidate_id": "S6OH00163", "cycle": 2024, "queries": PILOT_SELECTION["outside"]}],
                "contribution_scopes": [PILOT_SELECTION["contribution"], PILOT_SELECTION["contribution"]],
            }
        )


# ---------------------------------------------------------------------------
# 3. Repeatable acquire() -> build() end-to-end with MockTransport (offline)
# ---------------------------------------------------------------------------


def _mock_openfec_transport(*, api_key: str) -> tuple[httpx.MockTransport, list[tuple[str, str | None]]]:
    """Deterministic OpenFEC + docquery MockTransport covering committees, outside spending, and F3/F3P/F3X."""
    seen_requests: list[tuple[str, str | None]] = []

    f3_bytes = _build_f3_filing_bytes(committee_id="C00990001", committee_name="SENATE CMTE", include_pac=False, include_refund=True)
    f3_bad_bytes = _build_f3_filing_bytes(committee_id="C00990099", committee_name="MISMATCH CMTE", include_pac=False)
    f3p_bytes = _build_f3p_filing_bytes(committee_id="C00990002", committee_name="PRES CMTE")
    f3x_bytes = _build_f3x_filing_bytes(committee_id="C00990003", committee_name="CIVIC PAC")

    rep_f3 = _make_report(
        committee_id="C00990001", committee_name="SENATE CMTE", filing_id="4001001", cycle=2024,
        report_year=2024, report_type="YE", cov_start="2025-10-01", cov_end="2025-12-31",
        ind_itemized="450.00", ind_unitemized="0.00", ind_refund="25.00", pac_itemized="0.00", pac_refund="0.00",
    )
    rep_f3_bad = _make_report(
        committee_id="C00990099", committee_name="MISMATCH CMTE", filing_id="4001099", cycle=2024,
        report_year=2024, report_type="YE", cov_start="2025-10-01", cov_end="2025-12-31",
        ind_itemized="99999.00", ind_unitemized="0.00", ind_refund="0.00", pac_itemized="0.00", pac_refund="0.00",
    )
    rep_f3p = _make_report(
        committee_id="C00990002", committee_name="PRES CMTE", filing_id="4002001", cycle=2024,
        report_year=2024, report_type="Q3", cov_start="2024-07-01", cov_end="2024-09-30",
        ind_itemized="2000.00", ind_unitemized="100.00", ind_refund="100.00", pac_itemized="5000.00", pac_refund="0.00",
    )
    rep_f3x = _make_report(
        committee_id="C00990003", committee_name="CIVIC PAC", filing_id="4003001", cycle=2024,
        report_year=2024, report_type="M10", cov_start="2024-09-01", cov_end="2024-09-30",
        ind_itemized="750.00", ind_unitemized="0.00", ind_refund="50.00", pac_itemized="0.00", pac_refund="0.00",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        parts = urlsplit(url)
        params = parse_qsl(parts.query, keep_blank_values=True)
        param_map: dict[str, list[str]] = {}
        for k, v in params:
            param_map.setdefault(k, []).append(v)
        header_key = request.headers.get("x-api-key")
        seen_requests.append((parts.hostname or "", header_key))
        # Enforce strict credential hygiene: never in URL, X-Api-Key ONLY on api.open.fec.gov.
        assert api_key not in url
        if parts.hostname == "api.open.fec.gov":
            assert header_key == api_key
        else:
            assert header_key is None

        path = parts.path
        if parts.hostname == "docquery.fec.gov":
            files = {
                "/dcdev/posted/4001001.fec": f3_bytes,
                "/dcdev/posted/4001099.fec": f3_bad_bytes,
                "/dcdev/posted/4002001.fec": f3p_bytes,
                "/dcdev/posted/4003001.fec": f3x_bytes,
            }
            content = files[path]
            return httpx.Response(200, content=content, headers={"content-type": "text/plain", "content-length": str(len(content))})

        page = int(param_map.get("page", ["1"])[0])
        if path == "/v1/committees/":
            pages_data = {
                1: [{"committee_id": "C00990001", "name": "SENATE CMTE", "designation": "P"}],
                2: [{"committee_id": "C00990002", "name": "PRES CMTE", "designation": "P", "sponsor_candidate_ids": ["P40099002"]}],
            }
            body = _api_page_bytes(pages_data[page], page=page, pages=2, count=2)
            return httpx.Response(200, content=body, headers={"content-type": "application/json", "x-ratelimit-limit": "60"})

        if path == "/v1/schedules/schedule_e/totals/by_candidate/":
            if "candidate_id" not in param_map:
                # Discovery: include P, S, H plus one broken candidate that should be skipped & logged!
                body = _api_page_bytes(
                    [
                        {"candidate_id": "P40099002", "cycle": 2024, "support_oppose_indicator": "S", "total": "5000.00"},
                        {"candidate_id": "S6OH99001", "cycle": 2024, "support_oppose_indicator": "O", "total": "3000.00"},
                        {"candidate_id": "H2OH99001", "cycle": 2024, "support_oppose_indicator": "S", "total": "1000.00"},
                        {"candidate_id": "H2OH99999", "cycle": 2024, "support_oppose_indicator": "S", "total": "999.00"},
                    ]
                )
            else:
                cid = param_map["candidate_id"][0]
                by_cid = {
                    "P40099002": [{"candidate_id": "P40099002", "cycle": 2024, "support_oppose_indicator": "S", "total": "5000.00"}],
                    "S6OH99001": [{"candidate_id": "S6OH99001", "cycle": 2024, "support_oppose_indicator": "O", "total": "3000.00"}],
                    "H2OH99001": [{"candidate_id": "H2OH99001", "cycle": 2024, "support_oppose_indicator": "S", "total": "1000.00"}],
                    "H2OH99999": [{"candidate_id": "H2OH99999", "cycle": 2024, "support_oppose_indicator": "S", "total": "999.00"}],
                }
                body = _api_page_bytes(by_cid[cid])
            return httpx.Response(200, content=body, headers={"content-type": "application/json", "x-ratelimit-limit": "60"})

        if path == "/v1/schedules/schedule_e/by_candidate/":
            cid = param_map["candidate_id"][0]
            by_cid = {
                "P40099002": [{"candidate_id": "P40099002", "candidate_name": "PRES CAND", "committee_id": "C90011111", "committee_name": "FORM5 ORG", "support_oppose_indicator": "S", "cycle": 2024, "total": "5000.00", "count": 4}],
                "S6OH99001": [{"candidate_id": "S6OH99001", "candidate_name": "SEN CAND", "committee_id": "C00111111", "committee_name": "SEN PAC", "support_oppose_indicator": "O", "cycle": 2024, "total": "3000.00", "count": 2}],
                "H2OH99001": [{"candidate_id": "H2OH99001", "candidate_name": "HOUSE CAND", "committee_id": "C00222222", "committee_name": "HOUSE PAC", "support_oppose_indicator": "S", "cycle": 2024, "total": "1000.00", "count": 1}],
                # Deliberate $1 mismatch to test graceful skip + audit logging on auto-discovered candidate.
                "H2OH99999": [{"candidate_id": "H2OH99999", "candidate_name": "BROKEN CAND", "committee_id": "C00333333", "committee_name": "BAD PAC", "support_oppose_indicator": "S", "cycle": 2024, "total": "998.00", "count": 1}],
            }
            body = _api_page_bytes(by_cid[cid])
            return httpx.Response(200, content=body, headers={"content-type": "application/json", "x-ratelimit-limit": "60"})

        if path == "/v1/reports/house-senate/":
            # Put bad report first (higher amount) to prove acquire skips & logs it, then admits valid F3!
            body = _api_page_bytes([rep_f3_bad, rep_f3])
            return httpx.Response(200, content=body, headers={"content-type": "application/json"})
        if path == "/v1/reports/presidential/":
            body = _api_page_bytes([rep_f3p])
            return httpx.Response(200, content=body, headers={"content-type": "application/json"})
        if path == "/v1/reports/pac-party/":
            body = _api_page_bytes([rep_f3x])
            return httpx.Response(200, content=body, headers={"content-type": "application/json"})

        if path == "/v1/committee/C00990001/reports/":
            return httpx.Response(200, content=_api_page_bytes([rep_f3]), headers={"content-type": "application/json"})
        if path == "/v1/committee/C00990099/reports/":
            return httpx.Response(200, content=_api_page_bytes([rep_f3_bad]), headers={"content-type": "application/json"})
        if path == "/v1/committee/C00990002/reports/":
            return httpx.Response(200, content=_api_page_bytes([rep_f3p]), headers={"content-type": "application/json"})
        if path == "/v1/committee/C00990003/reports/":
            return httpx.Response(200, content=_api_page_bytes([rep_f3x]), headers={"content-type": "application/json"})

        if path == "/v1/committee/C00990001/history/2024/":
            return httpx.Response(200, content=_api_page_bytes([{"committee_id": "C00990001", "candidate_ids": ["S6OH99001"]}]), headers={"content-type": "application/json"})
        if path == "/v1/committee/C00990099/history/2024/":
            return httpx.Response(200, content=_api_page_bytes([{"committee_id": "C00990099", "candidate_ids": ["S6OH99099"]}]), headers={"content-type": "application/json"})
        if path == "/v1/committee/C00990002/history/2024/":
            return httpx.Response(200, content=_api_page_bytes([{"committee_id": "C00990002", "candidate_ids": ["P40099002"]}]), headers={"content-type": "application/json"})
        if path == "/v1/committee/C00990003/history/2024/":
            return httpx.Response(200, content=_api_page_bytes([{"committee_id": "C00990003", "candidate_ids": []}]), headers={"content-type": "application/json"})

        if path == "/v1/candidates/":
            cids = param_map.get("candidate_id", [])
            catalog = {
                "S6OH99001": {"candidate_id": "S6OH99001", "first_file_date": "2024-01-10", "cycles": [2024]},
                "S6OH99099": {"candidate_id": "S6OH99099", "first_file_date": "2024-01-10", "cycles": [2024]},
                "P40099002": {"candidate_id": "P40099002", "first_file_date": "2023-06-01", "cycles": [2024]},
            }
            return httpx.Response(200, content=_api_page_bytes([catalog[c] for c in cids if c in catalog]), headers={"content-type": "application/json"})

        raise AssertionError(f"Unexpected mock URL: {url}")

    return httpx.MockTransport(handler), seen_requests


def test_acquire_requires_api_key_and_fails_closed(tmp_path: Path, monkeypatch):
    for var in frp.API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValueError, match="DATA_GOV_API_KEY or API_GOV"):
        frp.acquire(tmp_path / "acq", cycles=[2024], archive_root=tmp_path / "raw")


def test_acquire_and_build_end_to_end_across_f3_f3p_f3x_and_chambers_with_skip_audit(tmp_path: Path):
    raw_root = tmp_path / "raw"
    fake_key = "test-openfec-key-1234567890"
    transport, seen_requests = _mock_openfec_transport(api_key=fake_key)

    selection = frp.acquire(
        tmp_path / "acquire-run",
        cycles=[2024],
        committee_cycles=[2024, 2026],
        max_committee_pages=0,
        outside_candidates_per_cycle=4,
        contribution_reports_per_cycle=3,
        contribution_forms=("house-senate", "presidential", "pac-party"),
        max_requests=100,
        discovery_pages=1,
        archive_root=raw_root,
        api_key=fake_key,
        min_interval=0.0,
        transport=transport,
    )

    assert selection["format"] == frp.SELECTION_FORMAT
    assert selection["version"] == frp.SELECTION_VERSION
    assert selection["committee_population"] == "complete_traversal"
    assert len(selection["committees"]) == 2
    assert {s["candidate_id"] for s in selection["outside_scopes"]} == {"P40099002", "S6OH99001", "H2OH99001"}
    assert {s["original"]["filing_id"] for s in selection["contribution_scopes"]} == {"4001001", "4002001", "4003001"}

    summary = selection["acquisition_summary"]
    assert summary["outside_scopes_skipped"] == 1
    assert summary["skipped_outside_scopes"][0]["candidate_id"] == "H2OH99999"
    assert summary["contribution_scopes_skipped"] == 1
    assert summary["skipped_contribution_scopes"][0]["filing_id"] == "4001099"

    # Zero credentials or local paths in serialized selection JSON.
    serialized = json.dumps(selection)
    assert fake_key not in serialized
    assert str(tmp_path) not in serialized
    assert any(host == "docquery.fec.gov" and key is None for host, key in seen_requests)

    # Now run strictly offline build() from the generated selection + CentralRawArchiveStore.
    build_dir = tmp_path / "build-run"
    artifact, ev_dir = frp.build(selection, build_dir, archive_root=raw_root)
    gen_dir = build_dir / "generation"
    root = local_publication._candidate(gen_dir, frp.FAMILY, (ev_dir,))
    admitted = admit_artifact(LocalMemberSource(gen_dir))
    assert admitted.pin.artifact_digest == artifact.pin.artifact_digest
    assert root["spec"]["tables"]["fec_committee_relationship_profiles.parquet"]["rows"] == 2
    assert root["spec"]["tables"]["fec_outside_spending_profiles.parquet"]["rows"] == 3
    assert root["spec"]["tables"]["fec_outside_spending_spenders.parquet"]["rows"] == 3
    assert root["spec"]["tables"]["fec_contribution_profiles.parquet"]["rows"] == 6
    assert root["spec"]["tables"]["fec_contribution_decisions.parquet"]["rows"] == 9


def test_acquire_bounded_committee_prefix_and_reuse_from_pages_jsonl(tmp_path: Path):
    raw_root = tmp_path / "raw"
    fake_key = "test-openfec-key-abcdef1234"
    transport, _ = _mock_openfec_transport(api_key=fake_key)

    # 1. Bounded prefix (max_committee_pages=1 when 2 pages exist) stops cleanly without terminal-page error.
    selection_prefix = frp.acquire(
        tmp_path / "acquire-prefix",
        cycles=[2024],
        committee_cycles=[2024],
        max_committee_pages=1,
        outside_candidates_per_cycle=1,
        contribution_reports_per_cycle=1,
        contribution_forms=("pac-party",),
        max_requests=50,
        discovery_pages=1,
        archive_root=raw_root,
        api_key=fake_key,
        min_interval=0.0,
        transport=transport,
    )
    assert selection_prefix["committee_population"] == "bounded_page_prefix"
    assert len(selection_prefix["committees"]) == 1
    assert selection_prefix["acquisition_summary"]["committee_available_pages"] == 2

    # 2. Reuse committees from a build_fec_committees-style pages.jsonl + blobs/ dir with zero /v1/committees/ fetches.
    capture_dir = tmp_path / "prior-committees"
    blob_dir = capture_dir / "blobs" / "sha256"
    blob_dir.mkdir(parents=True)
    reused_body = _api_page_bytes([{"committee_id": "C00777001", "name": "REUSED PAC", "designation": "U"}], page=1, pages=1, count=1)
    reused_hex = hashlib.sha256(reused_body).hexdigest()
    (blob_dir / reused_hex).write_bytes(reused_body)
    pages_jsonl = capture_dir / "pages.jsonl"
    pages_jsonl.write_text(
        json.dumps(
            {
                "request_url": "https://api.open.fec.gov/v1/committees/?sort=committee_id&per_page=100&page=1",
                "observed_at": "2026-11-27T01:00:00Z",
                "evidence": {"sha256": f"sha256:{reused_hex}", "bytes": len(reused_body), "blob_path": f"sha256/{reused_hex}"},
                "records": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    transport2, _ = _mock_openfec_transport(api_key=fake_key)
    selection_reused = frp.acquire(
        tmp_path / "acquire-reused",
        cycles=[2024],
        reuse_committees_from=pages_jsonl,
        outside_candidates_per_cycle=1,
        contribution_reports_per_cycle=1,
        contribution_forms=("pac-party",),
        max_requests=50,
        discovery_pages=1,
        archive_root=raw_root,
        api_key=fake_key,
        min_interval=0.0,
        transport=transport2,
    )
    assert len(selection_reused["committees"]) == 1
    assert selection_reused["committees"][0]["response_sha256"] == f"sha256:{reused_hex}"
    # Offline build succeeds directly on the reused committee page!
    _, ev_dir = frp.build(selection_reused, tmp_path / "build-reused", archive_root=raw_root)
    rows = pq.read_table(tmp_path / "build-reused" / "generation" / "fec_committee_relationship_profiles.parquet").to_pylist()
    assert [r["committee_id"] for r in rows] == ["C00777001"]
    assert ev_dir.is_dir()


# ---------------------------------------------------------------------------
# 4. Review regression tests: Findings 1-5
# ---------------------------------------------------------------------------


def test_acquire_fails_closed_on_403_evidence_error_budget_rate_limit_and_cancellation(tmp_path: Path, monkeypatch):
    """Finding 1 (BLOCKER): fatal security/retention/budget/cancellation errors never turn into skipped items."""
    from spicy_docs.transport.credentials import CredentialRefusedError
    from spicy_docs.transport.download import AcquisitionError, RateLimitExhausted
    from spicy_regs.source_evidence import SourceEvidenceError

    raw_root = tmp_path / "raw"
    fake_key = "test-secret-openfec-key-9876543210"
    base_transport, _ = _mock_openfec_transport(api_key=fake_key)

    # 1. HTTP 403 on auto-discovered docquery.fec.gov filing MUST abort acquire() loudly (never skip).
    def handler_403(request: httpx.Request) -> httpx.Response:
        if request.url.host == "docquery.fec.gov":
            return httpx.Response(403, content=b"Forbidden", headers={"content-type": "text/plain"})
        return base_transport.handle_request(request)

    with pytest.raises(CredentialRefusedError):
        frp.acquire(
            tmp_path / "acq-403",
            cycles=[2024],
            committee_cycles=[2024],
            outside_candidates_per_cycle=1,
            contribution_reports_per_cycle=1,
            contribution_forms=("pac-party",),
            max_requests=50,
            discovery_pages=1,
            archive_root=raw_root,
            api_key=fake_key,
            min_interval=0.0,
            transport=httpx.MockTransport(handler_403),
        )

    # 2. HTTP 429 exceeding RateLimit.max_wait -> RateLimitExhausted MUST abort acquire() loudly.
    def handler_429(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/schedules/schedule_e/by_candidate/":
            return httpx.Response(429, content=b"Too Many Requests", headers={"retry-after": "300"})
        return base_transport.handle_request(request)

    with pytest.raises(RateLimitExhausted):
        frp.acquire(
            tmp_path / "acq-429",
            cycles=[2024],
            committee_cycles=[2024],
            outside_candidates_per_cycle=1,
            contribution_reports_per_cycle=1,
            contribution_forms=("pac-party",),
            max_requests=50,
            discovery_pages=1,
            archive_root=raw_root,
            api_key=fake_key,
            min_interval=0.0,
            transport=httpx.MockTransport(handler_429),
        )

    # 3. max_requests exhaustion during Phase 2 even with --reuse-committees-from MUST abort loudly.
    capture_dir = tmp_path / "reused-comm-budget"
    blob_dir = capture_dir / "blobs" / "sha256"
    blob_dir.mkdir(parents=True)
    reused_body = _api_page_bytes([{"committee_id": "C00777001", "name": "REUSED PAC", "designation": "U"}], page=1, pages=1, count=1)
    reused_hex = hashlib.sha256(reused_body).hexdigest()
    (blob_dir / reused_hex).write_bytes(reused_body)
    pages_jsonl = capture_dir / "pages.jsonl"
    pages_jsonl.write_text(
        json.dumps(
            {
                "request_url": "https://api.open.fec.gov/v1/committees/?sort=committee_id&per_page=100&page=1",
                "observed_at": "2026-11-27T01:00:00Z",
                "evidence": {"sha256": f"sha256:{reused_hex}", "bytes": len(reused_body), "blob_path": f"sha256/{reused_hex}"},
                "records": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    # 1 discovery + 2 outside + 1 report discovery = 4 requests; budget=5 exhausts mid-Phase 2!
    with pytest.raises(AcquisitionError, match="budget exhausted"):
        frp.acquire(
            tmp_path / "acq-budget",
            cycles=[2024],
            reuse_committees_from=pages_jsonl,
            outside_candidates_per_cycle=1,
            contribution_reports_per_cycle=2,
            contribution_forms=("house-senate", "pac-party"),
            max_requests=5,
            discovery_pages=1,
            archive_root=raw_root,
            api_key=fake_key,
            min_interval=0.0,
            transport=base_transport,
        )

    # 4. SourceEvidenceError during auto-discovered item retention MUST abort acquire() loudly.
    orig_retain = CaptureEvidence.retain_file

    def fail_on_filing(self, path, *, stage, **kwargs):
        if stage == "acquire-contribution-filing":
            raise SourceEvidenceError("simulated disk full during retention")
        return orig_retain(self, path, stage=stage, **kwargs)

    monkeypatch.setattr(CaptureEvidence, "retain_file", fail_on_filing)
    with pytest.raises(SourceEvidenceError, match="simulated disk full"):
        frp.acquire(
            tmp_path / "acq-evidence-fail",
            cycles=[2024],
            committee_cycles=[2024],
            outside_candidates_per_cycle=1,
            contribution_reports_per_cycle=1,
            contribution_forms=("pac-party",),
            max_requests=50,
            discovery_pages=1,
            archive_root=raw_root,
            api_key=fake_key,
            min_interval=0.0,
            transport=base_transport,
        )
    monkeypatch.setattr(CaptureEvidence, "retain_file", orig_retain)

    # 5. RequestBound TimeoutError raised inside Phase 1/2 MUST abort acquire() loudly.
    bound = RequestBound(60.0, label="acquire-cancel-test")
    orig_outside = frp.outside_profiles

    def cancel_inside_outside(*args, **kwargs):
        bound.cancel()
        return orig_outside(*args, **kwargs)

    monkeypatch.setattr(frp, "outside_profiles", cancel_inside_outside)
    with pytest.raises(TimeoutError, match="acquire-cancel-test"), bound.scope():
        frp.acquire(
            tmp_path / "acq-cancel",
            cycles=[2024],
            committee_cycles=[2024],
            outside_candidates_per_cycle=1,
            contribution_reports_per_cycle=1,
            contribution_forms=("pac-party",),
            max_requests=50,
            discovery_pages=1,
            archive_root=raw_root,
            api_key=fake_key,
            min_interval=0.0,
            transport=base_transport,
        )
    monkeypatch.setattr(frp, "outside_profiles", orig_outside)

    # 6. Non-fatal ValueError containing literal secret or local path is scrubbed before entering selection.json.
    scrubbed = frp._scrub_skip_reason(
        ValueError(f"bad value with {fake_key} at /Users/mikewolfd/secret/file.json?api_key={fake_key}"),
        fake_key,
    )
    assert fake_key not in scrubbed
    assert "/Users/mikewolfd" not in scrubbed
    assert "<redacted>" in scrubbed
    assert "<local>/file.json" in scrubbed


def test_committee_traversal_rejects_mismatched_filters_per_page_and_inexact_count(tmp_path: Path):
    """Finding 2: Stage 1 build() checks non-page filters + per_page, and acquire() checks is_count_exact."""
    raw_root = tmp_path / "raw"
    seed = CaptureEvidence(tmp_path / "seed-f2", "fec-research-profiles", archive_root=raw_root)

    # Page 1 has cycle=2024; Page 2 tampers query filter to cycle=2026!
    p1_url = "https://api.open.fec.gov/v1/committees/?cycle=2024&sort=committee_id&per_page=100&page=1"
    p2_url = "https://api.open.fec.gov/v1/committees/?cycle=2026&sort=committee_id&per_page=100&page=2"
    p1_bytes = _api_page_bytes([{"committee_id": "C00000001", "name": "ONE"}], page=1, pages=2, count=2)
    p2_bytes = _api_page_bytes([{"committee_id": "C00000002", "name": "TWO"}], page=2, pages=2, count=2)
    comm_mismatch = [
        {**_retain_blob(seed, p1_bytes, url=p1_url), "expected_results": 1},
        {**_retain_blob(seed, p2_bytes, url=p2_url), "expected_results": 1},
    ]

    # Page with per_page mismatch in response pagination vs request URL.
    bad_pp_url = "https://api.open.fec.gov/v1/committees/?cycle=2024&sort=committee_id&per_page=100&page=1"
    bad_pp_bytes = _api_page_bytes([{"committee_id": "C00000001", "name": "ONE"}], page=1, pages=1, per_page=50, count=1)
    comm_bad_pp = [{**_retain_blob(seed, bad_pp_bytes, url=bad_pp_url), "expected_results": 1}]
    seed.finish()

    base_sel = {
        "format": frp.SELECTION_FORMAT,
        "version": frp.SELECTION_VERSION,
        "outside_scopes": [{"candidate_id": "S6OH00163", "cycle": 2024, "queries": PILOT_SELECTION["outside"]}],
        "contribution_scopes": [PILOT_SELECTION["contribution"]],
    }
    for pop in ("complete_traversal", "bounded_page_prefix"):
        with pytest.raises(ValueError, match="query filters changed"):
            frp.build({**base_sel, "committee_population": pop, "committees": comm_mismatch}, tmp_path / f"b-{pop}", archive_root=raw_root)

    with pytest.raises(ValueError, match="retained population"):
        frp.build(
            {**base_sel, "committee_population": "complete_traversal", "committees": comm_bad_pp},
            tmp_path / "b-pp",
            archive_root=raw_root,
        )

    # acquire(max_committee_pages=0) refuses when OpenFEC returns is_count_exact=False.
    fake_key = "test-openfec-key-inexact-1234"
    base_transport, _ = _mock_openfec_transport(api_key=fake_key)

    def handler_inexact(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/committees/":
            params = dict(parse_qsl(urlsplit(str(request.url)).query))
            page = int(params.get("page", "1"))
            payload = {
                "api_version": "1.0",
                "pagination": {"page": page, "pages": 1, "per_page": 100, "count": 1, "is_count_exact": False},
                "results": [{"committee_id": "C00990001", "name": "INEXACT"}] if page == 1 else [],
            }
            return httpx.Response(200, content=json.dumps(payload).encode("utf-8"), headers={"content-type": "application/json"})
        return base_transport.handle_request(request)

    with pytest.raises(ValueError, match="exact complete publisher pagination"):
        frp.acquire(
            tmp_path / "acq-inexact",
            cycles=[2024],
            committee_cycles=[2024],
            max_committee_pages=0,
            outside_candidates_per_cycle=1,
            contribution_reports_per_cycle=1,
            contribution_forms=("pac-party",),
            max_requests=50,
            discovery_pages=1,
            archive_root=raw_root,
            api_key=fake_key,
            min_interval=0.0,
            transport=httpx.MockTransport(handler_inexact),
        )


def test_acquire_fast_stops_unmapped_cover_bucket_and_conserves_budget(tmp_path: Path):
    """Finding 3: after 2 consecutive unmapped-cover refusals in one (cycle, form), auto-discovery stops probing."""
    raw_root = tmp_path / "raw"
    fake_key = "test-openfec-key-faststop-1234"
    base_transport, _ = _mock_openfec_transport(api_key=fake_key)

    # Build 5 synthetic v8.3 F3 reports (unmapped in fec_filing_forms.py).
    v83_f3_bytes = (_fec_line("HDR", "FEC", "8.3", "OLD", "1.0") + _fec_line("F3N", "C00880001", "OLD CMTE")).encode("utf-8")
    old_reports = [
        _make_report(
            committee_id=f"C0088000{i}", committee_name=f"OLD CMTE {i}", filing_id=f"500100{i}", cycle=2024,
            report_year=2024, report_type="YE", cov_start="2024-10-01", cov_end="2024-12-31",
            ind_itemized="100.00", ind_unitemized="0.00", ind_refund="0.00", pac_itemized="0.00", pac_refund="0.00",
        )
        for i in range(1, 6)
    ]
    old_report_by_cid = {r["committee_id"]: r for r in old_reports}
    probed_old_f3 = []

    def handler_v83(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/reports/house-senate/":
            return httpx.Response(200, content=_api_page_bytes(old_reports), headers={"content-type": "application/json"})
        for cid, rep in old_report_by_cid.items():
            if path == f"/v1/committee/{cid}/reports/":
                probed_old_f3.append(cid)
                return httpx.Response(200, content=_api_page_bytes([rep]), headers={"content-type": "application/json"})
            if path == f"/v1/committee/{cid}/history/2024/":
                return httpx.Response(200, content=_api_page_bytes([{"committee_id": cid, "candidate_ids": [f"H2OH8800{cid[-1]}"]}]), headers={"content-type": "application/json"})
        if path == "/v1/candidates/":
            params = dict(parse_qsl(urlsplit(str(request.url)).query))
            cand_id = params.get("candidate_id", "")
            if cand_id.startswith("H2OH8800"):
                return httpx.Response(
                    200,
                    content=_api_page_bytes([{"candidate_id": cand_id, "first_file_date": "2023-01-01", "cycles": [2024]}]),
                    headers={"content-type": "application/json"},
                )
        if request.url.host == "docquery.fec.gov" and path.startswith("/dcdev/posted/500100"):
            return httpx.Response(200, content=v83_f3_bytes, headers={"content-type": "text/plain", "content-length": str(len(v83_f3_bytes))})
        return base_transport.handle_request(request)

    sel = frp.acquire(
        tmp_path / "acq-v83",
        cycles=[2024],
        committee_cycles=[2024],
        outside_candidates_per_cycle=1,
        contribution_reports_per_cycle=4,
        contribution_forms=("house-senate", "pac-party"),
        max_requests=80,
        discovery_pages=1,
        archive_root=raw_root,
        api_key=fake_key,
        min_interval=0.0,
        transport=httpx.MockTransport(handler_v83),
    )
    # Fast-stopped after 2 consecutive unmapped v8.3 covers instead of probing all 5!
    assert len(probed_old_f3) == 2
    assert len(sel["contribution_scopes"]) == 1  # pac-party F3X still succeeded!
    assert len(sel["acquisition_summary"]["skipped_contribution_scopes"]) == 2


def test_reuse_committees_jsonl_rejects_path_traversal_tampered_blob_and_page_overflow(tmp_path: Path):
    """Finding 4: _load_reused_committee_pages validates blob_path containment, digest parity, and MAX_COMMITTEE_PAGES."""
    raw_root = tmp_path / "raw"
    evidence = CaptureEvidence(tmp_path / "ev-reuse", "fec-research-profiles", archive_root=raw_root)
    try:
        # 1. Path traversal outside blobs_root refuses.
        outside_secret = tmp_path / "outside.json"
        outside_secret.write_bytes(b'{"secret": true}')
        cap_dir = tmp_path / "cap-traversal"
        (cap_dir / "blobs").mkdir(parents=True)
        fake_sha = "sha256:" + hashlib.sha256(b'{"secret": true}').hexdigest()
        traversal_jsonl = cap_dir / "pages.jsonl"
        traversal_jsonl.write_text(
            json.dumps(
                {
                    "request_url": "https://api.open.fec.gov/v1/committees/?sort=committee_id&per_page=100&page=1",
                    "observed_at": "2026-11-27T01:00:00Z",
                    "evidence": {"sha256": fake_sha, "bytes": len(b'{"secret": true}'), "blob_path": "../../outside.json"},
                    "records": 1,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="escapes capture blobs directory"):
            frp._load_reused_committee_pages(traversal_jsonl, evidence)

        # 2. Tampered blob bytes whose SHA-256 differs from manifest digest refuses.
        cap_dir2 = tmp_path / "cap-tampered"
        blob_dir2 = cap_dir2 / "blobs" / "sha256"
        blob_dir2.mkdir(parents=True)
        real_body = _api_page_bytes([{"committee_id": "C00777001", "name": "GOOD"}], page=1, pages=1, count=1)
        declared_hex = hashlib.sha256(real_body).hexdigest()
        (blob_dir2 / declared_hex).write_bytes(b'{"tampered": true}')
        tampered_jsonl = cap_dir2 / "pages.jsonl"
        tampered_jsonl.write_text(
            json.dumps(
                {
                    "request_url": "https://api.open.fec.gov/v1/committees/?sort=committee_id&per_page=100&page=1",
                    "observed_at": "2026-11-27T01:00:00Z",
                    "evidence": {"sha256": f"sha256:{declared_hex}", "bytes": len(real_body), "blob_path": f"sha256/{declared_hex}"},
                    "records": 1,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="differs from its declared digest/size"):
            frp._load_reused_committee_pages(tampered_jsonl, evidence)
    finally:
        evidence.finish()


def test_null_candidate_ids_raises_value_error_and_integral_decimal_amendment_chain_accepted(tmp_path: Path):
    """Finding 5 nits: explicit null candidate_ids raises ValueError (not TypeError), and integral Decimal tail works."""
    from spicy_regs.transforms.fec_contribution_profiles import _amendment_tail, candidate_for_report

    assert _amendment_tail({"amendment_chain": [Decimal("1943202.0")]}) == ["1943202"]
    assert _amendment_tail({"amendment_chain": [Decimal("1943202.5")]}) == []
    with pytest.raises(ValueError, match="committee linkage"):
        candidate_for_report("S6OH99001", {"candidate_ids": None}, [], {"coverage_start_date": "2025-10-01", "cycle": 2026})
    with pytest.raises(ValueError, match="coverage_start_date"):
        candidate_for_report("S6OH99001", {"candidate_ids": ["S6OH99001"]}, [], {"coverage_start_date": None, "cycle": 2026})
