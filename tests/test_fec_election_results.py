"""Multi-cycle FEC election results transform, reconciliation, and pipeline tests."""

import hashlib
from io import BytesIO
import json
from xml.sax.saxutils import escape
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.fec.election_results import READER_VERSION, edition_spec, workbook_rows
from spicy_regs.pipelines.fec_election_results import _normalize_editions, build
from spicy_regs.transforms.fec_election_results import (
    NATIVE,
    POLICY,
    RESULTS,
    SCHEMAS,
    interpret,
    reconcile_general_contests,
)


def _sha(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _mini_xlsx(sheets: dict[str, dict[str, str | int]]) -> bytes:
    buf = BytesIO()
    with ZipFile(buf, "w") as zf:
        wb_sheets = []
        rels = []
        for idx, (name, cells) in enumerate(sheets.items(), 1):
            wb_sheets.append(f'<sheet name="{escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>')
            rels.append(
                f'<Relationship Id="rId{idx}" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                f'Target="worksheets/sheet{idx}.xml"/>'
            )
            by_row: dict[int, list[tuple[str, str | int]]] = {}
            for coord, val in cells.items():
                row_num = int("".join(ch for ch in coord if ch.isdigit()))
                by_row.setdefault(row_num, []).append((coord, val))
            row_xml = []
            for row_num in sorted(by_row):
                c_xml = []
                for coord, val in sorted(by_row[row_num]):
                    if isinstance(val, int):
                        c_xml.append(f'<c r="{coord}"><v>{val}</v></c>')
                    else:
                        c_xml.append(f'<c r="{coord}" t="inlineStr"><is><t>{escape(str(val))}</t></is></c>')
                row_xml.append(f'<row r="{row_num}">{"".join(c_xml)}</row>')
            zf.writestr(
                f"xl/worksheets/sheet{idx}.xml",
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                f"<sheetData>{''.join(row_xml)}</sheetData></worksheet>",
            )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f"<sheets>{''.join(wb_sheets)}</sheets></workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            f"{''.join(rels)}</Relationships>",
        )
    return buf.getvalue()


def _base_edition_sheets(cycle: int) -> dict[str, dict[str, str | int]]:
    spec = edition_spec(cycle)
    sheets: dict[str, dict[str, str | int]] = {}
    sheets.setdefault(spec.title_sheet, {})[spec.title_cell] = spec.title_text
    for sname, coord, text in spec.sentinel_cells:
        sheets.setdefault(sname, {})[coord] = text
    for s in spec.sheets:
        sheets.setdefault(s.name, {}).update(s.headers)
    return sheets


def _capture(body: bytes, *, cycle: int, url: str) -> dict:
    return {
        "kind": "workbook",
        "cycle": cycle,
        "request_url": url,
        "response_sha256": _sha(body),
        "observed_at": "2026-11-27T20:00:00+00:00",
        "byte_size": len(body),
    }


def test_2022_senate_and_house_keep_fusion_rcv_unexpired_and_reconcile():
    sheets = _base_edition_sheets(2022)
    senate = sheets["7. US Senate Results by State"]
    senate.update(
        {
            "B2": "AK",
            "C2": "Alaska",
            "D2": "S",
            "E2": "S4AK00099",
            "I2": "Murkowski, Lisa",
            "K2": "R",
            "P2": 113495,
            "Y2": 114118,
            "AA2": 116895,
            "AC2": 136330,
            "B3": "AK",
            "C3": "Alaska",
            "D3": "S",
            "E3": "S2AK00150",
            "I3": "Kelley, Buzz A.",
            "K3": "R",
            "P3": 7688,
            "Y3": 7768,
            "AA3": 0,
            "AC3": 0,
            "B4": "AK",
            "C4": "Alaska",
            "D4": "S",
            "J4": "Total State Votes:",
            "P4": 121183,
            "B5": "OK",
            "C5": "Oklahoma",
            "D5": "S-Unexpired Term",
            "E5": "S2OK00210",
            "I5": "Mullin, Markwayne",
            "K5": "R",
            "P5": 710643,
            "C6": "Oklahoma",
            "D6": "S-Unexpired Term",
            "J6": "Total State Votes:",
            "P6": 710643,
        }
    )
    house = sheets["8. US House Results by State"]
    house.update(
        {
            "B2": "IN",
            "C2": "Indiana",
            "I2": "DISTRICT 2",
            "P2": "            ",  # whitespace-only cell on district header must skip cleanly
            "B3": "IN",
            "C3": "Indiana",
            "D3": "02-Full Term",
            "E3": "H2IN02295",
            "I3": "Yakym, Rudolph (Rudy), III",
            "K3": "R",
            "L3": "#",
            "P3": 125537,
            "B4": "IN",
            "C4": "Indiana",
            "D4": "02-Full Term",
            "J4": "Full Term District Votes:",
            "P4": 125537,
            "B5": "IN",
            "C5": "Indiana",
            "D5": "02-Unexpired Term",
            "E5": "H2IN02295",
            "I5": "Yakym, Rudolph (Rudy), III",
            "K5": "R",
            "L5": "*",
            "P5": 118997,
            "B6": "IN",
            "C6": "Indiana",
            "D6": "02-Unexpired Term",
            "J6": "Unexpired Term District Votes:",
            "P6": 118997,
            "B7": "NY",
            "C7": "New York",
            "D7": "14",
            "E7": "H8NY15148",
            "I7": "Ocasio-Cortez, Alexandria",
            "K7": "D",
            "L7": "Unopposed",
            "P7": 74050,
            "T7": 82453,
            "B8": "NY",
            "C8": "New York",
            "D8": "14",
            "E8": "H8NY15148",
            "I8": "Ocasio-Cortez, Alexandria",
            "K8": "WF",
            "L8": "Unopposed",
            "P8": 8403,
            "T8": 82453,
            "B9": "NY",
            "C9": "New York",
            "D9": "14",
            "J9": "District Votes:",
            "P9": 82453,
            "B10": "NY",
            "C10": "New York",
            "D10": "H",
            "J10": "Total State Votes:",
            "P10": 82453,
        }
    )
    body = _mini_xlsx(sheets)
    cap = _capture(body, cycle=2022, url="https://www.fec.gov/documents/5676/federalelections2022.xlsx")
    tables = interpret(workbook_rows(body, sha256=cap["response_sha256"], cycle=2022), cap)
    results = tables[RESULTS]
    assert all(r["interpretation_version"] == POLICY for r in results)
    assert all(r["reader_version"] == READER_VERSION for r in results)

    # Zero elimination survives as exact zero in RCV round 3.
    zero_rcv = [
        r for r in results if r["office"] == "senate" and r["state"] == "AK" and r["round"] == "rcv_3" and r["votes"] == 0
    ]
    assert len(zero_rcv) == 1 and zero_rcv[0]["vote_basis"] == "ranked_choice_round"

    # Concurrent IN-02 full and unexpired terms remain distinct under normalized district '02'.
    in02_full = [
        r
        for r in results
        if r["office"] == "house" and r["state"] == "IN" and r["district"] == "02" and r["seat_term"] == "full"
        and r["stage"] == "general" and r["round"] == "reported" and r["row_kind"] == "candidate"
    ]
    in02_unexp = [
        r
        for r in results
        if r["office"] == "house" and r["state"] == "IN" and r["district"] == "02" and r["seat_term"] == "unexpired"
        and r["stage"] == "general" and r["round"] == "reported" and r["row_kind"] == "candidate"
    ]
    assert in02_full[0]["votes"] == 125537 and in02_unexp[0]["votes"] == 118997
    assert in02_full[0]["candidate_id_status"] == "source_id"

    # NY-14 combined party witnesses stay distinct from ballot lines, and House state_total uses district 'H'.
    ny_comb = [
        r
        for r in results
        if r["state"] == "NY" and r["vote_basis"] == "combined_party_witness" and r["row_kind"] == "candidate"
    ]
    assert len(ny_comb) == 2 and {r["votes"] for r in ny_comb} == {82453}
    ny_state_tot = [r for r in results if r["state"] == "NY" and r["row_kind"] == "state_total" and r["stage"] == "general" and r["round"] == "reported"]
    assert len(ny_state_tot) == 1 and ny_state_tot[0]["district"] == "H"

    rec = reconcile_general_contests(results)
    assert rec == {"reconciled_contests": 5, "verified_publisher_discrepancies": 0}


def test_2020_presidential_general_and_primary_separate_electoral_fusion_and_rcv():
    sheets = _base_edition_sheets(2020)
    pg = sheets["9. 2020 Pres General Results"]
    pg.update(
        {
            "B2": "P80000722",
            "C2": "Maine",
            "D2": "ME",
            "H2": "Biden, Joseph R.",
            "J2": "D",
            "K2": 435072,
            "P2": "W*",
            "Q2": 3,
            "B3": "P80001571",
            "C3": "Maine",
            "D3": "ME",
            "H3": "Trump, Donald J.",
            "J3": "R",
            "K3": 360737,
            "Q3": 1,
            "D4": "ME",
            "I4": "Total State Votes:",
            "M4": 795809,
            "B5": "P80000722",
            "C5": "New York",
            "D5": "NY",
            "H5": "Biden, Joseph R.",
            "J5": "Combined Parties:",
            "N5": 5244886,
            "P5": "W",
            "Q5": 29,
            "B6": "P80000722",
            "C6": "New York",
            "D6": "NY",
            "H6": "Biden, Joseph R.",
            "J6": "D",
            "K6": 4858273,
            "N6": 5244886,
            "Q6": "[29]",
            "B7": "P80000722",
            "C7": "New York",
            "D7": "NY",
            "H7": "Biden, Joseph R.",
            "J7": "WF",
            "K7": 386613,
            "N7": 5244886,
            "Q7": "[29]",
            "D8": "NY",
            "I8": "Total State Votes:",
            "M8": 5244886,
            "D9": "All States",
            "I9": "Total All States:",
            "M9": 6040695,
            "Q9": 33,
        }
    )
    pp = sheets["10. 2020 Pres Primary Results"]
    pp.update(
        {
            "D2": "KS",
            "K2": "Final Round (Round 4)",
            "B3": "P80000722",
            "D3": "KS",
            "H3": "Biden, Joseph R.",
            "J3": "D",
            "K3": 110041,
            "D4": "KS",
            "I4": "Total Party Votes:",
            "J4": "D",
            "K4": 143183,
            "D5": "KS",
            "I5": "Total State Votes:",
            "K5": 143183,
            "D6": "KS",
            "K6": "First Round (Round 1)",
            "B7": "P80000722",
            "D7": "KS",
            "H7": "Biden, Joseph R.",
            "J7": "D",
            "K7": 102829,
            "M7": 103165,
            "O7": 110041,
            "B8": "P00009183",
            "D8": "KS",
            "H8": "Gabbard, Tulsi",
            "J8": "D",
            "K8": 1604,
            "M8": 0,
            "D9": "KS",
            "I9": "Total Party Votes:",
            "J9": "D",
            "K9": 146873,
            "O9": 143183,
            "D10": "KS",
            "I10": "Total State Votes:",
            "K10": 146873,
            # A subsequent non-RCV party in the same state must reset to reported/ballot_line.
            "B11": "P80001571",
            "D11": "KS",
            "H11": "Trump, Donald J.",
            "J11": "R",
            "K11": 50000,
            # Alaska-style 8-round excerpt + multi-round block must also stay non-colliding.
            "D12": "AK",
            "K12": "Final Round (Round 8)",
            "B13": "P80000722",
            "D13": "AK",
            "H13": "Biden, Joseph R.",
            "J13": "D",
            "K13": 10834,
            "D14": "AK",
            "I14": "Total Party Votes:",
            "J14": "D",
            "K14": 19589,
            "D15": "AK",
            "I15": "Total State Votes:",
            "K15": 19589,
            "D16": "AK",
            "K16": "First Round (Round 1)",
            "B17": "P80000722",
            "D17": "AK",
            "H17": "Biden, Joseph R.",
            "J17": "D",
            "K17": 9862,
            "S17": 10834,
            "D18": "AK",
            "I18": "Total Party Votes:",
            "J18": "D",
            "K18": 19759,
            "S18": 19589,
        }
    )
    body = _mini_xlsx(sheets)
    cap = _capture(body, cycle=2020, url="https://www.fec.gov/documents/4228/federalelections2020.xlsx")
    tables = interpret(workbook_rows(body, sha256=cap["response_sha256"], cycle=2020), cap)
    results = tables[RESULTS]

    # Electoral votes never enter ballot_line; NY bracketed [29] stays combined_party_witness.
    ev_cand = [r for r in results if r["vote_basis"] == "electoral_vote" and r["row_kind"] == "candidate"]
    assert sum(r["votes"] for r in ev_cand) == 33  # ME 3 + ME 1 + NY 29 (zero double count!)
    ny_bracket = [r for r in results if r["state"] == "NY" and r["source_column"] == "Q" and r["votes_raw"] == "[29]"]
    assert len(ny_bracket) == 2 and all(
        r["vote_basis"] == "combined_party_witness" and r["votes"] is None and r["votes_status"] == "source_text"
        for r in ny_bracket
    )
    # Presidential Primary RCV rounds stay strictly separate and retain explicit 0.
    ks_zero = [
        r for r in results if r["stage"] == "primary" and r["state"] == "KS" and r["round"] == "rcv_2" and r["votes"] == 0
    ]
    assert len(ks_zero) == 1 and ks_zero[0]["vote_basis"] == "ranked_choice_round"

    # Regression (BLOCKER 1): querying rcv_4 (KS) or rcv_8 (AK) yields 1x candidate & party_total rows (never 2x).
    ks_rcv4_cand = [
        r for r in results if r["stage"] == "primary" and r["state"] == "KS" and r["round"] == "rcv_4" and r["row_kind"] == "candidate"
    ]
    ks_rcv4_party = [
        r for r in results if r["stage"] == "primary" and r["state"] == "KS" and r["round"] == "rcv_4" and r["row_kind"] == "party_total"
    ]
    ks_summary = [
        r for r in results if r["stage"] == "primary" and r["state"] == "KS" and r["round"] == "rcv_4_final_summary"
    ]
    assert len(ks_rcv4_cand) == 1 and ks_rcv4_cand[0]["votes"] == 110041 and ks_rcv4_cand[0]["source_column"] == "O"
    assert len(ks_rcv4_party) == 1 and ks_rcv4_party[0]["votes"] == 143183 and ks_rcv4_party[0]["source_column"] == "O"
    assert len(ks_summary) == 3 and {r["row_kind"] for r in ks_summary} == {"candidate", "party_total", "contest_total"}

    ak_rcv8_cand = [
        r for r in results if r["stage"] == "primary" and r["state"] == "AK" and r["round"] == "rcv_8" and r["row_kind"] == "candidate"
    ]
    ak_rcv8_party = [
        r for r in results if r["stage"] == "primary" and r["state"] == "AK" and r["round"] == "rcv_8" and r["row_kind"] == "party_total"
    ]
    ak_summary = [
        r for r in results if r["stage"] == "primary" and r["state"] == "AK" and r["round"] == "rcv_8_final_summary"
    ]
    assert len(ak_rcv8_cand) == 1 and ak_rcv8_cand[0]["votes"] == 10834 and ak_rcv8_cand[0]["source_column"] == "S"
    assert len(ak_rcv8_party) == 1 and ak_rcv8_party[0]["votes"] == 19589 and ak_rcv8_party[0]["source_column"] == "S"
    assert len(ak_summary) == 3

    # Non-RCV party after RCV block in KS resets to reported / ballot_line.
    ks_rep = [r for r in results if r["stage"] == "primary" and r["state"] == "KS" and r["party"] == "R"]
    assert len(ks_rep) == 1 and ks_rep[0]["round"] == "reported" and ks_rep[0]["vote_basis"] == "ballot_line"
    assert reconcile_general_contests(results) == {"reconciled_contests": 2, "verified_publisher_discrepancies": 0}


def test_2024_presidential_matrix_reconciles_electoral_and_popular_votes():
    sheets = _base_edition_sheets(2024)
    sheet = sheets["OFFICIAL 2024 PRES GE RESULTS"]
    sheet.update(
        {
            "A2": "ME",
            "B2": 4,
            "C2": 1,
            "D2": 3,
            "M2": 435652,
            "Y2": 377977,
            "AD2": 581,
            "AE2": 814210,
            "A3": "NV",
            "B3": 6,
            "C3": 6,
            "M3": 705197,
            "Y3": 751205,
            "AC3": 19625,
            "AE3": 1476027,
            "A53": "Total:",
            "B53": 10,
            "C53": 7,
            "D53": 3,
            "M53": 1140849,
            "Y53": 1129182,
            "AC53": 19625,
            "AD53": 581,
            "AE53": 2290237,
        }
    )
    body = _mini_xlsx(sheets)
    cap = _capture(body, cycle=2024, url="https://www.fec.gov/documents/5645/2024presgeresults.xlsx")
    tables = interpret(workbook_rows(body, sha256=cap["response_sha256"], cycle=2024), cap)
    results = tables[RESULTS]

    me_ev = [r for r in results if r["state"] == "ME" and r["vote_basis"] == "electoral_vote" and r["row_kind"] == "candidate"]
    assert {(r["candidate_name"], r["votes"]) for r in me_ev} == {("TRUMP", 1), ("HARRIS", 3)}
    nv_none = [r for r in results if r["state"] == "NV" and r["candidate_name"] == "NONE OF THESE CANDIDATES"]
    assert len(nv_none) == 1 and nv_none[0]["votes"] == 19625 and nv_none[0]["candidate_id_status"] == "not_resolved"
    assert reconcile_general_contests(results) == {"reconciled_contests": 2, "verified_publisher_discrepancies": 0}


def test_reconciliation_refuses_tampered_general_total():
    sheets = _base_edition_sheets(2024)
    sheets["OFFICIAL 2024 PRES GE RESULTS"].update({"A2": "AL", "B2": 9, "C2": 9, "M2": 100, "Y2": 200, "AE2": 301})
    body = _mini_xlsx(sheets)
    cap = _capture(body, cycle=2024, url="https://www.fec.gov/documents/5645/2024presgeresults.xlsx")
    tables = interpret(workbook_rows(body, sha256=cap["response_sha256"], cycle=2024), cap)
    with pytest.raises(ValueError, match="failed reconciliation"):
        reconcile_general_contests(tables[RESULTS])


def test_multi_cycle_pipeline_builds_receipts_free_generation(tmp_path, monkeypatch):
    raw_root = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw_root))
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    store = CentralRawArchiveStore(raw_root)

    # Build two minimal valid workbooks (2022 + 2024) and store them in central raw archive.
    s22 = _base_edition_sheets(2022)
    s22["7. US Senate Results by State"].update(
        {
            "B2": "AL",
            "C2": "Alabama",
            "D2": "S",
            "E2": "S2AL00145",
            "I2": "Britt, Katie",
            "K2": "R",
            "P2": 942154,
            "B3": "AL",
            "C3": "Alabama",
            "D3": "S",
            "J3": "Total State Votes:",
            "P3": 942154,
        }
    )
    b22 = _mini_xlsx(s22)
    store.put_stream([b22], max_bytes=len(b22))

    s24 = _base_edition_sheets(2024)
    s24["OFFICIAL 2024 PRES GE RESULTS"].update({"A2": "AL", "B2": 9, "C2": 9, "M2": 772412, "Y2": 1462616, "AE2": 2235028})
    b24 = _mini_xlsx(s24)
    store.put_stream([b24], max_bytes=len(b24))

    selection = {
        "editions": [
            _capture(b22, cycle=2022, url="https://www.fec.gov/documents/5676/federalelections2022.xlsx"),
            _capture(b24, cycle=2024, url="https://www.fec.gov/documents/5645/2024presgeresults.xlsx"),
        ]
    }
    out = tmp_path / "multi-cycle-gen"
    artifact, _ = build(selection, out)
    artifact_json = json.loads((out / "generation" / "artifact.json").read_text())
    assert "etlReceipts" not in artifact_json
    assert artifact.pin.artifact_digest.startswith("sha256:")

    res_table = pq.read_table(out / "generation" / "fec_election_results.parquet")
    assert res_table.schema.equals(SCHEMAS[RESULTS])
    assert set(res_table.column("source_cycle").to_pylist()) == {2022, 2024}
    assert set(res_table.column("office").to_pylist()) == {"senate", "president"}

    nat_table = pq.read_table(out / "generation" / "fec_election_workbook_rows.parquet")
    assert nat_table.schema.equals(SCHEMAS[NATIVE])
    assert set(nat_table.column("source_cycle").to_pylist()) == {2022, 2024}


def test_2016_and_2018_editions_faithless_electors_blank_district_and_id_rules():
    # 2016: Table 2 faithless electors, Pres General NY fusion, Pres Primary, Senate blank-D/ID trim/cross-office ID, House unexpired.
    s16 = _base_edition_sheets(2016)
    s16["2016 Pres General Results"].update(
        {
            "B2": "P00003392",
            "C2": "New York",
            "D2": "NY",
            "H2": "Clinton, Hillary",
            "J2": "Combined Parties:",
            "L2": 4556118,
            "M2": "0.59",
            "N2": "W",
            "B3": "P00003392",
            "C3": "New York",
            "D3": "NY",
            "H3": "Clinton, Hillary",
            "J3": "DEM",
            "K3": 4379783,
            "B4": "P00003392",
            "C4": "New York",
            "D4": "NY",
            "H4": "Clinton, Hillary",
            "J4": "WF",
            "K4": 176335,
            "D5": "NY",
            "I5": "Total State Votes:",
            "L5": 4556118,
        }
    )
    s16["Table 2. Electoral &  Pop Vote"].update(
        {
            "A5": "ME",
            "B5": 1,
            "C5": 3,
            "A16": "HI",
            "C16": "3**",
            "A48": "TX",
            "B48": "36*",
            "A52": "WA",
            "C52": "8**",
            "A56": "Total:   ",
            "B56": "304*",
            "C56": "227**",
            "A61": "* Texas has 38 Electoral Votes. 1 was cast for John Kasich and 1 was cast for Ron Paul.",
            "A62": "** Hawaii has 4 Electoral Votes. 1 was cast for Bernie Sanders. Washington has 12 Electoral Votes.",
        }
    )
    s16["2016 Pres Primary Results"].update(
        {
            "B2": "P00003392",
            "D2": "AL",
            "H2": "Clinton, Hillary",
            "J2": "D",
            "K2": 309071,
            "D3": "AL",
            "I3": "Total State Votes: ",
            "K3": 309071,
        }
    )
    s16["2016 US Senate Results by State"].update(
        {
            "B2": "UT",
            "C2": "Utah",
            "D2": "S",
            "E2": "S0UT00165 ",  # trailing space must trim & match source_id
            "I2": "Lee, Mike",
            "K2": "R",
            "P2": 760241,
            "B3": "UT",
            "C3": "Utah",
            "D3": "S",
            "E3": "H4NC11086",  # cross-office House ID on Senate sheet must remain not_resolved
            "I3": "Hill, Tom",
            "K3": "IND",
            "P3": 100,
            "B4": "UT",
            "C4": "Utah",
            "E4": "n/a",  # blank D must inherit prior_term ("full")
            "I4": "Traceski, John M.",
            "K4": "W",
            "P4": 12,
            "B5": "UT",
            "C5": "Utah",
            "D5": "S",
            "J5": "Total State Votes:",
            "P5": 760353,
        }
    )
    s16["2016 US House Results by State"].update(
        {
            "B2": "KY",
            "C2": "Kentucky",
            "D2": "1 - UNEXPIRED TERM",
            "E2": "H6KY01110 ",
            "I2": "Comer, James R.",
            "K2": "R",
            "P2": 209810,
            "B3": "KY",
            "C3": "Kentucky",
            "J3": "District Votes:",  # blank D inherits district='01', seat_term='unexpired'
            "P3": 209810,
        }
    )
    b16 = _mini_xlsx(s16)
    c16 = _capture(b16, cycle=2016, url="https://www.fec.gov/documents/1890/federalelections2016.xlsx")
    r16 = interpret(workbook_rows(b16, sha256=c16["response_sha256"], cycle=2016), c16)[RESULTS]

    # Table 2 faithless electors stay source_text with footnotes; ME stays exact.
    ev16 = [r for r in r16 if r["vote_basis"] == "electoral_vote"]
    ev_by_state = {(r["state"], r["source_column"]): r for r in ev16}
    assert ev_by_state[("ME", "B")]["votes"] == 1 and ev_by_state[("ME", "C")]["votes"] == 3
    assert ev_by_state[("TX", "B")]["votes"] is None and ev_by_state[("TX", "B")]["votes_status"] == "source_text"
    assert "Kasich" in (ev_by_state[("TX", "B")]["footnotes"] or "")
    assert ev_by_state[("HI", "C")]["votes_raw"] == "3**" and ev_by_state[("WA", "C")]["votes_raw"] == "8**"
    assert ev_by_state[("US", "B")]["row_kind"] == "national_total" and ev_by_state[("US", "B")]["votes_raw"] == "304*"

    # Senate candidate_id trim vs cross-office ID refusal & blank-D term inheritance.
    ut_gen = {
        r["candidate_name"]: r
        for r in r16
        if r["office"] == "senate" and r["state"] == "UT" and r["stage"] == "general" and r["round"] == "reported"
        and r["row_kind"] == "candidate"
    }
    assert ut_gen["Lee, Mike"]["candidate_id"] == "S0UT00165"
    assert ut_gen["Lee, Mike"]["candidate_id_status"] == "source_id"
    assert ut_gen["Hill, Tom"]["candidate_id"] == "H4NC11086"
    assert ut_gen["Hill, Tom"]["candidate_id_status"] == "not_resolved"
    assert ut_gen["Traceski, John M."]["district"] == "S" and ut_gen["Traceski, John M."]["seat_term"] == "full"

    # House single-digit unexpired district normalization + blank-D District Votes inheritance.
    ky_tot = [
        r for r in r16 if r["office"] == "house" and r["state"] == "KY" and r["row_kind"] == "contest_total"
        and r["stage"] == "general" and r["round"] == "reported"
    ]
    assert len(ky_tot) == 1 and ky_tot[0]["district"] == "01" and ky_tot[0]["seat_term"] == "unexpired"
    assert reconcile_general_contests(r16) == {"reconciled_contests": 3, "verified_publisher_discrepancies": 0}

    # 2018: Senate unexpired blank-D inherits prior_term='unexpired'; House unnamed write-in & Total Votes:.
    s18 = _base_edition_sheets(2018)
    s18["2018 US Senate Results by State"].update(
        {
            "B2": "MS",
            "C2": "Mississippi",
            "D2": "S-UNEXPIRED TERM",
            "E2": "S8MS00261",
            "I2": "Hyde-Smith, Cindy",
            "K2": "N",
            "P2": 389995,
            "R2": 486769,
            "B3": "MS",
            "C3": "Mississippi",
            "J3": "Total Unexpired Term State Votes:",  # blank D inherits prior_term='unexpired'
            "P3": 389995,
            "R3": 486769,
        }
    )
    s18["2018 US House Results by State"].update(
        {
            "B2": "NY",
            "C2": "New York",
            "D2": "05",
            "E2": "H8NY06048",
            "I2": "Meeks, Gregory W.",
            "K2": "D",
            "P2": 160500,
            "B3": "NY",
            "C3": "New York",
            "D3": "05",
            "K3": "W",  # unnamed write-in row with blank I and blank J
            "P3": 971,
            "B4": "NY",
            "C4": "New York",
            "D4": "05",
            "J4": "District Votes:",
            "P4": 161471,
            "B5": "DC",
            "C5": "District of Columbia",
            "D5": "H",
            "J5": "Total Votes:",
            "P5": 228769,
        }
    )
    b18 = _mini_xlsx(s18)
    c18 = _capture(b18, cycle=2018, url="https://www.fec.gov/documents/2706/federalelections2018.xlsx")
    r18 = interpret(workbook_rows(b18, sha256=c18["response_sha256"], cycle=2018), c18)[RESULTS]
    ms_tot = [
        r for r in r18 if r["office"] == "senate" and r["state"] == "MS" and r["row_kind"] == "contest_total"
        and r["stage"] == "general" and r["round"] == "reported"
    ]
    assert len(ms_tot) == 1 and ms_tot[0]["seat_term"] == "unexpired"
    dc_tot = [
        r for r in r18 if r["office"] == "house" and r["state"] == "DC" and r["row_kind"] == "state_total"
        and r["stage"] == "general" and r["round"] == "reported"
    ]
    assert len(dc_tot) == 1 and dc_tot[0]["district"] == "H" and dc_tot[0]["votes"] == 228769
    assert reconcile_general_contests(r18) == {"reconciled_contests": 2, "verified_publisher_discrepancies": 0}


def test_verified_tx_discrepancy_allowlist_and_near_miss_refusal():
    def _make_2020_pres(state: str, cand_votes: int, scattered: int, total_votes: int):
        sheets = _base_edition_sheets(2020)
        sheets["9. 2020 Pres General Results"].update(
            {
                "B2": "P80001571",
                "C2": state,
                "D2": state,
                "H2": "Trump, Donald J.",
                "J2": "R",
                "K2": cand_votes,
                "B3": "n/a",
                "C3": state,
                "D3": state,
                "H3": "Scattered",
                "J3": "W",
                "K3": scattered,
                "D4": state,
                "I4": "Total State Votes:",
                "M4": total_votes,
            }
        )
        body = _mini_xlsx(sheets)
        cap = _capture(body, cycle=2020, url="https://www.fec.gov/documents/4228/federalelections2020.xlsx")
        return interpret(workbook_rows(body, sha256=cap["response_sha256"], cycle=2020), cap)[RESULTS]

    # Exact visually verified TX (+460) discrepancy is accepted and counted explicitly.
    tx_exact = _make_2020_pres("TX", 11315056, 460, 11315056)
    assert reconcile_general_contests(tx_exact) == {"reconciled_contests": 0, "verified_publisher_discrepancies": 1}

    # Near-miss on TX (+461 instead of +460) refuses closed.
    tx_near_miss = _make_2020_pres("TX", 11315056, 461, 11315056)
    with pytest.raises(ValueError, match="failed reconciliation"):
        reconcile_general_contests(tx_near_miss)

    # Identical +460 offset on a non-TX state (FL) refuses closed.
    fl_same_offset = _make_2020_pres("FL", 11315056, 460, 11315056)
    with pytest.raises(ValueError, match="failed reconciliation"):
        reconcile_general_contests(fl_same_offset)


def test_single_capture_backward_compat_and_normalize_editions_errors(tmp_path, monkeypatch):
    raw_root = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw_root))
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    store = CentralRawArchiveStore(raw_root)
    s22 = _base_edition_sheets(2022)
    s22["7. US Senate Results by State"].update(
        {
            "B2": "AL",
            "C2": "Alabama",
            "D2": "S",
            "E2": "S2AL00145",
            "I2": "Britt, Katie",
            "K2": "R",
            "P2": 942154,
            "B3": "AL",
            "C3": "Alabama",
            "D3": "S",
            "J3": "Total State Votes:",
            "P3": 942154,
        }
    )
    b22 = _mini_xlsx(s22)
    store.put_stream([b22], max_bytes=len(b22))

    # Backward-compatible single-capture payload without 'cycle' defaults to cycle=2022.
    legacy_capture = {
        "kind": "workbook",
        "request_url": "https://www.fec.gov/documents/5676/federalelections2022.xlsx",
        "response_sha256": _sha(b22),
        "observed_at": "2026-11-13T11:39:25+00:00",
        "byte_size": len(b22),
    }
    out = tmp_path / "single-cap"
    artifact, _ = build(legacy_capture, out, cycle=2022)
    assert artifact.pin.artifact_digest.startswith("sha256:")
    tbl = pq.read_table(out / "generation" / "fec_election_results.parquet")
    assert set(tbl.column("source_cycle").to_pylist()) == {2022}

    # _normalize_editions validation error branches.
    good_a = {"cycle": 2022, "response_sha256": "sha256:" + "a" * 64}
    good_b = {"cycle": 2024, "response_sha256": "sha256:" + "b" * 64}
    assert [e["cycle"] for e in _normalize_editions({"editions": [good_b, good_a]})] == [2022, 2024]

    with pytest.raises(ValueError, match="non-empty editions"):
        _normalize_editions({"editions": []})
    with pytest.raises(ValueError, match="integer cycle"):
        _normalize_editions({"editions": [{**good_a, "cycle": True}]})
    with pytest.raises(ValueError, match="integer cycle"):
        _normalize_editions({"editions": [{**good_a, "cycle": "2022"}]})
    with pytest.raises(ValueError, match="Duplicate election cycle"):
        _normalize_editions({"editions": [good_a, {**good_b, "cycle": 2022}]})
    with pytest.raises(ValueError, match="sha256 response_sha256"):
        _normalize_editions({"editions": [{**good_a, "response_sha256": "md5:1234"}]})
    with pytest.raises(ValueError, match="Duplicate workbook digest"):
        _normalize_editions({"editions": [good_a, {**good_b, "response_sha256": good_a["response_sha256"]}]})
