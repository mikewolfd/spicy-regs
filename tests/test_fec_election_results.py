"""Multi-cycle FEC election results transform, reconciliation, and pipeline tests."""

import hashlib
from io import BytesIO
import json
from xml.sax.saxutils import escape
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.fec.election_results import READER_VERSION, edition_spec, workbook_rows
from spicy_regs.pipelines.fec_election_results import build
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
            "K4": "First Round (Round 1)",
            "B5": "P80000722",
            "D5": "KS",
            "H5": "Biden, Joseph R.",
            "J5": "D",
            "K5": 102829,
            "M5": 103165,
            "O5": 110041,
            "B6": "P00009183",
            "D6": "KS",
            "H6": "Gabbard, Tulsi",
            "J6": "D",
            "K6": 1604,
            "M6": 0,
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
    ks_zero = [r for r in results if r["stage"] == "primary" and r["state"] == "KS" and r["round"] == "rcv_2" and r["votes"] == 0]
    assert len(ks_zero) == 1 and ks_zero[0]["vote_basis"] == "ranked_choice_round"
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
