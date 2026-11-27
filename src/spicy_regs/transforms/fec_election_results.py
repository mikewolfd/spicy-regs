"""Interpret reviewed multi-cycle FEC Federal Elections workbooks without discarding source rows.

General ballot lines, repeated combined-party totals, ranked-choice voting
rounds, and Presidential electoral votes are distinct observations. Only
explicitly selected ballot-line popular votes may be summed. Appendix and
summary worksheets remain separately queryable in the literal workbook table.
"""

from collections import defaultdict
import hashlib
import json
import re
from typing import Any

import pyarrow as pa

from spicy_docs.sources.fec.election_results import EDITIONS, edition_spec

POLICY = "fec-federal-election-results/2"
NATIVE = "fec_election_workbook_rows"
RESULTS = "fec_election_results"
COMMON = "record_id source_sha256 source_url captured_at source_sheet reader_version".split()
NATIVE_SCHEMA = pa.schema(
    [(n, pa.string()) for n in COMMON + ["cells_json"]]
    + [("source_row", pa.int32()), ("source_cycle", pa.int32())]
)
RESULT_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON
        + (
            "source_column office state state_raw district district_raw seat_term stage round "
            "vote_basis row_kind candidate_id candidate_id_status candidate_name party "
            "votes_raw votes_status percentage_raw footnotes primary_winner_raw "
            "general_winner_raw interpretation_version overlap_note"
        ).split()
    ]
    + [("source_row", pa.int32()), ("source_cycle", pa.int32()), ("votes", pa.int64())]
)
SCHEMAS = {NATIVE: NATIVE_SCHEMA, RESULTS: RESULT_SCHEMA}

OVERLAP_NOTE = (
    "Combined-party witnesses repeat across ballot lines; never add them. "
    "Ranked-choice rounds, electoral votes, state/national totals, and appendix "
    "observations overlap or measure different bases than popular ballot-line votes."
)

# Physical columns are licensed publisher facts, not analytical filters borrowed
# from third-party readers. See spicy-docs/docs/sources/fec-election-results.md.
_CONGRESS_BASE_VOTES: dict[str, tuple[str, str, str]] = {
    "L": ("primary", "reported", "M"),
    "N": ("primary_runoff", "reported", "O"),
    "P": ("general", "reported", "Q"),
    "R": ("general_runoff", "reported", "S"),
    "T": ("general", "combined_party", "U"),
}
_CONGRESS_2022_VOTES: dict[str, tuple[str, str, str]] = {
    **_CONGRESS_BASE_VOTES,
    "Y": ("general", "rcv_1", "Z"),
    "AA": ("general", "rcv_2", "AB"),
    "AC": ("general", "rcv_3", "AD"),
}
_RCV_COLS_2020_PRES: tuple[tuple[str, str], ...] = (
    ("M", "rcv_2"),
    ("N", "rcv_3"),
    ("O", "rcv_4"),
    ("P", "rcv_5"),
    ("Q", "rcv_6"),
    ("R", "rcv_7"),
    ("S", "rcv_8"),
)
_STATE_RE = re.compile(r"^[A-Z]{2}$")
_INT_RE = re.compile(r"^[0-9]+$")
_HOUSE_DISTRICT_RE = re.compile(r"^([0-9]{1,2})(?:\s*-\s*(FULL TERM|UNEXPIRED TERM))?$", re.I)

# Visually verified FEC source-publication discrepancy confirmed in both
# federalelections2020.xlsx (sheet '9. 2020 Pres General Results', rows 569-583)
# and federalelections2020.pdf (page 42): FEC prints 14 Texas candidate lines
# summing to 11,315,516 alongside Total State Votes: 11,315,056 (diff = +460,
# matching the printed Scattered (W) = 460 row). Literal publisher values are
# retained unchanged without silently modifying official figures.
VERIFIED_PUBLISHER_TOTAL_DISCREPANCIES: dict[tuple[int, str, str, str, str], tuple[int, int]] = {
    (2020, "president", "TX", "P", "not_applicable"): (11315516, 11315056),
}

_OFFICE_ID_PREFIX = {"president": "P", "senate": "S", "house": "H"}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _id(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_json(value).encode()).hexdigest()


def _parse_vote(raw: Any) -> tuple[int | None, str]:
    if raw is None:
        return None, "missing"
    if isinstance(raw, str) and _INT_RE.fullmatch(raw):
        votes = int(raw)
        if votes > 2**63 - 1:
            raise ValueError("Vote count exceeds the exact output range")
        return votes, "exact"
    return None, "source_text"


def _candidate_id_fields(raw_cid: Any, office: str) -> tuple[str | None, str]:
    prefix = _OFFICE_ID_PREFIX[office]
    if isinstance(raw_cid, str):
        cleaned = raw_cid.strip()
        if re.fullmatch(rf"{prefix}[A-Z0-9]{{8}}", cleaned):
            return cleaned, "source_id"
        return raw_cid, "not_resolved"
    return None, "not_resolved"


def _has_nonblank(fields: dict[str, Any], cols: tuple[str, ...] | list[str]) -> bool:
    for col in cols:
        val = fields.get(col)
        if isinstance(val, str):
            if val.strip():
                return True
        elif val is not None:
            return True
    return False


def _record(
    *,
    common: dict[str, Any],
    key: list[Any],
    col: str,
    office: str,
    state: str,
    state_raw: str | None,
    district: str,
    district_raw: str | None,
    seat_term: str,
    stage: str,
    round_: str,
    vote_basis: str,
    row_kind: str,
    candidate_id: str | None,
    candidate_id_status: str,
    candidate_name: str | None,
    party: str | None,
    votes_raw: str | None,
    votes_status: str,
    votes: int | None,
    percentage_raw: str | None,
    footnotes: str | None,
    primary_winner_raw: str | None,
    general_winner_raw: str | None,
) -> dict[str, Any]:
    return {
        **common,
        "record_id": _id([*key, col]),
        "source_column": col,
        "office": office,
        "state": state,
        "state_raw": state_raw,
        "district": district,
        "district_raw": district_raw,
        "seat_term": seat_term,
        "stage": stage,
        "round": round_,
        "vote_basis": vote_basis,
        "row_kind": row_kind,
        "candidate_id": candidate_id,
        "candidate_id_status": candidate_id_status,
        "candidate_name": candidate_name,
        "party": party,
        "votes_raw": votes_raw,
        "votes_status": votes_status,
        "votes": votes,
        "percentage_raw": percentage_raw,
        "footnotes": footnotes,
        "primary_winner_raw": primary_winner_raw,
        "general_winner_raw": general_winner_raw,
        "interpretation_version": POLICY,
        "overlap_note": OVERLAP_NOTE,
    }


def _interpret_congress(
    sheet_rows: list[tuple[dict[str, Any], dict[str, Any], list[Any]]],
    *,
    office: str,
    layout: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    votes_map = _CONGRESS_2022_VOTES if layout == "congress_2022" else _CONGRESS_BASE_VOTES
    vote_cols = list(votes_map)
    has_primary_winner = layout in ("congress_2022", "congress_2020")
    primary_w_col = "V" if has_primary_winner else None
    general_w_col = "W" if has_primary_winner else "V"
    footnote_col = "X" if has_primary_winner else "W"

    prior_state: str | None = None
    prior_name: str | None = None
    prior_district: str | None = None
    prior_term: str | None = None

    for row, common, key in sheet_rows:
        if row["row"] == 1:
            continue
        fields = {col: cell["value"] for col, cell in row["cells"].items()}
        state_raw = fields.get("B")
        state_clean = state_raw.strip() if isinstance(state_raw, str) else ""
        state_name = fields.get("C")
        j_norm = (fields.get("J") or "").strip() if isinstance(fields.get("J"), str) else ""
        i_norm = (fields.get("I") or "").strip() if isinstance(fields.get("I"), str) else ""
        has_vote = _has_nonblank(fields, vote_cols)

        if state_clean and _STATE_RE.fullmatch(state_clean):
            if state_clean != prior_state:
                prior_district = prior_term = None
            state = state_clean
            prior_state, prior_name = state_clean, state_name
        elif not state_clean and state_name and state_name == prior_name and prior_state:
            state = prior_state
        elif not state_clean and j_norm in ("Party Votes:", "Total State Votes:") and prior_state:
            state = prior_state
        elif has_vote:
            raise ValueError(f"Unresolved {office} result geography at row {row['row']}")
        else:
            continue

        d_raw = fields.get("D")
        d_clean = d_raw.strip() if isinstance(d_raw, str) else ""

        # Classify row_kind before checking district so blank spacer/section rows skip cleanly.
        if j_norm in ("Party Votes:", "Party Caucus Votes:"):
            row_kind = "party_total"
        elif j_norm in ("Total State Votes:", "Total Votes:"):
            row_kind = "contest_total" if office == "senate" else "state_total"
        elif j_norm in (
            "Total Full Term State Votes:",
            "Total Unexpired Term State Votes:",
            "District Votes:",
            "Full Term District Votes:",
            "Unexpired Term District Votes:",
        ):
            row_kind = "contest_total"
        elif not d_clean and not has_vote and not j_norm:
            # Section-header rows (e.g., "DISTRICT 1", "AT LARGE DISTRICT", "FULL TERM:") and blanks.
            continue
        elif i_norm:
            row_kind = "candidate"
        elif has_vote and isinstance(fields.get("K"), str) and fields["K"].strip() == "W":
            # Unnamed write-in vote row (e.g., 2018 NY-05 row 2581).
            row_kind = "candidate"
        elif not has_vote:
            continue
        else:
            raise ValueError(f"Unreviewed {office} row kind at row {row['row']}")

        if office == "senate":
            if d_clean in ("S", ""):
                district, seat_term = "S", "full"
            elif d_clean.upper() in ("S-FULL TERM", "S - FULL TERM"):
                district, seat_term = "S", "full"
            elif d_clean.upper() in ("S-UNEXPIRED TERM", "S - UNEXPIRED TERM"):
                district, seat_term = "S", "unexpired"
            else:
                raise ValueError(f"Unreviewed Senate seat label: {d_raw!r}")
        else:
            if row_kind == "state_total":
                district, seat_term = "H", "not_applicable"
            else:
                m = _HOUSE_DISTRICT_RE.fullmatch(d_clean)
                if m:
                    district = m.group(1).zfill(2)
                    seat_term = "unexpired" if (m.group(2) and "UNEXPIRED" in m.group(2).upper()) else "full"
                    prior_district, prior_term = district, seat_term
                elif not d_clean and row_kind in ("party_total", "contest_total") and prior_district and prior_term:
                    district, seat_term = prior_district, prior_term
                else:
                    raise ValueError(f"Unreviewed House district label: {d_raw!r} at row {row['row']}")

        cid, cid_status = _candidate_id_fields(fields.get("E"), office)
        cand_name = fields.get("I") if row_kind == "candidate" else None
        for col, (stage, round_, pct_col) in votes_map.items():
            raw = fields.get(col)
            votes, votes_status = _parse_vote(raw)
            vote_basis = (
                "combined_party_witness"
                if col == "T"
                else "ranked_choice_round"
                if col in {"Y", "AA", "AC"}
                else "ballot_line"
                if row_kind == "candidate"
                else "reported_total"
            )
            results.append(
                _record(
                    common=common,
                    key=key,
                    col=col,
                    office=office,
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=d_raw,
                    seat_term=seat_term,
                    stage=stage,
                    round_=round_,
                    vote_basis=vote_basis,
                    row_kind=row_kind,
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=fields.get("K"),
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=fields.get(pct_col),
                    footnotes=fields.get(footnote_col),
                    primary_winner_raw=fields.get(primary_w_col) if primary_w_col else None,
                    general_winner_raw=fields.get(general_w_col),
                )
            )
    return results


def _interpret_pres_general(
    sheet_rows: list[tuple[dict[str, Any], dict[str, Any], list[Any]]],
    *,
    layout: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    is_2020 = layout == "pres_general_2020"
    total_col = "M" if is_2020 else "L"
    pct_col = "L" if is_2020 else "M"
    winner_col = "P" if is_2020 else "N"
    footnote_col = "R" if is_2020 else None

    for row, common, key in sheet_rows:
        if row["row"] == 1:
            continue
        fields = {col: cell["value"] for col, cell in row["cells"].items()}
        state_raw = fields.get("D")
        state_clean = state_raw.strip() if isinstance(state_raw, str) else ""
        i_norm = (fields.get("I") or "").strip() if isinstance(fields.get("I"), str) else ""
        j_norm = (fields.get("J") or "").strip() if isinstance(fields.get("J"), str) else ""
        h_norm = (fields.get("H") or "").strip() if isinstance(fields.get("H"), str) else ""

        if state_clean == "All States" and i_norm == "Total All States:":
            state, district, row_kind = "US", "US", "national_total"
        elif _STATE_RE.fullmatch(state_clean):
            state, district = state_clean, "P"
            if i_norm == "Total State Votes:":
                row_kind = "contest_total"
            elif h_norm:
                row_kind = "candidate"
            else:
                continue
        else:
            if _has_nonblank(fields, ("K", total_col, "N", "Q") if is_2020 else ("K", total_col)):
                raise ValueError(f"Unresolved Presidential general geography at row {row['row']}")
            continue

        cid, cid_status = _candidate_id_fields(fields.get("B"), "president")
        footnotes = fields.get(footnote_col) if footnote_col else None
        winner = fields.get(winner_col)
        party = fields.get("J")
        cand_name = fields.get("H") if row_kind == "candidate" else None

        if row_kind in ("contest_total", "national_total"):
            raw = fields.get(total_col)
            votes, votes_status = _parse_vote(raw)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col=total_col,
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="reported",
                    vote_basis="reported_total",
                    row_kind=row_kind,
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=None,
                    party=party,
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=None,
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=winner,
                )
            )
            if is_2020 and fields.get("Q") is not None:
                ev_raw = fields.get("Q")
                ev_votes, ev_status = _parse_vote(ev_raw)
                results.append(
                    _record(
                        common=common,
                        key=key,
                        col="Q",
                        office="president",
                        state=state,
                        state_raw=state_raw,
                        district=district,
                        district_raw=None,
                        seat_term="not_applicable",
                        stage="general",
                        round_="electoral_vote",
                        vote_basis="electoral_vote",
                        row_kind=row_kind,
                        candidate_id=cid,
                        candidate_id_status=cid_status,
                        candidate_name=None,
                        party=party,
                        votes_raw=ev_raw,
                        votes_status=ev_status,
                        votes=ev_votes,
                        percentage_raw=None,
                        footnotes=footnotes,
                        primary_winner_raw=None,
                        general_winner_raw=winner,
                    )
                )
            continue

        # Candidate row: keep Combined Parties summary rows strictly separate from ballot lines.
        if j_norm == "Combined Parties:":
            comb_col = "N" if is_2020 else "L"
            comb_pct = "O" if is_2020 else "M"
            raw = fields.get(comb_col)
            votes, votes_status = _parse_vote(raw)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col=comb_col,
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="combined_party",
                    vote_basis="combined_party_witness",
                    row_kind="candidate",
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=fields.get(comb_pct),
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=winner,
                )
            )
        else:
            raw = fields.get("K")
            votes, votes_status = _parse_vote(raw)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="K",
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="reported",
                    vote_basis="ballot_line",
                    row_kind="candidate",
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=fields.get(pct_col),
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=winner,
                )
            )
            if is_2020 and fields.get("N") is not None:
                c_raw = fields.get("N")
                c_votes, c_status = _parse_vote(c_raw)
                results.append(
                    _record(
                        common=common,
                        key=key,
                        col="N",
                        office="president",
                        state=state,
                        state_raw=state_raw,
                        district=district,
                        district_raw=None,
                        seat_term="not_applicable",
                        stage="general",
                        round_="combined_party",
                        vote_basis="combined_party_witness",
                        row_kind="candidate",
                        candidate_id=cid,
                        candidate_id_status=cid_status,
                        candidate_name=cand_name,
                        party=party,
                        votes_raw=c_raw,
                        votes_status=c_status,
                        votes=c_votes,
                        percentage_raw=fields.get("O"),
                        footnotes=footnotes,
                        primary_winner_raw=None,
                        general_winner_raw=winner,
                    )
                )

        if is_2020 and fields.get("Q") is not None:
            ev_raw = fields.get("Q")
            ev_votes, ev_status = _parse_vote(ev_raw)
            # Bracketed "[29]" repeats NY's combined electoral total across party lines; keep non-additive.
            ev_basis = (
                "combined_party_witness"
                if isinstance(ev_raw, str) and ev_raw.strip().startswith("[")
                else "electoral_vote"
            )
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="Q",
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="electoral_vote",
                    vote_basis=ev_basis,
                    row_kind="candidate",
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=ev_raw,
                    votes_status=ev_status,
                    votes=ev_votes,
                    percentage_raw=None,
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=winner,
                )
            )
    return results


def _interpret_pres_electoral_2016(
    sheet_rows: list[tuple[dict[str, Any], dict[str, Any], list[Any]]],
) -> list[dict[str, Any]]:
    """Extract only Electoral Vote columns from 2016 Table 2 without duplicating popular votes."""
    by_row = {row["row"]: (row, common, key) for row, common, key in sheet_rows}
    notes = []
    for rnum in sorted(by_row):
        if rnum > 56:
            txt = by_row[rnum][0]["cells"].get("A", {}).get("value")
            if isinstance(txt, str) and txt.strip():
                notes.append(txt.strip())
    table_footnotes = " ".join(notes) or None

    candidates = (("B", "Trump, Donald J.", "R"), ("C", "Clinton, Hillary", "D"))
    results: list[dict[str, Any]] = []
    for rnum in sorted(by_row):
        if rnum < 5:
            continue
        row, common, key = by_row[rnum]
        fields = {col: cell["value"] for col, cell in row["cells"].items()}
        state_raw = fields.get("A")
        state_clean = state_raw.strip() if isinstance(state_raw, str) else ""
        if _STATE_RE.fullmatch(state_clean):
            state, district, row_kind = state_clean, "P", "candidate"
        elif state_clean.startswith("Total"):
            state, district, row_kind = "US", "US", "national_total"
        else:
            continue
        for col, cand_name, party in candidates:
            raw = fields.get(col)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            votes, votes_status = _parse_vote(raw)
            row_note = table_footnotes if (isinstance(raw, str) and "*" in raw) else None
            results.append(
                _record(
                    common=common,
                    key=key,
                    col=col,
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="electoral_vote",
                    vote_basis="electoral_vote",
                    row_kind=row_kind,
                    candidate_id=None,
                    candidate_id_status="not_resolved",
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=None,
                    footnotes=row_note,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
    return results


def _interpret_pres_primary(
    sheet_rows: list[tuple[dict[str, Any], dict[str, Any], list[Any]]],
    *,
    layout: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    is_2020 = layout == "pres_primary_2020"
    footnote_col = "T" if is_2020 else "M"
    current_state: str | None = None
    rcv_mode: str | None = None

    for row, common, key in sheet_rows:
        if row["row"] == 1:
            continue
        fields = {col: cell["value"] for col, cell in row["cells"].items()}
        state_raw = fields.get("D")
        state_clean = state_raw.strip() if isinstance(state_raw, str) else ""
        if not _STATE_RE.fullmatch(state_clean):
            if _has_nonblank(fields, ("K",)):
                raise ValueError(f"Unresolved Presidential primary geography at row {row['row']}")
            continue
        if state_clean != current_state:
            current_state = state_clean
            rcv_mode = None

        k_val = fields.get("K")
        k_clean = k_val.strip() if isinstance(k_val, str) else ""
        if is_2020 and k_clean.startswith("Final Round (Round "):
            m = re.fullmatch(r"Final Round \(Round ([0-9]+)\)", k_clean)
            if not m:
                raise ValueError(f"Unreviewed Presidential RCV header: {k_val!r}")
            rcv_mode = f"rcv_{m.group(1)}"
            continue
        if is_2020 and k_clean == "First Round (Round 1)":
            rcv_mode = "multi_round"
            continue

        i_norm = (fields.get("I") or "").strip() if isinstance(fields.get("I"), str) else ""
        h_norm = (fields.get("H") or "").strip() if isinstance(fields.get("H"), str) else ""
        if i_norm == "Total Party Votes:":
            row_kind = "party_total"
        elif i_norm == "Total State Votes:":
            row_kind = "contest_total"
        elif h_norm:
            row_kind = "candidate"
        else:
            continue

        cid, cid_status = _candidate_id_fields(fields.get("B"), "president")
        footnotes = fields.get(footnote_col)
        party = fields.get("J")
        cand_name = fields.get("H") if row_kind == "candidate" else None

        if rcv_mode is None:
            votes, votes_status = _parse_vote(k_val)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="K",
                    office="president",
                    state=state_clean,
                    state_raw=state_raw,
                    district="P",
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="primary",
                    round_="reported",
                    vote_basis="ballot_line" if row_kind == "candidate" else "reported_total",
                    row_kind=row_kind,
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=k_val,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=fields.get("L"),
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
        elif rcv_mode == "multi_round":
            k_votes, k_status = _parse_vote(k_val)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="K",
                    office="president",
                    state=state_clean,
                    state_raw=state_raw,
                    district="P",
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="primary",
                    round_="rcv_1",
                    vote_basis="ranked_choice_round",
                    row_kind=row_kind,
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=k_val,
                    votes_status=k_status,
                    votes=k_votes,
                    percentage_raw=fields.get("L"),
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
            for col, round_label in _RCV_COLS_2020_PRES:
                r_val = fields.get(col)
                if r_val is None or (isinstance(r_val, str) and not r_val.strip()):
                    continue
                r_votes, r_status = _parse_vote(r_val)
                results.append(
                    _record(
                        common=common,
                        key=key,
                        col=col,
                        office="president",
                        state=state_clean,
                        state_raw=state_raw,
                        district="P",
                        district_raw=None,
                        seat_term="not_applicable",
                        stage="primary",
                        round_=round_label,
                        vote_basis="ranked_choice_round",
                        row_kind=row_kind,
                        candidate_id=cid,
                        candidate_id_status=cid_status,
                        candidate_name=cand_name,
                        party=party,
                        votes_raw=r_val,
                        votes_status=r_status,
                        votes=r_votes,
                        percentage_raw=None,
                        footnotes=footnotes,
                        primary_winner_raw=None,
                        general_winner_raw=None,
                    )
                )
        else:
            k_votes, k_status = _parse_vote(k_val)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="K",
                    office="president",
                    state=state_clean,
                    state_raw=state_raw,
                    district="P",
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="primary",
                    round_=rcv_mode,
                    vote_basis="ranked_choice_round",
                    row_kind=row_kind,
                    candidate_id=cid,
                    candidate_id_status=cid_status,
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=k_val,
                    votes_status=k_status,
                    votes=k_votes,
                    percentage_raw=fields.get("L"),
                    footnotes=footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
    return results


def _interpret_pres_matrix_2024(
    sheet_rows: list[tuple[dict[str, Any], dict[str, Any], list[Any]]],
) -> list[dict[str, Any]]:
    by_row = {row["row"]: (row, common, key) for row, common, key in sheet_rows}
    if 1 not in by_row:
        raise ValueError("Missing header row in 2024 Presidential results")
    headers = {col: cell["value"] for col, cell in by_row[1][0]["cells"].items() if cell["value"] is not None}
    pct_row = {col: cell["value"] for col, cell in by_row[54][0]["cells"].items()} if 54 in by_row else {}
    note_lines = []
    for rnum in sorted(by_row):
        if rnum >= 56:
            txt = by_row[rnum][0]["cells"].get("B", {}).get("value")
            if isinstance(txt, str) and txt.strip():
                note_lines.append(txt.strip())
    sheet_footnotes = " ".join(note_lines) or None

    ev_candidates = (("C", "TRUMP", "R"), ("D", "HARRIS", "D"))
    pop_cols = [col for col in headers if col not in ("A", "B", "C", "D", "AE")]
    results: list[dict[str, Any]] = []

    for rnum in sorted(by_row):
        if rnum == 1:
            continue
        row, common, key = by_row[rnum]
        fields = {col: cell["value"] for col, cell in row["cells"].items()}
        state_raw = fields.get("A")
        state_clean = state_raw.strip() if isinstance(state_raw, str) else ""
        if _STATE_RE.fullmatch(state_clean):
            state, district, is_total_row = state_clean, "P", False
        elif state_clean == "Total:":
            state, district, is_total_row = "US", "US", True
        else:
            continue

        # Electoral vote total (col B) and candidate electoral votes (cols C, D).
        if fields.get("B") is not None:
            b_votes, b_status = _parse_vote(fields.get("B"))
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="B",
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="electoral_vote",
                    vote_basis="electoral_vote",
                    row_kind="national_total" if is_total_row else "contest_total",
                    candidate_id=None,
                    candidate_id_status="not_resolved",
                    candidate_name=None,
                    party=None,
                    votes_raw=fields.get("B"),
                    votes_status=b_status,
                    votes=b_votes,
                    percentage_raw=None,
                    footnotes=sheet_footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
        for col, cand_name, party in ev_candidates:
            raw = fields.get(col)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            votes, votes_status = _parse_vote(raw)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col=col,
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="electoral_vote",
                    vote_basis="electoral_vote",
                    row_kind="national_total" if is_total_row else "candidate",
                    candidate_id=None,
                    candidate_id_status="not_resolved",
                    candidate_name=cand_name,
                    party=party,
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=None,
                    footnotes=sheet_footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )

        # Popular votes by candidate column (cols E..AD) and state/national total (col AE).
        for col in pop_cols:
            raw = fields.get(col)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            votes, votes_status = _parse_vote(raw)
            results.append(
                _record(
                    common=common,
                    key=key,
                    col=col,
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="reported",
                    vote_basis="reported_total" if is_total_row else "ballot_line",
                    row_kind="national_total" if is_total_row else "candidate",
                    candidate_id=None,
                    candidate_id_status="not_resolved",
                    candidate_name=headers[col],
                    party="W" if headers[col] == "WRITE-INS (SCATTERED)" else None,
                    votes_raw=raw,
                    votes_status=votes_status,
                    votes=votes,
                    percentage_raw=pct_row.get(col) if is_total_row else None,
                    footnotes=sheet_footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
        if fields.get("AE") is not None:
            ae_votes, ae_status = _parse_vote(fields.get("AE"))
            results.append(
                _record(
                    common=common,
                    key=key,
                    col="AE",
                    office="president",
                    state=state,
                    state_raw=state_raw,
                    district=district,
                    district_raw=None,
                    seat_term="not_applicable",
                    stage="general",
                    round_="reported",
                    vote_basis="reported_total",
                    row_kind="national_total" if is_total_row else "contest_total",
                    candidate_id=None,
                    candidate_id_status="not_resolved",
                    candidate_name=None,
                    party=None,
                    votes_raw=fields.get("AE"),
                    votes_status=ae_status,
                    votes=ae_votes,
                    percentage_raw=pct_row.get("AE") if is_total_row else None,
                    footnotes=sheet_footnotes,
                    primary_winner_raw=None,
                    general_winner_raw=None,
                )
            )
    return results


def reconcile_general_contests(results: list[dict[str, Any]]) -> dict[str, int]:
    """Reconcile every interpreted general contest ballot-line sum against official contest totals."""
    ballot_sums: dict[tuple[int, str, str, str, str], int] = defaultdict(int)
    contest_totals: dict[tuple[int, str, str, str, str], int] = {}
    for rec in results:
        if rec["stage"] != "general" or rec["round"] != "reported":
            continue
        contest = (rec["source_cycle"], rec["office"], rec["state"], rec["district"], rec["seat_term"])
        if rec["row_kind"] == "candidate" and rec["vote_basis"] == "ballot_line":
            if rec["votes_status"] == "exact" and rec["votes"] is not None:
                ballot_sums[contest] += rec["votes"]
        elif rec["row_kind"] == "contest_total" and rec["vote_basis"] == "reported_total":
            if rec["votes_status"] == "exact" and rec["votes"] is not None:
                if contest in contest_totals:
                    raise ValueError(f"Duplicate official general contest total for {contest}")
                contest_totals[contest] = rec["votes"]

    mismatches = []
    verified_discrepancies = 0
    for contest in sorted(set(ballot_sums) | set(contest_totals)):
        expected = contest_totals.get(contest)
        observed = ballot_sums.get(contest, 0)
        if expected is None:
            # Contests with unopposed/non-numeric general total may have no numeric total; otherwise refuse.
            if observed > 0:
                mismatches.append((contest, observed, expected))
        elif observed != expected:
            if VERIFIED_PUBLISHER_TOTAL_DISCREPANCIES.get(contest) == (observed, expected):
                verified_discrepancies += 1
            else:
                mismatches.append((contest, observed, expected))
    if mismatches:
        sample = mismatches[:5]
        raise ValueError(f"General contest ballot-line totals failed reconciliation ({len(mismatches)} diffs): {sample}")
    return {
        "reconciled_contests": len(contest_totals) - verified_discrepancies,
        "verified_publisher_discrepancies": verified_discrepancies,
    }


def interpret(rows, capture: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return literal workbook rows and interpreted election results across reviewed sheets."""
    native: list[dict[str, Any]] = []
    grouped: dict[tuple[int, str], list[tuple[dict[str, Any], dict[str, Any], list[Any]]]] = defaultdict(list)

    for row in rows:
        if row["source_sha256"] != capture["response_sha256"]:
            raise ValueError("Election row is outside its selected capture")
        common = dict(
            source_sha256=row["source_sha256"],
            source_url=capture["request_url"],
            captured_at=capture["observed_at"],
            source_sheet=row["sheet"],
            source_row=row["row"],
            source_cycle=row["cycle"],
            reader_version=row["reader_version"],
        )
        key = [row["source_sha256"], row["sheet"], row["row"]]
        native.append({**common, "record_id": _id(key), "cells_json": _json(row["cells"])})
        if row["cycle"] in EDITIONS:
            grouped[(row["cycle"], row["sheet"])].append((row, common, key))

    results: list[dict[str, Any]] = []
    cycles_seen = sorted({cycle for cycle, _ in grouped})
    for cycle in cycles_seen:
        spec = edition_spec(cycle)
        for sheet_spec in spec.sheets:
            sheet_rows = grouped.get((cycle, sheet_spec.name), [])
            if not sheet_rows:
                continue
            if sheet_spec.layout.startswith("congress_"):
                results.extend(_interpret_congress(sheet_rows, office=sheet_spec.office, layout=sheet_spec.layout))
            elif sheet_spec.layout in ("pres_general_2020", "pres_general_2016"):
                results.extend(_interpret_pres_general(sheet_rows, layout=sheet_spec.layout))
            elif sheet_spec.layout == "pres_electoral_table2_2016":
                results.extend(_interpret_pres_electoral_2016(sheet_rows))
            elif sheet_spec.layout in ("pres_primary_2020", "pres_primary_2016"):
                results.extend(_interpret_pres_primary(sheet_rows, layout=sheet_spec.layout))
            elif sheet_spec.layout == "pres_matrix_2024":
                results.extend(_interpret_pres_matrix_2024(sheet_rows))
            else:
                raise ValueError(f"Unsupported reviewed worksheet layout: {sheet_spec.layout}")

    if not results:
        raise ValueError("No reviewed election results in selected workbook")
    return {NATIVE: native, RESULTS: results}
