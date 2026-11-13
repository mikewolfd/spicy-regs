"""Interpret the reviewed FEC Senate worksheet without discarding source rows.

General ballot lines, repeated combined-party totals and RCV rounds are different
observations. Only explicitly selected ballot-line votes may be added. Appendix
and summary rows remain separately queryable in the literal workbook table.
"""

import hashlib
import json
import re

import pyarrow as pa

from spicy_docs.sources.fec.election_results import SENATE_2022

POLICY = "fec-senate-results/1"
NATIVE = "fec_election_workbook_rows"
RESULTS = "fec_election_results"
COMMON = "record_id source_sha256 source_url captured_at source_sheet reader_version".split()
NATIVE_SCHEMA = pa.schema([(n, pa.string()) for n in COMMON + ["cells_json"]]
                         + [("source_row", pa.int32()), ("source_cycle", pa.int32())])
RESULT_SCHEMA = pa.schema([(n, pa.string()) for n in COMMON + (
    "source_column state state_raw district_raw seat_term stage round vote_basis row_kind candidate_id "
    "candidate_id_status candidate_name party votes_raw votes_status percentage_raw footnotes "
    "primary_winner_raw general_winner_raw interpretation_version overlap_note"
).split()] + [("source_row", pa.int32()), ("source_cycle", pa.int32()), ("votes", pa.int64())])
SCHEMAS = {NATIVE: NATIVE_SCHEMA, RESULTS: RESULT_SCHEMA}
# Physical columns are licensed publisher facts, not analytical filters borrowed
# from third-party readers. See docs/fec-election-results.md for reuse decisions.
VOTES = {"L": ("primary", "reported", "M"), "N": ("primary_runoff", "reported", "O"),
         "P": ("general", "reported", "Q"), "R": ("general_runoff", "reported", "S"),
         "T": ("general", "combined_party", "U"), "Y": ("general", "rcv_1", "Z"),
         "AA": ("general", "rcv_2", "AB"), "AC": ("general", "rcv_3", "AD")}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _id(value):
    return "sha256:" + hashlib.sha256(_json(value).encode()).hexdigest()


def interpret(rows, capture):
    """Return literal rows and Senate vote observations; missing never means zero."""
    native, results = [], []
    prior_state = prior_name = None
    for row in rows:
        if row["source_sha256"] != capture["response_sha256"]:
            raise ValueError("Election row is outside its selected capture")
        common = dict(source_sha256=row["source_sha256"], source_url=capture["request_url"],
                      captured_at=capture["observed_at"], source_sheet=row["sheet"], source_row=row["row"],
                      source_cycle=row["cycle"], reader_version=row["reader_version"])
        key = [row["source_sha256"], row["sheet"], row["row"]]
        native.append({**common, "record_id": _id(key), "cells_json": _json(row["cells"])})
        if row["sheet"] != SENATE_2022 or row["row"] == 1:
            continue
        fields = {col: cell["value"] for col, cell in row["cells"].items()}
        # Publisher leaves B blank on some totals; inherit only within its
        # explicitly repeated full state name. The raw blank remains available.
        state, name = fields.get("B"), fields.get("C")
        if state and re.fullmatch(r"[A-Z]{2}", state):
            prior_state, prior_name = state, name
        elif not state and name and name == prior_name:
            state = prior_state
        elif any(fields.get(c) is not None for c in VOTES):
            raise ValueError(f"Unresolved Senate result geography at row {row['row']}")
        else:
            continue
        district = fields.get("D")
        if district not in ("S", "S ", "S-Full Term", "S-Unexpired Term"):
            raise ValueError(f"Unreviewed Senate seat label: {district!r}")
        kind = "candidate" if fields.get("I") else "party_total" if fields.get("J") == "Party Votes:" else (
            "contest_total" if fields.get("J") == "Total State Votes:" else "unresolved")
        if kind == "unresolved":
            raise ValueError(f"Unreviewed Senate row kind at row {row['row']}")
        cid = fields.get("E")
        for col, (stage, round_, percentage) in VOTES.items():
            raw = fields.get(col)
            exact = isinstance(raw, str) and re.fullmatch(r"[0-9]+", raw) is not None
            votes = int(raw) if exact else None
            if votes is not None and votes > 2**63 - 1:
                raise ValueError("Vote count exceeds the exact output range")
            results.append({**common, "record_id": _id([*key, col]), "source_column": col,
                "state": state, "state_raw": fields.get("B"), "district_raw": district,
                "seat_term": "unexpired" if district == "S-Unexpired Term" else "full",
                "stage": stage, "round": round_, "row_kind": kind,
                "vote_basis": "combined_party_witness" if col == "T" else "ranked_choice_round" if col in
                              {"Y", "AA", "AC"} else "ballot_line" if kind == "candidate" else "reported_total",
                "candidate_id": cid, "candidate_id_status": "source_id" if re.fullmatch(r"S[A-Z0-9]{8}", cid or "")
                                else "not_resolved", "candidate_name": fields.get("I"), "party": fields.get("K"),
                "votes_raw": raw, "votes_status": "exact" if exact else "missing" if raw is None else "source_text",
                "votes": votes, "percentage_raw": fields.get(percentage), "footnotes": fields.get("X"),
                "primary_winner_raw": fields.get("V"), "general_winner_raw": fields.get("W"),
                "interpretation_version": POLICY,
                "overlap_note": "Combined-party witnesses repeat across ballot lines; never add them. "
                                "Rounds and appendix observations overlap other results."})
    if not results:
        raise ValueError("No Senate results in selected workbook")
    return {NATIVE: native, RESULTS: results}
