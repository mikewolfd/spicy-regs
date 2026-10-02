"""Pins the MCP's ledger qualification: built from the ledger, refused when stale, reported only for its publisher."""

import json

import duckdb
import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs import mcp_server, output_ledger
from spicy_regs.sources import publication
from tests.test_check_ledger_pins import _family
from tests.test_mcp_server import _tool_data

DESTINATION = "https://pub-fork.example"
LEDGER = f"""\
Public data destination: `{DESTINATION}/`. Local candidates are not fork publications.

| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T10 | `run-rollup-committee-reports` | `committee_reports.parquet` | FAILED at `5e1e73ad…` (2026-09-25): \
the notice publishes `not_flagged` |
| T08 | `run-rollup-laws` | `laws.parquet` | qualified at `43130abc…` (2026-09-25): every row replays |
| T16 | `run-rollup-bill-subjects` | `bill_subjects.parquet` | current `1b067027…` is PARTIAL (2026-09-25); \
earlier selection qualified at `86137cc2…` (2026-09-23) |
| T04 | `run-rollup-fec-committees` | `fec_committees.parquet` | qualified at `dfda14da…` (2026-09-21). \
Published at `4b1ca622…` (2026-09-25); the field audit is pending |
| T06 | `run-pipeline` | `dockets.parquet` | publication verified at table digest `308b35c6…` \
(2026-09-26; ETag `50d8cfba…`) |
| T15 | `run-rollup-discovery-signals` | `discovery_signals.parquet` | withdrawn 2026-09-23 |
| T17 | `materialize-rulemaking` | `rulemaking_lifecycles.parquet` | qualified at `snapshot_91b19da7…` (2026-09-27) |
| T06 | `run-pipeline` | `comments/agency_code=<agency>/part-0.parquet` | unproduced on the fork |
"""
RECORD = output_ledger.qualification_record(LEDGER)
BUNDLED_DESTINATION = output_ledger.ledger_destination(output_ledger.LEDGER.read_text(encoding="utf-8"))
SERVED = ("committee_reports", "laws", "bill_subjects", "fec_committees", "dockets", "discovery_signals")


def _index(committee_reports: str = "95810b26", bill_subjects: str = "1b067027") -> dict:
    """Live pins: laws and bill_subjects match the ledger, fec_committees has moved on, dockets is a managed table."""
    families = {
        "committee-reports": _family("committee-reports", committee_reports, {"committee_reports.parquet": 143}),
        "laws": _family("laws", "43130abc", {"laws.parquet": 113}),
        "bill-subjects": _family("bill-subjects", bill_subjects, {"bill_subjects.parquet": 1}),
        "fec-committees": _family("fec-committees", "4b1ca622", {"fec_committees.parquet": 1}),
        "dockets": _family("dockets", "abcdef01", {"dockets.parquet": 1}, table_head="308b35c6"),
    }
    document = {"format": "spicy-regs-publication", "version": 1, "families": families}
    return publication.parse_index(json.dumps(document).encode())


def _serve(monkeypatch, index: dict, *, bundled: bool = False):
    """A server over an injected connection pinned to ``index``, reading the (fixture or bundled) ledger's publisher."""
    con = duckdb.connect()
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
    for table in SERVED:
        con.execute(f'CREATE TABLE "{table}" (id VARCHAR)')
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    if not bundled:
        monkeypatch.setattr(mcp_server, "_ledger", lambda: mcp_server._ledger_index(RECORD))
    monkeypatch.setattr(mcp_server, "R2_BASE_URL", BUNDLED_DESTINATION if bundled else DESTINATION)
    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    return mcp_server.build_server()


def _audits(table: str) -> list[dict]:
    return [audit for row in RECORD["rows"] if table in row["tables"] for audit in row["audits"]]


def test_each_audit_keeps_the_rows_word_pin_and_kind():
    assert RECORD["destination"] == DESTINATION
    assert _audits("committee_reports") == [
        {"disposition": "FAILED", "pin": "5e1e73ad", "pin_kind": "artifact", "date": "2026-09-25"}
    ]
    assert [(a["disposition"], a["pin"]) for a in _audits("bill_subjects")] == [
        ("PARTIAL", "1b067027"),
        ("qualified", "86137cc2"),
    ]
    assert [a["pin"] for a in _audits("fec_committees")] == ["dfda14da"]  # "Published at" is not an audit
    assert _audits("dockets") == [
        {"disposition": "verified", "pin": "308b35c6", "pin_kind": "table", "date": "2026-09-26", "etag": "50d8cfba"}
    ]
    [withdrawn] = [row for row in RECORD["rows"] if row["tables"] == ["discovery_signals"]]
    assert (withdrawn["audits"], withdrawn["statement"]) == ([], "withdrawn 2026-09-23")
    assert [row["task"] for row in RECORD["rows"]].count("T06") == 1  # a partition pattern names no table


@pytest.mark.parametrize(
    "state",
    [
        "re-qualified at `7007ca03…`; live not re-qualified",
        "PUBLISHED-UNAUDITED at `12345678…` (2026-09-25)",
        "current `5990abbb…` is partial (2026-09-25)",
        "verified at table digest `27a2ed4a…` (2026-09-21)",
    ],
)
def test_audit_wording_outside_the_recognized_phrases_refuses(state):
    ledger = f"Public data destination: `{DESTINATION}`\n| T08 | `x` | `treaties.parquet` | {state} |\n"
    with pytest.raises(ValueError, match=r"T08 \(treaties.parquet\)"):
        output_ledger.qualification_record(ledger)


def test_the_bundled_record_is_a_fresh_build_of_the_ledger():
    assert dd.qualification_errors() == []


@pytest.mark.parametrize(("live_pin", "generation"), [
    ("dc9634ba", "current generation audited"),
    ("01234567", "newer generation, not yet audited"),
])
def test_candidate_retained_audit_is_recorded_only_for_its_generation(live_pin, generation):
    """The retained whole-table comparison qualifies its selected generation, not subsequent refreshes."""
    record = output_ledger.qualification_record(output_ledger.LEDGER.read_text(encoding="utf-8"))
    rows = [row for row in record["rows"] if "fec_candidate_history" in row["tables"]]
    result = mcp_server._table_qualification(rows, {"artifact": live_pin}, statements=True)
    assert result["status"] == "recorded"
    assert result["generation"] == generation
    assert (result["ledger_pin"], result["ledger_date"], result["ledger_disposition"]) == (
        "dc9634ba", "2026-09-30", "qualified",
    )
    assert result["ledger_statements"] == [row["statement"] for row in rows]


def test_a_stale_record_fails_the_dictionary_check(tmp_path, monkeypatch, capsys):
    ledger, record = tmp_path / "ledger.md", tmp_path / "table_qualification.json"
    ledger.write_text(LEDGER, encoding="utf-8")
    monkeypatch.setattr(output_ledger, "LEDGER", ledger)
    monkeypatch.setattr(output_ledger, "RECORD", record)
    record.write_bytes(dd.qualification_bytes())
    assert dd.qualification_errors() == []
    ledger.write_text(LEDGER.replace("FAILED at `5e1e73ad…`", "qualified at `5e1e73ad…`"), encoding="utf-8")
    assert dd.cmd_check(dd.build_parser().parse_args(["check"])) == 1
    assert "table_qualification.json is stale" in capsys.readouterr().err
    record.unlink()
    assert "is stale" in dd.qualification_errors()[0]


def _audit(server, table: str) -> dict:
    """describe_table's audit for ``table``, without the ledger scope and statements every table shares."""
    report = _tool_data(server, "describe_table", {"table": table})["qualification"]
    shared = {"ledger", "ledger_destination", "basis", "ledger_tasks", "ledger_statements"}
    return {key: value for key, value in report.items() if key not in shared}


def test_live_and_ledger_pins_are_separate_fields_never_one_verified_flag(monkeypatch):
    server = _serve(monkeypatch, _index())
    tables = {
        name: _audit(server, name) for name in ("laws", "fec_committees", "bill_subjects", "dockets", "discovery_signals")
    }
    assert tables["laws"] == {
        "status": "recorded",
        "generation": "current generation audited",
        "live_pin": "43130abc",
        "ledger_pin": "43130abc",
        "pin_kind": "artifact",
        "ledger_date": "2026-09-25",
        "ledger_disposition": "qualified",
    }
    fec = tables["fec_committees"]
    assert fec["generation"] == "newer generation, not yet audited"
    assert (fec["live_pin"], fec["ledger_pin"], fec["ledger_disposition"]) == ("4b1ca622", "dfda14da", "qualified")
    # The live generation's PARTIAL audit is reported, not the older selection the same row qualified.
    subjects = tables["bill_subjects"]
    assert (subjects["generation"], subjects["ledger_disposition"]) == ("current generation audited", "PARTIAL")
    # A base object's table digest is compared with its managed table digest, not the family pin.
    assert (tables["dockets"]["pin_kind"], tables["dockets"]["live_pin"]) == ("table", "308b35c6")
    assert tables["dockets"]["generation"] == "current generation audited"
    assert tables["discovery_signals"] == {"status": "no_audit_recorded", "live_pin": None}
    assert not any("verified" in key for entry in tables.values() for key in entry)


def test_a_drifted_table_reports_the_ledgers_latest_audit_not_its_last_phrase(monkeypatch):
    server = _serve(monkeypatch, _index(bill_subjects="750784bd"))
    subjects = _audit(server, "bill_subjects")
    assert subjects["generation"] == "newer generation, not yet audited"
    assert (subjects["ledger_pin"], subjects["ledger_date"], subjects["ledger_disposition"]) == (
        "1b067027",
        "2026-09-25",
        "PARTIAL",
    )


@pytest.mark.parametrize(
    ("live", "generation"),
    [("95810b26", "newer generation, not yet audited"), ("5e1e73ad", "current generation audited")],
)
def test_a_failed_disposition_is_reported_verbatim_with_its_statement(monkeypatch, live, generation):
    server = _serve(monkeypatch, _index(committee_reports=live))
    described = _tool_data(server, "describe_table", {"table": "committee_reports"})["qualification"]
    assert described["generation"] == generation
    assert (described["live_pin"], described["ledger_pin"]) == (live, "5e1e73ad")
    assert described["ledger_disposition"] == "FAILED"
    assert described["ledger_tasks"] == ["T10"]
    assert described["ledger_statements"] == ["FAILED at `5e1e73ad…` (2026-09-25): the notice publishes `not_flagged`"]
    assert described["ledger_destination"] == DESTINATION


def test_a_table_the_index_does_not_manage_cannot_be_compared(monkeypatch):
    index = _index()
    del index["families"]["dockets"]
    described = _tool_data(_serve(monkeypatch, index), "describe_table", {"table": "dockets"})["qualification"]
    assert described["generation"] == "live generation not pinned by the publication index; cannot compare"
    assert (described["live_pin"], described["ledger_pin"], described["ledger_disposition"]) == (
        None,
        "308b35c6",
        "verified",
    )


@pytest.mark.parametrize(
    ("snapshot_id", "generation"),
    [("snapshot_91b19da7" + "0" * 24, "current generation audited"), ("snapshot_" + "f" * 32, "newer generation, not yet audited")],
)
def test_a_rulemaking_table_is_compared_with_the_snapshot_its_pointer_named(monkeypatch, snapshot_id, generation):
    server = _serve(monkeypatch, _index())
    con = mcp_server._get_connection()
    pinned = {"snapshot_id": snapshot_id, "tables": {"rulemaking_lifecycles.parquet": {"sha256": "0" * 64, "rows": 0}},
              "manifest": {"snapshot_id": snapshot_id}}  # the shape publication.load_rulemaking_snapshot pins
    con.execute("CREATE TABLE _spicy_rulemaking (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_rulemaking VALUES (?)", [json.dumps(pinned)])
    con.execute("CREATE TABLE rulemaking_lifecycles (id VARCHAR)")
    described = _tool_data(server, "describe_table", {"table": "rulemaking_lifecycles"})
    report = described["qualification"]
    assert (report["pin_kind"], report["live_pin"], report["ledger_pin"]) == ("snapshot", snapshot_id[:17], "snapshot_91b19da7")
    assert (report["generation"], report["ledger_disposition"]) == (generation, "qualified")
    assert described["publication"]["snapshot_id"] == snapshot_id


@pytest.mark.parametrize("reads", ["another publisher", "a local directory"])
def test_qualification_is_unknown_away_from_the_ledgers_publisher(monkeypatch, tmp_path, reads):
    server = _serve(monkeypatch, _index())
    if reads == "another publisher":
        monkeypatch.setattr(mcp_server, "R2_BASE_URL", "https://data.spicy-regs.dev")
    else:
        monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    described = _tool_data(server, "describe_table", {"table": "committee_reports"})["qualification"]
    assert described["status"] == "unknown_for_publisher"
    assert DESTINATION in described["reason"]
    assert not {"tables", "ledger_pin", "ledger_disposition", "generation"} & set(described)


def test_the_bundled_record_is_read_once_per_process(monkeypatch):
    reads = []
    real = mcp_server.files
    monkeypatch.setattr(mcp_server, "files", lambda package: reads.append(package) or real(package))
    mcp_server._ledger.cache_clear()
    mcp_server._table_metadata.cache_clear()
    mcp_server._joins.cache_clear()
    server = _serve(monkeypatch, _index(), bundled=True)
    for _ in range(3):
        described = _tool_data(server, "describe_table", {"table": "laws"})
    assert len(reads) == 3  # table_metadata.json, table_qualification.json and table_joins.json, once each
    assert described["qualification"]["ledger_destination"] == BUNDLED_DESTINATION
    assert described["qualification"]["status"] != "unknown_for_publisher"


@pytest.mark.parametrize("state", [
    "qualified with limits at `snapshot_dcda51db…` (2026-09-26): an unrecognized audit phrase",
    "qualified at `350e49f5…` (2026-09-25 run; audited 2026-09-26): a date the phrase does not take",
])
def test_an_audit_phrase_with_words_or_a_date_outside_the_vocabulary_refuses(state):
    """A near-miss spelling must fail the build, never read as "no audit recorded"."""
    text = "Public data destination: `https://pub.example`\n| T1 | `run-x` | `x.parquet` | " + state + " |\n"
    with pytest.raises(ValueError, match="outside the recognized phrases"):
        output_ledger.qualification_record(text)
