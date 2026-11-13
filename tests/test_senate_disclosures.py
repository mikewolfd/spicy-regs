"""Small consumer/failure checks; live-source acceptance is recorded separately."""

import json

import duckdb
import httpx
import pytest

from spicy_docs.sources.senate_disclosures import ORIGIN, HEADERS, SenateDisclosureError, SenateEfdReader
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.senate_disclosures import amount_range, build, capture_year

IDS = [
    "10000000-0000-0000-0000-000000000001",
    "10000000-0000-0000-0000-000000000002",
    "10000000-0000-0000-0000-000000000003",
]


def rows():
    return [
        [
            "Jane",
            "Example",
            "Senator",
            f'<a href="/search/view/{kind}/{identity}/">Periodic Transaction Report for 06/01/2025{amendment}</a>',
            filed,
        ]
        for identity, kind, amendment, filed in [
            (IDS[0], "ptr", "", "06/01/2025"),
            (IDS[1], "ptr", " (Amendment 1)", "06/02/2025"),
            (IDS[2], "paper", "", "06/01/2025"),
        ]
    ]


def report(amended=False):
    suffix, filed = (" (Amendment 1)", "06/02/2025") if amended else ("", "06/01/2025")
    cells = ["1", "05/15/2025", "", "--", "Example Asset", "Stock", "Purchase", "$1,001 - $15,000", "--"]
    return (
        f"<h1>Periodic Transaction Report for 06/01/2025{suffix}</h1>"
        f'<h2 class="filedReport">Jane Example (Example, Jane)</h2><p class="muted"><strong>Filed {filed} @ 1:01 PM</strong></p>'
        "<h3>Transactions</h3><ul><li>(1 transactions total)</li></ul><table><thead><tr>"
        + "".join(f"<th>{h}</th>" for h in HEADERS)
        + "</tr></thead><tbody><tr>"
        + "".join(f"<td>{cell}</td>" for cell in cells)
        + "</tr></tbody></table>"
    ).encode()


def reader(requests, *, data=None):
    def respond(request):
        requests.append(str(request.url))
        is_search = request.url.path == "/search/report/data/"
        body = (
            json.dumps(
                {"result": "ok", "recordsTotal": 3, "recordsFiltered": 3, "data": rows() if data is None else data}
            ).encode()
            if is_search
            else report(IDS[1] in str(request.url))
        )
        return httpx.Response(
            200,
            stream=httpx.ByteStream(body),
            headers={"Content-Type": "application/json" if is_search else "text/html"},
        )

    return SenateEfdReader(
        cookie="sessionid=private", csrf="csrf-private", transport=httpx.MockTransport(respond), interval=0
    )


def test_capture_replay_refresh_and_source_linked_consumer(tmp_path):
    archive = tmp_path / "raw"
    requests = []
    r = reader(requests)
    try:
        captures = capture_year(tmp_path / "capture", year=2025, reader=r, archive_root=archive)
    finally:
        r.close()
    assert len(requests) == 3
    artifact = build(captures, tmp_path / "built", archive_root=archive)
    assert verify_generation(tmp_path / "built" / "generation", expected_pin=artifact.pin).pin == artifact.pin
    con = duckdb.connect()
    con.read_parquet(str(tmp_path / "built/generation/senate_disclosure_reports.parquet")).create_view("reports")
    con.read_parquet(str(tmp_path / "built/generation/senate_disclosure_transactions.parquet")).create_view(
        "transactions"
    )
    result = con.execute(
        "SELECT r.amendment_status,t.owner,t.ticker,t.ticker_raw,t.amount_lower_usd,t.amount_upper_usd,t.source_url,t.source_locator FROM reports r JOIN transactions t USING(report_id) ORDER BY r.submitted_date"
    ).fetchall()
    assert len(result) == 2
    assert result[1][:6] == ("numbered", "", None, "--", 1001, 15000)
    assert result[1][6:] == (ORIGIN + "/search/view/ptr/" + IDS[1] + "/", "table tbody tr:nth-of-type(1)")
    assert con.execute(
        "SELECT original_candidate_ids,amendment_link_status FROM reports WHERE amendment_status='numbered'"
    ).fetchone() == ([IDS[0]], "unique_candidate")
    assert con.execute(
        "SELECT parse_status,report_sha256,transaction_count FROM reports WHERE report_kind='paper'"
    ).fetchone() == ("unsupported_paper", None, None)
    requests = []
    r = reader(requests)
    try:
        refreshed = capture_year(tmp_path / "refresh", year=2025, prior=captures, reader=r, archive_root=archive)
    finally:
        r.close()
    assert len(requests) == 1
    assert json.loads(refreshed.read_text())["reused_reports"] == 2
    replay = build(refreshed, tmp_path / "replayed", archive_root=archive)
    assert replay.root["spec"]["tables"] == artifact.root["spec"]["tables"]


def test_incomplete_selection_and_original_tamper_refuse(tmp_path):
    requests = []
    r = reader(requests)
    try:
        capture = capture_year(tmp_path / "capture", year=2025, reader=r, archive_root=tmp_path / "raw")
    finally:
        r.close()
    selection = json.loads(capture.read_text())
    selection["reports"].pop(IDS[0])
    capture.write_text(json.dumps(selection))
    with pytest.raises(SenateDisclosureError, match="population"):
        build(capture, tmp_path / "bad-population", archive_root=tmp_path / "raw")
    selection["complete"] = False
    capture.write_text(json.dumps(selection))
    with pytest.raises(SenateDisclosureError, match="complete"):
        build(capture, tmp_path / "incomplete", archive_root=tmp_path / "raw")


def test_search_truncation_retains_failed_evidence(tmp_path):
    requests = []
    r = reader(requests, data=rows()[:1])
    try:
        with pytest.raises(SenateDisclosureError, match="truncated"):
            capture_year(tmp_path / "failed", year=2025, reader=r, archive_root=tmp_path / "raw")
    finally:
        r.close()
    assert json.loads((tmp_path / "failed/captures.partial.json").read_text())["complete"] is False
    outcomes = list((tmp_path / "failed/source-evidence").glob("*/artifact/artifact.json"))
    assert outcomes and json.loads(outcomes[0].read_text())["spec"]["outcome"] == "failed"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("$1,001 - $15,000", (1001, 15000, True, "closed_range")),
        ("Over $1,000,000", (1000000, None, False, "open_upper")),
        ("unknown", (None, None, None, "unresolved")),
    ],
)
def test_amounts_are_ranges_or_qualified_unknowns(raw, expected):
    assert amount_range(raw) == expected


def test_retained_original_size_pin_is_enforced(tmp_path):
    requests = []
    r = reader(requests)
    try:
        captures = capture_year(tmp_path / "capture", year=2025, reader=r, archive_root=tmp_path / "raw")
    finally:
        r.close()
    selected = json.loads(captures.read_text())
    selected["reports"][IDS[0]]["bytes"] += 1
    captures.write_text(json.dumps(selected))
    with pytest.raises(SenateDisclosureError, match="checksum/size"):
        build(captures, tmp_path / "bad-original", archive_root=tmp_path / "raw")


def test_duplicate_originals_keep_ambiguous_amendment_candidates(tmp_path):
    extra_id = "10000000-0000-0000-0000-000000000004"
    listing = rows()
    listing.append([*listing[0][:3], listing[0][3].replace(IDS[0], extra_id), listing[0][4]])

    def respond(request):
        body = (
            json.dumps({"result": "ok", "recordsTotal": 4, "recordsFiltered": 4, "data": listing}).encode()
            if request.url.path == "/search/report/data/"
            else report(IDS[1] in str(request.url))
        )
        return httpx.Response(
            200,
            stream=httpx.ByteStream(body),
            headers={"Content-Type": "application/json" if request.url.path == "/search/report/data/" else "text/html"},
        )

    r = SenateEfdReader(
        cookie="sessionid=private", csrf="csrf-private", transport=httpx.MockTransport(respond), interval=0
    )
    try:
        captures = capture_year(tmp_path / "capture", year=2025, reader=r, archive_root=tmp_path / "raw")
    finally:
        r.close()
    build(captures, tmp_path / "built", archive_root=tmp_path / "raw")
    with duckdb.connect() as con:
        result = con.execute(
            "SELECT original_candidate_ids,amendment_link_status FROM read_parquet(?) WHERE amendment_status='numbered'",
            [str(tmp_path / "built/generation/senate_disclosure_reports.parquet")],
        ).fetchone()
    assert result == ([IDS[0], extra_id], "ambiguous")


def test_explicit_partial_retains_declared_population_and_cannot_publish(tmp_path):
    from rulespec_artifacts import LocalMemberSource
    from spicy_regs.sources.publication import PublicationError, _stage_verified_generation

    requests = []
    r = reader(requests)
    try:
        capture = capture_year(tmp_path / "capture", year=2025, reader=r, archive_root=tmp_path / "raw")
    finally:
        r.close()
    selected = json.loads(capture.read_text())
    selected["reports"].pop(IDS[0])
    selected["complete"] = False
    selected["source_refusal"] = {"stage": "agreement", "status_code": 403, "body_retained": False}
    capture.write_text(json.dumps(selected))
    artifact = build(capture, tmp_path / "partial", archive_root=tmp_path / "raw", local_partial=True)
    assert artifact.root["spec"]["publicationStatus"] == "local-partial"
    with duckdb.connect() as con:
        result = con.execute(
            "SELECT report_id,parse_status,transaction_count FROM read_parquet(?) ORDER BY report_id",
            [str(tmp_path / "partial/generation/senate_disclosure_reports.parquet")],
        ).fetchall()
    assert result == [
        (IDS[0], "unrequested_electronic", None),
        (IDS[1], "parsed", 1),
        (IDS[2], "unsupported_paper", None),
    ]
    uploads = []
    with pytest.raises(PublicationError, match="partial candidate"):
        _stage_verified_generation(
            artifact,
            LocalMemberSource(tmp_path / "partial/generation"),
            upload_member=lambda *a, **k: uploads.append(a),
            client=None,
            bucket="unused",
            index={"families": {}},
        )
    assert not uploads
