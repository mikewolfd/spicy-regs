"""The FEC bulk master histories: acquire a selection once, build only from a selection.

Every test runs for both the candidate and the committee master over the real SpicyDocs ``FecClient`` and
readers; only the remote HTTP peer is replaced (``httpx.MockTransport``), and each request is counted.
"""

import io
import json
import shutil
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import ModuleType

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactVerificationError, MemberNotFoundError
from spicy_docs.schemas import fec_candidate_history, fec_committee_history
from spicy_docs.sources.fec import candidate_master, committee_master
from spicy_docs.sources.fec import client as client_module
from spicy_docs.sources.fec.client import FecClient
from spicy_docs.transport.download import AcquisitionError

from spicy_regs.transforms import fec_master_history as history
from spicy_regs.transforms.build_fec_candidate_history import build_fec_candidate_history
from spicy_regs.transforms.build_fec_committee_history import build_fec_committee_history


@dataclass(frozen=True)
class Case:
    label: str
    build: Callable[..., Path]
    schema: object  # the SpicyDocs table module, whose projection a mapping-only test replaces
    projection: str
    contract: object
    source: ModuleType
    member: str
    identity: str
    ids: tuple[str, str]
    row: Callable[..., bytes]
    extra: tuple[str, str]  # a column the row projection fills, and its value

    @property
    def fields(self) -> tuple[str, ...]:
        return getattr(self.source, f"{self.label.upper()}_MASTER_FIELDS")

    @property
    def header(self) -> bytes:
        return (",".join(self.fields) + "\n").encode()

    def url(self, cycle: int) -> str:
        return getattr(self.source, f"{self.label}_master_url")(cycle)

    def zip(self, *rows: bytes) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(self.member, b"".join(row + b"\n" for row in rows))
        return buffer.getvalue()

    @property
    def stage(self) -> str:
        return f"fec-{self.label}-master"


CASES = [
    Case(
        "candidate",
        build_fec_candidate_history,
        fec_candidate_history,
        "project_candidate_master_row",
        fec_candidate_history.FEC_CANDIDATE_HISTORY,
        candidate_master,
        "cn.txt",
        "candidate_id",
        ("H0AA00001", "H0AA00002"),
        lambda ident, name, party="DEM": (
            f"{ident}|{name}|{party}|2026|AL|H|01|O|C|C00000000|PO BOX||MOBILE|AL|36685".encode()
        ),
        ("principal_committee_id", "C00000000"),
    ),
    Case(
        "committee",
        build_fec_committee_history,
        fec_committee_history,
        "project_committee_master_row",
        fec_committee_history.FEC_COMMITTEE_HISTORY,
        committee_master,
        "cm.txt",
        "committee_id",
        ("C00000001", "C00000002"),
        lambda ident, name, party="DEM": f"{ident}|{name}|T|S1||CITY|ST|00000|P|H|{party}|Q|||H0XX00000".encode(),
        ("candidate_id", "H0XX00000"),
    ),
]


@pytest.fixture(params=CASES, ids=[case.label for case in CASES])
def case(request):
    return request.param


class Wire:
    """The real ``FecClient`` over a mock publisher that counts every request.

    ``files`` maps a URL to its body (any other URL answers 404); ``redirects`` maps a URL to the URL it
    redirects to. A client built with an explicit transport (the evidence tee) keeps it.
    """

    def __init__(self, monkeypatch, case: Case, files: dict[str, bytes], redirects: dict[str, str] | None = None):
        self.case, self.files, self.redirects = case, files, redirects or {}
        self.requests: list[httpx.Request] = []
        self.transports: list[object] = []
        self.forbidden = False
        self.observe: Callable[[str], None] | None = None
        wire = self

        class Client(FecClient):
            def __init__(self, *, store, **options):
                assert not wire.forbidden, "no client may be constructed"
                wire.transports.append(options.get("transport"))
                options["transport"] = options.get("transport") or httpx.MockTransport(wire.respond)
                options["min_interval"] = 0
                super().__init__(store=store, **options)

        monkeypatch.setattr(client_module, "FecClient", Client)

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = str(request.url)
        if self.observe is not None:
            self.observe(url)
        if url in self.redirects:
            return httpx.Response(302, headers={"Location": self.redirects[url]}, stream=httpx.ByteStream(b""))
        body = self.files.get(url)
        return httpx.Response(
            404 if body is None else 200,
            stream=httpx.ByteStream(b"missing" if body is None else body),
            headers={"Content-Type": "text/csv" if url == self.case.source.HEADER_URL else "application/zip"},
        )

    def served(self) -> list[str]:
        return [str(request.url) for request in self.requests]


def three_cycles(case: Case) -> dict[str, bytes]:
    one, two = case.ids
    return {
        case.source.HEADER_URL: case.header,
        case.url(1980): case.zip(case.row(one, "ONE")),
        case.url(1982): case.zip(case.row(one, "ONE AGAIN"), case.row(two, "TWO", party="")),
        case.url(1984): case.zip(case.row(two, "TWO")),
    }


def attempt_of(raw: Path, case: Case) -> Path:
    (attempt,) = raw.glob(f"{case.stage}-*")
    return attempt


def test_every_cycle_is_read_whole_and_projected_onto_the_contract(tmp_path, monkeypatch, case):
    wire = Wire(monkeypatch, case, three_cycles(case))
    out = case.build(tmp_path / "out", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    table = pq.read_table(out)
    assert tuple(table.column_names) == case.contract.columns
    rows = table.to_pylist()
    one, two = case.ids
    assert [(r[case.identity], r["cycle"], r["name"]) for r in rows] == [
        (one, "1980", "ONE"),
        (one, "1982", "ONE AGAIN"),
        (two, "1982", "TWO"),
        (two, "1984", "TWO"),
    ]
    assert rows[2]["party"] is None and rows[0][case.extra[0]] == case.extra[1]
    assert wire.served() == [case.source.HEADER_URL, *map(case.url, (1980, 1982, 1984))]


def test_a_repeat_within_a_cycle_refuses(tmp_path, monkeypatch, case):
    one = case.ids[0]
    Wire(
        monkeypatch,
        case,
        {case.source.HEADER_URL: case.header, case.url(1980): case.zip(case.row(one, "ONE"), case.row(one, "DUP"))},
    )
    with pytest.raises(ValueError, match=f"repeats {one} in cycle 1980"):
        case.build(tmp_path / "out", capture_dir=tmp_path / "raw", today=date(1980, 1, 1))
    assert not (tmp_path / "out" / f"{case.contract.name}.parquet").exists()


def test_every_response_goes_through_the_evidence_transport(tmp_path, monkeypatch, case):
    wire = Wire(
        monkeypatch, case, {case.source.HEADER_URL: case.header, case.url(1980): case.zip(case.row("X1", "ONE"))}
    )
    tee, events, stages = httpx.MockTransport(wire.respond), [], []

    class Evidence:
        def transport(self, transport=None, *, stage, max_bytes):
            stages.append((stage, max_bytes))
            return tee

        def event(self, event, **fields):
            events.append((event, fields))

    case.build(tmp_path / "out", capture_dir=tmp_path / "raw", evidence=Evidence(), today=date(1980, 6, 1))
    assert stages == [(case.stage + "-response", history.MAX_FILE_BYTES)]
    assert wire.transports == [tee] and len(wire.requests) == 2
    assert [event for event, _ in events] == ["selection", "cycle-rows"]
    assert events[0][1] == {"stage": case.stage, "cycles": [1980, 1980]}
    assert events[1][1]["rows_by_cycle"] == {"1980": 1}


@pytest.mark.parametrize(
    ("today", "cycle"), [(date(2026, 9, 27), 2026), (date(2027, 1, 2), 2028), (date(2028, 12, 31), 2028)]
)
def test_the_current_cycle_is_the_even_year_at_or_after_today(today, cycle):
    assert history.current_cycle(today) == cycle


def test_refresh_retains_selection_and_rebuild_makes_no_request(tmp_path, monkeypatch, case):
    wire = Wire(monkeypatch, case, three_cycles(case))
    refreshed = case.build(tmp_path / "first", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    assert len(wire.requests) == 4
    attempt = attempt_of(tmp_path / "raw", case)
    selection = json.loads((attempt / "selection.json").read_text())
    assert selection["complete"] is True and selection["format"] == history.SELECTION_FORMAT
    assert [entry["cycle"] for entry in selection["cycles"]] == [1980, 1982, 1984]
    assert selection["header"]["resolvedUrl"] == case.source.HEADER_URL
    for entry in selection["cycles"]:
        assert entry["resolvedUrl"] == entry["requestUrl"] == case.url(entry["cycle"])
    for capture in (selection["header"], *selection["cycles"]):
        assert (attempt / "blobs" / "sha256" / capture["responseSha256"].removeprefix("sha256:")).is_file()
    wire.forbidden = True
    rebuilt = case.build(tmp_path / "second", selection=attempt / "selection.json")
    assert len(wire.requests) == 4
    assert pq.read_table(rebuilt).equals(pq.read_table(refreshed))


def test_mapping_only_rebuild_reads_only_the_selection(tmp_path, monkeypatch, case):
    wire = Wire(monkeypatch, case, three_cycles(case))
    case.build(tmp_path / "first", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    selection = attempt_of(tmp_path / "raw", case) / "selection.json"
    original = getattr(case.schema, case.projection)
    monkeypatch.setattr(
        case.schema, case.projection, lambda row: {**original(row), "name": "MAPPED " + original(row)["name"]}
    )
    wire.forbidden = True
    rebuilt = case.build(tmp_path / "second", selection=selection)
    assert {name.startswith("MAPPED ") for name in pq.read_table(rebuilt)["name"].to_pylist()} == {True}
    assert len(wire.requests) == 4


def _edit(selection: Path, change: Callable[[dict], None]) -> None:
    value = json.loads(selection.read_text())
    change(value)
    selection.write_text(json.dumps(value))


def _drop(cycle: int):
    return lambda value: value["cycles"].remove(next(e for e in value["cycles"] if e["cycle"] == cycle))


def _swap_capture(value: dict) -> None:
    # The 1982 entry keeps its cycle but carries 1980's own capture.
    value["cycles"][1] = {**value["cycles"][0], "cycle": 1982}


REFUSALS = {
    "other-family": (lambda v: v.update(family="fec_candidate_history fec_committee_history"), ValueError, "is not a"),
    "wrong-format": (lambda v: v.update(format="fec-master-selection-v0"), ValueError, "is not a"),
    "incomplete": (lambda v: v.update(complete=False), ValueError, "incomplete"),
    "missing-cycle": (_drop(1982), ValueError, "every cycle"),
    "extra-cycle": (lambda v: v["cycles"].append({**v["cycles"][-1], "cycle": 1986}), ValueError, "every cycle"),
    "duplicate-cycle": (lambda v: v["cycles"].insert(1, dict(v["cycles"][1])), ValueError, "every cycle"),
    "reordered": (lambda v: v["cycles"].reverse(), ValueError, "every cycle"),
    "bad-closing-cycle": (lambda v: v.update(through_cycle=1983), ValueError, "even years"),
    "other-cycle-capture": (_swap_capture, ValueError, "not this cycle's ZIP"),
    "header-is-a-zip": (
        lambda v: v.update(header={k: x for k, x in v["cycles"][0].items() if k != "cycle"}),
        ValueError,
        "not the selected header",
    ),
    "extra-capture-field": (
        lambda v: v["cycles"][0].update(objectKey="elsewhere", surprise="x"),
        ValueError,
        "fields differ",
    ),
}


@pytest.mark.parametrize("name", REFUSALS)
def test_selection_refusals_make_no_request_and_write_no_output(tmp_path, monkeypatch, case, name):
    wire = Wire(monkeypatch, case, three_cycles(case))
    case.build(tmp_path / "first", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    attempt = tmp_path / "copy"
    shutil.copytree(attempt_of(tmp_path / "raw", case), attempt)
    change, error, match = REFUSALS[name]
    _edit(attempt / "selection.json", change)
    wire.forbidden = True
    with pytest.raises(error, match=match):
        case.build(tmp_path / "second", selection=attempt / "selection.json")
    assert len(wire.requests) == 4
    assert not list((tmp_path / "second").glob("*.parquet"))


@pytest.mark.parametrize(
    ("damage", "error"),
    [
        ("changed-bytes", ArtifactVerificationError),
        ("short-blob", ArtifactVerificationError),
        ("missing-blob", MemberNotFoundError),
    ],
)
def test_retained_original_that_changed_is_refused_by_digest(tmp_path, monkeypatch, case, damage, error):
    wire = Wire(monkeypatch, case, three_cycles(case))
    case.build(tmp_path / "first", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    attempt = tmp_path / "copy"
    shutil.copytree(attempt_of(tmp_path / "raw", case), attempt)
    selection = json.loads((attempt / "selection.json").read_text())
    blob = attempt / "blobs" / "sha256" / selection["cycles"][-1]["responseSha256"].removeprefix("sha256:")
    original = blob.read_bytes()
    blob.unlink()
    if damage == "changed-bytes":
        blob.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
    elif damage == "short-blob":
        blob.write_bytes(original[:-1])
    wire.forbidden = True
    with pytest.raises(error):
        case.build(tmp_path / "second", selection=attempt / "selection.json")
    assert len(wire.requests) == 4
    assert not list((tmp_path / "second").glob("*.parquet"))


def test_each_download_is_on_disk_before_the_next_begins(tmp_path, monkeypatch, case):
    """A kill between two downloads must not lose the earlier captures: the manifest is rewritten after each."""
    wire = Wire(monkeypatch, case, three_cycles(case))
    raw, seen = tmp_path / "raw", []

    def observe(url: str) -> None:
        manifest = json.loads((attempt_of(raw, case) / "selection.json").read_text())
        header = manifest["header"] and manifest["header"]["requestUrl"]
        seen.append((url, manifest["complete"], header, [entry["cycle"] for entry in manifest["cycles"]]))

    wire.observe = observe
    case.build(tmp_path / "out", capture_dir=raw, today=date(1983, 5, 1))
    header_url = case.source.HEADER_URL
    assert seen == [
        (header_url, False, None, []),
        (case.url(1980), False, header_url, []),
        (case.url(1982), False, header_url, [1980]),
        (case.url(1984), False, header_url, [1980, 1982]),
    ]


def test_write_json_replaces_the_file_whole_or_not_at_all(tmp_path):
    target = tmp_path / "run.json"
    history.write_json(target, {"status": "complete"})
    with pytest.raises(TypeError):
        history.write_json(target, {"status": object()})
    assert json.loads(target.read_text()) == {"status": "complete"}
    assert [path.name for path in tmp_path.iterdir()] == ["run.json"]


def test_failed_download_keeps_partial_attempt(tmp_path, monkeypatch, case):
    files = three_cycles(case)
    del files[case.url(1982)]
    wire = Wire(monkeypatch, case, files)
    with pytest.raises(AcquisitionError, match="HTTP 404"):
        case.build(tmp_path / "out", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    attempt = attempt_of(tmp_path / "raw", case)
    selection = json.loads((attempt / "selection.json").read_text())
    assert selection["complete"] is False and "404" in selection["failure"]
    assert selection["header"]["requestUrl"] == case.source.HEADER_URL
    assert [entry["cycle"] for entry in selection["cycles"]] == [1980]
    for capture in (selection["header"], *selection["cycles"]):
        assert (attempt / "blobs" / "sha256" / capture["responseSha256"].removeprefix("sha256:")).is_file()
    wire.forbidden = True
    with pytest.raises(ValueError, match="incomplete"):
        case.build(tmp_path / "again", selection=attempt / "selection.json")


@pytest.mark.parametrize("failure", ["header", "duplicate"])
def test_parser_or_mapping_failure_keeps_complete_selection(tmp_path, monkeypatch, case, failure):
    one = case.ids[0]
    files = {
        case.source.HEADER_URL: case.header if failure == "duplicate" else b"X\n",
        case.url(1980): case.zip(case.row(one, "ONE"), *([case.row(one, "AGAIN")] if failure == "duplicate" else [])),
    }
    wire = Wire(monkeypatch, case, files)
    match = "repeats" if failure == "duplicate" else "header differs"
    with pytest.raises(ValueError, match=match):
        case.build(tmp_path / "out", capture_dir=tmp_path / "raw", today=date(1980, 1, 1))
    attempt = attempt_of(tmp_path / "raw", case)
    selection = json.loads((attempt / "selection.json").read_text())
    assert selection["complete"] is True and "failure" not in selection
    for capture in (selection["header"], *selection["cycles"]):
        assert (attempt / "blobs" / "sha256" / capture["responseSha256"].removeprefix("sha256:")).is_file()
    wire.forbidden = True
    with pytest.raises(ValueError, match=match):
        case.build(tmp_path / "again", selection=attempt / "selection.json")
    assert len(wire.requests) == 2
    assert not list((tmp_path / "out").glob("*.parquet")) and not list((tmp_path / "again").glob("*.parquet"))


def test_a_cycle_redirected_to_another_cycles_file_is_refused(tmp_path, monkeypatch, case):
    monkeypatch.setenv("FEC_API_KEY", "private-api-test-credential")
    wire = Wire(monkeypatch, case, three_cycles(case), redirects={case.url(1982): case.url(1980)})
    with pytest.raises(ValueError, match="resolved to a different selected file"):
        case.build(tmp_path / "out", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    assert wire.requests and all("x-api-key" not in request.headers for request in wire.requests)
    assert not list((tmp_path / "out").glob("*.parquet"))


def test_exactly_one_route(tmp_path, monkeypatch, case):
    wire = Wire(monkeypatch, case, three_cycles(case))
    case.build(tmp_path / "first", capture_dir=tmp_path / "raw", today=date(1983, 5, 1))
    selection = attempt_of(tmp_path / "raw", case) / "selection.json"
    wire.forbidden = True
    for options in (
        {},
        {"capture_dir": tmp_path / "raw", "selection": selection},
        {"selection": selection, "evidence": object()},
    ):
        with pytest.raises(ValueError, match="capture_dir to refresh|makes no acquisition"):
            case.build(tmp_path / "second", **options)
    assert len(wire.requests) == 4 and not (tmp_path / "second").exists()
