"""Refuse incomplete CFR source walks before replacing an existing table."""

import importlib
import json
from pathlib import Path
from xml.etree import ElementTree

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.transport import retry
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.sources import cfr_sections as cfr

build = importlib.import_module("spicy_regs.transforms.build_cfr_sections")
FIXTURES = Path(__file__).parent / "fixtures" / "cfr"
PACKAGE_ID = "CFR-2025-title1-vol1"
PACKAGE = {"packageId": PACKAGE_ID, "lastModified": "2026-09-11T17:30:58Z", "title": "General Provisions"}
GRANULE = {"granuleId": f"{PACKAGE_ID}-sec1-1", "title": "Definitions.", "granuleClass": "CONTENT"}
KEY = "test-key-never-in-query"


def page(key, records, *, count=None, next_page=None):
    return {key: records, "count": len(records) if count is None else count, "nextPage": next_page}


class Transport(httpx.MockTransport):
    def __init__(self, *bodies):
        self.bodies = iter(bodies)
        self.calls = []
        super().__init__(self.handle)

    def handle(self, request):
        self.calls.append(request)
        body = next(self.bodies)
        if isinstance(body, int):
            return httpx.Response(
                body,
                stream=httpx.ByteStream(b'{"error":"source refused"}'),
                headers={"content-type": "application/json"},
            )
        if isinstance(body, bytes):
            return httpx.Response(200, stream=httpx.ByteStream(body), headers={"content-type": "application/json"})
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )


@pytest.fixture(autouse=True)
def no_delay(monkeypatch):
    monkeypatch.setattr(retry.random, "uniform", lambda *_: 0)
    monkeypatch.setattr(cfr, "_MAX_REQUESTS_PER_PAGE", 1)


def reader(transport):
    return cfr.CfrSectionsReader(api_key=KEY, since_year=2025, until_year=2025, transport=transport)


def test_replays_native_granules_and_keeps_all_package_fields():
    native = json.loads((FIXTURES / "govinfo-package-granules.json").read_bytes())
    # Keep the two native rows, explicitly make this a synthetic terminal fixture.
    native.update(count=2, nextPage=None)
    package = {**PACKAGE, "dateIssued": "2025-01-01", "congress": None, "source-extra": ["preserved"]}
    transport = Transport(page("packages", [package]), native)
    rows = list(reader(transport).iter_records())
    assert len(rows) == 2
    for row, original in zip(rows, native["granules"], strict=True):
        assert {key: row[key] for key in original} == original
        assert row["_package"] == package
    assert all(call.headers["x-api-key"] == KEY for call in transport.calls)
    assert all("api_key" not in call.url.params for call in transport.calls)
    assert all("offset" not in call.url.params for call in transport.calls)
    assert transport.calls[1].url.params["offsetMark"] == "*"


def test_follows_publisher_opaque_continuation_exactly():
    next_url = f"{cfr.API_BASE}/packages/{PACKAGE_ID}/granules?offsetMark=opaque%2Bcursor&pageSize=1"
    second = {**GRANULE, "granuleId": f"{PACKAGE_ID}-sec1-2"}
    transport = Transport(
        page("packages", [PACKAGE]),
        page("granules", [GRANULE], count=2, next_page=next_url),
        page("granules", [second], count=2),
    )
    assert len(list(reader(transport).iter_records())) == 2
    assert str(transport.calls[2].url) == next_url


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"error": "denied"},
        {"packages": []},
        {"packages": [], "count": None},
        {"packages": {}, "count": 0},
        {"packages": [None], "count": 1},
        page("packages", [{}]),
        page("packages", [{"packageId": "FR-2025-01-01"}]),
        page("packages", [], count=1),
        page("packages", [PACKAGE], count=True),
        b"not JSON",
    ],
)
def test_bad_package_page_refuses(payload):
    with pytest.raises((cfr.CfrSectionsError, PagedJsonSourceError)):
        list(reader(Transport(payload)).iter_records())


@pytest.mark.parametrize(
    "payload",
    [
        page("granules", [{}]),
        page("granules", [None]),
        {"granules": [], "count": None},
        page("granules", [], count=1),
        page("granules", [GRANULE], count=2),
        page("granules", [GRANULE, GRANULE]),
        page("granules", [{**GRANULE, "granuleId": "CFR-2024-title1-vol1-sec1-1"}]),
    ],
)
def test_bad_granule_page_refuses(payload):
    with pytest.raises((cfr.CfrSectionsError, PagedJsonSourceError)):
        list(reader(Transport(page("packages", [PACKAGE]), payload)).iter_records())


def test_missing_or_repeated_continuation_cannot_finish_a_partial_selection():
    next_url = f"{cfr.API_BASE}/packages/{PACKAGE_ID}/granules?offsetMark=next&pageSize=1"
    transport = Transport(
        page("packages", [PACKAGE]),
        page("granules", [GRANULE], count=2, next_page=next_url),
        page("granules", [], count=2, next_page=next_url),
    )
    with pytest.raises(PagedJsonSourceError, match="repeats|repeated"):
        list(reader(transport).iter_records())


def test_page_bound_refuses(monkeypatch):
    monkeypatch.setattr(cfr, "_MAX_PAGES", 1)
    next_url = f"{cfr.API_BASE}/packages/{PACKAGE_ID}/granules?offsetMark=next"
    transport = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE], count=2, next_page=next_url))
    with pytest.raises(PagedJsonSourceError, match="page bound"):
        list(reader(transport).iter_records())


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_http_failures_are_not_empty_selections(status):
    transport = Transport(status)
    with pytest.raises((CredentialRefusedError, PagedJsonSourceError, httpx.HTTPError)) as refused:
        list(reader(transport).iter_records())
    assert KEY not in str(refused.value)
    assert len(transport.calls) == 1


def test_explicit_empty_selection_is_valid():
    assert list(reader(Transport(page("packages", []))).iter_records()) == []
    assert list(reader(Transport(page("packages", [PACKAGE]), page("granules", []))).iter_records()) == []


@pytest.mark.parametrize("failure", ["missing-key", "http", "partial", "missing-identity"])
def test_source_failure_preserves_prior_output_byte_for_byte(monkeypatch, tmp_path, failure):
    prior_row = build._shape({**GRANULE, "_package_id": PACKAGE_ID})
    prior_file = tmp_path / "_cfr_prior.parquet"
    output = tmp_path / build.OUTPUT
    table = pa.Table.from_pylist([prior_row], schema=build._SCHEMA)
    pq.write_table(table, prior_file)
    pq.write_table(table, output)
    before = output.read_bytes()
    prior_before = prior_file.read_bytes()
    if failure == "missing-key":
        selected = cfr.CfrSectionsReader(api_key="")
    else:
        payload = {
            "http": 403,
            "partial": page("granules", [GRANULE], count=2),
            "missing-identity": page("granules", [{}]),
        }[failure]
        selected = reader(Transport(page("packages", [PACKAGE]), payload))
    monkeypatch.setattr(build, "CfrSectionsReader", lambda **_: selected)
    with pytest.raises((cfr.CfrSectionsError, PagedJsonSourceError, CredentialRefusedError)):
        build.build_cfr_sections(tmp_path, since_year=2025)
    assert output.read_bytes() == before
    assert prior_file.read_bytes() == prior_before
    assert not (tmp_path / "_cfr_new.parquet").exists()


def test_explicit_empty_build_preserves_prior_rows(monkeypatch, tmp_path):
    prior_row = build._shape({**GRANULE, "_package_id": PACKAGE_ID})
    pq.write_table(pa.Table.from_pylist([prior_row], schema=build._SCHEMA), tmp_path / "_cfr_prior.parquet")
    selected = reader(Transport(page("packages", [])))
    monkeypatch.setattr(build, "CfrSectionsReader", lambda **_: selected)
    output = build.build_cfr_sections(tmp_path, since_year=2025)
    assert pq.read_table(output).to_pylist() == [prior_row]


def test_native_ancestry_disproves_section_prefix_inference():
    root = ElementTree.fromstring((FIXTURES / "ecfr-api-title14-numbering.xml").read_bytes())
    native_part = root.find(".//*[@TYPE='PART']")
    assert native_part is not None
    native_section = native_part.find(".//*[@TYPE='SECTION']")
    assert native_section is not None
    assert native_part.attrib["N"] == "241"
    assert native_section.attrib["N"] == "19-8.1"
    # This corresponding ID is pinned in the independent public-table audit.
    row = build._shape({"granuleId": "CFR-2025-title14-vol4-sec19-8-1"})
    assert row["part"] is None
    assert row["section"] == "19-8-1"
    assert row["cfr_ref"] is None
    # Current eCFR ancestry is not assigned to an annual edition without evidence.


def test_granule_modification_date_takes_precedence_over_package_date():
    row = build._shape(
        {**GRANULE, "lastModified": "2025-01-02T00:00:00Z", "_package_last_modified": "2026-09-11T17:30:58Z"}
    )
    assert row["last_modified"] == "2025-01-02T00:00:00Z"


@pytest.mark.parametrize("identity", [None, "", "  ", [], 12])
def test_shaper_refuses_missing_or_malformed_identity(identity):
    with pytest.raises(cfr.CfrSectionsError):
        build._shape({"granuleId": identity})


def test_fresh_annual_records_keep_native_tokens_without_false_part_19():
    fixture = json.loads((FIXTURES / "govinfo-title14-counterexamples.json").read_bytes())
    package = fixture["package"]
    assert package["packageId"] == "CFR-2025-title14-vol4"
    assert package["lastModified"] == "2025-06-17T21:29:08Z"
    expected = {
        "CFR-2025-title14-vol4-sec19-8-1": ("19-8-1", "Purpose."),
        "CFR-2025-title14-vol4-sec19-8-10": ("19-8-10", "Staff review."),
    }
    assert {row["granuleId"] for row in fixture["granules"]} == set(expected)
    for raw in fixture["granules"]:
        row = build._shape(
            {**raw, "_package_id": package["packageId"], "_package_last_modified": package["lastModified"]}
        )
        section, heading = expected[raw["granuleId"]]
        assert (row["section"], row["heading"]) == (section, heading)
        assert row["part"] is None and row["cfr_ref"] is None
        assert row["edition_year"] == "2025" and row["title"] == "14"
        assert row["last_modified"] == package["lastModified"]
