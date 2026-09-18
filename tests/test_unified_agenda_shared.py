"""Shared-reader parity with the frozen pre-migration SpicyRegs implementation."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from spicy_docs.sources.unified_agenda import UnifiedAgendaSourceError
from spicy_docs.sources.unified_agenda_records import scan_unified_agenda_records

from spicy_regs.sources.unified_agenda import UnifiedAgendaReader, _normalize
from spicy_regs.transforms.build_unified_agenda import _shape
from tests import unified_agenda_oracle as frozen

FIXTURES = Path(__file__).parent / "fixtures" / "unified_agenda"
PINS = json.loads((FIXTURES / "pins.json").read_text())
IDENTITY = b"<RIN>0503-AA90</RIN><PUBLICATION><PUBLICATION_ID>202510</PUBLICATION_ID></PUBLICATION>"


def document(record: bytes) -> bytes:
    return b'<?xml version="1.0"?><REGINFO_RIN_DATA>' + record + b"</REGINFO_RIN_DATA>"


def record(fields: bytes = b"", *, identity: bytes = IDENTITY) -> bytes:
    return b"<RIN_INFO>" + identity + fields + b"</RIN_INFO>"


def old_records(body: bytes, edition: str = "202510") -> list[dict]:
    reader = frozen.UnifiedAgendaReader(editions=(edition,))
    with patch.object(reader, "_download", return_value=body):
        return list(reader._fetch_edition(edition))


def shared_records(body: bytes, edition: str = "202510") -> list[dict]:
    rows: list[dict] = []
    scan_unified_agenda_records(body, on_record=lambda item: rows.append(_normalize(item, edition)))
    return rows


def reader(body: bytes, edition: str = "202510", status: int = 200) -> UnifiedAgendaReader:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            status, stream=httpx.ByteStream(body), headers={"content-type": "application/xml"}
        )
    )
    return UnifiedAgendaReader(editions=(edition,), transport=transport)


@pytest.mark.parametrize("pin", PINS, ids=lambda pin: pin["file"])
def test_exact_source_excerpts(pin):
    raw = (FIXTURES / pin["file"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == pin["record_sha256"]
    assert len(raw) == pin["byte_end"] - pin["byte_start"]
    body = document(raw)
    if pin["contains_invalid_control"]:
        assert raw.count(b"\x19") == 1
        with pytest.raises(ET.ParseError):
            old_records(body, pin["edition"])
        with pytest.raises(UnifiedAgendaSourceError):
            next(reader(body, pin["edition"]).iter_records())
    else:
        expected = old_records(body, pin["edition"])
        assert shared_records(body, pin["edition"]) == expected
        actual = list(reader(body, pin["edition"]).iter_records())
        assert actual == expected
        assert [_shape(row) for row in actual] == [_shape(row) for row in expected]


@pytest.mark.parametrize(
    "fields",
    [
        pytest.param(b"", id="all-optional-fields-missing"),
        pytest.param(b"<RULE_TITLE/><ABSTRACT>  </ABSTRACT><MAJOR/>", id="empty-scalars"),
        pytest.param(b"<RULE_TITLE> first </RULE_TITLE><RULE_TITLE>second</RULE_TITLE>", id="repeated-scalar"),
        pytest.param(b"<RULE_TITLE> a\n  b &amp; c </RULE_TITLE>", id="interior-whitespace-and-entity"),
        pytest.param(b"<ABSTRACT> lead <b>child</b> tail </ABSTRACT>", id="mixed-text-leading-only"),
        pytest.param(b"<ABSTRACT><b>child</b> tail </ABSTRACT>", id="mixed-text-no-leading-text"),
        pytest.param(b"<ABSTRACT><![CDATA[<p> literal markup </p>]]></ABSTRACT>", id="cdata-literal-markup"),
        pytest.param(b"<ABSTRACT>before<!--x-->after<?p x?>last</ABSTRACT>", id="comments-and-pi"),
        pytest.param(b"<AGENCY><CODE>42</CODE><NAME> Name </NAME></AGENCY>", id="agency-code-fallback"),
        pytest.param(b"<AGENCY><ACRONYM> </ACRONYM><CODE>42</CODE></AGENCY>", id="empty-agency-acronym"),
        pytest.param(b"<AGENCY/><AGENCY><CODE>42</CODE><NAME>Later</NAME></AGENCY>", id="first-agency-only"),
        pytest.param(b"<PARENT_AGENCY><ACRONYM>P</ACRONYM></PARENT_AGENCY>", id="no-parent-agency-fallback"),
        pytest.param(b"<CFR_LIST/><LEGAL_AUTHORITY_LIST/><TIMETABLE_LIST/>", id="empty-lists"),
        pytest.param(b"<CFR_LIST><CFR/><CFR> </CFR><CFR>40  CFR\n60</CFR></CFR_LIST>", id="cfr-empty-filter"),
        pytest.param(b"<CFR_LIST><CFR>40 CFR 60</CFR><CFR>40 CFR 60</CFR></CFR_LIST>", id="cfr-duplicates-retained"),
        pytest.param(b"<CFR_LIST/><CFR_LIST><CFR>40 CFR 60</CFR></CFR_LIST>", id="all-cfr-containers"),
        pytest.param(
            b"<CFR_LIST><OTHER>unknown</OTHER><CFR>40 <b>CFR</b> 60</CFR></CFR_LIST>",
            id="unknown-cfr-child-and-mixed-text",
        ),
        pytest.param(b"<LEGAL_AUTHORITY_LIST><LEGAL_AUTHORITY/></LEGAL_AUTHORITY_LIST>", id="empty-legal-item"),
        pytest.param(
            b"<LEGAL_AUTHORITY_LIST/><LEGAL_AUTHORITY_LIST><LEGAL_AUTHORITY> 5 USC  301 </LEGAL_AUTHORITY></LEGAL_AUTHORITY_LIST>",
            id="all-legal-containers",
        ),
        pytest.param(b"<TIMETABLE_LIST><TIMETABLE/></TIMETABLE_LIST>", id="empty-timetable-item-retained"),
        pytest.param(
            b"<TIMETABLE_LIST/><TIMETABLE_LIST><TIMETABLE><TTBL_ACTION>Final</TTBL_ACTION></TIMETABLE></TIMETABLE_LIST>",
            id="all-timetable-containers",
        ),
        pytest.param(
            b"<TIMETABLE_LIST><TIMETABLE><TTBL_DATE>To Be Determined</TTBL_DATE><TTBL_DATE>01/01/2025</TTBL_DATE></TIMETABLE></TIMETABLE_LIST>",
            id="first-timetable-date",
        ),
        pytest.param(
            b"<TIMETABLE_LIST><TIMETABLE><TTBL_DATE>06/00/2025</TTBL_DATE><FR_CITATION> 90 FR 123 </FR_CITATION><UNKNOWN>value</UNKNOWN></TIMETABLE></TIMETABLE_LIST>",
            id="literal-month-only-date",
        ),
        pytest.param(b'<CFR_LIST x="1"><CFR x="2">40 CFR 60</CFR></CFR_LIST>', id="attributes-do-not-change-mapping"),
        pytest.param(
            b'<CFR_LIST xmlns:x="urn:test"><x:CFR>excluded</x:CFR><CFR>retained</CFR></CFR_LIST>',
            id="expanded-child-name",
        ),
        pytest.param(b'<CFR_LIST xmlns="urn:test"><CFR>excluded</CFR></CFR_LIST>', id="qualified-container-excluded"),
        pytest.param(
            b"<UNKNOWN><RULE_TITLE>excluded</RULE_TITLE></UNKNOWN><ADDITIONAL_INFO>not a table field</ADDITIONAL_INFO>",
            id="non-table-fields",
        ),
    ],
)
def test_normalization_mutations_match_frozen_walker(fields):
    body = document(record(fields))
    assert shared_records(body) == old_records(body)
    assert list(reader(body).iter_records()) == old_records(body)


@pytest.mark.parametrize(
    "publication",
    [
        pytest.param(b"<PUBLICATION/>", id="empty-first-container"),
        pytest.param(b"<PUBLICATION><OTHER/></PUBLICATION>", id="unrelated-first-child"),
        pytest.param(b"<PUBLICATION><PUBLICATION_ID/></PUBLICATION>", id="empty-first-id"),
    ],
)
def test_first_complete_publication_path_matches_frozen_walker(publication):
    body = document(record(identity=publication + IDENTITY))
    assert shared_records(body) == old_records(body)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(document(b""), id="no-records"),
        pytest.param(document(record(identity=b"")), id="missing-identities"),
        pytest.param(document(record(identity=b"<RIN/>")), id="empty-rin"),
        pytest.param(document(record() + record()), id="duplicate-rin"),
        pytest.param(document(record(identity=IDENTITY.replace(b"202510", b"202504"))), id="wrong-edition"),
        pytest.param(document(record(identity=IDENTITY + b"<RIN>0503-AA91</RIN>")), id="repeated-rin-field"),
        pytest.param(
            document(record(identity=IDENTITY + b"<PUBLICATION><PUBLICATION_ID>202510</PUBLICATION_ID></PUBLICATION>")),
            id="repeated-publication-id",
        ),
        pytest.param(document(b"<OTHER>" + record() + b"</OTHER>"), id="misplaced-rin-info"),
        pytest.param(document(record()).replace(b"REGINFO_RIN_DATA", b"WRONG_ROOT"), id="wrong-root"),
    ],
)
def test_source_identity_refusals_intentionally_replace_unchecked_rows(body):
    # The old walker accepted these, including an empty result for no records.
    old_records(body)
    with pytest.raises(UnifiedAgendaSourceError):
        next(reader(body).iter_records())


def test_malformed_trailer_never_yields_a_partial_edition():
    body = document(record()).replace(b"</REGINFO_RIN_DATA>", b"<broken>")
    previous = frozen.UnifiedAgendaReader()
    with patch.object(previous, "_download", return_value=body):
        old = previous._fetch_edition("202510")
        assert next(old)["rin"] == "0503-AA90"
        with pytest.raises(ET.ParseError):
            next(old)
    with pytest.raises(UnifiedAgendaSourceError):
        next(reader(body).iter_records())


def test_second_scan_limit_never_yields_provisional_rows():
    # Identity validation succeeds; the full field scan fails after record one.
    large = b"<ABSTRACT>" + b"x" * (2 * 1024**2) + b"</ABSTRACT>"
    body = document(record() + record(large, identity=IDENTITY.replace(b"AA90", b"AA91")))
    assert len(old_records(body)) == 2
    with pytest.raises(UnifiedAgendaSourceError, match="max_text_characters"):
        next(reader(body).iter_records())


def test_unavailable_edition_raises_instead_of_silently_yielding_nothing():
    previous = frozen.UnifiedAgendaReader()
    with patch.object(previous, "_download", return_value=None):
        assert list(previous._fetch_edition("202510")) == []
    with pytest.raises(UnifiedAgendaSourceError, match="HTTP 404"):
        next(reader(b"missing", status=404).iter_records())


def test_completed_editions_remain_available_when_a_later_edition_fails():
    requests = []

    def respond(request):
        requests.append(request.url.params["f"])
        body = document(record()) if len(requests) == 1 else b"missing"
        return httpx.Response(
            200 if len(requests) == 1 else 404,
            stream=httpx.ByteStream(body),
            headers={"content-type": "application/xml"},
        )

    rows = UnifiedAgendaReader(editions=("202510", "202504"), transport=httpx.MockTransport(respond)).iter_records()
    assert next(rows) == old_records(document(record()))[0]
    with pytest.raises(UnifiedAgendaSourceError, match="HTTP 404"):
        next(rows)
    assert requests == ["REGINFO_RIN_DATA_202510.xml", "REGINFO_RIN_DATA_202504.xml"]


def test_base_source_imports_do_not_require_optional_wheel():
    code = """
import importlib.abc
import sys
class WithoutOwner(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "spicy_docs" or fullname.startswith("spicy_docs."):
            raise ModuleNotFoundError("optional wheel absent", name="spicy_docs")
sys.meta_path.insert(0, WithoutOwner())
import spicy_regs.sources
import spicy_regs.cli
import spicy_regs.mcp_server
from spicy_regs.sources.unified_agenda import UnifiedAgendaReader
try:
    next(UnifiedAgendaReader().iter_records())
except RuntimeError as error:
    assert "spicy-regs[source-readers]" in str(error)
else:
    raise AssertionError("missing optional wheel was not explained")
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)
