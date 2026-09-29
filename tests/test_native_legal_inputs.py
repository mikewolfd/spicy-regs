"""Complete retained native inputs establish identities; mismatches refuse."""

from pathlib import Path
import hashlib
import json

import pytest
from spicy_docs.sources.uscode import ReleasePoint, TitleSelection, title_xml_locator
from spicy_docs.sources.uscode.archive import read_title_archive
from spicy_regs.transforms.native_legal_inputs import qualify_source

FIXTURES = Path(__file__).parent / "fixtures/native_legal_references"


def test_complete_usc_archive_membership_and_edition():
    archive = (FIXTURES / "title01-119-103.zip").read_bytes()
    selection = TitleSelection(ReleasePoint(119, 103), "01")
    title = read_title_archive(archive, selection=selection)
    spec = {
        "source_family": "uscode",
        "source_record_key": selection.identifier,
        "edition": "119-103",
        "source_locator": title_xml_locator(selection),
        "qualification": {"kind": "uscode-title-archive", "title": "01", "release_point": "119-103", "archive": {}},
    }
    retained = []
    facts = qualify_source(spec, title.xml_bytes, lambda _: archive, lambda b, **_: retained.append(b))
    assert facts is not None
    assert facts["member"]["metadata"]["release_point"] == "Online@119-103"
    assert retained == [archive]
    with pytest.raises(ValueError, match="archive member"):
        qualify_source(spec, title.xml_bytes + b"\n", lambda _: archive, lambda *a, **kw: None)
    spec["edition"] = "119-102"
    with pytest.raises(ValueError, match="edition"):
        qualify_source(spec, title.xml_bytes, lambda _: archive, lambda *a, **kw: None)


def test_complete_ecfr_identity_and_receipt_mutations():
    body = (FIXTURES / "full-ecfr-title1.xml").read_bytes()
    url = "https://www.ecfr.gov/api/versioner/v1/full/2026-08-10/title-1.xml"
    row = {
        "title": 1,
        "url": url,
        "date": "2026-08-10",
        "http_status": 200,
        "ok": True,
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }
    spec = {
        "source_family": "ecfr",
        "source_record_key": "ecfr/title/1",
        "edition": "requested-as-of:2026-08-10",
        "source_locator": url,
        "qualification": {"kind": "ecfr-retained-title", "title": 1, "requested_date": "2026-08-10", "receipt": {}},
    }

    def read(_):
        return json.dumps({"titles": [row]}).encode()

    facts = qualify_source(spec, body, read, lambda *a, **kw: None)
    assert facts is not None
    assert facts["metadata"]["title"] == 1
    assert "date:request-url" in facts["metadata"]["identity_basis"]
    row["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="receipt disagrees"):
        qualify_source(spec, body, read, lambda *a, **kw: None)
    row["sha256"] = hashlib.sha256(body).hexdigest()
    spec["edition"] = "2026-08-10"
    with pytest.raises(ValueError, match="request-based"):
        qualify_source(spec, body, read, lambda *a, **kw: None)


def test_complete_native_exact_href_shapes_and_abstentions():
    from collections import Counter
    from xml.etree import ElementTree as ET
    from spicy_docs.interpretation.native_legal_references import NATIVE_HREF_RULE, interpret_native_reference

    title = read_title_archive(
        (FIXTURES / "title01-119-103.zip").read_bytes(),
        selection=TitleSelection(ReleasePoint(119, 103), "01"),
    )
    counts = Counter()
    for element in ET.fromstring(title.xml_bytes).iter():
        if "href" not in element.attrib:
            continue
        row = {
            "scope_id": "fixture",
            "occurrence_index": "1",
            "source_family": "uscode",
            "source_record_key": "/us/usc/t1",
            "observation_kind": "native_reference",
            "element_tag": element.tag,
            "href": element.attrib["href"],
        }
        reading = interpret_native_reference(row)
        status, candidates = reading.status, list(reading.candidates)
        counts[status] += 1
        if status != "unsupported_href":
            assert len(candidates) == 1
            assert candidates[0]["matched_text"] == element.attrib["href"]
            assert candidates[0]["derivation_rule"] == NATIVE_HREF_RULE == "native-legal-exact-href/002"
        if element.attrib["href"] == "/us/pl/117/228":
            assert candidates[0]["target_key"] == "117-public-228"
        if element.attrib["href"] == "/us/stat/61/633":
            assert candidates[0]["target_key"] == "61-633"
        if element.attrib["href"] == "/us/pl/107/207/s2/b":
            assert status == "unsupported_href" and not candidates
    assert counts["native_statute_href"] > 0 and counts["native_public_law_href"] > 0
    assert counts["unsupported_href"] > 0
