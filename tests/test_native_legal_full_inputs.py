"""Bill/law native admission checks exact identity without inventing editions."""

from pathlib import Path

import pytest

from spicy_regs.transforms.native_legal_inputs import qualify_source

FIXTURES = Path(__file__).resolve().parent / "fixtures/native_legal_full_inputs"


def test_law_qualified_identity_and_unknown_edition():
    body = (FIXTURES / "plaw-119publ77.xml").read_bytes()
    spec = {
        "source_family": "law",
        "source_record_key": "119-public-77",
        "edition": None,
        "source_locator": "https://www.govinfo.gov/bulkdata/PLAW/119/public/PLAW-119publ77.xml",
        "qualification": {"kind": "law-native-xml", "congress": 119, "law_kind": "public", "number": 77},
    }
    qualified = qualify_source(spec, body, lambda _: b"", lambda *a, **kw: None)
    assert qualified is not None
    assert qualified["metadata"]["congress"] == "119"
    for field, wrong in [
        ("source_record_key", "119-private-77"),
        ("source_family", "bill"),
        ("edition", "2026-10-02"),
        ("source_locator", "synthetic:wrong"),
    ]:
        with pytest.raises(ValueError):
            qualify_source({**spec, field: wrong}, body, lambda _: b"", lambda *a, **kw: None)
    spec["qualification"]["number"] = 78
    with pytest.raises(ValueError):
        qualify_source(spec, body, lambda _: b"", lambda *a, **kw: None)


def test_bill_printing_identity():
    body = (FIXTURES / "constructed-bill-divisions.xml").read_bytes()
    spec = {
        "source_family": "bill",
        "source_record_key": "BILLS-119hr4000ih",
        "edition": "ih",
        "source_locator": "https://www.govinfo.gov/content/pkg/BILLS-119hr4000ih/xml/BILLS-119hr4000ih.xml",
        "qualification": {
            "kind": "bill-dtd-xml",
            "congress": 119,
            "bill_type": "hr",
            "number": 4000,
            "package_id": "BILLS-119hr4000ih",
        },
    }
    qualified = qualify_source(spec, body, lambda _: b"", lambda *a, **kw: None)
    assert qualified is not None
    assert qualified["metadata"]["version_code"] == "ih"
    with pytest.raises(ValueError):
        qualify_source({**spec, "edition": "enr"}, body, lambda _: b"", lambda *a, **kw: None)
