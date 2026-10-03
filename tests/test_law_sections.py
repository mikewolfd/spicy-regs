"""Law text reaches query tables from the law's own captured XML."""

from __future__ import annotations

import shutil
from types import SimpleNamespace

import pytest
import pyarrow.parquet as pq

from tests.test_laws import (
    LAW_119_1, StubListingReader, StubUslm, USLM_BYTES, _build, _by_law, _rows,
)


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")


def test_law_sections_are_published_from_the_same_capture(tmp_path, scoped):
    source = StubUslm(unavailable={110, 109, 104})
    outputs = _build(tmp_path, uslm=source)
    by_name = {path.stem: path for path in outputs}
    assert "law_sections" in by_name
    law = _by_law(by_name["laws"])["119-public-1"]
    sections = _rows(by_name["law_sections"])
    assert law["law_text_outcome"] == "parsed"
    assert law["law_text_url"] == "https://www.govinfo.gov/bulkdata/PLAW/119/public/PLAW-119publ1.xml"
    assert int(law["law_section_count"]) == len(sections) == 3
    assert {row["law_id"] for row in sections} == {"119-public-1"}
    assert {row["source_sha256"] for row in sections} == {law["uslm_sha256"]}
    assert source.selections.count(source.selections[-1]) == 1
    assert any("Laken Riley Act" in row["body"] for row in sections)


def test_unacquired_laws_do_not_claim_empty_sections(tmp_path, scoped):
    by_name = {path.stem: path for path in _build(tmp_path)}
    law = _by_law(by_name["laws"])["119-public-110"]
    assert law["law_text_outcome"] == "not_requested"
    assert law["law_section_count"] is None


def _hold(outputs, directory):
    for path in outputs:
        shutil.copyfile(path, directory / f"_{path.stem}_prior.parquet")


def test_parsed_law_is_not_fetched_again_and_sections_stay(tmp_path, scoped):
    first = _build(tmp_path)
    expected = _rows(first[-1])
    _hold(first, tmp_path)
    source = StubUslm(unavailable={110, 109, 104})
    second = _build(tmp_path, uslm=source)
    assert [s.number for s in source.selections] == [110, 109, 104]
    assert _rows(second[-1]) == expected


def test_missing_child_rows_force_reread_despite_current_parent_status(tmp_path, scoped):
    first = _build(tmp_path)
    expected = _rows(first[-1])
    _hold(first, tmp_path)
    prior = tmp_path / "_law_sections_prior.parquet"
    pq.write_table(pq.read_table(prior).slice(0, 1), prior)
    source = StubUslm(unavailable={110, 109, 104})
    second = _build(tmp_path, uslm=source)
    assert [s.number for s in source.selections] == [110, 109, 104, 1]
    assert _rows(second[-1]) == expected


def test_first_text_refusal_preserves_metadata_without_claiming_empty_text(tmp_path, scoped, monkeypatch):
    import importlib
    from spicy_docs.sources.govinfo.uslm import UslmSourceError

    module = importlib.import_module("spicy_regs.transforms.build_laws")

    def refuse(*args, **kwargs):
        raise UslmSourceError("fixture: section layout refused")

    monkeypatch.setattr(module, "read_law_sections", refuse)
    outputs = _build(tmp_path)
    law = _by_law(outputs[0])["119-public-1"]
    assert law["uslm_outcome"] == "captured"
    assert law["statutes_at_large_cite"] == "139 Stat. 3"
    assert law["law_text_outcome"] == "refused"
    assert law["law_text_reason"] == "section_structure_refused"
    assert law["law_section_count"] is None and law["law_body_remainder"] is None
    assert _rows(outputs[-1]) == []


def test_changed_law_replaces_all_sections_and_failed_parse_preserves_pair(tmp_path, scoped):
    from spicy_docs.sources.govinfo.uslm import public_law_xml_locator, validate_public_law_xml
    from spicy_docs.transport.captured import CapturedBodyResponse

    first = _build(tmp_path)
    _hold(first, tmp_path)
    # One changed native section, while the list's update date advances.
    from xml.etree import ElementTree as ET
    root = ET.fromstring(USLM_BYTES)
    ns = {"u": "http://schemas.gpo.gov/xml/uslm"}
    main = root.find("u:main", ns)
    assert main is not None
    for section in main.findall("u:section", ns)[1:]:
        main.remove(section)
    reduced = ET.tostring(root, encoding="utf-8")

    class Source(StubUslm):
        body = reduced

        def acquire_public_law(self, selection, *, max_bytes=None):
            if selection.number != 1:
                return super().acquire_public_law(selection, max_bytes=max_bytes)
            self.selections.append(selection)
            url = public_law_xml_locator(selection)
            meta = validate_public_law_xml(self.body, selection=selection, final_url=url)
            capture = CapturedBodyResponse(url, url, 200, "application/xml", "2026-10-02T00:00:00Z", self.body)
            return SimpleNamespace(metadata=meta, capture=capture)

    record = dict(LAW_119_1, updateDate="2026-10-02T00:00:00Z")
    reader = StubListingReader({119: [record]})
    source = Source(unavailable={110, 109, 104})
    updated = _build(tmp_path, reader=reader, uslm=source)
    children = _rows(updated[-1])
    assert len(children) == 1
    law = _by_law(updated[0])["119-public-1"]
    assert law["law_section_count"] == "1"
    assert children[0]["source_sha256"] == law["uslm_sha256"]
    _hold(updated, tmp_path)

    # Valid identity and source content, but no native sections to publish.
    for child in list(main):
        main.remove(child)
    ET.SubElement(main, "{http://schemas.gpo.gov/xml/uslm}content").text = "Unsupported layout"
    source.body = ET.tostring(root, encoding="utf-8")
    reader = StubListingReader({119: [dict(record, updateDate="2026-10-03T00:00:00Z")]})
    refused = _build(tmp_path, reader=reader, uslm=source)
    assert _by_law(refused[0])["119-public-1"] == law
    assert _rows(refused[-1]) == children
