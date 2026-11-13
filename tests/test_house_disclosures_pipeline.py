"""Real retained House source rows through candidate, generation, replay and refusal paths."""

from copy import deepcopy
import hashlib
from io import BytesIO
import json
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines import house_disclosures as house
from spicy_regs.source_evidence import CaptureEvidence

FIXTURES = Path(__file__).parent / "fixtures" / "house_disclosures"


@pytest.fixture
def selected(tmp_path, monkeypatch):
    root = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(root))
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    evidence = CaptureEvidence(tmp_path / "inputs", house.FAMILY, archive_root=root)
    for name, source in manifest.items():
        receipt = evidence.retain_file(FIXTURES / name, stage="original-test-input", raw_source=True,
                                      original_location=source["url"])
        assert receipt["sha256"] == source["sha256"]
    evidence.finish()
    index = {**manifest["2025FD.zip"], "year": 2025}
    reports = [{**manifest[doc + ".pdf"], "doc_id": doc, "year": 2025}
               for doc in ["20030591", "20030594", "8220747"]]
    return {"format": "house-disclosures-captures", "version": 1, "index": index,
            "reports": reports, "scope": "Exact retained fixture originals; source-wide coverage is not inferred."}


def rows(path, table):
    return pq.read_table(path / "generation" / (table + ".parquet")).to_pylist()


def planted_name(replacement, *, change_signature=False):
    """Change only identity text on a real original; keep the result derived."""
    import pymupdf

    old = "Hon. Bradley S. Schneider"
    with pymupdf.open(stream=(FIXTURES / "20030591.pdf").read_bytes(), filetype="pdf") as document:
        page = document[0]
        spans = [span for block in page.get_text("dict")["blocks"]
                 for line in block.get("lines", []) for span in line["spans"]
                 if old in span["text"] and (span["text"] == old or change_signature)]
        for span in spans:
            page.add_redact_annot(span["bbox"])
        page.apply_redactions()
        if replacement is not None:
            for span in spans:
                page.insert_text(span["origin"], span["text"].replace(old, replacement),
                                 fontname="tiro", fontsize=span["size"])
        return document.tobytes()


class DerivedInputs:
    """A test-only reader/evidence sink, never the central original archive."""

    def __init__(self, selected, body):
        self.selected, self.body, self.retained = selected, body, []

    def open(self, digest):
        source = self.selected["index"]
        return BytesIO((FIXTURES / "2025FD.zip").read_bytes() if digest == source["sha256"] else self.body)

    def retain_bytes(self, body, **fields):
        self.retained.append((body, fields))

    def event(self, *args, **fields):
        pass


@pytest.mark.parametrize("replacement,reason", [
    (None, "native_filer_name_missing_or_ambiguous"),
    ("Hon. Other Person", "native_header_signature_name_mismatch"),
])
def test_real_original_missing_or_wrong_name_cannot_qualify_consumer_transactions(selected, tmp_path, replacement, reason):
    changed = deepcopy(selected)
    changed["reports"] = [changed["reports"][0]]
    body = planted_name(replacement)
    changed["reports"][0].update(sha256="sha256:" + hashlib.sha256(body).hexdigest(), bytes=len(body))
    inputs = DerivedInputs(changed, body)
    house.candidate(changed, tmp_path / "refused-name", inputs, store=inputs)
    report = pq.read_table(tmp_path / "refused-name" / (house.REPORTS + ".parquet")).to_pylist()[0]
    assert report["extraction_status"] == "refused" and reason in report["refusal_reason"]
    assert report["filer_name_raw"] == replacement and report["transaction_count"] == 0
    assert pq.read_table(tmp_path / "refused-name" / (house.TRANSACTIONS + ".parquet")).num_rows == 0
    assert any(kept == body for kept, _fields in inputs.retained)


@pytest.mark.parametrize("name", ["Hon. Other Person", "Hon. Bradley Scott Schneider", "Hon. Bradley S. SCHNEIDER"])
def test_coherent_native_name_must_match_literal_selected_index_name(selected, tmp_path, name):
    from spicy_docs.reading.house_ptr import read_ptr_pdf

    changed = deepcopy(selected)
    changed["reports"] = [changed["reports"][0]]
    body = planted_name(name, change_signature=True)
    changed["reports"][0].update(sha256="sha256:" + hashlib.sha256(body).hexdigest(), bytes=len(body))
    reading = read_ptr_pdf(body, doc_id="20030591")
    assert reading["status"] == "native_layout" and reading["filer_name_raw"] == name
    inputs = DerivedInputs(changed, body)
    with pytest.raises(ValueError, match="native filer name differs"):
        house.candidate(changed, tmp_path / "index-name-mismatch", inputs, store=inputs)
    assert not (tmp_path / "index-name-mismatch").exists()
    assert any(kept == body for kept, _fields in inputs.retained)


def test_build_and_bounded_replay_preserve_amendments_blanks_ranges_and_refusals(selected, tmp_path):
    artifact, evidence, summary = house.build(selected, tmp_path / "first")
    assert summary["rows"] == {house.INDEX: 2935, house.REPORTS: 3, house.PAGES: 4,
                               house.TRANSACTIONS: 2, house.LINKS: 0}
    transactions = rows(tmp_path / "first", house.TRANSACTIONS)
    assert [row["owner_raw"] for row in transactions] == ["", ""]
    assert [row["filing_status_raw"] for row in transactions] == ["New", "Amended"]
    assert [row["transaction_date"].year for row in transactions] == [2024, 2025]
    assert all((row["amount_lower_usd"], row["amount_upper_usd"]) == (1001, 15000) for row in transactions)
    report = next(row for row in rows(tmp_path / "first", house.REPORTS) if row["doc_id"] == "8220747")
    assert report["extraction_status"] == "refused" and "image_page" in report["refusal_reason"]
    assert report["transaction_count"] == 0  # The refusal remains a different fact from a valid empty report.
    assert len([r for r in rows(tmp_path / "first", house.INDEX) if r["doc_id"] == "10078188"]) == 2
    assert artifact.root["spec"]["family"] == house.FAMILY
    assert artifact.root["inputs"]
    journal = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    assert any(event["event"] == "house-report-reading" and event["status"] == "refused" for event in journal)
    house.build(selected, tmp_path / "replay")
    for table in house.SCHEMAS:
        first = (tmp_path / "first/generation" / (table + ".parquet")).read_bytes()
        assert first == (tmp_path / "replay/generation" / (table + ".parquet")).read_bytes()


def test_corrupt_pin_and_non_ptr_selection_cannot_make_a_generation(selected, tmp_path):
    changed = deepcopy(selected)
    changed["reports"][0]["bytes"] += 1
    with pytest.raises(ValueError, match="selected size or SHA"):
        house.build(changed, tmp_path / "corrupt")
    assert not (tmp_path / "corrupt/generation").exists()
    changed = deepcopy(selected)
    changed["reports"][0]["doc_id"] = "10077643"
    with pytest.raises(ValueError, match="not P"):
        house.build(changed, tmp_path / "annual")
    assert not (tmp_path / "annual/generation").exists()


def test_acquisition_reuses_exact_originals_and_preserves_access_refusal(selected, tmp_path):
    requests = []

    def no_network(request):
        requests.append(str(request.url))
        raise AssertionError("Exact selected originals should replay without network")

    replay = house.acquire(selected, ["20030591"], tmp_path / "reuse", transport=httpx.MockTransport(no_network))
    assert replay["reports"] == sorted(selected["reports"], key=lambda row: row["doc_id"])
    assert not requests
    with pytest.raises(ValueError, match="P filings"):
        house.acquire(selected, ["10077643"], tmp_path / "not-p", transport=httpx.MockTransport(no_network))
    refused = deepcopy(selected)
    refused["reports"] = []
    from spicy_docs.transport.credentials import CredentialRefusedError

    with pytest.raises(CredentialRefusedError):
        house.acquire(refused, ["20030591"], tmp_path / "denied", transport=httpx.MockTransport(
            lambda request: httpx.Response(403, stream=httpx.ByteStream(b"access denied"),
                                           headers={"content-type": "text/html"})))
    journals = list((tmp_path / "denied").glob("source-evidence/*/artifact/journal.jsonl"))
    events = [json.loads(line) for line in journals[0].read_text().splitlines()]
    assert any(event["event"] == "refusal" for event in events)


def test_unreadable_and_open_ended_amounts_never_become_exact_values():
    assert house.amount_range("$1,001 -\n$15,000") == (1001, 15000, "reported_range", "USD")
    assert house.amount_range("Over $50,000,000") == (50000000, None, "reported_exclusive_lower_bound", "USD")
    assert house.amount_range("") == (None, None, "blank", None)
    assert house.amount_range("not disclosed") == (None, None, "unparsed", None)
    assert house.amount_range("$15,000 - $1,001") == (None, None, "unparsed", None)


def test_index_refresh_retains_all_categories_in_the_requested_archive(tmp_path):
    body = (FIXTURES / "2025FD.zip").read_bytes()
    raw = tmp_path / "explicit-archive"
    selection = house.acquire_index(2025, tmp_path / "refresh", archive_root=raw,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, stream=httpx.ByteStream(body), headers={"content-type": "application/zip"})))
    assert selection["reports"] == [] and selection["index"]["bytes"] == len(body)
    index, kept, rows = house._index(house.open_raw(raw), selection)
    assert kept == body and len(rows) == 2935
    assert {row.filing_type for row in rows} >= {"A", "P", "O"}


def test_reviewed_reference_is_bound_to_source_versions_and_review_bytes(selected, tmp_path):
    changed = deepcopy(selected)
    review = tmp_path / "review.md"
    review.write_text("Independent original review of an explicit previous filing date.\n")
    changed["review"] = {"path": str(review), "sha256": "sha256:" + hashlib.sha256(review.read_bytes()).hexdigest()}
    original, amended = changed["reports"][:2]
    changed["reviewed_amendment_links"] = [{
        "original_doc_id": original["doc_id"], "amended_doc_id": amended["doc_id"],
        "original_source_sha256": original["sha256"], "amended_source_sha256": amended["sha256"],
        "original_page_number": 1, "amended_page_number": 1, "prior_filing_date_raw": "07/01/2025",
        "link_status": "reviewed_date_reference", "source_statement": "Fixture reviewed reference",
        "basis": "Fixture only; no live relationship assertion.", "uncertainty": "Fixture relationship control."}]
    house.build(changed, tmp_path / "reviewed")
    link = rows(tmp_path / "reviewed", house.LINKS)[0]
    assert link["original_doc_id"] == "20030591" and link["review_sha256"] == changed["review"]["sha256"]
    changed["reviewed_amendment_links"][0]["amended_source_sha256"] = original["sha256"]
    with pytest.raises(ValueError, match="different original version"):
        house.build(changed, tmp_path / "changed-source")
    review.write_text("changed")
    with pytest.raises(ValueError, match="review differs"):
        house.build(changed, tmp_path / "changed-review")
