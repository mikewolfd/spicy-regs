"""Finite inert SEC captures exercise maintained parsing and atomic integration."""

import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import retained_native_collections as native

LISTING = "https://www.sec.gov/comments/s7-11-23/s71123.htm"
FILE = "https://www.sec.gov/comments/s7-11-23/s71123-1.htm"
SECTION = "https://www.sec.gov/comments/s7-11-23/s71123-typea.htm"
RULE = "https://www.sec.gov/rules-regulations/2025/06/s7-11-23"


def capture(tmp_path, name, url, body):
    path = tmp_path / name
    path.write_bytes(body)
    return {
        "path": path.name,
        "requested_url": url,
        "resolved_url": url,
        "status_code": 200,
        "content_type": "text/html",
        "observed_at": "2026-09-24T23:56:34Z",
        "bytes": len(body),
        "sha256": native._digest(body),
    }


def listing(page, *, more=False):
    next_link = '<a class="usa-pagination__next-page" href="?page=1">Next</a>' if more else ""
    return (
        f'<html><h1>Selected comments</h1><table class="views-table cols-1"><tr><td>'
        f'<a href="{SECTION}">A: 2</a></td></tr></table>'
        '<table class="views-table cols-3"><tr><td class="views-field-field-comment-date">'
        '<time datetime="2023-08-15T12:00:00Z">Aug 15</time></td>'
        '<td class="views-field-field-comment-letter-type-1">Public Comment</td>'
        f'<td><a href="{FILE}">Literal name</a></td></tr></table>'
        f'<nav class="usa-pagination"><a class="usa-current" href="?page={page}">Current</a>'
        f"{next_link}</nav></html>"
    ).encode()


def selection(tmp_path, *, publisher=False):
    pages = [
        capture(tmp_path, "page0", LISTING, listing(0, more=True)),
        capture(tmp_path, "page1", LISTING + "?page=1", listing(1)),
    ]
    files = [
        capture(tmp_path, "file", FILE, b"<html>File No. S7-11-23</html>"),
        capture(tmp_path, "section", SECTION, b"<html>S7-11-23 template</html>"),
    ]
    unit = {"kind": "sec-comments", "docket": "S7-11-23", "listings": pages, "files": files, "expected_files": 2}
    if publisher:
        rule = (
            '<html><h1>Native title</h1><div class="field--name-field-file-number">'
            '<div class="field__label">File Number</div><div class="field__item">S7-11-23</div></div>'
            '<div class="field--name-field-comments-received"><div class="field__item">'
            f'<a href="{LISTING}">Comments</a></div></div></html>'
        ).encode()
        unit["rule"] = capture(tmp_path, "rule", RULE, rule)
    path = tmp_path / "selection.json"
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    return path, unit


@pytest.fixture(autouse=True)
def small_runtime_pin(monkeypatch):
    # Full runtime pins are exercised by the separately granted real selection;
    # small fixtures do not repeatedly hash every installed source-reader file.
    monkeypatch.setattr(native, "_code_pins", lambda: {"fixture_runtime": "inert"})


def test_complete_native_rows_keep_repeated_declarations_and_all_witnesses(tmp_path):
    path, _ = selection(tmp_path, publisher=True)
    output = tmp_path / "candidate"
    result = native.build(path, output)
    rules = pq.read_table(output / result["outputs"]["sec_rulemakings"]["path"]).to_pylist()
    files = pq.read_table(output / result["outputs"]["sec_comment_files"]["path"]).to_pylist()
    assert len(rules) == 1 and len(files) == 2
    assert rules[0]["title"] == "Native title" and rules[0]["discovery_method"] == "publisher-stated"
    assert all(row["rulemaking_id"] == rules[0]["rulemaking_id"] for row in files)
    comment = next(row for row in files if row["file_url"] == FILE)
    occurrences = json.loads(comment["occurrences_json"])
    assert [x["listing_url"] for x in occurrences] == [LISTING, LISTING + "?page=1"]
    assert occurrences[0]["native"]["date"] == "2023-08-15T12:00:00Z"
    assert occurrences[0]["native"]["link_text"] == "Literal name"
    section = next(row for row in files if row["file_url"] == SECTION)
    assert all(x["kind"] == "sec-comment-section" for x in json.loads(section["occurrences_json"]))
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == 3 and all(x["generation_id"] == result["generation"] for x in receipts)
    file_receipt = next(
        x
        for x in receipts
        if x["dataset"] == "sec_comment_files" and any(w["source_uri"] == FILE for w in x["witnesses"])
    )
    assert [w["source_uri"] for w in file_receipt["witnesses"]] == [RULE, LISTING, LISTING + "?page=1", FILE]
    with pytest.raises(FileExistsError):
        native.build(path, output)


def test_legacy_listing_has_no_invented_rule_page(tmp_path):
    path, _ = selection(tmp_path)
    out = tmp_path / "candidate"
    result = native.build(path, out)
    row = pq.read_table(out / result["outputs"]["sec_rulemakings"]["path"]).to_pylist()[0]
    assert row["rule_url"] is None and row["discovery_method"] == "legacy-derived-listing"
    assert json.loads(row["source_record_json"])["rule"] is None


@pytest.mark.parametrize(
    "change,match",
    [
        ("missing-file", "file set"),
        ("extra-file", "file set"),
        ("missing-page", "not terminal"),
        ("repeat-page", "continuation"),
        ("bad-hash", "hash/size"),
        ("wrong-final", "locator"),
        ("refusal", "successful"),
        ("wrong-docket", "legacy selection"),
        ("wrong-count", "file count"),
    ],
)
def test_incomplete_or_misidentified_selections_never_publish(tmp_path, change, match):
    path, unit = selection(tmp_path)
    if change == "missing-file":
        unit["files"].pop()
    elif change == "extra-file":
        unit["files"].append({**unit["files"][0], "requested_url": FILE + "extra"})
    elif change == "missing-page":
        unit["listings"].pop()
    elif change == "repeat-page":
        unit["listings"][1] = unit["listings"][0]
    elif change == "bad-hash":
        unit["files"][0]["sha256"] = "sha256:" + "0" * 64
    elif change == "wrong-final":
        unit["files"][0]["resolved_url"] = "https://example.test/other"
    elif change == "refusal":
        unit["files"][0]["status_code"] = 403
    elif change == "wrong-docket":
        unit["docket"] = "S7-08-23"
    elif change == "wrong-count":
        unit["expected_files"] = 3
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    output = tmp_path / "candidate"
    with pytest.raises(ValueError, match=match):
        native.build(path, output)
    assert not output.exists()


def test_complete_format_failure_refuses_file_and_leaves_no_candidate(tmp_path):
    path, unit = selection(tmp_path)
    unit["files"][0] = capture(tmp_path, "file", FILE, b"<html>Wrong file number</html>")
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    with pytest.raises(ValueError, match="docket"):
        native.build(path, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_source_drift_after_rows_refuses_atomic_publication(tmp_path, monkeypatch):
    path, _ = selection(tmp_path)
    original = native.combine_receipts

    def drift(*args):
        result = original(*args)
        (tmp_path / "file").write_bytes(b"changed")
        return result

    monkeypatch.setattr(native, "combine_receipts", drift)
    with pytest.raises(ValueError, match="hash/size"):
        native.build(path, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_read_budget_refuses_before_loading_original(tmp_path, monkeypatch):
    path, _ = selection(tmp_path)
    monkeypatch.setattr(native, "MAX_INPUT_READS", 1)
    with pytest.raises(ValueError, match="read budget"):
        native.build(path, tmp_path / "candidate")


def test_finite_builder_selects_bounded_reference_validation(tmp_path, monkeypatch):
    path, _ = selection(tmp_path)
    original = native.write_dataset
    backends = []

    def bounded(*args, **kwargs):
        backends.append(kwargs.get("validation_backend"))
        return original(*args, **kwargs)

    monkeypatch.setattr(native, "write_dataset", bounded)
    native.build(path, tmp_path / "candidate")
    assert backends == ["reference", "reference"]


def test_empty_terminal_listing_requires_no_invented_files(tmp_path):
    # A requested-empty selected page is truthful source evidence. A nonempty
    # file claim must still fail; a future empty-scope policy is explicit.
    path, unit = selection(tmp_path)
    body = b'<html><h1>No rows stated</h1><table class="views-table cols-3"></table></html>'
    unit["listings"] = [capture(tmp_path, "page0", LISTING, body)]
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    with pytest.raises(ValueError, match="file set"):
        native.build(path, tmp_path / "candidate")


def ferc_selection(tmp_path):
    from spicy_docs.sources.ferc.elibrary import ADVANCED_SEARCH_URL, file_list_url
    from spicy_docs.sources.ferc.originals import ORIGINAL_DOWNLOAD_URL, original_download_body

    guid = "0731EC70-0165-CA3E-91B7-912DB6700000"
    accession = "20240807-5052"
    pdf = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n"
    body = {
        "curPage": 1,
        "resultsPerPage": 2,
        "idolResultID": "",
        "docketSearches": [{"docketNumber": "RM24-5", "subDocketNumbers": []}],
        "classTypes": [{"documentClass": "Comments/Protest", "documentType": "Rulemaking Comment"}],
    }
    record = {
        "reference": "publisher-reference",
        "acesssionNumber": accession,
        "description": "Literal description",
        "score": 85.35,
        "affiliations": [{"unknown": "retained"}],
        "transmittals": [{"fileId": guid, "fileName": "original.pdf", "fileSize": len(pdf)}],
    }
    metadata = {
        "ID": guid,
        "Accession_Number": accession,
        "Availability_Mode": "P",
        "File_Size_Num": len(pdf),
        "Orig_File_Name": "original.pdf",
        "MimeType": "application/pdf",
        "File_Type_Code": "PDF",
        "unused": None,
    }

    def entry(name, url, data, request=None, media="application/json"):
        value = capture(tmp_path, name, url, data)
        value.update(content_type=media, method="POST" if request is not None else "GET", request_body=request)
        return value

    page = entry(
        "search.json",
        ADVANCED_SEARCH_URL,
        json.dumps(
            {"success": True, "errorMessage": None, "totalHits": 1, "numHits": 1, "searchHits": [record]}
        ).encode(),
        json.dumps(body),
    )
    listing = entry(
        "files.json", file_list_url(accession), json.dumps({"DataList": [metadata], "ErrorList": []}).encode()
    )
    original = entry(
        "original.pdf", ORIGINAL_DOWNLOAD_URL, pdf, original_download_body(guid).decode(), "application/octet-stream"
    )
    unit = {
        "kind": "ferc-originals",
        "scope": {"docket": "RM24-5", "class": "Comments/Protest", "type": "Rulemaking Comment"},
        "pages": [page],
        "files": [
            {
                "accession": accession,
                "file_id": guid,
                "filename": "original.pdf",
                "declared_bytes": len(pdf),
                "metadata": listing,
                "original": original,
            }
        ],
        "expected_files": 1,
    }
    path = tmp_path / "ferc-selection.json"
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    return path, unit


def test_ferc_native_validation_raw_decimal_and_post_evidence(tmp_path):
    from decimal import Decimal
    from spicy_docs.reading.json_input import load_decimal_json

    path, unit = ferc_selection(tmp_path)
    output = tmp_path / "candidate"
    result = native.build(path, output)
    assert set(result["outputs"]) == set(native.TABLES["ferc-originals"])
    assert all(item["rows"] == 1 for item in result["outputs"].values())
    row = pq.read_table(output / result["outputs"]["ferc_search_documents"]["path"]).to_pylist()[0]
    raw_page = (tmp_path / "search.json").read_text()
    assert row["source_page_json"] == raw_page
    record = load_decimal_json(row["source_page_json"].encode(), source="fixture FERC page")["searchHits"][
        row["record_index"]
    ]
    assert record["score"] == Decimal("85.35") and record["affiliations"] == [{"unknown": "retained"}]
    assert record["acesssionNumber"] == row["accession"]
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == 3
    original = next(row for row in receipts if row["dataset"] == "ferc_original_files")
    assert [w["locator"] for w in original["witnesses"]] == ["searchHits:0", "DataList:0", "original-file"]
    capture_value = native.Captures(path).read(unit["files"][0]["original"])
    assert (
        capture_value.method == "POST"
        and capture_value.request_body == unit["files"][0]["original"]["request_body"].encode()
    )


@pytest.mark.parametrize(
    "fault",
    ["scope", "method", "body", "extra-page", "missing-file", "metadata-size", "pdf", "publisher-failure", "total"],
)
def test_ferc_selection_refusals_publish_nothing(tmp_path, fault):
    path, unit = ferc_selection(tmp_path)
    if fault == "scope":
        unit["scope"]["docket"] = "RM24-6"
    elif fault == "method":
        unit["pages"][0]["method"] = "GET"
    elif fault == "body":
        unit["files"][0]["original"]["request_body"] = "{}"
    elif fault == "extra-page":
        unit["pages"].append(unit["pages"][0])
    elif fault == "missing-file":
        unit["files"] = []
    else:
        entry = (
            unit["pages"][0]
            if fault in ("publisher-failure", "total")
            else unit["files"][0]["metadata" if fault == "metadata-size" else "original"]
        )
        target = tmp_path / entry["path"]
        if fault == "pdf":
            raw = b"x" * entry["bytes"]
        else:
            value = json.loads(target.read_bytes())
            if fault == "metadata-size":
                value["DataList"][0]["File_Size_Num"] += 1
            elif fault == "publisher-failure":
                value["success"] = False
            else:
                value["totalHits"] = 2
            raw = json.dumps(value).encode()
        target.write_bytes(raw)
        entry.update(bytes=len(raw), sha256=native._digest(raw))
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    output = tmp_path / "candidate"
    with pytest.raises((ValueError, KeyError)):
        native.build(path, output)
    assert not output.exists()


def test_ferc_media_refusal_keeps_original_capture_and_post_body(tmp_path):
    from spicy_docs.sources.ferc.download import FercElibraryDownloadBudget
    from spicy_docs.sources.ferc.elibrary import read_file_list
    import httpx

    path, unit = ferc_selection(tmp_path)
    loader = native.Captures(path)
    item = unit["files"][0]
    file_list = read_file_list(loader.read(item["metadata"]), accession=item["accession"])
    item["original"]["content_type"] = "text/plain"
    with native._RetainedFercOriginal(
        budget=FercElibraryDownloadBudget(native.MAX_CAPTURE, 10, 0), transport=httpx.MockTransport(native._no_network)
    ) as acquirer:
        acquirer.retained_init([item["original"]], loader)
        with pytest.raises(ValueError, match="media/size") as raised:
            acquirer.acquire(file_list, file_id=item["file_id"])
    assert raised.value.capture.method == "POST"
    assert raised.value.capture.request_body == item["original"]["request_body"].encode()
    assert raised.value.refused_response.response_bytes == (tmp_path / "original.pdf").read_bytes()


def cftc_selection(tmp_path, *, listing_only=False, pages=1):
    from spicy_docs.sources.cftc_comments.pages import CHANGE_PAGE_PARAM, comment_list_url, view_comment_url, pdf_url

    rule, comment, fileid = 7639, 113985, 35788
    listings = []
    for number in range(1, pages + 1):
        row = f'<tr class="rgRow"><td>9/1/2026</td><td>Literal rule</td><td>First</td><td>Last</td><td>Org<br>Other</td><td><a href="ViewComment.aspx?id={comment + number - 1}"></a></td></tr>'
        footer = ""
        if pages > 1:
            next_link = (
                f'<a title="Next Page" href="CommentList.aspx?id={rule}&amp;{CHANGE_PAGE_PARAM}={number + 1}">next</a>'
                if number < pages
                else ""
            )
            footer = f'<tfoot><tr class="rgPager"><td><div class="rgNumPart"><a class="rgCurrentPage">{number}</a>{next_link}</div><div class="rgInfoPart"><strong>{pages}</strong> items in <strong>{pages}</strong> pages</div></td></tr></tfoot>'
        body = f'<html><body><div id="cphContentMain_MainContent_gvCommentList"><table class="rgMasterTable" id="ctl00_ctl00_cphContentMain_MainContent_gvCommentList_ctl00"><thead><tr><th>Date Received</th><th>Release</th><th>First Name</th><th>Last Name</th><th>Organization</th><th>Edit</th></tr></thead>{footer}<tbody>{row}</tbody></table></div></body></html>'.encode()
        listings.append(
            capture(
                tmp_path,
                f"cftc-list-{number}.html",
                comment_list_url(rule, page=number) if number > 1 else comment_list_url(rule),
                body,
            )
        )
    details = []
    files = []
    if not listing_only:
        for number in range(pages):
            detail = f'<html><body><a id="lnkReturnToReleaseComments" href="CommentList.aspx?id={rule}">View all comments for literal citation</a><div><strong>From:</strong>First Last<strong>Organization(s):</strong>Org<br>Other<strong>Comment No:</strong>{comment + number}<strong>Date:</strong>9/1/2026<strong>Comment Text:</strong>Keep this text</div><a id="hlPdfStaticLink" href="../Handlers/PdfHandler.ashx?id={fileid + number}">Letter{number}.pdf</a></body></html>'.encode()
            details.append(capture(tmp_path, f"cftc-detail-{number}.html", view_comment_url(comment + number), detail))
            body = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n"
            entry = capture(tmp_path, f"cftc-file-{number}.pdf", pdf_url(fileid + number), body)
            entry.update(
                content_type="application/pdf",
                provider_evidence={"transport": "fixture-provider", "request_id": f"fixture-{number}"},
            )
            files.append(entry)
    unit = {
        "kind": "cftc-comments",
        "rule_id": rule,
        "scope": "listing-only" if listing_only else "details-and-files",
        "listings": listings,
        "details": details,
        "files": files,
        "expected_comments": pages,
    }
    path = tmp_path / "cftc-selection.json"
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    return path, unit


@pytest.mark.parametrize("listing_only,pages", [(False, 1), (True, 2)])
def test_cftc_complete_native_scope_and_provenance(tmp_path, listing_only, pages):
    path, unit = cftc_selection(tmp_path, listing_only=listing_only, pages=pages)
    output = tmp_path / "candidate"
    result = native.build(path, output)
    assert set(result["outputs"]) == set(native.TABLES["cftc-comments"])
    comments = pq.read_table(output / result["outputs"]["cftc_comments"]["path"]).to_pylist()
    assert len(comments) == pages
    assert all(json.loads(row["native_listing_row_json"])["organizations"] == ["Org", "Other"] for row in comments)
    assert all(row["detail_state"] == ("not-selected" if listing_only else "complete") for row in comments)
    assert all((row["native_detail_json"] is None) == listing_only for row in comments)
    if not listing_only:
        detail = json.loads(comments[0]["native_detail_json"])
        assert detail["text"] == "Keep this text" and detail["rule_citation"] == "literal citation"
        files = pq.read_table(output / result["outputs"]["cftc_comment_files"]["path"]).to_pylist()
        assert len(files) == 1 and json.loads(files[0]["provider_evidence_json"])["transport"] == "fixture-provider"
    else:
        assert result["outputs"]["cftc_comment_files"]["rows"] == 0
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == 1 + pages + (0 if listing_only else 1)


@pytest.mark.parametrize(
    "fault", ["missing-pdf", "missing-detail", "wrong-detail-rule", "count", "missing-continuation"]
)
def test_cftc_incomplete_selection_refuses_without_candidate(tmp_path, fault):
    path, unit = cftc_selection(
        tmp_path, listing_only=fault == "missing-continuation", pages=2 if fault == "missing-continuation" else 1
    )
    if fault == "missing-pdf":
        unit["files"] = []
    elif fault == "missing-detail":
        unit["details"] = []
    elif fault == "wrong-detail-rule":
        entry = unit["details"][0]
        target = tmp_path / entry["path"]
        raw = target.read_bytes().replace(b"id=7639", b"id=7640")
        target.write_bytes(raw)
        entry.update(bytes=len(raw), sha256=native._digest(raw))
    elif fault == "count":
        unit["expected_comments"] += 1
    else:
        unit["listings"] = unit["listings"][:1]
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    with pytest.raises(ValueError):
        native.build(path, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_retained_post_request_types_cannot_substitute_zero_for_false(tmp_path):
    path, unit = ferc_selection(tmp_path)
    body = json.loads(unit["files"][0]["original"]["request_body"])
    body["Islegacy"] = 0
    unit["files"][0]["original"]["request_body"] = json.dumps(body)
    path.write_text(json.dumps({"format": "spicy-retained-native-inputs", "version": 1, "sources": [unit]}))
    with pytest.raises(ValueError, match="POST body differs"):
        native.build(path, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
