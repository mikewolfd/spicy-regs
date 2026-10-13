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
