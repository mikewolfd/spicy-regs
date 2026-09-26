"""The generation audit over local fake public bases: each blind spot of the hand-written audits is caught."""

import gzip
import hashlib
import json
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import MemberNotFoundError, canonical_json_bytes, parse_canonical_json
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.content_checks import Scan, configured_secrets, expected_kind, sniff
from spicy_regs.generation_audit import Declaration, PublicBase, audit, declared_tables, main
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store

FIXTURES = Path(__file__).parent / "fixtures"
HTML_200 = FIXTURES / "generation_audit" / "plaw-119pvtl1-http-200.html"
USLM = FIXTURES / "congress_laws" / "plaw-119pvtl1.xml"
PLAW_URL = "https://www.govinfo.gov/content/pkg/PLAW-119pvtl1/xml/PLAW-119pvtl1.xml"
KEY = "Qz7xK2mW9pL4vR8tN3yB6cH1jD5sF0gA"
DECLARED = {"t": Declaration((("id", "VARCHAR"), ("v", "VARCHAR")), ("id",), "test")}
CITING = {"t": Declaration((("id", "VARCHAR"), ("capture_sha256", "VARCHAR")), ("id",), "test")}


def rows(*pairs, v=pa.string()):
    return pa.table({"id": pa.array([p[0] for p in pairs], pa.string()), "v": pa.array([p[1] for p in pairs], v)})


def publish(tmp_path, store, name, tables, *, prior=None, evidence=None):
    """Build and publish one ``test`` generation into the fake bucket; returns the new index."""
    source = tmp_path / f"{name}-source"
    source.mkdir()
    for key, table in tables.items():
        pq.write_table(table, source / key)
    directory = tmp_path / name
    build_generation(directory, family="test", files=[source / key for key in tables], expected_keys=list(tables),
                     read_snapshot=prior, inputs=evidence.inputs() if evidence else ())
    return pub.publish_generation(directory, client=store, bucket="test", prior_index=prior or pub.empty_index(),
                                  evidence_directories=(evidence.artifact_dir,) if evidence else ())


def public_base(store, directory):
    """The bucket's objects laid out as a public base directory, as anonymous readers see them."""
    for key, raw in store.objects.items():
        path = directory / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    return PublicBase(str(directory))


def codes(report, severity=None):
    return {f["code"] for f in report["findings"] if severity is None or f["severity"] == severity}


def test_a_duplicate_identity_is_found_before_rows_are_paired(tmp_path):
    store = Store()
    prior = publish(tmp_path, store, "prior", {"t.parquet": rows(("1", "a"), ("2", "b"))})
    publish(tmp_path, store, "current", {"t.parquet": rows(("1", "a"), ("1", "a"), ("2", "b"))}, prior=prior)

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=DECLARED)

    identity = report["sections"]["identity"]["t"]
    assert (identity["status"], identity["duplicate_identities"], identity["rows_in_duplicate_identities"]) == (
        "duplicates", 1, 2)
    assert identity["duplicate_sample"] == [{"identity": {"id": "1"}, "rows": 2}]
    conservation = report["sections"]["conservation"]["t"]
    assert conservation["method"].startswith("multiset")
    assert conservation["current_rows_not_in_prior"] == {
        "rows": 1, "sample": [{"record": {"id": "1", "v": "a"}, "surplus": 1}]}
    assert conservation["prior_rows_not_in_current"] == {"rows": 0, "sample": []}
    assert conservation["identities_removed"] == conservation["identities_added"] == 0
    assert "duplicate-identities" in codes(report, "fail")
    assert report["dispositions"]["t"]["identity"] == "duplicates"


def test_identity_indexing_as_the_hand_written_audits_did_hides_that_duplicate(tmp_path):
    store = Store()
    prior = publish(tmp_path, store, "prior", {"t.parquet": rows(("1", "a"), ("2", "b"))})
    index = publish(tmp_path, store, "current", {"t.parquet": rows(("1", "a"), ("1", "a"), ("2", "b"))}, prior=prior)
    base = public_base(store, tmp_path / "public")

    def indexed(entry):
        return {r["id"]: r for r in pq.read_table(base.path(entry["prefix"] + "/t.parquet")).to_pylist()}

    assert indexed(index["families"]["test"]) == indexed(prior["families"]["test"])
    assert audit(base, family="test", declarations=DECLARED)["summary"]["fail"] >= 1


def test_a_type_mismatch_is_found_where_names_match(tmp_path):
    store = Store()
    publish(tmp_path, store, "current", {"t.parquet": rows(("1", 10), ("2", 20), v=pa.int64())})

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=DECLARED)

    schema = report["sections"]["schema"]["t"]
    assert (schema["missing_columns"], schema["extra_columns"], schema["order_matches"]) == ([], [], True)
    assert schema["type_mismatches"] == [{"column": "v", "declared": "VARCHAR", "observed": "BIGINT"}]
    assert schema["status"] == "differs-from-declaration"
    assert "schema-differs-from-declaration" in codes(report, "fail")
    assert report["sections"]["publication"]["admission"]["admitted"]


def test_conservation_reports_removed_added_and_changed_rows_in_both_directions(tmp_path):
    store = Store()
    first = publish(tmp_path, store, "first", {"t.parquet": rows(("1", "a"), ("2", "b"), ("3", "c"))})
    second = publish(tmp_path, store, "second", {"t.parquet": rows(("1", "a"), ("2", "b"), ("3", "c"), ("5", "e"))},
                     prior=first)
    publish(tmp_path, store, "third", {"t.parquet": rows(("1", "A"), ("2", "b"), ("4", "d"), ("5", "e"))},
            prior=second)
    first_pin = first["families"]["test"]["artifactDigest"]

    # The pin is resolved through the captured chain, two generations back.
    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=DECLARED,
                   prior=first_pin[7:15])

    assert report["sections"]["prior"]["artifactDigest"] == first_pin
    assert report["sections"]["prior"]["admission"].startswith("admitted")
    conservation = report["sections"]["conservation"]["t"]
    assert conservation["method"].startswith("identity-pairing")
    assert (conservation["identities_removed"], conservation["identities_added"]) == (1, 2)
    assert conservation["removed_sample"] == [{"id": "3"}]
    assert conservation["added_sample"] == [{"id": "4"}, {"id": "5"}]
    assert conservation["changed_cells_by_column"] == {
        "v": {"rows": 1, "sample": [{"identity": {"id": "1"}, "prior": "a", "current": "A"}]}}
    assert conservation["status"] == "changed"
    assert "prior-identities-or-rows-removed" in codes(report, "review")
    assert not codes(report, "fail")


def test_an_unknown_prior_pin_refuses(tmp_path):
    store = Store()
    publish(tmp_path, store, "current", {"t.parquet": rows(("1", "a"))})
    with pytest.raises(RuntimeError, match="captured prior chain"):
        audit(public_base(store, tmp_path / "public"), family="test", declarations=DECLARED, prior="deadbeef")


def test_a_chain_root_whose_own_digest_field_lies_refuses(tmp_path):
    """The content digest omits the root's ``artifactDigest`` field, so only publication.family_root sees it."""
    store = Store()
    first = publish(tmp_path, store, "first", {"t.parquet": rows(("1", "a"))})
    second = publish(tmp_path, store, "second", {"t.parquet": rows(("1", "b"))}, prior=first)
    publish(tmp_path, store, "third", {"t.parquet": rows(("1", "c"))}, prior=second)
    key = f"{second['families']['test']['prefix']}/artifact.json"
    root = parse_canonical_json(store.objects[key])
    store.objects[key] = canonical_json_bytes({**root, "artifactDigest": "sha256:" + "0" * 64})

    with pytest.raises(RuntimeError, match="differs from the pin that named it"):
        audit(public_base(store, tmp_path / "public"), family="test", declarations=DECLARED,
              prior=first["families"]["test"]["artifactDigest"])


def evidence_generation(tmp_path, store, captures):
    evidence = CaptureEvidence(tmp_path, "test")
    for capture in captures:
        evidence.capture(capture, stage="source")
    cited = "sha256:" + hashlib.sha256(HTML_200.read_bytes()).hexdigest()
    table = pa.table({"id": ["119-private-1"], "capture_sha256": [cited]})
    return publish(tmp_path, store, "current", {"t.parquet": table}, evidence=evidence)


def capture(url, body, *, content_type="application/xml", status=200, encoding="identity"):
    return CapturedBodyResponse(url, url, status, content_type, "2026-09-25T00:00:00Z", body, content_encoding=encoding)


def test_an_html_body_served_with_200_where_xml_was_requested_is_flagged(tmp_path):
    prolog_xhtml = (b'<?xml version="1.0" encoding="utf-8"?>\n<!-- error -->\n'
                    b'<html xmlns="http://www.w3.org/1999/xhtml"><head><title>404</title></head></html>')
    store = Store()
    evidence_generation(tmp_path, store, [
        capture(PLAW_URL, HTML_200.read_bytes(), content_type="text/html; charset=UTF-8"),
        capture("https://source.test/law/119?format=json", prolog_xhtml, content_type="application/json"),
        capture("https://source.test/PLAW-119pvtl1.xml", USLM.read_bytes()),
        capture("https://source.test/missing.xml", b"<html>gone</html>", status=404),
    ])

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=CITING)

    evidence = report["sections"]["evidence"]
    assert evidence["admission"]["admitted"]
    assert evidence["binding"] == {"family_matches": True, "inherited_inputs_match_generation": True}
    unexpected = {item["requested_url"]: item for item in evidence["body_shapes"]["unexpected_2xx"]}
    assert set(unexpected) == {PLAW_URL, "https://source.test/law/119?format=json"}
    assert unexpected[PLAW_URL]["observed"] == "html"
    assert unexpected["https://source.test/law/119?format=json"] | {"expected": "json", "observed": "html"} == \
        unexpected["https://source.test/law/119?format=json"]
    # A published row cites the HTML body's digest as its capture, so that one is a failure, not a note.
    assert unexpected[PLAW_URL]["cited_by"] == {"t": {"capture_sha256": 1}}
    flagged = [f for f in report["findings"] if f["code"] == "2xx-body-is-not-the-expected-kind"]
    assert sorted(f["severity"] for f in flagged) == ["fail", "review"]


def test_a_key_in_an_evidence_blob_is_flagged_without_reporting_it(tmp_path, monkeypatch):
    compressed = gzip.compress(b'{"echo": "' + KEY.encode() + b'", "n": 1}')
    store = Store()
    evidence_generation(tmp_path, store, [
        capture("https://api.test/v3/law/119?format=json", b'{"next": "https://api.test/v3/law?API_KEY=abcdEFGH1234'
                b'5678&offset=20"}', content_type="application/json"),
        capture("https://api.test/v3/bill?format=json", compressed, content_type="application/json", encoding="gzip"),
        capture("https://api.test/v3/ok?format=json", b'{"url": "https://api.test/v3/law?api_key=<redacted>"}',
                content_type="application/json"),
    ])

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=CITING,
                   secrets={"env:DATA_GOV_API_KEY": KEY})

    hits = {hit["object"]: hit for hit in report["sections"]["evidence"]["credentials"]["objects_with_hits"]}
    assert len(hits) == 2
    assert sorted(hit["parameter_matches"] for hit in hits.values()) == [0, 1]
    assert [hit["configured_values"] for hit in hits.values() if hit["configured_values"]] == [["env:DATA_GOV_API_KEY"]]
    assert "credential-in-object" in codes(report, "fail")
    assert report["sections"]["evidence"]["credentials"]["by_encoding"]["gzip"] == 1
    serialized = json.dumps(report)
    assert KEY not in serialized and "abcdEFGH12345678" not in serialized


def test_a_key_in_a_table_cell_is_flagged_in_text_and_list_columns(tmp_path):
    table = rows(("1", f"see https://x.test/?api_key={'Z' * 40}"), ("2", "plain"), ("3", f"token {KEY}"))
    table = table.append_column("refs", pa.array([[], [f"https://x.test/?x-api-key={'Z' * 40}"], []],
                                                 pa.list_(pa.string())))
    table = table.append_column("n", pa.array([1, 2, 3], pa.int64()))
    store = Store()
    prior = publish(tmp_path, store, "prior", {"t.parquet": rows(("1", "old"), ("2", "plain"), ("3", "old"))
                                               .append_column("refs", pa.array([[], [], []], pa.list_(pa.string())))
                                               .append_column("n", pa.array([1, 2, 3], pa.int64()))})
    publish(tmp_path, store, "current", {"t.parquet": table}, prior=prior)

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=DECLARED,
                   secrets={"env:KEY": KEY})

    cells = report["sections"]["evidence"]["credentials"]["table_cells"]["t"]
    assert cells["columns_scanned"] == 3
    assert cells["rows_with_hits"] == {"v": {"parameter": 1, "env:KEY": 1}, "refs": {"parameter": 1}}
    # The changed-cell samples show these values; each is redacted before it is clipped, so no prefix survives.
    changed = report["sections"]["conservation"]["t"]["changed_cells_by_column"]
    assert set(changed) == {"v", "refs"}
    serialized = json.dumps(report)
    assert "Z" * 8 not in serialized and KEY[:8] not in serialized


def test_an_empty_output_is_reported_as_state_not_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("R2_ALLOW_SHRINK", "1")
    store = Store()
    prior = publish(tmp_path, store, "prior", {"t.parquet": rows(), "u.parquet": rows(("1", "a"))})
    publish(tmp_path, store, "current", {"t.parquet": rows(), "u.parquet": rows(("1", "a"))}, prior=prior)

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations={**DECLARED, "u": DECLARED["t"]})

    assert report["sections"]["state"]["t"] == {"rows": 0, "state": "empty"}
    assert report["summary"]["empty_tables"] == ["t"]
    assert report["dispositions"]["t"] == {
        "publication": "verified", "schema": "matches-declaration", "identity": "not-applicable-empty",
        "conservation": "bytes-equal", "state": "empty", "source_qualification": "not-assessed",
        "deployment": "not-assessed"}
    assert not report["findings"]
    assert any("cannot tell an intentionally uncomputed output" in limit for limit in report["limits"])


def test_changed_member_bytes_fail_admission_and_the_cli_exit(tmp_path, capsys):
    store = Store()
    index = publish(tmp_path, store, "current", {"t.parquet": rows(("1", "a"))})
    base = public_base(store, tmp_path / "public")
    member = Path(base.path(index["families"]["test"]["prefix"] + "/t.parquet"))
    pq.write_table(rows(("1", "b")), member)

    assert main(["--family", "test", "--base", base.location, "--prior", "none",
                 "--output", str(tmp_path / "report.json")]) == 1

    report = json.loads((tmp_path / "report.json").read_text())
    assert not report["sections"]["publication"]["admission"]["admitted"]
    assert "generation-not-admitted" in codes(report, "fail")
    assert report["dispositions"]["t"]["publication"] == "failed"
    assert f"at {base.location}: fail=" in capsys.readouterr().err


def test_a_frozen_index_without_its_base_refuses_before_any_read(tmp_path, capsys):
    """2026-09-26: a fork index audited against the defaulted upstream base failed all 57 checks."""
    index = tmp_path / "publication.json"
    index.write_text("{}")

    with pytest.raises(SystemExit) as refused:
        main(["--family", "test", "--index", str(index)])

    assert refused.value.code == 2
    assert "--index requires --base" in capsys.readouterr().err


def test_an_unreadable_root_leaves_evidence_unknown_not_absent(tmp_path):
    store = Store()
    index = publish(tmp_path, store, "current", {"t.parquet": rows(("1", "a"))})
    base = public_base(store, tmp_path / "public")
    Path(base.path(index["families"]["test"]["prefix"] + "/artifact.json")).unlink()

    report = audit(base, family="test", declarations=DECLARED, prior="none")

    assert report["sections"]["evidence"]["status"] == "unknown"
    assert any("root is unreadable" in limit for limit in report["limits"])
    assert not any("declares no source-evidence input" in limit for limit in report["limits"])


def test_sniff_sees_past_an_xml_prolog_and_the_scan_counts_a_split_match_once():
    assert sniff(HTML_200.read_bytes()[:4096]) == "html"
    assert sniff(USLM.read_bytes()[:4096]) == "xml"
    assert sniff(b'\xef\xbb\xbf<?xml version="1.0"?><!DOCTYPE html><html/>') == "html"
    assert sniff(b"  [1, 2]") == "json" and sniff(b"") == "empty"
    assert expected_kind(PLAW_URL, "text/html") == "xml"
    assert expected_kind("https://x.test/a?Format=JSON", None) == "json"
    assert expected_kind("https://x.test/page", "text/html") is None

    body = b"x" * 5000 + b"&api_key=ABCDEFGHIJKLMNOP&y=1 " + KEY.encode() + b" api_key=<redacted>"
    for size in (1, 7, 64, len(body)):
        scan = Scan({"k": KEY.encode()})
        for start in range(0, len(body), size):
            scan.update(body[start:start + size])
        scan.close()
        assert (scan.pattern_count, scan.configured) == (1, {"k"}), size
        assert scan.pattern_hits == [{"offset": 5001, "name": "api_key", "value_length": 16}]


def test_configured_secrets_take_every_named_env_file_value_but_urls_and_only_secret_named_environment(tmp_path):
    env = tmp_path / ".env"
    env.write_text(f"API_GOV={KEY}\nR2_PUBLIC_URL=https://pub-0123456789abcdef.r2.dev\nSHORT=abc\nCOPY={KEY}\n")
    found = configured_secrets([env], environ={"PATH": "/usr/local/bin:/usr/bin:/bin", "SAM_GOV": "s" * 40})
    assert found == {"env:SAM_GOV": "s" * 40, f"{env}:API_GOV": KEY}


def test_an_unreadable_prior_table_is_a_finding_and_the_report_is_still_written(tmp_path):
    store = Store()
    prior = publish(tmp_path, store, "prior", {"t.parquet": rows(("1", "a"))})
    publish(tmp_path, store, "current", {"t.parquet": rows(("1", "b"))}, prior=prior)
    base = public_base(store, tmp_path / "public")
    Path(base.path(prior["families"]["test"]["prefix"] + "/t.parquet")).unlink()

    report = audit(base, family="test", declarations=DECLARED)

    assert {"prior-not-admitted", "prior-table-unreadable"} <= codes(report, "fail")
    assert report["dispositions"]["t"]["conservation"] == "prior-unreadable"
    assert report["dispositions"]["t"]["publication"] == "verified"


def test_a_stage_expectation_judges_a_capture_whose_own_media_type_says_html(tmp_path):
    store = Store()
    evidence_generation(tmp_path, store, [
        capture("https://api.test/filings/?page=1", b'{"results": []}', content_type="application/json"),
        capture("https://api.test/filings/?page=2", b"<br />\n<b>Warning</b>: upstream", content_type="text/html"),
    ])

    report = audit(public_base(store, tmp_path / "public"), family="test", declarations=CITING)

    (item,) = report["sections"]["evidence"]["body_shapes"]["unexpected_2xx"]
    assert (item["requested_url"], item["expected"], item["expected_from"], item["observed"]) == (
        "https://api.test/filings/?page=2", "json", "stage", "html")


def test_a_second_gzip_member_is_decoded_and_scanned():
    body = gzip.compress(b"first ") + gzip.compress(b"second api_key=ABCDEFGH12345678 " + KEY.encode())
    scan = Scan({"k": KEY.encode()})
    for start in range(0, len(body), 5):
        scan.update(body[start:start + 5])
    scan.close()
    assert (scan.encoding, scan.pattern_count, scan.configured) == ("gzip", 1, {"k"})


def test_an_https_base_keeps_a_receipt_per_read_and_refuses_absence():
    def respond(request):
        if request.url.path == "/missing.json":
            return httpx.Response(404)
        return httpx.Response(200, content=b"" if request.method == "HEAD" else b"{}", headers={"etag": '"e1"'})

    base = PublicBase("https://data.test", client=httpx.Client(transport=httpx.MockTransport(respond)))
    assert base.read("a.json") == b"{}"
    with pytest.raises(MemberNotFoundError):
        base.read("missing.json")
    assert base.etag("a.json") == '"e1"'
    (receipt,) = base.receipts
    assert {k: receipt[k] for k in ("key", "status", "etag", "bytes", "complete")} == {
        "key": "a.json", "status": 200, "etag": '"e1"', "bytes": 2, "complete": True}
    assert receipt["sha256"] == "sha256:" + hashlib.sha256(b"{}").hexdigest()


def test_declared_identity_comes_from_the_contract_then_the_dictionary():
    declared = declared_tables()
    assert (declared["laws"].identity, declared["laws"].identity_source) == (
        ("congress", "law_type", "number"), "spicy-docs contract")
    assert (declared["dockets"].identity, declared["dockets"].identity_source) == (("docket_id",), "spicy-docs contract")
    assert (declared["fec_committees"].identity, declared["fec_committees"].identity_source) == (
        ("committee_id",), "data dictionary")
    assert declared["feed_summary"].identity == ()
