"""Complete tiny originals exercise independent source and receipt qualification."""

import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import exact_json
from spicy_regs.sources import uscode_editions as builder
from spicy_regs.sources import uscode_editions_audit as auditor


# Literal publisher fields: fixture expectations do not import reader grammars.
HEADER = (
    "AUTHORITIES-PUBLICATION-NAME",
    "AUTHORITIES-PUBLICATION-ID",
    "AUTHORITIES-PUBLICATION-YEAR",
    "AUTHORITIES-LAWS-ENACTED-THROUGH-DATE",
    "SEARCHABLE-LAWS-ENACTED-THROUGH-DATE",
    "AUTHORITIES-USC-TITLE-NAME",
    "AUTHORITIES-USC-TITLE-ENUM",
    "AUTHORITIES-USC-TITLE-STATUS",
    "CONVERSION-PROGRAM",
    "CONVERSION-DATETIME",
)


def title(native_year, enum, publication):
    values = (
        publication,
        "NATIVE-ID",
        str(native_year),
        "19950101",
        "January 1, 1995",
        f"TITLE {enum}",
        enum,
        "positive-law",
        f"fixture-{native_year}",
        "19950101",
    )
    header = "".join(f"<!-- {key}:{value} -->" for key, value in zip(HEADER, values, strict=True))
    body = (
        (
            "<!-- documentid:one usckey:K --><!-- itempath:/40/Sec. 1 -->"
            "<!-- field-start:sectionhead --><h3 class='native'>Heading</h3><!-- field-end:sectionhead -->"
            "<!-- field-start:statute -->A<!-- field-start:note -->N<!-- field-end:note -->B<!-- field-end:statute -->"
            "<!-- documentid:two usckey:K --><!-- itempath:/40/Sec. 1 -->"
            "<!-- documentid:three usckey:E --><!-- itempath:/40/Sec. 2 -->"
            "<!-- field-start:statute --><!-- field-end:statute -->"
            "<!-- documentid:four usckey:R --><!-- itempath:/40/[Secs. 3 to 4 -->"
            "<!-- field-start:statute -->INVALID<!-- field-end:statute -->"
        )
        .encode()
        .replace(b"INVALID", b"\xff")
    )
    return ("<!DOCTYPE html><html><head>" + header + "</head><body>").encode() + body + b"</body></html>"


def fixture(tmp_path, year=1995):
    members = [
        (f"{year}usc40a.htm", title(1994, "40a", "1994 Main Edition")),
        (f"{year}usc01.htm", title(year, "1", "1995 Supplement I" if year == 1995 else "1994 Main Edition")),
        ("usc.css", b"body{}"),
    ]
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members:
            archive.writestr(name, body)
    original = tmp_path / "original.zip"
    original.write_bytes(payload.getvalue())
    selection = tmp_path / "selection.json"
    selection.write_text(
        json.dumps(
            {
                "format": "spicy-uscode-edition-inputs",
                "version": 1,
                "sources": [{"kind": "annual", "year": year, "path": original.name, "sha256": auditor.sha(original)}],
            }
        )
    )
    candidate = tmp_path / "candidate"
    builder.build(selection, candidate)  # Production is allowed only in fixture setup.
    return candidate, selection, members


def snapshot(root):
    return {str(p.relative_to(root)): auditor.sha(p) for p in root.rglob("*") if p.is_file()}


def table_rows(candidate, name):
    return pq.read_table(candidate / name / f"{name}.parquet").to_pylist()


def change_subject(candidate, name, change):
    path = candidate / name / f"{name}.parquet"
    records = table_rows(candidate, name)
    change(records)
    pq.write_table(pa.Table.from_pylist(records, schema=builder.SCHEMAS[name]), path)
    rebuild_path = candidate / "rebuild.json"
    rebuild = json.loads(rebuild_path.read_text())
    rebuild["outputs"][name].update(sha256=auditor.sha(path), rows=len(records))
    rebuild_path.write_text(json.dumps(rebuild))


@pytest.mark.parametrize("year", [1994, 1995])
def test_complete_fixture_identity_title_routing_and_library_read_only(tmp_path, year):
    candidate, selection, members = fixture(tmp_path, year)
    before = snapshot(tmp_path)
    result = auditor.audit(candidate, selection, annual_year=year)
    assert result["verdict"] == "APPROVE"
    assert result["rows"] == {"uscode_editions": 3, "uscode_edition_structure": 8, "uscode_edition_sections": 8}
    assert snapshot(tmp_path) == before  # Library creates no report or scratch file.
    editions = table_rows(candidate, "uscode_editions")
    for index, (name, body) in enumerate(members, 1):
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        # Execute upstream identity construction rather than feeding an expected ID.
        expected = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    [("annual", year, None, None), auditor.sha(tmp_path / "original.zip"), index, name, digest],
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        )
        assert editions[index - 1]["edition_id"] == expected
    assert editions[0]["title"] == "40a"
    assert editions[0]["carried_forward"] == str(year != 1994).lower()
    assert editions[2]["member_role"] == "supporting-file" and editions[2]["metadata_json"] is None
    content = table_rows(candidate, "uscode_edition_sections")
    assert [r["body_text"] for r in content[:4]] == ["ANB", None, "", "�"]
    assert content[0]["native_identifier"] == content[1]["native_identifier"] == "K"
    assert content[0]["observation_id"] != content[1]["observation_id"]
    value = json.loads(content[0]["source_observation_json"])
    assert [(f["name"], f["depth"]) for f in value["fields"]] == [("sectionhead", 0), ("statute", 0), ("note", 1)]
    assert value["structure"]["heading"]["text"] == "Heading"
    assert "invalid_utf8" in json.loads(content[3]["source_observation_json"])["issues"]


def test_audit_never_calls_producer_or_acquisition_content_readers(tmp_path, monkeypatch):
    candidate, selection, _ = fixture(tmp_path)

    def poison(*args, **kwargs):
        raise AssertionError("maintained reader called by independent audit")

    monkeypatch.setattr(builder, "build", poison)
    monkeypatch.setattr(builder, "_code_pins", poison)
    for module, names in (
        (builder.archive, ("read_annual_archive", "read_corpus_archive", "read_title_archive")),
        (builder.annual, ("scan_uscode_annual_sections", "validate_annual_title_html")),
        (builder.section_bodies, ("scan_annual_section_bodies", "scan_xml_section_bodies")),
        (builder.structure, ("scan_uscode_structure",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, poison)
    assert auditor.audit(candidate, selection, annual_year=1995)["verdict"] == "APPROVE"


@pytest.mark.parametrize("change", ["body", "hierarchy", "position", "identity", "title", "missing", "extra"])
def test_corrupt_subject_even_with_updated_output_hash_refuses(tmp_path, change):
    candidate, selection, _ = fixture(tmp_path)
    table = "uscode_editions" if change == "title" else "uscode_edition_sections"

    def corrupt(records):
        if change == "missing":
            records.pop()
        elif change == "extra":
            records.append(records[-1].copy())
        elif change == "hierarchy":
            observed = json.loads(records[0]["source_observation_json"])
            observed["structure"]["itempath"] = "/wrong/Sec. 1"
            records[0]["source_observation_json"] = json.dumps(observed)
        else:
            key = {"body": "body_text", "position": "source_position", "identity": "observation_id", "title": "title"}[
                change
            ]
            records[0][key] = "corrupt"

    change_subject(candidate, table, corrupt)
    with pytest.raises((AssertionError, StopIteration)):
        auditor.audit(candidate, selection, annual_year=1995)


@pytest.mark.parametrize("change", ["shard", "witness", "generation", "version", "extra", "schema"])
def test_corrupt_receipts_refuse(tmp_path, change):
    candidate, selection, _ = fixture(tmp_path)
    path = candidate / "etl_receipts.parquet"
    records = pq.read_table(path).to_pylist()
    if change == "schema":
        pq.write_table(pa.table({"wrong": ["x"]}), path)
    elif change == "shard":
        shard = candidate / "uscode_editions/etl_receipts.parquet"
        values = pq.read_table(shard).to_pylist()
        values[0]["attempt_id"] = "changed"
        pq.write_table(pa.Table.from_pylist(values, schema=auditor.RECEIPT_SCHEMA), shard)
    else:
        if change == "extra":
            records.append(records[-1])
        elif change == "witness":
            records[0]["witnesses"][1]["locator"] = "wrong.htm"
        else:
            records[0][{"generation": "generation_id", "version": "subject_version"}[change]] = "sha256:" + "f" * 64
        pq.write_table(pa.Table.from_pylist(records, schema=auditor.RECEIPT_SCHEMA), path)
        # Match shards too so witness/protocol checks, not only duplication, run.
        if change != "extra":
            shard = candidate / "uscode_editions/etl_receipts.parquet"
            pq.write_table(pa.Table.from_pylist(records[:3], schema=auditor.RECEIPT_SCHEMA), shard)
    with pytest.raises((AssertionError, ValueError, StopIteration)):
        auditor.audit(candidate, selection, annual_year=1995)


@pytest.mark.parametrize("year", [1994, 1996])
def test_wrong_expected_or_outside_grant_year_refuses(tmp_path, year):
    candidate, selection, _ = fixture(tmp_path)
    with pytest.raises(AssertionError):
        auditor.audit(candidate, selection, annual_year=year)


@pytest.mark.parametrize(
    "value",
    [
        {"notes": "RESERVATION:\r\nPublisher text"},
        json.dumps({"notes": "RESERVATION:\r\nPublisher text"}),
    ],
)
def test_publisher_crlf_json_accepts(value):
    auditor.public(value, "fixture")


@pytest.mark.parametrize(
    "value",
    [
        {"path": "hidden"},
        {"note": "/Users/private/source"},
        {"note": "C:\\private\\source"},
        json.dumps({"secret": "hidden"}),
        json.dumps({"notes": "/home/private/source"}),
        '{"bad":"/tmp/source"',
        {"url": "https://example.test/?api_key=hidden"},
    ],
)
def test_public_private_locator_or_credential_refuses(value):
    with pytest.raises(AssertionError):
        auditor.public(value, "fixture")


def test_native_crossed_field_markers_refuse():
    with pytest.raises(AssertionError, match="crossed"):
        auditor.native_fields(b"<!-- field-start:statute --><!-- field-end:notes -->", 0, 60)


def test_cli_success_failure_and_no_overwrite(tmp_path):
    candidate, selection, _ = fixture(tmp_path)
    report = tmp_path / "qualification.json"
    args = [str(candidate), "--selection", str(selection), "--annual-year", "1995", "--output", str(report)]
    assert auditor.main(args) == 0
    saved = report.read_bytes()
    assert json.loads(saved)["verdict"] == "APPROVE"
    assert auditor.main(args) == 1 and report.read_bytes() == saved
    failure = tmp_path / "failure.json"
    args[-1] = str(failure)
    args[4] = "1994"
    assert auditor.main(args) == 1
    assert json.loads(failure.read_text())["verdict"] == "REQUEST CHANGES"
    saved = failure.read_bytes()
    assert auditor.main(args) == 1 and failure.read_bytes() == saved


@pytest.mark.parametrize("target", ["inside", "selection", "original", "hardlink", "symlink", "dangling"])
def test_cli_report_alias_refuses_without_changing_evidence(tmp_path, target):
    candidate, selection, _ = fixture(tmp_path)
    report = {"inside": candidate / "new.json", "selection": selection, "original": tmp_path / "original.zip"}.get(
        target
    )
    if report is None:
        report = tmp_path / "alias.json"
        if target == "hardlink":
            report.hardlink_to(selection)
        else:
            report.symlink_to(selection if target == "symlink" else tmp_path / "missing.json")
    before = snapshot(tmp_path)
    assert (
        auditor.main([str(candidate), "--selection", str(selection), "--annual-year", "1995", "--output", str(report)])
        == 1
    )
    assert snapshot(tmp_path) == before
    if target == "dangling":
        assert report.is_symlink() and not (tmp_path / "missing.json").exists()


def test_module_entry_exposes_required_arguments():
    result = subprocess.run(
        [sys.executable, "-B", "-m", "spicy_regs.sources.uscode_editions_audit", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0 and "--annual-year" in result.stdout and "--selection" in result.stdout


def test_consistently_relabelled_generation_with_valid_receipts_refuses(tmp_path):
    candidate, selection, _ = fixture(tmp_path)
    generation = "sha256:" + "f" * 64
    for path in (
        candidate / "etl_receipts.parquet",
        *(candidate / name / "etl_receipts.parquet" for name in builder.SCHEMAS),
    ):
        records = pq.read_table(path).to_pylist()
        for record in records:
            record["generation_id"] = generation
            record["receipt_id"] = (
                "sha256:"
                + hashlib.sha256(
                    exact_json({key: value for key, value in record.items() if key != "receipt_id"}).encode()
                ).hexdigest()
            )
            auditor.validate_receipt_row(record, auditor.POLICIES)
        pq.write_table(pa.Table.from_pylist(records, schema=auditor.RECEIPT_SCHEMA), path)
    rebuild_path = candidate / "rebuild.json"
    rebuild = json.loads(rebuild_path.read_text())
    rebuild["generation"] = generation
    rebuild_path.write_text(json.dumps(rebuild))
    with pytest.raises(AssertionError, match="generation differs"):
        auditor.audit(candidate, selection, annual_year=1995)


def test_symlink_selection_resolves_relative_source_before_report_reservation(tmp_path):
    candidate, selection, _ = fixture(tmp_path)
    other = tmp_path / "links"
    other.mkdir()
    linked = other / "selection.json"
    linked.symlink_to(selection)
    original = tmp_path / "original.zip"
    original.unlink()
    before = snapshot(tmp_path)
    assert (
        auditor.main([str(candidate), "--selection", str(linked), "--annual-year", "1995", "--output", str(original)])
        == 1
    )
    assert not original.exists() and snapshot(tmp_path) == before


@pytest.mark.parametrize("mode", ["multiple", "xml", "source-hash", "manifest-format"])
def test_selection_scope_and_original_pin_refuse(tmp_path, mode):
    candidate, selection, _ = fixture(tmp_path)
    selected = json.loads(selection.read_text())
    if mode == "multiple":
        selected["sources"].append(selected["sources"][0].copy())
    elif mode == "xml":
        selected["sources"][0]["kind"] = "release-point-title"
    elif mode == "source-hash":
        selected["sources"][0]["sha256"] = "sha256:" + "f" * 64
    else:
        selected["format"] = "unsupported"
    selection.write_text(json.dumps(selected))
    (candidate / "selection.json").write_bytes(selection.read_bytes())
    rebuild_path = candidate / "rebuild.json"
    rebuild = json.loads(rebuild_path.read_text())
    rebuild["manifest_sha256"] = auditor.sha(selection)
    rebuild_path.write_text(json.dumps(rebuild))
    with pytest.raises(AssertionError):
        auditor.audit(candidate, selection, annual_year=1995)


def test_recompression_between_initial_output_hash_and_snapshot_refuses(tmp_path, monkeypatch):
    candidate, selection, _ = fixture(tmp_path)
    target = candidate / "uscode_edition_sections/uscode_edition_sections.parquet"
    original_sha = auditor.sha
    expected = original_sha(target)
    calls = 0

    def changing_sha(path):
        nonlocal calls
        if Path(path) == target:
            calls += 1
            if calls == 2:
                records = pq.read_table(target)
                pq.write_table(records, target, compression="NONE")
                assert original_sha(target) != expected
        return original_sha(path)

    monkeypatch.setattr(auditor, "sha", changing_sha)
    with pytest.raises(AssertionError, match="final output digest"):
        auditor.audit(candidate, selection, annual_year=1995)
    assert calls >= 3
