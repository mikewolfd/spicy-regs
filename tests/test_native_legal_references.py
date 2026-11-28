"""Native XML generation and fail-closed correction, without network acquisition."""

import hashlib
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.transforms.native_legal_references import OUTPUTS, build_native_legal_references

FIXTURES = Path(__file__).parent / "fixtures/native_legal_references"


def digest(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


def run(manifest, directory, prior=None):
    directory.mkdir(parents=True, exist_ok=True)
    evidence = CaptureEvidence(directory, "native-legal-references")

    def download(key, destination):
        if prior is None:
            return False
        shutil.copyfile(prior / key, destination)
        return True

    files = build_native_legal_references(manifest, directory, evidence=evidence, download_prior=download)
    return files, evidence


def test_retained_native_occurrences_and_generation(tmp_path):
    files, evidence = run(FIXTURES / "manifest.json", tmp_path)
    rows = pq.read_table(files[0]).to_pylist()
    reads = pq.read_table(files[1]).to_pylist()
    assert len(rows) == 33 and sorted(int(r["occurrence_count"]) for r in reads) == [2, 31]
    assert all(r["edition"] is None and r["source_path"] for r in rows)
    assert len({(r["scope_id"], r["occurrence_index"]) for r in rows}) == len(rows)
    note = next(r for r in rows if r["observation_kind"] == "authority")
    assert note["cfr_title"] is None and note["cfr_part"] == "18"
    assert "E.O. 10530" in note["text"]
    assert note["interpretation_status"] == "partial_text_findings"
    reference = next(r for r in rows if r["href"] == "/us/usc/t5/s401")
    [target] = json.loads(reference["target_candidates_json"])
    assert target["target_kind"] == "usc_section" and target["normalized_key"] == "5-401"
    assert target["target_status"] == "not_checked"  # no selected target bytes
    from spicy_regs import legislative_tables
    from spicy_regs.legislative_receipts import FILE_POLICY, policy, write_legislative_outputs
    from spicy_regs.native_types import described_schema
    from spicy_regs.navigation_read_outcomes import FILE_TABLES
    from spicy_regs.pipelines.rollups.native_legal_references import NativeLegalReferencesRollup

    # Receipts are retired for native-legal-references: a generation declaring receipts is refused.
    bundle = tmp_path / "receipt-bundle"
    write_legislative_outputs(files, bundle, generation_id="native-test")
    with pytest.raises(ValueError, match="Receipts are retired"):
        build_generation(
            tmp_path / "refused-receipts",
            family="native-legal-references",
            files=[bundle / "native_legal_references.parquet", bundle / "native_legal_reference_reads.parquet"],
            expected_keys=list(OUTPUTS),
            schemas={name: described_schema(policy(name).subject_schema) for name in (
                "native_legal_references", "native_legal_reference_reads")},
            inputs=evidence.inputs(),
            receipt_path=bundle / "etl_receipts.parquet",
            receipt_policies=[policy("native_legal_references"), policy("native_legal_reference_reads"), FILE_POLICY],
            receipt_generation_id="native-test",
        )

    # Receiptless native conversion & admission succeeds without etl_receipts.parquet.
    plain = tmp_path / "plain-bundle"
    plain.mkdir()
    mapped = {p.stem: legislative_tables.to_public(p.stem, p, plain / p.name) for p in files}
    file_table = FILE_TABLES["native-legal-references"]
    outcomes = legislative_tables.write_file_outcomes(
        file_table, [(p.stem, p) for p in files], mapped, plain, plain / f"{file_table}.parquet",
        generation_id="native-test",
    )
    artifact = build_generation(
        tmp_path / "candidate",
        family="native-legal-references",
        files=[plain / "native_legal_references.parquet", plain / "native_legal_reference_reads.parquet", outcomes],
        expected_keys=list(NativeLegalReferencesRollup.outputs),
        schemas=NativeLegalReferencesRollup(manifest=FIXTURES / "manifest.json").generation_schemas(),
        inputs=evidence.inputs(),
    )
    assert "etlReceipts" not in artifact.root["spec"]
    verify_generation(tmp_path / "candidate", expected_pin=artifact.pin)
    verify_evidence(evidence.artifact_dir, expected_pin=evidence.artifact.pin)


def manifest_for(directory, body, *, family="uscode", key="/us/usc/t5/s423", targets=None):
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "input.xml"
    source.write_bytes(body)
    spec = {
        "sources": [
            {
                "path": "input.xml",
                "sha256": digest(body),
                "source_family": family,
                "source_record_key": key,
                "edition": None,
                "source_locator": "synthetic:control",
            }
        ]
    }
    if targets is not None:
        spec["targets"] = targets
    path = directory / "manifest.json"
    path.write_text(json.dumps(spec))
    return path


def test_empty_correction_replaces_digest_scope_and_preserves_other_records(tmp_path):
    first, _ = run(FIXTURES / "manifest.json", tmp_path / "first")
    empty = manifest_for(tmp_path / "empty", b"<section/>")
    second, _ = run(empty, tmp_path / "second", prior=first[0].parent)
    rows = pq.read_table(second[0]).to_pylist()
    assert len(rows) == 2 and all(r["source_family"] == "ecfr" for r in rows)
    reads = pq.read_table(second[1]).to_pylist()
    assert sorted(r["occurrence_count"] for r in reads) == ["0", "2"]
    assert next(r for r in reads if r["source_family"] == "uscode")["input_sha256"] == digest(b"<section/>")


@pytest.mark.parametrize("failure", ["malformed", "hash", "duplicate", "oversize"])
def test_failed_or_incomplete_reads_do_not_replace_prior(tmp_path, failure):
    files, _ = run(FIXTURES / "manifest.json", tmp_path / "prior")
    before = [p.read_bytes() for p in files]
    raw = b'<section><ref href="/us/usc/t5/s401"/>' if failure == "malformed" else b"<section/>"
    manifest = manifest_for(tmp_path / "input", raw)
    spec = json.loads(manifest.read_text())
    if failure == "hash":
        spec["sources"][0]["sha256"] = "sha256:" + "0" * 64
    if failure == "duplicate":
        spec["sources"] *= 2
    if failure == "oversize":
        (manifest.parent / "input.xml").write_bytes(b" " * (16 * 1024 * 1024 + 1))
    manifest.write_text(json.dumps(spec))
    with pytest.raises(Exception):
        run(manifest, tmp_path / "failed", prior=files[0].parent)
    assert before == [p.read_bytes() for p in files]
    assert not list((tmp_path / "failed").glob("*.parquet"))


def test_pinned_release_point_lookup_range_stub_cross_title_and_unknown_href(tmp_path):
    target = tmp_path / "release_point_sections.parquet"
    pq.write_table(
        pa.table({"usc_title": ["5", "25"], "usc_section_key": ["401", "2601...2606"]}),
        target,
    )
    manifest = manifest_for(
        tmp_path,
        b'<section><ref href="/us/usc/t5/s401"/><ref href="/us/usc/t25/s2603"/>'
        b'<ref href="/us/usc/t1/s2603"/><ref href="/us/usc/t2/s184a"/>'
        b'<ref href="opaque:other"/><ref href="/us/usc/t5/s401#x"/></section>',
        targets={"usc_release_point_sections": {"path": target.name, "sha256": digest(target.read_bytes())}},
    )
    files, _ = run(manifest, tmp_path / "output")
    rows = sorted(pq.read_table(files[0]).to_pylist(), key=lambda r: int(r["occurrence_index"]))
    found = json.loads(rows[0]["target_candidates_json"])[0]
    assert found["resolution_rule"] == "selected-target-lookup/2"
    assert found["target_table_selected"] == "usc_release_point_sections"
    assert found["target_status"] == "found" and found["expected_cardinality"] == "many"
    assert found["match_basis"] == "printed_section"
    assert found["candidate_keys"] == [{"usc_title": "5", "usc_section_key": "401"}]
    assert "release point" in found["target_grain"] and found["target_snapshot"]["sha256"] == digest(
        target.read_bytes()
    )
    assert "disposition" not in found
    stub = json.loads(rows[1]["target_candidates_json"])[0]
    assert stub["target_status"] == "found" and stub["match_basis"] == "range_stub"
    assert stub["candidate_keys"] == [{"usc_title": "25", "usc_section_key": "2601...2606"}]
    # Cross-title range negative control: 1-2603 must NOT match Title 25's 2601...2606 stub.
    cross_title = json.loads(rows[2]["target_candidates_json"])[0]
    assert cross_title["target_status"] == "missing" and cross_title["match_count"] == 0
    assert "disposition" not in cross_title
    # Letter-suffix negative control: 2-184a is never auto-repaired.
    letter_suffix = json.loads(rows[3]["target_candidates_json"])[0]
    assert letter_suffix["target_status"] == "missing" and letter_suffix["match_count"] == 0
    assert [r["interpretation_status"] for r in rows[4:]] == ["unsupported_href", "unsupported_href"]


def test_pinned_uscode_edition_sections_target_and_legacy_or_conflicting_target_refusals(tmp_path):
    editions_dir = tmp_path / "editions"
    editions_dir.mkdir()
    editions = editions_dir / "uscode_edition_sections.parquet"
    pq.write_table(
        pa.table({
            "observation_id": ["o1"],
            "edition_id": ["rp-119-102"],
            "source_kind": ["release-point-corpus"],
            "source_context": ["main-body-candidate"],
            "native_identifier": ["/us/usc/t5/s401"],
        }),
        editions,
    )
    manifest = manifest_for(
        editions_dir,
        b'<section><ref href="/us/usc/t5/s401"/></section>',
        targets={"uscode_edition_sections": {"path": editions.name, "sha256": digest(editions.read_bytes())}},
    )
    files, _ = run(manifest, tmp_path / "editions_out")
    [row] = pq.read_table(files[0]).to_pylist()
    [found] = json.loads(row["target_candidates_json"])
    assert found["resolution_rule"] == "selected-target-lookup/2"
    assert found["target_table_selected"] == "usc_release_point_sections"
    assert found["target_status"] == "found" and found["match_basis"] == "printed_section"
    assert found["candidate_keys"] == [{"usc_title": "5", "usc_section_key": "401"}]
    assert found["target_snapshot"]["sha256"] == digest(editions.read_bytes())

    # Legacy /1 law_code_sections target fails closed under selected-target-lookup/2.
    legacy_dir = tmp_path / "legacy_refused"
    legacy_dir.mkdir()
    legacy_target = legacy_dir / "law_code_sections.parquet"
    pq.write_table(
        pa.table({"usc_title": ["5"], "usc_section_key": ["401"], "congress": ["117"], "session": ["2"], "seq": ["1"]}),
        legacy_target,
    )
    bad_legacy = manifest_for(
        legacy_dir,
        b'<section><ref href="/us/usc/t5/s401"/></section>',
        targets={"law_code_sections": {"path": legacy_target.name, "sha256": digest(legacy_target.read_bytes())}},
    )
    with pytest.raises(ValueError, match="unsupported target table"):
        run(bad_legacy, tmp_path / "legacy_out")

    # Supplying both usc_release_point_sections and uscode_edition_sections refuses explicitly.
    conflict_dir = tmp_path / "conflict"
    conflict_dir.mkdir()
    rp_target = conflict_dir / "rp.parquet"
    pq.write_table(pa.table({"usc_title": ["5"], "usc_section_key": ["401"]}), rp_target)
    editions_copy = conflict_dir / "uscode_edition_sections.parquet"
    shutil.copyfile(editions, editions_copy)
    bad_conflict = manifest_for(
        conflict_dir,
        b'<section><ref href="/us/usc/t5/s401"/></section>',
        targets={
            "usc_release_point_sections": {"path": rp_target.name, "sha256": digest(rp_target.read_bytes())},
            "uscode_edition_sections": {"path": editions_copy.name, "sha256": digest(editions_copy.read_bytes())},
        },
    )
    with pytest.raises(ValueError, match="conflicting U.S. Code target tables"):
        run(bad_conflict, tmp_path / "conflict_out")


def test_part_only_cfr_and_unsupported_source_shapes_are_not_sections(tmp_path):
    manifest = manifest_for(
        tmp_path,
        b'<DIV5 TYPE="PART" N="18"><AUTH>3 CFR part 18</AUTH><PARAUTH>ignored form</PARAUTH></DIV5>',
        family="ecfr",
    )
    files, _ = run(manifest, tmp_path / "output")
    [row] = pq.read_table(files[0]).to_pylist()
    targets = json.loads(row["target_candidates_json"])
    assert targets and all(t["target_kind"] == "cfr_part" and t["target_status"] == "unsupported" for t in targets)
    assert "PARAUTH" in json.loads(pq.read_table(files[1]).to_pylist()[0]["unsupported_shapes_json"])


def test_same_source_rechecks_new_target_selection_without_stacking(tmp_path):
    target = tmp_path / "targets.parquet"
    fields = {"usc_title": ["5"], "usc_section_key": ["999"]}
    pq.write_table(pa.table(fields), target)
    body = b'<section><ref href="/us/usc/t5/s401"/><ref href="/us/usc/t5/s401"/></section>'

    def manifest():
        return manifest_for(
            tmp_path, body, targets={"usc_release_point_sections": {"path": target.name, "sha256": digest(target.read_bytes())}}
        )

    files, _ = run(manifest(), tmp_path / "first")
    before = pq.read_table(files[0]).to_pylist()
    assert all(json.loads(row["target_candidates_json"])[0]["target_status"] == "missing" for row in before)
    fields["usc_section_key"] = ["401"]
    pq.write_table(pa.table(fields), target)
    files, evidence = run(manifest(), tmp_path / "second", prior=tmp_path / "first")
    after = pq.read_table(files[0]).to_pylist()
    assert len(after) == 2 and {r["input_sha256"] for r in after} == {digest(body)}
    assert all(json.loads(row["target_candidates_json"])[0]["target_status"] == "found" for row in after)
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    event = next(event for event in events if event["event"] == "native-reference-resolution")
    assert event["distinct_target_keys_read"] == 1 and event["input_occurrences"] == 2


def test_unknown_xml_namespace_preserved_without_claimed_native_target(tmp_path):
    manifest = manifest_for(tmp_path, b'<section xmlns:x="urn:unknown"><x:ref href="/us/usc/t5/s401"/></section>')
    files, _ = run(manifest, tmp_path / "output")
    [row] = pq.read_table(files[0]).to_pylist()
    assert row["element_tag"] == "{urn:unknown}ref" and row["interpretation_status"] == "unsupported_href"


def test_repeated_local_replay_uses_latest_outputs_as_prior(tmp_path):
    files, _ = run(FIXTURES / "manifest.json", tmp_path / "output")
    empty = manifest_for(tmp_path / "empty", b"<section/>")
    files, _ = run(empty, tmp_path / "output")
    assert pq.read_table(files[0]).num_rows == 2
    assert pq.read_table(files[1]).num_rows == 2


def test_rows_are_spicy_docs_rows_under_its_rule_and_spelling(tmp_path):
    """The shapers key each row, the reading completes it under ``/003``, and ``json_column`` spells its candidates.

    A source credit citing ``Pub. L. 104–199`` keeps the en dash in the JSON it decodes to, spelled ``\\u2013``: the
    host no longer re-serializes the candidates (its own spelling kept non-ASCII literal, the 14 live values 0.52.0
    re-spelled). Every row and the read name ``NATIVE_LEGAL_REFERENCE_RULE``, and share the shapers' scope.
    """
    from spicy_docs.schemas.native_reference_rows import NATIVE_LEGAL_REFERENCE_RULE, native_reference_scope_id

    body = (
        '<section><ref href="/us/usc/t5/s401"/>'
        "<sourceCredit>(Pub. L. 104–199, § 3, Sept. 21, 1996, 110 Stat. 2419.)</sourceCredit></section>"
    ).encode()
    files, _ = run(manifest_for(tmp_path, body), tmp_path / "output")
    rows = sorted(pq.read_table(files[0]).to_pylist(), key=lambda r: int(r["occurrence_index"]))
    [read] = pq.read_table(files[1]).to_pylist()
    scope = native_reference_scope_id("uscode", "/us/usc/t5/s423", None)
    assert {r["rule_version"] for r in rows} == {read["rule_version"]} == {NATIVE_LEGAL_REFERENCE_RULE}
    assert {r["scope_id"] for r in rows} == {read["scope_id"]} == {scope}
    credit = next(r for r in rows if r["observation_kind"] == "source_credit")
    assert credit["target_candidates_json"].isascii() and "\\u2013" in credit["target_candidates_json"]
    assert any("104–199" in c["matched_text"] for c in json.loads(credit["target_candidates_json"]))


def test_bill_and_law_scopes_replay_correct_and_fail_closed(tmp_path):
    bill = b'<bill><external-xref legal-doc="usc" parsable-cite="usc/10/2802"/></bill>'
    first, _ = run(manifest_for(tmp_path / "bill", bill, family="bill", key="BILLS-119hr1ih"), tmp_path / "first")
    [row] = pq.read_table(first[0]).to_pylist()
    assert row["source_family"] == "bill" and row["href"] is None
    assert json.loads(row["target_candidates_json"])[0]["normalized_key"] == "10-2802"
    law = b'<pLaw xmlns="http://schemas.gpo.gov/xml/uslm"><ref href="/us/pl/119/1"/></pLaw>'
    second, _ = run(
        manifest_for(tmp_path / "law", law, family="law", key="119-public-2"),
        tmp_path / "second",
        prior=first[0].parent,
    )
    rows = pq.read_table(second[0]).to_pylist()
    assert {r["source_family"] for r in rows} == {"bill", "law"}
    empty = manifest_for(tmp_path / "empty-law", b"<pLaw/>", family="law", key="119-public-2")
    third, _ = run(empty, tmp_path / "third", prior=second[0].parent)
    assert [r["source_family"] for r in pq.read_table(third[0]).to_pylist()] == ["bill"]
    assert (
        next(r for r in pq.read_table(third[1]).to_pylist() if r["source_family"] == "law")["occurrence_count"] == "0"
    )
    bad = manifest_for(
        tmp_path / "bad-law", b'<pLaw><ref href="/us/pl/119/1"/></broken>', family="law", key="119-public-2"
    )
    with pytest.raises(ValueError):
        run(bad, tmp_path / "failed", prior=third[0].parent)
    assert not (tmp_path / "failed/native_legal_references.parquet").exists()


def test_wave1_declared_cohort_manifest_respects_bounds_and_publish_hygiene():
    from spicy_docs.schemas.native_reference_rows import NATIVE_LEGAL_REFERENCE_RULE, native_reference_scope_id
    from spicy_regs.citation_resolution import NATIVE_RESOLUTION_RULE
    from spicy_regs.source_evidence import _LOCAL_PATH_BYTES

    assert NATIVE_RESOLUTION_RULE == "selected-target-lookup/2"
    assert NATIVE_LEGAL_REFERENCE_RULE == "native-legal-reference/004"

    manifest_path = Path(__file__).resolve().parents[1] / "manifests/native_legal_wave1.json"
    raw = manifest_path.read_bytes()
    assert len(raw) <= 1024 * 1024
    assert _LOCAL_PATH_BYTES.search(raw) is None, "published manifest must not embed local machine paths"

    manifest = json.loads(raw)
    sources = manifest["sources"]
    assert 1 <= len(sources) <= 75
    total_bytes = sum(entry["bytes"] for entry in sources)
    assert total_bytes <= 32 * 1024 * 1024
    assert manifest["bounds"]["measured_inputs"] == len(sources)
    assert manifest["bounds"]["measured_total_xml_bytes"] == total_bytes
    assert manifest["bounds"]["measured_observations"] <= 8_000

    scopes = set()
    families = {"bill": 0, "law": 0}
    for entry in sources:
        family = entry["source_family"]
        assert family in families
        families[family] += 1
        assert entry["sha256"].startswith("sha256:") and len(entry["sha256"]) == 71
        assert 0 < entry["bytes"] <= 16 * 1024 * 1024
        assert entry["source_locator"].startswith("https://www.govinfo.gov/")
        scope = native_reference_scope_id(family, entry["source_record_key"], entry["edition"])
        assert scope not in scopes
        scopes.add(scope)
        qual = entry["qualification"]
        if family == "bill":
            assert qual["kind"] == "bill-dtd-xml" and qual["congress"] == 119
            assert entry["edition"] and entry["source_record_key"] == qual["package_id"]
        else:
            assert qual["kind"] == "law-native-xml" and qual["congress"] == 119
            assert entry["edition"] is None
            assert entry["source_record_key"] == f"{qual['congress']}-{qual['law_kind']}-{qual['number']}"
    assert families["bill"] >= 15 and families["law"] >= 15


def test_statute_replay_emits_verified_spans_and_merges_over_legacy_xpath_only_prior(tmp_path):
    from spicy_docs.reading.xml_text import TEXT_OFFSET_UNIT, USLM_RENDITION, read_xml_text_index
    from spicy_docs.schemas.native_reference_rows import (
        NATIVE_LEGAL_REFERENCE_READS,
        NATIVE_LEGAL_REFERENCE_RULE,
        NATIVE_LEGAL_REFERENCES,
    )
    from spicy_regs.legislative_receipts import policy

    # 1. Stage a legacy XPath-only (/003) prior lacking the 6/3 appended character-offset span columns.
    prior_dir = tmp_path / "legacy-prior"
    prior_dir.mkdir()
    legacy_ref_cols = [
        c
        for c in NATIVE_LEGAL_REFERENCES.columns
        if c not in {"text_rendition", "text_offset_unit", "text_rendition_sha256", "text_span_start", "text_span_end", "span_text"}
    ]
    legacy_read_cols = [
        c
        for c in NATIVE_LEGAL_REFERENCE_READS.columns
        if c not in {"text_rendition", "text_offset_unit", "text_rendition_sha256"}
    ]
    legacy_scope = "sha256:" + "1" * 64
    legacy_input = "sha256:" + "2" * 64
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "scope_id": legacy_scope,
                    "source_family": "uscode",
                    "source_record_key": "/us/usc/t1",
                    "edition": "119-103",
                    "input_sha256": legacy_input,
                    "source_locator": "retained:legacy",
                    "occurrence_index": "0",
                    "source_path": "/*[1]/*[2]/*[1]",
                    "element_tag": "{http://xml.house.gov/schemas/uslm/1.0}ref",
                    "attributes_json": '{"href":"/us/usc/t1/s1"}',
                    "ancestors_json": "[]",
                    "observation_kind": "native_reference",
                    "href": "/us/usc/t1/s1",
                    "text": None,
                    "text_runs_json": None,
                    "cfr_title": None,
                    "cfr_part": None,
                    "interpretation_status": "native_section_href",
                    "target_candidates_json": "[]",
                    "rule_version": "native-legal-reference/003",
                }
            ],
            schema=pa.schema([(c, pa.string()) for c in legacy_ref_cols]),
        ),
        prior_dir / "native_legal_references.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "scope_id": legacy_scope,
                    "source_family": "uscode",
                    "source_record_key": "/us/usc/t1",
                    "edition": "119-103",
                    "input_sha256": legacy_input,
                    "source_locator": "retained:legacy",
                    "source_bytes": "123",
                    "occurrence_count": "1",
                    "read_status": "complete_selected_shapes",
                    "selected_shapes_json": '["href","sourceCredit"]',
                    "unsupported_shapes_json": "[]",
                    "manifest_sha256": "sha256:" + "3" * 64,
                    "rule_version": "native-legal-reference/003",
                }
            ],
            schema=pa.schema([(c, pa.string()) for c in legacy_read_cols]),
        ),
        prior_dir / "native_legal_reference_reads.parquet",
    )

    # 2. Replay real STATUTE-137 excerpt with native volume qualification and a pinned target table.
    statute_body = (FIXTURES / "statute-137-excerpt.xml").read_bytes()
    statute_dir = tmp_path / "statute-input"
    statute_dir.mkdir()
    (statute_dir / "STATUTE-137.xml").write_bytes(statute_body)
    target = statute_dir / "usc_release_point_sections.parquet"
    pq.write_table(
        pa.table({"usc_title": ["50"], "usc_section_key": ["1622"]}),
        target,
    )
    manifest_path = statute_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "path": "STATUTE-137.xml",
                        "sha256": digest(statute_body),
                        "source_family": "statute",
                        "source_record_key": "STATUTE-137",
                        "edition": None,
                        "source_locator": "https://www.govinfo.gov/bulkdata/STATUTE/137/STATUTE-137.xml",
                        "qualification": {"kind": "statute-native-xml", "volume": 137},
                    }
                ],
                "targets": {
                    "usc_release_point_sections": {"path": target.name, "sha256": digest(target.read_bytes())},
                },
            }
        )
    )

    files, evidence = run(manifest_path, tmp_path / "out", prior=prior_dir)
    rows = pq.read_table(files[0]).to_pylist()
    reads = pq.read_table(files[1]).to_pylist()
    assert len(reads) == 2
    legacy_rows = [r for r in rows if r["scope_id"] == legacy_scope]
    statute_rows = [r for r in rows if r["source_family"] == "statute"]
    assert len(legacy_rows) == 1 and len(statute_rows) == 28

    # Untouched legacy XPath-only prior survives with intact XPath and NULL span columns.
    assert legacy_rows[0]["source_path"] == "/*[1]/*[2]/*[1]"
    assert legacy_rows[0]["rule_version"] == "native-legal-reference/003"
    assert legacy_rows[0]["text_rendition"] is None and legacy_rows[0]["text_span_start"] is None

    # Every fresh STATUTE row slices the exact cited text in uslm-block-boundaries/1 beside source_path.
    text_index = read_xml_text_index(statute_body, source_family="statute")
    statute_read = next(r for r in reads if r["source_family"] == "statute")
    assert statute_read["rule_version"] == NATIVE_LEGAL_REFERENCE_RULE
    assert statute_read["text_rendition"] == USLM_RENDITION
    assert statute_read["text_offset_unit"] == TEXT_OFFSET_UNIT
    assert statute_read["text_rendition_sha256"] == text_index.rendition_sha256
    assert statute_read["occurrence_count"] == "28"

    for row in statute_rows:
        assert row["source_path"].startswith("/*[1]")
        assert row["rule_version"] == NATIVE_LEGAL_REFERENCE_RULE
        assert row["text_rendition"] == USLM_RENDITION
        assert row["text_offset_unit"] == TEXT_OFFSET_UNIT
        assert row["text_rendition_sha256"] == text_index.rendition_sha256
        s, e = int(row["text_span_start"]), int(row["text_span_end"])
        assert text_index.text[s:e] == row["span_text"]
        for cand in json.loads(row["target_candidates_json"]):
            assert (cand["rendition_span_start"], cand["rendition_span_end"]) == (s, e)
            assert text_index.text[cand["rendition_span_start"] : cand["rendition_span_end"]] == row["span_text"]

    # Repeated identical citations retain distinct XPaths and strictly ordered spans.
    repeated = sorted(
        [r for r in statute_rows if r["span_text"] == "38 USC 1114 note"],
        key=lambda r: int(r["text_span_start"]),
    )
    assert len(repeated) == 3
    assert len({r["source_path"] for r in repeated}) == 3
    assert int(repeated[0]["text_span_end"]) < int(repeated[1]["text_span_start"])
    assert int(repeated[1]["text_span_end"]) < int(repeated[2]["text_span_start"])

    # Resolved USC section check on 50 U.S.C. 1622 under selected-target-lookup/2.
    s1622 = next(r for r in statute_rows if r["href"] == "/us/usc/t50/s1622")
    assert s1622["span_text"] == "50 U.S.C. 1622"
    [cand1622] = json.loads(s1622["target_candidates_json"])
    assert cand1622["target_status"] == "found" and cand1622["normalized_key"] == "50-1622"
    assert cand1622["resolution_rule"] == "selected-target-lookup/2"
    assert cand1622["target_table_selected"] == "usc_release_point_sections"
    assert cand1622["candidate_keys"] == [{"usc_title": "50", "usc_section_key": "1622"}]

    # Seal under legislative-documents/4 (refs) and /3 (reads) without receipts and verify native Arrow spans.
    from spicy_regs import legislative_tables
    from spicy_regs.navigation_read_outcomes import FILE_TABLES
    from spicy_regs.pipelines.rollups.native_legal_references import NativeLegalReferencesRollup

    bundle = tmp_path / "sealed-bundle"
    bundle.mkdir()
    mapped = {p.stem: legislative_tables.to_public(p.stem, p, bundle / p.name) for p in files}
    file_table = FILE_TABLES["native-legal-references"]
    outcomes = legislative_tables.write_file_outcomes(
        file_table, [(p.stem, p) for p in files], mapped, bundle, bundle / f"{file_table}.parquet",
        generation_id="statute-v4-test",
    )
    sealed_refs = pq.read_table(bundle / "native_legal_references.parquet")
    assert pa.types.is_int64(sealed_refs.schema.field("text_span_start").type)
    assert pa.types.is_int64(sealed_refs.schema.field("text_span_end").type)
    assert pa.types.is_string(sealed_refs.schema.field("span_text").type)
    assert policy("native_legal_references").policy_version == "legislative-documents/4"
    assert policy("native_legal_reference_reads").policy_version == "legislative-documents/3"
    artifact = build_generation(
        tmp_path / "candidate",
        family="native-legal-references",
        files=[bundle / "native_legal_references.parquet", bundle / "native_legal_reference_reads.parquet", outcomes],
        expected_keys=list(NativeLegalReferencesRollup.outputs),
        schemas=NativeLegalReferencesRollup(manifest=manifest_path).generation_schemas(),
        inputs=evidence.inputs(),
    )
    assert "etlReceipts" not in artifact.root["spec"]
    verify_generation(tmp_path / "candidate", expected_pin=artifact.pin)
    verify_evidence(evidence.artifact_dir, expected_pin=evidence.artifact.pin)


def test_native_lookup_resolves_more_than_500_distinct_usc_sections_without_target_key_limit(tmp_path):
    import inspect

    from spicy_regs.citation_resolution import resolve_citations
    from spicy_regs.transforms.native_legal_references import MAX_OBSERVATIONS, NATIVE_MAX_TARGET_KEYS

    assert MAX_OBSERVATIONS == 10_000
    assert NATIVE_MAX_TARGET_KEYS == 10_000
    assert inspect.signature(resolve_citations).parameters["max_target_keys"].default == 500

    key_count = 800
    sections = [str(i) for i in range(1, key_count + 1)]
    target = tmp_path / "usc_release_point_sections.parquet"
    pq.write_table(
        pa.table({"usc_title": ["42"] * key_count, "usc_section_key": sections}),
        target,
    )
    refs = "".join(f'<ref href="/us/usc/t42/s{sec}">42 U.S.C. {sec}</ref>' for sec in sections)
    body = f"<section>{refs}</section>".encode()
    manifest = manifest_for(
        tmp_path,
        body,
        key="/us/usc/t42/s1",
        targets={"usc_release_point_sections": {"path": target.name, "sha256": digest(target.read_bytes())}},
    )
    files, evidence = run(manifest, tmp_path / "output")
    rows = pq.read_table(files[0]).to_pylist()
    assert len(rows) == key_count

    candidates = [json.loads(row["target_candidates_json"])[0] for row in rows]
    assert all(c["resolution_rule"] == "selected-target-lookup/2" for c in candidates)
    assert all(c["target_table_selected"] == "usc_release_point_sections" for c in candidates)
    assert all(c["target_status"] == "found" for c in candidates)
    assert all(c["match_basis"] == "printed_section" for c in candidates)
    assert all(c["reason"] is None for c in candidates)
    assert {c["normalized_key"] for c in candidates} == {f"42-{sec}" for sec in sections}

    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    resolution = next(event for event in events if event["event"] == "native-reference-resolution")
    assert resolution["max_target_keys"] == NATIVE_MAX_TARGET_KEYS
    assert resolution["distinct_target_keys_selected"] == key_count
    assert resolution["distinct_target_keys_read"] == key_count
    assert resolution["target_status_counts"] == {"found": key_count}
    assert resolution["reason_counts"] == {}
    assert resolution["partial"] is False
