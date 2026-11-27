"""Recorded native legal navigation survives typing and exact historical replay."""
import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.citation_resolution import LEGACY_USC_SECTION_ROUTE, NATIVE_RESOLUTION_RULE, NATIVE_ROUTES
from spicy_regs.legislative_documents import (
    LegislativeShapeError, NATIVE_TARGET_KEY_FIELDS, map_subject, recorded_subject,
)
from spicy_regs.legislative_receipts import restore_prior, write_legislative_outputs
from tests.test_legislative_documents import row
from tests.test_native_legal_references import FIXTURES, run


def observation(candidates, **extra):
    return row("native_legal_references", scope_id="scope", input_sha256="sha256:" + "a" * 64,
               occurrence_index="3", source_family="uscode", source_record_key="/us/usc/t5/s552",
               source_locator="retained:uslm", source_path="/*[1]/*[2]", element_tag="sourceCredit",
               text="The original passage", interpretation_status="partial_text_findings",
               rule_version="native-legal-reference/003", target_candidates_json=json.dumps(candidates), **extra)


@pytest.mark.parametrize("kind", NATIVE_ROUTES)
def test_every_maintained_route_keeps_its_complete_identity_and_recorded_pin(kind):
    route = NATIVE_ROUTES[kind]
    keys = {name: f"literal-{name}" for name in route.identity}
    pin = {"status": "published", "family": "selected-family", "generation_id": "original-generation",
           "artifact_digest": "sha256:" + "b" * 64, "receipt_sha256": "sha256:" + "c" * 64}
    candidate = dict(cite_kind=kind, target_kind=kind, target_key="retained-key", target_resolved=True,
                     candidate_keys=[keys, keys], expected_cardinality=route.expected_cardinality,
                     target_table_selected=route.table, target_status="found", match_count=2,
                     target_snapshot=pin, resolution_rule=NATIVE_RESOLUTION_RULE,
                     derivation_rule="retained-rule", derivation_version="004", occurrence_key="original:0")
    result = map_subject("native_legal_references", observation([candidate, None, candidate]))
    assert result is not None
    assert result["source_path"] == "/*[1]/*[2]" and result["text"] == "The original passage"
    assert result["rule_version"] == "native-legal-reference/003"
    assert result["interpretation_status"] == "partial_text_findings"
    first, null, repeat = result["target_candidates"]
    assert null is None and first["candidate_ordinal"] == 0 and repeat["candidate_ordinal"] == 2
    assert first["target_snapshot"]["receipt_sha256"] == pin["receipt_sha256"]
    assert first["resolution_rule"] == candidate["resolution_rule"]
    assert first["target_status"] == "found" and first["target_resolved"] is True
    assert first["match_count"] == 2
    assert [key["target_key_ordinal"] for key in first["candidate_keys"]] == [0, 1]
    for key in first["candidate_keys"]:
        assert {name: key[name] for name in route.identity} == keys
        assert all(key[name] is None for name in set(NATIVE_TARGET_KEY_FIELDS) - set(route.identity))


def test_typed_key_union_is_exactly_the_native_route_identities():
    """native_legal_references records candidates under NATIVE_ROUTES (/2) plus carried LEGACY_USC_SECTION_ROUTE (/1)."""
    assert NATIVE_RESOLUTION_RULE == "selected-target-lookup/2"
    assert set(NATIVE_TARGET_KEY_FIELDS) == {
        name for route in (*NATIVE_ROUTES.values(), LEGACY_USC_SECTION_ROUTE) for name in route.identity
    }


@pytest.mark.parametrize("status,reason,count,keys", [
    ("missing", None, 0, []), ("ambiguous", None, 2, [{"number": "1"}, {"number": "2"}]),
    ("unsupported", "unsupported_citation_kind", None, []),
    ("not_checked", "target_snapshot_unavailable", None, []),
    ("not_checked", "candidate_limit", 103, [{"congress": "119"}]),
    ("not_checked", "target_identity_incomplete", 1, [{"congress": None}]),
    ("not_checked", "target_read_failure", None, []),
])
def test_recorded_outcomes_and_refusals_are_preserved_without_a_new_lookup(status, reason, count, keys):
    candidate = dict(target_status=status, reason=reason, match_count=count, candidate_keys=keys,
                     source_status="current_text", target_resolved=False, error_type="TimeoutError",
                     match_basis="page_range", text_sha256="sha256:" + "d" * 64)
    mapped = map_subject("native_legal_references", observation([candidate]))
    assert mapped is not None
    [result] = mapped["target_candidates"]
    for name in ("target_status", "reason", "match_count", "source_status", "target_resolved",
                 "error_type", "match_basis", "text_sha256"):
        assert result[name] == candidate[name]
    assert len(result["candidate_keys"]) == len(keys)


@pytest.mark.parametrize("candidates,status", [
    ([], "unsupported_href"), ([], "no_qualified_text_findings"), (None, None),
])
def test_unsupported_empty_and_unread_observations_remain_visible(candidates, status):
    raw = observation(candidates) | {"interpretation_status": status, "href": "opaque:retained"}
    result = map_subject("native_legal_references", raw)
    assert result is not None
    assert result["href"] == "opaque:retained" and result["text"] == raw["text"]
    assert result["interpretation_status"] == status
    assert result["target_candidates"] == candidates


@pytest.mark.parametrize("candidate", [
    {"unexpected": "value"}, {"candidate_keys": [{"unreviewed_key": "value"}]},
    {"target_snapshot": {"unreviewed_pin": "value"}}, {"candidate_ordinal": 4},
    {"candidate_keys": [{"target_key_ordinal": 4}]},
])
def test_unreviewed_candidate_properties_refuse(candidate):
    with pytest.raises(LegislativeShapeError):
        map_subject("native_legal_references", observation([candidate]))


def test_main_read_scope_keeps_zero_empty_unknown_and_selected_shapes():
    raw = row("native_legal_reference_reads", scope_id="empty", source_family="ecfr",
              source_record_key="title-1", input_sha256="sha256:" + "a" * 64, occurrence_count="0",
              source_bytes="17", selected_shapes_json='["AUTH","SOURCE"]', unsupported_shapes_json='[]',
              source_locator="retained:xml", read_status="complete_selected_shapes", rule_version="rule-3",
              manifest_sha256="sha256:" + "b" * 64)
    result = map_subject("native_legal_reference_reads", raw)
    assert result is not None
    assert result["occurrence_count"] == 0 and result["source_bytes"] == 17
    assert result["edition"] is None and result["unsupported_shapes"] == []
    assert result["selected_shapes"] == ["AUTH", "SOURCE"]
    assert result["body_version_id"] == "body:sha256:" + "a" * 64
    assert result["input_sha256"] == raw["input_sha256"]
    assert result["manifest_sha256"] == raw["manifest_sha256"]


def test_exact_earlier_receipt_only_reads_and_stripped_subjects_restore_before_upgrade(tmp_path, monkeypatch):
    from spicy_regs import legislative_receipts as receipts
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    files, _ = run(FIXTURES / "manifest.json", tmp_path / "source")
    from spicy_regs.policy_migrations import earlier_policy
    old = {name: earlier_policy(name, "legislative-documents/1")
           for name in ("native_legal_references", "native_legal_reference_reads")}  # the order ``files`` has
    current_policy, current_map = receipts.policy, receipts.map_subject

    def old_map(dataset, raw):
        if dataset not in old:
            return current_map(dataset, raw)
        if old[dataset].receipt_only:
            return None
        return recorded_subject(dataset, raw, dict.fromkeys(old[dataset].subject_schema.names))

    with monkeypatch.context() as patch:
        patch.setattr(receipts, "policy", lambda name: old.get(name) or current_policy(name))
        patch.setattr(receipts, "map_subject", old_map)
        manifest = write_legislative_outputs(files, tmp_path / "old-bundle", generation_id="old-generation")
    assert manifest["subjects"]["native_legal_reference_reads"] == []
    selections = [SelectedDataset(name, tuple(tmp_path / "old-bundle" / p for p in paths),
                                  tmp_path / "old-bundle/etl_receipts.parquet", "old-generation")
                  for name, paths in manifest["subjects"].items()]
    remember_selection(tmp_path / "state", selections)
    from spicy_regs.local_data import local_selection
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.sources import publication
    import duckdb
    selected = local_selection(tmp_path / "state")
    assert "native_legal_reference_reads" not in selected.files
    assert "native_legal_references" in selected.files
    local_native = {item.dataset: {"subjects": item.subjects, "receipts": item.receipts,
                                   "generation_id": item.generation_id} for item in selections}
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, publication.empty_index(), "unused", local_native=local_native)
        reads = list(adapter.selected_rows("native_legal_reference_reads"))
        assert [r["raw_source_row"] for r in reads] == pq.read_table(files[1]).to_pylist()
        assert "scope_id" not in reads[0]  # Original observed receipt, no invented main row.
        index = publication.empty_index()
        shared = tmp_path / "old-bundle/etl_receipts.parquet"
        identity = publication.file_identity(shared)
        index["families"]["native-legal-references"] = {
            "prefix": "generations/native/" + "a" * 64, "tables": {},
            "etlReceipts": {"key": "etl_receipts.parquet", "sha256": identity["sha256"],
                            "byteSize": identity["bytes"], "rows": pq.read_metadata(shared).num_rows,
                            "generationId": "old-generation", "datasets": list(old)},
        }
        remote = ReceiptAdapter(con, index, "https://never-download.invalid")
        fetched = []
        def fetch(member, destination):
            import shutil
            fetched.append(member.key)
            assert member.key == "etl_receipts.parquet"
            shutil.copyfile(shared, destination)
        monkeypatch.setattr(remote, "_fetch", fetch)
        assert list(remote.selected_rows("native_legal_reference_reads")) == reads
        assert fetched == ["etl_receipts.parquet"]
    prior = SelectedPriors(tmp_path / "selected", root=tmp_path / "state", public_url="")
    restored = [prior.get(name) for name in old]
    for original, recovered in zip(files, restored, strict=True):
        assert pq.read_table(original).equals(pq.read_table(recovered), check_metadata=True)
    direct = restore_prior(tmp_path / "old-bundle", tmp_path / "direct")
    assert set(direct) == set(old)
    upgraded = write_legislative_outputs(restored, tmp_path / "new-bundle", generation_id="new-generation")
    assert not any(upgraded["refused_rows"].values())
    assert pq.read_metadata(tmp_path / "new-bundle/native_legal_reference_reads.parquet").num_rows == 2
    assert not field_registry()["native_legal_reference_reads"]["processing_only"]


def test_actual_hosted_family_writes_main_scopes_and_reuses_selected_originals(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.native_legal_references import NativeLegalReferencesRollup
    from spicy_regs.source_evidence import CaptureEvidence

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    work = tmp_path / "work"
    work.mkdir()
    pipeline = NativeLegalReferencesRollup(manifest=FIXTURES / "manifest.json", output_dir=work)
    pipeline.source_evidence = CaptureEvidence(work, pipeline.name)
    outputs = pipeline.build(work)
    assert {p.name for p in outputs} == {
        "native_legal_references.parquet", "native_legal_reference_reads.parquet",
        "native_legal_references_document_file_outcomes.parquet",
    }
    assert not pipeline.receipt_only_tables
    assert sorted(pq.read_table(work / "native_legal_reference_reads.parquet")["occurrence_count"].to_pylist()) == [2, 31]
    before = {p.name: pq.read_table(p) for p in outputs}
    again = NativeLegalReferencesRollup(manifest=FIXTURES / "manifest.json", output_dir=work)
    again.source_evidence = CaptureEvidence(work, again.name)
    repeated = again.build(work)
    assert {p.name for p in repeated} == set(before)
    for name in ("native_legal_references.parquet", "native_legal_reference_reads.parquet"):
        assert before[name].equals(pq.read_table(work / name))


def test_v2_rule1_prior_restores_and_coexists_with_v4_rule2_and_explorer_navigation(tmp_path, monkeypatch):
    """Prior legislative-documents/2 (/1 keys) and /3 (/2 keys, no spans) generations restore cleanly and coexist in /4."""
    import pyarrow as pa
    from spicy_regs import legislative_receipts as receipts
    from spicy_regs.explorer_navigation import declarations, target_keys
    from spicy_regs.policy_migrations import earlier_policy

    assert receipts.policy("native_legal_references").policy_version == "legislative-documents/4"
    v2 = earlier_policy("native_legal_references", "legislative-documents/2")
    v3 = earlier_policy("native_legal_references", "legislative-documents/3")

    legacy_candidate = {
        "cite_kind": "usc_section", "target_kind": "usc_section", "target_key": "5-401",
        "normalized_key": "5-401", "target_resolved": True,
        "candidate_keys": [{"congress": "117", "session": "2", "seq": "1"}],
        "expected_cardinality": "many", "target_table_selected": "law_code_sections",
        "target_status": "found", "match_count": 1, "resolution_rule": "selected-target-lookup/1",
        "source_status": "native_field", "occurrence_key": "legacy:0",
    }
    raw_v2 = observation([legacy_candidate]) | {"scope_id": "legacy-scope", "occurrence_index": "0"}
    raw_v2_path = tmp_path / "v2_source" / "native_legal_references.parquet"
    raw_v2_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([raw_v2]), raw_v2_path)

    current_policy, current_map = receipts.policy, receipts.map_subject

    def earlier_map(target_policy):
        def _map(dataset, raw):
            if dataset != "native_legal_references":
                return current_map(dataset, raw)
            return recorded_subject(
                dataset, raw, dict.fromkeys(target_policy.subject_schema.names),
                policy_version=target_policy.policy_version,
            )
        return _map

    with monkeypatch.context() as patch:
        patch.setattr(receipts, "policy", lambda name: v2 if name == "native_legal_references" else current_policy(name))
        patch.setattr(receipts, "map_subject", earlier_map(v2))
        write_legislative_outputs([raw_v2_path], tmp_path / "v2-bundle", generation_id="v2-gen")

    # Restore exact /2 prior under current /4 code.
    restored = restore_prior(tmp_path / "v2-bundle", tmp_path / "v2-restored")
    assert pq.read_table(restored["native_legal_references"]).equals(pq.read_table(raw_v2_path))

    # Seal and restore an exact /3 (ss-xpyk rule/2, no span columns) prior under current /4 code.
    v3_candidate = {
        "cite_kind": "usc_section", "target_kind": "usc_section", "target_key": "5-553",
        "normalized_key": "5-553", "target_resolved": True,
        "candidate_keys": [{"usc_title": "5", "usc_section_key": "553"}],
        "expected_cardinality": "many", "target_table_selected": "usc_release_point_sections",
        "target_status": "found", "match_count": 1, "match_basis": "printed_section",
        "resolution_rule": "selected-target-lookup/2",
        "source_status": "native_field", "occurrence_key": "v3:0",
    }
    raw_v3 = observation([v3_candidate]) | {"scope_id": "fresh-scope", "occurrence_index": "0"}
    raw_v3_path = tmp_path / "v3_prior_source" / "native_legal_references.parquet"
    raw_v3_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([raw_v3]), raw_v3_path)
    with monkeypatch.context() as patch:
        patch.setattr(receipts, "policy", lambda name: v3 if name == "native_legal_references" else current_policy(name))
        patch.setattr(receipts, "map_subject", earlier_map(v3))
        v3_manifest = write_legislative_outputs([raw_v3_path], tmp_path / "v3-prior-bundle", generation_id="v3-prior-gen")
    assert v3_manifest["refused_rows"]["native_legal_references"] == 0
    restored_v3_prior = restore_prior(tmp_path / "v3-prior-bundle", tmp_path / "v3-prior-restored")
    assert pq.read_table(restored_v3_prior["native_legal_references"]).equals(pq.read_table(raw_v3_path))

    # Merge carried /1 row alongside fresh /2 usc_release_point_sections row in a /4 bundle.
    merged_path = tmp_path / "v4_source" / "native_legal_references.parquet"
    merged_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([raw_v2, raw_v3]), merged_path)
    manifest = write_legislative_outputs([merged_path], tmp_path / "v4-bundle", generation_id="v4-gen")
    assert manifest["refused_rows"]["native_legal_references"] == 0
    restored_v4 = restore_prior(tmp_path / "v4-bundle", tmp_path / "v4-restored")
    assert pq.read_table(restored_v4["native_legal_references"]).num_rows == 2

    # Explorer navigation resolves BOTH /2 usc_release_point_sections and carried /1 law_code_sections links.
    targets = {
        t["table"]: t
        for t in next(s for s in declarations() if s["id"] == "native_legal_targets")["targets"]
    }
    assert "usc_release_point_sections" in targets and "law_code_sections" in targets
    assert target_keys(
        targets["usc_release_point_sections"],
        {"target_table_selected": "usc_release_point_sections", "target_status": "found",
         "usc_title": "5", "usc_section_key": "553"},
        {},
    ) == ["5", "553"]
    assert target_keys(
        targets["law_code_sections"],
        {"target_table_selected": "law_code_sections", "target_status": "found",
         "congress": "117", "session": "2", "seq": "1"},
        {},
    ) == ["117", "2", "1"]


def test_legislative_documents_v2_xpath_only_bundle_restores_and_upgrades_to_v3(tmp_path, monkeypatch):
    import pyarrow as pa

    from spicy_regs import legislative_receipts as receipts
    from spicy_regs.policy_migrations import earlier_policy

    files, _ = run(FIXTURES / "manifest.json", tmp_path / "source")
    span_cols = ("text_rendition", "text_offset_unit", "text_rendition_sha256",
                 "text_span_start", "text_span_end", "span_text")
    read_span_cols = ("text_rendition", "text_offset_unit", "text_rendition_sha256")

    # Materialize genuine XPath-only 20-col / 13-col /003 raw tables before sealing v2-bundle.
    v2_raw_dir = tmp_path / "v2-raw"
    v2_raw_dir.mkdir()
    v2_files = []
    for path in files:
        rows = pq.read_table(path).to_pylist()
        if path.name == "native_legal_references.parquet":
            stripped = []
            for item in rows:
                cands = json.loads(item["target_candidates_json"] or "[]")
                for cand in cands:
                    cand.pop("rendition_span_start", None)
                    cand.pop("rendition_span_end", None)
                stripped.append({
                    **{k: v for k, v in item.items() if k not in span_cols},
                    "rule_version": "native-legal-reference/003",
                    "target_candidates_json": json.dumps(cands),
                })
            out = v2_raw_dir / path.name
            pq.write_table(pa.Table.from_pylist(stripped), out)
            assert len(pq.read_schema(out).names) == 20
            v2_files.append(out)
        elif path.name == "native_legal_reference_reads.parquet":
            stripped = [
                {**{k: v for k, v in item.items() if k not in read_span_cols},
                 "rule_version": "native-legal-reference/003"}
                for item in rows
            ]
            out = v2_raw_dir / path.name
            pq.write_table(pa.Table.from_pylist(stripped), out)
            assert len(pq.read_schema(out).names) == 13
            v2_files.append(out)
        else:
            v2_files.append(path)

    old_v2 = {name: earlier_policy(name, "legislative-documents/2")
              for name in ("native_legal_references", "native_legal_reference_reads")}
    current_policy, current_map = receipts.policy, receipts.map_subject

    def v2_map(dataset, raw):
        if dataset not in old_v2:
            return current_map(dataset, raw)
        return recorded_subject(
            dataset, raw, dict.fromkeys(old_v2[dataset].subject_schema.names), policy_version="legislative-documents/2"
        )

    with monkeypatch.context() as patch:
        patch.setattr(receipts, "policy", lambda name: old_v2.get(name) or current_policy(name))
        patch.setattr(receipts, "map_subject", v2_map)
        v2_manifest = write_legislative_outputs(v2_files, tmp_path / "v2-bundle", generation_id="v2-generation")
    assert not any(v2_manifest["refused_rows"].values())
    assert "text_span_start" not in pq.read_schema(tmp_path / "v2-bundle/native_legal_references.parquet").names
    assert "text_rendition" not in pq.read_schema(tmp_path / "v2-bundle/native_legal_reference_reads.parquet").names

    restored = restore_prior(tmp_path / "v2-bundle", tmp_path / "restored")
    restored_ref_cols = pq.read_schema(restored["native_legal_references"]).names
    restored_read_cols = pq.read_schema(restored["native_legal_reference_reads"]).names
    assert len(restored_ref_cols) == 20 and not (set(span_cols) & set(restored_ref_cols))
    assert len(restored_read_cols) == 13 and not (set(read_span_cols) & set(restored_read_cols))

    upgraded = write_legislative_outputs(
        [restored["native_legal_references"], restored["native_legal_reference_reads"]],
        tmp_path / "v3-bundle",
        generation_id="v3-generation",
    )
    assert not any(upgraded["refused_rows"].values())
    upgraded_refs = pq.read_table(tmp_path / "v3-bundle/native_legal_references.parquet")
    upgraded_reads = pq.read_table(tmp_path / "v3-bundle/native_legal_reference_reads.parquet")
    assert set(span_cols) <= set(upgraded_refs.schema.names)
    assert set(read_span_cols) <= set(upgraded_reads.schema.names)
    for item in upgraded_refs.to_pylist():
        assert item["source_path"]
        assert all(item[col] is None for col in span_cols)
        for cand in item["target_candidates"] or []:
            assert cand["rendition_span_start"] is None and cand["rendition_span_end"] is None
    assert all(all(item[col] is None for col in read_span_cols) for item in upgraded_reads.to_pylist())
