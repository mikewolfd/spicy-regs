"""Synthetic release fixtures verify compatibility; none represents a production pin."""

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

import pytest

from spicy_regs import fec_release as release
from spicy_regs.relationship_views.sql_views import SQLView


def digest(value):
    return release.sha256(value.encode())


def fixture(tmp_path):
    code = tmp_path / "installed"
    code.mkdir()
    identity_files = {}
    for group in ("policies", "dictionaries", "definitions"):
        path = code / (group + ".json")
        path.write_text(json.dumps({"identity": group}))
        identity_files[group] = {group + "/1": path}
    (code / "consumer.py").write_text("VERSION = 1\n")
    consumer = release.runtime_consumer(digest("image"), package_root=code, packages=("duckdb",))
    index = {"families": {}}
    for family, tables in {"fec-query": ["fec_receipts", "fec_reports"], "members": ["members"]}.items():
        index["families"][family] = {
            "artifactDigest": digest(family),
            "tables": {table + ".parquet": {
                "sha256": digest(table), "columns": [["id", "INTEGER"]], "rows": 1, "byteSize": 100,
            } for table in tables},
        }
    views = [
        SQLView("fec_test_money_members", {"fec_receipts": ("id",), "members": ("id",)},
                lambda _: "SELECT a.id FROM fec_receipts a JOIN members b ON a.id = b.id",
                "Synthetic matched membership", ("id",), "synthetic/1"),
        SQLView("fec_test_reports", {"fec_reports": ("id",)}, lambda _: "SELECT id FROM fec_reports",
                "Synthetic reported records", ("id",), "synthetic/1"),
    ]
    specs = tuple(release.QualifiedView(v, "mapping/1", "identity/1", identity_files, "selected-fixture", "fixture-as-of", {}) for v in views)
    configuration = release.capture_configuration(specs, receipt_digest=None, image_digest=None, base_url="unused",
                                                  consumer=consumer)
    view_receipts = {}
    receipt = dict(format=release.FORMAT, version=release.VERSION,
                   output_membership={table: release.captured_table(index, table) for table in ["fec_receipts", "fec_reports"]},
                   consumer=consumer, recovery={"source_archive_sha256": digest("archive"),
                                                "retained_generations": {family: [entry["artifactDigest"]] for family, entry in index["families"].items()},
                                                "rollback_receipts": []}, views=view_receipts)
    for spec in specs:
        state = configuration["views"][spec.view.name]
        view_receipts[spec.view.name] = {
            "dependencies": {name: release.captured_table(index, name) for name in spec.view.required},
            **{key: state[key] for key in ["sql_sha256", "interpretation", "population", "as_of", "evidence_generations"]},
            "acceptance_receipts": [digest("synthetic-acceptance")],
        }
    return specs, index, receipt, consumer


def capture(tmp_path, specs, receipt, consumer):
    raw = json.dumps(receipt, sort_keys=True).encode()
    path = tmp_path / (release.sha256(raw).removeprefix("sha256:") + ".json")
    path.write_bytes(raw)
    return release.capture_configuration(specs, receipt_digest=release.sha256(raw), image_digest=None, base_url="unused",
                                         local_path=path, local_mode=True, consumer=consumer)


def check(spec, config, index):
    return release.check_view(spec, config, index, ["fec_receipts", "fec_reports", "members"])


def test_exact_matching_receipt_exposes_actual_pins_and_measured_identities(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    config = capture(tmp_path, specs, receipt, consumer)
    result = check(specs[0], config, index)
    assert result["status"] == "compatible" and result["reasons"] == []
    assert result["receipt_sha256"] == config["receipt_sha256"]
    assert result["dependencies"] == receipt["views"][specs[0].view.name]["dependencies"]
    assert result["consumer"] == consumer
    assert "deployment_assertion" in result["image_identity_basis"]
    # Caller mutation cannot change already captured config values.
    receipt["consumer"]["image_digest"] = digest("later")
    assert config["consumer"]["image_digest"] == digest("image")


@pytest.mark.parametrize("field", ["generation", "digest", "schema", "family"])
def test_required_parent_advancement_disables_only_affected_view_even_same_schema(tmp_path, field):
    specs, index, receipt, consumer = fixture(tmp_path)
    config = capture(tmp_path, specs, receipt, consumer)
    changed = deepcopy(index)
    parent = changed["families"]["members"]
    if field == "generation":
        parent["artifactDigest"] = digest("later-parent")
    elif field == "digest":
        parent["tables"]["members.parquet"]["sha256"] = digest("later-table")
    elif field == "schema":
        parent["tables"]["members.parquet"]["columns"] = [["id", "BIGINT"]]
    else:
        changed["families"]["other-owner"] = changed["families"].pop("members")
    assert check(specs[0], config, changed)["status"] == "disabled"
    assert check(specs[1], config, changed)["status"] == "compatible"
    assert check(specs[0], config, index)["status"] == "compatible"


@pytest.mark.parametrize("change", ["sql", "policy", "dictionary", "definition", "missing_identity", "extra_identity", "bad_dependency"])
def test_interpretation_drift_or_receipt_damage_fails_affected_spec(tmp_path, change):
    specs, index, receipt, consumer = fixture(tmp_path)
    name = specs[0].view.name
    if change == "sql":
        receipt["views"][name]["sql_sha256"] = digest("different-sql")
    elif change in {"policy", "dictionary", "definition"}:
        group = {"policy": "policies", "dictionary": "dictionaries", "definition": "definitions"}[change]
        receipt["views"][name]["interpretation"][group][group + "/1"] = digest("different")
    elif change == "missing_identity":
        del receipt["views"][name]["interpretation"]["identity_version"]
    elif change == "extra_identity":
        receipt["views"][name]["interpretation"]["surprise"] = digest("not-running")
    else:
        receipt["views"][name]["dependencies"]["members"] = None
    config = capture(tmp_path, specs, receipt, consumer)
    assert check(specs[0], config, index)["status"] == "disabled"
    assert check(specs[1], config, index)["status"] == "compatible"


def test_split_table_member_digest_and_order_are_exact_dependencies(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    descriptor = index["families"]["members"]["tables"]["members.parquet"]
    del descriptor["sha256"]
    descriptor.update(partitionColumns=["id"], members=[
        dict(key="members/id=1/part-000001.parquet", sha256=digest("part1"), rows=1, byteSize=100, partition={"id": "1"})
    ])
    receipt["views"][specs[0].view.name]["dependencies"]["members"] = release.captured_table(index, "members")
    config = capture(tmp_path, specs, receipt, consumer)
    assert check(specs[0], config, index)["status"] == "compatible"
    descriptor["members"][0]["sha256"] = digest("changed-part")
    result = check(specs[0], config, index)
    assert result["status"] == "disabled"
    assert any(r["reason"] == "exact_table_pin_mismatch" for r in result["reasons"])


def test_runtime_measures_code_and_packages_instead_of_copying_receipt(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    (tmp_path / "installed" / "consumer.py").write_text("VERSION = 2\n")
    actual = release.runtime_consumer(consumer["image_digest"], package_root=tmp_path / "installed", packages=("duckdb",))
    assert actual["code_sha256"] != consumer["code_sha256"]
    assert actual["package_versions"]["duckdb"]
    config = capture(tmp_path, specs, receipt, actual)
    assert any(r["path"] == "consumer.code_sha256" for r in check(specs[0], config, index)["reasons"])
    actual["image_digest"] = digest("wrong-image")
    actual["package_versions"]["duckdb"] = "wrong-version"
    config = capture(tmp_path, specs, receipt, actual)
    paths = {r["path"] for r in check(specs[0], config, index)["reasons"]}
    assert {"consumer.code_sha256", "consumer.image_digest", "consumer.package_versions"} <= paths


def test_receipt_bytes_pin_format_duplicate_keys_and_bound_are_checked(tmp_path):
    _, _, receipt, _ = fixture(tmp_path)
    raw = json.dumps(receipt).encode()
    assert release.parse_receipt(raw, release.sha256(raw)) == receipt
    with pytest.raises(ValueError, match="differ"):
        release.parse_receipt(raw + b" ", release.sha256(raw))
    bad = b'{"format":"x","format":"y"}'
    with pytest.raises(ValueError, match="repeats"):
        release.parse_receipt(bad, release.sha256(bad))
    with pytest.raises(ValueError, match="limit"):
        release.parse_receipt(b" " * (release.LIMIT + 1), digest("anything"))
    receipt["sql"] = "DROP TABLE members"
    bad = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="fields"):
        release.parse_receipt(bad, release.sha256(bad))
    bad = b'{"anything":NaN}'
    with pytest.raises(ValueError, match="Non-finite"):
        release.parse_receipt(bad, release.sha256(bad))


def test_invalid_output_membership_cannot_support_a_compatible_release(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    receipt["output_membership"]["fec_receipts"]["descriptor"]["sha256"] = "not-a-digest"
    config = capture(tmp_path, specs, receipt, consumer)
    assert config["receipt_error"]
    assert check(specs[0], config, index)["status"] == "disabled"


def test_receipt_can_be_finalized_after_code_without_a_hash_cycle(tmp_path):
    specs, _, receipt, consumer = fixture(tmp_path)
    capture(tmp_path, specs, receipt, consumer)
    assert release.runtime_consumer(consumer["image_digest"], package_root=tmp_path / "installed", packages=("duckdb",)) == consumer


def test_receipt_loader_reuses_immutable_evidence_path_and_bounded_owner_reader(tmp_path, monkeypatch):
    from spicy_regs.sources import publication

    _, _, receipt, _ = fixture(tmp_path)
    raw = json.dumps(receipt).encode()
    pin = release.sha256(raw)
    calls = []

    def get(url, **options):
        calls.append((url, options))
        return raw

    monkeypatch.setattr(publication, "_bounded_get", get)
    assert release.load_receipt(pin, "https://fixture.invalid") == receipt
    assert calls == [("https://fixture.invalid/source-evidence/blobs/sha256/" + pin[7:], {"allow_missing": False, "limit": release.LIMIT})]


def test_missing_local_receipt_never_fetches_remote_or_qualifies(tmp_path, monkeypatch):
    specs, index, _, consumer = fixture(tmp_path)
    monkeypatch.setattr(release, "load_receipt", lambda *a, **k: pytest.fail("local mode fetched without a path"))
    config = release.capture_configuration(specs, receipt_digest=digest("missing"), image_digest=None,
                                           base_url="unused", local_mode=True, consumer=consumer)
    result = check(specs[0], config, index)
    assert result["status"] == "disabled" and "Local serving" in result["reasons"][0]["reason"]


def test_missing_interpretation_file_affects_only_its_registered_spec(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    identities = deepcopy(specs[0].identities)
    identities["policies"] = {"missing/1": Path("/missing-policy-file")}
    specs = (replace(specs[0], identities=identities), specs[1])
    config = capture(tmp_path, specs, receipt, consumer)
    assert check(specs[0], config, index)["status"] == "disabled"
    assert check(specs[1], config, index)["status"] == "compatible"


def test_release_import_does_not_load_source_processors():
    program = '''
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyarrow','polars','spicy_docs'}:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from spicy_regs import fec_release, mcp_server
assert fec_release.VERSION == 2
assert fec_release.SUPPORTED_VERSIONS == (1, 2)
assert mcp_server.FEC_QUALIFIED_VIEWS
assert all(isinstance(spec, fec_release.QualifiedView) for spec in mcp_server.FEC_QUALIFIED_VIEWS)
assert not any(name.startswith('spicy_regs.transforms') for name in sys.modules)
'''
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_each_capture_remeasures_changed_shared_interpretation_file(tmp_path):
    specs, _, _, consumer = fixture(tmp_path)
    kwargs = dict(receipt_digest=None, image_digest=None, base_url="unused", consumer=consumer)
    first = release.capture_configuration(specs, **kwargs)
    shared = specs[0].identities["dictionaries"]["dictionaries/1"]
    shared.write_text('{"identity":"changed-between-captures"}')
    second = release.capture_configuration(specs, **kwargs)
    for spec in specs:
        name = spec.view.name
        previous = first["views"][name]["interpretation"]["dictionaries"]
        current = second["views"][name]["interpretation"]["dictionaries"]
        assert previous != current
        assert current["dictionaries/1"] == release.sha256(shared.read_bytes())


def evidence_fixture(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    pin = digest('synthetic-non-sql-parent')
    evidence = {'source-proof': pin}
    specs = (replace(specs[0], evidence_generations=evidence), specs[1])
    index['families']['source-proof'] = {
        'artifactDigest': pin, 'prefix': 'generations/source-proof/' + pin[7:],
        'logicalId': 'urn:test:source-proof', 'tables': {},
    }
    receipt['views'][specs[0].view.name]['evidence_generations'] = dict(evidence)
    receipt['recovery']['retained_generations']['source-proof'] = [pin]
    return specs, index, receipt, consumer


def test_non_sql_parent_is_captured_and_requires_matching_family_and_recovery(tmp_path):
    specs, index, receipt, consumer = evidence_fixture(tmp_path)
    config = capture(tmp_path, specs, receipt, consumer)
    good = check(specs[0], config, index)
    assert good['status'] == 'compatible'
    assert good['evidence_generations'] == good['required_evidence_generations'] == specs[0].evidence_generations
    assert 'source-proof' not in good['dependencies']
    # Caller mutation does not rewrite the selected application's captured pin.
    specs[0].evidence_generations['source-proof'] = digest('later')
    assert check(specs[0], config, index)['status'] == 'compatible'
    later = capture(tmp_path, specs, receipt, consumer)
    assert check(specs[0], later, index)['status'] == 'disabled'
    assert check(specs[1], later, index)['status'] == 'compatible'


@pytest.mark.parametrize('change', ['absent', 'advanced', 'wrong-family', 'malformed', 'prefix', 'unretained', 'missing-declaration'])
def test_non_sql_parent_failure_keeps_unrelated_spec_available(tmp_path, change):
    specs, index, receipt, consumer = evidence_fixture(tmp_path)
    if change == 'absent':
        del index['families']['source-proof']
    elif change == 'advanced':
        pin = digest('advanced')
        index['families']['source-proof'].update(artifactDigest=pin, prefix='generations/source-proof/' + pin[7:])
    elif change == 'wrong-family':
        index['families']['other-proof'] = index['families'].pop('source-proof')
    elif change == 'malformed':
        index['families']['source-proof']['artifactDigest'] = None
    elif change == 'prefix':
        index['families']['source-proof']['prefix'] = 'generations/other-proof/' + digest('synthetic-non-sql-parent')[7:]
    elif change == 'unretained':
        del receipt['recovery']['retained_generations']['source-proof']
    else:
        del receipt['views'][specs[0].view.name]['evidence_generations']
    config = capture(tmp_path, specs, receipt, consumer)
    assert check(specs[0], config, index)['status'] == 'disabled'
    assert check(specs[1], config, index)['status'] == 'compatible'


def _shared_multi_view_fixture(tmp_path):
    """Multi-view v1 fixture exercising shared acceptance, metadata, output_membership, and non-output deps."""
    specs, index, v1, consumer = evidence_fixture(tmp_path)
    extra_view = SQLView(
        "fec_test_money_members_alt",
        {"fec_receipts": ("id",), "members": ("id",)},
        lambda _: "SELECT b.id FROM fec_receipts a JOIN members b ON a.id = b.id",
        "Second view sharing non-output dependency",
        ("id",),
        "synthetic/1",
    )
    shared_evidence = dict(specs[0].evidence_generations)
    shared_pop = "Retained synthetic FEC selection pinned by " + digest("population")
    shared_as_of = "Retained synthetic generation sealed 2026-09-30T00:00:00+00:00 for release parity checks"
    specs = (
        replace(specs[0], population=shared_pop, as_of=shared_as_of, evidence_generations=shared_evidence),
        replace(specs[1], population=shared_pop, as_of=shared_as_of, evidence_generations=shared_evidence),
        replace(specs[0], view=extra_view, population=shared_pop, as_of=shared_as_of,
                evidence_generations=shared_evidence),
    )
    configuration = release.capture_configuration(
        specs, receipt_digest=None, image_digest=None, base_url="unused", consumer=consumer
    )
    acceptance_order = [digest(f"acceptance-{i}") for i in range(8)]
    v1["version"] = 1
    v1["views"] = {
        spec.view.name: {
            "dependencies": {name: release.captured_table(index, name) for name in spec.view.required},
            **{
                key: configuration["views"][spec.view.name][key]
                for key in ("sql_sha256", "interpretation", "population", "as_of", "evidence_generations")
            },
            "acceptance_receipts": list(acceptance_order),
        }
        for spec in specs
    }
    return specs, index, v1, consumer


def test_v1_receipt_remains_readable_and_strict_during_rollout(tmp_path, monkeypatch):
    from spicy_regs.sources import publication

    specs, index, v1, consumer = _shared_multi_view_fixture(tmp_path)
    raw = release.serialize_receipt(v1)
    pin = release.sha256(raw)
    parsed = release.parse_receipt(raw, pin)
    assert parsed["version"] == 1 and parsed == v1
    monkeypatch.setattr(publication, "_bounded_get", lambda *a, **k: raw)
    assert release.load_receipt(pin, "https://fixture.invalid") == v1
    config = capture(tmp_path, specs, v1, consumer)
    for spec in specs:
        res = check(spec, config, index)
        assert res["status"] == "compatible" and res["reasons"] == []
        assert res["acceptance_receipts"] == v1["views"][spec.view.name]["acceptance_receipts"]
    # v1 strictly rejects the v2-only shared section.
    v1_with_shared = {**deepcopy(v1), "shared": {"acceptance_receipts": {"1": [digest("x")]}}}
    bad_raw = release.serialize_receipt(v1_with_shared)
    with pytest.raises(ValueError, match="fields"):
        release.parse_receipt(bad_raw, release.sha256(bad_raw))
    # v1 never expands v2 reference fields; check_view refuses the affected view.
    v1_with_ref = deepcopy(v1)
    name = specs[0].view.name
    del v1_with_ref["views"][name]["acceptance_receipts"]
    v1_with_ref["views"][name]["acceptance_receipts_ref"] = "1"
    bad_config = capture(tmp_path, specs, v1_with_ref, consumer)
    assert check(specs[0], bad_config, index)["status"] == "disabled"
    assert check(specs[1], bad_config, index)["status"] == "compatible"


def test_v2_compact_receipt_round_trips_identically_and_reduces_bytes(tmp_path, monkeypatch):
    from spicy_regs.sources import publication

    specs, index, v1, consumer = _shared_multi_view_fixture(tmp_path)
    v2 = release.compact_receipt(v1)
    assert v2["version"] == 2
    assert release.compact_receipt(v2) == v2
    assert set(v2["shared"]) == {
        "acceptance_receipts",
        "dependencies",
        "interpretation",
        "population",
        "as_of",
        "evidence_generations",
    }
    assert v2["views"][specs[0].view.name]["output_dependencies"] == ["fec_receipts"]
    assert v2["views"][specs[0].view.name]["dependency_refs"] == {"members": "1"}
    assert "dependencies" not in v2["views"][specs[0].view.name]
    raw_v1 = release.serialize_receipt(v1)
    raw_v2 = release.serialize_receipt(v2)
    assert len(raw_v2) < len(raw_v1)
    pin_v1, pin_v2 = release.sha256(raw_v1), release.sha256(raw_v2)
    parsed_v1 = release.parse_receipt(raw_v1, pin_v1)
    parsed_v2 = release.parse_receipt(raw_v2, pin_v2)
    assert parsed_v2["version"] == 2 and "shared" not in parsed_v2
    for key in ("format", "consumer", "output_membership", "recovery", "views"):
        assert parsed_v2[key] == parsed_v1[key]
    # Expanded shared objects must be isolated copies, never aliased across views.
    parsed_v2["views"][specs[0].view.name]["acceptance_receipts"].append(digest("mutated"))
    parsed_v2["views"][specs[0].view.name]["dependencies"]["fec_receipts"]["generation"] = digest("mutated")
    assert parsed_v2["views"][specs[1].view.name]["acceptance_receipts"] == parsed_v1["views"][specs[1].view.name]["acceptance_receipts"]
    assert parsed_v2["output_membership"]["fec_receipts"] == parsed_v1["output_membership"]["fec_receipts"]
    # Remote load_receipt normalizes v2 cleanly.
    monkeypatch.setattr(publication, "_bounded_get", lambda *a, **k: raw_v2)
    assert release.load_receipt(pin_v2, "https://fixture.invalid") == release.parse_receipt(raw_v2, pin_v2)
    # Full check_view + release_summary parity between v1 and v2.
    cfg_v1 = capture(tmp_path / "v1", specs, v1, consumer) if (tmp_path / "v1").mkdir() is None else None
    cfg_v2 = capture(tmp_path / "v2", specs, v2, consumer) if (tmp_path / "v2").mkdir() is None else None
    assert cfg_v1 is not None and cfg_v2 is not None
    for spec in specs:
        res_v1 = check(spec, cfg_v1, index)
        res_v2 = check(spec, cfg_v2, index)
        assert res_v1["status"] == res_v2["status"] == "compatible"
        assert {**res_v2, "receipt_sha256": None} == {**res_v1, "receipt_sha256": None}
        assert {**release.release_summary(res_v2), "receipt_sha256": None} == {
            **release.release_summary(res_v1), "receipt_sha256": None
        }


@pytest.mark.parametrize(
    "fault",
    [
        "table_digest",
        "table_schema",
        "evidence_gen",
        "consumer_code",
        "sql_drift",
        "policy_drift",
        "bad_dependency",
        "missing_evidence_decl",
        "empty_acceptance",
        "unretained_gen",
    ],
)
def test_v2_refusal_outcomes_match_v1_exactly(tmp_path, fault):
    specs, index, v1, consumer = _shared_multi_view_fixture(tmp_path)
    name = specs[0].view.name
    actual_consumer = deepcopy(consumer)
    changed_index = deepcopy(index)
    if fault == "table_digest":
        changed_index["families"]["members"]["tables"]["members.parquet"]["sha256"] = digest("drifted")
    elif fault == "table_schema":
        changed_index["families"]["members"]["tables"]["members.parquet"]["columns"] = [["id", "BIGINT"]]
    elif fault == "evidence_gen":
        pin = digest("later-proof")
        changed_index["families"]["source-proof"].update(artifactDigest=pin, prefix="generations/source-proof/" + pin[7:])
    elif fault == "consumer_code":
        actual_consumer["code_sha256"] = digest("different-code")
    elif fault == "sql_drift":
        v1["views"][name]["sql_sha256"] = digest("different-sql")
    elif fault == "policy_drift":
        v1["views"][name]["interpretation"]["policies"]["policies/1"] = digest("different-policy")
    elif fault == "bad_dependency":
        v1["views"][name]["dependencies"]["members"] = None
    elif fault == "missing_evidence_decl":
        del v1["views"][name]["evidence_generations"]
    elif fault == "empty_acceptance":
        v1["views"][name]["acceptance_receipts"] = []
    elif fault == "unretained_gen":
        v1["recovery"]["retained_generations"]["members"] = [digest("other-gen")]

    v2 = release.compact_receipt(v1)
    dir_v1, dir_v2 = tmp_path / "v1", tmp_path / "v2"
    dir_v1.mkdir()
    dir_v2.mkdir()
    cfg_v1 = capture(dir_v1, specs, v1, actual_consumer)
    cfg_v2 = capture(dir_v2, specs, v2, actual_consumer)
    saw_disabled = False
    for spec in specs:
        res_v1 = check(spec, cfg_v1, changed_index)
        res_v2 = check(spec, cfg_v2, changed_index)
        assert {**res_v2, "receipt_sha256": None} == {**res_v1, "receipt_sha256": None}
        if res_v1["status"] == "disabled":
            saw_disabled = True
    assert saw_disabled


@pytest.mark.parametrize(
    "case,pattern",
    [
        ("missing_shared_section", "Unknown shared acceptance_receipts"),
        ("dangling_acceptance_ref", "Unknown shared acceptance_receipts"),
        ("dangling_interpretation_ref", "Unknown shared interpretation"),
        ("dangling_population_ref", "Unknown shared population"),
        ("dangling_as_of_ref", "Unknown shared as_of"),
        ("dangling_evidence_ref", "Unknown shared evidence_generations"),
        ("dangling_dependency_ref", "Unknown shared dependencies"),
        ("dangling_output_dependency", "Unknown output_membership dependency"),
        ("inline_and_ref_conflict", "repeats inline and shared"),
        ("dup_inline_and_output_dep", "Duplicate dependency table"),
        ("dup_within_output_deps", "Duplicate dependency table"),
        ("dup_output_and_dep_ref", "Duplicate dependency table"),
        ("unreferenced_shared_entry", "Unreferenced shared"),
        ("unknown_shared_section", "shared section"),
        ("empty_shared_section", "shared section"),
        ("empty_shared_inventory", "Invalid shared acceptance_receipts"),
        ("bad_shared_acceptance_digest", "Invalid shared acceptance_receipts"),
        ("unknown_top_level_key", "format or fields"),
        ("empty_output_dependencies", "Invalid output_dependencies"),
        ("empty_dependency_refs", "Invalid dependency_refs"),
    ],
)
def test_v2_invalid_shared_sections_and_dangling_refs_refuse(tmp_path, case, pattern):
    specs, _, v1, _ = _shared_multi_view_fixture(tmp_path)
    v2 = release.compact_receipt(v1)
    name = specs[0].view.name
    if case == "missing_shared_section":
        del v2["shared"]
    elif case == "dangling_acceptance_ref":
        v2["views"][name]["acceptance_receipts_ref"] = "missing"
    elif case == "dangling_interpretation_ref":
        v2["views"][name]["interpretation_ref"] = "missing"
    elif case == "dangling_population_ref":
        v2["views"][name]["population_ref"] = "missing"
    elif case == "dangling_as_of_ref":
        v2["views"][name]["as_of_ref"] = "missing"
    elif case == "dangling_evidence_ref":
        v2["views"][name]["evidence_generations_ref"] = "missing"
    elif case == "dangling_dependency_ref":
        v2["views"][name]["dependency_refs"]["members"] = "missing"
    elif case == "dangling_output_dependency":
        v2["views"][name]["output_dependencies"].append("nonexistent_table")
    elif case == "inline_and_ref_conflict":
        v2["views"][name]["acceptance_receipts"] = [digest("inline")]
    elif case == "dup_inline_and_output_dep":
        v2["views"][name]["dependencies"] = {"fec_receipts": deepcopy(v2["output_membership"]["fec_receipts"])}
    elif case == "dup_within_output_deps":
        v2["views"][name]["output_dependencies"].append("fec_receipts")
    elif case == "dup_output_and_dep_ref":
        v2["views"][name]["dependency_refs"]["fec_receipts"] = "1"
    elif case == "unreferenced_shared_entry":
        v2["shared"]["acceptance_receipts"]["unused"] = [digest("unused")]
    elif case == "unknown_shared_section":
        v2["shared"]["sql"] = {"1": "DROP TABLE members"}
    elif case == "empty_shared_section":
        v2["shared"] = {}
    elif case == "empty_shared_inventory":
        v2["shared"]["acceptance_receipts"] = {}
    elif case == "bad_shared_acceptance_digest":
        v2["shared"]["acceptance_receipts"]["1"] = ["not-a-digest"]
    elif case == "unknown_top_level_key":
        v2["compression"] = "gzip"
    elif case == "empty_output_dependencies":
        v2["views"][name]["output_dependencies"] = []
    elif case == "empty_dependency_refs":
        v2["views"][name]["dependency_refs"] = {}
    raw = release.serialize_receipt(v2)
    with pytest.raises(ValueError, match=pattern):
        release.parse_receipt(raw, release.sha256(raw))


def test_v2_unknown_view_key_isolates_affected_view_and_refuses_compaction(tmp_path):
    specs, index, v1, consumer = _shared_multi_view_fixture(tmp_path)
    name = specs[0].view.name
    v2 = release.compact_receipt(v1)
    v2["views"][name]["acceptance_receipt_ref"] = "1"
    config = capture(tmp_path, specs, v2, consumer)
    assert check(specs[0], config, index)["status"] == "disabled"
    assert check(specs[1], config, index)["status"] == "compatible"
    v1["views"][name]["sql"] = "DROP TABLE members"
    with pytest.raises(ValueError, match="view fields"):
        release.compact_receipt(v1)


@pytest.mark.parametrize("bad_version", [0, 3, -1, "2", True, False, None, 2.0])
def test_corrupt_digest_and_unsupported_versions_refuse(tmp_path, bad_version):
    _, _, v1, _ = _shared_multi_view_fixture(tmp_path)
    v2 = release.compact_receipt(v1)
    v2["version"] = bad_version
    raw = release.serialize_receipt(v2)
    with pytest.raises(ValueError, match="format or fields"):
        release.parse_receipt(raw, release.sha256(raw))
    good_raw = release.serialize_receipt(release.compact_receipt(v1))
    with pytest.raises(ValueError, match="differ"):
        release.parse_receipt(good_raw, digest("wrong-digest"))
    with pytest.raises(ValueError, match="limit"):
        release.parse_receipt(good_raw, "not-a-digest")
    dup = b'{"shared":{"acceptance_receipts":{"1":[],"1":[]}}}'
    with pytest.raises(ValueError, match="repeats"):
        release.parse_receipt(dup, release.sha256(dup))


def test_v2_enforces_unchanged_1mib_limit_and_recovers_headroom_for_large_receipts(tmp_path):
    assert release.LIMIT == 1024 * 1024
    specs, index, v1, consumer = _shared_multi_view_fixture(tmp_path)
    # Construct a 71-view / 66-acceptance-digest v1 receipt near/over 1 MiB.
    acceptance_66 = [digest(f"acc-{i}") for i in range(66)]
    base_view = v1["views"][specs[0].view.name]
    large_desc = deepcopy(v1["output_membership"]["fec_receipts"])
    large_desc["descriptor"]["columns"] = [[f"col_{i:03d}", "VARCHAR"] for i in range(400)]
    v1["output_membership"]["fec_receipts"] = large_desc
    v1["views"] = {
        f"fec_view_{i:02d}": {
            **deepcopy(base_view),
            "sql_sha256": digest(f"sql-{i}"),
            "dependencies": {
                "fec_receipts": deepcopy(large_desc),
                "members": deepcopy(base_view["dependencies"]["members"]),
            },
            "acceptance_receipts": list(acceptance_66),
        }
        for i in range(71)
    }
    raw_v1 = release.serialize_receipt(v1)
    assert len(raw_v1) > release.LIMIT
    with pytest.raises(ValueError, match="limit"):
        release.parse_receipt(raw_v1, release.sha256(raw_v1))

    v2 = release.compact_receipt(v1)
    raw_v2 = release.serialize_receipt(v2)
    assert len(raw_v2) < release.LIMIT // 4
    parsed_v2 = release.parse_receipt(raw_v2, release.sha256(raw_v2))
    assert parsed_v2["views"] == v1["views"]

    # An oversized v2 receipt still fails the unchanged 1 MiB byte bound via parse_receipt and load_receipt.
    oversized = raw_v2 + b" " * (release.LIMIT + 1 - len(raw_v2))
    assert len(oversized) == release.LIMIT + 1
    oversized_pin = release.sha256(oversized)
    with pytest.raises(ValueError, match="limit"):
        release.parse_receipt(oversized, oversized_pin)
    oversized_path = tmp_path / "oversized-v2.json"
    oversized_path.write_bytes(oversized)
    with pytest.raises(ValueError, match="limit"):
        release.load_receipt(oversized_pin, "https://fixture.invalid", local_path=oversized_path)

    # An expansion-bomb v2 receipt under 1 MiB wire size still fails the expanded byte safety cap.
    huge_acceptance = [digest(f"bomb-{i}") for i in range(4000)]
    bomb = deepcopy(v2)
    bomb["shared"]["acceptance_receipts"] = {"1": huge_acceptance}
    bomb["views"] = {
        f"v_{i}": {
            "sql_sha256": digest(f"bsql-{i}"),
            "acceptance_receipts_ref": "1",
            "interpretation_ref": "1",
            "population_ref": "1",
            "as_of_ref": "1",
            "evidence_generations_ref": "1",
            "output_dependencies": ["fec_receipts"],
            "dependency_refs": {"members": "1"},
        }
        for i in range(100)
    }
    bomb_raw = release.serialize_receipt(bomb)
    assert len(bomb_raw) < release.LIMIT
    with pytest.raises(ValueError, match="Expanded release receipt exceeds byte limit"):
        release.parse_receipt(bomb_raw, release.sha256(bomb_raw))
