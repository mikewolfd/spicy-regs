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
    specs = tuple(release.QualifiedView(v, "mapping/1", "identity/1", identity_files, "selected-fixture", "fixture-as-of") for v in views)
    configuration = release.capture_configuration(specs, receipt_digest=None, image_digest=None, base_url="unused",
                                                  consumer=consumer)
    receipt = dict(format=release.FORMAT, version=release.VERSION,
                   output_membership={table: release.captured_table(index, table) for table in ["fec_receipts", "fec_reports"]},
                   consumer=consumer, recovery={"source_archive_sha256": digest("archive"),
                                                "retained_generations": {family: [entry["artifactDigest"]] for family, entry in index["families"].items()},
                                                "rollback_receipts": []}, views={})
    for spec in specs:
        state = configuration["views"][spec.view.name]
        receipt["views"][spec.view.name] = {
            "dependencies": {name: release.captured_table(index, name) for name in spec.view.required},
            **{key: state[key] for key in ["sql_sha256", "interpretation", "population", "as_of"]},
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
assert fec_release.VERSION == 1
assert mcp_server.FEC_QUALIFIED_VIEWS == ()
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
