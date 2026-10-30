"""Paired query filters refer to one identity, not two records in a response."""

import json

import pytest

from spicy_regs.source_catalog.__main__ import main
from spicy_regs.source_catalog.raw_archive import merge_content
from spicy_regs.source_catalog.raw_content import extract_metadata
from spicy_regs.source_catalog.raw_mapping import SourceMapper
from spicy_regs.source_catalog.storage import connect, schema


def fact(target, value, locator, *, origin="content", namespace=None, **extra):
    return {
        "origin": origin,
        "target": target,
        "value": value,
        "locator": locator,
        "rule": "primary-native-identity" if target == "native_id" else "native-document-version",
        **({"namespace": namespace} if namespace else {}),
        **extra,
    }


def record(identity, version, path):
    return [
        fact("native_id", identity, path + "/id", namespace="test-record"),
        fact("source_version", version, path + "/version"),
    ]


@pytest.fixture
def query(tmp_path, monkeypatch, capsys):
    def run(mappings, *filters):
        index = tmp_path / "index.sqlite"
        index.unlink(missing_ok=True)
        with connect(index) as db:
            schema(db)
            db.execute("INSERT INTO collections VALUES ('test', 'Test', '{}')")
            for name, mapping in mappings.items():
                db.execute(
                    "INSERT INTO files VALUES (?, 'test', NULL, ?, 'held', NULL, NULL, NULL, ?)",
                    (name, name, json.dumps({"mapping": mapping})),
                )
        monkeypatch.setattr("sys.argv", ["catalog", "query", "--index", str(index), *filters])
        main()
        return [row["id"] for row in json.loads(capsys.readouterr().out)]

    return run


def test_id_and_version_must_belong_to_the_same_record(query):
    mapping = {"evidence": record("A", "v1", "/results/1") + record("B", "v2", "/results/10")}
    assert query({"page": mapping}, "--native-id", "A", "--source-version", "v2") == []
    assert query({"page": mapping}, "--native-id", "A", "--source-version", "v1") == ["page"]
    assert query({"page": mapping}, "--native-id", "B") == ["page"]
    assert query({"page": mapping}, "--source-version", "v2") == ["page"]


def test_scalar_only_mappings_remain_supported(query):
    mapping = {"native_id": "package", "source_version": "edition"}
    assert query({"scalar": mapping}, "--native-id", "package", "--source-version", "edition") == ["scalar"]


def test_normalized_integer_receipt_scalars_remain_supported(query):
    mapping = SourceMapper().map_source({"native_id": 1, "source_version": 2025})
    assert query({"scalar": mapping}, "--native-id", "1", "--source-version", "2025") == ["scalar"]


def test_receipt_pair_remains_supported_with_child_content_identities(query):
    mapping = {
        "native_id": "package",
        "source_version": "edition",
        "evidence": [
            fact("native_id", "package", "record.native_id", origin="receipt"),
            fact("source_version", "edition", "record.source_version", origin="receipt"),
            *record("child", "draft", "/results/0"),
        ],
    }
    assert query({"page": mapping}, "--native-id", "package", "--source-version", "edition") == ["page"]
    assert query({"page": mapping}, "--native-id", "child", "--source-version", "edition") == []
    assert query({"page": mapping}, "--native-id", "package", "--source-version", "draft") == []


def test_root_content_identity_and_version_do_not_belong_to_child_records(query):
    mapping = {"evidence": record("package", "edition", "") + record("child", "draft", "/results/0")}
    assert query({"page": mapping}, "--native-id", "child", "--source-version", "edition") == []
    assert query({"page": mapping}, "--native-id", "package", "--source-version", "draft") == []
    assert query({"page": mapping}, "--native-id", "package", "--source-version", "edition") == ["page"]


@pytest.mark.parametrize("receipt_field", ["native_id", "source_version"])
def test_merged_scalars_cannot_borrow_a_value_from_another_origin(query, receipt_field):
    values = {"native_id": "capture", "source_version": "captured-version"}
    mapping = {
        receipt_field: values[receipt_field],
        "evidence": [fact(receipt_field, values[receipt_field], "record." + receipt_field, origin="receipt")],
    }
    mapping = merge_content(
        mapping,
        {
            "outcome": "ok",
            "native_id": "child",
            "source_version": "draft",
            "evidence": record("child", "draft", "/results/0"),
        },
        "sha256:test",
    )
    assert (
        query({"page": mapping}, "--native-id", mapping["native_id"], "--source-version", mapping["source_version"])
        == []
    )
    assert query({"page": mapping}, "--native-id", "child", "--source-version", "draft") == ["page"]


@pytest.mark.parametrize("version_namespace,expected", [(None, []), ("capture", []), ("test-record", ["page"])])
def test_untagged_version_cannot_choose_between_identity_namespaces(query, version_namespace, expected):
    evidence = [
        fact("native_id", "child", "/id", namespace="test-record"),
        fact("native_id", "capture", "/capture_id", namespace="capture"),
        fact("source_version", "v1", "/version", namespace=version_namespace),
    ]
    assert query({"page": {"evidence": evidence}}, "--native-id", "child", "--source-version", "v1") == expected


@pytest.mark.parametrize("change", ["missing-locator", "missing-namespace", "invalid", "sibling-record"])
def test_ambiguous_content_pair_is_not_rescued_by_scalar_values(query, change):
    evidence = record("A", "v1", "/results/0")
    if change == "missing-locator":
        evidence[0].pop("locator")
    elif change == "missing-namespace":
        evidence[0].pop("namespace")
    elif change == "invalid":
        evidence[1]["valid"] = False
    else:
        evidence[1]["locator"] = "/results/1/version"
    mapping = {"native_id": "A", "source_version": "v1", "evidence": evidence}
    assert query({"page": mapping}, "--native-id", "A", "--source-version", "v1") == []


@pytest.mark.parametrize(
    "body,identity,version",
    [
        (
            b'{"packageId":"BILLS-118hr1ih","congress":118,"billType":"hr","billNumber":1,"billVersion":"ih"}',
            "BILLS-118hr1ih",
            "ih",
        ),
        (
            b'<bill bill-stage="Introduced-in-House"><form><congress>118th CONGRESS</congress>'
            b"<legis-num>H.R. 1</legis-num></form></bill>",
            "hr1-118",
            "Introduced-in-House",
        ),
        (
            b'<mods xmlns="http://www.loc.gov/mods/v3"><extension><collectionCode>BILLS</collectionCode>'
            b"<congress>118</congress><docClass>hr</docClass><billNumber>1</billNumber>"
            b"<billVersion>ih</billVersion><accessId>BILLS-118hr1ih</accessId></extension></mods>",
            "BILLS-118hr1ih",
            "ih",
        ),
    ],
)
def test_supported_native_document_shapes_preserve_their_version_pair(query, body, identity, version):
    content = extract_metadata("family:bill-family", body)
    assert content["outcome"] == "ok"
    mapping = merge_content({}, content, "sha256:test")
    assert query({"document": mapping}, "--native-id", identity, "--source-version", version) == ["document"]


def test_paired_filter_runs_before_limit_and_offset(query):
    false_match = {"evidence": record("A", "v0", "/results/0") + record("B", "v1", "/results/1")}
    same_record = {"evidence": record("A", "v1", "/results/0")}
    mappings = {"0-false": false_match, "1-true": same_record, "2-true": same_record}
    assert query(mappings, "--native-id", "A", "--source-version", "v1", "--limit", "1", "--offset", "1") == ["2-true"]
