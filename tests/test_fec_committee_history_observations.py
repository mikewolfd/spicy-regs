"""Committee history snapshots preserve native fields and evidence boundaries."""

import copy
import json

import pytest
from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS, HEADER_URL, committee_master_url
from spicy_docs.sources.fec.postgres import TABLE, decoder_arguments

from spicy_regs.transforms import fec_committee_history_observations as history
from spicy_regs.transforms.fec_typed_batch import typed_batch

PIN = "sha256:" + "a" * 64
GEN = "sha256:" + "b" * 64
DATA = "sha256:" + "c" * 64
SCHEMA = "sha256:" + "d" * 64


def master():
    header_source = dict(sha256=SCHEMA, byte_offset=0, byte_length=158, encoding="utf-8")
    header_locator = dict(
        **header_source, collection_id="header", source_record_id=f"{SCHEMA}/original/{0:020d}", ordinal=0, member=None
    )
    header = dict(
        collection_id="header",
        source_record_id=header_locator["source_record_id"],
        source_sha256=SCHEMA,
        source_url=HEADER_URL,
        source_locator_json=json.dumps(header_locator),
        metadata_json=json.dumps(dict(kind="row", fields=list(COMMITTEE_MASTER_FIELDS), source=header_source)),
    )
    capture = dict(
        requestUrl=committee_master_url(2024),
        responseSha256=PIN,
        byteSize=1234,
        representation="zip",
        observedAt="2026-09-30T00:00:00Z",
        via="local-retained-byte-verification",
    )
    scope = dict(
        capture=capture,
        format="delimited",
        encoding="utf-8",
        member=dict(ordinal=0, name="cm.txt"),
        delimiter="|",
        quoting="literal",
        max_records_per_page=1000,
        max_record_bytes=131072,
        max_members=10000,
        max_decoded_bytes=67108864,
    )
    entry = dict(
        collection_id="master",
        source_family="fec_committees",
        profile="positional",
        scope=scope,
        field_mapping=dict(
            header_collection_id="header",
            header_row_ordinal=0,
            data_has_header=False,
            relationship_family="committee_master",
            cycle=2024,
        ),
    )
    fields = dict.fromkeys(COMMITTEE_MASTER_FIELDS, "")
    fields.update(CMTE_ID="C00000001", CMTE_NM="  Native name  ", CONNECTED_ORG_NM="Company name, not an ID")
    endpoint = {
        **{k: header[k] for k in ("collection_id", "source_record_id", "source_sha256", "source_url")},
        "source_locator": header_locator,
    }
    locator = dict(
        collection_id="master",
        source_record_id=f"{PIN}/0/{0:020d}",
        ordinal=0,
        member=scope["member"],
        sha256=DATA,
        byte_offset=0,
        byte_length=100,
        encoding="utf-8",
        field_mapping=endpoint,
    )
    row = dict(
        collection_id="master",
        source_record_id=locator["source_record_id"],
        source_sha256=PIN,
        source_url=capture["requestUrl"],
        profile="positional",
        source_locator_json=json.dumps(locator),
        metadata_json=json.dumps(fields),
    )
    return entry, header, row


def postgres():
    capture = dict(
        requestUrl="https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/data-dump/schedules/ofec_committee_history.dump",
        responseSha256=PIN,
        byteSize=1234,
        representation="opaque",
        observedAt="2026-09-30T00:00:00Z",
    )
    columns = [
        dict(name=n, sql_type="text") for n in ("committee_id", "cycle", "empty", "nullable", "array", "escaped")
    ]
    derivation = dict(
        table=TABLE,
        toolVersion="pg_restore (PostgreSQL) 16.14",
        arguments=decoder_arguments(PIN),
        originalSha256=PIN,
        outputs={n: dict(sha256=p, byteSize=1234) for n, p in [("toc", GEN), ("schema", SCHEMA), ("data", DATA)]},
        columns=columns,
    )
    entry = dict(
        collection_id="pg",
        source_family="fec_committees",
        profile="postgres",
        scope=dict(capture=capture, derivation=derivation, max_records_per_page=1000, max_record_bytes=131072),
    )
    source = dict(sha256=DATA, original_sha256=PIN, table=TABLE, byte_offset=123, byte_length=99, encoding="utf-8")
    values = ["C00000001", "1992", "", None, "{}", "a\tb\n\\"]
    raw = ["C00000001", "1992", "", r"\N", "{}", r"a\tb\n\\"]
    native = dict(
        kind="postgres-copy",
        fields=raw,
        values=values,
        named_fields=dict(zip([c["name"] for c in columns], values)),
        source=source,
    )
    locator = dict(
        **source,
        column_schema_sha256=SCHEMA,
        member=None,
        ordinal=0,
        collection_id="pg",
        source_record_id=f"{PIN}/original/{0:020d}",
    )
    row = dict(
        collection_id="pg",
        source_record_id=locator["source_record_id"],
        source_sha256=PIN,
        source_url=capture["requestUrl"],
        profile="postgres",
        metadata_json=json.dumps(native),
        source_locator_json=json.dumps(locator),
    )
    return entry, row


def test_master_reuses_owner_names_preserves_blank_and_snapshot_identity():
    entry, header, row = master()
    prepared = history.prepare_history(entry, source_generation_pin=GEN, header_row=header)
    result, evidence = history.map_history(row, prepared)
    assert typed_batch([result], prepared.schema).to_pylist() == [result]
    assert result["native_fields"] == json.loads(row["metadata_json"])
    assert result["name"] == "  Native name  "
    assert result["candidate_id"] is None and result["native_fields"]["CAND_ID"] == ""
    assert result["cycle"] == "2024" and result["source_cycle"] == 2024
    assert result["current_record_status"] == "unqualified"
    assert [e["role"] for e in evidence] == ["primary", "field_definition"]
    changed = copy.deepcopy(entry)
    changed["collection_id"] = "older-snapshot"
    changed_row = copy.deepcopy(row)
    changed_row["collection_id"] = "older-snapshot"
    loc = json.loads(changed_row["source_locator_json"])
    loc["collection_id"] = "older-snapshot"
    changed_row["source_locator_json"] = json.dumps(loc)
    other, _ = history.map_history(
        changed_row, history.prepare_history(changed, source_generation_pin=GEN, header_row=header)
    )
    assert other["record_id"] != result["record_id"]


@pytest.mark.parametrize("case", ["cycle", "redirect", "header-fields", "header-pin", "member", "delimiter", "family"])
def test_master_selection_refuses_wrong_definition_or_scope(case):
    entry, header, _ = master()
    if case == "cycle":
        entry["field_mapping"]["cycle"] = 2026
    if case == "redirect":
        entry["scope"]["capture"]["resolvedUrl"] = committee_master_url(2022)
    if case == "header-fields":
        native = json.loads(header["metadata_json"])
        native["fields"].reverse()
        header["metadata_json"] = json.dumps(native)
    if case == "header-pin":
        header["source_sha256"] = PIN
    if case == "member":
        entry["scope"]["member"]["name"] = "other.txt"
    if case == "delimiter":
        entry["scope"]["delimiter"] = "\t"
    if case == "family":
        entry["source_family"] = "fec_candidates"
    with pytest.raises(ValueError):
        history.prepare_history(entry, source_generation_pin=GEN, header_row=header)


def test_postgres_native_null_empty_escapes_array_and_columns_roundtrip():
    entry, row = postgres()
    prepared = history.prepare_history(entry, source_generation_pin=GEN)
    result, evidence = history.map_history(row, prepared)
    assert typed_batch([result], prepared.schema).to_pylist() == [result]
    native = json.loads(row["metadata_json"])
    assert result["native_fields"] == native["named_fields"]
    assert result["raw_copy_fields"] == native["fields"]
    assert result["native_fields"]["nullable"] is None and result["native_fields"]["empty"] == ""
    assert result["native_fields"]["array"] == "{}"
    assert result["cycle"] == "1992" and result["source_cycle"] is None
    assert result["current_record_status"] == "unqualified" and len(evidence) == 1
    assert json.loads(result["derivation_json"]) == entry["scope"]["derivation"]
    assert json.loads(result["capture_json"])["responseSha256"] == PIN


@pytest.mark.parametrize(
    "case",
    ["named", "values", "width", "kind", "original", "derived", "schema", "locator", "profile", "url", "position"],
)
def test_postgres_mismatched_native_values_and_coordinates_refuse(case):
    entry, row = postgres()
    prepared = history.prepare_history(entry, source_generation_pin=GEN)
    native = json.loads(row["metadata_json"])
    loc = json.loads(row["source_locator_json"])
    if case == "named":
        native["named_fields"]["empty"] = None
    if case == "values":
        native["values"][2] = 123
    if case == "width":
        native["fields"].pop()
    if case == "kind":
        native["kind"] = "row"
    if case == "original":
        native["source"]["original_sha256"] = GEN
    if case == "derived":
        native["source"]["sha256"] = GEN
    if case == "schema":
        loc["column_schema_sha256"] = GEN
    if case == "locator":
        loc["byte_offset"] += 1
    if case == "profile":
        row["profile"] = "positional"
    if case == "url":
        row["source_url"] += "?other"
    if case == "position":
        loc["ordinal"] = 1
    row["metadata_json"] = json.dumps(native)
    row["source_locator_json"] = json.dumps(loc)
    with pytest.raises(ValueError):
        history.map_history(row, prepared)


def test_postgres_early_derivation_drift_refuses():
    entry, _ = postgres()
    entry["scope"]["derivation"]["arguments"] = {}
    with pytest.raises(ValueError):
        history.prepare_history(entry, source_generation_pin=GEN)
