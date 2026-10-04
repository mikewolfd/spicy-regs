"""Retained FEC observation meanings through MCP; all release fixtures are local.

Input/expected decision: mcp-chaos-2026-10-02/audit-asha/
11-three-name-detail.json and independently written 18-independent-source-analysis.sql.
Penton, FEC report 1966662, transaction 9444692: reported USD 50, blank memo code.
The audit also checked the official native filing; this is NOT current net money.
Only the necessary public fields are transcribed below, not a generated policy oracle.
"""

from copy import deepcopy
from decimal import Decimal
import json
import base64
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

import duckdb
import pytest
from starlette.testclient import TestClient

from spicy_regs import fec_release, mcp_server as server
from spicy_regs.fec_receipt_adapter import processing_declarations, qualified_views
from spicy_regs.etl_receipts import ReceiptContext
from spicy_regs.sources import publication
from spicy_regs.transforms.fec_subject_receipts import write_fec_subjects
from spicy_regs.relationship_views.sql_views import SQLView, install_sql_views, view_columns
from tests.test_fec_release import digest
from tests.test_mcp_query_results import call
from tests.test_mcp_server import _records, _tool_data

RECORD = "sha256:e96bf34c98722f2d6157a95155748c7243f9b73accf45e1d963b9ce932ea32e9"
NAME = "fec_receipts_source_analysis_decision"
SOURCE = "sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0"
POPULATION = "Retained source observations, not deduplicated transactions or a current financial population."
AS_OF = "Sealed 2026-09-30T23:37:04.213958+00:00; not a common publisher-current date."
OBSERVATION = {
    "record_id": RECORD,
    "identity_version": "fec-typed-observation/1",
    "mapping_version": "fec-bulk-individual-receipt/2",
    "value_mapping_version": "fec-exact-financial-values/2",
    "mapping_status": "mapped",
    "source_authority": "official-fec",
    "source_namespace": "fec-bulk-individual-contributions",
    "source_representation_role": "snapshot",
    "correction_operation": "none",
    "amount": Decimal("50.000000000"),
    "amount_raw": "50",
    "amount_status": "exact",
    "currency": "USD",
    "memo_indicator": "",
}


@pytest.fixture
def financial_server(tmp_path, monkeypatch, request):
    specs = tuple(s for s in qualified_views(dict(
        source_generation_pin=SOURCE, population=POPULATION, as_of=AS_OF, namespace_evidence={},
    )) if s.view.name in {"fec_receipts_" + purpose + "_decision" for purpose in (
        "source_analysis", "detailed_summary_component", "gross_receipts", "net_receipts",
    )})
    generation = "financial-native-fixture"
    declaration = processing_declarations()["fec_receipts"]
    schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declaration["arrow_schema"])))
    observation = {**OBSERVATION, "memo_indicator": getattr(request, "param", "")}
    (subject, etl), _ = write_fec_subjects(
        [observation], tmp_path / "native", table="fec_receipts", input_schema=schema,
        generation_id=generation,
        context_for=lambda row, ordinal: ReceiptContext(generation, str(ordinal), "financial-fixture",
            [{"source_id": "retained-fec-row", "sha256": SOURCE, "locator": RECORD}]),
    )
    assert subject is not None
    native = pq.ParquetFile(subject).read()
    columns = [[c["column_name"], c["column_type"]] for c in server._table_metadata()["fec_receipts"]["columns"]]
    prefix = "generations/fec-query/" + digest("local-financial-fixture")[7:]
    locations, members = {}, []
    def identity(path):
        info = publication.file_identity(path)
        return {"sha256": info["sha256"], "byteSize": info["bytes"], "rows": pq.read_metadata(path).num_rows}
    # Many captured members exercise the compact response budget with real bytes.
    for i in range(26):
        key = f"fec_receipts/currency=USD/part-{i:06}.parquet"
        path = tmp_path / f"part-{i}.parquet"
        pq.write_table(native if i == 0 else native.slice(0, 0), path)
        locations[prefix + "/" + key] = path
        members.append({"key": key, **identity(path), "partition": {"currency": "USD"}})
    descriptor = {"columns": columns, "rows": 1, "byteSize": sum(p["byteSize"] for p in members),
                  "partitionColumns": ["currency"], "members": members}
    locations[prefix + "/etl_receipts.parquet"] = etl
    index = {"families": {
        "fec-query": {"artifactDigest": digest("local-financial-fixture"), "prefix": prefix,
                      "tables": {"fec_receipts.parquet": descriptor},
                      "etlReceipts": {"key": "etl_receipts.parquet", **identity(etl),
                                      "datasets": ["fec_receipts"], "generationId": generation}},
        "fec-observations": {"artifactDigest": SOURCE, "prefix": f"generations/fec-observations/{SOURCE[7:]}",
                             "tables": {}},
    }}
    def fetch(base, member, destination, *args, **kwargs):
        source = locations[member.path]
        assert (member.sha256, member.byte_size) == (identity(source)["sha256"], identity(source)["byteSize"])
        shutil.copyfile(source, destination)
        return True
    monkeypatch.setattr(publication, "fetch_member", fetch)
    consumer = {"image_digest": digest("test-image"), "code_sha256": digest("test-code"),
                "package_versions": {"test": "1"}}
    config = fec_release.capture_configuration(specs, receipt_digest=None, image_digest=None,
                                               base_url="unused", consumer=consumer)
    receipt = {
        "format": fec_release.FORMAT, "version": fec_release.VERSION, "consumer": consumer,
        "output_membership": {"fec_receipts": fec_release.captured_table(index, "fec_receipts")},
        "recovery": {"source_archive_sha256": digest("test-archive"), "rollback_receipts": [],
                     "retained_generations": {f: [e["artifactDigest"]] for f, e in index["families"].items()}},
        "views": {s.view.name: {
            "dependencies": {"fec_receipts": fec_release.captured_table(index, "fec_receipts")},
            **{k: config["views"][s.view.name][k] for k in (
                "sql_sha256", "interpretation", "population", "as_of", "evidence_generations",
            )}, "acceptance_receipts": [digest("test-acceptance")],
        } for s in specs},
    }
    raw = json.dumps(receipt).encode()
    path = tmp_path / "release.json"
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_FILE", str(path))
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", fec_release.sha256(raw))
    monkeypatch.setattr(fec_release, "runtime_consumer", lambda _: deepcopy(consumer))
    monkeypatch.setattr(server, "FEC_QUALIFIED_VIEWS", specs)
    monkeypatch.setattr(server, "TABLES", ("fec_receipts",))
    monkeypatch.setattr(server, "DATA_DIR", None)
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MB"}) as con:
        con.register("native_input", native)
        con.execute("CREATE TABLE fec_receipts AS SELECT * FROM native_input")
        con.unregister("native_input")
        con.execute("CREATE TABLE _spicy_publication(snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        yield server.build_server(), con, specs, receipt


def test_retained_decision_rows_and_warnings_are_not_presentation_generated(financial_server):
    mcp, _, _, _ = financial_server
    result = _tool_data(mcp, "query_sql", {"sql": f"""SELECT target_record_id, status, reason, value,
        measure_basis, warnings, current_financial_total_qualified FROM {NAME}"""})
    assert _records(result) == [{
        "target_record_id": RECORD, "status": "eligible", "reason": "source_defined_purpose",
        "value": "50.000000000", "measure_basis": "signed_reported_observation",
        "warnings": ["source_observation_not_current_net_money"], "current_financial_total_qualified": False,
    }]


def test_value_only_keeps_registry_limits_and_exact_release_scope(financial_server):
    mcp, con, specs, receipt = financial_server
    before = deepcopy(server._publication_status(con))
    relationships = deepcopy(server._connection_relationships(con))
    captured = deepcopy(server._pinned_record(con, "_spicy_fec_release"))
    assert captured is not None
    with TestClient(server.build_app()) as client:
        wire = call(client, "query_sql", {"sql": f"SELECT value FROM {NAME}", "max_rows": 1})
    assert not wire["isError"]
    reply = wire["structuredContent"]
    assert _records(reply) == [{"value": "50.000000000"}]
    assert reply["columns"] == ["value"] and not reply["truncated"]
    pin = reply["publication"][NAME]
    assert pin["meaning"] == next(s.view.meaning for s in specs if s.view.name == NAME)
    assert "current financial totals and group additivity are never qualified" in pin["meaning"]
    assert "Eligibility is limited to the named purpose" in pin["meaning"]
    assert pin["coverage"] == "derived"
    assert pin["input_publications"]["fec_receipts"]["artifact_digest"] == digest("local-financial-fixture")
    release = pin["release_compatibility"]
    assert release["status"] == "compatible"
    assert release["receipt_sha256"] == captured["receipt_sha256"]
    assert release["sql_sha256"] == receipt["views"][NAME]["sql_sha256"]
    assert release["evidence_generations"] == {"fec-observations": SOURCE}
    assert (release["population"], release["as_of"]) == (POPULATION, AS_OF)
    assert "describe_table" in pin["details"] and "refresh" in pin["details"]
    assert "Compare" in pin["details"] and "pins" in pin["details"]
    assert set(release) == {"status", "receipt_sha256", "sql_sha256", "evidence_generations", "population", "as_of"}
    assert len(json.dumps(reply["publication"], separators=(",", ":")).encode()) < 4096
    description = _tool_data(mcp, "describe_table", {"table": NAME, "detail": True})
    assert description["publication"]["release_compatibility"] == relationships[NAME]["release_compatibility"]
    assert len(json.dumps(description["publication"]).encode()) > 4096
    # Response edits and a later description cannot mutate the admission/provenance pins.
    release["evidence_generations"]["fec-observations"] = "client edit"
    pin["input_publications"]["fec_receipts"]["artifact_digest"] = "client edit"
    assert server._publication_status(con) == before
    assert server._connection_relationships(con) == relationships
    assert server._pinned_record(con, "_spicy_fec_release") == captured


def test_financial_columns_explain_actual_decision_and_do_not_override_other_status(financial_server):
    mcp, con, _, _ = financial_server
    description = _tool_data(mcp, "describe_table", {"table": NAME})
    columns = {c["column_name"]: c["description"] for c in description["columns"]}
    assert "bulk_source_analysis" in columns["query"]
    assert all(word in columns["status"] for word in ("eligible", "excluded", "refused", "purpose"))
    assert "signed_reported_observation" in columns["measure_basis"]
    assert "not" in columns["measure_basis"] and "net" in columns["measure_basis"]
    assert "eligible" in columns["warnings"]
    assert "false" in columns["current_financial_total_qualified"].lower()
    assert all("described by this view" not in text for text in columns.values())
    assert "column_descriptions" not in description["metadata"]  # Only one column meaning list.
    view = SQLView("unrelated_status", {}, lambda _: "SELECT 'open' AS status", "Non-financial", ("status",))
    entry = install_sql_views(con, [], [view])[view.name]
    other = view_columns(con.execute("DESCRIBE unrelated_status").fetchall(), entry["metadata"].get("column_descriptions"))
    assert other[0]["description"] is None  # undeclared, so undescribed: no financial text leaks into another view


@pytest.mark.parametrize(("purpose", "financial_server", "status", "value"), [
    ("source_analysis", "X", "eligible", "50.000000000"),
    ("detailed_summary_component", "X", "excluded", None),
    ("gross_receipts", "", "refused", None),
    ("net_receipts", "", "refused", None),
], indirect=["financial_server"])
def test_purpose_distinctions_follow_publisher_memo_rule(financial_server, purpose, status, value):
    # Retained FEC individual-file description: X is not in the detailed summary,
    # but memo items should be included in source analysis. No net total follows.
    mcp, _, _, _ = financial_server
    reply = _tool_data(mcp, "query_sql", {"sql": f"SELECT status, value, current_financial_total_qualified "
                                       f"FROM fec_receipts_{purpose}_decision"})
    assert _records(reply) == [{"status": status, "value": value, "current_financial_total_qualified": False}]


@pytest.mark.parametrize("where", ["", "WHERE FALSE"])
def test_mixed_and_empty_results_keep_each_named_scope_without_adding_row_columns(financial_server, where):
    mcp, _, _, _ = financial_server
    sql = f"SELECT d.value, r.memo_indicator FROM {NAME.upper()} d JOIN fec_receipts r " \
          f"ON d.target_record_id = r.record_id {where}"
    reply = _tool_data(mcp, "query_sql", {"sql": sql})
    assert reply["sql"] == sql
    assert _records(reply) == ([] if where else [{"value": "50.000000000", "memo_indicator": ""}])
    assert reply["columns"] == ["value", "memo_indicator"]
    assert set(reply["publication"]) == {NAME, "fec_receipts"}
    assert "meaning" in reply["publication"][NAME]
    raw = reply["publication"]["fec_receipts"]
    assert raw["coverage"] == "sampled" and raw["status"] == "managed_generation"
    assert "release_compatibility" not in raw
    assert raw == _tool_data(mcp, "describe_table", {"table": "fec_receipts"})["publication"]


def test_one_row_qualified_publication_budget(financial_server):
    mcp, _, _, _ = financial_server
    reply = _tool_data(mcp, "query_sql", {"sql": f"SELECT value FROM {NAME}"})
    assert _records(reply) == [{"value": "50.000000000"}]
    assert len(json.dumps(reply["publication"], separators=(",", ":")).encode()) < 4096
