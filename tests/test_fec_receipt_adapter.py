"""Qualified SQL reads verified receipts without changing native public rows."""
import base64

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.fec_receipt_adapter import ReceiptAdapter, processing_declarations
from spicy_regs.relationship_views.sql_views import SQLView
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.fec_subject_receipts import write_fec_subjects
from tests.test_fec_subject_receipts import context, source_row


def fixture(tmp_path):
    declaration = processing_declarations()["fec_receipts"]
    schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declaration["arrow_schema"])))
    (subject, receipt), policy = write_fec_subjects(
        [source_row()], tmp_path / "bundle", table="fec_receipts", input_schema=schema,
        generation_id="generation-a", context_for=context)
    def descriptor(path):
        identity = file_identity(path)
        return {"sha256": identity["sha256"], "byteSize": identity["bytes"], "rows": 1}
    index = {"families": {"fec-query": {
        "prefix": "generations/fec-query/" + "a" * 64, "artifactDigest": "sha256:" + "a" * 64,
        "tables": {"fec_receipts.parquet": descriptor(subject)},
        "etlReceipts": {**descriptor(receipt), "key": "etl_receipts.parquet", "datasets": ["fec_receipts"],
                        "generationId": "generation-a"}}}}
    return subject, receipt, index


def test_restoration_preserves_subject_and_exact_financial_processing(tmp_path):
    subject, receipt, index = fixture(tmp_path)
    with duckdb.connect() as con:
        con.register("fec_receipts", pq.read_table(subject))
        adapter = ReceiptAdapter(con, index, "unused", local_directory=subject.parent)
        # mapping_status is held only by the receipt, so each query below runs only if its table reference was
        # rebound to the restored relation. amount_status is a column of the public table too (owner decision,
        # 2026-10-05) and could not show that: the public table would answer for it.
        spec = SQLView("receipt_eligibility", {"fec_receipts": ("record_id", "amount_status", "mapping_status")},
                       lambda _: "SELECT record_id, amount_status, mapping_status FROM fec_receipts", "test", ("record_id",))
        prepared = adapter.prepare(spec, spec.query({}))
        assert con.execute(prepared.query({})).fetchall() == [(source_row()["record_id"], "exact", "mapped")]
        public = {r[0] for r in con.execute("DESCRIBE fec_receipts").fetchall()}
        assert "mapping_status" not in public and {"amount_status", "source_namespace"} <= public
        assert con.execute("SELECT amount_status, source_namespace FROM fec_receipts").fetchall() == [
            ("exact", "fec-bulk-individual-contributions")]
        assert adapter.restore("fec_receipts") == adapter.restore("fec_receipts")
        qualified = adapter.prepare(spec, "SELECT fec_receipts.mapping_status FROM fec_receipts")
        assert con.execute(qualified.query({})).fetchall() == [("mapped",)]
        cte = adapter.prepare(spec, "WITH fec_receipts AS (SELECT mapping_status FROM main.fec_receipts) SELECT * FROM fec_receipts")
        assert con.execute(cte.query({})).fetchall() == [("mapped",)]


@pytest.mark.parametrize("fault", ["generation", "bytes", "subject_version", "missing_receipt"])
def test_bad_receipts_never_create_restored_relation(tmp_path, fault):
    subject, receipt, index = fixture(tmp_path)
    if fault == "generation":
        index["families"]["fec-query"]["etlReceipts"]["generationId"] = "other"
    elif fault == "bytes":
        receipt.write_bytes(b"bad")
    elif fault == "missing_receipt":
        receipt.unlink()
    else:
        rows = pq.read_table(subject).to_pylist()
        rows[0]["currency"] = "different"
        pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(subject)), subject)
        identity = file_identity(subject)
        index["families"]["fec-query"]["tables"]["fec_receipts.parquet"].update(
            sha256=identity["sha256"], byteSize=identity["bytes"])
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, index, "unused", local_directory=subject.parent)
        with pytest.raises((ValueError, OSError)):
            adapter.restore("fec_receipts")
        assert con.execute("SHOW TABLES").fetchall() == []


def test_navigation_restores_receipt_only_context_and_exact_relationship_witness(tmp_path):
    import json

    from spicy_regs.relationship_views.fec import FEC_VIEWS
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = tmp_path / 'bundle'
    tables = ('fec_collections', 'fec_source_records', 'fec_relationships')
    locator = json.dumps({'collection_id': 'collection-a', 'source_record_id': 'record-a'})
    with IdentityReceiptWriter(bundle, generation_id='g-nav', tables=tables) as writer:
        writer.emit('fec_collections', {
            'collection_id': 'collection-a', 'source_family': 'bulk', 'profile': 'bulk',
            'record_count': 1, 'relationship_count': 1,
            'requested_scope_json': '["https://www.fec.gov/files/bulk-downloads/2026/cm.zip"]',
        }, input_witness=WITNESS)
        writer.emit('fec_source_records', {
            'collection_id': 'collection-a', 'source_record_id': 'record-a',
            'source_sha256': WITNESS['sha256'], 'source_locator_json': locator,
        }, input_witness=WITNESS)
        writer.emit('fec_relationships', {
            'subject_id': 'C00000001', 'subject_type': 'committee', 'object_id': 'H0CA00001',
            'object_type': 'candidate', 'relationship_type': 'supports', 'value_status': 'reported',
            'source_sha256': WITNESS['sha256'], 'source_locator_json': locator,
            'source_fields_json': '{}', 'cycle': '2026',
        }, input_witness=WITNESS)
    def descriptor(path):
        value = file_identity(path)
        return {'sha256': value['sha256'], 'byteSize': value['bytes'], 'rows': pq.read_metadata(path).num_rows}
    index = {'families': {'fec-source': {
        'prefix': 'generations/fec-source/' + 'b' * 64, 'artifactDigest': 'sha256:' + 'b' * 64,
        'tables': {'fec_relationships.parquet': descriptor(bundle / 'fec_relationships.parquet')},
        'etlReceipts': {**descriptor(bundle / 'etl_receipts.parquet'), 'key': 'etl_receipts.parquet',
                        'generationId': 'g-nav', 'datasets': list(tables)},
    }}}
    with duckdb.connect() as con:
        con.register('fec_relationships', pq.read_table(bundle / 'fec_relationships.parquet'))
        adapter = ReceiptAdapter(con, index, 'unused', local_directory=bundle)
        for spec in FEC_VIEWS:
            prepared = adapter.prepare(spec, spec.query({}))
            con.execute(f'CREATE VIEW {spec.name} AS {prepared.query({})}')
        assert con.execute('SELECT cycle, cycle_status FROM fec_collection_cycles').fetchall() == [('2026', 'bulk_directory')]
        assert con.execute('SELECT target_status, recorded_digest_status FROM fec_relationship_evidence').fetchall() == [('found', 'matches')]
        assert not (bundle / 'fec_collections.parquet').exists()
        assert 'source_locator_json' not in pq.read_schema(bundle / 'fec_relationships.parquet').names


def test_local_discovery_does_not_restore_unselected_published_fec_receipts(monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.sources.publication import empty_index

    index = empty_index()
    index['families']['fec-source'] = {
        'prefix': 'generations/fec-source/' + 'a' * 64, 'tables': {},
        'etlReceipts': {'key': 'etl_receipts.parquet', 'datasets': ['fec_collections', 'fec_source_records']},
    }
    with duckdb.connect() as con:
        monkeypatch.setattr(mcp_server, '_publication_status', lambda _: {'tables': [], 'publication': {}})
        monkeypatch.setattr(mcp_server, '_connection_index', lambda _: index)
        monkeypatch.setattr(mcp_server, '_connection_local_selection', lambda _: {
            'directory': '/not-used', 'receipt_members': {},
        })
        monkeypatch.setattr(mcp_server, '_fec_release_configuration', lambda *_: None)
        monkeypatch.setattr(ReceiptAdapter, 'restore', lambda *_: pytest.fail('unselected receipt read'))
        mcp_server._install_relationship_views(con)
        assert con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='fec_collection_cycles'").fetchall() == [(0,)]


def test_mixed_native_and_old_dependency_refuses_before_any_restoration(tmp_path):
    from spicy_regs.relationship_views.fec import FEC_VIEWS
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = tmp_path / 'native'
    with IdentityReceiptWriter(bundle, generation_id='native', tables=['fec_relationships']) as writer:
        writer.emit('fec_relationships', {'subject_id': 'C00000001', 'subject_type': 'committee',
            'object_id': 'H0CA00001', 'object_type': 'candidate', 'relationship_type': 'supports',
            'value_status': 'reported', 'source_sha256': WITNESS['sha256'],
            'source_locator_json': '{"collection_id":"c","source_record_id":"r"}',
            'source_fields_json': '{}', 'cycle': '2026'}, input_witness=WITNESS)
    selected = {'fec_relationships': {'subjects': [str(bundle / 'fec_relationships.parquet')],
                'receipts': str(bundle / 'etl_receipts.parquet'), 'generation_id': 'native'}}
    with duckdb.connect() as con:
        con.register('fec_relationships', pq.read_table(bundle / 'fec_relationships.parquet'))
        con.register('fec_source_records', pa.Table.from_pylist([{
            'collection_id': 'c', 'source_record_id': 'r', 'source_sha256': WITNESS['sha256']}]))
        adapter = ReceiptAdapter(con, {'families': {}}, 'unused', local_native=selected)
        spec = next(view for view in FEC_VIEWS if view.name == 'fec_relationship_evidence')
        with pytest.raises(ValueError, match='require selected native receipts: fec_source_records'):
            adapter.prepare(spec, spec.query({}))
        assert con.execute('SHOW TABLES').fetchall() == [('fec_relationships',), ('fec_source_records',)]
        with pytest.raises(ValueError, match='require selected native receipts'):
            adapter.restore('fec_source_records')


def test_mcp_reports_mixed_native_receipt_dependencies_before_install(monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.sources.publication import empty_index
    import spicy_regs.relationship_views as relationships
    with duckdb.connect() as con:
        monkeypatch.setattr(mcp_server, '_publication_status', lambda _: {
            'tables': ['fec_relationships', 'fec_source_records'], 'publication': {}})
        monkeypatch.setattr(mcp_server, '_connection_index', lambda _: empty_index())
        monkeypatch.setattr(mcp_server, '_connection_local_selection', lambda _: {
            'directory': '/unused', 'receipt_members': {}, 'native': {'fec_relationships': {}}})
        monkeypatch.setattr(relationships, 'install_relationship_views',
                            lambda *a, **k: pytest.fail('installed views before dependency validation'))
        with pytest.raises(ValueError, match='require selected native receipts: fec_source_records'):
            mcp_server._install_relationship_views(con)
        assert con.execute('SHOW TABLES').fetchall() == []


@pytest.mark.parametrize('conversion', [None, [], 'invalid', 'missing', {}, 'valid'])
def test_identity_conversion_inputs_require_exact_scalar_source(tmp_path, conversion):
    import json
    from spicy_regs.etl_receipts import _digest, _unpack, exact_json, RECEIPT_SCHEMA
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = tmp_path / 'identity'
    with IdentityReceiptWriter(bundle, generation_id='identity', tables=['fec_relationships']) as writer:
        writer.emit('fec_relationships', {'subject_id': 'C', 'object_id': 'H', 'relationship_type': 'supports',
                    'source_sha256': WITNESS['sha256'], 'source_locator_json': '{}',
                    'source_fields_json': '{}', 'cycle': '02026'}, input_witness=WITNESS)
    receipts = bundle / 'etl_receipts.parquet'
    [receipt] = pq.read_table(receipts).to_pylist()
    fields = _unpack(json.loads(receipt['processing_json']))
    if conversion == 'missing':
        del fields['conversion_inputs']
    elif conversion != 'valid':
        fields['conversion_inputs'] = conversion
    receipt['processing_json'] = exact_json(fields)
    receipt['receipt_id'] = _digest({k: v for k, v in receipt.items() if k != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist([receipt], schema=RECEIPT_SCHEMA), receipts)
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {'families': {}}, 'unused', local_native={'fec_relationships': {
            'subjects': [str(bundle / 'fec_relationships.parquet')], 'receipts': str(receipts), 'generation_id': 'identity'}})
        if conversion == 'valid':
            restored = adapter.restore('fec_relationships')
            assert con.execute(f'SELECT cycle FROM {restored}').fetchall() == [('02026',)]
            assert pq.read_table(bundle / 'fec_relationships.parquet')['cycle'].to_pylist() == [2026]
        else:
            with pytest.raises(ValueError, match='conversion_inputs must be a mapping|missing retained conversion input'):
                adapter.restore('fec_relationships')
            assert con.execute('SHOW TABLES').fetchall() == []


# --- Qualified views that read a field returned to its table (owner decision, 2026-10-05) ---------------------
# The views' SQL still reads the restored processing relation. What changed is where restoration finds the
# value: in the subject row for a listed table, in the receipt for the others. Each view's answer over a native
# pair is held to an implementation that never saw the split: the Python policy, on the builder's own row.

_PIN = "sha256:" + "9" * 64
_PROOF = "sha256:" + "8" * 64


def _qualified():
    from spicy_regs.fec_receipt_adapter import qualified_views
    from spicy_regs.relationship_views.fec_filing_associations import FILE_NUMBER_MAPPINGS

    scope = dict(source_generation_pin=_PIN, population="test", as_of="test",
                 namespace_evidence=dict.fromkeys(FILE_NUMBER_MAPPINGS, _PROOF))
    return {spec.view.name: spec.view for spec in qualified_views(scope)}


def _reading(views):
    """Views whose declared columns include a returned field, on a table it returned to."""
    from tests.test_fec_subject_receipts import returned_columns

    return {name: view for name, view in views.items()
            if any(set(columns) & set(returned_columns(table)) for table, columns in view.required.items())}


def _native(directory, table, rows):
    """Mapper rows written as a native subject and receipt pair: the adapter's local selection for the table."""
    from tests.test_fec_subject_receipts import declared_schema

    (subject, receipt), _ = write_fec_subjects(rows, directory / table, table=table, input_schema=declared_schema(table),
                                               generation_id="generation-a", context_for=context)
    assert pq.read_table(receipt, columns=["outcome"])["outcome"].to_pylist() == ["accepted"] * len(rows)
    return {"subjects": [str(subject)], "receipts": str(receipt), "generation_id": "generation-a"}


def _native_identity(directory, table, rows):
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = directory / table
    with IdentityReceiptWriter(bundle, generation_id="generation-a", tables=[table]) as writer:
        for row in rows:
            writer.emit(table, row, input_witness=WITNESS)
    subject = bundle / (table + ".parquet")
    return {"subjects": [str(subject)] if subject.exists() else [], "receipts": str(bundle / "etl_receipts.parquet"),
            "generation_id": "generation-a"}


def _answers(view, selected):
    """The view's rows over the selected native pairs, bound through the adapter as the server binds it."""
    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB"}) as con:
        adapter = ReceiptAdapter(con, {"families": {}}, "unused", local_native=selected)
        prepared = adapter.prepare(view, view.query({}))
        assert set(prepared.required) == {"_spicy_fec_processing_" + table for table in view.required}
        cursor = con.execute(prepared.query({}))
        names = [item[0] for item in cursor.description]
        return [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]


def _twin(observation, identity, **changes):
    from tests.test_fec_financial_policy import pin

    return {**observation, "record_id": pin(identity), **changes}


def _decision_observations():
    """Per table: observations a rule accepts, each with a twin that differs only in the field under test."""
    from spicy_regs.transforms import fec_financial_policy as policy
    from tests.test_fec_financial_policy import amount, bulk, row, summary

    def amounts(*names):
        return {key: value for name in names for key, value in amount(name, "123.123456789").items()}

    def with_twins(*accepted, inexact=False):
        rows = list(accepted)
        for ordinal, observation in enumerate(accepted):
            rows.append(_twin(observation, f"other-layout-{ordinal}", source_namespace="fec-other-layout"))
            if inexact:
                rows.append(_twin(observation, f"inexact-{ordinal}", amount=None, amount_raw="1e2",
                                  amount_status="unsupported_spelling"))
        return rows

    allocated = {**amount("total_amount", "100"), **amount("federal_share", "60"), **amount("nonfederal_share", "40")}
    return {
        "fec_receipts": with_twins(bulk("receipt"), inexact=True),
        "fec_intercommittee_transactions": with_twins(
            bulk("transfer", source_namespace="fec-bulk-other-committee-transactions"), inexact=True),
        "fec_loans": with_twins(row("loan", definition_set_id=policy._LOAN_LAYOUT,
                                    **amounts("original_loan_amount", "payments_to_date", "outstanding_balance"))),
        "fec_debts": with_twins(row("debt", definition_set_id=policy._DEBT_LAYOUT, **amounts(
            "opening_balance", "incurred_in_period", "paid_in_period", "closing_balance"))),
        "fec_allocated_disbursements": with_twins(row("allocated", definition_set_id=policy._ALLOCATION_LAYOUT, **allocated)),
        "fec_reported_financial_summaries": with_twins(
            summary(TTL_CONTB="123", TTL_CONTB_REF="-5", NET_CONTB="128", TTL_RECEIPTS="150", TTL_DISB="90"),
            summary("candidate-web-summary/1", TTL_RECEIPTS="100", TTL_DISB="80", TRANS_FROM_AUTH="10", TRANS_TO_AUTH="2"),
        ),
    }


def test_decision_views_read_the_returned_fields_from_the_subject_and_agree_with_the_policy(tmp_path):
    import json
    from decimal import Decimal

    from tests.test_fec_financial_meaning import oracle

    observations = _decision_observations()
    decisions = {name: view for name, view in _reading(_qualified()).items() if name.endswith("_decision")}
    assert {table for view in decisions.values() for table in view.required} == set(observations)
    selected = {table: _native(tmp_path, table, rows) for table, rows in observations.items()}
    outcomes = {}
    for name, view in decisions.items():
        [table] = view.required
        by_id = {row["record_id"]: row for row in observations[table]}
        answers = _answers(view, selected)
        assert sorted(answer["target_record_id"] for answer in answers) == sorted(by_id)
        for answer in answers:
            observation = by_id[answer.pop("target_record_id")]
            assert answer.pop("source_table") == table
            requested, query = answer.pop("requested_fields"), answer["query"]
            arguments = ({"fields": tuple(requested)} if query == "allocated_payment_measure" else
                         {"field": requested[0]} if requested else {})
            answer["definitions"] = json.loads(answer.pop("definitions_json"))
            expected = oracle(observation, query, **arguments)
            if expected["value"] is not None:
                expected["value"] = Decimal(expected["value"])
            assert answer == expected, (name, observation["record_id"])
            outcomes[name, observation["record_id"]] = (answer["status"], answer["reason"])
        # The answer turns on the field: some accepted observation and its other-namespace twin are decided apart.
        accepted = [row for row in observations[table] if row["source_namespace"] != "fec-other-layout"
                    and row.get("amount_status", "exact") == "exact"]
        twins = [row for row in observations[table] if row["source_namespace"] == "fec-other-layout"]
        assert all(outcomes[name, twin["record_id"]][0] == "refused" for twin in twins), name
        assert any(outcomes[name, a["record_id"]] != outcomes[name, t["record_id"]] for a, t in zip(accepted, twins)), name
    # Written out for one view: the namespace and the amount status each decide a row, by themselves.
    stated = {row["record_id"]: (row["source_namespace"], row["amount_status"]) for row in observations["fec_receipts"]}
    assert {stated[record]: outcome for (name, record), outcome in outcomes.items()
            if name == "fec_receipts_source_analysis_decision"} == {
        ("fec-bulk-individual-contributions", "exact"): ("eligible", "source_defined_purpose"),
        ("fec-other-layout", "exact"): ("refused", "input_mapping_or_authority_unqualified"),
        ("fec-bulk-individual-contributions", "unsupported_spelling"): ("refused", "amount_or_currency_unqualified"),
    }
    # amount_status did not return to fec_intercommittee_transactions; the same rule still gets it, from the receipt.
    stated = {row["record_id"]: row["amount_status"] for row in observations["fec_intercommittee_transactions"]
              if row["source_namespace"] != "fec-other-layout"}
    assert {stated[record]: outcome for (name, record), outcome in outcomes.items()
            if name == "fec_intercommittee_transactions_source_analysis_decision" and record in stated} == {
        "exact": ("eligible", "source_defined_purpose"),
        "unsupported_spelling": ("refused", "amount_or_currency_unqualified"),
    }


#: Each file-number association view and the bulk layout its table's rows come from. fec_communication_costs is
#: the control: its source namespace did not return, so the same view reads it from the receipt.
_FILE_NUMBER_LAYOUTS = {
    "fec_receipts": "fec-bulk-individual-contributions",
    "fec_intercommittee_transactions": "fec-bulk-other-committee-transactions",
    "fec_disbursements": "fec-bulk-operating-expense-ordered-header",
    "fec_independent_expenditures": "fec-bulk-independent-expenditure-csv",
    "fec_communication_costs": "fec-bulk-communication-cost-csv",
}


@pytest.mark.parametrize("table", _FILE_NUMBER_LAYOUTS)
def test_file_number_association_routes_each_row_by_its_source_namespace(tmp_path, table):
    from spicy_regs.relationship_views.fec_filing_associations import FILE_NUMBER_FILER_FIELDS, FILE_NUMBER_MAPPINGS
    from spicy_regs.transforms.fec_filing_associations import associate_filing_numbers
    from tests.test_fec_filing_number_view import metadata, source
    from tests.test_fec_subject_receipts import SOURCE_NAMESPACE_TABLES

    namespace = _FILE_NUMBER_LAYOUTS[table]
    filed = {key: value for key, value in source().items() if key != "reporting_committee_id"}
    filed.update({"source_namespace": namespace, "mapping_version": FILE_NUMBER_MAPPINGS[namespace],
                  FILE_NUMBER_FILER_FIELDS[namespace]: "C00000001"})
    rows = [filed, _twin(filed, "other-layout", source_namespace="fec-other-layout")]
    view = _qualified()[table + "_native_filing_associations"]
    assert (view.name in _reading(_qualified())) == (table in SOURCE_NAMESPACE_TABLES)
    selected = {table: _native(tmp_path, table, rows), "fec_filings": _native_identity(tmp_path, "fec_filings", [metadata()])}
    published = pq.read_table(selected[table]["subjects"][0])
    if table in SOURCE_NAMESPACE_TABLES:
        assert sorted(published["source_namespace"].to_pylist()) == sorted(row["source_namespace"] for row in rows)
    else:
        assert "source_namespace" not in published.schema.names
    answers = sorted(_answers(view, selected), key=lambda row: row["target_record_id"])
    expected = associate_filing_numbers(rows, table=table, filings=[metadata()], source_generation_pin=_PIN,
                                        namespace_evidence=dict.fromkeys(FILE_NUMBER_MAPPINGS, _PROOF))
    expected = sorted(({k: v for k, v in row.items() if k != "record_id"} for row in expected),
                      key=lambda row: row["target_record_id"])
    assert answers == expected
    assert {row["target_record_id"]: row["association_status"] for row in answers} == {
        filed["record_id"]: "resolved_native_filing_key",
        rows[1]["record_id"]: "unresolved_native_namespace",
    }


def test_individual_snapshot_inclusion_selects_receipts_by_their_source_namespace(tmp_path):
    from spicy_regs.fec_versions import INDIVIDUAL_SNAPSHOT_POLICY
    from tests.test_fec_financial_policy import bulk, pin

    view = _reading(_qualified())["fec_individual_snapshot_inclusion"]
    held = dict(collection_id="selection", source_sha256=pin("member"))
    individual = {**bulk("individual"), **held}
    rows = [individual, _twin(individual, "other-layout", source_namespace="fec-other-layout")]
    selection = dict(record_id="policy-row", policy_version=INDIVIDUAL_SNAPSHOT_POLICY, source_generation_pin=_PIN,
                     purpose="retained-reported-individual-contribution-snapshot", selection_status="included",
                     selection_reason="main-file", selected_collection_id="selection",
                     equivalence_evidence_sha256=pin("equivalence"), **held)
    selected = {"fec_receipts": _native(tmp_path, "fec_receipts", rows),
                "fec_collection_selection": _native_identity(tmp_path, "fec_collection_selection", [selection])}
    answers = _answers(view, selected)
    # Only the individual-contribution layout is in scope; the other row is not decided at all.
    assert [(row["target_record_id"], row["selection_status"], row["policy_record_id"]) for row in answers] == [
        (individual["record_id"], "included", "policy-row")]


def test_no_other_kind_of_view_reads_a_returned_field():
    """A new reader of either field is a new place a value could be read twice or not at all: look at it."""
    reading = _reading(_qualified())
    assert {name for name in reading if not name.endswith("_decision")} == {
        "fec_individual_snapshot_inclusion",
        *(table + "_native_filing_associations" for table in _FILE_NUMBER_LAYOUTS if table != "fec_communication_costs"),
    }
    # Views bound to the native tables themselves name their columns; none names either field.
    from spicy_regs.relationship_views import FEC_QUERY_VIEWS, SQL_RELATIONSHIP_VIEWS

    assert not _reading({view.name: view for view in (*FEC_QUERY_VIEWS, *SQL_RELATIONSHIP_VIEWS)})
