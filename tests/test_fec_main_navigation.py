"""Main-row FEC navigation retains qualified context and every observation."""

from copy import deepcopy
from decimal import Decimal
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.assemble_fec_query import TypedInput, FilingAssociationInputs, assemble_subject_table
from spicy_regs.transforms.fec_subject_receipts import read_fec_with_receipts, dataset_policy
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, read_identity_rows
from spicy_regs.transforms.fec_identity_context_fields import normalize_record
from tests.test_fec_filing_associations import financial, filing, pin, GEN, EVIDENCE, NAMESPACE
from tests.test_fec_subject_receipts import context
from tests.test_fec_identity_receipts import WITNESS


def member(tmp, name, rows):
    path = tmp / name
    table = pa.Table.from_pylist(rows)
    # Published mapper schemas declare these VARCHAR even when every value is NULL.
    for name in ("filing_key", "filing_link_status", "filing_header_record_id", "filing_header_locator_json"):
        if name in table.schema.names and pa.types.is_null(table.schema.field(name).type):
            table = table.set_column(
                table.schema.get_field_index(name), name, pa.array([None] * len(rows), type=pa.string())
            )
    pq.write_table(table, path)
    return TypedInput(path, "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(), len(rows))


def build(tmp, rows, targets, **options):
    source = member(tmp, "financial.parquet", rows)
    target = member(tmp, "filings.parquet", targets)
    selected = FilingAssociationInputs(GEN, pin("target-generation"), [target], {NAMESPACE: EVIDENCE})
    result = assemble_subject_table(
        [source],
        table="fec_receipts",
        schema=pq.read_schema(source.path),
        output=tmp / "main",
        generation_id="generation-a",
        context_for=context,
        check_resources=lambda: None,
        filing_associations=options.get("selection", selected),
    )
    return result, pq.read_table(result["subject_path"]).to_pylist(), selected


def test_qualified_filing_main_fields_do_not_multiply_financial_rows_and_restore_inputs(tmp_path):
    rows = [
        {
            **financial(record_id=pin(str(i))),
            "amount": Decimal("5"),
            "filing_key": None,
            "filing_link_status": "unresolved",
        }
        for i in range(2)
    ]
    result, actual, selected = build(tmp_path, rows, [filing(), filing(rid="second-observation")])
    assert len(actual) == 2 and [r["amount"] for r in actual] == [Decimal("5"), Decimal("5")]
    assert [r["filing_association_target_count"] for r in actual] == [2, 2]
    assert [r["filing_key"] for r in actual] == [filing()["filing_key"]] * 2
    assert all(r["filing_association_status"] == "resolved_native_filing_key" for r in actual)
    assert all(r["filing_association_target_record_ids"] == ["metadata-1", "second-observation"] for r in actual)
    assert all(r["filing_association_source_generation_pin"] == GEN for r in actual)
    assert all(r["filing_association_target_generation_pin"] == selected.filing_generation_pin for r in actual)
    assert result["exact_mapper_rows_compared"] == 2 and result["filing_association_inputs"]["filings"]
    policy = dataset_policy("fec_receipts", pq.read_schema(tmp_path / "financial.parquet"))
    assert (
        list(
            read_fec_with_receipts(
                [result["subject_path"]], [result["receipt_path"]], policy, generation_id="generation-a"
            )
        )
        == rows
    )


@pytest.mark.parametrize(
    "fault,status",
    [
        ("conflicting_target", "unresolved_target_identity_conflict"),
        ("missing_definition", "unresolved_namespace_evidence"),
        ("bad_number", "unresolved_invalid_file_number"),
    ],
)
def test_unresolved_filing_decisions_remain_main_without_an_edge(tmp_path, fault, status):
    row = {**financial(), "filing_key": None}
    targets = [filing()]
    if fault == "conflicting_target":
        targets.append({**filing(rid="other"), "form_type": "F1"})
    if fault == "bad_number":
        row["report_number"] = "1e3"
    selection = None
    if fault == "missing_definition":
        target = member(tmp_path, "selected-target.parquet", targets)
        selection = FilingAssociationInputs(GEN, pin("target"), [target], {})
    result, actual, _ = build(tmp_path, [row], targets, **({"selection": selection} if selection else {}))
    assert actual[0]["filing_key"] is None
    assert actual[0]["filing_association_status"] == status
    assert result["subject_rows"] == 1


def test_changed_dependency_is_refused_before_output_and_inapplicable_rows_gain_no_filing_fields(tmp_path):
    source = member(tmp_path, "agency.parquet", [{"record_id": "a", "report_id": "r"}])
    target = member(tmp_path, "filings.parquet", [filing()])
    selected = FilingAssociationInputs(GEN, pin("target"), [TypedInput(target.path, pin("wrong"), 1)], {})
    with pytest.raises(ValueError, match="applicable"):
        assemble_subject_table(
            [source],
            table="fec_agency_reports",
            schema=pq.read_schema(source.path),
            output=tmp_path / "result",
            generation_id="generation-a",
            context_for=context,
            check_resources=lambda: None,
            filing_associations=selected,
        )
    assert not (tmp_path / "result").exists()
    row = {**financial(), "filing_key": None}
    source = member(tmp_path, "receipt.parquet", [row])
    with pytest.raises(ValueError, match="selected bytes"):
        assemble_subject_table(
            [source],
            table="fec_receipts",
            schema=pq.read_schema(source.path),
            output=tmp_path / "bad",
            generation_id="generation-a",
            context_for=context,
            check_resources=lambda: None,
            filing_associations=selected,
        )
    assert not (tmp_path / "bad").exists()


def test_evidence_repeats_are_main_occurrences_and_collection_context_is_not_a_source_record(tmp_path):
    row = dict(
        target_table="fec_receipts",
        target_record_id=pin("r"),
        target_generation_scope="self",
        witness_generation_scope="external",
        witness_generation_pin=pin("prior"),
        role="field_definition",
        endpoint_kind="collection_context",
        collection_id="definitions",
        source_record_id=None,
        context_column="collection_outcome_json",
        context_pointer="/tableFieldDefinitions",
        witness_sha256=pin("definition"),
        witness_locator_json='{"table":0}',
    )
    with IdentityReceiptWriter(tmp_path / "evidence", generation_id="g1", tables=["fec_record_evidence"]) as writer:
        writer.emit("fec_record_evidence", row, input_witness=WITNESS)
        writer.emit("fec_record_evidence", row, input_witness=WITNESS)
    main = pq.read_table(tmp_path / "evidence/fec_record_evidence.parquet").to_pylist()
    assert [r["observation_ordinal"] for r in main] == [0, 1]
    assert all(r["source_record_id"] is None and r["endpoint_kind"] == "collection_context" for r in main)
    assert all(r["target_generation_scope"] == "self" and r["witness_generation_pin"] == pin("prior") for r in main)
    assert len(list(read_identity_rows(tmp_path / "evidence", "fec_record_evidence", generation_id="g1"))) == 2


def relationship(**changes):
    return dict(
        subject_id="C00000001",
        subject_type="committee",
        subject_id_status="source_id_shape",
        relationship_type="committee_candidate",
        object_id="H4NC05146",
        object_type="candidate",
        object_id_status="source_id_shape",
        object_name=None,
        value_status="reported",
        source_family="committee_api_current",
        cycle="2024",
        candidate_election_year=None,
        source_sha256=pin("source"),
        source_url=None,
        observed_at=None,
        source_locator_json=json.dumps(
            {
                "collection_id": "c",
                "source_record_id": "r",
                "json_pointer": "/results/7",
                "array_field": "candidate_ids",
                "array_index": 0,
                "ordinal": 7,
            }
        ),
        source_fields_json="{}",
        **changes,
    )


def test_relationship_main_coordinates_repeats_and_native_cycle_guards(tmp_path):
    a = relationship()
    b = deepcopy(a)
    loc = json.loads(b["source_locator_json"])
    loc["array_index"] = 1
    b["source_locator_json"] = json.dumps(loc)
    with IdentityReceiptWriter(tmp_path / "relations", generation_id="g1", tables=["fec_relationships"]) as writer:
        for row in [a, b]:
            writer.emit("fec_relationships", row, input_witness=WITNESS)
    main = pq.read_table(tmp_path / "relations/fec_relationships.parquet").to_pylist()
    assert len(main) == 2 and main[0]["record_id"] != main[1]["record_id"]
    assert [r["subrecord_ordinal"] for r in main] == [0, 1]
    assert [r["subrecord_pointer"] for r in main] == ["/results/7/candidate_ids/0", "/results/7/candidate_ids/1"]
    assert all(r["cycle"] == 2024 and r["object_endpoint_status"] == "lookup_eligible" for r in main)
    assert all(r["collection_id"] == "c" and r["source_record_id"] == "r" and r["source_ordinal"] == 7 for r in main)


@pytest.mark.parametrize(
    "change,status",
    [
        ({"cycle": None}, "unavailable_source_cycle"),
        ({"object_id": None, "object_name": "Reported name"}, "name_only"),
        ({"value_status": "reported_none"}, "no_edge_reported_none"),
        ({"object_type": "organization"}, "unsupported_entity_type"),
        ({"object_id_status": "invalid_source_id_shape"}, "invalid_native_id"),
    ],
)
def test_relationship_no_edges_remain_explicit(change, status):
    row = relationship()
    row.update(change)
    mapped = normalize_record("fec_relationships", row)
    assert mapped["object_endpoint_status"] == status
    assert mapped["object_name"] == row["object_name"]
    row["source_locator_json"] = '{"json_pointer":"/results/0","array_index":true}'
    mapped = normalize_record("fec_relationships", row)
    assert mapped["collection_id"] is None and mapped["source_record_id"] is None
    assert mapped["companion_status"] == "unavailable_locator_coordinates" and mapped["subrecord_ordinal"] is None


@pytest.mark.parametrize(
    "change",
    [
        {"source_authority": None},
        {"source_record_id": None},
        {"source_locator_json": "{}"},
        {"source_locator_json": "not-json"},
        {"selection_evidence_sha256": None},
    ],
)
def test_unavailable_source_context_preserves_main_row_without_a_filing_edge(tmp_path, change):
    row = {**financial(), "filing_key": None, **change}
    result, main, _ = build(tmp_path, [row], [filing()])
    assert result["subject_rows"] == 1 and result["exact_mapper_rows_compared"] == 1
    assert main[0]["filing_association_status"] == "unresolved_source_context"
    assert main[0]["filing_key"] is None and main[0]["filing_association_record_id"] is None
    assert main[0]["filing_association_target_count"] is None
    assert main[0]["filing_association_target_record_ids"] is None
    assert main[0]["source_locator_json"] == row["source_locator_json"]


@pytest.mark.parametrize(
    "fault,status",
    [
        (None, "resolved_native_filing_key"),
        ("repeat", "unresolved_header_association_ambiguous"),
        ("wrong_generation", "unresolved_header_association_absent"),
        ("changed_locator", "unresolved_header_locator_mismatch"),
        ("changed_policy", "unresolved_header_policy_version"),
    ],
)
def test_main_header_associations_reuse_exact_qualified_scope_without_expanding_rows(tmp_path, fault, status):
    from tests.test_fec_filing_associations import headers, header_inputs

    hs, _ = header_inputs()
    association = headers()[0]
    row = {
        **financial(),
        "filing_key": None,
        "filing_header_record_id": "header",
        "filing_header_locator_json": hs[0]["source_locator_json"],
        "amount": Decimal("7.25"),
    }
    if fault == "wrong_generation":
        association["source_generation_pin"] = pin("different-generation")
    if fault == "changed_locator":
        row["filing_header_locator_json"] = "{}"
    if fault == "changed_policy":
        association["policy_version"] = "unknown/9"
    target = member(tmp_path, "target-selected.parquet", [filing()])
    header = member(tmp_path, "headers-selected.parquet", [association] * (2 if fault == "repeat" else 1))
    selection = FilingAssociationInputs(GEN, pin("target-generation"), [target], {NAMESPACE: EVIDENCE}, [header])
    result, main, _ = build(tmp_path, [row], [filing()], selection=selection)
    assert len(main) == 1 and main[0]["amount"] == Decimal("7.25")
    assert main[0]["filing_association_status"] == status
    assert main[0]["filing_key"] == (filing()["filing_key"] if fault is None else None)
    assert result["exact_mapper_rows_compared"] == 1
    if fault in ("repeat", "wrong_generation", "changed_locator"):
        assert main[0]["filing_association_target_count"] is None
        assert main[0]["filing_association_target_record_ids"] is None
    else:
        assert main[0]["filing_association_target_count"] == 1


def test_header_claims_do_not_rebind_changed_filing_witnesses(tmp_path):
    from tests.test_fec_filing_associations import headers, header_inputs

    hs, _ = header_inputs()
    target = member(tmp_path, "target-selected.parquet", [filing(rid="different-observation")])
    header = member(tmp_path, "header-selected.parquet", headers())
    selected = FilingAssociationInputs(GEN, pin("target-generation"), [target], {NAMESPACE: EVIDENCE}, [header])
    with pytest.raises(ValueError, match="complete filing witnesses"):
        build(
            tmp_path,
            [
                {
                    **financial(),
                    "filing_key": None,
                    "filing_header_record_id": "header",
                    "filing_header_locator_json": hs[0]["source_locator_json"],
                }
            ],
            [filing()],
            selection=selected,
        )
    assert not (tmp_path / "main").exists()


def test_dependency_admission_refuses_duplicate_member_and_bad_row_membership(tmp_path):
    target = member(tmp_path, "selected-target.parquet", [filing()])
    for items in ([target, target], [TypedInput(target.path, target.sha256, True)]):
        selected = FilingAssociationInputs(GEN, pin("target-generation"), items, {NAMESPACE: EVIDENCE})
        with pytest.raises(ValueError, match="twice|integer row"):
            build(tmp_path, [{**financial(), "filing_key": None}], [filing()], selection=selected)


def test_many_source_batches_reuse_one_selected_filing_population(tmp_path, monkeypatch):
    from spicy_regs.transforms import fec_filing_associations as rules

    calls = []
    original = rules._index

    def counted(rows):
        calls.append(len(rows))
        return original(rows)

    monkeypatch.setattr(rules, "_index", counted)
    rows = [{**financial(record_id=pin(str(i))), "filing_key": None} for i in range(513)]
    result, main, _ = build(tmp_path, rows, [filing(), filing(rid="second-observation")])
    assert result["exact_mapper_rows_compared"] == len(main) == 513
    assert calls == [2]
    assert all(row["filing_association_target_count"] == 2 for row in main)


def test_dependency_definition_selection_is_a_fixed_copy(tmp_path):
    target = member(tmp_path, "selected-target.parquet", [filing()])
    definitions = {NAMESPACE: EVIDENCE}
    selection = FilingAssociationInputs(GEN, pin("target"), [target], definitions)
    source = member(tmp_path, "selected-source.parquet", [{**financial(), "filing_key": None}])
    apply, proof = selection.prepare("fec_receipts", pq.read_schema(source.path), lambda: None)
    definitions.clear()
    [(original, decision)] = list(apply(pq.read_table(source.path).to_pylist()))
    assert original["filing_key"] is None
    assert decision["filing_association_status"] == "resolved_native_filing_key"
    assert proof["namespace_evidence"] == {NAMESPACE: EVIDENCE}
