"""Receipt-only context may emit only the tables the registry admits from receipts (ss-j4vz B11)."""

import pytest

from spicy_regs.transforms.build_fec_identity_context import _emit_mapping
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY


class RecordingWriter:
    """Fixture: records emits instead of writing receipts."""

    policies = {"fec_record_evidence": object()}

    def __init__(self):
        self.emitted = []

    def emit(self, table, row, *, input_witness, evidence=None, source_input=None):
        self.emitted.append((table, row.get("record_id"), [e["role"] for e in evidence or []]))


def evidence(table, record_id, role):
    return dict(target_table=table, target_record_id=record_id, role=role)


def test_admitted_set_is_the_receipt_describing_tables_plus_those_awaiting_an_original_rebuild():
    admitted = {table for table, rules in REGISTRY.items() if rules.get("admitted_from_receipts")}
    describing = {"fec_research_context_dispositions", "fec_research_response_outcomes"}
    # Each leaves this set when its originals producer lands (B8 meetings, B9 feed and pages).
    pending_rebuild = {
        "fec_research_meeting_observations",
        "fec_research_filing_feed_items",
        "fec_research_source_pages",
    }
    assert admitted == describing | pending_rebuild


def test_receipt_only_input_cannot_emit_a_subject_table_and_nothing_is_written():
    writer = RecordingWriter()
    tables = {
        "fec_research_response_outcomes": [dict(record_id="r1")],
        "fec_filings": [dict(record_id="f1")],
    }
    with pytest.raises(ValueError, match="Receipt-only input cannot produce table output: fec_filings"):
        _emit_mapping(writer, tables, [], {}, None, from_receipts=True)
    assert writer.emitted == []


def test_native_source_mapping_is_not_subject_to_the_receipt_rule():
    writer = RecordingWriter()
    _emit_mapping(writer, {"fec_filings": [dict(record_id="f1")]}, [], {}, None)
    assert writer.emitted == [("fec_filings", "f1", [])]


def test_evidence_is_attached_per_row_by_table_and_record_identity():
    writer = RecordingWriter()
    tables = {"fec_research_source_pages": [dict(record_id="a"), dict(record_id="b")]}
    links = [
        evidence("fec_research_source_pages", "a", "primary"),
        evidence("fec_research_source_pages", "b", "primary"),
        evidence("fec_research_source_pages", "a", "field_definition"),
        evidence("fec_research_meeting_observations", "a", "other_table"),
    ]
    _emit_mapping(writer, tables, links, {}, None, from_receipts=True)
    assert writer.emitted[:2] == [
        ("fec_research_source_pages", "a", ["primary", "field_definition"]),
        ("fec_research_source_pages", "b", ["primary"]),
    ]
    assert [e[0] for e in writer.emitted[2:]] == ["fec_record_evidence"] * 4
