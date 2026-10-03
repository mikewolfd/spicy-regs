import json

import pyarrow as pa

from spicy_regs.transforms.build_congress_index import RIN_COLUMNS, _read_flags, _repair_rin, _shape_communication


def _repaired(table: pa.Table) -> pa.Table:
    """The RIN repair over each row as the run reads it: read where its detail was (here, by the old marker)."""
    return _repair_rin(table, _read_flags(table, "committees_json"))


def test_consumer_extracts_all_rins_and_marks_unread():
    source = {"congress": 119, "number": 1, "communicationType": {"code": "EC"},
              "reportNature": "(RIN: 3235-AK79; 3235-AK80)"}
    row = _shape_communication(source, source)
    occurrences = row["rin_occurrences_json"]
    assert isinstance(occurrences, str)
    assert [item["rin"] for item in json.loads(occurrences)] == ["3235-AK79", "3235-AK80"]
    assert _shape_communication(source, None)["rin_occurrences_json"] is None


def test_retained_repair_handles_old_schema_without_fetching_or_false_empty():
    old = pa.table({"report_nature": ["(RIN: 3235-AK79; 3235-AK80)", None, None],
                    "committees_json": ["[]", "[]", None]})
    repaired = _repaired(old)
    values = repaired["rin_occurrences_json"].to_pylist()
    assert len(json.loads(values[0])) == 2
    assert values[1:] == ["[]", None]
    assert repaired["rin_rule"].to_pylist() == ["report_nature_rin_label/2", "unmatched", None]
    assert _repaired(repaired).equals(repaired)


def test_a_held_row_is_re_derived_as_the_shaper_derives_it_from_the_same_field():
    """A row the run does not re-read loses the 0.50.0 scalar: NOAA's cut-short RIN and the old rule name go."""
    fields = ["Final rule (RIN: 0648-XE368)", "Final rule (RIN: 2120-Aa64)", "Final rule (RIN: 3133-AF97)"]
    held = pa.table({
        "report_nature": fields,
        "committees_json": ["[]"] * 3,
        "rin": ["0648-XE36", None, "3133-AF97"],
        "rin_occurrences_json": ["[]", "[]", "[]"],
        "rin_rule": ["report_nature_rin_label", "unmatched", "report_nature_rin_label"],
        "rin_matched_text": ["RIN: 0648-XE36", None, "RIN: 3133-AF97"],
    })
    repaired = _repaired(held).to_pylist()
    shaped = [
        _shape_communication(detail, detail)
        for detail in ({"congress": 119, "number": n, "communicationType": {"code": "EC"}, "reportNature": field}
                       for n, field in enumerate(fields))
    ]
    assert [{c: row[c] for c in RIN_COLUMNS} for row in repaired] == [{c: row[c] for c in RIN_COLUMNS} for row in shaped]
    assert [(row["rin"], row["rin_rule"], row["rin_matched_text"]) for row in repaired] == [
        (None, "unmatched", None),
        ("2120-AA64", "report_nature_rin_label/2", "RIN: 2120-Aa64"),
        ("3133-AF97", "report_nature_rin_label/2", "RIN: 3133-AF97"),
    ]
