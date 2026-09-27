import json

import pyarrow as pa

from spicy_regs.transforms.build_congress_index import _repair_rin_occurrences, _shape_communication


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
    repaired = _repair_rin_occurrences(old)
    values = repaired["rin_occurrences_json"].to_pylist()
    assert len(json.loads(values[0])) == 2
    assert values[1:] == ["[]", None]
    assert _repair_rin_occurrences(repaired).equals(repaired)
