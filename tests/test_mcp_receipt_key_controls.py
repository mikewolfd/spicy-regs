"""Exact keys survive producer receipts and the public lookup boundary."""

from tests.receipt_lookup_fixtures import one_family
from tests.test_mcp_receipt_lookup import call, fcc


def test_control_characters_and_literal_escapes_remain_distinct_keys(tmp_path, monkeypatch):
    identifiers = [f"before{chr(point)}after" for point in range(32)] + [
        r"before\u000bafter", r"before\nafter", 'before"\\after', "é漢字😀", "00123", "123",
    ]
    one_family(tmp_path, monkeypatch, {"fcc_filings": [
        fcc(identifier, text_data=f"value:{ordinal}") for ordinal, identifier in enumerate(identifiers)
    ]}, row_group=3)
    requested = list(reversed(identifiers)) + [identifiers[11], identifiers[0]]
    reply = call("fcc_filings", [{"id_submission": value} for value in requested], ["text_data"])
    assert [entry["key"]["id_submission"] for entry in reply["keys"]] == requested
    expected = {identifier: f"value:{ordinal}" for ordinal, identifier in enumerate(identifiers)}
    assert all(entry["receipt"] == "found" for entry in reply["keys"])
    assert [entry["fields"]["text_data"] for entry in reply["keys"]] == [
        {"state": "stated", "value": expected[identifier]} for identifier in requested
    ]
