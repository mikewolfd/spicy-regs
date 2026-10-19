"""Exercise request-time native receipt selection using only the image's packages."""

from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import hashlib
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, select_receipts


def check_public_download():
    from spicy_regs.local_data import file_state_from_stat
    from spicy_regs.sources import publication

    data = b"checked runtime download"
    member = publication.Member("fixture.parquet", "sha256:" + hashlib.sha256(data).hexdigest(), len(data), 1)
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def iter_bytes(self): yield data
    with TemporaryDirectory() as temp, patch.object(publication.httpx, "stream", return_value=Response()):
        target = Path(temp) / "download.parquet"
        checked = publication.fetch_member("https://fixture.invalid", member, target)
        assert isinstance(checked, publication.DownloadedMember)
        assert checked.sha256 == member.sha256 and checked.byte_size == len(data)
        assert target.read_bytes() == data
        assert checked.state == file_state_from_stat(target.lstat())
    assert "rulespec_artifacts" not in sys.modules


def check_native_selection():
    # Selection must retain all outcomes, in file order, including failed
    # attempts without subjects. Validation belongs to the subsequent reader.
    rows = [
        {"dataset": "document_citations", "outcome": "accepted", "receipt_id": "first"},
        {"dataset": "other", "outcome": "accepted", "receipt_id": "unrelated"},
        {"dataset": "document_citations", "outcome": "error", "receipt_id": "failed"},
    ]
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source, selected = root / "shared.parquet", root / "selected.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), source)
        select_receipts(source, selected, dataset="document_citations")
        result = pq.read_table(selected)
        assert result.schema == RECEIPT_SCHEMA
        assert result.column("receipt_id").to_pylist() == ["first", "failed"]
        select_receipts(source, selected, dataset="absent")
        assert pq.read_table(selected).num_rows == 0
    assert "spicy_regs.transforms" not in sys.modules


def check_split_vote_readers():
    """Two synthetic votes exercise installed readers, not publication admission."""
    import duckdb

    from spicy_regs import receipt_lookup
    from spicy_regs.congress_subjects import map_record
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, read_with_receipts, split_record
    from spicy_regs.subject_catalog import descriptors

    policy = DatasetPolicy.from_descriptor(descriptors()["member_votes"])
    generation = "runtime-vote-smoke"
    with TemporaryDirectory() as temp:
        root = Path(temp)
        subjects, receipts, expected, members = [], [], [], []
        for ordinal, position in enumerate(("Yea", "Nay")):
            mapped = map_record("member_votes", {
                "vote_id": "119-house-1-1", "member_key": f"name:M{ordinal}",
                "congress": "119", "chamber": "house", "position": position,
            })
            assert mapped.subject is not None
            row = mapped.subject | {"source_fields": mapped.source_fields, "entry_kind": "row"}
            context = ReceiptContext(generation, f"member:{ordinal}", "runtime-fixture", [{
                "source_id": "synthetic-runtime", "source_uri": None, "sha256": None,
                "locator": f"member:{ordinal}", "body_version": "fixture-v1",
            }])
            subject, receipt = split_record(policy, row, context)
            path = root / f"receipt-{ordinal}.parquet"
            pq.write_table(pa.Table.from_pylist([receipt], schema=RECEIPT_SCHEMA), path)
            subjects.append(subject)
            receipts.append(receipt)
            expected.append(row)
            members.append(path)
        members = tuple(members)
        subject_path, selected = root / "subjects.parquet", root / "selected.parquet"
        pq.write_table(pa.Table.from_pylist(subjects, schema=policy.subject_schema), subject_path)
        select_receipts(members, selected, dataset="member_votes")
        assert pq.read_table(selected).to_pylist() == receipts
        replay = list(read_with_receipts([subject_path], members, policy, generation_id=generation))
        assert sorted(replay, key=lambda row: row["member_key"]) == expected
        local = {"native": {"member_votes": {
            "subjects": [str(subject_path)], "receipts": list(map(str, members)),
            "generation_id": generation,
        }}}
        with duckdb.connect(config={"threads": 1, "memory_limit": "128MB"}) as con:
            con.read_parquet(str(subject_path)).create_view("member_votes")
            key = {name: expected[-1][name] for name in policy.identity_fields}
            result = receipt_lookup.read_fields(
                con, table="member_votes", keys=[key], fields=["position"], index={},
                local=local, base_url="", entry={}, plain=lambda value: value,
            )
        assert result["keys"] == [{
            "key": key, "receipt": "found", "fields": {"position": {"state": "stated", "value": "Nay"}},
        }]
        assert result["receipts"]["members"] == [
            {"location": str(path), "sha256": None} for path in members
        ]
        assert result["receipts"]["generation_id"] == generation
        assert receipt_lookup.SCAN_ROW_BOUND == 2_000_000
    assert not {"rulespec_artifacts", "spicy_docs", "boto3", "polars", "spicy_regs.transforms"} & sys.modules.keys()


if __name__ == "__main__":
    check_native_selection()
    check_public_download()
    check_split_vote_readers()
    print("Native citation selection, checked public download and split vote selection/replay/lookup passed")
