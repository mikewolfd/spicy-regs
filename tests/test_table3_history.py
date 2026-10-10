"""Historical source positions and all-or-refusal output; no network."""

import io
import json
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.table3_history import digest, historical_records, replay

FIXTURE = Path(__file__).parent / "fixtures/congress_laws/table3-bulk-119-73-excerpt.xml"


def archive(member: bytes) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as writer:
        writer.writestr("fulldump@119-73.xml", member)
    return stream.getvalue()


def test_historical_chapters_blank_labels_and_fragment_context_survive():
    body = archive(FIXTURE.read_bytes())
    rows = list(historical_records(body, expected_sha256=digest(body), release_point="119-73"))
    chapter = [r for r in rows if r["act_key"] == "1789-08-07:9"]
    assert chapter and chapter[0]["act_public_law"] is None
    split = [r for r in rows if r["act_key"] == "87-845"]
    assert {r["statutes_at_large_volume"] for r in split} == {"76", "76A"}
    assert [r["act_record_position"] for r in split] == list(range(len(split)))
    act = [r for r in rows if r["act_key"] == "119-37"]
    assert len(act) == 110
    assert [r["act_record_position"] for r in act if r["act_section"] is None] == [3, 4, 5, 17]
    assert len({r["id"] for r in rows}) == len(rows)
    assert json.loads(act[0]["raw_source_row"])["record"]["record_id"] == act[0]["record_native_id"]


def test_release_point_and_input_digest_refuse_before_rows():
    body = archive(FIXTURE.read_bytes())
    with pytest.raises(ValueError, match="digest mismatch"):
        list(historical_records(body, expected_sha256="sha256:" + "0" * 64, release_point="119-73"))
    with pytest.raises(ValueError, match="different release point"):
        list(historical_records(body, expected_sha256=digest(body), release_point="119-74"))


def test_late_unknown_vocabulary_leaves_only_refusal_receipt(tmp_path):
    member = FIXTURE.read_bytes().replace(b"</act>", b"<unsupported/></act>", 1)
    body = archive(FIXTURE.read_bytes() + member)
    source = tmp_path / "source.zip"
    source.write_bytes(body)
    output = tmp_path / "refused"
    with pytest.raises(Exception, match="no field"):
        replay(source, output, expected_sha256=digest(body), release_point="119-73")
    assert [p.name for p in output.iterdir()] == ["receipt.json"]
    assert json.loads((output / "receipt.json").read_text())["status"] == "refused"


def test_replay_writes_new_candidate_and_refuses_overwrite(tmp_path):
    body = archive(FIXTURE.read_bytes())
    source = tmp_path / "source.zip"
    source.write_bytes(body)
    output = tmp_path / "candidate"
    receipt = replay(source, output, expected_sha256=digest(body), release_point="119-73")
    table = pq.read_table(output / "table3_historical_records.parquet")
    assert table.num_rows == receipt["records"]
    assert receipt["status"] == "candidate-verified"
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == table.num_rows
    for item, row in zip(receipts, table.to_pylist()):
        position = json.loads(item["witnesses"][0]["locator"])
        assert position["act_record_position"] == row["act_record_position"]
        assert item["witnesses"][1]["sha256"] == row["member_sha256"]
        assert item["attempt_id"] == row["id"]
    with pytest.raises(FileExistsError):
        replay(source, output, expected_sha256=digest(body), release_point="119-73")
