"""Historical source positions and all-or-refusal output; no network."""

import io
import json
import zipfile
from pathlib import Path

import pyarrow as pa
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
        assert item["witnesses"][0]["source_uri"] == digest(body)
        assert item["generation_id"] == receipt["generation"] != digest(body)
        assert str(tmp_path) not in json.dumps(item)
    with pytest.raises(FileExistsError):
        replay(source, output, expected_sha256=digest(body), release_point="119-73")


@pytest.mark.usefixtures("ample_disk_space")
def test_prepare_generation_from_fresh_replay_and_candidate_passes_local_publication_dry_run(tmp_path):
    """Both fresh and reused candidates produce an admitted table3-history generation that passes publish dry-run."""
    import duckdb

    from spicy_regs import local_publication
    from spicy_regs.sources import publication
    from spicy_regs.sources.table3_history import FAMILY, prepare_generation

    body = archive(FIXTURE.read_bytes())
    sha = digest(body)
    source = tmp_path / "olrc-table3-119-73.zip"
    source.write_bytes(body)

    live_index = tmp_path / "publication.v2.json"
    live_index.write_text(json.dumps(publication.empty_index()))

    # 1. Fresh replay -> prepared generation -> publish-local-generation --dry-run.
    prepared = prepare_generation(
        source,
        tmp_path / "prepared-fresh",
        expected_sha256=sha,
        release_point="119-73",
        read_snapshot=publication.empty_index(),
    )
    assert prepared["status"] == "prepared-table3-history-generation"
    assert prepared["family"] == FAMILY
    assert prepared["reused_candidate"] is False
    assert prepared["published"] is False
    assert prepared["records"] > 0

    dry_out = tmp_path / "dry-run-fresh"
    local_publication.main(
        [
            "--generation",
            prepared["generation"],
            "--family",
            FAMILY,
            "--evidence",
            prepared["evidence"],
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(live_index),
            "--output",
            str(dry_out),
        ]
    )
    dry_receipt = json.loads((dry_out / "dry-run.json").read_text())
    assert dry_receipt["status"] == "dry_run_accepted"
    assert dry_receipt["family"] == FAMILY
    assert dry_receipt["artifactDigest"] == prepared["artifactDigest"]
    assert dry_receipt["tables"]["table3_historical_records.parquet"]["rows"] == prepared["records"]
    assert dry_receipt["etlReceipts"]["rows"] == prepared["records"]

    # 2. Consumer SQL join against laws: modern public laws join cleanly while pre-1957 chapters stay separate.
    gen_parquet = Path(prepared["generation"]) / "table3_historical_records.parquet"
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE laws AS SELECT * FROM (VALUES ('119-public-37', 'Law 119-37'), ('87-public-845', 'Law 87-845')) t(law_id, title)"
        )
        modern = con.execute(
            f"""
            WITH modern AS (
                SELECT *, replace(act_key, '-', '-public-') AS law_id
                FROM read_parquet('{gen_parquet}')
                WHERE release_point = '119-73'
                  AND source_sha256 = '{sha}'
                  AND regexp_full_match(act_key, '[1-9][0-9]{{0,2}}-[1-9][0-9]*')
                  AND split_part(act_key, '-', 1) = congress
                  AND act_key = act_number
            )
            SELECT m.law_id, l.title, count(*) AS placements
            FROM modern m
            LEFT JOIN laws l ON l.law_id = m.law_id
            GROUP BY 1, 2
            ORDER BY 1
            """
        ).fetchall()
        chapters = con.execute(
            f"SELECT DISTINCT act_key FROM read_parquet('{gen_parquet}') WHERE NOT regexp_full_match(act_key, '[1-9][0-9]{{0,2}}-[1-9][0-9]*')"
        ).fetchall()
    assert ("119-public-37", "Law 119-37", 110) in modern
    assert ("87-public-845", "Law 87-845", 15) in modern
    assert ("118-public-2", None, 1) in modern
    assert chapters == [("1789-08-07:9",)]

    # 3. Reusing an already-verified replay candidate produces an admitted generation too.
    candidate = tmp_path / "existing-replay"
    replay(source, candidate, expected_sha256=sha, release_point="119-73")
    reused = prepare_generation(
        source,
        tmp_path / "prepared-reused",
        expected_sha256=sha,
        release_point="119-73",
        candidate=candidate,
    )
    assert reused["reused_candidate"] is True
    assert reused["records"] == prepared["records"]
    assert reused["receipt_generation_id"] == prepared["receipt_generation_id"]

    # 4. Tampered candidate release_point or receipt generation_id is refused before generation creation.
    bad_candidate = tmp_path / "tampered-candidate"
    bad_candidate.mkdir()
    table = pq.read_table(candidate / "table3_historical_records.parquet")
    idx = table.schema.get_field_index("release_point")
    tampered = table.set_column(idx, "release_point", pa.array(["119-99"] * table.num_rows, type=pa.string()))
    pq.write_table(tampered, bad_candidate / "table3_historical_records.parquet")
    (bad_candidate / "etl_receipts.parquet").write_bytes((candidate / "etl_receipts.parquet").read_bytes())
    with pytest.raises(ValueError, match="release_point"):
        prepare_generation(
            source,
            tmp_path / "prepared-tampered",
            expected_sha256=sha,
            release_point="119-73",
            candidate=bad_candidate,
        )
