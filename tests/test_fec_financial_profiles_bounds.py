"""Memory, writer-batch, duplicate-witness, and capacity-refusal bounds for build_fec_financial_profiles (ss-hm80)."""

from __future__ import annotations

import json
import tracemalloc

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import build_fec_financial_profiles as module
from tests.test_fec_financial_profiles import PIN, candidate, captured, committee


def test_streaming_profile_matches_materialized_json_and_refuses_duplicate_record_id(tmp_path):
    """Streaming reduction matches json_bytes(list) verbatim and detects duplicate physical record_id."""
    captures, store = captured(
        tmp_path,
        candidates=[candidate()],
        committees=[
            committee(CMTE_ID="C00880898", CAND_ID="H6CA00001"),
            committee(CMTE_ID="C00880898", CAND_ID="H6CA00002"),
            committee(CMTE_ID="C00880898", CAND_ID="H6CA00003"),
        ],
    )
    selection = module.select(captures)
    output = tmp_path / module.OUTPUT
    module.build_profiles(selection, store, output, selection_sha256=PIN)
    rows = {r["entity_id"]: r for r in pq.read_table(output).to_pylist()}
    witnesses = json.loads(rows["C00880898"]["witnesses_json"])
    assert len(witnesses) == 3
    assert rows["C00880898"]["witnesses_json"] == module.json_bytes(witnesses).decode()

    # Duplicate physical record_id in a single entity group must refuse closed.
    dup_capture = selection["captures"][1]
    dup_rows = [witnesses[0], witnesses[0]]
    with pytest.raises(ValueError, match="Duplicate physical witness record_id"):
        module.profile(dup_rows, dup_capture, selection)


def test_entity_witness_capacity_refusal_preserves_prior_destination_and_cleans_scratch(tmp_path):
    """Exceeding max_entity_witness_bytes fails closed without touching the prior destination or leaking scratch."""
    captures, store = captured(
        tmp_path,
        candidates=[candidate()],
        committees=[committee(CMTE_ID="C99999999", CAND_ID=f"H6CA{i:05d}") for i in range(40)],
    )
    selection = module.select(captures)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    destination = out_dir / module.OUTPUT
    destination.write_bytes(b"prior-verified-profiles")

    with pytest.raises(ValueError, match="exceed max_witness_bytes"):
        module.build_profiles(
            selection,
            store,
            destination,
            selection_sha256=PIN,
            max_entity_witness_bytes=32 * 1024,
        )
    assert destination.read_bytes() == b"prior-verified-profiles"
    assert [p.name for p in out_dir.iterdir()] == [module.OUTPUT]


def test_duckdb_memory_limit_refusal_preserves_prior_destination_and_cleans_scratch(tmp_path):
    """DuckDB OOM under max_temp_directory_size=0B fails closed and leaves prior destination untouched."""
    captures, store = captured(
        tmp_path,
        candidates=[candidate(Cand_Id=f"H6CA{i:05d}") for i in range(250)],
        committees=[committee(CMTE_ID=f"C{i:08d}") for i in range(250)],
    )
    selection = module.select(captures)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    destination = out_dir / module.OUTPUT
    destination.write_bytes(b"prior-verified-profiles")

    with pytest.raises(duckdb.OutOfMemoryException):
        module.build_profiles(
            selection,
            store,
            destination,
            selection_sha256=PIN,
            duckdb_memory_limit="1MB",
        )
    assert destination.read_bytes() == b"prior-verified-profiles"
    assert [p.name for p in out_dir.iterdir()] == [module.OUTPUT]


def test_writer_batch_byte_bound_flushes_multiple_row_groups_with_identical_rows(tmp_path):
    """Tight max_writer_batch_bytes splits Parquet writes without altering any row value."""
    captures, store = captured(
        tmp_path,
        candidates=[candidate(Cand_Id=f"H6CA{i:05d}") for i in range(10)],
        committees=[committee(CMTE_ID=f"C{i:08d}") for i in range(25)],
    )
    selection = module.select(captures)
    wide_dest = tmp_path / "wide.parquet"
    tight_dest = tmp_path / "tight.parquet"
    module.build_profiles(selection, store, wide_dest, selection_sha256=PIN, max_writer_batch_bytes=8 * 1024**2)
    module.build_profiles(selection, store, tight_dest, selection_sha256=PIN, max_writer_batch_bytes=32 * 1024)

    assert pq.ParquetFile(wide_dest).metadata.num_row_groups == 1
    assert pq.ParquetFile(tight_dest).metadata.num_row_groups > 1
    assert pq.read_table(tight_dest).to_pylist() == pq.read_table(wide_dest).to_pylist()


def test_large_entity_group_streams_without_materializing_all_witness_dicts(tmp_path):
    """A 1,000-witness entity group streams incrementally with bounded Python heap and exact JSON output."""
    n = 1_000
    captures, store = captured(
        tmp_path,
        candidates=[candidate()],
        committees=[committee(CMTE_ID="C99999999", CAND_ID=f"H6CA{i:05d}") for i in range(n)],
    )
    selection = module.select(captures)
    destination = tmp_path / module.OUTPUT
    tracemalloc.start()
    try:
        summary = module.build_profiles(selection, store, destination, selection_sha256=PIN)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert summary["profiles"] == {"selected": 2}
    rows = {r["entity_id"]: r for r in pq.read_table(destination).to_pylist()}
    assert rows["C99999999"]["witness_count"] == n
    assert rows["C99999999"]["selection_reason"] == "equivalent_witnesses"
    witnesses = json.loads(rows["C99999999"]["witnesses_json"])
    assert len(witnesses) == n
    assert len({w["candidate_native_id"] for w in witnesses}) == n
    # Baseline list(group) held ~75+ MiB of witness dicts for 1,000 rows; streaming holds only 1 dict + compact JSON.
    assert peak_bytes < 55 * 1024 * 1024, f"Unexpectedly high Python peak: {peak_bytes / 1024**2:.2f} MiB"
