from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints

KEY = b"spicy_regs.read_checkpoints.sample.v1"


def _write(path: Path, metadata: dict[bytes, bytes]) -> None:
    table = pa.table({"id": pa.array([], type=pa.string())}).replace_schema_metadata(metadata)
    pq.write_table(table, path)


def test_checkpoints_survive_an_empty_output_and_preserve_other_metadata(tmp_path):
    prior = tmp_path / "prior.parquet"
    _write(prior, {b"source": b"retained", b"spicy_regs.read_checkpoints.other.v1": b"[]"})
    records = [{"package_id": "package", "last_modified": None, "rule_version": "001"}]
    metadata = checkpoint_metadata(prior, "sample", records)
    assert metadata["source"] == "retained"
    assert metadata["spicy_regs.read_checkpoints.other.v1"] == "[]"
    assert "ARROW:schema" not in metadata
    output = tmp_path / "output.parquet"
    _write(output, {key.encode(): value.encode() for key, value in metadata.items()})
    assert pq.read_metadata(output).num_rows == 0
    assert read_checkpoints(output, "sample") == records


@pytest.mark.parametrize("encoded", [b"not-json", b"null", b"{}", b'[{}, "wrong"]', b"\xff", b'[{"page": NaN}]'])
def test_malformed_metadata_never_establishes_a_successful_read(tmp_path, encoded):
    path = tmp_path / "prior.parquet"
    _write(path, {KEY: encoded})
    assert read_checkpoints(path, "sample") == []
    assert checkpoint_metadata(path, "sample", []) == {KEY.decode(): "[]"}


def test_absent_metadata_does_not_establish_a_successful_read(tmp_path):
    assert read_checkpoints(None, "sample") == []
    path = tmp_path / "prior.parquet"
    _write(path, {})
    assert read_checkpoints(path, "sample") == []


def test_unrelated_non_utf8_metadata_is_not_silently_lost(tmp_path):
    path = tmp_path / "prior.parquet"
    _write(path, {b"other": b"\xff"})
    with pytest.raises(UnicodeDecodeError):
        checkpoint_metadata(path, "sample", [])


def test_nonfinite_values_cannot_be_written_as_checkpoint_json():
    with pytest.raises(ValueError):
        checkpoint_metadata(None, "sample", [{"page": float("nan")}])
