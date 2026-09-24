"""Pins the manifest seed: Mirrulations keys derived from published ids, and the publish guard."""

import json
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts import seed_manifest_from_published as seed
from spicy_regs.manifest import MANIFEST_SCHEMA, Manifest
from spicy_regs.schemas import COMMENT, DOCKET, DOCUMENT
from spicy_regs.sources import iceberg


def _write(path: Path, record_type, rows: list[dict]) -> Path:
    full = [{col: None for col in record_type.schema} | row for row in rows]
    pl.DataFrame(full, schema=record_type.schema).write_parquet(path)
    return path


@pytest.fixture
def published(tmp_path: Path) -> dict[str, Path]:
    return {
        "docket": _write(
            tmp_path / "dockets.parquet",
            DOCKET,
            [{"docket_id": "EPA-HQ-OAR-2024-0001", "agency_code": "EPA"}],
        ),
        "documents": _write(
            tmp_path / "documents.parquet",
            DOCUMENT,
            [
                {"document_id": "EPA-HQ-OAR-2024-0001-0002", "docket_id": "EPA-HQ-OAR-2024-0001", "agency_code": "EPA"},
                # No docket_id: the mirror files it under the id minus its last segment.
                {"document_id": "NIH_FRDOC_0001-3422", "docket_id": None, "agency_code": "NIH"},
                # The mirror files by agencyId, not the docket's prefix.
                {"document_id": "DHS_FRDOC_0001-2737", "docket_id": None, "agency_code": "CISA"},
            ],
        ),
        "comments": _write(
            tmp_path / "comments.parquet",
            COMMENT,
            [
                # A comment filed under a docket its id does not name.
                {"comment_id": "EPA-HQ-OAR-2023-0009-0007", "docket_id": "EPA-HQ-OAR-2024-0001", "agency_code": "EPA"},
                {"comment_id": None, "docket_id": "EPA-HQ-OAR-2024-0001", "agency_code": "EPA"},
            ],
        ),
    }


def test_build_derives_one_mirror_key_per_published_id(tmp_path: Path, published: dict[str, Path]) -> None:
    receipt = seed.build(tmp_path / "seed", published)

    manifest = tmp_path / "seed" / "manifest.parquet"
    assert pq.read_schema(manifest).equals(MANIFEST_SCHEMA)
    assert pq.read_table(manifest).column("key").to_pylist() == [
        "raw-data/CISA/DHS_FRDOC_0001/text-DHS_FRDOC_0001/documents/DHS_FRDOC_0001-2737.json",
        "raw-data/EPA/EPA-HQ-OAR-2024-0001/text-EPA-HQ-OAR-2024-0001/comments/EPA-HQ-OAR-2023-0009-0007.json",
        "raw-data/EPA/EPA-HQ-OAR-2024-0001/text-EPA-HQ-OAR-2024-0001/docket/EPA-HQ-OAR-2024-0001.json",
        "raw-data/EPA/EPA-HQ-OAR-2024-0001/text-EPA-HQ-OAR-2024-0001/documents/EPA-HQ-OAR-2024-0001-0002.json",
        "raw-data/NIH/NIH_FRDOC_0001/text-NIH_FRDOC_0001/documents/NIH_FRDOC_0001-3422.json",
    ]
    assert {kind: item["keys"] for kind, item in receipt["inputs"].items()} == {
        "docket": 1,
        "documents": 3,
        "comments": 1,
    }
    assert receipt["inputs"]["comments"]["null_ids"] == 1
    assert receipt["remote_writes"] == 0
    assert json.loads((tmp_path / "seed" / "manifest-seed.json").read_text()) == receipt
    # The ETL's own loader reads it.
    loaded = Manifest.load(tmp_path / "seed")
    assert "raw-data/NIH/NIH_FRDOC_0001/text-NIH_FRDOC_0001/documents/NIH_FRDOC_0001-3422.json" in loaded


def test_build_is_byte_reproducible(tmp_path: Path, published: dict[str, Path]) -> None:
    first = seed.build(tmp_path / "a", published)["output"]["sha256"]
    assert seed.build(tmp_path / "b", published)["output"]["sha256"] == first


def test_file_identity_matches_the_r2_etag_forms(tmp_path: Path) -> None:
    small = tmp_path / "small"
    small.write_bytes(b"x")
    assert seed.file_identity(small)["etag"] == '"9dd4e461268c8034f5c8564e155c67a6"'
    large = tmp_path / "large"
    large.write_bytes(b"\0" * (seed._ETAG_PART + 1))
    assert seed.file_identity(large)["etag"].endswith('-2"')


class _FakeS3:
    def __init__(self, objects: dict[str, tuple[str, int]]) -> None:
        self.objects = objects

    def head_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803 — boto3 kwargs
        from botocore.exceptions import ClientError

        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        etag, size = self.objects[Key]
        return {"ETag": etag, "ContentLength": size}


@pytest.fixture
def live(tmp_path: Path, published: dict[str, Path], monkeypatch: pytest.MonkeyPatch):
    """The seed, a bucket still holding its inputs, and a catalog seeded from them."""
    receipt = seed.build(tmp_path / "seed", published)
    s3 = _FakeS3({item["object"]: (item["etag"], item["bytes"]) for item in receipt["inputs"].values()})
    catalog = duckdb.connect()
    catalog.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    for kind, record_type in seed.CATALOG_KINDS.items():
        iceberg._ensure_table(catalog, record_type)
        catalog.execute(
            f"INSERT INTO {iceberg._qualified(record_type)} ({record_type.dedup_key}) "
            f"SELECT {record_type.dedup_key} FROM read_parquet('{published[kind]}') "
            f"WHERE {record_type.dedup_key} IS NOT NULL"
        )
    monkeypatch.setattr(seed.r2, "get_r2_client", lambda: s3)
    # A cursor per check: check closes its connection, the catalog lives on.
    monkeypatch.setattr(seed.iceberg, "_connect", catalog.cursor)
    return tmp_path / "seed", s3, catalog


def test_check_passes_when_the_catalog_holds_every_seeded_id(live) -> None:
    output_dir, _, _ = live
    assert seed.check(output_dir) == []


def test_check_refuses_a_changed_input_an_existing_manifest_and_a_missing_id(live) -> None:
    output_dir, s3, catalog = live
    s3.objects["comments.parquet"] = ('"changed-3"', 1)
    s3.objects["manifest.parquet"] = ('"m"', 1)
    catalog.execute(f"DELETE FROM {iceberg._qualified(DOCKET)}")

    problems = seed.check(output_dir)

    assert len(problems) == 3
    assert "comments.parquet is now" in problems[0]
    assert "already exists on R2" in problems[1]
    assert "1 of the seed's 1 dockets ids are not in the catalog" in problems[2]


def test_check_reports_a_missing_catalog_table_after_the_r2_findings(live, monkeypatch: pytest.MonkeyPatch) -> None:
    output_dir, s3, catalog = live
    s3.objects["manifest.parquet"] = ('"m"', 1)
    catalog.execute(f"DROP TABLE {iceberg._qualified(COMMENT)}")

    problems = seed.check(output_dir)

    assert len(problems) == 2
    assert "already exists on R2" in problems[0]
    assert problems[1].startswith(f"catalog table {iceberg._qualified(COMMENT)} is missing")
    # The CLI turns the same findings into exit 1 instead of a traceback.
    monkeypatch.setattr(seed, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["seed", "--output-dir", str(output_dir), "--check"])
    assert seed.main() == 1


def test_check_reports_an_unreachable_catalog_without_its_token(live, monkeypatch: pytest.MonkeyPatch) -> None:
    output_dir, _, _ = live
    token = "catalog-token-0123456789"
    monkeypatch.setenv("R2_CATALOG_TOKEN", token)

    def refuse():
        raise RuntimeError(f"HTTP 401 for token {token}\nsecond line")

    monkeypatch.setattr(seed.iceberg, "_connect", refuse)

    [problem] = seed.check(output_dir)

    assert problem == "the R2 Data Catalog cannot be reached: RuntimeError: HTTP 401 for token <redacted>"
