"""Publish the compatible comments mirror once per verified catalog snapshot."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import json
from os import getenv
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.comments_health import check_comments, check_retained_ids
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, r2, publication
from spicy_regs.pipelines.comments_generation import build_comments_generation
from spicy_regs.transforms.partition_comments import agency_comments

FORMAT_VERSION = 1
RECEIPT_KEY = "comments-publication.json"
MIN_EXPECTED_ROWS = 1_000_000
_AGENCY_KEY = re.compile(r"comments/agency/agency_code=[A-Za-z0-9_-]+/part-0\.parquet\Z")


@dataclass(frozen=True)
class PublicPredecessor:
    etag: str
    agencies: frozenset[str]


def validate_export(output_dir: Path, previous_url: str, *,
                    resources: ExportResources | None = None) -> PublicPredecessor:
    """Keep every prior ID; refuse a moving predecessor and inconsistent coverage."""
    resources = resources or ExportResources()
    before = r2.public_object_version(previous_url)
    if before is None:
        raise RuntimeError("Prior comments mirror is missing; cannot verify its population")
    with TemporaryDirectory(prefix="comments-check-", dir=output_dir) as spill, duckdb.connect() as con:
        resources.configure(con, Path(spill))
        con.from_parquet(str(output_dir / "comments.parquet")).create_view("candidate")
        con.from_parquet(str(output_dir / "comments_index.parquet")).create_view("candidate_index")
        con.from_parquet(previous_url).create_view("previous")
        errors = check_comments(con, "SELECT * FROM candidate", "SELECT * FROM candidate_index")
        errors += check_retained_ids(con, "SELECT * FROM previous", "SELECT * FROM candidate")
        if errors:
            raise RuntimeError("Refusing comments publication: " + "; ".join(errors))
        agencies = frozenset(row[0] for row in con.execute("SELECT DISTINCT agency_code FROM previous").fetchall())
        if any(not isinstance(a, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", a) for a in agencies):
            raise RuntimeError("Prior comments mirror has invalid agency codes")
    if r2.public_object_version(previous_url) != before:
        raise RuntimeError("Prior comments mirror changed during validation; retry on a stable predecessor")
    return PublicPredecessor(before["etag"], agencies)


def _retain_empty_agencies(result: dict[str, Path], predecessor: PublicPredecessor, receipt: dict | None) -> None:
    """Clear old fixed agency URLs when their last row moves to another agency."""
    agencies = set(predecessor.agencies)
    # Carry forward earlier empty replacements as well as the current parent.
    if isinstance(receipt, dict) and isinstance(receipt.get("files"), dict):
        agencies.update(key.split("agency_code=", 1)[1].split("/", 1)[0]
                        for key in receipt["files"] if _AGENCY_KEY.fullmatch(key))
    schema = pq.ParquetFile(result["comments"]).schema_arrow
    schema = pa.schema([field for field in schema if field.name != "agency_code"])
    for agency in sorted(agencies):
        path = result["partitions"] / f"agency_code={agency}" / "part-0.parquet"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_batches([], schema=schema), path, compression="zstd")


def check_receipt_rows(output_dir: Path, files: dict[str, dict], exported_rows: int) -> None:
    """Refuse a receipt whose ``rows`` the published files do not bear out.

    The server states ``rows`` from the receipt without opening a file, so each must be the sum of its own file's row
    groups; the monolith's must be the count the export validated; and the agency files must sum to it, the rule
    :func:`_receipt_matches` holds a published receipt to. It runs on the local files before any upload, so a refusal
    leaves the public files and their receipt describing the same generation.
    """
    errors = []
    for key, item in files.items():
        metadata = pq.ParquetFile(output_dir / key).metadata
        grouped = sum(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups))
        if item["rows"] != grouped:
            errors.append(f"{key} states {item['rows']:,} rows and its row groups hold {grouped:,}")
    if files["comments.parquet"]["rows"] != exported_rows:
        errors.append(f"comments.parquet states {files['comments.parquet']['rows']:,} rows; the export validated "
                      f"{exported_rows:,}")
    agencies = sum(item["rows"] for key, item in files.items() if _AGENCY_KEY.fullmatch(key))
    if agencies != exported_rows:
        errors.append(f"the agency files hold {agencies:,} rows; the export validated {exported_rows:,}")
    if errors:
        raise RuntimeError("Refusing the comments receipt: " + "; ".join(errors))


def _receipt_matches(receipt: dict | None, snapshot: iceberg.CatalogPairSnapshot, base_url: str) -> bool:
    """A successful receipt plus unchanged public/storage versions permits a no-op."""
    if not isinstance(receipt, dict) or receipt.get("format_version") != FORMAT_VERSION:
        return False
    if receipt.get("source") != asdict(snapshot):
        return False
    generation = receipt.get("generation")
    if not isinstance(generation, dict):
        return False
    if publication.current_index(base_url)["families"].get("comments") != generation:
        return False
    files = receipt.get("files")
    if not isinstance(files, dict) or not {"comments.parquet", "comments_index.parquet"} < files.keys():
        return False
    for key, item in files.items():
        if key not in {"comments.parquet", "comments_index.parquet"} and not _AGENCY_KEY.fullmatch(key):
            return False
        if (not isinstance(item, dict) or not isinstance(item.get("etag"), str) or not item["etag"]
                or not isinstance(item.get("bytes"), int) or item["bytes"] <= 0
                or not isinstance(item.get("rows"), int) or item["rows"] < 0
                or not isinstance(item.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])):
            return False
    if files["comments.parquet"]["rows"] < MIN_EXPECTED_ROWS or files["comments_index.parquet"]["rows"] < 1:
        return False
    if sum(item["rows"] for key, item in files.items() if _AGENCY_KEY.fullmatch(key)) != files["comments.parquet"]["rows"]:
        return False

    def unchanged(entry: tuple[str, dict]) -> bool:
        key, item = entry
        expected = {name: item[name] for name in ("etag", "bytes")}
        return (r2.object_version(key) == expected
                and r2.public_object_version(f"{base_url}/{key}") == expected)

    with ThreadPoolExecutor(max_workers=8) as pool:
        return all(list(pool.map(unchanged, files.items())))


def _prepare_catalog(record_type) -> None:
    """Finish or resume native pair selection before pinning a mirror read."""
    prepared = iceberg._connect_for_table(record_type)
    prepared.close()


def publish_comments_mirror(output_dir: Path, *, resources: ExportResources | None = None,
                            skip_upload: bool = False, force: bool = False) -> bool:
    """Build/verify/publish; return False only for a verified unchanged publication.

    The caller holds comments-catalog-write for the whole operation. Fixed public
    keys are not an atomic generation. Advance the receipt only after all files
    upload and their public bytes match; any interruption stays retryable.
    """
    resources = resources or ExportResources()
    rt = RECORD_TYPES["comments"]
    if not skip_upload and not getenv("R2_ACCESS_KEY_ID"):
        raise RuntimeError("R2 upload credentials are required to publish the comments mirror")
    # Establish the selected native pair before pinning the export snapshot.
    # This also resumes a migration interrupted after empty-table preparation.
    _prepare_catalog(rt)
    snapshot = iceberg.catalog_snapshot(rt)
    base_url = resolve_r2_base_url().rstrip("/")
    receipt = None
    if not skip_upload:
        if not getenv("R2_ACCESS_KEY_ID"):
            raise RuntimeError("R2 upload credentials are required to publish the comments mirror")
        receipt = None if force else r2.read_json_object(RECEIPT_KEY)
        if not force and _receipt_matches(receipt, snapshot, base_url):
            logger.info("Comments snapshot {} already published; verified object versions, skipping build", snapshot.snapshot_id)
            return False

    prior_index = publication.current_index(base_url) if not skip_upload else publication.empty_index()
    result = iceberg.export_public_comments(output_dir, rt, resources=resources, snapshot=snapshot)
    n_rows = pq.ParquetFile(result["comments"]).metadata.num_rows
    if n_rows < MIN_EXPECTED_ROWS:
        raise RuntimeError(f"Comments export has {n_rows:,} rows, below the {MIN_EXPECTED_ROWS:,} safety floor")
    previous_url = f"{base_url}/comments.parquet"
    predecessor = validate_export(output_dir, previous_url, resources=resources)
    _retain_empty_agencies(result, predecessor, receipt)
    agency_files = sorted(result["partitions"].glob("agency_code=*/part-0.parquet"))
    with TemporaryDirectory(prefix="comments-check-", dir=output_dir) as spill, duckdb.connect() as con:
        resources.configure(con, Path(spill))
        agency_comments(con, agency_files).create_view("partitions")
        con.from_parquet(str(result["index"])).create_view("candidate_index")
        con.from_parquet(str(result["comments"])).create_view("candidate")
        errors = check_comments(con, "SELECT * FROM partitions", "SELECT * FROM candidate_index")
        errors += check_retained_ids(con, "SELECT * FROM candidate", "SELECT * FROM partitions")
        if errors:
            raise RuntimeError("Invalid comments partitions: " + "; ".join(errors))
    (output_dir / "comments-build.json").write_text(json.dumps({
        "format_version": FORMAT_VERSION, "source": asdict(snapshot),
        "resources": asdict(resources), "rows": n_rows, "predecessor_etag": predecessor.etag,
    }, indent=2, sort_keys=True) + "\n")
    generation_directory = build_comments_generation(output_dir, result, snapshot)
    if skip_upload:
        logger.info("Verified local comments generation in {}", generation_directory)
        return True

    # The export pins subjects and receipts together. A rejected attempt can change
    # only receipts, so check both tables; compaction may move either without changing rows.
    if not iceberg.rows_unchanged_since(rt, snapshot):
        raise RuntimeError("Catalog changed during export; refusing publication")
    current = r2.public_object_version(previous_url)
    if current is None or current["etag"] != predecessor.etag:
        raise RuntimeError("Prior comments mirror changed before publication; refusing overwrite")
    files = [result["comments"], *agency_files, result["index"]]
    rows = {path.relative_to(output_dir).as_posix(): {"rows": pq.ParquetFile(path).metadata.num_rows} for path in files}
    check_receipt_rows(output_dir, rows, n_rows)
    r2.preflight_uploads(output_dir, files)
    r2.upload_file(result["comments"], remote_key="comments.parquet")
    r2.upload_comment_partitions(output_dir, agency_files)

    def verify(path: Path) -> tuple[str, dict]:
        key = path.relative_to(output_dir).as_posix()
        return key, {**r2.verify_public_file(path, key, base_url), **rows[key]}

    with ThreadPoolExecutor(max_workers=4) as pool:
        descriptors = dict(pool.map(verify, files))
    published_index = publication.publish_generation(
        generation_directory, client=r2.get_r2_client(), bucket=getenv("R2_BUCKET_NAME", "spicy-regs"),
        prior_index=prior_index)
    receipt = {"format_version": FORMAT_VERSION, "source": asdict(snapshot), "files": descriptors,
               "generation": published_index["families"]["comments"]}
    receipt_path = output_dir / RECEIPT_KEY
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    r2.upload_file(receipt_path, remote_key=RECEIPT_KEY, cache_control="no-cache, must-revalidate")
    r2.verify_public_file(receipt_path, RECEIPT_KEY, base_url)
    logger.info("Published and read back comments snapshot {}", snapshot.snapshot_id)
    return True
