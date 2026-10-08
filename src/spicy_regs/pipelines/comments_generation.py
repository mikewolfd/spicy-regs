"""Seal the comments mirror and its aggregate index with exact ETL receipts."""
from dataclasses import asdict
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow.parquet as pq
import pyarrow as pa

from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, write_dataset
from spicy_regs.generations import build_generation
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.regulations_receipts import policy


def _index_records(path, generation_id, witness):
    ordinal = 0
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=2000):
            for row in batch.to_pylist():
                yield row, ReceiptContext(generation_id, str(ordinal), "comments-index/1", [witness])
                ordinal += 1


def stage_comments_remote(records, current, *, failures, generation_id, snapshot,
                          output_dir, client, bucket, staging_prefix, max_bytes,
                          previous_url, batch_size=128):
    """Write both large members from one input pass, retaining only narrow index scratch.

    Returned upload receipts remain unpublished and require full generation
    admission. The same splitter, history inheritance, index builder and
    predecessor checks serve local and remote output.
    """
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _dataset_batches
    from spicy_regs.sources.remote_parquet import remote_parquet_writer, open_pinned_s3
    from spicy_regs.sources.iceberg import _build_comments_index
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.pipelines.comments_mirror import validate_export

    if (not staging_prefix or staging_prefix != staging_prefix.strip("/")
            or client.list_objects_v2(Bucket=bucket, Prefix=staging_prefix + "/", MaxKeys=1).get("Contents")):
        raise ValueError("Remote Comments needs a new, empty attempt staging prefix")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with remote_parquet_writer(client=client, bucket=bucket, key=staging_prefix + "/etl_receipts.parquet",
                               schema=RECEIPT_SCHEMA, max_bytes=max_bytes) as receipt_upload:
        with remote_parquet_writer(client=client, bucket=bucket, key=staging_prefix + "/comments.parquet",
                                   schema=current.subject_schema, max_bytes=max_bytes) as subject_upload:
            for subject, receipt in _dataset_batches(records, current, failures=failures, batch_size=batch_size):
                if subject is not None:
                    subject_upload.write(subject)
                receipt_upload.write(receipt)
        subject = subject_upload.stored
        assert subject is not None
        columns = ("comment_id", "agency_code", "docket_id", "posted_date")

        def candidates(con):
            # This is narrow matching/index data, never a complete local body.
            # Pinned reads refuse a changed remote member on every request.
            with open_pinned_s3(client=client, bucket=bucket, key=subject.key, etag=subject.etag) as stream, \
                    pq.ParquetFile(stream) as parquet:
                if not parquet.schema_arrow.equals(current.subject_schema, check_metadata=True):
                    raise ValueError("Staged Comments schema differs")
                schema = pa.schema([current.subject_schema.field(name) for name in columns])
                reader = pa.RecordBatchReader.from_batches(
                    schema, parquet.iter_batches(batch_size=2000, columns=columns, use_threads=False))
                con.register("remote_comments", reader)
                con.execute("CREATE TABLE candidate AS SELECT * FROM remote_comments")
                con.unregister("remote_comments")
            _build_comments_index(con, RECORD_TYPES["comments"], output_dir,
                                  source_sql="SELECT * FROM candidate")
            con.from_parquet(str(output_dir / "comments_index.parquet")).create_view("candidate_index")

        predecessor = validate_export(output_dir, previous_url, candidate_builder=candidates)
        index_policy = policy("comments_index")
        witness = {"source_id": "comments", "source_uri": None, "sha256": subject.sha256,
                   "locator": json.dumps(snapshot, sort_keys=True), "body_version": generation_id}
        with remote_parquet_writer(client=client, bucket=bucket, key=staging_prefix + "/comments_index.parquet",
                                   schema=index_policy.subject_schema, max_bytes=max_bytes) as index_upload:
            for index, receipt in _dataset_batches(
                    _index_records(output_dir / "comments_index.parquet", generation_id, witness),
                    index_policy, batch_size=2000):
                assert index is not None
                index_upload.write(index)
                receipt_upload.write(receipt)
    assert receipt_upload.stored is not None and index_upload.stored is not None
    return subject, receipt_upload.stored, index_upload.stored, predecessor


def publish_prepared_comments_remote(output_dir, *, client, bucket, reader, snapshot,
                                     prepared_sha256, source_guard):
    """Re-admit pinned staging; check source and predecessor immediately before each pointer attempt."""
    from rulespec_artifacts import ArtifactPin
    from spicy_regs.remote_generations import publish_remote_generation
    from spicy_regs.sources.remote_parquet import StoredParquet
    from spicy_regs.sources import r2
    from spicy_regs.pipelines.comments_mirror import _check_prepared_snapshot
    from spicy_regs.public_url import resolve_r2_base_url

    output_dir = Path(output_dir)
    path = output_dir / "comments-remote-preparation.json"

    def prepared():
        if file_identity(path)["sha256"] != prepared_sha256:
            raise ValueError("Remote preparation record differs from its explicit pin")
        return json.loads(path.read_text())

    record = prepared()
    derivation = record["derivation"]
    if (record["format"] != "comments-remote-preparation/1"
            or record["status"] != "remote-generation-validated-unpublished" or record["published"] is not False
            or record["bucket"] != bucket or record["source"] != asdict(snapshot)
            or derivation["snapshot"] != record["source"] or derivation["rows"] != 26_418_078
            or derivation["current_policy"] != policy("comments").descriptor()):
        raise ValueError("Expected one fully qualified remote Comments preparation")
    directory = Path(record["generationDirectory"])
    if not directory.resolve().is_relative_to(output_dir.resolve()):
        raise ValueError("Remote generation metadata is outside its owned output")
    members = [StoredParquet(**member) for member in record["members"]]
    if {Path(member.key).name for member in members} != {
            "comments.parquet", "comments_index.parquet", "etl_receipts.parquet"}:
        raise ValueError("Remote Comments membership differs")
    base = resolve_r2_base_url()

    def guard():
        if prepared() != record:
            raise ValueError("Remote preparation changed before publication")
        source_guard()
        _check_prepared_snapshot(reader, snapshot)
        version = r2.public_object_version(base + "/comments.parquet")
        if version is None or version["etag"] != record["predecessor"]["etag"]:
            raise ValueError("Prior Comments mirror changed before publication")

    guard()
    pin = record["artifactPin"]
    return publish_remote_generation(
        directory, client=client, bucket=bucket, staging_prefix=record["stagingPrefix"], members=members,
        prior_index=record["priorIndex"], bounded_receipts=True,
        expected_pin=ArtifactPin(pin["logicalId"], pin["artifactDigest"]), before_pointer=guard)


def build_comments_generation(output_dir, result, snapshot):
    """Require the catalog's selected pair; derive index receipts from that body."""
    metadata = json.loads(result["generation"].read_text())
    if metadata.get("dataset") != "comments" or not metadata.get("generation_id"):
        raise ValueError("Comments export has no selected receipt generation")
    generation_id = metadata["generation_id"]
    comments = result["comments"]
    if metadata.get("snapshot") != asdict(snapshot):
        raise ValueError("Comments receipts name a different catalog snapshot")
    index_policy = policy("comments_index")
    witness = {"source_id": "comments", "source_uri": None,
               "sha256": file_identity(comments)["sha256"],
               "locator": json.dumps(asdict(snapshot), sort_keys=True), "body_version": generation_id}
    with TemporaryDirectory(prefix="comments-index-receipts-", dir=output_dir) as temporary:
        temporary = Path(temporary)
        index_subject, index_receipts = write_dataset(
            _index_records(result["index"], generation_id, witness), temporary / "index", index_policy)
        assert index_subject is not None
        shared = combine_receipts([result["receipts"], index_receipts], temporary / "etl_receipts.parquet")
        directory = output_dir / ".comments-generations" / uuid4().hex
        build_generation(directory, family="comments", files=[comments, index_subject],
                         expected_keys=("comments.parquet", "comments_index.parquet"),
                         parents={"catalog_comments": {"sha256": witness["sha256"], "byteSize": comments.stat().st_size}},
                         receipt_path=shared, receipt_policies=[policy("comments"), index_policy],
                         receipt_generation_id=generation_id, adopt_owned_receipt=True)
    return directory
