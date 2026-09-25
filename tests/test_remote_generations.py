"""Remote staging uses the ordinary byte, schema, membership and pointer gates."""

from dataclasses import replace
from hashlib import sha256
from io import BytesIO
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs import remote_generations as remote
from spicy_regs.sources import publication as pub
from spicy_regs.sources.remote_parquet import StoredParquet
from tests.generation_fakes import Store, error


class RemoteStore(Store):
    def __init__(self):
        super().__init__()
        self.before_copy = None
        self.ranges = []
        self.copied = []
        self.aborted = []

    def head_object(self, *, Bucket, Key):
        result = super().head_object(Bucket=Bucket, Key=Key)
        result["ETag"] = self.get_object(Bucket=Bucket, Key=Key)["ETag"]
        return result

    def get_object(self, *, Bucket, Key, Range=None, IfMatch=None):
        result = super().get_object(Bucket=Bucket, Key=Key)
        if IfMatch is not None and IfMatch != result["ETag"]:
            result["Body"].close()
            raise error("PreconditionFailed")
        if Range is None:
            return result
        self.ranges.append((Key, Range, IfMatch))
        raw = result["Body"].read()
        result["Body"].close()
        match = re.fullmatch(r"bytes=(\d*)-(\d*)", Range)
        assert match
        start = int(match[1]) if match[1] else max(0, len(raw) - int(match[2]))
        end = min(int(match[2]) if match[1] and match[2] else len(raw) - 1, len(raw) - 1)
        if start >= len(raw):
            failure = error("InvalidRange")
            failure.response["Error"]["ActualObjectSize"] = str(len(raw))
            raise failure
        return {"Body": BytesIO(raw[start:end + 1]), "ETag": result["ETag"], "ContentLength": end - start + 1,
                "ContentRange": f"bytes {start}-{end}/{len(raw)}",
                "ResponseMetadata": {"HTTPStatusCode": 206, "RetryAttempts": 0}}

    def upload_part_copy(self, *, Bucket, Key, UploadId, PartNumber,
                         CopySource, CopySourceIfMatch, CopySourceRange):
        if self.before_copy:
            self.before_copy(CopySource["Key"])
        result = self.get_object(**CopySource, IfMatch=CopySourceIfMatch, Range=CopySourceRange)
        try:
            self.uploads[Key].append(result["Body"].read())
        finally:
            result["Body"].close()
        assert PartNumber == len(self.uploads[Key])
        self.copied.append((Key, CopySource["Key"], CopySourceIfMatch, CopySourceRange))
        return {"CopyPartResult": {"ETag": str(PartNumber)}}

    def abort_multipart_upload(self, **kwargs):
        self.aborted.append(kwargs["Key"])
        return super().abort_multipart_upload(**kwargs)


def staged(store, prefix="stage/one", *, value="<p>literal &amp; Δ</p>", empty=False):
    schema = pa.schema([("id", pa.int64()), ("text", pa.string()), ("active", pa.bool_())])
    table = (schema.empty_table() if empty else pa.Table.from_pylist([
        {"id": 1, "text": value, "active": True}, {"id": 2, "text": "", "active": False},
        {"id": 3, "text": None, "active": None},
    ], schema=schema))
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    raw = buffer.getvalue()
    key = prefix + "/body.parquet"
    store.objects[key] = raw
    etag = store.get_object(Bucket="fork", Key=key)["ETag"]
    member = StoredParquet(key, len(raw), "sha256:" + sha256(raw).hexdigest(), etag, table.num_rows)
    return member, table


SCHEMAS = {"body": [("id", "BIGINT"), ("text", "VARCHAR"), ("active", "BOOLEAN")]}


def prepared(tmp_path, store, prefix="stage/one", **kwargs):
    member, table = staged(store, prefix, **kwargs)
    directory = tmp_path / prefix.replace("/", "-")
    artifact = remote.prepare_remote_generation(
        directory, client=store, bucket="fork", staging_prefix=prefix, members=[member],
        family="test", expected_keys=["body.parquet"], schemas=SCHEMAS,
    )
    return directory, member, table, artifact


def publish(store, directory, member, prior=None):
    return remote.publish_remote_generation(
        directory, client=store, bucket="fork", staging_prefix=member.key.rsplit("/", 1)[0],
        members=[member], prior_index=prior or pub.empty_index(),
    )


@pytest.mark.parametrize("empty", [False, True])
def test_full_remote_generation_and_publication_have_no_local_table_copy(tmp_path, monkeypatch, empty):
    store = RemoteStore()
    directory, member, table, artifact = prepared(tmp_path, store, empty=empty)
    assert {p.name for p in directory.iterdir()} == {"artifact.json", "members.json"}
    monkeypatch.setattr(remote, "PART_BYTES", 128)
    index = publish(store, directory, member)
    key, descriptor = pub.table_location(index, "body.parquet")
    assert descriptor is not None
    assert index["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    assert descriptor["rows"] == table.num_rows
    assert store.objects[key] == store.objects[member.key]
    assert pq.read_table(BytesIO(store.objects[key])).equals(table, check_metadata=True)
    assert len(store.copied) > 1
    assert store.writes[-1] == pub.INDEX_KEY
    assert store.ranges and all(pin == member.etag for _, _, pin in store.ranges)


def test_changed_source_after_hash_before_parquet_decode_refuses(tmp_path, monkeypatch):
    store = RemoteStore()
    directory, member, _, _ = prepared(tmp_path, store)
    original = remote._RemoteGenerationSource.table_info

    def changed(source, key):
        staged(store, value="different valid parquet")
        return original(source, key)

    monkeypatch.setattr(remote._RemoteGenerationSource, "table_info", changed)
    with pytest.raises(OSError, match="PreconditionFailed"):
        publish(store, directory, member)
    assert not store.copied and pub.INDEX_KEY not in store.objects


def test_changed_source_during_promotion_aborts_and_preserves_pointer(tmp_path):
    store = RemoteStore()
    old_dir, old, _, _ = prepared(tmp_path, store)
    prior = publish(store, old_dir, old)
    prior_bytes = store.objects[pub.INDEX_KEY]
    directory, member, _, _ = prepared(tmp_path, store, "stage/two", value="new source")
    store.before_copy = lambda key: store.objects.__setitem__(key, b"changed")
    with pytest.raises(Exception, match="PreconditionFailed"):
        publish(store, directory, member, prior)
    assert store.objects[pub.INDEX_KEY] == prior_bytes
    assert store.aborted and not store.uploads


@pytest.mark.parametrize("bad_field", ["rows", "sha256", "byte_size"])
def test_producer_receipt_is_not_independent_admission(tmp_path, bad_field):
    store = RemoteStore()
    member, _ = staged(store)
    wrong = {"rows": member.rows + 1, "sha256": "sha256:" + "0" * 64, "byte_size": member.byte_size + 1}
    with pytest.raises((ValueError, ArtifactVerificationError)):
        remote.prepare_remote_generation(
            tmp_path / "bad", client=store, bucket="fork", staging_prefix="stage/one",
            members=[replace(member, **{bad_field: wrong[bad_field]})], family="test",
            expected_keys=["body.parquet"], schemas=SCHEMAS,
        )
    assert pub.INDEX_KEY not in store.objects and not store.writes


def test_extra_remote_object_is_not_filtered_out(tmp_path):
    store = RemoteStore()
    member, _ = staged(store)
    store.objects["stage/one/unexpected"] = b"must not disappear from membership"
    with pytest.raises(ArtifactVerificationError):
        remote.prepare_remote_generation(
            tmp_path / "bad", client=store, bucket="fork", staging_prefix="stage/one",
            members=[member], family="test", expected_keys=["body.parquet"], schemas=SCHEMAS,
        )
    assert not store.writes


def test_existing_wrong_destination_cannot_pass_by_etag_or_size(tmp_path):
    store = RemoteStore()
    directory, member, _, artifact = prepared(tmp_path, store)
    prefix = "generations/test/" + artifact.pin.artifact_digest.removeprefix("sha256:")
    original = store.objects[member.key]
    store.objects[prefix + "/body.parquet"] = b"!" + original[1:]
    with pytest.raises(ArtifactVerificationError, match="digest"):
        publish(store, directory, member)
    assert pub.INDEX_KEY not in store.objects
    assert store.objects[prefix + "/body.parquet"] == b"!" + original[1:]


def test_corruption_after_promotion_never_updates_pointer(tmp_path):
    store = RemoteStore()
    directory, member, _, _ = prepared(tmp_path, store)
    store.corrupt_key = "body.parquet"
    with pytest.raises(ArtifactVerificationError, match="digest"):
        publish(store, directory, member)
    assert pub.INDEX_KEY not in store.objects


def test_concurrent_pointer_change_survives(tmp_path):
    store = RemoteStore()
    old_dir, old, _, _ = prepared(tmp_path, store)
    prior = publish(store, old_dir, old)
    directory, member, _, _ = prepared(tmp_path, store, "stage/two", value="new")
    rivals = []

    def concurrent(key):
        if key == pub.INDEX_KEY:  # a new object version on every attempt
            rivals.append(store.objects[key] + b" ")
            store.objects[key] = rivals[-1]

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="concurrently"):
        publish(store, directory, member, prior)
    assert store.objects[pub.INDEX_KEY] == rivals[-1]


def test_stale_family_refuses_before_promotion(tmp_path):
    store = RemoteStore()
    old_dir, old, _, _ = prepared(tmp_path, store)
    prior = publish(store, old_dir, old)
    new_dir, new, _, _ = prepared(tmp_path, store, "stage/two", value="new")
    publish(store, new_dir, new, prior)
    before = store.objects[pub.INDEX_KEY]
    copies = len(store.copied)
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, old_dir, old, prior)
    assert store.objects[pub.INDEX_KEY] == before and len(store.copied) == copies
