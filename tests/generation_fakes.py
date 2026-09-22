"""In-memory S3 fake for tests: conditional puts, multipart uploads, pagination; no credentials or network access."""

from hashlib import sha256
from io import BytesIO

from botocore.exceptions import ClientError


def error(code):
    return ClientError({"Error": {"Code": code}}, "test")


class Store:
    """Records successful put keys in order; ``before_put`` and ``corrupt_key`` are fault-injection hooks."""

    def __init__(self):
        self.objects = {}
        self.writes = []
        self.copies = []
        self.uploads = {}
        self.before_put = None
        self.corrupt_key = None

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise error("NoSuchKey")
        return {"ContentLength": len(self.objects[Key])}

    def copy_object(self, *, Bucket, Key, CopySource, **kwargs):
        source_key = CopySource["Key"]
        if source_key not in self.objects:
            raise error("NoSuchKey")
        if Key in self.objects:
            raise error("PreconditionFailed")
        self.objects[Key] = self.objects[source_key]
        self.writes.append(Key)
        self.copies.append(Key)
        return {}

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise error("NoSuchKey")
        raw = self.objects[Key]
        return {"Body": BytesIO(raw), "ETag": '"' + sha256(raw).hexdigest() + '"'}

    def put_object(self, *, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, **kwargs):
        if self.before_put:
            self.before_put(Key)
        if IfNoneMatch == "*" and Key in self.objects:
            raise error("PreconditionFailed")
        if IfMatch is not None:
            if Key not in self.objects or IfMatch != self.get_object(Bucket=Bucket, Key=Key)["ETag"]:
                raise error("PreconditionFailed")
        raw = Body.read() if hasattr(Body, "read") else Body
        if self.corrupt_key and Key.endswith(self.corrupt_key):
            raw = raw[:-1] + bytes([raw[-1] ^ 1])
        self.objects[Key] = raw
        self.writes.append(Key)
        return {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        # Two pages prove the adapter consumes the whole prefix listing.
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        yield {"Contents": [{"Key": k} for k in keys[:1]]}
        yield {"Contents": [{"Key": k} for k in keys[1:]]}

    def create_multipart_upload(self, *, Bucket, Key, **kwargs):
        self.uploads[Key] = []
        return {"UploadId": Key}

    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body, ContentMD5):
        assert PartNumber == len(self.uploads[Key]) + 1
        self.uploads[Key].append(Body)
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload, IfNoneMatch):
        self.put_object(Bucket=Bucket, Key=Key, Body=b"".join(self.uploads[Key]), IfNoneMatch=IfNoneMatch)
        del self.uploads[Key]

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        self.uploads.pop(Key, None)
