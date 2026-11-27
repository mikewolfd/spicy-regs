"""Retain owner captures and bind their bytes to a separate audit artifact.

Original responses and acquisition receipts use the central raw archive. Run
metadata and generated inputs stay with the output; EvidenceMemberSource reads
original bytes without copying them into each run. This records observed
responses, not complete source coverage. SpicyDocs owns acquisition and storage;
Rulespec owns integrity and artifact admission.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import zlib
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, IO, Any
from uuid import uuid4

from rulespec_artifacts import (
    ArtifactInput,
    LocalMemberSource,
    MemberSource,
    Producer,
    admit_artifact,
    build_artifact_root,
    canonical_json_bytes,
    describe_member,
    write_member_manifest,
)

if TYPE_CHECKING:
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_docs.transport.captured import CapturedBodyResponse

KIND = "spicy-regs-source-evidence"
INPUT_ROLE = "source-evidence"
PRIOR_ROLE = "prior-generation"
JOURNAL = "journal.jsonl"
_CHUNK = 1024 * 1024
POLICIES = frozenset({"full", "hash_only", "metadata_only"})
EVIDENCE_VERSION = 2
#: Journaled policy decision of evidence not bound to a publisher; a root ``CompletedCapture`` carries the same one.
_DEFAULT_POLICY_DECISION = "existing-full-default"
#: Receipt fields only the retaining operation sets; caller metadata never replaces them.
_COMPRESSED_ENCODINGS = frozenset({"gzip", "x-gzip", "deflate"})
_COMPRESSION_FIELDS = frozenset(
    {
        "wire_sha256",
        "wire_byte_size",
        "decoded_sha256",
        "decoded_byte_size",
        "decoded_status",
    }
)
_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_OWNER_FIELDS = frozenset(
    {
        "event",
        "recorded_at",
        "publisher_id",
        "evidence_policy",
        "parser_version",
        "policy_decision_id",
        "capture_id",
        "response_complete",
        "stage",
        "name",
        "sha256",
        "byte_size",
        "body_retained",
        "blob_member",
        "blob_store",
        "request_body",
        "response_headers",
        "archive_receipt_key",
        *_COMPRESSION_FIELDS,
    }
)
ARCHIVE_REFERENCE = "source-archive.json"
#: A path on a machine that builds: a Mac or Linux home, a temporary, system or mounted-volume directory, a GitHub
#: runner workspace or a home-relative path, alone or in a file: URL. Journals are published (ss-81bv, 2026-11-10:
#: owner home paths in five families). URLs, object keys and XPaths do not match, nor Windows drive paths: no build
#: runs on Windows, and publishers print them (a House PDF names G:\GSDD\SOD\...).
_LOCAL_ROOT = (r"(?:(?<![\w.:/~-])|(?<=file://))(?:/Users|/home|/root|/private|/var|/tmp|/Volumes|/mnt|/opt|/usr|/srv"
               r"|/__w|/github)/|(?<![\w.:/~-])~/")
_LOCAL_PATH = re.compile(_LOCAL_ROOT)
_LOCAL_PATH_BYTES = re.compile(_LOCAL_ROOT.encode())
#: Derived inputs a build writes as JSON text for itself (by file suffix, or bytes that open an object or array); a
#: retained one is published, so it may not name a local path. Binary inputs such as Parquet are not read for paths.
_JSON_CONTROLS = frozenset({".json", ".jsonl"})
_JSON_TEXT = re.compile(rb"\s*[\[{]")
#: The same path to its end, in free text such as an exception message.
_LOCAL_SPAN = re.compile(rf"(?:{_LOCAL_ROOT})[^\s'\"`<>|,;)\]}}]*")
#: Free-text diagnostics: a path inside one is cut to its file name rather than refused, so an unexpected exception
#: message never fails a run. Every other journaled string naming a local path is refused.
_DIAGNOSTICS = frozenset({"message", "error", "reason"})


def local_path_name(text: str) -> str:
    """``text`` with each local path cut to ``<local>/`` and its file name: what a published diagnostic may say."""
    return _LOCAL_SPAN.sub(lambda match: "<local>/" + re.split(r"[\\/]", match[0].rstrip("\\/"))[-1], text)


def without_local_paths(value):
    """A JSON value with every string (key or value) that holds a local path cut to its file name.

    For control files a build writes for itself, such as a selection or normalization manifest, whose published copy
    must not name the build machine's directories. Digests and URLs in it are unchanged.
    """
    if isinstance(value, str):
        return local_path_name(value)
    if isinstance(value, dict):
        result = {local_path_name(key): without_local_paths(item) for key, item in value.items()}
        if len(result) != len(value):
            raise ValueError("Two local paths with one file name cannot be told apart once cut")
        return result
    if isinstance(value, list):
        return [without_local_paths(item) for item in value]
    return value


def is_original(event: dict) -> bool:
    """Whether a journal event records a publisher original rather than a file the build made for itself.

    A capture is an original. A retained file is one only when ``raw_source=True`` sent its bytes to the central
    raw archive, which the event records as ``blob_store: central-raw``; selections, manifests, receipts and
    normalized inputs stay with their evidence. The marker is as old as the archive pointer
    (``ARCHIVE_REFERENCE``), so every journal beside a pointer states it.
    """
    return event.get("event") == "capture" or (
        event.get("event") == "retained-file" and event.get("blob_store") == "central-raw")


class SourceEvidenceError(RuntimeError):
    """Source retention failed; never turn this into a skippable source refusal."""


@dataclass(frozen=True)
class _RetainedBlobSource:
    """Open pinned raw members without the blob reader's preliminary hash pass."""

    store: CentralRawArchiveStore

    def open(self, blob_ref: str):
        return self.store.open(blob_ref)


@contextmanager
def acquisition_evidence(family: str, *, output_dir: Path | None = None):
    """Finish an acquisition's durable receipt even when its reader fails.

    Standalone readers need no persistent run directory: central originals and
    receipts survive removal of their temporary artifact metadata.
    """
    workspace = nullcontext(output_dir) if output_dir is not None else tempfile.TemporaryDirectory(prefix="source-evidence-")
    with workspace as directory:
        evidence = CaptureEvidence(Path(directory), family)
        try:
            yield evidence
        except BaseException as error:
            evidence.finish(error)
            raise
        else:
            evidence.finish()


class EvidenceMemberSource:
    """Read run metadata locally and source bodies from the central archive.

    The local pointer is transport configuration, outside the immutable artifact.
    Artifact admission still hashes every original body against its member pin.
    Moving an archive requires only updating SPICYGOV_RAW_ROOT, never its receipts.
    """

    def __init__(self, directory: Path, *, archive_root: Path | None = None):
        self.local = LocalMemberSource(directory)
        pointer = directory.parent / ARCHIVE_REFERENCE
        self.references = json.loads(pointer.read_bytes()) if pointer.exists() else None
        self.blobs = {}
        if self.references is not None:
            with self.local.open(JOURNAL) as stream:
                for line in stream:
                    event = json.loads(line)
                    for body in (event, event.get("request_body"), event.get("response")):
                        if isinstance(body, dict) and body.get("blob_store") == "central-raw":
                            key = "blobs/sha256/" + body["sha256"].removeprefix("sha256:")
                            if body.get("blob_member") != key:
                                raise SourceEvidenceError("Invalid central source member reference")
                            self.blobs[key] = body
        if self.references is None:
            self.store = None
        else:
            from spicy_docs.storage.raw_archive import CentralRawArchiveStore

            self.store = CentralRawArchiveStore(
                archive_root or Path(os.environ.get("SPICYGOV_RAW_ROOT", self.references["root"])), create=False
            )

    def keys(self):
        return sorted(set(self.local.keys()) | self.blobs.keys())

    def open(self, object_key):
        if object_key in self.blobs:
            assert self.store is not None
            return self.store.open(self.blobs[object_key]["sha256"])
        return self.local.open(object_key)


def _normalize_encoding(content_encoding: str | None) -> str:
    """Normalized lower-case HTTP content-encoding token, defaulting to ``identity``."""
    return (content_encoding or "identity").strip().lower() or "identity"


def _is_valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def _new_decompressor(encoding: str) -> Any:
    if encoding in ("gzip", "x-gzip"):
        return zlib.decompressobj(16 + zlib.MAX_WBITS)
    if encoding == "deflate":
        return zlib.decompressobj(zlib.MAX_WBITS)
    return None


class _StreamCheck:
    """One pass over streamed bytes: byte bound, credential scan across chunk boundaries and SHA-256.

    Only the last ``len(secret) - 1`` bytes carry over between chunks, so no
    chunk is copied whole and nothing grows with the stream. ``scan_paths`` also
    notes, in the same pass, whether the bytes name a local path (``local_path``).
    For compressed transport encodings (``gzip``, ``x-gzip``, ``deflate``), the
    same streaming pass incrementally decompresses in ``_CHUNK`` slices to bind
    ``decoded_sha256`` and ``decoded_byte_size`` separately from the retained wire
    bytes without buffering the decoded payload in memory.
    """

    def __init__(
        self,
        evidence: CaptureEvidence | SourceEvidenceContext,
        max_bytes: int,
        *,
        scan_paths: bool = False,
        content_encoding: str | None = "identity",
        max_decoded_bytes: int | None = None,
    ):
        self.evidence, self.max_bytes = evidence, max_bytes
        self.secret = evidence.credential.encode()
        self.digest, self.size, self.tail = hashlib.sha256(), 0, b""
        self.scan_paths, self.local_path, self.path_tail = scan_paths, False, b""
        self.encoding = _normalize_encoding(content_encoding)
        self.compressed = self.encoding != "identity"
        self.supported_compression = self.encoding in _COMPRESSED_ENCODINGS
        self.max_decoded_bytes = max_decoded_bytes
        self.decoded_digest = hashlib.sha256()
        self.decoded_size = 0
        self.decoded_tail = b""
        self.decoded_failed = False
        self._decoder: Any = _new_decompressor(self.encoding) if self.supported_compression else None
        self._deflate_started = False

    def _scan_secret(self, data: bytes, *, decoded: bool) -> None:
        if not self.secret or not data:
            return
        keep = len(self.secret) - 1
        tail = self.decoded_tail if decoded else self.tail
        if self.secret in data or self.secret in tail + data[:keep]:
            from spicy_docs.transport.credentials import CredentialRefusedError

            raise CredentialRefusedError("Source response refused; credential-bearing bytes were not retained")
        new_tail = (tail + data[-keep:])[-keep:] if keep else b""
        if decoded:
            self.decoded_tail = new_tail
        else:
            self.tail = new_tail

    def _update_decoded(self, decoded: bytes) -> None:
        if not decoded:
            return
        self.decoded_size += len(decoded)
        if self.max_decoded_bytes is not None and self.decoded_size > self.max_decoded_bytes:
            raise self.evidence._failure("Decoded source response exceeds its retention byte bound")
        self._scan_secret(decoded, decoded=True)
        self.decoded_digest.update(decoded)

    def _decompress_chunk(self, chunk: bytes) -> None:
        if not self.supported_compression or self._decoder is None or self.decoded_failed or not chunk:
            return
        pending = chunk
        is_gzip = self.encoding in ("gzip", "x-gzip")
        try:
            while pending:
                if self._decoder.eof:
                    if not is_gzip:
                        raise zlib.error("Trailing data after deflate stream")
                    self._decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
                try:
                    decoded = self._decoder.decompress(pending, _CHUNK)
                except zlib.error:
                    if self.encoding == "deflate" and not self._deflate_started:
                        # RFC 1951 raw deflate fallback on the opening chunk, matching HTTPX.
                        self._decoder = zlib.decompressobj(-zlib.MAX_WBITS)
                        self._deflate_started = True
                        decoded = self._decoder.decompress(pending, _CHUNK)
                    else:
                        raise
                self._deflate_started = True
                if decoded:
                    self._update_decoded(decoded)
                if self._decoder.unconsumed_tail:
                    pending = self._decoder.unconsumed_tail
                elif self._decoder.eof and self._decoder.unused_data:
                    pending = self._decoder.unused_data
                else:
                    pending = b""
        except zlib.error:
            self.decoded_failed = True
            self._decoder = None

    def update(self, chunk: bytes) -> None:
        self.size += len(chunk)
        if self.size > self.max_bytes:
            # A retention failure, not an item refusal: the bound equals the owner's own, so only a response
            # that states no length can get here, and letting its bytes reach the owner unretained would leave
            # a used response the run could not prove. The run fails loudly instead.
            raise self.evidence._failure("Source response exceeds its retention byte bound")
        self._scan_secret(chunk, decoded=False)
        if self.scan_paths and not self.local_path:
            # A root and its lookbehind ("file://") span at most 17 bytes, so 24 carried bytes see one a chunk splits.
            window = self.path_tail + chunk[:24]
            self.local_path = bool(_LOCAL_PATH_BYTES.search(chunk) or _LOCAL_PATH_BYTES.search(window))
            self.path_tail = (self.path_tail + chunk[-24:])[-24:]
        self.digest.update(chunk)
        if self.compressed:
            self._decompress_chunk(chunk)

    def finish(self) -> None:
        """Finalize streaming decompression and verify stream completion (catching truncated payloads)."""
        if not self.supported_compression or self.decoded_failed:
            return
        if self._decoder is None or self.size == 0:
            self.decoded_failed = True
            self._decoder = None
            return
        try:
            rest = self._decoder.flush()
            if rest:
                self._update_decoded(rest)
        except zlib.error:
            self.decoded_failed = True
            self._decoder = None
            return
        if not self._decoder.eof or self._decoder.unused_data:
            self.decoded_failed = True
            self._decoder = None

    @property
    def sha256(self) -> str:
        return "sha256:" + self.digest.hexdigest()

    @property
    def decoded_sha256(self) -> str | None:
        if not self.compressed:
            return self.sha256
        if not self.supported_compression or self.decoded_failed:
            return None
        return "sha256:" + self.decoded_digest.hexdigest()

    def compression_fields(self, policy: str) -> dict[str, str | int]:
        """Separate wire and decoded identity fields for compressed captures; empty for ``identity``."""
        if not self.compressed:
            return {}
        fields: dict[str, str | int] = {"wire_byte_size": self.size}
        if policy != "metadata_only":
            fields["wire_sha256"] = self.sha256
        if not self.supported_compression:
            fields["decoded_status"] = "unsupported_encoding"
        elif self.decoded_failed:
            fields["decoded_status"] = "undecodable"
        else:
            fields["decoded_byte_size"] = self.decoded_size
            if policy != "metadata_only":
                assert self.decoded_sha256 is not None
                fields["decoded_sha256"] = self.decoded_sha256
        return fields


def _check_expected_digests(
    evidence: CaptureEvidence | SourceEvidenceContext,
    check: _StreamCheck,
    *,
    expected_wire_sha256: str | None = None,
    expected_decoded_sha256: str | None = None,
) -> None:
    """Reject mismatches on either wire (compressed) or decoded digests before journaling."""
    if expected_wire_sha256 is not None:
        if not _is_valid_sha256(expected_wire_sha256):
            raise evidence._failure("Invalid expected wire_sha256 digest")
        if check.sha256 != expected_wire_sha256:
            raise evidence._failure("Captured wire digest differs from expected wire_sha256")
    if expected_decoded_sha256 is not None:
        if not _is_valid_sha256(expected_decoded_sha256):
            raise evidence._failure("Invalid expected decoded_sha256 digest")
        if check.decoded_sha256 is None:
            raise evidence._failure("Compressed capture could not be decoded to verify expected decoded_sha256")
        if check.decoded_sha256 != expected_decoded_sha256:
            raise evidence._failure("Captured decoded digest differs from expected decoded_sha256")


class CaptureEvidence:
    """A durable journal followed by an immutable, independently admitted artifact.

    Any retention failure is remembered: once one has been raised, this
    evidence seals only as ``failed``, even when a caller's broad ``except``
    treated the error as one item's source refusal.
    """

    def __init__(self, output_dir: Path, family: str, *, archive_root: Path | None = None):
        from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
        from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root

        self.directory = output_dir / "source-evidence" / uuid4().hex
        self.artifact_dir = self.directory / "artifact"
        self.artifact_dir.mkdir(parents=True)
        self.store = CentralRawArchiveStore(archive_root or default_raw_archive_root())
        self.local_store = LocalSourceNativeBlobStore(self.artifact_dir / "blobs")
        self.run_id = self.directory.name
        self._pending_blobs: set[str] = set()
        self._pending_archive_receipts: list[dict] = []
        self._write_archive_reference()
        self.family = family
        self.policy = "full"
        self._source_policies: dict[str, tuple[str, str, str]] = {}
        self._restricted_diagnostics = False
        self.credential = ""
        self.artifact = None
        self.prior_input: ArtifactInput | None = None
        self.read_snapshot: dict | None = None
        self.retention_failure: str | None = None
        #: Where the prior generation's own evidence journal is, and its events once read.
        self._prior_evidence: tuple[str | None, dict] | None = None
        self._prior_events: list[dict] | None = None
        self._used_input_scope: tuple[str, bool] | None = None
        self._used_inputs: dict[tuple, dict] = {}
        self._direct_selection = False
        self.event(
            "run",
            family=family,
            packages={name: version(name) for name in ("spicy-regs", "spicy-docs")},
            coverage="Observed responses only; caps, refusals and inherited gaps remain explicit.",
        )

    def for_source(
        self, publisher_id: str, policy: str = "hash_only", *, parser_version: str, policy_decision_id: str
    ) -> SourceEvidenceContext:
        """Bind immutable publisher policy to this run's one evidence artifact."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot bind a source to sealed evidence")
        if policy not in POLICIES or not all(
            isinstance(v, str) and v.strip() for v in (publisher_id, parser_version, policy_decision_id)
        ):
            raise SourceEvidenceError("Invalid source evidence policy context")
        selection = (policy, parser_version, policy_decision_id)
        if publisher_id in self._source_policies and self._source_policies[publisher_id] != selection:
            raise self._failure("Contradictory source evidence policy context")
        if publisher_id not in self._source_policies:
            self.event(
                "source-policy",
                publisher_id=publisher_id,
                evidence_policy=policy,
                parser_version=parser_version,
                policy_decision_id=policy_decision_id,
            )
        self._source_policies[publisher_id] = selection
        self._restricted_diagnostics |= policy != "full"
        return SourceEvidenceContext(self, publisher_id, policy, parser_version, policy_decision_id)

    def _body(self, body: bytes, *, raw_source: bool = True, digest: str | None = None) -> dict:
        """Public receipt for bytes, retaining them only under full policy."""
        result: dict[str, str | int | bool] = {"byte_size": len(body), "body_retained": self.policy == "full"}
        if self.policy != "metadata_only":
            computed = digest if digest is not None else "sha256:" + hashlib.sha256(body).hexdigest()
            result["sha256"] = computed
            if self.policy == "full":
                self._blob(body, raw_source=raw_source, digest=computed)
                result["blob_member"] = "blobs/sha256/" + computed.removeprefix("sha256:")
                if raw_source:
                    result["blob_store"] = "central-raw"
        return result

    def _safe(self, value, field: str = ""):
        """Redact the run's credential, and refuse a local path, in ``value`` (at ``field``) before it is journaled.

        The journal is published. A diagnostic (``_DIAGNOSTICS``) keeps only a local path's file name; any other
        string naming one raises, so its caller records a URL, a logical name or a content digest instead.
        """
        if isinstance(value, str):
            from spicy_docs.transport.credentials import scrub_credential

            # The owner scrubber handles named query credentials. The literal
            # replacement also covers short injected test credentials.
            result = scrub_credential(value, self.credential)
            result = result.replace(self.credential, "<redacted>") if self.credential else result
            if field.rsplit(".", 1)[-1] in _DIAGNOSTICS:
                return local_path_name(result)
            if _LOCAL_PATH.search(result):
                raise self._failure(
                    f"{self.family}: {field or 'a journaled value'} names a local path, and the journal is published; "
                    "record a URL, a logical name or a content digest"
                )
            return result
        if isinstance(value, dict):
            for key in value:
                if isinstance(key, str) and _LOCAL_PATH.search(key):
                    raise self._failure(f"{self.family}: a key of {field or 'a journaled value'} names a local path, "
                                        "and the journal is published")
            return {key: self._safe(item, f"{field}.{key}" if field else key) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [self._safe(item, field + "[]") for item in value]
        return value

    def _failure(self, message: str) -> SourceEvidenceError:
        """Remember the first retention failure and return the error to raise for it."""
        self.retention_failure = self.retention_failure or message
        return SourceEvidenceError(message)

    def event(self, event: str, **fields) -> str | None:
        """Append one fsynced event and return its central receipt key when one was committed."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if event in {"capture", "retained-file", "refusal", "capture-incomplete"}:
            fields.setdefault("evidence_policy", self.policy)
            fields.setdefault("parser_version", version("spicy-docs"))
            fields.setdefault("policy_decision_id", _DEFAULT_POLICY_DECISION)
            fields.setdefault("publisher_id", None)
        # Outside the try below: a refused local path names its event, stage and field, not a storage failure.
        record = self._safe({"event": event, "recorded_at": datetime.now(UTC).isoformat(), **fields},
                            f"{event}({fields['stage']})" if fields.get("stage") else event)
        try:
            key = None
            raw = canonical_json_bytes(record)
            # Preserve both the shared serving-rung identity and legacy archive dates beside the captures.
            if event in {"capture", "refusal", "capture-incomplete", "build-outcome",
                         "senate-archive-capture", "source-serving-rung"} or (
                event == "retained-file" and fields.get("blob_store") == "central-raw"
            ):
                # Commit source provenance before the disposable run journal.
                # A run-output failure must not orphan a retained original.
                key = self._archive_receipt(json.loads(raw))
            with (self.artifact_dir / JOURNAL).open("ab") as stream:
                stream.write(raw + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            return key
        except Exception as error:
            raise self._failure("Cannot retain source evidence journal") from error

    def _write_archive_reference(self) -> None:
        pointer = self.directory / ARCHIVE_REFERENCE
        temporary = pointer.with_suffix(".tmp")
        with temporary.open("wb") as stream:
            stream.write(canonical_json_bytes({"root": str(self.store.root)}))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(pointer)

    def _archive_receipt(self, receipt: dict) -> str | None:
        self._pending_archive_receipts.append(receipt)
        return self._flush_archive_receipts()

    def _flush_archive_receipts(self) -> str | None:
        key = None
        while self._pending_archive_receipts:
            receipt = self._pending_archive_receipts[0]
            key = self._commit_archive_receipt(receipt)
            for body in (receipt, receipt.get("request_body"), receipt.get("response")):
                if isinstance(body, dict) and body.get("blob_store") == "central-raw":
                    self._pending_blobs.discard(body["sha256"])
            self._pending_archive_receipts.pop(0)
        return key

    def _commit_archive_receipt(self, receipt: dict) -> str:
        files = []
        identity = {key: receipt[key] for key in (
            "dataset_id", "native_id", "covered_period", "source_version", "name", "original_location",
            "source_family", "source_record_key", "edition", "source_locator", "source_validation",
        ) if key in receipt}
        for body in (receipt, receipt.get("request_body"), receipt.get("response")):
            if isinstance(body, dict) and body.get("blob_store") == "central-raw":
                compression = {
                    key: body[key]
                    for key in ("content_encoding", *_COMPRESSION_FIELDS)
                    if key in body
                }
                files.append({
                    **identity,
                    **compression,
                    "sha256": body["sha256"], "bytes": body["byte_size"],
                    "family": self.family,
                    "source_url": receipt.get("resolved_url") or receipt.get("source_url")
                    or receipt.get("requested_url") or body.get("request_key"),
                    "observed_at": receipt.get("observed_at"),
                    "acquired_at": receipt.get("observed_at") if receipt.get("event") == "capture" else None,
                    "http_status": receipt.get("status_code"),
                    "response_complete": receipt.get("response_complete"),
                    "role": "request-body" if body is receipt.get("request_body") else receipt.get("role", "source-body"),
                })
        try:
            result = self.store.record_receipt(files=files, receipt={**receipt, "run_id": self.run_id, "family": self.family})
            return result["object"]
        except Exception as error:
            raise self._failure("Cannot retain source acquisition receipt") from error

    def _put_body(self, digest: str, byte_size: int, chunks, *, raw_source: bool) -> None:
        store = self.store if raw_source else self.local_store
        store.put_blob(digest, byte_size, chunks)
        if raw_source:
            self._pending_blobs.add(digest)

    def _blob(self, body: bytes, *, raw_source: bool = True, digest: str | None = None) -> dict:
        """Store response bytes content-addressed; refuses once the artifact has been sealed."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        computed = digest if digest is not None else "sha256:" + hashlib.sha256(body).hexdigest()
        try:
            self._put_body(computed, len(body), [body], raw_source=raw_source)
        except Exception as error:
            raise self._failure("Cannot retain source response bytes") from error
        return {"sha256": computed, "byte_size": len(body)}

    def capture(
        self,
        capture: CapturedBodyResponse,
        *,
        stage: str,
        source_validation: str | None = None,
        expected_wire_sha256: str | None = None,
        expected_decoded_sha256: str | None = None,
        max_decoded_bytes: int | None = None,
        **fields,
    ) -> dict:
        """Retain one owner capture's body (and request body) into the evidence store.

        The returned ``archive_receipt_key`` locates the committed central receipt.
        Anything that is not a ``CapturedBodyResponse`` raises
        ``SourceEvidenceError``; a credential-bearing or 401/403 response raises
        ``CredentialRefusedError`` and journals only a refusal reason and status.
        For compressed captures (``gzip``, ``x-gzip``, ``deflate``), wire and
        decoded digests are bound separately via bounded streaming decompression,
        and mismatches against ``expected_wire_sha256`` or ``expected_decoded_sha256``
        refuse before retention.
        """
        from spicy_docs.transport.captured import CapturedBodyResponse
        from spicy_docs.transport.credentials import CredentialRefusedError

        if not isinstance(capture, CapturedBodyResponse):
            raise SourceEvidenceError("Source result has no owner capture")
        access_refused = capture.status_code in (401, 403)
        if access_refused or (
            self.credential
            and (
                any(self.credential.encode() in body for body in (capture.body, capture.request_body or b""))
                or any(self.credential in value for _name, value in capture.response_headers)
            )
        ):
            # Never store a credential-refused original, including one returned
            # by an injected adapter that bypassed the owner's transport.
            self.event(
                "refusal",
                stage=stage,
                error_type="CredentialRefusedError",
                reason_code="access_refused" if access_refused else "credential_echo",
                status_code=capture.status_code,
                body_retained=False,
                credential_refused=True,
                message=None,
                response=None,
            )
            message = (
                f"Source access refused (HTTP {capture.status_code}); response bytes were not retained"
                if access_refused
                else "Source response refused; credential-bearing bytes were not retained"
            )
            raise CredentialRefusedError(message)
        if self.policy != "full" and source_validation not in {None, "refused"}:
            raise SourceEvidenceError("Non-retaining source validation must use the fixed refusal marker")
        check = _StreamCheck(
            self,
            len(capture.body),
            content_encoding=capture.content_encoding,
            max_decoded_bytes=max_decoded_bytes,
        )
        try:
            body_len = len(capture.body)
            for offset in range(0, body_len, _CHUNK):
                check.update(capture.body[offset : offset + _CHUNK])
            check.finish()
        except CredentialRefusedError:
            self.event(
                "refusal",
                stage=stage,
                error_type="CredentialRefusedError",
                reason_code="credential_echo",
                status_code=capture.status_code,
                body_retained=False,
                credential_refused=True,
                message=None,
                response=None,
            )
            raise
        _check_expected_digests(
            self,
            check,
            expected_wire_sha256=expected_wire_sha256,
            expected_decoded_sha256=expected_decoded_sha256,
        )
        response = {
            **self._body(capture.body, digest=check.sha256),
            **check.compression_fields(self.policy),
        }
        return self._record_capture(
            response,
            stage=stage,
            requested_url=capture.requested_url,
            resolved_url=capture.resolved_url,
            observed_at=capture.observed_at,
            status_code=capture.status_code,
            content_type=capture.content_type,
            content_encoding=capture.content_encoding,
            method=capture.method,
            request_body=capture.request_body,
            **({"response_headers": list(capture.response_headers)}
               if capture.response_headers and self.policy == "full" else {}),
            **({"source_validation": source_validation} if source_validation is not None else {}),
            **{key: value for key, value in fields.items() if key not in _OWNER_FIELDS
               and key not in {"requested_url", "resolved_url", "observed_at", "status_code",
                               "content_type", "content_encoding", "method"}},
        )

    def _retain_file(
        self,
        stream: IO[bytes],
        *,
        sha256: str,
        byte_size: int,
        stage: str,
        requested_url: str,
        resolved_url: str,
        observed_at: str,
        status_code: int,
        content_type: str | None,
        content_encoding: str | None,
        method: str,
        request_body: bytes | None,
        response_headers: list[tuple[str, str]] | None = None,
        compression: dict[str, str | int] | None = None,
    ) -> dict:
        """Retain a complete, already bounded and credential-scanned file in one read.

        The store re-hashes while it copies and refuses bytes that differ from
        ``sha256``/``byte_size``, so a file changed after acquisition never seals.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if status_code in (401, 403) or (self.credential and request_body and self.credential.encode() in request_body):
            from spicy_docs.transport.credentials import CredentialRefusedError

            raise CredentialRefusedError("Source response refused; credential-bearing bytes were not retained")
        response = {**self._file_blob(stream, sha256=sha256, byte_size=byte_size), **(compression or {})}
        return self._record_capture(
            response,
            stage=stage,
            requested_url=requested_url,
            resolved_url=resolved_url,
            observed_at=observed_at,
            status_code=status_code,
            content_type=content_type,
            content_encoding=content_encoding,
            method=method,
            request_body=request_body,
            **({"response_headers": response_headers} if response_headers else {}),
        )

    def _record_capture(self, response: dict, *, request_body: bytes | None, **metadata) -> dict:
        """Journal already retained response bytes before exposing a completed reference."""
        request = self._body(request_body) if request_body is not None else None
        receipt = dict(
            **metadata,
            capture_id=uuid4().hex,
            evidence_policy=self.policy,
            response_complete=True,
            request_body=request,
            **response,
        )
        key = self.event("capture", **receipt)
        if key is not None:
            receipt["archive_receipt_key"] = key
        return receipt

    def _file_blob(self, stream: IO[bytes], *, sha256: str, byte_size: int, raw_source: bool = True) -> dict:
        """Store bounded chunks, checking the complete bytes against their selected identity."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        try:
            stream.seek(0)
            if self.policy == "full":
                self._put_body(sha256, byte_size, iter(lambda: stream.read(_CHUNK), b""), raw_source=raw_source)
            else:
                check = _StreamCheck(self, byte_size)
                for chunk in iter(lambda: stream.read(_CHUNK), b""):
                    check.update(chunk)
                if check.sha256 != sha256 or check.size != byte_size:
                    raise ValueError("Changed source file")
        except Exception as error:
            raise self._failure("Cannot retain source file bytes") from error
        result: dict[str, str | int | bool] = {"byte_size": byte_size, "body_retained": self.policy == "full"}
        if self.policy != "metadata_only":
            result["sha256"] = sha256
        if self.policy == "full":
            result["blob_member"] = "blobs/sha256/" + sha256.removeprefix("sha256:")
            if raw_source:
                result["blob_store"] = "central-raw"
        return result

    def retain_bytes(self, body: bytes, *, stage: str, raw_source: bool = False, **fields) -> dict:
        """Retain already bounded local input bytes without inventing an HTTP capture.

        Use the existing local-input receipt, with opaque observation identity.
        Restricted policies omit caller metadata because it can contain source
        excerpts or byte-derived identifiers, just as ``retain_file`` does.
        Set ``raw_source=True`` only for publisher originals or acquisition
        receipts. Selected fields, manifests and computed inputs stay run-local.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if not isinstance(body, bytes):
            raise SourceEvidenceError("Retained input must be immutable bytes")
        if self.credential and self.credential.encode() in body:
            from spicy_docs.transport.credentials import CredentialRefusedError

            raise CredentialRefusedError("Source input refused; credential-bearing bytes were not retained")
        if not raw_source and _JSON_TEXT.match(body) and _LOCAL_PATH_BYTES.search(body):
            raise self._local_control(stage)
        return self._retained(self._body(body, raw_source=raw_source), stage=stage, fields=fields)

    def retain_control(self, path: Path, *, stage: str, **fields) -> dict:
        """Retain a JSON control file the build wrote for itself, such as a selection or a manifest, cut of local paths.

        Its local copy names this machine's directories, and retained inputs are published (ss-81bv). The cut copy
        keeps what was selected and pinned, digests and URLs unchanged; ``local_sha256`` binds it to the exact local
        bytes, which stay where the build read them.
        """
        raw = path.read_bytes()
        body = json.dumps(without_local_paths(json.loads(raw)), sort_keys=True, separators=(",", ":")).encode()
        return self.retain_bytes(body, stage=stage, original_location=path.name,
                                 local_sha256="sha256:" + hashlib.sha256(raw).hexdigest(), **fields)

    def _local_control(self, stage: str, name: str | None = None) -> SourceEvidenceError:
        return self._failure(
            f"{self.family}: derived JSON input {name or ''}({stage}) names a local path, and retained inputs are "
            "published; retain a copy cut by without_local_paths, or record a URL or digest"
        )

    def _retained(self, blob: dict, *, stage: str, fields: dict, name: str | None = None) -> dict:
        """Journal one local retained input; caller metadata cannot replace an owner-set field."""
        receipt = {k: v for k, v in fields.items() if k not in _OWNER_FIELDS} if self.policy == "full" else {}
        if self.policy == "full" and name is not None:
            receipt["name"] = name
        receipt.update(capture_id=uuid4().hex, evidence_policy=self.policy, response_complete=True, stage=stage, **blob)
        key = self.event("retained-file", **receipt)
        if key is not None:
            receipt["archive_receipt_key"] = key
        return receipt

    def retain_file(self, path: Path, *, stage: str, raw_source: bool = False, **fields) -> dict:
        """Retain a local file the run read rather than fetched, its exact bytes and digest, and journal it.

        For a one-time input kept outside the repository, such as a converted table, so the generation's evidence
        holds the bytes its rows came from. Hash and scan bounded chunks, then let the existing store verify
        them again while retaining them. A changed file cannot be journaled as a successful retention.
        ``raw_source=True`` sends an explicit publisher original to the central
        archive; converted tables and other generated inputs remain run-local.

        ``original_location`` defaults to the file's name: its directory is the build machine's, and the journal is
        published (ss-81bv). Pass the URL the bytes came from, or a logical name, when one exists.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        from spicy_docs.transport.credentials import CredentialRefusedError

        def identity(info):
            return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns

        try:
            with path.open("rb") as stream:
                before = identity(os.fstat(stream.fileno()))
                check = _StreamCheck(self, before[2], scan_paths=not raw_source and path.suffix in _JSON_CONTROLS)
                for chunk in iter(lambda: stream.read(_CHUNK), b""):
                    check.update(chunk)
                if (
                    check.size != before[2]
                    or identity(os.fstat(stream.fileno())) != before
                    or identity(path.stat()) != before
                ):
                    raise self._failure("Source file changed during retention")
                if check.local_path:
                    raise self._local_control(stage, path.name)
                blob = self._file_blob(stream, sha256=check.sha256, byte_size=check.size, raw_source=raw_source)
                if identity(os.fstat(stream.fileno())) != before or identity(path.stat()) != before:
                    raise self._failure("Source file changed during retention")
        except (SourceEvidenceError, CredentialRefusedError):
            raise
        except Exception as error:
            raise self._failure("Cannot retain source file bytes") from error
        # Arbitrary caller fields and paths may contain excerpts; non-retaining
        # contexts publish only the bounded receipt, never local extraction names.
        return self._retained(blob, stage=stage, fields={"original_location": path.name, **fields}, name=path.name)

    def transport(
        self,
        transport=None,
        *,
        stage: str,
        max_bytes: int,
        response_header_names: tuple[str, ...] = (),
        max_decoded_bytes: int | None = None,
        expected_wire_sha256: str | None = None,
        expected_decoded_sha256: str | None = None,
    ):
        """Observe streamed HTTP responses before owner parsing, including retries.

        The source still owns requests, pacing and validation. This wrapper only
        retains exact transport bytes and refuses retention failures.
        ``response_header_names`` opts into selected response metadata under full
        retention; the default copies no headers. Credential echoes still refuse.
        Redirects
        are individual observations, so signed credentials are scrubbed by the
        same journal policy as ordinary captures. Full-retention bytes go
        directly through the maintained private blob writer, then the owner
        replays those retained bytes with renewed integrity. Restricted
        policies use a private temporary replay file and retain no body blob.
        For compressed responses, wire and decoded digests are bound separately
        in one streaming pass without buffering the decoded body in memory.
        """
        import httpx
        from spicy_docs.transport.captured import CompletedCapture
        from spicy_docs.transport.credentials import CredentialRefusedError

        evidence = self
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if max_decoded_bytes is not None and (type(max_decoded_bytes) is not int or max_decoded_bytes < 1):
            raise ValueError("max_decoded_bytes must be positive")

        class RetainedStream(httpx.SyncByteStream):
            def __init__(self, stream, request, request_body, response, observed_at):
                self.stream, self.request, self.response = stream, request, response
                self.request_body, self.observed_at = request_body, observed_at
                # RetainedTransport rebinds this to the final response's own mapping (HTTPX copies the argument).
                self.extensions: dict[str, Any] = {}

            def __iter__(self):
                retained = False
                metadata: dict[str, Any] = dict(
                    stage=stage,
                    requested_url=str(self.request.url),
                    resolved_url=str(self.request.url),
                    observed_at=self.observed_at,
                    status_code=self.response.status_code,
                    content_type=self.response.headers.get("content-type"),
                    content_encoding=self.response.headers.get("content-encoding", "identity"),
                    method=self.request.method,
                    request_body=self.request_body,
                    **({"response_headers": [
                        (name, value) for name in response_header_names for value in self.response.headers.get_list(name)
                    ]} if response_header_names and evidence.policy == "full" else {}),
                )
                check = _StreamCheck(
                    evidence,
                    max_bytes,
                    content_encoding=metadata["content_encoding"],
                    max_decoded_bytes=max_decoded_bytes,
                )
                try:
                    if evidence.artifact is not None:
                        raise SourceEvidenceError("Cannot append to sealed source evidence")
                    if self.response.status_code in (401, 403) or (
                        evidence.credential and self.request_body and evidence.credential.encode() in self.request_body
                    ) or (
                        evidence.credential and any(
                            evidence.credential in value for _name, value in metadata.get("response_headers", ())
                        )
                    ):
                        raise CredentialRefusedError(
                            "Source response refused; credential-bearing bytes were not retained"
                        )
                    if evidence.policy == "full":
                        source_error = None

                        def checked_chunks():
                            nonlocal source_error
                            try:
                                for chunk in self.stream:
                                    check.update(chunk)
                                    yield chunk
                            except BaseException as error:
                                source_error = error
                                raise

                        try:
                            # Rulespec's private .pending file replaces the old
                            # evidence spool; no public blob appears before EOF
                            # and the complete credential scan.
                            result = evidence.store.put_stream(checked_chunks(), max_bytes=max_bytes)
                        except BaseException as error:
                            if error is source_error or not isinstance(error, Exception):
                                raise
                            raise evidence._failure("Cannot retain source response bytes") from error
                        check.finish()
                        if result.blob_ref != check.sha256 or result.byte_size != check.size:
                            raise evidence._failure("Retained wire bytes differ from observed stream digest")
                        _check_expected_digests(
                            evidence,
                            check,
                            expected_wire_sha256=expected_wire_sha256,
                            expected_decoded_sha256=expected_decoded_sha256,
                        )
                        receipt = evidence._record_capture(
                            {
                                "sha256": result.blob_ref,
                                "byte_size": result.byte_size,
                                "body_retained": True,
                                "blob_store": "central-raw",
                                "blob_member": "blobs/sha256/" + result.blob_ref.removeprefix("sha256:"),
                                **check.compression_fields(evidence.policy),
                            },
                            **metadata,
                        )
                        completed = CompletedCapture(
                            source=_RetainedBlobSource(evidence.store),
                            sha256=result.blob_ref,
                            byte_size=result.byte_size,
                            capture_id=receipt["capture_id"],
                            publisher_id=getattr(evidence, "publisher_id", None),
                            evidence_policy=evidence.policy,
                            parser_version=getattr(evidence, "parser_version", version("spicy-docs")),
                            policy_decision_id=getattr(evidence, "policy_decision_id", _DEFAULT_POLICY_DECISION),
                            content_encoding=metadata["content_encoding"],
                            _failure=evidence._failure,
                        )
                        retained = True
                        self.extensions["spicy_docs.completed_capture"] = completed
                        # Keep raw encoded evidence even if HTTPX's subsequent
                        # decoding refuses it. Every consumer exhausts a fresh,
                        # bounded integrity pass over the retained blob.
                        yield from completed.iter_bytes()
                    else:
                        with tempfile.NamedTemporaryFile(prefix="spicy-source-private-") as stream:

                            def staged(action, *args):
                                # Only the private spool is a retention failure; a source error propagates unchanged.
                                try:
                                    return action(*args)
                                except (OSError, ValueError) as error:
                                    raise evidence._failure("Cannot stage private source response bytes") from error

                            for chunk in self.stream:
                                check.update(chunk)
                                staged(stream.write, chunk)
                            staged(stream.flush)
                            check.finish()
                            _check_expected_digests(
                                evidence,
                                check,
                                expected_wire_sha256=expected_wire_sha256,
                                expected_decoded_sha256=expected_decoded_sha256,
                            )
                            evidence._retain_file(
                                stream,
                                sha256=check.sha256,
                                byte_size=check.size,
                                compression=check.compression_fields(evidence.policy),
                                **metadata,
                            )
                            retained = True
                            stream.seek(0)
                            while chunk := stream.read(_CHUNK):
                                yield chunk
                except BaseException as error:
                    if not retained:
                        evidence.event(
                            "capture-incomplete",
                            stage=stage,
                            error_type=type(error).__name__,
                            evidence_policy=evidence.policy,
                            response_complete=False,
                            body_retained=False,
                            requested_url=str(self.request.url),
                            bytes_received=check.size,
                        )
                    raise

            def close(self):
                self.stream.close()

        class RetainedTransport(httpx.BaseTransport):
            def __init__(self):
                self.inner = transport if transport is not None else httpx.HTTPTransport()

            def handle_request(self, request):
                # Read the body before it is sent: a redirect's request carries an explicit
                # stream whose content httpx never reads (RequestNotRead), and read() swaps
                # in a replayable stream, so the wire send still has every byte.
                request_body = request.read() or None
                observed_at = datetime.now(UTC).isoformat()
                response = self.inner.handle_request(request)
                stream = RetainedStream(response.stream, request, request_body, response, observed_at)
                # A reference appears in this shared extensions mapping only
                # after complete capture, retention and journal completion.
                extensions = dict(response.extensions)
                extensions.pop("spicy_docs.completed_capture", None)
                observed = httpx.Response(
                    response.status_code,
                    headers=response.headers,
                    stream=stream,
                    extensions=extensions,
                )
                stream.extensions = observed.extensions
                return observed

            def close(self):
                self.inner.close()

        return RetainedTransport()

    def refusal(self, error: BaseException, *, stage: str) -> None:
        """Retain a refusal event, attaching any capture the error carries.

        A ``SourceEvidenceError`` is re-raised unchanged, and a retention
        failure is wrapped in one.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if isinstance(error, SourceEvidenceError):
            raise error
        from spicy_docs.reading.refusals import RefusedResponse, retain_refused_response
        from spicy_docs.transport.captured import attached_capture
        from spicy_docs.transport.credentials import CredentialRefusedError

        retained = None
        restricted_root = (
            self.policy == "full" and self._restricted_diagnostics and not isinstance(self, SourceEvidenceContext)
        )
        if not isinstance(error, CredentialRefusedError) and not restricted_root:
            capture = attached_capture(error)
            if capture is not None:
                # The exact bytes are retained whole; a second, bounded copy would only duplicate them.
                self.capture(capture, stage=stage + ":refused", source_validation="refused")
            elif isinstance(error, Exception) and self.policy != "full":
                refused = getattr(error, "refused_response", None)
                if isinstance(refused, RefusedResponse) and refused.response_bytes is not None:
                    if self.credential and self.credential.encode() in refused.response_bytes:
                        raise CredentialRefusedError(
                            "Source response refused; credential-bearing bytes were not retained"
                        )
                    retained = self._body(refused.response_bytes)
            elif isinstance(error, Exception):
                try:
                    retained = retain_refused_response(
                        error, store=self.store, max_bytes=24 * 1024 * 1024, credential=self.credential
                    )
                    if retained and "sha256" in retained:
                        retained.update(
                            body_retained=True,
                            byte_size=retained.pop("bytes"),
                            blob_member="blobs/sha256/" + retained["sha256"].removeprefix("sha256:"),
                            blob_store="central-raw",
                        )
                except Exception as failure:
                    raise self._failure("Cannot retain refused source response") from failure
        self.event(
            "refusal",
            stage=stage,
            error_type=type(error).__name__,
            evidence_policy=self.policy,
            message=self._safe(str(error), "message")[:2000] if self.policy == "full" and not restricted_root else None,
            reason_code="source_refused",
            source_validation="refused",
            response=retained,
            credential_refused=isinstance(error, CredentialRefusedError),
        )

    def inherit(self, prior_index: dict, *, public_url: str | None) -> None:
        """Record the prior generation's pins; a managed prior without its immutable root raises."""
        from spicy_regs.sources.publication import load_family_root

        self.read_snapshot = prior_index
        prior = prior_index["families"].get(self.family)
        if prior is None:
            self.event(
                "lineage",
                prior_generation=None,
                evidence_status="No managed prior; this run starts new source evidence.",
            )
            return
        self.prior_input = ArtifactInput(PRIOR_ROLE, prior["logicalId"], prior["artifactDigest"])
        if public_url is None:
            raise SourceEvidenceError("Managed prior evidence requires its immutable root")
        raw, root = load_family_root(public_url, prior)
        self._record_prior(prior_index, raw, root, public_url=public_url)

    def inherit_admitted(self, prior_index: dict, artifact) -> None:
        """Bind an offline run to its already admitted exact prior, retaining its root.

        The caller owns the prior's live byte-admission guard. This records
        inherited provenance, not new acquisition or requalified coverage.
        An inherited journal is not implicitly downloaded by this local path.
        """
        prior = prior_index["families"].get(self.family)
        if (prior is None or artifact.pin.as_dict() != {
                "logicalId": prior["logicalId"], "artifactDigest": prior["artifactDigest"]}):
            raise SourceEvidenceError("Admitted prior differs from the captured family")
        self.inherit_pinned_root(prior_index, canonical_json_bytes(artifact.root))

    def inherit_pinned_root(self, prior_index: dict, raw: bytes) -> None:
        """Retain a captured prior's exact root locally, without fetching its bodies.

        This is the same metadata-only inheritance as ``inherit``. Root pins
        qualify lineage; they do not admit prior tables or requalify coverage.
        """
        from spicy_regs.generations import KIND as GENERATION_KIND
        from spicy_regs.sources.publication import family_root

        prior = prior_index["families"].get(self.family)
        if prior is None:
            raise SourceEvidenceError("Captured family has no managed prior")
        root = family_root(raw, prior)
        if root.get("kind") != GENERATION_KIND or root["spec"].get("family") != self.family:
            raise SourceEvidenceError("Pinned prior belongs to a different generation family")
        self._record_prior(prior_index, raw, root, public_url=None)

    def _record_prior(self, prior_index, raw, root, *, public_url):
        self.read_snapshot = prior_index
        prior = prior_index["families"][self.family]
        self.prior_input = ArtifactInput(PRIOR_ROLE, prior["logicalId"], prior["artifactDigest"])
        pins = [item for item in root["inputs"] if item.get("role") == INPUT_ROLE]
        if len(pins) > 1:
            raise self._failure("Managed prior names more than one source-evidence input")
        self._prior_evidence = (public_url, pins[0]) if pins else None
        receipt = self._blob(raw, raw_source=False)
        self.event(
            "lineage",
            prior_generation=prior,
            prior_root=receipt,
            prior_inputs=root["inputs"],
            evidence_status=(
                "Inherited pins; prior source coverage is not requalified by this run."
                if pins
                else "Prior generation names no source evidence; this run does not qualify its source coverage."
            ),
        )

    def inherited_event(self, event: str, **match) -> dict | None:
        """The prior generation's last journaled ``event`` whose fields equal ``match``, or ``None``.

        Returns ``None`` without a managed prior or when the prior names no
        source evidence; a prior naming more than one is refused when it is recorded.
        The journal is read once, on first use, and pinned through the prior root.
        """
        if self._prior_evidence is None:
            return None
        public_url, pin = self._prior_evidence
        if public_url is None:
            raise SourceEvidenceError("Offline inherited journal is not attached; remote lookup is not authorized")
        if self._prior_events is None:
            from spicy_regs.sources.publication import load_evidence_journal

            self._prior_events = [
                json.loads(line) for line in load_evidence_journal(public_url, pin).splitlines()
            ]
        found = [
            row
            for row in self._prior_events
            if row.get("event") == event and all(row.get(k) == v for k, v in match.items())
        ]
        return found[-1] if found else None

    def published_input(self, key: str, *, sha256: str) -> dict | None:
        """The generation in this run's read snapshot that published ``key`` with exactly ``sha256``.

        ``None`` without a snapshot, when no family publishes ``key``, or when
        the bytes read differ from that generation's member (a local copy).
        """
        for family, entry in ((self.read_snapshot or {}).get("families") or {}).items():
            member = entry["tables"].get(key)
            if member is not None:
                if member.get("sha256") != sha256:
                    return None
                return {"family": family, "logicalId": entry["logicalId"], "artifactDigest": entry["artifactDigest"]}
        return None

    def seal(self, *, outcome: str):
        """Build and admit the immutable artifact, returning the cached one on repeat calls.

        Refuses ``build-complete`` after any retention failure in this run.
        """
        if self.artifact is not None:
            return self.artifact
        if outcome == "build-complete" and self.retention_failure:
            raise SourceEvidenceError(
                f"Source retention failed ({self.retention_failure}); evidence cannot seal as a complete build"
            )
        self._flush_archive_receipts()
        if self._pending_blobs:
            raise self._failure("Unclaimed source body has no acquisition receipt")
        with (self.artifact_dir / JOURNAL).open() as stream:
            lineage_keys = self._finalize_used_inputs(
                (json.loads(line) for line in stream), success=outcome == "build-complete"
            )
        self.event("build-outcome", outcome=outcome)
        from spicy_regs.generations import implementation_id

        source = EvidenceMemberSource(self.artifact_dir, archive_root=self.store.root)
        members = [
            describe_member(
                source,
                object_key=key,
                role="journal" if key == JOURNAL else "lineage-metadata" if key in lineage_keys else "source-body",
                media_type="application/octet-stream",
            )
            for key in sorted(source.keys())
        ]
        with (self.artifact_dir / "members.json").open("wb") as stream:
            manifest = write_member_manifest(
                stream, scope_kind="global", scope_id=self.family, object_key="members.json", members=members
            )
        implementation = implementation_id()
        root = build_artifact_root(
            kind=KIND,
            spec={
                "evidence_version": EVIDENCE_VERSION,
                "family": self.family,
                "outcome": outcome,
                "coverage": "observed-responses; inherited source gaps are recorded in journal.jsonl",
            },
            producer=Producer("spicy-regs", implementation, "urn:spicy-regs:source-evidence", "1", implementation),
            inputs=[self.prior_input] if self.prior_input else [],
            manifests=[manifest],
        )
        (self.artifact_dir / "artifact.json").write_bytes(canonical_json_bytes(root))
        artifact = admit_artifact(source)
        _verify_receipts(source, artifact)
        self.artifact = artifact
        return self.artifact

    def declare_used_inputs(self, inputs, *, scope_id: str, complete: bool = True) -> None:
        """Accumulate successful parser reads; resolve their receipts only at successful seal.

        Incremental readers may repeat one incomplete scope. A complete finite
        selection has one caller, so incompatible scopes never imply a merger.
        """
        from spicy_regs.source_catalog.publication import count, sha

        if self.artifact is not None:
            raise SourceEvidenceError("Cannot declare inputs on sealed source evidence")
        if (self._direct_selection or not isinstance(scope_id, str) or not scope_id.strip()
                or type(complete) is not bool or not isinstance(inputs, (list, tuple))):
            raise self._failure("Invalid or conflicting used-input declaration")
        scope = (scope_id, complete)
        if self._used_input_scope is not None and (self._used_input_scope != scope or complete):
            raise self._failure("Incompatible used-input scopes or repeated complete selection")
        normalized = []
        required_keys = {"location", "sha256", "bytes", "asset_kind"}
        optional_keys = {"wire_sha256", "wire_bytes", "decoded_sha256", "decoded_bytes"}
        try:
            for item in inputs:
                if (
                    not isinstance(item, dict)
                    or not (required_keys <= set(item) <= required_keys | optional_keys)
                    or any(
                        not isinstance(item[key], str) or not item[key].strip()
                        for key in ("location", "asset_kind")
                    )
                ):
                    raise ValueError("Invalid used-input descriptor")
                wire_sha = sha(item["sha256"])
                wire_bytes = count(item["bytes"])
                if "wire_sha256" in item and sha(item["wire_sha256"]) != wire_sha:
                    raise ValueError("Used-input wire_sha256 differs from sha256")
                if "wire_bytes" in item and count(item["wire_bytes"]) != wire_bytes:
                    raise ValueError("Used-input wire_bytes differs from bytes")
                if ("decoded_sha256" in item) != ("decoded_bytes" in item):
                    raise ValueError("Used-input decoded_sha256 and decoded_bytes must be declared together")
                entry: dict[str, Any] = {
                    "location": self._safe(item["location"], "used input location"),
                    "sha256": wire_sha,
                    "bytes": wire_bytes,
                    "asset_kind": item["asset_kind"],
                }
                if "decoded_sha256" in item:
                    entry["decoded_sha256"] = sha(item["decoded_sha256"])
                    entry["decoded_bytes"] = count(item["decoded_bytes"])
                normalized.append(entry)
        except (ValueError, TypeError, KeyError) as error:
            raise self._failure("Invalid used-input descriptor") from error
        self._used_input_scope = scope
        for item in normalized:
            dedup_key = (
                item["location"],
                item["sha256"],
                item["bytes"],
                item["asset_kind"],
                item.get("decoded_sha256"),
                item.get("decoded_bytes"),
            )
            self._used_inputs[dedup_key] = item

    def _finalize_used_inputs(self, events, *, success: bool) -> set[str]:
        """Resolve one indexed journal pass; captures alone never create declarations."""
        from spicy_regs.source_catalog.input_proof import source_observation, validate_selected_inputs

        resolving = success and self._used_input_scope is not None
        wanted = {(item["sha256"], item["bytes"], item["location"]) for item in self._used_inputs.values()} if resolving else set()
        indexed, refused, lineage_keys = {}, set(), set()
        for event in events:
            if event.get("event") == "lineage" and event.get("prior_root"):
                lineage_keys.add("blobs/sha256/" + event["prior_root"]["sha256"].removeprefix("sha256:"))
            if not resolving:
                continue
            if event.get("event") == "selected-inputs":
                raise self._failure("Conflicting direct and accumulated selected inputs")
            observation = source_observation(event)
            if observation is not None:
                pin = observation["sha256"], observation["bytes"]
                for location in observation["locations"]:
                    key = (*pin, location)
                    if key not in wanted:
                        continue
                    if event.get("source_validation") == "refused" or str(event.get("stage", "")).endswith(":refused"):
                        refused.add(key)
                    elif (observation["provenance"] == "capture-observation"
                          and event.get("response_complete") is True
                          and (event["event"] == "retained-file" or 200 <= event.get("status_code", 0) < 300)):
                        native_id = next((str(event[field]) for field in ("native_id", "source_id")
                                          if event.get(field) is not None), location)
                        identity = native_id, observation["source_version"], observation["version_kind"]
                        indexed.setdefault(key, {})[identity] = (observation, event)
            response = event.get("response")
            if event.get("event") == "refusal" and isinstance(response, dict) and response.get("sha256"):
                for location in {response.get(key) or event.get(key) for key in
                                 ("requested_url", "resolved_url", "original_location", "source_url")} - {None}:
                    key = response["sha256"], response.get("byte_size", response.get("bytes")), location
                    if key in wanted:
                        refused.add(key)
        if not resolving:
            return lineage_keys
        used = {}
        for item in self._used_inputs.values():
            key = item["sha256"], item["bytes"], item["location"]
            matches = indexed.get(key, {})
            if key in refused or not matches:
                raise self._failure("Used original lacks a successful recorded capture or central-raw receipt")
            if len(matches) != 1:
                raise self._failure("Used original has ambiguous recorded native identity or version")
            identity = next(iter(matches))[0]
            matched, capture_event = next(iter(matches.values()))
            if capture_event.get("decoded_status") in {"undecodable", "unsupported_encoding"}:
                raise self._failure("Used compressed original could not be decoded")
            is_compressed = _normalize_encoding(capture_event.get("content_encoding")) != "identity"
            if is_compressed:
                if not capture_event.get("decoded_sha256") or capture_event.get("decoded_byte_size") is None:
                    raise self._failure("Used compressed original lacks a verified decoded digest")
                actual_decoded = (capture_event["decoded_sha256"], capture_event["decoded_byte_size"])
            else:
                actual_decoded = (matched["sha256"], matched["bytes"])
            if "decoded_sha256" in item and (item["decoded_sha256"], item["decoded_bytes"]) != actual_decoded:
                raise self._failure("Used original differs from its recorded decoded digest")
            row = {**item, "source_id": identity, "source_version": matched["source_version"],
                   "version_kind": matched["version_kind"]}
            if is_compressed:
                row["wire_sha256"] = matched["sha256"]
                row["decoded_sha256"], row["decoded_bytes"] = actual_decoded
            native = tuple(row[key] for key in ("source_id", "source_version", "version_kind", "asset_kind"))
            if native in used and (
                (used[native]["sha256"], used[native]["bytes"]) != key[:2]
                or (used[native].get("decoded_sha256"), used[native].get("decoded_bytes"))
                != (row.get("decoded_sha256"), row.get("decoded_bytes"))
            ):
                raise self._failure("Used native identity has conflicting bytes")
            used.setdefault(native, row)
        scope_id, complete = self._used_input_scope
        manifest = {"format": "spicy-selected-inputs", "version": 1,
                    "scope": {"id": scope_id, "finite": True}, "complete": complete,
                    "used": list(used.values()), "available": [], "expected": []}
        validate_selected_inputs(manifest)
        self.event("selected-inputs", manifest=manifest)
        return lineage_keys

    def selected_inputs(self, manifest: dict, *, base: Path | None = None) -> None:
        """Declare the build's exact used set and separate available/expected sets.

        Call after input selection and before ``inputs()`` seals the journal.
        Large inventories may be pinned Parquet files kept outside the raw
        archive. A complete declaration requires a named finite scope; it does
        not qualify inherited generations or publisher-wide coverage.
        """
        from spicy_regs.source_catalog.input_proof import validate_selected_inputs

        if self._used_input_scope is not None or self._direct_selection:
            raise self._failure("Conflicting direct and accumulated selected inputs")
        validate_selected_inputs(manifest)
        self._direct_selection = True
        manifest = json.loads(json.dumps(manifest))
        for item in manifest.get("coverage_inventories", []):
            path = (base or Path.cwd()) / item["path"]
            actual = self.retain_file(path, stage="coverage-inventory", raw_source=False,
                                      original_location="coverage-inventory", inventory_role=item["role"])
            if actual["sha256"] != item["sha256"] or actual["byte_size"] != item["bytes"]:
                raise self._failure("Coverage inventory differs from its declared pin")
            item["path"] = "blobs/sha256/" + actual["sha256"].removeprefix("sha256:")
            item.setdefault("scope", manifest["scope"])
        self.event("selected-inputs", manifest=manifest)

    def inputs(self) -> list[ArtifactInput]:
        """Seal as build-complete and return this run's artifact inputs for a generation."""
        artifact = self.seal(outcome="build-complete")
        return [
            ArtifactInput(INPUT_ROLE, artifact.pin.logical_id, artifact.pin.artifact_digest),
            *([self.prior_input] if self.prior_input else []),
        ]

    def finish(self, error: BaseException | None = None) -> None:
        """Retain final run status outside the sealed artifact, even on upload failure.

        A retention failure that a caller swallowed still finishes the run as
        failed, and is raised once that outcome is retained.
        """
        swallowed = None
        if error is None and self.retention_failure and self.artifact is None:
            error = swallowed = SourceEvidenceError(f"Source retention failed: {self.retention_failure}")
        seal_error = None
        try:
            if self.artifact is None:
                if error is not None and not isinstance(error, SourceEvidenceError):
                    self.refusal(error, stage="run")
                self.seal(outcome="failed" if error else "build-complete")
        except BaseException as failure:
            # A failed admission still needs a durable failure receipt. It must
            # never invent an admitted artifact pin or complete-build outcome.
            error = seal_error = failure
        outcome = self._safe({
            "event": "run-outcome",
            "outcome": "failed" if error else "complete",
            "artifact": self.artifact.pin.as_dict() if self.artifact is not None else None,
            "error_type": type(error).__name__ if error else None,
            "message": self._safe(str(error), "message")[:2000] if error and not self._restricted_diagnostics else None,
        })
        try:
            self._archive_receipt(outcome)
        finally:
            (self.directory / "run-outcome.json").write_bytes(canonical_json_bytes(outcome))
        from spicy_regs.catalog_refresh_request import notify

        self.catalog_refresh_request = notify("acquisition-complete", {
            "runId":self.run_id,"family":self.family,"outcome":outcome["outcome"],
            "runOutcome":str(self.directory / "run-outcome.json"),
        },raw_root=self.store.root)
        if seal_error is not None:
            raise seal_error
        if swallowed is not None:
            raise swallowed


@dataclass(frozen=True)
class SourceEvidenceContext:
    """Immutable publisher binding; every operation writes the parent artifact."""

    root: CaptureEvidence
    publisher_id: str
    policy: str
    parser_version: str
    policy_decision_id: str

    @property
    def artifact(self):
        return self.root.artifact

    @property
    def credential(self):
        return self.root.credential

    @property
    def store(self):
        return self.root.store

    @property
    def _restricted_diagnostics(self):
        return self.root._restricted_diagnostics

    @property
    def family(self):
        return self.root.family

    def event(self, event: str, **fields) -> str | None:
        fields.update(
            publisher_id=self.publisher_id,
            evidence_policy=self.policy,
            parser_version=self.parser_version,
            policy_decision_id=self.policy_decision_id,
        )
        return self.root.event(event, **fields)

    def _failure(self, message: str):
        return self.root._failure(message)

    def _safe(self, value, field: str = ""):
        return self.root._safe(value, field)

    def _put_body(self, *args, **kwargs):
        return self.root._put_body(*args, **kwargs)

    _blob = CaptureEvidence._blob
    _body = CaptureEvidence._body
    _record_capture = CaptureEvidence._record_capture
    _file_blob = CaptureEvidence._file_blob
    _retained = CaptureEvidence._retained
    _local_control = CaptureEvidence._local_control
    _retain_file = CaptureEvidence._retain_file
    capture = CaptureEvidence.capture
    retain_file = CaptureEvidence.retain_file
    retain_bytes = CaptureEvidence.retain_bytes
    retain_control = CaptureEvidence.retain_control
    transport = CaptureEvidence.transport
    refusal = CaptureEvidence.refusal


def require_current_evidence_root(root) -> None:
    """Refuse evidence this runtime cannot consume: another kind, an unfinished build, or another receipt version."""
    spec = root.get("spec")
    if root.get("kind") != KIND or not isinstance(spec, dict) or spec.get("outcome") != "build-complete":
        raise SourceEvidenceError("Only completed-build evidence can support a generation")
    _require_current_version(spec)


def predates_current_evidence(root) -> bool:
    """Whether ``root`` is source evidence written before ``evidence_version`` 2: unversioned, or version 1.

    That evidence is history, not an input. A caller that inherits nothing from it (a native conversion, which
    writes current evidence of its own) may leave it with its generation, whatever its outcome (an absent or null
    ``evidence_version`` counts as unversioned). Any other root, a wrong kind or a version 2 unfinished build among
    them, still goes through :func:`require_current_evidence_root`.
    """
    spec = root.get("spec")
    version_ = spec.get("evidence_version") if isinstance(spec, dict) else 0
    return root.get("kind") == KIND and (version_ is None or (type(version_) is int and version_ == 1))


def _require_current_version(spec) -> None:
    version_ = spec.get("evidence_version")
    if type(version_) is not int or version_ != EVIDENCE_VERSION:
        raise SourceEvidenceError(
            f"Unsupported source evidence version; expected {EVIDENCE_VERSION}. "
            "Rebuild the family to write current evidence"
        )


def _stream_verify_compressed(source: MemberSource, key: str, event: dict) -> None:
    """Verify wire and decoded digests of a retained compressed blob in one bounded streaming pass."""
    encoding = _normalize_encoding(event.get("content_encoding"))
    wire_digest = hashlib.sha256()
    wire_size = 0
    decoded_digest = hashlib.sha256()
    decoded_size = 0
    is_gzip = encoding in ("gzip", "x-gzip")
    decoder = _new_decompressor(encoding)
    deflate_started = False
    decode_failed = False

    with source.open(key) as stream:
        while chunk := stream.read(_CHUNK):
            wire_size += len(chunk)
            wire_digest.update(chunk)
            if decoder is None or decode_failed:
                continue
            pending = chunk
            try:
                while pending:
                    if decoder.eof:
                        if not is_gzip:
                            raise zlib.error("Trailing data after deflate stream")
                        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    try:
                        decoded = decoder.decompress(pending, _CHUNK)
                    except zlib.error:
                        if encoding == "deflate" and not deflate_started:
                            decoder = zlib.decompressobj(-zlib.MAX_WBITS)
                            deflate_started = True
                            decoded = decoder.decompress(pending, _CHUNK)
                        else:
                            raise
                    deflate_started = True
                    if decoded:
                        decoded_size += len(decoded)
                        decoded_digest.update(decoded)
                    if decoder.unconsumed_tail:
                        pending = decoder.unconsumed_tail
                    elif decoder.eof and decoder.unused_data:
                        pending = decoder.unused_data
                    else:
                        pending = b""
            except zlib.error:
                decode_failed = True
                decoder = None

    if decoder is not None and not decode_failed:
        if wire_size == 0:
            decode_failed = True
        else:
            try:
                rest = decoder.flush()
                if rest:
                    decoded_size += len(rest)
                    decoded_digest.update(rest)
            except zlib.error:
                decode_failed = True
            else:
                if not decoder.eof or decoder.unused_data:
                    decode_failed = True

    actual_wire = "sha256:" + wire_digest.hexdigest()
    if actual_wire != event["wire_sha256"] or wire_size != event["wire_byte_size"]:
        raise SourceEvidenceError("Wire digest differs from retained compressed bytes")
    if "decoded_sha256" in event:
        if decode_failed:
            raise SourceEvidenceError("Retained compressed bytes cannot decode to declared decoded_sha256")
        actual_decoded = "sha256:" + decoded_digest.hexdigest()
        if actual_decoded != event["decoded_sha256"] or decoded_size != event["decoded_byte_size"]:
            raise SourceEvidenceError("Decoded digest differs from retained compressed bytes")
    elif event.get("decoded_status") == "undecodable" and not decode_failed:
        raise SourceEvidenceError("Retained compressed bytes decoded despite undecodable status")


def _verify_receipts(source: MemberSource, artifact) -> None:
    """Validate versioned receipt policy and every declared body/member binding."""
    _require_current_version(artifact.root["spec"])

    def require(condition, message):
        if not condition:
            raise SourceEvidenceError(message)

    with source.open(JOURNAL) as stream:
        events = [json.loads(line) for line in stream if line.strip()]
    members = {}
    for manifest in artifact.manifests:
        with source.open(manifest.object_key) as stream:
            for member in json.load(stream)["members"]:
                members[member["objectKey"]] = member
    claimed = {JOURNAL}
    require(JOURNAL in members and members[JOURNAL]["role"] == "journal", "Invalid evidence journal role")
    capture_ids = set()
    policies = {}
    verified_captures: dict[tuple[str, int], list[dict]] = {}

    def body(receipt, policy, *, event_record=False):
        require(isinstance(receipt, dict), "Invalid source body receipt")
        if policy != "full" and not event_record:
            require(set(receipt) <= {"byte_size", "body_retained", "sha256"}, "Unexpected non-retaining body fields")
        require(
            type(receipt.get("byte_size")) is int and receipt["byte_size"] >= 0,
            "Source receipt needs measured byte size",
        )
        require(receipt.get("body_retained") is (policy == "full"), "Body retention contradicts policy")
        if policy == "metadata_only":
            require(
                "sha256" not in receipt
                and "blob_member" not in receipt
                and "wire_sha256" not in receipt
                and "decoded_sha256" not in receipt,
                "Metadata-only receipt exposes a body identity",
            )
        else:
            digest = receipt.get("sha256")
            require(_is_valid_sha256(digest), "Invalid source body digest")
        if policy != "full":
            require("blob_member" not in receipt, "Non-retaining receipt names a source blob")
            return
        key = receipt.get("blob_member")
        require(key == "blobs/sha256/" + receipt["sha256"][7:], "Body member differs from source digest")
        require(key in members, "Retained source body is not an admitted member")
        member = members[key]
        require(member["role"] in {"source-body", "lineage-metadata"}, "Invalid retained source member role")
        require(
            member["sha256"] == receipt["sha256"] and member["byteSize"] == receipt["byte_size"],
            "Source body receipt differs from admitted bytes",
        )
        claimed.add(key)

    def compression_binding(event, policy):
        encoding = _normalize_encoding(event.get("content_encoding"))
        compressed = encoding != "identity"
        if policy == "metadata_only":
            require(
                "wire_sha256" not in event and "decoded_sha256" not in event,
                "Metadata-only receipt exposes a body identity",
            )
            if "wire_byte_size" in event:
                require(
                    type(event["wire_byte_size"]) is int and event["wire_byte_size"] == event["byte_size"],
                    "Wire byte size differs from captured byte size",
                )
            if "decoded_byte_size" in event:
                require(
                    type(event["decoded_byte_size"]) is int and event["decoded_byte_size"] >= 0,
                    "Invalid decoded byte size",
                )
            return
        if "wire_sha256" in event:
            require(
                _is_valid_sha256(event["wire_sha256"]) and event["wire_sha256"] == event["sha256"],
                "Wire digest differs from captured sha256",
            )
        if "wire_byte_size" in event:
            require(
                type(event["wire_byte_size"]) is int and event["wire_byte_size"] == event["byte_size"],
                "Wire byte size differs from captured byte size",
            )
        has_decoded_sha = "decoded_sha256" in event
        has_decoded_size = "decoded_byte_size" in event
        require(has_decoded_sha == has_decoded_size, "Decoded digest and byte size must appear together")
        if has_decoded_sha:
            require(_is_valid_sha256(event["decoded_sha256"]), "Invalid decoded body digest")
            require(
                type(event["decoded_byte_size"]) is int and event["decoded_byte_size"] >= 0,
                "Invalid decoded byte size",
            )
            require("decoded_status" not in event, "Decoded capture cannot also claim a decode failure status")
        if "decoded_status" in event:
            require(
                event["decoded_status"] in {"undecodable", "unsupported_encoding"},
                "Invalid decoded capture status",
            )
        if not compressed:
            if has_decoded_sha:
                require(
                    event["decoded_sha256"] == event["sha256"] and event["decoded_byte_size"] == event["byte_size"],
                    "Uncompressed capture decoded digest differs from wire digest",
                )
            return
        require(
            "wire_sha256" in event and "wire_byte_size" in event,
            "Compressed capture must bind wire_sha256 and wire_byte_size",
        )
        if encoding in _COMPRESSED_ENCODINGS:
            require(
                has_decoded_sha or event.get("decoded_status") == "undecodable",
                "Compressed capture must bind decoded_sha256 or explicit undecodable status",
            )
            if policy == "full":
                _stream_verify_compressed(source, event["blob_member"], event)
        else:
            require(
                event.get("decoded_status") == "unsupported_encoding",
                "Unsupported compressed encoding must record unsupported_encoding status",
            )

    for event in events:
        require(isinstance(event, dict), "Evidence journal event must be an object")
        kind = event.get("event")
        if kind == "source-policy":
            publisher = event.get("publisher_id")
            value = (event.get("evidence_policy"), event.get("parser_version"), event.get("policy_decision_id"))
            require(
                isinstance(publisher, str)
                and publisher
                and value[0] in POLICIES
                and all(isinstance(v, str) and v for v in value),
                "Invalid source policy declaration",
            )
            require(publisher not in policies or policies[publisher] == value, "Contradictory source policies")
            policies[publisher] = value
        if kind in {"capture", "retained-file", "refusal", "capture-incomplete"}:
            policy = event.get("evidence_policy")
            require(policy in POLICIES, "Unknown capture evidence policy")
            require(
                all(isinstance(event.get(k), str) and event[k] for k in ("parser_version", "policy_decision_id")),
                "Missing parser or policy decision identity",
            )
            publisher = event.get("publisher_id")
            if publisher is not None:
                require(
                    policies.get(publisher) == (policy, event["parser_version"], event["policy_decision_id"]),
                    "Capture differs from declared source policy",
                )
            if kind in {"capture", "retained-file"}:
                capture_id = event.get("capture_id")
                require(
                    isinstance(capture_id, str)
                    and len(capture_id) == 32
                    and all(c in "0123456789abcdef" for c in capture_id)
                    and capture_id not in capture_ids,
                    "Capture ID must be an opaque unique observation ID",
                )
                capture_ids.add(capture_id)
                require(event.get("response_complete") is True, "Capture must distinguish complete response")
                if kind == "capture":
                    require(
                        all(
                            isinstance(event.get(field), str) and event[field]
                            for field in ("requested_url", "resolved_url", "observed_at", "method")
                        )
                        and type(event.get("status_code")) is int
                        and 100 <= event["status_code"] <= 599
                        and "content_type" in event
                        and (event["content_type"] is None or isinstance(event["content_type"], str)),
                        "Capture lacks required HTTP observation metadata",
                    )
                body(event, policy, event_record=True)
                if kind == "capture":
                    compression_binding(event, policy)
                    if policy != "metadata_only":
                        verified_captures.setdefault((event["sha256"], event["byte_size"]), []).append(event)
                if event.get("request_body") is not None:
                    body(event["request_body"], policy)
            elif kind == "capture-incomplete":
                require(
                    event.get("response_complete") is False
                    and event.get("body_retained") is False
                    and not {"sha256", "blob_member", "request_body"} & event.keys(),
                    "Incomplete response must not claim complete body identity",
                )
            elif event.get("response") is not None:
                require(isinstance(event["response"], dict), "Invalid refusal response receipt")
                body_fields = {"sha256", "byte_size", "body_retained", "blob_member"}
                if policy != "full" or body_fields.intersection(event["response"]):
                    body(event["response"], policy)
            if policy != "full":
                allowed = {
                    "event",
                    "recorded_at",
                    "stage",
                    "publisher_id",
                    "evidence_policy",
                    "parser_version",
                    "policy_decision_id",
                    "capture_id",
                    "response_complete",
                    "requested_url",
                    "resolved_url",
                    "status_code",
                    "content_type",
                    "content_encoding",
                    "method",
                    "observed_at",
                    "byte_size",
                    "body_retained",
                    "sha256",
                    "request_body",
                    "error_type",
                    "reason_code",
                    "message",
                    "response",
                    "credential_refused",
                    "bytes_received",
                    "source_validation",
                    *_COMPRESSION_FIELDS,
                }
                require(set(event) <= allowed, "Unexpected non-retaining receipt fields")
                require(event.get("message") is None, "Non-retaining diagnostics contain source text")
                require("source_validation" not in event or event["source_validation"] == "refused",
                        "Non-retaining source validation must use the fixed refusal marker")
        if kind == "selected-inputs" and isinstance(event.get("manifest"), dict):
            for used_row in event["manifest"].get("used", ()):
                if not isinstance(used_row, dict):
                    continue
                if "wire_sha256" in used_row:
                    require(
                        used_row["wire_sha256"] == used_row.get("sha256"),
                        "Selected used input wire_sha256 differs from sha256",
                    )
                if "decoded_sha256" in used_row:
                    pin = (used_row.get("sha256"), used_row.get("bytes"))
                    matched = verified_captures.get(pin, [])
                    require(
                        any(
                            (
                                cap.get("decoded_sha256") == used_row["decoded_sha256"]
                                and cap.get("decoded_byte_size") == used_row.get("decoded_bytes")
                            )
                            or (
                                _normalize_encoding(cap.get("content_encoding")) == "identity"
                                and cap.get("sha256") == used_row["decoded_sha256"]
                                and cap.get("byte_size") == used_row.get("decoded_bytes")
                            )
                            for cap in matched
                        ),
                        "Selected used input decoded digest differs from retained capture",
                    )
        if kind == "lineage" and event.get("prior_root") is not None:
            receipt = event["prior_root"]
            key = "blobs/sha256/" + receipt["sha256"].removeprefix("sha256:")
            require(
                key in members
                and members[key]["role"] == "lineage-metadata"
                and members[key]["sha256"] == receipt["sha256"]
                and members[key]["byteSize"] == receipt["byte_size"],
                "Invalid lineage metadata member",
            )
            claimed.add(key)
    require(set(members) == claimed, "Unclaimed or orphan evidence member")
    controls = {"artifact.json", *(m.object_key for m in artifact.manifests)}
    require(set(source.keys()) <= claimed | controls, "Unadmitted file in evidence tree")


def verify_evidence(evidence: Path | MemberSource, *, expected_pin=None, archive_root: Path | None = None):
    """Admit the separate artifact from a directory or any member source; it must never masquerade as a table family."""
    source = EvidenceMemberSource(evidence, archive_root=archive_root) if isinstance(evidence, Path) else evidence
    artifact = admit_artifact(source, expected_pin=expected_pin)
    require_current_evidence_root(artifact.root)
    _verify_receipts(source, artifact)
    return artifact
