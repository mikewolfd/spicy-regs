"""Keep the exact Mirrulations originals that feed regulations rows and derived text.

SpicyDocs exposes each successful GET body on ``KeyedPayload`` and each fetched
``DerivedAttachment``. Keep those bytes in CaptureEvidence's central raw archive
without downloading again or serializing the parsed JSON or decoded text.
Failed reads stay unresolved; they cannot authorize a merge or key retirement.

Originals are content-addressed: the central raw archive locally, and R2's
shared ``source-evidence/blobs/sha256/<hex>`` prefix through the existing
evidence publisher, which the generation-retention planner never deletes.
Identical bytes are stored once; a changed upstream object hashes to a new
blob beside the old one. Each artifact also gets an append-only index at
``source-evidence/indexes/regulations-mirrulations/`` so a key's original
can be found without walking every journal.

:meth:`MirrulationsOriginals.commit` is a durability barrier. It waits for any
detached batch to finish and refuses if retention failed or if a key about
to be used has no retained original. Retention aborts on the first failure;
already in-flight source reads may finish, but no further writes are allowed.

Only journal appends and batch swaps hold the state lock. A rollover seals
and publishes its detached batch without that lock, so concurrent writers
can fill the new batch. At most one batch can be publishing; backpressure
at the next rollover bounds scratch disk to two batches. Scratch is removed
only after R2 admits an artifact.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from threading import Condition, Lock
from time import perf_counter_ns
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from loguru import logger

from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError

if TYPE_CHECKING:
    from spicy_docs.sources.mirrulations import DerivedAttachment, KeyedPayload, MirrulationsReader

    from spicy_regs.sources.derived_text import DerivedFill

FAMILY = "regulations-mirrulations"
#: Objects per evidence artifact. Its journal and member manifest grow by about
#: 1.2 KB per object, and readers bound both at 64 MiB (``EVIDENCE_CONTROL_LIMIT``).
OBJECTS_PER_ARTIFACT = 20_000
#: The largest body retained, as the largest any Mirrulations caller here reads
#: (``derived_text.MAX_ATTACHMENT_BYTES``). A larger object is not read here, so
#: a reader that consumes it anyway fails the commit's conservation check.
MAX_OBJECT_BYTES = 64 * 1024 * 1024
INDEX_NAME = "originals-index.parquet"


class RetentionHalted(BaseException):
    """Stops acquisition on evidence failure rather than treating it as a failed source read."""


class CapturingReader:
    """Preserve the reader's accounting and expose both streams from a single capture path."""

    def __init__(self, reader: MirrulationsReader, originals: MirrulationsOriginals) -> None:
        self.reader, self.originals = reader, originals

    def __getattr__(self, name: str) -> Any:
        return getattr(self.reader, name)

    def iter_keyed_records(self) -> Iterator[KeyedPayload]:
        for record in self.reader.iter_keyed_records():
            self.originals.retain_keyed(record)
            yield record

    def iter_records(self) -> Iterator[dict]:
        for record in self.iter_keyed_records():
            yield record.payload


class MirrulationsOriginals:
    """A run's retained originals, made durable at each :meth:`commit`."""

    def __init__(
        self, output_dir: Path, bucket: str, *, publish: bool, scratch: bool = False, track_keys: bool = True,
    ) -> None:
        if scratch and not publish:
            raise ValueError("Local originals are scratch only when R2 keeps them")
        self.output_dir, self.bucket, self.publish, self.scratch = output_dir, bucket, publish, scratch
        self._lock = Lock()
        self._state = Condition(self._lock)
        self._rolling = self._committing = self._closed = False
        self._lock_count = self._lock_wait_ns = self._lock_max_ns = 0
        self._current: CaptureEvidence | None = None
        self._rows: list[dict] = []
        self._track_keys = track_keys
        #: Keys retained, and keys a durable write will use, since the last commit.
        self.retained: set[str] = set()
        self._expected: set[str] = set()
        #: The first retention failure. It halts the run and every later commit refuses.
        self.failure: str | None = None

    @contextmanager
    def _locked(self) -> Iterator[None]:
        started = perf_counter_ns()
        with self._state:
            waited = perf_counter_ns() - started
            self._lock_count += 1
            self._lock_wait_ns += waited
            self._lock_max_ns = max(self._lock_max_ns, waited)
            yield

    def retain_keyed(self, record: KeyedPayload, *, version_id: str | None = None) -> str:
        return self.retain(record.key, record.raw_bytes, {
            "ETag": record.etag, "LastModified": record.last_modified, "VersionId": version_id,
        })

    def retain_attachment(self, attachment: DerivedAttachment) -> None:
        self.retain(attachment.key, attachment.raw_bytes, {"ETag": attachment.etag})

    def retain(self, key: str, content: bytes | None, response: dict) -> str:
        """Retain one GET's exact bytes; roll a full batch before admitting the next item."""
        while True:
            with self._locked():
                self._state.wait_for(lambda: self.failure is not None or self._closed or (
                    not self._committing and (len(self._rows) < OBJECTS_PER_ARTIFACT or not self._rolling)
                ))
                if self.failure is not None:
                    raise RetentionHalted(f"Mirrulations original retention failed: {self.failure}")
                if self._closed:
                    raise RetentionHalted("Mirrulations original retention is closed")
                if len(self._rows) < OBJECTS_PER_ARTIFACT:
                    return self._append(key, content, response)
                batch = self._detach()
                self._rolling = True
            self._rollover(batch)

    def _append(self, key: str, content: bytes | None, response: dict) -> str:
        """Append under the state lock; the current evidence journal has a single writer."""
        try:
            if content is None:
                raise SourceEvidenceError("Reader did not supply original GET bytes")
            if len(content) > MAX_OBJECT_BYTES:
                raise SourceEvidenceError(f"Body exceeds the {MAX_OBJECT_BYTES} byte retention cap")
            last_modified = response.get("LastModified")
            if isinstance(last_modified, datetime):
                last_modified = last_modified.isoformat()
            source = {
                "source_version": response.get("ETag"),
                "version_id": response.get("VersionId"),
                "last_modified": last_modified,
                "observed_at": datetime.now(UTC).isoformat(),
            }
            if self._current is None:
                root = self.output_dir / "mirrulations-originals" / uuid4().hex if self.scratch else None
                self._current = CaptureEvidence(self.output_dir, FAMILY, archive_root=root)
            receipt = self._current.retain_bytes(
                content, raw_source=True, stage="mirrulations-object",
                source_url=f"s3://{self.bucket}/{key}", source_record_key=key, **source,
            )
            digest = receipt["sha256"]
            self._rows.append({"key": key, "sha256": digest, "bytes": len(content), **source})
            if self._track_keys:
                self.retained.add(key)
            return digest
        except BaseException as error:
            self.failure = self.failure or f"{type(error).__name__} retaining {key}"
            self._state.notify_all()
            raise RetentionHalted(f"Mirrulations original retention failed: {self.failure}") from error

    def expect_fill(self, fill: DerivedFill) -> None:
        """Register the derived-text objects a fill's provenance cites; the next commit requires them."""
        keys = {item["key"] for item in json.loads(fill.provenance)["attachments"]}
        with self._locked():
            self._state.wait_for(lambda: not self._committing)
            if self._closed or self.failure is not None:
                raise RetentionHalted(f"Mirrulations original retention unavailable: {self.failure or 'closed'}")
            self._expected |= keys

    def _detach(self) -> tuple[CaptureEvidence | None, list[dict]]:
        batch = self._current, self._rows
        self._current, self._rows = None, []
        return batch

    def _rollover(self, batch: tuple[CaptureEvidence | None, list[dict]]) -> None:
        started = perf_counter_ns()
        try:
            self._flush(*batch)
        except BaseException as error:
            with self._locked():
                self.failure = self.failure or f"{type(error).__name__} publishing originals"
            raise RetentionHalted(f"Mirrulations original retention failed: {self.failure}") from error
        finally:
            flush_ms = (perf_counter_ns() - started) / 1e6
            with self._locked():
                self._rolling = False
                self._state.notify_all()
                count, total, peak = self._lock_count, self._lock_wait_ns, self._lock_max_ns
            logger.info(
                "Mirrulations originals rollover: objects={} flush_ms={:.3f} "
                "lock_acquires={} lock_wait_total_ms={:.3f} lock_wait_max_ms={:.3f}",
                len(batch[1]), flush_ms, count, total / 1e6, peak / 1e6,
            )

    def _flush(self, evidence: CaptureEvidence | None, rows: list[dict]) -> None:
        """Seal, publish and finish an exclusive batch without the state lock."""
        if evidence is None:
            return
        try:
            self._publish(evidence, rows)
        except BaseException as error:
            with suppress(Exception):
                evidence.finish(error)
            raise
        evidence.finish()
        if self.scratch:
            # R2 has admitted the bytes, journal and index; the runner copy was scratch.
            rmtree(evidence.store.root, ignore_errors=True)
            rmtree(evidence.directory, ignore_errors=True)

    def _publish(self, evidence: CaptureEvidence, rows: list[dict]) -> None:
        artifact = evidence.seal(outcome="build-complete")
        digest = artifact.pin.artifact_digest
        index = evidence.directory / INDEX_NAME
        _write_index(index, [{**row, "artifact_digest": digest} for row in rows])
        if self.publish:
            from spicy_regs.sources import publication, r2

            r2.require_credentials("Mirrulations original retention")
            client, bucket = r2.get_r2_client(), os.getenv("R2_BUCKET_NAME", "spicy-regs")
            publication.publish_evidence(client, bucket, evidence.artifact_dir, artifact, archive_root=evidence.store.root)
            location = publication.publish_evidence_index(client, bucket, FAMILY, index)
            logger.info("Retained {:,} Mirrulations originals in {} (index {})", len(rows), digest, location)
        else:
            logger.info("Retained {:,} Mirrulations originals locally in {}", len(rows), digest)

    def commit(self, retiring: Iterable[str] = ()) -> None:
        """Make every retained original durable before a write can use or retire what was read."""
        self._commit(retiring, close=False)

    @contextmanager
    def _barrier(self, *, close: bool) -> Iterator[None]:
        """Serialize commit/close, stop appends, and wait for a detached rollover."""
        with self._locked():
            self._state.wait_for(lambda: not self._committing)
            if self._closed:
                raise SourceEvidenceError("Mirrulations original retention is closed")
            self._committing = True
            self._closed = close
        try:
            with self._locked():
                self._state.wait_for(lambda: not self._rolling)
            yield
        finally:
            with self._locked():
                self._committing = False
                self._state.notify_all()

    def _commit(self, retiring: Iterable[str], *, close: bool) -> None:
        with self._barrier(close=close):
            if self.failure is not None:
                raise SourceEvidenceError(f"Mirrulations original retention failed: {self.failure}")
            missing = (set(retiring) | self._expected) - self.retained
            if missing:
                raise SourceEvidenceError(
                    f"{len(missing)} Mirrulations objects would be used without a retained original, "
                    f"e.g. {sorted(missing)[0]}"
                )
            try:
                with self._locked():
                    batch = self._detach()
                self._flush(*batch)
            except BaseException as error:
                with self._locked():
                    self.failure = self.failure or f"{type(error).__name__} publishing originals"
                if not isinstance(error, Exception):
                    raise
                raise SourceEvidenceError(f"Mirrulations original retention failed: {self.failure}") from error
            with self._locked():
                self.retained.clear()
                self._expected.clear()

    def __enter__(self) -> MirrulationsOriginals:
        return self

    def __exit__(self, kind, error, traceback) -> None:
        """Commit on success; finish any unflushed artifact as failed on error."""
        if error is not None:
            with self._barrier(close=True):
                self._finish_failed(error)
            return
        try:
            self._commit((), close=True)
        except BaseException as failure:
            self._finish_failed(failure)
            raise

    def _finish_failed(self, error: BaseException) -> None:
        with self._locked():
            evidence, _ = self._detach()
        if evidence is not None:
            # The run reports its own error, not a failure to write this receipt.
            with suppress(Exception):
                evidence.finish(error)


def _write_index(path: Path, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    columns = ("key", "sha256", "bytes", "source_version", "version_id", "last_modified", "observed_at",
               "artifact_digest")
    schema = pa.schema([(name, pa.int64() if name == "bytes" else pa.string()) for name in columns])
    table = pa.Table.from_pylist([{name: row.get(name) for name in columns} for row in rows], schema=schema)
    pq.write_table(table, path, compression="zstd")
