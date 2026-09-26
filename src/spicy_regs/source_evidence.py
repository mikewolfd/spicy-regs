"""Retain owner captures and bind their bytes to a separate audit artifact.

This records responses used by a run, not a complete source population or a
source-native release. Acquisition, credential checks and parsing stay in
SpicyDocs. Rulespec owns blob integrity and artifact admission.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import IO
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
from spicy_docs.reading.refusals import retain_refused_response
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
from spicy_docs.transport.captured import CapturedBodyResponse, attached_capture
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

KIND = "spicy-regs-source-evidence"
INPUT_ROLE = "source-evidence"
PRIOR_ROLE = "prior-generation"
_CHUNK = 1024 * 1024


class SourceEvidenceError(RuntimeError):
    """Source retention failed; never turn this into a skippable source refusal."""


class _StreamCheck:
    """One pass over streamed bytes: byte bound, credential scan across chunk boundaries and SHA-256.

    Only the last ``len(secret) - 1`` bytes carry over between chunks, so no
    chunk is copied whole and nothing grows with the stream.
    """

    def __init__(self, evidence: CaptureEvidence, max_bytes: int):
        self.evidence, self.max_bytes = evidence, max_bytes
        self.secret = evidence.credential.encode()
        self.digest, self.size, self.tail = hashlib.sha256(), 0, b""

    def update(self, chunk: bytes) -> None:
        self.size += len(chunk)
        if self.size > self.max_bytes:
            # A retention failure, not an item refusal: the bound equals the owner's own, so only a response
            # that states no length can get here, and letting its bytes reach the owner unretained would leave
            # a used response the run could not prove. The run fails loudly instead.
            raise self.evidence._failure("Source response exceeds its retention byte bound")
        if self.secret:
            keep = len(self.secret) - 1
            if self.secret in chunk or self.secret in self.tail + chunk[:keep]:
                raise CredentialRefusedError("Source response refused; credential-bearing bytes were not retained")
            self.tail = (self.tail + chunk[-keep:])[-keep:] if keep else b""
        self.digest.update(chunk)

    @property
    def sha256(self) -> str:
        return "sha256:" + self.digest.hexdigest()


class CaptureEvidence:
    """A durable journal followed by an immutable, independently admitted artifact.

    Any retention failure is remembered: once one has been raised, this
    evidence seals only as ``failed``, even when a caller's broad ``except``
    treated the error as one item's source refusal.
    """

    def __init__(self, output_dir: Path, family: str):
        self.directory = output_dir / "source-evidence" / uuid4().hex
        self.artifact_dir = self.directory / "artifact"
        self.artifact_dir.mkdir(parents=True)
        self.store = LocalSourceNativeBlobStore(self.artifact_dir / "blobs")
        self.family = family
        self.credential = ""
        self.artifact = None
        self.prior_input: ArtifactInput | None = None
        self.read_snapshot: dict | None = None
        self.retention_failure: str | None = None
        self.event(
            "run",
            family=family,
            packages={name: version(name) for name in ("spicy-regs", "spicy-docs")},
            coverage="Observed responses only; caps, refusals and inherited gaps remain explicit.",
        )

    def _safe(self, value):
        """Recursively redact the run's credential from a value before it is journaled."""
        if isinstance(value, str):
            # The owner scrubber handles named query credentials. The literal
            # replacement also covers short injected test credentials.
            result = scrub_credential(value, self.credential)
            return result.replace(self.credential, "<redacted>") if self.credential else result
        if isinstance(value, dict):
            return {key: self._safe(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [self._safe(item) for item in value]
        return value

    def _failure(self, message: str) -> SourceEvidenceError:
        """Remember the first retention failure and return the error to raise for it."""
        self.retention_failure = self.retention_failure or message
        return SourceEvidenceError(message)

    def event(self, event: str, **fields) -> None:
        """Append one fsynced journal event; refuses once the artifact has been sealed."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        try:
            raw = canonical_json_bytes(
                self._safe({"event": event, "recorded_at": datetime.now(UTC).isoformat(), **fields})
            )
            with (self.artifact_dir / "journal.jsonl").open("ab") as stream:
                stream.write(raw + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
        except Exception as error:
            raise self._failure("Cannot retain source evidence journal") from error

    def _blob(self, body: bytes) -> dict:
        """Store response bytes content-addressed; refuses once the artifact has been sealed."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        try:
            self.store.put_blob(digest, len(body), [body])
        except Exception as error:
            raise self._failure("Cannot retain source response bytes") from error
        return {"sha256": digest, "byte_size": len(body)}

    def capture(self, capture: CapturedBodyResponse, *, stage: str) -> None:
        """Retain one owner capture's body (and request body) into the evidence store.

        Anything that is not a ``CapturedBodyResponse`` raises
        ``SourceEvidenceError``; a credential-bearing or 401/403 response raises
        ``CredentialRefusedError`` and stores nothing.
        """
        if not isinstance(capture, CapturedBodyResponse):
            raise SourceEvidenceError("Source result has no owner capture")
        if capture.status_code in (401, 403) or (
            self.credential
            and any(self.credential.encode() in body for body in (capture.body, capture.request_body or b""))
        ):
            # Never store a credential-refused original, including one returned
            # by an injected adapter that bypassed the owner's transport.
            raise CredentialRefusedError("Source response refused; credential-bearing bytes were not retained")
        response = self._blob(capture.body)
        request = self._blob(capture.request_body) if capture.request_body is not None else None
        self.event(
            "capture",
            stage=stage,
            requested_url=capture.requested_url,
            resolved_url=capture.resolved_url,
            status_code=capture.status_code,
            content_type=capture.content_type,
            content_encoding=capture.content_encoding,
            method=capture.method,
            observed_at=capture.observed_at,
            **response,
            request_body=request,
        )

    def _retain_file(
        self, stream: IO[bytes], *, sha256: str, byte_size: int, stage: str, requested_url: str,
        resolved_url: str, observed_at: str, status_code: int, content_type: str | None,
        content_encoding: str | None, method: str, request_body: bytes | None,
    ) -> dict:
        """Retain a complete, already bounded and credential-scanned file in one read.

        The store re-hashes while it copies and refuses bytes that differ from
        ``sha256``/``byte_size``, so a file changed after acquisition never seals.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if status_code in (401, 403) or (self.credential and request_body and self.credential.encode() in request_body):
            raise CredentialRefusedError("Source response refused; credential-bearing bytes were not retained")
        stream.seek(0)
        try:
            self.store.put_blob(sha256, byte_size, iter(lambda: stream.read(_CHUNK), b""))
        except Exception as error:
            raise self._failure("Cannot retain source file bytes") from error
        response = {"sha256": sha256, "byte_size": byte_size}
        request = self._blob(request_body) if request_body is not None else None
        self.event("capture", stage=stage, requested_url=requested_url, resolved_url=resolved_url,
                   observed_at=observed_at, status_code=status_code, content_type=content_type,
                   content_encoding=content_encoding, method=method, request_body=request, **response)
        return response

    def transport(self, transport=None, *, stage: str, max_bytes: int):
        """Observe streamed HTTP responses before owner parsing, including retries.

        The source still owns requests, pacing and validation. This wrapper only
        retains exact transport bytes and refuses retention failures. Redirects
        are individual observations, so signed credentials are scrubbed by the
        same journal policy as ordinary captures. Each byte is hashed and
        scanned as it is written, then read once by the store and once more
        by the owner; no response is held in memory.
        """
        import httpx

        evidence = self
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be positive")

        class RetainedStream(httpx.SyncByteStream):
            def __init__(self, stream, request, response, observed_at):
                self.stream, self.request, self.response = stream, request, response
                self.observed_at = observed_at

            def __iter__(self):
                check = _StreamCheck(evidence, max_bytes)
                # Temporary files are private and deleted on success, overflow,
                # interruption or credential refusal. Only verified files seal.
                with tempfile.NamedTemporaryFile(dir=evidence.directory) as stream:
                    retained = False
                    try:
                        for chunk in self.stream:
                            check.update(chunk)
                            stream.write(chunk)
                        stream.flush()
                        evidence._retain_file(
                            stream, sha256=check.sha256, byte_size=check.size, stage=stage,
                            requested_url=str(self.request.url), resolved_url=str(self.request.url),
                            observed_at=self.observed_at, status_code=self.response.status_code,
                            content_type=self.response.headers.get("content-type"),
                            content_encoding=self.response.headers.get("content-encoding", "identity"),
                            method=self.request.method, request_body=self.request.content or None,
                        )
                        retained = True
                        # Retain before HTTP decoding too: malformed gzip must
                        # not discard the native response that explains refusal.
                        stream.seek(0)
                        while chunk := stream.read(_CHUNK):
                            yield chunk
                    except BaseException as error:
                        if not retained:
                            evidence.event("capture-incomplete", stage=stage, error_type=type(error).__name__,
                                           requested_url=str(self.request.url), bytes_received=check.size)
                        raise

            def close(self):
                self.stream.close()

        class RetainedTransport(httpx.BaseTransport):
            def __init__(self):
                self.inner = transport if transport is not None else httpx.HTTPTransport()

            def handle_request(self, request):
                observed_at = datetime.now(UTC).isoformat()
                response = self.inner.handle_request(request)
                return httpx.Response(response.status_code, headers=response.headers,
                                      stream=RetainedStream(response.stream, request, response, observed_at),
                                      extensions=response.extensions)

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
        retained = None
        if not isinstance(error, CredentialRefusedError):
            capture = attached_capture(error)
            if capture is not None:
                # The exact bytes are retained whole; a second, bounded copy would only duplicate them.
                self.capture(capture, stage=stage + ":refused")
            elif isinstance(error, Exception):
                try:
                    retained = retain_refused_response(
                        error, store=self.store.root, max_bytes=24 * 1024 * 1024, credential=self.credential
                    )
                except Exception as failure:
                    raise self._failure("Cannot retain refused source response") from failure
        self.event(
            "refusal",
            stage=stage,
            error_type=type(error).__name__,
            message=self._safe(str(error))[:2000],
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
                evidence_status="No managed prior; any legacy or local merged inputs have no capture qualification.",
            )
            return
        self.prior_input = ArtifactInput(PRIOR_ROLE, prior["logicalId"], prior["artifactDigest"])
        if public_url is None:
            raise SourceEvidenceError("Managed prior evidence requires its immutable root")
        raw, root = load_family_root(public_url, prior)
        receipt = self._blob(raw)
        self.event(
            "lineage",
            prior_generation=prior,
            prior_root=receipt,
            prior_inputs=root["inputs"],
            evidence_status=(
                "Inherited pins; prior source coverage is not requalified by this run."
                if root["inputs"]
                else "Legacy prior has inputs=[]; inherited raw-source evidence gap."
            ),
        )

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
        self.event("build-outcome", outcome=outcome)
        from spicy_regs.generations import implementation_id

        source = LocalMemberSource(self.artifact_dir)
        members = [
            describe_member(source, object_key=key, role="evidence", media_type="application/octet-stream")
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
                "family": self.family,
                "outcome": outcome,
                "coverage": "observed-responses; inherited source gaps are recorded in journal.jsonl",
            },
            producer=Producer("spicy-regs", implementation, "urn:spicy-regs:source-evidence", "1", implementation),
            inputs=[self.prior_input] if self.prior_input else [],
            manifests=[manifest],
        )
        (self.artifact_dir / "artifact.json").write_bytes(canonical_json_bytes(root))
        self.artifact = admit_artifact(source)
        return self.artifact

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
        if self.artifact is None:
            if error is not None and not isinstance(error, SourceEvidenceError):
                self.refusal(error, stage="run")
            self.seal(outcome="failed" if error else "build-complete")
        assert self.artifact is not None
        (self.directory / "run-outcome.json").write_bytes(
            canonical_json_bytes(
                self._safe(
                    {
                        "outcome": "failed" if error else "complete",
                        "artifact": self.artifact.pin.as_dict(),
                        "error_type": type(error).__name__ if error else None,
                        "message": self._safe(str(error))[:2000] if error else None,
                    }
                )
            )
        )
        if swallowed is not None:
            raise swallowed


def verify_evidence(evidence: Path | MemberSource, *, expected_pin=None):
    """Admit the separate artifact from a directory or any member source; it must never masquerade as a table family."""
    source = LocalMemberSource(evidence) if isinstance(evidence, Path) else evidence
    artifact = admit_artifact(source, expected_pin=expected_pin)
    if artifact.root["kind"] != KIND or artifact.root["spec"]["outcome"] != "build-complete":
        raise SourceEvidenceError("Only completed-build evidence can support a generation")
    return artifact
