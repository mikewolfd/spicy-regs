"""Retain owner captures and bind their bytes to a separate audit artifact.

This records responses used by a run, not a complete source population or a
source-native release. Acquisition, credential checks and parsing stay in
SpicyDocs. Rulespec owns blob integrity and artifact admission.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
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
from spicy_docs.reading.refusals import RefusedResponse, retain_refused_response
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
from spicy_docs.transport.captured import CapturedBodyResponse, attached_capture
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

KIND = "spicy-regs-source-evidence"
INPUT_ROLE = "source-evidence"
PRIOR_ROLE = "prior-generation"
JOURNAL = "journal.jsonl"
_CHUNK = 1024 * 1024
POLICIES = frozenset({"full", "hash_only", "metadata_only"})
EVIDENCE_VERSION = 2


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
        self.policy = "full"
        self._source_policies: dict[str, tuple[str, str, str]] = {}
        self._restricted_diagnostics = False
        self.credential = ""
        self.artifact = None
        self.prior_input: ArtifactInput | None = None
        self.read_snapshot: dict | None = None
        self.retention_failure: str | None = None
        #: Where the prior generation's own evidence journal is, and its events once read.
        self._prior_evidence: tuple[str, dict] | None = None
        self._prior_events: list[dict] | None = None
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

    def _body(self, body: bytes) -> dict:
        """Public receipt for bytes, retaining them only under full policy."""
        result: dict[str, str | int | bool] = {"byte_size": len(body), "body_retained": self.policy == "full"}
        if self.policy != "metadata_only":
            result["sha256"] = "sha256:" + hashlib.sha256(body).hexdigest()
        if self.policy == "full":
            self._blob(body)
            result["blob_member"] = "blobs/sha256/" + hashlib.sha256(body).hexdigest()
        return result

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
        if event in {"capture", "retained-file", "refusal", "capture-incomplete"}:
            fields.setdefault("evidence_policy", self.policy)
            fields.setdefault("parser_version", version("spicy-docs"))
            fields.setdefault("policy_decision_id", "existing-full-default")
            fields.setdefault("publisher_id", None)
        try:
            raw = canonical_json_bytes(
                self._safe({"event": event, "recorded_at": datetime.now(UTC).isoformat(), **fields})
            )
            with (self.artifact_dir / JOURNAL).open("ab") as stream:
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

    def capture(self, capture: CapturedBodyResponse, *, stage: str) -> dict:
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
        response = self._body(capture.body)
        request = self._body(capture.request_body) if capture.request_body is not None else None
        receipt = dict(
            capture_id=uuid4().hex,
            evidence_policy=self.policy,
            response_complete=True,
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
        self.event("capture", **receipt)
        return receipt

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
    ) -> dict:
        """Retain a complete, already bounded and credential-scanned file in one read.

        The store re-hashes while it copies and refuses bytes that differ from
        ``sha256``/``byte_size``, so a file changed after acquisition never seals.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if status_code in (401, 403) or (self.credential and request_body and self.credential.encode() in request_body):
            raise CredentialRefusedError("Source response refused; credential-bearing bytes were not retained")
        response = self._file_blob(stream, sha256=sha256, byte_size=byte_size)
        request = self._body(request_body) if request_body is not None else None
        receipt = dict(
            capture_id=uuid4().hex,
            evidence_policy=self.policy,
            response_complete=True,
            stage=stage,
            requested_url=requested_url,
            resolved_url=resolved_url,
            observed_at=observed_at,
            status_code=status_code,
            content_type=content_type,
            content_encoding=content_encoding,
            method=method,
            request_body=request,
            **response,
        )
        self.event("capture", **receipt)
        return receipt

    def _file_blob(self, stream: IO[bytes], *, sha256: str, byte_size: int) -> dict:
        """Store bounded chunks, checking the complete bytes against their selected identity."""
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        try:
            stream.seek(0)
            if self.policy == "full":
                self.store.put_blob(sha256, byte_size, iter(lambda: stream.read(_CHUNK), b""))
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
        return result

    def retain_bytes(self, body: bytes, *, stage: str, **fields) -> dict:
        """Retain already bounded local input bytes without inventing an HTTP capture.

        Use the existing local-input receipt, with opaque observation identity.
        Restricted policies omit caller metadata because it can contain source
        excerpts or byte-derived identifiers, just as ``retain_file`` does.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if not isinstance(body, bytes):
            raise SourceEvidenceError("Retained input must be immutable bytes")
        if self.credential and self.credential.encode() in body:
            raise CredentialRefusedError("Source input refused; credential-bearing bytes were not retained")
        blob = self._body(body)
        receipt = dict(fields) if self.policy == "full" else {}
        receipt.update(
            capture_id=uuid4().hex,
            evidence_policy=self.policy,
            response_complete=True,
            stage=stage,
            **blob,
        )
        self.event("retained-file", **receipt)
        return receipt

    def retain_file(self, path: Path, *, stage: str, **fields) -> dict:
        """Retain a local file the run read rather than fetched, its exact bytes and digest, and journal it.

        For a one-time input kept outside the repository, such as a converted table, so the generation's evidence
        holds the bytes its rows came from. Hash and scan bounded chunks, then let the existing store verify
        them again while retaining them. A changed file cannot be journaled as a successful retention.
        """
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")

        def identity(info):
            return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns

        try:
            with path.open("rb") as stream:
                before = identity(os.fstat(stream.fileno()))
                check = _StreamCheck(self, before[2])
                for chunk in iter(lambda: stream.read(_CHUNK), b""):
                    check.update(chunk)
                if (
                    check.size != before[2]
                    or identity(os.fstat(stream.fileno())) != before
                    or identity(path.stat()) != before
                ):
                    raise self._failure("Source file changed during retention")
                blob = self._file_blob(stream, sha256=check.sha256, byte_size=check.size)
                if identity(os.fstat(stream.fileno())) != before or identity(path.stat()) != before:
                    raise self._failure("Source file changed during retention")
        except (SourceEvidenceError, CredentialRefusedError):
            raise
        except Exception as error:
            raise self._failure("Cannot retain source file bytes") from error
        # Arbitrary caller fields and paths may contain excerpts; non-retaining
        # contexts publish only the bounded receipt, never local extraction names.
        receipt = dict(capture_id=uuid4().hex, evidence_policy=self.policy, response_complete=True, stage=stage, **blob)
        if self.policy == "full":
            receipt.update(name=path.name, **fields)
        self.event("retained-file", **receipt)
        if not isinstance(self, SourceEvidenceContext):
            return {"sha256": blob["sha256"], "byte_size": blob["byte_size"]}
        return receipt

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
            def __init__(self, stream, request, request_body, response, observed_at):
                self.stream, self.request, self.response = stream, request, response
                self.request_body, self.observed_at = request_body, observed_at

            def __iter__(self):
                check = _StreamCheck(evidence, max_bytes)
                # Temporary files are private and deleted on success, overflow,
                # interruption or credential refusal. Only verified files seal.
                with tempfile.NamedTemporaryFile(prefix="spicy-source-private-") as stream:
                    retained = False
                    try:
                        for chunk in self.stream:
                            check.update(chunk)
                            stream.write(chunk)
                        stream.flush()
                        evidence._retain_file(
                            stream,
                            sha256=check.sha256,
                            byte_size=check.size,
                            stage=stage,
                            requested_url=str(self.request.url),
                            resolved_url=str(self.request.url),
                            observed_at=self.observed_at,
                            status_code=self.response.status_code,
                            content_type=self.response.headers.get("content-type"),
                            content_encoding=self.response.headers.get("content-encoding", "identity"),
                            method=self.request.method,
                            request_body=self.request_body,
                        )
                        retained = True
                        # Retain before HTTP decoding too: malformed gzip must
                        # not discard the native response that explains refusal.
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
                return httpx.Response(
                    response.status_code,
                    headers=response.headers,
                    stream=RetainedStream(response.stream, request, request_body, response, observed_at),
                    extensions=response.extensions,
                )

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
        restricted_root = (
            self.policy == "full" and self._restricted_diagnostics and not isinstance(self, SourceEvidenceContext)
        )
        if not isinstance(error, CredentialRefusedError) and not restricted_root:
            capture = attached_capture(error)
            if capture is not None:
                # The exact bytes are retained whole; a second, bounded copy would only duplicate them.
                self.capture(capture, stage=stage + ":refused")
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
                        error, store=self.store.root, max_bytes=24 * 1024 * 1024, credential=self.credential
                    )
                    if retained and "sha256" in retained:
                        retained.update(
                            body_retained=True,
                            byte_size=retained.pop("bytes"),
                            blob_member="blobs/sha256/" + retained["sha256"].removeprefix("sha256:"),
                        )
                except Exception as failure:
                    raise self._failure("Cannot retain refused source response") from failure
        self.event(
            "refusal",
            stage=stage,
            error_type=type(error).__name__,
            evidence_policy=self.policy,
            message=self._safe(str(error))[:2000] if self.policy == "full" and not restricted_root else None,
            reason_code="source_refused",
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
        pin = next((item for item in root["inputs"] if item.get("role") == INPUT_ROLE), None)
        self._prior_evidence = None if pin is None else (public_url, pin)
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

    def inherited_event(self, event: str, **match) -> dict | None:
        """The prior generation's last journaled ``event`` whose fields equal ``match``, or ``None``.

        ``None`` also when the prior retained no evidence: no managed prior, or a
        legacy one with ``inputs=[]``. The journal is read once, on first use,
        and pinned through the prior root, so a rollup that never asks pays nothing.
        """
        if self._prior_evidence is None:
            return None
        if self._prior_events is None:
            from spicy_regs.sources.publication import load_evidence_journal

            self._prior_events = [
                json.loads(line) for line in load_evidence_journal(*self._prior_evidence).splitlines()
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
        self.event("build-outcome", outcome=outcome)
        from spicy_regs.generations import implementation_id

        source = LocalMemberSource(self.artifact_dir)
        lineage_keys = set()
        with (self.artifact_dir / JOURNAL).open() as stream:
            for line in stream:
                event = json.loads(line)
                if event.get("event") == "lineage" and event.get("prior_root"):
                    lineage_keys.add("blobs/sha256/" + event["prior_root"]["sha256"].removeprefix("sha256:"))
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
                        "message": self._safe(str(error))[:2000]
                        if error and not self._restricted_diagnostics
                        else None,
                    }
                )
            )
        )
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

    def event(self, event: str, **fields) -> None:
        fields.update(
            publisher_id=self.publisher_id,
            evidence_policy=self.policy,
            parser_version=self.parser_version,
            policy_decision_id=self.policy_decision_id,
        )
        self.root.event(event, **fields)

    def _failure(self, message: str):
        return self.root._failure(message)

    def _safe(self, value):
        return self.root._safe(value)

    _blob = CaptureEvidence._blob
    _body = CaptureEvidence._body
    _file_blob = CaptureEvidence._file_blob
    _retain_file = CaptureEvidence._retain_file
    capture = CaptureEvidence.capture
    retain_file = CaptureEvidence.retain_file
    retain_bytes = CaptureEvidence.retain_bytes
    transport = CaptureEvidence.transport
    refusal = CaptureEvidence.refusal


def _verify_receipts(source: MemberSource, artifact) -> None:
    """Validate versioned receipt policy and every declared body/member binding."""
    evidence_version = artifact.root["spec"].get("evidence_version")
    if evidence_version is None:
        return  # Existing full-retention artifacts retain their original identity.
    if type(evidence_version) is not int or evidence_version != EVIDENCE_VERSION:
        raise SourceEvidenceError("Unknown source evidence version")

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
                "sha256" not in receipt and "blob_member" not in receipt,
                "Metadata-only receipt exposes a body identity",
            )
        else:
            digest = receipt.get("sha256")
            require(
                isinstance(digest, str)
                and len(digest) == 71
                and digest.startswith("sha256:")
                and all(c in "0123456789abcdef" for c in digest[7:]),
                "Invalid source body digest",
            )
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
                if policy != "full" or "body_retained" in event["response"]:
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
                }
                require(set(event) <= allowed, "Unexpected non-retaining receipt fields")
                require(event.get("message") is None, "Non-retaining diagnostics contain source text")
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


def verify_evidence(evidence: Path | MemberSource, *, expected_pin=None):
    """Admit the separate artifact from a directory or any member source; it must never masquerade as a table family."""
    source = LocalMemberSource(evidence) if isinstance(evidence, Path) else evidence
    artifact = admit_artifact(source, expected_pin=expected_pin)
    if artifact.root["kind"] != KIND or artifact.root["spec"]["outcome"] != "build-complete":
        raise SourceEvidenceError("Only completed-build evidence can support a generation")
    _verify_receipts(source, artifact)
    return artifact
