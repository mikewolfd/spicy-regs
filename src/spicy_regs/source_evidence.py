"""Retain owner captures and bind their bytes to a separate audit artifact.

This records responses used by a run, not a complete source population or a
source-native release. Acquisition, credential checks and parsing stay in
SpicyDocs. Rulespec owns blob integrity and artifact admission.
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from rulespec_artifacts import (
    ArtifactInput,
    LocalMemberSource,
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


class SourceEvidenceError(RuntimeError):
    """Source retention failed; never turn this into a skippable source refusal."""


class CaptureEvidence:
    """A durable journal followed by an immutable, independently admitted artifact."""

    def __init__(self, output_dir: Path, family: str):
        self.directory = output_dir / "source-evidence" / uuid4().hex
        self.artifact_dir = self.directory / "artifact"
        self.artifact_dir.mkdir(parents=True)
        self.store = LocalSourceNativeBlobStore(self.artifact_dir / "blobs")
        self.family = family
        self.credential = ""
        self.artifact = None
        self.prior_input: ArtifactInput | None = None
        self.event(
            "run",
            family=family,
            packages={name: version(name) for name in ("spicy-regs", "spicy-docs")},
            coverage="Observed responses only; caps, refusals and inherited gaps remain explicit.",
        )

    def _safe(self, value):
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

    def event(self, event: str, **fields) -> None:
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
            raise SourceEvidenceError("Cannot retain source evidence journal") from error

    def _blob(self, body: bytes) -> dict:
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        try:
            self.store.put_blob(digest, len(body), [body])
        except Exception as error:
            raise SourceEvidenceError("Cannot retain source response bytes") from error
        return {"sha256": digest, "byte_size": len(body)}

    def capture(self, capture: CapturedBodyResponse, *, stage: str) -> None:
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

    def refusal(self, error: BaseException, *, stage: str) -> None:
        if self.artifact is not None:
            raise SourceEvidenceError("Cannot append to sealed source evidence")
        if isinstance(error, SourceEvidenceError):
            raise error
        retained = None
        if not isinstance(error, CredentialRefusedError):
            capture = attached_capture(error)
            if capture is not None:
                self.capture(capture, stage=stage + ":refused")
            if isinstance(error, Exception):
                try:
                    retained = retain_refused_response(
                        error, store=self.store.root, max_bytes=24 * 1024 * 1024, credential=self.credential
                    )
                except Exception as failure:
                    raise SourceEvidenceError("Cannot retain refused source response") from failure
        self.event(
            "refusal",
            stage=stage,
            error_type=type(error).__name__,
            message=self._safe(str(error))[:2000],
            response=retained,
            credential_refused=isinstance(error, CredentialRefusedError),
        )

    def inherit(self, prior_index: dict, *, public_url: str | None) -> None:
        from spicy_regs.sources.publication import load_family_root

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

    def seal(self, *, outcome: str):
        if self.artifact is not None:
            return self.artifact
        self.event("build-outcome", outcome=outcome)
        from spicy_regs.generations import _implementation_id

        source = LocalMemberSource(self.artifact_dir)
        members = [
            describe_member(source, object_key=key, role="evidence", media_type="application/octet-stream")
            for key in sorted(source.keys())
        ]
        with (self.artifact_dir / "members.json").open("wb") as stream:
            manifest = write_member_manifest(
                stream, scope_kind="global", scope_id=self.family, object_key="members.json", members=members
            )
        implementation = _implementation_id()
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
        artifact = self.seal(outcome="build-complete")
        return [
            ArtifactInput(INPUT_ROLE, artifact.pin.logical_id, artifact.pin.artifact_digest),
            *([self.prior_input] if self.prior_input else []),
        ]

    def finish(self, error: BaseException | None = None) -> None:
        """Retain final run status outside the sealed artifact, even on upload failure."""
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


def verify_evidence(directory: Path, *, expected_pin=None):
    """Admit the separate artifact; it must never masquerade as a table family."""
    artifact = admit_artifact(LocalMemberSource(directory), expected_pin=expected_pin)
    if artifact.root["kind"] != KIND or artifact.root["spec"]["outcome"] != "build-complete":
        raise SourceEvidenceError("Only completed-build evidence can support a generation")
    return artifact
