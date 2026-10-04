"""Replay ordered private scorecard captures through the normal publisher readers.

The same sequence handles HTML, workbook and JSON readers, including GraphQL
POSTs. Publisher parsing, completeness and evidence policy remain unchanged.
"""

from hashlib import sha256
import json
from pathlib import Path

from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.scorecards.acquisition import MAX_BYTES, validate_limits


class ScorecardReplayError(ValueError):
    """A retained capture does not reproduce the requested source observation."""


class RetainedScorecardSequence:
    """Require exact request order and input hashes; never fetch missing inputs.

    The private manifest is a list of captures with ``body_file``, bare-hex
    ``sha256``, ``byte_size`` and captured HTTP metadata. POST also names its
    exact ``request_body_file`` and ``request_body_sha256``. Pass the instance
    as ``fetch``; its ``request`` method supplies the shared POST seam.
    """

    def __init__(self, root: Path, *, manifest: str = "captures.json", expected_sha256: str, max_bytes=MAX_BYTES):
        validate_limits(max_bytes, 1)
        self.root = root.resolve()
        self.max_bytes = max_bytes
        raw = self._bytes(manifest)
        if sha256(raw).hexdigest() != expected_sha256:
            raise ScorecardReplayError("Retained scorecard manifest differs from its qualification pin")
        self.records = json.loads(raw)
        if not isinstance(self.records, list) or not self.records or not all(isinstance(r, dict) for r in self.records):
            raise ScorecardReplayError("Retained scorecard manifest must contain ordered captures")
        self.position = 0
        self.last_record = None

    def _bytes(self, name):
        if not isinstance(name, str) or not name or Path(name).is_absolute():
            raise ScorecardReplayError("Retained capture filename must be relative")
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root):
            raise ScorecardReplayError("Retained capture filename leaves the selected private corpus")
        with path.open("rb") as stream:
            raw = stream.read(self.max_bytes + 1)
        if len(raw) > self.max_bytes:
            raise ScorecardReplayError("Retained capture exceeds its byte bound")
        return raw

    def __call__(self, url):
        return self.request(url, method="GET", content=None)

    def request(self, url, *, method, content, request_headers=None):
        if self.position >= len(self.records):
            raise ScorecardReplayError("Publisher requested a capture absent from the qualified sequence")
        record = self.records[self.position]
        if record.get("requested_url") != url or record.get("method", "GET") != method:
            raise ScorecardReplayError("Publisher request differs from the qualified sequence")
        body = self._bytes(record.get("body_file"))
        if sha256(body).hexdigest() != record.get("sha256") or len(body) != record.get("byte_size"):
            raise ScorecardReplayError("Retained source bytes differ from the qualified capture")
        request_body = None
        if method == "POST":
            request_body = self._bytes(record.get("request_body_file"))
            if sha256(request_body).hexdigest() != record.get("request_body_sha256"):
                raise ScorecardReplayError("Retained publisher query differs from the qualified capture")
        if content != request_body:
            raise ScorecardReplayError("Publisher request body differs from the qualified capture")
        if "request_headers_sha256" in record:
            header_bytes = json.dumps(request_headers or {}, sort_keys=True, separators=(",", ":")).encode()
            if sha256(header_bytes).hexdigest() != record["request_headers_sha256"]:
                raise ScorecardReplayError("Publisher headers differ from the qualified private capture")
        response = CapturedBodyResponse(
            requested_url=url,
            resolved_url=record["resolved_url"],
            status_code=record["http_status"],
            content_type=record["content_type"],
            observed_at=record["observed_at"],
            body=body,
            method=method,
            request_body=request_body,
            content_encoding=record.get("content_encoding", "identity"),
        )
        self.position += 1
        self.last_record = record
        return response

    def complete(self):
        if self.position != len(self.records):
            raise ScorecardReplayError("Publisher did not consume the entire qualified capture sequence")
