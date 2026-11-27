"""Rollup pipeline: person_identity_decisions.parquet (reviewed cross-source person links).

Publishes the **entire** hash-chained ``person-identity-review/1`` decision log in every
generation (unpinned generations older than three are pruned, so the log must be complete in
every generation), with evidence and provenance held directly in columns (receipts retired).

Implemented as a :class:`RollupPipeline` subclass — **not** a ``LOCAL_FAMILIES`` entry — so
local verification never hits ``local_publication.py``'s 40 GiB dry-run disk floor.

**HARD OWNER-APPROVAL GATE:** Live R2 publication (``skip_upload=False``) is blocked until
the owner reviews the seeded 50-pair precision read sheet and explicitly records matching-rule
approval (supplied via ``owner_approval_ref`` / ``PERSON_IDENTITY_OWNER_APPROVAL``).
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
from cyclopts import App, Parameter
from dotenv import load_dotenv

from spicy_regs.identity_review import (
    PERSON_RULE,
    _canonical,
    verify_person_history,
)
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

FAMILY = "person-identity-decisions"
TABLE = "person_identity_decisions"
OUTPUT = f"{TABLE}.parquet"
POLICY_VERSION = PERSON_RULE
IDENTITY_FIELDS = ("event_id",)
OWNER_APPROVAL_ENV = "PERSON_IDENTITY_OWNER_APPROVAL"
DECISIONS_LOG_ENV = "PERSON_IDENTITY_DECISIONS_LOG"

#: Explicit Arrow schema for ``person_identity_decisions.parquet`` (receipts retired; evidence in columns).
SCHEMA = pa.schema([
    pa.field("event_ordinal", pa.int64(), nullable=False),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("previous_event_id", pa.string(), nullable=True),
    pa.field("policy_version", pa.string(), nullable=False),
    pa.field("rule", pa.string(), nullable=False),
    pa.field("decision", pa.string(), nullable=False),
    pa.field("candidate_id", pa.string(), nullable=False),
    pa.field("candidate_digest", pa.string(), nullable=False),
    pa.field("witness_key", pa.string(), nullable=False),
    pa.field("external_native_id", pa.string(), nullable=False),
    pa.field("relation", pa.string(), nullable=False),
    pa.field("role", pa.string(), nullable=False),
    pa.field("stated_name", pa.string(), nullable=False),
    pa.field("stated_org", pa.string(), nullable=False),
    pa.field("body_sha256", pa.string(), nullable=False),
    pa.field("page_or_offset", pa.string(), nullable=False),
    pa.field("quoted_span", pa.string(), nullable=False),
    pa.field("release_id", pa.string(), nullable=True),
    pa.field("source_generation_digest", pa.string(), nullable=True),
    pa.field("reviewer", pa.string(), nullable=False),
    pa.field("decided_at", pa.string(), nullable=False),
    pa.field("reason", pa.string(), nullable=False),
    pa.field("valid_from", pa.string(), nullable=True),
    pa.field("valid_to", pa.string(), nullable=True),
    pa.field("validity_basis", pa.string(), nullable=False),
    pa.field("evidence_json", pa.string(), nullable=False),
    pa.field("scope_json", pa.string(), nullable=False),
    pa.field("event_json", pa.string(), nullable=False),
])


def read_decisions_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a bounded JSONL person-identity decision log and refuse incomplete final lines."""
    if not path.exists():
        raise FileNotFoundError(f"Person identity decisions log not found: {path}")
    raw = path.read_bytes()
    if len(raw) > 50 * 1024 * 1024:
        raise ValueError("Person identity decisions log exceeds 50 MiB bound")
    text = raw.decode("utf-8")
    if text and not text.endswith("\n"):
        raise ValueError("Person identity decisions log has an incomplete final line")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def event_to_row(event: Mapping[str, Any], *, ordinal: int) -> dict[str, Any]:
    """Flatten one verified ``person-identity-review/1`` event into a typed Parquet row."""
    candidate = event["candidate"]
    scope = event["scope"]
    return {
        "event_ordinal": ordinal,
        "event_id": event["event_id"],
        "previous_event_id": event["previous_event_id"],
        "policy_version": event["policy_version"],
        "rule": event["rule"],
        "decision": event["decision"],
        "candidate_id": candidate["candidate_id"],
        "candidate_digest": event["candidate_digest"],
        "witness_key": event["witness_key"],
        "external_native_id": event["external_native_id"],
        "relation": scope["relationship"],
        "role": scope["role"],
        "stated_name": candidate["stated_name"],
        "stated_org": candidate["stated_org"],
        "body_sha256": candidate["body_sha256"],
        "page_or_offset": candidate["page_or_offset"],
        "quoted_span": candidate["quoted_span"],
        "release_id": candidate.get("release_id"),
        "source_generation_digest": candidate.get("source_generation_digest"),
        "reviewer": event["reviewer"],
        "decided_at": event["decided_at"],
        "reason": event["reason"],
        "valid_from": scope.get("valid_from"),
        "valid_to": scope.get("valid_to"),
        "validity_basis": scope["validity_basis"],
        "evidence_json": _canonical(event["evidence"]).decode("utf-8"),
        "scope_json": _canonical(scope).decode("utf-8"),
        "event_json": _canonical(dict(event)).decode("utf-8"),
    }


def build_person_identity_decisions(
    output_dir: Path,
    *,
    history: Sequence[Mapping[str, Any]] | None = None,
    history_path: Path | None = None,
    evidence: CaptureEvidence | None = None,
) -> Path:
    """Verify the complete hash-chained person decision log and materialize ``person_identity_decisions.parquet``."""
    if history is not None and history_path is not None:
        raise ValueError("Specify either history or history_path, not both")
    resolved_path: Path | None = None
    if history is None:
        if history_path is None:
            env_path = os.environ.get(DECISIONS_LOG_ENV)
            if not env_path:
                raise ValueError(
                    f"A person identity decisions log is required (pass history_path or set {DECISIONS_LOG_ENV})"
                )
            resolved_path = Path(env_path)
        else:
            resolved_path = history_path
        events = read_decisions_jsonl(resolved_path)
    else:
        events = [dict(e) for e in history]

    verified = verify_person_history(events)
    if evidence is not None:
        if resolved_path is not None:
            evidence.retain_file(
                resolved_path,
                stage="person-identity-decisions-log",
                raw_source=False,
                policy_version=POLICY_VERSION,
                event_count=len(verified),
            )
        else:
            canonical_log = b"".join(_canonical(event) + b"\n" for event in verified)
            evidence.retain_bytes(
                canonical_log,
                stage="person-identity-decisions-log",
                raw_source=False,
                policy_version=POLICY_VERSION,
                event_count=len(verified),
            )

    rows = [event_to_row(event, ordinal=idx) for idx, event in enumerate(verified)]

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / OUTPUT
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, out_path, compression="zstd")
    return out_path


def verify_decisions_parquet(path: Path) -> list[dict[str, Any]]:
    """Read back ``person_identity_decisions.parquet`` and verify full hash-chain continuity."""
    table = pq.read_table(path)
    if table.schema != SCHEMA:
        raise ValueError("person_identity_decisions.parquet schema differs from declared SCHEMA")
    rows = sorted(table.to_pylist(), key=lambda r: r["event_ordinal"])
    events: list[dict[str, Any]] = []
    for expected_ord, row in enumerate(rows):
        if row["event_ordinal"] != expected_ord:
            raise ValueError(f"Non-contiguous event_ordinal {row['event_ordinal']} (expected {expected_ord})")
        event = json.loads(row["event_json"])
        if event["event_id"] != row["event_id"] or event["previous_event_id"] != row["previous_event_id"]:
            raise ValueError("Column event_id/previous_event_id disagrees with canonical event_json")
        if event["witness_key"] != row["witness_key"] or event["candidate_digest"] != row["candidate_digest"]:
            raise ValueError("Column witness_key/candidate_digest disagrees with canonical event_json")
        events.append(event)
    return verify_person_history(events)


class PersonIdentityDecisionsRollup(RollupPipeline):
    """Reviewed cross-source person identity decisions (CT witness_key <-> external native ID)."""

    name: ClassVar[str] = FAMILY
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = OUTPUT

    def __init__(
        self,
        *,
        output_dir: Path | None = None,
        skip_upload: bool = True,
        history_path: Path | None = None,
        history: Sequence[Mapping[str, Any]] | None = None,
        owner_approval_ref: str | None = None,
    ) -> None:
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        self.history_path = history_path
        self.history = history
        self.owner_approval_ref = owner_approval_ref or os.environ.get(OWNER_APPROVAL_ENV)

    def generation_schemas(self) -> dict[str, list[tuple[str, str]]]:
        """Declare ``person_identity_decisions`` schema so local generation admission works cleanly."""
        schemas = super().generation_schemas()
        return {**schemas, TABLE: described_schema(SCHEMA)}

    def run(self, *, read_operation=None) -> None:
        """Enforce the mandatory owner-approval gate before any non-local upload."""
        if not self.skip_upload and not (self.owner_approval_ref and self.owner_approval_ref.strip()):
            raise RuntimeError(
                "Live publication of person-identity-decisions is blocked until explicit owner approval "
                f"of the matching rule is recorded (pass owner_approval_ref or set {OWNER_APPROVAL_ENV})"
            )
        super().run(read_operation=read_operation)

    def build(self, output_dir: Path) -> Path:
        out_path = build_person_identity_decisions(
            output_dir,
            history=self.history,
            history_path=self.history_path,
            evidence=self.source_evidence,
        )
        verified = verify_decisions_parquet(out_path)
        if not self.skip_upload and not verified:
            raise RuntimeError("Refusing to publish an empty person-identity-decisions log to live R2")
        return out_path


app = App(
    name=f"run-rollup-{PersonIdentityDecisionsRollup.name}",
    help="Materialize and verify the hash-chained person_identity_decisions log.",
)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    history_path: Annotated[Path | None, Parameter(help="JSONL decision log path")] = None,
    owner_approval_ref: Annotated[str | None, Parameter(help="Explicit owner rule-approval reference")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (mandatory until owner approval)")] = True,
) -> None:
    load_dotenv()
    PersonIdentityDecisionsRollup(
        output_dir=output_dir,
        skip_upload=skip_upload,
        history_path=history_path,
        owner_approval_ref=owner_approval_ref,
    ).run()


if __name__ == "__main__":
    app()
