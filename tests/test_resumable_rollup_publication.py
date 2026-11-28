"""Interruption, partial-upload resume, lost pointer-ack reconciliation, and refusal tests (ss-dbtx)."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from botocore.exceptions import ConnectionClosedError
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs import generations
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
from spicy_regs.pipelines.rollups.base import (
    RESUME_CANDIDATES_DIR,
    RESUME_GENERATION_ENV,
    RESUME_REQUIRE_ENV,
    ResumeRefusedError,
    RollupPipeline,
)
from spicy_regs.pipelines.rollups.committee_rosters import CommitteeRostersRollup
from spicy_regs.pipelines.rollups.fec_observations import FecObservationsRollup
from spicy_regs.sources import cloudflare, publication as pub, r2
from tests.generation_fakes import Store, error
from tests.test_generation_publication import build, publish


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"


def _setup_r2_env(monkeypatch: pytest.MonkeyPatch, store: Store) -> None:
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "fake")
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)
    monkeypatch.setattr(
        pub,
        "load_index",
        lambda _url: pub.parse_index(store.objects[pub.INDEX_V2_KEY])
        if pub.INDEX_V2_KEY in store.objects
        else pub.empty_index(),
    )


def _policy(dataset: str) -> DatasetPolicy:
    return DatasetPolicy(
        dataset=dataset,
        subject_schema=pa.schema([("id", pa.string()), ("payload", pa.string())]),
        identity_fields=("id",),
        receipt_fields=(),
        policy_version="test/1",
    )


class _TwoTableEvidenceReceiptRollup(RollupPipeline):
    """2-table + receipt + source-evidence rollup with build/fetch counters for resume tests."""

    name: ClassVar[str] = "resume-demo"
    outputs: ClassVar[tuple[str, ...]] = ("alpha.parquet", "beta.parquet")
    retain_source_evidence: ClassVar[bool] = True
    receipt_policies: ClassVar[tuple] = (_policy("alpha"), _policy("beta"))

    def __init__(self, *, counters: dict[str, int], deferred: BaseException | None = None, payload_repeat: int = 40, **kwargs):
        super().__init__(**kwargs)
        self._counters = counters
        self._inject_deferred = deferred
        self._payload_repeat = payload_repeat

    def generation_schemas(self):
        return {
            "alpha": [["id", "VARCHAR"], ["payload", "VARCHAR"]],
            "beta": [["id", "VARCHAR"], ["payload", "VARCHAR"]],
        }

    def build(self, output_dir: Path) -> tuple[Path, Path]:
        from spicy_docs.transport.captured import CapturedBodyResponse

        self._counters["build"] = self._counters.get("build", 0) + 1
        self._counters["fetch"] = self._counters.get("fetch", 0) + 1
        assert self.source_evidence is not None
        self.source_evidence.capture(
            CapturedBodyResponse(
                "https://source.test/demo.json",
                "https://source.test/demo.json",
                200,
                "application/json",
                "2026-11-28T00:00:00Z",
                b'{"items": ["a", "b"]}',
            ),
            stage="fetch-demo",
        )
        alpha_rcpt = output_dir / "_alpha_rcpt"
        beta_rcpt = output_dir / "_beta_rcpt"

        def _make_dataset(ds: str, target_dir: Path) -> tuple[Path, Path]:
            pol = _policy(ds)
            ctx = ReceiptContext(
                self.receipt_generation_id,
                f"{ds}-1",
                pol.policy_version,
                [{"source_id": f"src-{ds}", "source_uri": f"https://source.test/{ds}",
                  "sha256": "sha256:" + "a" * 64, "locator": "row:0", "body_version": "v1"}],
                {},
            )
            payload = ("A" if ds == "alpha" else "B") * self._payload_repeat
            subject, rpath = write_dataset(
                [({"id": f"{ds}-1", "payload": payload}, ctx)],
                target_dir,
                pol,
                generation_id=self.receipt_generation_id,
            )
            assert subject is not None
            return subject, rpath

        alpha_path, alpha_rpath = _make_dataset("alpha", alpha_rcpt)
        beta_path, beta_rpath = _make_dataset("beta", beta_rcpt)
        combine_receipts([alpha_rpath, beta_rpath], output_dir / "etl_receipts.parquet")
        if self._inject_deferred is not None:
            self.deferred_failure = self._inject_deferred
        return (alpha_path, beta_path)


class _SimpleCounterRollup(RollupPipeline):
    name: ClassVar[str] = "simple-resume"
    output: ClassVar[str] = "simple.parquet"

    def __init__(self, *, counters: dict[str, int], value: str = "v1", deferred: BaseException | None = None, **kwargs):
        super().__init__(**kwargs)
        self._counters = counters
        self._value = value
        self._inject_deferred = deferred

    def build(self, output_dir: Path) -> Path:
        self._counters["build"] = self._counters.get("build", 0) + 1
        path = output_dir / self.output
        pq.write_table(pa.table({"id": [self._value]}), path)
        if self._inject_deferred is not None:
            self.deferred_failure = self._inject_deferred
        return path


# ---------------------------------------------------------------------------
# 1. Partial-transfer upload resume (single-part + multipart + receipts + evidence)
# ---------------------------------------------------------------------------


def test_partial_transfer_resume_skips_staged_multipart_members_without_rebuilding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Partial upload failure resumes with 0 builds, 0 fetches, 0 re-uploads of staged members, and 0 orphan evidence dirs."""
    from spicy_regs import etl_policy_registry

    monkeypatch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
    # Force multipart upload on alpha.parquet and beta.parquet.
    monkeypatch.setattr(pub, "PART_BYTES", 256)

    store = Store()
    _setup_r2_env(monkeypatch, store)
    counters: dict[str, int] = {}

    def fail_on_beta(key: str) -> None:
        if key.endswith("/beta.parquet"):
            raise OSError("mid-upload transport reset")

    store.before_put = fail_on_beta

    with pytest.raises(OSError, match="mid-upload transport reset"):
        _TwoTableEvidenceReceiptRollup(
            output_dir=tmp_path, skip_upload=False, counters=counters, payload_repeat=600
        ).run()

    assert counters == {"build": 1, "fetch": 1}
    assert pub.INDEX_V2_KEY not in store.objects

    # Exactly one source-evidence run dir exists and records the failed upload outcome while keeping sealed artifact.
    ev_runs = sorted((tmp_path / "source-evidence").iterdir())
    assert len(ev_runs) == 1
    assert json.loads((ev_runs[0] / "run-outcome.json").read_text())["outcome"] == "failed"
    assert (ev_runs[0] / "artifact" / "artifact.json").is_file()

    # Candidate binding exists under non-hidden resume-candidates/<family>/<digest>.json.
    bindings = list((tmp_path / RESUME_CANDIDATES_DIR / "resume-demo").glob("*.json"))
    assert len(bindings) == 1
    digest_hex = bindings[0].stem
    alpha_key = f"generations/resume-demo/{digest_hex}/alpha.parquet"
    beta_key = f"generations/resume-demo/{digest_hex}/beta.parquet"
    assert alpha_key in store.objects
    assert len(store.objects[alpha_key]) > pub.PART_BYTES  # proved multipart
    assert beta_key not in store.objects

    alpha_sent_before = store.sent[alpha_key]
    assert alpha_sent_before > 0

    # Clear fault and resume: zero build(), zero fetch, zero re-upload of alpha.parquet!
    store.before_put = None
    _TwoTableEvidenceReceiptRollup(
        output_dir=tmp_path, skip_upload=False, resume=True, counters=counters, payload_repeat=600
    ).run()

    assert counters == {"build": 1, "fetch": 1}
    assert store.sent[alpha_key] == alpha_sent_before, "already-staged multipart member must not be re-uploaded"
    assert beta_key in store.objects
    assert pub.INDEX_V2_KEY in store.objects
    live = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert live["families"]["resume-demo"]["artifactDigest"] == f"sha256:{digest_hex}"
    # Mandatory remote admission ran against ALL members (including HEAD-skipped alpha.parquet).
    assert store.reads[alpha_key] >= 1
    assert store.reads[beta_key] >= 1
    # remember_generation updated .native-state/selection.json.
    selection_path = tmp_path / ".native-state" / "selection.json"
    assert selection_path.is_file()
    selection = json.loads(selection_path.read_text())
    assert set(selection) >= {"alpha", "beta"}
    # Zero orphan source-evidence directories created on resume.
    assert sorted((tmp_path / "source-evidence").iterdir()) == ev_runs
    # Clean live publish unlinked the binding so future runs do not stall on stale candidates.
    assert list((tmp_path / RESUME_CANDIDATES_DIR / "resume-demo").glob("*.json")) == []


def test_stage_verified_generation_rejects_size_mismatch_and_same_size_corruption(tmp_path: Path) -> None:
    """Already-staged immutable member with wrong ContentLength refuses immediately; same-size corruption fails admission."""
    directory, artifact = build(tmp_path)
    prefix = f"generations/test/{artifact.pin.artifact_digest.removeprefix('sha256:')}"

    # 1. Wrong ContentLength refuses fail-closed before upload or pointer move.
    store_wrong_len = Store()
    store_wrong_len.objects[f"{prefix}/a.parquet"] = b"truncated"
    with pytest.raises(pub.PublicationError, match="ContentLength"):
        publish(store_wrong_len, directory)
    assert pub.INDEX_V2_KEY not in store_wrong_len.objects

    # 2. Same ContentLength with 1-bit flip is caught by mandatory admit_artifact byte verification.
    store_corrupt = Store()
    raw = bytearray((directory / "a.parquet").read_bytes())
    raw[-1] ^= 0x01
    store_corrupt.objects[f"{prefix}/a.parquet"] = bytes(raw)
    with pytest.raises(ArtifactVerificationError, match="digest"):
        publish(store_corrupt, directory)
    assert pub.INDEX_V2_KEY not in store_corrupt.objects


# ---------------------------------------------------------------------------
# 2. Lost pointer-acknowledgement reconciliation (in-process & cross-process)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "lost_ack_error",
    [
        error("PreconditionFailed"),
        ConnectionClosedError(endpoint_url="https://r2.test"),
        ConnectionError("socket reset after write"),
    ],
    ids=["412-precondition", "botocore-connection-closed", "os-connection-error"],
)
def test_in_process_lost_pointer_ack_reconciles_without_duplicate_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lost_ack_error: BaseException
) -> None:
    """When INDEX_V2_KEY write lands but its response is lost (412 or transport error), reconcile cleanly."""
    old, _ = build(tmp_path, "old", value="one")
    new, artifact = build(tmp_path, "new", value="two")
    store = Store()
    prior = publish(store, old)

    real_put = store.put_object
    pointer_puts = 0

    def landed_then_lost(**kwargs):
        nonlocal pointer_puts
        result = real_put(**kwargs)
        if kwargs["Key"] == pub.INDEX_V2_KEY:
            pointer_puts += 1
            raise lost_ack_error
        return result

    monkeypatch.setattr(store, "put_object", landed_then_lost)
    published = publish(store, new, prior)

    assert pointer_puts == 1, "reconciled lost ack must not rewrite the pointer"
    live = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert published == live
    assert live["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest


def test_in_process_transport_error_before_write_propagates_and_competing_generation_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Transport error BEFORE write landed propagates; genuinely competing generation on 412 still refuses."""
    old, _ = build(tmp_path, "old", value="one")
    new, _ = build(tmp_path, "new", value="two")
    competing, comp_artifact = build(tmp_path, "competing", value="three")
    store = Store()
    prior = publish(store, old)

    # Case A: ConnectionClosedError before write lands -> propagates, prior stands.
    def fail_before_write(key: str) -> None:
        if key == pub.INDEX_V2_KEY:
            raise ConnectionClosedError(endpoint_url="https://r2.test")

    store.before_put = fail_before_write
    with pytest.raises(ConnectionClosedError):
        publish(store, new, prior)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == prior

    # Case B: Competing writer moves family to a DIFFERENT generation -> 412 must NOT reconcile; raises PublicationError.
    other = Store()
    other.objects = dict(store.objects)
    publish(other, competing, prior)
    rival_bytes = other.objects[pub.INDEX_V2_KEY]

    def rival_wins(key: str) -> None:
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            store.objects[key] = rival_bytes

    store.before_put = rival_wins
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, new, prior)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY])["families"]["test"]["artifactDigest"] == (
        comp_artifact.pin.artifact_digest
    )


def test_cross_process_lost_pointer_ack_retry_reconciles_with_zero_builds_uploads_or_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Process 1 commits INDEX_V2_KEY then crashes before returning; Process 2 resumes and reconciles cleanly."""
    store = Store()
    _setup_r2_env(monkeypatch, store)
    counters: dict[str, int] = {}

    # Crash immediately after pointer write (during CDN purge) so Process 1 exits non-zero after R2 already committed.
    monkeypatch.setattr(cloudflare, "purge_urls", lambda _urls: (_ for _ in ()).throw(OSError("post-write crash")))
    with pytest.raises(OSError, match="post-write crash"):
        _SimpleCounterRollup(output_dir=tmp_path, skip_upload=False, counters=counters).run()

    assert counters == {"build": 1}
    live_before = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    published_at_before = live_before["families"]["simple-resume"]["publishedAt"]
    writes_before = list(store.writes)
    sent_before = dict(store.sent)

    # Process 2 resumes after clearing post-write crash.
    purged: list[list[str]] = []
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: purged.append(list(urls)))
    monkeypatch.setattr(pub, "_instant", lambda seconds=None: "2030-01-01T00:00:00Z")

    _SimpleCounterRollup(output_dir=tmp_path, skip_upload=False, resume=True, counters=counters).run()

    assert counters == {"build": 1}, "reconciliation must not rebuild"
    assert store.writes == writes_before, "reconciliation must perform zero uploads and zero pointer writes"
    assert dict(store.sent) == sent_before
    live_after = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert live_after["families"]["simple-resume"]["publishedAt"] == published_at_before
    assert purged == [["https://example.test/publication.v2.json"]]
    assert list((tmp_path / RESUME_CANDIDATES_DIR / "simple-resume").glob("*.json")) == []


# ---------------------------------------------------------------------------
# 3. Refusal matrix (zero builds, zero R2 writes, preserves prior & evidence)
# ---------------------------------------------------------------------------


def _seed_failed_upload_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: Store, rollup_cls=None, **rollup_kwargs
) -> tuple[dict[str, int], bytes | None]:
    """Run one rollup whose publish fails at pointer write, leaving a verified retained candidate on disk."""
    _setup_r2_env(monkeypatch, store)
    counters: dict[str, int] = {}
    prior_bytes = store.objects.get(pub.INDEX_V2_KEY)

    def fail_pointer(key: str) -> None:
        if key == pub.INDEX_V2_KEY:
            raise OSError("simulated pointer outage")

    store.before_put = fail_pointer
    cls = rollup_cls or _SimpleCounterRollup
    with pytest.raises(OSError, match="simulated pointer outage"):
        cls(output_dir=tmp_path, skip_upload=False, counters=counters, **rollup_kwargs).run()
    store.before_put = None
    assert counters["build"] == 1
    assert store.objects.get(pub.INDEX_V2_KEY) == prior_bytes
    return counters, prior_bytes


def test_refusal_matrix_stale_prior_and_drifted_parent_and_soft_and_remote_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """STALE_PRIOR, managed parent drift, soft_inputs drift, and remote_inputs ETag drift all refuse cleanly."""
    # --- A. STALE_PRIOR (live family moved to a different generation) ---
    store_a = Store()
    old_dir, _ = build(tmp_path, "a-old", family="simple-resume", keys=("simple.parquet",), value="v0")
    prior_a = publish(store_a, old_dir)
    out_a = tmp_path / "out-a"
    counters_a, _ = _seed_failed_upload_candidate(out_a, monkeypatch, store_a, value="v1")
    # Move live family to a competing generation v2.
    comp_dir, _ = build(tmp_path, "a-comp", family="simple-resume", keys=("simple.parquet",), value="v2")
    publish(store_a, comp_dir, prior_a)
    live_bytes_a = store_a.objects[pub.INDEX_V2_KEY]
    writes_a = list(store_a.writes)

    with pytest.raises(ResumeRefusedError, match="Live family 'simple-resume' moved"):
        _SimpleCounterRollup(output_dir=out_a, skip_upload=False, resume=True, counters=counters_a).run()
    assert counters_a["build"] == 1
    assert store_a.writes == writes_a and store_a.objects[pub.INDEX_V2_KEY] == live_bytes_a

    # --- B. Managed parent table moved (DRIFTED_INPUTS) ---
    class _DerivedRollup(RollupPipeline):
        name: ClassVar[str] = "derived-resume"
        inputs: ClassVar[tuple[str, ...]] = ("parent_tbl.parquet",)
        output: ClassVar[str] = "derived.parquet"

        def __init__(self, *, counters: dict[str, int], **kwargs):
            super().__init__(**kwargs)
            self._counters = counters

        def build(self, output_dir: Path) -> Path:
            self._counters["build"] = self._counters.get("build", 0) + 1
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["d1"]}), path)
            return path

    store_b = Store()
    p1_dir, _ = build(tmp_path, "b-p1", family="parent-fam", keys=("parent_tbl.parquet",), value="p1")
    p_prior = publish(store_b, p1_dir)
    monkeypatch.setattr(
        r2,
        "download",
        lambda key, dest: (
            dest.write_bytes(store_b.objects[pub.single_member(pub.current_index("https://example.test"), key).path])
            or True
        ),
    )
    out_b = tmp_path / "out-b"
    counters_b, _ = _seed_failed_upload_candidate(out_b, monkeypatch, store_b, rollup_cls=_DerivedRollup)
    # Move parent family to p2.
    p2_dir, _ = build(tmp_path, "b-p2", family="parent-fam", keys=("parent_tbl.parquet",), value="p2")
    publish(store_b, p2_dir, p_prior)
    writes_b = list(store_b.writes)

    with pytest.raises(ResumeRefusedError, match="Managed parent changed"):
        _DerivedRollup(output_dir=out_b, skip_upload=False, resume=True, counters=counters_b).run()
    assert counters_b["build"] == 1 and store_b.writes == writes_b

    # --- C. Soft input pin changed ---
    class _SoftInputRollup(_SimpleCounterRollup):
        name: ClassVar[str] = "soft-resume"
        soft_inputs: ClassVar[tuple[str, ...]] = ("soft_tbl.parquet",)

    store_c = Store()
    out_c = tmp_path / "out-c"
    counters_c, _ = _seed_failed_upload_candidate(out_c, monkeypatch, store_c, rollup_cls=_SoftInputRollup)
    # Now publish soft_tbl.parquet after candidate was built without it.
    s_dir, _ = build(tmp_path, "c-soft", family="soft-fam", keys=("soft_tbl.parquet",), value="s1")
    publish(store_c, s_dir)
    writes_c = list(store_c.writes)

    with pytest.raises(ResumeRefusedError, match="Soft inputs changed"):
        _SoftInputRollup(output_dir=out_c, skip_upload=False, resume=True, counters=counters_c).run()
    assert counters_c["build"] == 1 and store_c.writes == writes_c

    # --- D. Unmanaged remote input ETag changed ---
    class _RemoteInputRollup(_SimpleCounterRollup):
        name: ClassVar[str] = "remote-resume"
        remote_inputs: ClassVar[tuple[str, ...]] = ("legacy_remote.parquet",)

    store_d = Store()
    remote_version = {"etag": '"etag-v1"', "bytes": 1234}
    monkeypatch.setattr(r2, "public_object_version", lambda _url: dict(remote_version))
    out_d = tmp_path / "out-d"
    counters_d, _ = _seed_failed_upload_candidate(out_d, monkeypatch, store_d, rollup_cls=_RemoteInputRollup)
    writes_d = list(store_d.writes)

    remote_version["etag"] = '"etag-v2"'
    with pytest.raises(ResumeRefusedError, match="Remote inputs changed"):
        _RemoteInputRollup(output_dir=out_d, skip_upload=False, resume=True, counters=counters_d).run()
    assert counters_d["build"] == 1 and store_d.writes == writes_d


def test_refusal_matrix_identity_scope_tampering_partial_and_ambiguity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """validation_identity, implementationId, invocation_scope, tampering, local-partial, and multiple candidates all refuse."""
    from spicy_regs import etl_policy_registry

    monkeypatch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)

    # --- A. validation_identity and implementationId drift ---
    store = Store()
    out = tmp_path / "out-id"
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    counters, _ = _seed_failed_upload_candidate(out, monkeypatch, store)
    writes_before = list(store.writes)

    real_vid = generations.validation_identity()
    monkeypatch.setattr(generations, "validation_identity", lambda: {**real_vid, "spicy_docs_code": "deadbeef"})
    with pytest.raises(ResumeRefusedError, match="validation_identity changed"):
        _SimpleCounterRollup(output_dir=out, skip_upload=False, resume=True, counters=counters).run()
    assert counters["build"] == 1 and store.writes == writes_before
    monkeypatch.setattr(generations, "validation_identity", lambda: real_vid)

    # --- B. invocation_scope drift (env var changed) ---
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    with pytest.raises(ResumeRefusedError, match="Invocation scope changed"):
        _SimpleCounterRollup(output_dir=out, skip_upload=False, resume=True, counters=counters).run()
    assert counters["build"] == 1 and store.writes == writes_before
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")

    # --- C. implementationId drift ---
    real_impl = generations.implementation_id()
    monkeypatch.setattr(generations, "implementation_id", lambda: "urn:spicy-regs:implementation:sha256:" + "0" * 64)
    with pytest.raises(ResumeRefusedError, match="implementationId changed"):
        _SimpleCounterRollup(output_dir=out, skip_upload=False, resume=True, counters=counters).run()
    assert counters["build"] == 1 and store.writes == writes_before
    monkeypatch.setattr(generations, "implementation_id", lambda: real_impl)

    # --- D. Candidate Parquet file tampered on disk ---
    [gen_dir] = list((out / "generations").iterdir())
    parquet_path = gen_dir / "simple.parquet"
    original_parquet = parquet_path.read_bytes()
    tampered = bytearray(original_parquet)
    tampered[-1] ^= 0xFF
    parquet_path.write_bytes(bytes(tampered))
    with pytest.raises(ResumeRefusedError, match="cryptographic admission"):
        _SimpleCounterRollup(output_dir=out, skip_upload=False, resume=True, counters=counters).run()
    assert counters["build"] == 1 and store.writes == writes_before
    parquet_path.write_bytes(original_parquet)

    # --- E. Source-evidence blob tampered on disk ---
    store_ev = Store()
    out_ev = tmp_path / "out-ev"
    counters_ev, _ = _seed_failed_upload_candidate(out_ev, monkeypatch, store_ev, rollup_cls=_TwoTableEvidenceReceiptRollup)
    writes_ev = list(store_ev.writes)
    [ev_run] = list((out_ev / "source-evidence").iterdir())
    journal_path = ev_run / "artifact" / "journal.jsonl"
    journal_path.write_bytes(journal_path.read_bytes() + b"\n")
    with pytest.raises(ResumeRefusedError, match="source evidence failed verification"):
        _TwoTableEvidenceReceiptRollup(output_dir=out_ev, skip_upload=False, resume=True, counters=counters_ev).run()
    assert counters_ev["build"] == 1 and store_ev.writes == writes_ev
    # Existing failure receipt in source-evidence was preserved untouched.
    assert json.loads((ev_run / "run-outcome.json").read_text())["outcome"] == "failed"

    # --- F. local-partial candidate refused when skip_upload=False ---
    class _PartialRollup(RollupPipeline):
        name: ClassVar[str] = "partial-only"
        publication_family: ClassVar[str] = "parent-family"
        output: ClassVar[str] = "a.parquet"

        def __init__(self, *, counters: dict[str, int], **kwargs):
            super().__init__(**kwargs)
            self._counters = counters

        def build(self, output_dir: Path) -> Path:
            self._counters["build"] = self._counters.get("build", 0) + 1
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["p"]}), path)
            return path

    store_p = Store()
    _setup_r2_env(monkeypatch, store_p)
    out_p = tmp_path / "out-partial"
    counters_p: dict[str, int] = {}
    _PartialRollup(output_dir=out_p, skip_upload=True, counters=counters_p).run()
    assert counters_p["build"] == 1
    with pytest.raises(ResumeRefusedError, match="local-partial"):
        _PartialRollup(output_dir=out_p, skip_upload=False, resume=True, counters=counters_p).run()
    assert counters_p["build"] == 1 and not store_p.writes

    # --- G. Multiple ambiguous candidates in resume-candidates/<family>/ ---
    out_ambig = tmp_path / "out-ambig"
    counters_ambig: dict[str, int] = {}
    _SimpleCounterRollup(output_dir=out_ambig, skip_upload=True, value="v1", counters=counters_ambig).run()
    _SimpleCounterRollup(output_dir=out_ambig, skip_upload=True, value="v2", counters=counters_ambig).run()
    assert len(list((out_ambig / RESUME_CANDIDATES_DIR / "simple-resume").glob("*.json"))) == 2
    with pytest.raises(ResumeRefusedError, match="Multiple retained generation candidates"):
        _SimpleCounterRollup(output_dir=out_ambig, skip_upload=False, resume=True, counters=counters_ambig).run()
    assert counters_ambig["build"] == 2

    # --- H. Subclass invocation_scope() catches constructor-level scope differences ---
    roster_a = CommitteeRostersRollup(output_dir=tmp_path)
    roster_b = CommitteeRostersRollup(output_dir=tmp_path)
    roster_b.senate_paid_ladder = True
    assert roster_a.invocation_scope() != roster_b.invocation_scope()

    m1 = tmp_path / "m1.json"
    m2 = tmp_path / "m2.json"
    m1.write_text('{"a": 1}\n')
    m2.write_text('{"a": 2}\n')
    assert FecObservationsRollup(manifest=m1).invocation_scope() != FecObservationsRollup(manifest=m2).invocation_scope()


def test_direct_run_tables_resumes_candidate_without_calling_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Calling RollupPipeline._run_tables directly also discovers and resumes a retained candidate."""
    store = Store()
    counters, _ = _seed_failed_upload_candidate(tmp_path, monkeypatch, store)
    assert pub.INDEX_V2_KEY not in store.objects

    _SimpleCounterRollup(output_dir=tmp_path, skip_upload=False, resume=True, counters=counters)._run_tables(tmp_path)
    assert counters["build"] == 1
    assert pub.INDEX_V2_KEY in store.objects


# ---------------------------------------------------------------------------
# 4. deferred_failure preservation on both publish-resume & already-live reconcile
# ---------------------------------------------------------------------------


def test_deferred_failure_is_preserved_and_reraised_on_resume_and_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A candidate with deferred_failure publishes/reconciles without rebuilding and NEVER exits green."""
    # Case 1: Mid-upload failure during run with deferred_failure -> retry publishes AND still raises deferred failure.
    store = Store()
    _setup_r2_env(monkeypatch, store)
    counters: dict[str, int] = {}

    def fail_pointer(key: str) -> None:
        if key == pub.INDEX_V2_KEY:
            raise OSError("transient R2 pointer drop")

    store.before_put = fail_pointer
    with pytest.raises(OSError, match="transient R2 pointer drop"):
        _SimpleCounterRollup(
            output_dir=tmp_path / "case1",
            skip_upload=False,
            counters=counters,
            deferred=RuntimeError("senate 403 preserved prior rows"),
        ).run()
    assert counters["build"] == 1
    assert pub.INDEX_V2_KEY not in store.objects

    store.before_put = None
    with pytest.raises(RuntimeError, match="senate 403 preserved prior rows"):
        _SimpleCounterRollup(output_dir=tmp_path / "case1", skip_upload=False, resume=True, counters=counters).run()
    assert counters["build"] == 1, "must not rebuild"
    assert pub.INDEX_V2_KEY in store.objects, "healthy generation must be published before raising deferred_failure"

    # Case 2: Re-running resume AGAIN while already live reconciles with zero writes AND still raises deferred failure.
    writes_after_publish = list(store.writes)
    with pytest.raises(RuntimeError, match="senate 403 preserved prior rows"):
        _SimpleCounterRollup(output_dir=tmp_path / "case1", skip_upload=False, resume=True, counters=counters).run()
    assert counters["build"] == 1
    assert store.writes == writes_after_publish


# ---------------------------------------------------------------------------
# 5. Attempt-1 build failure fall-through vs explicit require_resume_candidate
# ---------------------------------------------------------------------------


def test_missing_candidate_falls_through_when_optional_and_refuses_when_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Optional resume falls through to fresh build; require_resume_candidate / SPICY_REGS_RESUME_REQUIRE fails closed."""
    store = Store()
    _setup_r2_env(monkeypatch, store)
    counters: dict[str, int] = {}

    # Required via flag -> refuses with zero builds.
    with pytest.raises(ResumeRefusedError, match="No retained generation candidate"):
        _SimpleCounterRollup(
            output_dir=tmp_path / "req-flag",
            skip_upload=False,
            resume=True,
            require_resume_candidate=True,
            counters=counters,
        ).run()
    assert counters.get("build", 0) == 0

    # Required via env vars -> refuses with zero builds.
    monkeypatch.setenv(RESUME_GENERATION_ENV, "true")
    monkeypatch.setenv(RESUME_REQUIRE_ENV, "true")
    with pytest.raises(ResumeRefusedError, match="No retained generation candidate"):
        _SimpleCounterRollup(output_dir=tmp_path / "req-env", skip_upload=False, counters=counters).run()
    assert counters.get("build", 0) == 0
    monkeypatch.delenv(RESUME_GENERATION_ENV, raising=False)
    monkeypatch.delenv(RESUME_REQUIRE_ENV, raising=False)

    # Optional resume (e.g. automatic github.run_attempt > 1 where Attempt 1 died during build) -> falls through cleanly.
    _SimpleCounterRollup(
        output_dir=tmp_path / "auto-fallthrough",
        skip_upload=False,
        resume=True,
        require_resume_candidate=False,
        counters=counters,
    ).run()
    assert counters["build"] == 1
    assert pub.INDEX_V2_KEY in store.objects


# ---------------------------------------------------------------------------
# 6. _rollup.yml staging step behavioral verification
# ---------------------------------------------------------------------------


def test_rollup_workflow_stage_script_preserves_invocation_and_refuses_scope_drift(tmp_path: Path) -> None:
    """Execute the actual staging Python snippet from _rollup.yml against synthetic restored artifacts."""
    reusable = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())
    stage_step = next(
        step
        for step in reusable["jobs"]["rollup"]["steps"]
        if step.get("name") == "Stage retained generation candidate into output"
    )
    script = stage_step["run"]

    def _run_stage(
        work: Path,
        *,
        prior_inputs: dict,
        current_inputs: dict,
        prior_rev: str = "abc123",
        current_rev: str = "abc123",
        include_candidate: bool = True,
        explicit: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
        work.mkdir(parents=True, exist_ok=True)
        out_dir = work / "output"
        out_dir.mkdir()
        current_inv = {
            "started_at": "2026-11-28T02:00:00Z",
            "code_revision": current_rev,
            "run_id": "200",
            "run_attempt": "2",
            "inputs": current_inputs,
        }
        (out_dir / "invocation.json").write_text(json.dumps(current_inv), encoding="utf-8")

        runner_temp = work / "runner_temp"
        restored_out = runner_temp / "restored-output"
        restored_aud = runner_temp / "restored-audit"
        restored_out.mkdir(parents=True)
        restored_aud.mkdir(parents=True)
        prior_inv = {
            "started_at": "2026-11-28T01:00:00Z",
            "code_revision": prior_rev,
            "run_id": "100",
            "run_attempt": "1",
            "inputs": prior_inputs,
        }
        (restored_aud / "invocation.json").write_text(json.dumps(prior_inv), encoding="utf-8")
        (restored_aud / "source-evidence" / "run1" / "artifact").mkdir(parents=True)
        (restored_aud / "source-evidence" / "run1" / "artifact" / "artifact.json").write_text("{}")
        if include_candidate:
            (restored_out / "resume-candidates" / "demo").mkdir(parents=True)
            (restored_out / "resume-candidates" / "demo" / ("a" * 64 + ".json")).write_text("{}")
            (restored_out / "generations" / ("a" * 64)).mkdir(parents=True)
            (restored_out / "generations" / ("a" * 64) / "artifact.json").write_text("{}")

        github_env = work / "github_env.txt"
        github_env.write_text("", encoding="utf-8")
        result = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", script],
            cwd=work,
            env={
                "PATH": os.environ["PATH"],
                "RUNNER_TEMP": str(runner_temp),
                "SOURCE_RUN_ID": "100",
                "SOURCE_ATTEMPT": "1",
                "OUTPUT_ARTIFACT_NAME": "rollup-output-run-rollup-demo-100-1",
                "AUDIT_ARTIFACT_NAME": "rollup-audit-run-rollup-demo-100-1",
                "EXPLICIT_RESUME": "true" if explicit else "false",
                "GITHUB_ENV": str(github_env),
            },
            capture_output=True,
            text=True,
            timeout=15,
        )
        return result, out_dir, github_env

    # 1. Matching scope copies candidate + evidence, preserves current invocation.json, and sets resume env vars.
    base_inputs = {"command": "run-rollup-demo", "bill_family_congresses": "118", "skip_upload": True}
    retry_inputs = {"command": "run-rollup-demo", "bill_family_congresses": "118", "skip_upload": False}
    res, out_ok, env_ok = _run_stage(
        tmp_path / "stage-ok", prior_inputs=base_inputs, current_inputs=retry_inputs, explicit=True
    )
    assert res.returncode == 0, res.stderr
    inv = json.loads((out_ok / "invocation.json").read_text())
    assert inv["run_id"] == "200" and inv["run_attempt"] == "2", "current invocation.json must not be clobbered"
    assert inv["resumed_from"]["run_id"] == "100"
    assert (out_ok / "resume-candidates" / "demo" / ("a" * 64 + ".json")).is_file()
    assert (out_ok / "generations" / ("a" * 64) / "artifact.json").is_file()
    assert (out_ok / "source-evidence" / "run1" / "artifact" / "artifact.json").is_file()
    env_lines = env_ok.read_text().splitlines()
    assert "SPICY_REGS_RESUME_GENERATION=true" in env_lines
    assert "SPICY_REGS_RESUME_REQUIRE=true" in env_lines

    # 2. Scope mismatch with explicit resume fails closed (exit non-zero).
    drifted_inputs = {"command": "run-rollup-demo", "bill_family_congresses": "119", "skip_upload": False}
    res_fail, out_fail, env_fail = _run_stage(
        tmp_path / "stage-drift", prior_inputs=base_inputs, current_inputs=drifted_inputs, explicit=True
    )
    assert res_fail.returncode != 0
    assert not (out_fail / "resume-candidates").exists()
    assert env_fail.read_text() == ""

    # 3. Automatic retry with missing candidate falls through cleanly without setting resume env.
    res_auto, out_auto, env_auto = _run_stage(
        tmp_path / "stage-auto-empty",
        prior_inputs=base_inputs,
        current_inputs=retry_inputs,
        include_candidate=False,
        explicit=False,
    )
    assert res_auto.returncode == 0
    assert env_auto.read_text() == ""
    assert not (out_auto / "resume-candidates").exists()
