"""A live pointer is unchanged only when all publishable rollup inputs agree."""

import hashlib
import json
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.base import RollupPipeline, Unchanged
from spicy_regs.sources import publication, r2
from tests.generation_fakes import Store


class _ObservedRollup(RollupPipeline):
    name: ClassVar[str] = "observed"
    output: ClassVar[str] = "observed.parquet"
    retain_source_evidence: ClassVar[bool] = True
    version = "v1"
    refused = False
    early = False

    def build(self, output_dir):
        assert self.source_evidence is not None
        self.source_evidence.event("publisher-revision", version=self.version)
        if self.refused:
            self.source_evidence.refusal(ValueError("publisher replied 403"), stage="north-source")
        if self.early:
            raise Unchanged("publisher has no new table data")
        out = output_dir / self.output
        pq.write_table(pa.table({"id": ["same"]}), out)
        return out


@pytest.fixture
def live(monkeypatch):
    store = Store()
    for key in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"):
        monkeypatch.setenv(key, "test")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://r2.example.test")
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN", raising=False)
    monkeypatch.delenv("CLOUDFLARE_ZONE_ID", raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)

    def read(url, *, allow_missing, **_):
        raw = store.objects.get(url.removeprefix("https://r2.example.test/"))
        if raw is None and not allow_missing:
            raise AssertionError(f"Missing fixture object: {url}")
        return raw

    def fetch(_url, member, path, _label):
        raw = store.objects[member.path]
        assert member.sha256 == "sha256:" + hashlib.sha256(raw).hexdigest()
        path.write_bytes(raw)
        return len(raw)

    monkeypatch.setattr(publication, "_bounded_get", read)
    monkeypatch.setattr(publication, "fetch_member", fetch)
    return store


def _family(store):
    return publication.parse_index(store.objects[publication.INDEX_V2_KEY])["families"]["observed"]


@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("change", [None, "provenance", "refusal", "selection"])
def test_skip_only_exact_rollup_delta(live, tmp_path, monkeypatch, early, change):
    _ObservedRollup(output_dir=tmp_path / "first", skip_upload=False).run()
    baseline = _family(live)
    live.writes.clear()
    live.sent.clear()

    rollup = _ObservedRollup(output_dir=tmp_path / "again", skip_upload=False)
    rollup.early = early
    if change == "provenance":
        rollup.version = "v2"
    elif change == "refusal":
        rollup.refused = True
    elif change == "selection":
        monkeypatch.setenv("COMMITTEE_REPORTS_SINCE", "2026-10-01")
    rollup.run()
    current = _family(live)
    assert current["tables"] == baseline["tables"]

    if change is None:
        assert current == baseline
        assert live.writes == [] and sum(live.sent.values()) == 0
        assert not list((tmp_path / "again").glob("resume-candidates/*/*.json"))
        assert not list((tmp_path / "again").glob("generations/*"))
    else:
        assert current["artifactDigest"] != baseline["artifactDigest"]
        assert live.writes[-1] == publication.INDEX_V2_KEY
        table = current["prefix"] + "/observed.parquet"
        assert table in live.copies and live.sent[table] == 0

    (journal,) = (tmp_path / "again").glob("source-evidence/*/artifact/journal.jsonl")
    records = [json.loads(line) for line in journal.read_bytes().splitlines()]
    assert any(row["event"] == "publisher-revision" and row["version"] == rollup.version for row in records)
    assert any(row["event"] == "refusal" for row in records) == (change == "refusal")
    if early and change is not None:
        assert any(row["event"] == "rollup-carried-evidence" for row in records)


@pytest.mark.parametrize("early", [False, True])
def test_refusal_removal_after_successful_rebuild_is_a_delta(live, tmp_path, early):
    first = _ObservedRollup(output_dir=tmp_path / "first", skip_upload=False)
    first.refused = True
    first.run()
    prior = _family(live)
    again = _ObservedRollup(output_dir=tmp_path / "again", skip_upload=False)
    again.early = early
    again.run()
    assert _family(live)["artifactDigest"] != prior["artifactDigest"]


def test_refused_evidence_read_does_not_become_zero_delta(live, tmp_path):
    _ObservedRollup(output_dir=tmp_path / "first", skip_upload=False).run()
    prior = _family(live)
    (journal,) = [
        key for key in live.objects
        if key.startswith(publication.EVIDENCE_PREFIX + "/") and key.endswith("/journal.jsonl")
    ]
    live.objects[journal] += b"corruption"
    with pytest.raises(publication.PublicationError, match="differs from its declared digest"):
        _ObservedRollup(output_dir=tmp_path / "again", skip_upload=False).run()
    assert _family(live) == prior


def test_log_zero_delta_for_skip_upload(live, tmp_path, capsys):
    from loguru import logger

    _ObservedRollup(output_dir=tmp_path / "first", skip_upload=False).run()
    prior = _family(live)
    live.writes.clear()
    sink = logger.add(lambda msg: print(msg, end=""))
    try:
        _ObservedRollup(output_dir=tmp_path / "again", skip_upload=True).run()
    finally:
        logger.remove(sink)
    out = capsys.readouterr().out
    assert f'{prior["tables"]["observed.parquet"]["byteSize"]} table bytes not staged' in out
    assert not live.writes


def test_main_early_signal_does_not_swallow_deferred_failure(live, tmp_path, monkeypatch):
    _ObservedRollup(output_dir=tmp_path / "first", skip_upload=False).run()
    prior = _family(live)
    original = _ObservedRollup.build

    def build(self, output_dir: Path):
        self.deferred_failure = RuntimeError("required publisher refused")
        return original(self, output_dir)

    monkeypatch.setattr(_ObservedRollup, "build", build)
    rollup = _ObservedRollup(output_dir=tmp_path / "again", skip_upload=False)
    rollup.early = True
    with pytest.raises(RuntimeError, match="required publisher refused"):
        rollup.run()
    assert _family(live)["artifactDigest"] != prior["artifactDigest"]
    assert list((tmp_path / "again").glob("resume-candidates/*/*.json"))


def test_scheduled_ofac_rollup_bounded_dry_run(live, tmp_path, monkeypatch, capsys):
    """Replay the active CLI against a pinned publish and fixed publisher responses."""
    from time import monotonic

    import httpx
    from loguru import logger

    from spicy_regs.pipelines.rollups import base, source_refreshes
    from tests.test_ofac import export_zip

    archives = {"sdn": export_zip(), "consolidated": export_zip("consolidated")}

    def serve(request):
        filename = request.url.path.rsplit("/", 1)[-1]
        if request.url.host == "sanctionslistservice.ofac.treas.gov":
            return httpx.Response(302, headers={
                "location": "https://wc2h-sls-prod-public-published.s3.us-gov-west-1.amazonaws.com/Published/a/"
                + filename,
            })
        body = archives["sdn" if "SDN" in filename else "consolidated"]
        return httpx.Response(200, content=body, headers={"content-type": "application/zip"})

    monkeypatch.setattr(source_refreshes.OfacSanctionsRollup, "transport", httpx.MockTransport(serve))
    monkeypatch.setattr(base, "load_dotenv", lambda: None)
    source_refreshes.OfacSanctionsRollup(output_dir=tmp_path / "seed", skip_upload=False).run()
    before = live.objects[publication.INDEX_V2_KEY]
    tables = publication.parse_index(before)["families"]["ofac-sanctions"]["tables"]
    live.writes.clear()
    live.sent.clear()
    output = tmp_path / "dry"
    sink = logger.add(lambda msg: print(msg, end=""))
    started = monotonic()
    try:
        with pytest.raises(SystemExit) as completed:
            source_refreshes.ofac_sanctions_app(["--skip-upload", "--output-dir", str(output)])
        assert completed.value.code == 0
    finally:
        logger.remove(sink)
    elapsed = monotonic() - started
    table_bytes = sum(entry["byteSize"] for entry in tables.values())
    diagnostic = capsys.readouterr().out
    assert "zero delta; skipped generation publish" in diagnostic
    assert f"{table_bytes} table bytes not staged" in diagnostic
    assert not list(output.glob("generations/*"))
    assert live.objects[publication.INDEX_V2_KEY] == before
    assert not live.writes and sum(live.sent.values()) == 0
    print(f"OFAC fixture dry-run: {table_bytes} table bytes not staged; 0 upload bytes; {elapsed:.3f}s")


def test_active_receipt_retry_identity_is_not_dropped(live, tmp_path):
    from tests.test_resumable_rollup_publication import _TwoTableEvidenceReceiptRollup

    def run(name, receipt_id):
        rollup = _TwoTableEvidenceReceiptRollup(output_dir=tmp_path / name, skip_upload=False, counters={})
        rollup.receipt_generation_id = receipt_id
        rollup.run()
        return publication.parse_index(live.objects[publication.INDEX_V2_KEY])["families"]["resume-demo"]

    prior = run("baseline", "run-one")
    live.writes.clear()
    assert run("same-receipt", "run-one") == prior
    assert not live.writes
    newer = run("new-receipt", "run-two")
    assert newer["tables"] == prior["tables"]
    assert newer["etlReceipts"]["generationId"] == "run-two"
    assert newer["etlReceipts"]["sha256"] != prior["etlReceipts"]["sha256"]
    assert live.writes[-1] == publication.INDEX_V2_KEY
