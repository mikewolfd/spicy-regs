"""Preparation retains opaque observation IDs and checks all scopes before ETL."""

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from spicy_regs.scorecards.extraction_replay import page_bytes
from tests.test_scorecard_extraction_replay import page

from spicy_regs.scorecards.operations import prepare as PREPARE

ROOT = Path(__file__).resolve().parents[1]


def test_prepare_retains_canonical_pdf_id_and_hash_only_evidence(tmp_path, monkeypatch):
    import json
    from hashlib import sha256

    plan = dict(schema_version="1", entries=[dict(adapter="lcv", edition=dict(publisher_id="lcv"))])
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    index = tmp_path / "index.json"
    index.write_text(json.dumps(PREPARE.publication.empty_index()))
    edition = SimpleNamespace(scorecard_id="lcv:test")
    fake = SimpleNamespace(
        entries={"lcv:test": dict(edition_object=edition)}, reference_publishers={}, complete=lambda: None
    )
    captured = {}

    def construct(root, entries, *, retain_observations, **limits):
        captured["retain"] = retain_observations
        return fake

    def build(directory, *, evidence, fetch_factory, validate_acquisitions, **options):
        scope = evidence.for_source("lcv", "hash_only", parser_version="test/1", policy_decision_id="test")
        with fetch_factory(scope):
            document = SimpleNamespace(capture_id="original-capture", body=b"%PDF-original")
            receipt = captured["retain"](document, page_bytes(page()), stage="semantic-test")
            assert str(UUID(receipt["observation_id"])) == receipt["observation_id"]
            assert UUID(receipt["observation_id"]).version == 4
        validate_acquisitions()
        # Stop after the relevant boundary; no generation or publication is requested.
        raise RuntimeError("test completed private retention")

    monkeypatch.setattr(PREPARE, "RetainedScorecardBatch", construct)
    monkeypatch.setattr(PREPARE, "build_scorecards", build)
    monkeypatch.setattr(PREPARE, "installed_provider", lambda: SimpleNamespace(get_adapter=lambda name: None))
    args = SimpleNamespace(
        output=tmp_path / "candidate",
        private_observations=tmp_path / "private",
        plan=plan_path,
        plan_sha256=sha256(plan_path.read_bytes()).hexdigest(),
        index=index,
        corpus=tmp_path,
        public_url="https://data.example",
    )
    with pytest.raises(RuntimeError, match="test completed"):
        PREPARE.prepare(args)
    receipts = json.loads((args.private_observations / "receipts.json").read_text())
    assert receipts[0]["observation_capture_id"] == UUID(receipts[0]["observation_id"]).hex
    raw = b"\n".join(path.read_bytes() for path in args.output.rglob("*") if path.is_file())
    assert b"retained-image" not in raw
