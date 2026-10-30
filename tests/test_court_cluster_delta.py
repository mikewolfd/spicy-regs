"""A bounded additive search precedes all full-prior processing."""

from datetime import date
import hashlib
import importlib
import json

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import LocalMemberSource, canonical_json_bytes, iter_member_descriptors
from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget, search_url

from spicy_regs.court_receipts import build_court_generation, local_receipt_selection, write_court_rows
from spicy_regs.generations import verify_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import courtlistener, publication

delta = importlib.import_module("spicy_regs.transforms.catch_up_court_opinion_clusters")


def digest(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def selected(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICY_REGS_DISK_FLOOR_GIB", "0")
    monkeypatch.setattr(courtlistener, "MIN_REQUEST_INTERVAL_SECONDS", 0)
    witness = {
        "source_id": "original",
        "source_uri": None,
        "sha256": "sha256:" + "a" * 64,
        "locator": None,
        "body_version": None,
    }
    rows = [
        {
            "cluster_id": "2",
            "case_name": "prior literal \\u001f",
            "attorneys": [None, "", "one"],
            "date_created": "2026-06-30T08:00:00Z",
            "ingest_source": "bulk",
        },
        {"cluster_id": "1", "case_name": "older", "date_filed": "2025-01-01"},
        {"cluster_id": "3", "citation_count": "unsupported"},
    ]
    subject = write_court_rows(
        delta.DATASET, rows, tmp_path / "source", witnesses=[witness], generation_id="original-generation"
    )
    prior = tmp_path / "prior"
    artifact = build_court_generation(prior, family=delta.FAMILY, files=[subject])
    members = iter_member_descriptors(artifact, LocalMemberSource(prior))
    snapshot = publication.empty_index()
    snapshot["families"][delta.FAMILY] = publication.generation_entry(artifact, members)
    work = tmp_path / "work"
    evidence = CaptureEvidence(work, delta.FAMILY)
    evidence.inherit_pinned_root(snapshot, canonical_json_bytes(artifact.root))
    edition = tmp_path / "edition.json"
    edition.write_text(json.dumps({"source_edition": "2026-06-30", "prior": artifact.pin.artifact_digest}))
    courts = tmp_path / "courts.csv"
    courts.write_text("id,jurisdiction\nca9,F\n")
    # Only the already maintained parser is replaced here; this fixture's tiny
    # courts bytes remain pinned, and the caller must select their edition.
    monkeypatch.setattr(delta, "court_jurisdictions", lambda **kwargs: {"ca9": "F"})
    kwargs = dict(
        prior_directory=prior,
        expected_prior=artifact.pin.artifact_digest,
        source_edition=date(2026, 6, 30),
        source_edition_evidence=edition,
        source_edition_sha256=digest(edition),
        courts_local_file=courts,
        courts_sha256=digest(courts),
        search_through=6,
        search_budget=CourtListenerTraversalBudget(2, 0),
        evidence=evidence,
    )
    return work, kwargs


def transport(monkeypatch, *pages):
    queued = iter(pages)
    calls = []

    def handle(request):
        calls.append(request)
        response = next(queued)
        return httpx.Response(
            response if isinstance(response, int) else 200,
            json=response if isinstance(response, dict) else {"detail": "refused"},
        )

    # SpicyDocs 0.66.6 refuses keyless live search; the catch-up requires the token before any work.
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "fixture-token")
    monkeypatch.setattr(
        delta,
        "CourtListenerOpinionSearchReader",
        lambda **kwargs: courtlistener.CourtListenerOpinionSearchReader(
            transport=httpx.MockTransport(handle), **kwargs
        ),
    )
    return calls


def page(ids, *, total=None, next_url=None):
    return {
        "count": len(ids) if total is None else total,
        "next": next_url,
        "previous": None,
        "results": [{"cluster_id": identity, "court_id": "ca9", "caseName": "new", "attorney": []} for identity in ids],
    }


def test_disjoint_append_preserves_prior_values_receipts_and_ordinary_admission(selected, monkeypatch):
    work, kwargs = selected
    prior = kwargs["prior_directory"]
    original = {name: (prior / name).read_bytes() for name in (delta.OUTPUT, "etl_receipts.parquet")}
    old_rows = pq.read_table(prior / delta.OUTPUT).to_pylist()
    old_receipts = pq.read_table(prior / "etl_receipts.parquet").to_pylist()
    calls = transport(monkeypatch, page([6, 4]))  # Search order is filing date, not cluster ID.
    monkeypatch.setattr(
        "spicy_regs.court_receipts.restore_processing_input",
        lambda *args, **kwargs: pytest.fail("Native append must not restore the prior"),
    )
    result = delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert pq.read_table(result).to_pylist()[: len(old_rows)] == old_rows
    assert pq.read_table(result)["cluster_id"].to_pylist() == ["2", "1", "6", "4"]
    receipts, _ = local_receipt_selection(result)
    assert pq.read_table(receipts).to_pylist()[: len(old_receipts)] == old_receipts
    assert all((prior / name).read_bytes() == value for name, value in original.items())
    assert calls[0].url.params["q"] == "cluster_id:[3 TO 6]"
    assert not (work / delta.OUTPUT).exists()  # No public local selection before admission.
    generation = work / "generation"
    build_court_generation(
        generation,
        family=delta.FAMILY,
        files=[result],
        read_snapshot=kwargs["evidence"].read_snapshot,
        inputs=kwargs["evidence"].inputs(),
    )
    assert verify_generation(generation).root["spec"]["tables"][delta.OUTPUT]["rows"] == 4


@pytest.mark.parametrize("failure", ["http", "budget", "range"])
def test_incomplete_search_refuses_before_conversion_or_prior_copy(selected, monkeypatch, failure):
    work, kwargs = selected
    kwargs["search_budget"] = CourtListenerTraversalBudget(1, 0)
    next_url = search_url(kind="o", q="cluster_id:[3 TO 6]") + "&cursor=next"  # a continuation keeps its selection
    calls = transport(
        monkeypatch,
        429 if failure == "http" else page([7]) if failure == "range" else page([4], total=2, next_url=next_url),
    )
    for name in ("finish_court_output", "_append_native"):
        monkeypatch.setattr(delta, name, lambda *args, **kwargs: pytest.fail("Source must complete first"))
    with pytest.raises((ValueError, httpx.HTTPError)):
        delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert len(calls) == 1
    assert not list(work.glob(".clusters-delta-*.parquet"))
    events = [json.loads(line) for line in (kwargs["evidence"].artifact_dir / "journal.jsonl").read_text().splitlines()]
    assert any(event["event"] == "refusal" for event in events)


@pytest.mark.parametrize("pin", ["expected_prior", "source_edition_sha256", "courts_sha256"])
def test_wrong_input_pins_refuse_before_search(selected, monkeypatch, pin):
    work, kwargs = selected
    kwargs[pin] = "sha256:" + "0" * 64
    calls = transport(monkeypatch, page([4]))
    with pytest.raises(ValueError, match="differs|differ"):
        delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert calls == []


def test_overlap_is_refused_without_replacing_prior(selected, tmp_path):
    _, kwargs = selected
    prior = kwargs["prior_directory"] / delta.OUTPUT
    original = prior.read_bytes()
    fresh = tmp_path / "overlap.parquet"
    pq.write_table(pq.read_table(prior).slice(0, 1), fresh)
    with pytest.raises(ValueError, match="overlaps"):
        delta._append_native(prior, fresh, tmp_path / "combined.parquet", guard=lambda: None)
    assert prior.read_bytes() == original


@pytest.mark.parametrize("member", ["subject", "receipts", "edition"])
def test_input_mutation_during_search_refuses_before_conversion(selected, monkeypatch, member):
    work, kwargs = selected
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "fixture-token")
    target = {
        "subject": kwargs["prior_directory"] / delta.OUTPUT,
        "receipts": kwargs["prior_directory"] / "etl_receipts.parquet",
        "edition": kwargs["source_edition_evidence"],
    }[member]

    def changed(self):
        yield {"cluster_id": 4}
        target.write_bytes(target.read_bytes() + b"changed")

    monkeypatch.setattr(delta.CourtListenerOpinionSearchReader, "iter_records", changed)
    monkeypatch.setattr(
        delta, "finish_court_output", lambda *args, **kwargs: pytest.fail("Changed input must refuse before conversion")
    )
    with pytest.raises(ValueError, match="changed"):
        delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert not list(work.glob(".clusters-delta-*.parquet"))


def test_catch_up_requires_token_before_reading_its_prior(selected, monkeypatch):
    """SpicyDocs 0.66.6 refuses keyless live search; the weekly catch-up says so before any work."""
    work, kwargs = selected
    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)
    monkeypatch.setattr(delta.publication, "table_owner", lambda *_: pytest.fail("prior read before the token check"))
    with pytest.raises(courtlistener.CourtListenerError, match="requires COURTLISTENER_API_TOKEN"):
        delta.catch_up_court_opinion_clusters(work, **kwargs)


def test_bulk_build_requires_token_before_reading_unless_search_is_skipped(tmp_path, monkeypatch):
    build = importlib.import_module("spicy_regs.transforms.build_court_opinion_clusters")
    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)
    monkeypatch.setattr(build.r2, "download", lambda *_: pytest.fail("prior read before the token check"))
    with pytest.raises(courtlistener.CourtListenerError, match="requires COURTLISTENER_API_TOKEN"):
        build.build_court_opinion_clusters(tmp_path)
