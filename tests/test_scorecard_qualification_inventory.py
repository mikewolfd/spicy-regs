"""Coverage metadata requires pinned complete source facts, including reader renditions."""

from dataclasses import asdict
from hashlib import sha256
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from spicy_docs.sources.scorecards import ScorecardEdition


from spicy_regs.scorecards.operations import inventory as integrations, qualifications

def inputs(tmp_path, monkeypatch, *, complete=True, counts=None):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    directory = tmp_path / "metadata"
    directory.mkdir()
    (corpus / "reader.py").write_bytes(b"SOURCE_READER = True\n")
    (corpus / "observations.json").write_bytes(b'{"reviewed": true}')
    (corpus / "captures.json").write_bytes(b'[{"sha256": "retained-private-body"}]')
    edition = ScorecardEdition("lcv", "synthetic", "https://source.example", "Source edition")
    reference = {
        "scorecard_snapshots": [
            dict(
                scorecard_id=edition.scorecard_id,
                completeness_status="complete" if complete else "incomplete",
                completeness_rule="Complete source-defined roster reconciled",
                parser_version="semantic/2",
                evidence_policy="hash_only",
            )
        ],
        "scorecard_member_ratings": [{"value_text": "N/A"}],
    }
    (corpus / "reference.json").write_text(json.dumps(reference))
    (corpus / "qualification.json").write_text(
        json.dumps(dict(status="qualified", counts=counts or {k: len(v) for k, v in reference.items()}))
    )

    class SourceReader:
        parser_version = "semantic/2"

        def __init__(self, qualified_observations, retain_observations):
            raise AssertionError("Coverage preflight must not execute the reader")

    adapter = SimpleNamespace(__file__=str(corpus / "reader.py"), parser_version="native/1", SourceReader=SourceReader)
    monkeypatch.setattr(qualifications, "get_adapter", lambda name: adapter)

    def pin(name):
        return sha256((corpus / name).read_bytes()).hexdigest()

    entry = dict(
        adapter="lcv",
        edition=asdict(edition),
        reader_class="SourceReader",
        reader_sha256=pin("reader.py"),
        observations_file="observations.json",
        observations_sha256=pin("observations.json"),
        captures_root=".",
        manifest_sha256=pin("captures.json"),
        reference_file="reference.json",
        reference_sha256=pin("reference.json"),
        qualification_file="qualification.json",
        qualification_sha256=pin("qualification.json"),
    )
    plan = corpus / "plan.json"
    plan.write_text(json.dumps(dict(entries=[entry])))
    return plan, pin("plan.json"), corpus, directory


def test_qualified_semantic_rendition_uses_its_class_version_and_preserves_private_boundary(tmp_path, monkeypatch):
    args = inputs(tmp_path, monkeypatch)
    ledger = qualifications.build(*args, observed_at="2026-10-04T04:00:00Z")
    receipt = json.loads((args[-1] / ledger["editions"][0]["receipt"]).read_bytes())
    assert receipt["parser_version"] == "semantic/2"
    assert receipt["published"] is False
    assert receipt["counts"]["scorecard_member_ratings"] == 1
    assert "N/A" not in json.dumps(ledger)
    assert "retained-private-body" not in json.dumps(receipt)
    assert qualifications.build(*args, observed_at="2026-10-04T04:00:00Z") == ledger


@pytest.mark.parametrize(
    "changes,error",
    [({"complete": False}, "complete reader scope"), ({"counts": {"scorecard_member_ratings": 0}}, "table counts")],
)
def test_incomplete_or_unreconciled_source_cannot_advance_coverage(tmp_path, monkeypatch, changes, error):
    args = inputs(tmp_path, monkeypatch, **changes)
    with pytest.raises(ValueError, match=error):
        qualifications.build(*args, observed_at="2026-10-04T04:00:00Z")
    assert not (args[-1] / "integration_qualifications.json").exists()


def test_changed_private_semantic_asset_cannot_advance_coverage(tmp_path, monkeypatch):
    args = inputs(tmp_path, monkeypatch)
    (args[2] / "observations.json").write_bytes(b'{"reviewed": false}')
    with pytest.raises(ValueError, match="pin"):
        qualifications.build(*args, observed_at="2026-10-04T04:00:00Z")


@pytest.mark.parametrize("legacy_keys", [False, True])
def test_required_reader_assets_must_bind_before_coverage_advances(tmp_path, monkeypatch, legacy_keys):
    args = inputs(tmp_path, monkeypatch)
    adapter = qualifications.get_adapter("lcv")

    class SourceReader:
        parser_version = "semantic/2"

        def __init__(self, qualified_observations, retain_observations):
            raise AssertionError("Coverage preflight must not execute the reader")

    adapter.SourceReader = SourceReader
    document = json.loads(args[0].read_bytes())
    entry = document["entries"][0]
    for field in ("observations_file", "observations_sha256"):
        value = entry.pop(field)
        if legacy_keys:
            entry["qualified_" + field] = value
    args[0].write_text(json.dumps(document))
    with pytest.raises(ValueError, match="reader inputs"):
        qualifications.build(args[0], sha256(args[0].read_bytes()).hexdigest(), *args[2:],
                             observed_at="2026-10-04T04:00:00Z")
    assert not (args[-1] / "integration_qualifications.json").exists()


def test_named_assets_must_match_the_selected_reader_parameters(tmp_path, monkeypatch):
    args = inputs(tmp_path, monkeypatch)
    document = json.loads(args[0].read_bytes())
    entry = document["entries"][0]
    entry["reader_inputs"] = {"wrong_asset_name": {
        "file": entry.pop("observations_file"), "sha256": entry.pop("observations_sha256")}}
    args[0].write_text(json.dumps(document))
    with pytest.raises(ValueError, match="reader inputs"):
        qualifications.build(args[0], sha256(args[0].read_bytes()).hexdigest(), *args[2:],
                             observed_at="2026-10-04T04:00:00Z")
    assert not (args[-1] / "integration_qualifications.json").exists()


@pytest.mark.parametrize("limit", [1, 1024])
def test_qualification_uses_and_records_explicit_reference_bound(tmp_path, monkeypatch, limit):
    args = inputs(tmp_path, monkeypatch)
    plan = json.loads(args[0].read_bytes())
    plan["entries"][0]["reference_max_bytes"] = limit
    args[0].write_text(json.dumps(plan))
    pin = sha256(args[0].read_bytes()).hexdigest()
    if limit == 1:
        with pytest.raises(ValueError, match="bound"):
            qualifications.build(args[0], pin, *args[2:], observed_at="2026-10-04T04:00:00Z")
        assert not (args[-1] / "integration_qualifications.json").exists()
    else:
        ledger = qualifications.build(args[0], pin, *args[2:], observed_at="2026-10-04T04:00:00Z")
        receipt = json.loads((args[-1] / ledger["editions"][0]["receipt"]).read_bytes())
        assert receipt["reference_max_bytes"] == limit


def test_publication_observation_merges_by_exact_edition_without_duplicates(tmp_path, monkeypatch):
    args = inputs(tmp_path, monkeypatch)
    initial = qualifications.build(*args, observed_at="2026-10-04T04:00:00Z")
    published_record = {**initial["editions"][0], "state": "published", "receipt": "retained-publication.json"}
    published = tmp_path / "publications.json"
    published.write_text(json.dumps(dict(editions=[published_record], readers={"lcv": "lcv"})))
    merged = qualifications.build(*args, observed_at="2026-10-04T04:00:00Z", published_ledger=published)
    assert merged["editions"] == [published_record]
    published.write_text(json.dumps(dict(editions=[published_record, published_record], readers={"lcv": "lcv"})))
    with pytest.raises(ValueError, match="repeats a scope"):
        qualifications.build(*args, observed_at="2026-10-04T04:00:00Z", published_ledger=published)


@pytest.mark.parametrize("failure", ["later-source", "publication-ledger", "ledger-replace"])
def test_failed_requalification_keeps_prior_ledger_and_referenced_bytes(tmp_path, monkeypatch, failure):
    args = inputs(tmp_path, monkeypatch)
    initial = qualifications.build(*args, observed_at="2026-10-04T04:00:00Z")
    directory, corpus = args[-1], args[2]
    before_ledger = (directory / "integration_qualifications.json").read_bytes()
    before_receipts = {row["receipt"]: (directory / row["receipt"]).read_bytes() for row in initial["editions"]}
    plan = json.loads(args[0].read_bytes())
    reference = json.loads((corpus / "reference.json").read_bytes())
    reference["scorecard_snapshots"][0]["completeness_rule"] = "Updated complete roster reconciliation"
    (corpus / "reference.json").write_text(json.dumps(reference))
    plan["entries"][0]["reference_sha256"] = sha256((corpus / "reference.json").read_bytes()).hexdigest()
    options = {}
    if failure == "later-source":
        (corpus / "refused.json").write_text(json.dumps(dict(status="refused")))
        second = deepcopy(plan["entries"][0])
        second["edition"]["edition_id"] = "later"
        second.update(qualification_file="refused.json", qualification_sha256=sha256((corpus / "refused.json").read_bytes()).hexdigest())
        plan["entries"].append(second)
    elif failure == "publication-ledger":
        published = tmp_path / "publications.json"
        published.write_text(json.dumps(dict(editions=initial["editions"], readers=initial["readers"])))
        options["published_ledger"] = published
    else:
        from spicy_regs.scorecards.operations import results

        def refuse_replace(*args):
            raise OSError("Interrupted ledger replacement")

        monkeypatch.setattr(results.os, "replace", refuse_replace)
    args[0].write_text(json.dumps(plan))
    with pytest.raises((ValueError, OSError), match="Refused|unfinished|Interrupted"):
        qualifications.build(args[0], sha256(args[0].read_bytes()).hexdigest(), *args[2:],
                             observed_at="2026-10-08T04:00:00Z", **options)
    assert (directory / "integration_qualifications.json").read_bytes() == before_ledger
    for row in initial["editions"]:
        assert (directory / row["receipt"]).read_bytes() == before_receipts[row["receipt"]]
        assert integrations.read_receipt(directory, row)["qualified"] is True
    assert not list(directory.glob(".*.tmp"))


def test_successful_requalification_retains_old_immutable_evidence(tmp_path, monkeypatch):
    args = inputs(tmp_path, monkeypatch)
    initial = qualifications.build(*args, observed_at="2026-10-04T04:00:00Z")
    plan = json.loads(args[0].read_bytes())
    plan["entries"][0]["reference_max_bytes"] = 1024
    args[0].write_text(json.dumps(plan))
    current = qualifications.build(args[0], sha256(args[0].read_bytes()).hexdigest(), *args[2:],
                                   observed_at="2026-10-08T04:00:00Z")
    assert initial["editions"][0]["receipt"] != current["editions"][0]["receipt"]
    for row in (*initial["editions"], *current["editions"]):
        assert integrations.read_receipt(args[-1], row)["qualified"] is True


def publication_observation(tmp_path, *, readback_status="independent_public_readback_passed", rating_count=1):
    qualification = dict(parser_version="source/1", counts={"scorecard_member_ratings": 1})
    readback = dict(
        status=readback_status,
        source_generation="sha256:" + "a" * 64,
        ratings_by_scorecard={"lcv:2025": rating_count},
    )
    for name, value in (("qualification.json", qualification), ("readback.json", readback)):
        (tmp_path / name).write_text(json.dumps(value))
    receipt = dict(
        format_version="scorecard-integration-published-observation/1",
        publisher_id="lcv",
        scorecard_id="lcv:2025",
        parser_version="source/1",
        counts=qualification["counts"],
        source_qualification="qualification.json",
        source_qualification_sha256=sha256((tmp_path / "qualification.json").read_bytes()).hexdigest(),
        qualification_counts_path=["counts"],
        public_readback="readback.json",
        public_readback_sha256=sha256((tmp_path / "readback.json").read_bytes()).hexdigest(),
        readback_rating_path=["ratings_by_scorecard", "lcv:2025"],
        observed_source_generation=readback["source_generation"],
    )
    (tmp_path / "receipt.json").write_text(json.dumps(receipt))
    record = dict(
        publisher_id="lcv",
        scorecard_id="lcv:2025",
        state="published",
        receipt="receipt.json",
        receipt_sha256=sha256((tmp_path / "receipt.json").read_bytes()).hexdigest(),
    )
    return record


def test_recorded_publication_requires_qualified_counts_and_passed_readback(tmp_path):
    record = publication_observation(tmp_path)
    receipt = integrations.read_receipt(tmp_path, record)
    assert receipt["observed_source_generation"] == "sha256:" + "a" * 64
    (tmp_path / "readback.json").write_text("{}")
    with pytest.raises(ValueError, match="pin"):
        integrations.read_receipt(tmp_path, record)


@pytest.mark.parametrize("changes", [{"rating_count": 0}, {"readback_status": "refused"}])
def test_failed_or_unreconciled_readback_cannot_claim_publication(tmp_path, changes):
    record = publication_observation(tmp_path, **changes)
    with pytest.raises(ValueError, match="public readback"):
        integrations.read_receipt(tmp_path, record)
