"""Complete roster preparation binds published lineage and exact consumed inputs."""

from copy import deepcopy
from hashlib import sha256
from pathlib import Path
import json

import httpx
import pyarrow.parquet as pq
import pytest

from scripts import prepare_members_native as preparation
from spicy_docs.sources.legislators import LegislatorsBudget
from spicy_regs.generations import verify_generation
from spicy_regs.sources import publication
from spicy_regs.sources.member_rosters import ReviewedMemberRosters, SELECTION
from tests.generation_fakes import Store
from tests.test_member_rosters import capture


@pytest.mark.parametrize("failure", ["missing_url", "missing_prior", "different_prior"])
def test_preparation_refuses_missing_or_different_published_prior_before_build(tmp_path, monkeypatch, failure):
    expected = "sha256:" + "a" * 64
    index = publication.empty_index()
    if failure == "missing_url":
        monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    else:
        monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
        if failure == "different_prior":
            index["families"]["members"] = {"artifactDigest": "sha256:" + "b" * 64}
    monkeypatch.setattr(publication, "load_index", lambda _: index)
    work = tmp_path / "work"
    with pytest.raises(ValueError, match="published prior|Published member prior"):
        preparation.prepare(work, expected_prior=expected)
    assert not work.exists()


def test_complete_preparation_restores_all_tables_and_retains_captured_prior(tmp_path, monkeypatch):
    keys = ("members.parquet", "member_terms.parquet", "member_party_affiliations.parquet")
    bodies = {"current": capture(), "historical": capture().replace(b"A000001", b"B000002").replace(b"S001", b"S002")}
    supplied = json.loads(SELECTION.read_bytes())
    for name, member in supplied.items():
        member.update(sha256=sha256(bodies[name]).hexdigest(), bytes=len(bodies[name]))
    consumed = deepcopy(supplied)
    requests = []

    def respond(request):
        requests.append(str(request.url))
        roster = "current" if str(request.url).endswith("-current.json") else "historical"
        supplied[roster]["sha256"] = "0" * 64  # caller mutation cannot change consumed pins or summary
        return httpx.Response(200, stream=httpx.ByteStream(bodies[roster]), headers={"content-type": "application/json"})

    def initial_response(request):
        roster = "current" if str(request.url).endswith("-current.json") else "historical"
        return httpx.Response(200, stream=httpx.ByteStream(bodies[roster]), headers={"content-type": "application/json"})

    initial_owner = ReviewedMemberRosters(
        budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), selection=consumed,
        transport=httpx.MockTransport(initial_response),
    )
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    initial = preparation.CompleteRosterMembers(roster_owner=initial_owner, output_dir=tmp_path / "old", skip_upload=True)
    with initial_owner:
        initial.run()
    (old,) = (tmp_path / "old" / "generations").iterdir()
    artifact = verify_generation(old)
    assert initial.source_evidence is not None
    prior = publication.publish_generation(
        old, client=Store(), bucket="test", prior_index=publication.empty_index(),
        evidence_directories=(initial.source_evidence.artifact_dir,),
    )
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    observations = []

    def load_index(url):
        observations.append(url)
        return prior

    monkeypatch.setattr(publication, "load_index", load_index)
    monkeypatch.setattr(publication, "load_family_root", lambda url, entry: ((old / "artifact.json").read_bytes(), artifact.root))

    owner = ReviewedMemberRosters(
        budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), selection=supplied,
        transport=httpx.MockTransport(respond),
    )
    monkeypatch.setattr(preparation, "ReviewedMemberRosters", lambda **_: owner)
    result = preparation.prepare(tmp_path / "work", expected_prior=artifact.pin.artifact_digest)
    candidate = verify_generation(Path(result["generation"]))
    assert observations == ["https://test.invalid"]
    assert requests == [consumed[name]["url"] for name in ("current", "historical")]
    assert result["sourceRosterSelection"] == consumed
    assert result["capturedPrior"] == prior
    assert not result["published"]
    assert any(item["role"] == "prior-generation" and item["artifactDigest"] == artifact.pin.artifact_digest
               for item in candidate.root["inputs"])
    restored = result["restoredProcessingInputs"]
    assert set(restored) == {key.removesuffix(".parquet") for key in keys}
    members = pq.read_table(restored["members"]).to_pylist()
    assert {row["bioguide_id"] for row in members} == {"A000001", "B000002"}
    assert all(row["name_nickname"] == "  Literal  " for row in members)
    assert pq.read_table(restored["member_terms"]).num_rows == 2
    assert pq.read_table(restored["member_party_affiliations"]).num_rows == 0
