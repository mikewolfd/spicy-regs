"""Conditional FEC selection with checked proof objects and retained query bytes."""

from copy import deepcopy
from types import SimpleNamespace
import sys

import pytest
from rulespec_artifacts import LocalMemberSource, canonical_json_bytes, iter_member_descriptors

from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_generation_publication import build, publish


@pytest.fixture
def release(tmp_path, monkeypatch):
    store = Store()
    old, _ = build(tmp_path, "old", family="fec-observations", keys=("a.parquet",))
    query, _ = build(tmp_path, "query", family="fec-query", keys=("b.parquet",))
    prior = publish(store, query, publish(store, old))
    new, artifact = build(tmp_path, "new", family="fec-observations", keys=("a.parquet",), value="new")
    checks = []
    observations = SimpleNamespace(
        root=artifact.root, pin=artifact.pin,
        members=tuple(iter_member_descriptors(artifact, LocalMemberSource(new))),
        proof={"priorPin": {k: prior["families"]["fec-observations"][k] for k in ("logicalId", "artifactDigest")}},
        check=lambda: checks.append("observations"),
    )
    compatibility = SimpleNamespace(entry=deepcopy(prior["families"]["fec-query"]),
                                    observations_pin=artifact.pin, check=lambda: checks.append("query"))
    obs_basis, query_basis = {"path": "obs-proof", "sha256": "obs"}, {"path": "query-proof", "sha256": "query"}

    def verify_obs(directory, basis):
        assert directory == new and basis == obs_basis
        return observations

    def verify_query(obs, entry, basis):
        assert obs is observations and entry == prior["families"]["fec-query"] and basis == query_basis
        return compatibility

    module = SimpleNamespace(verify_publication_basis=verify_obs, verify_retained_query_compatibility=verify_query)
    monkeypatch.setitem(sys.modules, "spicy_regs.fec_release_verification", module)
    monkeypatch.setitem(sys.modules, "spicy_regs.fec_query_compatibility", module)
    # The composed path must not return to the old local full receipt validator.
    monkeypatch.setattr("spicy_regs.generations.verify_generation", lambda *a, **k: pytest.fail("old validator"))
    store.writes.clear()
    store.reads.clear()
    store.sent.clear()

    def run():
        return pub.publish_fec_release(new, observations_basis=obs_basis, query_basis=query_basis,
                                       client=store, bucket="test", prior_index=prior)

    return SimpleNamespace(store=store, prior=prior, observations=observations,
                           compatibility=compatibility, module=module, checks=checks, run=run)


def test_one_conditional_selection_reuses_exact_query_without_body_reads(release):
    r = release
    selected = r.run()
    assert r.store.writes.count(pub.INDEX_V2_KEY) == 1
    assert selected["families"]["fec-query"] == r.prior["families"]["fec-query"]
    assert selected["families"]["fec-observations"]["artifactDigest"] == r.observations.pin.artifact_digest
    prefix = r.prior["families"]["fec-query"]["prefix"] + "/"
    assert not any(k.startswith(prefix) for k in r.store.writes + list(r.store.reads))
    assert r.checks == ["observations", "query", "observations", "query"]


@pytest.mark.parametrize("failure", ["a.parquet", "members.json", "artifact.json"])
def test_failed_staging_keeps_exact_old_pair(release, failure):
    r = release
    before = r.store.objects[pub.INDEX_V2_KEY]

    def interrupt(key):
        if key.endswith(failure):
            raise OSError("interrupted")

    r.store.before_put = interrupt
    with pytest.raises(OSError, match="interrupted"):
        r.run()
    assert r.store.objects[pub.INDEX_V2_KEY] == before


def test_bound_input_changed_after_staging_refuses_pointer(release):
    r = release
    before = r.store.objects[pub.INDEX_V2_KEY]
    count = 0

    def changed():
        nonlocal count
        count += 1
        if count == 2:
            raise ValueError("bound compatibility input changed")

    r.compatibility.check = changed
    with pytest.raises(ValueError, match="input changed"):
        r.run()
    assert r.store.objects[pub.INDEX_V2_KEY] == before


@pytest.mark.parametrize("field", ["priorPin", "entry", "observations_pin"])
def test_mismatched_checked_selection_refuses_uploads(release, field):
    r = release
    if field == "priorPin":
        r.observations.proof[field] = {}
    else:
        setattr(r.compatibility, field, None)
    with pytest.raises(pub.PublicationError, match="another"):
        r.run()
    assert not r.store.writes


@pytest.mark.parametrize("family", ["fec-observations", "fec-query"])
def test_concurrent_pair_change_refuses_overwrite(release, family):
    r = release
    raced = deepcopy(r.prior)
    raced["families"][family]["publishedAt"] = "2026-10-16T00:00:00Z"
    raw = canonical_json_bytes(raced)

    def race(key):
        if key == pub.INDEX_V2_KEY:
            r.store.before_put = None
            r.store.objects[key] = raw

    r.store.before_put = race
    with pytest.raises(pub.PublicationError, match="Family changed"):
        r.run()
    assert r.store.objects[pub.INDEX_V2_KEY] == raw


def test_unrelated_concurrent_family_is_preserved(release):
    r = release
    raced = deepcopy(r.prior)
    other = deepcopy(raced["families"]["fec-query"])
    other["prefix"] = other["prefix"].replace("/fec-query/", "/other/")
    other["tables"] = {"c.parquet": other["tables"]["b.parquet"]}
    raced["families"]["other"] = other

    def race(key):
        if key == pub.INDEX_V2_KEY:
            r.store.before_put = None
            r.store.objects[key] = canonical_json_bytes(raced)

    r.store.before_put = race
    selected = r.run()
    assert selected["families"]["other"] == raced["families"]["other"]
    assert selected["families"]["fec-query"] == r.prior["families"]["fec-query"]


def test_corrupt_observation_upload_never_moves_pair(release):
    from rulespec_artifacts import ArtifactVerificationError

    r = release
    before = r.store.objects[pub.INDEX_V2_KEY]
    r.store.corrupt_key = "a.parquet"
    with pytest.raises(ArtifactVerificationError, match="digest"):
        r.run()
    assert r.store.objects[pub.INDEX_V2_KEY] == before


def test_rejected_compatibility_basis_never_uploads(release):
    def reject(*args):
        raise ValueError("incomplete serving compatibility proof")

    release.module.verify_retained_query_compatibility = reject
    with pytest.raises(ValueError, match="incomplete"):
        release.run()
    assert not release.store.writes
