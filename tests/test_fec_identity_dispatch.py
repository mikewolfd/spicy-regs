import pytest
from spicy_regs.transforms.fec_identity_dispatch import prepare_identity_job, map_identity_record, IdentityJob


def test_dispatch_refuses_unselected_collection_and_unknown_mapping():
    with pytest.raises(ValueError, match="outside"):
        map_identity_record({"collection_id": "other"}, IdentityJob("registry", "selected", None))
    with pytest.raises(ValueError, match="Unsupported"):
        prepare_identity_job("guess", {}, source_generation_pin="sha256:" + "a" * 64)


def test_dispatch_calls_maintained_mapper_with_prepared_selection(monkeypatch):
    from spicy_regs.transforms import fec_committee_observations

    source = {"collection_id": "x"}
    selection = object()

    def mapper(row, prepared):
        assert row is source
        assert prepared is selection
        return {"fec_committee_observations": [{"record_id": "kept"}]}, [{"role": "primary"}, {"role": "primary"}]

    monkeypatch.setattr(fec_committee_observations, "map_committee_api", mapper)
    tables, evidence = map_identity_record(source, IdentityJob("committee_api", "x", selection))
    assert tables["fec_committee_observations"][0]["record_id"] == "kept"
    assert len(evidence) == 2


def test_committee_dispatch_prepares_the_selected_document_scope():
    from spicy_docs.sources.fec.document_profile import document_scope
    from spicy_regs.transforms.fec_committee_observations import CommitteeSelection

    pin = "sha256:" + "a" * 64
    capture = dict(requestUrl="https://api.open.fec.gov/v1/committees/?sort=committee_id&per_page=100&cycle=2024",
                   observedAt="2026-10-11T12:00:00Z", responseSha256=pin, byteSize=128, representation="opaque")
    entry = dict(collection_id="x", profile="document", scope=document_scope(capture, format="api-json", api_mode="page"))
    job = prepare_identity_job("committee_api", entry, source_generation_pin=pin)
    assert isinstance(job.prepared, CommitteeSelection)
    assert job.prepared.selection.source_cycle is None and job.prepared.request_url == capture["requestUrl"]
    entry["source_cycle"] = 2024
    with pytest.raises(ValueError, match="NULL source_cycle"):
        prepare_identity_job("committee_api", entry, source_generation_pin=pin)


def test_registry_dispatch_keeps_its_independent_cycle_metadata():
    pin = "sha256:" + "a" * 64
    entry = dict(collection_id="x", source_cycle=2024, scope={"capture": {"responseSha256": pin}})
    job = prepare_identity_job("registry", entry, source_generation_pin=pin)
    assert job.prepared.source_cycle == 2024
