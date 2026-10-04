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
