"""Exercise the owned rollup entry point and its selected receipt prior."""

import hashlib
import json
import shutil

import pyarrow.parquet as pq
import pytest

from spicy_regs.legislative_receipts import migrate_outputs, restore_prior
from spicy_regs.legislative_rollups import _selected_prior, family_policies
from spicy_regs.pipelines.rollups.laws import LawsRollup

from .test_legislative_receipts import retained, row


def test_laws_rollup_runs_native_generation_with_verified_local_prior(tmp_path, monkeypatch):
    from .test_laws import StubListingReader, StubUslm, StubOlrc, LISTED_119
    from spicy_regs import data_dictionary
    from spicy_regs.native_types import described_schema
    from spicy_regs.generations import verify_generation
    from spicy_regs.pipelines.rollups import laws
    from spicy_regs.transforms.build_laws import build_laws

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("LEGISLATIVE_PRIOR_BUNDLE", raising=False)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    calls = []

    def builder(directory, **kwargs):
        uslm = StubUslm(unavailable={110, 109, 104})
        calls.append(uslm)
        return build_laws(directory, reader=StubListingReader({119: LISTED_119}),
                          uslm=uslm, olrc=StubOlrc(), **kwargs)

    monkeypatch.setattr(laws, "build_laws", builder)
    # Global dictionary regeneration is integration-owned. Supply its exact
    # required native schemas here, keeping generation and receipt checks real.
    monkeypatch.setattr(data_dictionary, "expected_schemas", lambda: {
        p.dataset: described_schema(p.subject_schema)
        for p in LawsRollup.receipt_policies if not p.receipt_only
    })
    first = tmp_path / "first"
    LawsRollup(output_dir=first).run()
    [generation] = (first / "generations").iterdir()
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["etlReceipts"]["generationId"]
    assert "uslm_outcome" not in pq.read_schema(first / "laws.parquet").names
    monkeypatch.setenv("LEGISLATIVE_PRIOR_BUNDLE", str(first / ".legislative-bundle"))
    LawsRollup(output_dir=tmp_path / "second").run()
    assert 1 in [s.number for s in calls[0].selections]
    assert 1 not in [s.number for s in calls[1].selections]


def test_selected_pinned_generation_restores_processing_only_reads(tmp_path, monkeypatch):
    from spicy_regs.sources import publication

    source = retained(tmp_path / "source", "committee_report_reads", [
        row("committee_report_reads", package_id="CRPT-119hrpt1", outcome="complete", rule_version="r1")
    ])
    migrate_outputs([source], tmp_path / "bundle", generation_id="prior")
    receipt = tmp_path / "bundle/etl_receipts.parquet"
    policies = family_policies("committee_report_reads")

    class Pipeline:
        name = "committee-reports"
        publication_family = None
        receipt_policies = policies

    index = publication.empty_index()
    index["families"][Pipeline.name] = {
        "prefix": "generations/committee-reports/" + "a" * 64, "tables": {},
        "etlReceipts": {"key": receipt.name, "sha256": "sha256:" + hashlib.sha256(receipt.read_bytes()).hexdigest(),
                        "byteSize": receipt.stat().st_size, "rows": pq.read_metadata(receipt).num_rows,
                        "generationId": "prior", "datasets": [p.dataset for p in policies]},
    }
    monkeypatch.delenv("LEGISLATIVE_PRIOR_BUNDLE", raising=False)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.invalid")
    monkeypatch.setattr(publication, "current_index", lambda _: index)
    fetched = []

    def fetch(base, member, target, label):
        assert member.sha256 == index["families"][Pipeline.name]["etlReceipts"]["sha256"]
        fetched.append(member.key)
        shutil.copyfile(receipt, target)
        return True

    monkeypatch.setattr(publication, "fetch_member", fetch)
    selected = _selected_prior(Pipeline(), tmp_path / "selected")
    assert selected is not None
    assert fetched == ["etl_receipts.parquet"]
    assert json.loads((selected / "legislative-bundle.json").read_text())["generation_id"] == "prior"
    restored = restore_prior(selected, tmp_path / "restored")
    assert pq.read_table(restored["committee_report_reads"]).equals(pq.read_table(source))
    del index["families"][Pipeline.name]["etlReceipts"]
    with pytest.raises(ValueError, match="no ETL receipts"):
        _selected_prior(Pipeline(), tmp_path / "rejected")
    monkeypatch.setenv("LEGISLATIVE_PRIOR_BUNDLE", str(tmp_path / "bundle"))
    with pytest.raises(ValueError, match="differs from the complete owned family"):
        _selected_prior(LawsRollup(), tmp_path / "wrong-family")
