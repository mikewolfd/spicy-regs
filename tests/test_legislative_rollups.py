"""Exercise the owned rollup entry point and its selected receipt prior."""

import pyarrow.parquet as pq

from spicy_regs.pipelines.rollups.laws import LawsRollup


def test_laws_rollup_runs_native_generation_with_verified_local_prior(tmp_path, monkeypatch):
    from .test_laws import StubListingReader, StubUslm, StubOlrc, LISTED_119
    from spicy_regs.generations import verify_generation
    from spicy_regs.legislative_receipts import policy
    from spicy_regs.pipelines.rollups import laws
    from spicy_regs.transforms.build_laws import build_laws

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("LEGISLATIVE_PRIOR_BUNDLE", raising=False)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    calls = []

    def builder(directory, **kwargs):
        uslm = StubUslm(unavailable={110, 109, 104})
        calls.append(uslm)
        return build_laws(directory, reader=StubListingReader({119: LISTED_119}), uslm=uslm, olrc=StubOlrc(), **kwargs)

    monkeypatch.setattr(laws, "build_laws", builder)
    first = tmp_path / "first"
    LawsRollup(output_dir=first).run()
    [generation] = (first / "generations").iterdir()
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["etlReceipts"]["generationId"]
    # What the read of each law's PLAW established is published beside the row; which reader made it is not.
    published = pq.read_table(first / "laws.parquet")
    assert published.schema.equals(policy("laws").subject_schema)
    assert "uslm_reader_version" not in published.schema.names
    laws_read = {law["law_id"]: law for law in published.to_pylist()}
    assert (laws_read["119-public-1"]["uslm_outcome"], laws_read["119-public-1"]["uslm_reason"]) == ("captured", None)
    assert laws_read["119-public-1"]["law_text_outcome"] == "parsed"
    assert {(laws_read[f"119-public-{n}"]["uslm_outcome"], laws_read[f"119-public-{n}"]["uslm_reason"])
            for n in (104, 109, 110)} == {("unavailable", "source_unavailable")}
    LawsRollup(output_dir=first).run()
    assert 1 in [s.number for s in calls[0].selections]
    assert 1 not in [s.number for s in calls[1].selections]
