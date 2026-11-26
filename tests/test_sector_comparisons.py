"""Source-local classifications cannot multiply money or hide unresolved coverage."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from spicy_regs.transforms.sector_comparisons import compare, issue_views

FIXTURE = json.loads((Path(__file__).parent / "fixtures/fec-research-profiles/sector-observations.json").read_text())


def run(data):
    return compare(data["fec"], data["lda"], mapping=data["mapping"], scope=data["scope"])


def totals(rows):
    result = {}
    for r in rows:
        key = (r["source_family"], r["measure"])
        result[key] = result.get(key, Decimal(0)) + Decimal(r["known_reported_amount"] or "0")
    return result


def test_two_real_source_comparisons_and_unclassified_denominators():
    rows = run(FIXTURE)["sector_financial_comparisons"]
    for code in ["62", "81"]:
        assert any(r["sector_code"] == code and r["source_family"] == "fec" and r["qualified_measure_profiles"] == 1 for r in rows)
        assert any(r["sector_code"] == code and r["source_family"] == "lda" and r["measure"] == "income" and r["known_reported_amount"] is not None for r in rows)
    assert all(r["population_profiles"] == 26 for r in rows if r["source_family"] == "lda")
    assert any(r["unclassified_population_profiles"] > 0 for r in rows)
    assert any(r["unresolved_measure_profiles"] > 0 for r in rows)
    assert any(r["known_reported_amount"] is None for r in rows)
    evidence = run(FIXTURE)["sector_classification_evidence"]
    assert any(r["classification_status"] == "unresolved_identity" and r["method"] == "not_used_for_money" for r in evidence)


def test_repeated_industry_labels_do_not_repeat_additive_money():
    original = run(FIXTURE)["sector_financial_comparisons"]
    data = deepcopy(FIXTURE)
    data["mapping"]["assignments"] *= 2
    repeated = run(data)["sector_financial_comparisons"]
    assert [(r["sector_code"], r["source_family"], r["measure"], r["known_reported_amount"]) for r in repeated] == [
        (r["sector_code"], r["source_family"], r["measure"], r["known_reported_amount"]) for r in original]


def test_mapping_change_moves_classification_without_changing_source_money():
    original = run(FIXTURE)["sector_financial_comparisons"]
    data = deepcopy(FIXTURE)
    data["mapping"]["version"] += "/changed"
    data["mapping"]["assignments"][2]["sector_codes"] = ["62", "81"]
    changed = run(data)["sector_financial_comparisons"]
    assert totals(original) == totals(changed)
    assert {r["record_id"] for r in original}.isdisjoint({r["record_id"] for r in changed})
    assert any(r["uncertain_population_profiles"] > 0 for r in changed)


@pytest.mark.parametrize("change", ["period", "population", "duplicate", "name_only", "issue_code", "evidence"])
def test_unsupported_comparisons_and_classifications_refuse(change):
    data = deepcopy(FIXTURE)
    if change == "period":
        data["lda"][0]["period_end"] = "2026-03-31"
    elif change == "population":
        data["fec"][0]["entity_kind"] = "candidate"
    elif change == "duplicate":
        data["lda"].append(data["lda"][0])
    elif change == "evidence":
        data["mapping"]["assignments"][0]["evidence"] = []
    else:
        data["mapping"]["assignments"][0]["method"] = change
    with pytest.raises(ValueError):
        run(data)


def test_multi_issue_views_preserve_whole_filing_amount_and_ignore_activity_multiplicity():
    original = issue_views(FIXTURE["lda"], FIXTURE["activities"], source_generation="sha256:" + "a" * 64)
    repeated = issue_views(FIXTURE["lda"], FIXTURE["activities"] * 3, source_generation="sha256:" + "a" * 64)
    assert original == repeated
    filings = {r["filing_uuid"] for r in original}
    assert any(sum(r["filing_uuid"] == uid for r in original) > 1 for uid in filings)
    assert all(r["amount_basis"] == "whole_selected_filing_not_allocated_by_issue" for r in original)


def test_same_client_number_under_another_registrant_does_not_inherit_industry():
    data = deepcopy(FIXTURE)
    row = next(r for r in data["lda"] if r["client_id"] == "64160")
    row["registrant_id"] = "99999"
    before = run(FIXTURE)["sector_financial_comparisons"]
    after = run(data)["sector_financial_comparisons"]
    assert totals(before) == totals(after)
    prior = next(r for r in before if r["sector_code"] == "62" and r["source_family"] == "lda" and r["measure"] == "income")
    current = next(r for r in after if r["sector_code"] == "62" and r["source_family"] == "lda" and r["measure"] == "income")
    assert current["group_profiles"] == prior["group_profiles"] - 1


def test_pipeline_streams_activities_beyond_64mib_cap_and_documents_explorer_tables(tmp_path, monkeypatch):
    from datetime import date
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_regs.explorer_metadata import build_bundle
    from spicy_regs.generations import build_generation
    from spicy_regs.pipelines import sector_comparisons as pipeline
    from spicy_regs.sources import publication

    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    store = CentralRawArchiveStore(raw)

    assert pipeline.MAX_INPUT_BYTES["lobbying_activities"] >= 279_727_677

    def write_gen(name, table_name, rows, schema=None):
        gen_dir = tmp_path / f"{name}-gen"
        src_dir = tmp_path / f"{name}-src"
        src_dir.mkdir(parents=True, exist_ok=True)
        path = src_dir / f"{table_name}.parquet"
        table = pa.Table.from_pylist(rows, schema=schema) if schema is not None else pa.Table.from_pylist(rows)
        pq.write_table(table, path)
        artifact = build_generation(
            gen_dir,
            family=name,
            files=[path],
            expected_keys=[path.name],
        )
        digest = "sha256:" + hashlib.file_digest((gen_dir / path.name).open("rb"), "sha256").hexdigest()
        return gen_dir, artifact.pin.artifact_digest, digest

    fec_rows = [
        {
            **{k: v for k, v in row.items() if k not in {"period_start", "period_end"}},
            "period_start": date.fromisoformat(row["period_start"]),
            "period_end": date.fromisoformat(row["period_end"]),
        }
        for row in FIXTURE["fec"]
    ]
    lda_rows = [
        {
            **{k: v for k, v in row.items() if k not in {"period_start", "period_end"}},
            "period_start": date.fromisoformat(row["period_start"]),
            "period_end": date.fromisoformat(row["period_end"]),
        }
        for row in FIXTURE["lda"]
    ]
    act_rows = [
        *FIXTURE["activities"],
        {"filing_uuid": "unrelated-not-in-selected-profiles", "general_issue_code": "TAX", "extra_ignored": "payload"},
    ]

    fec_dir, fec_gen, fec_hash = write_gen("fec-financial-profiles", "fec_financial_profiles", fec_rows)
    lda_dir, lda_gen, lda_hash = write_gen("lobbying-quarterly-profiles", "lobbying_quarterly_profiles", lda_rows)
    act_dir, act_gen, act_hash = write_gen("lobbying-filings", "lobbying_activities", act_rows)

    scope = deepcopy(FIXTURE["scope"])
    scope["fec"]["generation"] = fec_gen
    scope["fec"]["table_sha256"] = fec_hash
    scope["lda"]["generation"] = lda_gen
    scope["lda"]["table_sha256"] = lda_hash

    cap_bytes = b'{"source":"synthetic"}'
    cap_digest = "sha256:" + hashlib.sha256(cap_bytes).hexdigest()
    store.put_blob(cap_digest, len(cap_bytes), [cap_bytes])
    selection = {
        "scope": scope,
        "mapping": FIXTURE["mapping"],
        "classification_captures": [
            {
                "request_url": "https://example.org/mapping",
                "observed_at": "2026-11-25T00:00:00Z",
                "sha256": cap_digest,
                "byte_size": len(cap_bytes),
            }
        ],
        "inputs": {
            "fec": {
                "directory": str(fec_dir),
                "name": "fec_financial_profiles",
                "expected_generation": fec_gen,
                "expected_hash": fec_hash,
            },
            "lda": {
                "directory": str(lda_dir),
                "name": "lobbying_quarterly_profiles",
                "expected_generation": lda_gen,
                "expected_hash": lda_hash,
            },
            "lda_activities": {
                "directory": str(act_dir),
                "name": "lobbying_activities",
                "expected_generation": act_gen,
                "expected_hash": act_hash,
            },
        },
    }

    # Exercise multi-batch streaming over lobbying_activities with small batch_size.
    orig_iter = pipeline._iter_activities
    monkeypatch.setattr(pipeline, "_iter_activities", lambda *a, **kw: orig_iter(*a, batch_size=2, **kw))

    out = tmp_path / "sector-out"
    artifact, evidence = pipeline.build(selection, out)
    assert artifact.pin.artifact_digest and (evidence / "journal.jsonl").exists()
    issues = pq.read_table(out / "generation" / "lobbying_issue_financial_observations.parquet").to_pylist()
    assert all(row["filing_uuid"] != "unrelated-not-in-selected-profiles" for row in issues)
    assert len(issues) == len(issue_views(FIXTURE["lda"], FIXTURE["activities"], source_generation=lda_gen))

    # Explorer metadata recognizes all 3 sector-comparisons tables and their inputs.
    index = publication.empty_index()
    index["version"] = 2
    for fam, d in (
        ("fec-financial-profiles", fec_dir),
        ("lobbying-quarterly-profiles", lda_dir),
        ("lobbying-filings", act_dir),
        ("sector-comparisons", out / "generation"),
    ):
        adm = pipeline.verify_generation(d)
        index["families"][fam] = publication.generation_entry(
            adm, iter_member_descriptors(adm, LocalMemberSource(d))
        )
    bundle = build_bundle(index)
    for name in pipeline.SCHEMAS:
        assert bundle["tables"][name]["metadataStatus"] == bundle["tables"][name]["sourceStatus"] == "documented"
    assert bundle["tables"]["lobbying_issue_financial_observations"]["inputs"] == [
        "lobbying_quarterly_profiles",
        "lobbying_activities",
    ]

    from spicy_regs import local_publication

    assert "sector-comparisons" not in local_publication.LOCAL_FAMILIES
