"""A prepared publication reuses exact admitted bytes and repeats proof, never the source writer."""

import json
from pathlib import Path

import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication
from tests.test_native_conversion import (
    BASE, BUCKET, CFR, COMMON, DOCKET, MAIN, STATE, WHEEL,
    bucket as _bucket,
    convert, publish_old, publish_split_sections, stored,
)

bucket = _bucket  # Expose the existing hermetic fixture to pytest in this module.


def publish_prepared(path, family="cfr-sections", **kwargs):
    return conversion.publish_prepared(path, allowed=[family], expected_main=MAIN, expected_spicy_docs=WHEEL,
                                       expect_bucket=BUCKET, state=lambda _: dict(STATE), **kwargs)


@pytest.mark.parametrize("family", ["cfr-sections", "courtlistener", "bill-family"])
def test_prepared_publication_reuses_exact_generation_without_running_writer(tmp_path, monkeypatch, bucket, family):
    if family == "bill-family":
        old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    else:
        tables = {"cfr_sections": CFR} if family == "cfr-sections" else {"court_dockets": [DOCKET]}
        old = publish_old(bucket, monkeypatch, tmp_path, family, tables)
    work = tmp_path / "work"
    dry = convert(family, work)
    generation = Path(dry["generation"]["directory"])
    original = {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError("Prepared publication must never run the source writer")

    monkeypatch.setattr(conversion, "_convert_rollup", no_writer)
    monkeypatch.setattr(conversion, "_convert_court", no_writer)
    done = publish_prepared(work / conversion.RECEIPT, family)
    assert done["generation"] == dry["generation"]
    assert done["tables"] == dry["tables"]
    assert done["published"]["entry"]["artifactDigest"] == dry["generation"]["artifactDigest"]
    assert {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()} == original
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"][family] == old


@pytest.mark.parametrize("damage", [
    "unsealed", "refused", "published", "attempted", "source", "proof", "resealed-proof",
    "snapshot", "retained", "subject", "artifact", "missing-artifact",
])
def test_prepared_changes_or_incomplete_proofs_refuse_before_upload(tmp_path, monkeypatch, bucket, damage):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    receipt = convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    if damage in {"unsealed", "refused", "published", "attempted", "source", "proof", "resealed-proof"}:
        if damage == "unsealed":
            receipt.pop("prepared")
        elif damage == "refused":
            receipt["refused"] = []
        elif damage == "published":
            receipt["published"] = {}
        elif damage == "attempted":
            receipt["publish_attempt"] = {}
        elif damage == "source":
            receipt["source"]["checkout"] = "1a" * 20
        else:
            receipt["tables"]["cfr_sections"]["rows_only_in_retained"] = 7
        if damage not in {"unsealed", "proof"}:
            receipt["prepared"]["receiptDigest"] = conversion._prepared_digest(receipt)
        path.write_text(json.dumps(receipt))
    else:
        generation = Path(receipt["generation"]["directory"])
        target = {"snapshot": work / "captured-publication.v2.json", "retained": work / "retained/cfr_sections.parquet",
                  "subject": generation / "cfr_sections.parquet", "artifact": generation / "artifact.json",
                  "missing-artifact": generation / "artifact.json"}[damage]
        if damage == "missing-artifact":
            target.unlink()
        else:
            target.write_bytes(target.read_bytes() + b"changed")
    before = list(bucket.writes)
    with pytest.raises((conversion.ConversionRefused, ValueError)):
        publish_prepared(path)
    assert bucket.writes == before and stored(bucket) == old


def test_prepared_allow_runtime_and_target_remain_explicit(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    before = list(bucket.writes)
    for changes in ({"allowed": []}, {"expect_bucket": "other"}, {"state": lambda _: STATE | {"uncommitted": ["changed"]}},
                    {"state": lambda _: STATE | {"main": "ff" * 20}}, {"state": lambda _: STATE | {"spicy_docs": "other"}}):
        options: dict = dict(allowed=["cfr-sections"], expect_bucket=BUCKET, state=lambda _: dict(STATE)) | changes
        with pytest.raises(conversion.ConversionRefused):
            conversion.publish_prepared(path, expected_main=MAIN, expected_spicy_docs=WHEEL, **options)
    assert bucket.writes == before


@pytest.mark.parametrize("when", ["before-upload", "pointer-retry"])
def test_prepared_exact_prior_includes_timestamp_on_every_pointer_attempt(tmp_path, monkeypatch, bucket, when):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    changed = old | {"publishedAt": "2001-01-01T00:00:00Z"}

    def change(key):
        if key == publication.INDEX_V2_KEY:
            index = publication.parse_index(bucket.objects[key])
            index["families"]["cfr-sections"] = changed
            bucket.objects[key] = canonical_json_bytes(index)

    before = list(bucket.writes)
    if when == "before-upload":
        change(publication.INDEX_V2_KEY)
    else:
        bucket.before_put = change
    with pytest.raises(conversion.ConversionRefused):
        publish_prepared(work / conversion.RECEIPT)
    assert stored(bucket) == changed
    if when == "before-upload":
        assert bucket.writes == before


def test_prepared_cli_and_second_publication_refusal(tmp_path, monkeypatch, bucket, capsys):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda _: dict(STATE))
    work = tmp_path / "work"
    convert("cfr-sections", work)
    args = ["--publish-prepared", str(work / conversion.RECEIPT), *COMMON, "--expect-bucket", BUCKET]
    assert conversion.main(args) == 0
    assert "published and read back" in capsys.readouterr().out
    before = list(bucket.writes)
    assert conversion.main(args) == 1
    assert bucket.writes == before


def test_prepared_native_admission_is_repeated_instead_of_trusting_saved_success(tmp_path, monkeypatch, bucket):
    from spicy_regs import etl_receipts

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)

    def refuse(*args, **kwargs):
        raise ValueError("native admission refused this selected bundle")

    monkeypatch.setattr(etl_receipts, "validate_receipt_bundle", refuse)
    before = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="native admission refused"):
        publish_prepared(work / conversion.RECEIPT)
    assert bucket.writes == before and stored(bucket) == old


def test_prepared_rechecks_code_after_qualification_before_upload(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    states = iter([dict(STATE), STATE | {"main": "be" * 20}])
    before = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="Main or the pinned wheel moved"):
        conversion.publish_prepared(work / conversion.RECEIPT, allowed=["cfr-sections"], expected_main=MAIN,
                                    expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: next(states))
    assert bucket.writes == before


def test_prepared_pointer_retry_preserves_another_familys_new_entry(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    other = publish_old(bucket, monkeypatch, tmp_path / "sibling", "nominations", {"nominations": [{"citation": "PN1-119"}]})
    changed = other | {"artifactDigest": "sha256:" + "cf" * 32, "prefix": "generations/nominations/" + "cf" * 32}
    work = tmp_path / "work"
    convert("cfr-sections", work)
    raced = []

    def change(key):
        if key == publication.INDEX_V2_KEY and not raced:
            raced.append(True)
            index = publication.parse_index(bucket.objects[key])
            index["families"]["nominations"] = changed
            bucket.objects[key] = canonical_json_bytes(index)

    bucket.before_put = change
    done = publish_prepared(work / conversion.RECEIPT)
    assert raced and done["published"]["entry"]["artifactDigest"] == done["generation"]["artifactDigest"]
    assert publication.current_index(BASE)["families"]["nominations"] == changed
