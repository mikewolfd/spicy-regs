"""Hosted dispatch refuses a changed whole prior before starting a conversion."""
from copy import deepcopy
import json

import pytest

from scripts.native_conversion_invocation import entry_digest, record_invocation


def prior():
    return {"artifactDigest": "sha256:" + "a" * 64, "publishedAt": "2026-10-03T00:00:00Z",
            "tables": {"dockets.parquet": {"rows": 3, "byteSize": 40, "sha256": "b" * 64}}}


def run(tmp_path, selected, **overrides):
    args = {"family": "dockets", "expected_entry": entry_digest(prior()), "publish": False,
            "source_run": "", "bucket": "", "output": tmp_path / "invocation",
            "public_url": "https://public.example", "read_index": lambda _: {"families": {"dockets": selected}}}
    args.update(overrides)
    return record_invocation(**args)


def test_preparation_records_full_entry_and_has_no_publication_target(tmp_path):
    result = run(tmp_path, prior())
    assert result["entry"] == prior()
    assert not result["publicationRequested"] and result["expectedBucket"] is None
    assert json.loads((tmp_path / "invocation/invocation.json").read_text()) == result


@pytest.mark.parametrize("change", ["timestamp", "rows", "member", "native", "missing"])
def test_changed_full_entry_refuses_without_writing_invocation(tmp_path, change):
    entry = deepcopy(prior())
    if change == "timestamp":
        entry["publishedAt"] = "2026-10-04T00:00:00Z"
    elif change == "rows":
        entry["tables"]["dockets.parquet"]["rows"] = 4
    elif change == "member":
        entry["tables"]["dockets.parquet"]["sha256"] = "c" * 64
    elif change == "native":
        entry["etlReceipts"] = {"rows": 3}
    else:
        entry = None
    with pytest.raises(ValueError, match="prior entry changed"):
        run(tmp_path, entry)
    assert not (tmp_path / "invocation").exists()


@pytest.mark.parametrize("overrides", [
    {"family": "../../other"}, {"expected_entry": "a" * 64}, {"publish": True},
    {"source_run": "123"}, {"publish": True, "bucket": "named", "source_run": "../123"},
    {"public_url": "http://public.example"},
])
def test_invalid_dispatch_refuses_before_any_public_metadata_read(tmp_path, overrides):
    def unexpected(_):
        pytest.fail("Invalid dispatch must refuse before reading published data")
    with pytest.raises(ValueError):
        run(tmp_path, prior(), read_index=unexpected, **overrides)


def test_explicit_prior_run_is_recorded_without_rebuilding(tmp_path):
    result = run(tmp_path, prior(), publish=True, bucket="explicit-production", source_run="12345")
    assert result["sourceRunId"] == "12345"
    assert result["publicationRequested"] and result["expectedBucket"] == "explicit-production"


def test_already_native_family_refuses_even_with_its_matching_digest(tmp_path):
    entry = prior()
    entry["etlReceipts"] = {"rows": 3}
    with pytest.raises(ValueError, match="already native"):
        run(tmp_path, entry, expected_entry=entry_digest(entry))
    assert not (tmp_path / "invocation").exists()
