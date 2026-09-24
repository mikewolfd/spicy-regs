"""Reviewed exclusions preserve source meaning and refuse unreviewed changes."""

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

import pytest

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.transforms import write_staging
from spicy_regs.transforms.reviewed_comments import ExcludeReviewedComments


FIXTURES = Path(__file__).parent / "fixtures/reviewed_comments"


def test_exact_source_observations_are_excluded_and_evidence_hashes_match():
    transform = ExcludeReviewedComments()
    payloads = []
    for identity, decision in transform.decisions.items():
        body = (FIXTURES / f"{identity}.json").read_bytes()
        assert sha256(body).hexdigest() == decision["source_sha256"]
        assert decision["reason"] and decision["source"] and decision["evidence"]
        payloads.append(json.loads(body))
    assert list(transform.apply(payloads)) == []
    # A byte-formatting change is not a new source observation.
    assert list(transform.apply(json.loads(json.dumps(payloads, indent=4, sort_keys=True)))) == []


def test_changed_observation_requires_review_and_test_keyword_does_not_filter():
    payload = json.loads((FIXTURES / "CFTC-2026-0595-0003.json").read_bytes())
    transform = ExcludeReviewedComments()
    changed = deepcopy(payload)
    changed["data"]["attributes"]["comment"] = "A substantive new comment"
    with pytest.raises(ValueError, match="changed; recheck its exclusion"):
        list(transform.apply([changed]))
    unrelated = deepcopy(payload)
    unrelated["data"]["id"] = "CFTC-2026-9999-0001"
    assert list(transform.apply([unrelated])) == [unrelated]


def test_direct_catalog_entry_refuses_invalid_staging_before_connecting(tmp_path, monkeypatch):
    row = {**dict.fromkeys(COMMENT.schema), "comment_id": "unreviewed", "agency_code": "CFTC", "docket_id": "../unsafe"}
    write_staging("CFTC", "comments", [row], tmp_path / "stage", COMMENT.schema)

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid staged comments reached the persistent catalog")

    monkeypatch.setattr(iceberg, "_connect_for_table", forbidden)
    with pytest.raises(ValueError, match="invalid coordinates"):
        iceberg.merge_comments(tmp_path / "stage", tmp_path / "output", COMMENT)
