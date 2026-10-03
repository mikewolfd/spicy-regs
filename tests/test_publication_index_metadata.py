"""Optional publication time must not weaken immutable table-index validation."""

import json

import pytest

from spicy_regs.sources import publication as pub


def index(version=2):
    digest = "a" * 64
    return {
        "format": "spicy-regs-publication",
        "version": version,
        "families": {
            "members": {
                "prefix": "generations/members/" + digest,
                "logicalId": "urn:test:members",
                "artifactDigest": "sha256:" + digest,
                "tables": {
                    "members.parquet": {
                        "sha256": "sha256:" + "b" * 64,
                        "byteSize": 1,
                        "rows": 1,
                        "columns": [["bioguide_id", "VARCHAR"]],
                    }
                },
            }
        },
    }


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize(
    "timestamp",
    ["2026-10-03T11:17:53Z", "2026-10-03T11:17:53.123456Z", "2026-10-03T07:17:53-04:00", "2026-10-03T11:17:53+00:00"],
)
def test_timezone_publication_time_survives_parsing_and_v1_derivation(version, timestamp):
    value = index(version)
    value["families"]["members"]["publishedAt"] = timestamp
    parsed = pub.parse_index(json.dumps(value).encode())
    assert parsed == value
    assert pub.derive_v1(parsed)["families"]["members"]["publishedAt"] == timestamp


def test_publication_time_remains_optional():
    value = index()
    assert pub.parse_index(json.dumps(value).encode()) == value


@pytest.mark.parametrize(
    "timestamp",
    [
        None,
        True,
        0,
        {},
        [],
        "",
        "2026-10-03",
        "2026-10-03T11:17:53",
        "2026-02-30T11:17:53Z",
        "2026-10-03T25:17:53Z",
        "2026-10-03T11:17:53+25:00",
        "2026-10-03T11:17:53+00:99",
        "2026-10-03T11:17:53-00:60",
        "2026-10-03T11:17:53Z ",
        "2026-10-03 11:17:53Z",
        "20261003T111753Z",
    ],
)
def test_publication_time_requires_calendar_timestamp_and_timezone(timestamp):
    value = index()
    value["families"]["members"]["publishedAt"] = timestamp
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        pub.parse_index(json.dumps(value).encode())


@pytest.mark.parametrize("mutation", ["missing_prefix", "unknown_field", "bad_artifact_digest", "bad_table_digest"])
def test_optional_time_does_not_weaken_family_or_digest_checks(mutation):
    value = index()
    family = value["families"]["members"]
    family["publishedAt"] = "2026-10-03T11:17:53Z"
    if mutation == "missing_prefix":
        del family["prefix"]
    elif mutation == "unknown_field":
        family["unexpected"] = "metadata"
    elif mutation == "bad_artifact_digest":
        family["artifactDigest"] = "sha256:bad"
    else:
        family["tables"]["members.parquet"]["sha256"] = "sha256:bad"
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        pub.parse_index(json.dumps(value).encode())


def test_duplicate_publication_time_is_refused():
    value = index()
    value["families"]["members"]["publishedAt"] = "2026-10-03T11:17:53Z"
    raw = json.dumps(value).replace('"publishedAt": ', '"publishedAt": "2026-10-02T11:17:53Z", "publishedAt": ')
    with pytest.raises(pub.PublicationError):
        pub.parse_index(raw.encode())
