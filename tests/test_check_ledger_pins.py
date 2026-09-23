"""Pins the ledger pin check: each row's qualified pin against the live index and rulemaking pointer."""

import json

import pytest

from scripts import check_ledger_pins as pins
from spicy_regs.sources import publication

SNAPSHOT = "snapshot_0e799850778e1c92bde682d0d0f500be"


def _family(name: str, head: str, tables: dict[str, int]) -> dict:
    digest = head + "0" * (64 - len(head))
    return {
        "prefix": f"generations/{name}/{digest}",
        "logicalId": f"urn:spicy-regs:{name}",
        "artifactDigest": f"sha256:{digest}",
        "tables": {
            key: {"sha256": "sha256:" + "1" * 64, "byteSize": 1, "rows": rows, "columns": [["id", "VARCHAR"]]}
            for key, rows in tables.items()
        },
    }


# Parsed by the real validator, so the fixture cannot drift from the index contract.
INDEX = publication.parse_index(
    json.dumps(
        {
            "format": "spicy-regs-publication",
            "version": 1,
            "families": {
                "amendments": _family("amendments", "c943cb37", {"amendments.parquet": 7095}),
                "nominations": _family("nominations", "8fb0a72e", {"nominations.parquet": 2204}),
                "court-citations": _family(
                    "court-citations", "f1e2e523", {"court_citations.parquet": 10, "court_citation_map.parquet": 20}
                ),
                "court-opinion-bodies": _family(
                    "court-opinion-bodies", "abcdef01", {"court_opinion_bodies.parquet": 3}
                ),
            },
        }
    ).encode()
)
POINTER = {
    "dataset": "rulemaking",
    "format_version": 2,
    "manifest_key": f"materialized/rulemaking/snapshots/{SNAPSHOT}/manifest.json",
    "snapshot_id": SNAPSHOT,
}
MANIFEST = {
    "snapshot_id": SNAPSHOT,
    "artifacts": {
        "rule_targets.parquet": {"rows": 517311, "visibility": "public"},
        "proceedings.parquet": {"rows": 515121, "visibility": "public"},
        "comment_periods.parquet": {"rows": 306582, "visibility": "public"},
        "_proceedings_state.parquet": {"rows": 1, "visibility": "internal"},
    },
}

LEDGER = """\
| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T08 | `run-rollup-amendments` | `amendments.parquet` | republished; qualified at `c943cb37…` (2026-09-23); every cell matches |
| T08 | `run-rollup-nominations` | `nominations.parquet` | qualified at `4a40b55e…` (2026-09-22); newer run unaudited |
| T13 | `run-rollup-court-citations` | `court_citations.parquet`, `court_citation_map.parquet`, `generations/<f>/` \
| qualified at `f1e2e523…` (2026-09-22) |
| T15 | `run-rollup-lifecycles` | `rulemaking_lifecycles.parquet` | qualified at `2f001194…` (2026-09-22) |
| T13 | `run-rollup-court-opinion-bodies` | `court_opinion_bodies.parquet` | withdrawn 2026-09-22 |
| T11 | `materialize-rulemaking` | `rule_targets.parquet`, `proceedings.parquet` \
| qualified at `snapshot_0e799850…` (2026-09-23) |
| T11 | `materialize-rulemaking` | `comment_periods.parquet` | qualified at `snapshot_12345678…` (2026-09-21) |
| T11 | `materialize-rulemaking` | `_proceedings_state.parquet` | qualified at `snapshot_0e799850…` (2026-09-23) |
| T08 | `run-rollup-treaties` | `treaties.parquet` | re-qualified at `7007ca03…`; live not re-qualified |
| T06 | `run-pipeline` | `dockets.parquet` | verified at table digest `27a2ed4a…` (2026-09-21; ETag `0a1b2c3d…`) |
| T06 | `run-pipeline` | `documents.parquet` | verified at table digest `7907ebe3…` (2026-09-22; ETag `0a1b2c3d…`) |
| T07 | `publish-comments-mirror.yml` | `comments.parquet` | verified at table digest `fca7afb7…` (2026-09-23) |
"""
OBJECTS = {"dockets.parquet": ("0a1b2c3d", 1000), "documents.parquet": ("ffffffff", 2000)}


def _results() -> dict[str, tuple[str, str]]:
    rows = pins.check(LEDGER, pins.rollup_pins(INDEX), pins.snapshot_pins(POINTER, MANIFEST), OBJECTS)
    return {detail.split()[1].rstrip(","): (status, detail) for status, detail in rows}


def test_each_row_is_classified_against_its_own_live_source():
    statuses = {table: status for table, (status, _) in _results().items()}
    assert statuses == {
        "amendments.parquet": "OK",
        "nominations.parquet": "DRIFT",
        "court_citations.parquet": "OK",
        "rulemaking_lifecycles.parquet": "NOT-LIVE",
        "court_opinion_bodies.parquet": "NO-PIN",
        "rule_targets.parquet": "OK",
        "comment_periods.parquet": "DRIFT",
        "_proceedings_state.parquet": "NOT-LIVE",  # internal artifacts are not live
        "treaties.parquet": "MALFORMED",  # a pin mention without the dated phrase is not NO-PIN
        "dockets.parquet": "OK",
        "documents.parquet": "DRIFT",  # the object was re-uploaded since its digest was verified
        "comments.parquet": "NO-PIN",  # a table digest without an ETag cannot be checked by HEAD
    }


def test_details_carry_both_pins_and_live_rows():
    results = _results()
    assert "qualified=4a40b55e (2026-09-22)  live=8fb0a72e  rows=2,204" in results["nominations.parquet"][1]
    multi = results["court_citations.parquet"][1]
    assert "court_citations.parquet, court_citation_map.parquet  " in multi  # the generations/<f>/ token is ignored
    assert "live=f1e2e523  rows=10/20" in multi
    assert "live=snapshot_0e799850  rows=517,311/515,121" in results["rule_targets.parquet"][1]
    assert "qualified=-  live=abcdef01  rows=3" in results["court_opinion_bodies.parquet"][1]
    assert "qualified=0a1b2c3d (2026-09-22)  live=ffffffff  bytes=2,000" in results["documents.parquet"][1]


def test_a_row_with_two_conforming_phrases_is_malformed():
    row = "| T08 | `x` | `amendments.parquet` | qualified at `c943cb37…` (2026-09-23); qualified at `c943cb37…` (2026-09-24) |"
    assert pins.check(row, pins.rollup_pins(INDEX), {})[0][0] == "MALFORMED"


def test_a_pointer_naming_another_manifest_refuses():
    with pytest.raises(publication.PublicationError):
        pins.snapshot_pins(POINTER, {**MANIFEST, "snapshot_id": "snapshot_" + "f" * 32})


def test_fetch_live_follows_the_pointer_to_its_manifest(monkeypatch):
    base = "https://data.example"
    documents = {f"{base}/{pins.SNAPSHOT_POINTER}": POINTER, f"{base}/{POINTER['manifest_key']}": MANIFEST}
    monkeypatch.setattr(publication, "load_index", lambda url: INDEX if url == base else pytest.fail(url))
    monkeypatch.setattr(pins, "_get_json", documents.get)
    heads = {f"{base}/dockets.parquet": ('"0a1b2c3d9e8f7a6b5c4d3e2f1a0b9c8d-303"', "1000")}
    monkeypatch.setattr(pins.httpx, "head", lambda url, **_: _Head(*heads[url]) if url in heads else _Head())
    rollups, snapshots, objects = pins.fetch_live(base, LEDGER)
    assert rollups["court_citation_map.parquet"] == ("f1e2e523", 20)
    assert snapshots["proceedings.parquet"] == ("snapshot_0e799850", 515121)
    assert objects == {"dockets.parquet": ("0a1b2c3d", 1000)}  # documents answers 404; comments records no ETag


class _Head:
    def __init__(self, etag: str | None = None, length: str | None = None) -> None:
        self.status_code = 200 if etag else 404
        self.headers = {"ETag": etag, "Content-Length": length}

    def raise_for_status(self) -> None:
        pass


def test_exit_code_fails_only_on_drift_not_live_or_malformed(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(pins, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        pins, "fetch_live", lambda url, text: (pins.rollup_pins(INDEX), pins.snapshot_pins(POINTER, MANIFEST), OBJECTS)
    )
    ledger = tmp_path / "ledger.md"
    ledger.write_text(LEDGER, encoding="utf-8")
    assert pins.main(["--ledger", str(ledger), "--index-url", "https://data.example"]) == 1
    assert "OK=4 NO-PIN=2 DRIFT=3 NOT-LIVE=2 MALFORMED=1" in capsys.readouterr().out
    clean = [line for line in LEDGER.splitlines() if "amendments" in line or "withdrawn" in line]
    ledger.write_text("\n".join(clean), encoding="utf-8")
    assert pins.main(["--ledger", str(ledger), "--index-url", "https://data.example"]) == 0
