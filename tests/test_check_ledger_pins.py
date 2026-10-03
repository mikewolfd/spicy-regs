"""Pins the ledger pin check: each row's qualified pin against the live index and rulemaking pointer."""

import json

import pytest

from scripts import check_ledger_pins as pins
from spicy_regs.sources import publication

SNAPSHOT = "snapshot_0e799850778e1c92bde682d0d0f500be"


def _family(name: str, head: str, tables: dict[str, int], table_head: str = "") -> dict:
    digest = head + "0" * (64 - len(head))
    table_digest = table_head + "1" * (64 - len(table_head))
    return {
        "prefix": f"generations/{name}/{digest}",
        "logicalId": f"urn:spicy-regs:{name}",
        "artifactDigest": f"sha256:{digest}",
        "tables": {
            key: {"sha256": f"sha256:{table_digest}", "byteSize": 1, "rows": rows, "columns": [["id", "VARCHAR"]]}
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
    "format_version": 2,
    "dataset": "rulemaking",
    "snapshot_id": SNAPSHOT,
    "artifacts": {
        key: {"remote_key": f"materialized/rulemaking/snapshots/{SNAPSHOT}/{key}", "rows": rows, "visibility": visibility}
        for key, rows, visibility in (
            ("rule_targets.parquet", 517311, "public"),
            ("proceedings.parquet", 515121, "public"),
            ("comment_periods.parquet", 306582, "public"),
            ("_proceedings_state.parquet", 1, "internal"),
        )
    },
}
BASE = "https://data.example"


def _serve(monkeypatch, pointer: dict = POINTER, manifest: dict = MANIFEST) -> None:
    """Answer the publication module's bounded GETs with the rulemaking pointer and manifest."""
    documents = {f"{BASE}/{publication.SNAPSHOT_POINTER}": pointer, f"{BASE}/{pointer['manifest_key']}": manifest}
    monkeypatch.setattr(publication, "_bounded_get",
                        lambda url, **_: json.dumps(documents[url]).encode() if url in documents else None)


def _snapshot(pointer: dict = POINTER, manifest: dict = MANIFEST) -> dict | None:
    """The fixture read by the real loader, so it cannot drift from the snapshot contract."""
    with pytest.MonkeyPatch.context() as patch:
        _serve(patch, pointer, manifest)
        return publication.load_rulemaking_snapshot(BASE)

LEDGER = """\
| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T08 | `run-rollup-amendments` | `amendments.parquet` | republished; qualified at `c943cb37…` (2026-09-23); every cell matches |
| T08 | `run-rollup-nominations` | `nominations.parquet` | qualified at `4a40b55e…` (2026-09-22); newer run unaudited |
| T13 | `run-rollup-court-citations` | `court_citations.parquet`, `court_citation_map.parquet`, `generations/<f>/` \
| qualified at `f1e2e523…` (2026-09-22) |
| T15 | `run-rollup-retired` | `retired_table.parquet` | qualified at `2f001194…` (2026-09-22) |
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
    rows = pins.check(LEDGER, pins.rollup_pins(INDEX), pins.snapshot_pins(_snapshot()), OBJECTS)
    return {detail.split()[1].rstrip(","): (status, detail) for status, detail in rows}


def test_each_row_is_classified_against_its_own_live_source():
    statuses = {table: status for table, (status, _) in _results().items()}
    assert statuses == {
        "amendments.parquet": "OK",
        "nominations.parquet": "DRIFT",
        "court_citations.parquet": "OK",
        "retired_table.parquet": "NOT-LIVE",  # a family no longer in the index
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
        _snapshot(manifest={**MANIFEST, "snapshot_id": "snapshot_" + "f" * 32})


def test_no_published_pointer_pins_no_snapshot_table():
    assert pins.snapshot_pins(None) == {}


def test_fetch_live_follows_the_pointer_to_its_manifest(monkeypatch):
    monkeypatch.setattr(publication, "load_index", lambda url: INDEX if url == BASE else pytest.fail(url))
    _serve(monkeypatch)
    heads = {f"{BASE}/dockets.parquet": ('"0a1b2c3d9e8f7a6b5c4d3e2f1a0b9c8d-303"', "1000")}
    monkeypatch.setattr(pins.httpx, "head", lambda url, **_: _Head(*heads[url]) if url in heads else _Head())
    monkeypatch.setattr(publication, "load_comments_publication", lambda url: {"receipt_sha256": "sha256:" + "7" * 64})
    rollups, snapshots, objects, receipt = pins.fetch_live(BASE, LEDGER)
    assert receipt == "77777777"
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
    monkeypatch.setattr(
        pins, "fetch_live", lambda url, text: (pins.rollup_pins(INDEX), pins.snapshot_pins(_snapshot()), OBJECTS, None)
    )
    ledger = tmp_path / "ledger.md"
    ledger.write_text(LEDGER, encoding="utf-8")
    assert pins.main(["--ledger", str(ledger), "--index-url", "https://data.example"]) == 1
    assert "OK=4 NO-PIN=2 DRIFT=3 NOT-LIVE=2 MALFORMED=1" in capsys.readouterr().out
    clean = [line for line in LEDGER.splitlines() if "amendments" in line or "withdrawn" in line]
    ledger.write_text("\n".join(clean), encoding="utf-8")
    assert pins.main(["--ledger", str(ledger), "--index-url", "https://data.example"]) == 0


def test_the_default_publisher_is_the_one_the_ledger_states_not_the_environment(monkeypatch, tmp_path, capsys):
    """An unset or foreign R2_PUBLIC_URL must not decide which bucket a ledger is checked against."""
    monkeypatch.setenv("R2_PUBLIC_URL", "https://another-bucket.example")
    seen = []
    monkeypatch.setattr(pins, "fetch_live", lambda url, text: seen.append(url) or ({}, {}, {}, None))
    ledger = tmp_path / "ledger.md"
    ledger.write_text("Public data destination: `https://pub-fork.example/`. Local candidates are not publications.\n")
    assert pins.main(["--ledger", str(ledger)]) == 0
    assert seen == ["https://pub-fork.example"]
    assert "(the ledger's stated destination)" in capsys.readouterr().out


@pytest.mark.parametrize("text", ["no destination here\n",
                                  "Public data destination: `https://a.example`\nPublic data destination: `https://b.example`\n"])
def test_a_ledger_without_exactly_one_destination_refuses_unless_one_is_given(monkeypatch, tmp_path, capsys, text):
    monkeypatch.setattr(pins, "fetch_live", lambda url, text: ({}, {}, {}, None))
    ledger = tmp_path / "ledger.md"
    ledger.write_text(text)
    assert pins.main(["--ledger", str(ledger)]) == 1
    assert "pass --index-url" in capsys.readouterr().err
    assert pins.main(["--ledger", str(ledger), "--index-url", "https://explicit.example"]) == 0
    assert "an explicit --index-url" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# The prose pass: dictionary sentences pinned to a generation that is no longer live (round 6, L1).
# --------------------------------------------------------------------------- #
PROSE = {
    "amendments": {
        "data_quality": "Read again later. On generation c943cb37… (2026-09-23) every row held one. "
                        "On generation 4a40b55e… (2026-09-22) 12 rows were wrong.",
        "columns": {"amendment_id": "Keyed on it; on receipt sha256:77a08369… (2026-10-03) all rows state it."},
    },
    "rule_targets": {"data_quality": "Measured on snapshot_0e799850 and on snapshot_62318069 (digits only)."},
    "nominations": {"summary": "No pin here: a UUID such as 7866327b-c892-4430 is not one."},
}


def _live_prose(receipt: str | None = "77a08369") -> set[str]:
    return pins.live_prose_pins(pins.rollup_pins(INDEX), pins.snapshot_pins(_snapshot()), receipt)


def test_the_prose_pass_lists_each_sentence_whose_pin_is_not_the_live_one():
    """The GAO sentence of 2026-10-03 named generation 42d9c8a5 and was false on the next one; nothing listed it."""
    listed = pins.prose_not_live(PROSE, _live_prose())
    assert [(table, field, pin) for table, field, pin, _ in listed] == [
        ("amendments", "data_quality", "4a40b55e"),
        ("rule_targets", "data_quality", "snapshot_62318069"),
    ]
    assert listed[0][3] == "On generation 4a40b55e… (2026-09-22) 12 rows were wrong."


def test_a_receipt_pin_is_live_only_while_the_export_receipt_is_the_named_one():
    listed = pins.prose_not_live(PROSE, _live_prose(receipt=None))
    assert ("amendments", "columns.amendment_id", "77a08369") in [entry[:3] for entry in listed]


def test_the_prose_pass_is_a_report_and_never_fails_the_check(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(pins, "fetch_live", lambda url, text: (
        pins.rollup_pins(INDEX), pins.snapshot_pins(_snapshot()), OBJECTS, "77a08369"))
    ledger = tmp_path / "ledger.md"
    ledger.write_text("\n".join(line for line in LEDGER.splitlines() if "amendments" in line), encoding="utf-8")
    descriptions = tmp_path / "descriptions.yaml"
    descriptions.write_text("tables:\n  amendments:\n    data_quality: On generation 4a40b55e… (2026-09-22) it held 12.\n",
                            encoding="utf-8")
    argv = ["--ledger", str(ledger), "--index-url", "https://data.example", "--descriptions", str(descriptions)]
    assert pins.main(argv) == 0
    out = capsys.readouterr().out
    assert "PROSE-NOT-LIVE amendments.data_quality 4a40b55e" in out and "PROSE-NOT-LIVE=1" in out
