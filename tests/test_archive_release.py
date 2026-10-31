"""The archive release pin document and manifest verification (L2-A3, L2-A4)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from spicy_regs import archive_release
from spicy_regs.archive_release import ArchiveReleaseError

#: The live c94bfedc manifest's files[] (coverage dicts dropped; they are not part of the id).
FIXTURE = Path(__file__).parent / "fixtures" / "archive_release" / "c94bfedc_manifest.json"
LIVE = "c94bfedcfcf0287ce9868b4532766c7dcf4b3202187d48db07874a05fd703a78"


def _manifest() -> dict:
    return json.loads(FIXTURE.read_text())


def _pins(**changes) -> str:
    document = {"version": 1, "updated_at": "2026-10-30T00:00:00Z", "bound": LIVE, "releases": [LIVE]}
    return json.dumps({**document, **changes})


def test_shipped_pins_are_valid_and_bind_a_pinned_release():
    pins = archive_release.load_pins()
    assert pins["bound"] in pins["releases"]
    assert pins["releases"] == sorted(set(pins["releases"]))


@pytest.mark.parametrize("changes", [
    {"version": 2},
    {"releases": [LIVE.upper()], "bound": LIVE.upper()},
    {"releases": [LIVE[:-1]], "bound": LIVE[:-1]},
    {"releases": [LIVE, LIVE]},
    {"releases": []},
    {"bound": "0" * 64},
    {"updated_at": None},
])
def test_pins_outside_the_wire_contract_refuse(changes):
    with pytest.raises(ArchiveReleaseError):
        archive_release.parse_pins(_pins(**changes))


def test_release_id_is_recomputed_from_the_live_manifest_files():
    manifest = _manifest()
    assert archive_release.release_identity(manifest["schema_version"], manifest["files"]) == LIVE
    assert archive_release.verify_manifest(LIVE, manifest) is manifest


@pytest.mark.parametrize("field", ["sha256", "size", "rows", "name"])
def test_a_single_mutated_file_entry_refuses_even_when_the_stated_id_matches(field):
    manifest = _manifest()
    entry = manifest["files"][6]
    entry[field] = entry[field] + 1 if isinstance(entry[field], int) else "0" + entry[field][1:]
    with pytest.raises(ArchiveReleaseError, match="hashes to"):
        archive_release.verify_manifest(LIVE, manifest)


def test_a_manifest_for_another_release_refuses():
    with pytest.raises(ArchiveReleaseError, match="states"):
        archive_release.verify_manifest("0" * 64, _manifest())


def test_bound_files_are_the_named_archive_files_only():
    found = archive_release.bound_files(_manifest())
    assert list(found) == list(archive_release.BOUND_FILES)
    assert {entry["producer"] for entry in found.values()} == {"archive"}
    assert found["meetings"]["rows"] == 18687 and found["list"]["rows"] == 367008


def test_a_bound_file_republished_by_spicy_regs_or_sharded_refuses():
    for change in ({"producer": "spicy-regs"}, {"shard": 0}, {"row_range": [0, 10]}):
        manifest = copy.deepcopy(_manifest())
        next(item for item in manifest["files"] if item["name"] == "meetings.parquet").update(change)
        with pytest.raises(ArchiveReleaseError):
            archive_release.bound_files(manifest)
