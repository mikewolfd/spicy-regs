"""The archive release pin document and manifest verification (L2-A3, L2-A4)."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from spicy_regs import archive_release
from spicy_regs.archive_release import ArchiveReleaseError

FIXTURES = Path(__file__).parent / "fixtures" / "archive_release"
#: Live 7d16a4d5 manifest's files[] (coverage dicts dropped; they are not part of the id).
FIXTURE = FIXTURES / "7d16a4d5_manifest.json"
LIVE = "7d16a4d5b93102beaf54c525dcadd9e446ed33187f6ae7a36786bc8303b4abf7"
#: Prior pinned Oct 30 release kept in releases[] during reader rollout.
PREVIOUS_FIXTURE = FIXTURES / "c94bfedc_manifest.json"
PREVIOUS = "c94bfedcfcf0287ce9868b4532766c7dcf4b3202187d48db07874a05fd703a78"


def _manifest(path: Path = FIXTURE) -> dict:
    return json.loads(path.read_text())


def _pins(**changes) -> str:
    document = {
        "version": 1,
        "updated_at": "2026-11-24T00:00:00Z",
        "bound": LIVE,
        "releases": [LIVE, PREVIOUS],
    }
    return json.dumps({**document, **changes})


def test_shipped_pins_are_valid_and_keep_both_transition_releases():
    pins = archive_release.load_pins()
    assert pins["bound"] == LIVE
    assert pins["bound"] in pins["releases"]
    assert pins["releases"] == sorted(set(pins["releases"]))
    assert set(pins["releases"]) >= {PREVIOUS, LIVE}


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


@pytest.mark.parametrize(("path", "release_id"), [(FIXTURE, LIVE), (PREVIOUS_FIXTURE, PREVIOUS)])
def test_release_id_is_recomputed_from_the_live_manifest_files(path, release_id):
    manifest = _manifest(path)
    assert archive_release.release_identity(manifest["schema_version"], manifest["files"]) == release_id
    assert archive_release.verify_manifest(release_id, manifest) is manifest


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
    assert found["meetings"]["rows"] == 18754 and found["list"]["rows"] == 328855
    assert found["people"]["rows"] == 102438
    assert found["person_aliases"]["rows"] == 57
    assert found["witness_key_map"]["rows"] == 155878


def test_bound_files_refuses_prior_release_without_people_tables():
    with pytest.raises(ArchiveReleaseError, match="has no archive"):
        archive_release.bound_files(_manifest(PREVIOUS_FIXTURE))


def test_a_bound_file_republished_by_spicy_regs_or_sharded_refuses():
    for change in ({"producer": "spicy-regs"}, {"shard": 0}, {"row_range": [0, 10]}):
        manifest = copy.deepcopy(_manifest())
        next(item for item in manifest["files"] if item["name"] == "meetings.parquet").update(change)
        with pytest.raises(ArchiveReleaseError):
            archive_release.bound_files(manifest)


@pytest.mark.parametrize(("url", "spelling"), [
    ("https://www.Example.gov:443/a/b/?q=1#frag", "example.gov/a/b?q=1"),
    ("http://example.gov:80/a", "example.gov/a"),
    ("https://example.gov:8443/a//", "example.gov:8443/a/"),
    ("HTTPS://docs.house.gov/meetings/AP/AP00/20240101/1/HHRG.pdf", "docs.house.gov/meetings/AP/AP00/20240101/1/HHRG.pdf"),
])
def test_lookup_key_follows_the_fixed_lookup_url_spelling(url, spelling):
    assert archive_release.lookup_key(url) == hashlib.sha256(f"lookup:{spelling}".encode()).digest()[:8]


def test_relative_urls_have_no_lookup_key():
    assert archive_release.lookup_key("/meetings/x.pdf") is None


def test_the_published_pin_list_is_exactly_the_wire_contract(tmp_path):
    out = tmp_path / "archive-releases.json"
    assert archive_release.main(["--output", str(out)]) == 0
    published = json.loads(out.read_bytes())
    shipped = archive_release.load_pins()
    # ct-cov3 reads version/updated_at/releases; the binding record's ``bound`` is not published.
    assert published == {"version": 1, "updated_at": shipped["updated_at"], "releases": shipped["releases"]}
    assert archive_release.PINS_URL == "https://data.spicygov.ai/pins/archive-releases.json"


def test_publish_uploads_the_rendered_list_to_its_key_and_reads_it_back(tmp_path, monkeypatch):
    from spicy_regs.sources import r2

    calls = []
    monkeypatch.setattr(r2, "upload_file", lambda path, key, cache_control: calls.append(("put", path.read_bytes(), key)))
    monkeypatch.setattr(r2, "verify_public_file", lambda path, key, base: calls.append(("read", key, base)) or {})
    # A configured base elsewhere (the workflow's fallback) must not be what the read-back proves.
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.spicy-regs.dev")
    out = tmp_path / "archive-releases.json"
    archive_release.main(["--output", str(out), "--publish"])
    assert calls == [("put", out.read_bytes(), archive_release.PINS_KEY),
                     ("read", archive_release.PINS_KEY, "https://data.spicygov.ai")]
