"""Retained feed bytes through shared reading, Parquet intermediates and generation."""

from copy import deepcopy
import hashlib
from pathlib import Path
import socket

import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines import house_floor_feed as house

FIXTURE = Path(__file__).parent / "fixtures/house_floor/retained-entries.xml"


@pytest.fixture
def selected(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))

    def no_network(*args, **kwargs):
        raise AssertionError("The retained feed pipeline must not open a network connection")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    body = FIXTURE.read_bytes()
    capture = dict(
        source_uri=house.FEED_URL,
        observed_at="2026-10-04T23:25:36.224562Z",
        sha256="sha256:" + hashlib.sha256(body).hexdigest(),
        bytes=len(body),
        local_path=str(FIXTURE),
    )
    return dict(
        format="house-floor-feed-captures",
        version=1,
        captures=[capture],
        scope="Retained three-entry fixture; does not establish current publisher coverage.",
    )


def rows(directory, table):
    return pq.read_table(directory / (table + ".parquet")).to_pylist()


def test_feed_revisions_and_literal_links_survive_actual_generation(selected, tmp_path):
    output = tmp_path / "first"
    artifact, evidence, summary = house.build(selected, output)
    assert summary["rows"] == dict(house_floor_feeds=1, house_floor_entries=3, house_floor_links=104)
    assert artifact.root["spec"]["publicationStatus"] == "local-partial"
    entries = rows(output / "generation", "house_floor_entries")
    assert entries[0]["weekly_url"] == entries[1]["weekly_url"]
    assert entries[0]["entry_id"] != entries[1]["entry_id"]
    assert entries[0]["entry_sha256"] != entries[1]["entry_sha256"]
    assert "H. Rept. 116-364" in entries[1]["content_html"]
    links = rows(output / "generation", "house_floor_links")
    relative = next(link for link in links if link["href"].startswith("Download.aspx?"))
    assert relative["resolved_url"].startswith("http://docs.house.gov/floor/Download.aspx?")
    for name in house.SCHEMAS:
        assert rows(output / "candidate/captures/000000", name) == rows(output / "generation", name)
    journal = (evidence / "journal.jsonl").read_text()
    assert "/Users/" not in journal and "retained-house-floor-feed" in journal
    # Replay uses only the central original, never the old local fixture path.
    replay = deepcopy(selected)
    replay["captures"][0].pop("local_path")
    house.build(replay, tmp_path / "replay")
    for name in house.SCHEMAS:
        assert rows(output / "generation", name) == rows(tmp_path / "replay/generation", name)


def test_observations_remain_separate_even_when_body_and_entry_ids_repeat(selected, tmp_path):
    selected["captures"].append(dict(selected["captures"][0], observed_at="2026-10-05T00:00:00Z"))
    house.build(selected, tmp_path / "history")
    feeds = rows(tmp_path / "history/generation", "house_floor_feeds")
    entries = rows(tmp_path / "history/generation", "house_floor_entries")
    assert len(feeds) == 2 and len({feed["capture_id"] for feed in feeds}) == 2
    assert len(entries) == 6 and len({entry["entry_id"] for entry in entries}) == 3
    links = rows(tmp_path / "history/generation", "house_floor_links")
    feed_keys = {feed["capture_id"] for feed in feeds}
    entry_keys = {(entry["capture_id"], entry["entry_index"]) for entry in entries}
    assert len(entry_keys) == len(entries)
    assert {entry["capture_id"] for entry in entries} == feed_keys
    assert {(link["capture_id"], link["entry_index"]) for link in links} == entry_keys
    assert len({(link["capture_id"], link["entry_index"], link["link_index"]) for link in links}) == len(links)


def test_repaired_target_and_original_spelling_both_reach_generation(selected, tmp_path):
    path = FIXTURE.parent / "retained-link-repair.xml"
    body = path.read_bytes()
    selected["captures"][0].update(
        local_path=str(path), bytes=len(body), sha256="sha256:" + hashlib.sha256(body).hexdigest()
    )
    house.build(selected, tmp_path / "repair")
    link = next(
        row
        for row in rows(tmp_path / "repair/generation", "house_floor_links")
        if row["entry_index"] == "0" and row["link_index"] == "20"
    )
    target = "https://www.govinfo.gov/content/pkg/CPRT-118HPRT52431/pdf/CPRT-118HPRT52431.pdf"
    assert link["href"] == "/" + target and link["resolved_url"] == "http://docs.house.gov/" + target
    assert link["repair_url"] == target
    assert link["repair_basis"] == "remove_leading_slash_exact_target_stated_in_feed"


@pytest.mark.parametrize("change", ["hash", "size", "duplicate", "naive_time", "malformed"])
def test_bad_selection_or_incomplete_source_cannot_publish_empty_success(selected, tmp_path, change):
    capture = selected["captures"][0]
    if change == "hash":
        capture["sha256"] = "sha256:" + "0" * 64
    if change == "size":
        capture["bytes"] -= 1
    if change == "duplicate":
        selected["captures"].append(dict(capture))
    if change == "naive_time":
        capture["observed_at"] = "2026-10-04T23:25:36"
    if change == "malformed":
        body = FIXTURE.read_bytes()[:-20]
        path = tmp_path / "incomplete.xml"
        path.write_bytes(body)
        capture.update(local_path=str(path), bytes=len(body), sha256="sha256:" + hashlib.sha256(body).hexdigest())
    with pytest.raises(ValueError):
        house.build(selected, tmp_path / "bad")
    assert not (tmp_path / "bad/generation/artifact.json").exists()


def test_valid_empty_feed_has_a_capture_and_empty_typed_tables(selected, tmp_path):
    body = FIXTURE.read_bytes().split(b"<entry>")[0] + b"</feed>"
    path = tmp_path / "empty.xml"
    path.write_bytes(body)
    selected["captures"][0].update(
        local_path=str(path), bytes=len(body), sha256="sha256:" + hashlib.sha256(body).hexdigest()
    )
    house.build(selected, tmp_path / "empty")
    assert rows(tmp_path / "empty/generation", "house_floor_feeds")[0]["entry_count"] == "0"
    assert rows(tmp_path / "empty/generation", "house_floor_entries") == []
    assert rows(tmp_path / "empty/generation", "house_floor_links") == []
