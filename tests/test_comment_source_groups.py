"""Identity grouping covers every source copy and keeps local pins on the workstation."""

import hashlib

import polars as pl
import pytest

from spicy_regs.pipelines import comment_fields, comment_source_groups
from spicy_regs.pipelines.comment_fields_evidence import comment_key_identity


def test_source_groups_conserve_all_keys_and_custody(tmp_path):
    def key(identity, suffix="", directory="agency"):
        return f"raw-data/{directory}/docket/text-docket/comments/{identity}{suffix}.json"

    mixed = [key("ABC-2026-0001-0020"), key("ABC-2026-0001-0020", "(1)"),
             key("ABC-2026-0001-0020", "(1)(2)", "other-directory")]
    runner = [key("ZZZ-2026-0001-0030"), key("ZZZ-2026-0001-0030", " (2)")]
    manifest = tmp_path / "manifest.parquet"
    pl.DataFrame({"key": [*mixed, *runner]}).write_parquet(manifest)
    pins = tmp_path / "pins.parquet"
    pl.DataFrame({
        "key": [mixed[0], mixed[1], runner[0]], "raw_custody": ["local", "r2", "r2"],
        "etag": ['"pin"'] * 3, "size": [10] * 3, "version_id": ["v1"] * 3, "raw_sha256": ["a" * 64] * 3,
    }).write_parquet(pins)

    out = tmp_path / "groups"
    receipt = comment_source_groups.split(manifest, pins, out)
    assert receipt["keys"] == 5
    assert receipt["profile"] == "attachments"
    groups = {name: set(pl.read_parquet(out / name / "manifest.parquet")["key"])
              for name in (group["group"] for group in receipt["groups"])}
    local = next(name for name in groups if name.startswith("local-"))
    hosted = next(name for name in groups if name.startswith("runner-"))
    assert groups == {local: set(mixed), hosted: set(runner)}
    assert len({comment_key_identity(value) for value in groups[local]}) == 1
    for group in receipt["groups"]:
        name = group["group"]
        directory = out / name
        for kind in ("manifest", "source_pins"):
            member = group[kind]
            assert hashlib.sha256((directory / member["file"]).read_bytes()).hexdigest() == member["sha256"]
        plan = comment_fields.plan(directory / "work", directory / "manifest.parquet",
                                   source_pins=directory / "source-pins.parquet",
                                   observe_source=True, profile="attachments")
        assert set(pl.read_parquet(plan)["key"]) == groups[name]
    with pytest.raises(ValueError, match="size"):
        comment_source_groups.split(manifest, pins, tmp_path / "too-large", max_keys=2)
    assert not (tmp_path / "too-large").exists()


def test_source_groups_refuse_a_duplicate_manifest_key(tmp_path):
    key = "raw-data/A/D/text-D/comments/A-2026-0001-0010.json"
    manifest = tmp_path / "manifest.parquet"
    pl.DataFrame({"key": [key, key]}).write_parquet(manifest)
    pins = tmp_path / "pins.parquet"
    pl.DataFrame({
        "key": [key], "raw_custody": ["local"], "etag": ['"pin"'], "size": [10],
        "version_id": ["v1"], "raw_sha256": ["a" * 64],
    }).write_parquet(pins)
    out = tmp_path / "groups"
    with pytest.raises(ValueError, match="uniqueness"):
        comment_source_groups.split(manifest, pins, out)
    assert not out.exists()
