"""Actual vote emission/carry/rollup/publication, with only synthetic local inputs."""

from copy import deepcopy
import json
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.congress_receipts import CongressInput
from spicy_regs.etl_receipts import RECEIPT_ROW_GROUP, decode_exact_json
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup
from spicy_regs.receipt_members import receipt_paths
from spicy_regs.sources import publication as pub, r2
from spicy_regs.votes_batch_receipts import restore_processing_input, write_congress_dataset
from tests.generation_fakes import Store
from tests.test_congress_receipts import shaped


def row(congress, member, **changes):
    return {
        "vote_id": f"{congress}-house-1-10",
        "member_key": f"name:{member}",
        "member_name": member,
        "congress": str(congress),
        "chamber": "house",
        "position": "Yea",
        "vote_date": "2025-01-01",
        "bioguide_id": None,
    } | changes


def table(paths):
    return pa.concat_tables([pq.read_table(path) for path in receipt_paths(paths)])


class Votes(SubjectReceiptRollup):
    name = "votes"
    outputs = ("roll_call_votes.parquet", "member_votes.parquet")
    split_vote_receipts = True

    def build(self, output_dir):
        def build(work, *, download_prior):
            self.source = work / "member_votes.parquet"
            # Exercise the normal selected-prior validation/restoration and carry route.
            download_prior(self.source.name, self.source)
            shaped(self.source, self.rows, {b"fixture": b"ordered votes"})
            self.rollcalls = work / "roll_call_votes.parquet"
            download_prior(self.rollcalls.name, self.rollcalls)
            shaped(self.rollcalls, [{"vote_id": "119-house-1-10", "congress": "119", "chamber": "house"}])
            return self.rollcalls, self.source

        return self.build_receipts(output_dir, build)


def test_actual_rollup_keeps_history_bytes_and_uploads_only_changes(tmp_path, monkeypatch, record_property):
    from spicy_regs.vote_receipt_members import VoteReceiptMembers

    emission = {"rows": 0, "calls": 0, "seconds": 0.0}
    real_emit = VoteReceiptMembers.write_table

    def timed_emit(self, incoming, **kwargs):
        start = perf_counter()
        try:
            return real_emit(self, incoming, **kwargs)
        finally:
            emission["rows"] += incoming.num_rows
            emission["calls"] += 1
            emission["seconds"] += perf_counter() - start

    monkeypatch.setattr(VoteReceiptMembers, "write_table", timed_emit)
    store = Store()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.invalid")
    monkeypatch.setattr(
        pub,
        "current_index",
        lambda _: (
            pub.parse_index(store.objects[pub.INDEX_V2_KEY]) if pub.INDEX_V2_KEY in store.objects else pub.empty_index()
        ),
    )
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(r2, "require_credentials", lambda *_: None)
    monkeypatch.setattr("spicy_regs.sources.cloudflare.purge_urls", lambda *_: None)

    def fetch(_base, member, target, _label):
        target.write_bytes(store.objects[member.path])
        return target

    monkeypatch.setattr(pub, "fetch_member", fetch)
    monkeypatch.setattr(
        "spicy_regs.pipelines.rollups.subject_receipts.combine_receipts",
        lambda *_: pytest.fail("split votes must not combine completed receipt members"),
    )
    history = [row(117, f"H{i}") for i in range(RECEIPT_ROW_GROUP + 3)]
    first = [row(119, "A"), row(119, "B"), *history, row(116, "Old")]
    corrected = [row(119, "A", position="Nay"), row(119, "D"), row(119, "E"), *history, row(116, "Old")]
    crosswalk = deepcopy(corrected)
    crosswalk[3]["bioguide_id"] = "X000001"
    crosswalk[3]["vote_date"] = "2025-01-02"
    prior, rollcall_prior, previous, controls, measurements = None, None, {}, [], []
    for night, rows in enumerate((first, first, corrected, crosswalk)):
        pipeline = Votes(output_dir=tmp_path / f"run-{night}", skip_upload=False)
        pipeline.rows = rows
        before = dict(store.sent)
        pipeline.run()
        index = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
        family = index["families"]["votes"]
        generation = pipeline.output_dir / "generations" / family["artifactDigest"].removeprefix("sha256:")
        members = tuple(generation / member["key"] for member in family["etlReceipts"]["members"])
        assert family["etlReceipts"]["version"] == 2
        assert not list(pipeline.output_dir.glob(".builds/**/candidate/etl_receipts.parquet"))
        assert not list(pipeline.output_dir.glob(".builds/**/datasets/member_votes/etl_receipts.parquet"))
        assert [path.name for path in members] == [
            "roll_call_votes_receipts.parquet",
            "member_votes_metadata.parquet",
            "member_votes_congress_119.parquet",
            "member_votes_congress_117.parquet",
            "member_votes_congress_116.parquet",
            "acquisition-receipts.parquet",
        ]
        # Independent monolithic emission uses the same input, context and prior.
        _, reference = write_congress_dataset(
            pipeline.source,
            tmp_path / f"reference-{night}",
            dataset="member_votes",
            generation_id=pipeline.receipt_generation_id,
            prior=prior,
            validate=False,
        )
        actual = table(members)
        _, rollcall_reference = write_congress_dataset(
            pipeline.rollcalls,
            tmp_path / f"rollcall-reference-{night}",
            dataset="roll_call_votes",
            generation_id=pipeline.receipt_generation_id,
            prior=rollcall_prior,
            validate=False,
        )
        assert table(members[:-1]).equals(table((rollcall_reference, reference)), check_metadata=True)
        restored = restore_processing_input(
            generation / "member_votes.parquet",
            members,
            tmp_path / f"restored-{night}.parquet",
            dataset="member_votes",
            generation_id=pipeline.receipt_generation_id,
            scoped=True,
            validate=False,
        )
        assert pq.read_table(restored).equals(pq.read_table(pipeline.source), check_metadata=True)
        current = {path.name: path.read_bytes() for path in members}
        for path in members:
            key = family["prefix"] + "/etl_receipts/" + path.name
            sent = store.sent[key] - before.get(key, 0)
            assert store.objects[key] == current[path.name]
            if night and current[path.name] == previous[path.name]:
                assert sent == 0 and key in store.copies
            else:
                assert sent == len(current[path.name])
            assert store.reads[key] >= 1  # Full remote readback is deliberately retained.
            measurements.append(
                {
                    "night": night,
                    "member": path.name,
                    "bytes": len(current[path.name]),
                    "uploaded": sent,
                    "readback_gets": store.reads[key],
                }
            )
        if night == 1:
            assert {key: value for key, value in current.items() if key.startswith("member_votes_")} == {
                key: value for key, value in previous.items() if key.startswith("member_votes_")
            }
        if night == 2:
            assert current["member_votes_congress_117.parquet"] == previous["member_votes_congress_117.parquet"]
            assert current["member_votes_congress_116.parquet"] == previous["member_votes_congress_116.parquet"]
        if night == 3:
            assert current["member_votes_congress_117.parquet"] != previous["member_votes_congress_117.parquet"]
            assert current["member_votes_congress_116.parquet"] == previous["member_votes_congress_116.parquet"]
        controls.append(actual.to_pylist())
        prior = CongressInput(generation / "member_votes.parquet", members, pipeline.receipt_generation_id)
        rollcall_prior = CongressInput(generation / "roll_call_votes.parquet", members, pipeline.receipt_generation_id)
        previous = current

    def accepted(rows):
        return {
            dict(decode_exact_json(r["identity_json"]))["member_key"]: r
            for r in rows
            if r["outcome"] == "accepted" and r["dataset"] == "member_votes"
        }

    old, new, linked = (accepted(controls[i]) for i in (1, 2, 3))
    assert "name:B" not in new and {"name:D", "name:E"} <= set(new)
    assert (
        decode_exact_json(new["name:A"]["diagnostic_json"])["prior_receipt"]["receipt_id"]
        == old["name:A"]["receipt_id"]
    )
    assert (
        decode_exact_json(linked["name:H0"]["diagnostic_json"])["prior_receipt"]["receipt_id"]
        == new["name:H0"]["receipt_id"]
    )
    assert linked["name:Old"] == old["name:Old"]
    record_property("member_transfers", json.dumps(measurements, sort_keys=True))
    record_property("synthetic_emission_only", json.dumps(emission, sort_keys=True))


@pytest.mark.parametrize("damage", ["interleaved", "missing-congress"])
def test_layout_refusal_preserves_prior_and_attempt_stage(tmp_path, damage):
    source = shaped(tmp_path / "prior-source.parquet", [row(119, "A"), row(117, "B")])
    subject, prior = write_congress_dataset(
        source, tmp_path / "prior", dataset="member_votes", generation_id="old", split_receipts=True
    )
    saved = {path: path.read_bytes() for path in prior}
    rows = [row(119, "A"), row(117, "B"), row(119, "C")]
    if damage == "missing-congress":
        rows[2]["congress"] = None
    source = shaped(tmp_path / "current-source.parquet", rows)
    with pytest.raises(ValueError, match="contiguous|explicit positive Congress"):
        write_congress_dataset(
            source,
            tmp_path / "failed",
            dataset="member_votes",
            generation_id="new",
            prior=CongressInput(subject, prior, "old"),
            split_receipts=True,
        )
    assert not (tmp_path / "failed").exists()
    assert list(tmp_path.glob(".congress-*/bundle/etl_receipts.parquet"))
    assert all(path.read_bytes() == content for path, content in saved.items())


def test_unresolved_refusal_tail_and_fresh_failure_keep_exact_occurrences(tmp_path):
    rows = [row(119, "A"), row(117, "B", vote_id=None)]
    old_source = shaped(tmp_path / "old-source.parquet", rows)
    subject, receipts = write_congress_dataset(
        old_source, tmp_path / "old", dataset="member_votes", generation_id="old"
    )
    prior = CongressInput(subject, receipts, "old")
    source = shaped(tmp_path / "source.parquet", rows[:1])
    _, reference = write_congress_dataset(
        source, tmp_path / "reference", dataset="member_votes", generation_id="new", prior=prior
    )
    _, members = write_congress_dataset(
        source, tmp_path / "split", dataset="member_votes", generation_id="new", prior=prior, split_receipts=True
    )
    assert members[-1].name == "member_votes_refusals.parquet"
    assert table(members).equals(pq.read_table(reference), check_metadata=True)
    assert pq.read_table(members[-1]).to_pylist() == [
        r for r in pq.read_table(receipts).to_pylist() if r["outcome"] == "refused"
    ]
    calls = []
    _, again = write_congress_dataset(
        old_source,
        tmp_path / "again",
        dataset="member_votes",
        generation_id="again",
        prior=prior,
        split_receipts=True,
        on_fresh_failure=lambda: calls.append("refused"),
    )
    assert calls == ["refused"]
    assert [r for r in table(again).to_pylist() if r["outcome"] == "refused"] == pq.read_table(members[-1]).to_pylist()


def test_split_is_disabled_by_default_and_rejects_other_datasets(tmp_path):
    from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup

    assert not RollCallVotesRollup.split_vote_receipts
    with pytest.raises(ValueError, match="member_votes bulk writer"):
        write_congress_dataset(
            tmp_path / "unused",
            tmp_path / "unused-output",
            dataset="members",
            generation_id="test",
            split_receipts=True,
        )


@pytest.mark.parametrize(
    "bad_rows",
    [
        [row(119, "A"), row(117, "B"), row(119, "C")],
        [row(119, "A", vote_id=None)],
    ],
)
def test_failed_optin_rollup_keeps_prior_selection_and_private_receipts(tmp_path, monkeypatch, bad_rows):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    output = tmp_path / "output"
    output.mkdir()
    first = Votes(output_dir=output)
    first.rows = [row(119, "A")]
    first.build(output)
    protected = (output / "member_votes.parquet", *first._receipt_output_members)
    contents = {path: path.read_bytes() for path in protected}
    second = Votes(output_dir=output)
    second.rows = bad_rows
    with pytest.raises(ValueError, match="contiguous|Subject conversion refused"):
        second.build(output)
    assert all(path.read_bytes() == content for path, content in contents.items())
    from spicy_regs.selected_generations import SelectedInputs

    assert (
        SelectedInputs(output, tmp_path / "read", public_url="").select("member_votes").generation_id
        == first.receipt_generation_id
    )
