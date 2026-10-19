"""Ordered vote receipts keep exact carry meaning through real admission and fake-store publication."""

from copy import deepcopy
from hashlib import sha256
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs.congress_receipts import CongressInput, policy
from spicy_regs.congress_subjects import INPUT_COLUMNS
from spicy_regs.etl_receipts import decode_exact_json, read_with_receipts
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.receipt_members import receipt_paths
from spicy_regs.selected_generations import SelectedInputs, remember_generation
from spicy_regs.sources import publication as pub
from spicy_regs.votes_batch_receipts import restore_processing_input, write_congress_dataset
from tests.generation_fakes import Store
from tests.test_congress_receipts import shaped
from tests.test_generation_publication import publish
from tests.test_votes_batch_current import WITNESS

DATASET = "member_votes"


@pytest.fixture
def votes(tmp_path):
    rows = [
        dict.fromkeys(INPUT_COLUMNS[DATASET])
        | {
            "vote_id": f"{congress}-house-1-101",
            "member_key": f"name:{member}",
            "member_name": member,
            "congress": str(congress),
            "chamber": "house",
            "position": position,
        }
        for congress, member, position in ((117, "M0", "Yea"), (117, "M1", "Nay"), (119, "M2", "Yea"))
    ]
    metadata = {b"fixture": b"synthetic vote carry"}
    source = shaped(tmp_path / "old-source.parquet", rows, metadata)
    subject, receipts = write_congress_dataset(
        source, tmp_path / "old", dataset=DATASET, generation_id="old", witnesses=[WITNESS]
    )
    old_table = pq.read_table(receipts)
    parts = tmp_path / "old-members"
    parts.mkdir()
    old = tuple(parts / name for name in ("00-metadata.parquet", "01-historical.parquet", "02-current.parquet"))
    for path, offset, length in zip(old, (0, 1, 3), (1, 2, 1), strict=True):
        pq.write_table(old_table.slice(offset, length), path, compression="zstd")
    changed = deepcopy(rows)
    changed[2]["position"] = "Nay"
    new_source = shaped(tmp_path / "new-source.parquet", changed, metadata)
    new_subject, new_receipts = write_congress_dataset(
        new_source,
        tmp_path / "new",
        dataset=DATASET,
        generation_id="new",
        prior=CongressInput(subject, receipts, "old"),
        witnesses=[WITNESS | {"sha256": "cd" * 32, "locator": "corrected vote"}],
    )
    new_table = pq.read_table(new_receipts)
    parts = tmp_path / "new-members"
    parts.mkdir()
    current = parts / old[-1].name
    pq.write_table(new_table.slice(3, 1), current, compression="zstd")
    return {
        "old_subject": subject,
        "old_receipts": receipts,
        "old_members": old,
        "old_table": old_table,
        "subject": new_subject,
        "receipts": new_receipts,
        "members": (*old[:2], current),
        "table": new_table,
        "source": new_source,
    }


def generation(path, votes, *, old=False, members=None, **kwargs):
    return build_generation(
        path,
        family="votes",
        files=[votes["old_subject" if old else "subject"]],
        expected_keys=[DATASET + ".parquet"],
        receipt_paths=members if members is not None else votes["old_members" if old else "members"],
        receipt_policies=[policy(DATASET)],
        receipt_generation_id="old" if old else "new",
        **kwargs,
    )


def fetch_from(store, monkeypatch):
    def fetch(_url, member, target, _key):
        target.write_bytes(store.objects[member.path])
        return target

    monkeypatch.setattr(pub, "fetch_member", fetch)


def test_exact_round_trip_uploads_only_changed_receipt_member(tmp_path, votes, monkeypatch, record_property):
    old, new = tmp_path / "generation-old", tmp_path / "generation-new"
    generation(old, votes, old=True)
    store = Store()
    prior = publish(store, old)
    artifact = generation(new, votes, read_snapshot=prior)
    sent_before, reads_before = dict(store.sent), dict(store.reads)
    index = publish(store, new, prior)
    declaration = index["families"]["votes"]["etlReceipts"]
    assert declaration["version"] == 2 and "key" not in declaration and "keyIndex" not in declaration
    assert [m["key"] for m in declaration["members"]] == [f"etl_receipts/{p.name}" for p in votes["members"]]
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == index
    prefix = index["families"]["votes"]["prefix"]
    transfers = []
    for member in declaration["members"]:
        key = f"{prefix}/{member['key']}"
        if member["key"].endswith("02-current.parquet"):
            assert store.sent[key] - sent_before.get(key, 0) == votes["members"][-1].stat().st_size
        else:
            assert store.sent[key] == 0
            assert key in store.copies
        assert store.objects[key] == (new / member["key"]).read_bytes()
        reads = store.reads[key] - reads_before.get(key, 0)
        assert reads >= 1  # Normal remote byte admission still reads reused history.
        transfers.append(
            {
                "key": member["key"],
                "upload_bytes": store.sent[key] - sent_before.get(key, 0),
                "server_copies": store.copies.count(key),
                "get_count": reads,
                "readback_bytes": reads * len(store.objects[key]),
            }
        )
    record_property("receipt_member_transfers", json.dumps(transfers, sort_keys=True))

    fetch_from(store, monkeypatch)
    inputs = SelectedInputs(
        tmp_path / "remote-root", tmp_path / "fetch", index=index, public_url="https://example.test"
    )
    selected = inputs.select(DATASET)
    assert selected.generation_id == "new" and selected.key_index is None
    assert len(receipt_paths(selected.receipts)) == 3
    table = pa.concat_tables([pq.read_table(p) for p in receipt_paths(selected.receipts)])
    assert table.equals(votes["table"], check_metadata=True)
    before, after = votes["old_table"].to_pylist(), table.to_pylist()
    assert after[:3] == before[:3]  # Whole old observations, attempts, witnesses and processing, not only subjects.
    assert after[3]["generation_id"] == "new" and after[3]["witnesses"] != before[3]["witnesses"]
    predecessor = decode_exact_json(after[3]["diagnostic_json"])["prior_receipt"]
    assert predecessor == {
        "receipt_id": before[3]["receipt_id"],
        "generation_id": "old",
        "processing_sha256": "sha256:" + sha256(before[3]["processing_json"].encode()).hexdigest(),
    }
    native_before, native_after = (
        pq.read_table(votes["old_subject"]).to_pylist(),
        pq.read_table(votes["subject"]).to_pylist(),
    )
    assert native_before[:2] == native_after[:2]
    assert {
        name: (native_before[2][name], native_after[2][name])
        for name in native_before[2]
        if native_before[2][name] != native_after[2][name]
    } == {"position": ("Yea", "Nay")}
    replay = list(
        read_with_receipts(selected.subjects, receipt_paths(selected.receipts), policy(DATASET), generation_id="new")
    )
    assert replay == list(
        read_with_receipts([votes["subject"]], [votes["receipts"]], policy(DATASET), generation_id="new")
    )
    # Scoped input is already exactly this dataset: streaming restoration must not combine/filter it again.
    monkeypatch.setattr(
        "spicy_regs.votes_batch_receipts.select_receipts", lambda *_a, **_k: pytest.fail("unexpected combined file")
    )
    for bulk in (True, False):
        restored = restore_processing_input(
            selected.subjects,
            selected.receipts,
            tmp_path / f"restored-{bulk}.parquet",
            dataset=DATASET,
            generation_id="new",
            bulk=bulk,
            scoped=True,
        )
        assert pq.read_table(restored).equals(pq.read_table(votes["source"]), check_metadata=True)
    inputs.check()

    remember_generation(tmp_path / "local", new, artifact)
    local = SelectedInputs(tmp_path / "local", tmp_path / "local-read", public_url="").select(DATASET)
    assert local.receipts == tuple(new / m["key"] for m in declaration["members"])
    assert local.subjects == (new / (DATASET + ".parquet"),)


@pytest.mark.parametrize("damage", ["missing", "duplicate", "stale", "mixed-old-plus-delta", "index", "adopt"])
def test_invalid_complete_membership_refuses(tmp_path, votes, damage):
    members, kwargs = list(votes["members"]), {}
    if damage == "missing":
        members.pop(1)
    elif damage == "duplicate":
        members.append(members[1])
    elif damage == "stale":
        members[-1] = votes["old_members"][-1]
    elif damage == "mixed-old-plus-delta":
        members = [votes["old_receipts"], members[-1]]
    elif damage == "index":
        kwargs["receipt_index"] = (tmp_path / "unused-index", {})
    else:
        kwargs["adopt_owned_receipt"] = True
    with pytest.raises(ValueError, match="Missing|receipt|Receipt|Split|duplicate"):
        generation(tmp_path / "refused", votes, members=members, **kwargs)


@pytest.mark.parametrize("damage", ["mutated", "missing", "undeclared"])
def test_selected_membership_pin_and_directory_refusals(tmp_path, votes, monkeypatch, damage):
    directory = tmp_path / "generation"
    generation(directory, votes)
    store = Store()
    index = publish(store, directory)
    fetch_from(store, monkeypatch)
    inputs = SelectedInputs(tmp_path, tmp_path / "read", index=index, public_url="https://example.test")
    selected = inputs.select(DATASET)
    if damage == "mutated":
        path = selected.receipts[1]
        path.write_bytes(path.read_bytes() + b"mutation")
        with pytest.raises(ValueError, match="changed"):
            inputs.select(DATASET)
    elif damage == "missing":
        del store.objects[pub.receipt_members(index, dataset=DATASET)[1].path]
        with pytest.raises(KeyError):
            SelectedInputs(tmp_path, tmp_path / "missing", index=index, public_url="https://example.test").select(
                DATASET
            )
    else:
        (directory / "etl_receipts" / "undeclared.parquet").write_bytes(votes["members"][0].read_bytes())
        with pytest.raises((ValueError, ArtifactVerificationError)):
            verify_generation(directory)


@pytest.mark.parametrize("failure", ["transfer", "corrupt-copy", "cas"])
def test_failed_publication_preserves_prior_selection(tmp_path, votes, failure):
    old, new = tmp_path / "old-generation", tmp_path / "new-generation"
    generation(old, votes, old=True)
    store = Store()
    prior = publish(store, old)
    generation(new, votes, read_snapshot=prior)
    before = store.objects[pub.INDEX_V2_KEY]
    if failure == "transfer":

        def interrupt(key):
            if key.endswith("02-current.parquet"):
                raise OSError("interrupted")

        store.before_put = interrupt
        error = OSError
    elif failure == "corrupt-copy":
        # The immutable prior object no longer matches its declaration: copied bytes must fail readback.
        path = pub.receipt_members(prior, dataset=DATASET)[1].path
        store.objects[path] += b"damage"
        error = ArtifactVerificationError
    else:
        attempts = []

        def contention(key):
            if key == pub.INDEX_V2_KEY:
                attempts.append(key)
                store.objects[key] = json.dumps(prior, indent=len(attempts)).encode()

        store.before_put = contention
        error = pub.PublicationError
    with pytest.raises(error):
        publish(store, new, prior)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == prior
    if failure != "cas":
        assert store.objects[pub.INDEX_V2_KEY] == before


def test_priors_bind_every_original_member_without_replacing_provenance(tmp_path, votes, monkeypatch):
    from spicy_regs.pipelines.rollups.subject_receipts import NativeReceiptLifecycle, SelectedPriors
    from spicy_regs.sources import r2

    directory, root = tmp_path / "generation", tmp_path / "selected"
    artifact = generation(directory, votes)
    remember_generation(root, directory, artifact)
    prior = SelectedPriors(tmp_path / "prior", public_url="", root=root, reuse_scoped_datasets=(DATASET,))
    reads = []
    monkeypatch.setattr(r2, "note_pinned_read", lambda key, pin: reads.append((key, pin)))
    binding = prior.input_provenance(DATASET)
    originals = receipt_paths(prior.selected.select(DATASET).receipts)
    expected = [prior.selected.member_pin(path) for path in originals]
    assert binding["receipts"] == expected
    assert reads[-3:] == [(f"{DATASET}/etl_receipts/{ordinal}.parquet", pin) for ordinal, pin in enumerate(expected)]
    assert prior.selections[DATASET][1] == originals
    assert prior.scoped_receipts[DATASET] not in originals
    assert pq.read_table(prior.get(DATASET)).equals(pq.read_table(votes["source"]), check_metadata=True)

    class Dependent(NativeReceiptLifecycle):
        def build(self, output_dir):
            raise AssertionError("No operational build")

        inputs = (DATASET + ".parquet",)

        def _new_receipt_work(self, _directory, _snapshot):
            return tmp_path, prior

        def _own(self, _key, _family):
            return False

    parents = Dependent()._prime(tmp_path / "work")
    assert {key: value for key, value in parents.items() if "/etl_receipts/" in key} == dict(reads[-3:])
    prior.selected.check()


@pytest.mark.parametrize("local", [False, True])
def test_mcp_and_processing_adapter_read_all_members_lookup_succeeds(tmp_path, votes, monkeypatch, local):
    from spicy_regs import mcp_server, receipt_lookup
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from tests.test_generation_mcp import connection_fixture
    from tests.test_local_native_selection import connection

    directory = tmp_path / "generation"
    artifact = generation(directory, votes)
    store = Store()
    index = publish(store, directory)
    if local:
        root = tmp_path / "native"
        remember_generation(root, directory, artifact)
        con = connection(root, monkeypatch)
        selected = mcp_server._connection_local_selection(con)
        assert len(selected["native"][DATASET]["receipts"]) == 3
        adapter = ReceiptAdapter(con, pub.empty_index(), "", local_native=selected["native"])
        lookup_index = pub.empty_index()
    else:
        locations = {
            member.path: directory / member.key
            for member in (*pub.table_members(index, DATASET + ".parquet"), *pub.receipt_members(index))
        }
        monkeypatch.setattr(mcp_server, "DATA_DIR", None)
        with monkeypatch.context() as connection_patch:
            connection_fixture(connection_patch, index, locations, tables=(DATASET,))
            con = mcp_server._build_connection()
        selected = None
        # Same public selection, with its already pinned local download for processing replay.
        adapter = ReceiptAdapter(
            con,
            index,
            "",
            local_directory=directory,
            local_receipts={m.path: str(directory / m.key) for m in pub.receipt_members(index)},
        )
        lookup_index = index
    try:
        assert con.execute("SELECT count(*) FROM etl_receipts").fetchone() == (4,)
        assert con.execute("SELECT position FROM member_votes ORDER BY member_key").fetchall() == [
            ("Yea",),
            ("Nay",),
            ("Nay",),
        ]
        if not local:
            status = mcp_server._receipt_families(index, None)["votes"]
            assert status["members"] == index["families"]["votes"]["etlReceipts"]["members"]
            assert "sha256" not in status  # No invented aggregate byte hash.
        expected = list(
            read_with_receipts([votes["subject"]], [votes["receipts"]], policy(DATASET), generation_id="new")
        )
        assert list(adapter.selected_rows(DATASET)) == expected
        base_url = mcp_server.R2_BASE_URL
        assert len(receipt_lookup.selected_members(lookup_index, selected, base_url, DATASET)) == 3
        assert DATASET in receipt_lookup.tables_with_receipts(lookup_index, selected, base_url)
        lookup_cursor = con
        if not local:

            class LookupCursor:
                def execute(self, sql, parameters=None):
                    # The connection fixture rewrites URL literals; lookup also
                    # binds member URLs as parameters. Refuse unknown targets.
                    import re

                    def resolved(value):
                        if isinstance(value, str) and value.startswith(("https://", "http://")):
                            key = value.removeprefix(base_url + "/")
                            if key not in locations:
                                raise AssertionError("Unrecognized fake-remote target: " + value)
                            return str(locations[key])
                        return value

                    for url in re.findall(r"https?://[^'\s]+", sql):
                        sql = sql.replace(url, resolved(url))
                    params = [resolved(value) for value in parameters] if parameters is not None else None
                    return con.inner.execute(sql, params) if params is not None else con.inner.execute(sql)

            lookup_cursor = LookupCursor()
            with pytest.raises(AssertionError, match="Unrecognized fake-remote target"):
                lookup_cursor.execute("SELECT ?", ["https://unmapped.invalid/member.parquet"])
        reply = receipt_lookup.read_fields(
            lookup_cursor,
            table=DATASET,
            keys=[{"vote_id": "119-house-1-101", "member_key": "name:M2"}],
            fields=["position"],
            index=lookup_index,
            local=selected,
            base_url=base_url,
            entry={},
            plain=lambda value: value,
        )
        assert reply["keys"][0]["fields"]["position"] == {"state": "stated", "value": "Nay"}
        assert reply["receipts"]["generation_id"] == "new"
    finally:
        con.close()


def test_remote_staging_preserves_declared_order_and_exact_member_pins(tmp_path, votes):
    from spicy_regs.remote_generations import verify_remote_generation
    from spicy_regs.sources.remote_parquet import StoredParquet
    from tests.test_remote_generations import RemoteStore
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    directory = tmp_path / "generation"
    artifact = generation(directory, votes)
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    for name in ("artifact.json", "members.json"):
        (metadata / name).write_bytes((directory / name).read_bytes())
    store, staged, prefix = RemoteStore(), [], "stage/votes"
    for member in iter_member_descriptors(artifact, LocalMemberSource(directory)):
        key = prefix + "/" + member.object_key
        store.objects[key] = (directory / member.object_key).read_bytes()
        staged.append(
            StoredParquet(
                key,
                member.byte_size,
                member.sha256,
                store.head_object(Bucket="test", Key=key)["ETag"],
                member.record_count,
            )
        )
    verified = verify_remote_generation(
        metadata,
        client=store,
        bucket="test",
        staging_prefix=prefix,
        members=staged,
        expected_pin=artifact.pin,
        bounded_receipts=True,
    )
    assert verified.root["spec"]["etlReceipts"] == artifact.root["spec"]["etlReceipts"]
    store.objects[prefix + "/etl_receipts/undeclared.parquet"] = votes["members"][0].read_bytes()
    with pytest.raises((ValueError, ArtifactVerificationError)):
        verify_remote_generation(
            metadata, client=store, bucket="test", staging_prefix=prefix, members=staged, expected_pin=artifact.pin
        )


def test_split_descriptor_refuses_unsafe_ambiguous_or_unpinned_members(tmp_path, votes):
    directory = tmp_path / "generation"
    generation(directory, votes)
    index = publish(Store(), directory)
    mutations = [
        lambda value: value.update(version=True),
        lambda value: value.update(members=[]),
        lambda value: value["members"].append(value["members"][0]),
        lambda value: value.update(rows=value["rows"] + 1),
        lambda value: value["members"][0].update(key="../outside.parquet"),
        lambda value: value["members"][0].update(sha256="unknown"),
        lambda value: value["members"][0].pop("byteSize"),
        lambda value: value.update(keyIndex={}),
        lambda value: value.update(key="etl_receipts.parquet"),
    ]
    for mutate in mutations:
        bad = deepcopy(index)
        mutate(bad["families"]["votes"]["etlReceipts"])
        with pytest.raises(pub.PublicationError):
            pub.parse_index(json.dumps(bad).encode())
