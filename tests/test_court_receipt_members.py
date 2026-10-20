"""Two ordinary catch-ups reuse exact receipts; the next full refresh can still read them."""

from contextlib import nullcontext
from copy import deepcopy
from datetime import date
from hashlib import sha256
import importlib
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import court_receipts, receipt_lookup
from spicy_regs.etl_receipts import decode_exact_json
from spicy_regs.receipt_members import receipt_declarations, receipt_paths
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication, r2
from tests.generation_fakes import Store
from tests.receipt_lookup_fixtures import serving
from tests.test_court_cluster_delta import selected as _selected, page, transport
from tests.test_generation_publication import publish

delta = importlib.import_module("spicy_regs.transforms.catch_up_court_opinion_clusters")
normal = importlib.import_module("spicy_regs.transforms.build_court_opinion_clusters")
pipeline = importlib.import_module("spicy_regs.pipelines.rollups.court_opinion_clusters")
selected = _selected


def table(paths):
    return pa.concat_tables([pq.read_table(path) for path in receipt_paths(paths)])


def test_two_catchups_publish_exact_members_then_normal_refresh_carries(
    selected, monkeypatch, tmp_path, record_property
):
    _, kwargs = selected
    store = Store()
    index = publish(store, kwargs["prior_directory"])
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.invalid")
    monkeypatch.setattr(publication, "snapshot", lambda _: nullcontext(index))
    monkeypatch.setattr(publication, "current_index", lambda _: index)
    monkeypatch.setattr(
        publication,
        "load_family_root",
        lambda _, entry: (
            store.objects[entry["prefix"] + "/artifact.json"],
            json.loads(store.objects[entry["prefix"] + "/artifact.json"]),
        ),
    )
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(r2, "require_credentials", lambda *_: None)
    monkeypatch.setattr("spicy_regs.sources.cloudflare.purge_urls", lambda *_: None)
    monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
    monkeypatch.setattr(delta, "combine_receipts", lambda *_: pytest.fail("Split catch-up must not recombine receipts"))
    original_copy = shutil.copyfile
    copies = []

    def copy(source, destination, **options):
        result = original_copy(source, destination, **options)
        if destination.parent.name == "etl_receipts":
            copies.append((str(destination), destination.stat().st_size))
        return result

    monkeypatch.setattr(shutil, "copyfile", copy)
    prior = kwargs["prior_directory"]
    prior_receipts = table(prior / "etl_receipts.parquet")
    prior_subjects = pq.read_table(prior / delta.OUTPUT)
    measurements = []
    for number, ids in enumerate(([6, 4], [9, 8])):
        old = index["families"][delta.FAMILY]
        old_declarations = receipt_declarations(old["etlReceipts"], pinned=True)
        prior_bytes = {item["key"]: (prior / item["key"]).read_bytes() for item in old_declarations}
        work = tmp_path / f"run-{number}"
        calls = transport(monkeypatch, page(ids))
        before_sent, before_reads, before_copy = dict(store.sent), dict(store.reads), len(copies)
        pipeline.catch_up(
            output_dir=work,
            prior_directory=prior,
            expected_prior=old["artifactDigest"],
            source_edition=kwargs["source_edition"],
            source_edition_evidence=kwargs["source_edition_evidence"],
            source_edition_sha256=kwargs["source_edition_sha256"],
            courts_local_file=kwargs["courts_local_file"],
            courts_sha256=kwargs["courts_sha256"],
            search_through=max(ids),
            retained_search_directory=tmp_path / f"search-{number}",
            max_requests=2,
            max_wait_seconds=0,
            skip_upload=False,
            split_receipts=True,
        )
        assert len(calls) == 1
        index = publication.parse_index(store.objects[publication.INDEX_V2_KEY])
        entry = index["families"][delta.FAMILY]
        generation = work / "generations" / entry["artifactDigest"][7:]
        members = receipt_declarations(entry["etlReceipts"], pinned=True)
        paths = tuple(generation / item["key"] for item in members)
        actual = table(paths)
        assert actual.slice(0, prior_receipts.num_rows).equals(prior_receipts, check_metadata=True)
        subjects = pq.read_table(generation / delta.OUTPUT)
        assert subjects.slice(0, prior_subjects.num_rows).equals(prior_subjects, check_metadata=True)
        assert subjects["cluster_id"].to_pylist()[prior_subjects.num_rows :] == list(map(str, ids))
        source_rows = [
            decode_exact_json(row["processing_json"]) for row in actual.to_pylist()[prior_receipts.num_rows :]
        ]
        assert [json.loads(row["raw_source_record"])["cluster_id"] for row in source_rows] == ids
        assert all((prior / key).read_bytes() == value for key, value in prior_bytes.items())
        assert members[0]["key"] == "etl_receipts/etl_receipts.parquet"  # First transition changes the old key.
        member_costs = []
        for ordinal, item in enumerate(members):
            key = entry["prefix"] + "/" + item["key"]
            sent = store.sent[key] - before_sent.get(key, 0)
            reads = store.reads[key] - before_reads.get(key, 0)
            if number and ordinal < len(old_declarations):
                assert item == old_declarations[ordinal]
                assert sent == 0 and key in store.copies
            else:
                assert sent == item["byteSize"]
            assert store.objects[key] == paths[ordinal].read_bytes()
            assert reads >= 1  # Every member still passes normal remote byte admission.
            member_costs.append(
                {
                    "key": item["key"],
                    "uploaded_bytes": sent,
                    "server_copies": store.copies.count(key),
                    "readback_bytes": reads * item["byteSize"],
                }
            )
        local = copies[before_copy:]
        assert sum(size for _, size in local) == 2 * sum(item["byteSize"] for item in members)
        measurements.append(
            {"run": number, "local_receipt_copy_bytes": sum(size for _, size in local), "members": member_costs}
        )
        # Compare bulk restoration against the maintained row decoder over the same complete selection.
        expected = list(
            court_receipts.processing_rows(
                court_receipts.read_court_rows(
                    generation / delta.OUTPUT,
                    dataset=delta.DATASET,
                    receipt_path=paths,
                    generation_id=entry["etlReceipts"]["generationId"],
                ),
                dataset=delta.DATASET,
                schema=normal._SCHEMA,
            )
        )
        restored = court_receipts.restore_processing_input(
            generation / delta.OUTPUT,
            tmp_path / f"restored-{number}.parquet",
            dataset=delta.DATASET,
            schema=normal._SCHEMA,
            receipt_path=paths,
            generation_id=entry["etlReceipts"]["generationId"],
        )
        assert pq.read_table(restored).to_pylist() == expected
        prior, prior_receipts, prior_subjects = generation, actual, subjects
    record_property("court_receipt_costs", json.dumps(measurements, sort_keys=True))
    _lookup_members(tmp_path, monkeypatch, store, index, prior, paths)
    _normal_refresh(tmp_path, monkeypatch, store, index, prior, prior_receipts)
    # Every later member is byte-bound before search, not only the historical first member.
    changed = paths[-1]
    raw = changed.read_bytes()
    changed.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    work = tmp_path / "changed-member"
    evidence = CaptureEvidence(work, delta.FAMILY)
    entry = index["families"][delta.FAMILY]
    evidence.inherit_pinned_root(index, store.objects[entry["prefix"] + "/artifact.json"])
    monkeypatch.setattr(
        delta, "CourtListenerOpinionSearchReader", lambda **_: pytest.fail("Changed prior must refuse first")
    )
    pointer = store.objects[publication.INDEX_V2_KEY]
    with pytest.raises(ValueError, match="prior bytes differ"):
        delta.catch_up_court_opinion_clusters(
            work,
            **(
                kwargs
                | {
                    "prior_directory": prior,
                    "expected_prior": entry["artifactDigest"],
                    "evidence": evidence,
                    "split_receipts": True,
                    "search_through": 12,
                }
            ),
        )
    assert store.objects[publication.INDEX_V2_KEY] == pointer


def _lookup_members(tmp_path, monkeypatch, store, index, generation, paths):
    # A local fake public bucket exercises published, downloaded and native selection, with no HTTP.
    root = tmp_path / "bucket"
    for key, data in store.objects.items():
        target = root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    entry = index["families"][delta.FAMILY]
    with serving(monkeypatch, root, index) as con:
        for local in (
            None,
            {
                "selected_tables": [delta.DATASET],
                "receipt_members": {
                    entry["prefix"] + "/" + member["key"]: str(path)
                    for member, path in zip(entry["etlReceipts"]["members"], paths, strict=True)
                },
            },
            {
                "native": {
                    delta.DATASET: {
                        "subjects": [str(generation / delta.OUTPUT)],
                        "receipts": list(map(str, paths)),
                        "generation_id": entry["etlReceipts"]["generationId"],
                    }
                }
            },
        ):
            members = receipt_lookup.selected_members(index, local, str(root), delta.DATASET)
            assert len(members) == 3
            reply = receipt_lookup.read_fields(
                con,
                table=delta.DATASET,
                keys=[{"cluster_id": "9"}, {"cluster_id": "2"}],
                fields=["ingest_source"],
                index=index,
                local=local,
                base_url=str(root),
                entry={},
                plain=lambda v: v,
            )
            assert [row["fields"]["ingest_source"]["value"] for row in reply["keys"]] == ["search", "bulk"]
        monkeypatch.setattr(receipt_lookup, "SCAN_ROW_BOUND", 1)
        monkeypatch.setattr(
            receipt_lookup, "_stated_identities", lambda *_: pytest.fail("Budget precedes receipt scan")
        )
        with pytest.raises(ValueError, match="over the 1-receipt bound"):
            receipt_lookup.read_fields(
                con,
                table=delta.DATASET,
                keys=[{"cluster_id": "9"}],
                fields=["ingest_source"],
                index=index,
                local=None,
                base_url=str(root),
                entry={},
                plain=lambda v: v,
            )


def _normal_refresh(tmp_path, monkeypatch, store, index, prior, prior_receipts):
    work = tmp_path / "full-refresh"
    work.mkdir()
    (work / "_clusters_prior.parquet").write_bytes((prior / delta.OUTPUT).read_bytes())
    fetched, witnesses = [], []

    def fetch(_base, member, target, _label):
        fetched.append(member)
        target.write_bytes(store.objects[member.path])
        return target

    real_witness = normal.file_witness

    def witness(path):
        value = real_witness(path)
        witnesses.append(value["sha256"])
        return value

    monkeypatch.setattr(publication, "fetch_member", fetch)
    monkeypatch.setattr(normal, "file_witness", witness)
    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("Exact prior was retained locally"))
    original_carry = court_receipts._carry
    reference = tmp_path / "monolith-reference.parquet"
    monolith = tmp_path / "monolith-prior.parquet"
    pq.write_table(prior_receipts, monolith)

    def carry(dataset, current, priors, **options):
        assert isinstance(priors, tuple) and len(priors) == 3
        reference.write_bytes(current.read_bytes())
        original_carry(dataset, current, priors, **options)
        original_carry(dataset, reference, monolith, **options)

    monkeypatch.setattr(court_receipts, "_carry", carry)
    from spicy_docs.sources.courtlistener.bulk import CourtListenerBulkReader

    def export(_):
        yield {"id": "10", "docket_id": "1", "case_name": "fresh bulk row"}

    monkeypatch.setattr(CourtListenerBulkReader, "iter_records", export)
    source = tmp_path / "fresh.csv"
    source.write_text("tiny retained fixture\n")
    result = normal.build_court_opinion_clusters(
        work,
        local_file=source,
        dump_date=date(2026, 6, 30),
        skip_court_scope=True,
        skip_search_catchup=True,
    )
    assert fetched == list(publication.receipt_members(index, dataset=delta.DATASET))
    assert all(member.sha256 in witnesses for member in fetched)
    restored = list(court_receipts.read_court_rows(result, dataset=delta.DATASET))
    assert {row["cluster_id"] for row in restored} == {"1", "2", "4", "6", "8", "9", "10"}
    current_table = table(court_receipts.local_receipt_selection(result)[0])
    assert current_table.equals(pq.read_table(reference), check_metadata=True)
    current = current_table.to_pylist()
    by_id = {row["receipt_id"]: row for row in current}
    by_record = {row["record_id"]: row for row in current if row["outcome"] == "accepted"}
    for old in prior_receipts.to_pylist():
        if old["receipt_id"] in by_id:
            assert by_id[old["receipt_id"]] == old
        else:
            # Normal full refresh may reshape processing inputs; that existing
            # behavior must retain the exact predecessor, just as with a monolith.
            assert old["outcome"] == "accepted"
            assert decode_exact_json(by_record[old["record_id"]]["diagnostic_json"])["prior_receipt"] == {
                "receipt_id": old["receipt_id"],
                "generation_id": old["generation_id"],
                "processing_sha256": "sha256:" + sha256(old["processing_json"].encode()).hexdigest(),
            }


@pytest.mark.parametrize("damage", ["duplicate", "missing", "foreign-family"])
def test_private_split_membership_refuses_before_selection(tmp_path, damage):
    bundle = tmp_path / "bundle"
    (bundle / "etl_receipts").mkdir(parents=True)
    path = bundle / delta.OUTPUT
    path.touch()
    (bundle / "etl_receipts/one.parquet").touch()
    declaration = {"version": 2, "members": [{"key": "etl_receipts/one.parquet", "rows": 1}], "rows": 1}
    values = {"generation_id": "fixture", "datasets": [delta.DATASET], "etlReceipts": declaration}
    if damage == "duplicate":
        declaration["members"].append(deepcopy(declaration["members"][0]))
        declaration["rows"] = 2
    elif damage == "missing":
        (bundle / "etl_receipts/one.parquet").unlink()
    else:
        values["datasets"].append("court_dockets")
    (bundle / "court-family.json").write_text(json.dumps(values))
    with pytest.raises(ValueError, match="duplicate|every declared|only the cluster"):
        court_receipts.local_receipt_selection(path)
