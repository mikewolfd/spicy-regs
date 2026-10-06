"""Receipt storage order must not change derived companion evidence."""

import json
import shutil

import duckdb
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_KEY
from spicy_regs.fec_receipt_adapter import ReceiptAdapter
from spicy_regs.relationship_views.fec import FEC_VIEWS
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
from tests.test_fec_identity_receipts import WITNESS

TABLES = ("fec_collections", "fec_source_records", "fec_relationships")


def navigation_bundle(directory, digests):
    """One relationship whose companion coordinates name every digest in ``digests`` (an ambiguous companion)."""
    locator = json.dumps({"collection_id": "collection-a", "source_record_id": "record-a"})
    with IdentityReceiptWriter(directory, generation_id="g-nav", tables=TABLES) as writer:
        writer.emit("fec_collections", {
            "collection_id": "collection-a", "source_family": "bulk", "profile": "bulk",
            "record_count": 1, "relationship_count": 1,
            "requested_scope_json": '["https://www.fec.gov/files/bulk-downloads/2026/cm.zip"]',
        }, input_witness=WITNESS)
        for digest in digests:
            writer.emit("fec_source_records", {
                "collection_id": "collection-a", "source_record_id": "record-a",
                "source_sha256": digest, "source_locator_json": locator,
            }, input_witness=WITNESS)
        writer.emit("fec_relationships", {
            "subject_id": "C00000001", "subject_type": "committee", "object_id": "H0CA00001",
            "object_type": "candidate", "relationship_type": "supports", "value_status": "reported",
            "source_sha256": WITNESS["sha256"], "source_locator_json": locator,
            "source_fields_json": "{}", "cycle": "2026",
        }, input_witness=WITNESS)
    return directory


def served(bundle):
    """What the hosted connection serves from ``bundle``: the restored table, and each adapted view's rows."""
    def descriptor(path):
        value = file_identity(path)
        return {"sha256": value["sha256"], "byteSize": value["bytes"], "rows": pq.read_metadata(path).num_rows}

    index = {"families": {"fec-source": {
        "prefix": "generations/fec-source/" + "b" * 64, "artifactDigest": "sha256:" + "b" * 64,
        "tables": {table+".parquet":descriptor(bundle/(table+".parquet")) for table in TABLES},
        "etlReceipts": {**descriptor(bundle / RECEIPT_KEY), "key": RECEIPT_KEY,
                        "generationId": "g-nav", "datasets": list(TABLES)},
    }}}
    with duckdb.connect() as con:
        con.register("fec_relationships", pq.read_table(bundle / "fec_relationships.parquet"))
        adapter = ReceiptAdapter(con, index, "unused", local_directory=bundle)
        for spec in FEC_VIEWS:
            prepared = adapter.prepare(spec, spec.query({}))
            con.execute(f"CREATE VIEW {spec.name} AS {prepared.query({})}")
        restored = [row[0] for row in con.execute(
            'SELECT source_sha256 FROM "_spicy_fec_processing_fec_source_records"').fetchall()]
        evidence = con.execute(
            "SELECT target_count, target_status, companion_candidates_json, recorded_digest_status "
            "FROM fec_relationship_evidence").fetchall()
        cycles = con.execute("SELECT * FROM fec_collection_cycles").fetchall()
    return restored, evidence, cycles


def test_relationship_evidence_is_stable_across_receipt_member_order(tmp_path):
    digests = ["sha256:" + f"{i:x}" * 64 for i in range(1, 9)]
    emitted = navigation_bundle(tmp_path / "emitted", digests)
    published = tmp_path / "published"
    shutil.copytree(emitted, published)
    receipts = pq.read_table(emitted / RECEIPT_KEY)
    pq.write_table(receipts.take(list(reversed(range(len(receipts))))), published / RECEIPT_KEY)
    # The same receipts, by value, in another order.
    before, after = (pq.ParquetFile(d / RECEIPT_KEY).read().to_pylist() for d in (emitted, published))
    assert before != after and sorted(r["receipt_id"] for r in before) == sorted(r["receipt_id"] for r in after)

    restored_emitted, evidence_emitted, cycles_emitted = served(emitted)
    restored_published, evidence_published, cycles_published = served(published)
    assert sorted(restored_emitted) == sorted(restored_published) == sorted(digests)
    assert cycles_emitted == cycles_published
    assert restored_emitted == digests  # builder order reaches the restored relation unchanged
    # Every column but the candidate list agrees.
    assert [row[:2] + row[3:] for row in evidence_emitted] == [row[:2] + row[3:] for row in evidence_published]
    # Both the candidate multiset and its serialized representation remain stable.
    assert sorted(map(json.dumps, json.loads(evidence_emitted[0][2]))) == sorted(
        map(json.dumps, json.loads(evidence_published[0][2])))
    assert evidence_emitted[0][2] == evidence_published[0][2], "companion_candidates_json differs by member order"
