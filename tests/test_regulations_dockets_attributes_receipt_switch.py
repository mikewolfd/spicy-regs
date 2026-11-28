"""Switch tests for retiring per-row receipts on dockets, docket_attributes, and document_attributes (ss-vse3)."""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import subject_catalog
from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset, finish_dataset, restore_dataset
from spicy_regs.pipelines.rollups import regulatory_base
from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
from spicy_regs.selected_generations import SelectedInputs, unique_build_directory
from spicy_regs.sources import publication
from spicy_regs.transforms.regulations_shape import TYPES


def _source_schema(dataset: str) -> pa.Schema:
    return pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[dataset]])


def _table(dataset: str, rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=_source_schema(dataset))


def _except_both_ways(left: str, right: str) -> tuple[int, int]:
    l_path = left.replace("'", "''")
    r_path = right.replace("'", "''")
    with duckdb.connect() as con:
        row_left = con.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM read_parquet('{l_path}') EXCEPT ALL SELECT * FROM read_parquet('{r_path}'))"
        ).fetchone()
        row_right = con.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM read_parquet('{r_path}') EXCEPT ALL SELECT * FROM read_parquet('{l_path}'))"
        ).fetchone()
    assert row_left is not None and row_right is not None
    return int(row_left[0]), int(row_right[0])


def test_retired_markers_include_documents_and_historical_regulations():
    retired = subject_catalog.retired_receipt_datasets()
    assert {
        "dockets",
        "documents",
        "docket_attributes",
        "document_attributes",
        "comment_attributes",
        "ecfr_snapshots",
        "ecfr_snapshot_sections",
        "federal_register_issues",
    } <= retired
    assert not hasattr(regulatory_base, "_BaseTableFamily")
    for deleted in ("DocketsFamily", "DocketAttributesFamily", "DocumentAttributesFamily", "CommentAttributesFamily"):
        assert not hasattr(regulatory_base, deleted)


def test_dockets_retired_roundtrip_parity_and_identity_enforcement(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    rows = [
        {"docket_id": "EPA-HQ-2026-0001", "agency_code": "EPA", "title": "Clean Air", "rin": "2060-AA01"},
        {"docket_id": "FAA-2026-0002", "agency_code": "FAA", "docket_type": "Rulemaking"},
    ]
    retired_root = tmp_path / "retired"
    source = tmp_path / "dockets.parquet"
    pq.write_table(_table("dockets", rows), source)
    inputs = SelectedInputs(retired_root, unique_build_directory(retired_root))
    retired_out = finish_dataset(retired_root, "dockets", source, publish=True, inputs=inputs)

    index = publication.parse_index(remote.objects[publication.INDEX_V2_KEY])
    assert "etlReceipts" not in index["families"]["dockets"]
    selected = inputs.select("dockets")
    assert selected is not None and selected.receipts is None
    publication.require_live_receipt_pins(index, ["dockets"])

    restored = tmp_path / "restored.parquet"
    assert restore_dataset(retired_root, "dockets", restored, inputs=inputs)
    assert _except_both_ways(str(source), str(restored)) == (0, 0)

    # Compare retired output against the receipted path on identical source rows.
    receipted_set = subject_catalog.retired_receipt_datasets() - {"dockets"}
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: receipted_set)
    receipted_root = tmp_path / "receipted"
    receipted_out = finish_dataset(receipted_root, "dockets", source, publish=False)
    assert _except_both_ways(str(retired_out), str(receipted_out)) == (0, 0)

    # Duplicate or NULL identities must refuse without advancing publication.v2.json.
    monkeypatch.undo()
    before = remote.objects[publication.INDEX_V2_KEY]
    for bad_rows in (
        [{"docket_id": "EPA-1"}, {"docket_id": "EPA-1"}],
        [{"docket_id": "EPA-1"}, {"docket_id": None}],
    ):
        bad = tmp_path / "bad.parquet"
        pq.write_table(_table("dockets", bad_rows), bad)
        with pytest.raises(ValueError, match="identity|NULL|duplicate|refused"):
            finish_dataset(retired_root, "dockets", bad, publish=True, inputs=inputs)
        assert remote.objects[publication.INDEX_V2_KEY] == before


@pytest.mark.parametrize(
    ("dataset", "key", "extra"),
    [
        ("docket_attributes", "docket_id", {"flex_field1": "x", "display_properties_json": '[{"name":"n"}]'}),
        ("document_attributes", "document_id", {"page_count": 3, "open_for_comment": True, "city": "Austin"}),
    ],
)
def test_retired_attributes_upsert_matches_receipted_output(tmp_path, monkeypatch, dataset, key, extra):
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    seed_rows = [{key: "ID-1", **extra}, {key: "ID-2"}]
    fresh_rows = [{key: "ID-2", **extra}, {key: "ID-3"}]
    seed = tmp_path / f"{dataset}-seed.parquet"
    fresh = tmp_path / f"{dataset}-fresh.parquet"
    pq.write_table(_table(dataset, seed_rows), seed)
    pq.write_table(_table(dataset, fresh_rows), fresh)

    retired_root = tmp_path / "retired"
    retired_inputs = SelectedInputs(retired_root, unique_build_directory(retired_root))
    finish_dataset(retired_root, dataset, seed, publish=True, inputs=retired_inputs)
    retired_out = finish_attribute_dataset(retired_root, dataset, fresh, publish=True, inputs=retired_inputs)

    family = dataset.replace("_", "-")
    index = publication.parse_index(remote.objects[publication.INDEX_V2_KEY])
    assert "etlReceipts" not in index["families"][family]
    publication.require_live_receipt_pins(index, [dataset])

    # Refused fresh row stops before publication.
    before = remote.objects[publication.INDEX_V2_KEY]
    bad = tmp_path / f"{dataset}-bad.parquet"
    pq.write_table(_table(dataset, [{key: "ID-BAD", "display_properties_json": "[not-json"}]), bad)
    with pytest.raises(ValueError, match="native conversion refused"):
        finish_attribute_dataset(retired_root, dataset, bad, publish=True, inputs=retired_inputs)
    assert remote.objects[publication.INDEX_V2_KEY] == before

    # Compare against the receipted path on identical seed + fresh inputs (local-only so remote retired index isn't read).
    receipted_set = subject_catalog.retired_receipt_datasets() - {dataset}
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: receipted_set)
    receipted_root = tmp_path / "receipted"
    receipted_inputs = SelectedInputs(receipted_root, unique_build_directory(receipted_root), public_url="")
    finish_dataset(receipted_root, dataset, seed, publish=False, inputs=receipted_inputs)
    receipted_out = finish_attribute_dataset(receipted_root, dataset, fresh, publish=False, inputs=receipted_inputs)
    assert _except_both_ways(str(retired_out), str(receipted_out)) == (0, 0)


def test_documents_retired_roundtrip_parity_and_identity_enforcement(tmp_path, monkeypatch):
    """Retiring documents receipts preserves exact 21-column parity and writes zero receipt files (ss-vse3, ss-ojz3)."""
    import json

    from spicy_regs.pipelines.docket_reconcile import OUTCOME_SCHEMA, OUTCOMES, annotate_documents
    from spicy_regs.receipt_switch import receipt_files, table_differences
    from spicy_regs.sources import r2
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    rows = [
        {
            "document_id": "EPA-HQ-2026-0001-0001",
            "docket_id": "EPA-HQ-2026-0001",
            "agency_code": "EPA",
            "title": "Clean Air Notice",
            "document_type": "Notice",
            "posted_date": "2026-11-01T00:00:00Z",
            "modify_date": "2026-11-02T00:00:00Z",
            "attachments_json": json.dumps([{"url": "https://example.test/doc.pdf", "format": "pdf", "size": 42}]),
            "attachment_records_json": json.dumps([
                {
                    "id": "att-1",
                    "type": "attachments",
                    "attributes": {
                        "title": "Appendix A",
                        "fileFormats": [{"fileUrl": "https://example.test/a.pdf", "format": "pdf", "size": 10}],
                        "restrictReason": None,
                        "restrictReasonType": None,
                    },
                }
            ]),
            "withdrawn": "false",
            "additional_rins": json.dumps(["2060-AA01"]),
            "publisher_status": None,
            "removed_observed_at": None,
        },
        {
            "document_id": "FAA-2026-0002-0001",
            "docket_id": "FAA-2026-0002",
            "agency_code": "FAA",
            "title": "Airspace Rule",
            "document_type": "Proposed Rule",
            "withdrawn": "true",
            "reason_withdrawn": "superseded",
        },
    ]
    outcomes = tmp_path / OUTCOMES
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "document_id": "EPA-HQ-2026-0001-0001",
                    "docket_id": "EPA-HQ-2026-0001",
                    "publisher_status": "removed",
                    "observed_at": "2026-11-10T00:00:00+00:00",
                    "removed_observed_at": "2026-11-10T00:00:00+00:00",
                    "http_status": 404,
                    "body_sha256": None,
                }
            ],
            schema=pa.schema([(k, pa.int64() if k == "http_status" else pa.string()) for k in OUTCOME_SCHEMA]),
        ),
        outcomes,
    )

    def working(remote_key, local_path):
        if remote_key != OUTCOMES:
            return False
        local_path.write_bytes(outcomes.read_bytes())
        return True

    monkeypatch.setattr(r2, "download_working_copy", working)

    source = tmp_path / "documents.parquet"
    pq.write_table(_table("documents", rows), source)
    annotate_documents(source)

    retired_root = tmp_path / "retired"
    inputs = SelectedInputs(retired_root, unique_build_directory(retired_root))
    retired_out = finish_dataset(retired_root, "documents", source, publish=True, inputs=inputs)

    assert receipt_files(retired_out.parent) == []
    index = publication.parse_index(remote.objects[publication.INDEX_V2_KEY])
    assert "etlReceipts" not in index["families"]["documents"]
    selected = inputs.select("documents")
    assert selected is not None and selected.receipts is None
    publication.require_live_receipt_pins(index, ["documents"])

    restored = tmp_path / "restored.parquet"
    assert restore_dataset(retired_root, "documents", restored, inputs=inputs)
    restored_rows = {row["document_id"]: row for row in pq.read_table(restored).to_pylist()}
    assert json.loads(restored_rows["EPA-HQ-2026-0001-0001"]["additional_rins"]) == ["2060-AA01"]
    assert restored_rows["FAA-2026-0002-0001"]["additional_rins"] is None

    # Night-2 roundtrip: feeding published_as_source output back into finish_dataset must preserve exact 21-column parity.
    night2_root = tmp_path / "night2"
    night2_out = finish_dataset(night2_root, "documents", restored, publish=False)
    assert table_differences(retired_out, night2_out, key=("document_id",)) == []

    # Exact 21-column parity against the legacy receipted route on identical annotated source rows.
    with monkeypatch.context() as patch:
        receipted_set = subject_catalog.retired_receipt_datasets() - {"documents"}
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: receipted_set)
        receipted_root = tmp_path / "receipted"
        receipted_out = finish_dataset(receipted_root, "documents", source, publish=False)
    assert table_differences(retired_out, receipted_out, key=("document_id",)) == []

    # Duplicate or NULL identities must refuse without advancing publication.v2.json.
    before = remote.objects[publication.INDEX_V2_KEY]
    for bad_rows in (
        [{"document_id": "EPA-1"}, {"document_id": "EPA-1"}],
        [{"document_id": "EPA-1"}, {"document_id": None}],
    ):
        bad = tmp_path / "bad-doc.parquet"
        pq.write_table(_table("documents", bad_rows), bad)
        with pytest.raises(ValueError, match="identity|NULL|duplicate|refused"):
            finish_dataset(retired_root, "documents", bad, publish=True, inputs=inputs)
        assert remote.objects[publication.INDEX_V2_KEY] == before


def test_require_live_receipt_pins_verifies_unmigrated_live_and_allows_retired():
    from spicy_regs.generation_retention import receipt_archive_pins

    pins = receipt_archive_pins()
    index = {
        "families": {
            "dockets": {
                "tables": ["dockets.parquet"],
                "artifactDigest": pins["dockets"]["artifactDigest"],
                "etlReceipts": {"datasets": ["dockets"]},
            },
            "docket-attributes": {
                "tables": ["docket_attributes.parquet"],
                "artifactDigest": pins["docket_attributes"]["artifactDigest"],
                "etlReceipts": {"datasets": ["docket_attributes"]},
            },
            "document-attributes": {
                "tables": ["document_attributes.parquet"],
                "artifactDigest": "sha256:" + "d" * 64,
            },
        }
    }
    publication.require_live_receipt_pins(index, ["dockets", "docket_attributes", "document_attributes"])
    index["families"]["dockets"]["artifactDigest"] = "sha256:" + "e" * 64
    with pytest.raises(publication.PublicationError, match="receipt-archive\\.json pins"):
        publication.require_live_receipt_pins(index, ["dockets"])
