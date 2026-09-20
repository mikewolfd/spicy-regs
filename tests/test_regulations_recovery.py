"""The host persists outcomes and never turns an empty/refused response into coverage."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from spicy_docs.sources import mirrulations

from spicy_regs.manifest import Manifest, save_manifest
from spicy_regs.pipelines.regulations import RegulationsPipeline
from spicy_regs.pipelines.regulations_state import UnresolvedKeys
from spicy_regs.schemas import DOCKET
from tests.test_regulations_pipeline import _FakeS3Resource, _comment_key, _docket_key, _docket_payload


@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("body", [b"", b"{}", b"null", b"[]", b"42", b"{ broken"])
def test_unproductive_answer_is_retained_not_manifested(tmp_path, monkeypatch, chunked, body):
    key = _comment_key("c1", "EPA-2026-0001")
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource({key: body}))
    pipeline = RegulationsPipeline(
        agency="EPA", output_dir=tmp_path, only_comments=True, enrich_text=False,
        use_iceberg=chunked, chunk_size=1 if chunked else 0,
    )
    pipeline.run()
    pipeline.run()
    assert key not in Manifest.load(tmp_path)
    rows = pq.read_table(tmp_path / "failed_keys.parquet").to_pylist()
    assert len(rows) == 1 and rows[0]["attempts"] == 2
    assert rows[0]["status"] == ("unreadable" if body == b"{ broken" else "requested-empty")
    assert not (tmp_path / "comments_index.parquet").exists()


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("chunked", [False, True])
def test_access_refusal_aborts_without_checkpoint(tmp_path, monkeypatch, status, chunked):
    key = _comment_key("c1", "EPA-2026-0001")

    class Refused(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            class Object:
                def get(self):
                    raise ClientError(
                        {"Error": {"Code": str(status)}, "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject"
                    )
            return Object()

    monkeypatch.setattr(mirrulations, "s3_resource", lambda: Refused({key: b"{}"}))
    with pytest.raises(mirrulations.MirrulationsAccessRefusedError):
        RegulationsPipeline(
            agency="EPA", output_dir=tmp_path, only_comments=True, enrich_text=False,
            use_iceberg=chunked, chunk_size=1 if chunked else 0,
        ).run()
    assert not (tmp_path / "manifest.parquet").exists()
    assert not (tmp_path / "failed_keys.parquet").exists()


def test_legacy_false_coverage_is_retried_before_new_work(tmp_path, monkeypatch):
    old, new = _docket_key("EPA-2024-0001"), _docket_key("EPA-2025-0002")
    save_manifest(tmp_path, {old})
    pq.write_table(pa.Table.from_pylist([{"key": old, "kind": "parse", "run_at": "2026-09-19"}]),
                   tmp_path / "failed_keys.parquet")
    asked = []

    class Recording(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            asked.append(key)
            return super().Object(name, key)

    store = {key: json.dumps(_docket_payload(identifier, "2026-01-01")).encode()
             for key, identifier in ((new, "EPA-2025-0002"), (old, "EPA-2024-0001"))}
    resource = Recording(store)
    original = mirrulations.reader_factory
    monkeypatch.setattr(mirrulations, "reader_factory", lambda *a, **kw: original(
        *a, **kw, download_workers=1, resource_factory=lambda: resource))
    RegulationsPipeline(agency="EPA", output_dir=tmp_path, skip_comments=True).run()
    assert asked == [old, new]
    assert pq.read_table(tmp_path / "dockets.parquet").num_rows == 2
    assert not (tmp_path / "failed_keys.parquet").exists()


def test_scoped_run_preserves_other_agencies_outcomes(tmp_path):
    state = UnresolvedKeys(tmp_path)
    outcome = mirrulations.KeyOutcome("raw-data/FDA/key", "requested-empty", "empty-object", "2026-09-20", 3)
    state.update([], {("FDA", "dockets"): [outcome]})
    state.update([], {("EPA", "dockets"): []})
    assert UnresolvedKeys(tmp_path).for_reader("FDA", DOCKET) == [outcome]
