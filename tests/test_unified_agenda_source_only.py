"""Source-only replay cannot pull or merge prior rows; literal values survive."""

from importlib import import_module

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.unified_agenda import normalize_retained_edition

builder = import_module("spicy_regs.transforms.build_unified_agenda")


def test_offline_normalization_preserves_strip_only_values_and_receipt():
    xml = b"<REGINFO_RIN_DATA><RIN_INFO><RIN>1234-AA00</RIN><PUBLICATION><PUBLICATION_ID>202510</PUBLICATION_ID></PUBLICATION><RULE_TITLE> a  b </RULE_TITLE></RIN_INFO></REGINFO_RIN_DATA>"
    receipts = []
    [row] = normalize_retained_edition(xml, edition="202510", on_source=receipts.append)
    assert row["title"] == "a  b"
    assert receipts[0]["records"] == 1 and receipts[0]["repair"] is None
    with pytest.raises(ValueError):
        normalize_retained_edition(xml[:-1], edition="202510", on_source=receipts.append)
    assert len(receipts) == 1


def test_source_only_never_requests_prior_and_keeps_native_values(tmp_path):
    def prior(*_):
        pytest.fail("source-only replay requested a prior table")

    out = builder.build_unified_agenda(
        tmp_path,
        editions=("202510",),
        records=lambda _: iter([{"rin": "1234-AA00", "agenda_edition": "202510", "title": "a  b"}]),
        download_prior=prior,
        source_only=True,
    )
    [row] = pq.read_table(out).to_pylist()
    assert row["rin"] == "1234-AA00" and row["title"] == "a  b"


def test_source_only_requires_explicit_offline_selection(tmp_path):
    with pytest.raises(ValueError, match="explicit editions"):
        builder.build_unified_agenda(tmp_path, source_only=True)


def test_source_only_refuses_prior_file_and_duplicate_native_ids(tmp_path):
    (tmp_path / "_ua_prior.parquet").write_bytes(b"prior")
    with pytest.raises(ValueError, match="prior merge file"):
        builder.build_unified_agenda(tmp_path, editions=("202510",), records=lambda _: [], source_only=True)
    (tmp_path / "_ua_prior.parquet").unlink()
    with pytest.raises(ValueError, match="unique non-null"):
        builder.build_unified_agenda(
            tmp_path,
            editions=("202510",),
            records=lambda _: [{"rin": "1234-AA00", "agenda_edition": "202510"}] * 2,
            source_only=True,
        )


def test_source_selection_manifest_is_pinned_in_output_metadata(tmp_path):
    import hashlib
    import json

    xml = b"<REGINFO_RIN_DATA><RIN_INFO><RIN>1234-AA00</RIN><PUBLICATION><PUBLICATION_ID>202510</PUBLICATION_ID></PUBLICATION></RIN_INFO></REGINFO_RIN_DATA>"
    receipts = []

    def read(stem):
        return normalize_retained_edition(xml, edition=stem, on_source=receipts.append)

    out = builder.build_unified_agenda(
        tmp_path, editions=("202510",), records=read, source_only=True, source_receipts=receipts
    )
    encoded = (tmp_path / "unified_agenda_source_inputs.json").read_bytes()
    metadata = pq.ParquetFile(out).schema_arrow.metadata
    assert metadata[b"unified_agenda_source_inputs_sha256"].decode() == hashlib.sha256(encoded).hexdigest()
    assert json.loads(encoded)[0]["input_sha256"] == hashlib.sha256(xml).hexdigest()
    assert metadata[b"unified_agenda_source_only"] == b"true"
    assert metadata[b"unified_agenda_repairs"] == b"[]"


def test_source_only_refuses_existing_output_before_reading(tmp_path):
    (tmp_path / "unified_agenda.parquet").write_bytes(b"held-output")

    def read(_):
        pytest.fail("existing output should refuse before reading")

    with pytest.raises(ValueError, match="existing output"):
        builder.build_unified_agenda(tmp_path, editions=("202510",), records=read, source_only=True)
    assert (tmp_path / "unified_agenda.parquet").read_bytes() == b"held-output"


def test_source_only_uses_falsey_offline_callable(tmp_path, monkeypatch):
    class OfflineReader:
        def __bool__(self):
            return False

        def __call__(self, edition):
            return [{"rin": "1234-AA00", "agenda_edition": edition}]

    def live_reader(*args, **kwargs):
        pytest.fail("source-only replay selected the live reader")

    monkeypatch.setattr(builder, "UnifiedAgendaReader", live_reader)
    out = builder.build_unified_agenda(tmp_path, editions=("202510",), records=OfflineReader(), source_only=True)
    assert pq.read_table(out).to_pylist()[0]["rin"] == "1234-AA00"
