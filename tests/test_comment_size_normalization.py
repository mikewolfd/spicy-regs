"""Actual refused originals retain their literal sizes and produce explained native unknowns."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pytest
from spicy_docs.schemas.regulations import _extract_comment

from spicy_regs.comments_bulk import _attachments
from spicy_regs.etl_receipts import ReceiptContext, decode_exact_json
from spicy_regs.pipelines.comment_fields_write import only_adds_attachments, _v2_attachment_type
from spicy_regs.transforms.regulations_receipts import map_regulations_attempt, _processor_input
from spicy_regs.transforms.regulations_shape import _native, normalize_legacy_comment_sizes, comment_size_normalizations
from tests import test_comment_fields_write as catalog_helpers

catalog = catalog_helpers.catalog


@pytest.fixture(params=["HHS-OASH-2022-0006-0002", "FWS-R4-ES-2020-0109-0210"])
def original(request):
    body = (Path(__file__).parent / "fixtures/comment_attachment_unknown_sizes" / f"{request.param}.json").read_bytes()
    return json.loads(body), hashlib.sha256(body).hexdigest()


def test_original_to_receipt_to_native_and_historical_negative(original):
    raw, digest = original
    context = ReceiptContext(
        "g",
        "attempt",
        "real-original-probe",
        [
            {
                "source_id": raw["data"]["id"],
                "source_uri": None,
                "sha256": digest,
                "locator": "/",
                "body_version": raw["data"]["attributes"]["modifyDate"],
            }
        ],
    )
    native, receipt = map_regulations_attempt(
        "comments", raw, context, project=lambda value: _extract_comment(dict(value))
    )
    assert receipt["outcome"] == "accepted"
    diagnostics = decode_exact_json(receipt["diagnostic_json"])["attachment_size_normalizations"]
    assert len(diagnostics) == (1 if raw["data"]["id"].startswith("HHS") else 2)
    assert all(d["original"] == -1 and d["normalized"] is None and "unknown" in d["reason"] for d in diagnostics)
    processing = decode_exact_json(receipt["processing_json"])
    assert processing["raw_source_record"] == raw
    assert [r["attachment_id"] for r in native["attachments"]] == [r["id"] for r in raw["included"]]
    for before, after in zip(raw["included"], native["attachments"], strict=True):
        value = before["attributes"]["fileFormats"][0]["size"]
        assert after["formats"][0]["size"] == (None if value == -1 else value)
    converted = _processor_input("comments", native | processing)
    assert converted["attachments_json"] == _extract_comment(raw)["attachments_json"]
    # Earlier native generations really published -1. Their exact input remains readable,
    # while a new write uses None and keeps the original value plus reason in its receipt.
    prior = deepcopy(native)
    for before, after in zip(raw["included"], prior["attachments"], strict=True):
        after["formats"][0]["size"] = before["attributes"]["fileFormats"][0]["size"]
    assert _processor_input("comments", prior | processing) == converted
    prior["attachments"][0]["title"] = "unrelated mutation"
    with pytest.raises(ValueError, match="differs from selected native subject"):
        _processor_input("comments", prior | processing)
    assert _attachments(pa.array([converted["attachments_json"]]))[0].as_py()["reference"]


def test_promoted_negative_fill_is_narrow_and_preserves_id_guards(original):
    raw, _ = original
    read = _extract_comment(raw)["attachments_json"]
    entries = json.loads(read)
    previous = [{k: v for k, v in item.items() if k in {"title", "formats"}} for item in entries]
    native = _native("comments", "attachments_json", json.dumps(previous))
    held = pa.array([native], type=_v2_attachment_type()).to_pylist()[0]
    for stated, target in zip(previous, held, strict=True):
        target["formats"][0]["size"] = stated["formats"][0]["size"]
    encoded = json.dumps(held, ensure_ascii=False, separators=(",", ":"))
    assert only_adds_attachments(read, encoded, True)
    assert not only_adds_attachments(read, encoded, False)
    changed = deepcopy(held)
    changed[0]["title"] = "different"
    assert not only_adds_attachments(read, json.dumps(changed, ensure_ascii=False, separators=(",", ":")), True)
    changed = deepcopy(held)
    changed[0]["attachment_id"] = "different"
    assert not only_adds_attachments(read, json.dumps(changed, ensure_ascii=False, separators=(",", ":")), True)
    changed = deepcopy(held)
    for entry in changed:
        if entry["formats"][0]["size"] == -1:
            entry["formats"][0]["size"] = -2
            break
    assert not only_adds_attachments(read, json.dumps(changed, ensure_ascii=False, separators=(",", ":")), True)


def test_legacy_comparison_cannot_turn_an_unstated_size_into_a_match():
    held = [{"formats": [{"size": -1}]}]
    assert normalize_legacy_comment_sizes(held, '[{"formats":[{"size":null}]}]') == held


def test_valid_unstated_attachment_attributes_need_no_normalization():
    assert comment_size_normalizations({"data": {}, "included": [{"type": "attachments", "attributes": None}]}) == []


def test_native_flat_replay_does_not_import_optional_source_readers(original, monkeypatch):
    import builtins

    raw, digest = original
    flat = _extract_comment(raw)
    actual_import = builtins.__import__

    def without_source_readers(name, *args, **kwargs):
        if name == "spicy_docs" or name.startswith("spicy_docs."):
            raise ModuleNotFoundError("source-readers are deliberately unavailable")
        return actual_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_source_readers)
    context = ReceiptContext("g", "attempt", "base-replay", [{"source_id": raw["data"]["id"], "sha256": digest}])
    native, receipt = map_regulations_attempt("comments", flat, context)
    processing = decode_exact_json(receipt["processing_json"])
    assert receipt["outcome"] == "accepted"
    assert _processor_input("comments", native | processing)["attachments_json"] == flat["attachments_json"]


@pytest.mark.parametrize("size", [-2, "-1", -1.0, True, 2**63])
def test_invalid_native_sizes_still_refuse(size):
    with pytest.raises(ValueError):
        _native("comments", "attachments_json", json.dumps([{"formats": [{"size": size}]}]))


@pytest.mark.parametrize("held_negative", [False, True])
def test_same_version_fill_writes_explained_native_unknowns(original, held_negative, catalog, tmp_path, monkeypatch):
    from spicy_regs.pipelines import comment_fields_write as writer
    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import regulatory_catalog
    from spicy_regs.transforms import regulations_shape, regulations_receipts
    from tests.test_comment_fields_write import seed, reads, table

    raw, _ = original
    extracted = _extract_comment(raw)
    earlier = [
        {"title": item["title"], "formats": item["formats"]} for item in json.loads(extracted["attachments_json"])
    ]
    prior = {key: None if extracted.get(key) is None else str(extracted[key]) for key in COMMENT.schema}
    prior["attachments_json"] = json.dumps(earlier) if held_negative else None
    # Reproduce the historical writer's literal-size behavior for the prior generation only.
    # The source is still the exact observed original, and its prior receipt remains retained.
    with monkeypatch.context() as legacy:
        legacy.setattr(regulations_shape, "_comment_formats", lambda value, field: value)
        legacy.setattr(regulations_receipts, "_normalization_context", lambda dataset, shaped, context: context)
        seed(catalog, [prior])
    if held_negative:
        with catalog() as con:
            [held] = con.execute(f"SELECT attachments FROM {regulatory_catalog.qualified(COMMENT)}").fetchone()
        assert any(item["formats"][0]["size"] == -1 for item in held)
    reads(
        tmp_path,
        [
            {
                "key": "observed.json",
                "comment_id": extracted["comment_id"],
                "modify_date": extracted["modify_date"],
                "attachments_json": extracted["attachments_json"],
            }
        ],
        agency=extracted["agency_code"],
    )
    with catalog() as con:
        prepared = writer.prepare(tmp_path, profile="attachments", con=con)
        assert prepared["rows_to_fill"] == 1 and prepared["other_version_only"] == 0
        written = writer.write(tmp_path, con=con)
        assert written["rows_changed"] == 1
        [native] = con.execute(f"SELECT attachments FROM {regulatory_catalog.qualified(COMMENT)}").fetchone()
        [diagnostic] = con.execute(
            f"SELECT diagnostic_json FROM {regulatory_catalog.receipts_table()} "
            "WHERE dataset='comments' AND outcome='accepted'"
        ).fetchone()
    assert [item["attachment_id"] for item in native] == [item["id"] for item in raw["included"]]
    for before, after in zip(raw["included"], native, strict=True):
        size = before["attributes"]["fileFormats"][0]["size"]
        assert after["formats"][0]["size"] == (None if size == -1 else size)
    assert decode_exact_json(diagnostic)["attachment_size_normalizations"]
    assert table(catalog)[extracted["comment_id"]]["attachments_json"] == extracted["attachments_json"]
