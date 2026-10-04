"""A retained GAO caption supplies every number and the decision day independently of release day."""
from datetime import date
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.gao_decision_pages import DecisionPageCapture
from spicy_regs.transforms.build_gao_reports import _build_decisions, _decision_rows
from tests.test_gao_listing import _decision, _run


def test_caption_survives_native_etl_and_listing_only_refresh(tmp_path, monkeypatch):
    from spicy_regs.transforms import build_gao_reports as builder
    from importlib import import_module
    module = import_module(builder.__module__)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    capture = tmp_path / "capture"
    blobs = capture / "blobs/sha256"
    blobs.mkdir(parents=True)
    body = (Path(__file__).parent / "fixtures/gao_decision_pages/b-424477.html").read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    link = "/products/b-424477%2Cb-424477.2"
    (blobs / digest).write_bytes(body)
    (capture / "receipts.jsonl").write_text(json.dumps({"url": "https://www.gao.gov" + link, "status_code": 200, "sha256": digest}) + "\n")
    item = _decision(link, "B-424477", released="2026-08-28")
    item.status, item.listing_page = "We deny the protest.", None
    rows = _decision_rows(_run(decisions=[item]), capture)
    out = _build_decisions(tmp_path, rows)
    row, = pq.read_table(out).to_pylist()
    assert row["b_numbers"] == ["B-424477", "B-424477.2"]
    assert row["decided_date"] == date(2026, 8, 7)
    assert row["released_date"] == date(2026, 8, 28)
    out.rename(tmp_path / "_gao_decisions_prior.parquet")
    refreshed = _build_decisions(tmp_path, _decision_rows(_run(decisions=[item])))
    again, = pq.read_table(refreshed).to_pylist()
    assert again == row
    (blobs / digest).write_bytes(b"changed")
    with pytest.raises(ValueError, match="differs from its receipt"):
        DecisionPageCapture(capture).page("https://www.gao.gov" + link)
