"""Native retained PDF derivations qualify bounded court adapters, not all courts."""

from collections import Counter
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.citation_sources import document_key, source_digests
from spicy_regs.transforms.held_citations import Selection, build_held_citations, NAMESPACE
from spicy_regs.transforms.read_checkpoints import read_checkpoints


def test_retained_court_text_pdf_keys_spans_and_source_specific_abstention(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures/court_citations/derived-text-cohort.json").read_text())
    source = fixture["rows"]
    kind = "court_opinion_derived_pdf"
    with duckdb.connect() as con:
        con.register("court_opinion_pdf_extractions", pa.Table.from_pylist(source))
        path = build_held_citations(
            tmp_path,
            cursor=con,
            selections=[Selection(kind, (r["opinion_id"], r["source_sha256"])) for r in source],
            input_pins={"court_opinion_pdf_extractions": {"artifactDigest": fixture["publication"]}},
            download_prior=lambda *_: False,
        )
        rows = pq.read_table(path).to_pylist()
        assert Counter(json.loads(r["document_key"])[0] for r in rows) == {"11264531": 12, "11209378": 6}
        assert not any(r["cite_kind"] == "case_docket_number" for r in rows)  # Governor proclamation No.20-50.
        for r in rows:
            opinion, pdf_sha = json.loads(r["document_key"])
            native = next(x for x in source if x["opinion_id"] == opinion and x["source_sha256"] == pdf_sha)
            text = native["text_content"]
            digest = "sha256:" + hashlib.sha256(text.encode()).hexdigest()
            assert r["text_sha256"] == digest and digest != pdf_sha
            assert text[int(r["span_start"]) : int(r["span_end"])] == r["matched_text"]
            assert r["body_rendition"] == "pdf" and r["body_derivation"].startswith("derived_pdf:")
            assert source_digests(con, kind, r["document_key"]) == [(digest,)]
        # Changing only the PDF digest must not select the old body's text.
        assert source_digests(con, kind, document_key(kind, ("11264531", "sha256:" + "0" * 64))) == []
    checkpoints = read_checkpoints(path, NAMESPACE)
    assert len(checkpoints) == 3
    zero = next(c for c in checkpoints if json.loads(c["document_key"])[0] == "11264529")
    assert zero["findings"] == 0
    assert all(
        "case_docket_number" in json.loads(c["processing_version"])["excluded_source_rules"] for c in checkpoints
    )
