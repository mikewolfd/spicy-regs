"""Role-specific citations remain tied to source fields and complete native keys."""

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from spicy_docs.extraction.body_text import rendition_text
from spicy_docs.sources.congress.record_communications import parse_record_communications
from spicy_docs.schemas.congress_index_tables import shape_record_communication
from spicy_regs.transforms.held_citations import Selection, build_held_citations


def test_native_communication_roles_exact_spans_and_refused_split(tmp_path):
    body = (
        Path(__file__).parent / "fixtures/communication_citations/CREC-2016-02-12-pt1-PgH815-4.excerpt.htm"
    ).read_bytes()
    entries = parse_record_communications(
        rendition_text(body, rendition="htm").text,
        package_id="CREC-2016-02-12",
        granule_id="CREC-2016-02-12-pt1-PgH815-4",
    )
    source = [shape_record_communication(e, congress=114, record_date="2016-02-12") for e in entries]
    assert next(e for e in entries if e.number == 4340).split_resolved is False
    columns = ("congress", "communication_type", "number", "legal_authority", "report_nature", "record_entry_text")
    table = pa.Table.from_pylist(
        [{k: r[k] for k in columns} for r in source], schema=pa.schema([(k, pa.string()) for k in columns])
    )
    with duckdb.connect() as con:
        con.register("house_communications", table)
        path = build_held_citations(
            tmp_path,
            cursor=con,
            selections=[
                Selection(kind, ("114", "ec", "4329"))
                for kind in ("communication_authority", "communication_report_nature", "communication_record_entry")
            ],
            input_pins={"house_communications": {"sha256": "sha256:" + hashlib.sha256(body).hexdigest()}},
            download_prior=lambda *_: False,
        )
    rows = pq.read_table(path).to_pylist()
    authority = [r for r in rows if r["document_kind"] == "communication_authority"]
    assert {r["target_key"] for r in authority} >= {"5-801", "104-public-121"}
    for row in rows:
        key = json.loads(row["document_key"])
        assert key == ["114", "ec", "4329"]
        src = next(r for r in source if r["number"] == "4329")
        field = {
            "communication_authority": "legal_authority",
            "communication_report_nature": "report_nature",
            "communication_record_entry": "record_entry_text",
        }[row["document_kind"]]
        text = src[field]
        assert isinstance(text, str)
        assert text[int(row["span_start"]) : int(row["span_end"])] == row["matched_text"]
        assert row["text_sha256"] == "sha256:" + hashlib.sha256(text.encode()).hexdigest()
