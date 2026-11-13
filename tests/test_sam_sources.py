"""Local SAM delivery and failure controls; these fixtures are not a live census."""

import csv
import hashlib
import io
import json
from zipfile import ZipFile

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.sam_public import REQUIRED, csv_records, download_url
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.contract_types import described_schema
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import sam_sources as pipeline
from spicy_regs.transforms import build_sam_sources as module


def csv_body(kind="exclusions", rows=None):
    columns = sorted(REQUIRED[kind] | ({"Record Status", "Creation_Date", "CAGE"} if kind == "exclusions" else set()))
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(columns)
    for row in rows if rows is not None else [{"SAM Number":"S4NATIVE", "Name":"Not registered",
                                              "Additional Comments":"PII data has been masked from view"}]:
        writer.writerow([row.get(key, "") for key in columns])
    return stream.getvalue().encode()


def capture(body, kind="exclusions", filename="SAM_Exclusions_Public_Extract_V2_26316.ZIP", observed="2026-11-13T12:00:00Z"):
    return {"request_url": download_url(kind, filename), "response_sha256":"sha256:"+hashlib.sha256(body).hexdigest(),
            "byte_size":len(body), "observed_at":observed}


def test_literal_exclusions_blank_uei_repeats_and_raw_fields_survive(tmp_path):
    body=csv_body(rows=[{"SAM Number":"S4","Name":"One","Unique Entity ID":""},
                        {"SAM Number":"S4","Name":"Two","Termination Date":"Indefinite"}])
    paths,summary=module.build_sam_source(csv_records(io.BytesIO(body),kind="exclusions"),tmp_path,
        kind="exclusions", capture=capture(body), selection={"population":"all-rows-in-public-extract"})
    rows=pq.read_table(paths[0]).to_pylist()
    assert [r["native_id"] for r in rows]==["S4","S4"]
    assert rows[0]["uei"]=="" and rows[1]["termination_date_raw"]=="Indefinite"
    assert json.loads(rows[0]["raw_fields_json"])["Unique Entity ID"]==""
    assert summary["source_rows"]==summary["selected_rows"]==2 and summary["distinct_native_ids"]==1
    assert "exclusion_status_flag" not in pq.read_schema(paths[0]).names


def test_notice_window_keeps_amendments_missing_deadlines_and_no_award_join(tmp_path):
    body=csv_body("opportunities",[{"NoticeId":"n1","CGAC":"047","PostedDate":"2026-11-01 12:00:00","Title":"original"},
        {"NoticeId":"n1","CGAC":"047","PostedDate":"2026-11-02 12:00:00","Title":"revision","ResponseDeadLine":"2026-11-20"},
        {"NoticeId":"n2","CGAC":"047","PostedDate":"2026-10-31"},
        {"NoticeId":"n3","CGAC":"097","PostedDate":"2026-11-02"}])
    paths,summary=module.build_sam_source(csv_records(io.BytesIO(body),kind="opportunities"),tmp_path,
        kind="opportunities",capture=capture(body,"opportunities","ContractOpportunitiesFullCSV.csv"),
        selection={"agency_code":"047","posted_from":"2026-11-01","posted_to":"2026-11-07"})
    rows=pq.read_table(paths[0]).to_pylist()
    assert [row["title_raw"] for row in rows]==["original","revision"]
    assert rows[0]["response_deadline_raw"]=="" and rows[1]["response_deadline_raw"]=="2026-11-20"
    assert summary["source_rows"]==4 and summary["selected_rows"]==2


def test_parse_failure_does_not_replace_previous_table(tmp_path):
    body=csv_body()
    options=dict(kind="exclusions",capture=capture(body),selection={"population":"all-rows-in-public-extract"})
    paths,_=module.build_sam_source(csv_records(io.BytesIO(body),kind="exclusions"),tmp_path,**options)
    before={p.name:p.read_bytes() for p in paths}
    def broken():
        yield from csv_records(io.BytesIO(body),kind="exclusions")
        raise OSError("truncated retained source")
    with pytest.raises(OSError,match="truncated"):
        module.build_sam_source(broken(),tmp_path,**options)
    assert {p.name:p.read_bytes() for p in paths}==before


def test_valid_empty_retains_population_summary(tmp_path):
    body=csv_body(rows=[])
    paths,summary=module.build_sam_source(csv_records(io.BytesIO(body),kind="exclusions"),tmp_path,
        kind="exclusions",capture=capture(body),selection={"population":"all-rows-in-public-extract"})
    assert pq.read_table(paths[0]).num_rows==0
    assert summary["source_rows"]==summary["selected_rows"]==0
    assert pq.read_table(paths[1]).num_rows==1


def test_native_generation_replay_query_and_update(tmp_path,monkeypatch):
    raw=tmp_path/"raw"
    store=CentralRawArchiveStore(raw)
    summaries=[]
    outputs=[]
    for index,comment in enumerate(["original explanation","updated explanation"]):
        stream=io.BytesIO()
        with ZipFile(stream,"w") as archive:
            archive.writestr("SAM_Exclusions_Public_Extract_V2_26316.CSV",csv_body(rows=[
                {"SAM Number":"S4NATIVE","Name":"Unregistered party","Additional Comments":comment}]))
        body=stream.getvalue()
        descriptor=capture(body)
        store.put_blob(descriptor["response_sha256"],len(body),[body])
        output=tmp_path/str(index)
        artifact,summary=pipeline.build("exclusions","SAM_Exclusions_Public_Extract_V2_26316.ZIP",descriptor,
            {"population":"all-rows-in-public-extract"},output,archive_root=raw)
        assert artifact.pin.artifact_digest.startswith("sha256:")
        monkeypatch.setattr(mcp_server,"DATA_DIR",output/"generation")
        with mcp_server._build_connection() as con:
            row=con.execute("SELECT native_id,name_raw,uei,additional_comments_raw,source_sha256 FROM sam_exclusion_observations WHERE native_id='S4NATIVE'").fetchone()
        assert row[:4]==("S4NATIVE","Unregistered party","",comment)
        assert summary["text_encoding"] == "utf-8-sig"
        summaries.append(summary)
        outputs.append(output)
    assert summaries[0]["snapshot_id"]!=summaries[1]["snapshot_id"]
    assert pq.read_table(outputs[0]/"generation/sam_exclusion_observations.parquet")["additional_comments_raw"].to_pylist()==["original explanation"]
    with pytest.raises(ValueError,match="new SAM generation"):
        pipeline.build("exclusions","SAM_Exclusions_Public_Extract_V2_26316.ZIP",descriptor,
            {"population":"all-rows-in-public-extract"},outputs[-1],archive_root=raw)


def test_ordinary_acquisition_retains_public_redirects_without_signed_query(tmp_path,monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT",str(tmp_path/"raw"))
    target="https://falextracts.s3.amazonaws.com/Exclusions/Public%20V2/SAM_Exclusions_Public_Extract_V2_26316.ZIP?X-Amz-Signature=private-signature"
    calls=[]
    body=b"PK\x03\x04bounded-fixture"
    def answer(request):
        calls.append(str(request.url))
        if request.url.host=="sam.gov":
            return httpx.Response(303,headers={"Location":target},stream=httpx.ByteStream(b""))
        return httpx.Response(200,headers={"Content-Type":"application/zip"},stream=httpx.ByteStream(body))
    out=tmp_path/"acquire"
    result=pipeline.acquire("exclusions","SAM_Exclusions_Public_Extract_V2_26316.ZIP",out,max_bytes=1024,
                            transport=httpx.MockTransport(answer))
    assert result["request_url"]==download_url("exclusions","SAM_Exclusions_Public_Extract_V2_26316.ZIP")
    assert result["byte_size"]==len(body) and len(calls)==2
    text="\n".join(path.read_text() for path in out.rglob("*.jsonl"))
    assert "private-signature" not in text


def test_installed_schemas_and_source_descriptions_are_consumable():
    schemas=data_dictionary.expected_schemas()
    curated=data_dictionary.load_curated_descriptions()
    for name,schema in module.SCHEMAS.items():
        assert schemas[name]==described_schema(schema)
        assert set(curated[name]["columns"])==set(schema.names)
        assert name in mcp_server.TABLES
    index={"format":"spicy-regs-publication","version":2,"families":{
        "sam-exclusions":{"artifactDigest":"sha256:"+"0"*64,"tables":{name+".parquet":{"columns":[list(pair) for pair in described_schema(module.SCHEMAS[name])]}
            for name in [module.TABLES["exclusions"],module.SNAPSHOTS["exclusions"]]}}}}
    bundle=build_bundle(index)
    assert bundle["tables"]["sam_exclusion_observations"]["sources"][0]["id"]=="sam-public"


def test_byte_preserving_notice_decoding_records_its_uncertainty(tmp_path):
    body=csv_body("opportunities",[{"NoticeId":"x","CGAC":"047","PostedDate":"2026-11-01",
                                   "Title":"Ja\u0092Quan"}]).decode().encode("latin-1")
    options=dict(kind="opportunities",capture=capture(body,"opportunities","ContractOpportunitiesFullCSV.csv"),
        selection={"agency_code":"047","posted_from":"2026-11-01","posted_to":"2026-11-07"})
    paths,summary=module.build_sam_source(csv_records(io.BytesIO(body),kind="opportunities",encoding="latin-1"),
                                        tmp_path,**options,text_encoding="latin-1")
    row=pq.read_table(paths[0]).to_pylist()[0]
    assert row["title_raw"].encode("latin-1")==b"Ja\x92Quan"
    assert row["text_encoding"]==summary["text_encoding"]=="latin-1"
    assert row["text_interpretation_status"] == "byte-preserving-unqualified"
    assert json.loads(row["unqualified_text_fields_json"]) == ["Title"]
    assert summary["unqualified_text_rows"] == 1
