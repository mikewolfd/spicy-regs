"""Tests for receipts-free streaming 50-table ``fec-query`` builder & ``/2`` identity (ss-42zk / ss-42nr)."""

from __future__ import annotations

from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
from zipfile import ZipFile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.fec.delimited import verified_archive_session
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.fec_financial_rules import IDENTITY_VERSION, LEGACY_IDENTITY_VERSION
from spicy_regs.generations import admit_generation_for_publication
from spicy_regs.relationship_views.fec_filing_associations import (
    LEGACY_TARGET_IDENTITY_VERSION,
    TARGET_IDENTITY_VERSION,
)
from spicy_regs.transforms.fec_agency import WORD, AgencySelection
from spicy_regs.transforms.fec_bulk_financial import OPPEXP_FIELDS
from spicy_regs.transforms.fec_bulk_selection import (
    build_archive_inner_member_proof,
    build_weball_webl_subset_proof,
)
from spicy_regs.transforms.fec_filing_financial import RECEIPTS, load_layout
from spicy_regs.transforms.fec_filing_forms import filing_text_mapping_for
from spicy_regs.transforms.fec_identity_observations import QUALITY_NOTICE
from spicy_regs.transforms.fec_query import (
    CollectionSelection,
    legacy_observation_id,
    observation_id,
)
from spicy_regs.transforms.fec_query_builder import (
    RETIRED_FEC_QUERY_TABLES,
    FecQueryBuildInputs,
    _authority_for_collection,
    _extract_cycle_from_url,
    build_fec_query_from_inputs,
    build_fec_query_generation,
    classify_collection,
    discover_retained_mur_captures,
    load_fec_typed_schemas,
    load_verified_checkpoint,
    make_filing_body_reader,
    verify_fec_query_parity,
)
from spicy_regs.transforms.fec_repaired_builder import build_repaired_fec_tables
from spicy_regs.transforms.fec_summaries import CANDIDATE_WEB, map_summary
from tests.test_fec_original_tables import DETAIL, TOTAL, admit, ie_workbook

GEN = "sha256:" + "a" * 64
GEN_PRIOR = "sha256:" + "0" * 64
EVIDENCE = "sha256:" + "b" * 64
FILING_SHA = "sha256:" + "1" * 64
ARCHIVE_SHA = "sha256:" + "2" * 64
OPPEXP_SHA = "sha256:" + "3" * 64
OPPEXP_HDR_SHA = "sha256:" + "4" * 64
AGG_SHA = "sha256:" + "5" * 64
WEBALL_SHA = "sha256:" + "6" * 64
WEBL_SHA = "sha256:" + "7" * 64
NOTICE_SHA = "sha256:" + "8" * 64
LEGAL_SHA = "sha256:" + "9" * 64
AGENCY_OVERSIGHT_SHA = "sha256:" + "c" * 64
AGENCY_WORD_SHA = "sha256:" + "d" * 64
INDIV_SHA = "sha256:" + "e" * 64


def _locator(cid: str, rid: str, **extra) -> str:
    return json.dumps({"collection_id": cid, "source_record_id": rid, **extra}, sort_keys=True, separators=(",", ":"))


def _sel(cid: str, sha: str, cycle: int | None = 2026) -> CollectionSelection:
    return CollectionSelection(cid, sha, GEN, "official-fec", cycle, "snapshot", EVIDENCE)


def _shared_repaired_fixture():
    """Shared realistic fixture covering all repaired ``ss-u9s8`` families for oracle parity testing."""
    f_domain_sel = _sel("filings-f13-selected", FILING_SHA, None)
    f_dup_sel = _sel("retained-api-003bf112d9dfbfa667ff", FILING_SHA, None)
    filing_1981955 = dict(
        file_number=1981955,
        sub_id="sub-1981955",
        committee_id="C00677286",
        form_type="F3",
        fec_url="https://docquery.fec.gov/dcdev/posted/1981955.fec",
        amendment_chain=[1981955, 1889009],
    )
    filing_1889009 = dict(
        file_number=1889009,
        sub_id="sub-1889009",
        committee_id="C00696948",
        form_type="F3P",
        fec_url="https://docquery.fec.gov/dcdev/posted/1889009.fec",
    )
    filing_metadata_rows = [
        (
            dict(
                collection_id="filings-f13-selected",
                source_record_id="f/0",
                source_sha256=FILING_SHA,
                source_url="https://api.open.fec.gov/v1/filings/",
                profile="filing",
                source_locator_json=_locator("filings-f13-selected", "f/0", ordinal=0),
                metadata_json=json.dumps(filing_1981955),
            ),
            f_domain_sel,
        ),
        (
            dict(
                collection_id="filings-f13-selected",
                source_record_id="f/1",
                source_sha256=FILING_SHA,
                source_url="https://api.open.fec.gov/v1/filings/",
                profile="filing",
                source_locator_json=_locator("filings-f13-selected", "f/1", ordinal=1),
                metadata_json=json.dumps(filing_1889009),
            ),
            f_domain_sel,
        ),
        (
            dict(
                collection_id="retained-api-003bf112d9dfbfa667ff",
                source_record_id="dup/0",
                source_sha256=FILING_SHA,
                source_url="https://api.open.fec.gov/v1/filings/",
                profile="document",
                source_locator_json=_locator("retained-api-003bf112d9dfbfa667ff", "dup/0", ordinal=0),
                metadata_json=json.dumps({"kind": "api-record-observation", "metadata": filing_1981955}),
            ),
            f_dup_sel,
        ),
    ]

    archive_url = (
        "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com"
        "/bulk-downloads/electronic/20260609.zip"
    )
    member = dict(ordinal=135, name="1981955.fec")
    arch_cid = "retained-filing-archive-fb814c4311cb-0135"
    arch_sel = _sel(arch_cid, ARCHIVE_SHA, None)
    hdr_row = dict(
        collection_id=arch_cid,
        source_record_id="hdr/0",
        source_sha256=ARCHIVE_SHA,
        source_locator_json=_locator(arch_cid, "hdr/0", sha256=ARCHIVE_SHA, ordinal=0, member=member),
        metadata_json=json.dumps(dict(kind="header", format_version="8.5", fields=["HDR", "FEC", "8.5"])),
    )
    sa_layout = load_layout(RECEIPTS)
    sa_fields = dict.fromkeys(map(str, range(len(sa_layout["fields"]))), "")
    sa_fields.update({"0": "SA11AI", "1": "C00677286", "2": "SA11A.1", "7": "Smith", "19": "20260601", "20": "250.50"})
    sa_row = dict(
        collection_id=arch_cid,
        source_record_id="row/1",
        source_sha256=ARCHIVE_SHA,
        source_locator_json=_locator(arch_cid, "row/1", sha256=ARCHIVE_SHA, ordinal=1, member=member),
        metadata_json=json.dumps(dict(kind="record", record_type="SA11AI", field_count=len(sa_layout["fields"]), fields=sa_fields)),
    )
    text_mapping = filing_text_mapping_for("8.5", "TEXT")
    assert text_mapping is not None
    text_layout = load_layout(text_mapping)
    text_fields = dict.fromkeys(map(str, range(len(text_layout["fields"]))), "")
    text_fields.update({"0": "TEXT", "1": "C00677286"})
    del text_fields["5"]
    body_ref = dict(sha256=ARCHIVE_SHA, byte_offset=10, byte_length=15, encoding="utf-8", delimiter="\x1c", field_index=5)
    text_row = dict(
        collection_id=arch_cid,
        source_record_id="row/2",
        source_sha256=ARCHIVE_SHA,
        source_locator_json=_locator(
            arch_cid, "row/2", sha256=ARCHIVE_SHA, ordinal=2, member=member, embedded_bodies=[body_ref]
        ),
        metadata_json=json.dumps(
            dict(kind="record", record_type="TEXT", field_count=len(text_layout["fields"]), fields=text_fields, embedded_bodies=[body_ref])
        ),
    )

    opp_sel = _sel("bulk-oppexp-2026", OPPEXP_SHA, 2026)
    opp_hdr = dict(
        collection_id="bulk-oppexp-header",
        source_record_id="hdr/0",
        source_sha256=OPPEXP_HDR_SHA,
        source_locator_json=_locator("bulk-oppexp-header", "hdr/0", ordinal=0),
        metadata_json=json.dumps({"kind": "row", "fields": list(OPPEXP_FIELDS)}),
    )
    opp_fields = [
        "C00696948", "N", "2025", "M4", "202504189756130114", "23", "F3P", "SB", "ADP, LLC", "EL PASO", "TX",
        "799128023", "03/28/2025", "3716.38", "P2020", "PROCESSING FEES", "", "", "", "", "ORG",
        "4042220251186031739", "1889009", "500201474", "", "",
    ]
    opp_row = dict(
        collection_id="bulk-oppexp-2026",
        source_record_id="opp/0",
        source_sha256=OPPEXP_SHA,
        source_locator_json=_locator("bulk-oppexp-2026", "opp/0", ordinal=0),
        metadata_json=json.dumps({"kind": "row", "fields": opp_fields}),
    )

    agg_sel = _sel("bulk-agg-2024", AGG_SHA, 2024)
    agg_rows = [
        (
            dict(
                collection_id="bulk-agg-2024",
                source_record_id="agg/0",
                source_sha256=AGG_SHA,
                source_locator_json=_locator("bulk-agg-2024", "agg/0", ordinal=0),
                metadata_json=json.dumps(
                    dict(CANDIDATE_ID="P00000001", CANDIDATE_NAME="All candidates,", CONTRIB_RECEIPT_AMOUNT="1000.00", SIZE_OF_CONTRIBUTION="200")
                ),
            ),
            agg_sel,
            "size-band",
        ),
        (
            dict(
                collection_id="bulk-agg-2024",
                source_record_id="agg/1",
                source_sha256=AGG_SHA,
                source_locator_json=_locator("bulk-agg-2024", "agg/1", ordinal=1),
                metadata_json=json.dumps(
                    dict(CANDIDATE_ID="P80001571", CANDIDATE_NAME="CAND REAL", CONTRIB_RECEIPT_AMOUNT="500.00", SIZE_OF_CONTRIBUTION="200")
                ),
            ),
            agg_sel,
            "size-band",
        ),
    ]

    web_fields = {k: "" for k in set(CANDIDATE_WEB.money_fields) | set(CANDIDATE_WEB.attributes)}
    web_fields.update(CAND_ID="H4MI05180", CAND_NAME="HOUSE CAND", CVG_END_DT="12/31/2024", TTL_RECEIPTS="4200.00")
    pres_fields = {**web_fields, "CAND_ID": "P80001571", "CAND_NAME": "PRES CAND", "TTL_RECEIPTS": "9900.00"}
    weball_sel = _sel("bulk-weball24-zip", WEBALL_SHA, 2024)
    webl_sel = _sel("bulk-webl24-zip", WEBL_SHA, 2024)
    summary_inputs = [
        (
            dict(
                collection_id="bulk-weball24-zip",
                source_record_id="wa/0",
                source_sha256=WEBALL_SHA,
                source_locator_json=_locator("bulk-weball24-zip", "wa/0", ordinal=0),
                metadata_json=json.dumps(web_fields),
            ),
            weball_sel,
            CANDIDATE_WEB,
        ),
        (
            dict(
                collection_id="bulk-weball24-zip",
                source_record_id="wa/1",
                source_sha256=WEBALL_SHA,
                source_locator_json=_locator("bulk-weball24-zip", "wa/1", ordinal=1),
                metadata_json=json.dumps(pres_fields),
            ),
            weball_sel,
            CANDIDATE_WEB,
        ),
        (
            dict(
                collection_id="bulk-webl24-zip",
                source_record_id="wl/0",
                source_sha256=WEBL_SHA,
                source_locator_json=_locator("bulk-webl24-zip", "wl/0", ordinal=0),
                metadata_json=json.dumps(web_fields),
            ),
            webl_sel,
            CANDIDATE_WEB,
        ),
    ]

    notice_sel = _sel("bulk-notices", NOTICE_SHA, None)
    notice_rows = [
        (
            dict(
                collection_id="bulk-notices",
                source_record_id="qn/0",
                source_sha256=NOTICE_SHA,
                source_url="https://www.fec.gov/bulk-downloads/data.fec.gov/FalseFictitiousFilings.csv",
                observed_at="2026-09-12T13:43:15+00:00",
                source_locator_json=_locator("bulk-notices", "qn/0", ordinal=0),
                metadata_json=json.dumps(
                    dict(committee_id="P20000055", committee_name="Fictitious Cand", filed_cmte_tp_desc="P", filings_url="", first_receipt_dt="31-DEC-24")
                ),
            ),
            notice_sel,
            QUALITY_NOTICE,
        )
    ]

    legal_sel = _sel("af-2025", LEGAL_SHA, None)
    af_row = (
        dict(
            collection_id="af-2025",
            source_record_id="af/0",
            source_sha256=LEGAL_SHA,
            source_url="https://api.open.fec.gov/v1/legal/search/",
            profile="document",
            source_locator_json=_locator("af-2025", "af/0", pointer="/admin_fines/0"),
            metadata_json=json.dumps(
                dict(
                    kind="api-record-observation",
                    metadata=dict(
                        type="admin_fines",
                        no="42",
                        name="Subject PAC",
                        committee_id="C00677286",
                        final_determination_amount="1250.00",
                        open_date="2025-01-15",
                        close_date=None,
                        commission_votes=[{"action": "Approved", "vote_date": "2025-02-01"}],
                    ),
                )
            ),
        ),
        legal_sel,
    )
    mur_bytes = json.dumps({"docs": [{"type": "murs", "no": "1604", "mur_name": "MUR 1604", "open_date": "2004-01-01", "close_date": None}]}).encode()
    mur_sha = "sha256:" + hashlib.sha256(mur_bytes).hexdigest()
    mur_captures = [
        dict(
            collection_id="native-context-mur-1604",
            source_sha256=mur_sha,
            source_url="https://api.open.fec.gov/v1/legal/docs/murs/1604?api_key=<redacted>",
            payload_bytes=mur_bytes,
        )
    ]

    wrapper_url = "https://gcc02.safelinks.protection.outlook.com/?url=https%3A%2F%2Fwww.fec.gov%2Foig.pdf&data=05"
    unwrapped_url = "https://www.fec.gov/oig.pdf"
    oversight_rows = [
        dict(
            collection_id="agency-oversight",
            source_record_id="ov/0",
            source_sha256=AGENCY_OVERSIGHT_SHA,
            source_url="https://www.oversight.gov/reports/r1",
            source_locator_json=_locator("agency-oversight", "ov/0", ordinal=0),
            metadata_json=json.dumps(
                dict(kind="oversight-report", report=dict(metadata=dict(title="OIG Report", fields=[]), source=dict(sha256=AGENCY_OVERSIGHT_SHA)))
            ),
        ),
        dict(
            collection_id="agency-oversight",
            source_record_id="ov/1",
            source_sha256=AGENCY_OVERSIGHT_SHA,
            source_url="https://www.oversight.gov/reports/r1",
            source_locator_json=_locator("agency-oversight", "ov/1", ordinal=1),
            metadata_json=json.dumps(
                dict(
                    kind="oversight-asset",
                    ordinal_in_report=0,
                    asset=dict(url=wrapper_url, decoded_wrapper_targets=[unwrapped_url], role="declared_report_link", label="PDF"),
                )
            ),
        ),
    ]

    def _el(tag, parent=None, text=None):
        return dict(tag=tag, parent=parent, text=text, tail=None, attributes={}, child_indices=[], namespace_declarations={})

    w_elems = [
        _el(WORD + "document"),
        _el(WORD + "body", 0),
        _el(WORD + "tbl", 1),
        _el(WORD + "tr", 2),
        _el(WORD + "tc", 3),
        _el(WORD + "p", 4),
        _el(WORD + "r", 5),
        _el(WORD + "t", 6, "AGENCY OVERALL"),
        _el(WORD + "tc", 3),
        _el(WORD + "p", 8),
        _el(WORD + "r", 9),
        _el(WORD + "t", 10, "76"),
    ]
    word_native: list[dict[str, object]] = [
        dict(kind="word-report", report=dict(metadata=dict(representation="word-flat-opc"), source=dict(sha256=AGENCY_WORD_SHA)))
    ]
    word_native += [dict(kind="word-element", ordinal_in_report=i, element=e) for i, e in enumerate(w_elems)]
    word_native += [
        dict(kind="word-body", ordinal_in_report=i, body=dict(element=ix, text=w_elems[ix]["text"], tag=w_elems[ix]["tag"]))
        for i, ix in enumerate([7, 11])
    ]
    word_rows = [
        dict(
            collection_id="agency-word-2009",
            source_record_id=f"w/{i}",
            source_sha256=AGENCY_WORD_SHA,
            source_url="https://www.fec.gov/foiareport2009.xml",
            source_locator_json=_locator("agency-word-2009", f"w/{i}", ordinal=i),
            metadata_json=json.dumps(n),
        )
        for i, n in enumerate(word_native)
    ]

    buf1, buf2 = io.BytesIO(), io.BytesIO()
    with ZipFile(buf1, "w") as z1:
        z1.comment = b"a"
        z1.writestr("cm.txt", b"C00677286|COMMITTEE\n")
    with ZipFile(buf2, "w") as z2:
        z2.comment = b"b"
        z2.writestr("cm.txt", b"C00677286|COMMITTEE\n")
    cm1_bytes, cm2_bytes = buf1.getvalue(), buf2.getvalue()
    cm1_sha = "sha256:" + hashlib.sha256(cm1_bytes).hexdigest()
    cm2_sha = "sha256:" + hashlib.sha256(cm2_bytes).hexdigest()
    cm_proof = build_archive_inner_member_proof(
        {"bulk-cm24-zip": (cm1_sha, cm1_bytes), "retained-committee-master-2024-0131eb48": (cm2_sha, cm2_bytes)},
        member_name="cm.txt",
        selected_collection_id="bulk-cm24-zip",
        source_generation_pin=GEN,
    )
    collections = [
        dict(collection_id="filings-f13-selected", profile="filing", record_count=2, scope=dict(captures=[dict(responseSha256=FILING_SHA)])),
        dict(collection_id="retained-api-003bf112d9dfbfa667ff", profile="document", record_count=1, scope=dict(capture=dict(responseSha256=FILING_SHA))),
        dict(collection_id="bulk-cm24-zip", profile="positional", record_count=1, scope=dict(capture=dict(responseSha256=cm1_sha), member=dict(ordinal=0, name="cm.txt"), format="delimited", delimiter="|", quoting="literal", encoding="utf-8")),
        dict(collection_id="retained-committee-master-2024-0131eb48", profile="positional", record_count=1, scope=dict(capture=dict(responseSha256=cm2_sha), member=dict(ordinal=0, name="cm.txt"), format="delimited", delimiter="|", quoting="literal", encoding="utf-8")),
        dict(collection_id="bulk-weball24-zip", profile="positional", record_count=2, scope=dict(capture=dict(responseSha256=WEBALL_SHA), member=dict(ordinal=0, name="weball24.txt"))),
        dict(collection_id="bulk-webl24-zip", profile="positional", record_count=1, scope=dict(capture=dict(responseSha256=WEBL_SHA), member=dict(ordinal=0, name="webl24.txt"))),
    ]

    mapped_weball = [map_summary(r, s, m)[0] for r, s, m in summary_inputs if r["collection_id"] == "bulk-weball24-zip"]
    mapped_webl = [map_summary(r, s, m)[0] for r, s, m in summary_inputs if r["collection_id"] == "bulk-webl24-zip"]
    web_proof = build_weball_webl_subset_proof(
        [
            dict(
                superset_collection_id="bulk-weball24-zip",
                superset_sha256=WEBALL_SHA,
                superset_rows=mapped_weball,
                subset_collection_id="bulk-webl24-zip",
                subset_sha256=WEBL_SHA,
                subset_rows=mapped_webl,
            )
        ],
        source_generation_pin=GEN,
    )

    return dict(
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
        collections=collections,
        archive_inner_proofs=(cm_proof,),
        weball_webl_proofs=(web_proof,),
        filing_metadata_rows=filing_metadata_rows,
        filing_headers=[hdr_row],
        header_scopes={arch_cid: dict(capture=dict(requestUrl=archive_url, responseSha256=ARCHIVE_SHA), member=member)},
        header_authorities={arch_cid: "official-fec"},
        namespace_evidence={"fec-bulk-operating-expense-ordered-header": EVIDENCE},
        filing_schedule_rows=[(sa_row, arch_sel, RECEIPTS, sa_layout, hdr_row)],
        filing_text_rows=[(text_row, arch_sel, hdr_row, lambda _: "Schedule A note", text_mapping, text_layout)],
        operating_expense_rows=[(opp_row, opp_sel, opp_hdr)],
        contribution_aggregate_rows=agg_rows,
        summary_rows=summary_inputs,
        registry_rows=notice_rows,
        legal_rows=[af_row],
        retained_mur_captures=mur_captures,
        agency_collections=[
            (oversight_rows, AgencySelection("agency-oversight", AGENCY_OVERSIGHT_SHA, GEN, "oversight.gov", EVIDENCE)),
            (word_rows, AgencySelection("agency-word-2009", AGENCY_WORD_SHA, GEN, "fec.gov", EVIDENCE)),
        ],
    )


def _read_generation_table(generation_dir: Path, table: str) -> pa.Table:
    flat = generation_dir / f"{table}.parquet"
    if flat.is_file():
        return pq.ParquetFile(flat).read()
    parts = sorted((generation_dir / table).rglob("*.parquet"))
    assert parts, f"Expected parquet files for {table}"
    return pa.concat_tables([pq.ParquetFile(p).read() for p in parts])


def _bulk_indiv_row(idx: int, amount: str, *, memo: str = "") -> tuple[dict, CollectionSelection]:
    cid = "bulk-indiv-2026"
    rid = f"indiv/{idx}"
    fields = {
        "CMTE_ID": "C00677286",
        "AMNDT_IND": "N",
        "RPT_TP": "Q1",
        "TRANSACTION_PGI": "P2026",
        "IMAGE_NUM": "202604150000000001",
        "TRANSACTION_TP": "15",
        "ENTITY_TP": "IND",
        "NAME": f"DONOR {idx}",
        "CITY": "WASHINGTON",
        "STATE": "DC",
        "ZIP_CODE": "20001",
        "EMPLOYER": "SELF",
        "OCCUPATION": "ENGINEER",
        "TRANSACTION_DT": "03152026",
        "TRANSACTION_AMT": amount,
        "OTHER_ID": "",
        "TRAN_ID": f"T-{idx}",
        "FILE_NUM": "1981955",
        "MEMO_CD": memo,
        "MEMO_TEXT": "",
        "SUB_ID": f"404222026000000{idx:04d}",
    }
    row = dict(
        collection_id=cid,
        source_record_id=rid,
        source_sha256=INDIV_SHA,
        source_locator_json=_locator(cid, rid, ordinal=idx, encoding="utf-8"),
        metadata_json=json.dumps(fields),
    )
    return row, _sel(cid, INDIV_SHA, 2026)


def test_50_table_schema_parity_with_processing_declarations():
    schemas = load_fec_typed_schemas(verify_processing_parity=True)
    assert len(schemas) == 50
    assert RETIRED_FEC_QUERY_TABLES.isdisjoint(schemas)


def test_streaming_builder_matches_repaired_oracle_arrow_for_arrow(tmp_path):
    """Oracle 2 + Oracle 1: streaming 50-table builder equals ``fec_repaired_builder`` row-for-row & seals receipts-free."""
    fx = _shared_repaired_fixture()
    oracle = build_repaired_fec_tables(**fx, output_dir=tmp_path / "oracle")

    inputs = FecQueryBuildInputs(**fx)
    result = build_fec_query_from_inputs(
        inputs,
        output_dir=tmp_path / "streaming",
        batch_rows=2,
        row_group_rows=2,
        part_rows=4,
        c1_threads=2,
    )

    # 1. Receipts-free admission: NO etlReceipts, NO etl_receipts.parquet.
    artifact = admit_generation_for_publication(result.generation_dir)
    assert artifact.pin.artifact_digest == result.artifact_digest
    assert "etlReceipts" not in artifact.root["spec"]
    assert not (result.generation_dir / "etl_receipts.parquet").exists()
    assert len(result.table_rows) == 50

    # 2. Exact 50-table Arrow schema equality against fec_typed_schemas.json == fec_processing_schemas.json.
    declared = load_fec_typed_schemas(verify_processing_parity=True)
    for table_name, expected_schema in declared.items():
        actual_table = _read_generation_table(result.generation_dir, table_name)
        assert actual_table.schema.equals(expected_schema, check_metadata=False), f"Schema mismatch on {table_name}"

    # 3. Partitioned non-empty tables vs flat 0-row tables.
    assert "fec_receipts" in result.partitioned_tables
    assert "fec_disbursements" in result.partitioned_tables
    assert (result.generation_dir / "fec_receipts").is_dir()
    assert list((result.generation_dir / "fec_receipts").rglob("source_namespace=*/part-000000.parquet"))
    assert (result.generation_dir / "fec_loans.parquet").is_file()  # 0 rows -> clean flat file

    # 4. Exact row/Arrow parity with fec_repaired_builder on every repaired table.
    #    Note: multi-mapper tables (e.g. fec_contribution_aggregates = bulk aggregates +
    #    API inaugural aggregates) carry the full unified fec_typed_schemas.json columns
    #    with sibling-mapper-only columns set to NULL; every oracle column must match
    #    Arrow-for-Arrow identically!
    for table_name in sorted(oracle.schemas):
        oracle_arrow = oracle.arrow_table(table_name)
        streamed_arrow = _read_generation_table(result.generation_dir, table_name)
        assert streamed_arrow.num_rows == oracle_arrow.num_rows, f"Row count drift on {table_name}"
        extra_cols = [c for c in streamed_arrow.schema.names if c not in set(oracle_arrow.schema.names)]
        for extra in extra_cols:
            assert streamed_arrow.column(extra).null_count == streamed_arrow.num_rows, (
                f"Expected NULL in sibling-mapper column {table_name}.{extra}"
            )
        projected = streamed_arrow.select(oracle_arrow.schema.names)
        assert projected.schema.equals(oracle_arrow.schema, check_metadata=False), (
            f"Oracle schema drift on {table_name}"
        )
        # Sort by deterministic key for comparison (fec_record_evidence stage order may differ from oracle).
        if table_name == "fec_record_evidence":
            def _ev_key(r: dict) -> tuple:
                return (r["target_table"], r["target_record_id"], r["source_record_id"], r["role"])

            assert sorted(projected.to_pylist(), key=_ev_key) == sorted(oracle_arrow.to_pylist(), key=_ev_key)
        else:
            assert projected.equals(oracle_arrow), f"Arrow data mismatch on {table_name}"

    # 5. Receipt and ID-mapping proof.
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    assert receipt["receipts_free"] is True
    assert receipt["identity_version"] == IDENTITY_VERSION == "fec-typed-observation/2"
    assert receipt["table_count"] == 50
    assert result.telemetry.c1_control_matches == 0
    id_map = pq.read_table(result.id_mapping_path)
    assert id_map.num_rows > 0
    assert set(id_map.schema.names) == {"table", "old_record_id", "new_record_id"}


def test_identity_version_v2_strips_encoding_top_level_and_embedded_bodies():
    assert IDENTITY_VERSION == "fec-typed-observation/2"
    assert LEGACY_IDENTITY_VERSION == "fec-typed-observation/1"
    assert TARGET_IDENTITY_VERSION == IDENTITY_VERSION
    assert LEGACY_TARGET_IDENTITY_VERSION == LEGACY_IDENTITY_VERSION

    # Top-level encoding difference leaves /2 observation_id unchanged, while /1 moves.
    base_row = dict(
        collection_id="c-1",
        source_record_id="r/1",
        source_sha256=ARCHIVE_SHA,
        source_locator_json=_locator("c-1", "r/1", ordinal=1, encoding="latin-1"),
    )
    reencoded_row = {
        **base_row,
        "source_locator_json": _locator("c-1", "r/1", ordinal=1, encoding="cp1252"),
    }
    assert observation_id("fec_receipts", base_row, "official-fec") == observation_id(
        "fec_receipts", reencoded_row, "official-fec"
    )
    assert legacy_observation_id("fec_receipts", base_row, "official-fec") != legacy_observation_id(
        "fec_receipts", reencoded_row, "official-fec"
    )

    # Nested embedded_bodies[0].encoding (used by positional kind=="text" filings) ALSO stays invariant under /2.
    body_l1 = dict(sha256=ARCHIVE_SHA, byte_offset=20, byte_length=40, encoding="latin-1")
    body_cp = dict(sha256=ARCHIVE_SHA, byte_offset=20, byte_length=40, encoding="cp1252")
    text_l1 = dict(
        collection_id="retained-filing-b6e6dffafef1febe",
        source_record_id="text/2",
        source_sha256=ARCHIVE_SHA,
        source_locator_json=_locator("retained-filing-b6e6dffafef1febe", "text/2", embedded_bodies=[body_l1]),
    )
    text_cp = {
        **text_l1,
        "source_locator_json": _locator("retained-filing-b6e6dffafef1febe", "text/2", embedded_bodies=[body_cp]),
    }
    assert observation_id("fec_filing_text_observations", text_l1, "official-fec") == observation_id(
        "fec_filing_text_observations", text_cp, "official-fec"
    )
    assert legacy_observation_id("fec_filing_text_observations", text_l1, "official-fec") != legacy_observation_id(
        "fec_filing_text_observations", text_cp, "official-fec"
    )

    # Even encoding-free rows move once from /1 to /2 ("every ID moves once").
    no_enc = dict(
        collection_id="filings-f13-selected",
        source_record_id="f/0",
        source_sha256=FILING_SHA,
        source_locator_json=_locator("filings-f13-selected", "f/0", ordinal=0),
    )
    assert observation_id("fec_filings", no_enc, "official-fec") != legacy_observation_id(
        "fec_filings", no_enc, "official-fec"
    )


def test_decimal_38_9_exact_roundtrip_via_pyarrow_and_duckdb(tmp_path):
    """Oracle 2: exact ``DECIMAL(38,9)`` preservation for 1E-9, -0.01, max 38-digit, and NULL."""
    max_38_9 = "99999999999999999999999999999.999999999"
    bulk_rows = [
        _bulk_indiv_row(0, "0.000000001"),
        _bulk_indiv_row(1, "-0.01"),
        _bulk_indiv_row(2, max_38_9),
        _bulk_indiv_row(3, ""),
    ]
    inputs = FecQueryBuildInputs(
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
        bulk_receipt_rows=bulk_rows,
    )
    result = build_fec_query_from_inputs(inputs, output_dir=tmp_path / "dec_out", batch_rows=2, c1_threads=2)

    arrow_tbl = _read_generation_table(result.generation_dir, "fec_receipts")
    assert arrow_tbl.schema.field("amount").type == pa.decimal128(38, 9)
    by_rid = {r["source_record_id"]: r["amount"] for r in arrow_tbl.to_pylist()}
    expected = {
        "indiv/0": Decimal("0.000000001"),
        "indiv/1": Decimal("-0.010000000"),
        "indiv/2": Decimal(max_38_9),
        "indiv/3": None,
    }
    assert by_rid == expected

    # Verify exact DECIMAL(38,9) readback through DuckDB as well.
    parts = [str(p) for p in sorted((result.generation_dir / "fec_receipts").rglob("*.parquet"))]
    with duckdb.connect() as con:
        rows = dict(
            con.execute(
                "SELECT source_record_id, amount FROM read_parquet(?, hive_partitioning=false)",
                [parts],
            ).fetchall()
        )
        [(col_type,)] = con.execute(
            "SELECT typeof(amount) FROM read_parquet(?, hive_partitioning=false) LIMIT 1",
            [parts],
        ).fetchall()
    assert col_type == "DECIMAL(38,9)"
    assert rows == expected


def test_refuse_c1_controls_rejects_raw_and_json_escaped_c1_and_resolves_cp1252_body(tmp_path):
    """Oracle 4: C1 bytes (raw or JSON-escaped) block sealing; cp1252 body reader decodes cleanly."""
    # 1. Raw C1 control in a text field MUST fail before sealing.
    raw_c1_row, sel = _bulk_indiv_row(0, "10.00")
    meta = json.loads(raw_c1_row["metadata_json"])
    meta["NAME"] = "AFC PAC\x92s Donor"
    raw_c1_row["metadata_json"] = json.dumps(meta, ensure_ascii=False)
    out_raw = tmp_path / "c1_raw"
    with pytest.raises(ValueError, match="C1 control"):
        build_fec_query_from_inputs(
            FecQueryBuildInputs(source_generation_pin=GEN, selection_evidence_sha256=EVIDENCE, bulk_receipt_rows=[(raw_c1_row, sel)]),
            output_dir=out_raw,
            c1_threads=2,
        )
    assert not (out_raw / "build" / "generation" / "artifact.json").exists()

    # 2. JSON-escaped \u0092 inside a JSON column (e.g. source_locator_json) MUST ALSO fail before sealing.
    esc_row, esc_sel = _bulk_indiv_row(1, "20.00")
    esc_row["source_locator_json"] = json.dumps(
        {"collection_id": esc_row["collection_id"], "source_record_id": esc_row["source_record_id"], "note": "Bell\x92s"},
        ensure_ascii=True,
    )
    assert "\\u0092" in esc_row["source_locator_json"]
    out_esc = tmp_path / "c1_escaped"
    with pytest.raises(ValueError, match="C1 control"):
        build_fec_query_from_inputs(
            FecQueryBuildInputs(source_generation_pin=GEN, selection_evidence_sha256=EVIDENCE, bulk_receipt_rows=[(esc_row, esc_sel)]),
            output_dir=out_esc,
            c1_threads=2,
        )
    assert not (out_esc / "build" / "generation" / "artifact.json").exists()

    # 3. Clean cp1252 body reader over ZIP member decodes AFC PAC’s / “Agreement” cleanly.
    store = CentralRawArchiveStore(tmp_path / "raw")
    member_payload = b"HDR\x1cFEC\x1c8.5\r\nAFC PAC\x92s \x93Agreement\x94\r\n"
    member_sha = "sha256:" + hashlib.sha256(member_payload).hexdigest()
    buf = io.BytesIO()
    with ZipFile(buf, "w") as zf:
        zf.writestr("1981885.fec", member_payload)
    zip_bytes = buf.getvalue()
    zip_sha = "sha256:" + hashlib.sha256(zip_bytes).hexdigest()
    store.put_blob(zip_sha, len(zip_bytes), (zip_bytes,))

    scope = {
        "capture": {"requestUrl": "https://example.gov/20260608.zip", "responseSha256": zip_sha, "byteSize": len(zip_bytes)},
        "member": {"ordinal": 0, "name": "1981885.fec"},
    }
    body_ref = {
        "sha256": member_sha,
        "byte_offset": len(b"HDR\x1cFEC\x1c8.5\r\n"),
        "byte_length": len(b"AFC PAC\x92s \x93Agreement\x94"),
        "encoding": "cp1252",
    }
    with verified_archive_session():
        reader = make_filing_body_reader(store, scope)
        decoded = reader(body_ref)
    assert decoded == "AFC PAC\u2019s \u201cAgreement\u201d"


def test_checkpoint_resume_preserves_evidence_and_id_mapping_after_crash(tmp_path):
    """Crash mid-build -> restart with resume=True preserves all earlier evidence & ID mappings."""
    fx = _shared_repaired_fixture()
    clean_result = build_fec_query_from_inputs(
        FecQueryBuildInputs(**fx),
        output_dir=tmp_path / "clean",
        batch_rows=2,
        c1_threads=2,
    )

    crash_dir = tmp_path / "crash_resume"
    calls = {"n": 0}

    def _crash_after_stage1():
        calls["n"] += 1
        # Trigger crash after Stage 1 tables + sidecars commit, during Stage 2.
        if (crash_dir / "checkpoints" / "fec_filing_header_associations.checkpoint.json").is_file() and calls["n"] > 12:
            raise RuntimeError("simulated mid-build crash")

    with pytest.raises(RuntimeError, match="simulated mid-build crash"):
        build_fec_query_from_inputs(
            FecQueryBuildInputs(**fx),
            output_dir=crash_dir,
            batch_rows=2,
            c1_threads=2,
            check_resources=_crash_after_stage1,
        )
    assert not (crash_dir / "build" / "generation" / "artifact.json").exists()
    assert (crash_dir / "checkpoints" / "fec_filings.checkpoint.json").is_file()
    assert (crash_dir / "checkpoints" / ".stage_sidecars" / "1b_filings_and_links.sidecar.json").is_file()

    # Corrupt one checkpoint Parquet member and verify load_verified_checkpoint detects it.
    sel_file = crash_dir / "build" / "tables" / "fec_collection_selection.parquet"
    sel_file.write_bytes(b"corrupted")
    schemas = load_fec_typed_schemas(verify_processing_parity=False)
    assert (
        load_verified_checkpoint(
            table="fec_collection_selection",
            tables_dir=crash_dir / "build" / "tables",
            checkpoints_dir=crash_dir / "checkpoints",
            schema=schemas["fec_collection_selection"],
            source_generation_pin=GEN,
        )
        is None
    )

    # Restart cleanly with resume=True -> resumes intact Stage 1 tables, rebuilds corrupted + pending tables!
    resumed = build_fec_query_from_inputs(
        FecQueryBuildInputs(**fx),
        output_dir=crash_dir,
        batch_rows=2,
        c1_threads=2,
        resume=True,
    )
    assert resumed.telemetry.tables_resumed >= 4
    assert resumed.table_rows == clean_result.table_rows
    assert resumed.telemetry.evidence_rows_written == clean_result.telemetry.evidence_rows_written
    assert resumed.telemetry.id_mapping_rows == clean_result.telemetry.id_mapping_rows

    # Prove resumed fec_record_evidence and id_mapping.parquet match the clean build identically!
    assert _read_generation_table(resumed.generation_dir, "fec_record_evidence").equals(
        _read_generation_table(clean_result.generation_dir, "fec_record_evidence")
    )
    assert pq.read_table(resumed.id_mapping_path).equals(pq.read_table(clean_result.id_mapping_path))


def test_verify_fec_query_parity_maps_v1_to_v2_and_catches_drift(tmp_path):
    """Bidirectional parity under id_mapping.parquet passes across /1 -> /2 + pin change and catches value drift."""
    bulk_rows = [_bulk_indiv_row(0, "125.00"), _bulk_indiv_row(1, "350.25")]
    result = build_fec_query_from_inputs(
        FecQueryBuildInputs(source_generation_pin=GEN, selection_evidence_sha256=EVIDENCE, bulk_receipt_rows=bulk_rows),
        output_dir=tmp_path / "cand_build",
        c1_threads=2,
    )

    # Synthesize a prior /1 generation with old_record_id, identity_version=/1, and old source_generation_pin.
    id_map = {
        r["new_record_id"]: r["old_record_id"]
        for r in pq.read_table(result.id_mapping_path).to_pylist()
        if r["table"] == "fec_receipts"
    }
    cand_receipts = _read_generation_table(result.generation_dir, "fec_receipts")
    prior_rows = [
        {
            **r,
            "record_id": id_map[r["record_id"]],
            "identity_version": LEGACY_IDENTITY_VERSION,
            "source_generation_pin": GEN_PRIOR,
        }
        for r in cand_receipts.to_pylist()
    ]
    prior_dir = tmp_path / "prior_gen"
    prior_dir.mkdir()
    pq.write_table(pa.Table.from_pylist(prior_rows, schema=cand_receipts.schema), prior_dir / "fec_receipts.parquet")

    report = verify_fec_query_parity(
        prior_dir=prior_dir,
        candidate_dir=result.generation_dir,
        id_mapping_path=result.id_mapping_path,
        tables=("fec_receipts",),
        expected_source_generation_pin=GEN,
        threads=2,
        admit_candidate=True,
    )
    assert report["c1_control_matches"] == 0
    assert report["candidate"]["receipts_free"] is True
    assert report["tables"]["fec_receipts"]["status"] == "parity_verified"
    assert report["tables"]["fec_receipts"]["prior_only_rows"] == 0
    assert report["tables"]["fec_receipts"]["candidate_only_rows"] == 0

    # Inject value drift in prior and confirm verify_fec_query_parity raises!
    drifted_rows = [{**prior_rows[0], "amount": Decimal("999.990000000")}, prior_rows[1]]
    pq.write_table(pa.Table.from_pylist(drifted_rows, schema=cand_receipts.schema), prior_dir / "fec_receipts.parquet")
    with pytest.raises(ValueError, match="Unintended value parity diff on fec_receipts"):
        verify_fec_query_parity(
            prior_dir=prior_dir,
            candidate_dir=result.generation_dir,
            id_mapping_path=result.id_mapping_path,
            tables=("fec_receipts",),
            threads=2,
            admit_candidate=True,
        )


def test_rebuilt_tables_wiring_and_collection_classifier(tmp_path):
    """Verify ``fec_original_tables.rebuild()`` wiring and collection classification routes."""
    store = CentralRawArchiveStore(tmp_path / "raw")
    entry = admit(
        store,
        "fec_historical_ie_statistics",
        ie_workbook([DETAIL, TOTAL]),
        source_url="https://www.fec.gov/files/bulk-downloads/1978/ie_1977_1978.xlsx",
    )
    result = build_fec_query_from_inputs(
        FecQueryBuildInputs(
            source_generation_pin=GEN,
            selection_evidence_sha256=EVIDENCE,
            rebuilt_tables_store=store,
            rebuilt_originals=[entry],
            verify_rebuilt_expected_rows=False,
        ),
        output_dir=tmp_path / "rebuilt_out",
        c1_threads=2,
    )
    admit_generation_for_publication(result.generation_dir)
    assert result.table_rows["fec_historical_ie_statistics"] == 2
    assert "fec_historical_ie_statistics" in result.refusal_paths
    assert result.refusal_paths["fec_historical_ie_statistics"].is_file()

    # Collection classifier route checks across real FEC collection families.
    def _col(cid, profile, url="", fmt=None, outcome="published"):
        scope = {"capture": {"requestUrl": url}} if url else {}
        if fmt:
            scope["format"] = fmt
        return {
            "collection_id": cid,
            "profile": profile,
            "record_outcome": outcome,
            "requested_scope_json": json.dumps(scope),
        }

    assert classify_collection(_col("disp-1", "document", outcome="retained_unparsed"))[0] == "skip"
    assert classify_collection(_col("filings-1", "filing"))[0] == "filing_metadata"
    assert classify_collection(_col("api-cands", "document", "https://api.open.fec.gov/v1/candidates/"))[0] == "candidate_api"
    assert classify_collection(_col("api-cmtes", "document", "https://api.open.fec.gov/v1/committees/"))[0] == "committee_api"
    assert classify_collection(_col("api-loans", "document", "https://api.open.fec.gov/v1/schedules/schedule_c/"))[0] == "api_financial"
    assert classify_collection(_col("api-legal", "document", "https://api.open.fec.gov/v1/legal/search/"))[0] == "legal"
    assert classify_collection(_col("ag-1", "agency"))[0] == "agency"
    assert classify_collection(_col("pg-1", "postgres")) == ("committee_history", "postgres")
    assert classify_collection(_col("f-1", "positional", "https://docquery.fec.gov/dcdev/posted/1.fec", fmt="fec"))[0] == "filing_fec"
    assert classify_collection(_col("f-1-physical-lines", "positional"))[0] == "filing_physical_text"
    assert classify_collection(_col("indiv26", "positional", "https://www.fec.gov/files/bulk-downloads/2026/indiv26.zip"))[0] == "bulk_receipt"
    assert classify_collection(_col("oth26", "positional", "https://www.fec.gov/files/bulk-downloads/2026/oth26.zip"))[0] == "bulk_financial"
    assert classify_collection(_col("oppexp26", "positional", "https://www.fec.gov/files/bulk-downloads/2026/oppexp26.zip"))[0] == "operating_expense"
    assert classify_collection(_col("weball24", "positional", "https://www.fec.gov/files/bulk-downloads/2024/weball24.zip"))[0] == "summary"
    assert classify_collection(_col("cm24", "positional", "https://www.fec.gov/files/bulk-downloads/2024/cm24.zip")) == ("committee_history", "positional")
    assert classify_collection(_col("cn24", "positional", "https://www.fec.gov/files/bulk-downloads/2024/cn24.zip"))[0] == "skip"
    with pytest.raises(ValueError, match="Unrouted"):
        classify_collection(_col("mystery", "document", "https://api.open.fec.gov/v1/unknown-route/"))


def test_rebuilt_source_pages_enforces_ss_j4vz_dispositions_and_records_artifact(tmp_path):
    """Verify ss-j4vz per-SHA dispositions fail closed when absent and serialize when declared."""
    store = CentralRawArchiveStore(tmp_path / "raw")
    shell_html = b"<html><head><title>Directory</title></head><body><h1>Directory</h1><p>Rows</p></body></html>"
    entry = admit(store, "fec_reference_source_pages", shell_html, page_type="filing_form_reference")
    sha = entry["source_sha256"]
    disposition = {
        "disposition": "excluded_with_reason",
        "category": "table_scope_refusal",
        "reason_code": "directory_page_out_of_scope",
        "extractor_refusal": "unsupported_body_boundaries",
        "source_url": entry["source_url"],
        "covered_by_tables": ["fec_filings"],
        "visual_review_doc": "docs/fec-source-pages-visual-review.md",
        "rationale": "Visual review confirmed directory shell rather than reference prose.",
    }

    with pytest.raises(ValueError, match="lacks an explicit disposition"):
        build_fec_query_from_inputs(
            FecQueryBuildInputs(
                source_generation_pin=GEN,
                selection_evidence_sha256=EVIDENCE,
                rebuilt_tables_store=store,
                rebuilt_originals=[entry],
                rebuilt_dispositions={},
                verify_rebuilt_expected_rows=False,
            ),
            output_dir=tmp_path / "missing_disp",
            c1_threads=2,
        )

    result = build_fec_query_from_inputs(
        FecQueryBuildInputs(
            source_generation_pin=GEN,
            selection_evidence_sha256=EVIDENCE,
            rebuilt_tables_store=store,
            rebuilt_originals=[entry],
            rebuilt_dispositions={sha: disposition},
            verify_rebuilt_expected_rows=False,
        ),
        output_dir=tmp_path / "with_disp",
        c1_threads=2,
    )
    admit_generation_for_publication(result.generation_dir)
    assert result.table_rows["fec_reference_source_pages"] == 0
    assert "fec_reference_source_pages" in result.disposition_paths
    written = json.loads(result.disposition_paths["fec_reference_source_pages"].read_text(encoding="utf-8"))
    assert written == {sha: disposition}
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    assert "fec_reference_source_pages" in receipt["disposition_paths"]


def test_build_fec_query_generation_end_to_end_from_spliced_observations(tmp_path):
    """End-to-end splice -> build_fec_query_generation() over full observations + missing-prior refusal."""
    from tests.test_fec_observations_splice import CP1252_SPLICE_COLLECTION_IDS, _build_splice_fixture
    from spicy_regs.transforms.fec_observations_splice import splice_fec_observations

    fx = _build_splice_fixture(tmp_path, schema_mode="full")
    spliced = splice_fec_observations(
        prior_dir=fx["prior_dir"],
        output_dir=tmp_path / "obs_spliced",
        store=fx["store"],
        manifest_sha256=fx["manifest_sha"],
        target_collection_ids=CP1252_SPLICE_COLLECTION_IDS,
        c1_threads=2,
    )
    corrected_manifest_sha = spliced.manifest_diff["corrected_manifest_sha256"]

    query_result = build_fec_query_generation(
        observations_dir=spliced.generation_dir,
        output_dir=tmp_path / "query_e2e",
        source_generation_pin=spliced.artifact_digest,
        selection_evidence_sha256=corrected_manifest_sha,
        namespace_evidence={},
        store=fx["store"],
        include_rebuilt_tables=False,
        c1_threads=2,
    )
    admit_generation_for_publication(query_result.generation_dir)
    assert len(query_result.table_rows) == 50
    assert query_result.table_rows["fec_filing_header_associations"] == 5
    assert query_result.table_rows["fec_filing_text_observations"] == 5
    texts = [
        frag["text"]
        for r in _read_generation_table(query_result.generation_dir, "fec_filing_text_observations").to_pylist()
        for frag in (r["text_fragments"] or [])
        if frag.get("text")
    ]
    assert any("AFC PAC\u2019s" in t for t in texts)
    assert sum("\u201cAgreement\u201d" in t for t in texts) == 2

    # Explicitly requested missing prior table must fail closed instead of silently skipping.
    empty_prior = tmp_path / "empty_prior"
    empty_prior.mkdir()
    with pytest.raises(FileNotFoundError, match="Prior table fec_receipts not found"):
        verify_fec_query_parity(
            prior_dir=empty_prior,
            candidate_dir=query_result.generation_dir,
            id_mapping_path=query_result.id_mapping_path,
            tables=("fec_receipts",),
            threads=2,
            admit_candidate=True,
        )


def test_authority_and_cycle_extraction_match_live_784b23da_semantics():
    """BLOCKER 2 & BLOCKER 4: senate-unofficial authority and NULL source_cycle on 5 summary/aggregate collections."""
    # BLOCKER 2: 597 live senate-unofficial-* collections carry 'unofficial-senate-retained'.
    assert (
        _authority_for_collection({"collection_id": "senate-unofficial-filing-10", "requested_scope_json": "{}"})
        == "unofficial-senate-retained"
    )
    assert (
        _authority_for_collection({"collection_id": "retained-filing-1", "requested_scope_json": "{}"})
        == "official-fec"
    )

    bucket = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads"
    # BLOCKER 4: All 5 live-NULL summary/aggregate collections MUST return None.
    for url in (
        f"{bucket}/data.fec.gov/leadership2024.csv",
        f"{bucket}/data.fec.gov/leadership2026.csv",
        f"{bucket}/Presidential_Map/2024/2024_overall_summary.csv",
        f"{bucket}/Presidential_Map/2024/Contributions_by_Size.csv",
        f"{bucket}/Presidential_Map/2024/Contributions_by_3Zip.csv",
    ):
        assert _extract_cycle_from_url(url) is None, url

    # Standard cycle-bearing bulk collections preserve exact even cycles.
    assert _extract_cycle_from_url(f"{bucket}/2024/candidate_summary_2024.csv") == 2024
    assert _extract_cycle_from_url(f"{bucket}/2026/committee_summary_2026.csv") == 2026
    assert _extract_cycle_from_url(f"{bucket}/2026/indiv26.zip") == 2026
    assert _extract_cycle_from_url(f"{bucket}/2024/weball24.zip") == 2024
    assert _extract_cycle_from_url(f"{bucket}/2024/webl24.zip") == 2024
    assert _extract_cycle_from_url(f"{bucket}/2024/cm24.zip") == 2024
    assert _extract_cycle_from_url(f"{bucket}/data.fec.gov/independent_expenditure_2024.csv") == 2024
    assert _extract_cycle_from_url(f"{bucket}/data.fec.gov/CommunicationCosts_2024.csv") == 2024
    assert _extract_cycle_from_url(f"{bucket}/data.fec.gov/ElectioneeringComm_2024.csv") == 2024


def test_build_fec_query_generation_hydrates_individual_snapshot_and_skips_blank_indiv_rows(tmp_path):
    """BLOCKER 3 + NIT 8: hydrate scope/field_mapping from fec_collections.parquet & skip blank indiv rows."""
    from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_SCHEMA

    bucket = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads"
    indiv_url = f"{bucket}/2026/indiv26.zip"
    field_mapping = {"header_collection_id": "bulk-2026-indiv-header", "header_row_ordinal": 0, "data_has_header": False}
    seg_bytes = b"line1\nline2\n"
    seg_sha = "sha256:" + hashlib.sha256(seg_bytes).hexdigest()

    def _scope(ordinal: int, member_name: str) -> dict:
        return {
            "capture": {"requestUrl": indiv_url, "responseSha256": INDIV_SHA, "byteSize": 100, "representation": "zip"},
            "format": "delimited",
            "delimiter": "|",
            "quoting": "literal",
            "encoding": "utf-8",
            "member": {"ordinal": ordinal, "name": member_name},
        }

    # Raw Parquet rows: requested_scope_json + collection_outcome_json ONLY (no pre-hydrated scope/field_mapping keys).
    col_rows = [
        {
            "collection_id": "bulk-indiv26-main",
            "source_family": "fec_bulk",
            "profile": "positional",
            "source_system_id": "fec-bulk",
            "source_state_scope": "retained",
            "record_count": 2,
            "relationship_count": 0,
            "record_outcome": "published",
            "requested_scope_json": json.dumps(_scope(0, "itcont.txt")),
            "coverage_limits_json": "[]",
            "collection_outcome_json": json.dumps({"tableFieldMapping": field_mapping}),
            "artifact_sha256": None,
        },
        {
            "collection_id": "bulk-indiv26-date-01",
            "source_family": "fec_bulk",
            "profile": "positional",
            "source_system_id": "fec-bulk",
            "source_state_scope": "retained",
            "record_count": 2,
            "relationship_count": 0,
            "record_outcome": "published",
            "requested_scope_json": json.dumps(_scope(1, "by_date/itcont_2026_01.txt")),
            "coverage_limits_json": "[]",
            "collection_outcome_json": json.dumps({"tableFieldMapping": field_mapping}),
            "artifact_sha256": None,
        },
    ]
    sample_row, _ = _bulk_indiv_row(0, "45.00")

    def _rec_row(cid: str, rid: str, meta_json: str) -> dict:
        return {
            "collection_id": cid,
            "source_family": "fec_bulk",
            "profile": "positional",
            "source_record_id": rid,
            "committee_id": "C00677286",
            "candidate_id": None,
            "filing_id": None,
            "legal_doc_id": None,
            "audit_case_id": None,
            "source_sha256": INDIV_SHA,
            "source_url": indiv_url,
            "observed_at": "2026-09-21T00:00:00Z",
            "source_locator_json": _locator(cid, rid, ordinal=0, encoding="utf-8"),
            "metadata_json": meta_json,
            "assets_json": None,
            "embedded_bodies_json": None,
            "source_record_json": None,
        }

    # Include a blank line {"kind":"row","fields":[]} in each member (counts=2 matching proof linefeeds=2, skipped cleanly by mapper).
    rec_rows = [
        _rec_row("bulk-indiv26-main", "m/0", sample_row["metadata_json"]),
        _rec_row("bulk-indiv26-main", "m/1", json.dumps({"kind": "row", "fields": []})),
        _rec_row("bulk-indiv26-date-01", "d/0", sample_row["metadata_json"]),
        _rec_row("bulk-indiv26-date-01", "d/1", json.dumps({"kind": "row", "fields": []})),
    ]
    obs_dir = tmp_path / "obs"
    obs_dir.mkdir()
    pq.write_table(pa.Table.from_pylist(col_rows, schema=COLLECTION_SCHEMA), obs_dir / "fec_collections.parquet")
    pq.write_table(pa.Table.from_pylist(rec_rows, schema=RECORD_SCHEMA), obs_dir / "fec_source_records.parquet")

    proof_obj = {
        "status": "passed-exact-complete-member-sequence-equivalence",
        "source_generation_pin": GEN,
        "archive_sha256": INDIV_SHA,
        "main_collection_id": "bulk-indiv26-main",
        "main_bytes": len(seg_bytes),
        "main_rows": 2,
        "date_members": 1,
        "segments": [
            {
                "member_ordinal": 1,
                "member_name": "by_date/itcont_2026_01.txt",
                "main_offset": 0,
                "bytes": len(seg_bytes),
                "linefeeds": 2,
                "ends_with_linefeed": True,
                "sha256": seg_sha,
            }
        ],
    }
    proof_bytes = json.dumps(proof_obj, sort_keys=True).encode("utf-8")
    proof_sha = "sha256:" + hashlib.sha256(proof_bytes).hexdigest()

    result = build_fec_query_generation(
        observations_dir=obs_dir,
        output_dir=tmp_path / "out_indiv",
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
        namespace_evidence={},
        store=CentralRawArchiveStore(tmp_path / "raw"),
        individual_snapshot_proof=(proof_bytes, proof_sha),
        include_rebuilt_tables=False,
        c1_threads=2,
    )
    admit_generation_for_publication(result.generation_dir)
    sel_rows = _read_generation_table(result.generation_dir, "fec_collection_selection").to_pylist()
    indiv_sel = {r["collection_id"]: r["selection_status"] for r in sel_rows if r["purpose"] == "retained-reported-individual-contribution-snapshot"}
    assert indiv_sel == {"bulk-indiv26-main": "included", "bulk-indiv26-date-01": "duplicate"}
    # Only the selected main member's 1 non-blank row is emitted into fec_receipts; the
    # proven-equivalent duplicate date member is excluded from fec_receipts while kept in fec_collection_selection.
    assert result.table_rows["fec_receipts"] == 1
    receipt_rows = _read_generation_table(result.generation_dir, "fec_receipts").to_pylist()
    assert [r["collection_id"] for r in receipt_rows] == ["bulk-indiv26-main"]

    # Fail closed if multiple bulk_receipt collections share an archive responseSha256 without a matching proof.
    with pytest.raises(ValueError, match="without a matching individual_snapshot_proof"):
        build_fec_query_generation(
            observations_dir=obs_dir,
            output_dir=tmp_path / "out_indiv_no_proof",
            source_generation_pin=GEN,
            selection_evidence_sha256=EVIDENCE,
            namespace_evidence={},
            store=CentralRawArchiveStore(tmp_path / "raw"),
            individual_snapshot_proof=None,
            include_rebuilt_tables=False,
            c1_threads=2,
        )


def test_discover_and_qualify_retained_mur_captures_in_build_fec_query_generation(tmp_path):
    """BLOCKER 5: auto-discover HTTP-200 api_key=<redacted> MUR captures while skipping 404s and refusing real keys."""
    from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_SCHEMA

    store = CentralRawArchiveStore(tmp_path / "raw")
    mur_payload = json.dumps(
        {"docs": [{"type": "murs", "no": "1604", "mur_name": "MUR 1604", "open_date": "2004-01-01", "close_date": None}]}
    ).encode("utf-8")
    mur_sha = "sha256:" + hashlib.sha256(mur_payload).hexdigest()
    store.put_blob(mur_sha, len(mur_payload), (mur_payload,))

    def _disp_col(cid: str, url: str, statuses: list[int], sha: str, size: int) -> dict:
        # Match live 4cb0dffa nesting: callerContext = {"pin": ..., "facts": {"version": 1, "source_capture": ...}}
        outcome = {
            "providerOutcome": None,
            "receiverDisposition": {
                "status": "selection_context",
                "reason": "Literal retained JSON response context",
                "callerContext": {
                    "pin": {"path": f"context/{cid}.json", "sha256": "sha256:" + "0" * 64},
                    "facts": {
                        "version": 1,
                        "source_capture": {
                            "url": url,
                            "sha256": sha,
                            "bytes": size,
                            "observed_at": ["2026-09-11T18:30:44+00:00"],
                            "http_statuses": statuses,
                            "truncated": [False],
                        },
                    },
                },
            },
        }
        return {
            "collection_id": cid,
            "source_family": "fec_legal",
            "profile": None,
            "source_system_id": None,
            "source_state_scope": None,
            "record_count": 0,
            "relationship_count": 0,
            "record_outcome": "selection_context",
            "requested_scope_json": None,
            "coverage_limits_json": None,
            "collection_outcome_json": json.dumps(outcome),
            "artifact_sha256": None,
        }

    # Include both coverage-capture-* and native-context-* sharing the exact same SHA (matching live 4cb0dffa):
    # discovery must deduplicate by SHA and prefer native-context-* over coverage-capture-*.
    col_rows = [
        _disp_col("coverage-capture-mur-1604", "https://api.open.fec.gov/v1/legal/docs/murs/1604?api_key=<redacted>", [200], mur_sha, len(mur_payload)),
        _disp_col("native-context-mur-1604", "https://api.open.fec.gov/v1/legal/docs/murs/1604?api_key=<redacted>", [200], mur_sha, len(mur_payload)),
        _disp_col("native-context-mur-1366-404", "https://api.open.fec.gov/v1/legal/docs/murs/1366?api_key=<redacted>", [404], mur_sha, len(mur_payload)),
        _disp_col("native-context-mur-empty", "https://api.open.fec.gov/v1/legal/docs/murs/1999?api_key=<redacted>", [], mur_sha, len(mur_payload)),
    ]
    obs_dir = tmp_path / "obs_mur"
    obs_dir.mkdir()
    pq.write_table(pa.Table.from_pylist(col_rows, schema=COLLECTION_SCHEMA), obs_dir / "fec_collections.parquet")
    pq.write_table(pa.Table.from_pylist([], schema=RECORD_SCHEMA), obs_dir / "fec_source_records.parquet")

    ro_store = CentralRawArchiveStore(tmp_path / "raw", create=False)
    discovered = discover_retained_mur_captures(col_rows, store=ro_store)
    assert [c["collection_id"] for c in discovered] == ["native-context-mur-1604"]

    # Non-<redacted> credential value MUST fail closed, even on a duplicate coverage-capture-* row with the same SHA!
    leaked_col = _disp_col("native-context-mur-bad", "https://api.open.fec.gov/v1/legal/docs/murs/1604?api_key=REAL_SECRET", [200], mur_sha, len(mur_payload))
    with pytest.raises(ValueError, match="redacted|credential|api_key"):
        discover_retained_mur_captures([leaked_col], store=ro_store)
    leaked_dup = _disp_col("coverage-capture-mur-1604-leaked", "https://api.open.fec.gov/v1/legal/docs/murs/1604?api_key=REAL_SECRET", [200], mur_sha, len(mur_payload))
    with pytest.raises(ValueError, match="redacted|credential|api_key"):
        discover_retained_mur_captures([col_rows[1], leaked_dup], store=ro_store)

    # End-to-end build_fec_query_generation() auto-discovers and qualifies MUR 1604.
    result = build_fec_query_generation(
        observations_dir=obs_dir,
        output_dir=tmp_path / "out_mur",
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
        namespace_evidence={},
        store=ro_store,
        include_rebuilt_tables=False,
        c1_threads=2,
    )
    admit_generation_for_publication(result.generation_dir)
    assert result.table_rows["fec_legal_matters"] == 1
    matters = _read_generation_table(result.generation_dir, "fec_legal_matters").to_pylist()
    assert matters[0]["native_matter_id"] == "1604"
    assert matters[0]["title"] == "MUR 1604"


def test_stage3_per_table_checkpoint_resume_preserves_fec_receipts_on_disbursements_crash(tmp_path):
    """WARNING 6 + NIT 8: Stage 3a fec_receipts survives crash in 3c fec_disbursements + zero-copy id_mapping/."""
    fx = _shared_repaired_fixture()
    fx["bulk_receipt_rows"] = [_bulk_indiv_row(0, "10.00"), _bulk_indiv_row(1, "20.00")]

    clean_result = build_fec_query_from_inputs(
        FecQueryBuildInputs(**fx),
        output_dir=tmp_path / "clean_s3",
        batch_rows=2,
        c1_threads=2,
    )
    assert clean_result.id_mapping_path.is_dir()

    crash_dir = tmp_path / "crash_s3"

    def _crash_in_3c():
        # Crash AFTER 3a_fec_receipts commits its checkpoint + sidecar, while 3c_fec_disbursements is running.
        if (crash_dir / "checkpoints" / ".stage_sidecars" / "3a_fec_receipts.sidecar.json").is_file() and (
            crash_dir / "checkpoints" / ".stage_sidecars" / "3b_fec_intercommittee_transactions.sidecar.json"
        ).is_file():
            raise RuntimeError("simulated crash during 3c_fec_disbursements")

    with pytest.raises(RuntimeError, match="simulated crash during 3c_fec_disbursements"):
        build_fec_query_from_inputs(
            FecQueryBuildInputs(**fx),
            output_dir=crash_dir,
            batch_rows=2,
            c1_threads=2,
            check_resources=_crash_in_3c,
        )
    assert not (crash_dir / "build" / ".financial_mapped").exists()
    assert (crash_dir / "checkpoints" / "fec_receipts.checkpoint.json").is_file()
    assert not (crash_dir / "checkpoints" / "fec_disbursements.checkpoint.json").is_file()

    # Clear bulk_receipt_rows on restart to PROVE 3a_fec_receipts is genuinely resumed, not recomputed!
    resumed_inputs = FecQueryBuildInputs(**fx)
    resumed_inputs.bulk_receipt_rows = ()
    resumed = build_fec_query_from_inputs(
        resumed_inputs,
        output_dir=crash_dir,
        batch_rows=2,
        c1_threads=2,
        resume=True,
    )
    assert resumed.table_rows == clean_result.table_rows
    assert resumed.table_rows["fec_receipts"] == 3  # 2 bulk + 1 SA schedule preserved from checkpoint!
    assert _read_generation_table(resumed.generation_dir, "fec_receipts").equals(
        _read_generation_table(clean_result.generation_dir, "fec_receipts")
    )
    assert pq.read_table(resumed.id_mapping_path).equals(pq.read_table(clean_result.id_mapping_path))


def test_build_fec_query_generation_fails_closed_on_cm24_inner_member_mismatch(tmp_path):
    """WARNING 7: mismatched inner archive bytes in duplicate cm24 collections MUST raise, never silently pass."""
    from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_SCHEMA

    store = CentralRawArchiveStore(tmp_path / "raw")
    b1, b2 = io.BytesIO(), io.BytesIO()
    with ZipFile(b1, "w") as z1:
        z1.writestr("cm.txt", b"C00000001|ONE\n")
    with ZipFile(b2, "w") as z2:
        z2.writestr("cm.txt", b"C00000001|DIFFERENT\n")
    z1_bytes, z2_bytes = b1.getvalue(), b2.getvalue()
    z1_sha = "sha256:" + hashlib.sha256(z1_bytes).hexdigest()
    z2_sha = "sha256:" + hashlib.sha256(z2_bytes).hexdigest()
    store.put_blob(z1_sha, len(z1_bytes), (z1_bytes,))
    store.put_blob(z2_sha, len(z2_bytes), (z2_bytes,))

    url = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/2024/cm24.zip"
    col_rows = [
        {
            "collection_id": cid,
            "source_family": "fec_bulk",
            "profile": "positional",
            "source_system_id": "fec-bulk",
            "source_state_scope": "retained",
            "record_count": 1,
            "relationship_count": 0,
            "record_outcome": "published",
            "requested_scope_json": json.dumps(
                {"capture": {"requestUrl": url, "responseSha256": sha, "byteSize": size, "representation": "zip"}, "member": {"ordinal": 0, "name": "cm.txt"}}
            ),
            "coverage_limits_json": "[]",
            "collection_outcome_json": "{}",
            "artifact_sha256": None,
        }
        for cid, sha, size in [("bulk-cm24-zip", z1_sha, len(z1_bytes)), ("retained-cm24-dup", z2_sha, len(z2_bytes))]
    ]
    obs_dir = tmp_path / "obs_cm24"
    obs_dir.mkdir()
    pq.write_table(pa.Table.from_pylist(col_rows, schema=COLLECTION_SCHEMA), obs_dir / "fec_collections.parquet")
    pq.write_table(pa.Table.from_pylist([], schema=RECORD_SCHEMA), obs_dir / "fec_source_records.parquet")

    with pytest.raises(ValueError, match="Inner archive member bytes differ"):
        build_fec_query_generation(
            observations_dir=obs_dir,
            output_dir=tmp_path / "out_cm24_bad",
            source_generation_pin=GEN,
            selection_evidence_sha256=EVIDENCE,
            namespace_evidence={},
            store=CentralRawArchiveStore(tmp_path / "raw", create=False),
            include_rebuilt_tables=False,
            c1_threads=2,
        )
