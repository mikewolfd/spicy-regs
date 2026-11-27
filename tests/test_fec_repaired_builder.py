"""End-to-end verification of the maintained repaired FEC builder through DuckDB query consumers (ss-u9s8 / ss-42zk)."""

from decimal import Decimal
import hashlib
import io
import json
import zipfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.fec_native_conversion import header_subjects
from spicy_regs.relationship_views.fec_filing_associations import (
    financial_header_association_sql,
    financial_number_association_sql,
)
from spicy_regs.relationship_views.fec_typed import (
    cross_source_collection_inclusion,
    deduplicated_subject_sql,
    filing_reference_resolution,
    reported_summary_measures,
)
from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
from spicy_regs.transforms.fec_agency import AgencySelection, WORD
from spicy_regs.transforms.fec_bulk_financial import OPPEXP_FIELDS
from spicy_regs.transforms.fec_bulk_selection import (
    build_archive_inner_member_proof,
    build_weball_webl_subset_proof,
)
from spicy_regs.transforms.fec_filing_associations import ASSOCIATION_SCHEMA, associate_filing_headers
from spicy_regs.transforms.fec_filing_financial import RECEIPTS, load_layout
from spicy_regs.transforms.fec_filing_forms import filing_text_mapping_for
from spicy_regs.transforms.fec_identity_observations import QUALITY_NOTICE, filing_key
from spicy_regs.transforms.fec_query import CollectionSelection
from spicy_regs.transforms.fec_repaired_builder import build_repaired_fec_tables, register_repaired_fec_tables
from spicy_regs.transforms.fec_summaries import CANDIDATE_WEB

GEN = "sha256:" + "a" * 64
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


def _locator(cid, rid, **extra):
    return json.dumps({"collection_id": cid, "source_record_id": rid, **extra}, sort_keys=True, separators=(",", ":"))


def _sel(cid, sha, cycle=2026):
    return CollectionSelection(cid, sha, GEN, "official-fec", cycle, "snapshot", EVIDENCE)


def test_repaired_builder_end_to_end_through_duckdb_query_consumer(tmp_path):
    # 1. Filing metadata (including cross-source duplicate retained-api registration) + amendment chain link
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

    # 2. Official FEC bulk electronic archive member header + Schedule A + TEXT record
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

    # 3. Operating expense with trailing empty pipe [""] and FILE_NUM 1889009
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

    # 4. Contribution aggregates: rollup pseudo-candidate P00000001 + real candidate P80001571
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

    # 5. Summary rows (weball24 + webl24 subset)
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

    # 6. Quality notice (candidate ID + committee ID)
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

    # 7. Legal AF (subject committee typing + null close_date skipped) + retained redacted MUR JSON
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

    # 8. Agency Oversight (SafeLinks) + Word FOIA (tabular row)
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

    # Build cm24 inner-member proof + weball/webl subset proof
    buf1, buf2 = io.BytesIO(), io.BytesIO()
    with zipfile.ZipFile(buf1, "w") as z1:
        z1.comment = b"a"
        z1.writestr("cm.txt", b"C00677286|COMMITTEE\n")
    with zipfile.ZipFile(buf2, "w") as z2:
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

    # First run summaries through map_summary to build exact subset proof
    from spicy_regs.transforms.fec_summaries import map_summary
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

    built = build_repaired_fec_tables(
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
        output_dir=tmp_path / "repaired",
    )

    # Verify Parquet files wrote cleanly and register directly in DuckDB query consumer
    assert "fec_receipts" in built.paths and built.paths["fec_receipts"].is_file()
    with duckdb.connect() as con:
        register_repaired_fec_tables(con, built)

        # AC1: Deduplicated filings and summaries via SQL consumer while base tables keep all occurrences
        assert con.sql("SELECT count(*) FROM fec_filings").fetchone() == (3,)
        dedup_filings = con.sql(deduplicated_subject_sql("fec_filings", GEN)).fetchall()
        assert len(dedup_filings) == 2
        assert con.sql("SELECT count(*) FROM fec_reported_financial_summaries").fetchone() == (3,)
        dedup_summaries = con.sql(deduplicated_subject_sql("fec_reported_financial_summaries", GEN)).fetchall()
        assert len(dedup_summaries) == 2
        assert con.sql(
            f"SELECT count(*) FROM ({cross_source_collection_inclusion('fec_reported_financial_summaries', GEN)}) WHERE selection_status = 'duplicate'"
        ).fetchone() == (1,)
        assert con.sql(reported_summary_measures()).to_arrow_table().num_rows > 0

        # AC2: Materialized filing_key on electronic schedule, TEXT, and bulk oppexp + SQL views agree
        assert con.sql("SELECT filing_key, filing_link_status, amount FROM fec_receipts").fetchall() == [
            (filing_key("1981955"), "resolved_native_filing_key", Decimal("250.500000000"))
        ]
        assert con.sql(
            "SELECT filing_key, filing_link_status, mapping_status FROM fec_filing_text_observations"
        ).fetchall() == [(filing_key("1981955"), "resolved_native_filing_key", "mapped")]
        assert con.sql(
            "SELECT filing_key, filing_link_status, mapping_status, amount FROM fec_disbursements"
        ).fetchall() == [(filing_key("1889009"), "resolved_native_filing_key", "mapped", Decimal("3716.380000000"))]

        # Query-consumer association views match materialized filing_key
        hdr_view = con.sql(financial_header_association_sql("fec_receipts", GEN)).to_arrow_table().to_pylist()
        assert hdr_view[0]["filing_key"] == filing_key("1981955")
        assert hdr_view[0]["referenced_filing_key"] == filing_key("1981955")
        num_view = con.sql(
            financial_number_association_sql(
                "fec_disbursements",
                GEN,
                {"fec-bulk-operating-expense-ordered-header": EVIDENCE},
                columns=tuple(built.schemas["fec_disbursements"].names),
            )
        ).to_arrow_table().to_pylist()
        assert num_view[0]["filing_key"] == filing_key("1889009")

        # AC2: Source-backed filing links resolved in base table and match SQL view
        link_statuses = con.sql(
            "SELECT native_target_file_number, target_resolution_status FROM fec_filing_links WHERE collection_id = 'filings-f13-selected' ORDER BY native_target_file_number"
        ).fetchall()
        assert link_statuses == [("1889009", "resolved_native_filing_key"), ("1981955", "resolved_native_filing_key")]
        sql_links = con.sql(
            f"SELECT native_target_file_number, target_resolution_status FROM ({filing_reference_resolution()}) WHERE collection_id = 'filings-f13-selected' ORDER BY native_target_file_number"
        ).fetchall()
        assert sql_links == link_statuses

        # AC2: Rollup pseudo-candidate separation in SQL joins
        assert con.sql(
            "SELECT candidate_native_id, candidate_name, observation_grain, entity_reference_status FROM fec_contribution_aggregates ORDER BY candidate_name"
        ).fetchall() == [
            (None, "All candidates,", "rollup-pseudo-candidate-aggregate", "rollup_pseudo_candidate"),
            ("P80001571", "CAND REAL", "candidate-aggregate", "source_identifier_unresolved"),
        ]

        # AC2 & AC3: Quality notices, SafeLinks, Word tables, Legal AF typing, non-null events, and redacted MURs
        assert con.sql("SELECT committee_id, committee_id_status, notice_scope FROM fec_quality_notices").fetchall() == [
            ("P20000055", "candidate_id_shape", "source_listed_candidate")
        ]
        assert con.sql("SELECT url, body_status FROM fec_agency_report_documents").fetchall() == [
            (unwrapped_url, "deferred_pdf")
        ]
        assert con.sql("SELECT text_kind, text FROM fec_agency_report_text").fetchall() == [
            ("word_table_row", "AGENCY OVERALL | 76")
        ]
        assert con.sql(
            "SELECT matter_type, native_matter_id FROM fec_legal_matters ORDER BY matter_type"
        ).fetchall() == [("administrative_fine", "42"), ("enforcement", "1604")]
        assert con.sql("SELECT role, entity_type FROM fec_legal_parties").fetchall() == [
            ("Subject committee", "committee")
        ]
        assert con.sql(
            "SELECT event_type, event_date_status FROM fec_legal_events ORDER BY event_type, event_date"
        ).fetchall() == [
            ("commission_vote", "exact"),
            ("open", "exact"),
            ("open", "exact"),
        ]


def test_header_subjects_admits_controlled_archive_member_upgrade(tmp_path):
    """fec_native_conversion.header_subjects accepts official archive-member upgrades while refusing other drift."""
    archive_url = (
        "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com"
        "/bulk-downloads/electronic/20260609.zip"
    )
    member = dict(ordinal=135, name="1981955.fec")
    loc_json = _locator("c", "hdr/0", sha256=ARCHIVE_SHA, ordinal=0, member=member)
    hdr = dict(
        collection_id="c",
        source_record_id="hdr/0",
        source_sha256=ARCHIVE_SHA,
        source_locator_json=loc_json,
        metadata_json=json.dumps(dict(kind="header", format_version="8.5", fields=["HDR", "FEC", "8.5"])),
    )
    target = dict(
        record_id="f-1",
        identity_version="fec-typed-observation/1",
        mapping_version="fec-identity-observations/2",
        filing_key=filing_key("1981955"),
        report_number="1981955",
        source_authority="official-fec",
        source_namespace="fec-openfec-file-number",
        native_filer_id="C00677286",
        form_type="F3",
        fec_url="https://docquery.fec.gov/dcdev/posted/1981955.fec",
    )
    upgraded = associate_filing_headers(
        [hdr],
        scopes={"c": dict(capture=dict(requestUrl=archive_url, responseSha256=ARCHIVE_SHA), member=member)},
        authorities={"c": "official-fec"},
        filings=[target],
        source_generation_pin=GEN,
        selection_evidence_sha256=EVIDENCE,
    )
    old_row = {
        **upgraded[0],
        "policy_version": "fec-retained-filing-association/1",
        "association_basis": "native-api-original-url",
        "association_status": "unresolved_archive_member_identity",
        "referenced_filing_key": None,
        "filing_key": None,
        "report_number_raw": None,
        "filing_observation_count": 0,
        "filing_observation_ids": [],
    }
    orig_path = tmp_path / "orig-headers.parquet"
    cand_path = tmp_path / "cand-headers.parquet"
    pq.write_table(pa.Table.from_pylist([old_row], schema=ASSOCIATION_SCHEMA), orig_path)
    pq.write_table(pa.Table.from_pylist(upgraded, schema=ASSOCIATION_SCHEMA), cand_path)
    selection = FilingAssociationInputs(
        GEN, None, (), {}, [TypedInput(cand_path, _sha(cand_path), 1)], filing_generation_scope="self"
    )
    adopted = header_subjects(selection, {"fec_filing_header_associations.parquet": orig_path}, None, lambda: None)
    assert adopted[0]["association_status"] == "resolved_native_filing_key"
    assert adopted[0]["filing_key"] == filing_key("1981955")
