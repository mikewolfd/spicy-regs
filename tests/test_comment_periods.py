"""One comment period per notice: what merges, what anchors it, and which close date it states."""

from __future__ import annotations

import json
from collections import Counter

import pyarrow.parquet as pq

from loguru import logger

from spicy_regs.ontology.common import canonical_json, stable_id, write_parquet_rows
from spicy_regs.transforms.build_comment_periods import ACTOR_ID, build_comment_periods

DOCKET_COLUMNS = ("docket_id", "title")
DOCUMENT_COLUMNS = (
    "document_id",
    "docket_id",
    "fr_doc_num",
    "additional_rins",
    "title",
    "posted_date",
    "comment_start_date",
    "comment_end_date",
)
REGISTER_COLUMNS = (
    "document_number",
    "publication_date",
    "document_type",
    "title",
    "abstract",
    "volume",
    "start_page",
    "regulation_id_numbers_json",
    "comments_close_on",
)
PROCEEDING_COLUMNS = ("proceeding_id", "docket_ids_json", "fr_document_ids_json", "rins_json")


def _build(root, *, dockets=(), documents=(), register=(), links=(), proceedings=()):
    """Write the five inputs and build; ``links`` are ``(document_number, publication_date, docket label)``."""
    tables = {
        "dockets": (DOCKET_COLUMNS, [{"docket_id": docket, "title": title} for docket, title in dockets]),
        "documents": (DOCUMENT_COLUMNS, list(documents)),
        "federal_register": (REGISTER_COLUMNS, list(register)),
        "fr_docket_links": (
            ("document_number", "publication_date", "docket_id"),
            [{"document_number": n, "publication_date": d, "docket_id": label} for n, d, label in links],
        ),
        "proceedings": (PROCEEDING_COLUMNS, list(proceedings)),
    }
    for name, (columns, rows) in tables.items():
        write_parquet_rows(root / f"{name}.parquet", columns=columns, rows=rows)
    return pq.read_table(build_comment_periods(root)).to_pylist()


def _notice(number, published, closes, **fields):
    return {"document_number": number, "publication_date": published, "comments_close_on": closes, **fields}


def _document(document_id, docket, opens, closes_utc, **fields):
    """A Regulations.gov document whose window ends at ``closes_utc`` (its 11:59:59 PM Eastern)."""
    return {
        "document_id": document_id,
        "docket_id": docket,
        "posted_date": f"{opens}T04:00:00Z",
        "comment_start_date": f"{opens}T04:00:00Z",
        "comment_end_date": closes_utc,
        **fields,
    }


def _holding(period, evidence_id):
    return evidence_id in json.loads(period["evidence_ids_json"])


def _one_holding(periods, evidence_id):
    (period,) = [p for p in periods if _holding(p, evidence_id)]
    return period


def test_a_notice_and_its_regulations_gov_copy_are_one_period(tmp_path):
    """COLC 2026-11545: the Register prints the office's own docket number, so its copy's docket anchors it."""
    notice = "2026-11545@2026-06-09"
    periods = _build(
        tmp_path,
        dockets=[("COLC-2026-0100", "Exemptions to Permit Circumvention of Access Controls")],
        documents=[
            _document(
                "COLC-2026-0100-0001", "COLC-2026-0100", "2026-06-09", "2026-09-29T03:59:59Z", fr_doc_num="2026-11545"
            ),
        ],
        register=[_notice("2026-11545", "2026-06-09", "2026-09-28", document_type="Proposed Rule")],
        links=[("2026-11545", "2026-06-09", "Docket No. 2026-4")],
        proceedings=[
            {
                "proceeding_id": "proceeding_colc",
                "docket_ids_json": '["COLC-2026-0100"]',
                "fr_document_ids_json": f'["{notice}"]',
                "rins_json": "[]",
            }
        ],
    )
    (period,) = periods
    assert json.loads(period["evidence_ids_json"]) == ["2026-11545@2026-06-09", "COLC-2026-0100-0001"]
    assert json.loads(period["docket_ids_json"]) == ["COLC-2026-0100"]
    assert json.loads(period["proceeding_ids_json"]) == ["proceeding_colc"]
    assert period["anchor_kind"] == "docket"
    assert (period["open_date"], period["close_date"]) == ("2026-06-09", "2026-09-28")


def test_the_docket_the_register_names_wins_over_its_copys(tmp_path):
    """Decision 56's B: a copy's docket anchors a notice only when the Register names no docket."""
    periods = _build(
        tmp_path,
        dockets=[("EPA-HQ-OAR-2026-0001", "Named"), ("EPA-HQ-OAR-2026-0002", "Copy's")],
        documents=[
            _document(
                "EPA-HQ-OAR-2026-0002-0001",
                "EPA-HQ-OAR-2026-0002",
                "2026-03-02",
                "2026-04-02T03:59:59Z",
                fr_doc_num="2026-04200",
            ),
        ],
        register=[_notice("2026-04200", "2026-03-02", "2026-04-01", document_type="Proposed Rule")],
        links=[("2026-04200", "2026-03-02", "Docket No. EPA-HQ-OAR-2026-0001")],
    )
    (period,) = periods
    assert json.loads(period["docket_ids_json"]) == ["EPA-HQ-OAR-2026-0001"]


def test_feed_copies_join_their_notices_and_unrelated_feed_notices_stay_apart(tmp_path):
    """DEA 2026-17536 has copies in its own docket and in DEA_FRDOC_0001; another DEA notice's copy overlaps it there."""
    periods = _build(
        tmp_path,
        dockets=[
            ("DEA-2026-1519", "Placement of Cipepofol in Schedule IV"),
            ("DEA_FRDOC_0001", "Recently Posted DEA Rules and Notices."),
        ],
        documents=[
            _document(
                "DEA-2026-1519-0001", "DEA-2026-1519", "2026-08-27", "2026-09-29T03:59:59Z", fr_doc_num="2026-17536"
            ),
            _document(
                "DEA_FRDOC_0001-0540", "DEA_FRDOC_0001", "2026-08-27", "2026-09-29T03:59:59Z", fr_doc_num="2026-17536"
            ),
            _document(
                "DEA_FRDOC_0001-0536", "DEA_FRDOC_0001", "2026-07-30", "2026-08-29T03:59:59Z", fr_doc_num="2026-16375"
            ),
        ],
        register=[
            _notice("2026-17536", "2026-08-27", "2026-09-28", document_type="Rule"),
            _notice("2026-16375", "2026-07-30", "2026-08-28", document_type="Notice"),
        ],
        links=[("2026-17536", "2026-08-27", "Docket No. DEA 1713")],
    )
    cipepofol = _one_holding(periods, "2026-17536@2026-08-27")
    assert json.loads(cipepofol["evidence_ids_json"]) == [
        "2026-17536@2026-08-27",
        "DEA-2026-1519-0001",
        "DEA_FRDOC_0001-0540",
    ]
    assert json.loads(cipepofol["docket_ids_json"]) == ["DEA-2026-1519"]
    other = _one_holding(periods, "2026-16375@2026-07-30")
    assert json.loads(other["evidence_ids_json"]) == ["2026-16375@2026-07-30", "DEA_FRDOC_0001-0536"]
    assert (other["docket_ids_json"], other["anchor_kind"]) == ("[]", "none")
    assert len(periods) == 2
    assert not any("DEA_FRDOC_0001" in p["docket_ids_json"] for p in periods), "a feed docket is never an anchor"


def test_unrelated_notices_in_one_long_lived_docket_stay_apart_though_their_windows_adjoin(tmp_path):
    """The chaining the per-notice design fixes: a shared docket is context, never a merge key."""
    docket = "FDA-2011-N-0655"
    periods = _build(
        tmp_path,
        dockets=[(docket, "Animal Generic Drug User Fee Act")],
        documents=[
            _document(f"{docket}-0041", docket, "2026-04-17", "2026-05-18T03:59:59Z", title="Public Meeting"),
            _document(f"{docket}-0042", docket, "2026-05-18", "2026-06-18T03:59:59Z", title="Stakeholder Consultation"),
        ],
        register=[_notice("2026-09999", "2026-06-18", "2026-07-20", document_type="Notice", title="Reauthorization")],
        links=[("2026-09999", "2026-06-18", f"Docket No. {docket}")],
    )
    assert len(periods) == 3
    assert all(json.loads(p["docket_ids_json"]) == [docket] and p["anchor_kind"] == "docket" for p in periods)
    assert sorted((p["open_date"], p["close_date"]) for p in periods) == [
        ("2026-04-17", "2026-05-17"),
        ("2026-05-18", "2026-06-17"),
        ("2026-06-18", "2026-07-20"),
    ]


def test_a_declared_extension_of_the_same_rulemaking_lengthens_its_period(tmp_path):
    """An extension notice sharing the proposal's RIN adjoins it and merges; a later reopening starts anew."""
    rin = '["2060-AV16"]'
    periods = _build(
        tmp_path,
        register=[
            _notice(
                "2021-24202",
                "2021-11-15",
                "2022-01-14",
                document_type="Proposed Rule",
                title="Methane Standards",
                regulation_id_numbers_json=rin,
            ),
            _notice(
                "2021-27312",
                "2021-12-17",
                "2022-01-31",
                document_type="Proposed Rule",
                title="Methane Standards; Extension of Comment Period",
                regulation_id_numbers_json=rin,
            ),
            _notice(
                "2022-02001",
                "2022-03-01",
                "2022-03-31",
                document_type="Proposed Rule",
                title="Methane Standards; Reopening of Comment Period",
                regulation_id_numbers_json=rin,
            ),
        ],
    )
    extended = _one_holding(periods, "2021-24202@2021-11-15")
    assert json.loads(extended["evidence_ids_json"]) == ["2021-24202@2021-11-15", "2021-27312@2021-12-17"]
    assert (extended["open_date"], extended["close_date"]) == ("2021-11-15", "2022-01-31")
    reopened = _one_holding(periods, "2022-02001@2022-03-01")
    assert (reopened["open_date"], reopened["close_date"]) == ("2022-03-01", "2022-03-31")
    assert len(periods) == 2


def test_an_extension_citing_its_notice_by_page_merges_with_it(tmp_path):
    """No RIN: the extension's abstract cites the notice at 91 FR 40001, the page the notice starts on."""
    periods = _build(
        tmp_path,
        register=[
            _notice(
                "2026-10815",
                "2026-06-01",
                "2026-07-02",
                document_type="Notice",
                title="Request for Information",
                volume="91",
                start_page="40001",
            ),
            _notice(
                "2026-12605",
                "2026-06-23",
                "2026-07-31",
                document_type="Notice",
                title="Request for Information; Extension of Comment Period",
                abstract="On June 1, 2026, the Bureau published a request for information (91 FR 40001).",
                volume="91",
                start_page="45210",
            ),
        ],
    )
    (period,) = periods
    assert (period["open_date"], period["close_date"]) == ("2026-06-01", "2026-07-31")


def test_an_extension_citing_a_record_that_states_no_window_links_nothing(tmp_path):
    """The cited page starts a Register rule with no comment period: the extension stays its own period."""
    periods = _build(
        tmp_path,
        register=[
            _notice(
                "2017-11263",
                "2017-06-01",
                None,
                document_type="Rule",
                title="Final Rule",
                volume="82",
                start_page="25201",
            ),
            _notice(
                "2017-12324",
                "2017-06-14",
                "2017-08-25",
                document_type="Proposed Rule",
                title="Prospective Payment System; Extension of Comment Period",
                abstract="This notice extends the comment period for the rule at 82 FR 25201.",
                volume="82",
                start_page="27150",
            ),
        ],
    )
    (period,) = periods
    assert json.loads(period["evidence_ids_json"]) == ["2017-12324@2017-06-14"]


def test_an_extension_whose_rin_names_two_open_notices_links_neither(tmp_path):
    rin = '["2040-AG11"]'
    periods = _build(
        tmp_path,
        register=[
            _notice(
                "2022-26227",
                "2022-12-02",
                "2023-01-03",
                document_type="Proposed Rule",
                title="Small MS4 Clarification",
                regulation_id_numbers_json=rin,
            ),
            _notice(
                "2022-26300",
                "2022-12-05",
                "2023-01-05",
                document_type="Proposed Rule",
                title="Small MS4 Guidance",
                regulation_id_numbers_json=rin,
            ),
            _notice(
                "2022-28313",
                "2022-12-29",
                "2023-01-18",
                document_type="Proposed Rule",
                title="Small MS4 Clarification; Extension of Comment Period",
                regulation_id_numbers_json=rin,
            ),
        ],
    )
    assert len(periods) == 3


def test_both_sources_close_dates_are_kept_and_the_later_one_closes_the_period(tmp_path):
    """CDC-2026-1288: the Register says September 25, Regulations.gov takes comments through September 28."""
    periods = _build(
        tmp_path,
        dockets=[("CDC-2026-1288", "Customer Surveys Clearance")],
        documents=[
            _document(
                "CDC-2026-1288-0001", "CDC-2026-1288", "2026-07-27", "2026-09-29T03:59:59Z", fr_doc_num="2026-15078"
            ),
        ],
        register=[_notice("2026-15078", "2026-07-27", "2026-09-25", document_type="Notice")],
        links=[("2026-15078", "2026-07-27", "Docket No. CDC-2026-1288")],
    )
    (period,) = periods
    assert (period["register_close_date"], period["regulations_gov_close_date"]) == ("2026-09-25", "2026-09-28")
    assert period["close_date"] == "2026-09-28"


def test_a_placeholder_date_never_closes_a_period(tmp_path):
    """A 2099/2100 sentinel on Regulations.gov and a Register year typo (3008 for 2008) state no close."""
    periods = _build(
        tmp_path,
        dockets=[("FDA-2015-N-3469", "Bulk Drug Substances"), ("TSA-2007-0001", "TSA Shell Docket")],
        documents=[
            _document(
                "FDA-2015-N-3469-0001", "FDA-2015-N-3469", "2015-10-27", "2099-12-31T04:59:59Z", fr_doc_num="2015-27270"
            ),
            _document("TSA-2007-0001-0001", "TSA-2007-0001", "2007-10-02", "2100-01-01T04:59:59Z"),
        ],
        register=[
            _notice("2015-27270", "2015-10-27", "2015-12-28", document_type="Notice"),
            _notice("E8-20355", "2008-09-03", "3008-10-03", document_type="Notice"),
        ],
    )
    (period,) = periods
    assert json.loads(period["evidence_ids_json"]) == ["2015-27270@2015-10-27"]
    assert (period["close_date"], period["regulations_gov_close_date"]) == ("2015-12-28", None)


def test_a_bad_open_date_does_not_make_a_real_close_a_placeholder(tmp_path):
    """Legacy DOT-OST documents carry a 1982 posting date; their 2006 close is a real one."""
    periods = _build(
        tmp_path,
        dockets=[("DOT-OST-2006-24284", "Legacy docket")],
        documents=[_document("DOT-OST-2006-24284-0011", "DOT-OST-2006-24284", "1982-09-02", "2006-03-30T04:59:59Z")],
    )
    (period,) = periods
    assert (period["open_date"], period["close_date"]) == ("1982-09-02", "2006-03-29")


def test_a_notice_naming_only_an_omb_number_has_no_anchor_and_is_kept(tmp_path):
    """NPS 2026-17546, an information-collection notice with no Regulations.gov docket."""
    periods = _build(
        tmp_path,
        register=[_notice("2026-17546", "2026-08-28", "2026-09-28", document_type="Notice")],
        links=[("2026-17546", "2026-08-28", "OMB Control Number 1024-0236")],
    )
    (period,) = periods
    assert period["anchor_kind"] == "none"
    assert period["docket_ids_json"] == period["proceeding_ids_json"] == "[]"
    assert json.loads(period["opened_by_artifact_ids_json"]) == [
        "https://www.federalregister.gov/documents/2026/08/28/2026-17546"
    ]


def test_a_copy_with_no_docket_joins_its_notice_and_a_proceeding_alone_anchors_as_proceeding(tmp_path):
    periods = _build(
        tmp_path,
        documents=[_document("USCG-X-0001", None, "2026-03-02", "2026-04-02T03:59:59Z", fr_doc_num="2026-04100")],
        register=[_notice("2026-04100", "2026-03-02", "2026-04-01", document_type="Proposed Rule")],
        proceedings=[
            {
                "proceeding_id": "proceeding_uscg",
                "docket_ids_json": "[]",
                "fr_document_ids_json": '["2026-04100@2026-03-02"]',
                "rins_json": '["1625-AC11"]',
            }
        ],
    )
    (period,) = periods
    assert json.loads(period["evidence_ids_json"]) == ["2026-04100@2026-03-02", "USCG-X-0001"]
    assert (period["anchor_kind"], json.loads(period["rins_json"])) == ("proceeding", ["1625-AC11"])


def test_each_register_record_and_document_sits_in_at_most_one_period(tmp_path):
    docket = "EPA-HQ-OW-2026-2509"
    rin = '["2040-AG20"]'
    periods = _build(
        tmp_path,
        dockets=[(docket, "Water Quality"), ("EPA_FRDOC_0001", "Recently Posted EPA Rules and Notices.")],
        documents=[
            _document(f"{docket}-0001", docket, "2026-08-05", "2026-09-05T03:59:59Z", fr_doc_num="2026-15000"),
            _document(
                "EPA_FRDOC_0001-9001", "EPA_FRDOC_0001", "2026-08-05", "2026-09-05T03:59:59Z", fr_doc_num="2026-15000"
            ),
            _document(f"{docket}-0002", docket, "2026-09-02", "2026-10-06T03:59:59Z", fr_doc_num="2026-18000"),
            _document(f"{docket}-0003", docket, "2026-08-05", "2026-09-05T03:59:59Z", title="Supporting analysis"),
        ],
        register=[
            _notice(
                "2026-15000", "2026-08-05", "2026-09-04", document_type="Proposed Rule", regulation_id_numbers_json=rin
            ),
            _notice(
                "2026-18000",
                "2026-09-02",
                "2026-10-05",
                document_type="Proposed Rule",
                title="Water Quality; Extension of Comment Period",
                regulation_id_numbers_json=rin,
            ),
        ],
        links=[("2026-15000", "2026-08-05", f"Docket No. {docket}")],
    )
    held = Counter(evidence for p in periods for evidence in json.loads(p["evidence_ids_json"]))
    assert held and max(held.values()) == 1
    extended = _one_holding(periods, "2026-15000@2026-08-05")
    assert _holding(extended, "2026-18000@2026-09-02") and extended["close_date"] == "2026-10-05"
    assert {p["actor_id"] for p in periods} == {ACTOR_ID} == {"spicy-regs:comment-periods:v12"}


def test_a_copy_reopened_after_the_register_window_closed_is_a_second_period(tmp_path):
    """Regulations.gov reopened its copy a month after the Register's window closed: a gap starts a new period."""
    periods = _build(
        tmp_path,
        dockets=[("NHTSA-2026-0100", "Reopened")],
        documents=[
            _document(
                "NHTSA-2026-0100-0001", "NHTSA-2026-0100", "2026-05-01", "2026-06-01T03:59:59Z", fr_doc_num="2026-05000"
            ),
        ],
        register=[_notice("2026-05000", "2026-03-02", "2026-04-01", document_type="Proposed Rule")],
        links=[("2026-05000", "2026-03-02", "Docket No. NHTSA-2026-0100")],
    )
    assert sorted((p["open_date"], p["close_date"], p["evidence_ids_json"]) for p in periods) == [
        ("2026-03-02", "2026-04-01", '["2026-05000@2026-03-02"]'),
        ("2026-05-01", "2026-05-31", '["NHTSA-2026-0100-0001"]'),
    ]


def test_a_notices_register_record_is_listed_though_only_its_copy_states_a_window(tmp_path):
    """FDA 2026-18349 states no close and 2026-18400 an unusable one; their copies' windows make the periods."""
    periods = _build(
        tmp_path,
        dockets=[("FDA-2026-N-10232", "Guidance"), ("FDA-2026-N-10300", "Guidance")],
        documents=[
            _document(
                "FDA-2026-N-10232-0001",
                "FDA-2026-N-10232",
                "2026-09-09",
                "2026-10-10T03:59:59Z",
                fr_doc_num="2026-18349",
            ),
            _document(
                "FDA-2026-N-10300-0001",
                "FDA-2026-N-10300",
                "2026-09-10",
                "2026-10-11T03:59:59Z",
                fr_doc_num="2026-18400",
            ),
        ],
        register=[
            _notice("2026-18349", "2026-09-09", None, document_type="Notice"),
            _notice("2026-18400", "2026-09-10", "2026-09-01", document_type="Notice"),
        ],
    )
    guidance = _one_holding(periods, "FDA-2026-N-10232-0001")
    assert json.loads(guidance["evidence_ids_json"]) == ["2026-18349@2026-09-09", "FDA-2026-N-10232-0001"]
    assert guidance["source"] == "documents.comment_end_date"
    opened_by = ["https://www.regulations.gov/document/FDA-2026-N-10232-0001"]
    assert json.loads(guidance["opened_by_artifact_ids_json"]) == opened_by
    assert guidance["comment_period_id"] == stable_id("comment_period", canonical_json(opened_by))
    inverted = _one_holding(periods, "FDA-2026-N-10300-0001")
    assert json.loads(inverted["evidence_ids_json"]) == ["2026-18400@2026-09-10", "FDA-2026-N-10300-0001"]


def test_a_windowless_register_record_is_listed_once_when_its_copies_windows_split(tmp_path):
    periods = _build(
        tmp_path,
        dockets=[("FAA-2026-0100", "One"), ("FAA-2026-0200", "Two")],
        documents=[
            _document(
                "FAA-2026-0100-0001", "FAA-2026-0100", "2026-04-01", "2026-05-01T03:59:59Z", fr_doc_num="2026-05100"
            ),
            _document(
                "FAA-2026-0200-0001", "FAA-2026-0200", "2026-06-01", "2026-07-01T03:59:59Z", fr_doc_num="2026-05100"
            ),
        ],
        register=[_notice("2026-05100", "2026-04-01", None, document_type="Notice")],
    )
    assert sorted((p["open_date"], p["evidence_ids_json"]) for p in periods) == [
        ("2026-04-01", '["2026-05100@2026-04-01","FAA-2026-0100-0001"]'),
        ("2026-06-01", '["FAA-2026-0200-0001"]'),
    ]


def test_a_document_stating_a_close_but_no_opening_is_left_out_and_counted(tmp_path):
    """114 legacy documents (closing 1973-2011) state a comment end but neither a start nor a posting date."""
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        periods = _build(
            tmp_path,
            dockets=[("EPA-HQ-OAR-2002-0011", "Legacy")],
            documents=[
                {
                    "document_id": "EPA-HQ-OAR-2002-0011-0001",
                    "docket_id": "EPA-HQ-OAR-2002-0011",
                    "comment_end_date": "2002-08-13T03:59:59Z",
                }
            ],
        )
    finally:
        logger.remove(sink)
    assert periods == []
    assert any("skipped undated source windows (documents.comment_end_date=1)" in m for m in messages)
