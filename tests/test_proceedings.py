"""Golden reference-corpus test for proceedings and reopened comment periods."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import stable_id, write_parquet_rows
from spicy_regs.ontology.federal_register import catch_all_docket
from spicy_regs.transforms.build_comment_periods import (
    COLUMNS as COMMENT_PERIOD_COLUMNS,
    build_comment_periods,
)
from spicy_regs.transforms.build_proceedings import (
    COLUMNS as PROCEEDING_COLUMNS,
    _current_stage_from_events,
    build_proceedings,
)
from spicy_regs.transforms.build_regulatory_agenda import build_regulatory_agenda
from spicy_regs.transforms.build_rule_targets import CITES_ACTION_NOTICE, build_rule_targets


def _write(path, columns, rows):
    write_parquet_rows(path, columns=columns, rows=rows)


def test_current_stage_requires_unique_latest_stage_family_event():
    events = [
        {"stage": "proposed", "event_kind": "proceedingProposed", "effective_date": "2026-01-01"},
        {"stage": "final", "event_kind": "proceedingFinal", "effective_date": "2026-02-01"},
    ]
    assert _current_stage_from_events(events) == "final"

    events.append({"stage": "withdrawn", "event_kind": "proceedingWithdrawn", "effective_date": "2026-02-01"})
    assert _current_stage_from_events(events) is None
    assert (
        _current_stage_from_events([{"stage": "final", "event_kind": "proceedingFinal", "effective_date": None}])
        is None
    )
    with pytest.raises(ValueError, match="disagrees"):
        _current_stage_from_events(
            [{"stage": "final", "event_kind": "proceedingProposed", "effective_date": "2026-02-01"}]
        )


def test_reference_proceeding_threads_rinless_docket_and_preserves_reopening(tmp_path):
    docket_id = "EPA-HQ-OAR-2021-0044"
    rin = "2060-AV16"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket_id,
                "rin": None,
                "docket_type": "Rulemaking",
                "title": "Methane Emissions Standards",
                "agency_code": "EPA",
                "modify_date": "2024-03-08",
            }
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "fr_doc_num",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": "D-PROPOSAL",
                "docket_id": docket_id,
                "fr_doc_num": "2021-24202",
                "additional_rins": "[]",
                "document_type": "Proposed Rule",
                "title": "Standards proposal",
                "agency_code": "EPA",
                "posted_date": "2021-11-15",
                "comment_start_date": "2021-11-15",
                "comment_end_date": "2022-01-14",
            },
            {
                "document_id": "D-EXTENSION",
                "docket_id": docket_id,
                "fr_doc_num": "2021-27312",
                "additional_rins": "[]",
                "document_type": "Notice",
                "title": "Comment period extension",
                "agency_code": "EPA",
                "posted_date": "2021-12-17",
                "comment_start_date": "2021-12-17",
                "comment_end_date": "2022-01-31",
            },
            {
                "document_id": "D-SUPPLEMENTAL",
                "docket_id": docket_id,
                "fr_doc_num": "2022-24675",
                "additional_rins": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Supplemental proposal",
                "agency_code": "EPA",
                "posted_date": "2022-12-06",
                "comment_start_date": "2022-12-06",
                "comment_end_date": "2023-01-05",
            },
            {
                "document_id": "D-FINAL",
                "docket_id": docket_id,
                "fr_doc_num": "2024-00366",
                "additional_rins": f'["{rin}"]',
                "document_type": "Rule",
                "title": "Final standards",
                "agency_code": "EPA",
                "posted_date": "2024-03-08",
                "comment_start_date": None,
                "comment_end_date": None,
            },
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [
            {
                "document_number": "2021-24202",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Standards proposal",
                "publication_date": "2021-11-15",
                "comments_close_on": "2022-01-14",
            },
            {
                "document_number": "2021-27312",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Notice",
                "title": "Comment period extension",
                "publication_date": "2021-12-17",
                "comments_close_on": "2022-01-31",
            },
            {
                "document_number": "2022-24675",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Supplemental proposal",
                "publication_date": "2022-12-06",
                "comments_close_on": "2023-01-05",
            },
            {
                "document_number": "2024-00366",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Rule",
                "title": "Final standards",
                "publication_date": "2024-03-08",
                "comments_close_on": None,
            },
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage", "first_action_date"),
        [
            {
                "rin": rin,
                "agenda_edition": "202404",
                "title": "Methane Emissions Standards",
                "agency_code": "EPA",
                "rule_stage": "Final Rule Stage",
                "first_action_date": "2021-11-15",
            }
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": number, "docket_id": docket_id}
            for number in ("2021-24202", "2021-27312", "2022-24675", "2024-00366")
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"),
        [
            {
                "docket_id": docket_id,
                "rin": rin,
                "cfr_ref": "40-60",
                "cfr_title": "40",
                "cfr_part": "60",
                "cfr_section": None,
            }
        ],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [{"rin": rin, "usc_title": "42", "usc_section": "7401", "pl_number": None}],
    )

    proceedings_file = build_proceedings(
        tmp_path,
        run_id="reference-corpus",
        asserted_at="2026-07-23T12:00:00Z",
    )
    proceedings = pq.read_table(proceedings_file).to_pylist()
    assert pq.ParquetFile(proceedings_file).schema_arrow.names == list(PROCEEDING_COLUMNS)
    assert len(proceedings) == 1
    proceeding = proceedings[0]
    assert proceeding["rin"] == rin
    assert json.loads(proceeding["docket_ids_json"]) == [docket_id]
    assert proceeding["current_stage"] == "final"
    assert json.loads(proceeding["cfr_refs_json"]) == ["40-60"]
    assert json.loads(proceeding["cfr_target_iris_json"]) == ["urn:rkaf:us:cfr:40:60"]
    assert json.loads(proceeding["authority_refs_json"]) == []
    stages = {event["stage"] for event in json.loads(proceeding["stage_events_json"])}
    assert {"proposed", "supplemental", "final"} <= stages

    periods_file = build_comment_periods(
        tmp_path,
        run_id="reference-corpus",
        asserted_at="2026-07-23T12:00:00Z",
    )
    periods = pq.read_table(periods_file).to_pylist()
    assert pq.ParquetFile(periods_file).schema_arrow.names == list(COMMENT_PERIOD_COLUMNS)
    assert [(row["open_date"], row["close_date"]) for row in periods] == [
        ("2021-11-15", "2022-01-31"),
        ("2022-12-06", "2023-01-05"),
    ]
    assert all(json.loads(row["proceeding_ids_json"]) == [proceeding["proceeding_id"]] for row in periods)
    assert all(json.loads(row["docket_ids_json"]) == [docket_id] for row in periods)
    assert json.loads(periods[0]["opened_by_artifact_ids_json"]) == [
        "https://www.federalregister.gov/documents/2021/11/15/2021-24202",
        "https://www.regulations.gov/document/D-PROPOSAL",
    ]
    assert "D-EXTENSION" in json.loads(periods[0]["evidence_ids_json"])
    assert "https://www.regulations.gov/document/D-EXTENSION" not in json.loads(
        periods[0]["opened_by_artifact_ids_json"]
    )
    assert all(row["method"] == "deterministic" for row in periods)
    assert all(row["actor_id"] == "spicy-regs:comment-periods:v12" for row in periods)


def test_reused_rin_does_not_collapse_or_cross_assign_distinct_dockets(tmp_path):
    rin = "2120-AA64"
    dockets = ("FAA-2025-0001", "FAA-2026-0002")
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket,
                "rin": rin,
                "docket_type": "Rulemaking",
                "title": f"Airworthiness directive {index}",
                "agency_code": "FAA",
                "modify_date": f"202{index + 4}-01-01",
            }
            for index, docket in enumerate(dockets, start=1)
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "fr_doc_num",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": f"DOC-{index}",
                "docket_id": docket,
                "fr_doc_num": f"202{index + 4}-0000{index}",
                "additional_rins": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": f"Proposal {index}",
                "agency_code": "FAA",
                "posted_date": f"202{index + 4}-01-01",
                "comment_start_date": f"202{index + 4}-01-01",
                "comment_end_date": f"202{index + 4}-02-01",
            }
            for index, docket in enumerate(dockets, start=1)
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [
            {
                "document_number": f"202{index + 4}-0000{index}",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": f"Proposal {index}",
                "publication_date": f"202{index + 4}-01-01",
                "comments_close_on": f"202{index + 4}-02-01",
            }
            for index in range(1, 3)
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage", "first_action_date"),
        [],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": f"202{index + 4}-0000{index}", "docket_id": docket}
            for index, docket in enumerate(dockets, start=1)
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref"),
        [{"docket_id": docket, "rin": rin, "cfr_ref": "14-39"} for docket in dockets],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [],
    )

    proceeding_rows = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="reused-rin",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(proceeding_rows) == 2
    assert {tuple(json.loads(row["docket_ids_json"])) for row in proceeding_rows} == {
        (dockets[0],),
        (dockets[1],),
    }

    period_rows = pq.read_table(
        build_comment_periods(
            tmp_path,
            run_id="reused-rin",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(period_rows) == 2
    assert {tuple(json.loads(row["docket_ids_json"])) for row in period_rows} == {(dockets[0],), (dockets[1],)}


def test_untrusted_fr_administrative_labels_do_not_become_proceeding_dockets(tmp_path):
    valid_docket = "EPA-HQ-OAR-2026-0001"
    rin = "2060-ZZ01"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": valid_docket,
                "rin": rin,
                "docket_type": "Rulemaking",
                "title": "Valid proceeding",
                "agency_code": "EPA",
                "modify_date": "2026-01-01",
            },
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "fr_doc_num",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": f"{valid_docket}-0001",
                "docket_id": valid_docket,
                "fr_doc_num": "2026-00001",
                "additional_rins": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Valid proposal",
                "agency_code": "EPA",
                "posted_date": "2026-01-02",
                "comment_start_date": "2026-01-02",
                "comment_end_date": "2026-02-02",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [
            {
                "document_number": "2026-00001",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Valid proposal",
                "publication_date": "2026-01-02",
                "comments_close_on": "2026-02-02",
            }
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage"),
        [],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": "2026-00001", "docket_id": valid_docket},
            {"document_number": "2026-00001", "docket_id": "AID_FRDOC_0001"},
            {"document_number": "2026-00001", "docket_id": "Sequence No. 1"},
            {"document_number": "2026-00001", "docket_id": "A-570-831"},
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref"),
        [{"docket_id": valid_docket, "rin": rin, "cfr_ref": "40-60"}],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [],
    )

    proceeding_rows = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="fr-label-boundary",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()
    assert len(proceeding_rows) == 1
    assert json.loads(proceeding_rows[0]["docket_ids_json"]) == [valid_docket]

    period_rows = pq.read_table(
        build_comment_periods(
            tmp_path,
            run_id="fr-label-boundary",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()
    assert len(period_rows) == 1
    assert json.loads(period_rows[0]["docket_ids_json"]) == [valid_docket]


def test_one_docket_remains_one_proceeding_when_it_reports_multiple_rins(
    tmp_path,
):
    docket_id = "EPA-HQ-OAR-2026-9999"
    rins = ("2060-AA01", "2060-BB02")
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket_id,
                "rin": rins[0],
                "docket_type": "Rulemaking",
                "title": "Shared administrative docket",
                "agency_code": "EPA",
                "modify_date": "2026-01-01",
            }
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": "D-SECOND-RIN",
                "docket_id": docket_id,
                "additional_rins": f'["{rins[1]}"]',
                "document_type": "Notice",
                "title": "Second rulemaking",
                "agency_code": "EPA",
                "posted_date": "2026-01-02",
                "comment_start_date": None,
                "comment_end_date": None,
            },
            {
                "document_id": "D-RINLESS-COMMENT",
                "docket_id": docket_id,
                "additional_rins": "[]",
                "document_type": "Notice",
                "title": "Unscoped comment notice",
                "agency_code": "EPA",
                "posted_date": "2026-02-01",
                "comment_start_date": "2026-02-01",
                "comment_end_date": "2026-03-01",
            },
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage", "first_action_date"),
        [],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref"),
        [{"docket_id": docket_id, "rin": rin, "cfr_ref": "40-60"} for rin in rins],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [],
    )

    proceeding_rows = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="multi-proceeding-docket",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(proceeding_rows) == 1
    assert proceeding_rows[0]["rin"] is None
    assert json.loads(proceeding_rows[0]["rins_json"]) == list(rins)
    assert proceeding_rows[0]["current_stage"] is None
    assert json.loads(proceeding_rows[0]["stage_events_json"]) == []

    period_rows = pq.read_table(
        build_comment_periods(
            tmp_path,
            run_id="multi-proceeding-docket",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(period_rows) == 1
    assert json.loads(period_rows[0]["rins_json"]) == list(rins)
    assert json.loads(period_rows[0]["proceeding_ids_json"]) == [proceeding_rows[0]["proceeding_id"]]
    assert json.loads(period_rows[0]["docket_ids_json"]) == [docket_id]
    assert json.loads(period_rows[0]["opened_by_artifact_ids_json"]) == [
        "https://www.regulations.gov/document/D-RINLESS-COMMENT"
    ]


def test_proceeding_id_survives_new_earlier_docket_and_records_continuity(tmp_path):
    rin = "2060-ZZ99"
    original_docket = "ZZZ-2026-0001"
    added_docket = "AAA-2025-0001"

    def write_sources(dockets, links):
        _write(
            tmp_path / "dockets.parquet",
            ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
            [
                {
                    "docket_id": docket,
                    "rin": rin,
                    "docket_type": "Rulemaking",
                    "title": "Stable identity fixture",
                    "agency_code": "EPA",
                    "modify_date": "2026-01-01",
                }
                for docket in dockets
            ],
        )
        _write(
            tmp_path / "documents.parquet",
            (
                "document_id",
                "docket_id",
                "additional_rins",
                "document_type",
                "title",
                "agency_code",
                "posted_date",
            ),
            [],
        )
        _write(
            tmp_path / "federal_register.parquet",
            (
                "document_number",
                "regulation_id_numbers_json",
                "document_type",
                "title",
                "publication_date",
            ),
            [
                {
                    "document_number": "2026-00001",
                    "regulation_id_numbers_json": f'["{rin}"]',
                    "document_type": "Proposed Rule",
                    "title": "Stable identity fixture",
                    "publication_date": "2026-01-01",
                }
            ],
        )
        _write(
            tmp_path / "unified_agenda.parquet",
            ("rin", "agenda_edition", "title", "agency_code", "rule_stage"),
            [],
        )
        _write(
            tmp_path / "fr_docket_links.parquet",
            ("document_number", "docket_id"),
            [{"document_number": "2026-00001", "docket_id": docket} for docket in links],
        )
        _write(
            tmp_path / "rule_targets.parquet",
            ("docket_id", "rin", "cfr_ref"),
            [{"docket_id": docket, "rin": rin, "cfr_ref": "40-60"} for docket in dockets],
        )
        _write(
            tmp_path / "authority_edges.parquet",
            ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
            [],
        )

    write_sources((original_docket,), (original_docket,))
    first = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="identity-first",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(first) == 1
    stable_proceeding_id = first[0]["proceeding_id"]

    # The new docket sorts before the original and would change a stateless
    # min-docket hash. One FR document explicitly links both into one component.
    write_sources(
        (added_docket, original_docket),
        (added_docket, original_docket),
    )
    second = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="identity-second",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()

    assert len(second) == 1
    assert second[0]["proceeding_id"] == stable_proceeding_id
    assert json.loads(second[0]["docket_ids_json"]) == [
        added_docket,
        original_docket,
    ]
    assert second[0]["supersedes_id"] == stable_proceeding_id


def test_unscoped_rin_keeps_identity_when_one_docket_becomes_known(tmp_path):
    rin = "2060-YY98"
    docket_id = "EPA-HQ-OAR-2026-0098"

    def write_sources(*, include_docket: bool) -> None:
        _write(
            tmp_path / "dockets.parquet",
            ("docket_id", "rin", "docket_type", "title", "agency_code"),
            (
                [
                    {
                        "docket_id": docket_id,
                        "rin": rin,
                        "docket_type": "Rulemaking",
                        "title": "Scope transition fixture",
                        "agency_code": "EPA",
                    }
                ]
                if include_docket
                else []
            ),
        )
        _write(
            tmp_path / "documents.parquet",
            (
                "document_id",
                "docket_id",
                "additional_rins",
                "document_type",
                "title",
                "agency_code",
                "posted_date",
            ),
            [],
        )
        _write(
            tmp_path / "federal_register.parquet",
            (
                "document_number",
                "regulation_id_numbers_json",
                "document_type",
                "title",
                "publication_date",
            ),
            [
                {
                    "document_number": "2026-00098",
                    "regulation_id_numbers_json": f'["{rin}"]',
                    "document_type": "Proposed Rule",
                    "title": "Scope transition fixture",
                    "publication_date": "2026-01-01",
                }
            ],
        )
        _write(
            tmp_path / "unified_agenda.parquet",
            ("rin", "agenda_edition", "title", "agency_code", "rule_stage"),
            [],
        )
        _write(
            tmp_path / "fr_docket_links.parquet",
            ("document_number", "docket_id"),
            ([{"document_number": "2026-00098", "docket_id": docket_id}] if include_docket else []),
        )
        _write(
            tmp_path / "rule_targets.parquet",
            ("docket_id", "rin", "cfr_ref"),
            ([{"docket_id": docket_id, "rin": rin, "cfr_ref": "40-98"}] if include_docket else []),
        )
        _write(
            tmp_path / "authority_edges.parquet",
            ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
            [],
        )

    write_sources(include_docket=False)
    first = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="unscoped-first",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(first) == 1
    stable_id = first[0]["proceeding_id"]
    assert json.loads(first[0]["docket_ids_json"]) == []

    write_sources(include_docket=True)
    second = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="unscoped-second",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()
    assert len(second) == 1
    assert second[0]["proceeding_id"] == stable_id
    assert json.loads(second[0]["docket_ids_json"]) == [docket_id]
    assert second[0]["supersedes_id"] == stable_id


def test_a_nonrulemaking_docket_is_a_proceeding_only_on_action_evidence(tmp_path):
    """Decision 32 as amended: a docket needs a RIN, the exact type ``Rulemaking``, or action evidence.

    Action evidence is a document of its own with a RIN or a rule stage, a document of its
    own that cites an FR document with a RIN or a rule stage, or an FR link from such a
    document. A RIN-less notice naming a docket is none of these.
    """
    shell, with_rin, staged, rulemaking = (
        "FDA-2023-H-0001",
        "FAA-2023-0002",
        "DOT-OST-2023-0003",
        "EPA-HQ-OAR-2023-0004",
    )
    # Real: DOT-OST-2012-0168-0056 cites 2016-24862, which states RIN 2105-ZA02; and the
    # RIN-less notice 2021-06210 names FDA-2020-E-1269 (with two more dockets).
    cites, noticed = "DOT-OST-2012-0168", "FDA-2020-E-1269"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {"docket_id": shell, "rin": None, "docket_type": "Nonrulemaking", "title": "Civil money penalty"},
            {"docket_id": with_rin, "rin": "2120-AA64", "docket_type": "Nonrulemaking", "title": "Exemption"},
            {"docket_id": staged, "rin": None, "docket_type": "Nonrulemaking", "title": "Petition"},
            {"docket_id": rulemaking, "rin": None, "docket_type": "Rulemaking", "title": "Standards"},
            {"docket_id": cites, "rin": None, "docket_type": "Nonrulemaking", "title": "State freight plans"},
            {"docket_id": noticed, "rin": None, "docket_type": "Nonrulemaking", "title": "Patent term"},
        ],
    )
    # The shell's one document opens a comment period but is no stage and states no RIN.
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "posted_date",
            "comment_end_date",
            "fr_doc_num",
        ),
        [
            {
                "document_id": f"{shell}-0001",
                "docket_id": shell,
                "additional_rins": "[]",
                "document_type": "Notice",
                "title": "Complaint",
                "posted_date": "2023-01-05",
                "comment_end_date": "2023-02-06",
            },
            {
                "document_id": f"{cites}-0056",
                "docket_id": cites,
                "additional_rins": "[]",
                "document_type": "Notice",
                "title": "Guidance on State Freight Plans and State Freight Advisory Committees",
                "fr_doc_num": "2016-24862",
            },
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        ("document_number", "publication_date", "regulation_id_numbers_json", "document_type", "title"),
        [
            {
                "document_number": "2023-00001",
                "publication_date": "2023-01-03",
                "regulation_id_numbers_json": "[]",
                "document_type": "Proposed Rule",
                "title": "Proposed exemption standards",
            },
            {
                "document_number": "2016-24862",
                "publication_date": "2016-10-14",
                "regulation_id_numbers_json": '["2105-ZA02"]',
                "document_type": "Notice",
                "title": "Guidance on State Freight Plans and State Freight Advisory Committees",
            },
            {
                "document_number": "2021-06210",
                "publication_date": "2021-03-25",
                "regulation_id_numbers_json": "[]",
                "document_type": "Notice",
                "title": "Determination of Regulatory Review Period for Purposes of Patent Extension; BAROSTIM NEO",
            },
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("docket_id", "document_number", "publication_date"),
        [
            {"docket_id": f"Docket No. {staged}", "document_number": "2023-00001", "publication_date": "2023-01-03"},
            {
                "docket_id": "Docket Nos. FDA-2020-E-1269, FDA-2020-E-1273, and FDA-2020-E-1272",
                "document_number": "2021-06210",
                "publication_date": "2021-03-25",
            },
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet", ("docket_id", "rin", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"), []
    )

    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    by_docket = {docket: row for row in proceedings for docket in json.loads(row["docket_ids_json"])}
    assert set(by_docket) == {with_rin, staged, rulemaking, cites}, "neither the shell nor the noticed docket"
    assert json.loads(by_docket[staged]["fr_document_ids_json"]) == ["2023-00001@2023-01-03"]
    assert json.loads(by_docket[with_rin]["rins_json"]) == ["2120-AA64"]
    assert not any("2021-06210@2021-03-25" in row["fr_document_ids_json"] for row in proceedings)
    assert {row["actor_id"] for row in proceedings} == {"spicy-regs:proceedings:v12"}

    # Its comment period keeps the docket as its anchor, with no proceeding.
    (period,) = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    assert (json.loads(period["docket_ids_json"]), period["proceeding_ids_json"]) == ([shell], "[]")


def _empty_rulemaking_inputs(root):
    _write(
        root / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [],
    )
    _write(
        root / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "document_type", "title", "posted_date", "fr_doc_num"),
        [],
    )
    _write(
        root / "fr_docket_links.parquet",
        ("docket_id", "document_number", "publication_date"),
        [],
    )
    _write(root / "rule_targets.parquet", ("docket_id", "rin", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"), [])


def _fr_rows(root, rows):
    _write(
        root / "federal_register.parquet",
        (
            "document_number",
            "publication_date",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "agencies_json",
            "cfr_references_json",
        ),
        rows,
    )


def test_an_sro_register_notice_forms_no_proceeding(tmp_path):
    """An SEC SRO notice's title says "Proposed Rule Change"; its type says Notice (decision 56)."""
    _empty_rulemaking_inputs(tmp_path)
    _fr_rows(
        tmp_path,
        [
            {
                "document_number": "2024-12345",
                "publication_date": "2024-02-01",
                "regulation_id_numbers_json": "[]",
                "document_type": "Notice",
                "title": (
                    "Self-Regulatory Organizations; Notice of Filing of Proposed Rule Change by "
                    "the New York Stock Exchange, LLC"
                ),
            }
        ],
    )
    assert pq.read_table(build_proceedings(tmp_path)).to_pylist() == []


def test_a_1994_uncategorized_row_types_from_its_title_suffix(tmp_path):
    """Decision 59: "; Final Rule DEPARTMENT OF EDUCATION" founds a proceeding; no agency, none."""
    _empty_rulemaking_inputs(tmp_path)
    _fr_rows(
        tmp_path,
        [
            {
                "document_number": "94-29324",
                "publication_date": "1994-12-01",
                "regulation_id_numbers_json": "[]",
                "document_type": "Uncategorized Document",
                "title": "Federal Pell Grant Program; Final Rule DEPARTMENT OF EDUCATION",
            },
            {
                "document_number": "94-28708",
                "publication_date": "1994-11-22",
                "regulation_id_numbers_json": "[]",
                "document_type": "Uncategorized Document",
                "title": "Acid Rain Program: Permits; Final Rule",
            },
        ],
    )
    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    assert [row["current_stage"] for row in proceedings] == ["final"]
    assert json.loads(proceedings[0]["fr_document_ids_json"]) == ["94-29324@1994-12-01"]


def test_the_registers_type_wins_for_a_regulations_gov_copy(tmp_path):
    """Decision 60: a document whose fr_doc_num resolves to one Register row takes that row's stage."""
    _empty_rulemaking_inputs(tmp_path)
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {"docket_id": docket, "rin": None, "docket_type": "Nonrulemaking", "title": docket}
            for docket in ("NOAA-2024-0001", "OSHA-2024-0002", "EPA-2024-0003", "DOD-2024-0004")
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "document_type", "title", "posted_date", "fr_doc_num"),
        [
            {
                # A "Rule"-typed copy of a Register Notice: no stage, so no action docket.
                "document_id": "NOAA-2024-0001-0001",
                "docket_id": "NOAA-2024-0001",
                "additional_rins": "[]",
                "document_type": "Rule",
                "title": "Fishery Management Plan; Final Rule",
                "posted_date": "2024-03-01",
                "fr_doc_num": "2024-11111",
            },
            {
                # An untyped copy of a Register Proposed Rule: the Register's stage wins.
                "document_id": "OSHA-2024-0002-0001",
                "docket_id": "OSHA-2024-0002",
                "additional_rins": "[]",
                "document_type": "Other",
                "title": "Workplace standards",
                "posted_date": "2024-03-02",
                "fr_doc_num": "2024-22222",
            },
            {
                # A number the Register does not hold: the document's own type applies.
                "document_id": "EPA-2024-0003-0001",
                "docket_id": "EPA-2024-0003",
                "additional_rins": "[]",
                "document_type": "Proposed Rule",
                "title": "Air standards proposal",
                "posted_date": "2024-03-03",
                "fr_doc_num": "2024-99999",
            },
            {
                # A copy of a Register row that states no type: the document's own type
                # stands (decision 60 as amended 2026-09-26).
                "document_id": "DOD-2024-0004-0001",
                "docket_id": "DOD-2024-0004",
                "additional_rins": "[]",
                "document_type": "Rule",
                "title": "Defense acquisition final rule",
                "posted_date": "2024-03-04",
                "fr_doc_num": "2024-44444",
            },
        ],
    )
    _fr_rows(
        tmp_path,
        [
            {
                "document_number": "2024-44444",
                "publication_date": "2024-02-12",
                "regulation_id_numbers_json": "[]",
                "document_type": "Uncategorized Document",
                "title": "Defense Acquisition Regulations",
            },
            {
                "document_number": "2024-11111",
                "publication_date": "2024-02-10",
                "regulation_id_numbers_json": "[]",
                "document_type": "Notice",
                "title": "Fishery Management Plan; Notice",
            },
            {
                "document_number": "2024-22222",
                "publication_date": "2024-02-11",
                "regulation_id_numbers_json": "[]",
                "document_type": "Proposed Rule",
                "title": "Workplace standards; Proposed Rule",
            },
        ],
    )
    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    by_docket = {docket: row for row in proceedings for docket in json.loads(row["docket_ids_json"])}
    assert set(by_docket) == {"OSHA-2024-0002", "EPA-2024-0003", "DOD-2024-0004"}, (
        "the Notice-copy's docket is no action docket"
    )
    stages = {
        event["evidence_id"]: event["stage"]
        for row in by_docket.values()
        for event in json.loads(row["stage_events_json"])
    }
    assert stages == {
        "OSHA-2024-0002-0001": "proposed",
        # The Register proposal its copy states joins the copy's proceeding (decision 56).
        "2024-22222@2024-02-11": "proposed",
        "EPA-2024-0003-0001": "proposed",
        "DOD-2024-0004-0001": "final",
    }
    assert all(row["docket_ids_json"] != "[]" for row in proceedings)


def test_x_codes_decide_no_action_evidence_but_stay_recorded(tmp_path):
    """Decision 61: an X-pattern code (NOAA's 0648-X… here) is recorded evidence of nothing; other RINs act."""
    _empty_rulemaking_inputs(tmp_path)
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {"docket_id": "NOAA-NMFS-2024-0001", "rin": "0648-XC39", "docket_type": "Nonrulemaking", "title": "X-only"},
            {"docket_id": "EPA-2024-0002", "rin": "2060-AV12", "docket_type": "Nonrulemaking", "title": "Real RIN"},
            {"docket_id": "NOAA-NMFS-2024-0003", "rin": "0648-XC39", "docket_type": "Rulemaking", "title": "Typed"},
            {
                "docket_id": "NOAA-NMFS-2024-0004",
                "rin": None,
                "docket_type": "Nonrulemaking",
                "title": "Document shell",
            },
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "document_type", "title", "posted_date", "fr_doc_num"),
        [
            {
                "document_id": "NOAA-NMFS-2024-0004-0001",
                "docket_id": "NOAA-NMFS-2024-0004",
                "additional_rins": '["0648-XH24"]',
                "document_type": "Notice",
                "title": "In-season adjustment",
                "posted_date": "2024-03-01",
            }
        ],
    )
    _fr_rows(
        tmp_path,
        [
            {
                "document_number": "2024-33333",
                "publication_date": "2024-02-20",
                "regulation_id_numbers_json": '["0648-XW87"]',
                "document_type": "Notice",
                "title": "Fisheries of the Exclusive Economic Zone; Closure",
            },
            {
                "document_number": "2024-44444",
                "publication_date": "2024-02-21",
                "regulation_id_numbers_json": '["2120-AA64"]',
                "document_type": "Notice",
                "title": "Airspace revision",
            },
            {
                "document_number": "2024-55555",
                "publication_date": "2024-02-22",
                "regulation_id_numbers_json": '["0648-XA53"]',
                "document_type": "Rule",
                "title": "Fisheries of the Exclusive Economic Zone; Final rule",
            },
        ],
    )
    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    by_docket = {docket: row for row in proceedings for docket in json.loads(row["docket_ids_json"])}
    # The X-only docket founds nothing; the real RIN and the Rulemaking type still do.
    assert set(by_docket) == {"EPA-2024-0002", "NOAA-NMFS-2024-0003"}
    # The X RIN stays recorded on the proceeding that exists by its type.
    assert json.loads(by_docket["NOAA-NMFS-2024-0003"]["rins_json"]) == ["0648-XC39"]
    # An FR row whose only RIN is an X code founds no docket-less proceeding; a real RIN still does.
    fr_only = {
        json.loads(row["fr_document_numbers_json"])[0]: row for row in proceedings if row["docket_ids_json"] == "[]"
    }
    assert set(fr_only) == {"2024-44444", "2024-55555"}
    # A real Rule whose only RIN is an X code still forms its proceeding through its type, and
    # the X code stays recorded on it.
    assert json.loads(fr_only["2024-55555"]["rins_json"]) == ["0648-XA53"]


def _register_fixture(root, *, dockets, documents=(), register=(), links=(), rule_targets=(), build_targets=False):
    """Dockets ``(docket_id, rin[, docket_type])``, Rulemaking unless typed, their documents, rule_targets
    ``(docket_id, rin, source)`` rows or, with ``build_targets``, rule_targets built from the same inputs,
    and Register rows linked as ``links`` says."""
    assert not (rule_targets and build_targets)
    root.mkdir(parents=True, exist_ok=True)
    _empty_rulemaking_inputs(root)
    _write(
        root / "rule_targets.parquet",
        ("docket_id", "rin", "source", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"),
        [{"docket_id": docket, "rin": rin, "source": source} for docket, rin, source in rule_targets],
    )
    _write(
        root / "fr_docket_links.parquet",
        ("docket_id", "document_number", "publication_date"),
        [
            {"docket_id": docket, "document_number": number, "publication_date": "2024-02-01"}
            for docket, number in links
        ],
    )
    _write(
        root / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket,
                "rin": rin,
                "docket_type": kind[0] if kind else "Rulemaking",
                "title": docket,
                "agency_code": "EPA",
            }
            for docket, rin, *kind in dockets
        ],
    )
    _write(
        root / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "document_type", "title", "posted_date", "fr_doc_num"),
        [
            {
                "document_id": f"{docket}-{n:04d}",
                "docket_id": docket,
                "document_type": "Rule",
                "title": f"Copy of {number}",
                "posted_date": "2024-03-01",
                "fr_doc_num": number,
            }
            for n, (docket, number) in enumerate(documents, start=1)
        ],
    )
    _fr_rows(
        root,
        [
            {"publication_date": "2024-02-01", "document_type": "Rule", "regulation_id_numbers_json": "[]", **row}
            for row in register
        ],
    )
    if build_targets:
        build_rule_targets(root)
    return pq.read_table(build_proceedings(root)).to_pylist()


def _by_docket(proceedings):
    return {docket: row for row in proceedings for docket in json.loads(row["docket_ids_json"])}


def _docket_less(proceedings):
    return {
        json.loads(row["fr_document_numbers_json"])[0]: row for row in proceedings if row["docket_ids_json"] == "[]"
    }


def test_a_register_copy_joins_the_one_proceeding_its_copies_lie_in(tmp_path):
    """Decision 56 (B): an unlinked Register rule joins its copies' one proceeding; copies in two unite none."""
    proceedings = _register_fixture(
        tmp_path,
        dockets=[("EPA-2024-0001", None), ("EPA-2024-0002", None), ("EPA-2024-0003", None)],
        documents=[("EPA-2024-0001", "2024-10001"), ("EPA-2024-0002", "2024-10002"), ("EPA-2024-0003", "2024-10002")],
        register=[
            {"document_number": "2024-10001", "title": "One copy; final rule"},
            {"document_number": "2024-10002", "title": "Two copies; final rule"},
        ],
    )
    by_docket = _by_docket(proceedings)
    joined = by_docket["EPA-2024-0001"]
    assert json.loads(joined["fr_document_ids_json"]) == ["2024-10001@2024-02-01"]
    assert "2024-10001@2024-02-01" in {event["evidence_id"] for event in json.loads(joined["stage_events_json"])}
    # Copies in two proceedings: each docket stays its own, and the rule its own docket-less one.
    assert [json.loads(by_docket[d]["docket_ids_json"]) for d in ("EPA-2024-0002", "EPA-2024-0003")] == [
        ["EPA-2024-0002"],
        ["EPA-2024-0003"],
    ]
    assert set(_docket_less(proceedings)) == {"2024-10002"}


def test_an_unlinked_register_rule_attaches_by_a_specific_rin(tmp_path):
    """Decision 56 (E): a RIN one docketed proceeding alone holds attaches; shared and X RINs do not."""
    proceedings = _register_fixture(
        tmp_path,
        dockets=[
            ("DOT-2024-0001", "2120-AA01"),
            ("DOT-2024-0002", "2120-AA02"),
            ("DOT-2024-0003", "2120-AA02"),
            ("NOAA-NMFS-2024-0004", "0648-XC39"),
            ("DOT-2024-0005", "2120-AA05"),
        ],
        register=[
            {"document_number": "2024-20001", "regulation_id_numbers_json": '["2120-AA01"]'},
            {"document_number": "2024-20002", "regulation_id_numbers_json": '["2120-AA02"]'},
            {"document_number": "2024-20003", "regulation_id_numbers_json": '["0648-XC39"]'},
            {"document_number": "2024-20004", "regulation_id_numbers_json": '["2120-AA01", "2120-AA05"]'},
        ],
    )
    attached = _by_docket(proceedings)["DOT-2024-0001"]
    assert json.loads(attached["fr_document_ids_json"]) == ["2024-20001@2024-02-01"]
    assert attached["current_stage"] == "final"
    # Held by two proceedings, an X code, or specific RINs pointing to two: each stays apart.
    assert set(_docket_less(proceedings)) == {"2024-20002", "2024-20003", "2024-20004"}


def test_a_rin_taken_in_with_a_register_document_attracts_no_other(tmp_path):
    """Owner rulings on decision 56 (E): a proceeding holds a RIN only through docket-side evidence.

    USCG-2000-7206, a zebra-mussel docket, took in one linked safety-zone rule stating USCG's
    umbrella RIN 2115-AA97 and the part it amends (33 CFR 165), and drew the 1,016 other
    unlinked rules stating it. rule_targets is built from the same inputs, so the link reaches
    the docket as an fr_cfr_ref row carrying 2115-AA97: that row holds nothing (review 2b), and
    nor does a RIN an attached document brings. A copy's RINs, which rule_targets writes as its
    docket's document_fr_doc row, do hold: the owner kept them.
    """
    d = "@2024-02-01"
    proceedings = _register_fixture(
        tmp_path,
        dockets=[("USCG-2000-7206", None), ("USCG-2024-0001", "1625-AA01"), ("USCG-2024-0002", None)],
        documents=[("USCG-2024-0002", "2024-60005")],
        links=[("USCG-2000-7206", "2024-60001")],
        build_targets=True,
        register=[
            {
                "document_number": "2024-60001",
                "regulation_id_numbers_json": '["2115-AA97"]',
                "cfr_references_json": '[{"title": 33, "part": 165}]',
            },
            {"document_number": "2024-60002", "regulation_id_numbers_json": '["2115-AA97"]'},
            # Attaches by its specific 1625-AA01 and brings 1625-AA99 along ...
            {"document_number": "2024-60003", "regulation_id_numbers_json": '["1625-AA01", "1625-AA99"]'},
            # ... which draws no document after it.
            {"document_number": "2024-60004", "regulation_id_numbers_json": '["1625-AA99"]'},
            {"document_number": "2024-60005", "regulation_id_numbers_json": '["2115-AA98"]'},
            {"document_number": "2024-60006", "regulation_id_numbers_json": '["2115-AA98"]'},
        ],
    )
    targets = {
        (r["docket_id"], r["rin"], r["source"]) for r in pq.read_table(tmp_path / "rule_targets.parquet").to_pylist()
    }
    assert ("USCG-2000-7206", "2115-AA97", "fr_cfr_ref") in targets, "the link's RIN reaches rule_targets"
    assert ("USCG-2024-0002", "2115-AA98", "document_fr_doc") in targets, "so does the copy's"
    by_docket = _by_docket(proceedings)
    joins = {
        docket: {join["fr_document_id"]: join for join in json.loads(row["fr_document_joins_json"])}
        for docket, row in by_docket.items()
    }
    assert {docket: sorted(joined) for docket, joined in joins.items()} == {
        "USCG-2000-7206": [f"2024-60001{d}"],
        "USCG-2024-0001": [f"2024-60003{d}"],
        "USCG-2024-0002": [f"2024-60005{d}", f"2024-60006{d}"],
    }
    assert joins["USCG-2024-0002"][f"2024-60006{d}"]["holder_sources"] == ["rule_targets:document_fr_doc"]
    # The RINs taken in stay recorded on the proceedings that took them in.
    assert "2115-AA97" in json.loads(by_docket["USCG-2000-7206"]["rins_json"])
    assert "1625-AA99" in json.loads(by_docket["USCG-2024-0001"]["rins_json"])
    assert set(_docket_less(proceedings)) == {"2024-60002", "2024-60004"}


def test_a_copy_in_a_feed_docket_counts_toward_no_proceeding(tmp_path):
    """Decision 56 (B): only copies in trusted, non-feed dockets count, so a feed's copy splits nothing.

    A feed docket forms a proceeding only on a RIN of its own (decision 32 as amended); the rule
    it posts still joins the one other proceeding its copies lie in.
    """
    proceedings = _register_fixture(
        tmp_path,
        dockets=[("EPA_FRDOC_0001", "2060-AA09"), ("EPA-2024-0001", None)],
        documents=[("EPA_FRDOC_0001", "2024-90001"), ("EPA-2024-0001", "2024-90001")],
        register=[{"document_number": "2024-90001", "title": "Posted in a feed and filed in its docket; final rule"}],
    )
    by_docket = _by_docket(proceedings)
    assert set(by_docket) == {"EPA_FRDOC_0001", "EPA-2024-0001"}
    assert json.loads(by_docket["EPA-2024-0001"]["fr_document_joins_json"]) == [
        {"fr_document_id": "2024-90001@2024-02-01", "joined_by": "fr_copy"}
    ]
    assert json.loads(by_docket["EPA_FRDOC_0001"]["fr_document_ids_json"]) == []
    assert _docket_less(proceedings) == {}


def test_a_docketed_proceeding_keeps_its_own_agency_code(tmp_path):
    """Decision 56 (C) is for docket-less proceedings: a docketed one keeps its dockets' and documents' code."""
    nsf = json.dumps([{"id": 366, "name": "National Science Foundation", "parent_id": None}])
    proceedings = _register_fixture(
        tmp_path,
        dockets=[("EPA-2024-0001", "2060-AA01")],  # the fixture's dockets are EPA's
        links=[("EPA-2024-0001", "2024-91001"), ("EPA-2024-0001", "2024-91002")],
        register=[
            {"document_number": "2024-91001", "agencies_json": nsf},
            {"document_number": "2024-91002", "agencies_json": nsf},
            {"document_number": "2024-91003", "agencies_json": nsf, "regulation_id_numbers_json": '["2060-AA01"]'},
            {"document_number": "2024-91004", "agencies_json": nsf},
        ],
    )
    (docketed,) = [row for row in proceedings if row["docket_ids_json"] != "[]"]
    assert len(json.loads(docketed["fr_document_ids_json"])) == 3, "two linked, one attached by its RIN"
    assert docketed["agency_code"] == "EPA", "its Register documents' agency, three to one, changes nothing"
    assert _docket_less(proceedings)["2024-91004"]["agency_code"] == "NSF"


def test_a_non_noaa_x_code_decides_nothing_and_makes_no_rin_specific(tmp_path):
    """Decision 61, extended to every agency: NTIA's 0660-XC00 founds nothing and points nowhere.

    On the 2026-09-26 parents FIRSTNET-2017-0001 held 0660-XC00 through a document of its own
    and drew 15 unrelated NTIA notices and rules that stated it.
    """
    proceedings = _register_fixture(
        tmp_path,
        dockets=[("NTIA-2024-0001", "0660-XC00", "Nonrulemaking"), ("FIRSTNET-2017-0001", "0660-XC00")],
        register=[
            {"document_number": "2024-70001", "document_type": "Notice", "regulation_id_numbers_json": '["0660-XC00"]'},
            {"document_number": "2024-70002", "regulation_id_numbers_json": '["0660-XC00"]'},
        ],
    )
    by_docket = _by_docket(proceedings)
    # The Nonrulemaking docket's X code makes it no action docket; the Rulemaking one stands by its type.
    assert set(by_docket) == {"FIRSTNET-2017-0001"}
    assert json.loads(by_docket["FIRSTNET-2017-0001"]["rins_json"]) == ["0660-XC00"], "still recorded"
    assert json.loads(by_docket["FIRSTNET-2017-0001"]["fr_document_ids_json"]) == []
    # The Notice with only the code founds nothing; the Rule stands alone by its type.
    assert set(_docket_less(proceedings)) == {"2024-70002"}


def test_each_register_document_says_how_it_joined(tmp_path):
    """fr_document_joins_json: one entry per Register document; stage events carry only joined_by."""
    proceedings = _register_fixture(
        tmp_path,
        dockets=[("EPA-2024-0001", "2060-AA01"), ("EPA-2024-0002", None)],
        documents=[("EPA-2024-0001", "2024-80002")],
        links=[("EPA-2024-0002", "2024-80001"), ("EPA-2024-0002", "2024-80008")],
        rule_targets=[
            ("EPA-2024-0001", "2060-AA01", "document_fr_doc"),
            ("EPA-2024-0001", "2060-AA07", "document_rin"),
            ("EPA-2024-0002", "2060-AA05", "fr_cfr_ref"),
        ],
        register=[
            {"document_number": "2024-80001", "title": "Linked; final rule"},
            {"document_number": "2024-80001", "title": "Linked; final rule"},  # the same row twice
            {"document_number": "2024-80002", "title": "Copied; final rule"},
            {"document_number": "2024-80003", "regulation_id_numbers_json": '["2060-AA01"]'},
            {"document_number": "2024-80004"},
            # Its RIN only a link's fr_cfr_ref row puts on a docket: it points nowhere (review 2b).
            {"document_number": "2024-80005", "regulation_id_numbers_json": '["2060-AA05"]'},
            # No stage: a Notice that acts by its RIN.
            {"document_number": "2024-80006", "document_type": "Notice", "regulation_id_numbers_json": '["2060-AA07"]'},
            # Two RINs, each this proceeding's alone.
            {"document_number": "2024-80007", "regulation_id_numbers_json": '["2060-AA07", "2060-AA01"]'},
            # No action evidence at all, named by a docket link.
            {"document_number": "2024-80008", "document_type": "Notice"},
        ],
    )
    for row in proceedings:
        # One entry per Register document the proceeding holds, in id order.
        joins = [join["fr_document_id"] for join in json.loads(row["fr_document_joins_json"])]
        assert joins == json.loads(row["fr_document_ids_json"])
    joins = {
        join.pop("fr_document_id").removesuffix("@2024-02-01"): join
        for row in proceedings
        for join in json.loads(row["fr_document_joins_json"])
    }
    rule_targets_copy = "rule_targets:document_fr_doc"
    assert joins == {
        "2024-80001": {"joined_by": "fr_docket_link"},
        "2024-80002": {"joined_by": "fr_copy"},
        "2024-80003": {
            "joined_by": "specific_rin",
            "joined_rins": ["2060-AA01"],
            "holder_sources": ["docket_rin", rule_targets_copy],
        },
        "2024-80004": {"joined_by": "fr_document"},
        "2024-80005": {"joined_by": "fr_document"},
        "2024-80006": {"joined_by": "specific_rin", "joined_rins": ["2060-AA07"], "holder_sources": ["document_rin"]},
        "2024-80007": {
            "joined_by": "specific_rin",
            "joined_rins": ["2060-AA01", "2060-AA07"],
            "holder_sources": ["docket_rin", "document_rin", rule_targets_copy],
        },
        "2024-80008": {"joined_by": "fr_docket_link"},
    }
    events = [event for row in proceedings for event in json.loads(row["stage_events_json"])]
    assert {tuple(sorted(event)) for event in events} == {
        ("effective_date", "event_kind", "evidence_id", "joined_by", "source", "stage")
    }, "events carry only joined_by; the detail lives in fr_document_joins_json"
    assert {event["evidence_id"].removesuffix("@2024-02-01"): event["joined_by"] for event in events} == {
        "EPA-2024-0001-0001": "docket",
        "2024-80001": "fr_docket_link",
        "2024-80002": "fr_copy",
        "2024-80003": "specific_rin",
        "2024-80004": "fr_document",
        "2024-80005": "fr_document",
        "2024-80007": "specific_rin",
    }
    assert len(events) == 7, "the same document joined the same way is one event"
    (linked,) = [row for row in proceedings if row["docket_ids_json"] == '["EPA-2024-0002"]']
    assert "2060-AA05" in json.loads(linked["rins_json"]), "the fr_cfr_ref row's RIN stays recorded"


def test_a_docket_less_proceeding_takes_its_register_agency(tmp_path):
    """Decision 56 (C): one Regulations.gov code for a single-agency row; none for a joint one."""
    epa = {"id": 145, "name": "Environmental Protection Agency", "parent_id": None}
    nsf = {"id": 366, "name": "National Science Foundation", "parent_id": None}
    proceedings = _register_fixture(
        tmp_path,
        dockets=[],
        register=[
            # The Register's agencies_json is a JSON string, as the source column stores it.
            {"document_number": "2024-30001", "agencies_json": json.dumps([epa])},
            {"document_number": "2024-30002", "agencies_json": json.dumps([epa, nsf])},
        ],
    )
    docket_less = _docket_less(proceedings)
    assert (docket_less["2024-30001"]["agency_code"], docket_less["2024-30002"]["agency_code"]) == ("EPA", None)


def test_a_docket_less_proceeding_follows_the_agency_registry(tmp_path):
    """Proceedings v11: a bridged agency takes its counterpart's code, a renamed one its successor's."""
    energy = {"id": 136, "name": "Energy Department", "parent_id": None}
    hhs = {"id": 221, "name": "Health and Human Services Department", "parent_id": None}
    hcfa = {"id": 559, "name": "Health Care Finance Administration", "parent_id": 221}
    ins = {"id": 232, "name": "Immigration and Naturalization Service", "parent_id": 268}
    proceedings = _register_fixture(
        tmp_path,
        dockets=[],
        register=[
            {"document_number": "2024-30003", "agencies_json": json.dumps([energy])},
            {"document_number": "2024-30004", "agencies_json": json.dumps([hhs, hcfa])},
            {"document_number": "2024-30005", "agencies_json": json.dumps([ins])},
        ],
    )
    codes = {number: row["agency_code"] for number, row in _docket_less(proceedings).items()}
    assert codes == {"2024-30003": "DOE", "2024-30004": "CMS", "2024-30005": None}, "INS split three ways"


def test_register_documents_join_and_attach_without_merging_a_docketed_proceeding(tmp_path):
    """Decision 33 under 56: B and E add Register evidence but change no docketed id or docket set."""
    dockets = [("EPA-2024-0001", "2060-AA01"), ("EPA-2024-0002", "2060-AA02"), ("EPA-2024-0003", "2060-AA02")]
    documents = [("EPA-2024-0001", "2024-40001"), ("EPA-2024-0002", "2024-40002"), ("EPA-2024-0003", "2024-40002")]
    register = [
        {"document_number": "2024-40001"},  # one copy: joins EPA-2024-0001
        {"document_number": "2024-40002"},  # copies in two: stays
        {"document_number": "2024-40003", "regulation_id_numbers_json": '["2060-AA01"]'},  # specific: attaches
        {"document_number": "2024-40004", "regulation_id_numbers_json": '["2060-AA02"]'},  # shared: stays
        # One specific RIN beside a shared one: attaches to EPA-2024-0001.
        {"document_number": "2024-40005", "regulation_id_numbers_json": '["2060-AA01", "2060-AA02"]'},
    ]

    def docketed(proceedings):
        return {(row["proceeding_id"], row["docket_ids_json"]) for row in proceedings if row["docket_ids_json"] != "[]"}

    with_register = _register_fixture(tmp_path / "with", dockets=dockets, documents=documents, register=register)
    without = _register_fixture(tmp_path / "without", dockets=dockets, documents=documents)
    assert docketed(with_register) == docketed(without)
    assert {docket_ids for _, docket_ids in docketed(without)} == {f'["{docket}"]' for docket, _ in dockets}
    assert set(_docket_less(with_register)) == {"2024-40002", "2024-40004"}


def _identity_fixture(tmp_path, groups, prior):
    """Rulemaking dockets, one Proposed Rule uniting each group of two or more, and ``(id, dockets)`` prior rows."""
    dockets = sorted({docket for group in groups for docket in group})
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [{"docket_id": docket, "docket_type": "Rulemaking"} for docket in dockets],
    )
    _write(tmp_path / "documents.parquet", ("document_id", "docket_id", "additional_rins", "document_type"), [])
    united = [group for group in groups if len(group) > 1]
    numbers = [f"2026-{n:05d}" for n in range(1, len(united) + 1)]
    _write(
        tmp_path / "federal_register.parquet",
        ("document_number", "publication_date", "regulation_id_numbers_json", "document_type", "title"),
        [{"document_number": n, "publication_date": "2026-01-02", "document_type": "Proposed Rule"} for n in numbers],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("docket_id", "document_number", "publication_date"),
        [
            {"docket_id": docket, "document_number": number, "publication_date": "2026-01-02"}
            for number, group in zip(numbers, united)
            for docket in group
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet", ("docket_id", "rin", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"), []
    )
    _write(
        tmp_path / "_proceedings_prior.parquet",
        ("proceeding_id", "docket_ids_json", "fr_document_ids_json"),
        [{"proceeding_id": pid, "docket_ids_json": json.dumps(ds), "fr_document_ids_json": "[]"} for pid, ds in prior],
    )
    rows = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    assert len({row["proceeding_id"] for row in rows}) == len(rows)
    return {tuple(json.loads(row["docket_ids_json"])): row for row in rows}


def test_an_id_held_for_its_minting_group_is_released_once_that_group_takes_another(tmp_path):
    """Prior P = {a,b,c} under b's minted id; now A = {a,c} and B = {b,d,e}, and B continues Q.

    B mints P, so P is held for B while B has no id. B takes Q (more overlap), which releases
    P, and A, whose best match P is, continues it, as plain overlap scoring always did.
    """
    a, b, c, d, e = (f"EPA-HQ-OAR-2020-000{n}" for n in range(1, 6))
    p_id, q_id = stable_id("proceeding", "docket", b), stable_id("proceeding", "docket", d)
    by_dockets = _identity_fixture(tmp_path, [[a, c], [b, d, e]], [(p_id, [a, b, c]), (q_id, [d, e])])
    assert set(by_dockets) == {(a, c), (b, d, e)}
    assert by_dockets[(a, c)]["proceeding_id"] == by_dockets[(a, c)]["supersedes_id"] == p_id
    assert by_dockets[(b, d, e)]["proceeding_id"] == by_dockets[(b, d, e)]["supersedes_id"] == q_id


def test_a_hold_lifted_mid_pass_lets_the_waiting_group_take_its_best_id(tmp_path):
    """Prior P = {a,b,c} (b's minted id), Q = {b,d,e}, R = {a,f}; now A = {a,c}, B = {b,d,e}, F = {f}.

    B takes Q, which lifts the hold on P within the same pass, so A takes P (overlap 200),
    not R (100), and F continues R: what overlap alone kept before the hold existed.
    """
    a, b, c, d, e, f = (f"EPA-HQ-OAR-2020-000{n}" for n in range(1, 7))
    p_id = stable_id("proceeding", "docket", b)
    by_dockets = _identity_fixture(
        tmp_path,
        [[a, c], [b, d, e], [f]],
        [(p_id, [a, b, c]), ("proceeding_q", [b, d, e]), ("proceeding_r", [a, f])],
    )
    assert {key: row["proceeding_id"] for key, row in by_dockets.items()} == {
        (a, c): p_id,
        (b, d, e): "proceeding_q",
        (f,): "proceeding_r",
    }


@pytest.mark.parametrize(
    ("docket", "title", "expected"),
    [
        ("EPA_FRDOC_0001", "Recently Posted EPA Rules and Notices from FR Feed.", True),
        ("NOAA_FRDOC_0001", "FR Pending Documents", True),
        ("BSC_FRDOC_0001", None, True),  # the identifier alone
        ("FAA-2013-0259", "Federal Registers for Applications, Notices, and Orders - Miscellaneous", True),
        ("DOT-OST-2009-0092", "Federal Registers for Applications, Notices and Orders - Miscellaneous", True),
        ("TSA-2013-0001", "Federal Registers for Applications, Notices, and Orders - Miscellaneous", True),
        ("HHS-OS-2022-0008", "HHS 2022 Publications", True),
        ("EIA-2009-0002", "Title: This docket contains Federal Register Notices from the DOE EIA FDMS sandbox.", True),
        ("EERE-2011-OT-0001", "This docket contains Federal Register Notices from the EERE-OT FDMS Sandbox", True),
        ("BPA-2011-0001", "This docket contains Federal Register Notices from the BPA sandbox.", True),
        ("WAPA-2011-0001", "This docket contains Federal Register Notices from the DOE WAPA FDMS sandbox. ", True),
        ("FAA-2007-0004", "Duplicate FR Feed Documents", True),
        # Neither id nor title alone makes a feed.
        ("FAA-2013-0259", None, False),
        ("EPA-HQ-OAR-2021-0317", "Federal Registers for Applications, Notices, and Orders - Miscellaneous", False),
        ("CDC-2016-0088", "HHS 2022 Publications", False),
        ("CFPB-2018-0042", "Policy on No-Action Letters and the BCFP Product Sandbox", False),
        ("DOT-OST-2009-0083", "Standard Instrument Approach Procedures; Miscellaneous Amendments", False),
        ("EPA_FRDOC", None, False),
    ],
)
def test_catch_all_dockets_are_the_federal_register_feed_dockets(docket, title, expected):
    assert catch_all_docket(docket, title) is expected


def test_a_catch_all_docket_takes_nothing_from_the_documents_it_posts(tmp_path):
    """Real rows of the R5 parents (receipt frdoc-catchalls-2026-09-26/), owner ruling 2026-09-26.

    NOAA_FRDOC_0001 posts the Amendment 5 drift-gillnet proposal (2017-19662, RIN 0648-BG81,
    which names no docket) with the RIN on its own document; EPA_FRDOC_0001 posts the 2012
    extremely-hazardous-substances rule (2012-6910, RIN 2050-AF08), whose docket is
    EPA-HQ-SFUND-2010-0586; DOT filed its 2025 denied-boarding rule (2025-02814, RIN
    2105-AF30) under its Miscellaneous feed, DOT-OST-2009-0092, which the rule names. No feed
    forms a proceeding on its Rulemaking type or on those rules; FMC_FRDOC_0001 does on the
    RIN it states, and holds that alone. No feed tracks an agenda item, and the citations stay
    typed edges.
    """
    feeds = {
        "NOAA_FRDOC_0001": ("FR Pending Documents", "NOAA", "Rulemaking", "Not Assigned"),
        "EPA_FRDOC_0001": ("Recently Posted EPA Rules and Notices from FR Feed.", "EPA", "Rulemaking", "Not Assigned"),
        "DOT-OST-2009-0092": (
            "Federal Registers for Applications, Notices and Orders - Miscellaneous",
            "DOT",
            "Nonrulemaking",
            None,
        ),
    }
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "title", "docket_type", "rin", "agency_code", "modify_date"),
        [
            *(
                {
                    "docket_id": docket,
                    "title": title,
                    "docket_type": docket_type,
                    "rin": rin,
                    "agency_code": agency,
                    "modify_date": "2026-09-25T12:26:10Z",
                }
                for docket, (title, agency, docket_type, rin) in feeds.items()
            ),
            {
                "docket_id": "FMC_FRDOC_0001",
                "title": "Recently Posted FMC Rules and Notices.",
                "docket_type": "Rulemaking",
                "rin": "3072-AC92",
                "agency_code": "FMC",
                "modify_date": "2026-09-22T12:26:00Z",
            },
            {
                "docket_id": "EPA-HQ-SFUND-2010-0586",
                "title": "Emergency Planning and Community Right-to-Know Act; Amendments to Emergency Planning and Notification",
                "docket_type": "Rulemaking",
                "rin": "Not Assigned",
                "agency_code": "EPA",
                "modify_date": "2022-04-13T01:21:32Z",
            },
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "document_type",
            "title",
            "fr_doc_num",
            "additional_rins",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": "NOAA_FRDOC_0001-4419",
                "docket_id": "NOAA_FRDOC_0001",
                "document_type": "Proposed Rule",
                "title": "Fisheries off West Coast States: Highly Migratory Fisheries; Amendment 5 to the Highly Migratory Species Fishery Management Plan",
                "fr_doc_num": "2017-19662",
                "additional_rins": '["0648-BG81", "0648-BG81"]',
                "agency_code": "NOAA",
                "posted_date": "2017-09-15T04:00:00Z",
                "comment_start_date": "2017-09-15T04:00:00Z",
                "comment_end_date": "2017-11-15T04:59:59Z",
            },
            {
                "document_id": "EPA_FRDOC_0001-12068",
                "docket_id": "EPA_FRDOC_0001",
                "document_type": "Rule",
                "title": "Duplicate to be deleted: Emergency Planning and List of Extremely Hazardous Substances and Threshold Planning Quantities",
                "fr_doc_num": "2012-06910",
                "agency_code": "EPA",
                "posted_date": "2012-03-22T04:00:00Z",
            },
            {
                "document_id": "EPA-HQ-SFUND-2010-0586-0033",
                "docket_id": "EPA-HQ-SFUND-2010-0586",
                "document_type": "Rule",
                "title": "Emergency Planning and Notification; Emergency Planning and List of Extremely Hazardous Substances and Threshold Planning Quantities",
                "fr_doc_num": "2012-6910",
                "agency_code": "EPA",
                "posted_date": "2012-04-27T04:00:00Z",
            },
            {
                "document_id": "DOT-OST-2009-0092-0557",
                "docket_id": "DOT-OST-2009-0092",
                "document_type": "Rule",
                "title": "Periodic Revisions to Denied Boarding Compensation and Domestic Baggage Liability Limits",
                "fr_doc_num": "2025-02814",
                "agency_code": "DOT",
                "posted_date": "2025-02-20T05:00:00Z",
            },
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "publication_date",
            "document_type",
            "title",
            "docket_ids_json",
            "regulation_id_numbers_json",
            "cfr_references_json",
            "comments_close_on",
        ),
        [
            {
                "document_number": "2017-19662",
                "publication_date": "2017-09-15",
                "document_type": "Proposed Rule",
                "title": "Fisheries Off West Coast States; Highly Migratory Fisheries; Amendment 5 to the Highly Migratory Species Fishery Management Plan",
                "docket_ids_json": "[]",
                "regulation_id_numbers_json": '["0648-BG81"]',
                "cfr_references_json": '[{"chapter": null, "citation_url": null, "part": 660, "title": 50}]',
                "comments_close_on": "2017-11-14",
            },
            {
                "document_number": "2012-6910",
                "publication_date": "2012-03-22",
                "document_type": "Rule",
                "title": "Emergency Planning and Notification; Emergency Planning and List of Extremely Hazardous Substances and Threshold Planning Quantities",
                "docket_ids_json": '["EPA-HQ-SFUND-2010-0586", "FRL-9651-1"]',
                "regulation_id_numbers_json": '["2050-AF08"]',
                "cfr_references_json": '[{"chapter": null, "citation_url": null, "part": 355, "title": 40}]',
            },
            {
                "document_number": "2025-02814",
                "publication_date": "2025-02-20",
                "document_type": "Rule",
                "title": "Periodic Revisions to Denied Boarding Compensation and Domestic Baggage Liability Limits",
                "docket_ids_json": '["Docket No. DOT-OST-2009-0092"]',
                "regulation_id_numbers_json": '["2105-AF30"]',
                "cfr_references_json": "[]",
            },
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "publication_date", "docket_id"),
        [
            *(
                {"document_number": "2012-6910", "publication_date": "2012-03-22", "docket_id": docket}
                for docket in ("EPA-HQ-SFUND-2010-0586", "FRL-9651-1")
            ),
            {
                "document_number": "2025-02814",
                "publication_date": "2025-02-20",
                "docket_id": "Docket No. DOT-OST-2009-0092",
            },
        ],
    )
    _write(tmp_path / "unified_agenda.parquet", ("rin", "agenda_edition"), [])
    run_id, asserted_at = "catch-all", "2026-09-26T12:00:00Z"

    targets = pq.read_table(build_rule_targets(tmp_path, run_id=run_id, asserted_at=asserted_at)).to_pylist()
    proceedings = pq.read_table(build_proceedings(tmp_path, run_id=run_id, asserted_at=asserted_at)).to_pylist()
    items, links = (
        pq.read_table(path).to_pylist()
        for path in build_regulatory_agenda(tmp_path, run_id=run_id, asserted_at=asserted_at)
    )
    periods = pq.read_table(build_comment_periods(tmp_path, run_id=run_id, asserted_at=asserted_at)).to_pylist()

    # The citations stay visible: each feed docket's document cites its notice.
    assert {
        (row["docket_id"], reference["evidence_id"], reference["candidate_ids"][0])
        for row in targets
        if row["source"] == CITES_ACTION_NOTICE
        for reference in json.loads(row["fr_references_json"])
    } >= {
        ("NOAA_FRDOC_0001", "NOAA_FRDOC_0001-4419", "2017-19662@2017-09-15"),
        ("EPA_FRDOC_0001", "EPA_FRDOC_0001-12068", "2012-6910@2012-03-22"),
        ("DOT-OST-2009-0092", "DOT-OST-2009-0092-0557", "2025-02814@2025-02-20"),
    }
    by_dockets = {tuple(json.loads(row["docket_ids_json"])): row for row in proceedings}
    by_notice = {tuple(json.loads(row["fr_document_ids_json"])): row for row in proceedings}
    # No feed is a proceeding on its type or on the rules it posts; FMC's is, on its own RIN.
    assert set(by_dockets) == {("EPA-HQ-SFUND-2010-0586",), ("FMC_FRDOC_0001",), ()}
    fmc = by_dockets[("FMC_FRDOC_0001",)]
    assert json.loads(fmc["rins_json"]) == ["3072-AC92"]
    assert fmc["cfr_refs_json"] == fmc["stage_events_json"] == fmc["fr_document_ids_json"] == "[]"
    # The rules keep their RINs where they belong; the DOT rule is its own FR-only proceeding.
    noaa_notice = by_notice[("2017-19662@2017-09-15",)]
    assert json.loads(noaa_notice["rins_json"]) == ["0648-BG81"]
    dot_rule = by_notice[("2025-02814@2025-02-20",)]
    assert (json.loads(dot_rule["docket_ids_json"]), json.loads(dot_rule["rins_json"])) == ([], ["2105-AF30"])
    epa_rule = by_dockets[("EPA-HQ-SFUND-2010-0586",)]
    assert json.loads(epa_rule["rins_json"]) == ["2050-AF08"]
    assert json.loads(epa_rule["cfr_refs_json"]) == ["40-355"]
    # An agenda item tracks the rule's proceeding, never a feed's.
    assert {(row["rin"], row["proceeding_id"], row["source"]) for row in links} == {
        ("0648-BG81", noaa_notice["proceeding_id"], "federal_register_rin"),
        ("2050-AF08", epa_rule["proceeding_id"], "federal_register_rin"),
        ("2105-AF30", dot_rule["proceeding_id"], "federal_register_rin"),
        ("3072-AC92", fmc["proceeding_id"], "docket_rin"),
    }
    # A feed anchors no comment period (owner decision 2026-09-28): the feed's copy joins its notice's
    # period, which the notice's own proceeding anchors, under the notice's RIN.
    (notice_period,) = periods
    assert json.loads(notice_period["evidence_ids_json"]) == ["2017-19662@2017-09-15", "NOAA_FRDOC_0001-4419"]
    assert (notice_period["docket_ids_json"], notice_period["anchor_kind"]) == ("[]", "proceeding")
    assert json.loads(notice_period["proceeding_ids_json"]) == [noaa_notice["proceeding_id"]]
    assert json.loads(notice_period["rins_json"]) == ["0648-BG81"]
