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
    assert all(row["actor_id"] == "spicy-regs:comment-periods:v9" for row in periods)


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
    assert {row["actor_id"] for row in proceedings} == {"spicy-regs:proceedings:v10"}

    # Its comment period keeps the docket as its anchor, with no proceeding.
    (period,) = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    assert (json.loads(period["docket_ids_json"]), period["proceeding_ids_json"]) == ([shell], "[]")


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
    ("docket", "expected"),
    [
        ("EPA_FRDOC_0001", True),
        ("NOAA_FRDOC_0001", True),
        ("CCC_FRDOC_0001", True),
        ("EPA-HQ-OAR-2021-0317", False),
        ("FAA-2013-0259", False),  # a feed-like title, outside the ruling
        ("EPA_FRDOC", False),
        ("GIPSA-2010-FGIS-0014-NONRULEMAKING", False),
    ],
)
def test_catch_all_dockets_are_the_federal_register_feed_dockets(docket, expected):
    assert catch_all_docket(docket) is expected


def test_a_catch_all_docket_takes_nothing_from_the_documents_it_posts(tmp_path):
    """Real rows of the R5 parents (receipt frdoc-catchalls-2026-09-26/), owner ruling 2026-09-26.

    NOAA_FRDOC_0001 posts the Amendment 5 drift-gillnet proposal (2017-19662, RIN 0648-BG81,
    which names no docket) with the RIN on its own document; EPA_FRDOC_0001 posts the 2012
    extremely-hazardous-substances rule (2012-6910, RIN 2050-AF08), whose docket is
    EPA-HQ-SFUND-2010-0586. Each feed docket stays a proceeding by its type, holds no RIN,
    CFR part or stage of those rules and tracks no agenda item; the citations stay typed edges.
    """
    feeds = {
        "NOAA_FRDOC_0001": ("FR Pending Documents", "NOAA"),
        "EPA_FRDOC_0001": ("Recently Posted EPA Rules and Notices from FR Feed.", "EPA"),
    }
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "title", "docket_type", "rin", "agency_code", "modify_date"),
        [
            *(
                {
                    "docket_id": docket,
                    "title": title,
                    "docket_type": "Rulemaking",
                    "rin": "Not Assigned",
                    "agency_code": agency,
                    "modify_date": "2026-09-25T12:26:10Z",
                }
                for docket, (title, agency) in feeds.items()
            ),
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
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "publication_date", "docket_id"),
        [
            {"document_number": "2012-6910", "publication_date": "2012-03-22", "docket_id": docket}
            for docket in ("EPA-HQ-SFUND-2010-0586", "FRL-9651-1")
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
    }
    by_dockets = {tuple(json.loads(row["docket_ids_json"])): row for row in proceedings}
    for docket, (title, agency) in feeds.items():
        feed = by_dockets[(docket,)]  # a proceeding by its type, Rulemaking, like any other docket
        assert (feed["title"], feed["agency_code"]) == (title, agency)
        assert (
            feed["rins_json"]
            == feed["cfr_refs_json"]
            == feed["stage_events_json"]
            == feed["fr_document_ids_json"]
            == "[]"
        )
        assert feed["current_stage"] is None
    # The rules keep their RINs where they belong.
    noaa_notice = by_dockets[()]
    assert json.loads(noaa_notice["fr_document_ids_json"]) == ["2017-19662@2017-09-15"]
    assert json.loads(noaa_notice["rins_json"]) == ["0648-BG81"]
    epa_rule = by_dockets[("EPA-HQ-SFUND-2010-0586",)]
    assert json.loads(epa_rule["rins_json"]) == ["2050-AF08"]
    assert json.loads(epa_rule["cfr_refs_json"]) == ["40-355"]
    # An agenda item tracks the rule's proceeding, never the feed's.
    assert {(row["rin"], row["proceeding_id"], row["source"]) for row in links} == {
        ("0648-BG81", noaa_notice["proceeding_id"], "federal_register_rin"),
        ("2050-AF08", epa_rule["proceeding_id"], "federal_register_rin"),
    }
    assert {row["rin"]: row["scope_status"] for row in items} == {
        "0648-BG81": "single_observed",
        "2050-AF08": "single_observed",
    }
    # The feed document's own comment period keeps its own RIN, and no other.
    feed_proceeding = by_dockets[("NOAA_FRDOC_0001",)]["proceeding_id"]
    (feed_period,) = [row for row in periods if feed_proceeding in json.loads(row["proceeding_ids_json"])]
    assert json.loads(feed_period["rins_json"]) == ["0648-BG81"]
