"""Golden-file-style fixture test for every rule-target evidence source.

Pins which source edges become rows, that malformed JSON is skipped and counted
without dropping the other sources, and that only dockets present in the source
corpus join — never an unbacked or untrusted link.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_proceedings import build_proceedings
from spicy_regs.transforms.build_rule_targets import CITES_ACTION_NOTICE, COLUMNS, build_rule_targets


def _write(path, columns, rows):
    write_parquet_rows(path, columns=columns, rows=rows)


def test_rule_target_spine_emits_only_action_specific_edges(tmp_path):
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [{"docket_id": "EPA-HQ-OAR-2024-0001", "rin": "2060-AV12", "modify_date": "2024-01-10"}],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date", "modify_date"),
        [
            {
                "document_id": "EPA-HQ-OAR-2024-0001-0001",
                "docket_id": "EPA-HQ-OAR-2024-0001",
                "additional_rins": '["2060-AV13"]',
                "fr_doc_num": "2024-00001",
                "posted_date": "2024-02-01",
                "modify_date": "2024-02-02",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "cfr_references_json",
            "regulation_id_numbers_json",
            "publication_date",
        ),
        [
            {
                "document_number": "2024-00001",
                "cfr_references_json": '[{"title": 40, "part": 60}]',
                "regulation_id_numbers_json": '["2060-AV12"]',
                "publication_date": "2024-02-01",
            }
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [{"document_number": "2024-00001", "docket_id": "EPA-HQ-OAR-2024-0001"}],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        (
            "rin",
            "agenda_edition",
            "cfr_references_json",
            "first_action_date",
            "next_action_date",
        ),
        [
            {
                "rin": "2060-AV12",
                "agenda_edition": "202404",
                "cfr_references_json": '["40 CFR 63"]',
                "first_action_date": "2024-03-01",
                "next_action_date": "2024-05-01",
            },
            {
                "rin": "2060-AV13",
                "agenda_edition": "202404",
                "cfr_references_json": '["40 CFR 64"]',
                "first_action_date": "2024-03-02",
                "next_action_date": None,
            },
        ],
    )

    output = build_rule_targets(
        tmp_path,
        run_id="golden-run",
        asserted_at="2026-07-23T12:00:00Z",
    )
    rows = pq.read_table(output).to_pylist()
    observed = {(row["source"], row["cfr_ref"], row["rin"]) for row in rows}
    assert observed == {
        ("docket_rin", None, "2060-AV12"),
        ("document_rin", None, "2060-AV13"),
        ("fr_cfr_ref", "40-60", "2060-AV12"),
        ("document_fr_doc", "40-60", "2060-AV12"),
        ("docket_document_cites_action_notice", None, None),
    }
    assert pq.ParquetFile(output).schema_arrow.names == list(COLUMNS)
    assert all(row["method"] == "deterministic" for row in rows)
    assert all(row["actor_id"] == "spicy-regs:rule-targets:v6" for row in rows)
    assert all(row["run_id"] == "golden-run" for row in rows)


def test_rule_targets_skip_and_count_malformed_json_without_dropping_other_sources(tmp_path):
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [{"docket_id": "EPA-X", "rin": "2060-AV12", "modify_date": "2024-01-01"}],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date", "modify_date"),
        [{"document_id": "D1", "docket_id": "EPA-X", "additional_rins": "{bad", "fr_doc_num": None}],
    )
    _write(
        tmp_path / "federal_register.parquet",
        ("document_number", "cfr_references_json", "regulation_id_numbers_json", "publication_date"),
        [],
    )
    _write(tmp_path / "fr_docket_links.parquet", ("document_number", "docket_id"), [])
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "cfr_references_json", "first_action_date", "next_action_date"),
        [],
    )

    rows = pq.read_table(
        build_rule_targets(tmp_path, run_id="malformed", asserted_at="2026-07-23T12:00:00Z")
    ).to_pylist()
    assert [(row["source"], row["rin"]) for row in rows] == [("docket_rin", "2060-AV12")]


def test_rule_targets_accept_source_backed_underscores_and_reject_untrusted_links(tmp_path):
    valid_docket = "EPA-HQ-OAR-2026-0001"
    underscore_docket = "EPA_FRDOC_0001"
    rin = "2060-ZZ01"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [
            {"docket_id": valid_docket, "rin": rin, "modify_date": "2026-01-01"},
            {"docket_id": underscore_docket, "rin": rin, "modify_date": "2026-01-01"},
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date"),
        [
            {
                "document_id": f"{valid_docket}-0001",
                "docket_id": valid_docket,
                "additional_rins": f'["{rin}"]',
                "fr_doc_num": "2026-00001",
                "posted_date": "2026-01-02",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        ("document_number", "cfr_references_json", "regulation_id_numbers_json", "publication_date"),
        [
            {
                "document_number": "2026-00001",
                "cfr_references_json": '[{"title": 40, "part": 60}]',
                "regulation_id_numbers_json": f'["{rin}"]',
                "publication_date": "2026-01-02",
            }
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": "2026-00001", "docket_id": valid_docket},
            {"document_number": "2026-00001", "docket_id": underscore_docket},
            {"document_number": "2026-00001", "docket_id": "AID_FRDOC_0001"},
            {"document_number": "2026-00001", "docket_id": "Sequence No. 1"},
            # Syntactically plausible, but an antidumping case number rather
            # than a docket present in the Regulations.gov source corpus.
            {"document_number": "2026-00001", "docket_id": "A-570-831"},
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "cfr_references_json"),
        [],
    )

    rows = pq.read_table(
        build_rule_targets(
            tmp_path,
            run_id="fr-label-boundary",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()

    assert rows
    assert {row["docket_id"] for row in rows} == {valid_docket, underscore_docket}
    assert ("fr_cfr_ref", "40-60") in {(row["source"], row["cfr_ref"]) for row in rows}


# Real rows of snapshot_911969b4's parents (receipt typed-citation-join-2026-09-26/).
# FDA-1999-F-0118 holds the sucralose final rule 99-20888, which names only the docket's
# pre-2008 number "99F-0001"; its filing notice 99-518 states no RIN and no stage.
SUCRALOSE = {
    "dockets": [
        {
            "docket_id": "FDA-1999-F-0118",
            "title": "Filing of Food Additive Petition for Sucralose",
            "docket_type": "Nonrulemaking",
        }
    ],
    "documents": [
        {
            "document_id": "FDA-1999-F-0118-0001",
            "docket_id": "FDA-1999-F-0118",
            "document_type": "Notice",
            "title": "McNeil Specialty< Products Co.; Filing of Food Additive Petition",
            "fr_doc_num": "99-518",
            "posted_date": "1999-01-12T05:00:00Z",
        },
        {
            "document_id": "FDA-1999-F-0118-0002",
            "docket_id": "FDA-1999-F-0118",
            "document_type": "Notice",
            "title": "Food Additives Permitted for Direct Addition to Food for Human Consumption;\nSucralose",
            "fr_doc_num": "99 20888",
            "posted_date": "1999-08-18T04:00:00Z",
        },
    ],
    "federal_register": [
        {
            "document_number": "99-518",
            "publication_date": "1999-01-11",
            "document_type": "Notice",
            "title": "McNeil Specialty Products Co.; Filing of Food Additive Petition",
            "cfr_references_json": "[]",
            "regulation_id_numbers_json": "[]",
        },
        {
            "document_number": "99-20888",
            "publication_date": "1999-08-12",
            "document_type": "Rule",
            "title": "Food Additives Permitted for Direct Addition to Food for Human Consumption; Sucralose",
            "cfr_references_json": '[{"chapter": null, "citation_url": null, "part": 172, "title": 21}]',
            "regulation_id_numbers_json": "[]",
        },
    ],
    "fr_docket_links": [
        {"document_number": number, "publication_date": day, "docket_id": "Docket No. 99F-0001"}
        for number, day in (("99-518", "1999-01-11"), ("99-20888", "1999-08-12"))
    ],
}
# Fan-out: FDA's withdrawal of 128 suitability petitions, E7-3043, is posted in 42 petition
# dockets and names only "Docket No. 2004P-0262"; three of them.
WITHDRAWAL_DOCKETS = ("FDA-1982-N-0006", "FDA-1984-N-0016", "FDA-1984-P-0015")
WITHDRAWAL_DOCUMENTS = ("FDA-1982-N-0006-0012", "FDA-1984-N-0016-0001", "FDA-1984-P-0015-0001")
SUITABILITY = {
    "dockets": [
        {"docket_id": docket, "title": title, "docket_type": "Nonrulemaking"}
        for docket, title in zip(
            WITHDRAWAL_DOCKETS,
            (
                "Drug Products List for ANDA's",
                "Disopyramide Phosphate",
                "EXTEND LIST OF DRUG PROD SUITABLE FOR ANDA APPLICATIONS-CLOSED",
            ),
        )
    ],
    "documents": [
        {
            "document_id": document,
            "docket_id": docket,
            "document_type": "Notice",
            "title": "Withdrawal of Approval of 128 Suitability Petitions",
            "fr_doc_num": "E7-3043",
            "posted_date": "2007-03-01T05:00:00Z",
        }
        for docket, document in zip(WITHDRAWAL_DOCKETS, WITHDRAWAL_DOCUMENTS)
    ],
    "federal_register": [
        {
            "document_number": "E7-3043",
            "publication_date": "2007-02-23",
            "document_type": "Notice",
            "title": "Withdrawal of Approval of 128 Suitability Petitions",
            "cfr_references_json": "[]",
            "regulation_id_numbers_json": "[]",
        }
    ],
    "fr_docket_links": [
        {"document_number": "E7-3043", "publication_date": "2007-02-23", "docket_id": "Docket No. 2004P-0262"}
    ],
}
_CITATION_COLUMNS = {
    "dockets": ("docket_id", "title", "docket_type", "rin", "modify_date"),
    "documents": ("document_id", "docket_id", "document_type", "title", "fr_doc_num", "additional_rins", "posted_date"),
    "federal_register": (
        "document_number",
        "publication_date",
        "document_type",
        "title",
        "cfr_references_json",
        "regulation_id_numbers_json",
    ),
    "fr_docket_links": ("document_number", "publication_date", "docket_id"),
    "unified_agenda": ("rin", "agenda_edition"),
}


_RUN = "typed-citation"
_ASSERTED_AT = "2026-09-26T12:00:00Z"


def _build(root, tables):
    for name, columns in _CITATION_COLUMNS.items():
        _write(root / f"{name}.parquet", columns, tables.get(name, []))
    targets = pq.read_table(build_rule_targets(root, run_id=_RUN, asserted_at=_ASSERTED_AT)).to_pylist()
    return targets, _proceedings(root)


def _proceedings(root):
    return pq.read_table(build_proceedings(root, run_id=_RUN, asserted_at=_ASSERTED_AT)).to_pylist()


def _citations(targets):
    """Each (docket, citing document, its fr_doc_num, cited notice) the typed edges carry."""
    return {
        (row["docket_id"], reference["evidence_id"], reference["document_number"], tuple(reference["candidate_ids"]))
        for row in targets
        if row["source"] == CITES_ACTION_NOTICE
        for reference in json.loads(row["fr_references_json"])
    }


@pytest.mark.parametrize(
    ("tables", "expected_citations", "expected_proceedings"),
    [
        pytest.param(
            SUCRALOSE,
            {("FDA-1999-F-0118", "FDA-1999-F-0118-0002", "99 20888", ("99-20888@1999-08-12",))},
            {(("FDA-1999-F-0118",), ()), ((), ("99-20888@1999-08-12",))},
            id="FDA-1999-F-0118-cites-99-20888",
        ),
        pytest.param(
            SUITABILITY,
            {
                (docket, document, "E7-3043", ("E7-3043@2007-02-23",))
                for docket, document in zip(WITHDRAWAL_DOCKETS, WITHDRAWAL_DOCUMENTS)
            },
            {*(((docket,), ()) for docket in WITHDRAWAL_DOCKETS), ((), ("E7-3043@2007-02-23",))},
            id="E7-3043-fans-out-to-petition-dockets",
        ),
    ],
)
def test_a_cited_action_notice_is_a_typed_edge_that_merges_no_proceeding(
    tmp_path, tables, expected_citations, expected_proceedings
):
    targets, proceedings = _build(tmp_path, tables)

    assert _citations(targets) == expected_citations
    edges = [row for row in targets if row["source"] == CITES_ACTION_NOTICE]
    assert all(row["cfr_ref"] is None and row["rin"] is None for row in edges), "a relationship, not a rule target"
    assert all(row["evidence_id"] in {citation[1] for citation in expected_citations} for row in edges)
    # The docket stays its own proceeding and the notice its own FR-only one.
    assert {
        (tuple(json.loads(row["docket_ids_json"])), tuple(json.loads(row["fr_document_ids_json"])))
        for row in proceedings
    } == expected_proceedings

    # The edges change no proceeding: without them the stage writes the same rows.
    (tmp_path / "proceedings.parquet").unlink()
    _write(tmp_path / "rule_targets.parquet", COLUMNS, [row for row in targets if row["source"] != CITES_ACTION_NOTICE])
    assert _proceedings(tmp_path) == proceedings


def test_a_cited_notice_with_no_rin_or_stage_is_no_typed_edge(tmp_path):
    """99-518, the sucralose filing notice, is cited but is no action evidence."""
    tables = {**SUCRALOSE, "documents": SUCRALOSE["documents"][:1]}
    targets, proceedings = _build(tmp_path, tables)
    assert ("document_fr_doc", None, None) in {(row["source"], row["cfr_ref"], row["rin"]) for row in targets}
    assert not _citations(targets)
    assert {
        (tuple(json.loads(row["docket_ids_json"])), tuple(json.loads(row["fr_document_ids_json"])))
        for row in proceedings
    } == {((), ("99-20888@1999-08-12",))}, "a Nonrulemaking docket citing no action notice is no action docket"
