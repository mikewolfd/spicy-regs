"""Dated FR joins and explicit ambiguity across the materialized consumers.

These controlled source rows exercise joins; the unmodified native collision
fixture and its byte provenance are tested in test_federal_register.py.
"""

import json
import shutil

import pyarrow.parquet as pq

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_comment_periods import build_comment_periods
from spicy_regs.transforms.build_federal_register import build_federal_register
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links
from spicy_regs.transforms.build_proceedings import build_proceedings
from spicy_regs.transforms.build_regulatory_agenda import build_regulatory_agenda
from spicy_regs.transforms.build_rule_targets import build_rule_targets


def _write(root, name, columns, rows):
    write_parquet_rows(root / f"{name}.parquet", columns=columns, rows=rows)


def _inputs(root):
    records = [
        {
            "document_number": "00-111",
            "publication_date": "2000-01-14",
            "type": "Proposed Rule",
            "title": "First controlled rule",
            "docket_ids": ["EPA-FIRST"],
            "cfr_references": [{"title": 40, "part": 60}],
            "regulation_id_numbers": ["1111-AA11"],
            "comments_close_on": "2000-02-01",
        },
        {
            "document_number": "00-111",
            "publication_date": "2000-01-18",
            "type": "Proposed Rule",
            "title": "Second controlled rule",
            "docket_ids": ["FAA-SECOND"],
            "cfr_references": [{"title": 49, "part": 71}],
            "regulation_id_numbers": ["2222-BB22"],
            "comments_close_on": "2000-02-01",
        },
    ]
    build_federal_register(root, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    build_fr_docket_links(root)
    _write(
        root,
        "dockets",
        ("docket_id", "docket_type", "rin"),
        [{"docket_id": docket, "docket_type": "Rulemaking"} for docket in ("EPA-FIRST", "FAA-SECOND", "FDA-UNKNOWN")],
    )
    # A Regulations.gov posting date is not an FR publication-date qualifier.
    _write(
        root,
        "documents",
        ("document_id", "docket_id", "fr_doc_num", "additional_rins", "posted_date"),
        [
            {
                "document_id": "FDA-UNKNOWN-0001",
                "docket_id": "FDA-UNKNOWN",
                "fr_doc_num": "00-111",
                "additional_rins": "[]",
                "posted_date": "2000-01-14",
            }
        ],
    )
    _write(root, "unified_agenda", ("rin", "agenda_edition"), [])


def test_dated_bridge_never_cross_wires_colliding_documents(tmp_path):
    _inputs(tmp_path)
    links = pq.read_table(tmp_path / "fr_docket_links.parquet").to_pylist()
    assert {(r["docket_id"], r["publication_date"]) for r in links} == {
        ("EPA-FIRST", "2000-01-14"),
        ("FAA-SECOND", "2000-01-18"),
    }
    targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    assert {(r["docket_id"], r["cfr_ref"], r["rin"]) for r in targets if r["source"] == "fr_cfr_ref"} == {
        ("EPA-FIRST", "40-60", "1111-AA11"),
        ("FAA-SECOND", "49-71", "2222-BB22"),
    }
    unknown = next(r for r in targets if r["source"] == "document_fr_doc")
    assert unknown["cfr_ref"] is None and unknown["rin"] is None
    (reference,) = json.loads(unknown["fr_references_json"])
    assert reference == {
        "source": "documents.fr_doc_num",
        "evidence_id": "FDA-UNKNOWN-0001",
        "document_number": "00-111",
        "publication_date": None,
        "status": "ambiguous",
        "candidate_ids": ["00-111@2000-01-14", "00-111@2000-01-18"],
    }

    # An old collapsed FR-only row is a candidate predecessor of either date,
    # so neither date may silently inherit its identity.
    _write(
        tmp_path,
        "_proceedings_prior",
        ("proceeding_id", "docket_ids_json", "fr_document_numbers_json"),
        [{"proceeding_id": "legacy-collapsed", "docket_ids_json": "[]", "fr_document_numbers_json": '["00-111"]'}],
    )
    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    by_docket = {json.loads(r["docket_ids_json"])[0]: r for r in proceedings}
    assert len(proceedings) == 3 and "legacy-collapsed" not in {r["proceeding_id"] for r in proceedings}
    for docket, day in (("EPA-FIRST", "2000-01-14"), ("FAA-SECOND", "2000-01-18")):
        row = by_docket[docket]
        assert json.loads(row["fr_document_numbers_json"]) == ["00-111"]
        assert json.loads(row["fr_document_ids_json"]) == [f"00-111@{day}"]
        assert json.loads(row["unresolved_fr_references_json"])[0]["status"] == "ambiguous"
        assert json.loads(row["stage_events_json"])[0]["evidence_id"] == f"00-111@{day}"

    _, relationships_path = build_regulatory_agenda(tmp_path)
    relationships = pq.read_table(relationships_path).to_pylist()
    assert {(r["rin"], r["proceeding_id"], r["evidence_id"]) for r in relationships} == {
        ("1111-AA11", by_docket["EPA-FIRST"]["proceeding_id"], "00-111@2000-01-14"),
        ("2222-BB22", by_docket["FAA-SECOND"]["proceeding_id"], "00-111@2000-01-18"),
    }
    assert all("/documents/2000/01/" in r["evidence_uri"] for r in relationships)
    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    assert len(periods) == 2
    for period in periods:
        (docket,) = json.loads(period["docket_ids_json"])
        assert json.loads(period["proceeding_ids_json"]) == [by_docket[docket]["proceeding_id"]]
        assert json.loads(period["evidence_ids_json"]) == json.loads(by_docket[docket]["fr_document_ids_json"])

    shutil.copyfile(tmp_path / "proceedings.parquet", tmp_path / "_proceedings_prior.parquet")
    repeated = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    assert {r["proceeding_id"] for r in repeated} == {r["proceeding_id"] for r in proceedings}
    assert {r["proceeding_id"]: r["unresolved_fr_references_json"] for r in repeated} == {
        r["proceeding_id"]: r["unresolved_fr_references_json"] for r in proceedings
    }


def test_legacy_ambiguous_proceeding_is_unresolved_in_agenda_and_periods(tmp_path):
    _inputs(tmp_path)
    _write(
        tmp_path,
        "proceedings",
        ("proceeding_id", "docket_ids_json", "fr_document_numbers_json"),
        [{"proceeding_id": "legacy-collapsed", "docket_ids_json": "[]", "fr_document_numbers_json": '["00-111"]'}],
    )
    # An old bridge lacks date qualifiers, too. No whole-output refusal and no
    # selection of a colliding FR row are permitted.
    _write(
        tmp_path,
        "fr_docket_links",
        ("document_number", "docket_id"),
        [{"document_number": "00-111", "docket_id": "EPA-FIRST"}],
    )
    items_path, relationships_path = build_regulatory_agenda(tmp_path)
    assert pq.read_table(relationships_path).num_rows == 0
    items = pq.read_table(items_path).to_pylist()
    assert len(items) == 2
    assert all(r["scope_status"] == "unresolved" for r in items)
    assert all(json.loads(r["unresolved_fr_references_json"])[0]["status"] == "ambiguous" for r in items)
    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    assert len(periods) == 2
    assert len({r["comment_period_id"] for r in periods}) == 2
    assert all(r["proceeding_ids_json"] == r["docket_ids_json"] == "[]" for r in periods)
    assert all(json.loads(r["unresolved_fr_references_json"]) for r in periods)
