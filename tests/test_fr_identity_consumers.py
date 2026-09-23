"""Dated FR joins, labelled dockets, unpadded numbers and explicit ambiguity across the materialized consumers.

These controlled source rows exercise joins; the unmodified native collision
fixture and its byte provenance are tested in test_federal_register.py.
"""

import json
import shutil
import sys

import pyarrow.parquet as pq
import pytest
from loguru import logger

from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import RunContext, write_parquet_rows
from spicy_regs.ontology.federal_register import FederalRegisterIndex, linked_docket_id
from spicy_regs.pipelines import rulemaking_dataset
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


def _index(root, rows):
    _write(root, "federal_register", ("document_number", "publication_date"), rows)
    return FederalRegisterIndex(root / "federal_register.parquet")


def test_a_number_resolves_on_its_comparison_key_only_when_the_key_names_one_document(tmp_path):
    index = _index(
        tmp_path,
        [
            {"document_number": "2010-2394", "publication_date": "2010-02-05"},
            {"document_number": "2013-00123", "publication_date": "2013-01-04"},
            {"document_number": "E9-9366", "publication_date": "2009-04-24"},
            # Two documents the Register itself numbers apart, one key once unpadded.
            {"document_number": "94-0190", "publication_date": "1994-04-26"},
            {"document_number": "94-190", "publication_date": "1994-01-05"},
            {"document_number": "2018-28359", "publication_date": "2018-12-31"},
            {"document_number": "2015-00674", "publication_date": "2015-01-14"},
            # Corrections: a five-digit tail keys; the Register's short-tail form does not.
            {"document_number": "C1-2013-00123", "publication_date": "2013-02-01"},
            {"document_number": "C1-2012-9978", "publication_date": "2012-05-01"},
        ],
    )

    def resolve(number, day=None):
        reference = index.reference(number, day)
        return reference["status"], reference["candidate_ids"]

    # Regulations.gov pads where the Register did not.
    assert resolve("2010-02394") == ("unpadded_single_candidate_in_input", ["2010-2394@2010-02-05"])
    assert resolve("E9-09366") == ("unpadded_single_candidate_in_input", ["E9-9366@2009-04-24"])
    assert resolve("2010-02394", "2010-02-05") == ("unpadded_dated", ["2010-2394@2010-02-05"])
    assert resolve("2010-02394", "2010-02-06") == ("missing", [])
    # Both sides reduce: an unpadded reference reaches the Register's padded number.
    assert resolve("2013-123") == ("unpadded_single_candidate_in_input", ["2013-00123@2013-01-04"])
    # The exact spelling is tried first, so a held number never goes through the key.
    assert resolve("2013-00123") == ("single_candidate_in_input", ["2013-00123@2013-01-04"])
    assert resolve("94-190") == ("single_candidate_in_input", ["94-190@1994-01-05"])
    assert resolve("94-190", "1994-04-26") == ("missing", [])
    # A key two held numbers share is refused, not chosen between.
    assert resolve("94-00190") == ("ambiguous", ["94-0190@1994-04-26", "94-190@1994-01-05"])
    assert resolve("94-00190", "1994-04-26") == ("ambiguous", ["94-0190@1994-04-26", "94-190@1994-01-05"])
    assert resolve("2010-09999") == ("missing", [])
    assert resolve("SSA-2010-0037") == ("missing", [])
    # Only a dash or case differs: a fold, not an unpadding, and the status says so.
    assert resolve("2018–28359") == ("folded_single_candidate_in_input", ["2018-28359@2018-12-31"])
    assert resolve("2018–28359", "2018-12-31") == ("folded_dated", ["2018-28359@2018-12-31"])
    assert resolve("c1-2013-00123") == ("folded_single_candidate_in_input", ["C1-2013-00123@2013-02-01"])
    # Both sides padded, to different widths: neither is the unpadded number, so no match.
    assert resolve("2015-0674") == ("missing", [])
    assert resolve("2015–0674") == ("missing", [])
    assert resolve("2015-674") == ("unpadded_single_candidate_in_input", ["2015-00674@2015-01-14"])
    # A correction's short tail has no key: C1-2012-09978 keys to C1-2012-9978, which the
    # Register holds but cannot key, so it joins only exactly.
    assert resolve("C1-2012-09978") == ("missing", [])
    assert resolve("C1-2012-9978") == ("single_candidate_in_input", ["C1-2012-9978@2012-05-01"])


def test_the_index_answers_its_own_rows_record_ids(tmp_path):
    index = _index(tmp_path, [{"document_number": "00-111", "publication_date": "2000-01-14"}])
    assert index.record_id({"document_number": "00-111", "publication_date": "2000-01-14"}) == "00-111@2000-01-14"
    # A row the index does not hold is validated by its owner, as before.
    assert index.record_id({"document_number": "00-112", "publication_date": "2000-01-14"}) == "00-112@2000-01-14"


@pytest.mark.parametrize(
    ("stated", "docket"),
    [
        ("Docket No. SSA-2010-0037", "SSA-2010-0037"),
        ("DHS Docket No. USCIS-2025-0004", "USCIS-2025-0004"),
        ("epa-hq-oar-2021-0317", "EPA-HQ-OAR-2021-0317"),
        # SpicyDocs' column shape ends on digits; a literal identifier stays one.
        ("GIPSA-2010-FGIS-0014-NONRULEMAKING", "GIPSA-2010-FGIS-0014-NONRULEMAKING"),
        ("Docket No. RM98-1-000", None),  # a FERC docket, not a Regulations.gov one
        ("MM Docket No. 98-213", None),
        ("Sequence No. 1", None),
        ("PPWOCRADI0, PCU00RP14.R50000", None),
        (None, None),
    ],
)
def test_a_federal_register_docket_value_is_read_through_its_label(stated, docket):
    assert linked_docket_id(stated) == docket


def _labelled_inputs(root):
    records = [
        {
            "document_number": "2010-2394",
            "publication_date": "2010-02-05",
            "type": "Proposed Rule",
            "title": "Labelled docket rule",
            "docket_ids": ["Docket No. SSA-2010-0037", "Docket No. FAA-2010-0001"],
            "cfr_references": [{"title": 20, "part": 404}],
            "regulation_id_numbers": ["0960-AG21"],
            "comments_close_on": "2010-04-06",
        },
        {
            "document_number": "2010-2400",
            "publication_date": "2010-02-05",
            "type": "Rule",
            "title": "Literal docket rule",
            "docket_ids": ["GIPSA-2010-FGIS-0014-NONRULEMAKING"],
            "cfr_references": [{"title": 7, "part": 800}],
            "regulation_id_numbers": [],
        },
    ]
    build_federal_register(root, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    build_fr_docket_links(root)
    _write(
        root,
        "dockets",
        ("docket_id", "docket_type", "rin", "modify_date"),
        [
            # 22:30 Eastern on the 10th is the 11th in UTC.
            {
                "docket_id": "SSA-2010-0037",
                "docket_type": "Rulemaking",
                "rin": "0960-AG21",
                "modify_date": "2010-03-11T03:30:00Z",
            },
            {"docket_id": "GIPSA-2010-FGIS-0014-NONRULEMAKING", "docket_type": "Nonrulemaking"},
        ],
    )
    _write(
        root,
        "documents",
        ("document_id", "docket_id", "fr_doc_num", "additional_rins", "document_type", "posted_date", "modify_date"),
        [
            {
                "document_id": "SSA-2010-0037-0001",
                "docket_id": "SSA-2010-0037",
                "fr_doc_num": "2010-02394",  # Regulations.gov pads; the Register did not
                "additional_rins": "[]",
                "document_type": "Proposed Rule",
                "posted_date": "2010-02-05T05:00:00Z",
                "modify_date": "2010-02-06T02:00:00Z",
            }
        ],
    )
    _write(root, "unified_agenda", ("rin", "agenda_edition"), [])


def test_labelled_dockets_and_padded_numbers_join_every_rulemaking_table(tmp_path):
    _labelled_inputs(tmp_path)

    targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    edges = {(r["docket_id"], r["source"], r["cfr_ref"], r["rin"]): r for r in targets}
    assert set(edges) == {
        ("SSA-2010-0037", "docket_rin", None, "0960-AG21"),
        ("SSA-2010-0037", "fr_cfr_ref", "20-404", "0960-AG21"),
        ("SSA-2010-0037", "document_fr_doc", "20-404", "0960-AG21"),
        ("GIPSA-2010-FGIS-0014-NONRULEMAKING", "fr_cfr_ref", "7-800", None),
    }, "FAA-2010-0001 is named but no Regulations.gov record asserts it"
    corroboration = edges[("SSA-2010-0037", "document_fr_doc", "20-404", "0960-AG21")]
    (reference,) = json.loads(corroboration["fr_references_json"])
    assert reference["document_number"] == "2010-02394"
    assert reference["status"] == "unpadded_single_candidate_in_input"
    assert reference["candidate_ids"] == ["2010-2394@2010-02-05"]
    # Days, not instants: the Eastern day of each Regulations.gov stamp.
    assert (corroboration["first_seen"], corroboration["last_seen"]) == ("2010-02-05", "2010-02-05")
    docket_rin = edges[("SSA-2010-0037", "docket_rin", None, "0960-AG21")]
    assert (docket_rin["first_seen"], docket_rin["last_seen"]) == ("2010-03-10", "2010-03-10")
    assert {r["actor_id"] for r in targets} == {"spicy-regs:rule-targets:v3"}

    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    ssa = next(r for r in proceedings if "SSA-2010-0037" in json.loads(r["docket_ids_json"]))
    assert json.loads(ssa["fr_document_ids_json"]) == ["2010-2394@2010-02-05"], "not an FR-only proceeding"
    assert json.loads(ssa["cfr_refs_json"]) == ["20-404"]
    assert {(e["source"], e["effective_date"]) for e in json.loads(ssa["stage_events_json"])} == {
        ("documents.document_type", "2010-02-05"),
        ("federal_register.document_type", "2010-02-05"),
    }
    assert not any(json.loads(r["docket_ids_json"]) == [] for r in proceedings)
    assert {r["actor_id"] for r in proceedings} == {"spicy-regs:proceedings:v5"}

    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    (period,) = [r for r in periods if "federal_register.comments_close_on" in r["source"]]
    assert json.loads(period["docket_ids_json"]) == ["SSA-2010-0037"]
    assert json.loads(period["proceeding_ids_json"]) == [ssa["proceeding_id"]]
    assert {r["actor_id"] for r in periods} == {"spicy-regs:comment-periods:v6"}

    items_path, relationships_path = build_regulatory_agenda(tmp_path)
    (item,) = pq.read_table(items_path).to_pylist()
    assert (item["rin"], item["first_seen"], item["last_seen"]) == ("0960-AG21", "2010-02-05", "2010-03-10")
    relationships = {r["source"]: r for r in pq.read_table(relationships_path).to_pylist()}
    assert relationships["docket_rin"]["evidence_date"] == "2010-03-10"
    assert relationships["federal_register_rin"]["evidence_date"] == "2010-02-05"
    assert {r["actor_id"] for r in relationships.values()} == {"spicy-regs:agenda-item-proceedings:v3"}
    assert item["actor_id"] == "spicy-regs:regulatory-agenda-items:v3"


def test_the_rulemaking_generation_builds_one_federal_register_index(tmp_path, monkeypatch):
    _labelled_inputs(tmp_path)
    built: list[object] = []

    class CountingIndex(FederalRegisterIndex):
        def __init__(self, path):
            built.append(path)
            super().__init__(path)

    def refuse(*_args, **_kwargs):
        raise AssertionError("a stage rebuilt the Federal Register index")

    monkeypatch.setattr(rulemaking_dataset, "FederalRegisterIndex", CountingIndex)
    for module in (build_rule_targets, build_proceedings, build_regulatory_agenda, build_comment_periods):
        monkeypatch.setattr(sys.modules[module.__module__], "FederalRegisterIndex", refuse)
    pipeline = rulemaking_dataset.RulemakingDatasetPipeline(output_dir=tmp_path)
    context = RunContext.resolve(run_id="one-index", asserted_at="2026-09-23T12:00:00Z")
    for stage in pipeline._ordered_stages(pipeline.stages()):
        stage.build(tmp_path, context)

    assert built == [tmp_path / "federal_register.parquet"]
    assert all((tmp_path / name).exists() for name in pipeline.published_outputs)


def test_a_proceeding_merged_by_a_label_join_lists_the_old_ids_as_predecessors(tmp_path, monkeypatch):
    """The FR-only proceeding a label join absorbs survives as a predecessor, not as a lost id."""
    _labelled_inputs(tmp_path)
    build_rule_targets(tmp_path)
    # The prior generation read FR docket values syntax-only, so 2010-2394 stood alone.
    with monkeypatch.context() as before:
        before.setattr(sys.modules[build_proceedings.__module__], "linked_docket_id", normalize_regsgov_identifier)
        prior = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    prior_docket = next(r for r in prior if json.loads(r["docket_ids_json"]) == ["SSA-2010-0037"])
    prior_fr_only = next(r for r in prior if json.loads(r["fr_document_ids_json"]) == ["2010-2394@2010-02-05"])
    assert prior_fr_only["docket_ids_json"] == "[]"
    shutil.copyfile(tmp_path / "proceedings.parquet", tmp_path / "_proceedings_prior.parquet")

    merged = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    ssa = next(r for r in merged if "SSA-2010-0037" in json.loads(r["docket_ids_json"]))
    # Docket overlap outranks FR overlap, so the docket's id continues ...
    assert ssa["proceeding_id"] == prior_docket["proceeding_id"]
    assert ssa["supersedes_id"] == prior_docket["proceeding_id"]
    # ... and the absorbed FR-only id is named, never silently dropped.
    assert json.loads(ssa["identity_predecessors_json"]) == [prior_fr_only["proceeding_id"]]
    assert prior_fr_only["proceeding_id"] not in {r["proceeding_id"] for r in merged}
    assert len(merged) == len(prior) - 1


def test_rule_targets_count_the_cfr_references_they_cannot_read(tmp_path):
    _labelled_inputs(tmp_path)
    fr = pq.read_table(tmp_path / "federal_register.parquet").to_pylist()
    for row in fr:
        if row["document_number"] == "2010-2394":
            row["cfr_references_json"] = '[{"title": 20, "part": 404}, "20 CFR 416"]'
    _write(tmp_path, "federal_register", tuple(fr[0]), fr)
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    finally:
        logger.remove(sink)

    assert {r["cfr_ref"] for r in targets if r["docket_id"] == "SSA-2010-0037"} == {None, "20-404"}
    assert any("dropped 1 Federal Register CFR references that are not objects" in m for m in messages)
