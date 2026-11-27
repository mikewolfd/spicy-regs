"""The one-time repairs the spicy-docs 0.51.0/0.52.0 adoption owes its published rows: each exact, dry-runnable, idempotent."""

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.bill_family import _title_bill_id
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.tables import json_column

from spicy_regs.pipelines import prior_repairs as repairs


HEX = hashlib.sha256(b"bytes").hexdigest()


def _table(path: Path, rows: list[dict], columns=None, metadata=None) -> Path:
    columns = columns or list(rows[0])
    schema = pa.schema([(column, pa.string()) for column in columns], metadata=metadata)
    pq.write_table(pa.Table.from_pylist([{c: row.get(c) for c in columns} for row in rows], schema=schema), path)
    return path


class DetailSource:
    def __init__(self, wrong_identity=False, refuse=False):
        self.calls = []
        self.wrong_identity = wrong_identity
        self.refuse = refuse

    def detail(self, identity):
        self.calls.append(identity)
        if self.refuse:
            raise ConnectionError("source unavailable")
        return {
            "congress": identity.congress,
            "type": identity.bill_type.upper(),
            "number": identity.number + int(self.wrong_identity),
            "title": "Source-stated title",
            "originChamber": "House",
            "introducedDate": "1991-10-29",
            "cosponsors": {"count": 3},
            "latestAction": {"actionDate": "1992-02-04", "text": "Passed House"},
        }, "2026-09-30T06:00:00Z"


def test_missing_bills_preserves_rows_and_keeps_unread_fields_null(tmp_path):
    from spicy_regs.transforms.build_bill_family import BACKFILL_UNSUBSTANTIATED

    columns = TABLE_CONTRACTS["congress_bills"].columns
    prior = _table(
        tmp_path / "prior.parquet",
        [{"bill_id": "119-hr-1", "title": "Keep exactly"}],
        columns=columns,
        metadata={b"kept": b"yes"},
    )
    before = pq.read_table(prior).to_pylist()[0]
    out = tmp_path / "out.parquet"
    source = DetailSource()
    assert repairs.add_missing_bills(prior, out, ["102-hres-258", "119-hr-1", "102-hres-258"], source) == {
        "rows": 2,
        "added": 1,
        "already_present": 1,
    }
    rows = pq.read_table(out).to_pylist()
    assert rows[0] == before
    assert rows[1]["bill_id"] == "102-hres-258"
    assert rows[1]["title"] == "Source-stated title"
    assert rows[1]["cosponsor_count"] == "3"
    assert all(rows[1][column] is None for column in BACKFILL_UNSUBSTANTIATED)
    assert pq.read_schema(out).metadata == {b"kept": b"yes"}
    assert len(source.calls) == 1
    again = tmp_path / "again.parquet"
    assert repairs.add_missing_bills(out, again, ["102-hres-258"], source)["added"] == 0
    assert len(source.calls) == 1, "an existing bill needs no read and cannot be overwritten"
    assert pq.read_table(again).equals(pq.read_table(out))


@pytest.mark.parametrize(
    "source, error", [(DetailSource(wrong_identity=True), ValueError), (DetailSource(refuse=True), ConnectionError)]
)
def test_missing_bills_refuses_without_writing_output(tmp_path, source, error):
    prior = _table(tmp_path / "prior.parquet", [], columns=TABLE_CONTRACTS["congress_bills"].columns)
    out = tmp_path / "out.parquet"
    with pytest.raises(error):
        repairs.add_missing_bills(prior, out, ["102-hres-258"], source)
    assert not out.exists()


@pytest.mark.parametrize("ids", [[], ["102-HRES-258"], ["102-hres-0258"], ["bad"]])
def test_missing_bills_requires_explicit_canonical_ids(tmp_path, ids):
    with pytest.raises(ValueError):
        repairs.add_missing_bills(tmp_path / "unused", tmp_path / "out", ids, DetailSource())


def test_a_bare_digest_is_prefixed_once_and_nothing_else_moves():
    assert repairs.respell_digest(HEX) == f"sha256:{HEX}"
    assert repairs.respell_digest(f"sha256:{HEX}") == f"sha256:{HEX}"
    for other in (None, HEX.upper(), HEX[:-1], "5609cfaab8bb", f"x{HEX}"):
        assert repairs.respell_digest(other) == other


def test_pdf_results_respell_each_writer_shape_and_refuse_another_spelling():
    attempts = json.dumps([{"url": "u", "source_sha256": HEX, "status": "ok"}, {"url": "v", "source_sha256": None}])
    derived = json.dumps(
        {"tool": "pypdf", "attachments": [{"attachment": 1, "sha256": HEX}, {"attachment": 2, "sha256": None}]}
    )
    spelled = repairs.respell_pdf_results(attempts)
    assert json.loads(spelled or "")[0]["source_sha256"] == f"sha256:{HEX}"
    assert repairs.respell_pdf_results(spelled) == spelled, "a second pass changes nothing"
    again = repairs.respell_pdf_results(derived)
    assert [a["sha256"] for a in json.loads(again or "")["attachments"]] == [f"sha256:{HEX}", None]
    assert repairs.respell_pdf_results(again) == again
    assert repairs.respell_pdf_results(None) is None
    with pytest.raises(ValueError, match="not spelled as its writers"):
        repairs.respell_pdf_results(json.dumps([{"source_sha256": HEX}], separators=(",", ":")))


def test_a_summary_event_is_respelled_in_json_columns_spelling():
    event = json_column({"model": "m", "prompt_version": "1", "content_hash": HEX, "regenerated": False})
    spelled = repairs.respell_event_data(event)
    assert spelled == json_column(
        {"model": "m", "prompt_version": "1", "content_hash": f"sha256:{HEX}", "regenerated": False}
    )
    assert repairs.respell_event_data(spelled) == spelled


def _bill_family_priors(directory: Path) -> Path:
    directory.mkdir()
    _table(directory / "section_classifications.parquet", [{"bill_id": "119-hr-1", "prompt_hash": HEX}])
    _table(
        directory / "bill_summaries.parquet",
        [{"bill_id": "119-hr-1", "content_hash": HEX}, {"bill_id": "119-hr-2", "content_hash": f"sha256:{HEX}"}],
    )
    _table(directory / "diff_summaries.parquet", [{"bill_id": "119-hr-1", "content_hash": None}])
    summary = json_column({"model": "m", "prompt_version": "1", "content_hash": HEX, "regenerated": False})
    _table(
        directory / "public_activity_events.parquet",
        [
            {"bill_id": "119-hr-1", "event_type": "summary_generated", "event_data_json": summary},
            {"bill_id": "119-hr-1", "event_type": "stage_changed", "event_data_json": json_column({"from": HEX})},
        ],
    )
    return directory


def test_the_bill_family_respell_counts_rewrites_and_is_idempotent(tmp_path):
    priors = _bill_family_priors(tmp_path / "priors")
    before = {path.name: path.read_bytes() for path in priors.iterdir()}
    counts = repairs.dry_run("respell-bill-family-digests", prior_dir=priors)
    assert counts == {
        "section_classifications": {"rows": 1, "prompt_hash_respelled": 1},
        "bill_summaries": {"rows": 2, "content_hash_respelled": 1},
        "diff_summaries": {"rows": 1, "content_hash_respelled": 0},
        "public_activity_events": {"rows": 2, "event_data_json_respelled": 1},
    }
    assert {path.name: path.read_bytes() for path in priors.iterdir()} == before, "a dry run writes nothing"
    out = tmp_path / "out"
    out.mkdir()
    repairs.apply_repair(repairs.REPAIRS["respell-bill-family-digests"], priors, out)
    events = pq.read_table(out / "public_activity_events.parquet").to_pylist()
    assert json.loads(events[0]["event_data_json"])["content_hash"] == f"sha256:{HEX}"
    assert json.loads(events[1]["event_data_json"]) == {"from": HEX}, "only a summary_generated event is re-spelled"
    again = repairs.dry_run("respell-bill-family-digests", prior_dir=out)
    assert all(value == 0 for table in again.values() for key, value in table.items() if key != "rows")


def test_the_document_respell_rewrites_only_the_results_column(tmp_path):
    priors = tmp_path / "priors"
    priors.mkdir()
    attempts = json.dumps([{"url": "u", "source_sha256": HEX, "status": "ok", "page_count": 1, "error": None}])
    _table(
        priors / "documents.parquet",
        [
            {"document_id": "A-1", "pdf_extraction_results_json": attempts},
            {"document_id": "A-2", "pdf_extraction_results_json": None},
        ],
    )
    counts = repairs.dry_run("respell-document-digests", prior_dir=priors)
    assert counts == {"documents": {"rows": 2, "rows_with_results": 1, "pdf_extraction_results_json_respelled": 1}}
    out = tmp_path / "out"
    out.mkdir()
    repairs.apply_repair(repairs.REPAIRS["respell-document-digests"], priors, out)
    rows = pq.read_table(out / "documents.parquet").to_pylist()
    assert [row["document_id"] for row in rows] == ["A-1", "A-2"]
    assert json.loads(rows[0]["pdf_extraction_results_json"])[0]["source_sha256"] == f"sha256:{HEX}"
    assert (
        repairs.dry_run("respell-document-digests", prior_dir=out)["documents"]["pdf_extraction_results_json_respelled"]
        == 0
    )


def _cbo_row(bill: str, publication: str, title: str, source: str = "billstatus_bulk", **values) -> dict:
    congress, bill_type, number = bill.split("-")
    return {
        "bill_id": bill,
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": number,
        "publication_id": publication,
        "title": title,
        "source": source,
    } | values


def test_found_by_is_backfilled_on_billstatus_rows_only(tmp_path):
    priors = tmp_path / "priors"
    priors.mkdir()
    prior_columns = [
        c
        for c in TABLE_CONTRACTS["cbo_cost_estimates"].columns
        if c not in {"title_bill_id", "found_by", "title_bill_id_rule"}
    ]
    _table(
        priors / "cbo_cost_estimates.parquet",
        [
            _cbo_row("115-hr-1422", "52538", "H.R. 1422, Energy Efficiency Act"),
            _cbo_row("119-hconres-14", "61570", "Public Law 119-21, to Provide for Reconciliation"),
            _cbo_row("114-s-10", "50001", "Sequester Replacement Reconciliation Act"),
        ],
        columns=prior_columns,
        metadata={b"kept": b"yes"},
    )
    _table(priors / "laws.parquet", [{"law_id": "119-public-21", "bill_id": "119-hr-1"}])
    counts = repairs.dry_run("backfill-found-by", prior_dir=priors)
    assert counts == {
        "cbo_cost_estimates": {"rows": 3, "found_by_filled": 3, "title_bill_id_stated": 2, "title_bill_id_differs": 1}
    }
    out = tmp_path / "out"
    out.mkdir()
    repairs.apply_repair(repairs.REPAIRS["backfill-found-by"], priors, out)
    assert pq.read_schema(out / "cbo_cost_estimates.parquet").names == list(
        TABLE_CONTRACTS["cbo_cost_estimates"].columns
    )
    assert pq.read_schema(out / "cbo_cost_estimates.parquet").metadata == {b"kept": b"yes"}
    rows = {row["bill_id"]: row for row in pq.read_table(out / "cbo_cost_estimates.parquet").to_pylist()}
    assert {row["found_by"] for row in rows.values()} == {"billstatus"}
    assert {row["title_bill_id_rule"] for row in rows.values()} == {"cbo_title_citation/1"}
    assert rows["115-hr-1422"]["title_bill_id"] == "115-hr-1422"
    assert rows["119-hconres-14"]["title_bill_id"] == "119-hr-1", "a law title reads through the published laws"
    assert rows["114-s-10"]["title_bill_id"] is None
    assert repairs.dry_run("backfill-found-by", prior_dir=out)["cbo_cost_estimates"]["found_by_filled"] == 0


def test_a_feed_row_is_left_as_its_builder_shaped_it(tmp_path):
    prior = _table(
        tmp_path / "cbo.parquet",
        [_cbo_row("112-hr-1", "1", "H.R. 1, x", source="cbo_feed")],
        columns=list(TABLE_CONTRACTS["cbo_cost_estimates"].columns),
    )
    counts = repairs.backfill_found_by(prior, tmp_path / "out.parquet")
    assert counts["found_by_filled"] == 0


@pytest.mark.parametrize(
    "title",
    [
        "H.R. 1422, Energy Efficiency Act",
        "S. 106, Commitment to Veteran Support",
        "Public Law 119-21, to Provide",
        "P.L. 111-322, the Continuing Appropriations",
        "Sequester Replacement Reconciliation Act",
        "An act in Title IV of H.R. 1",
        "H.R. 5, H.R. 6 and more",
        None,
    ],
)
def test_the_title_rule_restated_here_answers_as_spicy_docs_does(title):
    law_bills = {"119-public-21": "119-hr-1", "111-public-322": "111-hr-3082"}
    for congress in (111, 115, 119):
        for laws in (None, law_bills):
            assert repairs.title_bill_id(congress, title, laws) == _title_bill_id(congress, title, laws)


def test_record_issues_follow_the_part_one_link(tmp_path):
    priors = tmp_path / "priors"
    priors.mkdir()
    entire = json.dumps(
        [
            {"part": "2", "url": "https://www.congress.gov/171/crec/2025/03/11/171/45/CREC-2025-03-11-bk2.pdf"},
            {"part": "1", "url": "https://www.congress.gov/171/crec/2025/03/11/171/45/CREC-2025-03-11.pdf"},
        ]
    )
    _table(
        priors / "record_issues.parquet",
        [
            {
                "volume": "171",
                "issue": "45",
                "entire_issue_json": entire,
                "package_id": "CREC-2025-03-11-bk2",
                "package_id_rule": "entire_issue_url_stem",
            },
            {"volume": "171", "issue": "46", "entire_issue_json": None, "package_id": None, "package_id_rule": None},
        ],
    )
    assert repairs.dry_run("rebuild-record-issues", prior_dir=priors) == {
        "record_issues": {"rows": 2, "package_id_moved": 1, "package_id_rule_moved": 1}
    }
    out = tmp_path / "out"
    out.mkdir()
    repairs.apply_repair(repairs.REPAIRS["rebuild-record-issues"], priors, out)
    row = pq.read_table(out / "record_issues.parquet").to_pylist()[0]
    assert (row["package_id"], row["package_id_rule"]) == ("CREC-2025-03-11", "entire_issue_url_stem/2")
    assert repairs.dry_run("rebuild-record-issues", prior_dir=out)["record_issues"]["package_id_moved"] == 0


def test_stage_suppression_counts_the_rules_moves(tmp_path):
    _table(
        tmp_path / "congress_bills.parquet",
        [{"bill_id": "119-hr-1", "stage": "introduced"}, {"bill_id": "119-hr-2", "stage": "committee"}],
    )
    _table(
        tmp_path / "bill_actions.parquet",
        [
            {
                "bill_id": "119-hr-1",
                "action_index": "0",
                "action_text": "Referred to the Committee on Rules.",
                "action_code": "H11100",
                "action_type": "IntroReferral",
                "action_date": "2026-01-02",
                "action_time": None,
            },
            {
                "bill_id": "119-hr-1",
                "action_index": "1",
                "action_text": "Introduced in House",
                "action_code": "Intro-H",
                "action_type": "IntroReferral",
                "action_date": "2026-01-02",
                "action_time": None,
            },
            {
                "bill_id": "119-hr-2",
                "action_index": "0",
                "action_text": "Referred to the Committee on Rules.",
                "action_code": "H11100",
                "action_type": "IntroReferral",
                "action_date": "2026-01-02",
                "action_time": None,
            },
        ],
    )
    counts = repairs.stage_suppression(tmp_path / "congress_bills.parquet", tmp_path / "bill_actions.parquet")
    assert counts["bills_with_actions"] == 2 and counts["stages_moved_by_the_rule"] == 1
    assert counts["largest_moves"] == [{"from": "introduced", "to": "committee", "bills": 1}]


def test_restore_pins_the_saved_generation_and_refuses_a_split_table():
    index = {
        "families": {
            "bill-family": {
                "prefix": "generations/bill-family/abc",
                "artifactDigest": "sha256:abc",
                "tables": {
                    "cbo_cost_estimates.parquet": {"sha256": "sha256:1", "byteSize": 9, "rows": 3},
                    "bill_sections.parquet": {"members": []},
                },
            }
        }
    }
    rollup = repairs.restore_rollup(index, "bill-family", ["cbo_cost_estimates"])
    assert rollup.publication_family == "bill-family" and rollup.outputs == ("cbo_cost_estimates.parquet",)
    with pytest.raises(ValueError, match="split"):
        repairs.restore_rollup(index, "bill-family", ["bill_sections"])


def _native_record_issue_selection(root):
    from spicy_regs.congress_receipts import write_congress_dataset
    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    row: dict[str, str | None] = dict.fromkeys(INPUT_COLUMNS["record_issues"])
    row.update(
        volume="171", issue="45", congress="119", issue_date="2025-03-11",
        entire_issue_json=json.dumps([{"part": "1", "type": "PDF",
            "url": "https://www.congress.gov/171/crec/2025/03/11/171/45/CREC-2025-03-11.pdf"}]),
        package_id="CREC-2025-03-11-bk2", package_id_rule="entire_issue_url_stem",
    )
    source = _table(root / "source.parquet", [row])
    subject, receipts = write_congress_dataset(
        source, root / "native", dataset="record_issues", generation_id="before"
    )
    assert subject is not None
    work = root / "repair"
    work.mkdir()
    remember_selection(work, [SelectedDataset("record_issues", (subject,), receipts, "before")])
    return work, row, receipts


def test_record_issue_repair_restores_receipt_held_inputs_and_writes_native(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.sources import r2

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("Repair must use its selected native prior"))
    work, original, _ = _native_record_issue_selection(tmp_path)
    rollup = repairs.repair_rollup("rebuild-record-issues", repairs.REPAIRS["rebuild-record-issues"])
    rollup(output_dir=work, skip_upload=True).run()
    repaired = pq.read_table(work / "record_issues.parquet").to_pylist()[0]
    assert repaired["package_id"] == "CREC-2025-03-11"
    assert "entire_issue" in repaired and "entire_issue_json" not in repaired
    assert "package_id_rule" not in repaired
    assert (work / "etl_receipts.parquet").exists()
    restored = SelectedPriors(tmp_path / "readback", root=work, public_url="").get("record_issues")
    assert restored is not None
    row = pq.read_table(restored).to_pylist()[0]
    assert row == original | {"package_id": "CREC-2025-03-11", "package_id_rule": "entire_issue_url_stem/2"}
    before = pq.read_table(work / "record_issues.parquet")
    rollup(output_dir=work, skip_upload=True).run()
    assert pq.read_table(work / "record_issues.parquet").equals(before)


def test_record_issue_repair_refuses_missing_selected_receipts(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    work, _, receipts = _native_record_issue_selection(tmp_path)
    receipts.unlink()
    rollup = repairs.repair_rollup("rebuild-record-issues", repairs.REPAIRS["rebuild-record-issues"])
    with pytest.raises((ValueError, OSError)):
        rollup(output_dir=work, skip_upload=True).run()
    assert not (work / "record_issues.parquet").exists()


def test_record_issue_dry_run_restores_local_native_selection_without_remote_fallback(tmp_path, monkeypatch):
    from spicy_regs.sources import publication

    work, _, _ = _native_record_issue_selection(tmp_path)
    selection = work / ".native-state" / "selection.json"
    before = selection.read_bytes()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example.test")
    monkeypatch.setattr(publication, "load_index", lambda *_: pytest.fail("Local dry run must not read remotely"))
    assert repairs.dry_run("rebuild-record-issues", prior_dir=work) == {
        "record_issues": {"rows": 1, "package_id_moved": 1, "package_id_rule_moved": 1}
    }
    assert selection.read_bytes() == before
    assert not (work / "record_issues.parquet").exists()


def test_record_issue_dry_run_refuses_native_subject_without_selected_receipts(tmp_path):
    from shutil import copyfile

    _native_record_issue_selection(tmp_path)
    bare = tmp_path / "bare"
    bare.mkdir()
    prior = bare / "record_issues.parquet"
    copyfile(tmp_path / "native" / "record_issues.parquet", prior)
    before = prior.read_bytes()
    with pytest.raises(ValueError, match="receipt-restored processing"):
        repairs.dry_run("rebuild-record-issues", prior_dir=bare)
    assert prior.read_bytes() == before


def test_record_issue_dry_run_uses_the_captured_native_selection(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from spicy_regs.pipelines.rollups import subject_receipts
    from spicy_regs.sources import publication, r2

    work, _, _ = _native_record_issue_selection(tmp_path)
    captured = object()
    seen = []
    selected_priors = subject_receipts.SelectedPriors

    @contextmanager
    def snapshot(url):
        seen.append(url)
        yield captured

    def selected(directory, *, index, public_url):
        assert index is captured
        assert public_url == "https://data.example.test"
        return selected_priors(directory, root=work, public_url="")

    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example.test")
    monkeypatch.setattr(publication, "snapshot", snapshot)
    monkeypatch.setattr(subject_receipts, "SelectedPriors", selected)
    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("Dry run must restore selected native input"))
    assert repairs.dry_run("rebuild-record-issues") == {
        "record_issues": {"rows": 1, "package_id_moved": 1, "package_id_rule_moved": 1}
    }
    assert seen == ["https://data.example.test"]
    assert not (work / "record_issues.parquet").exists()


# --------------------------------------------------------------------------- #
# ss-kj95: audited public_activity_events null-from repair & bill-family quality
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("layout", ["processing", "native"])
def test_repair_null_from_stage_events_drops_null_from_and_preserves_legitimate_transitions(tmp_path, layout):
    """Drops NULL/empty-from stage_changed rows without duplicating bill_added while keeping real transitions."""
    if layout == "processing":
        columns = list(TABLE_CONTRACTS["public_activity_events"].columns)
        rows = [
            {
                "bill_id": "119-hr-1",
                "event_type": "bill_added",
                "subject_id": "",
                "occurred_at": "2025-01-03",
                "detected_at": "2025-01-04T00:00:00Z",
                "event_data_json": json_column({"title": "Bill 1", "stage": "introduced", "sponsor_bioguide_id": "A1"}),
            },
            # Bogus NULL-from stage_changed on a bill that ALREADY has bill_added (18,899 cohort)
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-05",
                "detected_at": "2025-01-05T00:00:00Z",
                "event_data_json": json_column({"from": None, "to": "introduced", "rule": "r", "matcher": "m"}),
            },
            # Legitimate stage_changed on same bill
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-10",
                "detected_at": "2025-01-11T00:00:00Z",
                "event_data_json": json_column({"from": "introduced", "to": "committee", "rule": "r", "matcher": "m"}),
            },
            # Bogus NULL-from stage_changed on a bill WITHOUT bill_added (136,201 cohort)
            {
                "bill_id": "119-hr-2",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-06",
                "detected_at": "2025-01-06T00:00:00Z",
                "event_data_json": json_column({"from": None, "to": "introduced", "rule": "r", "matcher": "m"}),
            },
            # Bogus empty-string from stage_changed
            {
                "bill_id": "119-hr-3",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-07",
                "detected_at": "2025-01-07T00:00:00Z",
                "event_data_json": json_column({"from": "", "to": "introduced", "rule": "r", "matcher": "m"}),
            },
            # Second legitimate transition
            {
                "bill_id": "119-hr-4",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-02-01",
                "detected_at": "2025-02-02T00:00:00Z",
                "event_data_json": json_column({"from": "committee", "to": "passed_chamber", "rule": "r", "matcher": "m"}),
            },
            # Other sealed event types remain untouched
            {
                "bill_id": "119-hr-1",
                "event_type": "version_added",
                "subject_id": "119-hr-1\x1fih\x1fgovinfo",
                "occurred_at": "2025-01-03",
                "detected_at": "2025-01-04T00:00:00Z",
                "event_data_json": json_column({"version_code": "ih", "source": "govinfo", "kind": "full_text", "kind_rule": "full_text_slug"}),
            },
        ]
        prior = _table(tmp_path / "events.parquet", rows, columns=columns, metadata={b"kept": b"yes"})
    else:
        from spicy_regs.congress_subjects import subject_schema

        schema = subject_schema("public_activity_events").with_metadata({b"kept": b"yes"})
        base = dict.fromkeys(schema.names)
        rows = [
            base | {"bill_id": "119-hr-1", "event_type": "bill_added", "subject_id": "", "occurred_at": "2025-01-03", "stage": "introduced"},
            base | {"bill_id": "119-hr-1", "event_type": "stage_changed", "subject_id": "", "occurred_at": "2025-01-05", "from_stage": None, "to_stage": "introduced"},
            base | {"bill_id": "119-hr-1", "event_type": "stage_changed", "subject_id": "", "occurred_at": "2025-01-10", "from_stage": "introduced", "to_stage": "committee"},
            base | {"bill_id": "119-hr-2", "event_type": "stage_changed", "subject_id": "", "occurred_at": "2025-01-06", "from_stage": None, "to_stage": "introduced"},
            base | {"bill_id": "119-hr-3", "event_type": "stage_changed", "subject_id": "", "occurred_at": "2025-01-07", "from_stage": "", "to_stage": "introduced"},
            base | {"bill_id": "119-hr-4", "event_type": "stage_changed", "subject_id": "", "occurred_at": "2025-02-01", "from_stage": "committee", "to_stage": "passed_chamber"},
            base | {"bill_id": "119-hr-1", "event_type": "version_added", "subject_id": "119-hr-1\x1fih\x1fgovinfo", "occurred_at": "2025-01-03", "version_code": "ih", "kind": "full_text"},
        ]
        prior = tmp_path / "events.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), prior)

    out = tmp_path / "repaired.parquet"
    counts = repairs.repair_null_from_stage_events(prior, out)
    assert counts == {
        "rows": 4,
        "rows_before": 7,
        "rows_after": 4,
        "stage_changed_before": 5,
        "stage_changed_preserved": 2,
        "stage_changed_null_from_dropped": 3,
        "stage_changed_null_from_with_existing_bill_added": 1,
        "stage_changed_null_from_without_bill_added": 2,
        "bill_added_before": 1,
        "bill_added_after": 1,
        "bill_added_duplicate_bill_ids_after": 0,
    }
    assert pq.read_schema(out).metadata == {b"kept": b"yes"}
    kept = pq.read_table(out).to_pylist()
    assert [(r["bill_id"], r["event_type"], r["occurred_at"]) for r in kept] == [
        ("119-hr-1", "bill_added", "2025-01-03"),
        ("119-hr-1", "stage_changed", "2025-01-10"),
        ("119-hr-4", "stage_changed", "2025-02-01"),
        ("119-hr-1", "version_added", "2025-01-03"),
    ]
    again = tmp_path / "again.parquet"
    second = repairs.repair_null_from_stage_events(out, again)
    assert second["stage_changed_null_from_dropped"] == 0
    assert second["rows_before"] == second["rows_after"] == 4
    assert pq.read_table(again).equals(pq.read_table(out))


def test_repair_null_from_stage_events_empty_output_preserves_schema(tmp_path):
    columns = list(TABLE_CONTRACTS["public_activity_events"].columns)
    prior = _table(
        tmp_path / "only_null.parquet",
        [
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-05",
                "detected_at": "2025-01-05T00:00:00Z",
                "event_data_json": json_column({"from": None, "to": "introduced"}),
            }
        ],
        columns=columns,
    )
    out = tmp_path / "out.parquet"
    counts = repairs.repair_null_from_stage_events(prior, out)
    assert counts["rows_after"] == 0 and counts["stage_changed_null_from_dropped"] == 1
    assert pq.read_schema(out).names == columns and pq.read_table(out).num_rows == 0


def test_repair_bill_family_quality_dry_run_apply_and_idempotency(tmp_path):
    priors = tmp_path / "priors"
    priors.mkdir()
    _table(
        priors / "bill_versions.parquet",
        [
            {
                "bill_id": "119-hr-1",
                "version_code": "introduced-in-house",
                "source": "govinfo",
                "label": "Introduced in House",
                "version_date": "",
                "package_id": "BILLS-119hr1ih",
                "section_count": "2",
                "kind": "kind_uncertain",
                "kind_rule": "full_text_slug_thin",
                "kind_label": "Full bill text (short — verify)",
                "kind_warning": "unusually short — may be incomplete",
            },
            {
                "bill_id": "119-s-2",
                "version_code": "private-law",
                "source": "congress",
                "label": "Private Law",
                "version_date": "2025-03-01",
                "kind": "unknown",
                "kind_rule": "unknown",
                "kind_label": "Unknown format",
            },
        ],
        columns=list(TABLE_CONTRACTS["bill_versions"].columns),
    )
    _table(
        priors / "section_diffs.parquet",
        [
            {
                "bill_id": "119-hr-1",
                "from_version_code": "ih",
                "from_source": "govinfo",
                "to_version_code": "enr",
                "to_source": "govinfo",
                "from_version_date": "",
                "to_version_date": "",
            }
        ],
        columns=list(TABLE_CONTRACTS["section_diffs"].columns),
    )
    _table(
        priors / "bill_cosponsors.parquet",
        [
            {
                "bill_id": "119-hr-1",
                "input_sha256": f"sha256:{HEX}",
                "cosponsor_index": "0",
                "bioguide_id": "A000001",
                "sponsorship_date": "2025-01-03",
                "sponsorship_date_status": "valid",
                "sponsorship_withdrawn_date": "",
                "sponsorship_withdrawn_date_status": "empty",
                "district": "",
                "source_xml": "<item><sponsorshipWithdrawnDate/><district/></item>",
            }
        ],
        columns=list(TABLE_CONTRACTS["bill_cosponsors"].columns),
    )
    _table(
        priors / "public_activity_events.parquet",
        [
            {
                "bill_id": "119-hr-1",
                "event_type": "bill_added",
                "subject_id": "",
                "occurred_at": "2025-01-03",
                "detected_at": "2025-01-04T00:00:00Z",
                "event_data_json": json_column({"title": "T", "stage": "introduced", "sponsor_bioguide_id": None}),
            },
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-05",
                "detected_at": "2025-01-05T00:00:00Z",
                "event_data_json": json_column({"from": None, "to": "introduced"}),
            },
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-10",
                "detected_at": "2025-01-10T00:00:00Z",
                "event_data_json": json_column({"from": "introduced", "to": "committee"}),
            },
        ],
        columns=list(TABLE_CONTRACTS["public_activity_events"].columns),
    )

    before_bytes = {p.name: p.read_bytes() for p in priors.iterdir()}
    counts = repairs.dry_run("repair-bill-family-quality", prior_dir=priors)
    assert counts["bill_versions"] == {"rows": 2, "kind_moved": 2, "version_date_cleaned": 1, "rows_moved": 2}
    assert counts["section_diffs"] == {"rows": 1, "from_version_date_cleaned": 1, "to_version_date_cleaned": 1, "rows_moved": 1}
    assert counts["bill_cosponsors"] == {
        "rows": 1,
        "sponsorship_date_cleaned": 0,
        "sponsorship_withdrawn_date_cleaned": 1,
        "district_cleaned": 1,
        "rows_moved": 1,
    }
    assert counts["public_activity_events"]["stage_changed_null_from_dropped"] == 1
    assert counts["public_activity_events"]["stage_changed_preserved"] == 1
    assert counts["public_activity_events"]["bill_added_duplicate_bill_ids_after"] == 0
    assert {p.name: p.read_bytes() for p in priors.iterdir()} == before_bytes, "dry run must not mutate priors"

    out = tmp_path / "out"
    out.mkdir()
    repairs.apply_repair(repairs.REPAIRS["repair-bill-family-quality"], priors, out)
    second = repairs.dry_run("repair-bill-family-quality", prior_dir=out)
    assert second["bill_versions"]["rows_moved"] == 0
    assert second["section_diffs"]["rows_moved"] == 0
    assert second["bill_cosponsors"]["rows_moved"] == 0
    assert second["public_activity_events"]["stage_changed_null_from_dropped"] == 0


def _seed_native_bill_family_selection(root: Path, monkeypatch) -> Path:
    """Create a complete receipt-admitted local native bill-family selection holding pre-repair ss-kj95 defects."""
    from spicy_regs.pipelines.rollups import bill_family as bill_family_rollup
    from spicy_regs.transforms.build_bill_family import (
        ARCHIVE_COLUMNS,
        ARCHIVES_TABLE,
        BACKFILL_COLUMNS,
        BACKFILL_WALK_COLUMNS,
        BACKFILL_WALKS_TABLE,
        BACKFILLS_TABLE,
        FAMILY_TABLES,
        VOTE_REFERENCE_COLUMNS,
        VOTE_REFERENCES_TABLE,
    )

    own_columns = {
        ARCHIVES_TABLE: ARCHIVE_COLUMNS,
        VOTE_REFERENCES_TABLE: VOTE_REFERENCE_COLUMNS,
        BACKFILLS_TABLE: BACKFILL_COLUMNS,
        BACKFILL_WALKS_TABLE: BACKFILL_WALK_COLUMNS,
    }
    sample_rows: dict[str, list[dict]] = {
        "congress_bills": [
            {
                "bill_id": "119-hr-1",
                "congress": "119",
                "bill_type": "hr",
                "bill_number": "1",
                "title": "Sample Bill",
                "stage": "committee",
            }
        ],
        "bill_versions": [
            {
                "bill_id": "119-hr-1",
                "version_code": "introduced-in-house",
                "source": "govinfo",
                "label": "Introduced in House",
                "version_date": "",
                "package_id": "BILLS-119hr1ih",
                "section_count": "1",
                "kind": "kind_uncertain",
                "kind_rule": "full_text_slug_thin",
                "kind_label": "Full bill text (short — verify)",
                "kind_warning": "unusually short — may be incomplete",
                "kind_section_count": "1",
                "kind_body_bytes": None,
            },
            {
                "bill_id": "119-hr-1",
                "version_code": "rfs",
                "source": "govinfo",
                "label": "Referred in Senate",
                "version_date": "2025-02-01",
                "package_id": "BILLS-119hr1rfs",
                "section_count": "1",
                "kind": "unknown",
                "kind_rule": "unknown",
                "kind_label": "Unknown format",
                "kind_warning": None,
                "kind_section_count": "1",
                "kind_body_bytes": None,
            },
        ],
        "section_diffs": [
            {
                "bill_id": "119-hr-1",
                "from_version_code": "introduced-in-house",
                "from_source": "govinfo",
                "to_version_code": "rfs",
                "to_source": "govinfo",
                "from_version_date": "",
                "to_version_date": "",
                "added_count": "0",
                "removed_count": "0",
                "modified_count": "0",
                "unchanged_count": "1",
                "moved_count": "0",
                "item_count": "1",
            }
        ],
        "bill_cosponsors": [
            {
                "bill_id": "119-hr-1",
                "input_sha256": f"sha256:{HEX}",
                "cosponsor_index": "0",
                "bioguide_id": "A000001",
                "full_name": "Rep. Example",
                "sponsorship_date": "2025-01-03",
                "sponsorship_date_status": "valid",
                "is_original_raw": "True",
                "sponsorship_withdrawn_date": "",
                "sponsorship_withdrawn_date_status": "empty",
                "party": "D",
                "state": "CA",
                "district": "",
                "source_path": "/bill/cosponsors/item[1]",
                "source_xml": "<item><sponsorshipWithdrawnDate/><district/></item>",
            }
        ],
        "public_activity_events": [
            {
                "bill_id": "119-hr-1",
                "event_type": "bill_added",
                "subject_id": "",
                "occurred_at": "2025-01-03",
                "detected_at": "2025-01-04T00:00:00Z",
                "event_data_json": json_column({"title": "Sample Bill", "stage": "introduced", "sponsor_bioguide_id": None}),
            },
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-05",
                "detected_at": "2025-01-05T00:00:00Z",
                "event_data_json": json_column({"from": None, "to": "introduced", "rule": "r", "matcher": "m"}),
            },
            {
                "bill_id": "119-hr-1",
                "event_type": "stage_changed",
                "subject_id": "",
                "occurred_at": "2025-01-10",
                "detected_at": "2025-01-10T00:00:00Z",
                "event_data_json": json_column({"from": "introduced", "to": "committee", "rule": "r", "matcher": "m"}),
            },
        ],
    }

    def fake_build(output_dir: Path, **_kwargs) -> tuple[Path, ...]:
        written: list[Path] = []
        for contract, _ in FAMILY_TABLES:
            cols = TABLE_CONTRACTS[contract].columns
            if contract == "bill_sections":
                sec_dir = output_dir / "bill_sections" / "congress=119"
                sec_dir.mkdir(parents=True, exist_ok=True)
                _table(
                    sec_dir / "part-000000.parquet",
                    [
                        {
                            "bill_id": "119-hr-1",
                            "version_code": "introduced-in-house",
                            "source": "govinfo",
                            "seq": "0",
                            "match_path": "sec-1",
                            "body": "Short title.",
                            "body_chars": "12",
                            "body_sha256": f"sha256:{HEX}",
                            "congress": "119",
                        }
                    ],
                    columns=list(cols),
                )
                written.append(output_dir / "bill_sections")
            else:
                written.append(_table(output_dir / f"{contract}.parquet", sample_rows.get(contract, []), columns=list(cols)))
        written.append(
            _table(
                output_dir / "public_activity_events.parquet",
                sample_rows["public_activity_events"],
                columns=list(TABLE_CONTRACTS["public_activity_events"].columns),
            )
        )
        for name, cols in own_columns.items():
            written.append(_table(output_dir / f"{name}.parquet", [], columns=list(cols)))
        return tuple(written)

    work = root / "bill-family-work"
    work.mkdir()
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(bill_family_rollup, "build_bill_family", fake_build)
    bill_family_rollup.BillFamilyRollup(output_dir=work, skip_upload=True).run()
    return work


def test_bill_family_quality_repair_rollup_repairs_native_generation_and_preserves_receipts(tmp_path, monkeypatch):
    """End-to-end receipt-backed repair over a complete native bill-family selection, carrying bill_sections natively."""
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.sources import r2

    work = _seed_native_bill_family_selection(tmp_path, monkeypatch)
    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("Repair must restore selected native priors"))

    dry = repairs.dry_run("repair-bill-family-quality", prior_dir=work)
    assert dry["bill_versions"] == {"rows": 2, "kind_moved": 2, "version_date_cleaned": 1, "rows_moved": 2}
    assert dry["section_diffs"] == {"rows": 1, "from_version_date_cleaned": 1, "to_version_date_cleaned": 1, "rows_moved": 1}
    assert dry["bill_cosponsors"]["sponsorship_withdrawn_date_cleaned"] == 1
    assert dry["bill_cosponsors"]["district_cleaned"] == 1
    assert dry["public_activity_events"]["stage_changed_null_from_dropped"] == 1
    assert dry["public_activity_events"]["stage_changed_preserved"] == 1

    rollup = repairs.repair_rollup("repair-bill-family-quality", repairs.REPAIRS["repair-bill-family-quality"])
    rollup(output_dir=work, skip_upload=True).run()

    versions = {r["version_code"]: r for r in pq.read_table(work / "bill_versions.parquet").to_pylist()}
    assert versions["introduced-in-house"]["kind"] == "full_text"
    assert versions["introduced-in-house"]["version_date"] is None
    assert versions["rfs"]["kind"] == "full_text"
    [diff] = pq.read_table(work / "section_diffs.parquet").to_pylist()
    assert diff["from_version_date"] is None and diff["to_version_date"] is None
    [cosponsor] = pq.read_table(work / "bill_cosponsors.parquet").to_pylist()
    assert cosponsor["sponsorship_withdrawn_date"] is None and cosponsor["district"] is None
    events = pq.read_table(work / "public_activity_events.parquet").to_pylist()
    assert [(e["event_type"], e["from_stage"], e["to_stage"]) for e in events] == [
        ("bill_added", None, None),
        ("stage_changed", "introduced", "committee"),
    ]
    # Untouched partitioned bill_sections survives intact through native partition carry
    section_parts = sorted((work / "bill_sections").rglob("*.parquet"))
    assert len(section_parts) == 1 and pq.ParquetFile(section_parts[0]).read().num_rows == 1
    # Raw XML and date-status provenance remain intact in restored receipts
    restored_cosponsors = SelectedPriors(tmp_path / "readback", root=work, public_url="").get("bill_cosponsors")
    assert restored_cosponsors is not None
    [restored_row] = pq.read_table(restored_cosponsors).to_pylist()
    assert restored_row["sponsorship_withdrawn_date"] is None
    assert restored_row["sponsorship_withdrawn_date_status"] == "empty"
    assert restored_row["source_xml"] == "<item><sponsorshipWithdrawnDate/><district/></item>"
    # Audited prior-repair event recorded in source evidence journal
    journals = sorted((work / "source-evidence").rglob("journal.jsonl"))
    repair_events = [
        json.loads(line)
        for journal in journals
        for line in journal.read_text().splitlines()
        if json.loads(line).get("event") == "prior-repair"
    ]
    assert any(ev["repair"] == "repair-bill-family-quality" and ev["counts"]["public_activity_events"]["stage_changed_null_from_dropped"] == 1 for ev in repair_events)

    # Idempotency check on native selection
    again_dry = repairs.dry_run("repair-bill-family-quality", prior_dir=work)
    assert again_dry["bill_versions"]["rows_moved"] == 0
    assert again_dry["section_diffs"]["rows_moved"] == 0
    assert again_dry["bill_cosponsors"]["rows_moved"] == 0
    assert again_dry["public_activity_events"]["stage_changed_null_from_dropped"] == 0


def test_repair_bill_family_quality_workflow_guards_permissions_concurrency_and_shell_safety(tmp_path):
    """Verify repair-bill-family-quality.yml shares bill-family concurrency and guards shrink/shell/secrets."""
    import os
    import subprocess
    import yaml

    repo_root = Path(__file__).resolve().parents[1]
    workflow_path = repo_root / ".github/workflows/repair-bill-family-quality.yml"
    rollup_path = repo_root / ".github/workflows/rollup-bill-family.yml"

    raw_text = workflow_path.read_text(encoding="utf-8")
    doc = yaml.safe_load(raw_text)
    rollup_doc = yaml.safe_load(rollup_path.read_text(encoding="utf-8"))

    triggers = doc.get("on", doc.get(True, {}))
    assert set(triggers) == {"workflow_dispatch"}
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["skip_upload"]["type"] == "boolean"
    assert inputs["skip_upload"]["default"] is True
    assert inputs["allow_shrink_public_activity_events"]["type"] == "boolean"
    assert inputs["allow_shrink_public_activity_events"]["default"] is False

    assert doc["permissions"] == {"contents": "read"}
    assert doc["concurrency"]["group"] == rollup_doc["concurrency"]["group"]
    assert doc["concurrency"]["cancel-in-progress"] is False
    assert not workflow_path.name.startswith("rollup-")
    assert "run-rollup-" not in raw_text
    assert "_rollup.yml" not in raw_text
    assert "secrets.ZYTE_TOKEN" not in raw_text
    assert "secrets.FIRECRAWL_API_KEY" not in raw_text

    steps = {step["name"]: step for step in doc["jobs"]["repair"]["steps"] if "name" in step}
    for name, step in steps.items():
        if "run" in step:
            assert "${{ inputs." not in step["run"], f"{name} interpolates raw inputs.* in run:"
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)
            if name not in {"Install pinned dependencies", "Free runner disk"}:
                assert 'TMPDIR="$RUNNER_TEMP"' in step["run"]
                assert "unset SPICYGOV_RAW_ROOT R2_ALLOW_SHRINK R2_MIN_SIZE_RATIO" in step["run"]

    dry_step = steps["Repair bill-family quality (dry run)"]
    publish_step = steps["Repair and publish bill-family quality"]
    assert dry_step["if"] == "inputs.skip_upload != false"
    assert publish_step["if"] == "inputs.skip_upload == false"
    assert "R2_ACCESS_KEY_ID" not in dry_step.get("env", {})
    assert "R2_SECRET_ACCESS_KEY" not in dry_step.get("env", {})
    assert "R2_ALLOW_SHRINK" not in dry_step["run"] or "unset SPICYGOV_RAW_ROOT R2_ALLOW_SHRINK" in dry_step["run"]
    assert "export R2_ALLOW_SHRINK=1" not in dry_step["run"]
    assert "R2_ACCESS_KEY_ID" in publish_step["env"]
    assert "R2_SECRET_ACCESS_KEY" in publish_step["env"]
    assert 'if [ "$ALLOW_SHRINK_PUBLIC_ACTIVITY_EVENTS" = "true" ]; then' in publish_step["run"]
    assert '-H "User-Agent: $WORKFLOW_USER_AGENT"' in dry_step["run"]
    assert '-H "User-Agent: $WORKFLOW_USER_AGENT"' in publish_step["run"]
    assert "repair-bill-family-quality" in dry_step["run"] and "--dry-run" in dry_step["run"]
    assert "repair-bill-family-quality" in publish_step["run"] and "--no-skip-upload" in publish_step["run"]

    validate = steps["Validate inputs"]
    env = {k: v for k, v in os.environ.items() if k not in {"PYTEST_CURRENT_TEST", "SPICYGOV_RAW_ROOT"}}
    env.update({
        "RUNNER_TEMP": str(tmp_path.resolve()),
        "SKIP_UPLOAD": "maybe",
        "ALLOW_SHRINK_PUBLIC_ACTIVITY_EVENTS": "false",
    })
    bad = subprocess.run(["bash", "-c", validate["run"]], cwd=repo_root, env=env, capture_output=True, text=True, timeout=30)
    assert bad.returncode != 0
    assert "SKIP_UPLOAD" in (bad.stderr + bad.stdout)
