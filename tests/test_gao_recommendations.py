"""gao_recommendations: the daily export folded into an accumulator, and no director phone in anything published.

The export is SpicyDocs' fixture excerpt (``fixtures/gao_recommendations/``), served through a mock transport, so the
real acquirer and reader run; no Zyte request is made.
"""

from __future__ import annotations

import csv
import io
import json
import shutil
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas.gao_recommendation_tables import GAO_RECOMMENDATIONS
from spicy_docs.sources.gao.recommendations import (
    EXPORT_URL,
    GaoRecommendationsSourceError,
)

from spicy_regs.transforms import build_gao_recommendations as build

EXCERPT = (Path(__file__).parent / "fixtures" / "gao_recommendations" / "open-recs-2026-09-28-excerpt.csv").read_bytes()
STAMP = b"status as of Sep 28, 2026 at 7:05 PM EST"
NEXT_DAY = b"status as of Sep 29, 2026 at 7:06 PM EST"


def _records(body: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(body.decode(), newline="")))


#: Every value the export states in its Director Phone column: what no published or retained file may hold.
STATED = [row[4] for row in _records(EXCERPT)[6:] if row[4]]
PHONES = set(STATED)


def _without(positions: set[int], body: bytes = EXCERPT) -> bytes:
    """The export less the data records at ``positions``, every other byte kept (the last record has no terminator)."""
    records, start, quoted = [], 0, False
    for index, byte in enumerate(body):
        if byte == ord('"'):
            quoted = not quoted
        elif byte == ord("\n") and not quoted:
            records.append(body[start : index + 1])
            start = index + 1
    records.append(body[start:])
    head, data = records[:6], records[6:]
    kept = [record for index, record in enumerate(data) if index not in positions]
    kept[-1] = kept[-1].rstrip(b"\n")
    return b"".join(head + kept)


class Export(httpx.MockTransport):
    """GAO's answer, one request at a time; ``records`` stands in for the Zyte transport's proxy records."""

    records: tuple = ()

    def __init__(self, body: bytes = EXCERPT, status: int = 200):
        self.calls: list[str] = []

        def handle(request: httpx.Request) -> httpx.Response:
            self.calls.append(str(request.url))
            return httpx.Response(status, stream=httpx.ByteStream(body), headers={"content-type": "text/csv; charset=UTF-8"})

        super().__init__(handle)


def _prior(path: Path | None):
    """A ``download_prior`` that serves ``path`` as the published table, or reports none published."""

    def download(_key: str, local: Path) -> bool:
        if path is None:
            return False
        shutil.copyfile(path, local)
        return True

    return download


def _run(directory: Path, body: bytes = EXCERPT, prior: Path | None = None, evidence=None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    return build.build_gao_recommendations(
        directory, evidence=evidence, transport=Export(body), download_prior=_prior(prior)
    )


def _rows(path: Path) -> dict[str, dict]:
    return {row["recommendation_id"]: row for row in pq.read_table(path).to_pylist()}


def test_a_first_run_publishes_every_listed_recommendation_open(tmp_path):
    out = _run(tmp_path / "first")
    table = pq.read_table(out)
    assert tuple(table.column_names) == GAO_RECOMMENDATIONS.columns
    rows = table.to_pylist()
    assert len(rows) == 35 and len({row["recommendation_id"] for row in rows}) == 35
    numbered = [row for row in rows if row["recommendation_number"] is not None]
    assert len(numbered) == 33 and {row["recommendation_kind"] for row in numbered} == {"recommendation", "matter"}
    assert {(row["first_seen"], row["last_seen"], row["listed_open"]) for row in rows} == {
        ("2026-09-28", "2026-09-28", "true")
    }
    assert "director_phone" not in table.column_names


def test_a_second_run_keeps_first_seen_and_marks_what_the_export_dropped(tmp_path):
    first = _run(tmp_path / "first")
    dropped = {0, 5, 9, 22, 25}
    second = _run(tmp_path / "second", _without(dropped, EXCERPT.replace(STAMP, NEXT_DAY)), prior=first)
    before, after = _rows(first), _rows(second)
    assert set(after) == set(before), "a recommendation that leaves the export is never deleted"
    gone = {key for key, row in after.items() if row["listed_open"] == "false"}
    assert len(gone) == len(dropped)
    for key, row in after.items():
        assert row["first_seen"] == "2026-09-28"
        if key in gone:
            assert row == {**before[key], "listed_open": "false"}
        else:
            assert (row["last_seen"], row["status_as_of"]) == ("2026-09-29", "Sep 29, 2026 at 7:06 PM EST")


def test_a_recommendation_listed_again_is_open_again_and_keeps_its_first_seen(tmp_path):
    first = _run(tmp_path / "first")
    second = _run(tmp_path / "second", _without({3}, EXCERPT.replace(STAMP, NEXT_DAY)), prior=first)
    third = _run(tmp_path / "third", EXCERPT.replace(STAMP, b"status as of Sep 30, 2026 at 7:07 PM EST"), prior=second)
    assert {(row["first_seen"], row["last_seen"], row["listed_open"]) for row in _rows(third).values()} == {
        ("2026-09-28", "2026-09-30", "true")
    }


def test_the_status_as_last_listed_replaces_the_prior_status(tmp_path):
    first = _run(tmp_path / "first")
    changed = EXCERPT.replace(STAMP, NEXT_DAY).replace(b",Open,No,", b",Open--Partially Addressed,Yes,", 1)
    before, after = _rows(first), _rows(_run(tmp_path / "second", changed, prior=first))
    moved = [row for key, row in after.items() if row["status"] != before[key]["status"]]
    assert [(row["status"], row["priority"], row["first_seen"]) for row in moved] == [
        ("Open--Partially Addressed", "true", "2026-09-28")
    ]


def test_an_export_that_would_retire_more_than_half_the_open_rows_is_refused(tmp_path):
    """18 of 35 dropped is more than half: refused, and the published table is left as it was."""
    first = _run(tmp_path / "first")
    with pytest.raises(build.GaoRecommendationsFoldError, match="lists 17 of the 35"):
        _run(tmp_path / "second", _without(set(range(18)), EXCERPT.replace(STAMP, NEXT_DAY)), prior=first)
    assert not (tmp_path / "second" / build.OUTPUT).exists()


def test_just_over_half_still_listed_passes_the_guard(tmp_path):
    first = _run(tmp_path / "first")
    out = _run(tmp_path / "second", _without(set(range(17)), EXCERPT.replace(STAMP, NEXT_DAY)), prior=first)
    assert sum(row["listed_open"] == "false" for row in _rows(out).values()) == 17


def test_the_guard_counts_only_rows_the_prior_lists_open():
    open_row = {**dict.fromkeys(GAO_RECOMMENDATIONS.columns), "recommendation_id": "a", "listed_open": "true"}
    closed = [{**open_row, "recommendation_id": f"c{n}", "listed_open": "false"} for n in range(9)]
    other = {**open_row, "recommendation_id": "b"}
    assert build.fold([open_row, other], [open_row])[1]["retired"] == 1, "exactly half still listed passes"
    rows, retirement = build.fold([open_row, *closed], [open_row])
    assert len(rows) == 10 and retirement == {"open_before": 1, "still_listed": 1, "retired": 0, "mass_close_reason": None}
    with pytest.raises(build.GaoRecommendationsFoldError):
        build.fold([open_row, *closed], [])
    assert build.fold([], []) == ([], {"open_before": 0, "still_listed": 0, "retired": 0, "mass_close_reason": None})
    assert build.fold(closed, [])[0] == [{**row, "listed_open": "false"} for row in closed]


REASON = "GAO closed its 2019 audit series in one batch (dispatch by the owner)"


def test_a_stated_reason_lets_a_mass_close_through_and_the_evidence_keeps_it(tmp_path):
    """Refused without a reason (above); with one the run proceeds, and the journal names it and what it retired."""
    from spicy_regs.source_evidence import CaptureEvidence

    first = _run(tmp_path / "first")
    evidence = CaptureEvidence(tmp_path / "second", "gao-recommendations")
    out = build.build_gao_recommendations(
        tmp_path / "second",
        evidence=evidence,
        transport=Export(_without(set(range(18)), EXCERPT.replace(STAMP, NEXT_DAY))),
        download_prior=_prior(first),
        allow_mass_close_reason=REASON,
    )
    assert sum(row["listed_open"] == "false" for row in _rows(out).values()) == 18
    evidence.finish()
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    (allowed,) = [event for event in journal if event["event"] == "mass-close-allowed"]
    assert allowed["reason"] == REASON and allowed["mass_close_reason"] == REASON
    assert (allowed["retired"], allowed["open_before"], allowed["still_listed"]) == (18, 35, 17)
    assert evidence.artifact is not None and (evidence.artifact_dir / "journal.jsonl") in evidence.artifact_dir.iterdir()


def test_a_reason_is_journaled_only_when_the_guard_would_have_refused(tmp_path):
    from spicy_regs.source_evidence import CaptureEvidence

    first = _run(tmp_path / "first")
    evidence = CaptureEvidence(tmp_path / "second", "gao-recommendations")
    build.build_gao_recommendations(
        tmp_path / "second",
        evidence=evidence,
        transport=Export(_without({0}, EXCERPT.replace(STAMP, NEXT_DAY))),
        download_prior=_prior(first),
        allow_mass_close_reason=REASON,
    )
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    assert [event["retired"] for event in events if event["event"] == "fold"] == [1]
    assert not [event for event in events if event["event"] == "mass-close-allowed"]


def test_the_rollup_reads_the_reason_from_the_dispatch_and_a_schedule_never_sets_it(tmp_path, monkeypatch):
    import yaml

    from spicy_regs.pipelines.rollups import gao_recommendations as rollup

    seen = []
    monkeypatch.setattr(rollup, "build_gao_recommendations", lambda _dir, **kwargs: seen.append(kwargs))
    monkeypatch.setenv("GAO_ALLOW_MASS_CLOSE_REASON", f"  {REASON}  ")
    rollup.GaoRecommendationsRollup().build(tmp_path)
    monkeypatch.setenv("GAO_ALLOW_MASS_CLOSE_REASON", " ")
    rollup.GaoRecommendationsRollup().build(tmp_path)
    assert [kwargs["allow_mass_close_reason"] for kwargs in seen] == [REASON, None]
    workflows = Path(__file__).resolve().parents[1] / ".github" / "workflows"
    caller = yaml.safe_load((workflows / "rollup-gao-recommendations.yml").read_text())
    passed = caller["jobs"]["run"]["with"]["gao_allow_mass_close_reason"]
    assert passed == "${{ github.event_name == 'workflow_dispatch' && inputs.allow_mass_close_reason || '' }}"
    assert "GAO_ALLOW_MASS_CLOSE_REASON: ${{ inputs.gao_allow_mass_close_reason }}" in (workflows / "_rollup.yml").read_text()


def test_the_evidence_copy_is_spicy_docs_redaction_of_the_export(tmp_path, monkeypatch):
    """The one redactor lives beside the reader's header; the retained copy is its output, byte for byte."""
    from spicy_docs.sources.gao.recommendations import redact_director_phone

    _rollup(tmp_path, monkeypatch, EXCERPT).run()
    redacted, emptied = redact_director_phone(EXCERPT)
    blobs = [path.read_bytes() for path in (tmp_path / "source-evidence").rglob("sha256/*") if path.is_file()]
    assert redacted in blobs and EXCERPT not in blobs and emptied == len(STATED) == 14


def _published_text(root: Path) -> list[tuple[Path, str]]:
    """Every file under ``root`` as text a reader would see: Parquet decoded to its values, anything else as bytes."""
    seen = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.suffix == ".parquet":
            seen.append((path, json.dumps(pq.read_table(path).to_pylist(), ensure_ascii=False)))
        else:
            seen.append((path, path.read_bytes().decode("utf-8", "replace")))
    return seen


def _phones_in(root: Path) -> list[tuple[str, str]]:
    return [(str(path.relative_to(root)), phone) for path, text in _published_text(root) for phone in PHONES if phone in text]


def test_the_phone_check_finds_a_phone_where_one_is(tmp_path):
    """The scan below passes only if it can fail: the raw export, and a Parquet of it, both show their phones."""
    import pyarrow as pa

    (tmp_path / "raw.csv").write_bytes(EXCERPT)
    pq.write_table(pa.table({"phone": sorted(PHONES)}), tmp_path / "raw.parquet", compression="zstd")
    found = {path for path, _ in _phones_in(tmp_path)}
    assert found == {"raw.csv", "raw.parquet"}
    assert len(PHONES) == 8


def _rollup(tmp_path, monkeypatch, body: bytes):
    from spicy_regs.pipelines.rollups.gao_recommendations import GaoRecommendationsRollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(build, "_zyte_transport", lambda budget, evidence: Export(body))
    return GaoRecommendationsRollup(output_dir=tmp_path, skip_upload=True)


def test_no_generation_or_evidence_file_holds_a_director_phone(tmp_path, monkeypatch):
    """The whole local run, generation and source evidence both, as publication would upload them."""
    _rollup(tmp_path, monkeypatch, EXCERPT).run()
    generations = list((tmp_path / "generations").iterdir())
    evidence = list((tmp_path / "source-evidence").iterdir())
    assert len(generations) == 1 and len(evidence) == 1
    assert _phones_in(tmp_path) == []
    journal = [json.loads(line) for line in (evidence[0] / "artifact" / "journal.jsonl").read_text().splitlines()]
    (redacted,) = [event for event in journal if event["event"] == "redacted"]
    (capture,) = [event for event in journal if event["event"] == "capture"]
    import hashlib

    assert redacted["original_sha256"] == "sha256:" + hashlib.sha256(EXCERPT).hexdigest()
    assert redacted["original_byte_size"] == len(EXCERPT) and redacted["fields_emptied"] == len(STATED)
    assert capture["sha256"] == redacted["retained_sha256"] != redacted["original_sha256"]
    assert capture["requested_url"] == EXPORT_URL


def test_a_refused_export_leaves_no_director_phone_behind_either(tmp_path, monkeypatch):
    changed = EXCERPT.replace(b'"Publication  Number"', b'"Publication Number"')
    with pytest.raises(GaoRecommendationsSourceError, match="header"):
        _rollup(tmp_path, monkeypatch, changed).run()
    assert _phones_in(tmp_path) == []
    (evidence,) = (tmp_path / "source-evidence").iterdir()
    events = [json.loads(line)["event"] for line in (evidence / "artifact" / "journal.jsonl").read_text().splitlines()]
    assert "redacted" in events and "refusal" in events
