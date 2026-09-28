"""The one-time upstream GAO copy adds only the reviewed reports, keeps the fork's rows and says where they came from."""

import json

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.import_gao_upstream import (
    RULE, UPSTREAM_URL, GaoUpstreamImport, ImportRefused, import_upstream_rows, rows_digest,
)
from spicy_regs.transforms.build_gao_reports import _SCHEMA

WINDOW = ("2026-07-13", "2026-09-14")


def _row(report_id: str, published_date: str, title: str = "A report") -> dict:
    return {"report_id": report_id, "title": title, "report_type": "Report", "published_date": published_date,
            "abstract": "What GAO found", "agencies_json": "[]", "topics_json": "[]",
            "url": f"https://www.gao.gov/products/{report_id}"}


# The fork's own rows: one the feed gave both tables, and an explicit target whose unknown fields stay NULL.
FORK = [_row("gao-26-900", "2026-09-20"),
        {**_row("gao-17-317", "2017-02-15"), "report_type": None, "abstract": None, "agencies_json": None}]
COPIED = [_row("gao-26-100", "2026-07-13"), _row("gao-26-200", "2026-09-14")]
UPSTREAM = [*COPIED, _row("gao-26-900", "2026-09-20")]


def _write(path, rows):
    table = rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows, schema=_SCHEMA)
    pq.write_table(table, path)
    return path


def _import(tmp_path, *, fork=FORK, upstream=UPSTREAM, reviewed=COPIED):
    output = tmp_path / "gao_reports.parquet"
    report = import_upstream_rows(_write(tmp_path / "fork.parquet", fork), _write(tmp_path / "up.parquet", upstream),
                                  output, window=WINDOW, rows_sha256=rows_digest(reviewed))
    return report, output


def test_the_union_keeps_every_fork_row_and_adds_only_the_reports_it_lacks(tmp_path):
    report, output = _import(tmp_path)
    table = pq.read_table(output)
    rows = {row["report_id"]: row for row in table.to_pylist()}
    assert [(f.name, f.type) for f in table.schema] == [(f.name, f.type) for f in _SCHEMA]
    assert (report["matched_rows"], report["copied_rows"], report["candidate_rows"]) == (1, 2, 4)
    assert report["copied_report_ids"] == ["gao-26-100", "gao-26-200"]
    assert (report["first_published"], report["last_published"]) == WINDOW
    assert rows == {row["report_id"]: row for row in [*FORK, *COPIED]}


def test_a_shared_report_that_differs_in_any_cell_is_refused(tmp_path):
    edited = [*COPIED, _row("gao-26-900", "2026-09-20", title="Retitled upstream")]
    with pytest.raises(ImportRefused, match="1 report.*differ: gao-26-900"):
        _import(tmp_path, upstream=edited)
    assert not (tmp_path / "gao_reports.parquet").exists()


@pytest.mark.parametrize("change", ["extra column", "renamed column", "wider type"])
def test_a_schema_other_than_the_familys_is_refused(tmp_path, change):
    table = pa.Table.from_pylist(UPSTREAM, schema=_SCHEMA)
    if change == "extra column":
        table = table.append_column("pages", pa.array(["1"] * len(UPSTREAM)))
    elif change == "renamed column":
        table = table.rename_columns(["summary" if name == "abstract" else name for name in table.column_names])
    else:
        table = table.cast(pa.schema([(name, pa.large_string()) for name in table.column_names]))
    with pytest.raises(ImportRefused, match="schema differs"):
        _import(tmp_path, upstream=table)
    assert not (tmp_path / "gao_reports.parquet").exists()


@pytest.mark.parametrize(("upstream", "match"), [
    # Upstream read a feed item the fork has not yet: not a report the fork never captured.
    ([*UPSTREAM, _row("gao-26-999", "2026-09-28")], "outside 2026-07-13..2026-09-14: gao-26-999"),
    # A report inside the window that was not reviewed, or a reviewed one upstream no longer holds.
    ([*UPSTREAM, _row("gao-26-150", "2026-08-01")], "3 report.*differ from the reviewed rows"),
    ([COPIED[0], UPSTREAM[-1]], "1 report.*differ from the reviewed rows"),
    # A reviewed report whose cells changed since review.
    ([_row("gao-26-100", "2026-07-13", title="Changed"), *UPSTREAM[1:]], "2 report.*differ from the reviewed rows"),
], ids=["upstream ahead of the fork", "unreviewed report", "reviewed report missing", "reviewed report changed"])
def test_anything_but_the_reviewed_reports_is_refused(tmp_path, upstream, match):
    with pytest.raises(ImportRefused, match=match):
        _import(tmp_path, upstream=upstream)
    assert not (tmp_path / "gao_reports.parquet").exists()


def _published_prior(remote, monkeypatch, tmp_path):
    """Publish the fork's table as a complete gao-reports generation in the fake bucket, as the rollup would."""
    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication as pub, r2

    tmp_path.mkdir()
    prior = _write(tmp_path / "gao_reports.parquet", FORK)
    build_generation(tmp_path / "prior", family="gao-reports", files=[prior], expected_keys=[prior.name])
    pub.publish_generation(tmp_path / "prior", client=remote, bucket="spicy-regs", prior_index=pub.empty_index())

    def download(remote_key, local_path):
        location = pub.single_member(pub.parse_index(remote.objects[pub.INDEX_V2_KEY]), remote_key).path
        local_path.write_bytes(remote.objects[location])
        return True

    monkeypatch.setattr(r2, "download", download)
    monkeypatch.setattr(pub, "load_family_root", lambda url, entry: (
        raw := remote.objects[f"{entry['prefix']}/artifact.json"], pub.family_root(raw, entry)))
    return pub.parse_index(remote.objects[pub.INDEX_V2_KEY])["families"]["gao-reports"]


def _serve_upstream(monkeypatch, rows):
    body = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), body)
    headers = {"etag": '"e0555502034fb79e90b8a14b26bc8d05"', "last-modified": "Sun, 27 Sep 2026 19:59:08 GMT",
               "content-type": "application/octet-stream"}
    requests = []

    def serve(request):
        requests.append(str(request.url))
        return httpx.Response(200, headers=headers, content=body.getvalue().to_pybytes())

    monkeypatch.setattr(GaoUpstreamImport, "upstream_transport", httpx.MockTransport(serve))
    monkeypatch.setattr("scripts.import_gao_upstream.ROWS_SHA256", rows_digest(COPIED))
    monkeypatch.setattr("scripts.import_gao_upstream.WINDOW", WINDOW)
    return requests


def test_the_published_generation_names_its_parents_and_says_the_rows_were_copied(tmp_path, monkeypatch, remote):
    from spicy_regs.sources import publication as pub

    prior = _published_prior(remote, monkeypatch, tmp_path / "setup")
    requests = _serve_upstream(monkeypatch, UPSTREAM)

    GaoUpstreamImport(output_dir=tmp_path / "run", skip_upload=False).run()

    entry = pub.parse_index(remote.objects[pub.INDEX_V2_KEY])["families"]["gao-reports"]
    root = json.loads(remote.objects[f"{entry['prefix']}/artifact.json"])
    assert requests == [UPSTREAM_URL] and entry["tables"]["gao_reports.parquet"]["rows"] == 4
    parents = root["spec"]["parents"]
    assert parents["gao_reports.parquet"]["artifactDigest"] == prior["artifactDigest"]
    assert set(parents[UPSTREAM_URL]) == {"sha256", "byteSize"}
    assert {"role": "prior-generation", "logicalId": prior["logicalId"],
            "artifactDigest": prior["artifactDigest"]} in root["inputs"]
    evidence = next(item for item in root["inputs"] if item["role"] == "source-evidence")
    journal = [json.loads(line) for line in _journal(remote, evidence).splitlines()]
    [copy] = [event for event in journal if event["event"] == "upstream-publication-copy"]
    assert copy["rule"] == RULE and copy["source_url"] == UPSTREAM_URL
    assert copy["etag"] == '"e0555502034fb79e90b8a14b26bc8d05"' and copy["last_modified"].startswith("Sun, 27 Sep 2026")
    assert copy["copied_report_ids"] == ["gao-26-100", "gao-26-200"] and copy["matched_rows"] == 1
    assert "not captured from GAO by this fork" in copy["limits"]
    [capture] = [event for event in journal if event["event"] == "capture"]
    assert capture["requested_url"] == UPSTREAM_URL and capture["sha256"] == parents[UPSTREAM_URL]["sha256"]


def _journal(remote, pin) -> str:
    """The published evidence journal, read the way ``load_evidence_journal`` reads it, from the fake bucket."""
    from spicy_regs.sources import publication as pub

    served = lambda url, **_: remote.objects[url.removeprefix("https://example.test/")]  # noqa: E731
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(pub, "_bounded_get", served)
        return pub.load_evidence_journal("https://example.test", pin).decode()


def test_the_dry_run_builds_the_same_generation_and_publishes_nothing(tmp_path, monkeypatch, remote):
    from spicy_regs.sources import publication as pub

    prior = _published_prior(remote, monkeypatch, tmp_path / "setup")
    _serve_upstream(monkeypatch, UPSTREAM)
    writes = len(remote.writes)

    GaoUpstreamImport(output_dir=tmp_path / "run", skip_upload=True).run()

    assert len(remote.writes) == writes
    assert pub.parse_index(remote.objects[pub.INDEX_V2_KEY])["families"]["gao-reports"] == prior
    [generation] = (tmp_path / "run" / "generations").iterdir()
    assert pq.read_table(generation / "gao_reports.parquet").num_rows == 4
