"""White House retained replay, refresh versions, identity refusal and ordinary MCP consumption."""

from copy import deepcopy
import json

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.white_house import INDEX_URL, WhiteHouseError
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs import mcp_server
from spicy_regs.generations import verify_generation
from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.contract_types import described_schema
from spicy_regs.pipelines import white_house as pipeline
from spicy_regs.source_evidence import verify_evidence
from tests.test_mcp_server import _records, _tool_data

URL = "https://www.whitehouse.gov/presidential-actions/2026/11/nominations-sent-to-the-senate-test/"
OTHER = "https://www.whitehouse.gov/presidential-actions/2026/11/nominations-and-withdrawals-test/"
STAMP = "2026-11-12T17:04:41-05:00"


def html(url, body):
    return (f'<link rel="canonical" href="{url}">' + body).encode()


def article(url=URL, body="Nathan Anonick, to be a Commissioner."):
    return html(
        url,
        f'''<meta property="article:published_time" content="2026-11-12T22:04:41+00:00">
        <main><div class="entry-content wp-block-post-content"><div class="wp-block-whitehouse-topper">
        <h1>Nominations</h1><div class="wp-block-whitehouse-byline-subcategory"><a>Nominations &amp; Appointments</a></div>
        <time datetime="{STAMP}"></time></div><p>{body}</p></div></main>''',
    )


def listing():
    rows = "".join(
        f'''<li class="wp-block-post post-{i}"><h2><a href="{url}">Nominations</a></h2>
        <a rel="tag">Presidential Actions</a><a rel="tag">Nominations &amp; Appointments</a>
        <time datetime="{STAMP}"></time></li>'''
        for i, url in enumerate((URL, OTHER), 1)
    )
    return html(INDEX_URL, f"<main>{rows}</main>")


def transport(*, changed=False, status=200, calls=None):
    def respond(request):
        if calls is not None:
            calls.append(str(request.url))
        body = (
            listing()
            if str(request.url) == INDEX_URL
            else article(
                str(request.url),
                "Changed source version."
                if changed and str(request.url) == URL
                else "Nathan Anonick, to be a Commissioner.",
            )
        )
        return httpx.Response(status, headers={"content-type": "text/html"}, stream=httpx.ByteStream(body))

    return httpx.MockTransport(respond)


def capture(tmp_path, *, prior=None, changed=False):
    return pipeline.acquire(
        "2026-11-01",
        "2026-11-13",
        tmp_path / "acquire",
        archive_root=tmp_path / "raw",
        prior=prior,
        transport=transport(changed=changed),
    )


def test_real_delivery_path_builds_evidenced_generation_and_answers_query(tmp_path, monkeypatch):
    selected = capture(tmp_path)
    artifact, evidence, built = pipeline.build_from_captures(
        selected, tmp_path / "build", archive_root=tmp_path / "raw"
    )
    assert built["rows"] == 2
    assert pipeline.TABLE not in installed_policies()
    assert expected_schemas()[pipeline.TABLE] == described_schema(pipeline.SCHEMA)
    assert verify_generation(tmp_path / "build" / "generation").pin == artifact.pin
    verify_evidence(evidence, archive_root=tmp_path / "raw")
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "build" / "generation")
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    try:
        server = mcp_server.build_server()
        result = _tool_data(
            server,
            "query_sql",
            {
                "sql": f"SELECT source_url, document_type, source_sha256 FROM {pipeline.TABLE} WHERE body_text ILIKE '%Nathan Anonick%' ORDER BY source_url"
            },
        )
        assert {r["source_url"] for r in _records(result)} == {URL, OTHER}
        assert all(r["document_type"] == "Nominations & Appointments" for r in _records(result))
        described = _tool_data(server, "describe_table", {"table": pipeline.TABLE})
        assert "related" in json.dumps(described).lower()
    finally:
        con.close()


def test_changed_page_is_a_version_and_exact_refresh_keeps_prior_observation(tmp_path):
    selected = capture(tmp_path / "one")
    # Share the same explicitly selected archive; no default shared fixture archive.
    later = pipeline.acquire(
        "2026-11-01",
        "2026-11-13",
        tmp_path / "later-acquire",
        archive_root=tmp_path / "one" / "raw",
        prior=selected,
        transport=transport(changed=True),
    )
    assert later["actions"][1] == selected["actions"][1]
    assert later["actions"][0]["sha256"] != selected["actions"][0]["sha256"]
    _, _, built = pipeline.build_from_captures(later, tmp_path / "later", archive_root=tmp_path / "one" / "raw")
    assert built["rows"] == 3
    rows = pq.read_table(tmp_path / "later" / (pipeline.TABLE + ".parquet")).to_pylist()
    assert len({r["action_version_id"] for r in rows}) == 3
    assert len({r["source_url"] for r in rows}) == 2
    repeated = pipeline.acquire(
        "2026-11-01",
        "2026-11-13",
        tmp_path / "repeat-acquire",
        archive_root=tmp_path / "one" / "raw",
        prior=later,
        transport=transport(changed=True),
    )
    assert len(repeated["prior_actions"]) == 3
    _, _, built = pipeline.build_from_captures(repeated, tmp_path / "repeat", archive_root=tmp_path / "one" / "raw")
    assert built["rows"] == 3


def test_existing_routes_require_proven_identity_and_never_title_similarity(tmp_path):
    selected = capture(tmp_path)
    original = selected["actions"][0]
    existing = {
        "source_url": original["url"],
        "source_sha256": original["sha256"],
        "route": "govinfo",
        "document_id": "explicit-source-version",
    }
    _, _, built = pipeline.build_from_captures(
        selected, tmp_path / "reuse", archive_root=tmp_path / "raw", existing_records=[existing]
    )
    assert built["rows"] == 1 and built["decisions"][0]["outcome"] == "reuse-existing"
    with pytest.raises(ValueError, match="titles do not match"):
        pipeline.existing_route(original, [{"title": "Nominations", "document_id": "invented", "route": "govinfo"}])
    with pytest.raises(ValueError, match="Ambiguous"):
        pipeline.existing_route(original, [existing, existing])
    assert pipeline.existing_route({**original, "sha256": "sha256:" + "a" * 64}, [existing]) is None


def test_missing_capture_or_corrupt_input_cannot_make_a_partial_generation(tmp_path):
    selected = capture(tmp_path)
    missing = deepcopy(selected)
    missing["actions"].pop()
    with pytest.raises(ValueError, match="differ from"):
        pipeline.build_from_captures(missing, tmp_path / "missing", archive_root=tmp_path / "raw")
    assert not (tmp_path / "missing" / "generation").exists()
    corrupt = deepcopy(selected)
    corrupt["actions"][0]["sha256"] = "sha256:" + "0" * 64
    with pytest.raises((FileNotFoundError, ValueError)):
        pipeline.build_from_captures(corrupt, tmp_path / "corrupt", archive_root=tmp_path / "raw")
    assert not (tmp_path / "corrupt" / "generation").exists()


def test_access_refusal_is_journaled_and_stops_without_other_requests(tmp_path):
    calls = []
    with pytest.raises(CredentialRefusedError):
        pipeline.acquire(
            "2026-11-01",
            "2026-11-13",
            tmp_path / "refused",
            archive_root=tmp_path / "raw",
            transport=transport(status=403, calls=calls),
        )
    assert calls == [INDEX_URL]
    (journal,) = (tmp_path / "refused").glob("source-evidence/*/artifact/journal.jsonl")
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    assert any(e["event"] == "capture-incomplete" and e["error_type"] == "CredentialRefusedError" for e in events)
    assert any(e["event"] == "refusal" and e["credential_refused"] for e in events)
    assert events[-1]["outcome"] == "failed"


def test_window_cap_never_yields_a_partial_selection(tmp_path):
    body = listing().replace(
        b"</main>", f'<a class="wp-block-query-pagination-next" href="{INDEX_URL}page/2/">Next</a></main>'.encode()
    )
    mock = httpx.MockTransport(
        lambda request: httpx.Response(200, headers={"content-type": "text/html"}, stream=httpx.ByteStream(body))
    )
    with pytest.raises(WhiteHouseError, match="page cap"):
        pipeline.acquire(
            "2026-11-01", "2026-11-13", tmp_path / "capped", max_pages=1, archive_root=tmp_path / "raw", transport=mock
        )


def test_local_publication_cli_accepts_declared_cohort_and_keeps_guards(tmp_path, monkeypatch):
    """Fixtures prove CLI admission; real retained-source qualification is separate lane evidence."""
    from rulespec_artifacts import ArtifactInput

    from spicy_regs import local_publication
    from spicy_regs.generations import build_generation
    from spicy_regs.source_evidence import INPUT_ROLE, verify_evidence
    from spicy_regs.sources import publication

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    selected = capture(tmp_path)
    artifact, evidence, _ = pipeline.build_from_captures(selected, tmp_path / "build", archive_root=tmp_path / "raw")
    index = tmp_path / "private-index.json"
    index.write_text(json.dumps(publication.empty_index()))
    output = tmp_path / "dry-run"
    args = [
        "--generation",
        str(tmp_path / "build" / "generation"),
        "--family",
        pipeline.FAMILY,
        "--evidence",
        str(evidence),
        "--dry-run",
        "--scratch-raw",
        "--live-index",
        str(index),
        "--output",
        str(output),
    ]
    wrong = list(args)
    wrong[wrong.index(pipeline.FAMILY)] = "treasury-fiscal"
    with pytest.raises(publication.PublicationError, match="not the expected treasury-fiscal"):
        local_publication.main(wrong)
    missing = list(args)
    del missing[missing.index("--evidence") : missing.index("--evidence") + 2]
    with pytest.raises(publication.PublicationError, match="pass --evidence"):
        local_publication.main(missing)
    assert not output.exists()
    local_publication.main(args)
    receipt = json.loads((output / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == pipeline.FAMILY
    assert receipt["artifactDigest"] == artifact.pin.artifact_digest
    assert receipt["tables"][pipeline.TABLE + ".parquet"]["rows"] == 2
    assert receipt["other_family_changes"] == []
    assert json.loads(index.read_text()) == publication.empty_index()

    # Classify this control as partial through the builder; do not edit a pinned artifact.
    evidence_artifact = verify_evidence(evidence, archive_root=tmp_path / "raw")
    build_generation(
        tmp_path / "partial",
        family=pipeline.FAMILY,
        files=[tmp_path / "build" / (pipeline.TABLE + ".parquet")],
        expected_keys=[pipeline.TABLE + ".parquet"],
        publication_status="local-partial",
        inputs=[ArtifactInput(INPUT_ROLE, evidence_artifact.pin.logical_id, evidence_artifact.pin.artifact_digest)],
    )
    partial = list(args)
    partial[partial.index("--generation") + 1] = str(tmp_path / "partial")
    partial[partial.index("--output") + 1] = str(tmp_path / "partial-refused")
    with pytest.raises(publication.PublicationError, match="local partial candidate cannot be published"):
        local_publication.main(partial)
    assert not (tmp_path / "partial-refused").exists()
