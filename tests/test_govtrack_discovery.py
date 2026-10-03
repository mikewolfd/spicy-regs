"""Discovery review keeps canonical source status unchanged and aliases explicit."""

import copy
import hashlib
import json

import pytest

from spicy_regs.scorecards import govtrack_discovery as discovery

from spicy_regs.scorecards.govtrack_discovery import (
    DEFAULT_COMMIT,
    REPOSITORY,
    DiscoveryReconciliationError,
    main,
    reconcile,
)

NOW = "2026-10-03T20:00:00Z"


def lead(path="Example.yaml", name="Example Rights Group", url="https://example.org/scorecard"):
    path = "scorecards/" + path
    return {
        "discovery_id": "govtrack:" + path,
        "repository": REPOSITORY,
        "commit": DEFAULT_COMMIT,
        "repository_path": path,
        "repository_blob_sha1": "a" * 40,
        "capture_sha256": "sha256:" + "b" * 64,
        "metadata_sha256": "sha256:" + "c" * 64,
        "metadata_line_end": 7,
        "name": name,
        "abbreviation": None,
        "homepage_url": "https://example.org/",
        "original_scorecard_url": url,
        "updated_text": "2019-08-15",
        "period_text": "based on votes in the 116th Congress",
        "rating_unit_text": "percent",
        "observed_at": NOW,
        "discovered_via": f"https://github.com/{REPOSITORY}/blob/{DEFAULT_COMMIT}/{path}",
    }


def source(publisher="existing", name="Example Rights Group"):
    return {
        "publisher_id": publisher,
        "source_id": publisher + ":federal",
        "publisher_name": name,
        "publisher_aliases": [],
        "discovery_status": "supported",
        "verification_capture_ids": ["original-only"],
        "homepage_url": "https://publisher.test/",
    }


def run(leads=None, sources=None, rules=None):
    return reconcile(
        leads or [lead()], {"sources": sources or [source()]}, {"version": 1, "rules": rules or []}, imported_at=NOW
    )


def test_exact_match_keeps_original_status_and_verification_evidence_unchanged():
    catalog = {"sources": [source()]}
    prior = copy.deepcopy(catalog)
    result = reconcile([lead()], catalog, {"version": 1, "rules": []}, imported_at=NOW)
    assert catalog == prior
    (row,) = result["results"]
    assert row["publisher_id"] == "existing"
    assert row["catalog_status_at_import"] == "supported"
    assert row["discovery"]["updated_text"] == "2019-08-15"
    assert row["imported_at"] == NOW
    assert row["proposed_addition"] is None
    assert row["original_publisher_verified_by_this_import"] is False
    assert all(not flag for flag in result["authority"].values())


def test_explicit_alias_maps_renamed_publisher_and_records_reason():
    result = run(
        sources=[source(name="New Name")],
        rules=[
            {
                "repository_path": "scorecards/Example.yaml",
                "publisher_id": "existing",
                "reason": "Reviewed historical source name",
            }
        ],
    )
    (row,) = result["results"]
    assert row["publisher_id"] == "existing"
    assert row["rule"] == "explicit_alias_with_conflict_check"
    assert row["alias_reason"] == "Reviewed historical source name"


def test_alias_conflict_is_ambiguous_and_does_not_override_exact_evidence():
    result = run(
        sources=[source(), source("other", "Other Name")],
        rules=[{"repository_path": "scorecards/Example.yaml", "publisher_id": "other", "reason": "Conflicting review"}],
    )
    (row,) = result["results"]
    assert row["reconciliation_status"] == "ambiguous"
    assert row["candidate_publisher_ids"] == ["existing", "other"]
    assert row["publisher_id"] is None
    assert row["proposed_addition"] is None


def test_identical_names_preserve_ambiguity_and_multiple_source_files_survive():
    ambiguous = run(sources=[source(), source("other")])["results"][0]
    assert ambiguous["candidate_count"] == 2
    result = run(leads=[lead(), lead("Second.yaml")])
    assert len(result["results"]) == 2
    assert result["duplicate_lead_groups"] == [
        {
            "publisher_id": "existing",
            "discovery_ids": ["govtrack:scorecards/Example.yaml", "govtrack:scorecards/Second.yaml"],
        }
    ]


def test_novel_lead_id_stable_across_import_time_and_changed_metadata():
    first = run(sources=[source(name="Unrelated")])["results"][0]["proposed_addition"]
    second = reconcile(
        [lead(name="Changed Label")],
        {"sources": [source(name="Unrelated")]},
        {"version": 1, "rules": []},
        imported_at="2026-10-04T12:00:00Z",
    )["results"][0]["proposed_addition"]
    assert first["publisher_id"] == second["publisher_id"]
    assert first["discovery_status"] == "discovered"
    assert first["last_verified_at"] is None


@pytest.mark.parametrize(
    "rules",
    [
        [{"repository_path": "scorecards/Example.yaml", "publisher_id": "missing", "reason": "x"}],
        [{"repository_path": "scorecards/Absent.yaml", "publisher_id": "existing", "reason": "x"}],
        [{"repository_path": "scorecards/Example.yaml", "publisher_id": "existing", "reason": ""}],
        [{"repository_path": "scorecards/Example.yaml", "publisher_id": "existing", "reason": "x"}] * 2,
    ],
)
def test_missing_duplicate_and_stale_aliases_refuse(rules):
    with pytest.raises(DiscoveryReconciliationError):
        run(rules=rules)


@pytest.mark.parametrize(
    "leads",
    [
        [],
        [lead(), lead()],
        [lead() | {"ratings": [99]}],
        [lead() | {"name": None}],
        [lead() | {"observed_at": "2026-10-03"}],
    ],
)
def test_empty_ambiguous_or_injected_discovery_refuses(leads):
    with pytest.raises(DiscoveryReconciliationError):
        reconcile(leads, {"sources": [source()]}, {"version": 1, "rules": []}, imported_at=NOW)


def test_failed_replay_preserves_prior_review_output(tmp_path, monkeypatch):
    import spicy_regs.scorecards.govtrack_discovery as module

    output = tmp_path / "review.json"
    output.write_text("prior accepted review\n")

    def refuse(_directory):
        raise DiscoveryReconciliationError("retained acquisition incomplete")

    monkeypatch.setattr(module, "load_retained", refuse)
    with pytest.raises(DiscoveryReconciliationError):
        main(
            [
                "reconcile",
                "--retained-dir",
                str(tmp_path / "retained"),
                "--catalog",
                str(tmp_path / "catalog.json"),
                "--aliases",
                str(tmp_path / "aliases.json"),
                "--output",
                str(output),
            ]
        )
    assert output.read_text() == "prior accepted review\n"


def test_cli_emits_only_review_and_pins_inputs(tmp_path, monkeypatch):
    import spicy_regs.scorecards.govtrack_discovery as module

    catalog, aliases, output = (tmp_path / n for n in ("catalog.json", "aliases.json", "review.json"))
    catalog.write_text(json.dumps({"sources": [source()]}))
    aliases.write_text('{"version":1,"rules":[]}')
    prior = catalog.read_bytes()
    monkeypatch.setattr(
        module,
        "load_retained",
        lambda _: ([lead()], {"manifest_sha256": "a" * 64, "commit": DEFAULT_COMMIT, "capture_count": 2}),
    )
    main(
        [
            "reconcile",
            "--retained-dir",
            str(tmp_path / "retained"),
            "--catalog",
            str(catalog),
            "--aliases",
            str(aliases),
            "--output",
            str(output),
            "--imported-at",
            NOW,
        ]
    )
    data = json.loads(output.read_text())
    assert data["counts"] == {"matched": 1}
    assert data["input_pins"]["catalog_sha256"]
    assert catalog.read_bytes() == prior


@pytest.fixture
def retained_capture(tmp_path):
    """Synthetic private files exercise the installed provider's replay boundary."""
    from spicy_docs.sources.scorecards.govtrack_discovery import commit_url, metadata_url, tree_url

    root = "a" * 40
    path = "scorecards/Example.yaml"
    body = b"name: Example\nhomepage: https://example.org/\nlink: https://example.org/scorecard\nupdated: 2020-01-01\nbased-on: 116th\ntype: percent\n...\n99,98,PRIVATE_RATING\n"
    entry = {
        "path": path,
        "type": "blob",
        "mode": "100644",
        "size": len(body),
        "sha": hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest(),
    }
    values = [
        (
            "commit",
            commit_url(discovery.DEFAULT_COMMIT),
            json.dumps({"sha": discovery.DEFAULT_COMMIT, "tree": {"sha": root}}).encode(),
            "application/json",
        ),
        (
            "tree",
            tree_url(root),
            json.dumps({"sha": root, "truncated": False, "tree": [entry]}).encode(),
            "application/json",
        ),
        (path, metadata_url(discovery.DEFAULT_COMMIT, path), body, "text/plain"),
    ]
    rows = []
    for i, (key, url, raw, mime) in enumerate(values):
        filename = f"{i:04d}.body"
        (tmp_path / filename).write_bytes(raw)
        rows.append(
            {
                "key": key,
                "body_file": filename,
                "requested_url": url,
                "resolved_url": url,
                "status_code": 200,
                "content_type": mime,
                "observed_at": "2026-10-03T20:00:00Z",
                "content_encoding": "identity",
                "method": "GET",
                "sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
            }
        )
    manifest = {
        "schema_version": 1,
        "commit": discovery.DEFAULT_COMMIT,
        "status": "complete",
        "captures": rows,
        "body_retained_publicly": False,
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path, manifest


def test_retained_replay_reads_metadata_without_rating_tail(retained_capture):
    directory, _ = retained_capture
    leads, pin = discovery.load_retained(directory)
    assert len(leads) == 1
    assert leads[0]["name"] == "Example"
    assert pin["capture_count"] == 3
    assert "PRIVATE_RATING" not in json.dumps(leads)


@pytest.mark.parametrize("change", ["failed", "missing", "hash", "redirect", "symlink", "tree_pin"])
def test_corrupt_retained_run_refuses_without_replacing_review(retained_capture, change, tmp_path):
    from spicy_docs.sources.scorecards.govtrack_discovery import GovTrackDiscoveryError

    directory, manifest = retained_capture
    if change == "failed":
        manifest["status"] = "failed"
    elif change == "missing":
        manifest["captures"].pop()
    elif change == "hash":
        (directory / "0002.body").write_bytes(b"changed")
    elif change == "redirect":
        manifest["captures"][-1]["resolved_url"] = "https://example.org/redirect"
    elif change == "symlink":
        (directory / "0002.body").unlink()
        (directory / "0002.body").symlink_to(directory / "0001.body")
    else:
        manifest["captures"][1]["requested_url"] = manifest["captures"][1]["requested_url"].replace("a" * 40, "b" * 40)
    (directory / "manifest.json").write_text(json.dumps(manifest))
    output = tmp_path.parent / f"review-{change}.json"
    output.write_text("prior review")
    with pytest.raises((discovery.DiscoveryReconciliationError, GovTrackDiscoveryError)):
        discovery.main(
            [
                "reconcile",
                "--retained-dir",
                str(directory),
                "--catalog",
                str(tmp_path / "never-read-catalog.json"),
                "--aliases",
                str(tmp_path / "never-read-aliases.json"),
                "--output",
                str(output),
            ]
        )
    assert output.read_text() == "prior review"


def test_catalog_multiple_series_share_publisher_without_losing_source_status():
    first = source() | {"source_id": "existing:annual", "scorecard_name": "Annual", "discovery_status": "supported"}
    second = source() | {"source_id": "existing:lifetime", "scorecard_name": "Lifetime", "discovery_status": "profiled"}
    (row,) = run(sources=[first, second])["results"]
    assert row["candidate_publisher_ids"] == ["existing"]
    assert row["catalog_status_at_import"] is None
    assert row["catalog_sources_at_import"] == [
        {"source_id": "existing:annual", "scorecard_name": "Annual", "discovery_status": "supported"},
        {"source_id": "existing:lifetime", "scorecard_name": "Lifetime", "discovery_status": "profiled"},
    ]
    with pytest.raises(DiscoveryReconciliationError, match="source IDs"):
        run(sources=[first, first])


def test_acquisition_failure_retains_metadata_separately_without_error_or_body_bytes(tmp_path, monkeypatch):
    from spicy_docs.sources.scorecards import govtrack_discovery as provider
    from spicy_docs.transport.captured import CapturedBodyResponse, attach_capture

    url = provider.metadata_url(DEFAULT_COMMIT, "scorecards/Example.yaml")
    error = provider.GovTrackDiscoveryError("PRIVATE_ERROR_DETAIL")
    body = b"PRIVATE_REFUSED_BODY"
    attach_capture(error, CapturedBodyResponse(url, url, 404, "text/plain", NOW, body))
    vars(error)["govtrack_discovery"] = {
        "commit": DEFAULT_COMMIT,
        "path": "scorecards/Example.yaml",
        "requestCount": 3,
        "unknown": "PRIVATE_CONTEXT",
    }

    class FailingAcquirer:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def acquire(self, **_):
            raise error

    monkeypatch.setattr(provider, "GovTrackDiscoveryAcquirer", FailingAcquirer)
    with pytest.raises(provider.GovTrackDiscoveryError):
        discovery.acquire(tmp_path)
    raw = (tmp_path / "manifest.json").read_text()
    manifest = json.loads(raw)
    assert manifest["status"] == "failed"
    assert manifest["captures"] == []
    failure = manifest["failed_attempt"]
    assert failure["http_status"] == 404
    assert failure["requested_url"] == url
    assert failure["sha256"] == "sha256:" + hashlib.sha256(body).hexdigest()
    assert failure["context"]["request_count"] == 3
    assert failure["body_retained"] is False
    assert "PRIVATE_" not in raw
    assert not list(tmp_path.glob("*.body"))


def test_access_refusal_metadata_uses_shared_bounded_receipt_without_inventing_status():
    from spicy_docs.reading.refusals import RefusedResponse, attach_refused_response
    from spicy_docs.sources.scorecards.govtrack_discovery import commit_url
    from spicy_docs.transport.credentials import CredentialRefusedError

    error = CredentialRefusedError("PRIVATE_EXCEPTION")
    attach_refused_response(
        error,
        RefusedResponse(commit_url(DEFAULT_COMMIT), "transport", b"PRIVATE_BODY", "text/plain", "access-refused", 12),
    )
    failure = discovery._failure_metadata(error, commit=DEFAULT_COMMIT)
    assert failure["byte_size"] == 12
    assert "http_status" not in failure  # Shared receipt does not carry 401 versus 403.
    assert "PRIVATE" not in json.dumps(failure)
