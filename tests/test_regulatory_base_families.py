"""Scheduled base refreshes preserve selected native data and receipt evidence."""

import io
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
import yaml

from spicy_regs.pipelines.rollups.regulatory_base import DocketsFamily
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import publication as pub, r2
from tests.generation_fakes import Store

DOCKETS = RECORD_TYPES["dockets"]
REPO_ROOT = Path(__file__).resolve().parents[1]


def _dockets(ids: list[str]) -> pl.DataFrame:
    rows = [{**dict.fromkeys(DOCKETS.schema), "docket_id": i, "agency_code": "EPA"} for i in ids]
    return pl.DataFrame(rows, schema=DOCKETS.schema)


def _publish_source(tmp_path, ids):
    from spicy_regs.pipelines.regulatory_publication import finish_dataset

    root = tmp_path / "source"
    root.mkdir(exist_ok=True)
    path = root / "input.parquet"
    _dockets(ids).write_parquet(path)
    return finish_dataset(root, "dockets", path, publish=True)


def _published_ids(store: Store) -> list[str]:
    location = pub.single_member(pub.parse_index(store.objects[pub.INDEX_KEY]), "dockets.parquet").path
    return pl.read_parquet(io.BytesIO(store.objects[location]))["docket_id"].to_list()


def test_each_sweep_republishes_selected_native_rows_and_receipts(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    _publish_source(tmp_path, ["EPA-1", "EPA-2"])
    DocketsFamily(output_dir=tmp_path / "first", skip_upload=False).run()
    first = pub.parse_index(remote.objects[pub.INDEX_V2_KEY])["families"]["dockets"]
    assert first["etlReceipts"]["datasets"] == ["dockets"]
    assert _published_ids(remote) == ["EPA-1", "EPA-2"]
    _publish_source(tmp_path, ["EPA-1", "EPA-2", "EPA-3"])
    DocketsFamily(output_dir=tmp_path / "second", skip_upload=False).run()
    assert _published_ids(remote) == ["EPA-1", "EPA-2", "EPA-3"]


@pytest.mark.parametrize("ids", [["EPA-1", "EPA-1"], ["EPA-1", None]])
def test_a_repeated_or_missing_native_id_is_not_published(tmp_path, monkeypatch, ids):
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    with pytest.raises((RuntimeError, ValueError)):
        _publish_source(tmp_path, ids)
    assert pub.INDEX_V2_KEY not in remote.objects


def test_a_row_group_over_the_admission_bound_is_not_republished(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install
    from spicy_regs.pipelines.rollups import regulatory_base

    remote = install(monkeypatch)
    _publish_source(tmp_path, ["EPA-1", "EPA-2"])
    before = remote.objects[pub.INDEX_V2_KEY]
    monkeypatch.setattr(regulatory_base, "MAX_ROW_GROUP_BYTES", 1)
    with pytest.raises(RuntimeError, match="exceeds the admission bound"):
        DocketsFamily(output_dir=tmp_path / "republication", skip_upload=False).run()
    assert remote.objects[pub.INDEX_V2_KEY] == before


def test_working_copy_download_ignores_a_family_that_owns_the_key(tmp_path, monkeypatch):
    """The ETL primes from the bare object even when ``dockets.parquet`` resolves to a generation."""
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    requested = []

    @contextmanager
    def stream(method, url, **kwargs):
        requested.append(url)

        class Response:
            status_code = 200

            @staticmethod
            def iter_bytes():
                yield b"bytes"

        yield Response()

    def index(url):
        raise AssertionError("a working-copy read must not consult the publication index")

    monkeypatch.setattr(r2.httpx, "stream", stream)
    monkeypatch.setattr(pub, "current_index", index)
    assert r2.download_working_copy("dockets.parquet", tmp_path / "dockets.parquet")
    assert requested == ["https://example.test/dockets.parquet"]


def test_refresh_publishes_the_families_before_the_mirror_captures_base_versions():
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/_regulations-refresh.yml").read_text())
    jobs = workflow["jobs"]
    assert jobs["base-families"]["strategy"]["matrix"]["command"] == [
        "run-rollup-dockets",
        "run-rollup-documents",
        "run-rollup-docket-attributes",
        "run-rollup-document-attributes",
        "run-rollup-comment-attributes",
    ]
    assert jobs["base-families"]["with"]["skip_upload"] == "${{ inputs.skip_upload }}"
    assert jobs["mirror"]["needs"] == "base-families"
    assert jobs["derived"]["needs"] == "mirror" and jobs["org-links"]["needs"] == "mirror"


def test_refresh_fills_docket_gaps_before_the_families_and_publishes_them_even_if_it_fails():
    jobs = yaml.safe_load((REPO_ROOT / ".github/workflows/_regulations-refresh.yml").read_text())["jobs"]
    gaps = jobs["docket-gaps"]
    assert gaps["steps"][-1]["run"] == "uv run --frozen fill-docket-gaps --no-skip-upload"
    assert {"DATA_GOV_API_KEY", "R2_CATALOG_TOKEN"} <= set(gaps["steps"][-1]["env"])
    assert "!inputs.skip_upload" in gaps["if"]
    assert jobs["base-families"]["needs"] == "docket-gaps"
    assert jobs["base-families"]["if"] == "${{ !cancelled() }}"


def test_the_browser_search_blob_is_built_only_where_its_app_reads_this_bucket():
    """docket_search.json.gz has one reader, the web app; the fork's deployment reads upstream's copy."""
    jobs = yaml.safe_load((REPO_ROOT / ".github/workflows/_regulations-refresh.yml").read_text())["jobs"]
    assert "run-rollup-docket-search" not in jobs["derived"]["strategy"]["matrix"]["command"]
    assert jobs["docket-search"]["with"]["command"] == "run-rollup-docket-search"
    assert "vars.PUBLISH_DOCKET_SEARCH == 'true'" in jobs["docket-search"]["if"]
    assert "docket-search" in jobs["verify"]["needs"]
