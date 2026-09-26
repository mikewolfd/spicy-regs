"""The ETL's dockets and documents publish as managed families from its working copies.

The ETL keeps reading and rewriting the bare objects after every sweep batch;
the families are published once a sweep completes. These pin the two halves of
that split: the publisher and the ETL both read the bare working copy, never
the family, and the refresh publishes the families before any dependent reads.
"""

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



def _working_copy(monkeypatch, frame: pl.DataFrame) -> list[str]:
    read = []

    def working(remote_key, local_path):
        read.append(remote_key)
        frame.write_parquet(local_path)
        return True

    def managed(remote_key, local_path):
        raise AssertionError(f"the family publisher must not read a managed family: {remote_key}")

    monkeypatch.setattr(r2, "download_working_copy", working)
    monkeypatch.setattr(r2, "download", managed)
    return read


def _published_ids(store: Store) -> list[str]:
    location, _ = pub.table_location(pub.parse_index(store.objects[pub.INDEX_KEY]), "dockets.parquet")
    return pl.read_parquet(io.BytesIO(store.objects[location]))["docket_id"].to_list()


def test_each_sweep_republishes_the_working_copy_as_the_family(tmp_path, monkeypatch, remote):
    read = _working_copy(monkeypatch, _dockets(["EPA-1", "EPA-2"]))
    DocketsFamily(output_dir=tmp_path / "first", skip_upload=False).run()
    first = pub.parse_index(remote.objects[pub.INDEX_KEY])["families"]["dockets"]
    assert list(first["tables"]) == ["dockets.parquet"]
    assert _published_ids(remote) == ["EPA-1", "EPA-2"]

    # The next sweep's working copy replaces the family; the publisher still reads the bare copy.
    _working_copy(monkeypatch, _dockets(["EPA-1", "EPA-2", "EPA-3"]))
    DocketsFamily(output_dir=tmp_path / "second", skip_upload=False).run()
    second = pub.parse_index(remote.objects[pub.INDEX_KEY])["families"]["dockets"]
    assert second["artifactDigest"] != first["artifactDigest"]
    assert _published_ids(remote) == ["EPA-1", "EPA-2", "EPA-3"]
    assert read == ["dockets.parquet"]


@pytest.mark.parametrize("ids", [["EPA-1", "EPA-1"], ["EPA-1", None]])
def test_a_working_copy_with_a_repeated_or_missing_id_is_not_published(tmp_path, monkeypatch, remote, ids):
    _working_copy(monkeypatch, _dockets(ids))
    with pytest.raises(RuntimeError, match="lack docket_id"):
        DocketsFamily(output_dir=tmp_path, skip_upload=False).run()
    assert pub.INDEX_KEY not in remote.objects


def test_a_row_group_over_the_admission_bound_is_not_published(tmp_path, monkeypatch, remote):
    from spicy_regs.pipelines.rollups import regulatory_base

    _working_copy(monkeypatch, _dockets(["EPA-1", "EPA-2"]))
    monkeypatch.setattr(regulatory_base, "MAX_ROW_GROUP_BYTES", 1)
    with pytest.raises(RuntimeError, match="exceeds the admission bound"):
        DocketsFamily(output_dir=tmp_path, skip_upload=False).run()
    assert pub.INDEX_KEY not in remote.objects


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
    assert jobs["base-families"]["strategy"]["matrix"]["command"] == ["run-rollup-dockets", "run-rollup-documents"]
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
