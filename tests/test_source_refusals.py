"""A refused Table III bulk read is loud: the laws family still publishes, and the nightly check fails on it.

End to end over the real pieces: ``build_laws`` journals the refusal, the rollup's
generation and its source evidence are published into an in-memory bucket, and
``scripts/check_source_refusals.py`` reads them back through ``publication``'s
own digest-checked readers. The only seam is the HTTP GET, served from the bucket.
"""

from __future__ import annotations

import httpx
import pyarrow.parquet as pq
import pytest

from scripts import check_source_refusals as check
from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_incremental_rollups import no_download
from tests.test_laws import LISTED_119, MOVED, StubListingReader, StubOlrc, StubUslm, _without

BASE = "https://data.test"


def _published(tmp_path, monkeypatch, olrc, *, after=None, retain=True):
    """Run the laws build with ``olrc``, publish its generation and evidence, and serve the bucket at ``BASE``.

    ``after`` is an OLRC stub for an earlier run whose outputs this run takes as its published priors;
    ``retain=False`` publishes the generation without its source evidence.
    """
    import shutil

    from spicy_regs.transforms.build_laws import build_laws

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    build = tmp_path / "build"
    build.mkdir()

    def run(source, evidence=None):
        return build_laws(build, reader=StubListingReader({119: LISTED_119}),
                          uslm=StubUslm(unavailable={110, 109, 104}), olrc=source, download_prior=no_download,
                          evidence=evidence)

    if after is not None:
        for path in run(after):
            shutil.copyfile(path, build / f"_{path.stem}_prior.parquet")
    evidence = CaptureEvidence(tmp_path, "laws")
    evidence.inherit(pub.empty_index(), public_url=None)
    outputs = run(olrc, evidence)
    directory = tmp_path / "generation"
    build_generation(directory, family="laws", files=list(outputs), expected_keys=[p.name for p in outputs],
                     schemas=expected_schemas(), inputs=evidence.inputs() if retain else ())
    store = Store()
    pub.publish_generation(directory, client=store, bucket="test", prior_index=pub.empty_index(),
                           evidence_directories=(evidence.artifact_dir,) if retain else ())

    def get(url, *, allow_missing, headers=None, limit=pub.INDEX_LIMIT):
        key = url.removeprefix(BASE + "/")
        if key in store.objects:
            return store.objects[key]
        if allow_missing:
            return None
        raise httpx.HTTPStatusError("404", request=httpx.Request("GET", url), response=httpx.Response(404))

    monkeypatch.setattr(pub, "_bounded_get", get)
    return {path.name: pq.read_table(path).num_rows for path in outputs}


@pytest.mark.parametrize(
    ("olrc", "reason", "table3_rows"),
    [
        (StubOlrc(member_name="table3_xml_bulk.xml"), "file-refused", 112),  # the 2025-08 member's name
        (StubOlrc(bulk=MOVED, release_point="119-60"), "release-point-regressed", 112),
        (StubOlrc(bulk=_without(MOVED, b"41c326fe")), "acts-dropped-without-release-point-advance", 112),
        (StubOlrc(bulk=MOVED), None, 112),
    ],
    ids=["renamed-member", "release-point-regressed", "acts-dropped", "a-clean-correction"],
)
def test_a_refused_bulk_read_fails_the_nightly_check_and_blocks_nothing_else(tmp_path, monkeypatch, capsys, olrc,
                                                                               reason, table3_rows):
    """Each refusal leaves the prior Table III rows standing, publishes laws, and fails the check; a clean read passes."""
    rows = _published(tmp_path, monkeypatch, olrc, after=StubOlrc())
    assert rows == {"laws.parquet": 4, "law_code_sections.parquet": 9, "table3_records.parquet": table3_rows}
    assert check.main(["--base-url", BASE]) == (1 if reason else 0)
    out = capsys.readouterr().out
    if reason:
        assert "REFUSED: laws generation" in out and f'"reason": "{reason}"' in out
        if reason == "file-refused":
            assert "is not a fulldump file" in out, "the reader's own reason reaches the alert"
    else:
        assert "OK: laws generation" in out and "No current generation journals a refused source read." in out


def test_a_transport_failure_is_retried_next_run_not_alerted(tmp_path, monkeypatch, capsys):
    _published(tmp_path, monkeypatch, StubOlrc(bulk=ConnectionError("stub: incomplete chunked read")))
    assert check.main(["--base-url", BASE]) == 0


def test_a_family_that_cannot_show_its_refusals_fails(tmp_path, monkeypatch):
    _published(tmp_path, monkeypatch, StubOlrc(), retain=False)
    with pub.snapshot(BASE) as index:
        failures = check.refusals(BASE, index, {"laws": ("table3-bulk-refused",), "absent": ("x",)})
    assert failures[0].startswith("laws: generation sha256:") and failures[0].endswith("retained no source evidence")
    assert failures[1] == "absent: no published generation, so its source refusals cannot be read"
    assert len(failures) == 2
