"""End-to-end acquisition, normalization, arithmetic reconciliation, and MCP serving for US-CBO/eval-projections."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from spicy_docs.sources.cbo_eval_projections import (
    ALL_FILES,
    INPUT_FILES,
    OUTPUT_FILES,
    PINNED_COMMIT,
    CboEvalProjectionsRefused,
    parse_eval_bundle,
    raw_url,
)

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.generations import verify_generation
from spicy_regs.local_publication import LOCAL_FAMILIES, _family_schemas
from spicy_regs.pipelines import cbo_eval_projections as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_cbo_eval_projections import SCHEMAS, build_cbo_eval_projections, interpret_bundle
from tests.generation_fakes import Store
from tests.published_paths import local_paths

FIXTURES = Path(__file__).parent / "fixtures" / "cbo_eval_projections"


def _fixture_bytes() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for rel in (*INPUT_FILES, *OUTPUT_FILES):
        files[rel] = (FIXTURES / rel).read_bytes()
    files["LICENSE.md"] = (FIXTURES / "LICENSE.md").read_bytes()
    files["README.md"] = b"# Evaluating CBO's Projections of Components of the Federal Budget\n"
    files["CHANGE_LOG.md"] = b"# Change Log\n"
    return files


def _fixture_bundle():
    raw = {rel: (FIXTURES / rel).read_bytes() for rel in (*INPUT_FILES, *OUTPUT_FILES)}
    return parse_eval_bundle(raw, commit=PINNED_COMMIT, observed_at="2026-11-27T13:00:00+00:00")


def test_family_registration_and_schema_parity() -> None:
    assert pipeline.FAMILY in LOCAL_FAMILIES
    assert _family_schemas(pipeline.FAMILY) == SCHEMAS
    expected = data_dictionary.expected_schemas()
    for table in SCHEMAS:
        assert table in expected
        assert table in mcp_server.TABLES


def test_build_normalizes_all_tables_and_preserves_exclusions(tmp_path: Path) -> None:
    bundle = _fixture_bundle()
    tables = interpret_bundle(bundle, require_full_reconciliation=False)
    assert set(tables) == set(SCHEMAS)

    actuals = {(r["component"], r["subcategory"], r["fiscal_year"]): r for r in tables["cbo_eval_actuals"]}
    assert actuals[("outlay", "Fannie Freddie", 2025)]["excluded_from_error_analysis"] is True
    assert actuals[("outlay", "Student Loan Foregiveness", 2022)]["excluded_from_error_analysis"] is True
    assert actuals[("outlay", "Total", 2025)]["excluded_from_error_analysis"] is False

    changes = [
        r
        for r in tables["cbo_eval_baseline_changes"]
        if r["changes_baseline_date"].isoformat() == "2024-12-01"
        and r["subcategory"] == "Net Interest"
        and r["change_category"] == "Legislative"
        and r["projected_fiscal_year"] == 2024
    ]
    assert len(changes) == 1 and changes[0]["change_value_billions"] is None

    written = build_cbo_eval_projections(bundle, tmp_path / "out", require_full_reconciliation=False)
    assert len(written) == len(SCHEMAS)


def test_math_reconciliation_and_duplicate_key_refusals() -> None:
    bundle = _fixture_bundle()
    bad_adj = deepcopy(bundle)
    bad_adj_rows = list(bad_adj.projection_errors)
    first = bad_adj_rows[0]
    bad_adj_rows[0] = type(first)(
        **{
            **{k: getattr(first, k) for k in first.__dataclass_fields__},
            "adjusted_projection": first.adjusted_projection + 1.0,
        }
    )
    object.__setattr__(bad_adj, "projection_errors", tuple(bad_adj_rows))
    with pytest.raises(ValueError, match="adjusted_projection"):
        interpret_bundle(bad_adj, require_full_reconciliation=False)

    bad_dup = deepcopy(bundle)
    object.__setattr__(bad_dup, "actuals", (*bundle.actuals, bundle.actuals[0]))
    with pytest.raises(ValueError, match="Duplicate primary key"):
        interpret_bundle(bad_dup, require_full_reconciliation=False)


def test_acquire_build_publication_mcp_pub_61916_spot_checks_and_replay(tmp_path: Path, monkeypatch) -> None:
    fixtures = _fixture_bytes()
    url_to_rel = {raw_url(PINNED_COMMIT, rel): rel for rel in ALL_FILES}
    requested: list[str] = []

    def fetch(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        rel = url_to_rel[str(request.url)]
        return httpx.Response(200, stream=httpx.ByteStream(fixtures[rel]), headers={"Content-Type": "text/plain"})

    selection = pipeline.acquire(
        tmp_path / "acquire",
        commit=PINNED_COMMIT,
        transport=httpx.MockTransport(fetch),
    )
    assert len(requested) == len(ALL_FILES)

    output = tmp_path / "candidate"
    artifact, evidence = pipeline.build(
        selection,
        output,
        require_full_reconciliation=False,
    )
    assert verify_generation(output / "generation").pin == artifact.pin

    store = Store()
    index = publication.publish_generation(
        output / "generation",
        client=store,
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    metadata = build_bundle(index)
    for name in SCHEMAS:
        assert metadata["tables"][name]["sourceStatus"] == "documented"
        assert metadata["tables"][name]["metadataStatus"] == "documented"

    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        fy2025 = connection.execute(
            """
            SELECT component, subcategory, projected_value_billions, actual_value_billions,
                   projection_error_billions, projection_error_pct_actual, projection_error_pct_gdp
            FROM cbo_projection_errors
            WHERE baseline_date = DATE '2024-06-01' AND projected_fiscal_year = 2025
            ORDER BY component, subcategory
            """
        ).fetchall()
        by_key = {(r[0], r[1]): r[2:] for r in fy2025}
        assert by_key[("revenue", "Total")] == pytest.approx((5037.628, 5234.616, -334.303, -6.386, -1.101))
        assert by_key[("revenue", "Customs Duties")] == pytest.approx((76.187, 194.866, -118.679, -60.903, -0.391))
        assert by_key[("outlay", "Total")] == pytest.approx((6975.419, 7016.386, -120.614, -1.719, -0.397))
        assert by_key[("deficit", "Total")] == pytest.approx((-1937.791, -1781.770, 213.689, None, 0.704))
        assert by_key[("debt", "Total")] == pytest.approx((30187.839, 30172.402, 73.106, None, 0.241))

    # Exact offline replay
    replay = tmp_path / "replay"
    mirror = tmp_path / "mirror"
    pinned_index = mirror / "publication.v2/index.json"
    pinned_index.parent.mkdir(parents=True)
    pinned_index.write_text(json.dumps(index))
    prior = mirror / index["families"][pipeline.FAMILY]["prefix"] / "artifact.json"
    prior.parent.mkdir(parents=True)
    prior.write_bytes((output / "generation/artifact.json").read_bytes())
    _, replay_evidence = pipeline.build(
        selection,
        replay,
        read_index=pinned_index,
        require_full_reconciliation=False,
    )
    for name in SCHEMAS:
        assert (output / "generation" / f"{name}.parquet").read_bytes() == (
            replay / "generation" / f"{name}.parquet"
        ).read_bytes()
    assert local_paths(output / "generation", evidence, also=(str(tmp_path),)) == []
    publication.publish_generation(
        replay / "generation",
        client=store,
        bucket="fixture",
        prior_index=index,
        evidence_directories=(replay_evidence,),
    )


def test_wrong_pin_and_refusal_controls(tmp_path: Path) -> None:
    fixtures = _fixture_bytes()
    url_to_rel = {raw_url(PINNED_COMMIT, rel): rel for rel in ALL_FILES}
    selection = pipeline.acquire(
        tmp_path / "acquire",
        commit=PINNED_COMMIT,
        transport=httpx.MockTransport(
            lambda req: httpx.Response(
                200,
                stream=httpx.ByteStream(fixtures[url_to_rel[str(req.url)]]),
                headers={"Content-Type": "text/plain"},
            )
        ),
    )
    bad_selection = deepcopy(selection)
    bad_selection["files"]["input_data/actual_GDP.csv"]["byte_size"] += 1
    refused = tmp_path / "refused"
    with pytest.raises(ValueError, match="selected bytes/digest"):
        pipeline.build(bad_selection, refused, require_full_reconciliation=False)
    assert not (refused / "generation").exists()

    with pytest.raises(CboEvalProjectionsRefused):
        pipeline.acquire(
            tmp_path / "failed_403",
            commit=PINNED_COMMIT,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    403, stream=httpx.ByteStream(b"denied"), headers={"Content-Type": "text/plain"}
                )
            ),
        )
    outcomes = list((tmp_path / "failed_403").glob("source-evidence/*/run-outcome.json"))
    assert outcomes and json.loads(outcomes[0].read_bytes())["outcome"] == "failed"
