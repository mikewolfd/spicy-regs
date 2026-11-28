"""Runtime parity proof for retiring per-row receipts on scorecards and scorecard-analysis (ss-vse3)."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_policy_registry, subject_catalog
from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, write_dataset
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
from spicy_regs.receipt_switch import receipt_files, receipt_free_prior, table_differences
from spicy_regs.scorecards.etl import (
    ANALYSIS_NAMES,
    EARLIER_POLICIES,
    POLICIES,
    SOURCE_NAMES,
    generation_schemas,
)
from spicy_regs.scorecards.subject_shapes import IDENTITIES, SOURCE_COLUMNS, map_source_row
from spicy_regs.sources import publication as pub
from spicy_regs.transforms.build_scorecards import build_scorecards
from tests.generation_fakes import Store
from tests.test_scorecard_analysis import _inputs, _serve
from tests.test_scorecard_member_corrections import EDITION as DOMENICI_EDITION, SOURCE as DOMENICI_SOURCE
from tests.test_scorecard_refresh import NOW, capture, edition, evidence, no_fetch
from tests.test_scorecard_slice_carry import _combine_multi, _multi_publisher_provider, _multi_publisher_registry, _rich_bundle

pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def _write_legacy_v2_receipted_scorecards(directory: Path, bundle: dict[str, list[dict]], generation_id: str) -> tuple[list[Path], Path, list]:
    """Write a legacy scorecards generation under scorecard_items v2 (9-field references) + receipts."""
    v2_items = next(p for p in EARLIER_POLICIES["scorecard_items"] if p.policy_version == "scorecards-etl-v2")
    policies = [v2_items if name == "scorecard_items" else POLICIES[name] for name in SOURCE_NAMES]
    subjects, receipt_parts = [], []
    for pol in policies:
        records = []
        for idx, raw in enumerate(bundle[pol.dataset]):
            mapped = map_source_row(pol.dataset, raw)
            if pol.dataset == "scorecard_items" and mapped.get("references") is not None:
                mapped["references"] = [{k: v for k, v in ref.items() if k != "source_path"} for ref in mapped["references"]]
            ctx = ReceiptContext(
                generation_id,
                f"{pol.dataset}:{idx}",
                pol.policy_version,
                [{"source_id": raw["capture_id"], "body_version": "capture:" + raw["capture_id"]}],
            )
            records.append((mapped, ctx))
        subject, receipt = write_dataset(records, directory / pol.dataset, pol)
        assert subject is not None
        subjects.append(subject)
        receipt_parts.append(receipt)
    receipts = combine_receipts(receipt_parts, directory / "etl_receipts.parquet")
    return subjects, receipts, policies


def test_scorecards_cutover_and_plain_switch_parity_across_all_11_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prove one-time cutover upgrades v2 scorecard_items references.source_path and steady-state matches full rebuild."""
    reg_path = _multi_publisher_registry(tmp_path, ("lcv", "afscme"))
    editions = {
        "lcv:2025": edition("2025", publisher="lcv"),
        "afscme:2025": edition("2025", publisher="afscme"),
    }
    lcv_bundle = _rich_bundle(editions["lcv:2025"], "cap-lcv")
    afscme_bundle = _rich_bundle(editions["afscme:2025"], "cap-afscme")
    afscme_bundle["scorecard_items"][0]["references_json"] = json.dumps(
        [
            {
                "occurrence_id": "0",
                "citation_text": "H.R. 1",
                "kind": "bill",
                "source_path": "/billVotes/0/stateBillID",
                "congress_text": "119",
                "bill_citation_text": "H.R. 1",
            }
        ]
    )
    initial = _combine_multi(lcv_bundle, afscme_bundle)

    # Publish a v2 receipted prior (like live 2e5beb4b...) by temporarily unretiring scorecards.
    legacy_dir = tmp_path / "legacy-v2"
    subjects, receipts, v2_policies = _write_legacy_v2_receipted_scorecards(legacy_dir, initial, "legacy-v2-gen")
    unretired = subject_catalog.retired_receipt_datasets() - set(SOURCE_NAMES)
    v2_installed = dict(etl_policy_registry.installed_policies())
    for pol in v2_policies:
        v2_installed[pol.dataset] = pol
    with monkeypatch.context() as patch:
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: unretired)
        patch.setattr(etl_policy_registry, "installed_policies", lambda: v2_installed)
        gen_v2 = tmp_path / "gen-v2"
        build_generation(
            gen_v2,
            family="scorecards",
            files=subjects,
            expected_keys=[p.name for p in subjects],
            receipt_path=receipts,
            receipt_policies=v2_policies,
            receipt_generation_id="legacy-v2-gen",
        )
        store = Store()
        index_v2 = pub.publish_generation(gen_v2, client=store, bucket="test", prior_index=pub.empty_index())

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)

    def dl_v2(key, target):
        return fetch(None, pub.single_member(index_v2, key), target)

    def dl_v2_receipts(target):
        return fetch(None, pub.receipt_members(index_v2, dataset="scorecards")[0], target)

    # 1. Cutover run against v2 receipted prior: upgrades untouched afscme scorecard_items.references.source_path!
    ev_cutover = evidence(tmp_path / "cutover-ev")
    ev_cutover.read_snapshot = index_v2
    cutover_dir = tmp_path / "cutover-build"
    p_refresh = _multi_publisher_provider(editions, {"lcv:2025": lambda sel, cap: _rich_bundle(sel, cap, members=3)})
    build_scorecards(
        cutover_dir,
        evidence=ev_cutover,
        registry=reg_path,
        publishers=("lcv",),
        force=True,
        provider=p_refresh,
        fetch_factory=no_fetch,
        download_prior=dl_v2,
        download_prior_receipts=dl_v2_receipts,
        now=NOW,
    )
    assert receipt_files(cutover_dir) == []
    items_rows = pq.read_table(cutover_dir / "scorecard_items.parquet").to_pylist()
    afscme_item = next(r for r in items_rows if r["scorecard_id"] == "afscme:2025")
    assert afscme_item["references"][0]["source_path"] == "/billVotes/0/stateBillID"

    # 2. Steady-state receipt-free prior parity across ALL 11 tables (receipt_switch.table_differences == []).
    plain_prior_copy = receipt_free_prior(cutover_dir, tmp_path / "receipt-free-prior")
    assert receipt_files(plain_prior_copy) == []
    gen_plain = tmp_path / "gen-plain"
    build_generation(
        gen_plain,
        family="scorecards",
        files=tuple(plain_prior_copy / f"{n}.parquet" for n in SOURCE_NAMES),
        expected_keys=tuple(f"{n}.parquet" for n in SOURCE_NAMES),
        schemas=generation_schemas(SOURCE_NAMES),
    )
    plain_store = Store()
    index_plain = pub.publish_generation(gen_plain, client=plain_store, bucket="test", prior_index=pub.empty_index())

    def dl_plain(key, target):
        target.write_bytes(plain_store.objects[pub.single_member(index_plain, key).path])
        return True

    fixed_snap = "scorecard-snapshot:parity-fixed"

    def fixed_provider():
        base = _multi_publisher_provider(editions, {"lcv:2025": lambda sel, cap: _rich_bundle(sel, cap, members=5)})

        class FixedAdapter:
            parser_version = "test-v1"

            def __init__(self, pid):
                self._inner = base.get_adapter(pid)

            def list_scorecards(self, ctx):
                return self._inner.list_scorecards(ctx)

            def acquire_scorecard(self, sel, ctx):
                rec = ctx.capture(capture(b"FIXED"), stage="test")
                rows = _rich_bundle(sel, "fixed-cap", members=5)
                for group in rows.values():
                    for r in group:
                        if "snapshot_id" in r:
                            r["snapshot_id"] = fixed_snap
                        r["capture_id"] = rec["capture_id"]
                rows["scorecard_snapshots"][0]["capture_ids_json"] = json.dumps([rec["capture_id"]])
                rows["scorecard_snapshots"][0]["capture_roles_json"] = json.dumps(
                    [{"capture_id": rec["capture_id"], "role": "primary", "field_groups": ["all"]}]
                )
                return SimpleNamespace(
                    scorecard_id=sel.scorecard_id,
                    snapshot_id=fixed_snap,
                    tables=rows,
                    capture_ids=(rec["capture_id"],),
                    completeness_rule="Synthetic complete member table",
                )

        return SimpleNamespace(
            contracts=base.contracts,
            get_adapter=lambda name: FixedAdapter(name),
            validate=base.validate,
            context=base.context,
            refusal=base.refusal,
        )

    from uuid import UUID

    def make_uuids():
        n = 0

        def nxt():
            nonlocal n
            n += 1
            return UUID(int=n)

        return nxt

    for label, readback_cb in (("full", lambda _p, _c: None), ("slice", None)):
        with monkeypatch.context() as patch:
            patch.setattr("spicy_regs.source_evidence.uuid4", make_uuids())
            ev = evidence(tmp_path / f"ev-{label}")
            ev.read_snapshot = index_plain
            build_scorecards(
                tmp_path / f"build-{label}",
                evidence=ev,
                registry=reg_path,
                publishers=("lcv",),
                force=True,
                provider=fixed_provider(),
                fetch_factory=no_fetch,
                download_prior=dl_plain,
                now=NOW,
                validate_readback=readback_cb,
            )

    assert receipt_files(tmp_path / "build-full", tmp_path / "build-slice") == []
    for name in SOURCE_NAMES:
        assert (
            table_differences(
                tmp_path / "build-slice" / f"{name}.parquet",
                tmp_path / "build-full" / f"{name}.parquet",
                key=IDENTITIES[name],
            )
            == []
        ), name


def test_scorecard_analysis_plain_switch_parity_with_receiptless_scorecards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prove scorecard-analysis builds identically with source_receipt_path=None across all 3 analysis tables."""
    context = dict(
        scorecard_id="lcv:2025",
        snapshot_id="s1",
        capture_id="c1",
        source_url="https://publisher.example/2025",
        source_path="/billVotes/0",
    )
    ref_json = json.dumps(
        [
            {
                "occurrence_id": "0",
                "citation_text": "H.R. 1",
                "kind": "bill",
                "source_path": "/billVotes/0/stateBillID",
                "congress_text": "119",
                "bill_citation_text": "H.R. 1",
            }
        ]
    )
    overrides = {
        "scorecards": [context | dict(publisher_id="lcv", year_text="2025"), deepcopy(DOMENICI_EDITION)],
        "scorecard_members": [
            context
            | dict(
                publisher_member_key="p1",
                member_name="Alex Example",
                identifiers_json='[{"scheme":"bioguide","value":"X000001"}]',
                chamber_text="House",
            ),
            deepcopy(DOMENICI_SOURCE),
        ],
        "scorecard_items": [
            context
            | dict(
                item_id="i1",
                item_kind_text="Scorecard Vote",
                item_date_text="2025",
                congress_text="119",
                chamber_text="House",
                session_text="1",
                roll_number_text="24",
                publisher_position_text="No",
                references_json=ref_json,
            )
        ],
        "scorecard_member_item_results": [
            context
            | dict(
                item_id="i1",
                publisher_member_key="p1",
                result_id="reported",
                metric_id="annual",
                participation_id="scorecard",
                result_text="pro",
            )
        ],
        "scorecard_metric_items": [
            context
            | dict(
                item_id="i1",
                metric_id="annual",
                participation_id="scorecard",
                publisher_position_text="No",
            )
        ],
        "members": [
            {"bioguide_id": "X000001", "name_first": "Alex", "name_last": "Example"},
            {"bioguide_id": "D000407", "name_first": "Pete", "name_last": "Domenici"},
        ],
        "member_terms": [
            {
                "bioguide_id": "X000001",
                "term_index": "0",
                "term_type": "rep",
                "term_start": "2025-01-03",
                "term_end": "2027-01-03",
                "term_state": "CA",
                "term_district": "1",
            },
            {
                "bioguide_id": "D000407",
                "term_index": "3",
                "term_type": "sen",
                "term_start": "1991-01-03",
                "term_end": "1997-01-03",
                "term_state": "NM",
                "term_district": None,
            },
        ],
        "roll_call_votes": [{"vote_id": "119-house-1-24", "vote_date": "29-Jan-2025"}],
        "member_votes": [
            {
                "vote_id": "119-house-1-24",
                "member_key": "X000001",
                "bioguide_id": "X000001",
                "position": "Nay",
                "position_normalized": "nay",
            }
        ],
    }

    # 1. Plain receiptless route (scorecards has NO etlReceipts -> source_receipt_path=None).
    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()
    plain_store, plain_index = _inputs(plain_dir, row_overrides=overrides)
    _serve(monkeypatch, plain_store)
    plain_stage = tmp_path / "stage-plain"
    plain_stage.mkdir()
    plain_out = tmp_path / "analysis-plain"
    plain_out.mkdir()
    plain_rollup = ScorecardAnalysisRollup(output_dir=plain_out)
    plain_rollup._prime(plain_stage, plain_index)
    assert plain_rollup._scorecard_source_receipt is None
    plain_files = plain_rollup.build(plain_out)
    assert receipt_files(plain_out) == []
    gen_analysis = tmp_path / "gen-analysis-plain"
    art_analysis = build_generation(
        gen_analysis,
        family="scorecard-analysis",
        files=plain_files,
        expected_keys=[p.name for p in plain_files],
        schemas=generation_schemas(ANALYSIS_NAMES),
    )
    assert receipt_files(gen_analysis) == []
    assert "etlReceipts" not in art_analysis.root["spec"]

    # 2. Receipted source route (scorecards WITH receipts -> source_receipt_path!=None).
    sc_names = (
        "scorecards",
        "scorecard_members",
        "scorecard_items",
        "scorecard_member_item_results",
        "scorecard_metric_items",
        "scorecard_methodologies",
    )
    sc_rows = {
        name: [{col: row.get(col) for col in SOURCE_COLUMNS[name]} for row in overrides.get(name, [])]
        for name in sc_names
    }
    rec_src = tmp_path / "receipted-src"
    subjects, receipt_parts = [], []
    for name in sc_names:
        pol = POLICIES[name]
        recs = [
            (
                map_source_row(name, raw),
                ReceiptContext(
                    "src-rec-gen",
                    f"{name}:{idx}",
                    pol.policy_version,
                    [{"source_id": str(raw["capture_id"]), "body_version": f"capture:{raw['capture_id']}"}],
                ),
            )
            for idx, raw in enumerate(sc_rows[name])
        ]
        subj, rec = write_dataset(recs, rec_src / name, pol)
        assert subj is not None
        subjects.append(subj)
        receipt_parts.append(rec)
    combined_rec = combine_receipts(receipt_parts, rec_src / "etl_receipts.parquet")
    rec_store = Store()
    rec_store.objects.update(plain_store.objects)
    unretired = subject_catalog.retired_receipt_datasets() - set(sc_names)
    with monkeypatch.context() as patch:
        patch.setattr(subject_catalog, "retired_receipt_datasets", lambda: unretired)
        gen_rec = tmp_path / "gen-rec-scorecards"
        build_generation(
            gen_rec,
            family="scorecards",
            files=subjects,
            expected_keys=[p.name for p in subjects],
            receipt_path=combined_rec,
            receipt_policies=[POLICIES[n] for n in sc_names],
            receipt_generation_id="src-rec-gen",
        )
        base_idx = deepcopy(plain_index)
        base_idx["families"].pop("scorecards")
        rec_store.objects[pub.INDEX_V2_KEY] = json.dumps(base_idx).encode()
        rec_index = pub.publish_generation(gen_rec, client=rec_store, bucket="test", prior_index=base_idx)

    _serve(monkeypatch, rec_store)
    rec_out = tmp_path / "analysis-receipted"
    rec_out.mkdir()
    rec_rollup = ScorecardAnalysisRollup(output_dir=rec_out)
    rec_rollup._prime(rec_out, rec_index)
    assert rec_rollup._scorecard_source_receipt is not None
    rec_rollup.build(rec_out)

    # Exact parity across all 3 scorecard-analysis tables!
    for name in ANALYSIS_NAMES:
        assert (
            table_differences(
                plain_out / f"{name}.parquet",
                rec_out / f"{name}.parquet",
                key=IDENTITIES[name],
            )
            == []
        ), name

    # Spot-check both critical invariants: per-reference source_path AND Domenici correction!
    ref_link = next(
        r for r in pq.read_table(plain_out / "scorecard_item_links.parquet").to_pylist() if r["reference_id"] == "reference:0"
    )
    assert ref_link["source_path"] == "/billVotes/0/stateBillID"
    domenici_link = next(
        r
        for r in pq.read_table(plain_out / "scorecard_member_links.parquet").to_pylist()
        if r["scorecard_id"] == "lugar_mccourt:103-senate-html"
    )
    assert domenici_link["bioguide_id"] == "D000407"
    assert domenici_link["state_status"] == "corrected_from_reviewed_evidence"
    comp = pq.read_table(plain_out / "scorecard_action_comparisons.parquet").to_pylist()
    assert len(comp) == 1 and comp[0]["comparison_status"] == "same"
