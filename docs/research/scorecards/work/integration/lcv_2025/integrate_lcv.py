"""Resolve, locally publish, audit and query one qualified LCV candidate offline."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import importlib.util
import json
from pathlib import Path
import shutil

from mcp.types import CallToolResult

from qualify_lcv import digest, write
from spicy_regs.generation_audit import PublicBase, audit
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.local_data import selection_record
from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
from spicy_regs.scorecards.etl import LINK_NAMES, generation_options, read_family
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecard_analysis import OUTPUTS


def local_test_store():
    """Use the repository's existing no-network S3 fake without package overlays."""
    path = Path(__file__).parents[6] / "tests" / "generation_fakes.py"
    spec = importlib.util.spec_from_file_location("scorecard_qualification_store", path)
    if spec is None or spec.loader is None:
        raise ValueError("The existing local object-store test double is unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Store()


RATING_SQL = """SELECT p.name AS publisher, c.title AS scorecard, m.member_name,
       metrics.name AS metric, r.value_text AS published_value,
       r.source_url AS publisher_url, links.bioguide_id, links.resolution_status
FROM scorecard_member_ratings r
JOIN scorecards c USING (scorecard_id)
JOIN scorecard_publishers p USING (publisher_id)
JOIN scorecard_metrics metrics ON metrics.scorecard_id=r.scorecard_id AND metrics.metric_id=r.metric_id
JOIN scorecard_members m ON m.scorecard_id=r.scorecard_id AND m.publisher_member_key=r.publisher_member_key
JOIN scorecard_member_links links ON links.scorecard_id=r.scorecard_id AND links.publisher_member_key=r.publisher_member_key
WHERE m.publisher_member_id='https://www.lcv.org/moc/katie-britt/' ORDER BY r.metric_id LIMIT 10"""
ITEM_SQL = """SELECT p.name AS publisher, i.title AS item, i.item_date_text,
       i.publisher_position_text AS publisher_target, i.position_basis,
       i.source_url AS publisher_url, l.vote_id, l.resolution_status
FROM scorecard_items i JOIN scorecards c USING(scorecard_id)
JOIN scorecard_publishers p USING(publisher_id)
JOIN scorecard_item_links l USING(scorecard_id,item_id)
WHERE i.item_id='36294' LIMIT 10"""


def copy_selection(index, paths, output):
    output.mkdir(parents=True)
    selected = {}
    for name, files in paths.items():
        members = publication.table_members(index, name + ".parquet")
        for source, member in zip(files, members, strict=True):
            target = output / member.key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        selected[name] = selection_record(index, name)
    write(output / "download.json", {"version": 1, "status": "complete", "publication": index, "selected": selected})


async def mcp_readback(directory: Path):
    from spicy_regs import mcp_server

    mcp_server.DATA_DIR = directory
    mcp_server._reset_connection_cache()
    server = mcp_server.build_server()
    outputs = {}
    for name, args, label in (
        ("list_sources", {}, "list_sources"),
        ("describe_table", {"table": "scorecard_member_ratings"}, "describe_ratings"),
        ("query_sql", {"sql": RATING_SQL, "max_rows": 10}, "attributed_ratings"),
        ("query_sql", {"sql": ITEM_SQL, "max_rows": 10}, "attributed_target"),
    ):
        result = await server.call_tool(name, args)
        if not isinstance(result, CallToolResult) or result.is_error or result.structured_content is None:
            raise ValueError(f"MCP {name} failed: {result}")
        outputs[label] = result.structured_content
    return outputs


def integrate(source_report: Path, official_inputs: Path, output: Path):
    if output.exists():
        raise ValueError("Use a fresh output directory; retain prior qualification")
    output.mkdir(parents=True)
    source = json.loads(source_report.read_bytes())
    official = json.loads(official_inputs.read_bytes())
    source_directory = Path(source["source_generation_directory"])
    source_artifact = verify_generation(source_directory)
    assert source_artifact.pin.as_dict() == source["source_generation"]
    evidence_directory = Path(source["evidence_directory"])
    # Only the existing in-memory object-store test double receives writes.
    store = local_test_store()
    initial_index = official["selected_index"]
    store.objects[publication.INDEX_V2_KEY] = json.dumps(initial_index).encode()
    selected = publication.publish_generation(
        source_directory,
        client=store,
        bucket="local-qualification",
        prior_index=initial_index,
        evidence_directories=(evidence_directory,),
        receipt_only_tables=frozenset({"scorecard_snapshots.parquet"}),
    )
    paths = {name: [Path(p) for p in files] for name, files in official["paths"].items()}
    for name in ("scorecards", "scorecard_members", "scorecard_items"):
        paths[name] = [source_directory / (name + ".parquet")]
    build = output / "analysis"
    build.mkdir()
    for name, files in paths.items():
        for file, member in zip(files, publication.table_members(selected, name + ".parquet"), strict=True):
            target = build / member.key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(file, target)
    if "etlReceipts" in selected["families"]["scorecards"]:
        shutil.copyfile(source_directory / "etl_receipts.parquet", build / "source-etl-receipts.parquet")
    for name, receipt in official.get("receipt_paths", {}).items():
        shutil.copyfile(receipt, build / (name + "-etl-receipts.parquet"))
    pipeline = ScorecardAnalysisRollup(output_dir=build, skip_upload=True)
    parents = pipeline._prime(build, selected)
    outputs = pipeline.build(build)
    analysis_evidence = CaptureEvidence(build, "scorecard-analysis")
    analysis_evidence.read_snapshot = selected
    analysis_evidence.event("qualification-local-inputs", input_pins=parents, network_requests=0)
    generation = build / "generation"
    analysis_artifact = build_generation(
        generation,
        family="scorecard-analysis",
        files=outputs,
        expected_keys=OUTPUTS,
        **generation_options(build, LINK_NAMES),
        read_snapshot=selected,
        parents=parents,
        inputs=analysis_evidence.inputs(),
    )
    verify_generation(generation, expected_pin=analysis_artifact.pin)
    final_index = publication.publish_generation(
        generation,
        client=store,
        bucket="local-qualification",
        prior_index=selected,
        evidence_directories=(analysis_evidence.artifact_dir,),
    )
    public = output / "local-object-store"
    for key, body in store.objects.items():
        target = public / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    audits = {}
    for family in ("scorecards", "scorecard-analysis"):
        report = audit(PublicBase(str(public)), family=family, prior="none", index_raw=json.dumps(final_index).encode())
        write(output / (family + "-audit.json"), report)
        if report["summary"]["fail"]:
            raise ValueError(f"{family} audit failed: {report['summary']}")
        audits[family] = report["summary"]
    # Explicit managed local selection gives MCP the same immutable family pins.
    query_paths = {path.stem: [path] for path in source_directory.glob("*.parquet")}
    query_paths.update({path.stem: [path] for path in generation.glob("*.parquet")})
    query_dir = output / "query-selection"
    copy_selection(final_index, query_paths, query_dir)
    queries = asyncio.run(mcp_readback(query_dir))
    write(output / "mcp-readback.json", queries)
    reconstructed = read_family(
        generation, LINK_NAMES, generation_id=analysis_artifact.root["spec"]["etlReceipts"]["generationId"]
    )
    member_links = reconstructed["scorecard_member_links"]
    item_links = reconstructed["scorecard_item_links"]
    unresolved = [row for row in member_links + item_links if row["resolution_status"] != "resolved"]
    write(output / "unresolved.private.json", unresolved)
    zeldin = next(row for row in item_links if row["item_id"] == "36294")
    assert zeldin["vote_id"] == official["direct_official_readback"]["vote"]["vote_id"]
    assert (
        json.loads(zeldin["candidates_json"])[0]["vote_date"]
        == official["direct_official_readback"]["vote"]["vote_date"]
    )
    report = {
        "status": "qualified_local_source_and_analysis",
        "remote_writes": 0,
        "network_requests": 0,
        "source_qualification_sha256": digest(source_report),
        "official_selection_sha256": digest(official_inputs),
        "source_generation": source_artifact.pin.as_dict(),
        "analysis_generation": analysis_artifact.pin.as_dict(),
        "source_directory": str(source_directory),
        "analysis_directory": str(generation),
        "input_pins": {k.removesuffix(".parquet"): v for k, v in parents.items()},
        "resolution_statuses": {
            "members": dict(Counter(row["resolution_status"] for row in member_links)),
            "items": dict(Counter(row["resolution_status"] for row in item_links)),
        },
        "unresolved_count": len(unresolved),
        "audits": audits,
        "mcp_readback": str(output / "mcp-readback.json"),
        "direct_zeldin_readback": zeldin,
        "scope": "Actual publisher edition and exact links over the explicitly pinned local inputs. Local object-store publication simulation only; no production registry enablement, remote upload or current-coverage assertion.",
        "limits": official["limits"],
    }
    write(output / "integration-qualification.json", report)
    print(json.dumps({k: report[k] for k in ("status", "resolution_statuses", "audits")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-report", "official-inputs", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    integrate(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
