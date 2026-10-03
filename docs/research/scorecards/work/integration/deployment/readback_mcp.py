"""Read attributed scorecards through generic MCP, locally or on the hosted service.

Local mode downloads public immutable tables and uses the ordinary selected-file
MCP path. Hosted mode uses only the public MCP protocol. Neither mode publishes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from spicy_regs.local_data import selection_record
from spicy_regs.sources import publication

RATINGS = """SELECT p.name AS publisher, c.scorecard_id, count(*) AS ratings
FROM scorecard_member_ratings r JOIN scorecards c USING(scorecard_id)
JOIN scorecard_publishers p USING(publisher_id)
GROUP BY p.name,c.scorecard_id ORDER BY c.scorecard_id"""
ATTRIBUTED = """SELECT p.name AS publisher,c.title AS scorecard,m.member_name,
metrics.name AS metric,r.value_text AS published_value,r.source_url AS publisher_url,
l.bioguide_id,l.resolution_status
FROM scorecard_member_ratings r JOIN scorecards c USING(scorecard_id)
JOIN scorecard_publishers p USING(publisher_id)
JOIN scorecard_metrics metrics ON metrics.scorecard_id=r.scorecard_id AND metrics.metric_id=r.metric_id
JOIN scorecard_members m ON m.scorecard_id=r.scorecard_id AND m.publisher_member_key=r.publisher_member_key
JOIN scorecard_member_links l ON l.scorecard_id=r.scorecard_id AND l.publisher_member_key=r.publisher_member_key
QUALIFY row_number() OVER(PARTITION BY c.publisher_id ORDER BY m.member_name,r.metric_id)=1
ORDER BY publisher"""
LINKS = """SELECT scorecard_id,resolution_status,count(*) AS members
FROM scorecard_member_links GROUP BY scorecard_id,resolution_status ORDER BY scorecard_id,resolution_status"""
ITEMS = """SELECT i.scorecard_id,i.title,i.item_date_text,i.publisher_position_text AS publisher_target,
i.source_url AS publisher_url,l.vote_id,l.bill_id,l.resolution_status
FROM scorecard_items i JOIN scorecard_item_links l USING(scorecard_id,item_id)
QUALIFY row_number() OVER(PARTITION BY i.scorecard_id ORDER BY i.item_id,l.reference_id)=1
ORDER BY i.scorecard_id"""


def write(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


async def check(call, output, mode):
    replies = {}
    metadata_failures = []
    for tool, args, label in (
        ("list_sources", {}, "list_sources"),
        ("describe_table", {"table": "scorecard_member_ratings"}, "describe_ratings"),
        ("query_sql", {"sql": RATINGS, "max_rows": 10}, "publisher_counts"),
        ("query_sql", {"sql": ATTRIBUTED, "max_rows": 10}, "attributed_ratings"),
        ("query_sql", {"sql": LINKS, "max_rows": 20}, "resolution_counts"),
        ("query_sql", {"sql": ITEMS, "max_rows": 10}, "attributed_items"),
    ):
        result = await call(tool, args)
        payload = result.model_dump(by_alias=True, mode="json")
        write(output / (label + ".json"), payload)
        if payload.get("isError"):
            if mode == "hosted_public_mcp" and label == "describe_ratings":
                metadata_failures.append(label)
                continue
            raise ValueError(f"MCP refused {label}")
        body = payload.get("structuredContent")
        if body is None:
            body = json.loads(next(v["text"] for v in payload["content"] if v["type"] == "text"))
        if not isinstance(body, dict) or body.get("error"):
            raise ValueError(f"MCP returned an error for {label}")
        replies[label] = body
    available = {row["table"] for row in replies["list_sources"].get("tables", [])}
    expected_tables = {"scorecard_member_ratings", "scorecard_member_links", "scorecard_item_links"}
    if not expected_tables <= available:
        metadata_failures.append("list_sources_missing_scorecard_tables")
    expected_pins = {
        "scorecards": "sha256:7a8d9462a2c4711c4f5e2979bf1ff31c3daaacf09a570c3b3f9ff715551274eb",
        "scorecard-analysis": "sha256:f3fffcf37e4ffc7a6015c54cbd5757379bc93701416c0281e1ef76d0f5ef7fe2",
    }
    for label in ("publisher_counts", "attributed_ratings", "resolution_counts", "attributed_items"):
        for pin in replies[label]["publication"].values():
            if pin.get("artifact_digest") != expected_pins.get(pin.get("family")):
                raise ValueError("MCP returned a different publication pin")
    if metadata_failures and mode != "hosted_public_mcp":
        raise ValueError("Local MCP metadata does not include the qualified scorecard tables")
    if replies["publisher_counts"]["row_count_shown"] != 3 or replies["attributed_ratings"]["row_count_shown"] != 3:
        raise ValueError("MCP did not return all three attributed publishers")
    if {row["scorecard_id"]: row["ratings"] for row in replies["publisher_counts"]["rows"]} != {
        "afp:3933": 2120,
        "ijm:3995": 1912,
        "lcv:2025": 1102,
    }:
        raise ValueError("Public rating counts differ from qualified complete editions")
    write(
        output / "summary.json",
        {
            "status": "queries_passed_metadata_incomplete" if metadata_failures else "passed",
            "mode": mode,
            "queries": list(replies),
            "metadata_failures": metadata_failures,
        },
    )


async def run(args):
    if args.output.exists():
        raise ValueError("Use a fresh readback directory")
    args.output.mkdir(parents=True)
    if args.url:
        async with streamable_http_client(args.url) as (read, send):
            async with ClientSession(read, send, read_timeout_seconds=300) as session:
                initialized = await session.initialize()
                write(args.output / "initialize.json", initialized.model_dump(by_alias=True, mode="json"))
                await check(session.call_tool, args.output, "hosted_public_mcp")
        return
    index = publication.load_index("https://data.spicygov.ai")
    data = args.output / "public-tables"
    data.mkdir()
    selected = {}
    for family in ("scorecards", "scorecard-analysis"):
        for key in index["families"][family]["tables"]:
            name = key.removesuffix(".parquet")
            selected[name] = selection_record(index, name)
            for member in publication.table_members(index, key):
                target = data / member.key
                target.parent.mkdir(parents=True, exist_ok=True)
                if not publication.fetch_member("https://data.spicygov.ai", member, target):
                    raise ValueError("Public immutable table is absent")
    write(data / "download.json", {"version": 1, "status": "complete", "publication": index, "selected": selected})
    from spicy_regs import mcp_server

    mcp_server.DATA_DIR = data
    mcp_server._reset_connection_cache()
    server = mcp_server.build_server()
    await check(server.call_tool, args.output, "local_mcp_over_public_pinned_files")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
