"""Read qualified HRC data through generic hosted MCP, checking exact family pins."""

from __future__ import annotations

import argparse
import asyncio
from hashlib import sha256
import json
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

CARD = "hrc:118-final"
SOURCE_COUNTS = " UNION ALL ".join(
    f"SELECT '{label}' AS kind,count(*) AS rows FROM {table} WHERE scorecard_id='{CARD}'"
    for label, table in (
        ("members", "scorecard_members"),
        ("ratings", "scorecard_member_ratings"),
        ("items", "scorecard_items"),
        ("results", "scorecard_member_item_results"),
    )
)
RATING = f"""SELECT p.name AS publisher,c.title AS scorecard,m.member_name,
metric.name AS period,metric.chamber_text AS metric_chamber,r.value_text AS published_value,
r.value_number,r.source_url AS publisher_url,r.source_path
FROM scorecard_member_ratings r JOIN scorecards c USING(scorecard_id)
JOIN scorecard_publishers p USING(publisher_id)
JOIN scorecard_members m ON m.scorecard_id=r.scorecard_id AND m.publisher_member_key=r.publisher_member_key
JOIN scorecard_metrics metric ON metric.scorecard_id=r.scorecard_id AND metric.metric_id=r.metric_id
WHERE r.scorecard_id='{CARD}' AND r.value_text='N/A'
ORDER BY m.member_name,metric.name LIMIT 3"""
GLYPHS = f"""SELECT result_text,count(*) AS rows FROM scorecard_member_item_results
WHERE scorecard_id='{CARD}' GROUP BY result_text ORDER BY result_text"""
NA = f"""SELECT count(*) FILTER(WHERE value_text='N/A') AS rows,
count(value_number) FILTER(WHERE value_text='N/A') AS numeric_values,
count(*) FILTER(WHERE value_text='NA') AS standalone_na
FROM scorecard_member_ratings WHERE scorecard_id='{CARD}'"""
FAMILY_RATINGS = """SELECT c.publisher_id,count(*) AS rows
FROM scorecard_member_ratings r JOIN scorecards c USING(scorecard_id)
GROUP BY c.publisher_id ORDER BY c.publisher_id"""


def write(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def query_rows(body):
    """Read either documented row representation without changing retained replies."""
    columns = body["columns"]
    if any(not isinstance(name, str) for name in columns) or len(set(columns)) != len(columns):
        raise ValueError("Hosted query columns are ambiguous")
    rows = []
    for row in body["rows"]:
        if isinstance(row, dict) and set(row) == set(columns):
            rows.append(row)
        elif isinstance(row, list) and len(row) == len(columns):
            rows.append(dict(zip(columns, row, strict=True)))
        else:
            raise ValueError("Hosted query row does not match its columns")
    return rows


async def run(args):
    if args.output.exists():
        raise ValueError("Retain earlier readbacks; choose a fresh output directory")
    args.output.mkdir(parents=True)
    pins = {"scorecards": args.source_pin}
    if args.analysis_pin:
        pins["scorecard-analysis"] = args.analysis_pin
    calls = []
    async with streamable_http_client(args.url) as (read, send):
        async with ClientSession(read, send, read_timeout_seconds=300) as session:
            await session.initialize()

            async def call(tool, arguments, label):
                result = await session.call_tool(tool, arguments)
                payload = result.model_dump(by_alias=True, mode="json")
                path = args.output / (label + ".json")
                write(path, payload)
                if payload.get("isError"):
                    raise ValueError(f"Hosted HRC readback refused {label}")
                body = payload.get("structuredContent")
                if body is None:
                    body = json.loads(next(v["text"] for v in payload["content"] if v["type"] == "text"))
                if body.get("error"):
                    raise ValueError(f"Hosted HRC readback returned an error for {label}")
                if tool == "query_sql":
                    for pin in body["publication"].values():
                        if pin.get("artifact_digest") != pins.get(pin.get("family")):
                            raise ValueError("Hosted HRC query still uses a different publication pin")
                    body = {**body, "rows": query_rows(body)}
                calls.append({"tool": tool, "label": label, "response_sha256": sha256(path.read_bytes()).hexdigest()})
                return body

            listed = await call("list_sources", {}, "list_sources")
            listed_tables = list(listed.get("tables", []))
            listed_tables.extend(row for subject in listed.get("subjects", []) for row in subject["tables"])
            if "scorecard_member_ratings" not in {row["table"] for row in listed_tables}:
                raise ValueError("Hosted source table is not listed")
            described = await call("describe_table", {"table": "scorecard_member_ratings"}, "describe_ratings")
            if described["publication"]["artifact_digest"] != args.source_pin:
                raise ValueError("Hosted description has not refreshed to the HRC source generation")
            counts = await call("query_sql", {"sql": SOURCE_COUNTS, "max_rows": 10}, "source_counts")
            if {row["kind"]: row["rows"] for row in counts["rows"]} != {
                "members": 540,
                "ratings": 1620,
                "items": 56,
                "results": 19540,
            }:
                raise ValueError("Hosted HRC source counts differ from the qualified edition")
            na = await call("query_sql", {"sql": NA, "max_rows": 10}, "literal_na")
            if na["rows"] != [{"rows": 280, "numeric_values": 0, "standalone_na": 0}]:
                raise ValueError("Hosted HRC N/A literals differ or gained numeric conversions")
            family = await call("query_sql", {"sql": FAMILY_RATINGS, "max_rows": 10}, "family_rating_counts")
            if {row["publisher_id"]: row["rows"] for row in family["rows"]} != {
                "afp": 2120,
                "hrc": 1620,
                "ijm": 1912,
                "lcv": 1102,
            }:
                raise ValueError("Hosted source-family rating counts changed")
            glyphs = await call("query_sql", {"sql": GLYPHS, "max_rows": 10}, "literal_results")
            if {row["result_text"]: row["rows"] for row in glyphs["rows"]} != {
                "○": 9340,
                "●": 9306,
                "⊗": 471,
                "N/A": 422,
                "P": 1,
            }:
                raise ValueError("Hosted HRC result tokens differ from source-qualified counts")
            ratings = await call("query_sql", {"sql": RATING, "max_rows": 10}, "attributed_ratings")
            if len(ratings["rows"]) != 3 or any(
                row["publisher"] != "Human Rights Campaign"
                or row["published_value"] != "N/A"
                or row["value_number"] is not None
                for row in ratings["rows"]
            ):
                raise ValueError("Hosted rating attribution or literal values differ")
            if args.analysis_pin:
                for name in ("scorecard_member_links", "scorecard_item_links"):
                    sql = f"SELECT resolution_status,resolution_rule,count(*) AS rows FROM {name} WHERE scorecard_id='{CARD}' GROUP BY resolution_status,resolution_rule ORDER BY resolution_status,resolution_rule"
                    body = await call("query_sql", {"sql": sql, "max_rows": 100}, name)
                    if body.get("truncated") or not body["rows"]:
                        raise ValueError("HRC resolution summary is missing or truncated")
                    if name == "scorecard_member_links" and sum(row["rows"] for row in body["rows"]) != 540:
                        raise ValueError("HRC member links are incomplete")
    write(args.output / "summary.json", {"status": "passed", "scorecard_id": CARD, "family_pins": pins, "calls": calls})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://mcp.spicygov.ai/mcp")
    parser.add_argument("--source-pin", required=True)
    parser.add_argument("--analysis-pin")
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
