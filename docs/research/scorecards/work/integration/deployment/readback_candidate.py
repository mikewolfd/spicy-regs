"""Check a selected scorecard family through public files and generic hosted MCP.

Expected counts come from the qualified preparation, never a fixed publisher
list. Exact public member pins establish equality to the admitted generation;
hosted counts and attributed values independently check the consumer route.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path

import duckdb
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
import pyarrow.parquet as pq

from readback_hrc_mcp import query_rows, write
from spicy_regs.sources import publication
from spicy_regs.etl_receipts import decode_exact_json, validate_receipt_bundle
from spicy_regs.scorecards.etl import LINK_NAMES, admitted_read_policies


ATTRIBUTION = """SELECT p.publisher_id,p.name AS publisher,c.scorecard_id,
c.title AS scorecard,m.member_name,metric.name AS metric,r.value_text AS published_value,
CAST(r.value_number AS VARCHAR) AS numeric_value,p.scorecard_index_url AS publisher_url
FROM scorecard_member_ratings r JOIN scorecards c USING(scorecard_id)
JOIN scorecard_publishers p USING(publisher_id)
JOIN scorecard_members m ON m.scorecard_id=r.scorecard_id
 AND m.publisher_member_key=r.publisher_member_key
JOIN scorecard_metrics metric ON metric.scorecard_id=r.scorecard_id AND metric.metric_id=r.metric_id
QUALIFY row_number() OVER(PARTITION BY p.publisher_id ORDER BY c.scorecard_id,
 r.publisher_member_key,r.metric_id)=1 ORDER BY p.publisher_id"""


def local_rows(connection, sql):
    result = connection.execute(sql)
    columns = [column[0] for column in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def check_query(body, connection, sql, expected_pins, required_families, receipt_pins=None):
    if body.get("truncated") or body.get("truncated_cells") or not body.get("publication"):
        raise ValueError("Hosted reply is truncated or lacks publication pins")
    selected = set()
    for pin in body["publication"].values():
        if pin.get("status") == "managed_receipts":
            families = pin.get("families")
            if not isinstance(families, dict) or not families:
                raise ValueError("Hosted receipt query lacks family pins")
            for name, value in families.items():
                if value != (receipt_pins or {}).get(name):
                    raise ValueError("Hosted receipt query uses different generation-bound receipt pins")
                selected.add(name)
        else:
            name = pin.get("family")
            if name not in expected_pins or pin.get("artifact_digest") != expected_pins[name]:
                raise ValueError("Hosted query uses a different family generation")
            selected.add(name)
    if not required_families <= selected:
        raise ValueError("Hosted query uses a different family generation")
    if query_rows(body) != local_rows(connection, sql):
        raise ValueError("Hosted rows differ from the downloaded qualified members")


def selected_family(args):
    return getattr(args, "family", "scorecards")


def qualified_counts(prepared, family):
    counts = prepared["counts"] if family == "scorecards" else prepared["qualification"]["counts"]
    if (
        not isinstance(counts, dict)
        or not counts
        or any(type(value) is not int or value < 0 for value in counts.values())
        or family == "scorecard-analysis"
        and set(counts) != set(LINK_NAMES)
    ):
        raise ValueError("Qualified family counts are invalid")
    return counts


def analysis_parents(root, prepared, index):
    expected = {name + ".parquet": pin for name, pin in prepared["qualification"]["input_pins"].items()}
    if root["spec"]["parents"] != expected:
        raise ValueError("Analysis parents differ from the qualified captured inputs")
    for key, pin in expected.items():
        if publication.table_pin(root["spec"]["readSnapshot"], key) != pin:
            raise ValueError("Analysis parent differs from its captured publication: " + key)
        if pin["family"] == "scorecards" and publication.table_pin(index, key) != pin:
            raise ValueError("Analysis source parent differs from the current source generation")
    return expected


def analysis_outcomes_sql(generation):
    generation = generation.replace("'", "''")

    def field(column, name, identity=False):
        key = "$[1][0][1]" if identity else "$[0]"
        value = "$[1][1][1]" if identity else "$[1][1]"
        return (
            f"(SELECT json_extract_string(value,'{value}') FROM json_each({column},'$[1]') "
            f"WHERE json_extract_string(value,'{key}')='{name}')"
        )

    return (
        "SELECT dataset,outcome,"
        + field("identity_json", "scorecard_id", True)
        + " AS scorecard_id,"
        + field("identity_json", "publisher_member_key", True)
        + " AS publisher_member_key,"
        + field("identity_json", "item_id", True)
        + " AS item_id,"
        + field("processing_json", "resolution_status")
        + " AS resolution_status "
        + f"FROM etl_receipts WHERE generation_id='{generation}'"
    )


def verify_analysis_receipts(paths, family, root, prepared):
    """Reuse full receipt admission, then count all outcomes without materializing source contexts."""
    policies = admitted_read_policies(LINK_NAMES, descriptors=root["spec"]["etlReceipts"]["policies"])
    validate_receipt_bundle(
        {name: [paths[name]] for name in LINK_NAMES},
        [paths["etl_receipts"]],
        list(policies.values()),
        generation_id=family["etlReceipts"]["generationId"],
    )
    counts, statuses, scopes = Counter(), {name: Counter() for name in LINK_NAMES}, Counter()
    expected = prepared["qualification"]["input_pins"]
    for batch in pq.ParquetFile(paths["etl_receipts"]).iter_batches(
        batch_size=512, columns=["dataset", "identity_json", "processing_json", "witnesses"]
    ):
        for row in batch.to_pylist():
            identity = dict(decode_exact_json(row["identity_json"]))
            processing = decode_exact_json(row["processing_json"])
            if json.loads(processing["input_pins_json"]) != expected:
                raise ValueError("Analysis row parents differ from its qualified inputs")
            for name, pin in expected.items():
                witnesses = [w for w in row["witnesses"] if w["source_id"] == name]
                if (
                    len(witnesses) != 1
                    or witnesses[0]["sha256"] != pin["sha256"]
                    or witnesses[0]["body_version"] != pin["artifactDigest"]
                ):
                    raise ValueError("Analysis row witness differs from its qualified parent: " + name)
            name, status = row["dataset"], processing["resolution_status"]
            counts[name] += 1
            statuses[name][status] += 1
            scopes[(name, identity["scorecard_id"], status)] += 1
    if dict(counts) != qualified_counts(prepared, "scorecard-analysis"):
        raise ValueError("Complete analysis receipt populations differ from qualification")
    if statuses != prepared["qualification"]["resolution_statuses"]:
        raise ValueError("Analysis resolution statuses differ from qualification")
    return dict(
        counts=dict(counts),
        resolution_statuses=statuses,
        per_edition=[
            dict(dataset=d, scorecard_id=c, resolution_status=s, rows=n) for (d, c, s), n in sorted(scopes.items())
        ],
        receipt_validation="Complete shared subject/receipt admission and every row's qualified parents/witnesses",
    )


def public_files(args, prepared, authenticated):
    index = publication.load_index(args.public_url)
    write(args.output / "public-index.json", index)
    family_name = selected_family(args)
    family = index["families"][family_name]
    counts = qualified_counts(prepared, family_name)
    if family != authenticated["family_entry"] or family["artifactDigest"] != prepared["generation"]["artifactDigest"]:
        raise ValueError("Public source pointer differs from the authenticated qualified generation")
    tables = family["tables"]
    if set(tables) != {name + ".parquet" for name in counts if name != "scorecard_snapshots"}:
        raise ValueError("Public source table membership differs from the qualified preparation")
    paths, downloads = {}, []
    target = args.output / "members"
    target.mkdir()
    for key in tables:
        member = publication.single_member(index, key)
        paths[key.removesuffix(".parquet")] = target / key
        downloads.append((member, target / key))
    receipts = publication.receipt_members(
        index, dataset="scorecards" if family_name == "scorecards" else LINK_NAMES[0]
    )
    if len(receipts) != 1:
        raise ValueError("Expected one generation-bound source receipt member")
    paths["etl_receipts"] = target / "etl_receipts.parquet"
    downloads.append((receipts[0], paths["etl_receipts"]))

    def download(entry):
        member, path = entry
        if member.sha256 is None or not publication.fetch_member(args.public_url, member, path, timeout=120):
            raise ValueError("Qualified public member is unavailable or unpinned")
        rows = pq.ParquetFile(path).metadata.num_rows
        if rows != member.rows:
            raise ValueError("Public Parquet footer count differs from its pin")
        return dict(
            key=member.key,
            url=args.public_url.rstrip("/") + "/" + member.path,
            sha256=member.sha256,
            bytes=member.byte_size,
            rows=rows,
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        members = list(executor.map(download, downloads))
    if family_name == "scorecard-analysis":
        raw, root = publication.load_family_root(args.public_url, family)
        (args.output / "analysis-artifact.json").write_bytes(raw)
        if root["spec"]["family"] != family_name:
            raise ValueError("Published artifact uses a different family")
        parents = analysis_parents(root, prepared, index)
        write(args.output / "analysis-parents.json", parents)
        analysis = verify_analysis_receipts(paths, family, root, prepared)
        write(args.output / "analysis-outcomes.json", analysis)
        # Only small native publisher/source tables needed for attribution are read.
        for key in (
            "scorecards.parquet",
            "scorecard_members.parquet",
            "scorecard_items.parquet",
            "scorecard_publishers.parquet",
        ):
            member = publication.single_member(index, key)
            path = target / key
            members.append(download((member, path)))
            paths[key.removesuffix(".parquet")] = path
    connection = duckdb.connect()
    connection.execute("SET threads=2")
    for name, path in paths.items():
        connection.read_parquet(str(path)).create_view(name)
    for name, expected in counts.items():
        if family_name == "scorecard-analysis":
            expected = family["tables"][name + ".parquet"]["rows"]
        sql = (
            "SELECT count(*) AS rows FROM etl_receipts WHERE dataset='scorecard_snapshots' AND outcome='observed'"
            if name == "scorecard_snapshots"
            else f'SELECT count(*) AS rows FROM "{name}"'
        )
        if local_rows(connection, sql) != [{"rows": expected}]:
            raise ValueError("Public source count differs from preparation: " + name)
    sql = "SELECT generation_id,count(*) AS rows FROM etl_receipts GROUP BY generation_id ORDER BY generation_id"
    expected_receipts = [
        {"generation_id": family["etlReceipts"]["generationId"], "rows": family["etlReceipts"]["rows"]}
    ]
    if local_rows(connection, sql) != expected_receipts:
        raise ValueError("Public source receipts do not share the admitted generation")
    if family_name == "scorecard-analysis":
        connection.execute(
            "CREATE VIEW analysis_outcomes AS " + analysis_outcomes_sql(family["etlReceipts"]["generationId"])
        )
        status_rows = local_rows(
            connection,
            "SELECT dataset,resolution_status,count(*) AS rows FROM analysis_outcomes "
            "GROUP BY dataset,resolution_status ORDER BY dataset,resolution_status",
        )
        if status_rows != [
            dict(dataset=n, resolution_status=s, rows=v)
            for n, states in sorted(analysis["resolution_statuses"].items())
            for s, v in sorted(states.items())
        ]:
            raise ValueError("SQL analysis statuses differ from shared decoded receipts")
        scopes = local_rows(
            connection,
            "SELECT dataset,scorecard_id,resolution_status,count(*) AS rows "
            "FROM analysis_outcomes GROUP BY dataset,scorecard_id,resolution_status "
            "ORDER BY dataset,scorecard_id,resolution_status",
        )
        if scopes != analysis["per_edition"]:
            raise ValueError("SQL edition/status groups differ from shared decoded receipts")
    write(
        args.output / "public-files.json",
        dict(status="passed", family=family_name, generation=prepared["generation"], members=members),
    )
    return connection, family


async def hosted(args, connection, family, prepared=None):
    expected_pin = family["artifactDigest"]
    calls = []
    family_name = selected_family(args)
    index = json.loads((args.output / "public-index.json").read_bytes())
    expected_pins = {name: value["artifactDigest"] for name, value in index["families"].items()}
    receipt_pins = {
        name: dict(
            artifact_digest=value["artifactDigest"],
            generation_id=value["etlReceipts"]["generationId"],
            sha256=value["etlReceipts"]["sha256"],
            rows=value["etlReceipts"]["rows"],
            datasets=value["etlReceipts"]["datasets"],
        )
        for name, value in index["families"].items()
        if value.get("etlReceipts")
    }
    async with streamable_http_client(args.mcp_url) as (read, send):
        async with ClientSession(read, send, read_timeout_seconds=300) as session:
            await session.initialize()

            async def call(tool, arguments, label, *, required_families=None):
                result = await session.call_tool(tool, arguments)
                payload = result.model_dump(by_alias=True, mode="json")
                path = args.output / (label + ".json")
                write(path, payload)
                if payload.get("isError"):
                    raise ValueError("Hosted readback refused " + label)
                body = payload.get("structuredContent")
                if body is None:
                    body = json.loads(next(row["text"] for row in payload["content"] if row["type"] == "text"))
                if body.get("error"):
                    raise ValueError("Hosted readback returned an error for " + label)
                if tool == "query_sql":
                    check_query(
                        body,
                        connection,
                        arguments["sql"],
                        expected_pins,
                        required_families or {family_name},
                        receipt_pins,
                    )
                calls.append(dict(tool=tool, label=label, response_sha256=sha256(path.read_bytes()).hexdigest()))
                return body

            listed = await call("list_sources", {}, "list_sources")
            listed_tables = list(listed.get("tables", []))
            listed_tables.extend(row for subject in listed.get("subjects", []) for row in subject["tables"])
            expected_tables = {key.removesuffix(".parquet") for key in family["tables"]}
            if not expected_tables <= {row["table"] for row in listed_tables}:
                raise ValueError("Hosted source tables are not all listed")
            description_table = "scorecard_member_ratings" if family_name == "scorecards" else LINK_NAMES[0]
            described = await call(
                "describe_table",
                {"table": description_table},
                "describe_ratings" if family_name == "scorecards" else "describe_" + description_table,
            )
            if described["publication"].get("artifact_digest") != expected_pin:
                raise ValueError("Hosted source description has not refreshed")
            actual_columns = [[row["column_name"], row["column_type"]] for row in described["columns"]]
            if actual_columns != family["tables"][description_table + ".parquet"]["columns"]:
                raise ValueError("Hosted rating columns differ from the published native schema")
            for name in sorted(expected_tables):
                if family_name == "scorecard-analysis" and name != description_table:
                    description = await call("describe_table", {"table": name}, "describe_" + name)
                    if (
                        description["publication"].get("artifact_digest") != expected_pin
                        or [[r["column_name"], r["column_type"]] for r in description["columns"]]
                        != family["tables"][name + ".parquet"]["columns"]
                    ):
                        raise ValueError("Hosted analysis columns or pin differ")
                key = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
                sql = f'SELECT "{key}",count(*) AS rows FROM "{name}" GROUP BY "{key}" ORDER BY "{key}"'
                await call("query_sql", {"sql": sql, "max_rows": 500}, name + "_scope_counts")
            if family_name == "scorecards":
                await call("query_sql", {"sql": ATTRIBUTION, "max_rows": 500}, "attributed_ratings")
            else:
                if prepared is None:
                    raise ValueError("Analysis readback requires its qualified preparation")
                outcomes = analysis_outcomes_sql(family["etlReceipts"]["generationId"])
                for name, states in prepared["qualification"]["resolution_statuses"].items():
                    for status in states:
                        sql = (
                            "WITH analysis_outcomes AS ("
                            + outcomes
                            + ") SELECT scorecard_id,count(*) AS rows "
                            + f"FROM analysis_outcomes WHERE dataset='{name}' AND resolution_status='{status}' "
                            + "GROUP BY scorecard_id ORDER BY scorecard_id"
                        )
                        await call("query_sql", {"sql": sql, "max_rows": 500}, name + "_" + status + "_scope_counts")
                sql = (
                    "WITH analysis_outcomes AS (" + outcomes + ") SELECT p.publisher_id,p.name AS publisher,"
                    "c.scorecard_id,c.title AS scorecard,m.member_name,a.publisher_member_key,a.resolution_status,l.bioguide_id "
                    "FROM analysis_outcomes a JOIN scorecard_members m USING(scorecard_id,publisher_member_key) "
                    "JOIN scorecards c USING(scorecard_id) JOIN scorecard_publishers p USING(publisher_id) "
                    "LEFT JOIN scorecard_member_links l USING(scorecard_id,publisher_member_key) "
                    "WHERE a.dataset='scorecard_member_links' QUALIFY row_number() OVER(PARTITION BY p.publisher_id "
                    "ORDER BY c.scorecard_id,a.publisher_member_key)=1 ORDER BY p.publisher_id"
                )
                await call(
                    "query_sql",
                    {"sql": sql, "max_rows": 500},
                    "attributed_member_links",
                    required_families={"scorecards", "scorecard-analysis"},
                )
                sql = (
                    "SELECT p.publisher_id,p.name AS publisher,c.scorecard_id,c.title AS scorecard,i.item_id,"
                    "i.title AS item,l.reference_id,l.bill_id,l.amendment_id,l.vote_id FROM scorecard_item_links l "
                    "JOIN scorecard_items i USING(scorecard_id,item_id) JOIN scorecards c USING(scorecard_id) "
                    "JOIN scorecard_publishers p USING(publisher_id) QUALIFY row_number() OVER(PARTITION BY p.publisher_id "
                    "ORDER BY c.scorecard_id,i.item_id,l.reference_id)=1 ORDER BY p.publisher_id"
                )
                await call(
                    "query_sql",
                    {"sql": sql, "max_rows": 500},
                    "attributed_item_links",
                    required_families={"scorecards", "scorecard-analysis"},
                )
            receipt_description = await call("describe_table", {"table": "etl_receipts"}, "describe_receipts")
            if not receipt_description.get("available"):
                raise ValueError("Hosted shared ETL receipt table is unavailable")
            # Other families can share this view; restrict to this immutable source generation.
            generation = family["etlReceipts"]["generationId"].replace("'", "''")
            sql = (
                "SELECT dataset,outcome,count(*) AS rows FROM etl_receipts "
                f"WHERE generation_id='{generation}' GROUP BY dataset,outcome ORDER BY dataset,outcome"
            )
            # A shared receipt query legitimately returns other family pins.
            await call("query_sql", {"sql": sql, "max_rows": 500}, "source_receipt_counts")
    return calls


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--publication-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--public-url", default="https://data.spicygov.ai")
    parser.add_argument("--mcp-url", default="https://mcp.spicygov.ai/mcp")
    parser.add_argument("--family", choices=("scorecards", "scorecard-analysis"), default="scorecards")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Retain prior observations; choose a fresh readback directory")
    args.output.mkdir(parents=True)
    preparation_bytes = args.preparation.read_bytes()
    prepared = json.loads(preparation_bytes)
    authenticated = json.loads(args.publication_receipt.read_bytes())
    if (
        authenticated["status"] != "published_authenticated_readback"
        or authenticated["preparation_sha256"] != sha256(preparation_bytes).hexdigest()
        or authenticated["family"] != args.family
    ):
        raise ValueError("Publication receipt does not admit this qualified preparation")
    status = "public_files_pending"
    try:
        connection, family = public_files(args, prepared, authenticated)
        status = "public_files_verified_hosted_pending"
        calls = asyncio.run(hosted(args, connection, family, prepared))
        write(
            args.output / "summary.json",
            dict(
                status="passed",
                observed_at=datetime.now(UTC).isoformat(),
                generation=prepared["generation"],
                preparation_sha256=sha256(preparation_bytes).hexdigest(),
                publication_receipt_sha256=sha256(args.publication_receipt.read_bytes()).hexdigest(),
                family=args.family,
                counts=qualified_counts(prepared, args.family),
                calls=calls,
            ),
        )
        print(json.dumps(dict(status="passed", generation=prepared["generation"])))
    except Exception as error:
        write(args.output / "failure.json", dict(status=status, error_type=type(error).__name__, reason=str(error)))
        raise


if __name__ == "__main__":
    main()
