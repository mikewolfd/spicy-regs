"""Independently reconcile retained original CSV/HTML cells to candidate Parquet."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import io
import json
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup
import pyarrow.parquet as pq

from qualify_lcv import Retained, digest, write
from spicy_docs.sources.scorecards import lcv


def csv_rows(response):
    return list(csv.reader(io.StringIO(response.body.decode("utf-8-sig")), strict=True))


def cards(response):
    document = BeautifulSoup(response.body, "html.parser")
    found = {}
    for card in document.select(".congress-item"):
        anchor = card.select_one("a.card-link[href]")
        if anchor is None or anchor["href"] in found:
            raise ValueError("Repeated or absent original member link")
        found[anchor["href"]] = card
    assert found and not document.select(".pagination .next, a.next.page-numbers")
    return found


def readback(retained, source_report, output):
    report = json.loads(source_report.read_bytes())
    directory = Path(report["source_generation_directory"])
    tables = {path.stem: pq.ParquetFile(path).read().to_pylist() for path in directory.glob("*.parquet")}
    member_response = retained.fetch(lcv.MEMBERS + "?" + urlencode(lcv.MEMBER_QUERY))
    members = {
        row[-1]: row for row in csv_rows(member_response) if len(row) == 7 and row[-1].startswith(lcv.BASE + "/moc/")
    }
    html_members = cards(retained.fetch(lcv.MEMBERS + "?session_year=2025"))
    source_members = {row["publisher_member_id"]: row for row in tables["scorecard_members"]}
    assert members.keys() == html_members.keys() == source_members.keys()
    ratings = {(r["publisher_member_key"], r["metric_id"]): r["value_text"] for r in tables["scorecard_member_ratings"]}
    for url, raw in members.items():
        key = source_members[url]["publisher_member_key"]
        assert ratings[key, "annual"] == raw[4] and ratings[key, "lifetime"] == raw[5]
    catalog = retained.fetch(lcv.VOTES + "?" + urlencode(lcv.VOTE_QUERY))
    original_items = {
        row[-1]: row for row in csv_rows(catalog) if len(row) == 4 and row[-1].startswith(lcv.BASE + "/roll-call-vote/")
    }
    html_catalog = BeautifulSoup(retained.fetch(lcv.VOTES + "?session_year=2025").body, "html.parser")
    links = {
        str(a["href"])
        for a in html_catalog.select("a[href]")
        if str(a["href"]).startswith(lcv.BASE + "/roll-call-vote/")
    }
    assert original_items.keys() == links
    parsed_items = {r["item_id"]: r for r in tables["scorecard_items"]}
    parsed_results = {
        (r["item_id"], r["publisher_member_key"]): r["result_text"] for r in tables["scorecard_member_item_results"]
    }
    captures, total, native_ids = [], 0, set()
    for url, row in original_items.items():
        detail = retained.fetch(url)
        document = BeautifulSoup(detail.body, "html.parser")
        native = document.select_one('input[name="export-post-id"]')
        assert native is not None
        item_id = str(native["value"])
        native_ids.add(item_id)
        item = parsed_items[item_id]
        response = retained.fetch(item["source_url"])
        raw = csv_rows(response)
        preamble = {r[0]: r[1] for r in raw if len(r) == 2}
        results = {r[-1]: r for r in raw if len(r) == 9 and r[-1].startswith(lcv.BASE + "/moc/")}
        html_results = cards(detail)
        assert results.keys() == html_results.keys()
        assert item["title"] == preamble["Vote Title"] == row[0]
        assert item["item_date_text"] == preamble["Year"] == row[1]
        assert item["roll_number_text"] == preamble["Roll Call Vote Number"] == row[2]
        assert item["chamber_text"] == preamble["Congress"]
        assert item["publisher_position_text"] == preamble["Pro-Environment Vote"]
        for member_url, result in results.items():
            key = source_members[member_url]["publisher_member_key"]
            assert parsed_results[item_id, key] == result[-2] == html_results[member_url]["data-vote-type"]
        total += len(results)
        captures.append(
            {
                "item_id": item_id,
                "item_url": url,
                "csv_sha256": response.sha256,
                "html_sha256": detail.sha256,
                "csv_results": len(results),
                "html_results": len(html_results),
            }
        )
    assert native_ids == parsed_items.keys()
    assert total == len(parsed_results) == len(tables["scorecard_member_item_results"])
    output_report = {
        "status": "direct_original_cell_readback_passed",
        "source_qualification_sha256": digest(source_report),
        "member_catalog": {
            "csv_rows": len(members),
            "html_cards": len(html_members),
            "parsed_members": len(source_members),
            "parsed_rating_cells": len(ratings),
            "blank_annual_cells": sum(r[4] == "" for r in members.values()),
            "na_annual_cells": sum(r[4] == "na" for r in members.values()),
            "sha256": member_response.sha256,
        },
        "item_catalog": {
            "csv_rows": len(original_items),
            "html_links": len(links),
            "parsed_items": len(parsed_items),
            "sha256": catalog.sha256,
        },
        "all_item_results": {"csv_cells": total, "parsed_cells": len(parsed_results)},
        "per_item": captures,
        "member_context": dict(Counter(r["chamber_text"] for r in source_members.values())),
        "source_declared_aggregate_count": None,
        "count_basis": "Original complete CSV rows/EOF reconcile to whole HTML catalogs and per-item member tables; no independent publisher-declared aggregate count was found or invented.",
        "raw_original_readback": True,
    }
    write(output, output_report)
    print(
        json.dumps(
            {key: output_report[key] for key in ("status", "member_catalog", "item_catalog", "all_item_results")}
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retained-dir", type=Path, action="append", required=True)
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    readback(Retained(args.retained_dir), args.source_report, args.output)


if __name__ == "__main__":
    main()
