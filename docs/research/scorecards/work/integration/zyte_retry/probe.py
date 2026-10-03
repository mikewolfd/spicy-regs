"""Retry catalogued blocked original URLs once through Zyte; retain raw bodies privately."""

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

from spicy_docs.sources.zyte import HTTP_RESPONSE_BODY, ZyteHttpFetcher, require_zyte_token_from_environment
from spicy_docs.transport.credentials import read_api_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--key-file", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    token = read_api_key(args.key_file, "ZYTE_TOKEN") if args.key_file else require_zyte_token_from_environment()
    fetcher = ZyteHttpFetcher(token=token)
    inventory_raw = args.inventory.read_bytes()
    publishers = json.loads(inventory_raw)["publishers"]
    results = []
    for source in publishers:
        if source["discovery_status"] != "blocked":
            continue
        row = {
            "publisher_id": source["publisher_id"],
            "requested_url": source["original_scorecard_url"],
            "provider": "zyte",
            "mode": HTTP_RESPONSE_BODY,
            "production_qualified": False,
        }
        if not row["requested_url"]:
            row["outcome"] = "no_original_scorecard_url_to_request"
            results.append(row)
            continue
        row["observed_at"] = datetime.now(UTC).isoformat()
        try:
            response = fetcher.fetch(
                row["requested_url"], timeout_seconds=90, max_bytes=20 * 1024 * 1024, mode=HTTP_RESPONSE_BODY
            )
        except Exception as error:
            # Preserve only error type. No provider request, key or arbitrary
            # exception text enters a receipt. Stop on provider failure.
            row.update(outcome="provider_failed", error_type=type(error).__name__)
            results.append(row)
            break
        row.update(
            resolved_url=response.resolved_url,
            http_status=response.status_code,
            content_type=response.content_type,
            zyte_request_id=response.request_id,
            byte_size=len(response.body),
            sha256=hashlib.sha256(response.body).hexdigest(),
            body_is_publisher_bytes=True,
            outcome="response_received",
        )
        if response.status_code not in (401, 403):
            (args.output / (source["publisher_id"] + ".body")).write_bytes(response.body)
        results.append(row)
        print(json.dumps({k: row[k] for k in ("publisher_id", "http_status", "byte_size")}), flush=True)
        (args.output / "progress.json").write_text(json.dumps(results, indent=2) + "\n")
    report = {
        "inventory_sha256": hashlib.sha256(inventory_raw).hexdigest(),
        "results": results,
        "scope": "One acquisition probe per known blocked original URL; HTML success is not scorecard qualification.",
    }
    (args.output / "qualification.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
