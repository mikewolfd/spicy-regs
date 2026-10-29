"""Read native public shards as familiar records for existing semantic assertions."""

import json
from pathlib import Path


def read_file_page(path):
    path = Path(path)
    if path.suffix != ".parquet":
        return json.loads(path.read_text())
    import pyarrow.parquet as pq

    from spicy_regs.source_catalog.file_inventory import validate

    body = path.read_bytes()
    metadata = pq.ParquetFile(path).metadata.metadata
    generation = metadata[b"spicy_catalog.generation"].decode()
    dataset = metadata[b"spicy_catalog.dataset_id"].decode()
    inventory = metadata[b"spicy_catalog.inventory_kind"].decode()
    records = validate(body, generation=generation, dataset_id=dataset, inventory_kind=inventory)
    files = []
    for row in records:
        provenance = {
            "basis": row["provenance_basis"],
            "sourceUrls": [row["source_url"]] if row["source_url"] else row["source_urls"] or [],
            "availability": row["availability"],
        }
        if row["captured_at"] is not None:
            provenance["capturedAt"] = row["captured_at"].isoformat().replace("+00:00", "Z")
        if row["receipt_sha256"] is not None:
            provenance["receipt"] = {"sha256": "sha256:" + row["receipt_sha256"].hex()}
            if row["receipt_entry"] is not None:
                provenance["receipt"]["entry"] = row["receipt_entry"]
        if row["original_receipt_sha256"] is not None:
            provenance["originalReceiptSha256"] = "sha256:" + row["original_receipt_sha256"].hex()
        item = {"id": row["alias_id"], "name": row["name"], "status": row["status"], "provenance": provenance}
        if row["route_id"] is not None:
            item["routeId"] = row["route_id"]
        if row["content_sha256"] is not None:
            item["sha256"] = "sha256:" + row["content_sha256"].hex()
        if row["original_bytes"] is not None:
            item["bytes"] = row["original_bytes"]
        files.append(item)
    return {
        "format": "spicy-source-files",
        "version": 3,
        "generation": generation,
        "collectionId": dataset,
        "files": files,
    }
