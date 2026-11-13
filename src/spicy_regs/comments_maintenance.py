"""Temporarily suspend R2 maintenance on the selected native Comments pair."""
from __future__ import annotations

import argparse
from contextlib import closing
from copy import deepcopy
import json
import os
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, regulatory_catalog

TABLES = ("comments", "etl_receipts")
FIELDS = {
    "compaction": ("state", "target_size_mb"),
    "snapshot_expiration": ("state", "max_snapshot_age", "min_snapshots_to_keep"),
}
FORMAT = "comments-maintenance-guard/1"


def _catalog() -> tuple[str, str]:
    uri = urlsplit(os.environ["R2_CATALOG_URI"])
    parts = uri.path.strip("/").split("/")
    if (uri.scheme != "https" or uri.netloc != "catalog.cloudflarestorage.com"
            or uri.query or uri.fragment or len(parts) != 2 or not all(parts)):
        raise ValueError("Expected an R2 catalog URI naming one account and bucket")
    return parts[0], parts[1]


def _lock() -> None:
    if not os.getenv(iceberg.CATALOG_LOCK_ENV):
        raise ValueError("Hold comments-catalog-write before changing or verifying maintenance")


def _selected() -> tuple[str, dict]:
    with closing(iceberg._connect()) as con:
        namespace = iceberg._selected_catalog_namespace(con, COMMENT)
        pins = {name: iceberg._catalog_metadata(con, namespace, name) for name in TABLES}
    return namespace, {name: {key: value[key] for key in (
        "table-uuid", "current-snapshot-id", "current-schema-id"
    )} for name, value in pins.items()}


def _uuid(namespace: str, table: str) -> str:
    with closing(iceberg._connect()) as con:
        return iceberg._catalog_metadata(con, namespace, table)["table-uuid"]


def _writable(config: dict) -> dict:
    # Scheduling observations are read-only. Never manufacture an enabled/default policy.
    result = {}
    for operation, fields in FIELDS.items():
        value = config.get(operation)
        if not isinstance(value, dict) or value.get("state") not in {"enabled", "disabled"}:
            raise ValueError("Both table maintenance states must be explicit before suspension")
        result[operation] = {key: deepcopy(value[key]) for key in fields if key in value}
    return result


def _save(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(state, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


class MaintenanceAPI:
    def __init__(self, account: str, bucket: str, client: httpx.Client):
        self.base = ("https://api.cloudflare.com/client/v4/accounts/" + quote(account, safe="")
                     + "/r2-catalog/" + quote(bucket, safe=""))
        self.client = client

    def request(self, method: str, namespace: str, table: str, config: dict | None = None) -> dict:
        url = (self.base + "/namespaces/" + quote(namespace, safe="") + "/tables/"
               + quote(table, safe="") + "/maintenance-configs")
        response = self.client.request(method, url, json=config)
        response.raise_for_status()
        body = response.json()
        if body.get("success") is not True or not isinstance(body.get("result"), dict):
            raise ValueError("R2 did not confirm the table maintenance request")
        result = body["result"]
        return result["maintenance_config"] if method == "GET" else result


def _load(path: Path) -> dict:
    state = json.loads(path.read_text())
    account, bucket = _catalog()
    if (state.get("format") != FORMAT or state.get("account") != account or state.get("bucket") != bucket
            or state.get("namespace") != regulatory_catalog.namespace()
            or set(state.get("tables", {})) != set(TABLES)):
        raise ValueError("Maintenance state does not name the selected native Comments pair")
    return state


def _disabled(config: dict) -> bool:
    return all(config.get(name, {}).get("state") == "disabled"
               and config[name].get("next_eligible_reason") != "running" for name in FIELDS)


def restore(path: Path, api: MaintenanceAPI) -> dict:
    """Restore both tables, continuing after one failure; retained state permits retry."""
    _lock()
    if not path.exists():
        return {"restored": True, "reason": "no maintenance change was started"}
    state = _load(path)
    errors = []
    for name in TABLES:
        entry = state["tables"][name]
        try:
            if _uuid(state["namespace"], name) != entry["pin"]["table-uuid"]:
                raise ValueError("Table UUID changed; refusing to restore a different table")
            api.request("POST", state["namespace"], name, entry["original_writable"])
            observed = api.request("GET", state["namespace"], name)
            if (_writable(observed) != entry["original_writable"]
                    or observed.get("interval") != entry["original"].get("interval")):
                raise ValueError("Prior maintenance configuration was not restored exactly")
            entry["restored_observation"] = observed
            entry["restored"] = True
            entry.pop("restore_error", None)
        except Exception as error:
            entry["restored"] = False
            entry["restore_error"] = type(error).__name__
            errors.append(name)
        _save(path, state)
    state["held"] = False
    state["restored"] = not errors
    _save(path, state)
    if errors:
        raise RuntimeError("Maintenance restoration needs retry for: " + ", ".join(errors))
    return state


def suspend(path: Path, api: MaintenanceAPI) -> dict:
    _lock()
    if path.exists():
        raise FileExistsError("Use a new maintenance state path; retain earlier recovery evidence")
    namespace, pins = _selected()
    if namespace != regulatory_catalog.namespace():
        raise ValueError("Maintenance may only guard the selected native Comments namespace")
    account, bucket = _catalog()
    state = {"format": FORMAT, "account": account, "bucket": bucket, "namespace": namespace,
             "held": False, "restored": False, "tables": {}}
    # Complete both read-only captures before changing either table.
    for name in TABLES:
        original = api.request("GET", namespace, name)
        state["tables"][name] = {"pin": pins[name], "original": original,
                                 "original_writable": _writable(original), "restored": False}
    _save(path, state)
    try:
        for name in TABLES:
            entry = state["tables"][name]
            entry["disable_attempted"] = True
            _save(path, state)  # A lost POST response still requires restoration.
            api.request("POST", namespace, name, {operation: {"state": "disabled"} for operation in FIELDS})
            observed = api.request("GET", namespace, name)
            expected = deepcopy(entry["original_writable"])
            for operation in FIELDS:
                expected[operation]["state"] = "disabled"
            if (not _disabled(observed) or _writable(observed) != expected
                    or observed.get("interval") != entry["original"].get("interval")):
                raise ValueError("Both maintenance operations must be disabled without changing policy")
            entry["disabled_observation"] = observed
            _save(path, state)
        state["held"] = True
        _save(path, state)
        verify(path, api)
    except BaseException:
        restore(path, api)
        raise
    return state


def verify(path: Path, api: MaintenanceAPI) -> dict:
    _lock()
    state = _load(path)
    if not state.get("held") or state.get("restored"):
        raise ValueError("Maintenance guard is not held")
    namespace, pins = _selected()
    if namespace != state["namespace"]:
        raise ValueError("Selected native namespace changed")
    for name in TABLES:
        entry = state["tables"][name]
        if pins[name]["table-uuid"] != entry["pin"]["table-uuid"]:
            raise ValueError("Selected native table UUID changed")
        observed = api.request("GET", namespace, name)
        expected = deepcopy(entry["original_writable"])
        for operation in FIELDS:
            expected[operation]["state"] = "disabled"
        if (not _disabled(observed) or _writable(observed) != expected
                or observed.get("interval") != entry["original"].get("interval")):
            raise ValueError("Selected table maintenance is not disabled")
    return state


def run(operation: str, path: Path) -> dict:
    account, bucket = _catalog()
    with httpx.Client(headers={"Authorization": "Bearer " + os.environ["R2_CATALOG_TOKEN"]},
                      timeout=30, follow_redirects=False) as client:
        return {"suspend": suspend, "restore": restore, "verify": verify}[operation](
            path, MaintenanceAPI(account, bucket, client))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("suspend", "verify", "restore"))
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.operation, args.state)
    print(json.dumps({"operation": args.operation, "held": result.get("held", False),
                      "restored": result.get("restored", False)}))


if __name__ == "__main__":
    main()
