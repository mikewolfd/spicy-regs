"""Explicit installed dataset policies; family tasks own the descriptor files.

Each ``etl_policies/*.json`` is one DatasetPolicy.descriptor(). Registering a
policy makes receipt admission mandatory for new builds and publications of
that dataset. Historical generation reads continue to use their pinned policy.
"""
from importlib.resources import files
from collections.abc import Mapping
import json

from spicy_regs.etl_receipts import DatasetPolicy


def installed_policies() -> dict[str, DatasetPolicy]:
    root = files("spicy_regs").joinpath("etl_policies")
    if not root.is_dir():
        return {}
    policies = {}
    for path in sorted(root.iterdir(), key=lambda p: p.name):
        if path.name.endswith(".json"):
            policy = DatasetPolicy.from_descriptor(json.loads(path.read_text()))
            if path.name != policy.dataset + ".json" or policy.dataset in policies:
                raise ValueError("Installed field policy filename or identity differs")
            policies[policy.dataset] = policy
    return policies


def require_registered_receipts(tables, receipt_spec: Mapping | None) -> None:
    descriptors = receipt_spec["policies"] if receipt_spec is not None else []
    declared = {p["dataset"]: p for p in descriptors}
    if len(declared) != len(descriptors):
        raise ValueError("Receipt policies contain duplicate datasets")
    required = {name: p for name, p in installed_policies().items()
                if name + ".parquet" in tables or name in declared}
    if not required:
        return
    if receipt_spec is None:
        raise ValueError(f"Registered datasets require ETL receipts: {sorted(required)}")
    for name, policy in required.items():
        if declared.get(name) != policy.descriptor():
            raise ValueError(f"New generation differs from installed field policy: {name}")
