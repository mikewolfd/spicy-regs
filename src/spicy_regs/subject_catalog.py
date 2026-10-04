"""Installed subject/receipt declarations without build-time imports in serving."""
from functools import lru_cache
from importlib.resources import files
import json


@lru_cache(maxsize=1)
def descriptors():
    root = files("spicy_regs").joinpath("etl_policies")
    return {path.name.removesuffix(".json"): json.loads(path.read_text())
            for path in root.iterdir() if path.name.endswith(".json")}


@lru_cache(maxsize=1)
def policies():
    from .etl_policy_registry import installed_policies
    return installed_policies()


def subject_tables(declared):
    registry = descriptors()
    return tuple(name for name in dict.fromkeys((*declared, *sorted(registry)))
                 if name not in registry or not registry[name]["receipt_only"])


def receipt_datasets():
    return tuple(name for name, policy in descriptors().items() if policy["receipt_only"])
