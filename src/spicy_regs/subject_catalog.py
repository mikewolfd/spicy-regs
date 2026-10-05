"""Installed subject/receipt declarations without build-time imports in serving."""
from functools import lru_cache
from importlib.resources import files
import json

#: The one key a declaration may carry beside its ``DatasetPolicy.descriptor()``: ``"shared_log": true`` on a
#: receipt-only dataset that every family reading one source writes for itself (the acquisition and file-state
#: logs). Such a log has no owning family. Each family's rows are its own run history, stored in its own receipt
#: member and read back only through one of that family's subject datasets, so no publication index entry lists it
#: and nothing selects it by name. A retry checkpoint has the same policy shape and must not carry the marker: one
#: family owns a checkpoint, and its readers find it by name.
SHARED_LOG = "shared_log"


def is_shared_log(declared) -> bool:
    """Whether one installed declaration is a shared log; the marker on anything with a subject table is refused."""
    if SHARED_LOG not in declared:
        return False
    if declared[SHARED_LOG] is not True or declared["receipt_only"] is not True:
        raise ValueError(f"Only a receipt-only dataset can be declared a shared log: {declared['dataset']}")
    return True


@lru_cache(maxsize=1)
def _declarations():
    root = files("spicy_regs").joinpath("etl_policies")
    return {path.name.removesuffix(".json"): json.loads(path.read_text())
            for path in root.iterdir() if path.name.endswith(".json")}


@lru_cache(maxsize=1)
def descriptors():
    return {name: {key: value for key, value in declared.items() if key != SHARED_LOG}
            for name, declared in _declarations().items()}


@lru_cache(maxsize=1)
def shared_receipt_logs() -> frozenset[str]:
    """The receipt-only datasets several families write and none owns, from their declarations' marker."""
    return frozenset(name for name, declared in _declarations().items() if is_shared_log(declared))


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
