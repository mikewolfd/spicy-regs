"""Ordered receipt membership shared by artifacts, publication and selected readers."""

import re


KEY = "etl_receipts.parquet"
DIRECTORY = "etl_receipts"


def is_receipt_member_key(key):
    return isinstance(key, str) and re.fullmatch(r"etl_receipts/[A-Za-z0-9_-]+\.parquet", key) is not None


def receipt_paths(value):
    """Normalize a retained monolith or ordered local members without copying bytes."""
    return tuple(value) if isinstance(value, (tuple, list)) else (value,)


def receipt_declarations(spec, *, pinned=False):
    """Validate the membership shape; payload admission remains the caller's job."""
    if spec is None:
        return ()
    if "version" not in spec and "members" not in spec:
        if spec.get("key") != KEY:
            raise ValueError("Invalid receipt member key")
        return (spec,)
    if type(spec.get("version")) is not int or spec["version"] != 2 or "key" in spec:
        raise ValueError("Invalid receipt membership version")
    if "keyIndex" in spec:
        raise ValueError("Split receipt indexing is unsupported")
    members = spec.get("members")
    if not isinstance(members, list) or not members:
        raise ValueError("Receipt membership requires ordered members")
    keys = set()
    fields = {"key", "rows", "sha256", "byteSize"} if pinned else {"key", "rows"}
    for member in members:
        if not isinstance(member, dict) or set(member) != fields:
            raise ValueError("Invalid receipt member declaration")
        key = member["key"]
        if not is_receipt_member_key(key) or key in keys or type(member["rows"]) is not int or member["rows"] < 0:
            raise ValueError("Invalid or duplicate receipt member")
        if pinned and (
            not isinstance(member["sha256"], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", member["sha256"])
            or type(member["byteSize"]) is not int
            or member["byteSize"] < 0
        ):
            raise ValueError("Invalid receipt member pin")
        keys.add(key)
    if type(spec.get("rows")) is not int or sum(member["rows"] for member in members) != spec["rows"]:
        raise ValueError("Receipt member rows differ from the complete selection")
    return tuple(members)


def receipt_keys(spec, *, pinned=False):
    return tuple(member["key"] for member in receipt_declarations(spec, pinned=pinned))
