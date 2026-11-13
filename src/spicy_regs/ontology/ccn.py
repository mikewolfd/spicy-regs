"""Apply the pinned RefSpec CCN definition without importing a sibling checkout."""
import hashlib
from importlib.resources import files
import json
import re

DEFINITION_SHA256 = "143472e11b719cd424b449704a2c5c40f02330cc0a9df547abde3aba0eb48f87"


class NppesIdentifierError(ValueError):
    """The RefSpec CCN definition refuses the supplied identifier."""


def _definition():
    body = files("spicy_regs").joinpath("reference/refspec/ccn-definition.json").read_bytes()
    if hashlib.sha256(body).hexdigest() != DEFINITION_SHA256:
        raise ValueError("RefSpec CCN definition hash mismatch")
    return json.loads(body)["definition"]


def validate_ccn(value: str) -> str:
    definition = _definition()
    if not isinstance(value, str) or not value.strip():
        raise NppesIdentifierError(definition["empty_error"])
    text = value.strip()
    if re.fullmatch(definition["pattern"], text, flags=definition["flags"]) is None:
        raise NppesIdentifierError(definition["shape_error"])
    return text
