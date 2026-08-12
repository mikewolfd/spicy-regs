"""Read the pinned Rulespec schema bytes through a product-local reader.

The three schemas Rulespec Core owns are tracked byte-identically under
``spicy_regs/fixtures/rulespec/source-catalog-release-v1/`` beside ``pins.json``,
which records each file's ``$id``, byte size, SHA-256, and the schema-set
identity those three bytes derive.  This module reads the files and re-checks
every digest before handing anything to a validator, so a drifted copy fails at
load rather than at some later verdict.

This is a data crossing under REF-024: copied bytes, read locally, following
``fixtures/rulespec-core-release-v1.json``.  Nothing here imports `rulespec`,
and nothing here resolves a path into a sibling checkout.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from spicy_regs.source_catalog.universe import SourceCatalogError, canonical_digest

PINNED_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "rulespec" / "source-catalog-release-v1"
PINS_FILE = PINNED_SCHEMA_DIR / "pins.json"

# Schema roles, not member roles.  Each resolves to exactly one schema.
SCHEMA_ROLES: tuple[str, ...] = ("release-root", "member-manifest", "source-items")


@dataclass(frozen=True)
class PinnedSchema:
    """One pinned schema: its role, tracked file name, ``$id``, and bytes."""

    role: str
    file_name: str
    schema_id: str
    payload: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()

    @property
    def document(self) -> dict[str, Any]:
        return json.loads(self.payload.decode("utf-8"))

    def descriptor(self) -> dict[str, Any]:
        return {"roles": [self.role], "schemaId": self.schema_id, "schemaSha256": self.sha256}


@lru_cache(maxsize=1)
def pinned_schemas() -> dict[str, PinnedSchema]:
    """Load, digest-check, and return every pinned schema keyed by role."""

    try:
        pins = json.loads(PINS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise SourceCatalogError(f"pinned schema record is missing or unreadable: {PINS_FILE} ({error})") from error
    if not isinstance(pins, dict) or not isinstance(pins.get("schemas"), list):
        raise SourceCatalogError(f"pinned schema record is malformed: {PINS_FILE}")

    loaded: dict[str, PinnedSchema] = {}
    for entry in pins["schemas"]:
        role = entry.get("role")
        if role not in SCHEMA_ROLES:
            raise SourceCatalogError(f"pinned schema record names an unknown role {role!r}")
        path = PINNED_SCHEMA_DIR / str(entry.get("fileName"))
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise SourceCatalogError(f"pinned schema is absent: {path} ({error})") from error
        observed = hashlib.sha256(payload).hexdigest()
        if observed != entry.get("sha256") or len(payload) != entry.get("byteSize"):
            raise SourceCatalogError(
                f"pinned schema {path.name} drifted from pins.json: expected {entry.get('sha256')} of "
                f"{entry.get('byteSize')} bytes, read {observed} of {len(payload)}"
            )
        schema = PinnedSchema(role=role, file_name=path.name, schema_id=str(entry.get("schemaId")), payload=payload)
        if schema.document.get("$id") != schema.schema_id:
            raise SourceCatalogError(f"pinned schema {path.name} carries a $id pins.json does not name")
        loaded[role] = schema

    missing = sorted(set(SCHEMA_ROLES) - set(loaded))
    if missing:
        raise SourceCatalogError(f"pinned schema record is missing roles: {missing}")
    derived = schema_set_identity(loaded)
    if derived != pins.get("schemaSetId"):
        raise SourceCatalogError(
            f"pinned schema set derives {derived}, but pins.json records {pins.get('schemaSetId')}"
        )
    return loaded


def schema_descriptors(schemas: dict[str, PinnedSchema] | None = None) -> list[dict[str, Any]]:
    """The ``schemaSet.schemas`` descriptors, sorted by ``schemaId``."""

    schemas = schemas or pinned_schemas()
    return sorted(
        (schemas[role].descriptor() for role in SCHEMA_ROLES),
        key=lambda descriptor: descriptor["schemaId"],
    )


def schema_set_identity(schemas: dict[str, PinnedSchema] | None = None) -> str:
    """``urn:spicy:schema-set:v1:<digest over the sorted descriptors>``."""

    return f"urn:spicy:schema-set:v1:{canonical_digest(schema_descriptors(schemas))}"


def pinned_schema_bytes(role: str) -> bytes:
    """The exact tracked bytes of one pinned schema."""

    try:
        return pinned_schemas()[role].payload
    except KeyError:
        raise SourceCatalogError(f"unknown schema role {role!r}") from None


def pinned_schema_document(role: str) -> dict[str, Any]:
    return pinned_schemas()[role].document


def schema_id(role: str) -> str:
    return pinned_schemas()[role].schema_id


__all__ = [
    "PINNED_SCHEMA_DIR",
    "PINS_FILE",
    "SCHEMA_ROLES",
    "PinnedSchema",
    "pinned_schema_bytes",
    "pinned_schema_document",
    "pinned_schemas",
    "schema_descriptors",
    "schema_id",
    "schema_set_identity",
]
