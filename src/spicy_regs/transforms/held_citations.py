"""Read explicitly selected held fields into the existing shared citation table.

Each complete field is its own text scope. No section is called a complete
document, no comment attachment is implied by inline text, and no source body
is acquired here. Successful zero-result reads replace only that kind/key/text/
rule scope; refused reads leave prior findings and checkpoints untouched.
"""

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import shutil
from threading import Timer
from typing import Any, Mapping, Sequence

import duckdb
from spicy_docs.interpretation.citations import CITATION_RULES_BY_NAME, DOCUMENT_CITATION_KINDS, find_citations
from spicy_docs.schemas.document_citation_tables import DocumentProvenance, shape_document_citation

from spicy_regs.citation_sources import TEXT_SOURCES, document_key
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path, published_table

NAMESPACE = "held-citations-explicit-context"
ADAPTER_VERSION = "held-fields/1"
MAX_SELECTIONS = 100
MAX_FIELD_BYTES = 4 * 1024 * 1024
MAX_TOTAL_FIELD_BYTES = 32 * 1024 * 1024
# Retained report fields expose line-wrapped committee names that the shared
# name grammar truncates. Keep this adapter's default admission to qualified
# citation forms; callers may explicitly retain unresolved committee evidence.
DEFAULT_RULES = tuple(kind for kind in DOCUMENT_CITATION_KINDS if kind != "committee_name")


@dataclass(frozen=True)
class Selection:
    kind: str
    keys: tuple[str, ...]

    @property
    def key(self) -> str:
        return document_key(self.kind, self.keys)


def parse_selections(values: list[dict]) -> tuple[Selection, ...]:
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_SELECTIONS:
        raise ValueError(f"Select between 1 and {MAX_SELECTIONS} held fields")
    selections = []
    for value in values:
        if not isinstance(value, dict) or set(value) != {"kind", "keys"}:
            raise ValueError("Each selection requires only kind and keys")
        kind = value["kind"]
        if kind not in TEXT_SOURCES or not isinstance(value["keys"], list):
            raise ValueError("Unsupported source kind or non-list keys")
        selection = Selection(kind, tuple(value["keys"]))
        selection.key
        if selection in selections:
            raise ValueError("Repeated held-field selection")
        selections.append(selection)
    return tuple(selections)


def _read_field(cursor, selection: Selection, max_field_bytes: int) -> tuple[str | None, str]:
    spec = TEXT_SOURCES[selection.kind]
    where = " AND ".join(f'CAST("{key}" AS VARCHAR) = ?' for key in spec.keys)
    rows = cursor.execute(
        f'SELECT octet_length(encode("{spec.field}")), '
        f'CASE WHEN octet_length(encode("{spec.field}")) <= ? THEN "{spec.field}" END '
        f'FROM "{spec.table}" WHERE {where} LIMIT 2',
        [max_field_bytes, *selection.keys],
    ).fetchall()
    if len(rows) != 1:
        return None, "missing" if not rows else "ambiguous_source"
    size, text = rows[0]
    if size is None:
        return None, "unread_null"
    if size > max_field_bytes:
        return None, "field_byte_cap"
    if not isinstance(text, str):
        return None, "unsupported_field"
    return text, "complete_field"


def build_held_citations(
    output_dir: Path, *, cursor, selections: Sequence[Selection],
    input_pins: Mapping[str, Mapping[str, Any]], download_prior,
    kinds: tuple[str, ...] | None = None, max_field_bytes: int = MAX_FIELD_BYTES, evidence=None,
) -> Path:
    """Re-extract bounded explicit fields, retaining unrelated citations and failed scopes."""
    if not 0 < len(selections) <= MAX_SELECTIONS or not 0 < max_field_bytes <= MAX_FIELD_BYTES:
        raise ValueError("Held-field selection or byte bound exceeded")
    kinds = DEFAULT_RULES if kinds is None else kinds
    if not kinds or len(set(kinds)) != len(kinds) or set(kinds) - CITATION_RULES_BY_NAME.keys():
        raise ValueError("Select distinct supported citation rules")
    for selection in selections:
        selection.key
        table = TEXT_SOURCES[selection.kind].table
        pin = input_pins.get(table, {})
        if not any(isinstance(pin.get(key), str) and re.fullmatch(r"sha256:[0-9a-f]{64}", pin[key])
                   for key in ("artifactDigest", "sha256")):
            raise ValueError(f"{table} requires an explicit immutable input pin")
    if len(set(selections)) != len(selections):
        raise ValueError("Repeated held-field selection")
    output_dir.mkdir(parents=True, exist_ok=True)
    current = output_dir / "document_citations.parquet"
    if current.exists():
        shutil.copyfile(current, prior_scratch_path(output_dir, "document_citations"))
    prior = published_table(output_dir, "document_citations", download_prior)
    states = {(s.get("document_kind"), s.get("document_key"), s.get("text_sha256")): s
              for s in read_checkpoints(prior, NAMESPACE)}
    processing = json.dumps({"adapter": ADAPTER_VERSION, "bill_congress_policy": "explicit_only",
                             "rules": {kind: CITATION_RULES_BY_NAME[kind].version for kind in kinds}}, sort_keys=True)
    rows, replaced, receipts = [], set(), []
    bytes_read = 0
    for selection in selections:
        spec = TEXT_SOURCES[selection.kind]
        receipt: dict[str, Any] = {"document_kind": selection.kind, "document_key": selection.key,
                   "source_table": spec.table, "source_field": spec.field,
                   "source_keys": dict(zip(spec.keys, selection.keys, strict=True)),
                   "input_pin": dict(input_pins[spec.table]), "processing_version": processing}
        if bytes_read >= MAX_TOTAL_FIELD_BYTES:
            text, status = None, "run_byte_cap"
        else:
            timer = Timer(90, cursor.interrupt)
            timer.start()
            try:
                text, status = _read_field(cursor, selection, min(max_field_bytes, MAX_TOTAL_FIELD_BYTES - bytes_read))
            except duckdb.Error as error:
                text, status = None, "source_read_failure"
                receipt["error_type"] = type(error).__name__
            finally:
                timer.cancel()
        receipt["status"] = status
        if text is not None:
            encoded = text.encode()
            bytes_read += len(encoded)
            text_sha = "sha256:" + hashlib.sha256(encoded).hexdigest()
            if evidence is not None:
                evidence.store.put_blob(text_sha, len(encoded), [encoded])
            receipt["text_sha256"] = text_sha
            identity = (selection.kind, selection.key, text_sha)
            state = {**receipt, "text_sha256": text_sha, "scope": "complete selected field only"}
            previous = states.get(identity)
            if previous and all(previous.get(key) == value for key, value in state.items()):
                receipt.update(status="unchanged_complete_field", findings=previous.get("findings"))
            else:
                findings = find_citations(text, kinds=kinds, congress=None, bill_congress_policy="explicit_only")
                provenance = DocumentProvenance(selection.key, selection.kind, spec.rendition,
                                                f"literal-held-field:{spec.table}.{spec.field}/{ADAPTER_VERSION}", text_sha)
                rows.extend(shape_document_citation(f, provenance) for f in findings)
                replaced.update((*identity, kind) for kind in kinds)
                states[identity] = {**state, "findings": len(findings)}
                receipt.update(text_sha256=text_sha, findings=len(findings))
        receipts.append(receipt)
        if evidence is not None:
            evidence.event("held-citation-read", **receipt)
    # Retain a human-readable receipt for failures and caps; successful zeroes
    # are also checkpoints inside the exact published citation file.
    (output_dir / "held-citation-reads.json").write_text(json.dumps(receipts, indent=2) + "\n")
    return merge_contract_table(
        output_dir, "document_citations", rows, download_prior=download_prior,
        prior_present=prior is not None,
        replace_parents=(("document_kind", "document_key", "text_sha256", "rule_name"), replaced),
        parquet_metadata=checkpoint_metadata(prior, NAMESPACE, states.values()),
    )
