"""Read explicitly selected held fields into the existing shared citation table.

Each complete field is its own text scope. No section is called a complete
document, no comment attachment is implied by inline text, and no source body
is acquired here. Successful zero-result reads replace only that kind/key/text/
rule scope; refused reads leave prior findings and checkpoints untouched.

Every successful read is a checkpoint in ``document_citations``' metadata, and
``document_citation_reads`` (:func:`write_citation_reads`) publishes them as
rows, so a field read with no citation is told from one never read.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
from threading import Timer
from typing import Any, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from spicy_docs.interpretation.citations import (
    CITATION_RULE_SET_VERSION,
    CITATION_RULES_BY_NAME,
    DOCUMENT_CITATION_KINDS,
    find_citations,
)
from spicy_docs.schemas.document_citation_tables import DocumentProvenance, shape_document_citation

from spicy_regs.citation_sources import TEXT_SOURCES, document_key
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path, published_table

NAMESPACE = "held-citations-explicit-context"
#: One row per held-field read the citation table's checkpoints record: the field, the text read, the citation rule
#: set the read ran under, when, and how many citation rows it wrote (0 for a read that found none).
READS_TABLE = "document_citation_reads"
READS_COLUMNS = ("document_kind", "document_key", "text_sha256", "rule_set_version", "read_at", "citation_rows",
                 "source_table", "input_family", "input_generation", "input_sha256")
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


def _interruptible(cursor, sql: str, parameters: list) -> list[tuple]:
    timer = Timer(90, cursor.interrupt)
    timer.start()
    try:
        return cursor.execute(sql, parameters).fetchall()
    finally:
        timer.cancel()


def _selected_rows(cursor, kind: str, selected: Sequence[tuple[int, Selection]], column_sql: str) -> list[tuple]:
    """One scan of a kind's source table for every selection: (ordinal, column) per matching row.

    The join casts each selected key to its column's native type, so the scan
    filters the column itself; the literal comparison afterwards keeps the exact
    spelling rule (a selected ``"007"`` never matches ``seq`` 7).
    """
    spec = TEXT_SOURCES[kind]
    types = {row[0]: row[1] for row in cursor.execute(f'DESCRIBE "{spec.table}"').fetchall()}
    names = ", ".join(f"k{i}" for i in range(len(spec.keys)))
    values = ", ".join("(" + ", ".join(["?"] * (len(spec.keys) + 1)) + ")" for _ in selected)
    joined = " AND ".join(f't."{c}" = TRY_CAST(s.k{i} AS {types[c]})' for i, c in enumerate(spec.keys))
    exact = " AND ".join(f'CAST(t."{c}" AS VARCHAR) = s.k{i}' for i, c in enumerate(spec.keys))
    return _interruptible(
        cursor,
        f'SELECT s.ordinal, {column_sql} FROM (VALUES {values}) AS s(ordinal, {names}) '
        f'JOIN "{spec.table}" t ON {joined} WHERE {exact}',
        [value for ordinal, selection in selected for value in (ordinal, *selection.keys)],
    )


def _read_fields(cursor, selections: Sequence[Selection], max_field_bytes: int) -> list[tuple[str | None, str, str | None]]:
    """Read every selected field with two scans per source kind, in selection order.

    Returns ``(text, status, error_type)`` per selection. Sizes are read first;
    the run byte budget is then applied in selection order exactly as a
    one-field-at-a-time read would, and only admitted fields' text is fetched,
    so memory stays within ``MAX_TOTAL_FIELD_BYTES``. A read failure fails that
    kind's selections.
    """
    by_kind: dict[str, list[tuple[int, Selection]]] = {}
    for ordinal, selection in enumerate(selections):
        by_kind.setdefault(selection.kind, []).append((ordinal, selection))
    sizes: dict[int, list] = {ordinal: [] for ordinal in range(len(selections))}
    failed: dict[str, str] = {}
    for kind, selected in by_kind.items():
        try:
            for ordinal, size in _selected_rows(cursor, kind, selected,
                                                f'octet_length(encode(t."{TEXT_SOURCES[kind].field}"))'):
                sizes[ordinal].append(size)
        except duckdb.Error as error:
            failed[kind] = type(error).__name__
    results: list[tuple[str | None, str, str | None]] = []
    admitted: dict[str, list[tuple[int, Selection]]] = {}
    bytes_read = 0
    for ordinal, selection in enumerate(selections):
        if bytes_read >= MAX_TOTAL_FIELD_BYTES:
            results.append((None, "run_byte_cap", None))
        elif selection.kind in failed:
            results.append((None, "source_read_failure", failed[selection.kind]))
        elif len(sizes[ordinal]) != 1:
            results.append((None, "missing" if not sizes[ordinal] else "ambiguous_source", None))
        elif sizes[ordinal][0] is None:
            results.append((None, "unread_null", None))
        elif sizes[ordinal][0] > min(max_field_bytes, MAX_TOTAL_FIELD_BYTES - bytes_read):
            results.append((None, "field_byte_cap", None))
        else:
            bytes_read += sizes[ordinal][0]
            admitted.setdefault(selection.kind, []).append((ordinal, selection))
            results.append((None, "complete_field", None))
    for kind, selected in admitted.items():
        try:
            texts = dict(_selected_rows(cursor, kind, selected, f't."{TEXT_SOURCES[kind].field}"'))
        except duckdb.Error as error:
            texts, error_type = {}, type(error).__name__
        else:
            error_type = None
        for ordinal, _ in selected:
            text = texts.get(ordinal)
            if error_type is not None or ordinal not in texts:
                results[ordinal] = (None, "source_read_failure", error_type or "SourceChangedDuringRead")
            elif not isinstance(text, str):
                results[ordinal] = (None, "unsupported_field", None)
            elif len(text.encode()) != sizes[ordinal][0]:
                results[ordinal] = (None, "source_read_failure", "SourceChangedDuringRead")
            else:
                results[ordinal] = (text, "complete_field", None)
    return results


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
    rows, replaced, receipts = [], set(), []
    reads = _read_fields(cursor, selections, max_field_bytes)
    for selection, (text, status, error_type) in zip(selections, reads, strict=True):
        spec = TEXT_SOURCES[selection.kind]
        selected_kinds = tuple(kind for kind in kinds if kind not in spec.excluded_rules)
        if not selected_kinds:
            raise ValueError("No selected citation rules are qualified for this source")
        processing = json.dumps({"adapter": ADAPTER_VERSION, "bill_congress_policy": "explicit_only",
                                 "rules": {kind: CITATION_RULES_BY_NAME[kind].version for kind in selected_kinds},
                                 "excluded_source_rules": spec.excluded_rules}, sort_keys=True)
        receipt: dict[str, Any] = {"document_kind": selection.kind, "document_key": selection.key,
                   "source_table": spec.table, "source_field": spec.field,
                   "source_keys": dict(zip(spec.keys, selection.keys, strict=True)),
                   "input_pin": dict(input_pins[spec.table]), "processing_version": processing}
        if error_type is not None:
            receipt["error_type"] = error_type
        receipt["status"] = status
        if text is not None:
            encoded = text.encode()
            text_sha = "sha256:" + hashlib.sha256(encoded).hexdigest()
            if evidence is not None:
                evidence.retain_bytes(encoded, stage="held-citation-field",
                                      document_kind=selection.kind, document_key=selection.key,
                                      source_table=spec.table, source_field=spec.field)
            receipt["text_sha256"] = text_sha
            identity = (selection.kind, selection.key, text_sha)
            state = {**receipt, "text_sha256": text_sha, "scope": "complete selected field only"}
            previous = states.get(identity)
            if previous and all(previous.get(key) == value for key, value in state.items()):
                receipt.update(status="unchanged_complete_field", findings=previous.get("findings"))
            else:
                findings = find_citations(text, kinds=selected_kinds, congress=None, bill_congress_policy="explicit_only")
                provenance = DocumentProvenance(selection.key, selection.kind, spec.rendition,
                                                f"{spec.derivation}:{spec.table}.{spec.field}/{ADAPTER_VERSION}", text_sha)
                rows.extend(shape_document_citation(f, provenance) for f in findings)
                replaced.update((*identity, kind) for kind in kinds)
                # Beside the read's identity, never part of it: when it ran and under which rule set.
                states[identity] = {**state, "findings": len(findings), "rule_set_version": CITATION_RULE_SET_VERSION,
                                    "read_at": datetime.now(UTC).isoformat(timespec="seconds")}
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


def write_citation_reads(output_dir: Path, citations: Path) -> Path:
    """``document_citation_reads.parquet``: each held-field read ``citations``' checkpoints record, one row per read.

    Derived whole from the citation table's own checkpoints, so the two never disagree; a read checkpointed before
    ``read_at`` and ``rule_set_version`` were kept states both NULL. A refused or capped read writes no checkpoint and
    so no row: absence here is "not read". ``citation_rows`` counts the rows that read wrote, and the ``input_*``
    columns the source table's generation the read was pinned to (NULL where the pin names no such part). O(reads).
    """
    def text(value: Any) -> str | None:
        return None if value is None else str(value)

    def row(state: Mapping[str, Any]) -> dict[str, str | None]:
        pin = state.get("input_pin") or {}
        return {"document_kind": text(state.get("document_kind")), "document_key": text(state.get("document_key")),
                "text_sha256": text(state.get("text_sha256")), "rule_set_version": text(state.get("rule_set_version")),
                "read_at": text(state.get("read_at")), "citation_rows": text(state.get("findings")),
                "source_table": text(state.get("source_table")), "input_family": text(pin.get("family")),
                "input_generation": text(pin.get("artifactDigest")), "input_sha256": text(pin.get("sha256"))}

    states = read_checkpoints(citations, NAMESPACE)
    rows = sorted(
        (row(state) for state in states),
        key=lambda row: (row["document_kind"] or "", row["document_key"] or "", row["text_sha256"] or ""),
    )
    schema = pa.schema([(column, pa.string()) for column in READS_COLUMNS])
    out = output_dir / f"{READS_TABLE}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out, compression="zstd")
    return out
