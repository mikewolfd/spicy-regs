"""Bind compact filing definitions to existing retained workbook facts.

No source rows are invented for worksheets. A definition has one identity and
collection-context witnesses; financial rows reference that identity by key.
"""

import json
import re

import pyarrow as pa

from .fec_filing_financial import WORKBOOK, FilingMapping, _json, load_layout
from .fec_query import _digest

DEFINITION_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in "record_id family version form workbook_sha256 layout_json".split()
    ]
)


def _workbook_digest(context):
    capture = context.get("source_capture", {})
    return capture.get("member_sha256") if capture.get("member") is not None else capture.get("original_sha256")


def bind_definition(mapping: FilingMapping, contexts, source_generation_pin):
    """Verify definition labels and rule cells against pinned context facts.

    ``contexts`` maps sealed collection IDs to their callerContext.facts objects.
    Release validation separately verifies the collections against the sealed
    generation. Specification examples are retained but do not validate a filing.
    """
    _digest(source_generation_pin)
    layout = load_layout(mapping)
    facts = []
    sheets = {}
    for collection_id, context in contexts.items():
        if _workbook_digest(context) != mapping.workbook_sha256:
            raise ValueError("Filing context has a different workbook digest")
        for i, fact in enumerate(context["parsing"]["facts"]):
            facts.append((collection_id, i, fact))
            if fact["kind"] == "worksheet":
                ordinal, title = fact["sheet_ordinal"], fact["title"]
                if ordinal in sheets and sheets[ordinal] != title:
                    raise ValueError("Filing workbook has conflicting worksheet identities")
                sheets[ordinal] = title
    cells = {}
    for collection_id, i, fact in facts:
        if fact["kind"] != "cell":
            continue
        sheet = sheets.get(fact["sheet_ordinal"])
        if sheet is None:
            raise ValueError("Filing workbook cell has no worksheet witness")
        key = (sheet, fact["coordinate"])
        if key in cells:
            raise ValueError("Filing workbook cell has ambiguous context witnesses")
        cells[key] = (collection_id, i, fact)
    used = {}

    def require(sheet, coordinate, value):
        witness = cells.get((sheet, coordinate))
        if witness is None or witness[2].get("value") != value:
            raise ValueError(f"Filing definition cell differs from retained facts: {sheet}!{coordinate}")
        cid, index, _ = witness
        used.setdefault(cid, []).append(dict(sheet=sheet, cell=coordinate, fact_index=index))

    if layout.get("version_cell") == "HDR!E7":
        require("HDR", "E7", layout["version"])
    else:
        require(layout["sheet"], layout["version_cell"], layout["version"])
        require(layout["sheet"], layout["form_cell"], layout["form"])
    for field in layout["fields"]:
        require(layout["sheet"], field["cell"], field.get("formula", field["label"]))
        if "specification" not in field:
            continue
        match = re.fullmatch(r"B([0-9]+)", field["cell"])
        if match is None:
            raise ValueError("Filing specification label must have an exact column-B cell")
        row = int(match[1])
        for offset, value in enumerate(field["specification"]):
            # Text holds types, requirements, date spellings, rules and form
            # associations. Numeric examples are not interpretation rules.
            if isinstance(value, str):
                require(layout["sheet"], chr(ord("C") + offset) + str(row), value)
    definition = dict(
        record_id=mapping.layout_pin,
        family=layout["family"],
        version=layout["version"],
        form=layout["form"],
        workbook_sha256=mapping.workbook_sha256,
        layout_json=_json(layout),
    )
    evidence = []
    for cid, locators in sorted(used.items()):
        evidence.append(
            dict(
                target_table="fec_filing_definitions",
                target_record_id=mapping.layout_pin,
                target_generation_scope="self",
                witness_generation_scope="external",
                witness_generation_pin=source_generation_pin,
                role="field_definition",
                endpoint_kind="collection_context",
                collection_id=cid,
                source_record_id=None,
                context_column="collection_outcome_json",
                context_pointer="/receiverDisposition/callerContext/facts/parsing/facts",
                witness_sha256=mapping.workbook_sha256,
                witness_locator_json=_json(dict(cells=locators)),
            )
        )
    return definition, evidence


def definition_contexts_from_collections(rows, *, workbook_sha256=WORKBOOK):
    """Read caller contexts from sealed fec_collections, not workstation paths."""
    contexts = {}
    for row in rows:
        outcome = json.loads(row["collection_outcome_json"])
        context = (outcome.get("receiverDisposition") or {}).get("callerContext", {}).get("facts")
        if context and _workbook_digest(context) == workbook_sha256:
            if row["collection_id"] in contexts:
                raise ValueError("Duplicate filing definition collection")
            contexts[row["collection_id"]] = context
    return contexts
