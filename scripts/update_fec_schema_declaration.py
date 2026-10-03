"""Refresh supported FEC producer schemas without claiming a new data qualification.

Run from the checkout after a reviewed producer change. Existing receipt pins
remain labelled baseline evidence; generation/publication has its own checks.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
RESOURCE = ROOT / "data_dictionary/fec_typed_schemas.json"


def refresh():
    document = json.loads(RESOURCE.read_text())
    schemas = {}
    producers = {}
    for module_name in (
        "fec_agency",
        "fec_research_context",
        "fec_legal",
        "fec_identity_observations",
        "fec_committee_observations",
        "fec_candidate_observations",
    ):
        module = importlib.import_module("spicy_regs.transforms." + module_name)
        schemas.update(module.SCHEMAS)
        source = ROOT / "src/spicy_regs/transforms" / (module_name + ".py")
        producers[str(source.relative_to(ROOT))] = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    from spicy_regs.transforms import fec_filing_forms as forms, fec_committee_history_observations as history
    from spicy_regs.transforms.fec_query import SOURCE_TEXT
    from spicy_docs.schemas.fec_committee_history import FEC_COMMITTEE_HISTORY

    schemas.update(
        fec_filing_report_observations=forms.FILING_REPORT_SCHEMA, fec_filing_text_observations=forms.FILING_TEXT_SCHEMA
    )
    for table in (history.MASTER, history.POSTGRES):
        entry = document["tables"][table]
        if table == history.MASTER:
            native_columns = list(FEC_COMMITTEE_HISTORY.columns)
        elif "source_schema_columns" in entry:
            native_columns = entry["source_schema_columns"]
        else:
            old = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(entry["arrow_schema_ipc_base64"])))
            native_columns = [f.name for f in old.field("native_fields").type]
        entry["source_schema_columns"] = native_columns
        schemas[table] = pa.schema(
            [(n, pa.string()) for n in SOURCE_TEXT + ["source_url", "history_scope_status"]]
            + [("source_cycle", pa.int32())]
            + history._columns(native_columns)
        )
    with duckdb.connect() as con:
        for table, schema in schemas.items():
            con.register("schema_input", pa.Table.from_batches([], schema=schema))
            described = con.execute("DESCRIBE schema_input").fetchall()
            blob = schema.serialize().to_pybytes()
            document["tables"][table].update(
                columns=[[r[0], r[1]] for r in described],
                arrow_schema_ipc_base64=base64.b64encode(blob).decode(),
                arrow_schema_sha256="sha256:" + hashlib.sha256(blob).hexdigest(),
            )
    document["format_version"] = 2
    document["declares"] = (
        "Supported FEC producer schemas, including reviewed cleanup changes. "
        "Baseline receipt and source pins do not qualify these changed output schemas or publish data."
    )
    if "union_receipt_sha256" in document:
        document["baseline_union_receipt_sha256"] = document.pop("union_receipt_sha256")
    document["producer_code"].update(producers)
    for name in (
        "fec_query",
        "fec_filing_forms",
        "fec_committee_history_observations",
        "fec_identity_shape",
        "fec_context_shape",
    ):
        source = ROOT / "src/spicy_regs/transforms" / (name + ".py")
        document["producer_code"][str(source.relative_to(ROOT))] = (
            "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
        )
    RESOURCE.write_text(json.dumps(document, indent=2) + "\n")


if __name__ == "__main__":
    refresh()
