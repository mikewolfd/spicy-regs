"""Private citation processing rows from the same selected native ETL receipts."""
from __future__ import annotations

from collections.abc import Mapping
import json
import hashlib

from spicy_regs.fec_receipt_adapter import receipt_owner

INPUT_RECORD = "_spicy_citation_inputs"
PROCESSING_PREFIX = "_spicy_citation_processing_"
SOURCE_DIGEST_PREFIX = "_spicy_citation_digests_"
PROCESSING_TABLES = ("document_citations", "document_citation_reads", "house_activity_reports", "budget_volumes")


def install_citation_inputs(connection, adapter, available):
    """Bind verified source rows before the server locks down file access.

    The public subject tables keep native columns. Only citation tool code can
    use these regular in-memory tables; request cursors share them and public
    SQL cannot name the private prefix.
    """
    from spicy_regs.citation_resolution import SOURCE_TABLES

    def selected(table):
        return table in adapter.local_native or receipt_owner(adapter.index, table) is not None

    if "document_citations" not in available or not selected("document_citations"):
        return
    import pyarrow as pa
    from spicy_regs.legislative_documents import field_registry, map_subject, native_document_key

    registry = field_registry()
    tables = {}
    pins = {}
    for table in PROCESSING_TABLES:
        if not selected(table) or (table != "document_citation_reads" and table not in available):
            continue
        schema = pa.schema([(field["name"], pa.string()) for field in registry[table]["fields"]])

        def original(row, dataset=table):
            raw = row.get("raw_source_row")
            if not isinstance(raw, Mapping):
                raise ValueError(f"{dataset}: citation source input is missing from the selected receipt")
            mapped = map_subject(dataset, raw)
            # A serving release must admit the future court body key before its
            # writer is activated. Both supported keys derive from the same
            # retained source row; all other subject fields still match exactly.
            if (dataset == "document_citations" and raw.get("document_kind") == "court_opinion_derived_pdf"
                    and mapped is not None):
                body_key = native_document_key(raw["document_kind"], raw.get("document_key"))
                mapped["document_key"] = body_key if row.get("document_key") == body_key else raw["document_key"]
            if mapped is not None and any(row.get(name) != value for name, value in mapped.items()):
                raise ValueError(f"{dataset}: citation source input differs from its selected native subject")
            result = dict(raw)
            if dataset == "document_citations":
                result["document_key"] = native_document_key(raw.get("document_kind"), row["document_key"])
            elif dataset == "document_citation_reads":
                result["document_key"] = native_document_key(raw.get("document_kind"), raw.get("document_key"))
            return result

        tables[table] = adapter.restore_originals(table, schema, original, prefix=PROCESSING_PREFIX)
        owner = receipt_owner(adapter.index, table)
        native = adapter.local_native.get(table)
        pins[table] = (
            {"status": "native_selected", "generation_id": native["generation_id"]}
            if native is not None else
            {"status": "published", "family": owner[0], "artifact_digest": owner[1]["artifactDigest"],
             "generation_id": owner[1]["etlReceipts"]["generationId"],
             "receipt_sha256": owner[1]["etlReceipts"]["sha256"]}
        )
    # Literal held fields also pass the exact subject/receipt join. Keep only
    # keys and checked digests so large text bodies are not duplicated in RAM.
    from spicy_regs.citation_sources import TEXT_SOURCES
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.subject_catalog import descriptors
    sources = {}
    for table in sorted({spec.table for spec in TEXT_SOURCES.values()} & set(available)):
        if not selected(table):
            continue
        subject_policy = DatasetPolicy.from_descriptor(descriptors()[table])
        specs = [spec for spec in TEXT_SOURCES.values() if spec.table == table]
        keys = tuple(dict.fromkeys(name for spec in specs for name in spec.keys))
        text_fields = tuple(dict.fromkeys(spec.field for spec in specs))
        fields = (*keys, *text_fields)
        schema = pa.schema([subject_policy.subject_schema.field(name) for name in keys] +
                           [pa.field("sha256_" + name, pa.string()) for name in text_fields])

        def held_digests(row, dataset=table, names=fields, key_fields=keys, texts=text_fields,
                         native_names=set(subject_policy.subject_schema.names)):
            missing = set(names) - native_names
            # Congress capture text such as a reconstructed Record entry stays
            # in source_fields; native legal/body fields use their subject value.
            original = row.get("source_fields") if missing and dataset == "house_communications" else {}
            if missing and not isinstance(original, Mapping):
                raise ValueError(f"{dataset}: held text is missing its selected source receipt")
            if missing - set(original):
                raise ValueError(f"{dataset}: held text has no declared native or receipt field")
            values = {name: row.get(name) if name in native_names else original[name] for name in names}
            return ({name: values[name] for name in key_fields} |
                    {"sha256_" + name: "sha256:" + hashlib.sha256(values[name].encode("utf-8")).hexdigest()
                     if values[name] is not None else None for name in texts})

        sources[table] = adapter.restore_originals(table, schema, held_digests, prefix=SOURCE_DIGEST_PREFIX)
    record = {"tables": tables, "publication": pins, "sources": sources,
              "selected_sources": sorted(set(sources) | (set(tables) & set(SOURCE_TABLES.values())))}
    connection.execute(f"CREATE TABLE {INPUT_RECORD} (snapshot VARCHAR)")
    connection.execute(f"INSERT INTO {INPUT_RECORD} VALUES (?)", [json.dumps(record)])


def selected_citation_inputs(cursor):
    """Only a verified private binding can supply processing evidence to the tool."""
    import duckdb

    try:
        value = cursor.execute(f"SELECT snapshot FROM {INPUT_RECORD}").fetchone()
    except duckdb.CatalogException as error:
        raise ValueError("Citation resolution requires selected native ETL receipts") from error
    if value is None:
        raise ValueError("Citation resolution is missing its selected ETL receipt binding")
    return json.loads(value[0])
