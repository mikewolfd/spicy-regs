"""Bind qualified FEC SQL to exact, generation-validated processing receipts.

Declarations are cheap and import no source readers. Restoration happens only
for release-compatible views and preserves the public native subject relations.
"""
from dataclasses import replace
from collections.abc import Mapping
from importlib.resources import files
import base64
import json
from pathlib import Path
from tempfile import TemporaryDirectory


def processing_declarations():
    return json.loads(files("spicy_regs").joinpath("fec_processing_schemas.json").read_text())


def qualified_views(scope, *, legacy_associations=False):
    from .relationship_views.fec_query_views import fec_query_views
    schemas = {name: tuple(c[0] for c in item["columns"]) for name, item in processing_declarations().items()}
    specs = fec_query_views(**scope, processing_schemas=schemas, legacy_associations=legacy_associations)
    root = Path(__file__).parent
    return tuple(replace(spec, identities={**spec.identities, "policies": {
        **spec.identities["policies"], "fec-receipt-adapter/1": root / "fec_receipt_adapter.py",
        "shared-etl-receipts/1": root / "etl_receipts.py"}, "dictionaries": {
        **spec.identities["dictionaries"], "fec-processing-schemas": root / "fec_processing_schemas.json"}})
        for spec in specs)


def receipt_owner(index, table):
    owners = [(name, entry) for name, entry in index.get("families", {}).items()
              if table in entry.get("etlReceipts", {}).get("datasets", ())]
    if len(owners) > 1:
        raise ValueError(f"Ambiguous receipt ownership: {table}")
    return owners[0] if owners else None


def selected_qualified_views(native_specs, legacy_specs, index, local_native=None):
    """Choose old filing rules only for inputs with no selected native layout.

    The release receipt binds the resulting SQL and policy as well as the exact
    subject membership. Partial native selection stays native and is refused by
    prepare_qualified before any restoration.
    """
    from .sources.publication import table_owner

    local_native = local_native or {}
    legacy = {spec.view.name: spec for spec in legacy_specs}
    def native(table):
        owner = table_owner(index, table + ".parquet")
        return (table in local_native or receipt_owner(index, table) is not None
                or (owner is not None and "etlReceipts" in owner[1]))
    return tuple(spec if any(native(table) for table in spec.view.required)
                 else legacy.get(spec.view.name, spec) for spec in native_specs)


class ReceiptAdapter:
    """One connection's bounded restoration cache, scoped to captured members."""
    def __init__(self, connection, index, base_url, *, local_directory=None, local_receipts=None, local_native=None):
        self.connection, self.index, self.base_url = connection, index, base_url
        self.local_directory = Path(local_directory) if local_directory else None
        self.local_receipts = local_receipts
        self.local_native = local_native or {}
        self.restored = {}

    def _fetch(self, member, destination):
        from .sources.publication import fetch_member, file_identity
        if self.local_directory is None:
            fetch_member(self.base_url, member, destination)
        else:
            import shutil
            if member.key in {"etl_receipts.parquet", "etl_receipts.keys.parquet"} and self.local_receipts is not None:
                if member.path not in self.local_receipts:
                    raise ValueError("Receipt not part of selected local download")
                source = Path(self.local_receipts[member.path])
            else:
                source = self.local_directory / member.key
            identity = file_identity(source)
            if identity["sha256"] != member.sha256 or identity["bytes"] != member.byte_size:
                raise ValueError("Local selected receipt member differs from its generation pin")
            shutil.copyfile(source, destination)

    def require_selected(self, tables):
        missing = sorted(table for table in tables
                         if table not in self.local_native and receipt_owner(self.index, table) is None)
        if missing:
            raise ValueError("FEC dependencies require selected native receipts: " + ", ".join(missing))

    def selected_rows(self, table):
        """Read one dataset's exact native subjects and verified processing values."""
        from .etl_receipts import DatasetPolicy, read_with_receipts, read_attempts, select_receipts, selected_subject_policy
        from .subject_catalog import descriptors
        from .sources.publication import receipt_members, receipt_key_members, table_members

        self.require_selected((table,))
        policy = DatasetPolicy.from_descriptor(descriptors()[table])
        owner, native = receipt_owner(self.index, table), self.local_native.get(table)
        with TemporaryDirectory(prefix="selected-processing-receipts-") as directory:
            directory = Path(directory)
            if native is not None:
                shared = Path(native["receipts"])
                subjects = [Path(path) for path in native["subjects"]]
                generation = native["generation_id"]
            else:
                shared = directory / "shared.parquet"
                self._fetch(receipt_members(self.index, dataset=table)[0], shared)
                subjects = []
                if not policy.receipt_only:
                    for ordinal, member in enumerate(table_members(self.index, table + ".parquet")):
                        path = directory / f"subject-{ordinal}.parquet"
                        self._fetch(member, path)
                        subjects.append(path)
                generation = owner[1]["etlReceipts"]["generationId"]
            specification = native.get("key_index_descriptor") if native is not None else owner[1]["etlReceipts"].get("keyIndex")
            if specification is not None:
                from .receipt_key_index import check_reader
                from .sources.publication import file_identity
                import pyarrow.parquet as pq
                if native is not None:
                    if not native.get("key_index"):
                        raise ValueError("Selected native receipt key index is missing")
                    key_path = Path(native["key_index"])
                else:
                    key_path = directory / "keys.parquet"
                    indexes = receipt_key_members(self.index, dataset=table)
                    if len(indexes) != 1:
                        raise ValueError("Selected receipt key index is ambiguous")
                    self._fetch(indexes[0], key_path)
                identity = file_identity(shared)
                check_reader(self.connection, str(key_path), specification,
                             {"sha256": identity["sha256"], "byteSize": identity["bytes"],
                              "rows": pq.ParquetFile(shared).metadata.num_rows})
            receipt = select_receipts(shared, directory / "receipt.parquet", dataset=table)
            if policy.receipt_only:
                yield from (row["processing_fields"] for row in read_attempts(
                    [receipt], policy, generation_id=generation, outcomes=frozenset({"observed"})))
            else:
                yield from read_with_receipts(subjects, [receipt], selected_subject_policy(policy, subjects),
                                             generation_id=generation)

    def restore_originals(self, table, schema, original, *, prefix):
        """Materialize bounded verified inputs privately, shared by connection cursors."""
        import pyarrow as pa

        target = prefix + table
        if target in self.restored:
            return self.restored[target]
        temporary = target + "_batch"
        con = self.connection
        con.register(temporary, pa.Table.from_batches([], schema=schema))
        con.execute(f'CREATE TABLE "{target}" AS SELECT * FROM "{temporary}"')
        try:
            batch = []
            for selected in self.selected_rows(table):
                batch.append(original(selected))
                if len(batch) == 2000:
                    con.register(temporary, pa.Table.from_pylist(batch, schema=schema))
                    con.execute(f'INSERT INTO "{target}" SELECT * FROM "{temporary}"')
                    batch.clear()
            if batch:
                con.register(temporary, pa.Table.from_pylist(batch, schema=schema))
                con.execute(f'INSERT INTO "{target}" SELECT * FROM "{temporary}"')
        except BaseException:
            con.execute(f'DROP TABLE "{target}"')
            raise
        finally:
            con.unregister(temporary)
        self.restored[target] = target
        return target

    def restore(self, table):
        import pyarrow as pa
        from .etl_receipts import DatasetPolicy
        from .subject_catalog import descriptors

        self.require_selected((table,))
        policy = DatasetPolicy.from_descriptor(descriptors()[table])
        declaration = processing_declarations()[table]
        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declaration["arrow_schema"])))
        identity_scalars = ({} if policy.policy_version.startswith("fec-subject-receipts/") else
                            json.loads(files("spicy_regs").joinpath("fec_identity_context_fields.json").read_text())
                            [table]["native_scalars"])

        def original(row):
            if policy.policy_version.startswith("fec-subject-receipts/"):
                conversion = row.get("fec_conversion_inputs")
                columns = row.get("fec_input_columns")
                if not isinstance(conversion, Mapping) or not isinstance(columns, list) or any(
                    not isinstance(name, str) for name in columns
                ):
                    raise ValueError(f"{table}: declared FEC conversion inputs are missing or malformed")
                return {name: conversion[name] if name in conversion else row[name] for name in columns}
            conversion = row.get("conversion_inputs")
            if not isinstance(conversion, Mapping):
                raise ValueError(f"{table}: declared conversion_inputs must be a mapping")
            for name in schema.names:
                if name not in conversion and (name not in policy.input_fields or
                        (name in identity_scalars and row.get(name) is not None)):
                    raise ValueError(f"{table}: missing retained conversion input for {name}")
            return {name: conversion[name] if name in conversion else row.get(name) for name in schema.names}

        return self.restore_originals(table, schema, original, prefix="_spicy_fec_processing_")

    def prepare_qualified(self, spec, sql):
        """Serve exact receipt-bound legacy inputs, or restore every native input.

        A selected native owner with missing or malformed receipts never takes
        the legacy path. Mixed dependencies use require_selected before writes.
        """
        from .sources.publication import table_owner

        def native(table):
            owner = table_owner(self.index, table + ".parquet")
            return (table in self.local_native or receipt_owner(self.index, table) is not None
                    or (owner is not None and "etlReceipts" in owner[1]))
        if not any(native(table) for table in spec.required):
            return spec
        return self.prepare(spec, sql)

    def prepare(self, spec, sql):
        self.require_selected(spec.required)
        replacements = {table: self.restore(table) for table in spec.required}
        tree = json.loads(self.connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
        def visit(node, ctes=frozenset()):
            if isinstance(node, list):
                for item in node:
                    visit(item, ctes)
            elif isinstance(node, dict):
                if node.get("type") == "BASE_TABLE":
                    name, namespace = node["table_name"], node["schema_name"]
                    if name in replacements and (namespace == "main" or (not namespace and name not in ctes)):
                        node["table_name"] = replacements[name]
                        if not node.get("alias"):
                            node["alias"] = name
                    return
                if node.get("type") == "RECURSIVE_CTE_NODE":
                    ctes = ctes | {node["cte_name"]}
                mapping = node.get("cte_map")
                for entry in mapping["map"] if isinstance(mapping, dict) else ():
                    visit(entry["value"], ctes)
                    ctes = ctes | {entry["key"]}
                for key, value in node.items():
                    if key != "cte_map":
                        visit(value, ctes)
        visit(tree)
        rewritten = self.connection.execute("SELECT json_deserialize_sql(?)", [json.dumps(tree)]).fetchone()[0]
        return replace(spec, required={replacements[t]: c for t, c in spec.required.items()},
                       query=lambda _: rewritten)
