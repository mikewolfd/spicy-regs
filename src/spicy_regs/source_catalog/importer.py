"""Offline corpus import without acquisition, source mutation, or guessed mappings."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path

from .storage import Inputs, array_items, connect, schema, stable, stored_path


def encoded(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class Importer:
    def __init__(
        self,
        db,
        inputs: Inputs,
        config: dict,
        *,
        catalog_input: Path | None = None,
        table_inputs_input: Path | None = None,
    ):
        self.db, self.inputs, self.root = db, inputs, inputs.root
        self.catalog_input = catalog_input
        self.table_inputs_input = table_inputs_input
        self.bindings: dict[str, str] = {}
        self.families: dict[str, str] = {}
        self.artifacts: dict[str, set[str]] = {}
        self.expected_files: set[str] = set()
        self.config = config
        self.generated_at = "1970-01-01T00:00:00Z"
        for entry in config.get("collections", []):
            self.collection(entry["id"], entry.get("title", entry["id"]), entry)
            for name in entry.get("source_names", []):
                if name in self.bindings and self.bindings[name] != entry["id"]:
                    raise ValueError(f"Ambiguous source binding: {name}")
                self.bindings[name] = entry["id"]
            for family in entry.get("families", []):
                if family in self.families and self.families[family] != entry["id"]:
                    raise ValueError(f"Ambiguous family binding: {family}")
                self.families[family] = entry["id"]
        for route in config.get("routes", []):
            self.route(route)
        for kind in ["planned", "relationships", "periods", "observations"]:
            for entry in config.get(kind, []):
                self.evidence(entry.get("collection_id"), kind, entry)

    def collection(self, ident: str, title: str, data: dict | None = None) -> str:
        self.db.execute("INSERT OR IGNORE INTO collections VALUES(?,?,?)", (ident, title, encoded(data or {})))
        return ident

    def resolve(self, name: str, *, family: bool = False) -> str:
        mapping = self.families if family else self.bindings
        ident = mapping.get(name) or self.bindings.get(name) or "corpus:" + name
        self.collection(ident, name, {"mapping_status": "unresolved"} if ident.startswith("corpus:") else {})
        return ident

    def route(self, entry: dict) -> None:
        cid = entry["collection_id"]
        self.collection(cid, cid)
        self.db.execute("INSERT OR REPLACE INTO routes VALUES(?,?,?)", (entry["id"], cid, encoded(entry)))

    def evidence(self, cid: str | None, kind: str, entry: dict) -> None:
        if cid:
            self.collection(cid, cid)
        ident = entry.get("id") or kind + ":" + stable(cid, entry)
        self.db.execute("INSERT OR IGNORE INTO evidence VALUES(?,?,?,?)", (ident, cid, kind, encoded(entry)))

    def file(self, cid: str, name: str, row: dict, *, context: str, reference: str | None = None) -> None:
        sha = row.get("sha256") or row.get("blobRef")
        if sha and not sha.removeprefix("sha256:").isalnum():
            sha = None
        if sha:
            sha = "sha256:" + sha.removeprefix("sha256:")
        size = row.get("bytes", row.get("byteSize"))
        path = stored_path(self.root, reference or "", sha) if reference or sha else None
        status = row.get("status", "held")
        if status in ["advertised", "discovered", "listed"]:
            status = "known-not-acquired"
        if path is None and status not in ["external-by-choice", "known-not-acquired", "receipt-only"]:
            status = "missing-bytes"
        if sha and size is not None:
            existing = self.db.execute("SELECT bytes FROM objects WHERE sha256=?", (sha,)).fetchone()
            if existing and existing[0] != int(size):
                raise ValueError(f"Conflicting decoded sizes for {sha}")
            self.db.execute("INSERT OR IGNORE INTO objects VALUES(?,?)", (sha, int(size)))
        else:
            sha = None
        ident = "file:" + stable(cid, context, name)
        self.expected_files.add(ident)
        data = dict(row, verification="retained-manifest;presence-checked;not-rehashed")
        if path:
            stat = path.stat()
            data["encoded_bytes"] = stat.st_size
            data["presence_mtime_ns"] = stat.st_mtime_ns
        self.db.execute(
            "INSERT OR REPLACE INTO files VALUES(?,?,?,?,?,?,?,?,?)",
            (ident, cid, row.get("route_id"), name, status, sha, size, str(path) if path else reference, encoded(data)),
        )

    def tsv(self, name: str):
        override = self.table_inputs_input if name == "table-inputs.tsv" else None
        with self.inputs.pin(override or self.root / name).open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream, delimiter="\t", strict=bool(override))
            if override:
                fields = reader.fieldnames or []
                if (
                    len(fields) != len(set(fields))
                    or any(not field.strip() for field in fields)
                    or not {"table", "family"}.issubset(fields)
                ):
                    raise ValueError("Table-inputs overlay requires unique headers including table and family")
            for row in reader:
                if override and (
                    None in row
                    or any(value is None for value in row.values())
                    or not row["table"].strip()
                    or not row["family"].strip()
                ):
                    raise ValueError("Malformed table-inputs overlay row")
                yield row

    def run(self) -> None:
        catalog = self.inputs.json(self.catalog_input or self.root / "catalog.json")
        if self.catalog_input:
            if not isinstance(catalog, dict) or not isinstance(catalog.get("families"), dict):
                raise ValueError("Catalog overlay requires an object with a families object")
            if ("format" in catalog or "version" in catalog) and (
                catalog.get("format") != "spicyregs-local-source-collection" or catalog.get("version") != 2
            ):
                raise ValueError("Unsupported catalog overlay format/version")
            for family, data in catalog["families"].items():
                if not isinstance(family, str) or not family.strip() or not isinstance(data, dict):
                    raise ValueError("Malformed catalog overlay family")
                for field in ["tables", "source_artifacts"]:
                    values = data.get(field, [])
                    if not isinstance(values, list) or any(
                        not isinstance(value, str) or not value.strip() for value in values
                    ):
                        raise ValueError(f"Malformed catalog overlay {field}")
        self.generated_at = catalog.get("updated_at") or catalog.get("assembled_at") or self.generated_at
        for family, data in catalog.get("families", {}).items():
            cid = self.resolve(family, family=True)
            self.evidence(cid, "family", dict(data, family=family))
            for table in data.get("tables", []):
                row = {
                    "id": table.removesuffix(".parquet"),
                    "family": family,
                    "artifact_digest": data.get("current_generation"),
                    "status": "published-with-baseline" if data.get("baselines") else "publication-pinned",
                    "coverage_note": data.get("source_coverage_note"),
                }
                self.db.execute("INSERT OR REPLACE INTO tables VALUES(?,?,?,?)", (row["id"], cid, family, encoded(row)))
            for artifact in data.get("source_artifacts", []):
                self.artifacts.setdefault(artifact, set()).add(cid)
        for row in self.tsv("sources.tsv"):
            cid = self.resolve(row["identity"]) if row["kind"] == "download-collection" else self.resolve(row["name"])
            self.evidence(cid, "source_inventory", row)
            if row.get("identity", "").startswith("sha256:"):
                self.artifacts.setdefault(row["identity"], set()).add(cid)
            if row["kind"] == "processing-receipt":
                self.file(
                    cid, row.get("path") or row["name"], row, context="source-inventory", reference=row.get("path")
                )
        for row in self.tsv("table-inputs.tsv"):
            cid = self.resolve(row["family"], family=True)
            self.evidence(cid, "table_input", row)
            for parent in row.get("parent_tables", "").split(";"):
                if parent:
                    self.evidence(
                        cid, "relationships", {"kind": "derived-from", "from_id": row["table"], "to_id": parent}
                    )
            for baseline in row.get("baseline_paths", "").split(";"):
                if baseline:
                    self.evidence(cid, "baseline", {"path": baseline, "table": row["table"]})
            for receipt in row.get("receipt_paths", "").split(";"):
                if receipt:
                    self.evidence(cid, "processing_receipt", {"path": receipt, "table": row["table"]})
        for item in catalog.get("lineage", []):
            cid = self.resolve(item["family"], family=True)
            self.db.execute(
                "INSERT OR IGNORE INTO lineage VALUES(?,?,?)", ("lineage:" + stable(item), cid, encoded(item))
            )
        for item in catalog.get("missing_historical_roots", []):
            cid = self.resolve(item["family"], family=True)
            self.file(cid, item["digest"], {"status": "receipt-only", **item}, context="missing-historical-root")
        for path in sorted((self.root / "collections").glob("*/files.json")):
            manifest = self.inputs.json(path)
            cid = self.bindings.get(manifest["id"]) or self.resolve(manifest["id"])
            collection_data = json.loads(
                self.db.execute("SELECT data FROM collections WHERE id=?", (cid,)).fetchone()[0]
            )
            if manifest.get("label") and collection_data.get("mapping_status") == "unresolved":
                self.db.execute("UPDATE collections SET title=? WHERE id=?", (manifest["label"], cid))
            self.evidence(cid, "collection_manifest", {k: v for k, v in manifest.items() if k != "files"})
            for position, item in enumerate(manifest.get("files", [])):
                self.file(
                    cid,
                    item["path"],
                    item,
                    context=f"{manifest['id']}:{position}",
                    reference=item.get("object") or str(path.parent.relative_to(self.root) / item["path"]),
                )
        # The cached member DB is only a cross-check. Authoritative current roots and
        # every current member manifest drive the import, including post-cache additions.
        for base in ["source-evidence", "source-native"]:
            for path in sorted((self.root / base).glob("*/artifact.json")):
                artifact = self.inputs.json(path)
                aid = artifact["artifactDigest"]
                family = artifact.get("spec", {}).get("family") or artifact.get("spec", {}).get("sourceFamily")
                route_id = self.config.get("source_routes", {}).get(artifact.get("spec", {}).get("sourceSystemId"))
                linked = self.artifacts.get(aid) or {self.resolve(family, family=True) if family else self.resolve(aid)}
                cid = min(linked)
                if route_id:
                    route_owner = self.db.execute("SELECT collection_id FROM routes WHERE id=?", (route_id,)).fetchone()
                    if not route_owner:
                        raise ValueError(f"Unknown configured source route: {route_id}")
                    cid = route_owner[0]
                for linked_cid in sorted(linked):
                    self.evidence(
                        linked_cid,
                        "artifact_input_link",
                        {
                            "artifact_digest": aid,
                            "holdings_collection_id": cid,
                            "basis": "catalog-source-artifact-binding",
                        },
                    )
                self.evidence(cid, "source_artifact", artifact)
                scopes = path.parent / "records/scopes.jsonl"
                if scopes.is_file():
                    with self.inputs.pin(scopes).open() as stream:
                        for line in stream:
                            if not line.strip():
                                continue
                            scope = json.loads(line)
                            self.evidence(cid, "scope", dict(scope, artifact_digest=aid))
                            fields = scope.get("fields", scope)
                            if fields.get("publishedFrom") or fields.get("publishedThrough"):
                                self.evidence(
                                    cid,
                                    "periods",
                                    {
                                        "id": "period:" + stable(aid, scope),
                                        "kind": "publication-date",
                                        "label": str(fields.get("publishedFrom", "?"))
                                        + " to "
                                        + str(fields.get("publishedThrough", "?")),
                                        "start": fields.get("publishedFrom"),
                                        "end": fields.get("publishedThrough"),
                                        "basis": "requested-scope",
                                        "artifact_digest": aid,
                                        "route_id": route_id,
                                    },
                                )
                for descriptor in artifact.get("memberManifests", []):
                    member_path = stored_path(path.parent, descriptor["objectKey"], descriptor.get("sha256"))
                    if not member_path:
                        self.evidence(
                            cid, "import_error", {"status": "missing-bytes", "artifact": aid, "descriptor": descriptor}
                        )
                        continue
                    for position, item in enumerate(array_items(self.inputs, member_path, "members")):
                        object_key = (
                            item.get("objectKey") or item.get("blobRef") or item.get("sha256") or f"member-{position}"
                        )
                        reference = str(path.parent.relative_to(self.root) / object_key)
                        self.file(
                            cid,
                            object_key,
                            dict(item, route_id=route_id) if route_id else item,
                            context=f"{aid}:{descriptor['objectKey']}:{position}",
                            reference=reference,
                        )
        for key, kind in [
            ("local_artifact_index", "local_artifact"),
            ("download_collections", "download_collection"),
            ("retained_processing_receipts", "processing_receipt"),
        ]:
            if catalog.get(key) and (self.root / catalog[key]).is_file():
                value = self.inputs.json(self.root / catalog[key])
                rows = value if isinstance(value, list) else value.get("artifacts", value.get("collections", []))
                for item in rows:
                    cid = self.resolve(
                        item.get("family") or item.get("id") or item.get("digest", "unclassified"),
                        family=bool(item.get("family")),
                    )
                    self.evidence(cid, kind, item)
                    if kind == "processing_receipt" and item.get("relative_path"):
                        self.file(cid, item["relative_path"], item, context=kind, reference=item["relative_path"])
        for extra in ["metadata/baseline-files.json", "metadata/external-dependencies.json"]:
            extra_path = self.root / extra
            if extra_path.is_file():
                value = self.inputs.json(extra_path)
                self.evidence(None, extra_path.stem, {"value": value})
                rows = value if isinstance(value, list) else value.get("files", value.get("dependencies", []))
                if extra_path.stem == "external-dependencies" and isinstance(value, dict):
                    rows = [
                        dict(item, id=key, status="external-by-choice")
                        for key, item in value.items()
                        if isinstance(item, dict)
                    ]
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    cid = self.resolve(
                        item.get("family") or item.get("id") or "external", family=bool(item.get("family"))
                    )
                    self.evidence(cid, extra_path.stem, item)
                    if item.get("status") == "external-by-choice":
                        self.file(cid, item.get("upstream", item.get("id", "external")), item, context=extra)
                    reference = item.get("relative_path") or item.get("path")
                    if reference and item.get("sha256"):
                        self.file(cid, reference, item, context=extra, reference=reference)
        self.evidence(None, "corpus_catalog", {k: v for k, v in catalog.items() if k not in ["families", "lineage"]})

    def discovery(self, value: dict) -> None:
        if value.get("format") != "spicy-source-discovery" or value.get("version") != 1:
            raise ValueError("Unsupported discovery format")
        for item in value.get("collections", []):
            self.collection(item["id"], item.get("title", item["id"]), item)
            self.evidence(item["id"], "publisher_collection_observation", item)
        for item in value.get("routes", []):
            self.route(item)
        for item in value.get("items", []):
            cid = item.get("collection_id")
            if not cid and item.get("route_id"):
                found = self.db.execute("SELECT collection_id FROM routes WHERE id=?", (item["route_id"],)).fetchone()
                cid = found[0] if found else None
            cid = cid or self.resolve("unresolved-discovery")
            self.collection(cid, cid)
            self.file(
                cid,
                item.get("name") or item.get("native_id") or item["id"],
                dict(item, status=item.get("status", "known-not-acquired")),
                context=item.get("version_id") or item["id"],
            )
        observations = {item["id"]: item for item in value.get("observations", []) if item.get("id")}
        observation_targets = {}
        for kind in ["observations", "periods", "errors", "relationships"]:
            for item in value.get(kind, []):
                cid = item.get("collection_id")
                if not cid and item.get("route_id"):
                    found = self.db.execute(
                        "SELECT collection_id FROM routes WHERE id=?", (item["route_id"],)
                    ).fetchone()
                    cid = found[0] if found else None
                parent_observation = observations.get(item.get("observation_id"), {})
                targets = item.get("collection_ids") or ([cid] if cid else [])
                if kind == "errors" and not targets:
                    targets = observation_targets.get(item.get("observation_id"), [])
                if kind in ["observations", "errors"] and not targets:
                    observation_id = item.get("observation_id") or item.get("id") or stable(item)
                    unresolved_id = "inventory-observation:" + observation_id
                    self.collection(
                        unresolved_id,
                        "Unresolved inventory observation",
                        {
                            "mapping_status": "unresolved-observation",
                            "description": "This entry records a discovery scope or parsing failure. It does not establish that a dataset exists.",
                        },
                    )
                    targets = [unresolved_id]
                if kind == "observations" and item.get("id"):
                    observation_targets[item["id"]] = targets
                observation_ref = None
                if kind in ["observations", "errors"] and (
                    item.get("raw") is not None or item.get("raw_xml") is not None
                ):
                    observation_ref = item.get("id") or kind + ":" + stable(item)
                    self.db.execute(
                        "INSERT OR IGNORE INTO evidence VALUES(?,?,?,?)",
                        ("snapshot:" + stable(observation_ref), None, "discovery_snapshot", encoded(item)),
                    )
                for target in targets:
                    entry = {
                        key: value for key, value in item.items() if key not in ["raw", "raw_xml", "collection_ids"]
                    }
                    if kind == "errors":
                        entry.setdefault("status", parent_observation.get("status", "discovery-error"))
                        entry["note"] = (
                            item.get("message")
                            or item.get("error")
                            or item.get("reason")
                            or "Discovery error; see retained evidence."
                        )
                        if parent_observation.get("observed_at"):
                            entry.setdefault("observed_at", parent_observation["observed_at"])
                    if observation_ref:
                        entry["observation_ref"] = observation_ref
                    if len(targets) > 1 and entry.get("id"):
                        entry["id"] += ":" + stable(target)[:16]
                    self.evidence(target, kind, entry)


def build(
    corpus: Path,
    output: Path,
    *,
    config: Path | None = None,
    discovery: Sequence[Path] = (),
    catalog_input: Path | None = None,
    table_inputs_input: Path | None = None,
) -> dict:
    """Validate a complete temporary index before atomic activation."""
    output.parent.mkdir(parents=True, exist_ok=True)
    inputs = Inputs(corpus)
    for source in Path(__file__).parent.glob("*.py"):
        inputs.pin(source)
    default = Path(__file__).parent / "config/registry.v1.json"
    configuration = inputs.json(config or default) if (config or default.is_file()) else {}
    descriptor, temporary = tempfile.mkstemp(prefix=output.name + ".", suffix=".tmp", dir=output.parent)
    os.close(descriptor)
    path = Path(temporary)
    report: dict[str, object] = {"format": "spicy-source-reconciliation", "version": 1, "status": "building"}
    db = connect(path)
    try:
        schema(db)
        importer = Importer(
            db,
            inputs,
            configuration,
            catalog_input=catalog_input,
            table_inputs_input=table_inputs_input,
        )
        importer.run()
        for discovery_path in discovery:
            importer.discovery(inputs.json(discovery_path))
        for row in db.execute("""SELECT id FROM collections WHERE EXISTS
            (SELECT 1 FROM files WHERE files.collection_id=collections.id)
            AND NOT EXISTS (SELECT 1 FROM tables WHERE tables.collection_id=collections.id)
            AND NOT EXISTS (SELECT 1 FROM evidence WHERE evidence.collection_id=collections.id AND kind='planned')"""):
            importer.evidence(
                row[0],
                "planned",
                {
                    "id": "unassigned:" + stable(row[0]),
                    "status": "unassigned",
                    "reason": "No reviewed destination mapping or recorded table lineage for these inputs.",
                },
            )
        actual = {r[0] for r in db.execute("SELECT id FROM files")}
        if actual != importer.expected_files:
            raise RuntimeError("File identity reconciliation failed")
        inputs.check()
        for row in db.execute("SELECT * FROM files"):
            facts = json.loads(row["data"])
            reference = row["local_path"]
            current = stored_path(corpus, reference, row["sha256"]) if reference else None
            if "encoded_bytes" in facts:
                if (
                    not current
                    or current.stat().st_size != facts["encoded_bytes"]
                    or current.stat().st_mtime_ns != facts["presence_mtime_ns"]
                ):
                    raise RuntimeError(f"Retained object presence changed during build: {row['id']}")
            elif current and row["status"] == "missing-bytes":
                raise RuntimeError(f"Missing object appeared during build: {row['id']}")
        logical = hashlib.sha256()
        for table in ["collections", "routes", "objects", "files", "evidence", "tables", "lineage"]:
            for row in db.execute(f"SELECT * FROM {table} ORDER BY 1"):
                logical.update(encoded(tuple(row)).encode())
                logical.update(b"\n")
        for input_path, pin in inputs.pins.items():
            db.execute("INSERT INTO inputs VALUES(?,?,?)", (input_path, pin["sha256"], pin["bytes"]))
        generation = "sha256:" + stable(
            "source-catalog", 1, logical.hexdigest(), sorted((p, v["sha256"]) for p, v in inputs.pins.items())
        )
        report.update(
            status="validated",
            generation=generation,
            inputs=inputs.pins,
            files=len(actual),
            collections=db.execute("SELECT count(*) FROM collections").fetchone()[0],
        )
        for key, value in {
            "generation": generation,
            "generatedAt": importer.generated_at,
            "reconciliation": report,
        }.items():
            db.execute("INSERT INTO metadata VALUES(?,?)", (key, encoded(value)))
        if db.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError("Catalog references failed validation")
        db.commit()
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite integrity check failed")
        db.close()
        os.replace(path, output)
    except BaseException as error:
        db.close()
        path.unlink(missing_ok=True)
        report.update(status="failed", error=str(error), inputs=inputs.pins)
        raise
    finally:
        receipt = output.with_suffix(output.suffix + ".reconciliation.json")
        receipt.write_text(json.dumps(report, indent=2) + "\n")
    return report
