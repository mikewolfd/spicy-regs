"""Reconcile pinned publications with existing raw holdings, without importing raw again."""

from __future__ import annotations

import ctypes
import errno
import json
import os
import re
import sys
import tempfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .storage import Inputs, connect, stable


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"(?:sha256:)?[a-f0-9]{64}", value):
        raise ValueError("Expected an exact SHA-256 digest")
    return "sha256:" + value.removeprefix("sha256:")


def count(value):
    if type(value) is not int or value < 0:
        raise ValueError("Expected a nonnegative measured byte count")
    return value


def pinned(inputs, reference, base):
    if (not isinstance(reference, dict) or set(reference) != {"path", "sha256", "bytes"}
            or not isinstance(reference["path"], str) or not reference["path"]):
        raise ValueError("Publication inputs require path, sha256 and bytes")
    path = (base / reference["path"]).resolve()
    inputs.pin(path)
    actual = inputs.pins[str(path)]
    if actual != {"sha256": sha(reference["sha256"]), "bytes": count(reference["bytes"])}:
        raise ValueError(f"Publication input pin mismatch: {path}")
    return path


def import_publications(db, inputs: Inputs, selection_path: Path) -> dict:
    """One strict input boundary; normalized receipts never substitute names for hashes.

    Receipt producers verify native artifact/root/journal chains before emitting
    ``spicy-publication-inputs``. An incomplete or provisional chain stays unknown.
    The original native control pins are retained alongside the normalized result.
    """
    from spicy_regs.sources.publication import parse_index

    selection_path = selection_path.resolve()
    raw_roots = set()
    if (inputs.root / "snapshot.json").is_file() or (inputs.root / "acquisitions/sha256").is_dir():
        raw_roots.add(inputs.root)
    for row in db.execute("SELECT path FROM inputs WHERE path LIKE '%/snapshot.json'"):
        path = Path(row[0])
        if (path.parent / "objects/sha256").is_dir():
            raw_roots.add(path.parent.resolve())

    def external(path):
        if any(path.resolve().is_relative_to(root) for root in raw_roots):
            raise ValueError("Publication metadata and processing receipts must remain outside the raw archive")
        return path

    def read_pin(reference, base):
        if (not isinstance(reference, dict) or set(reference) != {"path", "sha256", "bytes"}
                or not isinstance(reference["path"], str) or not reference["path"]):
            raise ValueError("Publication inputs require path, sha256 and bytes")
        external((base / reference["path"]).resolve())
        return pinned(inputs, reference, base)

    external(selection_path)
    selection = inputs.json(selection_path)
    allowed = {"format", "version", "publication", "receipts", "historical_publications", "rulemaking"}
    if (not isinstance(selection, dict) or set(selection) - allowed
            or selection.get("format") != "spicy-publication-reconciliation"
            or type(selection.get("version")) is not int or selection["version"] != 1):
        raise ValueError("Unsupported publication reconciliation selection")
    base = selection_path.parent
    publication = parse_index(read_pin(selection["publication"], base).read_bytes())
    generations: dict[tuple[str, str], dict[str, Any]] = {}
    for reference in selection.get("receipts", []):
        path = read_pin(reference, base)
        value = inputs.json(path)
        if (not isinstance(value, dict) or value.get("format") != "spicy-publication-inputs"
                or type(value.get("version")) is not int or value["version"] != 1):
            raise ValueError("Unsupported publication input receipts")
        for item in value["generations"]:
            key = (item["family"], sha(item["artifact_digest"]))
            if type(item.get("complete")) is not bool:
                raise ValueError("Generation receipts must state whether input closure is complete")
            if not isinstance(item.get("conflicts", []), list):
                raise ValueError("Generation conflicts must be an array")
            for field in ("outputs", "sources", "parents"):
                if not isinstance(item.get(field), list):
                    raise ValueError(f"Generation receipts require {field}")
            for entry in [*item["outputs"], *item["sources"], *item["parents"]]:
                sha(entry["sha256"])
                count(entry["bytes"])
            for pin in item.get("evidence_inputs", []):
                read_pin(pin, path.parent)
            # Captured source journals describe observations. They do not qualify
            # every input consumed by a published table. No supported native
            # receipt reader currently establishes that stronger assertion.
            if item["complete"]:
                raise ValueError("Complete input closure requires supported native consumption evidence")
            item = dict(item, consumption_complete=False)
            if key in generations and generations[key] != item:
                raise ValueError("Conflicting duplicate generation receipts")
            generations[key] = item

    # Cache each shared generation's closure once; copied branches cannot make a
    # missing ancestor disappear. Sources are unique exact digest/size pairs.
    closures = {}

    def closure(key, visiting=frozenset()):
        if key in closures:
            return closures[key]
        if key in visiting:
            return set(), {"cyclic-generation-inputs"}, True
        item = generations.get(key)
        if item is None:
            return set(), {"generation-input-receipt-unavailable"}, False
        sources, reasons, conflict = set(), set(), bool(item.get("conflicts"))
        if not item["consumption_complete"]:
            reasons.add("incomplete-build-input-provenance")
        for source in item["sources"]:
            if source.get("provenance") == "provisional":
                reasons.add("provisional-receipt-cannot-prove-build-consumption")
            elif source.get("provenance") != "build-consumption":
                reasons.add("capture-observation-does-not-prove-build-consumption")
            sources.add((sha(source["sha256"]), source["bytes"]))
        for parent in item["parents"]:
            parent_key = (parent["family"], sha(parent["artifact_digest"]))
            owner = generations.get(parent_key)
            output = [row for row in (owner or {}).get("outputs", [])
                      if row["table"].removesuffix(".parquet") == parent["table"].removesuffix(".parquet")]
            if not output:
                reasons.add("parent-output-receipt-unavailable")
            elif not any((sha(row["sha256"]), row["bytes"]) == (sha(parent["sha256"]), parent["bytes"])
                         and (not parent.get("object_key") or row.get("object_key") == parent["object_key"])
                         for row in output):
                conflict = True
                reasons.add("parent-output-identity-disagrees")
            more, gaps, disagrees = closure(parent_key, visiting | {key})
            sources.update(more)
            reasons.update(gaps)
            conflict |= disagrees
        for prior in item.get("prior_generations", []):
            prior_key = (prior["family"], sha(prior["artifact_digest"]))
            more, gaps, disagrees = closure(prior_key, visiting | {key})
            sources.update(more)
            reasons.update(gaps)
            conflict |= disagrees
        if not sources:
            reasons.add("original-build-inputs-unidentified")
        result = sources, reasons, conflict
        closures[key] = result
        return result

    bindings = {}
    for row in db.execute("SELECT id,data FROM collections"):
        for family in json.loads(row["data"]).get("families", []):
            if family in bindings and bindings[family] != row["id"]:
                raise ValueError("Ambiguous catalog family binding")
            bindings[family] = row["id"]
    counts = Counter()
    current_ids = set()
    used_sources = set()
    # Replace only this importer's managed current selection. Historical lineage
    # and unrelated corpus rows retain their own identities and evidence.
    db.execute("DELETE FROM tables WHERE json_extract(data,'$.publication.scope')='current'")
    db.execute("DELETE FROM lineage WHERE id LIKE 'publication-lineage:%' AND json_extract(data,'$.publication.scope')='current'")

    local_matches = {}

    def local_sources(identity, size):
        key = identity, size
        if key in local_matches:
            return local_matches[key]
        reasons = set()
        obj = db.execute("SELECT bytes FROM objects WHERE sha256=?", (identity,)).fetchone()
        if obj and obj[0] != size:
            reasons.add("raw-object-byte-count-disagrees")
        held = []
        for row in db.execute("SELECT id,status,local_path,data FROM files WHERE sha256=? AND bytes=?", key):
            if row["status"].startswith("conflicting-"):
                reasons.add("conflicting-raw-file-evidence")
            if row["status"] != "held" or not row["local_path"]:
                continue
            facts = json.loads(row["data"])
            local = Path(row["local_path"])
            if local.is_file() and not local.is_symlink():
                stat = local.stat()
                if stat.st_size == facts.get("encoded_bytes", size) and (
                    "presence_mtime_ns" not in facts or stat.st_mtime_ns == facts["presence_mtime_ns"]
                ):
                    held.append(row["id"])
        local_matches[key] = held, reasons
        return held, reasons

    def add_table(family, version, table, descriptor, *, scope="current", snapshot=None):
        version = sha(version)
        cid = bindings.get(family) or "publication:" + family
        db.execute("INSERT OR IGNORE INTO collections VALUES(?,?,?)", (
            cid, "Publication: " + family, encoded({"mapping_status": "unknown-publication-association"}),
        ))
        sources, reasons, conflict = closure((family, version))
        reasons = set(reasons)
        members = descriptor.get("members", [{"key": table, **descriptor}])
        expected = {(part["key"], sha(part["sha256"]), count(part["byteSize"])) for part in members}
        member_keys = {part[0] for part in expected}
        def output_key(row):
            return row.get("object_key") or row["table"].removesuffix(".parquet") + ".parquet"

        output = [row for row in generations.get((family, version), {}).get("outputs", [])
                  if output_key(row) in member_keys]
        actual = {(output_key(row), sha(row["sha256"]), row["bytes"]) for row in output}
        if not output:
            reasons.add("published-output-receipt-unavailable")
        elif actual != expected:
            conflict = True
            reasons.add("published-output-identity-disagrees")
        matched, missing, file_ids = 0, 0, set()
        for identity, size in sorted(sources):
            held, source_reasons = local_sources(identity, size)
            if source_reasons:
                conflict = True
                reasons.update(source_reasons)
            if held:
                matched += 1
                file_ids.update(held)
            else:
                missing += 1
        status = ("conflicting_evidence" if conflict else "missing_provenance" if reasons else
                  "missing_local_files" if missing else "verified")
        if missing:
            reasons.add("exact-original-files-unavailable-locally")
        result = {"scope": scope, "version": version, "status": status, "reasons": sorted(reasons),
                  "sourceFiles": len(sources), "matchedSourceFiles": matched, "missingSourceFiles": missing,
                  "capturedSourceFiles": len(sources), "inputClosureComplete": not reasons and not conflict,
                  "evidenceLinks": [], "localFileIds": sorted(file_ids), "evidenceIds": []}
        value = {"id": table.removesuffix(".parquet"), "family": family, "artifact_digest": version,
                 "status": "publication-pinned", "publication": result, "publication_descriptor": descriptor}
        if snapshot:
            value["snapshot_id"] = snapshot
        ident = "publication-lineage:" + stable(scope, family, version, table)
        db.execute("INSERT OR REPLACE INTO lineage VALUES(?,?,?)", (ident, cid, encoded(value)))
        if scope == "current":
            if value["id"] in current_ids:
                raise ValueError("Current publication repeats a table identity")
            current_ids.add(value["id"])
            db.execute("DELETE FROM tables WHERE id=?", (value["id"],))
            db.execute("INSERT INTO tables VALUES(?,?,?,?)", (value["id"], cid, family, encoded(value)))
            counts[status] += 1
            used_sources.update(sources)

    for family, entry in publication["families"].items():
        for table, descriptor in entry["tables"].items():
            add_table(family, entry["artifactDigest"], table, descriptor)
    for reference in selection.get("historical_publications", []):
        historical = parse_index(read_pin(reference, base).read_bytes())
        for family, entry in historical["families"].items():
            for table, descriptor in entry["tables"].items():
                add_table(family, entry["artifactDigest"], table, descriptor, scope="historical")
    if "rulemaking" in selection:
        refs = selection["rulemaking"]
        pointer = inputs.json(read_pin(refs["pointer"], base))
        manifest = inputs.json(read_pin(refs["manifest"], base))
        snapshot = pointer.get("snapshot_id", "")
        prefix = "materialized/rulemaking/snapshots/" + snapshot
        if (pointer.get("dataset") != "rulemaking" or pointer.get("format_version") != 2
                or not re.fullmatch(r"snapshot_[A-Za-z0-9]+", snapshot)
                or pointer.get("manifest_key") != prefix + "/manifest.json"
                or manifest.get("dataset") != "rulemaking" or manifest.get("format_version") != 2
                or manifest.get("snapshot_id") != snapshot):
            raise ValueError("Rulemaking manifest differs from selected pointer")
        # Definition identity uses the same canonical JSON convention as the UI;
        # retain the actual captured file's pin separately in phase inputs.
        import hashlib

        version = "sha256:" + hashlib.sha256(encoded(manifest).encode()).hexdigest()
        for table, item in manifest["artifacts"].items():
            if item.get("visibility") != "public" or not table.endswith(".parquet"):
                continue
            if item.get("remote_key") != prefix + "/" + table:
                raise ValueError("Rulemaking member differs from selected snapshot")
            add_table("rulemaking", version, table, {
                "sha256": sha(item["sha256"]), "byteSize": count(item["bytes"]), "rows": count(item["rows"]),
            }, snapshot=snapshot)
    return {"currentTables": len(current_ids), "statuses": dict(sorted(counts.items())),
            "sourceObjectsReferenced": len(used_sources), "generationsVisited": len(closures), "sourceIdentitiesChecked": len(local_matches)}


def _clone_index(source: Path, target: Path, source_db):
    """Use APFS copy-on-write for large indexes; never silently copy gigabytes."""
    if source_db.execute("PRAGMA journal_mode").fetchone()[0] == "wal":
        raise ValueError("Reconciliation requires a checkpointed catalog outside WAL mode")
    if sys.platform == "darwin":
        target.unlink()
        library = ctypes.CDLL(None, use_errno=True)
        if library.clonefile(os.fsencode(source), os.fsencode(target), 0) == 0:
            return "clonefile"
        error = ctypes.get_errno()
        if source.stat().st_size > 512 * 1024 * 1024:
            raise OSError(error, "Large catalog requires a filesystem supporting clonefile")
    elif source.stat().st_size > 512 * 1024 * 1024:
        raise ValueError("Large catalog requires a filesystem supporting clonefile")
    target_db = connect(target)
    try:
        source_db.backup(target_db)
    finally:
        target_db.close()
    return "sqlite-backup"


def reconcile(index: Path, output: Path, selection: Path, *, expected_generation: str | None = None,
              require_verified: bool = False) -> dict:
    """Atomically select a small publication update over a pinned raw catalog.

    Base input pins describe the unchanged raw rows. New code and evidence pins
    belong to this reconciliation phase, rather than pretending to rebuild raw.
    """
    from .selection import check_identities, identity, metadata, plain, require

    index, output = plain(index), plain(output)
    require(index != output and (not output.exists() or not os.path.samefile(index, output)),
            'Publication reconciliation requires a separate candidate')
    require(not output.exists(), 'Publication reconciliation requires new candidate output')
    source_identity = identity(index)
    output.parent.mkdir(parents=True, exist_ok=True)
    source = connect(index, readonly=True, scope="all")
    descriptor, temporary = tempfile.mkstemp(prefix=output.name + ".", suffix=".tmp", dir=output.parent)
    os.close(descriptor)
    candidate = Path(temporary)
    db = None
    try:
        source.execute("BEGIN")
        for row in source.execute("SELECT path FROM inputs WHERE path LIKE '%/snapshot.json'"):
            raw_root = Path(row[0]).parent
            if (raw_root / "objects/sha256").is_dir() and output.resolve().is_relative_to(raw_root.resolve()):
                raise ValueError("The rebuildable catalog must live outside the raw archive")
        base_generation = json.loads(source.execute("SELECT value FROM metadata WHERE key='generation'").fetchone()[0])
        previous_time = source.execute("SELECT value FROM metadata WHERE key='generatedAt'").fetchone()
        base_generated_at = json.loads(previous_time[0]) if previous_time else None
        if expected_generation is not None and expected_generation != base_generation:
            raise ValueError("Selected base catalog generation changed")
        method = _clone_index(index, candidate, source)
        check_identities({index: source_identity}, 'Base catalog changed while cloning publication candidate')
        require(metadata(candidate)['generation'] == base_generation, 'Cloned publication base generation differs')
        db = connect(candidate)
        inputs = Inputs(selection.parent)
        for code in (Path(__file__), Path(__file__).with_name("storage.py"), Path(__file__).with_name("scope.py")):
            inputs.pin(code)
        result = import_publications(db, inputs, selection)
        selected_scope = db.execute("SELECT value FROM metadata WHERE key='source_scope'").fetchone()
        if selected_scope:
            from .scope import install_scope

            install_scope(db, json.loads(selected_scope[0]))
        inputs.check()
        if require_verified and (not result["currentTables"] or set(result["statuses"]) != {"verified"}):
            raise ValueError("Publication has unresolved source provenance or missing originals")
        generation = "sha256:" + stable("publication-reconciliation", base_generation, result, inputs.pins)
        report = {"format": "spicy-publication-reconciliation-result", "version": 1, "status": "validated",
                  "baseGeneration": base_generation, "generation": generation, "copyMethod": method,
                  "baseGeneratedAt": base_generated_at, "phaseInputs": inputs.pins, **result}
        for key, value in (("generation", generation), ("publication_reconciliation", report),
                           ("generatedAt", datetime.now(UTC).isoformat().replace("+00:00", "Z"))):
            db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)", (key, encoded(value)))
        for table in ("tables", "lineage"):
            if db.execute(f"PRAGMA foreign_key_check({table})").fetchone():
                raise RuntimeError("Publication reconciliation introduced invalid references")
        db.commit()
        db.close()
        db = None
        with candidate.open("rb") as stream:
            os.fsync(stream.fileno())
        # The source remains read-locked until the replacement is durable. No
        # full raw scan, code reclassification, or mutation of the base is needed.
        check_identities({index: source_identity}, 'Base catalog changed during publication reconciliation')
        os.link(candidate, output)
        directory = os.open(output.parent, os.O_RDONLY)
        try:
            try:
                os.fsync(directory)
            except OSError as error:
                if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                    raise
        finally:
            os.close(directory)
        return report
    finally:
        if db is not None:
            db.close()
        source.close()
        candidate.unlink(missing_ok=True)


def check_publication(index: Path, selection: Path) -> dict:
    """Fail closed unless all current rows and the exact selected input are verified."""
    from .storage import digest

    with connect(index, readonly=True, scope="all") as db:
        row = db.execute("SELECT value FROM metadata WHERE key='publication_reconciliation'").fetchone()
        report = json.loads(row[0]) if row else {}
        pin = report.get("phaseInputs", {}).get(str(selection.resolve()))
        selected = bool(pin and pin["sha256"] == digest(selection) and pin["bytes"] == selection.stat().st_size)
        counts = Counter()
        current = []
        for row in db.execute("SELECT data FROM tables"):
            publication = json.loads(row[0]).get("publication", {})
            if publication.get("scope") == "current":
                counts[publication["status"]] += 1
                item = json.loads(row[0])
                current.append((item['family'], publication['version'], item['id'], item.get('publication_descriptor')))
        # Check the selected target descriptors independently of provenance
        # completeness. Existing uncertainty remains valid catalogue information.
        from spicy_regs.sources.publication import parse_index

        inputs = Inputs(selection.parent)
        target = inputs.json(selection)
        publication = parse_index(pinned(inputs, target['publication'], selection.parent).read_bytes())
        expected = [(family, sha(entry['artifactDigest']), table.removesuffix('.parquet'), descriptor)
                    for family, entry in publication['families'].items() for table, descriptor in entry['tables'].items()]
        if 'rulemaking' in target:
            refs = target['rulemaking']
            pointer = inputs.json(pinned(inputs, refs['pointer'], selection.parent))
            manifest = inputs.json(pinned(inputs, refs['manifest'], selection.parent))
            prefix = 'materialized/rulemaking/snapshots/' + pointer['snapshot_id']
            if pointer['manifest_key'] != prefix + '/manifest.json' or manifest['snapshot_id'] != pointer['snapshot_id']:
                raise ValueError('Rulemaking target manifest differs from selected pointer')
            import hashlib

            version = 'sha256:' + hashlib.sha256(encoded(manifest).encode()).hexdigest()
            expected.extend(('rulemaking', version, table.removesuffix('.parquet'), {
                'sha256': sha(item['sha256']), 'byteSize': count(item['bytes']), 'rows': count(item['rows']),
            }) for table, item in manifest['artifacts'].items()
                if item.get('visibility') == 'public' and table.endswith('.parquet'))
        target_matches = sorted(map(encoded, current)) == sorted(map(encoded, expected))
        return {"verified": selected and bool(counts) and set(counts) == {"verified"},
                "selectionMatches": selected, 'targetMatches': target_matches,
                "currentTables": sum(counts.values()), "statuses": dict(counts)}
