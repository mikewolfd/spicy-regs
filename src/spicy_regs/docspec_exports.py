"""Admit explicitly trusted local DocSpec exports using one loopback producer."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx

from spicy_regs.local_data import LocalSelection, _NAME, _unique_pairs, file_signature
from spicy_regs.sources.publication import empty_index

_MAX_METADATA = 1 << 20


def _document(path: Path) -> tuple[bytes, dict]:
    with path.open("rb") as source:
        raw = source.read(_MAX_METADATA + 1)
    if len(raw) > _MAX_METADATA:
        raise RuntimeError("DocSpec binding exceeds the metadata allowance")
    try:
        value = json.loads(raw, object_pairs_hook=_unique_pairs)
    except (ValueError, UnicodeDecodeError):
        raise RuntimeError("DocSpec binding JSON is invalid") from None
    if not isinstance(value, dict):
        raise RuntimeError("DocSpec binding must be a JSON object")
    return raw, value


def binding_unchanged(saved: dict) -> bool:
    """Compare only bounded configuration bytes; never reopen export payloads."""
    _, document = _document(Path(saved["path"]))
    return _configuration_digest(document) == saved["sha256"]


def _configuration_digest(document: dict) -> str:
    # Formatting alone does not create a new admission or justify another hash.
    return hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()


def _metadata_url(raw: str) -> str:
    try:
        parsed = urlsplit(raw)
        valid = (parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "::1"}
                 and parsed.port is not None and not parsed.username and not parsed.password
                 and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment)
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise RuntimeError("DocSpec metadata URL must name an explicit literal loopback HTTP port")
    return raw.rstrip("/")


def _record(client: httpx.Client, url: str) -> dict:
    try:
        with client.stream("GET", url) as response:
            if response.status_code != 200:
                raise RuntimeError("DocSpec canonical metadata lookup refused")
            content = bytearray()
            for part in response.iter_bytes(chunk_size=65536):
                if len(content) + len(part) > _MAX_METADATA:
                    raise RuntimeError("DocSpec canonical metadata exceeds the allowance")
                content.extend(part)
        value = json.loads(content, object_pairs_hook=_unique_pairs)
    except httpx.HTTPError:
        raise RuntimeError("DocSpec canonical metadata is unavailable") from None
    if not isinstance(value, dict):
        raise RuntimeError("DocSpec canonical metadata must be a JSON object")
    return value


def _hash(binary: Path, path: Path) -> dict:
    """Use the existing native hash command once; it does not open an Engine."""
    original = file_signature(binary)
    with tempfile.TemporaryFile() as output:
        try:
            result = subprocess.run([str(binary), "hash", str(path)], stdout=output,
                                    stderr=subprocess.DEVNULL, timeout=120,
                                    env={"RUST_LOG": "off"}, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("DocSpec export hash command is unavailable") from None
        if result.returncode != 0 or file_signature(binary) != original:
            raise RuntimeError("DocSpec export hash command refused")
        output.seek(0)
        raw = output.read(4097)
    if len(raw) > 4096:
        raise RuntimeError("DocSpec hash response exceeds the allowance")
    value = json.loads(raw, object_pairs_hook=_unique_pairs)
    if not isinstance(value, dict) or set(value) != {"digest", "bytes"}:
        raise RuntimeError("DocSpec hash response is invalid")
    return value


def local_exports(directory: Path, configuration: Path) -> LocalSelection:
    """Bind local bytes to exact canonical exports; release evidence stays separate."""
    _, document = _document(configuration)
    if set(document) != {"version", "metadata_url", "binary", "tables"} or document["version"] != 1:
        raise RuntimeError("DocSpec binding format is unsupported")
    url = _metadata_url(document["metadata_url"])
    if not isinstance(document["binary"], str):
        raise RuntimeError("DocSpec binding executable must be a path")
    binary = Path(document["binary"])
    if not binary.is_absolute() or binary.is_symlink() or not binary.is_file():
        raise RuntimeError("DocSpec binding must name an explicit regular native executable")
    tables = document["tables"]
    if not isinstance(tables, dict) or not tables:
        raise RuntimeError("DocSpec binding must select named tables")
    # Each immutable local member is admitted once; aliases add no new proof.
    selected_files, selected_objects = set(), set()
    for selected in tables.values():
        if not isinstance(selected, dict) or set(selected) != {"file", "export"}:
            raise RuntimeError("DocSpec table binding is invalid")
        file = selected["file"]
        if not isinstance(file, str) or Path(file).name != file or not file.endswith(".parquet"):
            raise RuntimeError("DocSpec export must be a named direct local Parquet child")
        if file in selected_files:
            raise RuntimeError("DocSpec bindings must select each local file once")
        selected_files.add(file)
        identity = tuple(file_signature(directory / file)[:2])
        if identity in selected_objects:
            raise RuntimeError("DocSpec bindings must select each local file once")
        selected_objects.add(identity)
    files, bindings, signatures = {}, {}, {}
    # No ambient proxy, redirect, credentials, or remote-file authority is admitted.
    with httpx.Client(trust_env=False, follow_redirects=False, timeout=30) as client:
        for name, selected in tables.items():
            if not isinstance(name, str) or not _NAME.fullmatch(name):
                raise RuntimeError("DocSpec table binding is invalid")
            file = selected["file"]
            path = directory / file
            before = file_signature(path)
            expected = selected["export"]
            if not isinstance(expected, dict) or expected.get("format") != 1:
                raise RuntimeError("DocSpec export pin is unsupported")
            try:
                dataset, operation = quote(expected["dataset"], safe=""), quote(expected["id"], safe="")
                source = expected["source"]
                canonical = _record(client, f"{url}/datasets/{dataset}/exports/{operation}")
                if json.dumps(canonical, sort_keys=True) != json.dumps(expected, sort_keys=True):
                    raise RuntimeError("DocSpec expected export differs from its canonical pin")
                release = _record(client, f"{url}/datasets/{quote(source['dataset'], safe='')}/versions/{quote(source['version'], safe='')}")
                reference = {"dataset": release["dataset"]["dataset"], "version": release["id"], "manifest": release["manifest"]}
                if source != reference:
                    raise RuntimeError("DocSpec export source differs from the exact retained release")
                actual = _hash(binary, path)
                if actual != {"digest": expected["object"]["digest"], "bytes": expected["object"]["bytes"]}:
                    raise RuntimeError("DocSpec local export bytes differ from the admitted object")
            except (KeyError, TypeError, ValueError):
                raise RuntimeError("DocSpec export or release metadata is malformed") from None
            if file_signature(path) != before:
                raise RuntimeError("DocSpec local export changed during admission")
            files[name] = (path, "docspec-export")
            signatures[str(path)] = before
            # Full trusted pin states disclosure, schema and exact release identity.
            # It does not claim that flat rows carry occurrence-level receipt links.
            bindings[name] = {"status": "docspec_immutable_export", "export": expected,
                              "evidence": {"scope": "release", "version": source["version"],
                                           "manifest_blake3": source["manifest"]["digest"], "row_mapping": False}}
    return LocalSelection(directory, files, empty_index(), False, docspec=bindings,
                          docspec_signatures=signatures,
                          docspec_configuration={"path": str(configuration), "sha256": _configuration_digest(document)})
