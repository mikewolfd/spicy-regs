"""Build and pin a reproducible local overlay over the retained 0.66.3 wheel.

The raw archive module is added and refusal writes gain the shared store API.
Other current reader modules stay unchanged. The default check compares in-memory
reproduction with existing wheel, receipt, project and lock bytes. --write is
explicit and preserves identical files, including an existing overlay.
"""

import argparse
import base64
import csv
import hashlib
import io
import json
import re
import tomllib
import zipfile
from pathlib import Path


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def write_changed(path: Path, body: bytes) -> None:
    if not path.exists() or path.read_bytes() != body:
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_bytes(body)
        temporary.replace(path)


def main() -> None:
    consumer = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", type=Path, default=None,
                        help="Optional reviewed SpicyDocs source checkout; otherwise reuse the pinned archive module")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Compare exact existing bytes (default)")
    mode.add_argument("--write", action="store_true", help="Write reviewed wheel and matching project/lock pins")
    args = parser.parse_args()
    provider = args.provider.resolve() if args.provider is not None else None
    base = consumer / "vendor/spicy_docs-0.66.3-py3-none-any.whl"
    base_bytes = base.read_bytes()
    base_sha = "6df098b7f50f2e7d3b781e1987cb9ff5d37118334aa7ced23f6f603733916c96"
    if sha256(base_bytes) != base_sha:
        raise ValueError("retained base wheel does not match its qualified digest")

    project = consumer / "pyproject.toml"
    lock = consumer / "uv.lock"
    project_text = project.read_text()
    lock_text = lock.read_text()
    project_data = tomllib.loads(project_text)
    lock_data = tomllib.loads(lock_text)
    old_path = project_data["tool"]["uv"]["sources"]["spicy-docs"]["path"]
    match = re.fullmatch(r"vendor/spicy_docs-(0\.66\.3(?:\+centralraw\.[0-9a-f]{12})?)-py3-none-any\.whl", old_path)
    if match is None:
        raise ValueError("current reader pin is not the retained base or an owned centralraw overlay")
    old_version = match[1]
    old_name = Path(old_path).name
    (old_package,) = (package for package in lock_data["package"] if package["name"] == "spicy-docs")
    if old_package["version"] != old_version or old_package["source"] != {"path": old_path}:
        raise ValueError("project and lock reader pins disagree")
    old_sha = sha256((consumer / old_path).read_bytes())
    if old_package["wheels"] != [{"filename": old_name, "hash": "sha256:" + old_sha}]:
        raise ValueError("current locked reader wheel does not match its recorded digest")

    module_names = ["spicy_docs/storage/raw_archive.py"]
    if provider is None:
        retained = consumer / "vendor/spicy_docs-0.65.0+centralraw.523052e8a6fe-py3-none-any.whl"
        retained_bytes = retained.read_bytes()
        if sha256(retained_bytes) != "2049deea32da3460cbaf3eac78594dbcf9d092daa6bc2c24d22b762c834f74d4":
            raise ValueError("retained archive module provider does not match its qualified digest")
        with zipfile.ZipFile(io.BytesIO(retained_bytes)) as archive:
            overlay = {name: archive.read(name) for name in module_names}
    else:
        overlay = {name: (provider / "src" / name).read_bytes() for name in module_names}
    # Adapt the reviewed archive module to the current reader's one-pass
    # completed-capture path. All other 0.66.3 package modules stay unchanged.
    streaming = b"""    def put_stream(self, chunks: Iterable[bytes], *, max_bytes: int) -> SourceNativeBlobWrite:
        \"\"\"Retain an unknown identity, reusing even lossless legacy wrappers.\"\"\"
        class ExistingOriginal(Exception):
            pass

        digest, observed = hashlib.sha256(), 0

        def checked():
            nonlocal observed
            for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise TypeError("blob chunks must be bytes")
                if len(chunk) > max_bytes - observed:
                    raise BlobLimitError("raw source exceeds its retention byte bound")
                digest.update(chunk)
                observed += len(chunk)
                yield chunk
            try:
                self.describe_blob(digest.hexdigest(), observed)
            except FileNotFoundError:
                return
            raise ExistingOriginal

        try:
            result = self._writer("objects/sha256").put(checked(), max_bytes=max_bytes)
        except ExistingOriginal:
            return SourceNativeBlobWrite("sha256:" + digest.hexdigest(), observed, True, observed)
        except (BlobIntegrityError, BlobLimitError) as error:
            raise ImmutablePublicationError(str(error)) from error
        return SourceNativeBlobWrite(result.digest, result.byte_size, result.reused, result.bytes_written)

"""
    module = "spicy_docs/storage/raw_archive.py"
    target = b"    def record_receipt("
    if overlay[module].count(target) != 1 or b"    def put_stream(" in overlay[module]:
        raise ValueError("reviewed archive module streaming boundary changed")
    overlay[module] = overlay[module].replace(target, streaming + target)
    # Current refusal parsing is preserved; only its blob destination gains the
    # same maintained store interface used by successful captures.
    with zipfile.ZipFile(io.BytesIO(base_bytes)) as archive:
        refusal = archive.read("spicy_docs/reading/refusals.py")
    refusal = refusal.replace(b"from pathlib import Path", b"from pathlib import Path\nfrom spicy_docs.storage.blobs import SourceNativeBlobStore")
    refusal = refusal.replace(b"store: Path,", b"store: Path | SourceNativeBlobStore,")
    before = b"            written = LocalBlobWriter(store).put([response.response_bytes], max_bytes=max_bytes)\n            result.update(sha256=written.digest, bytes=written.byte_size)"
    after = b"""            if isinstance(store, Path):
                written = LocalBlobWriter(store).put([response.response_bytes], max_bytes=max_bytes)
                result.update(sha256=written.digest, bytes=written.byte_size)
            else:
                import hashlib
                if len(response.response_bytes) > max_bytes:
                    raise ValueError("refused response exceeds its retention byte bound")
                digest = "sha256:" + hashlib.sha256(response.response_bytes).hexdigest()
                retained = store.put_blob(digest, len(response.response_bytes), [response.response_bytes])
                result.update(sha256=retained.blob_ref, bytes=retained.byte_size)"""
    if refusal.count(before) != 1:
        raise ValueError("reviewed refusal writer boundary changed")
    overlay["spicy_docs/reading/refusals.py"] = refusal.replace(before, after)
    source_hashes = {name: sha256(body) for name, body in overlay.items()}
    identity = sha256(json.dumps(source_hashes, sort_keys=True).encode())[:12]
    version = "0.66.3+centralraw." + identity
    old_info = "spicy_docs-0.66.3.dist-info"
    new_info = f"spicy_docs-{version}.dist-info"
    with zipfile.ZipFile(io.BytesIO(base_bytes)) as archive:
        contents = {
            name.replace(old_info, new_info): archive.read(name)
            for name in archive.namelist()
            if not name.endswith(("/RECORD", "/"))
        }
    contents.update(overlay)
    metadata = new_info + "/METADATA"
    contents[metadata], count = re.subn(rb"(?m)^Version: 0\.66\.3$", ("Version: " + version).encode(), contents[metadata])
    if count != 1:
        raise ValueError("base wheel metadata does not declare exactly one expected version")
    record = io.StringIO()
    writer = csv.writer(record, lineterminator="\n")
    for name, body in sorted(contents.items()):
        encoded = base64.urlsafe_b64encode(hashlib.sha256(body).digest()).rstrip(b"=").decode()
        writer.writerow([name, "sha256=" + encoded, len(body)])
    writer.writerow([new_info + "/RECORD", "", ""])
    contents[new_info + "/RECORD"] = record.getvalue().encode()
    wheel = io.BytesIO()
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, body in sorted(contents.items()):
            entry = zipfile.ZipInfo(name, (2026, 10, 24, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, body)
    wheel_bytes = wheel.getvalue()
    wheel_sha = sha256(wheel_bytes)
    output = consumer / f"vendor/spicy_docs-{version}-py3-none-any.whl"
    receipt = {
        "version": version,
        "wheel": output.name,
        "sha256": wheel_sha,
        "base_wheel": base.name,
        "base_sha256": base_sha,
        "changed_package_files": source_hashes,
        "scope": "Local central raw archive adoption; all other packaged source bytes equal the pinned base wheel.",
    }
    receipt_bytes = (json.dumps(receipt, indent=2) + "\n").encode()

    # Read the current pin rather than assuming this is the first local overlay.
    project_text, count = re.subn(
        r'(spicy-docs\[[^"\n]+\]==)' + re.escape(old_version) + r'(?=")',
        lambda match: match[1] + version,
        project_text,
    )
    if count != 2:
        raise ValueError("expected matching source-readers extra and dependency group pins")
    project_text = project_text.replace(old_path, "vendor/" + output.name)
    lock_text = lock_text.replace(old_path, "vendor/" + output.name)
    block = re.compile(r'(?ms)^\[\[package\]\]\nname = "spicy-docs"\n.*?(?=^\[\[package\]\]|\Z)')


    def update_package(match: re.Match) -> str:
        return (
            match[0]
            .replace(f'version = "{old_version}"', f'version = "{version}"')
            .replace(old_name, output.name)
            .replace("sha256:" + old_sha, "sha256:" + wheel_sha)
        )


    lock_text, count = block.subn(update_package, lock_text)
    if count != 1:
        raise ValueError("expected exactly one spicy-docs lock package")
    new_package = next(package for package in tomllib.loads(lock_text)["package"] if package["name"] == "spicy-docs")
    if new_package["version"] != version or new_package["wheels"] != [
        {"filename": output.name, "hash": "sha256:" + wheel_sha}
    ]:
        raise ValueError("updated reader lock pin is inconsistent")

    outputs = ((output, wheel_bytes), (output.with_suffix(".json"), receipt_bytes),
               (project, project_text.encode()), (lock, lock_text.encode()))
    if args.write:
        for path, body in outputs:
            write_changed(path, body)
    else:
        for path, body in outputs:
            if not path.is_file() or path.read_bytes() != body:
                raise SystemExit(f"Rebuild differs: {path.relative_to(consumer)}; use --write only after review")
    print(json.dumps({"status": "written" if args.write else "verified", **receipt}, indent=2))


if __name__ == "__main__":
    main()
