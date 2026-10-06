"""Convert one old-shape rollup family to native subjects and receipts, once, through that family's own writer.

A scheduled rollup refuses a published family that carries no ETL receipts (``selected_generations``), so a family
whose builder reads its own prior cannot convert itself. This command does it explicitly, one named family at a time:
the family's retained published tables stand in for its builder's output, its maintained writer splits them into
subjects and receipts, and the standard admission publishes them. No scheduled path gains an old-shape reader.

The receipt it writes keeps the captured index entry, and is saved before the pointer write is attempted, so
``--rollback`` can put the pointer back whatever became of the process. Without ``--publish`` nothing is written
outside the work directory. Runbook: ``docs/native-family-conversion.md``.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib
import io
import json
import os
import pkgutil
import re
import shutil
import subprocess
import sys
import time
import tomllib
import zipfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import distribution, version
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.sources import publication, r2

RECEIPT = "conversion.json"
SCRIPT = "scripts/convert_family_to_native.py"
NOTHING_PUBLISHED = "nothing was published; the family's pointer is unchanged"
#: Journal events a later run reads back from its prior (``CaptureEvidence.inherited_event``). A conversion reads no
#: source, so it journals each one again unchanged; otherwise the next run would treat every held row as never read.
INHERITED_EVENTS = ("congress-index-selection", "scorecard-source-attempt")
#: The receipt outcomes of a row or observation that converted; any other outcome stops the conversion.
CONVERTED_OUTCOMES = frozenset({"accepted", "observed"})


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class ConversionRefused(RuntimeError):
    """The conversion stopped. ``state`` says what the bucket holds now, in words an operator can act on."""

    def __init__(self, message: str, *, state: str = NOTHING_PUBLISHED):
        super().__init__(message)
        self.state = state


def _wheel_state(root: Path, locked: Mapping) -> dict:
    """The pinned SpicyDocs wheel by digest: the lock's, the vendored file's, and installed files that differ from it."""
    wheels = locked.get("wheels", [])
    state = {"locked_sha256": wheels[0]["hash"] if len(wheels) == 1 else None, "file_sha256": None,
             "installed_differs": None}
    wheel = root / locked.get("source", {}).get("path", "")
    if state["locked_sha256"] is None or not wheel.is_file():
        return state  # Not a vendored wheel: there are no bytes here to hold the installed files against.
    with wheel.open("rb") as stream:
        state["file_sha256"] = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    installed, differs = distribution("spicy-docs"), []
    with zipfile.ZipFile(wheel) as archive:
        record = next(name for name in archive.namelist() if name.endswith(".dist-info/RECORD"))
        for name, digest, _size in csv.reader(io.TextIOWrapper(archive.open(record), "utf-8")):
            if not digest or ".data/" in name:
                continue
            algorithm, _, expected = digest.partition("=")
            target = Path(str(installed.locate_file(name)))
            actual = (base64.urlsafe_b64encode(hashlib.new(algorithm, target.read_bytes()).digest()).rstrip(b"=").decode()
                      if target.is_file() else None)
            if actual != expected:
                differs.append(name)
    state["installed_differs"] = differs[:20]
    return state


def source_state(remote: str) -> dict:
    """What this process would convert with: the checkout, the remote's main, and the SpicyDocs wheel beside its pin."""
    root = Path(__file__).resolve().parents[2]

    def git(*arguments: str) -> str:
        try:
            return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True, text=True,
                                  env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).stdout.strip()
        except subprocess.CalledProcessError as failure:
            raise ConversionRefused(f"git {arguments[0]} failed: {failure.stderr.strip()[:300]}") from failure

    url = git("remote", "get-url", remote)
    if not re.match(r"(https://|ssh://|git@)", url):
        raise ConversionRefused(f"--remote {remote} is {url!r}, not a hosted repository: main must be read from "
                                "where it is published, not from a local path")
    listed = git("ls-remote", url, "refs/heads/main").split()
    if not listed:
        raise ConversionRefused(f"--remote {remote} has no main branch")
    locked = next(package for package in tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))["package"]
                  if package["name"] == "spicy-docs")
    return {
        "checkout": git("rev-parse", "HEAD"),
        "uncommitted": git("status", "--porcelain", "--untracked-files=no").splitlines(),
        # Every module under src/ can be imported and every declaration there is read, tracked or not.
        "untracked": [name for name in git("ls-files", "--others", "--", "src", "scripts").splitlines()
                      if "__pycache__/" not in name and not name.endswith(".pyc")],
        "remote": re.sub(r"//[^/@]*@", "//", url),
        "main": listed[0],
        "spicy_docs": version("spicy-docs"),
        "pinned_spicy_docs": locked["version"],
        "spicy_docs_wheel": _wheel_state(root, locked),
    }


def _refuse_moved(state: Mapping, *, main: str, spicy_docs: str) -> None:
    """Field policies and the writer come from main and its pinned wheel; converting from anything else is refused."""
    problems = []
    if len(main) < 8 or not state["main"].startswith(main):
        problems.append(f"main is {state['main'][:12]}, not the {main[:12]} this run was started for")
    if state["checkout"] != state["main"]:
        problems.append(f"this checkout is {state['checkout'][:12]}, not main {state['main'][:12]}")
    if state["uncommitted"]:
        problems.append(f"the checkout has uncommitted changes: {', '.join(state['uncommitted'][:5])}")
    if state["untracked"]:
        problems.append(f"the checkout has untracked files the command could load: {', '.join(state['untracked'][:5])}")
    if state["spicy_docs"] != spicy_docs or state["pinned_spicy_docs"] != spicy_docs:
        problems.append(f"SpicyDocs is {state['spicy_docs']} installed and {state['pinned_spicy_docs']} pinned, "
                        f"not the {spicy_docs} this run was started for")
    wheel = state["spicy_docs_wheel"]
    if wheel["locked_sha256"] is None or wheel["file_sha256"] != wheel["locked_sha256"]:
        problems.append(f"the SpicyDocs wheel in this checkout is {wheel['file_sha256']}, and uv.lock pins "
                        f"{wheel['locked_sha256']}")
    elif wheel["installed_differs"]:
        problems.append("the installed SpicyDocs differs from the pinned wheel in "
                        + ", ".join(wheel["installed_differs"][:5]))
    if problems:
        raise ConversionRefused("Main or the pinned wheel moved: " + "; ".join(problems))


def _historical_producer(root: Path, revision: str, publisher: str) -> tuple[str, Mapping]:
    """Read retained Git blobs, with the same path ordering and bytes as generations.source_digest."""
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ConversionRefused("--expect-producer requires a full lowercase Git commit")

    def git(*arguments: str) -> bytes:
        try:
            return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True,
                                  env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).stdout
        except subprocess.CalledProcessError as failure:
            raise ConversionRefused(f"Prepared producer history cannot be verified ({arguments[0]}): "
                                    + failure.stderr.decode(errors="replace").strip()[:300]) from failure

    if git("cat-file", "-t", revision).strip() != b"commit":
        raise ConversionRefused("Prepared producer pin is not a commit in this repository")
    git("merge-base", "--is-ancestor", revision, publisher)
    historical_lock = git("show", f"{revision}:uv.lock")
    if historical_lock != git("show", f"{publisher}:uv.lock"):
        raise ConversionRefused("Prepared producer and publisher require the same locked runtime")
    locked = next((package for package in tomllib.loads(historical_lock.decode())["package"]
                   if package["name"] == "spicy-docs"), None)
    if locked is None:
        raise ConversionRefused("Prepared producer has no locked SpicyDocs runtime")
    wheels = locked.get("wheels", [])
    if len(wheels) != 1 or not locked.get("source", {}).get("path"):
        raise ConversionRefused("Prepared producer has no single locked vendored SpicyDocs wheel")
    wheel = git("show", f"{revision}:{locked['source']['path']}")
    if "sha256:" + hashlib.sha256(wheel).hexdigest() != wheels[0]["hash"]:
        raise ConversionRefused("Prepared producer wheel bytes differ from its lock")
    digest = hashlib.sha256()
    prefix = "src/spicy_regs/"
    files = []
    for entry in git("ls-tree", "-r", "-z", revision, "--", prefix).split(b"\0"):
        if not entry:
            continue
        properties, name = entry.split(b"\t", 1)
        path = Path(name.decode().removeprefix(prefix))
        if path.suffix != ".py" or "__pycache__" in path.parts:
            continue
        if properties.split()[0] not in (b"100644", b"100755"):
            raise ConversionRefused("Prepared producer Python source is not a regular Git blob")
        files.append(path)
    if not files:
        raise ConversionRefused("Prepared producer Python source is unavailable")
    for path in sorted(files):
        digest.update(path.as_posix().encode() + b"\0")
        digest.update(git("show", f"{revision}:{prefix}{path.as_posix()}"))
    return "urn:spicy-regs:implementation:sha256:" + digest.hexdigest(), locked


def _prepared_implementation(source: Mapping, publisher: Mapping, expected_producer: str | None) -> str:
    """Keep the sealed producer distinct from the current publisher, within the same locked runtime."""
    from spicy_regs.generations import implementation_id

    # Preserve same-revision callers. Reusing an earlier producer always needs an explicit full pin and Git proof.
    if expected_producer is None:
        if source != publisher:
            raise ConversionRefused("An earlier prepared producer requires --expect-producer")
        return implementation_id()
    if (source["checkout"] != expected_producer or source["main"] != expected_producer
            or source["remote"] != publisher["remote"]):
        raise ConversionRefused("Prepared producer pin or repository differs from its sealed source")
    _refuse_moved(source, main=expected_producer, spicy_docs=publisher["spicy_docs"])
    identity, locked = _historical_producer(Path(__file__).resolve().parents[2], expected_producer,
                                          publisher["checkout"])
    if (locked["version"] != source["pinned_spicy_docs"]
            or locked["wheels"][0]["hash"] != source["spicy_docs_wheel"]["locked_sha256"]
            or source["spicy_docs_wheel"] != publisher["spicy_docs_wheel"]):
        raise ConversionRefused("Prepared producer runtime differs from the current installed locked wheel")
    return identity


@dataclass(frozen=True)
class _Built:
    """One verified local generation and the two reads that prove it: restored locally, then from what is published."""

    generation: Path
    policies: Mapping[str, str]
    restore: Callable[[str], Path]
    read_published: Callable[[str, Mapping], int]
    evidence: tuple[Path, ...] = ()
    added_tables: frozenset[str] = frozenset()
    receipt_only_tables: frozenset[str] = frozenset()
    #: Validate actual native court subjects separately from original processing input types.
    declared: Callable[[str], pa.Schema] | None = None


def _rollup_class(family: str, base: type | None = None):
    """The scheduled rollup that owns ``family``: by default a subject/receipt one, or any subclass of ``base``."""
    import spicy_regs.pipelines.rollups as rollups
    from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup

    for module in pkgutil.iter_modules(rollups.__path__):
        for value in vars(importlib.import_module(f"{rollups.__name__}.{module.name}")).values():
            if (isinstance(value, type) and issubclass(value, base or SubjectReceiptRollup)
                    and value.__dict__.get("name") == family and value.publication_family is None):
                return value
    return None


def _regulatory_base_class(family: str):
    from spicy_regs.pipelines.rollups.regulatory_base import DocketsFamily, DocumentsFamily

    return {cls.name: cls for cls in (DocketsFamily, DocumentsFamily)}.get(family)


def _handed_on(base: str, replaced_root: Mapping) -> list[dict]:
    """The replaced generation's journal events that its next run would have read back (:data:`INHERITED_EVENTS`)."""
    from spicy_regs.source_evidence import INPUT_ROLE

    pin = next((item for item in replaced_root["inputs"] if item.get("role") == INPUT_ROLE), None)
    if pin is None:
        return []
    journal = publication.load_evidence_journal(base, pin).splitlines()
    return [{name: value for name, value in row.items() if name != "recorded_at"}
            for row in map(json.loads, journal) if row.get("event") in INHERITED_EVENTS]


def _journal(evidence, events: Sequence[Mapping]) -> None:
    for row in events:
        evidence.event(row["event"], **{name: value for name, value in row.items() if name != "event"})


def _table_files(path: Path) -> dict[str, Path]:
    """The physical layout of one held table; directories never imply extra Hive columns."""
    if path.is_symlink():
        raise ValueError(f"A retained table cannot be a symlink: {path}")
    if path.is_file():
        return {path.name: path}
    if not path.is_dir():
        raise ValueError(f"A retained table is missing: {path}")
    members = sorted(member for member in path.rglob("*") if member.is_symlink() or not member.is_dir())
    if any(member.is_symlink() or not member.is_file() or member.suffix != ".parquet" for member in members):
        raise ValueError(f"A retained split table has an unexpected member: {path}")
    return {member.relative_to(path).as_posix(): member for member in members}


def _table_rows(path: Path) -> int:
    return sum(pq.ParquetFile(member).metadata.num_rows for member in _table_files(path).values())


def _retain_table(base: str, captured: Mapping, key: str, directory: Path) -> Path:
    """Fetch every pinned member without collapsing its partition or changing its physical schema."""
    from spicy_regs.generations import _table_info

    descriptor = publication.table_descriptor(captured, key)
    if descriptor is None:
        raise ConversionRefused(f"{key}: no captured table descriptor")
    split = "members" in descriptor
    table_path = directory / (key.removesuffix(".parquet") if split else key)
    seen = set()
    for member in publication.table_members(captured, key):
        relative = Path(member.key)
        if relative.is_absolute() or ".." in relative.parts or publication.member_table(member.key) != key:
            raise ConversionRefused(f"{key}: member lies outside its captured table: {member.key}")
        if member.key in seen:
            raise ConversionRefused(f"{key}: duplicate captured member: {member.key}")
        seen.add(member.key)
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not publication.fetch_member(base, member, target, member.path):
            raise ConversionRefused(f"{member.path} is not readable")
        observed = _table_info(target)
        if observed["rows"] != member.rows or observed["columns"] != descriptor["columns"]:
            raise ConversionRefused(f"{member.path}: retained physical schema or row count differs from its pin")
    if _table_rows(table_path) != descriptor["rows"]:
        raise ConversionRefused(f"{key}: retained member rows differ from the captured table")
    return table_path


def _convert_rollup(cls, old: Mapping, retained: Mapping[str, Path], base: str, work: Path,
                    handed_on: Sequence[Mapping]) -> _Built:
    """Run the family's rollup with its builder replaced by "return the retained tables"; everything after is its own."""
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    if set(cls.source_outputs) != set(old["tables"]):
        raise ConversionRefused(
            f"{cls.name}: its rollup writes {sorted(cls.source_outputs)} and the published family holds "
            f"{sorted(old['tables'])}; convert only a family whose table set main still writes")
    # A rollup's own constructor arguments and inputs feed only its builder, which is replaced below.
    pipeline = cls.__new__(cls)
    RollupPipeline.__init__(pipeline, output_dir=work / "build", skip_upload=True)
    pipeline.inputs = ()

    def retained_tables(directory: Path, **_) -> tuple[Path, ...]:
        copied = []
        for key in cls.source_outputs:
            source = retained[key]
            target = directory / source.name
            if source.is_dir():
                shutil.copytree(source, target)
            else:
                shutil.copyfile(source, target)
            copied.append(target)
        return tuple(copied)

    def build(output_dir: Path):
        if pipeline.source_evidence:  # inherited from the replaced generation just before the build
            _journal(pipeline.source_evidence, handed_on)
        return pipeline.build_receipts(output_dir, retained_tables)

    pipeline.build = build
    pipeline.run()
    (generation,) = (work / "build" / "generations").iterdir()

    return _rollup_built(cls, generation, retained, base, work,
                         (pipeline.source_evidence.artifact_dir,) if pipeline.source_evidence else ())


def _rollup_built(cls, generation: Path, retained: Mapping[str, Path], base: str, work: Path,
                  evidence: tuple[Path, ...], selection_root: Path | None = None) -> _Built:
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    def exact(dataset: str, restored: Path) -> Path:
        if cls.name in {"roll-call-votes", "member-vote-terms"}:
            _check_processing_restore(retained[dataset + ".parquet"], restored, label="Congress")
        return restored

    def restore(dataset: str) -> Path:
        restored = SelectedPriors(work / "restored" / uuid4().hex,
                                  root=selection_root or work / "build", public_url="").get(dataset)
        return exact(dataset, restored)

    def read_published(dataset: str, index: Mapping) -> int:
        restored = exact(dataset, SelectedPriors(work / "read-back" / uuid4().hex, index=index, public_url=base).get(dataset))
        original = retained[dataset + ".parquet"]
        if original.is_dir() and _has_differences(_table_differences(restored, original)):
            raise ValueError(f"{dataset}: published native readback changed retained members")
        return _table_rows(restored)

    return _Built(
        generation,
        {policy.dataset: policy.policy_version for policy in cls.receipt_policies},
        restore,
        read_published,
        evidence,
        frozenset(cls.added_tables),
        frozenset(cls.receipt_only_tables),
    )


def _convert_court(family: str, old: Mapping, captured: Mapping, retained: Mapping[str, Path], base: str,
                   work: Path, handed_on: Sequence[Mapping], retain_evidence: bool) -> _Built:
    """Court tables: the shared court writer over each retained table, then the court generation admission."""
    from spicy_regs.court_receipts import (
        build_court_generation, finish_court_output,
    )
    evidence = None
    if retain_evidence:
        # As a scheduled run does: this generation names the one it replaces and one evidence artifact of its own.
        from spicy_regs.source_evidence import CaptureEvidence

        (work / "build").mkdir(parents=True, exist_ok=True)
        evidence = CaptureEvidence(work / "build", family)
        evidence.inherit(dict(captured), public_url=base)
        _journal(evidence, handed_on)
    generation_id = uuid4().hex
    subjects = {}
    for key in old["tables"]:
        (member,) = publication.table_members(captured, key)
        # The witness names the immutable published object, not this machine's copy of it.
        witness = {"source_id": member.path, "source_uri": None, "sha256": member.sha256, "locator": None,
                   "body_version": None}
        subjects[key.removesuffix(".parquet")] = finish_court_output(
            key.removesuffix(".parquet"), retained[key], work / "court", witnesses=[witness], generation_id=generation_id)
    generation = work / "build" / "generation"
    try:
        build_court_generation(generation, family=family, files=list(subjects.values()),
                               publication_status="complete-family", read_snapshot=captured,
                               inputs=evidence.inputs() if evidence else ())
    except BaseException as error:
        if evidence:
            evidence.finish(error)
        raise
    if evidence:
        evidence.finish()

    return _court_built(generation, subjects, retained, base, work, generation_id,
                        (evidence.artifact_dir,) if evidence else ())


def _convert_regulatory_base(cls, captured: Mapping, retained: Mapping[str, Path], base: str,
                             work: Path, handed_on: Sequence[Mapping], retain_evidence: bool) -> _Built:
    from spicy_regs.generations import build_generation
    from spicy_regs.transforms.regulations_receipts import policy, write_held_dataset

    dataset = cls.output.removesuffix(".parquet")
    generation_id = uuid4().hex
    evidence = None
    if retain_evidence:
        from spicy_regs.source_evidence import CaptureEvidence

        (work / "build").mkdir(parents=True, exist_ok=True)
        evidence = CaptureEvidence(work / "build", cls.name)
        evidence.inherit(dict(captured), public_url=base)
        _journal(evidence, handed_on)
    try:
        (member,) = publication.table_members(captured, cls.output)
        witness = {"source_id": member.path, "source_uri": None, "sha256": member.sha256,
                   "locator": None, "body_version": None}
        subject, receipts = write_held_dataset(
            dataset, retained[cls.output], work / "regulatory-native", generation_id=generation_id,
            witnesses=[witness], bulk=True,
        )
        owner = cls.__new__(cls)
        owner.validate_native_output(subject)
        generation = work / "build" / "generation"
        build_generation(
            generation, family=cls.name, files=[subject], expected_keys=[cls.output],
            schemas=owner.generation_schemas(), read_snapshot=captured,
            inputs=evidence.inputs() if evidence else (), receipt_path=receipts,
            receipt_policies=[policy(dataset)], receipt_generation_id=generation_id,
        )
    except BaseException as error:
        if evidence:
            evidence.finish(error)
        raise
    if evidence:
        evidence.finish()
    return _regulatory_base_built(cls, generation, retained, base, work, generation_id,
                                  (evidence.artifact_dir,) if evidence else ())


def _check_processing_restore(original: Path, restored: Path, *, label: str) -> Path:
    """Refuse any change in complete processing schema, metadata, ordered values or repetitions."""
    from itertools import zip_longest

    expected, actual = pq.ParquetFile(original), pq.ParquetFile(restored)
    if not expected.schema_arrow.equals(actual.schema_arrow, check_metadata=True):
        raise ValueError(f"{label} processing schema or metadata changed")
    for before, after in zip_longest(expected.iter_batches(batch_size=1000), actual.iter_batches(batch_size=1000)):
        if before is None or after is None or not before.equals(after):
            raise ValueError(f"{label} processing values, order or repetitions changed")
    return restored


def _regulatory_base_built(cls, generation: Path, retained: Mapping[str, Path], base: str, work: Path,
                           generation_id: str, evidence: tuple[Path, ...]) -> _Built:
    from spicy_regs.selected_generations import SelectedInputs
    from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, policy

    source_schema = pq.read_schema(retained[cls.output])
    dataset = cls.output.removesuffix(".parquet")

    def exact(restored: Path) -> Path:
        return _check_processing_restore(retained[cls.output], restored, label="Regulatory")

    def restore(name: str) -> Path:
        if name != dataset:
            raise ValueError("Regulatory prepared dataset differs from its maintained family")
        return exact(materialize_internal(
            ReceiptInput(dataset, (generation / cls.output,), generation / "etl_receipts.parquet", generation_id),
            work / "restored" / uuid4().hex / cls.output, bulk=True, source_schema=source_schema,
        ))

    def read_published(name: str, index: Mapping) -> int:
        if name != dataset:
            raise ValueError("Regulatory published dataset differs from its maintained family")
        directory = work / "read-back" / uuid4().hex
        selected = SelectedInputs(directory, directory / "native", index=index, public_url=base).select(dataset)
        if selected is None:
            raise ValueError("Published regulatory input selection is absent")
        restored = materialize_internal(
            ReceiptInput(dataset, selected.subjects, selected.receipts, selected.generation_id),
            directory / cls.output, bulk=True, source_schema=source_schema,
        )
        return _table_rows(exact(restored))

    return _Built(generation, {dataset: policy(dataset).policy_version}, restore, read_published, evidence)


def _court_built(generation: Path, subjects: Mapping[str, Path], retained: Mapping[str, Path], base: str,
                 work: Path, generation_id: str, evidence: tuple[Path, ...]) -> _Built:
    from spicy_regs.court_receipts import POLICIES, restore_processing_input
    from spicy_regs.court_subjects import SUBJECT_SCHEMAS

    def restore(dataset: str) -> Path:
        target = work / "restored" / f"{dataset}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        # The court restore writes whatever schema it is handed, so this file takes the retained table's columns
        # and types; its file metadata is dropped so that metadata the published table had shows as lost.
        restored = restore_processing_input(subjects[dataset], target, dataset=dataset,
                                            schema=pq.read_schema(retained[dataset + ".parquet"]).remove_metadata(),
                                            receipt_path=generation / "etl_receipts.parquet", generation_id=generation_id)
        return _check_processing_restore(retained[dataset + ".parquet"], restored, label="Court")

    def read_published(dataset: str, index: Mapping) -> int:
        target = work / "read-back" / uuid4().hex / f"{dataset}.parquet"
        target.parent.mkdir(parents=True)
        (member,) = publication.table_members(index, dataset + ".parquet")
        if not publication.fetch_member(base, member, target, member.path):
            raise ConversionRefused(f"{dataset}: published subject is unavailable")
        (receipt_member,) = publication.receipt_members(index, dataset=dataset)
        receipts = target.parent / "etl_receipts.parquet"
        if not publication.fetch_member(base, receipt_member, receipts, receipt_member.path):
            raise ConversionRefused(f"{dataset}: published receipts are unavailable")
        owner = publication.table_owner(index, dataset + ".parquet")
        if owner is None:
            raise ConversionRefused(f"{dataset}: no published subject owner")
        selected = owner[1]["etlReceipts"]["generationId"]
        restored = restore_processing_input(
            target, target.parent / "processing.parquet", dataset=dataset,
            schema=pq.read_schema(retained[dataset + ".parquet"]).remove_metadata(),
            receipt_path=receipts, generation_id=selected,
        )
        return _table_rows(_check_processing_restore(retained[dataset + ".parquet"], restored, label="Court"))

    return _Built(generation, {name: POLICIES[name].policy_version for name in subjects}, restore, read_published,
                  evidence, declared=SUBJECT_SCHEMAS.__getitem__)


def _differences(restored: Path, retained: Path) -> dict:
    """How the rows a native read restores differ from the retained table: columns, types, and rows each way."""
    import duckdb

    def literal(path: Path) -> str:
        return "'" + str(path).replace("'", "''") + "'"

    with duckdb.connect() as con:
        described = [dict((name, kind) for name, kind, *_ in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({literal(path)}, hive_partitioning=false)").fetchall())
                     for path in (restored, retained)]
        shared = [name for name in described[1] if name in described[0]]
        # A column of one type on both sides is compared as that type. Text is only the common ground for a column
        # whose type changed, which is reported and refused anyway; as text, ['a, b'] and ['a', 'b'] are one value.
        quoted = {name: '"' + name.replace('"', '""') + '"' for name in shared}
        text = ", ".join(quoted[name] if described[0][name] == described[1][name]
                         else f"CAST({quoted[name]} AS VARCHAR) AS {quoted[name]}" for name in shared) or "NULL"
        either = [con.execute(f"SELECT count(*) FROM (SELECT {text} FROM read_parquet({literal(a)}, hive_partitioning=false)"
                              f" EXCEPT ALL SELECT {text} FROM read_parquet({literal(b)}, hive_partitioning=false))")
                  .fetchall()[0][0]
                  for a, b in ((restored, retained), (retained, restored))]
    metadata = [pq.read_schema(path).metadata or {} for path in (restored, retained)]
    return {
        "metadata_changes": sorted(key.decode(errors="replace") for key in metadata[0].keys() | metadata[1].keys()
                                   if metadata[0].get(key) != metadata[1].get(key)),
        "columns_only_in_restored": sorted(set(described[0]) - set(described[1])),
        "columns_only_in_retained": sorted(set(described[1]) - set(described[0])),
        "type_changes": {name: [described[1][name], described[0][name]] for name in shared
                         if described[0][name] != described[1][name]},
        "rows_only_in_restored": either[0],
        "rows_only_in_retained": either[1],
    }


def _table_differences(restored: Path, retained: Path) -> dict:
    """Compare each exact member, including empty files and metadata, rather than only a table-wide row union."""
    if restored.is_dir() != retained.is_dir():
        raise ValueError("Restored table changed between a single file and a split directory")
    if not retained.is_dir():
        return _differences(restored, retained)
    actual, original = _table_files(restored), _table_files(retained)
    members = {name: _differences(actual[name], original[name]) for name in sorted(actual.keys() & original.keys())}
    for name, check in members.items():
        check["schema_changed"] = not pq.read_schema(actual[name]).equals(pq.read_schema(original[name]), check_metadata=True)
    only_actual, only_original = sorted(actual.keys() - original.keys()), sorted(original.keys() - actual.keys())
    return {
        "members": members, "members_only_in_restored": only_actual, "members_only_in_retained": only_original,
        "schema_changes": [name for name, check in members.items() if check["schema_changed"]],
        **{field: [f"{name}:{value}" for name, check in members.items() for value in check[field]]
           for field in ("metadata_changes", "columns_only_in_restored", "columns_only_in_retained")},
        "type_changes": {f"{name}:{column}": change for name, check in members.items()
                         for column, change in check["type_changes"].items()},
        "rows_only_in_restored": sum(check["rows_only_in_restored"] for check in members.values())
        + sum(_table_rows(actual[name]) for name in only_actual),
        "rows_only_in_retained": sum(check["rows_only_in_retained"] for check in members.values())
        + sum(_table_rows(original[name]) for name in only_original),
    }


def _has_differences(check: Mapping) -> bool:
    return any(check.get(field) for field in (
        "members_only_in_restored", "members_only_in_retained", "schema_changes", "metadata_changes", "type_changes",
        "columns_only_in_restored", "columns_only_in_retained", "rows_only_in_restored", "rows_only_in_retained"))


def _target(expected: str | None, action: str) -> dict:
    """The bucket and endpoint this run writes to, which must be the bucket the operator named."""
    try:
        r2.require_credentials(action)
    except RuntimeError as missing:
        raise ConversionRefused(str(missing)) from missing
    bucket, endpoint = os.getenv("R2_BUCKET_NAME"), os.getenv("R2_ENDPOINT")
    if not bucket or not endpoint:
        raise ConversionRefused("R2_BUCKET_NAME and R2_ENDPOINT must both be set: no bucket is assumed")
    if bucket != expected:
        raise ConversionRefused(f"this run would write to bucket {bucket}, not the --expect-bucket {expected}")
    target = {"bucket": bucket, "endpoint_host": urlsplit(endpoint).hostname}
    print(f"{action}: bucket {bucket} at {target['endpoint_host']}", flush=True)
    return target


def _same_generation(entry: Mapping | None, other: Mapping | None) -> bool:
    def named(value):
        return None if value is None else {key: item for key, item in value.items() if key != "publishedAt"}

    return named(entry) == named(other)


def _published_state(family: str, entry: Mapping | None, receipt_path: Path, bucket: str) -> str:
    return (f"{family} IS published as {(entry or {}).get('artifactDigest')}. Roll back with: {SCRIPT} --rollback "
            f"{receipt_path} --expect-bucket {bucket} (and the same --env-file)")


def _uncertain_state(action: str, receipt_path: Path, bucket: str | None) -> str:
    return (f"a {action} was attempted and its result is not known. "
            f"{SCRIPT} --rollback {receipt_path} --expect-bucket {bucket} reads the stored index and "
            "restores the captured entry if this conversion's generation is what it names")


def _write_receipt(path: Path, receipt: Mapping) -> None:
    """Keep the previous complete rollback record until its replacement is written and synced."""
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(receipt, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _prepared_digest(receipt: Mapping) -> str:
    value = dict(receipt)
    value["prepared"] = {key: item for key, item in value["prepared"].items() if key != "receiptDigest"}
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _seal_prepared(receipt: dict, built: _Built, work: Path) -> None:
    artifact = json.loads((built.generation / "artifact.json").read_bytes())
    receipt["prepared"] = {
        "format": "spicy-regs-native-conversion-prepared", "version": 1,
        "generationPin": {key: artifact[key] for key in ("logicalId", "artifactDigest")},
        "capturedIndexSha256": _file_digest(work / "captured-publication.v2.json"),
        "evidenceDirectories": [path.resolve().relative_to(work.resolve()).as_posix() for path in built.evidence],
    }
    receipt["prepared"]["receiptDigest"] = _prepared_digest(receipt)


def _prepared_path(work: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = work / path
    if path.is_symlink() or not path.resolve().is_relative_to(work.resolve()) or not path.exists():
        raise ConversionRefused("Prepared input is missing or outside its original work directory")
    return path


def _prepared_refusal_state(receipt_path: Path) -> str:
    return ("This invocation made no new publication attempt. The saved receipt does not establish the current "
            f"stored pointer. Reconcile any previous publication outcome read-only using {receipt_path} before "
            "another write.")


def _retained_inputs(captured: Mapping, old: Mapping, work: Path) -> dict[str, Path]:
    """Recheck retained bytes against publisher pins; no cached proof substitutes for these checks."""
    from spicy_regs.generations import _table_info

    retained = {}
    for key, descriptor in old["tables"].items():
        path = _prepared_path(work, "retained/" + (key.removesuffix(".parquet") if "members" in descriptor else key))
        members = publication.table_members(captured, key)
        expected = {member.key.removeprefix(key.removesuffix(".parquet") + "/") if path.is_dir() else member.key
                    for member in members}
        if set(_table_files(path)) != expected:
            raise ConversionRefused(f"{key}: retained member layout changed")
        for member in members:
            target = _prepared_path(work, "retained/" + member.key)
            if target.stat().st_size != member.byte_size or _file_digest(target) != member.sha256:
                raise ConversionRefused(f"{key}: retained member bytes changed")
            observed = _table_info(target)
            if observed["columns"] != descriptor["columns"] or observed["rows"] != member.rows:
                raise ConversionRefused(f"{key}: retained member schema or rows changed")
        retained[key] = path
    return retained


def publish_prepared(receipt_path: Path, *, allowed: Sequence[str], expected_main: str, expected_spicy_docs: str,
                     expect_bucket: str, remote: str = "origin", state: Callable[[str], dict] | None = None,
                     expected_producer: str | None = None) -> dict:
    """Publish one completed official dry run after rechecking its bytes and restoring its exact inputs again.

    The source writer never runs here. Separate field, server and coverage qualifications in the release package
    must name this receipt's exact artifact; their reports do not replace native admission or restoration.
    """
    state = state or source_state
    work = receipt_path.parent
    try:
        now = state(remote)
        _refuse_moved(now, main=expected_main, spicy_docs=expected_spicy_docs)
        if receipt_path.name != RECEIPT or receipt_path.is_symlink() or not receipt_path.is_file():
            raise ConversionRefused("--publish-prepared needs an existing official conversion.json")
        receipt = json.loads(receipt_path.read_bytes())
        sealed = receipt.get("prepared", {})
        if (sealed.get("format") != "spicy-regs-native-conversion-prepared" or sealed.get("version") != 1
                or sealed.get("receiptDigest") != _prepared_digest(receipt)
                or "published" not in receipt or receipt["published"] is not None
                or any(key in receipt for key in ("refused", "publish_attempt", "rollback_attempt", "rolled_back"))):
            raise ConversionRefused("Prepared receipt is incomplete, altered, refused, or already attempted")
        family, base = receipt["family"], receipt["public_url"]
        if family not in allowed:
            raise ConversionRefused(f"{family} is not in this run's allow-list")
        producer_implementation = _prepared_implementation(receipt["source"], now, expected_producer)
        if base != (os.getenv("R2_PUBLIC_URL") or "").rstrip("/"):
            raise ConversionRefused("Prepared public URL differs from R2_PUBLIC_URL")
        capture_path = _prepared_path(work, "captured-publication.v2.json")
        if _file_digest(capture_path) != sealed["capturedIndexSha256"]:
            raise ConversionRefused("Prepared publication snapshot changed")
        captured = publication.parse_index(capture_path.read_bytes())
        old = captured["families"][family]
        if old != receipt["captured"]["entry"] or "etlReceipts" in old:
            raise ConversionRefused("Prepared captured family differs or is already native")
        target = _target(expect_bucket, "Publishing a prepared conversion")
        if publication.stored_family(r2.get_r2_client(), target["bucket"], family) != old:
            raise ConversionRefused("Prepared family's complete captured entry changed; nothing was uploaded")
        retained = _retained_inputs(captured, old, work)
        from spicy_regs.generations import verify_generation

        generation = _prepared_path(work, receipt["generation"]["directory"])
        artifact = verify_generation(generation)
        spec = artifact.root["spec"]
        if (artifact.pin.as_dict() != sealed["generationPin"]
                or artifact.pin.artifact_digest != receipt["generation"]["artifactDigest"]
                or spec["family"] != family or spec["publicationStatus"] != "complete-family"
                or spec["readSnapshot"] != captured
                or spec["etlReceipts"]["generationId"] != receipt["generation"]["receiptGenerationId"]
                or artifact.root["producer"]["implementationId"] != producer_implementation
                or artifact.root["producer"]["verifierImplementationId"] != producer_implementation
                or any(version(name) != value for name, value in spec["packages"].items())):
            raise ConversionRefused("Prepared generation, captured inputs, or implementation differs")
        evidence = tuple(_prepared_path(work, value) for value in sealed["evidenceDirectories"])
        from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, verify_evidence

        admitted = [verify_evidence(path) for path in evidence]
        if ([{"role": INPUT_ROLE, **item.pin.as_dict()} for item in admitted]
                != [item for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE]
                or any(item.root["spec"]["family"] != family or item.root["inputs"] != [
                    value for value in artifact.root["inputs"] if value["role"] == PRIOR_ROLE] for item in admitted)):
            raise ConversionRefused("Prepared source evidence differs from the generation")
        from spicy_regs.court_receipts import POLICIES as court_policies
        from spicy_regs.pipelines.rollups.base import RollupPipeline

        regulatory = _regulatory_base_class(family)
        if regulatory is not None and (set(old["tables"]) != {regulatory.output}
                                       or "members" in old["tables"][regulatory.output]):
            raise ConversionRefused("Prepared regulatory family membership differs")
        if regulatory is not None:
            built = _regulatory_base_built(regulatory, generation, retained, base, work,
                                          spec["etlReceipts"]["generationId"], evidence)
        elif all(key.removesuffix(".parquet") in court_policies for key in old["tables"]):
            subjects = {key.removesuffix(".parquet"): generation / key for key in old["tables"]}
            built = _court_built(generation, subjects, retained, base, work, spec["etlReceipts"]["generationId"], evidence)
        elif (cls := _rollup_class(family)) is not None:
            if set(cls.source_outputs) != set(old["tables"]) or any(
                ("members" in table) != (key in cls.partitioned) or "members" in table
                and tuple(table["partitionColumns"]) != tuple(cls.partitioned[key]) for key, table in old["tables"].items()
            ):
                raise ConversionRefused("Prepared family membership or partition declaration differs")
            from spicy_regs.selected_generations import SelectedDataset, remember_selection

            selection_root = work / "prepared-selection" / uuid4().hex
            selections = []
            for policy in cls.receipt_policies:
                path = generation / (policy.dataset if "partitionColumns" in spec["tables"].get(policy.dataset + ".parquet", {})
                                     else policy.dataset + ".parquet")
                subjects = () if policy.receipt_only else tuple(_table_files(path).values())
                selections.append(SelectedDataset(policy.dataset, subjects, generation / "etl_receipts.parquet",
                                                  spec["etlReceipts"]["generationId"]))
            remember_selection(selection_root, selections)
            built = _rollup_built(cls, generation, retained, base, work, evidence, selection_root)
        else:
            raise ConversionRefused("Prepared family has no maintained converter reader")
        _, replaced = publication.load_family_root(base, old)
        scheduled = _rollup_class(family, RollupPipeline)
        retains = bool(replaced["inputs"]) or bool(scheduled and scheduled.retain_source_evidence)
        proof, problems, conversion_proof = _qualify_conversion(
            family, built, old, retained, replaced, retains, _handed_on(base, replaced))
        if problems or any(receipt.get(key) != value for key, value in proof.items()):
            raise ConversionRefused(f"Prepared restoration proof differs: {problems}")
    except (ConversionRefused, publication.PublicationError) as failure:
        raise ConversionRefused(str(failure), state=_prepared_refusal_state(receipt_path)) from failure
    except (KeyError, TypeError, ValueError, OSError) as failure:
        raise ConversionRefused(f"Prepared conversion cannot be verified: {type(failure).__name__}: {failure}",
                                state=_prepared_refusal_state(receipt_path)) from failure
    receipt["target"] = target
    return _publish_conversion(receipt, built, captured, work, expected_main=expected_main,
                               expected_spicy_docs=expected_spicy_docs, remote=remote, state=state, exact_prior=True,
                               publisher_source=now, conversion_proof=conversion_proof)


def convert(family: str, *, allowed: Sequence[str], work: Path, expected_main: str, expected_spicy_docs: str,
            publish: bool = False, remote: str = "origin", state: Callable[[str], dict] | None = None,
            expect_bucket: str | None = None) -> dict:
    """Convert ``family`` in ``work``; with ``publish``, move its pointer and read the result back anonymously."""
    if family not in allowed:
        raise ConversionRefused(f"{family} is not in this run's allow-list ({', '.join(sorted(allowed)) or 'empty'})")
    work = work.resolve()
    state = state or source_state
    started = state(remote)
    _refuse_moved(started, main=expected_main, spicy_docs=expected_spicy_docs)
    base = (os.getenv("R2_PUBLIC_URL") or "").rstrip("/")
    if not base:
        raise ConversionRefused("R2_PUBLIC_URL is not set: the published family is read from it")
    # A run that cannot publish says so before it converts anything.
    target = _target(expect_bucket, "Publishing a converted family") if publish else None
    if work.exists() and any(work.iterdir()):
        raise ConversionRefused(f"{work} is not empty; every conversion keeps its own receipt and build")
    work.mkdir(parents=True, exist_ok=True)
    receipt: dict = {"family": family, "started_at": _now(), "source": started, "public_url": base, "target": target}
    pointer = f"{base}/{publication.INDEX_V2_KEY}"
    before = r2.public_object_version(pointer)
    with publication.snapshot(base) as captured:
        if before is None or r2.public_object_version(pointer) != before:
            raise ConversionRefused("The publication index moved while it was being captured; run again")
        old = captured["families"].get(family)
        if old is None:
            raise ConversionRefused(f"{family} is not a published family")
        if "etlReceipts" in old:
            raise ConversionRefused(f"{family} already carries ETL receipts: it is native")
        (work / "captured-publication.v2.json").write_text(json.dumps(captured, sort_keys=True), encoding="utf-8")
        receipt["captured"] = {"etag": before["etag"], "bytes": before["bytes"], "entry": old}
        retained = {key: _retain_table(base, captured, key, work / "retained") for key in old["tables"]}
        from spicy_regs.court_receipts import POLICIES as court_policies
        from spicy_regs.pipelines.rollups.base import RollupPipeline

        cls = None
        regulatory = _regulatory_base_class(family)
        if regulatory is not None and (set(old["tables"]) != {regulatory.output}
                                       or "members" in old["tables"][regulatory.output]):
            raise ConversionRefused("Regulatory family must contain its one maintained unsplit output")
        if regulatory is not None:
            from spicy_regs.transforms.regulations_receipts import policy

            claimed = {policy(regulatory.output.removesuffix(".parquet")).dataset}
        elif all(key.removesuffix(".parquet") in court_policies for key in old["tables"]):
            if any(path.is_dir() for path in retained.values()):
                raise ConversionRefused(f"{family}: the court converter has no split-table writer")
            claimed = {key.removesuffix(".parquet") for key in old["tables"]}
        elif (cls := _rollup_class(family)) is not None:
            for key, table in old["tables"].items():
                if ("members" in table) != (key in cls.partitioned) or (
                    "members" in table and tuple(table["partitionColumns"]) != tuple(cls.partitioned[key])
                ):
                    raise ConversionRefused(f"{family}: {key} partition layout differs from its maintained writer")
            claimed = {policy.dataset for policy in cls.receipt_policies}
        else:
            raise ConversionRefused(f"{family} has no subject/receipt rollup or court writer to convert it through")
        # The index gives every dataset an entry lists one owning family; publication would refuse a second. A
        # shared log is listed by none.
        from spicy_regs.subject_catalog import shared_receipt_logs

        claimed -= shared_receipt_logs()
        for other, entry in captured["families"].items():
            owned = {key.removesuffix(".parquet") for key in entry["tables"]} | set(entry.get("etlReceipts", {}).get("datasets", ()))
            if other != family and (shared := sorted(claimed & owned)):
                raise ConversionRefused(f"{family}: its receipts would hold {shared}, which already belongs to family {other}")
        # A family whose generations carry source evidence keeps carrying it: the converted generation names the
        # one it replaces and hands on what that one's journal held for the next run.
        _, replaced = publication.load_family_root(base, old)
        scheduled = _rollup_class(family, RollupPipeline)
        retains = bool(replaced["inputs"]) or bool(scheduled and scheduled.retain_source_evidence)
        handed_on = _handed_on(base, replaced)
        elapsed = time.monotonic()
        try:
            if regulatory is not None:
                built = _convert_regulatory_base(regulatory, captured, retained, base, work, handed_on, retains)
            else:
                built = (_convert_court(family, old, captured, retained, base, work, handed_on, retains) if cls is None
                         else _convert_rollup(cls, old, retained, base, work, handed_on))
        except ValueError as refusal:  # how the writers refuse a row or a family they cannot classify
            raise ConversionRefused(f"{family}: {refusal}") from refusal
        receipt["convert_seconds"] = round(time.monotonic() - elapsed, 1)

    proof, problems, conversion_proof = _qualify_conversion(family, built, old, retained, replaced, retains, handed_on)
    receipt.update(proof)

    def save() -> dict:
        _write_receipt(work / RECEIPT, receipt)
        return receipt

    if problems:
        receipt["refused"] = problems
        save()
        raise ConversionRefused(f"{family}: " + "; ".join(problems))
    if not publish or target is None:
        receipt["published"] = None
        _seal_prepared(receipt, built, work)
        return save()

    return _publish_conversion(receipt, built, captured, work, expected_main=expected_main,
                               expected_spicy_docs=expected_spicy_docs, remote=remote, state=state,
                               conversion_proof=conversion_proof)


def _qualify_conversion(family: str, built: _Built, old: Mapping, retained: Mapping[str, Path],
                        replaced: Mapping, retains: bool, handed_on: Sequence[Mapping],
                        ) -> tuple[dict, list[str], publication.NativeConversionProof | None]:
    artifact = json.loads((built.generation / "artifact.json").read_bytes())
    outcomes = Counter(outcome for batch in pq.ParquetFile(built.generation / "etl_receipts.parquet").iter_batches(
        columns=["outcome"]) for outcome in batch.column(0).to_pylist())
    tables, problems = {}, []
    if unconverted := {name: count for name, count in outcomes.items() if name not in CONVERTED_OUTCOMES}:
        problems.append(f"receipts hold {unconverted}; every retained row must convert")
    for key, table in old["tables"].items():
        dataset = key.removesuffix(".parquet")
        subject = built.generation / (dataset if "members" in table else key)
        try:
            restored = built.restore(dataset)
            actual, original = _table_files(restored), _table_files(retained[key])
            if restored.is_dir() != retained[key].is_dir() or actual.keys() != original.keys():
                raise ValueError("Processing member layout changed")
            for name in original:
                _check_processing_restore(original[name], actual[name], label=dataset)
            check = _table_differences(restored, retained[key]) | {"retained_rows": table["rows"]}
        except ValueError as unreadable:
            problems.append(f"{dataset} does not restore: {unreadable}")
            continue
        if built.declared:
            # Domain types belong to the native subject. The original processing types are checked by exact restore.
            declared = built.declared(dataset)
            if not subject.exists() or any(not pq.read_schema(member).equals(declared)
                                           for member in _table_files(subject).values()):
                problems.append(f"{dataset}: actual native subject schema differs from its declared court policy")
        if subject.exists():
            check["subject_rows"] = _table_rows(subject)
            if subject.is_dir():
                source_members, subject_members = _table_files(retained[key]), _table_files(subject)
                if source_members.keys() != subject_members.keys() or any(
                    _table_rows(subject_members[name]) != _table_rows(source_members[name]) for name in source_members
                    if name in subject_members
                ):
                    problems.append(f"{key}: native subject members differ from the retained partition layout or counts")
        elif key not in built.receipt_only_tables:
            problems.append(f"{key} is neither a subject table nor a declared receipt-only table")
        tables[dataset] = check
        if check.get("subject_rows", table["rows"]) != table["rows"] or _has_differences(check):
            problems.append(f"{dataset} does not restore to its retained table: {check}")
    expected = (set(old["tables"]) | built.added_tables) - built.receipt_only_tables
    if (subjects := set(artifact["spec"]["tables"])) != expected:
        problems.append(f"the generation holds {sorted(subjects)}; publication expects {sorted(expected)}")
    # Keep the original physical-file guard as well as the logical membership check.
    # Receipt indexes are declared auxiliary members, never public subject tables.
    from spicy_regs.receipt_key_index import KEY as RECEIPT_INDEX_KEY

    auxiliary = {"etl_receipts.parquet"}
    if artifact["spec"].get("etlReceipts", {}).get("keyIndex"):
        auxiliary.add(RECEIPT_INDEX_KEY)
    physical = {publication.member_table(path.relative_to(built.generation).as_posix())
                for path in built.generation.rglob("*.parquet") if path.relative_to(built.generation).as_posix() not in auxiliary}
    if physical != subjects:
        problems.append(f"the generation's physical tables {sorted(physical)} differ from its declared tables {sorted(subjects)}")
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE

    lineage = {"replaced_generation_inputs": replaced["inputs"], "inputs": artifact["inputs"], "handed_on_events": len(handed_on)}
    if retains:
        prior = {"role": PRIOR_ROLE, "logicalId": old["logicalId"], "artifactDigest": old["artifactDigest"]}
        if prior not in artifact["inputs"] or sum(item["role"] == INPUT_ROLE for item in artifact["inputs"]) != 1:
            problems.append(f"{family} retains source evidence and the converted generation names {artifact['inputs']}: "
                            "it must name the replaced generation and one evidence artifact")
        journaled = [{name: value for name, value in row.items() if name != "recorded_at"}
                     for directory in built.evidence
                     for row in map(json.loads, (directory / "journal.jsonl").read_text().splitlines())
                     if row.get("event") in INHERITED_EVENTS]
        if journaled != list(handed_on):
            problems.append(f"{len(handed_on)} journal events the next run reads back were not all handed on")
    from rulespec_artifacts import canonical_json_bytes

    conversion_proof = (None if problems else publication.NativeConversionProof(
        artifact["logicalId"], artifact["artifactDigest"], canonical_json_bytes(old), frozenset(old["tables"])))
    return {
        "policy_versions": dict(built.policies),
        "receipt_outcomes": dict(outcomes),
        "tables": tables,
        "lineage": lineage,
        "generation": {"directory": str(built.generation), "artifactDigest": artifact["artifactDigest"],
                       "receiptGenerationId": artifact["spec"]["etlReceipts"]["generationId"]},
    }, problems, conversion_proof


def _publish_conversion(receipt: dict, built: _Built, captured: Mapping, work: Path, *,
                        expected_main: str, expected_spicy_docs: str, remote: str, state: Callable[[str], dict],
                        exact_prior: bool = False, publisher_source: Mapping | None = None,
                        conversion_proof: publication.NativeConversionProof | None = None) -> dict:
    exact_prior = exact_prior or conversion_proof is not None
    family, base, target = (receipt[name] for name in ("family", "public_url", "target"))
    started = publisher_source if publisher_source is not None else receipt["source"]
    old, tables = receipt["captured"]["entry"], receipt["tables"]
    artifact = json.loads((built.generation / "artifact.json").read_bytes())

    def save() -> dict:
        _write_receipt(work / RECEIPT, receipt)
        return receipt

    try:
        _refuse_moved(now := state(remote), main=expected_main, spicy_docs=expected_spicy_docs)
        if now != started:
            raise ConversionRefused("Main or the pinned wheel moved while the family converted")
        client, bucket = r2.get_r2_client(), target["bucket"]
    except (ConversionRefused, publication.PublicationError) as failure:
        if exact_prior:
            raise ConversionRefused(str(failure), state=_prepared_refusal_state(work / RECEIPT)) from failure
        raise
    # The rollback file exists before the pointer can move: it names the captured entry and the generation intended.
    from spicy_regs.generations import implementation_id

    attempt: dict = {"at": _now(), "outcome": "attempted", "error": None,
                     "publisher": {"source": now, "implementationId": implementation_id()}}
    receipt |= {"published": None, "publish_attempt": attempt}
    save()
    try:
        index = publication.publish_generation(
            built.generation, client=client, bucket=bucket, prior_index=captured, evidence_directories=built.evidence,
            added_tables=built.added_tables, receipt_only_tables=built.receipt_only_tables, exact_prior=exact_prior,
            conversion_proof=conversion_proof)
        entry = index["families"][family]
    except Exception as failure:
        # The publish call did not return. A conditional write whose response was lost has still moved the pointer,
        # so the stored index, read with credentials, says which of three states holds.
        reason = f"{type(failure).__name__}: {failure}"
        try:
            entry = publication.stored_family(client, bucket, family)
        except Exception as unreadable:
            attempt |= {"error": reason, "verification_error": f"{type(unreadable).__name__}: {unreadable}"}
            save()
            raise ConversionRefused(
                f"{family}: {reason}; stored index could not be read ({attempt['verification_error']})",
                state=_uncertain_state("publish", work / RECEIPT, bucket)) from unreadable
        if exact_prior and entry != old and _same_generation(entry, old):
            attempt |= {"outcome": "captured entry changed", "error": reason, "entry": entry}
            save()
            raise ConversionRefused(
                f"{family}: {reason}", state=f"{family}'s captured entry changed; this conversion's generation "
                "was not published by this attempt. No automatic rollback was performed") from failure
        if _same_generation(entry, old):
            attempt |= {"outcome": "not moved", "error": reason}
            save()
            raise ConversionRefused(f"{family}: {reason}") from failure
        if (entry or {}).get("artifactDigest") != artifact["artifactDigest"]:
            attempt |= {"outcome": "moved elsewhere", "error": reason, "entry": entry}
            save()
            raise ConversionRefused(
                f"{family}: {reason}", state=f"{family} now names {(entry or {}).get('artifactDigest')}, which is "
                "neither the captured generation nor this conversion's: another writer published it, and this "
                "receipt rolls nothing back") from failure
        attempt |= {"outcome": "moved to this conversion's generation", "error": reason}
        receipt["published"] = {"entry": entry}
        save()
        try:
            publication.rederive_v1(client, bucket)
        except Exception as repair:
            raise ConversionRefused(
                f"{family}: published, but the derived index could not be repaired ({type(repair).__name__}: {repair})",
                state=_published_state(family, entry, work / RECEIPT, bucket)) from repair
    else:
        attempt["outcome"] = "published"
    receipt["published"] = {"entry": entry}
    save()
    published = _published_state(family, entry, work / RECEIPT, bucket)
    try:
        from spicy_regs.sources.cloudflare import purge_urls

        purge_urls([f"{base}/{key}" for key in (publication.INDEX_V2_KEY, publication.INDEX_KEY)])
        read_index = publication.current_index(base)
        entry = read_index["families"].get(family) or {}
        read = {dataset: built.read_published(dataset, read_index) for dataset in tables}
    except Exception as failure:
        raise ConversionRefused(f"{family} could not be read back ({type(failure).__name__}: {failure})",
                                state=published) from failure
    receipt["read_back"] = {
        "artifactDigest": entry.get("artifactDigest"),
        "etl_receipts_rows": entry.get("etlReceipts", {}).get("rows"),
        "table_rows": {key: table["rows"] for key, table in entry.get("tables", {}).items()},
        "anonymous_read_rows": read,
    }
    save()
    wrong = [key for key, table in entry.get("tables", {}).items() if table["rows"] != old["tables"][key]["rows"]]
    wrong += [name for name, rows in read.items() if rows != old["tables"][name + ".parquet"]["rows"]]
    if entry.get("artifactDigest") != artifact["artifactDigest"] or "etlReceipts" not in entry or wrong:
        raise ConversionRefused(f"{family} does not read back as converted ({wrong or entry.get('artifactDigest')})",
                                state=published)
    return receipt


def rollback(receipt_path: Path, *, discard_newer: bool = False, expect_bucket: str | None = None) -> dict:
    """Point the family back at the entry the conversion captured, if it names what the conversion published.

    The receipt is enough from the moment a publish was attempted, whether or not the process saw the write land.
    The stored index decides: the captured generation (nothing to do), this conversion's (restore), or another
    writer's (refused unless ``discard_newer``).
    """
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not receipt.get("publish_attempt") and not receipt.get("published"):
        raise ConversionRefused(f"{receipt_path} records no publish attempt: there is nothing to roll back")
    target = _target(expect_bucket, "Rolling back a converted family")
    if receipt.get("target") and receipt["target"] != target:
        raise ConversionRefused(f"{receipt_path} was written to {receipt['target']}, not {target}")
    family, captured = receipt["family"], receipt["captured"]["entry"]
    client = r2.get_r2_client()
    stored = publication.stored_family(client, target["bucket"], family)
    ours = (stored or {}).get("artifactDigest") == receipt["generation"]["artifactDigest"]
    if not ours and not _same_generation(stored, captured) and not discard_newer:
        raise ConversionRefused(
            f"{family} names {(stored or {}).get('artifactDigest')}, neither the captured generation nor this "
            "conversion's", state=f"{family} is unchanged by this rollback; --discard-newer would replace that generation")
    attempt = {"at": _now(), "outcome": "attempted", "found_entry": stored,
               "discarded_newer": discard_newer}
    receipt["rollback_attempt"] = attempt

    def save() -> None:
        _write_receipt(receipt_path, receipt)

    save()
    try:
        publication.restore_family(client, target["bucket"], family, captured,
                                   expected=None if discard_newer else stored)
    except Exception as failure:
        attempt["error"] = f"{type(failure).__name__}: {failure}"
        try:
            observed = publication.stored_family(client, target["bucket"], family)
        except Exception as unreadable:
            attempt["verification_error"] = f"{type(unreadable).__name__}: {unreadable}"
            save()
            raise ConversionRefused(
                f"{family}: rollback failed ({attempt['error']}); stored index could not be read "
                f"({attempt['verification_error']})",
                state=_uncertain_state("rollback", receipt_path, expect_bucket)) from unreadable
        if observed != captured:
            attempt.update(outcome="not restored", entry=observed)
            save()
            raise ConversionRefused(
                f"{family}: rollback failed ({attempt['error']})",
                state=f"{family} names {(observed or {}).get('artifactDigest')} in the stored index; "
                      f"retry {SCRIPT} --rollback {receipt_path} --expect-bucket {expect_bucket}") from failure
        attempt["outcome"] = "restored after lost response"
    else:
        attempt["outcome"] = "restored"
    save()
    # Also repairs an interrupted earlier rollback whose v2 write already landed.
    from spicy_regs.sources.cloudflare import purge_urls

    base = receipt["public_url"]
    try:
        publication.rederive_v1(client, target["bucket"])
        purge_urls([f"{base}/{key}" for key in (publication.INDEX_V2_KEY, publication.INDEX_KEY)])
        entry = publication.current_index(base)["families"].get(family)
    except Exception as failure:
        raise ConversionRefused(
            f"{family}: rollback read-back or derived index failed ({type(failure).__name__}: {failure})",
            state=f"{family}'s stored v2 entry was restored; public read-back and derived index are unverified. "
                  f"Retry {SCRIPT} --rollback {receipt_path} --expect-bucket {expect_bucket}") from failure
    receipt["rolled_back"] = {"at": _now(), "entry": entry, "discarded_newer": discard_newer,
                              "found": "this conversion's generation" if ours else
                                       "the captured generation" if _same_generation(stored, captured) else "another generation"}
    save()
    if entry != captured:
        raise ConversionRefused(f"{family} does not read back as its captured entry after the rollback",
                                state=f"{family} names {(entry or {}).get('artifactDigest')} when read publicly")
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("family", nargs="?", help="The one published family to convert")
    parser.add_argument("--allow", action="append", default=[], metavar="FAMILY[,FAMILY...]",
                        help="This run's approved families; a family outside the list is refused")
    parser.add_argument("--work", type=Path, help="A new directory for this family's build and receipt")
    parser.add_argument("--expect-main", help="The main commit this run was started for")
    parser.add_argument("--expect-spicy-docs", help="The SpicyDocs version main pinned when this run was started")
    parser.add_argument("--remote", default="origin", help="The git remote whose main is checked (default: origin)")
    parser.add_argument("--publish", action="store_true", help="Move the family's pointer; without it nothing is uploaded")
    parser.add_argument("--publish-prepared", type=Path, metavar="CONVERSION_JSON",
                        help="Recheck and publish one completed official dry run without rerunning its writer")
    parser.add_argument("--expect-producer", help="Full retained producer commit for a prepared publication")
    parser.add_argument("--expect-bucket", help="The bucket --publish or --rollback may write to; any other is refused")
    parser.add_argument("--rollback", type=Path, metavar="RECEIPT", help="Restore the entry a conversion receipt captured")
    parser.add_argument("--discard-newer", action="store_true",
                        help="With --rollback: also discard a generation published after the conversion")
    parser.add_argument("--env-file", type=Path, help="Read R2_* settings from this file; nothing is loaded implicitly")
    args = parser.parse_args(argv)
    if args.rollback and (args.family or args.allow or args.work or args.expect_main or args.expect_spicy_docs or args.publish_prepared
                          or args.expect_producer or args.publish or args.remote != "origin"):
        parser.error("--rollback takes only --expect-bucket, --env-file and --discard-newer")
    if args.expect_producer and not args.publish_prepared:
        parser.error("--expect-producer belongs to --publish-prepared")
    if args.publish_prepared and (args.family or args.work or args.publish):
        parser.error("--publish-prepared cannot rebuild a family; omit FAMILY, --work and --publish")
    if (args.publish or args.publish_prepared or args.rollback) and not args.expect_bucket:
        parser.error("publication and rollback need --expect-bucket: the bucket this run may write to")
    if args.discard_newer and not args.rollback:
        parser.error("--discard-newer belongs to --rollback")
    entered_prepared = False
    try:
        if args.env_file:
            from dotenv import dotenv_values

            settings = {name: value for name, value in dotenv_values(args.env_file).items() if value is not None}
            if not settings:
                parser.error(f"{args.env_file} holds no settings")
            if differing := sorted(name for name, value in settings.items() if os.environ.get(name, value) != value):
                raise ConversionRefused(f"{', '.join(differing)} in the environment differ from {args.env_file}; "
                                        "unset them or correct the file, so the write target is the one written down")
            os.environ.update(settings)
        if args.rollback:
            done = rollback(args.rollback, discard_newer=args.discard_newer, expect_bucket=args.expect_bucket)
            print(f"{done['family']}: found {done['rolled_back']['found']}; the pointer names "
                  f"{done['rolled_back']['entry']['artifactDigest']}")
            return 0
        if args.publish_prepared:
            if not (args.allow and args.expect_main and args.expect_spicy_docs):
                parser.error("--publish-prepared needs --allow, --expect-main and --expect-spicy-docs")
            entered_prepared = True
            done = publish_prepared(
                args.publish_prepared, allowed=[name for value in args.allow for name in value.split(",") if name],
                expected_main=args.expect_main, expected_spicy_docs=args.expect_spicy_docs,
                remote=args.remote, expect_bucket=args.expect_bucket, expected_producer=args.expect_producer)
        elif not (args.family and args.work and args.expect_main and args.expect_spicy_docs):
            parser.error("a conversion needs FAMILY, --allow, --work, --expect-main and --expect-spicy-docs")
        else:
            done = convert(
                args.family, allowed=[name for value in args.allow for name in value.split(",") if name], work=args.work,
                expected_main=args.expect_main, expected_spicy_docs=args.expect_spicy_docs, publish=args.publish,
                remote=args.remote, expect_bucket=args.expect_bucket)
    except (ConversionRefused, publication.PublicationError) as refusal:
        print(f"REFUSED: {refusal}", file=sys.stderr)
        result_state = (_prepared_refusal_state(args.publish_prepared) if args.publish_prepared and not entered_prepared
                        else getattr(refusal, "state", NOTHING_PUBLISHED))
        print(f"STATE: {result_state}", file=sys.stderr)
        return 1
    except (Exception, KeyboardInterrupt) as failure:
        # Anything else is not a refusal the command chose, so it does not claim to know the bucket's state.
        operation_receipt = args.rollback or args.publish_prepared or (args.work / RECEIPT if args.work else None)
        attempted = operation_receipt and operation_receipt.exists() and json.loads(
            operation_receipt.read_text(encoding="utf-8")).get("rollback_attempt" if args.rollback else "publish_attempt")
        print(f"FAILED: {type(failure).__name__}: {failure}", file=sys.stderr)
        print("STATE: " + (_uncertain_state("rollback" if args.rollback else "publish", operation_receipt, args.expect_bucket)
                           if attempted else NOTHING_PUBLISHED), file=sys.stderr)
        return 130 if isinstance(failure, KeyboardInterrupt) else 2
    for name, table in done["tables"].items():
        print(f"{name}: {table['retained_rows']:,} retained rows restore exactly")
    print(f"{done['family']}: receipts {done['receipt_outcomes']}, generation {done['generation']['artifactDigest']}")
    print(f"published and read back; receipt {args.publish_prepared or args.work / RECEIPT}" if done["published"]
          else f"not published (no --publish); receipt {args.work / RECEIPT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
