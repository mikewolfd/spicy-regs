"""Convert one old-shape rollup family to native subjects and receipts, once, through that family's own writer.

A scheduled rollup refuses a published family that carries no ETL receipts (``selected_generations``), so a family
whose builder reads its own prior cannot convert itself. This command does it explicitly, one named family at a time:
the family's retained published tables stand in for its builder's output, its maintained writer splits them into
subjects and receipts, and the standard admission publishes them. No scheduled path gains an old-shape reader.

The receipt it writes keeps the captured index entry, so ``--rollback`` can put the pointer back. Without
``--publish`` nothing is written outside the work directory. Runbook: ``docs/native-family-conversion.md``.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import pkgutil
import shutil
import subprocess
import sys
import time
import tomllib
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from spicy_regs.sources import publication, r2

RECEIPT = "conversion.json"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class ConversionRefused(RuntimeError):
    """The conversion stopped; nothing published changed unless the message says the pointer already moved."""


def source_state(remote: str) -> dict:
    """What this process would convert with: the checkout, the remote's main, and the SpicyDocs wheel beside its pin."""
    root = Path(__file__).resolve().parents[2]

    def git(*arguments: str) -> str:
        return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True, text=True,
                              env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).stdout.strip()

    locked = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))["package"]
    return {
        "checkout": git("rev-parse", "HEAD"),
        "uncommitted": git("status", "--porcelain", "--untracked-files=no").splitlines(),
        "main": git("ls-remote", remote, "refs/heads/main").split()[0],
        "spicy_docs": version("spicy-docs"),
        "pinned_spicy_docs": next(package["version"] for package in locked if package["name"] == "spicy-docs"),
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
    if state["spicy_docs"] != spicy_docs or state["pinned_spicy_docs"] != spicy_docs:
        problems.append(f"SpicyDocs is {state['spicy_docs']} installed and {state['pinned_spicy_docs']} pinned, "
                        f"not the {spicy_docs} this run was started for")
    if problems:
        raise ConversionRefused("Main or the pinned wheel moved: " + "; ".join(problems))


@dataclass(frozen=True)
class _Built:
    """One verified local generation and the two reads that prove it: restored locally, then from what is published."""

    generation: Path
    policies: Mapping[str, str]
    restore: Callable[[str], Path]
    read_published: Callable[[str], int]
    evidence: tuple[Path, ...] = ()
    added_tables: frozenset[str] = frozenset()
    receipt_only_tables: frozenset[str] = frozenset()


def _rollup_class(family: str):
    """The scheduled subject/receipt rollup that owns ``family``, or ``None``."""
    import spicy_regs.pipelines.rollups as rollups
    from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup

    for module in pkgutil.iter_modules(rollups.__path__):
        for value in vars(importlib.import_module(f"{rollups.__name__}.{module.name}")).values():
            if (isinstance(value, type) and issubclass(value, SubjectReceiptRollup)
                    and value.__dict__.get("name") == family and value.publication_family is None):
                return value
    return None


def _convert_rollup(cls, old: Mapping, retained: Mapping[str, Path], base: str, work: Path) -> _Built:
    """Run the family's rollup with its builder replaced by "return the retained tables"; everything after is its own."""
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    if set(cls.source_outputs) != set(old["tables"]):
        raise ConversionRefused(
            f"{cls.name}: its rollup writes {sorted(cls.source_outputs)} and the published family holds "
            f"{sorted(old['tables'])}; convert only a family whose table set main still writes")
    # A rollup's own constructor arguments and inputs feed only its builder, which is replaced below.
    pipeline = cls.__new__(cls)
    RollupPipeline.__init__(pipeline, output_dir=work / "build", skip_upload=True)
    pipeline.inputs = ()

    def retained_tables(directory: Path, **_) -> tuple[Path, ...]:
        return tuple(Path(shutil.copyfile(retained[key], directory / key)) for key in cls.source_outputs)

    pipeline.build = lambda output_dir: pipeline.build_receipts(output_dir, retained_tables)
    pipeline.run()
    (generation,) = (work / "build" / "generations").iterdir()

    def read_published(dataset: str) -> int:
        restored = SelectedPriors(work / "read-back" / uuid4().hex, public_url=base).get(dataset)
        return pq.ParquetFile(restored).metadata.num_rows

    return _Built(
        generation,
        {policy.dataset: policy.policy_version for policy in cls.receipt_policies},
        lambda dataset: SelectedPriors(work / "restored" / uuid4().hex, root=work / "build", public_url="").get(dataset),
        read_published,
        (pipeline.source_evidence.artifact_dir,) if pipeline.source_evidence else (),
        frozenset(cls.added_tables),
        frozenset(cls.receipt_only_tables),
    )


def _convert_court(family: str, old: Mapping, captured: Mapping, retained: Mapping[str, Path], base: str,
                   work: Path) -> _Built:
    """Court tables: the shared court writer over each retained table, then the court generation admission."""
    from spicy_regs.court_receipts import (
        POLICIES, build_court_generation, finish_court_output, prior_receipt_selection, read_court_rows,
        restore_processing_input,
    )

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
    build_court_generation(generation, family=family, files=list(subjects.values()),
                           publication_status="complete-family", read_snapshot=captured)

    def restore(dataset: str) -> Path:
        target = work / "restored" / f"{dataset}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        return restore_processing_input(subjects[dataset], target, dataset=dataset,
                                        schema=pq.read_schema(retained[dataset + ".parquet"]))

    def read_published(dataset: str) -> int:
        target = work / "read-back" / uuid4().hex / f"{dataset}.parquet"
        target.parent.mkdir(parents=True)
        (member,) = publication.table_members(publication.current_index(base), dataset + ".parquet")
        publication.fetch_member(base, member, target, member.path)
        receipts, selected = prior_receipt_selection(target, dataset=dataset)
        return sum(1 for _ in read_court_rows(target, dataset=dataset, receipt_path=receipts, generation_id=selected))

    return _Built(generation, {name: POLICIES[name].policy_version for name in subjects}, restore, read_published)


def _differences(restored: Path, retained: Path) -> dict:
    """How the rows a native read restores differ from the retained table: columns, types, and rows each way."""
    import duckdb

    def literal(path: Path) -> str:
        return "'" + str(path).replace("'", "''") + "'"

    with duckdb.connect() as con:
        described = [dict((name, kind) for name, kind, *_ in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({literal(path)})").fetchall()) for path in (restored, retained)]
        shared = [name for name in described[1] if name in described[0]]
        text = ", ".join(f'CAST("{name}" AS VARCHAR) AS "{name}"' for name in shared) or "NULL"
        either = [con.execute(f"SELECT count(*) FROM (SELECT {text} FROM read_parquet({literal(a)}) EXCEPT ALL "
                              f"SELECT {text} FROM read_parquet({literal(b)}))").fetchall()[0][0]
                  for a, b in ((restored, retained), (retained, restored))]
    return {
        "columns_only_in_restored": sorted(set(described[0]) - set(described[1])),
        "columns_only_in_retained": sorted(set(described[1]) - set(described[0])),
        "type_changes": {name: [described[1][name], described[0][name]] for name in shared
                         if described[0][name] != described[1][name]},
        "rows_only_in_restored": either[0],
        "rows_only_in_retained": either[1],
    }


def convert(family: str, *, allowed: Sequence[str], work: Path, expected_main: str, expected_spicy_docs: str,
            publish: bool = False, remote: str = "origin", state: Callable[[str], dict] | None = None) -> dict:
    """Convert ``family`` in ``work``; with ``publish``, move its pointer and read the result back anonymously."""
    if family not in allowed:
        raise ConversionRefused(f"{family} is not in this run's allow-list ({', '.join(sorted(allowed)) or 'empty'})")
    state = state or source_state
    started = state(remote)
    _refuse_moved(started, main=expected_main, spicy_docs=expected_spicy_docs)
    base = (os.getenv("R2_PUBLIC_URL") or "").rstrip("/")
    if not base:
        raise ConversionRefused("R2_PUBLIC_URL is not set: the published family is read from it")
    if work.exists() and any(work.iterdir()):
        raise ConversionRefused(f"{work} is not empty; every conversion keeps its own receipt and build")
    work.mkdir(parents=True, exist_ok=True)
    receipt: dict = {"family": family, "started_at": _now(), "source": started, "public_url": base}
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
        if split := sorted(key for key, table in old["tables"].items() if "members" in table):
            raise ConversionRefused(f"{family}: {split} are stored as several files, which this command does not convert")
        (work / "captured-publication.v2.json").write_text(json.dumps(captured, sort_keys=True), encoding="utf-8")
        receipt["captured"] = {"etag": before["etag"], "bytes": before["bytes"], "entry": old}
        retained = {}
        for key in old["tables"]:
            (member,) = publication.table_members(captured, key)
            retained[key] = work / "retained" / key
            retained[key].parent.mkdir(parents=True, exist_ok=True)
            if not publication.fetch_member(base, member, retained[key], member.path):
                raise ConversionRefused(f"{member.path} is not readable")
        from spicy_regs.court_receipts import POLICIES as court_policies

        elapsed = time.monotonic()
        try:
            if all(key.removesuffix(".parquet") in court_policies for key in old["tables"]):
                built = _convert_court(family, old, captured, retained, base, work)
            elif (cls := _rollup_class(family)) is not None:
                built = _convert_rollup(cls, old, retained, base, work)
            else:
                raise ConversionRefused(f"{family} has no subject/receipt rollup or court writer to convert it through")
        except ValueError as refusal:  # how the writers refuse a row or a family they cannot classify
            raise ConversionRefused(f"{family}: {refusal}") from refusal
        receipt["convert_seconds"] = round(time.monotonic() - elapsed, 1)

    artifact = json.loads((built.generation / "artifact.json").read_bytes())
    outcomes = Counter(outcome for batch in pq.ParquetFile(built.generation / "etl_receipts.parquet").iter_batches(
        columns=["outcome"]) for outcome in batch.column(0).to_pylist())
    tables, problems = {}, []
    if refused := {name: outcomes[name] for name in ("refused", "error") if outcomes[name]}:
        problems.append(f"receipts hold {refused}; every retained row must convert")
    for key, table in old["tables"].items():
        dataset, subject = key.removesuffix(".parquet"), built.generation / key
        check = _differences(built.restore(dataset), retained[key]) | {"retained_rows": table["rows"]}
        if subject.exists():
            check["subject_rows"] = pq.ParquetFile(subject).metadata.num_rows
        elif key not in built.receipt_only_tables:
            problems.append(f"{key} is neither a subject table nor a declared receipt-only table")
        tables[dataset] = check
        if (check.get("subject_rows", table["rows"]) != table["rows"] or check["type_changes"]
                or any(check[name] for name in ("columns_only_in_restored", "columns_only_in_retained",
                                                "rows_only_in_restored", "rows_only_in_retained"))):
            problems.append(f"{dataset} does not restore to its retained table: {check}")
    expected = (set(old["tables"]) | built.added_tables) - built.receipt_only_tables
    if (subjects := {path.name for path in built.generation.glob("*.parquet")} - {"etl_receipts.parquet"}) != expected:
        problems.append(f"the generation holds {sorted(subjects)}; publication expects {sorted(expected)}")
    receipt |= {
        "policy_versions": dict(built.policies),
        "receipt_outcomes": dict(outcomes),
        "tables": tables,
        "generation": {"directory": str(built.generation), "artifactDigest": artifact["artifactDigest"],
                       "receiptGenerationId": artifact["spec"]["etlReceipts"]["generationId"]},
    }

    def save() -> dict:
        (work / RECEIPT).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return receipt

    if problems:
        receipt["refused"] = problems
        save()
        raise ConversionRefused(f"{family}: " + "; ".join(problems))
    if not publish:
        receipt["published"] = None
        return save()

    _refuse_moved(now := state(remote), main=expected_main, spicy_docs=expected_spicy_docs)
    if now != started:
        raise ConversionRefused("Main or the pinned wheel moved while the family converted; nothing was published")
    r2.require_credentials("Publishing a converted family")
    client, bucket = r2.get_r2_client(), os.getenv("R2_BUCKET_NAME", "spicy-regs")
    index = publication.publish_generation(
        built.generation, client=client, bucket=bucket, prior_index=captured, evidence_directories=built.evidence,
        added_tables=built.added_tables, receipt_only_tables=built.receipt_only_tables)
    receipt["published"] = {"entry": index["families"][family]}
    save()  # The pointer has moved: from here a failure is reported against a receipt that can roll it back.
    from spicy_regs.sources.cloudflare import purge_urls

    purge_urls([f"{base}/{key}" for key in (publication.INDEX_V2_KEY, publication.INDEX_KEY)])
    entry = publication.current_index(base)["families"].get(family) or {}
    read = {dataset: built.read_published(dataset) for dataset in tables}
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
        raise ConversionRefused(f"{family} is published but does not read back as converted ({wrong or entry.get('artifactDigest')}); "
                                f"roll back with --rollback {work / RECEIPT}")
    return receipt


def rollback(receipt_path: Path, *, discard_newer: bool = False) -> dict:
    """Point the family back at the entry the conversion captured, while it still names what the conversion published."""
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not receipt.get("published"):
        raise ConversionRefused(f"{receipt_path} records no publication: there is nothing to roll back")
    r2.require_credentials("Rolling back a converted family")
    family, captured = receipt["family"], receipt["captured"]["entry"]
    publication.restore_family(
        r2.get_r2_client(), os.getenv("R2_BUCKET_NAME", "spicy-regs"), family, captured,
        expected=None if discard_newer else receipt["published"]["entry"])
    from spicy_regs.sources.cloudflare import purge_urls

    base = receipt["public_url"]
    purge_urls([f"{base}/{key}" for key in (publication.INDEX_V2_KEY, publication.INDEX_KEY)])
    entry = publication.current_index(base)["families"].get(family)
    receipt["rolled_back"] = {"at": _now(), "entry": entry, "discarded_newer": discard_newer}
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if entry != captured:
        raise ConversionRefused(f"{family} does not read back as its captured entry after the rollback")
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
    parser.add_argument("--rollback", type=Path, metavar="RECEIPT", help="Restore the entry a conversion receipt captured")
    parser.add_argument("--discard-newer", action="store_true",
                        help="With --rollback: also discard a generation published after the conversion")
    parser.add_argument("--env-file", type=Path, help="Read R2_* settings from this file; nothing is loaded implicitly")
    args = parser.parse_args(argv)
    if args.env_file:
        from dotenv import load_dotenv

        if not load_dotenv(args.env_file):
            parser.error(f"{args.env_file} holds no settings")
    try:
        if args.rollback:
            done = rollback(args.rollback, discard_newer=args.discard_newer)
            print(f"{done['family']}: pointer restored to {done['rolled_back']['entry']['artifactDigest']}")
            return 0
        if not (args.family and args.work and args.expect_main and args.expect_spicy_docs):
            parser.error("a conversion needs FAMILY, --allow, --work, --expect-main and --expect-spicy-docs")
        done = convert(
            args.family, allowed=[name for value in args.allow for name in value.split(",") if name], work=args.work,
            expected_main=args.expect_main, expected_spicy_docs=args.expect_spicy_docs, publish=args.publish,
            remote=args.remote)
    except (ConversionRefused, publication.PublicationError) as refusal:
        print(f"REFUSED: {refusal}", file=sys.stderr)
        return 1
    for name, table in done["tables"].items():
        print(f"{name}: {table['retained_rows']:,} retained rows restore exactly")
    print(f"{done['family']}: receipts {done['receipt_outcomes']}, generation {done['generation']['artifactDigest']}")
    print(f"published and read back; receipt {args.work / RECEIPT}" if done["published"]
          else f"not published (no --publish); receipt {args.work / RECEIPT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
