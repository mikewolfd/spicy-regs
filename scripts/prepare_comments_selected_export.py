"""Derive current public comments from one qualified, retained v1 catalog pair.

The earlier-policy catalog export runs in a separate interpreter. This module
only reads that local pair and writes private files; it never opens a catalog.
"""

from collections import Counter
from hashlib import file_digest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4
import os
import subprocess
import sys

import duckdb
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.etl_receipts import (
    ReceiptContext, _rows, read_with_receipts, rebind_receipt,
    selected_subject_policy, validate_receipt_bundle, write_dataset,
)
from spicy_regs.transforms.regulations_receipts import policy


FIELDS = (
    "comment_reference_values_json", "text_extraction_status",
    "pdf_extraction_results_json",
)
EXPECTED_ROWS = 26_418_078
V1_EXPORT_REVISION = "e9dfd216dc999845ea94e6171afb41e0c9380a10"


def identity(path):
    with path.open("rb") as source:
        digest = file_digest(source, "sha256").hexdigest()
    return {"sha256": digest, "byteSize": path.stat().st_size}


def verify_original(pair):
    marker = json.loads((pair / "export-complete.json").read_text())
    metadata = json.loads((pair / "generation.json").read_text())
    if marker.get("metadata") != metadata or set(marker.get("members", {})) != {
        "comments.parquet", "etl_receipts.parquet"
    }:
        raise ValueError("Earlier export completion marker differs")
    for name, expected in marker["members"].items():
        if identity(pair / name) != expected:
            raise ValueError("Earlier export member changed after its snapshot was sealed")
    return marker


class V1CatalogReader:
    """Explicit earlier-policy metadata reads in an independent interpreter."""

    def __init__(self, checkout):
        self.checkout = Path(checkout).resolve()

    def _read(self, snapshot=None):
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.checkout,
                                           text=True).strip()
        if revision != V1_EXPORT_REVISION:
            raise ValueError("Historical reader revision changed")
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=self.checkout, text=True).strip():
            raise ValueError("Historical reader has local changes")
        code = """
from dataclasses import asdict
from pathlib import Path
import json, sys
from spicy_regs.sources import iceberg
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.regulations_receipts import policy
assert policy('comments').policy_version == 'regulations-native-v1'
rt = RECORD_TYPES['comments']
if len(sys.argv) == 2:
    result = asdict(iceberg.catalog_snapshot(rt))
else:
    value = json.loads(sys.argv[2])
    value['receipts'] = iceberg.CatalogSnapshot(**value['receipts'])
    result = iceberg.rows_unchanged_since(rt, iceberg.CatalogPairSnapshot(**value))
Path(sys.argv[1]).write_text(json.dumps(result))
"""
        environment = dict(os.environ, PYTHONPATH=str(self.checkout / "src"))
        arguments = [] if snapshot is None else [json.dumps(snapshot)]
        # Catalog diagnostics may use stdout. Keep the exact JSON result separate
        # and let both diagnostic streams reach the operation log.
        with TemporaryDirectory(prefix="comments-snapshot-") as temporary:
            result = Path(temporary) / "snapshot.json"
            subprocess.run([sys.executable, "-c", code, str(result), *arguments],
                           cwd=self.checkout, env=environment, check=True)
            return json.loads(result.read_text())

    def snapshot(self):
        from spicy_regs.sources.iceberg import CatalogPairSnapshot, CatalogSnapshot
        value = self._read()
        value["receipts"] = CatalogSnapshot(**value["receipts"])
        return CatalogPairSnapshot(**value)

    def rows_unchanged_since(self, snapshot):
        from dataclasses import asdict
        return self._read(asdict(snapshot)) is True


def prepare(pair: Path, destination: Path):
    """Keep the complete earlier pair and derive declared current subjects once."""
    pair, destination = Path(pair), Path(destination)
    sealed = verify_original(pair)
    metadata = json.loads((pair / "generation.json").read_text())
    if metadata.get("dataset") != "comments" or not metadata.get("snapshot"):
        raise ValueError("Exact catalog-pair snapshot metadata is required")
    subjects, receipts = pair / "comments.parquet", pair / "etl_receipts.parquet"
    current = policy("comments")
    earlier = selected_subject_policy(current, [subjects])
    if (earlier.policy_version != "regulations-native-v1"
            or current.policy_version != "regulations-native-v2"):
        raise ValueError("Expected exact declared earlier/current comments policies")
    if metadata.get("policy") != earlier.descriptor() or metadata.get("exportRevision") != V1_EXPORT_REVISION:
        raise ValueError("Earlier export authority differs from the shipped historical policy")
    if pq.ParquetFile(subjects).metadata.num_rows != EXPECTED_ROWS:
        raise ValueError("The complete captured comments population is required")
    source_identity = sealed["members"]["comments.parquet"]
    receipt_identity = sealed["members"]["etl_receipts.parquet"]
    generation = "comments-v2-" + uuid4().hex
    available = {name: Counter() for name in FIELDS}
    count = 0

    def records():
        nonlocal count
        for row in read_with_receipts(
            [subjects], [receipts], earlier, generation_id=metadata["generation_id"]
        ):
            # Promote the retained values directly. The earlier mapper remains
            # the authority for its normalized subject and its original record.
            for name in FIELDS:
                available[name]["absent" if name not in row else
                                "null" if row[name] is None else "recorded"] += 1
                row.setdefault(name, None)
            if set(row) != current.input_fields:
                raise ValueError("Retained comments fields differ from current policy")
            context = ReceiptContext(
                generation, str(count), "comments-selected-policy-promotion/2",
                [{"source_id": "selected-comments-v1", "source_uri": None,
                  "sha256": source_identity["sha256"],
                  "locator": json.dumps(metadata["snapshot"], sort_keys=True),
                  "body_version": metadata["generation_id"]}],
            )
            yield row, context
            count += 1

    # Select historical nonaccepted occurrences locally, retaining file order.
    # Admission still validates every output receipt under its exact policy.
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="comments-prior-attempts-", dir=destination.parent) as temporary:
        attempts = Path(temporary) / "attempts.parquet"
        with duckdb.connect() as con:
            ExportResources().configure(con, Path(temporary) / "spill")
            con.execute(
                "COPY (SELECT * EXCLUDE(file_row_number) FROM read_parquet(?, "
                "file_row_number=true, hive_partitioning=false) "
                "WHERE outcome<>'accepted' ORDER BY file_row_number) "
                "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(receipts), str(attempts)],
            )
        # Keep fresh output private until full-history inheritance and admission
        # complete. The maintained ordinal lookup avoids a whole-payload SQLite
        # index, while inherit_receipt preserves witnesses and exact payloads.
        from spicy_regs.current_receipt_history import inherit_current_receipts
        from rulespec_artifacts import publish_directory_no_replace
        staged = Path(temporary) / "fresh"
        subject, receipt = write_dataset(
            records(), staged, current, batch_size=128,
            failures=(rebind_receipt(row, generation_id=generation) for row in _rows(attempts)),
        )
        inherited = Path(temporary) / "inherited.parquet"
        inherit_current_receipts(receipt, [receipts], inherited,
                                 inherit_observations=False, require_unique_prior=True)
        inherited.replace(receipt)
        validate_receipt_bundle({"comments": [subject]}, [receipt], [current], generation_id=generation)
        publish_directory_no_replace(staged, destination)
        subject, receipt = destination / subject.name, destination / receipt.name
    if count != EXPECTED_ROWS:
        raise ValueError("Derived comments population differs from the captured population")
    evidence = {
        "generation_id": generation, "dataset": "comments", "rows": count,
        "snapshot": metadata["snapshot"],
        "original_pair": {"path": str(pair), "metadata": metadata,
                          "subjects": source_identity, "receipts": receipt_identity,
                          "policy": earlier.descriptor()},
        "current_policy": current.descriptor(),
        "recorded_fields": {name: dict(values) for name, values in available.items()},
        "native_catalog_mutated": False,
    }
    (destination / "generation.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return subject, receipt, evidence


def assemble(pair: Path, output: Path):
    """Use existing agency writers and index builder on the one derived artifact."""
    from spicy_regs.sources.iceberg import _build_comments_index
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.transforms.partition_comments import (
        assemble_comments, sort_comment_agencies, stage_comment_agencies,
    )
    output = Path(output)
    subject, receipts, evidence = prepare(pair, output / ".catalog-pairs" / "comments")
    resources = ExportResources()
    with TemporaryDirectory(prefix="comments-derived-sort-", dir=output) as temporary:
        temporary = Path(temporary)
        with duckdb.connect() as con:
            resources.configure(con, temporary / "spill")
            con.from_parquet(str(subject)).create_view("comments_derived")
            stage_comment_agencies(con, "SELECT * FROM comments_derived", temporary / "staging",
                                  resources=resources)
        partitions = sort_comment_agencies(temporary / "staging", output, resources=resources)
        with duckdb.connect() as con:
            resources.configure(con, temporary / "spill")
            monolith = assemble_comments(con, partitions, output, policy("comments").subject_schema.names,
                                        resources=resources)
            con.from_parquet(str(monolith)).create_view("comments_export")
            index = _build_comments_index(con, RECORD_TYPES["comments"], output,
                                         source_sql="SELECT * FROM comments_export")
    result = {"comments": monolith, "index": index, "partitions": partitions,
              "receipts": receipts, "generation": subject.parent / "generation.json"}
    members = [monolith, index, receipts, result["generation"],
               *sorted(partitions.glob("agency_code=*/part-0.parquet"))]
    manifest = {"format": "comments-prepared-export/1", "source": evidence["snapshot"],
                "derivation": evidence, "members": {
                    path.relative_to(output).as_posix(): identity(path) for path in members}}
    result["manifest"] = output / "comments-prepared-export.json"
    result["manifest"].write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return result


def export_v1(source_checkout: Path, pair: Path, *, retained_subject=None, retained_receipts=None):
    """Export in the historical interpreter; optionally admit checked retained bodies."""
    source_checkout, pair = Path(source_checkout).resolve(), Path(pair).resolve()
    if retained_receipts is not None and retained_subject is None:
        raise ValueError("Retained receipts require the matching retained subject")
    if (pair / "generation.json").exists():
        if not (pair / "export-complete.json").exists():
            raise ValueError("Earlier export lacks a sealed completion marker")
        return
    if pair.exists():
        raise FileExistsError("An incomplete earlier export requires explicit recovery")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source_checkout,
                                       text=True).strip()
    if revision != V1_EXPORT_REVISION:
        raise ValueError("Earlier export checkout differs from the exact merged revision")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=source_checkout, text=True).strip():
        raise ValueError("Earlier export checkout has unqualified local changes")
    code = """
from dataclasses import asdict
from pathlib import Path
import json
import hashlib, os
import sys
import pyarrow.parquet as pq
from spicy_regs.sources import iceberg, regulatory_catalog
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.regulations_receipts import policy
assert policy('comments').policy_version == 'regulations-native-v1'
rt = RECORD_TYPES['comments']
snapshot = iceberg.catalog_snapshot(rt)
pair = Path(sys.argv[1])
generation = 'catalog-comments-v1-' + str(snapshot.snapshot_id)
held = json.loads(sys.argv[3]) if len(sys.argv) >= 4 else None
held_receipts = json.loads(sys.argv[4]) if len(sys.argv) == 5 else None
if held is not None:
    if set(held) != {'path', 'sha256', 'byteSize', 'snapshot'} or held['snapshot'] != asdict(snapshot):
        raise ValueError('Retained subject does not name the exact selected pair')
    source = Path(held['path'])
    if not source.is_absolute() or source.is_symlink() or not source.is_file():
        raise ValueError('Retained subject must be an absolute regular file')
    with source.open('rb') as original:
        digest = hashlib.file_digest(original, 'sha256').hexdigest()
    if digest != held['sha256'] or source.stat().st_size != held['byteSize']:
        raise ValueError('Retained subject bytes changed')
    if pq.ParquetFile(source).metadata.num_rows != 26418078:
        raise ValueError('Retained subject is not the full population')
if held_receipts is not None:
    if held is None or set(held_receipts) != {'path', 'sha256', 'byteSize', 'snapshot'} or held_receipts['snapshot'] != asdict(snapshot):
        raise ValueError('Retained receipts do not name the exact selected pair')
    receipt_source = Path(held_receipts['path'])
    if not receipt_source.is_absolute() or receipt_source.is_symlink() or not receipt_source.is_file():
        raise ValueError('Retained receipts must be an absolute regular file')
    with receipt_source.open('rb') as original:
        digest = hashlib.file_digest(original, 'sha256').hexdigest()
    if digest != held_receipts['sha256'] or receipt_source.stat().st_size != held_receipts['byteSize']:
        raise ValueError('Retained receipt bytes changed')
    if pq.ParquetFile(receipt_source).metadata.num_rows != 26418079:
        raise ValueError('Retained receipts are not the full population')
con = iceberg._connect()
try:
    if held is None and con.execute('SELECT count(*) FROM ' + regulatory_catalog.qualified(rt)).fetchone()[0] != 26418078:
        raise ValueError('Full native population is required before export')
    if held is None:
        regulatory_catalog.export_pair(con, rt, pair,
            generation_id=generation, snapshot=snapshot)
    else:
        from spicy_regs.duckdb_settings import ExportResources
        # The historical entry point has no subject-reuse option. Keep its
        # receipt selection, rebinding and admission primitives unchanged.
        stage = pair.parent / 'original-v1-recovery-stage'
        stage.mkdir()
        pair.mkdir()
        os.link(source, pair / 'comments.parquet')  # Same filesystem; no copy fallback.
        ExportResources().configure(con, stage / 'spill')
        with regulatory_catalog._transaction(con):
            if not regulatory_catalog.initialized(con, 'comments'):
                raise ValueError('Native catalog has no qualified initialization receipt')
            if iceberg._read_pair_snapshot(con, rt) != snapshot:
                raise RuntimeError('Selected pair changed before receipt recovery')
            # Stream the exact native selection into the maintained rebinder.
            # Avoid COPY's raw intermediate and its second full disk pass.
            # Row batches are targets; the owned process limit still bounds
            # unusually wide rows and the native reader's allocations.
            if held_receipts is not None:
                os.link(receipt_source, pair / 'etl_receipts.parquet')
            else:
                with con.execute(
                    "SELECT * FROM " + regulatory_catalog.receipts_table() + " WHERE dataset='comments'"
                ).to_arrow_reader(batch_size=128) as reader:
                    regulatory_catalog.write_rows(
                        (regulatory_catalog.rebind_receipt(row, generation_id=generation)
                         for batch in reader for row in batch.to_pylist()),
                        pair / 'etl_receipts.parquet', regulatory_catalog.RECEIPT_SCHEMA)
            if pq.ParquetFile(pair / 'etl_receipts.parquet').metadata.num_rows != 26418079:
                raise ValueError('Receipt recovery is not the complete selected population')
            if held_receipts is not None:
                from spicy_regs.etl_receipts import validate_receipt_bundle
                # Complete admission is required; reconstructed processing rows
                # are unused here. Keep only join fields in its temporary index.
                validate_receipt_bundle(
                    {'comments': [pair / 'comments.parquet']},
                    [pair / 'etl_receipts.parquet'], [policy('comments')],
                    generation_id=generation)
            else:
                for _ in regulatory_catalog.read_with_receipts(
                        [pair / 'comments.parquet'], [pair / 'etl_receipts.parquet'],
                        policy('comments'), generation_id=generation):
                    pass
            if iceberg._read_pair_snapshot(con, rt) != snapshot:
                raise RuntimeError('Selected pair changed during receipt recovery')
        (pair / 'generation.json').write_text(json.dumps({
            'generation_id': generation, 'dataset': 'comments', 'snapshot': asdict(snapshot)},
            sort_keys=True) + '\\n')
finally:
    con.close()
if pq.ParquetFile(pair / 'comments.parquet').metadata.num_rows != 26418078:
    raise ValueError('Native export is not the complete captured comments population')
metadata_path = Path(sys.argv[1]) / 'generation.json'
metadata = json.loads(metadata_path.read_text())
metadata.update(policy=policy('comments').descriptor(), exportRevision=sys.argv[2])
metadata_path.write_text(json.dumps(metadata, sort_keys=True) + '\\n')
members = {}
for name in ('comments.parquet', 'etl_receipts.parquet'):
    path = Path(sys.argv[1]) / name
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    members[name] = {'sha256': digest, 'byteSize': path.stat().st_size}
if held is not None and members['comments.parquet'] != {
        'sha256': held['sha256'], 'byteSize': held['byteSize']}:
    raise ValueError('Retained subject changed during receipt recovery')
if held_receipts is not None and members['etl_receipts.parquet'] != {
        'sha256': held_receipts['sha256'], 'byteSize': held_receipts['byteSize']}:
    raise ValueError('Retained receipts changed during admission')
marker = Path(sys.argv[1]) / 'export-complete.json'
temporary = marker.with_suffix('.tmp')
with temporary.open('w') as output:
    output.write(json.dumps({'metadata': metadata, 'members': members}, sort_keys=True) + '\\n')
    output.flush()
    os.fsync(output.fileno())
temporary.replace(marker)
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(source_checkout / "src")
    arguments = [] if retained_subject is None else [json.dumps(retained_subject)]
    if retained_receipts is not None:
        arguments.append(json.dumps(retained_receipts))
    subprocess.run([sys.executable, "-c", code, str(pair), V1_EXPORT_REVISION, *arguments], cwd=source_checkout,
                   env=environment, check=True)


def encoding(path: Path, member_identity):
    """Read actual final footer evidence; writer targets are not encoding proof."""
    with pq.ParquetFile(path) as parquet:
        groups, codecs = [], set()
        for number in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(number)
            groups.append({"rows": group.num_rows, "totalByteSize": group.total_byte_size})
            codecs.update(group.column(column).compression for column in range(group.num_columns))
        return {**member_identity, "rows": parquet.metadata.num_rows,
                "physicalSchema": str(parquet.schema), "arrowSchema": str(parquet.schema_arrow),
                "codecs": sorted(codecs), "rowGroups": groups}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v1-source", type=Path, required=True)
    parser.add_argument("--original-pair", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--base-receipt", type=Path)
    args = parser.parse_args()
    args.v1_source = args.v1_source.resolve()
    args.original_pair = args.original_pair.resolve()
    args.output = args.output.resolve()
    if args.base_receipt is not None:
        args.base_receipt = args.base_receipt.resolve()
    if args.publish and (os.getenv("GITHUB_ACTIONS") != "true"
            or os.getenv("SPICY_REGS_CATALOG_LOCK") != "comments-catalog-write"
            or not os.getenv("GITHUB_WORKFLOW_REF", "").startswith(
                "mikewolfd/spicy-regs/.github/workflows/publish-comments-mirror.yml@")):
        raise RuntimeError("Publish requires the existing locked manual comments workflow")
    def verify_base():
        if args.base_receipt is None:
            if args.publish:
                raise ValueError("Prepared publication requires captured published base versions")
            return
        base = json.loads(args.base_receipt.read_text())
        if base.get("reuse_published_base") is not True or base.get("base_only") is not True:
            raise ValueError("Prepared publication requires an explicit earlier base-only capture")
        subprocess.run([sys.executable, str(Path(__file__).with_name("check_refresh_inputs.py")),
                        "verify", "--receipt", str(args.base_receipt)], check=True)

    verify_base()
    reader = V1CatalogReader(args.v1_source)
    manifest_path = args.output / "comments-prepared-export.json"
    if manifest_path.exists():
        from spicy_regs.pipelines.comments_mirror import _validate_prepared_export
        sealed = verify_original(args.original_pair)
        original = json.loads(manifest_path.read_text())["derivation"]["original_pair"]
        if (original["metadata"] != sealed["metadata"]
                or original["subjects"] != sealed["members"]["comments.parquet"]
                or original["receipts"] != sealed["members"]["etl_receipts.parquet"]):
            raise ValueError("Prepared output names a different original export")
        result = {"comments": args.output / "comments.parquet",
                  "index": args.output / "comments_index.parquet",
                  "partitions": args.output / "comments" / "agency",
                  "receipts": args.output / ".catalog-pairs" / "comments" / "etl_receipts.parquet",
                  "generation": args.output / ".catalog-pairs" / "comments" / "generation.json",
                  "manifest": manifest_path}
        _validate_prepared_export(args.output, result, reader.snapshot())
    else:
        if args.output.exists() and any(args.output.iterdir()):
            raise FileExistsError("Partial prepared output requires explicit recovery; refusing rebuild")
        export_v1(args.v1_source, args.original_pair)
        result = assemble(args.original_pair, args.output)
    manifest = json.loads(manifest_path.read_text())
    proof = {"comments": encoding(result["comments"], manifest["members"]["comments.parquet"]),
             "index": encoding(result["index"], manifest["members"]["comments_index.parquet"]),
             "originalPair": str(args.original_pair), "status": "prepared-unpublished"}
    evidence = args.output / "comments-exact-encoding.json"
    evidence.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")
    if args.publish:
        verify_base()
        from spicy_regs.pipelines.comments_mirror import publish_comments_mirror
        publish_comments_mirror(args.output, prepared_export=result,
                                prepared_reader=reader)
        proof["status"] = "published"
        proof["publication"] = json.loads((args.output / "comments-publication.json").read_text())
        evidence.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
