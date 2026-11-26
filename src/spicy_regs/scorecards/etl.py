"""Scorecard family adoption of the shared, generation-bound ETL receipt API."""

from __future__ import annotations

from collections.abc import Mapping

import hashlib
from dataclasses import replace
import json
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.policy_migrations import earlier_policies
from spicy_regs.etl_receipts import (
    DatasetPolicy,
    RECEIPT_KEY,
    RECEIPT_ROW_GROUP,
    RECEIPT_SCHEMA,
    ReceiptContext,
    _subjects,
    carry_receipt_history,
    combine_receipts,
    failure_receipt,
    open_receipt_writer,
    read_receipt_bundle,
    read_with_receipts,
    select_receipts,
    subject_identity,
    validate_receipt_bundle,
    visit_receipt_bundle,
    write_dataset,
)
from spicy_regs.scorecards.subject_shapes import (
    BOOLEAN_FIELDS,
    DECIMAL_FIELDS,
    DOMAIN_COLUMNS,
    IDENTITIES,
    INTEGER_FIELDS,
    POLICY_VERSION,
    RATING_POLICY_VERSION,
    MEMBER_LINK_POLICY_VERSION,
    SOURCE_COLUMNS,
    map_source_row,
    restore_source_row,
    source_row_index,
    subject_schema,
)

LINK_NAMES = ("scorecard_member_links", "scorecard_item_links")
ANALYSIS_NAMES = (*LINK_NAMES, "scorecard_action_comparisons")
SOURCE_NAMES = tuple(name for name in SOURCE_COLUMNS if name not in ANALYSIS_NAMES)


def policy(name):
    schema = subject_schema(name)
    fields = tuple(c for c in SOURCE_COLUMNS[name] if c not in schema.names)
    if set(DOMAIN_COLUMNS[name]) & (BOOLEAN_FIELDS | DECIMAL_FIELDS | INTEGER_FIELDS):
        fields += ("conversion_inputs",)
    return DatasetPolicy(
        name,
        schema,
        IDENTITIES[name] if schema.names else (),
        fields + ("raw_source",),
        policy_version={
            "scorecard_member_ratings": RATING_POLICY_VERSION,
            "scorecard_member_links": MEMBER_LINK_POLICY_VERSION,
        }.get(name, POLICY_VERSION),
        receipt_only=not schema.names,
    )


POLICIES = {name: policy(name) for name in SOURCE_COLUMNS}
# Source replay and shared admission use one exact historical declaration source.
EARLIER_POLICIES = {name: earlier_policies(policy) for name, policy in POLICIES.items()}
LEGACY_RATING_POLICY_V2, LEGACY_RATING_POLICY = (p for p in EARLIER_POLICIES["scorecard_member_ratings"]
                                                if p.policy_version in {"scorecards-etl-ratings-v2", "scorecards-etl-v1"})


def admitted_read_policies(names, *, descriptors=None, columns=None):
    """Select only known exact policies from a verified artifact or pinned table declarations."""
    from spicy_regs.contract_types import described_schema

    if (descriptors is None) == (columns is None):
        raise ValueError("Scorecard reads require exactly one admitted policy declaration")
    from itertools import product
    choices = {name: (POLICIES[name], *EARLIER_POLICIES[name]) for name in names}
    if descriptors is not None:
        if not isinstance(descriptors, list) or len(descriptors) != len(names):
            raise ValueError("Scorecard policy declaration count differs")
        declared = {value.get("dataset"): value for value in descriptors if isinstance(value, dict)}
        if len(declared) != len(names):
            raise ValueError("Scorecard declarations differ from supported exact policies: repeated dataset")
        selected = {}
        for name, candidates in choices.items():
            matches = [p for p in candidates if p.descriptor() == declared.get(name)]
            if len(matches) != 1:
                raise ValueError("Scorecard policy is not a supported exact declaration")
            selected[name] = matches[0]
        return selected
    assert columns is not None
    # Schema-only historical inputs must identify one exact known family layout.
    # Avoid a Cartesian search when each table's columns already decide its policy.
    matches = {}
    for name, candidates in choices.items():
        matches[name] = [p for p in candidates if (p.receipt_only and name not in columns) or
                         (not p.receipt_only and name in columns and
                          [tuple(c) for c in columns[name]] == described_schema(p.subject_schema))]
        if not matches[name]:
            raise ValueError("Scorecard columns are not a supported exact layout")
    for variant in product(*(matches[name] for name in names)):
        selected = dict(zip(names, variant))
        if set(columns) == {name for name, p in selected.items() if not p.receipt_only}:
            return selected
    raise ValueError("Scorecard input differs from the supported exact historical or current policies")


def _witnesses(row):
    captures = json.loads(row["capture_ids_json"]) if row.get("capture_ids_json") else [row.get("capture_id")]
    witnesses = []
    for capture in captures:
        if not capture:
            raise ValueError("Scorecard row lacks its retained capture identity")
        # Capture IDs are immutable observation identities, not byte hashes.
        # This preserves the metadata-only policy without inventing content evidence.
        witnesses.append(
            dict(
                source_id=capture,
                source_uri=row.get("source_url") if capture == row.get("capture_id") else None,
                sha256=None,
                locator=row.get("source_path") if capture == row.get("capture_id") else None,
                body_version="capture:" + capture,
            )
        )
    if row.get("input_pins_json"):
        for name, pin in json.loads(row["input_pins_json"]).items():
            witnesses.append(
                dict(
                    source_id=name,
                    source_uri=None,
                    sha256=pin.get("sha256"),
                    locator=None,
                    body_version=pin["artifactDigest"],
                )
            )
    return witnesses


def generation_options(directory: Path, names) -> dict:
    """Explicit options for generation admission; receipts are not subject outputs."""
    state = json.loads((directory / "scorecard-etl-build.json").read_text())
    if set(state["datasets"]) != set(names) or len(state["datasets"]) != len(names):
        raise ValueError("Scorecard receipt policy set differs from the selected build")
    from spicy_regs.contract_types import described_schema

    return dict(
        schemas={
            name: described_schema(POLICIES[name].subject_schema) for name in names if not POLICIES[name].receipt_only
        },
        receipt_path=directory / RECEIPT_KEY,
        receipt_policies=[POLICIES[n] for n in names],
        receipt_generation_id=state["generation_id"],
    )


def write_family(
    directory: Path,
    tables: Mapping,
    *,
    generation_id: str | None = None,
    attempt_failures=(),
    prior_receipts: Path | None = None,
) -> tuple[Path, ...]:
    """Write subjects and every source/resolution attempt, then verify persisted joins."""
    names = tuple(tables)
    generation_id = generation_id or "scorecard-build:" + uuid4().hex
    stage = directory / (".scorecard-etl-" + uuid4().hex)
    stage.mkdir(parents=True)
    receipts, outputs, errors = [], [], []

    def mapped_records(name, rows, failures):
        for index, raw in enumerate(rows):
            context = ReceiptContext(
                generation_id,
                f"{name}:{index}",
                raw.get("rule_version") or raw.get("parser_version") or POLICIES[name].policy_version,
                _witnesses(raw),
            )
            try:
                mapped = map_source_row(name, raw)
            except (ValueError, TypeError, OverflowError) as error:
                failures.append(
                    failure_receipt(
                        POLICIES[name],
                        replace(
                            context,
                            diagnostics={
                                "error_type": type(error).__name__,
                                "reason_code": "native_conversion_refused",
                            },
                        ),
                        outcome="error",
                        raw_fields={"raw_source": raw},
                    )
                )
                errors.append(f"{name}: {type(error).__name__}")
                continue
            # Unresolved is a valid recorded domain decision, not a failed ETL
            # conversion. Publish the complete association with no invented edge.
            yield mapped, context

    for name, rows in tables.items():
        failures: list[dict] = list(attempt_failures) if name == "scorecard_snapshots" else []
        subject, receipt = write_dataset(
            mapped_records(name, rows, failures), stage / name, POLICIES[name], failures=failures,
            validate=False,  # The complete private family is validated before final outputs are replaced.
        )
        receipts.append(receipt)
        if subject:
            outputs.append(subject)
    shared = combine_receipts(receipts, stage / RECEIPT_KEY)
    if prior_receipts is not None and not errors:
        # The caller admitted the complete prior family. Keep unchanged receipts
        # exactly; changed accepted rows name only their direct predecessor.
        shared = carry_receipt_history(shared, [prior_receipts], shared)
    validate_receipt_bundle(
        {n: [] if POLICIES[n].receipt_only else [stage / n / (n + ".parquet")] for n in names},
        [shared],
        [POLICIES[n] for n in names],
        generation_id=generation_id,
    )
    if errors:
        raise ValueError(f"Scorecard conversion refused; retained attempt receipts in {stage}: {errors}")
    paths = []
    for subject in outputs:
        target = directory / subject.name
        subject.replace(target)
        paths.append(target)
    shared.replace(directory / RECEIPT_KEY)
    (directory / "scorecard-etl-build.json").write_text(
        json.dumps({"generation_id": generation_id, "datasets": list(names)}, indent=2) + "\n"
    )
    rmtree(stage)
    return tuple(paths)


def _family_read_arguments(
    directory: Path,
    names,
    *,
    receipt_path: Path | None = None,
    generation_id: str | None = None,
    policies: Mapping[str, DatasetPolicy] | None = None,
) -> dict:
    receipt_path = receipt_path or directory / RECEIPT_KEY
    if generation_id is None:
        generation_id = generation_options(directory, names)["receipt_generation_id"]
    if policies is not None and (set(policies) != set(names) or any(p.dataset != name for name, p in policies.items())):
        raise ValueError("Scorecard read policy keys differ from their selected datasets")
    selected = (
        {n: POLICIES[n] for n in names}
        if policies is None
        else admitted_read_policies(names, descriptors=[p.descriptor() for p in policies.values()])
    )
    subjects = {n: [] if selected[n].receipt_only else [directory / (n + ".parquet")] for n in names}
    return dict(
        subjects=subjects,
        receipt_paths=[receipt_path],
        policies=[selected[n] for n in names],
        generation_id=generation_id,
        processing_outcomes={
            name: frozenset({"observed", "refused"} if name in LINK_NAMES else {"observed"}) for name in names
        },
    )


def read_family(
    directory: Path,
    names,
    *,
    receipt_path: Path | None = None,
    generation_id: str | None = None,
    policies: Mapping[str, DatasetPolicy] | None = None,
) -> dict:
    """Reconstruct provider rows only after validating the entire selected native family."""
    joined = read_receipt_bundle(**_family_read_arguments(
        directory, names, receipt_path=receipt_path, generation_id=generation_id, policies=policies,
    ))
    for name, rows in joined.items():
        for index, row in enumerate(rows):
            rows[index] = restore_source_row(name, row)
    return joined


def verify_family_readback(
    directory: Path,
    expected: Mapping,
    *,
    receipt_path: Path | None = None,
    generation_id: str | None = None,
    policies: Mapping[str, DatasetPolicy] | None = None,
) -> dict[str, int]:
    """Prove exact persisted source rows without constructing a second raw family.

    Only identity indexes referencing existing rows are retained. Each dataset's
    index is built when first visited and emptied as its persisted rows arrive;
    reordered native rows remain valid. Counts escape only after every receipt,
    join and literal source value has passed.
    """
    remaining = {}

    def visit(name, native):
        if name not in remaining:
            remaining[name] = source_row_index(name, expected[name])
        row = restore_source_row(name, native)
        key = tuple(row[field] for field in IDENTITIES[name])
        before = remaining[name].pop(key, None)
        if before is None or json.dumps(before, sort_keys=True) != json.dumps(row, sort_keys=True):
            raise ValueError("Persisted scorecard source row differs: " + name)
        if not remaining[name]:
            remaining[name].clear()  # Free the exhausted index's bucket storage; keep its visited marker.

    counts = visit_receipt_bundle(
        **_family_read_arguments(
            directory, tuple(expected), receipt_path=receipt_path, generation_id=generation_id, policies=policies,
        ),
        visit=visit,
    )
    if counts != {name: len(rows) for name, rows in expected.items()} or any(remaining.values()):
        raise ValueError("Persisted scorecard source family is incomplete")
    return counts


def read_indexed_family(directory: Path, names, family: Mapping, *, receipt_path: Path | None = None) -> dict:
    """Read caller-pinned downloads under the index's exact supported schema and receipt generation."""
    return read_family(
        directory,
        names,
        receipt_path=receipt_path,
        generation_id=family["etlReceipts"]["generationId"],
        policies=admitted_read_policies(
            names,
            columns={key.removesuffix(".parquet"): pin["columns"] for key, pin in family["tables"].items()},
        ),
    )


def read_source_inputs(paths, receipt_path: Path, *, generation_id: str) -> dict:
    """Receipt-checked resolver inputs; selected rows share one source generation."""
    result = {}
    with TemporaryDirectory(prefix="scorecard-analysis-inputs-") as temporary:
        for name, subject_paths in paths.items():
            selected = select_receipts(receipt_path, Path(temporary) / (name + ".parquet"), dataset=name)
            result[name] = [
                restore_source_row(name, row)
                for row in read_with_receipts(subject_paths, [selected], POLICIES[name], generation_id=generation_id)
            ]
    return result


def verified_receipt_download(index, target: Path, *, public_url: str | None, dataset="scorecards") -> Path:
    """Fetch the same selected family's receipt member and verify local bytes as well."""
    from spicy_regs.sources import publication

    members = publication.receipt_members(index, dataset=dataset)
    if len(members) != 1:
        raise ValueError("Scorecard input requires one selected receipt member")
    member = members[0]
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.is_file():
        if not public_url or not publication.fetch_member(public_url, member, target, RECEIPT_KEY):
            raise ValueError("Pinned scorecard receipt is unavailable")
    with target.open("rb") as stream:
        digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    if target.stat().st_size != member.byte_size or digest != member.sha256:
        raise ValueError("Pinned scorecard receipt changed")
    return target


def read_source_generation(directory: Path) -> dict:
    """Qualification read of an admitted source generation, with explicit legacy migration."""
    from spicy_regs.generations import verify_generation
    from spicy_regs.transforms.build_scorecards import installed_provider

    artifact = verify_generation(directory)
    spec = artifact.root["spec"]
    if spec["family"] != "scorecards":
        raise ValueError("Expected a scorecard source generation")
    if "etlReceipts" in spec:
        selected = admitted_read_policies(SOURCE_NAMES, descriptors=spec["etlReceipts"]["policies"])
        rows = read_family(
            directory, SOURCE_NAMES, generation_id=spec["etlReceipts"]["generationId"], policies=selected
        )
    else:
        if set(spec["tables"]) != {name + ".parquet" for name in SOURCE_NAMES}:
            raise ValueError("Incomplete legacy scorecard source generation")
        rows = {name: pq.ParquetFile(directory / (name + ".parquet")).read().to_pylist() for name in SOURCE_NAMES}
    installed_provider().validate(rows)
    return rows


def source_failure_receipts(failures, *, generation_id: str, registry: Path):
    """Safe scope-level refusals pinned to the actual input selection, without source bodies."""
    digest = "sha256:" + hashlib.sha256(registry.read_bytes()).hexdigest()
    return [
        failure_receipt(
            POLICIES["scorecard_snapshots"],
            ReceiptContext(
                generation_id,
                f"source-attempt:{index}",
                POLICY_VERSION,
                [dict(source_id="scorecard-registry", source_uri=None, sha256=digest, locator=None, body_version=None)],
                diagnostics={"reason_code": "source_acquisition_refused"},
            ),
            outcome="refused",
            raw_fields={"raw_source": raw},
        )
        for index, raw in enumerate(failures)
    ]


def selected_table_entry(snapshot: Mapping, key: str) -> Mapping:
    """Require a selected published table before reading its generation receipts."""
    from spicy_regs.sources import publication

    owner = publication.table_owner(snapshot, key)
    if owner is None:
        raise publication.PublicationError(f"Missing selected table owner: {key}")
    return owner[1]


SUBJECT_ROW_GROUP = 50_000


class _SubjectWriter:
    """Write Arrow subject batches in bounded row groups under the exact declared schema."""

    def __init__(self, path: Path, schema: pa.Schema, *, row_group_size: int = SUBJECT_ROW_GROUP) -> None:
        self._writer = pq.ParquetWriter(path, schema, compression="zstd")
        self._schema = schema
        self._row_group_size = row_group_size
        self._pending: list[pa.Table] = []
        self._held = 0
        self.rows = 0

    def write_table(self, table: pa.Table) -> None:
        if not table.schema.equals(self._schema):
            raise ValueError("Subject batch schema differs from declared policy schema")
        if not table.num_rows:
            return
        self._pending.append(table)
        self._held += table.num_rows
        self.rows += table.num_rows
        while self._held >= self._row_group_size:
            held = pa.concat_tables(self._pending)
            self._writer.write_table(held.slice(0, self._row_group_size), row_group_size=self._row_group_size)
            rest = held.slice(self._row_group_size)
            self._pending = [rest] if rest.num_rows else []
            self._held = rest.num_rows

    def close(self, *, flush: bool = True) -> None:
        try:
            if flush and self._pending:
                self._writer.write_table(pa.concat_tables(self._pending), row_group_size=self._row_group_size)
            self._pending.clear()
            self._held = 0
        finally:
            self._writer.close()

    def __enter__(self) -> _SubjectWriter:
        return self

    def __exit__(self, exc_type, *_) -> None:
        self.close(flush=exc_type is None)


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _prior_identities(
    con,
    policy: DatasetPolicy,
    view: str,
    path: Path,
    *,
    scratch: Path,
    kept_expr: str,
) -> None:
    """Compute (record_id, subject_version, identity_json, _kept) for prior rows in one SQL scan."""
    record_id, version, identity = etl_bulk.identity_sql(policy)
    reference = etl_bulk._subject_reference_sql(policy)
    con.execute(
        f"CREATE TABLE {view}_ids AS SELECT _o, {record_id} AS _rid, {version} AS _ver, "
        f"{identity} AS _idjson, ({reference}) IS NOT FALSE AS _ref, ({kept_expr}) AS _kept FROM {view}"
    )
    with TemporaryDirectory(prefix="scorecard-identities-", dir=scratch) as temporary:
        root = Path(temporary)
        wanted = iter(
            etl_bulk._ordinal_file(
                con, f"SELECT _o FROM {view}_ids WHERE _ref ORDER BY _o", [], root / "ordinals.parquet"
            )
        )
        pending = next(wanted, None)
        if pending is not None:
            schema = pa.schema([
                ("_o", pa.int64()),
                ("_rid", pa.string()),
                ("_ver", pa.string()),
                ("_idjson", pa.string()),
            ])
            with pq.ParquetWriter(root / "corrections.parquet", schema, compression="zstd") as output:
                with pq.ParquetFile(path) as source:
                    start = 0
                    for group in range(source.metadata.num_row_groups):
                        end = start + source.metadata.row_group(group).num_rows
                        if pending is not None and pending < end:
                            for batch in source.iter_batches(batch_size=SUBJECT_ROW_GROUP, row_groups=[group]):
                                stop = start + batch.num_rows
                                decided = []
                                while pending is not None and pending < stop:
                                    raw = batch.slice(pending - start, 1).to_pylist()[0]
                                    subject = _subjects(policy, [raw])[0]
                                    rid, ver, idjson = subject_identity(policy, subject)
                                    decided.append({"_o": pending, "_rid": rid, "_ver": ver, "_idjson": idjson})
                                    pending = next(wanted, None)
                                if decided:
                                    output.write_table(pa.Table.from_pylist(decided, schema=schema))
                                start = stop
                        start = end
            if pending is not None:
                raise ValueError(f"{policy.dataset}: routed subject ordinal is outside its source")
            con.execute(
                f"UPDATE {view}_ids SET _rid = c._rid, _ver = c._ver, _idjson = c._idjson, _ref = false "
                f"FROM read_parquet({etl_bulk._literal(str(root / 'corrections.parquet'))}) c "
                f"WHERE {view}_ids._o = c._o"
            )


def _gather_receipts_by_ordinal(
    prior_receipts: Path,
    ordinals_path: Path,
    writer: pq.ParquetWriter,
    *,
    batch_size: int = RECEIPT_ROW_GROUP,
) -> int:
    """Gather rows from ``prior_receipts`` at ascending ``ordinals_path`` into ``writer`` without Python row dicts."""
    from spicy_regs.receipt_history import _take_prior_rows

    written = 0
    with pq.ParquetFile(prior_receipts) as prior_pf, pq.ParquetFile(ordinals_path) as ord_pf:
        group_ends = []
        total = 0
        for group in range(prior_pf.metadata.num_row_groups):
            total += prior_pf.metadata.row_group(group).num_rows
            group_ends.append(total)
        file_ends = [prior_pf.metadata.num_rows]
        for ord_batch in ord_pf.iter_batches(batch_size=batch_size):
            ordinals = ord_batch.column(0).to_pylist()
            if not ordinals:
                continue
            table, positions = _take_prior_rows([prior_pf], file_ends, [group_ends], ordinals, RECEIPT_SCHEMA)
            if positions != list(range(len(ordinals))):
                raise ValueError("Scorecard receipt carry gathered a different row count than selected")
            table.validate(full=True)
            writer.write_table(table)
            written += table.num_rows
    return written


def _copy_filtered_subject_slice(
    prior_path: Path,
    fresh_path: Path,
    destination: Path,
    *,
    schema: pa.Schema,
    scope_column: str,
    replaced: set[str],
    expected_kept: int,
    expected_fresh: int,
) -> None:
    """Stream kept prior rows (excluding ``replaced``) unioned with ``fresh_path`` in bounded Arrow batches."""
    replaced_array = pa.array(sorted(replaced), type=pa.string()) if replaced else None
    kept_written = 0
    fresh_written = 0
    with _SubjectWriter(destination, schema) as writer:
        with pq.ParquetFile(prior_path) as prior_pf:
            if not prior_pf.schema_arrow.equals(schema):
                raise ValueError(f"Prior scorecard subject schema differs: {prior_path.name}")
            for batch in prior_pf.iter_batches(batch_size=SUBJECT_ROW_GROUP):
                batch.validate(full=True)
                scope_col = batch.column(scope_column)
                if scope_col.null_count:
                    raise ValueError(f"Prior scorecard table has null {scope_column}: {prior_path.name}")
                if replaced_array is not None:
                    mask = pc.call_function(
                        "invert",
                        [
                            pc.call_function(
                                "is_in",
                                [scope_col],
                                options=pc.SetLookupOptions(value_set=replaced_array),
                            )
                        ],
                    )
                    filtered = batch.filter(mask)
                else:
                    filtered = batch
                if filtered.num_rows:
                    writer.write_table(pa.Table.from_batches([filtered], schema=schema))
                    kept_written += filtered.num_rows
        with pq.ParquetFile(fresh_path) as fresh_pf:
            if not fresh_pf.schema_arrow.equals(schema):
                raise ValueError(f"Fresh scorecard subject schema differs: {fresh_path.name}")
            for batch in fresh_pf.iter_batches(batch_size=SUBJECT_ROW_GROUP):
                batch.validate(full=True)
                if batch.num_rows:
                    writer.write_table(pa.Table.from_batches([batch], schema=schema))
                    fresh_written += batch.num_rows
    if kept_written != expected_kept or fresh_written != expected_fresh:
        raise ValueError(
            f"Scorecard slice merge wrote {kept_written}+{fresh_written} rows, expected {expected_kept}+{expected_fresh}"
        )
    with pq.ParquetFile(destination) as written_pf:
        if not written_pf.schema_arrow.equals(schema) or written_pf.metadata.num_rows != expected_kept + expected_fresh:
            raise ValueError(f"Scorecard merged table failed output verification: {destination.name}")


def _check_prior_schemas(prior_dir: Path, prior_receipts: Path) -> None:
    """Refuse loudly if any prior subject or receipt file is missing or differs from current policy schemas."""
    for name in SOURCE_NAMES:
        prior_path = prior_dir / f"{name}.parquet"
        if not prior_path.is_file():
            raise ValueError(f"Missing prior scorecard subject table: {name}")
        with pq.ParquetFile(prior_path) as pf:
            if not pf.schema_arrow.equals(POLICIES[name].subject_schema):
                raise ValueError(f"Prior scorecard table {name} schema differs from current policy")
    if not prior_receipts.is_file():
        raise ValueError("Missing prior scorecard receipt table")
    with pq.ParquetFile(prior_receipts) as pf:
        if not pf.schema_arrow.equals(RECEIPT_SCHEMA):
            raise ValueError("Prior scorecard receipt schema differs from shared receipt schema")


def _load_prior_receipt_keys(con, prior_receipts: Path) -> None:
    """Validate prior receipt invariants and materialize narrow join keys in DuckDB."""
    lit = etl_bulk._literal
    con.execute(
        f"CREATE VIEW prior_r AS SELECT *, file_row_number AS _o "
        f"FROM read_parquet({lit(str(prior_receipts))}, file_row_number=true, hive_partitioning=false)"
    )
    allowed_datasets = ", ".join(lit(name) for name in SOURCE_NAMES)
    if con.execute(f"SELECT count(*) FROM prior_r WHERE dataset NOT IN ({allowed_datasets})").fetchone()[0]:
        raise ValueError("Prior scorecard receipts contain foreign datasets")
    if con.execute("SELECT count(*) FROM prior_r WHERE receipt_id IS NULL OR receipt_id = ''").fetchone()[0]:
        raise ValueError("Prior scorecard receipts contain null or empty receipt_id")
    if con.execute("SELECT count(*) - count(DISTINCT receipt_id) FROM prior_r").fetchone()[0]:
        raise ValueError("Prior scorecard receipts contain duplicate receipt_id")
    if con.execute(
        "SELECT EXISTS(SELECT 1 FROM prior_r WHERE outcome = 'accepted' "
        "GROUP BY dataset, record_id HAVING count(*) > 1)"
    ).fetchone()[0]:
        raise ValueError("Prior scorecard receipts contain duplicate accepted identities")
    if con.execute(
        "SELECT count(*) FROM prior_r WHERE outcome = 'accepted' AND "
        "(record_id IS NULL OR record_id = '' OR subject_version IS NULL OR identity_json IS NULL)"
    ).fetchone()[0]:
        raise ValueError("Prior scorecard accepted receipts lack subject identity fields")
    con.execute(
        "CREATE TABLE prior_r_keys AS SELECT _o, dataset, outcome, record_id, "
        "unhex(sha256(subject_version)) AS ver_hash, unhex(sha256(identity_json)) AS id_hash "
        "FROM prior_r"
    )


def _check_kept_cross_table_integrity(con, fresh: Mapping[str, list[dict]]) -> None:
    """Verify foreign keys, completeness, and snapshot disjointness across kept prior slices and fresh rows."""
    con.execute("CREATE TABLE fresh_publishers (publisher_id VARCHAR)")
    if fresh["scorecard_publishers"]:
        con.executemany(
            "INSERT INTO fresh_publishers VALUES (?)",
            [(row["publisher_id"],) for row in fresh["scorecard_publishers"]],
        )
    con.execute("CREATE TABLE fresh_snapshots (snapshot_id VARCHAR)")
    if fresh["scorecard_snapshots"]:
        con.executemany(
            "INSERT INTO fresh_snapshots VALUES (?)",
            [(row["snapshot_id"],) for row in fresh["scorecard_snapshots"]],
        )
    con.execute(
        "CREATE TABLE kept_publishers AS SELECT publisher_id FROM prior_scorecard_publishers "
        "WHERE publisher_id NOT IN (SELECT scope_val FROM replaced_scorecard_publishers) "
        "UNION SELECT publisher_id FROM fresh_publishers"
    )
    con.execute(
        "CREATE TABLE kept_scorecards AS SELECT scorecard_id, publisher_id, snapshot_id FROM prior_scorecards "
        "WHERE scorecard_id NOT IN (SELECT scope_val FROM replaced_scorecards)"
    )
    con.execute(
        "CREATE TABLE kept_snapshots AS SELECT snapshot_id, scorecard_id, completeness_status "
        "FROM prior_scorecard_snapshots "
        "WHERE scorecard_id NOT IN (SELECT scope_val FROM replaced_scorecards)"
    )
    if con.execute(
        "SELECT count(*) FROM kept_scorecards c LEFT JOIN kept_publishers p USING (publisher_id) "
        "WHERE p.publisher_id IS NULL"
    ).fetchone()[0]:
        raise ValueError("Kept prior scorecards reference a missing publisher")
    if con.execute("SELECT count(*) FROM kept_snapshots WHERE completeness_status <> 'complete'").fetchone()[0]:
        raise ValueError("Kept prior scorecard snapshot is incomplete")
    if (
        con.execute("SELECT count(*) FROM kept_scorecards").fetchone()[0]
        != con.execute("SELECT count(*) FROM kept_snapshots").fetchone()[0]
        or con.execute(
            "SELECT count(*) FROM kept_scorecards c LEFT JOIN kept_snapshots s "
            "ON c.scorecard_id = s.scorecard_id AND c.snapshot_id = s.snapshot_id "
            "WHERE s.snapshot_id IS NULL"
        ).fetchone()[0]
    ):
        raise ValueError("Kept prior scorecards and snapshots differ")
    if con.execute("SELECT count(*) FROM kept_snapshots k JOIN fresh_snapshots f USING (snapshot_id)").fetchone()[0]:
        raise ValueError("Fresh scorecard snapshot_id collides with kept prior snapshot")
    for child in (
        "scorecard_methodologies",
        "scorecard_metrics",
        "scorecard_items",
        "scorecard_metric_items",
        "scorecard_metric_components",
        "scorecard_members",
        "scorecard_member_ratings",
        "scorecard_member_item_results",
    ):
        if con.execute(
            f"SELECT count(*) FROM prior_{child} c LEFT JOIN kept_snapshots s "
            f"ON c.scorecard_id = s.scorecard_id AND c.snapshot_id = s.snapshot_id "
            f"WHERE c.scorecard_id NOT IN (SELECT scope_val FROM replaced_{child}) AND s.snapshot_id IS NULL"
        ).fetchone()[0]:
            raise ValueError(f"Kept prior {child} rows reference a missing or mismatched snapshot")


def _prepare_prior_slices(
    stage: Path,
    *,
    prior_dir: Path,
    prior_receipts: Path,
    fresh: Mapping[str, list[dict]],
    replaced_publishers: set[str],
    replaced_scorecards: set[str],
    contracts: Mapping | None,
    collect_retired: bool,
) -> tuple[dict[str, int], dict[str, list[list[str]]], int, Path, Path]:
    """Validate prior slices/receipts in DuckDB and write replaced + kept receipt ordinal selections."""
    lit = etl_bulk._literal
    replaced_receipts_path = stage / "replaced_receipts.parquet"
    kept_ordinals_path = stage / "kept_receipt_ordinals.parquet"
    kept_counts: dict[str, int] = {}
    retired_by_table: dict[str, list[list[str]]] = {}

    with etl_bulk.bulk_connection(stage) as (con, scratch):
        con.execute("SET preserve_insertion_order = false")
        _load_prior_receipt_keys(con, prior_receipts)
        con.execute("CREATE TABLE prior_replaced_keys (dataset VARCHAR, record_id VARCHAR)")
        total_prior_rows = 0

        for name in SOURCE_NAMES:
            policy = POLICIES[name]
            prior_path = prior_dir / f"{name}.parquet"
            scope_column = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
            replaced = replaced_publishers if name == "scorecard_publishers" else replaced_scorecards
            view = f"prior_{name}"
            con.execute(
                f"CREATE VIEW {view} AS SELECT *, file_row_number AS _o "
                f"FROM read_parquet({lit(str(prior_path))}, file_row_number=true, hive_partitioning=false)"
            )
            required_nonempty = [
                *dict.fromkeys((*IDENTITIES[name], scope_column, "capture_id", "source_url", "source_path"))
            ]
            empty_pred = " OR ".join(f"{_q(col)} IS NULL OR {_q(col)} = ''" for col in required_nonempty)
            if con.execute(f"SELECT count(*) FROM {view} WHERE {empty_pred}").fetchone()[0]:
                raise ValueError(f"Prior scorecard table {name} contains null or empty required identity")

            con.execute(f"CREATE TABLE replaced_{name} (scope_val VARCHAR)")
            if replaced:
                con.executemany(f"INSERT INTO replaced_{name} VALUES (?)", [(val,) for val in sorted(replaced)])

            kept_expr = f"{_q(scope_column)} NOT IN (SELECT scope_val FROM replaced_{name})"
            _prior_identities(con, policy, view, prior_path, scratch=scratch, kept_expr=kept_expr)
            if con.execute(f"SELECT count(*) - count(DISTINCT _rid) FROM {view}_ids").fetchone()[0]:
                raise ValueError(f"Prior scorecard table {name} contains duplicate identity")

            unmatched = con.execute(
                f"SELECT count(*) FROM {view}_ids s LEFT JOIN prior_r_keys r "
                f"ON r.dataset = {lit(name)} AND r.outcome = 'accepted' "
                f"AND s._rid = r.record_id AND unhex(sha256(s._ver)) = r.ver_hash "
                f"AND unhex(sha256(s._idjson)) = r.id_hash "
                f"WHERE r._o IS NULL"
            ).fetchone()[0]
            if unmatched:
                raise ValueError(f"Prior scorecard table {name} has rows without matching prior receipt")

            row_count, kept_count = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE _kept) FROM {view}_ids"
            ).fetchone()
            total_prior_rows += row_count
            kept_counts[name] = kept_count
            con.execute(f"INSERT INTO prior_replaced_keys SELECT {lit(name)}, _rid FROM {view}_ids WHERE NOT _kept")

            if collect_retired and replaced:
                id_cols = IDENTITIES[name]
                select_cols = ", ".join(_q(col) for col in id_cols)
                prior_replaced_keys = con.execute(
                    f"SELECT {select_cols} FROM {view} "
                    f"WHERE {_q(scope_column)} IN (SELECT scope_val FROM replaced_{name}) ORDER BY _o"
                ).fetchall()
                if prior_replaced_keys:
                    if contracts is not None and name in contracts:
                        current_keys = {contracts[name].key(row) for row in fresh[name]}
                    else:
                        current_keys = {tuple(row[col] for col in id_cols) for row in fresh[name]}
                    removed = [list(key) for key in prior_replaced_keys if key not in current_keys]
                    if removed:
                        retired_by_table[name] = removed
            con.execute(f"DROP TABLE {view}_ids")

        if con.execute("SELECT count(*) FROM prior_r_keys WHERE outcome = 'accepted'").fetchone()[0] != total_prior_rows:
            raise ValueError("Prior scorecard receipts contain orphan accepted records")
        _check_kept_cross_table_integrity(con, fresh)

        replaced_ordinals_path = scratch / "replaced_receipt_ordinals.parquet"
        con.execute(
            f"COPY (SELECT r._o FROM prior_r_keys r LEFT JOIN prior_replaced_keys k "
            f"ON r.dataset = k.dataset AND r.record_id = k.record_id "
            f"WHERE r.outcome <> 'accepted' OR k.record_id IS NOT NULL ORDER BY r._o) "
            f"TO {lit(str(replaced_ordinals_path))} (FORMAT PARQUET)"
        )
        con.execute(
            f"COPY (SELECT r._o FROM prior_r_keys r LEFT JOIN prior_replaced_keys k "
            f"ON r.dataset = k.dataset AND r.record_id = k.record_id "
            f"WHERE r.outcome = 'accepted' AND k.record_id IS NULL ORDER BY r._o) "
            f"TO {lit(str(kept_ordinals_path))} (FORMAT PARQUET)"
        )
        with open_receipt_writer(replaced_receipts_path) as replaced_writer:
            replaced_receipt_count = _gather_receipts_by_ordinal(
                prior_receipts, replaced_ordinals_path, replaced_writer
            )
    return kept_counts, retired_by_table, replaced_receipt_count, replaced_receipts_path, kept_ordinals_path


def _merge_receipts(
    prior_receipts: Path,
    kept_ordinals_path: Path,
    fresh_receipts_path: Path,
    destination: Path,
) -> None:
    """Union kept prior accepted receipts and carried fresh receipts in Arrow without Python round-trips."""
    with open_receipt_writer(destination) as receipt_writer:
        kept_receipt_count = _gather_receipts_by_ordinal(prior_receipts, kept_ordinals_path, receipt_writer)
        with pq.ParquetFile(fresh_receipts_path) as fresh_r_pf:
            if not fresh_r_pf.schema_arrow.equals(RECEIPT_SCHEMA):
                raise ValueError("Fresh scorecard receipt schema differs from shared schema")
            fresh_receipt_count = fresh_r_pf.metadata.num_rows
            for batch in fresh_r_pf.iter_batches(batch_size=RECEIPT_ROW_GROUP):
                batch.validate(full=True)
                receipt_writer.write_table(pa.Table.from_batches([batch], schema=RECEIPT_SCHEMA))
    with pq.ParquetFile(destination) as merged_r_pf:
        if merged_r_pf.metadata.num_rows != kept_receipt_count + fresh_receipt_count:
            raise ValueError("Scorecard merged receipt count differs from carried + fresh counts")


def merge_family_slices(
    directory: Path,
    *,
    prior_dir: Path,
    prior_receipts: Path,
    fresh: Mapping[str, list[dict]],
    replaced_publishers: set[str],
    replaced_scorecards: set[str],
    contracts: Mapping | None = None,
    evidence=None,
    generation_id: str | None = None,
    attempt_failures=(),
) -> tuple[Path, ...]:
    """Carry unchanged prior scorecard slices in Arrow/DuckDB and merge with freshly built scopes."""
    if set(fresh) != set(SOURCE_NAMES):
        raise ValueError("Scorecard fresh bundle must contain the complete frozen table set")
    generation_id = generation_id or "scorecard-build:" + uuid4().hex
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / (".scorecard-etl-" + uuid4().hex)
    stage.mkdir(parents=True)
    fresh_stage = stage / "fresh"
    fresh_stage.mkdir()
    merged_stage = stage / "merged"
    merged_stage.mkdir()

    _check_prior_schemas(prior_dir, prior_receipts)
    kept_counts, retired_by_table, replaced_receipt_count, replaced_receipts_path, kept_ordinals_path = (
        _prepare_prior_slices(
            stage,
            prior_dir=prior_dir,
            prior_receipts=prior_receipts,
            fresh=fresh,
            replaced_publishers=replaced_publishers,
            replaced_scorecards=replaced_scorecards,
            contracts=contracts,
            collect_retired=evidence is not None,
        )
    )

    # Build, carry, and readback-verify only the freshly acquired slice.
    write_family(
        fresh_stage,
        fresh,
        generation_id=generation_id,
        attempt_failures=attempt_failures,
        prior_receipts=replaced_receipts_path if replaced_receipt_count else None,
    )
    verify_family_readback(fresh_stage, fresh)

    # Merge kept prior slices and fresh slices directly in Arrow.
    for name in SOURCE_NAMES:
        scope_column = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
        replaced = replaced_publishers if name == "scorecard_publishers" else replaced_scorecards
        _copy_filtered_subject_slice(
            prior_dir / f"{name}.parquet",
            fresh_stage / f"{name}.parquet",
            merged_stage / f"{name}.parquet",
            schema=POLICIES[name].subject_schema,
            scope_column=scope_column,
            replaced=replaced,
            expected_kept=kept_counts[name],
            expected_fresh=len(fresh[name]),
        )

    merged_receipts_path = merged_stage / RECEIPT_KEY
    _merge_receipts(prior_receipts, kept_ordinals_path, fresh_stage / RECEIPT_KEY, merged_receipts_path)

    validate_receipt_bundle(
        {n: [merged_stage / f"{n}.parquet"] for n in SOURCE_NAMES},
        [merged_receipts_path],
        [POLICIES[n] for n in SOURCE_NAMES],
        generation_id=generation_id,
    )

    paths = []
    for name in SOURCE_NAMES:
        target = directory / f"{name}.parquet"
        (merged_stage / f"{name}.parquet").replace(target)
        paths.append(target)
    merged_receipts_path.replace(directory / RECEIPT_KEY)
    (directory / "scorecard-etl-build.json").write_text(
        json.dumps({"generation_id": generation_id, "datasets": list(SOURCE_NAMES)}, indent=2) + "\n"
    )
    if evidence is not None:
        for name, removed in retired_by_table.items():
            identity = list(contracts[name].identity) if contracts is not None and name in contracts else list(IDENTITIES[name])
            evidence.event(
                "rows-retired",
                table=name,
                key=identity,
                rows=removed,
                reason="complete-scorecard-replacement",
            )
    rmtree(stage)
    return tuple(paths)
