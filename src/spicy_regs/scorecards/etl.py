"""Plain-Parquet scorecard family writer, reader, and Arrow/DuckDB slice-carry merge."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import hashlib
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    DatasetPolicy,
    RECEIPT_KEY,
    read_with_receipts,
    select_receipts,
)
from spicy_regs.policy_migrations import earlier_policies
from spicy_regs.scorecards.subject_shapes import (
    BOOLEAN_FIELDS,
    COMPARISON_POLICY_VERSION,
    DECIMAL_FIELDS,
    DOMAIN_COLUMNS,
    IDENTITIES,
    INTEGER_FIELDS,
    ITEM_LINK_POLICY_VERSION,
    ITEMS_POLICY_VERSION,
    MEMBER_LINK_POLICY_VERSION,
    POLICY_VERSION,
    RATING_POLICY_VERSION,
    SOURCE_COLUMNS,
    map_source_row,
    native_to_source_row,
    restore_source_row,
    subject_schema,
)

LINK_NAMES = ("scorecard_member_links", "scorecard_item_links")
ANALYSIS_NAMES = (*LINK_NAMES, "scorecard_action_comparisons")
SOURCE_NAMES = tuple(name for name in SOURCE_COLUMNS if name not in ANALYSIS_NAMES)
SUBJECT_ROW_GROUP = 50_000
_WRITE_BATCH_ROWS = 2_000


def policy(name: str) -> DatasetPolicy:
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
            "scorecard_items": ITEMS_POLICY_VERSION,
            "scorecard_member_ratings": RATING_POLICY_VERSION,
            "scorecard_member_links": MEMBER_LINK_POLICY_VERSION,
            "scorecard_item_links": ITEM_LINK_POLICY_VERSION,
            "scorecard_action_comparisons": COMPARISON_POLICY_VERSION,
        }.get(name, POLICY_VERSION),
        receipt_only=not schema.names,
    )


POLICIES = {name: policy(name) for name in SOURCE_COLUMNS}
# Historical declarations retained for policy_migrations and one-time cutover reads.
EARLIER_POLICIES = {name: earlier_policies(pol) for name, pol in POLICIES.items()}


def generation_schemas(names: Iterable[str]) -> dict:
    """Declared subject schemas for generation admission."""
    from spicy_regs.contract_types import described_schema

    return {
        name: described_schema(POLICIES[name].subject_schema)
        for name in names
        if not POLICIES[name].receipt_only
    }


def admitted_read_policies(names, *, descriptors=None, columns=None):
    """Select only known exact policies from a verified artifact or pinned table declarations."""
    from itertools import product
    from spicy_regs.contract_types import described_schema

    if (descriptors is None) == (columns is None):
        raise ValueError("Scorecard reads require exactly one admitted policy declaration")
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
    matches = {}
    for name, candidates in choices.items():
        matches[name] = [
            p
            for p in candidates
            if (p.receipt_only and name not in columns)
            or (
                not p.receipt_only
                and name in columns
                and [tuple(c) for c in columns[name]] == described_schema(p.subject_schema)
            )
        ]
        if not matches[name]:
            raise ValueError("Scorecard columns are not a supported exact layout")
    for variant in product(*(matches[name] for name in names)):
        selected = dict(zip(names, variant))
        if set(columns) == {name for name, p in selected.items() if not p.receipt_only}:
            return selected
    raise ValueError("Scorecard input differs from the supported exact historical or current policies")


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


def write_family(directory: Path, tables: Mapping[str, Iterable[Mapping]]) -> tuple[Path, ...]:
    """Validate, convert, and atomically write plain native Parquet tables without per-row receipts."""
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / (".scorecard-etl-" + uuid4().hex)
    stage.mkdir(parents=True)
    outputs: list[Path] = []
    try:
        for name, rows in tables.items():
            schema = POLICIES[name].subject_schema
            columns = schema.names
            identity_fields = IDENTITIES[name]
            target = stage / f"{name}.parquet"
            seen: set[tuple[object, ...]] = set()
            batch: list[dict] = []
            with _SubjectWriter(target, schema) as writer:
                for raw in rows:
                    try:
                        mapped = map_source_row(name, raw)
                    except (ValueError, TypeError, OverflowError) as error:
                        raise ValueError(f"Scorecard conversion refused ({name}): {error}") from error
                    key = tuple(mapped[field] for field in identity_fields)
                    if any(part is None or part == "" for part in key) or key in seen:
                        raise ValueError(f"Duplicate or ambiguous scorecard identity: {name}")
                    seen.add(key)
                    batch.append({col: mapped[col] for col in columns})
                    if len(batch) >= _WRITE_BATCH_ROWS:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                        batch.clear()
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            outputs.append(target)
        paths = []
        for subject in outputs:
            dest = directory / subject.name
            subject.replace(dest)
            paths.append(dest)
        return tuple(paths)
    finally:
        rmtree(stage, ignore_errors=True)


def read_family(directory: Path, names: Iterable[str], *, include_omitted: bool = False) -> dict[str, list[dict]]:
    """Read verified native Parquet tables back into string/null source dictionaries."""
    result: dict[str, list[dict]] = {}
    for name in names:
        path = directory / f"{name}.parquet"
        schema = POLICIES[name].subject_schema
        with pq.ParquetFile(path) as parquet:
            if not parquet.schema_arrow.equals(schema):
                raise ValueError(f"Subject schema differs from policy: {name}")
            rows: list[dict] = []
            for batch in parquet.iter_batches(batch_size=SUBJECT_ROW_GROUP):
                rows.extend(native_to_source_row(name, row, include_omitted=include_omitted) for row in batch.to_pylist())
            result[name] = rows
    return result


def verify_family_readback(directory: Path, expected: Mapping[str, Sequence[Mapping]]) -> dict[str, int]:
    """Stream-verify persisted native Parquet rows against expected source rows without receipts."""
    counts: dict[str, int] = {}
    for name, rows in expected.items():
        schema = POLICIES[name].subject_schema
        columns = schema.names
        identity_fields = IDENTITIES[name]
        remaining: dict[tuple[object, ...], dict] = {}
        for raw in rows:
            mapped = map_source_row(name, raw)
            projected = {col: mapped[col] for col in columns}
            key = tuple(projected[field] for field in identity_fields)
            if any(part is None or part == "" for part in key) or key in remaining:
                raise ValueError("Duplicate or empty scorecard identity: " + name)
            remaining[key] = projected
        path = directory / f"{name}.parquet"
        seen_count = 0
        with pq.ParquetFile(path) as parquet:
            if not parquet.schema_arrow.equals(schema):
                raise ValueError("Persisted scorecard schema differs: " + name)
            for batch in parquet.iter_batches(batch_size=SUBJECT_ROW_GROUP):
                for persisted in batch.to_pylist():
                    key = tuple(persisted[field] for field in identity_fields)
                    before = remaining.pop(key, None)
                    if before is None or persisted != before:
                        raise ValueError("Persisted scorecard source row differs: " + name)
                    seen_count += 1
        if remaining or seen_count != len(rows):
            raise ValueError("Persisted scorecard source family is incomplete: " + name)
        counts[name] = seen_count
    return counts


def read_published_source_inputs(snapshot: Mapping, paths: Mapping[str, Sequence[Path]]) -> dict[str, list[dict]]:
    """Read receiptless published scorecard Parquet inputs with pin, schema, and population checks."""
    from spicy_regs.local_data import file_signature, file_state_from_stat
    from spicy_regs.scorecards.analysis_inputs import published_input
    from spicy_regs.sources import publication

    result: dict[str, list[dict]] = {}
    for name, member_paths in paths.items():
        owner = publication.table_owner(snapshot, name + ".parquet")
        if owner is None or not published_input(owner[1], name):
            raise ValueError("Scorecard inputs require published receipt-retired tables: " + name)
        members = publication.table_members(snapshot, name + ".parquet")
        if not member_paths or len(member_paths) != len(members):
            raise ValueError("Scorecard published inputs require every selected member: " + name)
        schema = POLICIES[name].subject_schema
        rows: list[dict] = []
        for path, member in zip(member_paths, members, strict=True):
            staged_before = file_state_from_stat(path.lstat())
            retained = path.resolve(strict=True)
            before = file_signature(retained)
            with retained.open("rb") as stream:
                digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != member.sha256 or retained.stat().st_size != member.byte_size:
                raise ValueError("Scorecard source input differs from its immutable pin: " + name)
            with pq.ParquetFile(retained) as source:
                if not source.schema_arrow.equals(schema, check_metadata=False):
                    raise ValueError("Scorecard source input differs from its native schema: " + name)
                for batch in source.iter_batches(batch_size=_WRITE_BATCH_ROWS):
                    rows.extend(native_to_source_row(name, row) for row in batch.to_pylist())
            if (
                file_state_from_stat(path.lstat()) != staged_before
                or path.resolve(strict=True) != retained
                or file_signature(retained) != before
            ):
                raise ValueError("Scorecard source input changed during its read: " + name)
        if len(rows) != owner[1]["tables"][name + ".parquet"]["rows"]:
            raise ValueError("Scorecard source input differs from its complete selected population: " + name)
        result[name] = rows
    return result


def _matching_policy(name: str, subject_paths: Sequence[Path]) -> DatasetPolicy:
    schema = pq.read_schema(subject_paths[0])
    for candidate in (POLICIES[name], *EARLIER_POLICIES[name]):
        if not candidate.receipt_only and schema.equals(candidate.subject_schema):
            return candidate
    raise ValueError(f"Subject schema differs from supported scorecard policies: {name}")


def read_source_inputs(paths: Mapping[str, Sequence[Path]], receipt_path: Path, *, generation_id: str) -> dict:
    """Transition reader for a still-receipted scorecards generation before its first receiptless publish."""
    result = {}
    with TemporaryDirectory(prefix="scorecard-analysis-inputs-") as temporary:
        for name, subject_paths in paths.items():
            selected_policy = _matching_policy(name, subject_paths)
            selected = select_receipts(receipt_path, Path(temporary) / (name + ".parquet"), dataset=name)
            result[name] = [
                restore_source_row(name, row)
                for row in read_with_receipts(subject_paths, [selected], selected_policy, generation_id=generation_id)
            ]
    return result


def upgrade_prior_table_from_receipts(
    prior_path: Path,
    receipt_path: Path,
    prior_policy: DatasetPolicy,
    *,
    generation_id: str,
) -> None:
    """One-time cutover helper: upgrade an earlier-policy staged table (e.g. scorecard_items v2 -> v3) via its receipts."""
    name = prior_policy.dataset
    schema = POLICIES[name].subject_schema
    columns = schema.names
    with TemporaryDirectory(prefix="scorecard-prior-upgrade-", dir=prior_path.parent) as temporary:
        temp_dir = Path(temporary)
        scoped = select_receipts(receipt_path, temp_dir / f"{name}-receipts.parquet", dataset=name)
        upgraded = temp_dir / f"{name}.parquet"
        batch: list[dict] = []
        with _SubjectWriter(upgraded, schema) as writer:
            for native in read_with_receipts([prior_path], [scoped], prior_policy, generation_id=generation_id):
                mapped = map_source_row(name, restore_source_row(name, native))
                batch.append({col: mapped[col] for col in columns})
                if len(batch) >= _WRITE_BATCH_ROWS:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch.clear()
        upgraded.replace(prior_path)


def verified_receipt_download(index, target: Path, *, public_url: str | None, dataset="scorecards") -> Path:
    """Fetch a selected family's receipt member and verify its pinned bytes."""
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
    """Qualification read of an admitted scorecard source generation."""
    from spicy_regs.generations import verify_generation

    artifact = verify_generation(directory)
    spec = artifact.root["spec"]
    if spec["family"] != "scorecards":
        raise ValueError("Expected a scorecard source generation")
    if "etlReceipts" in spec:
        selected = admitted_read_policies(SOURCE_NAMES, descriptors=spec["etlReceipts"]["policies"])
        generation_id = spec["etlReceipts"]["generationId"]
        receipt_path = directory / RECEIPT_KEY
        rows: dict[str, list[dict]] = {}
        with TemporaryDirectory(prefix="scorecard-gen-read-") as temporary:
            for name in SOURCE_NAMES:
                scoped = select_receipts(receipt_path, Path(temporary) / (name + ".parquet"), dataset=name)
                rows[name] = [
                    restore_source_row(name, row)
                    for row in read_with_receipts(
                        [directory / (name + ".parquet")], [scoped], selected[name], generation_id=generation_id
                    )
                ]
        return rows
    if set(spec["tables"]) != {name + ".parquet" for name in SOURCE_NAMES}:
        raise ValueError("Incomplete scorecard source generation")
    return read_family(directory, SOURCE_NAMES, include_omitted=True)


def selected_table_entry(snapshot: Mapping, key: str) -> Mapping:
    """Require a selected published table owner."""
    from spicy_regs.sources import publication

    owner = publication.table_owner(snapshot, key)
    if owner is None:
        raise publication.PublicationError(f"Missing selected table owner: {key}")
    return owner[1]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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


def _check_prior_schemas(prior_dir: Path) -> None:
    """Refuse loudly if any prior subject table is missing or differs from current policy schemas."""
    for name in SOURCE_NAMES:
        prior_path = prior_dir / f"{name}.parquet"
        if not prior_path.is_file():
            raise ValueError(f"Missing prior scorecard subject table: {name}")
        with pq.ParquetFile(prior_path) as pf:
            if not pf.schema_arrow.equals(POLICIES[name].subject_schema):
                raise ValueError(f"Prior scorecard table {name} schema differs from current policy")


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
    fresh: Mapping[str, list[dict]],
    replaced_publishers: set[str],
    replaced_scorecards: set[str],
    contracts: Mapping | None,
    collect_retired: bool,
) -> tuple[dict[str, int], dict[str, list[list[str]]]]:
    """Validate plain prior Parquet slices in DuckDB and compute kept counts and retired keys."""
    lit = etl_bulk._literal
    kept_counts: dict[str, int] = {}
    retired_by_table: dict[str, list[list[str]]] = {}

    with etl_bulk.bulk_connection(stage) as (con, _):
        con.execute("SET preserve_insertion_order = false")
        for name in SOURCE_NAMES:
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

            id_cols = IDENTITIES[name]
            select_cols = ", ".join(_q(col) for col in id_cols)
            if con.execute(f"SELECT count(*) - count(DISTINCT row({select_cols})) FROM {view}").fetchone()[0]:
                raise ValueError(f"Prior scorecard table {name} contains duplicate identity")

            con.execute(f"CREATE TABLE replaced_{name} (scope_val VARCHAR)")
            if replaced:
                con.executemany(f"INSERT INTO replaced_{name} VALUES (?)", [(val,) for val in sorted(replaced)])

            kept_counts[name] = con.execute(
                f"SELECT count(*) FROM {view} WHERE {_q(scope_column)} NOT IN (SELECT scope_val FROM replaced_{name})"
            ).fetchone()[0]

            if collect_retired and replaced:
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

        _check_kept_cross_table_integrity(con, fresh)
    return kept_counts, retired_by_table


def merge_family_slices(
    directory: Path,
    *,
    prior_dir: Path,
    fresh: Mapping[str, list[dict]],
    replaced_publishers: set[str],
    replaced_scorecards: set[str],
    contracts: Mapping | None = None,
    evidence=None,
) -> tuple[Path, ...]:
    """Carry unchanged prior scorecard Parquet slices in Arrow/DuckDB and merge with fresh scopes."""
    if set(fresh) != set(SOURCE_NAMES):
        raise ValueError("Scorecard fresh bundle must contain the complete frozen table set")
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / (".scorecard-etl-" + uuid4().hex)
    stage.mkdir(parents=True)
    fresh_stage = stage / "fresh"
    fresh_stage.mkdir()
    merged_stage = stage / "merged"
    merged_stage.mkdir()
    try:
        _check_prior_schemas(prior_dir)
        kept_counts, retired_by_table = _prepare_prior_slices(
            stage,
            prior_dir=prior_dir,
            fresh=fresh,
            replaced_publishers=replaced_publishers,
            replaced_scorecards=replaced_scorecards,
            contracts=contracts,
            collect_retired=evidence is not None,
        )

        # Build and readback-verify only the freshly acquired slice.
        write_family(fresh_stage, fresh)
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

        paths = []
        for name in SOURCE_NAMES:
            target = directory / f"{name}.parquet"
            (merged_stage / f"{name}.parquet").replace(target)
            paths.append(target)
        if evidence is not None:
            for name, removed in retired_by_table.items():
                identity = (
                    list(contracts[name].identity)
                    if contracts is not None and name in contracts
                    else list(IDENTITIES[name])
                )
                evidence.event(
                    "rows-retired",
                    table=name,
                    key=identity,
                    rows=removed,
                    reason="complete-scorecard-replacement",
                )
        return tuple(paths)
    finally:
        rmtree(stage, ignore_errors=True)
