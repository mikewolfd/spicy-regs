"""Read migrated or retired test outputs as legacy literal tables."""

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.government_receipts import BUILD_METADATA, internal_prior
from spicy_regs.transforms.government_source_shapes import (
    LEGACY_COLUMNS,
    SUBJECT_SCHEMAS,
    _GAO_INT_COLUMNS,
    native_to_legacy_rows,
)


def literal_table(path: Path, dataset: str | None = None):
    metadata = path.parent / BUILD_METADATA
    if dataset is None and metadata.exists():
        descriptor = json.loads(metadata.read_text())
        dataset = next(
            (key for key, value in descriptor["subjects"].items() if Path(value["path"]) == path.resolve()),
            path.stem,
        )
    name = dataset or path.stem
    if metadata.exists():
        return pq.read_table(internal_prior(name, path))
    if name not in SUBJECT_SCHEMAS:
        cols = tuple(pq.read_schema(path).names)
        name = next((ds for ds, schema in SUBJECT_SCHEMAS.items() if tuple(schema.names) == cols), name)
    if name in LEGACY_COLUMNS:
        rows = native_to_legacy_rows(name, path)
        schema = pa.schema([(c, pa.int64() if c in _GAO_INT_COLUMNS else pa.string()) for c in LEGACY_COLUMNS[name]])
        return pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
    return pq.read_table(path)


def copy_build_selection(source: Path, destination_dir: Path):
    metadata = source.parent / BUILD_METADATA
    if metadata.exists():
        shutil.copyfile(metadata, destination_dir / BUILD_METADATA)
