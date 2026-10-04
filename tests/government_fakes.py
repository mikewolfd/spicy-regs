"""Read migrated test outputs through the same validated internal receipt path."""

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.transforms.government_receipts import BUILD_METADATA, internal_prior


def literal_table(path: Path, dataset: str | None = None):
    if dataset is None:
        metadata = path.parent / BUILD_METADATA
        if metadata.exists():
            descriptor = json.loads(metadata.read_text())
            dataset = next(
                (key for key, value in descriptor["subjects"].items() if Path(value["path"]) == path.resolve()),
                path.stem,
            )
    return pq.read_table(internal_prior(dataset or path.stem, path))


def copy_build_selection(source: Path, destination_dir: Path):
    metadata = source.parent / BUILD_METADATA
    if metadata.exists():
        shutil.copyfile(metadata, destination_dir / BUILD_METADATA)
