"""Scheduled FEC identity/links rollups that publish native tables without per-row receipts (ss-vse3)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
import importlib
import json
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.selected_generations import unique_build_directory
from spicy_regs.transforms.build_fec_identity_rollup import BUILDERS, resolve_acquisition
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY, normalize_record, subject_schema
from spicy_regs.transforms.fec_master_history import write_json
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.transforms.table_merge import published_prior

#: Retired FEC identity tables readable back into their builder's all-VARCHAR source shape.
FEC_PUBLISHED_READABLE = frozenset({"fec_committees", "fec_committee_history"})


def published_as_fec_source(dataset: str, published: Path | Sequence[Path], destination: Path) -> Path:
    """Read a published native FEC table back into its builder's all-VARCHAR text shape."""
    if dataset not in FEC_PUBLISHED_READABLE:
        raise ValueError(f"{dataset}: unsupported published FEC source reconstruction")
    paths = [Path(published)] if isinstance(published, (str, Path)) else [Path(path) for path in published]
    rules = REGISTRY[dataset]
    columns = list(rules["input_fields"])
    list_sources = {raw: native_name for raw, (native_name, _) in rules["native_lists"].items()}
    required = {list_sources.get(col, col) for col in columns}
    schema = pa.schema([(col, pa.string()) for col in columns])

    def rows():
        for path in paths:
            with pq.ParquetFile(path) as source:
                if missing := sorted(required - set(source.schema_arrow.names)):
                    raise ValueError(f"{dataset}: published prior lacks {missing}")
                for batch in source.iter_batches(batch_size=10_000, use_threads=False):
                    for row in batch.to_pylist():
                        yield {
                            col: (
                                None if (v := row.get(list_sources[col])) is None
                                else json.dumps(v, ensure_ascii=False, allow_nan=False)
                            ) if col in list_sources else (
                                None if (v := row.get(col)) is None
                                else v.isoformat() if isinstance(v, date) else str(v)
                            )
                            for col in columns
                        }

    destination.parent.mkdir(parents=True, exist_ok=True)
    return write_rows(rows(), destination, schema)


def write_fec_table(table: str, source_path: Path, destination: Path, *, batch_size: int = 10_000) -> Path:
    """Map one FEC builder's output onto its declared native subject schema without writing receipts."""
    schema, identity_fields = subject_schema(table), tuple(REGISTRY[table]["identity_fields"])
    columns = schema.names
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    ordinal, seen = 0, set()
    try:
        with pq.ParquetFile(source_path) as source, pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            for batch in source.iter_batches(batch_size=batch_size, use_threads=False):
                mapped_rows = []
                for row in batch.to_pylist():
                    mapped = normalize_record(table, row, observation_ordinal=ordinal)
                    ordinal += 1
                    subject = {name: mapped[name] for name in columns}
                    key = tuple(subject[name] for name in identity_fields)
                    if any(value is None for value in key):
                        raise ValueError(f"{table}: null record identity")
                    if key in seen:
                        raise ValueError(f"{table}: duplicate record identity")
                    seen.add(key)
                    mapped_rows.append(subject)
                if mapped_rows:
                    writer.write_table(pa.Table.from_pylist(mapped_rows, schema=schema))
        temporary.replace(destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return destination


def _link_or_copy(source: Path, target: Path) -> None:
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)


class FecTableRollup(RollupPipeline):
    """One FEC identity/links table whose installed policy is marked ``receipts_retired``."""

    def __init_subclass__(cls, **kwargs):
        from spicy_regs.subject_catalog import retired_receipt_datasets

        super().__init_subclass__(**kwargs)
        if not isinstance(cls.output, str):
            raise ValueError("FEC table rollup requires an explicit output name")
        dataset = cls.output.removesuffix(".parquet")
        if dataset not in BUILDERS:
            raise ValueError(f"Unsupported FEC table rollup: {dataset}")
        if dataset not in retired_receipt_datasets():
            raise ValueError(f"{cls.__name__}: {dataset} still publishes receipts; mark its policy receipts_retired")

    def generation_schemas(self):
        table = self.output.removesuffix(".parquet")
        return {**super().generation_schemas(), table: described_schema(subject_schema(table))}

    def build_table(self, output_dir: Path, **builder_options) -> Path:
        """Run this rollup's FEC builder, preserve raw acquisition custody, and map directly to native Parquet."""
        from spicy_regs.phase_log import phase_log
        from spicy_regs.sources import publication

        output_dir, table = Path(output_dir), self.output.removesuffix(".parquet")
        local_inputs = []
        for key in self.inputs:
            path = output_dir / key
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Required published input unavailable: {key}")
            local_inputs.append(path)

        prior_mode: str | None = None
        local_prior: Path | None = None
        if table == "fec_committees":
            if public_url := os.getenv("R2_PUBLIC_URL"):
                if publication.table_owner(publication.current_index(public_url), self.output) is not None:
                    prior_mode = "remote"
            elif (candidate := (self.output_dir or output_dir) / self.output).is_file() and not candidate.is_symlink():
                prior_mode, local_prior = "local", candidate
            if prior_mode is None and not builder_options.get("full_walk"):
                raise ValueError("Committee increment requires a published prior table; first build requires full_walk")

        work: Path | None = None

        def ensure_evidence_dir() -> Path:
            nonlocal work
            if work is None:
                work = (
                    output_dir if output_dir.parent.name == ".builds"
                    else unique_build_directory(self.output_dir or output_dir)
                )
            return work / "native.inputs"

        if (acquisition := resolve_acquisition(table, builder_options, evidence_dir=ensure_evidence_dir)) is not None:
            evidence_dir = ensure_evidence_dir()
            evidence_dir.mkdir(exist_ok=False)
            write_json(evidence_dir / "acquisition.json", acquisition)

        output_dir.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".fec-identity-producer-", dir=work or output_dir) as temporary:
            stage = Path(temporary)
            for path in local_inputs:
                shutil.copyfile(path, stage / path.name)
            for key in self.remote_inputs:
                candidate = output_dir / key
                if candidate.is_file() and not candidate.is_symlink() and not (stage / key).exists():
                    _link_or_copy(candidate, stage / key)
            if table == "fec_committees":
                from spicy_regs.transforms.build_fec_committees import _SCHEMA

                prior_path = stage / "_fec_prior.parquet"
                if prior_mode == "remote":
                    published_path = stage / f"_published_{self.output}"
                    if not published_prior(self.output, published_path):
                        raise ValueError("Published prior fec_committees.parquet became unavailable")
                    with phase_log("prior-published-input", dataset=table):
                        published_as_fec_source(table, published_path, prior_path)
                    published_path.unlink()
                elif prior_mode == "local" and local_prior is not None:
                    with phase_log("prior-published-input", dataset=table):
                        published_as_fec_source(table, local_prior, prior_path)
                else:
                    write_rows((), prior_path, _SCHEMA)
            module, function = BUILDERS[table]
            built = getattr(importlib.import_module("spicy_regs.transforms." + module), function)(stage, **builder_options)
            with phase_log("public-table", dataset=table):
                return write_fec_table(table, built, output_dir / self.output)
