"""Receipt-writing local entrypoint for the existing FEC registry/history builders.

Existing acquisition and incremental selection run unchanged in private staging;
only the admitted native subject and shared receipts leave the build directory.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from .fec_identity_receipts import IdentityReceiptWriter, dataset_policy
from spicy_regs.etl_receipts import select_receipts, validate_receipt_bundle, read_with_receipts
from spicy_regs.selected_generations import SelectedDataset

BUILDERS = {
    "fec_committees": ("build_fec_committees", "build_fec_committees"),
    "fec_candidate_history": ("build_fec_candidate_history", "build_fec_candidate_history"),
    "fec_committee_history": ("build_fec_committee_history", "build_fec_committee_history"),
    "fec_source_catalog": ("build_fec_source_catalog", "build_fec_source_catalog"),
    "org_committee_links": ("build_org_committee_links", "build_org_committee_links"),
}


def build_fec_identity_rollup(
    table, output_dir, *, generation_id, inputs=(), prior_bundle=None, prior_generation_id=None,
    prior_selection=None, **builder_options
):
    """Run one named maintained producer and write its native receipt bundle.

    Input paths must be explicit local files. A caller may supply a fully local
    existing builder configuration; this function does not enable publication.
    """
    import importlib

    if table not in BUILDERS:
        raise ValueError("Unsupported FEC identity rollup")
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".fec-identity-producer-", dir=output_dir.parent) as temporary:
        stage = Path(temporary)
        # Copy caller-selected local inputs; producers must never rewrite them.
        import shutil

        for path in inputs:
            path = Path(path)
            if path.is_symlink() or not path.is_file():
                raise ValueError("Expected a selected local input file")
            if (stage / path.name).exists():
                raise ValueError("Duplicate input basename")
            shutil.copyfile(path, stage / path.name)
        policy = dataset_policy(table)
        if prior_selection is not None and not isinstance(prior_selection, SelectedDataset):
            raise ValueError("Identity prior must name its exact selected pair")
        if prior_selection is not None and prior_bundle is not None:
            raise ValueError("Select the prior receipt pair once")
        if prior_bundle is not None:
            if not prior_generation_id:
                raise ValueError("Identity prior requires its selected receipt generation")
            prior_root = Path(prior_bundle)
            prior_selection = SelectedDataset(table, () if policy.receipt_only else (prior_root / (table + ".parquet"),),
                                              prior_root / "etl_receipts.parquet", prior_generation_id)
        prior_receipts = []
        if prior_selection is not None:
            if prior_selection.dataset != table:
                raise ValueError("Identity prior belongs to another dataset")
            scoped = select_receipts(prior_selection.receipts, stage / "selected-prior.parquet", dataset=table)
            validate_receipt_bundle({table: prior_selection.subjects}, [scoped], [policy],
                                    generation_id=prior_selection.generation_id)
            prior_receipts.append(scoped)
        module, function = BUILDERS[table]
        if table == "fec_committees":
            from .build_fec_committees import COLUMNS, _SCHEMA
            from .parquet_rows import write_rows

            prior_path = stage / "_fec_prior.parquet"
            if prior_path.exists():
                raise ValueError("Committee prior must come from an admitted receipt bundle")
            if prior_selection is None:
                if not builder_options.get("full_walk"):
                    raise ValueError(
                        "Committee increment requires a selected prior receipt bundle; first build requires full_walk"
                    )
                prior_rows = ()
            else:
                def reconstructed():
                    assert prior_selection is not None
                    for row in read_with_receipts(prior_selection.subjects, prior_receipts, policy,
                                                  generation_id=prior_selection.generation_id):
                        originals = row["conversion_inputs"]
                        yield {name: originals[name] for name in COLUMNS}

                prior_rows = reconstructed()
            # An explicit empty full-walk prior also prevents an unqualified remote fallback.
            write_rows(prior_rows, prior_path, _SCHEMA)
        built = getattr(importlib.import_module("spicy_regs.transforms." + module), function)(stage, **builder_options)
        with built.open("rb") as stream:
            digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        # The mapped input must remain available for receipt witnesses after staging ends.
        evidence_dir = output_dir.with_name(output_dir.name + ".inputs")
        evidence_dir.mkdir(exist_ok=False)
        retained = evidence_dir / built.name
        shutil.copyfile(built, retained)
        witness = dict(source_id=table, source_uri=str(retained), sha256=digest, locator=None, body_version=None)
        with IdentityReceiptWriter(output_dir, generation_id=generation_id, tables=[table],
                                   prior_receipts=prior_receipts) as writer:
            with pq.ParquetFile(built) as source:
                for batch in source.iter_batches(batch_size=512, use_threads=False):
                    for row in batch.to_pylist():
                        writer.emit(table, row, input_witness=witness)
    return output_dir
