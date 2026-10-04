"""Receipt-aware producer integration for fully owned legislative rollups.

Bill-family adoption requires the Congress owner's policies for its sibling
tables. These complete owned families already use the shared base-class hooks.
"""

import json
from os import getenv
from pathlib import Path
import shutil

from spicy_regs.legislative_receipts import FILE_POLICY, FILE_STATES, build_with_receipts, policy
from spicy_regs.pipelines.rollups.base import RollupPipeline


def family_policies(*datasets):
    return tuple(policy(n) for n in datasets) + (FILE_POLICY,)


def _selected_prior(pipeline, directory: Path) -> Path | None:
    """Capture one explicit local bundle or the selected complete public generation."""
    local = getenv("LEGISLATIVE_PRIOR_BUNDLE")
    if local:
        selected = Path(local)
        manifest = json.loads((selected / "legislative-bundle.json").read_text())
        expected = {p.dataset for p in pipeline.receipt_policies} - {FILE_STATES}
        if set(manifest["datasets"]) != expected or manifest["unowned_outputs"]:
            raise ValueError("Explicit local prior differs from the complete owned family")
        return selected
    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        return None
    from spicy_regs.sources.publication import current_index, fetch_member, receipt_members, table_members

    index = current_index(public_url)
    family = index["families"].get(pipeline.publication_family or pipeline.name)
    if family is None:
        return None
    receipt_spec = family.get("etlReceipts")
    if receipt_spec is None:
        raise ValueError("Selected prior has no ETL receipts; first migrate its explicitly retained source outputs")
    expected = {p.dataset for p in pipeline.receipt_policies}
    declared = set(receipt_spec["datasets"])
    if expected != declared:
        raise ValueError("Selected prior policies differ from the complete owned family")
    directory.mkdir(parents=True, exist_ok=False)
    datasets = sorted(expected - {FILE_STATES})
    members = receipt_members(index, dataset=datasets[0])
    if len(members) != 1:
        raise ValueError("Owned rollup needs exactly one selected receipt generation")
    receipt_path = directory / "etl_receipts.parquet"
    if not fetch_member(public_url, members[0], receipt_path, members[0].path):
        raise ValueError("Selected receipt member is missing")
    subjects = {}
    for name in datasets:
        subjects[name] = []
        if policy(name).receipt_only:
            continue
        for member in table_members(index, name + ".parquet"):
            target = directory / member.key
            target.parent.mkdir(parents=True, exist_ok=True)
            if not fetch_member(public_url, member, target, member.path):
                raise ValueError("Selected subject member is missing")
            subjects[name].append(member.key)
    manifest = {
        "generation_id": receipt_spec["generationId"], "datasets": datasets, "subjects": subjects,
        "receipt_file": "etl_receipts.parquet", "unowned_outputs": [], "refused_rows": {},
    }
    # Restoration validates all bytes and source/subject links before a builder
    # receives any prior. Refused conversions cannot establish completeness.
    import pyarrow.parquet as pq
    for batch in pq.ParquetFile(receipt_path).iter_batches(columns=["dataset", "outcome"]):
        for row in batch.to_pylist():
            if row["outcome"] in {"refused", "error", "rejected"}:
                name = row["dataset"]
                manifest["refused_rows"][name] = manifest["refused_rows"].get(name, 0) + 1
    (directory / "legislative-bundle.json").write_text(json.dumps(manifest) + "\n")
    return directory


class LegislativeReceiptRollup(RollupPipeline):
    """An owned rollup's native writer, with provenance restored before merging."""

    def build_receipts(self, output_dir, builder, **builder_kwargs):
        prior = _selected_prior(self, output_dir / ".selected-legislative-prior")
        bundle = output_dir / ".legislative-bundle"
        manifest = build_with_receipts(
            builder, output_dir / ".legislative-source", bundle,
            generation_id=self.receipt_generation_id, prior_bundle=prior,
            builder_kwargs=builder_kwargs,
        )
        if manifest["unowned_outputs"]:
            raise ValueError("A complete legislative rollup emitted an unowned output")
        paths = []
        for dataset, members in manifest["subjects"].items():
            if policy(dataset).receipt_only:
                continue
            if members != [dataset + ".parquet"]:
                raise ValueError("This owned rollup expects a single member per subject table")
            target = output_dir / members[0]
            shutil.copyfile(bundle / members[0], target)
            paths.append(target)
        shutil.copyfile(bundle / "etl_receipts.parquet", output_dir / "etl_receipts.parquet")
        return tuple(paths)
