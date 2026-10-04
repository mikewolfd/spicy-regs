"""Scheduled FEC identity producers emit admitted subjects and shared receipts."""
import shutil

from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup
from spicy_regs.transforms.fec_identity_receipts import dataset_policy


class FecReceiptRollup(RollupPipeline):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        key = cls.output
        if not isinstance(key, str):
            raise ValueError("FEC receipt rollup requires an explicit output name")
        cls.receipt_policies = (dataset_policy(key.removesuffix(".parquet")),)
        cls.receipt_only_tables = tuple(p.dataset + ".parquet" for p in cls.receipt_policies if p.receipt_only)

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}

    def build_receipts(self, output_dir, **options):
        table = self.output.removesuffix(".parquet")
        work = output_dir / ".fec-builds" / self.receipt_generation_id
        work.mkdir(parents=True)
        priors = SelectedPriors(work / "selected-priors")
        prior_bundle, prior_generation = None, None
        if table == "fec_committees":
            if priors.get(table) is not None:
                _, receipt, prior_generation = priors.selections[table]
                prior_bundle = receipt.parent
        inputs = []
        for key in self.inputs:
            path = priors.get(key.removesuffix(".parquet"))
            if path is None:
                raise ValueError(f"Required selected processing input unavailable: {key}")
            inputs.append(path)
        bundle = build_fec_identity_rollup(
            table, work / "native", generation_id=self.receipt_generation_id,
            inputs=inputs, prior_bundle=prior_bundle, prior_generation_id=prior_generation, **options)
        shutil.copyfile(bundle / "etl_receipts.parquet", output_dir / "etl_receipts.parquet")
        if self.receipt_policies[0].receipt_only:
            return ()
        target = output_dir / self.output
        shutil.copyfile(bundle / self.output, target)
        return target
