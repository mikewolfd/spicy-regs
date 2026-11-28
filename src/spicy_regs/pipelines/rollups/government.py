"""Government-source declarations for the shared rollup generation writer."""

from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS


class GovernmentTableRollup(RollupPipeline):
    """A government-source family whose tables publish no per-row receipts (ss-vse3; policies ``receipts_retired``).

    Every government-source rollup subclasses this directly. Its run refuses at its start, in seconds, while
    docs/pins/receipt-archive.json does not pin the live generation that still lists those receipts
    (``publication.require_live_receipt_pins``), which its first receipts-free publish would refuse at the end.
    """

    def generation_schemas(self):
        schemas = super().generation_schemas()
        for key in self.outputs or (self.output,):
            dataset = key.removesuffix(".parquet")
            schemas[dataset] = described_schema(SUBJECT_SCHEMAS[dataset])
        return schemas

    def __init_subclass__(cls, **kwargs):
        from spicy_regs.subject_catalog import retired_receipt_datasets

        super().__init_subclass__(**kwargs)
        keys = cls.outputs or (cls.output,)
        if not all(isinstance(key, str) for key in keys):
            raise TypeError("Government rollups must declare their output table names")
        names = {key.removesuffix(".parquet") for key in keys if isinstance(key, str)}
        if unretired := sorted(names - retired_receipt_datasets()):
            raise ValueError(f"{cls.__name__}: {unretired} still publish receipts; retire their policies first")
