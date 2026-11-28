"""Legislative rollups use the shared native selection and build lifecycle."""

from spicy_regs.legislative_receipts import FILE_POLICY, policy
from spicy_regs.pipelines.rollups.subject_receipts import LegislativeTableRollup, SubjectReceiptRollup


def family_policies(*datasets):
    return tuple(policy(n) for n in datasets) + (FILE_POLICY,)


# Kept as domain names for the scheduled producers; behavior is shared in subject_receipts.
LegislativeReceiptRollup = SubjectReceiptRollup
__all__ = ["LegislativeReceiptRollup", "LegislativeTableRollup", "family_policies"]
