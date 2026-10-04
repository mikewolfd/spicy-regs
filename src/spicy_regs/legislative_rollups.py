"""Legislative rollups use the shared native selection and build lifecycle."""

from spicy_regs.legislative_receipts import FILE_POLICY, policy
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup


def family_policies(*datasets):
    return tuple(policy(n) for n in datasets) + (FILE_POLICY,)


# Kept as a domain name for the scheduled producers; all behavior is shared.
LegislativeReceiptRollup = SubjectReceiptRollup
