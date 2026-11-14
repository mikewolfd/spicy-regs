"""The exact old/current schema boundary for the nullable Comments attachment migration."""
from __future__ import annotations

import pyarrow as pa

from spicy_regs.policy_migrations import earlier_policy
from spicy_regs.transforms.regulations_receipts import policy


def attachment_additions(observed) -> list[pa.Field]:
    """Validate the complete Iceberg schema and return only the admitted nullable additions."""
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids, schema_to_pyarrow

    current = policy("comments").subject_schema
    prior = earlier_policy("comments", "regulations-native-v2").subject_schema

    def matches(schema):
        normalized = _pyarrow_to_schema_without_ids(schema)
        return schema_to_pyarrow(observed, include_field_ids=False).equals(
            schema_to_pyarrow(normalized, include_field_ids=False)
        )

    if matches(current):
        return []
    if not matches(prior):
        raise ValueError("Comments migration requires the exact prior native schema")
    prior_fields = prior.field("attachments").type.value_type
    current_fields = current.field("attachments").type.value_type
    added = list(current_fields)[len(prior_fields):]
    if (not pa.struct(list(current_fields)[:len(prior_fields)]).equals(prior_fields)
            or not all(field.nullable for field in added)
            or not current.remove(current.get_field_index("attachments")).equals(
                prior.remove(prior.get_field_index("attachments")))):
        raise ValueError("Comments migration must only append nullable attachment fields")
    return added
