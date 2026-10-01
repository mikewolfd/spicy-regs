"""Select exact retained filing formats for the new financial tables.

Versions identify source layouts, not old application APIs. Combined source
names stay combined; statement balances, aggregates and transactions stay apart.
"""

from functools import cache
import re

from .fec_filing_financial import MAPPINGS, _mapping
from .fec_filing_layout_data import COLUMNS, LAYOUTS


@cache
def retained_filing_mappings():
    """Load reviewed positions without interpreting source labels at runtime."""
    by_form = {mapping.form: mapping for mapping in MAPPINGS}
    result = list(MAPPINGS)
    for family, version, form, declared, base_form, pattern, pin, workbook, columns in LAYOUTS:
        base = by_form[base_form]
        names = COLUMNS[columns]
        amounts = {name for name, _ in base.fields.amounts}
        dates = {name for name, _, _ in base.fields.dates} | {"communication_date"}
        result.append(
            _mapping(
                form,
                pin.removeprefix("sha256:"),
                pattern,
                base.fields.table,
                names,
                " ".join(sorted(amounts & set(names.split()))),
                " ".join(sorted(dates & set(names.split()))),
                base.fields.constants,
                family=family,
                version=version,
                declared_versions=(declared,),
                workbook_sha256=workbook,
            )
        )
    return tuple(result)


@cache
def filing_mapping_for(declared_version: str, record_type: str):
    """Return one supported schedule; unsupported forms keep their native rows."""
    if record_type in {"SA3L", "SB3L"}:
        return None
    matches = [
        mapping
        for mapping in retained_filing_mappings()
        if declared_version in mapping.declared_versions and re.fullmatch(mapping.record_pattern, record_type)
    ]
    if len(matches) > 1:
        raise ValueError("Ambiguous retained filing mapping")
    return matches[0] if matches else None
