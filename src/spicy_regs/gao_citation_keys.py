"""The retained GAO printed-number key, falling back only on a SQL NULL."""

from collections.abc import Mapping

_FIELDS = ("report_number", "report_id")
CITATION_PRODUCT_SQL = "upper(coalesce(" + ", ".join(_FIELDS) + "))"


def citation_product_id(row: Mapping[str, str | None]) -> str | None:
    """Keep printed-number precedence, blanks and publisher page aliases intact."""
    import pyarrow as pa
    import pyarrow.compute as pc

    value = next((row.get(field) for field in _FIELDS if row.get(field) is not None), None)
    # Arrow and DuckDB use simple Unicode uppercase; Python str.upper can expand
    # characters (for example, ß to SS) and create a different lookup key.
    return pc.call_function("utf8_upper", [pa.scalar(value, type=pa.string())]).as_py()
