"""Native filing observations and their transactions; amendments remain separate."""

import pyarrow as pa

FAMILY = "senate-disclosures"
TABLES = ("senate_disclosure_reports", "senate_disclosure_transactions")
COMMON = [
    ("filing_year", pa.int32()),
    ("report_id", pa.string()),
    ("source_url", pa.string()),
    ("report_sha256", pa.string()),
]
REPORT = [
    ("first_name", pa.string()),
    ("last_name", pa.string()),
    ("filer_label", pa.string()),
    ("report_kind", pa.string()),
    ("report_title", pa.string()),
    ("report_date", pa.date32()),
    ("submitted_date", pa.date32()),
    ("amendment_status", pa.string()),
    ("amendment_number", pa.int32()),
    ("original_candidate_ids", pa.list_(pa.string())),
    ("amendment_link_status", pa.string()),
    ("parse_status", pa.string()),
    ("filer_heading", pa.string()),
    ("filed_at_raw", pa.string()),
    ("transaction_count", pa.int64()),
    ("search_sha256", pa.string()),
    ("search_start", pa.int64()),
    ("search_row", pa.int64()),
    ("raw_search_cells_json", pa.string()),
    ("observed_at", pa.string()),
]
TRANSACTION = [
    ("row_number", pa.int64()),
    ("row_position", pa.int64()),
    ("source_locator", pa.string()),
    ("transaction_date", pa.date32()),
    ("transaction_date_raw", pa.string()),
    ("owner", pa.string()),
    ("ticker", pa.string()),
    ("ticker_raw", pa.string()),
    ("asset_name", pa.string()),
    ("asset_type", pa.string()),
    ("transaction_type", pa.string()),
    ("amount_raw", pa.string()),
    ("amount_lower_usd", pa.int64()),
    ("amount_upper_usd", pa.int64()),
    ("lower_inclusive", pa.bool_()),
    ("amount_parse_status", pa.string()),
    ("comment", pa.string()),
    ("comment_raw", pa.string()),
    ("raw_cells_json", pa.string()),
]
SCHEMAS = {TABLES[0]: pa.schema(COMMON + REPORT), TABLES[1]: pa.schema(COMMON + TRANSACTION)}
