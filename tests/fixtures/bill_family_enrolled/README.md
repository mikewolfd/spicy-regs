# Bill family 6afdbce3: the rows the enrolled-text rule was measured on

Rows of bill family `6afdbce3385b9fc15b95bfb72036b5d4eb2a05d6f5464cf6afffcf1582357027` (published
2026-10-04T02:55:21Z), the generation spicy-docs 0.57.0's `enrolled_text_listed` rule was measured on. They back the
witness tests in `tests/test_bill_family_cbo.py`: read from published rows, the host lists 4,990 enrolled texts in
226,033 `bill_versions` rows and its re-stage pass moves 134 of the 233 bills published `passed_both` to `cleared`
and no action row, in the layout the generation publishes and in the native one. A reading that lists no row moves
no bill and raises nothing, so the counts are the witness.

| File | Rows | Bytes | SHA-256 | What it holds |
| --- | --- | --- | --- | --- |
| `6afdbce3-bill_versions.parquet` | 226,033 | 312,153 | `aa0474a05556f5118a70386888a8a33691b55fb35dbb45556413295259b49510` | Every `bill_versions` row, four of its columns: `bill_id`, `version_code`, `source`, `label` |
| `6afdbce3-passed_both-congress_bills.parquet` | 233 | 77,034 | `ef2f3918eae89c8b0172442c8ef072dd7fecf1b615dbcafc29c255e39172be48` | Every `congress_bills` row whose `stage` is `passed_both`, every column |
| `6afdbce3-passed_both-bill_actions.parquet` | 4,662 | 65,924 | `231ba5d65365a84f02324a6b09ba0370a9b2251d21b43e9f4e55b5edeebb4e17` | Every `bill_actions` row of those 233 bills, every column |

Cut on 2026-10-05 with DuckDB 1.5.5 from the generation's files at
`https://data.spicygov.ai/generations/bill-family/6afdbce3…/`, each checked first against the sha256 the publication
index states for it:

- `congress_bills.parquet` `3aa01689966d9c3ada4c47413e9091390f6aec83f7a6844529a142b77b7ff254`
- `bill_actions.parquet` `703da49671bdc2da80b64cdfbf42452dac3c537d39ef320838dca75a204f15b8`
- `bill_versions.parquet` `53fe56768eed5557bbf11917ae02c8af904316e14d703da0f5b475f96bdce1d4`

The three cuts, each written with `COPY (...) TO ... (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 19)`:

```sql
SELECT bill_id, version_code, source, label FROM bill_versions ORDER BY bill_id, version_code, source;
SELECT * FROM congress_bills WHERE stage = 'passed_both' ORDER BY bill_id;
SELECT a.* FROM bill_actions a SEMI JOIN (SELECT bill_id FROM congress_bills WHERE stage = 'passed_both') b
  USING (bill_id) ORDER BY a.bill_id, TRY_CAST(a.action_index AS INTEGER);
```

The values are as published. Another DuckDB version may write other bytes for the same rows, so the digests pin
these files, not the cut. The rows are U.S. government data in the public domain, as Congress.gov's BILLSTATUS and
GPO state them.
