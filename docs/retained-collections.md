# Administration-policy archive observations

`administration_policy_statements` lets a legislative researcher find the
statements that a pinned community archive associates with congressional bills.
`administration_policy_bill_links` provides each explicit bill-link occurrence.
The maintained SpicyDocs administration-policy reader supplies the native facts;
these tables add queryable rows and generation-bound ETL receipts.

A statement can have no identified bill, several bills, or repeated bill links.
Every statement remains in the first table. Link rows preserve source order and
repetitions. `native_bill_id` preserves `<native bill>-<Congress>` from the
archive reader. The maintained bill identity code converts `bill_id` to
`<Congress>-<type>-<number>` for joins. It does not assert that every linked bill exists
in a particular published bill generation.

The source is `unitedstates/statements-of-administration-policy`, a community
archive. `rescinded` is its assertion. `source_record_json` retains every native
field, including unknown additions and external page URLs. Literal issue dates
and their parser status remain separate. An archived PDF URL is a declared
locator; this integration does not acquire PDF bodies or infer political positions.

## Build a selected candidate

Use the maintained environment with source-reader dependencies available:

```sh
uv run --frozen --no-sync python -m spicy_regs.sources.retained_collections \
  --manifest /absolute/path/to/selection.json \
  --output /absolute/path/to/new-candidate
```

The manifest uses `format: spicy-retained-collection-inputs`, `version: 1`, and
`sources`, a bounded nonempty list. Every selected entry has:

- `kind: administration-policy`, a full lowercase `commit`, and `administration`
  such as `47-Trump`;
- `path` to retained YAML or losslessly compressed `.gz` bytes (relative paths
  resolve against the manifest), with original decoded `sha256` and `bytes`;
- `url`, exactly the selected commit's metadata URL, and `observed_at`, the
  acquisition observation recorded in retained evidence.

The caller must qualify the declared commit, URL and acquisition time against
retained acquisition evidence. A digest verifies selected content; it alone
cannot prove where or when the bytes were acquired. A filename alone proves none
of those facts. This builder makes no network request.

The builder refuses mismatched input hash/size, duplicate commit/administration
selections, unsupported kinds, malformed source records, and byte-bound
violations. It checks code, manifest and originals again before publishing the
candidate through the maintained no-replace helper. Failure removes temporary
rows; existing candidates survive.

`statement_id` identifies an observation by metadata URL, decoded input hash and
source record index. `source_record_id` retains the archive reader's commit-pinned
locator. `link_id` adds the source link position. Changed captures and archive
commits produce separate observations; no cross-edition deduplication is implied.

Outputs are table directories containing Parquet rows, `etl_receipts.parquet`,
`selection.json` and `build.json`. The build receipt records exact inputs,
processing code/runtime identities, row counts and output hashes. Every output
row has a witness pointing to its source capture and native record position.
A table with no rows still has a schema-bearing empty Parquet subject and a
zero-row entry in `build.json`; release packaging must preserve that distinction.

## Qualification and release

Compare all selected statement JSON with the maintained reader, verify every
bill-link occurrence and foreign key, validate dataset receipts, and record
schema/coverage limits. This proves the selected metadata files. Publisher-wide
coverage, held PDF-body coverage and downstream bill completeness remain separate.

A local candidate does not change a published table. Submit source bindings to
the catalog owner and release candidates to the release owner. Public exports
must exclude local input paths from `selection.json` and `build.json`.

Other retained collections have separate source-native units. SEC, FERC and CFTC
comments are not regulations.gov comments merely because their files contain
comments. EDIS documents are investigation filings; direct Supreme Court term
renders are separate observations from CourtListener opinions. CBO-linked
BILLSTATUS archives belong to the bill source route. Inspector-general metadata
is independent of GAO/CRS and current Oversight.gov pages. See the G execution
ledger's `decisions.md` and `bindings-proposed.json` for current dispositions and
the evidence behind each decision.
