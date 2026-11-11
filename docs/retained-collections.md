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

## Refresh from the archive

`spicy_regs.pipelines.administration_policy` is the maintained caller around
that builder. `acquire` reads each selected administration's metadata file at
one full archive commit through the `CaptureEvidence` transport, so every file
lands in the central raw archive with a receipt, and writes a capture
selection (commit, URL, SHA-256, size and observation time per file). Given
the published statements table, it reads every administration of that table
too and compares bytes: an unchanged file keeps its published capture, so its
`statement_id` values stay as published, and a changed file takes the new
commit's capture in its place. A new administration file is named explicitly.

```sh
python -m spicy_regs.pipelines.administration_policy acquire \
  --commit "$ARCHIVE_COMMIT" --prior-statements "$PUBLISHED/administration_policy_statements.parquet" \
  --output-dir "$RUN_DIR" --captures-out captures.json
python -m spicy_regs.pipelines.administration_policy build \
  --captures captures.json --output-dir "$POLICY_OUTPUT"
```

`build` writes the builder manifest over the raw originals, builds the
candidate in `$POLICY_OUTPUT/candidate` and admits its tables and receipts as
`$POLICY_OUTPUT/generation`. The generation pins a source-evidence artifact
that names each selected file's commit, URL, digest and observation time and
holds its bytes, as the other source families' evidence does. The archive
states no licence; the October 2026 generation's evidence carried digests
only. The refresh sequence through publication is in
[local-family-refresh.md](local-family-refresh.md).

## Qualification and release

Compare all selected statement JSON with the maintained reader, verify every
bill-link occurrence and foreign key, validate dataset receipts, and record
schema/coverage limits. This proves the selected metadata files. Publisher-wide
coverage, held PDF-body coverage and downstream bill completeness remain separate.

A local candidate does not change a published table. Submit source bindings to
the catalog owner and release candidates to the release owner. Public exports
must exclude local input paths from `selection.json` and `build.json`.

## Unpublished sample families

Six finite samples built from retained captures were published once on
2026-10-13 and taken out of the public index on 2026-11-11 (owner decision,
ticket ss-i1dy): `retained-sec-comments` (`882e25ee81465719`),
`retained-ferc-originals` (`ced75d6c013eb63f`), `retained-cftc-comments`
(`9365d7578737d906`), `retained-edis-filings` (`2ca35ac62c13af40`),
`retained-supreme-court` (`1b51fb35e0bf2aa5`) and `retained-inspectors-general`
(`a1bf8f85c1b02528`): fifteen tables, 718 rows. Their builder
(`sources/retained_native_collections.py`), publication wrapper, policies and
dictionary entries were removed with them. The generations stay stored under
`generations/<family>/<digest>/`; `publication.restore_family` puts one back
from its saved index entry. The SpicyDocs readers they used remain in SpicyDocs.
The raw captures stay in the raw archive, and the exact finite selections and
their qualification stay in the canonical corpus under
`metadata/source-catalog-program/20261010/execution-20261012/G/`.
