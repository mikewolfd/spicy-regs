# Run FEC catalog and retained-input builds

The `rollup-fec-source-catalog.yml` and `rollup-fec-observations.yml` workflows
use the existing rollup builders and generation publisher. Both are manual and
default to `skip_upload: true`. The catalog reads the pinned provider inventory;
it needs no FEC acquisition. The observation workflow consumes an explicit,
already retained selection. Neither workflow discovers or downloads new FEC
source data.

For data already on the current machine, keep using the direct invocation:

```sh
uv run --frozen build-fec-observations \
  --manifest /path/to/retained-inputs.json --output-dir /path/to/output
uv run --frozen run-rollup-fec-source-catalog --output-dir /path/to/catalog-output
```

The [input manifest documentation](fec-relationships.md#build-selected-retained-inputs)
defines supported release, API capture, original/ZIP-member and field-dictionary
inputs. Already sealed, qualified generations can go directly through the
[existing publisher](generation-publication.md); they do not need rebuilding or
transfer through this input workflow.

## Prepare an explicit portable selection

GitHub-hosted runners cannot read local `/Users/...` paths. Stage a fresh
directory containing only the selected inputs, with this layout:

```text
selected-inputs/
  source-manifest.json       # exact original manifest bytes
  manifest.json              # same selection, with relative filesystem paths
  blobs/sha256/<digest>      # selected capture/dictionary/source evidence bytes
  releases/<selection>/...   # complete selected immutable releases, if used
```

Copy the original manifest byte for byte into `source-manifest.json`. In
`manifest.json`, change only collection `blob_root`, `release_path` and optional
`field_mapping.dictionary.blob_root` paths to directories inside this bundle.
Use ordinary files and normalized relative POSIX paths. Preserve collection
order, acquisition URLs/timestamps, original byte sizes and digests, query scope,
ZIP-member choices, verifier IDs, release pins and field mappings exactly.

Copy only the referenced originals and dictionaries. For a source release,
include its complete declared members and referenced blob evidence, including
acquisition-ledger/evidence partitions. Copying only `artifact.json` or the
release directory without its blob store is insufficient. There is no automatic
corpus export or upload command in this workflow; the operator must stage and
retain this exact selection. Never copy an entire unrelated corpus to make a
missing reference disappear. The provider's existing reader verifies release
membership and replays source evidence during the build.

Create the archive from inside that prepared directory, naming its selected
entries explicitly. For example, if it uses the layout above:

```sh
tar -C selected-inputs -czf selected-inputs.tar.gz \
  source-manifest.json manifest.json blobs releases
shasum -a 256 selected-inputs.tar.gz selected-inputs/manifest.json
```

Omit entries that the selection does not use. The workflow accepts a tar or
compressed tar. It rejects symbolic/hard links, special files, sparse files,
duplicate paths, absolute paths, parent traversal and existing output targets.
It streams both hashing and extraction. The defaults allow at most 4 GB of
archive bytes, 8 GB of extracted bytes, 100,000 archive entries and 16 MiB per
input manifest. Transfer and extraction byte limits
are explicit dispatch inputs; make sure the runner can also hold temporary
source decoding, output tables and the sealed generation. Large existing FEC
seeds may fit better in the direct local build/publish path.

Validate the transfer locally before making it available to a runner:

```sh
uv run --frozen python scripts/prepare_fec_retained_inputs.py \
  --archive selected-inputs.tar.gz \
  --archive-sha256 <archive-sha256> --manifest-sha256 <manifest-sha256> \
  --output-dir /fresh/path/runner-inputs --audit-dir /fresh/path/input-audit
uv run --frozen build-fec-observations \
  --manifest /fresh/path/runner-inputs/manifest.json \
  --output-dir /fresh/path/verified-output
```

Both digests are required; bare lowercase hexadecimal and `sha256:` prefixes are
accepted. A successful transfer check establishes archive integrity and path-only
relocation. It does not claim source verification: the subsequent builder must
verify every selected source and complete all outputs before publication.
Changed acquisition facts, missing blobs and mismatched release pins refuse the
process. Earlier output generations remain intact.

## Transfer, build and retain evidence

Place the exact archive at an operator-controlled HTTPS URL, retaining it for
replay. This storage/transfer setup is an explicit operations prerequisite; the
workflow does not create it. Dispatch `rollup-fec-observations.yml` with that URL,
its `inputs_sha256`, the portable `manifest_sha256`, byte bounds and an appropriate
timeout. HTTPS redirects remain HTTPS. Download failures and integrity failures
stop before the rollup runs. The archive URL is excluded from the audit file
because temporary access URLs can contain credentials; the digest identifies
the transferred bytes.

Keep `skip_upload: true` for a build-only run. A successful run retains its
`output/` directory as an Actions artifact for 30 days. This includes the sealed
`generations/<digest>/artifact.json`, member manifest and Parquet tables, plus
available build/audit files. Failed runs also retain available outputs. Every
run retains `invocation.json` with code revision, run identity and explicit scope;
retained-input runs additionally preserve:

- `fec-inputs/source-manifest.json`: unchanged original manifest bytes.
- `fec-inputs/manifest.json`: exact portable manifest bytes.
- `fec-inputs/relocations.json`: original and portable paths by JSON Pointer.
- `fec-inputs/transfer.json`: archive/manifest pins, byte/member counts, verification
  status and transfer-check time; a refusal records its reason.

The transfer timestamp is separate from source `observedAt` values. Raw inputs
remain in the operator's retained archive; they are not uploaded a second time
as generated Actions outputs. Keep that archive beyond the Actions retention
window. Logs establish a network transfer failure before a transfer receipt
exists; the invocation and expected pins still survive.

After checking selected raw/output pairs and the complete generation, an explicit
`skip_upload: false` run uses the normal R2 credentials, shrink checks, remote
byte verification and conditional publication-index update. The two FEC workflows
serialize their own family runs without cancelling an active build. The existing
publisher also protects publication-index races with other families. A successful
workflow does not by itself prove complete historical coverage or consumer access.

The small checked-in `tests/fixtures/fec-retained-inputs` selection contains a real
retained candidate release and an official candidate/committee-linkage header
capture. Tests exercise transfer, unchanged acquisition facts and complete local
generation alongside unsafe-path, pin, source-member and extraction-limit failures.
