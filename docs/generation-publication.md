# Publish complete table generations

Rollups now retain a complete local generation and, when upload is enabled,
publish it through `publication.json`. Readers that understand this index resolve
its immutable table paths once per operation. Existing bare Parquet URLs remain
legacy, unversioned data; this change does not rewrite or certify them.

## What is checked

A rollup must return every declared output, including a real zero-row Parquet
file for successful empty results. Admission checks the exact member set,
expected schemas where declared, every data page through a bounded full-column
read, row counts, and every byte digest. It uses the installed Rulespec artifact
library for membership, canonical identity, manifests and verification.

The resulting artifact records the host implementation digest, installed package
versions, the captured publication index, and any unchanged siblings carried
forward by a partial writer. It does not establish source completeness, common
publisher timestamps, correct interpretation, or model qualification. The
captured index identifies managed table inputs, not all original source requests.

When `R2_PUBLIC_URL` is configured, each run uses a new retained directory under
`output/.builds/`. Old local priors therefore cannot bypass the captured index.
Offline runs without a public URL can still use explicitly retained local inputs;
they produce local candidates and cannot publish. Completed artifacts live under
`output/generations/<artifact-digest>/`.

## Publication and failure

All shrink guards run before any upload. Objects are created conditionally under
`generations/<family>/<artifact-digest>/`. The publisher rereads and verifies all
remote bytes before conditionally replacing the small publication index. A
concurrent change refuses the update; rebuild from a fresh index before retrying.
A crash before the pointer update can leave unreferenced immutable objects but
keeps the previous published family intact.
If the connection fails after submitting the pointer update, reread the index:
the complete new family may already be current even though the caller saw an error.

A family cannot silently change its table membership or take another family's
table. Those changes need an explicit migration. Successful empty tables still
face the existing shrink guard; an intentional destructive replacement requires
the existing explicit override after reviewing the result.

The Congress.gov archive walker is a partial writer of `bill-family`. Its bill
update preserves the existing wider columns using the existing merge. It copies
all sibling tables byte-for-byte from the captured complete family and records
which generation supplied them. These siblings are carried forward, not
reprocessed. A public cold start must first produce a complete `bill-family`
generation; a lone bill file cannot bootstrap a complete family.
An offline cold-start partial candidate is marked `local-partial`; the generic
publisher refuses it too. Merely retaining a valid Parquet artifact does not
promote that partial result to a complete family.

## Regulatory base tables

The ETL rewrites the bare `dockets.parquet` and `documents.parquet` after every
sweep batch and primes each run from those bare objects
(`r2.download_working_copy`), never from a managed family. Once a sweep
completes, the regulatory refresh publishes each as its own family,
`dockets` and `documents` (`run-rollup-dockets`, `run-rollup-documents`), from
the same working copy, refusing a null or repeated identity. It does this
before the comments mirror captures base versions, so every dependent reads
that sweep's snapshot, and `check_refresh_inputs.py` holds both family pins
along with the bare objects' ETags. Priming from a family instead would drop
the batches of a partly completed sweep whose keys the manifest had already
retired. Bare-URL readers, such as notebooks and the browser, keep reading
the working copies, which change batch by batch as before.

## Readers

- Rollup reads share one captured index. Managed download failures and digest
  mismatches abort; only an index 404 permits legacy resolution.
- MCP captures one index per cached connection, creates managed views at immutable
  URLs, checks their schemas, and refuses a connection if a managed member is
  missing. `list_sources` distinguishes actual available tables from declarations
  and labels managed generations versus legacy data. Query responses include the
  connection's publication pins. The remote query reader relies on immutability;
  it does not rehash whole tables for each query.
- CLI download accepts rollup names. When the requested set includes managed data,
  it downloads the requested set into one batch, verifies every managed member,
  records the index and selected keys, then switches `current` only on success.
  Local reads resolve this link once per command. Requested legacy members remain
  explicitly unversioned even when downloaded in that batch.
- Local MCP accepts that download root, its `current` link, or a specific batch
  directory through `SPICY_REGS_DATA_DIR`. A connection selects the batch once,
  rehashes managed members and checks their schemas. It exposes only the selected
  tables and reports their pins as `managed_download`; loose Parquet directories
  remain `local_unversioned`. File-change checks around tool statements refuse
  ordinary replacement or mutation after verification. A new `current` target
  takes effect when the cached connection rebuilds. These checks do not make
  writable local storage immutable or establish the source's completeness.
- Dictionary remote schema discovery captures the same index. Declared schema
  pages alone continue to make no claim of production availability.

## Rollout limits

The offline object-store tests exercise conditional creation, interrupted
uploads, concurrent index changes, byte corruption and stale inputs. They are
not a live R2 deployment rehearsal. Validate these operations in a disposable
bucket before enabling the new writer in production. Partitioned comments,
Iceberg publication and the browser's non-table `docket_search.json.gz` object
are outside this table-generation path; base regulations.gov dockets and
documents joined it as families (above). Historical rebuilds, semantic
qualification and public adoption remain separate work. No publication index is
created remotely by the test suite.
