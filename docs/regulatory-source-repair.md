# Correct retained regulatory source rows

The repair command rereads a selected SpicyDocs source release and corrects
existing rows: local Parquet for dockets and documents, the Iceberg catalog for
comments. It uses the installed source extractors and one shared merge rule.
This recovers mapped facts such as
attachment URLs, Federal Register references, withdrawal facts and RINs when
the publisher's modification date has not changed.

Ordinary acquisition remembers processed object keys. That state cannot detect
an extractor correction. The repair command bypasses that acquisition state for
its explicit input; it neither clears nor advances the acquisition manifest.
Each invocation rereads the selected retained input, including after a refusal.
It does not acquire source records, upload files or publish a generation.

For dockets and documents, prepare the intended prior Parquet files in a local
output directory, then run:

```sh
uv run --frozen python -m spicy_regs.pipelines.repair_regulations \
  --table documents \
  --release /retained/current-source-release \
  --blob-store /retained/source-blobs \
  --logical-id '<independently accepted source logical ID>' \
  --artifact-digest '<independently accepted source digest>' \
  --accepted-verifier-implementation-id '<trusted source producer implementation>' \
  --output-dir /local/candidate
```

Supported tables are `dockets`, `documents` and `comments`. SpicyDocs performs
source-release admission and checks the caller's pin and accepted verifier.
An unsupported legacy release or a release with unresolved records refuses.
Legacy evidence must be replayed through its owner's supported publisher and
retained with both old and new pins; the host does not bypass admission.

Every source row is staged before merging. Newer prior observations survive;
the fresh source mapping wins at an equal timestamp, including equivalent time
zone spellings. A fresh NULL clears the old mapped value. Independently produced
`text_content`, `text_extraction_status` and `pdf_extraction_results_json` survive
when the source reread supplies no replacement. These retained enrichment values
are not proof that an attachment body was reacquired or revalidated.

Invalid non-NULL comparison dates, duplicate input identities, unreadable
priors and incomplete input reads refuse. An undated fresh row cannot displace
a dated prior; two undated rows allow the explicit correction. Unrelated rows survive. An accepted
empty input clears no rows: this operation repairs the stated identities and
does not interpret omission from a source release as deletion.

For a few already captured objects, the `repair_records` Python API also accepts
records from the installed Mirrulations raw reader. Retain exact object paths,
digests and source metadata, use `fail_fast=True`, and check unresolved outcomes.
That route proves only its named records; it must not be described as an
agency-complete source release.

The September 21 recovery receipt under
`receipts/remaining-gaps-wave1-2026-09-21/sr2/` replays the original ACF evidence
ZIPs into current immutable releases and checks 391 dockets, 546 documents and
three separately pinned comments. Its 10,969 mapped-field comparisons have no
differences in the source-only outputs. The independently timed public-prior
repair preserves four newer public observations and recovers the measured
omissions without claiming a full public rebuild. Its three comments went
through the local partition path that decision 40 retired; comments now repair
in the catalog. The ETL's own upsert still replaces a catalog row only when the
source date is strictly newer.

## Comments

Comments live in the Iceberg catalog, so `--table comments` corrects the catalog
instead of local files (decision 70). The same admission, pins, staging and
merge rule apply. Dockets and documents are unchanged.

The command reads the catalog's current snapshot once. It reads the prior rows
for the staged identities at that snapshot and merges them with the rule above.
By default it is a dry run. It writes `comments-repair.json` to `--output-dir`
and touches nothing remote. The receipt holds the source pins, the snapshot,
the staged identities, any identity the catalog lacks, each changed cell before
and after, and the rows it would write. A catalog that holds a staged identity
twice refuses, like any duplicate prior; run the catalog dedupe first.

Review the receipt, then run again with `--apply`, and with
`--expected-snapshot` set to the receipt's `catalog_snapshot.snapshot_id`. The
apply refuses in three cases:

- the catalog is not at the reviewed snapshot;
- the catalog moved between the read and the write;
- a staged identity is missing from the catalog, because the repair corrects
  rows and never inserts them.

It then replaces only the changed rows, by `comment_id`, with the same DELETE
and INSERT the ETL uses. It checks that each repaired identity now appears
once, as written. If a DELETE left the old row behind, it fails loudly. Then run
the catalog dedupe and rerun the repair: at an equal date the dedupe may keep
either row. The receipt records the new snapshot.

Run it outside the ETL's schedule. The repair does not hold the
`comments-catalog-write` lock; the snapshot checks only detect a writer that
lands before the replace. Reading the priors scans the unpartitioned table once.

The per-agency mirror and `comments.parquet` are not touched. The normal mirror
job publishes the corrected rows from the next catalog snapshot.
