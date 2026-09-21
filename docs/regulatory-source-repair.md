# Correct retained regulatory source rows

The local repair command rereads a selected SpicyDocs source release and corrects
existing Parquet rows. It uses the installed source extractors and the host's
existing merge and comment-partition paths. This recovers mapped facts such as
attachment URLs, Federal Register references, withdrawal facts and RINs when
the publisher's modification date has not changed.

Ordinary acquisition remembers processed object keys. That state cannot detect
an extractor correction. The repair command bypasses that acquisition state for
its explicit input; it neither clears nor advances the acquisition manifest.
Each invocation rereads the selected retained input, including after a refusal.
It does not acquire source records, fetch remote priors, update Iceberg, upload
files or publish a generation.

Prepare the intended prior Parquet files in a local output directory, then run:

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

Comments use the existing local partition layout and index. Missing partition
coordinates refuse; a correction that would move a retained identity to another
partition requires a coherent rebuild. Each file replacement is atomic, but the
local multi-file operation is not a transaction. If a later file fails, rerun
the same retained input before admitting or publishing the candidate. No success
checkpoint can suppress that retry.

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
omissions without claiming a full public rebuild. Iceberg's strictly-newer
upsert policy remains a separate recovery boundary.
