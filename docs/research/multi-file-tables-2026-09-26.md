# Tables stored as several files (design draft, 2026-09-26)

Status: **draft for review** by spicy-stack-24 (publication format) and spicy-stack-83 (DocSpec, Engine).
Nothing in `sources/publication.py` or any reader changes until both agree.

Owner decision (2026-09-26): design multi-file tables for every large table first, as one cross-repository
decision, then apply it to `bill_sections`. Decisions 46 (split `fcc_filings` by year before it passes
~500 MB), 51 (FEC individual contributions, 2024 and 2026 only, after tables can be several files) and
36 (keep pinned generations plus the last N) depend on it.

## 1. What forces this, measured

`bill_sections` is not the forcing case. Bill-family run 36256715125 (2026-09-26, 1.9M `bill_sections`
rows, 685 MB) spent 13 s downloading the prior table, 39 s merging it and about 2 min publishing the
whole family, evidence included. The whole-table rewrite therefore costs about 2 minutes and 2 GB of
transfer per run. Storage per generation is bounded once decision 36's retention runs.

Three cases do need several files:

| Case | Size now | Why one file fails |
| --- | --- | --- |
| `court_opinion_clusters` | 3.95 GB, 10.1M rows, weekly | S3's single-request copy and PUT stop at 5 GiB. `_copy_unchanged_member` (`publication.py:309-319`) is one `copy_object`. R2's own cap is **unverified**. The table grows weekly. |
| FEC individual contributions (decision 51) | not yet built; "very large" per the decision | The owner deferred it until multi-file tables exist. |
| `fcc_filings` (decision 46) | growing by received-year backfill | The owner set a split by year before ~500 MB. |

Also: `fec_source_records` is 1.17 GB and the court citation tables are 0.16–0.44 GB each. The court
tables are rebuilt whole from each dump, so splitting helps readers there, not rewrites.

## 2. Constraints from the reviewers

- **24:** the generation and admission model is one member per table today. The least-change design is
  several members per table plus a table-level entry in the generation's index and root, not a new
  object layout. Verification stays single-pass (`operations.md` §1b: collapse the triple local
  verification; don't add a per-member pass).
- **83 (DocSpec 0.11.2, decision 0007, C27 step 2):**
  - each member is staged and hard-linked without rewriting, and its sealed digest equals the
    producer's member digest;
  - no Iceberg field IDs in the Parquet footer, and no row group over 256 MiB uncompressed;
  - one Iceberg table per logical table (`add_files` already takes a list);
  - row identity from the contract's declared key across all files, so a duplicate across files refuses;
  - the descriptor names every member with its digest and row count, so `count(*)` checks per table;
  - partition values inside the Parquet are preferred over file-name convention alone.

## 3. What assumes one file per table today

From a read-only survey (spicy-regs `2716473`, DocSpec 0.11.2, spicyengine `96d0914`,
spicysearch `0dc4d0a`):

- **The index is parsed strictly everywhere.**
  - spicy-regs `parse_index` (`sources/publication.py:79-80, 96-115`) requires `version == 1`, a
    `<name>.parquet` table key, and exactly `{sha256, byteSize, rows, columns}` per table.
  - DocSpec `adapters/generation_source.py:67, 205-212` does the same.
  - A reader that meets one new field refuses the **whole** index, not just that table. Deployed MCP
    containers and installed CLIs would stop working.
- **spicy-regs producer side:**
  - `generations.py:113-122, 196-199`: member = file name = table;
  - `remote_generations.py:51-54`: staged key = `<prefix>/<name>.parquet`;
  - `pipelines/rollups/base.py:181-190, 236-258`: one download and one parent digest per table;
  - `publication.py:555-570`: shrink guard per member key; line 569 refuses any change to a family's
    key set.
- **spicy-regs consumers and checks:**
  - `sources/r2.py:31-88` (one URL, digest-checked);
  - `transforms/table_merge.py:104-131, 213-225` (one prior in, one file out);
  - `build_bill_family.py:536-550, 595-605, 997-1024` (whole-table prior reads);
  - `mcp_server.py:411-446`, `cli.py:65-128` and `local_data.py:69-106`;
  - `generation_audit.py:309, 417-424, 980-985, 1049-1086`;
  - `scripts/check_table_joins.py`, `check_rollup_freshness.py:145-165` and
    `check_source_domain_drift.py`;
  - `data_dictionary.py:764-794` (`spicy-regs-dict check --source r2`).
  - Pin-level checks need no change: `check_ledger_pins`, `check_refresh_inputs` and
    `check_source_refusals`.
- **DocSpec:**
  - reading already handles many files: `iceberg_scan` in `adapters/storage/records.py:463-465`;
  - registering handles one: `register_parquet` (`records.py:1050-1110`), a single `memberDigest`
    (707) and a `files == [member_digest]` check (441-445);
  - `AdmittedGeneration` and `stage_generation` (`generation_source.py`) return one `path` and one
    `member`, and `admit_generation` (`application/generation_admission.py`) passes that one member to
    registration;
  - `generation_source.py:205-212` also requires each member's `record_count` to equal its table's
    `rows`, and `set(tables) == set(members)`;
  - staging already accepts nested keys (`generation_source.py:152, 213`);
  - everything downstream of the Iceberg table (identity minting, the later-generation join,
    membership, the occurrence index, `table()`, Engine) runs over the table relation and doesn't
    depend on how many files there are (confirmed by 83).
- **No direct readers:** spicyengine and spicysearch read only DocSpec states and prepared layers
  (`iceberg_scan`, any number of files). RefSpec, rulespec and spicyregs-web read no spicy-regs table.
- **Not verifiable here:** `spicy-regs-ui` reads bare legacy URLs (`table_metadata.json:160`).
- **Prior art to reuse:**
  - spicy-docs `public_tables` already names multi-member tables `data/<col>=<value>/part-NNNNNN.parquet`
    and declares `partitionColumns` and `maxRowsPerMember` (`public_tables/format.py:22-55, 160-206`).
    Its reader passes the member list to one `from_parquet` (`reader.py:194-207`).
  - The comments mirror (`comments/agency/agency_code=<X>/part-0.parquet`) partitions **outside** the
    generation format, overwriting fixed keys non-atomically (`comments_mirror.py:113`).

## 4. Proposal

### 4.1 Index: publish a second index, keep the first

Put multi-member tables in a new `publication.v2.json` and leave `publication.json` (version 1) exactly
as it is for every single-member table. So:

- every existing reader keeps working unchanged, including deployed MCP containers, installed CLIs and
  DocSpec 0.11.2;
- a reader that needs a split table moves to v2 when it is ready, with no stack-wide cutover day;
- v2 is the source of truth; v1 is a derived view written by the same publish;
- once no reader needs v1, it is retired in its own later change.

A v2 table entry keeps the v1 fields at table level and adds its members:

```json
"bill_sections": {
  "rows": 1903728, "byteSize": 684986563, "columns": [...],
  "partitionColumns": ["congress"],
  "members": [
    {"key": "bill_sections/congress=119/part-000000.parquet",
     "sha256": "sha256:…", "byteSize": 146203311, "rows": 453112, "partition": {"congress": "119"}}
  ]
}
```

A single-member table in v2 has exactly one member, and it is the same object as in v1. Each v2 family
entry carries what DocSpec pins on, as v1 does: `logicalId`, `artifactDigest` and `publicationStatus`.
A member's `key` is relative to the generation prefix, so a carried-forward member keeps the same `key`
and `sha256` from one generation to the next (§4.3 relies on this).

**Write order (24's answer).** v2 is written first, under its own conditional write (CAS); then v1,
under its own. v1 is always `derive_v1(v2)`, a pure function of v2, and is never merged on its own.
- A failed or raced v1 write is repaired by deriving it again from the current v2. A reader of a
  stale v1 sees an older but consistent state, never a mix.
- Concurrent writers are serialized by v2's CAS alone.
- The pointer-retry merge (`_merge_family`, `_assert_family_unchanged`) moves to v2 unchanged.

**A family with a split table in v1** is listed with its single-member tables only, and the split
table is omitted. DocSpec 0.11.2 checks `set(tables) == set(members)`, so it would refuse such a
family through v1. DocSpec must therefore read v2 for any family with a split table **before** that
family's first split publishes (§4.5 step 2). None of the families DocSpec admits today
(`federal_register`, `dockets`, `documents`) is proposed for splitting.

**Table keys** keep the `<name>.parquet` spelling in v2 (24's answer). One key string across both
indexes gives `table_members(index, key)` a single argument domain, and no reader maps between
spellings.

### 4.2 Members, and the partition column

- Member keys follow `public_tables`: `generations/<family>/<digest>/<table>/<col>=<value>/part-NNNNNN.parquet`.
  `NNNNNN` numbers the files within a partition when one would pass `maxRowsPerMember` or a byte cap.
  A proposed cap is 1 GiB per member, well under any single-request limit.
- **The partition value must be a function of the table's identity key** (24). Then a row can live in
  only one partition. Identity uniqueness across members reduces to uniqueness within each member,
  and carry-forward can never keep a stale copy of a row in an untouched partition while its fresh
  copy lands in another.
  - `bill_sections` satisfies it: `congress` is the prefix of `bill_id`.
  - `court_opinion_clusters` partitioned by `date_filed` would **not**: its identity is
    `cluster_id`, and a cluster whose `date_filed` changes would sit in two partitions. It is
    partitioned by a `cluster_id` range instead (§4.6).
  - The alternative, re-merging every partition that holds a prior copy of a fresh row, costs a
    lookup of every fresh identity against every partition. It is not proposed.
- The partition column must be a **declared column** of the table, stored inside the Parquet (83's
  preference). Readers read with `hive_partitioning=false`, so no synthetic column appears and
  `DESCRIBE` still equals the declared columns (the MCP at `mcp_server.py:438-442` and the freshness
  check at 153-155 compare them).
  - The bill tables have no `congress` column; it is the prefix of `bill_id`. So `bill_sections`
    (and later `section_diff_items` and `bill_publisher_summaries`) gains a declared `congress` column
    in its spicy-docs contract, derived from `bill_id`. The identity stays `bill_id`-based. An identity
    that included the new column would make DocSpec re-mint every row.
- Every member keeps DocSpec's limits: no Iceberg field IDs, row groups ≤ 256 MiB uncompressed. A
  1 GiB member cap is within DocSpec's exemption, since referenced files skip `max_member_bytes`.
- The table's declared identity must be unique across members. That costs nothing extra: DocSpec's
  identity pass is already one `GROUP BY` over the whole table relation, which becomes one Iceberg
  table over all members. No per-member pass is added.

### 4.3 Building: touch only the partitions that changed

- `merge_table` is applied per partition. A partition is re-merged only if it has fresh rows or a
  `replace_parents` scope. Every other partition is carried forward by its prior member digest, the
  way `carriedForward` already works per table (`base.py:181-190`).
- So a nightly bill-family run, which reads only the sitting Congress, rewrites one partition
  (~453k rows of 1.9M today) and copies the rest server-side.
- **Byte-identical output from an unchanged merge is not required**, because an untouched partition
  is not re-merged at all. (The survey found that byte-determinism of the merge is unmeasured.)
- **DocSpec gains the same saving.** When a later generation's member has the same `key` and `sha256`
  as the prior generation's, DocSpec admission can skip its all-column comparison for that partition
  and compare only the changed members' rows. Today that comparison runs over the whole table at
  about 2.6 s per million rows, roughly half a minute for `court_opinion_clusters`' 10M rows. Per
  partition, admission becomes O(changed partitions): seconds.
- The bill family's whole-table reads of `bill_sections` (`build_bill_family.py:595-605, 997-1024`)
  move to projected remote reads over the member list, as `remote_inputs` already does
  (`base.py:68-71, 261-277`).

### 4.4 Publishing and verification: single pass

- `_copy_unchanged_member` decides per member by the prior member's digest, read from v2.
- **Storage is not saved.** Carry-forward still copies every unchanged member server-side into each
  new generation, so storage per generation stays O(family bytes): about 4 GB of copies a week for
  the court family. That is fine under decision 36's retention. The savings are in transfer, merge
  time and admission, not storage.
- **Guards (24):**
  - The key-set guard (`publication.py:569` refuses any change to a family's key set) becomes: the
    family's **table** set stays fixed, and a declared partitioned table may **add** members (a new
    Congress, a new FCC year).
  - A member that disappears is a retirement, journaled like `rows-retired`.
  - The shrink guard runs **per table**, on the sum of member rows, not per member key, since one
    partition can legitimately shrink, for example after a retirement.
- Local verification happens **once** per member, following `operations.md` §1b(b): `_run_tables`
  passes its verified artifact to `_publish_verified_generation` instead of re-verifying.
- **Admission read-back.** `admit_artifact` still GETs every member, copies included
  (`publication.py:611`). That is O(family bytes), about 1 min for 0.7 GB, which is acceptable in
  phase 1.
  - Trusting a server-side copy by size and ETag, as `_publish_evidence` already does for evidence
    blobs (478-488), is phase 2. It needs rulespec_artifacts support and its own measurement, and
    it weakens what admission proves. It is **not** proposed here.
- Retention (decision 36) counts generations, not members. Each generation keeps its own copied
  members, so deleting a prefix never breaks another generation, and no reference counting is needed.

### 4.5 Readers, in order

1. **spicy-regs.** One `table_members(index, table)` resolver replaces `table_location`, returning
   `[(url, sha256, rows)]` with one element for a v1 table. `r2.download`, the MCP views
   (`read_parquet([...])`), the CLI, `generation_audit`, the nightly checks (including
   `check_table_joins`' `table_urls`, which builds one URL per table today) and the dictionary check
   all go through it. Ships in one spicy-regs release **before** any table is published split.
2. **DocSpec** (83):
   - `stage_generation` and `admit_generation` carry a member list;
   - `register_parquet` registers one Iceberg table over it (`add_files` over every member);
   - the row check changes from one equality to a sum: each member's footer rows equal its entry, and
     the entries sum to the table's `rows`;
   - `verify` compares the Iceberg data-file digests with the member digests as sorted sets, since the
     catalog's file order is not the descriptor's;
   - the sealed layer records every file's digest, as it does today, and nothing else in the seal
     changes;
   - it reads v2 only for families that have a split table.
3. **Other readers:** spicy-docs `check_source_domain_drift.py` (bare URLs) and the spicyregs plugin
   script (`query_spicy_regs.py`) either resolve through v2 or keep reading v1 tables only.
   `spicy-regs-ui` is outside this workspace; it reads legacy bare URLs and is unaffected.

### 4.6 Which tables, in which order

1. **`bill_sections`** first, as the owner chose: it has the simplest partition (congress), a
   nightly delta confined to one partition, and no external reader beyond spicy-regs itself.
2. **`court_opinion_clusters`**, by `cluster_id` range, before it reaches the single-object cap.
   `date_filed` (39 decades, at most 1.71M rows each) would read better but is not a function of
   the identity (§4.2). The range width is chosen to keep each member under the byte cap.
3. **`fcc_filings`** by received year (decision 46), when it approaches 500 MB.
4. **FEC individual contributions** (decision 51), born split, by cycle, then month or committee.
5. **Comments:** folding the agency mirror into generations would make it atomic, but would cost an
   8.5 GB read-back a day unless phase-2 copy trust exists. Not proposed now. DocSpec's admission of
   `comments` (182 agency files) waits on this design: once the mirror folds in, the v2 member list
   is what makes it admissible.

## Review record

- **spicy-stack-83 (DocSpec 0.11.2, Engine 0.9.1), 2026-09-26:** the shape works for DocSpec as drafted.
  Four corrections and additions were folded in: §3's DocSpec single-file spots, §4.1's family pin
  fields, §4.2's identity rule and existing uniqueness pass, §4.3's per-partition admission saving,
  and §4.5's sorted-set `verify`. It agrees with the 1 GiB member cap, the ≤ 256 MiB row groups, the
  phase-1 O(family) read-back ("documents' 77 MB took 5.4 s"), and not trusting size and ETag in
  admission. Engine needs nothing.
- **spicy-stack-24 (publication format), 2026-09-26:** the direction is right. Folded in:
  - §4.1: v2 is written first and v1 derived from it; a split table is omitted from v1; keys keep
    `<name>.parquet`;
  - §4.2: the partition value is a function of the identity (so court clusters go by `cluster_id`
    range);
  - §4.4: the key-set guard admits added members and journals vanished ones as retirements, the
    shrink guard runs per table, and storage stays O(family bytes);
  - §4.5: the resolver serves `check_table_joins`;
  - §5: a real 1.1 GiB `CopyObject` measurement.
- **Pending:** 83's confirmation that DocSpec reads v2 before any family it admits gets a split table
  (§4.1).

## 5. Open measurements before code

- R2's actual single-request `CopyObject` and PUT limits, measured with a real 1.1 GiB `CopyObject`
  before the 1 GiB member cap is fixed (it also decides how urgent `court_opinion_clusters` is).
- The share of `bill_sections` bytes the nightly run leaves unchanged once split. Estimate: 1.45 GB of
  1.9 GB after the 113th–114th land, since only the 119th changes.
- Admission read-back time for a 0.9 GB split family on the runner (phase-1 acceptability).
- Whether DuckDB `read_parquet([...])` over HTTP with about 7 members keeps MCP query latency within
  today's single-file numbers.

## 6. What this does not change

- Single-member tables: same keys, same v1 entries, same digests, same DocSpec admission.
- Generation identity, family pins, the output ledger's `qualified at` pins, and
  `check_ledger_pins` / `check_source_refusals`.
- The comments mirror.
