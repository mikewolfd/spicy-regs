# Court data coverage — what was captured, what was not, 2026-08-22

**Verdict: the opinion-text seam is closed and the decision-to-docket join now
exists. Opinion *text* coverage is a bounded slice, not a backfill, and the two
things standing between here and full coverage are 8.6 hours of single-threaded
bandwidth and a 100 GiB free-space floor — not missing code.**

This is the verification section the court-data expansion owes. Every number is
measured, and each check names the denominator it was measured against, because
a coverage claim is worth exactly what its denominator is worth.

## Where the denominators come from

| Question | Denominator | Authority |
|---|---|---|
| How much bulk data exists? | The bucket's own S3 listing | `storage.courtlistener.com`, captured 2026-08-22 and pinned in DocSpec at `fixtures/courtlistener-bulk-v1/` |
| How many APA suits are there? | `court_dockets` on R2 | itself the complete result of a `nature_of_suit=899` search |
| How many Supreme Court opinions? | the Court's term index pages | `supremecourt.gov/opinions/slipopinion/<term>` |

The publisher's listing is not an index we assembled — it is CourtListener's own
statement of what exists, which is why it is captured verbatim and pinned rather
than summarized. Reproduce with `scripts/verify_court_coverage.py`.

## What the publisher offers

The listing holds **1,076 objects totalling 1,598.65 GiB across 46 datasets**.
The newest dump of every dataset is dated **2026-06-30**. Exact sizes for the
ones that matter here:

| Dataset | Compressed | Ratio | Decompressed (est.) | Single-pass stream |
|---|---:|---:|---:|---:|
| `opinions` | **50.814 GiB** (54,561,543,156 B) | 8.31x | ~422 GiB | **8.6 h** |
| `dockets` | 4.670 GiB (5,014,469,248 B) | 6.08x | ~28 GiB | 46 min |
| `opinion-clusters` | 2.288 GiB (2,457,231,057 B) | 4.48x | ~10 GiB | 23 min |
| `citation-map` | 0.490 GiB | — | — | 5 min |
| `parentheticals` | 0.268 GiB | — | — | 3 min |
| `citations` | 0.119 GiB | 13.72x | ~1.6 GiB | 72 s |
| `courts` | 81,180 B | 9.43x | ~765 KiB | instant |

Ratios are measured on the first megabytes of each dump, not assumed. Throughput
is **1.74–1.79 MiB/s on a single connection**, measured three independent ways
(raw `curl` range read, streamed decompression, and the real ingest). The bucket
does not go faster for one client; the ingest takes that as given rather than
opening parallel connections against a service that gives its data away free.

## The access facts that shaped every bound

Verified 2026-08-22, keyless:

| Endpoint | Status |
|---|---|
| `/api/rest/v4/search/?type=r` (dockets) | **200** |
| `/api/rest/v4/search/?type=o` (opinion clusters) | **200** |
| `/api/rest/v4/search/?type=rd` (RECAP documents) | **200** |
| `/api/rest/v4/courts/` | **200** |
| `/api/rest/v4/opinions/` | **401** |
| `/api/rest/v4/clusters/` | **401** |
| `/api/rest/v4/recap-documents/` | **401** |
| `/api/rest/v4/recap-query/` | **401** |

This corrects a premise the work started from. The REST opinions endpoint does
**not** serve `html_with_citations` / `plain_text` keylessly — it does not answer
keylessly at all. `/search/?type=o` is keyless but returns only a `snippet`, not
a body. **The bulk dumps are therefore the sole keyless source of opinion text**,
which is why the ingest is bulk-first by necessity rather than by preference, and
why the 50.8 GiB figure above governs everything downstream.

No `COURTLISTENER_API_TOKEN` is configured on this machine, so every number here
is a keyless number.

## What was ingested, with exact bounds

Disk headroom at session start was **118 GiB free against a 100 GiB floor** — 18
GiB of usable room, against an `opinions` dump that is 50.8 GiB compressed before
it is decompressed at all. That single comparison decided the shape of the whole
ingest.

| Table | Bound | Result |
|---|---|---|
| `court_opinion_clusters` | **full** `opinion-clusters` dump, 2026-06-30, streamed in 23 min | **10,070,727 rows**, 3.94 GB parquet |
| `court_opinion_bodies` | **250,000 opinions**, read from 1.242 GiB of the 2026-06-30 `opinions` dump | 250,000 rows, 1.74 GB parquet |
| `courts` (reference read) | full dump | 3,361 courts — 397 federal (127 appellate, 125 district, 95 bankruptcy, 42 special), 2,618 state |

The `opinions` bound is the honest one to argue about, so here it is precisely.
250,000 opinions came out of 1.242 GiB of a 50.814 GiB dump, which puts the dump
at roughly **10.23M opinions** and this slice at **2.44%** of it.

The slice is a cross-section, not a prefix: ingested `opinion_id` values run from
**29 to 11,338,571**, essentially the full width of the corpus, because the dump
is not ordered by id. That makes the sample usable and representative. It does
not make it a backfill.

### The full backfill: implemented, costed, not run

`build_court_opinion_bodies` runs unbounded, and `check_headroom` refuses it
before a byte moves when the arithmetic does not work. On this machine it does
not work:

* **Disk.** Landing the compressed dump alone takes 118 GiB free down to 67 GiB,
  below the 100 GiB floor. Streaming avoids landing it, but the output table
  would be roughly 45–55 GiB of parquet at the measured bytes-per-opinion.
* **Time.** 8.6 hours of continuous transfer at the bucket's observed rate.

That is a scheduled-job cost on a machine with room, not a workstation cost. It
is refused here and recorded rather than half-attempted.

## Coverage verification

### 1. Bulk enumeration vs ingested rows

| Dataset | Publisher offers | Ingested | Coverage |
|---|---:|---:|---:|
| `opinion-clusters` 2026-06-30 | 10,070,727 rows (whole file read) | 10,070,727 | **100%** |
| `opinions` 2026-06-30 | ~10.23M rows (derived: 250,000 rows per 1.242 GiB over 50.814 GiB) | 250,000 | **2.44%** |
| `courts` 2026-06-30 | 3,361 rows | 3,361 | **100%** |
| `dockets` 2026-06-30 | not ingested | 0 | 0% — superseded by `court_dockets`, which is scoped to APA suits |

The clusters denominator is the dump's own row count, established by reading the
whole file rather than trusting a published figure, because CourtListener does
not publish row counts.

### 1a. What "has text" actually means

Judging opinion-text coverage by `plain_text` would have been wrong by a factor
of four, which is the single most useful thing this ingest measured:

| Body column | Rows populated (of 250,000) |
|---|---:|
| **any** text rendering | 249,988 (**100.0%**) |
| `html_with_citations` | 249,670 (**99.9%**) |
| `plain_text` | 55,335 (**22.1%**) |

CourtListener stores whichever rendering the upstream source supplied, and
`html_with_citations` is the one it computes for nearly everything. `plain_text`
is populated for under a quarter of opinions. A consumer that treated a null
`plain_text` as "no text available" would discard 78% of a corpus that is in fact
99.9% covered. That is why the table carries `available_text_fields` and
`text_char_count`: they make "the publisher holds no text" distinguishable from
"the publisher holds text, in a different column." The commonest combinations:

| `available_text_fields` | Rows |
|---|---:|
| `html_with_citations,xml_harvard` | 131,824 |
| `plain_text,html_with_citations` | 48,872 |
| `html_anon_2020,html_with_citations` | 28,631 |
| `html_lawbox,html_with_citations,xml_harvard` | 12,991 |

By role, the slice is 120,258 lead opinions, 96,293 combined, 13,290 trial-court,
10,182 dissents and 6,735 concurrences — so separately-authored opinions do come
through as their own rows, which is the grain the table promises.

### 2. APA docket set vs decisions matched

The join this expansion existed to create works, and it is far sparser than the
docket count suggests. Both facts are load-bearing.

| Measure | Count | Share |
|---|---:|---:|
| clusters ingested | 10,070,727 | — |
| clusters carrying a `cl_docket_id` | 10,070,727 | **100%** |
| APA dockets in `court_dockets` | 7,698 | — |
| **APA dockets with at least one decision** | **759** | **9.9%** |
| APA dockets with no decision at all | 6,939 | 90.1% |
| clusters sitting on an APA docket | 1,155 | 0.011% of all clusters |

**90% of the APA docket set has no decision attached, and that is a property of
the upstream data, not of this ingest.** The two halves come from different
places: `court_dockets` is RECAP, sourced from PACER, and records that a suit
exists. Opinion clusters are sourced from court-website scrapers and reporters,
and record that a decision was *published*. A district-court APA challenge that
settled, was voluntarily dismissed, or ended in an unpublished order leaves a
docket and no cluster. So the ceiling here is not something more streaming would
raise.

Two smaller numbers worth stating rather than rounding away:

* **17** of the 250,000 ingested opinion bodies land on an APA docket. That is
  the arithmetic working exactly as it must — APA clusters are 0.011% of all
  clusters, so a 2.44% sample would be expected to catch roughly 28 of the 1,155,
  and it caught 17. It is not a defect; it is what a uniform sample of a corpus
  buys you when the target is one part in ten thousand. **Targeting the APA set
  specifically is a `cluster_ids` filter over the same dump**, which the builder
  already supports — the cost is the full 8.6-hour pass, not new code.
* **237,334** of the 250,000 ingested opinions (94.9%) resolve to a cluster in
  the cluster table; **12,666 (5.1%) do not**, despite both tables coming from
  the same 2026-06-30 cut. Those opinions name a `cluster_id` the clusters dump
  does not contain — most likely blocked or withdrawn clusters that the two
  exports treat differently. Unexplained, and recorded as unexplained.

### 3. Supreme Court term index vs captured opinions

The Court's own index is the denominator:

| Term | Slip opinions on the index |
|---|---:|
| OT2021 | 66 |
| OT2022 | 58 |
| OT2023 | 60 |
| OT2024 | 67 |
| OT2025 | 68 |
| **total OT2021–OT2025** | **319** |

**`court_opinions.parquet` returns 404 from R2.** The SCOTUS ingest exists, is
tested, and has a rollup, a workflow, and a data-dictionary entry — but its table
has never been published. Coverage against those 319 opinions is therefore **0%
published**, and the only copies on this machine are 10-row samples inside sealed
corpus artifacts. This was already visible in the freshness config, where
`court_opinions` sits in `SKIPPED` for a different stated reason ("seasonal"),
which masks the real one.

## Named gaps, and what closing each costs

1. **Opinion text is 2.44% covered.** ~9.98M of ~10.23M opinions unread.
   *Cost:* 8.6 h of single-connection streaming plus ~45–55 GiB of output. Needs
   a machine with ≥160 GiB free, or a partitioned run that publishes per-slice.
   No new code — remove the bound.

2. **`court_opinions` is unpublished.** 319 Supreme Court opinions across
   OT2021–OT2025 exist upstream and zero are published.
   *Cost:* one `run-rollup-supreme-court-opinions --no-skip-upload`. Minutes. The
   `SKIPPED` reason should then be corrected, because "seasonal" is not why the
   table is absent.

3. **OT2020 and earlier cannot be parsed at all.** The term index for those years
   links preliminary-print PDFs under a different path, and
   `parse_term_index` rejects the URL outright (`unsafe Supreme Court opinion
   URL`). The guard is doing its job; the parser has no branch for that layout.
   *Cost:* a second URL shape plus tests. Half a day. Until then the Court's
   pre-2021 output is not merely unfetched, it is unreachable.

4. **RECAP documents are not captured, and are the expensive one.** There is **no
   `recap-documents` bulk dataset** — 46 datasets, and that is not among them —
   and `/recap-documents/` and `/recap-query/` are both 401. The only keyless
   path is `/search/?type=rd`.
   *Measured on 12 sampled APA dockets:* 11 have at least one RECAP entry, mean
   **47.7 documents per docket**, which projects to roughly **367,000 document
   rows** for the 7,698 dockets. Cost is **1.41 s per docket**, so **~3.0 hours
   for one page each** and materially more to paginate ~48 documents per docket —
   call it 6–9 hours keyless.
   The catch that decides it: only **4 of ~220 sampled document rows were marked
   `is_available`**, meaning the PDF is actually in RECAP. So ~6–9 hours buys
   mostly *docket-entry metadata*, and under 2% of it leads to a document.
   *Verdict:* not cheap, so not implemented. Worth revisiting only with an API
   token, and only if entry metadata alone is the goal.

5. **Only 17 opinion bodies land on an APA docket.** The sample is uniform; the
   target is 0.011% of the corpus. Nothing is wrong, but nobody should query
   `court_opinion_bodies` expecting APA coverage today.
   *Cost:* one targeted pass — `build_court_opinion_bodies(cluster_ids=...)` with
   the 1,155 APA cluster ids, which the builder already accepts. It still reads
   the whole 50.8 GiB dump to find them, so 8.6 hours; the *output* is tiny.
   This is the single highest-value follow-up.

6. **12,666 ingested opinions (5.1%) name a cluster the cluster dump does not
   contain**, though both come from the same 2026-06-30 cut. Probably blocked or
   withdrawn clusters the two exports handle differently.
   *Cost:* unknown until diagnosed — a day of reconciling against the publisher.
   Recorded as unexplained rather than rounded away.

7. **90.1% of APA dockets have no decision.** This is an upstream property, not a
   coverage failure: RECAP records that a suit exists, opinion clusters record
   that a decision was published, and most district-court APA suits end without a
   published opinion.
   *Cost:* not closable by more streaming. Closing it would mean capturing RECAP
   *documents* — see gap 4 — and accepting that most of what returns is docket
   metadata, not opinions.

8. **Clusters are the whole corpus, not just federal.** The dump has no
   `court_id`; that lives on the docket. Restricting decisions to the 397 federal
   courts requires joining the 4.67 GiB `dockets` dump.
   *Cost:* 46 min of streaming plus a docket→court map. Cheap, and the obvious
   next step if the table's size becomes a problem.

9. **The search catch-up did not run.** `CourtListenerOpinionSearchReader` is
   implemented and tested, but a local run was skipped: the window from the
   2026-06-30 dump to today spans every court, and keyless cursor pagination over
   it is a scheduled-job cost. Decisions filed after 2026-06-23 are therefore
   absent from `court_opinion_clusters`.
   *Cost:* it already runs in the workflow. Nothing to build.

## One defect found and fixed

The dumps escape an embedded quote as `\"`, not as the doubled `""` the stdlib
CSV dialect assumes. That does not raise — it **desyncs**, silently. Measured on
the first 3,000 rows of the 2026-06-30 `opinion-clusters` dump:

| Dialect | Rows whose `id` is a prose fragment | Rows retaining `docket_id` |
|---|---:|---:|
| stdlib default | **1,987 / 3,000** | 1,001 / 3,000 |
| `escapechar='\\'` | **0 / 3,000** | 3,000 / 3,000 |

Two thirds of the join column — the one this entire ingest exists to create —
would have gone missing in a way indistinguishable from CourtListener not having
the data. It surfaced only because duckdb refused to cast
`'<author id=\"b1326-17\">'` to an opinion id. Fixed in `6c7a654` and pinned by a
regression test. Worth stating plainly: had the merge step been more forgiving,
this would have shipped as a coverage number that was merely wrong.
