# Search and retrieve court opinion text

The named consumer is the SpicyRegs MCP `query_sql` tool. Its
`court_opinion_search_text(markdown)` function reads GFM plus footnotes using
Marko, the parser used by SpicyDocs' fidelity checks. It folds line breaks and
whitespace and searches visible words across inline formatting. Citation labels
and footnote bodies survive; link destinations and footnote labels are not search
terms. The stored Markdown, per-rendering digest (`source_text_sha256`), and
export-file digest (`source_sha256`) remain unchanged. NULL bodies stay NULL; a
search miss does not mean a body is missing.

Converter 003 can use GFM inline `<em>`/`<strong>` for exact emphasis boundaries
that Markdown delimiters cannot express. The search reader skips those formatting
tags, keeps their visible content, and preserves escaped literal tag text.

This is a literal text scan of selected Parquet bodies, with no duplicate corpus
or index. `court_opinion_texts` is partitioned by `opinion_block` (`opinion_id // 150000`),
not by court or filing date. Because a single court's opinions span dozens of blocks
(for example, First Circuit opinions span 68 of 77 blocks, and Supreme Court or Ninth
Circuit opinions span 64–70 blocks), an unscoped `court_id` scan with
`court_opinion_search_text()` can transfer tens of gigabytes and exceed the request
timeout. Always narrow by `opinion_block` or by `court_id` plus `date_filed` (for
example, recent filings in 2025+ fall into the newest blocks), and add a vectorized SQL
anchor-word pre-filter (`contains(lower(t.markdown), '<anchor>')`) inside the `CASE`
guard so Marko only parses candidate rows. `LIMIT` bounds the reply, not the bytes
read. No ranked-search or production latency claim follows from this path; a dedicated
full-text index belongs to a downstream search owner.

First check `describe_table` for availability and coverage. The schema and these
examples do not establish that a full body generation has been built or published.

```sql
SELECT t.opinion_id, t.cluster_id, c.court_id, c.case_name, c.date_filed,
       'https://www.courtlistener.com/opinion/' || c.cluster_id || '/' AS original_url,
       'https://www.courtlistener.com/api/rest/v4/opinions/' || t.opinion_id || '/' AS original_record_url,
       t.opinion_block, t.text_source, t.source_column, t.date_modified,
       t.converter_version, t.dump_date, t.source_sha256, t.source_text_sha256,
       left(court_opinion_search_text(t.markdown), 800) AS passage
FROM court_opinion_texts t
JOIN court_opinions o ON t.opinion_id = o.opinion_id AND t.cluster_id = o.cluster_id
JOIN court_opinion_clusters c ON o.cluster_id = c.cluster_id
WHERE c.court_is_federal
  AND c.court_id = 'ca1'
  AND c.date_filed >= '2025-01-01'
  AND contains(lower(court_opinion_search_text(
        CASE
          WHEN c.court_is_federal
           AND c.court_id = 'ca1'
           AND c.date_filed >= '2025-01-01'
           AND contains(lower(t.markdown), 'process')
          THEN t.markdown
        END)), 'due process')
LIMIT 5
```

The original court page uses **cluster_id**, while the native API record uses
**opinion_id**. They are different identifiers. `court_is_federal` is derived by
the existing cluster-to-court selection, not by searching court names. NULL
jurisdiction stays unresolved and is excluded from the federal selection. The
`CASE` guard keeps its argument tied to the narrowed parent selection and uses a
fast SQL substring pre-filter on an unsplit anchor word (`'process'`) so excluded
or non-candidate bodies stay NULL instead of invoking Marko.

For single-opinion retrieval, compute `opinion_block = int(opinion_id) // 150000`
and pass both quoted string filters to `query_sql`. MCP `query_sql` registers
partitioned split tables with explicit `hive_partitioning = true` and `hive_types`
(`{'opinion_block': 'VARCHAR'}`), so DuckDB prunes directly to the one matching
member file before opening Parquet footers (and skips non-matching row groups
inside that member using `opinion_id` statistics). Do not omit quotes on
`opinion_id` or `opinion_block`: an unquoted integer casts every row and defeats
row-group pruning.

```sql
SELECT opinion_id, cluster_id, markdown, text_source, source_column,
       source_text_sha256, date_modified, converter_version, dump_date, source_sha256
FROM court_opinion_texts
WHERE opinion_block = '47' AND opinion_id = '7090690'
```

Per-row ETL receipts are retired (`courts/2`, `receipts_retired: true`). Each row
carries its provenance directly: `dump_date` (export edition), `source_sha256`
(SHA-256 of the retained bulk export file, matching `court_opinions.source_sha256`),
`source_column` (the export column converted, NULL when `text_source = 'none'`),
`source_text_sha256` (SHA-256 of that rendering's raw bytes, NULL when
`text_source = 'none'`), and `converter_version`. Converter code digests, build
reconciliation counts, and any conversion refusals
(`court_opinion_texts.exceptions.parquet`) are retained in the generation's
source-evidence artifact. Database modification time is not first publication or
text-revision time; multi-export history is owned by ss-5cls/ss-2d2p/ss-4avf.

Opinions with `text_source = 'none'` retain a NULL body, NULL `source_column`,
and NULL `source_text_sha256`. Join the existing `court_opinion_pdf_extractions`
by `opinion_id` for separately evidenced PDF text; its source hash, extraction
details, and parent pins remain distinct. Neither missing PDF extraction nor a
refused conversion becomes empty text or an unsuccessful search. Reconcile
refusals separately via `court_opinion_texts.exceptions.parquet` and the build
run summary (`court_opinion_texts-run.json`). Native bodies with no text and
malformed HTML are not repaired by the query function. No new OCR or acquisition
runs are implied.

Converter 002 kept native `/c/` citation paths and resolved `opinion:ID` links
while giving unresolved citations an explicit CourtListener text-search link.
Converter 003 added exact GFM inline `<em>`/`<strong>` emphasis fallback for
punctuation-adjacent emphasis spans while keeping `source_column`,
`source_text_sha256`, `source_sha256`, and retained originals intact.
