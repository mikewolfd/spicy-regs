# Search and retrieve court opinion text

The named consumer is the SpicyRegs MCP `query_sql` tool. Its
`court_opinion_search_text(markdown)` function reads GFM plus footnotes using
Marko, the parser used by SpicyDocs' fidelity checks. It folds line breaks and
whitespace and searches visible words across inline formatting. Citation labels
and footnote bodies survive; link destinations and footnote labels are not search
terms. The stored Markdown, native source-column hash and export witness remain
unchanged. NULL bodies stay NULL; a search miss does not mean a body is missing.

Converter 003 can use GFM inline `<em>`/`<strong>` for exact emphasis boundaries
that Markdown delimiters cannot express. The search reader skips those formatting
tags, keeps their visible content, and preserves escaped literal tag text.

This is a literal text scan of selected Parquet bodies, with no duplicate corpus
or index. Filter court, date and opinion block before searching when possible.
An unrestricted search can read the full selected body population; LIMIT bounds
the reply, not the bytes read. Remote scan cost and the request timeout can make
an unrestricted query impractical. No ranked-search or production latency claim
follows from this path. A later index belongs to the maintained search owner and
its resource admission, preserving the protected existing runtime volume.

First check `describe_table` for availability and coverage. The schema and these
examples do not establish that a full body generation has been built or published.

```sql
SELECT t.opinion_id, t.cluster_id, c.court_id, c.case_name, c.date_filed,
       'https://www.courtlistener.com/opinion/' || c.cluster_id || '/' AS original_url,
       'https://www.courtlistener.com/api/rest/v4/opinions/' || t.opinion_id || '/' AS original_record_url,
       t.opinion_block, t.text_source, t.date_modified, t.converter_version,
       left(court_opinion_search_text(t.markdown), 800) AS passage
FROM court_opinion_texts t
JOIN court_opinions o ON t.opinion_id = o.opinion_id AND t.cluster_id = o.cluster_id
JOIN court_opinion_clusters c ON o.cluster_id = c.cluster_id
WHERE c.court_is_federal
  AND c.court_id = 'ca1'
  AND contains(lower(court_opinion_search_text(
        CASE WHEN c.court_is_federal AND c.court_id = 'ca1' THEN t.markdown END)), 'due process')
LIMIT 5
```

The original court page uses **cluster_id**, while the native API record uses
**opinion_id**. They are different identifiers. `court_is_federal` is derived by
the existing cluster-to-court selection, not by searching court names. NULL
jurisdiction stays unresolved and is excluded from the federal selection.
The CASE inside the function keeps its argument tied to the selected parent
court; it also leaves excluded bodies NULL instead of parsing their Markdown.

For the matching ID, compute `opinion_block = int(opinion_id) // 150000`, find
that block's member in the selected publication index and read that member.
The existing index carries block labels, not explicit per-member min/max IDs;
do not describe it as a general ID-range manifest. With `query_sql`, keep both
filters so the existing Parquet scan can prune files and row groups:

```sql
SELECT opinion_id, cluster_id, markdown, text_source, date_modified, converter_version
FROM court_opinion_texts
WHERE opinion_block = '0' AND opinion_id = '123'
```

Return the selected generation's processing receipts alongside any provenance
claim: they bind the original export hash, opinion ID, source column/value hash
and converter identity. Database modification time is not first publication or
text-revision time. Multi-export history is owned by ss-5cls/ss-2d2p/ss-4avf.

Opinions with `text_source = 'none'` retain a NULL body. Join the existing
`court_opinion_pdf_extractions` by opinion_id for separately evidenced PDF text;
its source hash, extraction details and parent pins remain distinct. Neither
missing PDF extraction nor a refused conversion becomes empty text or an
unsuccessful search. Reconcile them separately with the builder's run and
refusal receipts. Native bodies with no text and malformed HTML are not repaired
by the query function. No new OCR or acquisition runs are implied.

Converter 002 keeps native `/c/` citation paths and resolved `opinion:ID` links.
An unresolved citation lacking both gains an explicit CourtListener text-search
link. It asserts no matched opinion. Converter identity changes; completed 001
outputs retain their original identity. Source facts, source-column hashes and
originals remain intact.
