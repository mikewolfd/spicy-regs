# Compare historical presidential veto totals

`historical_veto_administrations` compares the Senate's published administration
spans from one dated [Summary of Bills Vetoed](https://www.senate.gov/legislative/vetoes/vetoCounts.htm).
`historical_veto_snapshots` retains its category totals, merged headers, footnotes
and source scope. Every row carries the original digest, URL, observation time
and exact source coordinates. Raw cells and HTML remain available beside typed
integers. An explicit zero stays zero; a blank count refuses rather than becoming
zero. Overridden vetoes are a subset of regular vetoes. Total is regular plus
pocket, with disputed cases treated as the publisher's footnotes state.

An administration span may cover several presidential terms. Nonconsecutive
spans for the same president stay separate. The source's `1945-53` spelling and
`26, 25` Congress label stay literal. The current administration's ending year
is a label, not proof that its full future span has been observed. This source
does not supply individual veto events, a complete bill population, a per-term
allocation of multi-term totals or line-item cancellations. Linked per-president
documents have not been acquired by this workflow.

Run from this repository with the `source-readers` environment and a SpicyDocs
version containing `spicy_docs.sources.historical_vetoes`:

```sh
uv run --frozen --extra source-readers python -m spicy_regs.pipelines.historical_vetoes acquire \
  --selection "$WORK/2026-11-13/selection.json" --output-dir "$WORK/2026-11-13/acquire"
uv run --frozen --extra source-readers python -m spicy_regs.pipelines.historical_vetoes build \
  --selection "$WORK/2026-11-13/selection.json" --output-dir "$WORK/2026-11-13/build"
```

Acquisition uses the maintained `BoundedAcquirer` and `CaptureEvidence`; the
central raw archive retains one bounded original GET before parsing. A new
dated selection is required for refresh. An existing selection cannot be
overwritten by the CLI. Build reads and hashes the selected original offline,
checks every administration's categories, reconciles all category sums against
the publisher's footer and creates an ordinary immutable generation with source
evidence. For a refresh of a published family, pass `--read-index "$LIVE_INDEX"`
to build, using the pinned mirrored index described in local family refresh.
The generation then records the prior selection for publication's concurrency
guard. Unsupported merged headers/cells, missing notes, truncated populations,
bad pins and challenge responses refuse explicitly. Failed attempts retain
their evidence and produce no successful generation.

Query a locally selected generation through the ordinary MCP query connection:

```sql
SELECT a.president, a.years_raw, a.regular_vetoes, a.pocket_vetoes,
       a.total_vetoes, a.overridden_vetoes, a.source_url,
       a.observed_at, a.source_locator_json, s.footnotes_json
FROM historical_veto_administrations a
JOIN historical_veto_snapshots s USING (snapshot_id)
WHERE a.president = 'Grover Cleveland'
ORDER BY a.years_raw;
```

This compares Cleveland's separate 1885–1889 and 1893–1897 spans without
double-counting overridden vetoes. No matching administration returns an empty
result, not a synthetic zero row. For an explicitly requested person-wide total,
sum only `total_vetoes` across selected spans in this one snapshot and state
that grouping. Never add the footer to administration rows or combine snapshots.

The family uses the maintained local publication path (`historical-vetoes` in
`local_publication.LOCAL_FAMILIES`). Build and local admission do not publish it.
The coordinator owns shared integration, independent review and any public
selection. See [local family refresh](local-family-refresh.md) for publication
guards. The bounded original fixture has its own exact provenance. No new parser
package or third-party code was added; the source reader reuses SpicyDocs HTML
reading, while this application owns the numeric/category interpretation.
