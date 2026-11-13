# Retained House floor feed

The shared SpicyDocs reader supplies complete Atom entry revisions and literal
links from the House BillsThisWeek feed. SpicyRegs selects retained captures,
checks their byte count and SHA-256, records them in the central raw archive,
and writes `house_floor_feeds`, `house_floor_entries` and `house_floor_links`.
The same shared table definitions own the column meanings.

Run the offline pipeline with the checkout's pinned source readers:

```sh
uv run --frozen python -m spicy_regs.pipelines.house_floor_feed \
  --selection captures.json --output-dir retained-feed-candidate
```

The selection is explicit:

```json
{
  "format": "house-floor-feed-captures",
  "version": 1,
  "scope": "Selected retained observations; current freshness is unverified.",
  "captures": [{
    "source_uri": "https://docs.house.gov/BillsThisWeek-RSS.xml",
    "observed_at": "2026-10-04T23:25:36.224562Z",
    "sha256": "sha256:<original 64-character digest>",
    "bytes": 38854265
  }]
}
```

Use the capture receipt's actual timestamp, byte count and digest. An optional
`local_path` adopts an already retained original into the central archive; without
it the pipeline reads that archive by digest. `--archive-root` selects an explicit
archive for a local test. Local paths are removed from published evidence copies.
No URL is fetched and no linked document is opened by this command.

Capture-sized Parquet intermediates live under `candidate/captures/`; the combined
candidate and immutable generation retain every selected capture. Identical
observations supplied twice refuse, while different capture times remain separate
even when the body is unchanged. Repeated entry IDs and weekly-page URLs never
collapse into one row. A valid empty feed produces a capture and typed empty entry
and link tables. A malformed or incomplete source fails the build with retained
failure evidence and no completed generation.

The generation is `local-partial`, so it cannot claim publication or complete
historical/current source coverage. It is not added to the hosted catalog. The
caller can inspect the three tables using their native Parquet reader. Feed dates
and scheduled weeks do not establish a vote, passage or enactment. Original
inline HTML is data and should be sanitized before any browser display.

The retained fixture's README pins the original and exact selected entries. Direct
review compared the XML, offline rendered HTML, capture-sized intermediary and
final Parquet rows: the first update changes the bill's `ih` text link to `rh` and
adds H. Rept. 116-364 Part 1 without losing the prior observation. The full retained
capture replay preserved every observed entry and link; this does not establish
freshness or completeness beyond that capture.
