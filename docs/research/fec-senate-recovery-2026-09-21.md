# Historical Senate filing recovery — September 21, 2026

The [gap register](../fec-gaps.md) consolidates the remaining discovery,
interpretation, evidence-portability and publication work. The
[coverage census](fec-coverage-2026-09-21.md) places this recovery within the full
source and bulk inventories.

The Senate archive still serves original files. Its discovery interfaces fail in
different ways; those failures do not establish that the underlying files are
absent. This investigation continues the
[FEC bulk delivery](fec-bulk-continuation-2026-09-21.md).

## Verified access paths

| Public request | Observed result |
| --- | --- |
| `https://docquery.fec.gov/senate/` | HTTP 404. This is the exact link on FEC's bulk page. |
| `https://docquery.fec.gov/senate/index.html` | HTTP 200; retained unofficial Senate filing search page. |
| The page's `/cgi-bin/senate_forms/` search action | HTTP 403 for the documented form submission. |
| The documented `/cgi-bin/senate_forms/C00484683/1678/f6S` report link | HTTP 403. Its original URL appears in FEC administrative-fine case 3234. |
| `https://docquery.fec.gov/senate/posted/` | HTTP 200; XML listing of 1,000 objects, explicitly truncated. |
| That listing with `marker`, `prefix` or `list-type=2` controls | Same first page; continuation did not advance. |
| Direct access to the bucket named in that XML | HTTP 403; no attempt to bypass access controls. |
| An exact known `/senate/posted/{id}.fec` original | Working download route, verified against retained bytes. |

The listing's first page contains 479 `.fec` originals, 478 `.sql` objects,
42 `.img` objects and one metadata marker. All 479 listed `.fec` files were
acquired, totaling 81,530,596 bytes. The other object types remain listed
metadata, not acquired originals. Listing ETags are retained as validators,
not assumed to be content hashes. The 2024 object-modification dates are
storage observations, not filing dates.

The missing directory index and ignored query controls are consistent with
public routing or cache configuration problems. This investigation did not
inspect the publisher's server configuration and does not establish a precise
infrastructure root cause.

## Discovery and provenance

The recovery keeps three different observations separate:

1. Live publisher XML names exact files and supplies size, ETag and modification
   metadata. A repeated first page remains a partial inventory.
2. Internet Archive indexes and captured official committee pages reveal
   additional historical file identifiers. Every selected identifier retains
   its actual archived URL or link location; archive coverage is not publisher
   completeness.
3. Current original-file responses establish which selected files FEC still
   serves. Each successful response has retained bytes, a SHA-256 digest,
   acquisition time and provenance. A failed request remains a failed request.

[NYTimes Fech documentation](https://nytimes.github.io/Fech/#unofficial-senate-filings)
and its source establish the historical Senate file route and the independently
known filing 853. FEC's own
[administrative-fine case 3234](https://www.fec.gov/files/legal/admin_fines/3234/17092716484.pdf)
supplies the report-1678 navigation witness. These references identify source
locations; they do not replace the original filing bytes.

The sealed selection names **599 distinct originals**. FEC served **598 files,
123,652,976 bytes**. The remaining file, `5.fec`, returned 404 from FEC; its
**8,220-byte** Internet Archive copy was recovered separately and matched the
archive index's digest. Its original historical URL, archive capture timestamp
and present download receipt remain separate provenance. It is not represented
as a successful live FEC capture or included in the official-source table input.

Four complete public archive-index queries returned 670 capture observations.
All distinct observed successful index and committee-page URLs received bounded
retrieval attempts. Across 30 HTML attempts, 24 succeeded, five returned 404 and
one failed to connect. The retrieved pages left no additional unattempted index
links. These statements concern the observed discovery evidence, not all pages
that ever existed.

An independent header audit found nine additional filenames through explicitly
prefixed `SEN-` amendment references. After recovery, all 64 mapped Senate-local
reference occurrences resolve within the live set. Four `FEC-` occurrences remain
distinct; their prefix was not silently changed to Senate. Exact-layout F3 date
observations span October 1, 2003 through June 6, 2018. Fifteen files with legacy
or differently spelled header versions remain unmapped for this semantic audit,
so these endpoints do not define the archive's complete period.

## Source fidelity

The existing SpicyDocs 0.26.0 positional reader handles the recovered originals;
no new downloader, parser or provider release was required. Ten literal filing
format versions are present. Files `64.fec`, `72.fec` and `82.fec` contain byte
`0xA0` in source fields. Their complete Latin-1 and CP1252 decodings agree; the
selected Latin-1 interpretation and the initial UTF-8 failures are recorded.

One source file, `48.fec`, has a malformed narrative terminator: `[ENDTEXT]"`.
The native parser refuses it as an unterminated text block. Its original 671
bytes and native refusal are retained. A separately labelled physical-line
collection preserves all 19 lines through the existing literal-delimited
reader, using an absent field separator so each nonblank line remains one
complete field. Blank lines remain records with byte coordinates. This fallback
does not repair the quote or assert native filing-field semantics.

Thus the live selection contains **597 native filing parses and one explicit
physical-line fallback**. Archive file `5.fec` was independently parsed offline
as 40 records in format 2.00 and remains outside those live-source inputs.

## Verified local delivery

The recovered live originals add **598 collections and 597,156 source records**:
597,137 native filing records plus the 19 physical lines described above. The
audit compared **26,503,841 literal fields and 928 narrative references** against
retained originals. Manual raw/output readings covered every observed native
version, both delimiter styles, legacy headers, narrative bodies, negative
values, literal quotes, encoding exceptions and the malformed terminator.

Combined with the earlier FEC generation, the local tables contain:

| Table | Rows |
| --- | ---: |
| `fec_source_records` | 13,717,161 |
| `fec_collections` | 649 |
| `fec_relationships` | 183,390 |
| `fec_source_catalog` | 26 |

Every combined output cell matches its audited input. The running MCP server
passed discovery, schema, count and selected-record checks; all 649 collection
family joins and all 183,390 relationship parent joins resolve. It also exposes
the physical-line representation and excludes the separately recovered mirror.
The Senate addition emits no interpreted relationships. Source identifiers
remain literal fields until verified mappings support promotion.

All 26 official bulk groups now have selected local outputs: 25 include parsed
records and one exposes a file inventory. These bulk groups differ from the
broader 26-family source catalog. Neither count means complete historical
coverage. The updated coverage receipt preserves each group's selected scope,
including the unexpanded individual-contribution base and PostgreSQL dump.

From the SpicyRegs repository, query the assembled generation through MCP:

```sh
SPICY_REGS_DATA_DIR=/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/final-tables \
  uv run --frozen spicy-regs-mcp
```

This recovery used the installed provider and existing host interfaces. No
application code or dependency changed. Outputs are local; no data upload,
package publication, push or deployment occurred. The publisher issue draft
remains unsent.

## Evidence

Campaign evidence stays outside the repositories:

- [Discovery, originals and output receipts](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/)
- [Initial recovered original and HTTP receipts](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-recovery-2026-09-21/FINDINGS.md)
- [Sealed discovery manifest](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/archive-discovery/known-originals-manifest-v4.json)
- [Independent live-listing and original-byte audit](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/listed-originals-independent-audit.json)
- [Header-reference closure and manual checks](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/raw-header-audit/round-2/MANUAL-AUDIT.md)
- [Delivery manifest and pinned output digests](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/delivery.json)
- [Updated bulk coverage and remaining scope](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/BULK-COVERAGE.md)
- [Exhaustive and manual raw/output audit](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/selected-publication/BULKSELECTED598-AUDIT.md)
- [Running MCP server verification](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/final-mcp-audit/summary.json)
- [Local draft of the publisher access issue](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/PUBLISHER-ISSUE.md)

The original zero-file attempt is retained as historical evidence. Later
successful recovery does not rewrite its observed 404 or make its collection
complete. Full 2008–2018 archive coverage still requires a complete publisher
inventory or another independently qualified population definition.
