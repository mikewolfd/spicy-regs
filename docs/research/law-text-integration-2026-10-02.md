# Native enacted-law text integration

The laws rollup now publishes native law sections from the same GovInfo XML
capture it uses for law identity and citations. Researchers can search the
enacted text, follow a section to its exact source, and distinguish unavailable
text from an empty result. This qualification is local; it does not establish
public publication or deployment.

## Data and relationships

- `laws` retains law identity, the originating `bill_id`, text outcome, reader
  version, section count, actual captured XML URL, and main-body text outside
  sections. The existing `url` remains the Congress.gov bill API URL.
- `law_sections` holds one native section occurrence, keyed by `(law_id, seq)`.
  It retains its number, heading, body, parent, hierarchy, quotation flag,
  publisher identifiers, XML path, source hash, and observation time.
- `law_sections.law_id` joins to `laws.law_id`; `laws.bill_id` joins to
  `congress_bills.bill_id`. Existing Code-classification tables remain separate.

Section labels and publisher identifiers can repeat or be absent. Parent
sections exclude their child sections' text, and the child rows hold that text
once. `parent_seq` states nesting; `is_quoted` states quotation ancestry.
`law_body_remainder` retains main-body material outside native sections,
including appropriations. Search both section text and the remainder when
searching a whole law. These are printed enactments, including amendment
instructions; they do not apply amendments to reconstruct current law.

Normalize whitespace for phrase searches: source block boundaries become
newlines. Searching only rows where `is_quoted = 'false'` can miss text quoted
by an enacted amendment; retain those matches and follow `parent_seq` when
counting the containing enacted sections. Section occurrences, containing
sections, and distinct laws answer different counting questions.

The rollup uses the acquired XML once, with no additional publisher request
for sections. A successful reread replaces the law's complete section set.
A failed reread preserves its previous validated metadata and text. Missing
or mismatched child counts/hashes trigger a reread. Unavailable XML retains
NULL text/count fields and its separate acquisition outcome.

## Source and package identity

SpicyDocs commit `f8431033f62633b1e7ffbb00a063748f582f6ab4` promotes the
existing worked USLM event tree and grammar into source-owned modules and
adds the law section reader and table definitions. The worked examples retain
their captured source and text content.

SpicyRegs pins `spicy_docs-0.53.0+laws.f8431033f626`, SHA-256
`b011d6a57622cc8756b54560c484fd5c7fa3b0a4a893298d75239a7b64766686`.
Two builds from the committed source archive produced identical wheel bytes.
Only the exported package version differs from the source checkout. The
consumer checks use this installed wheel, without a source-checkout import.

## Validation evidence

Receipts are retained under
`corpora/law-text-integration-20261002/` in the local corpora directory:

| Check | Evidence |
|---|---|
| Source repository gate | `source/full-gate-source-url.log`, `source/acceptance.json` |
| Reproducible wheel and installed import | `source/wheel.json`, `consumer/installed-source.json` |
| Independent retained-corpus comparison | `corpus-audit/report.md`, `corpus-audit/acceptance.json` |
| Canonical candidate construction | `build_candidate.py`, `candidate-final.json` |
| Every stored candidate section compared to XML | `audit_candidate.py`, `consumer/candidate-audit.json` |
| Consumer tests, type checks, dictionary and documentation | `consumer/gate-final/` |
| Blind research and independent answer review | `blind/alex/`, `blind/priya/`, `blind/answer-audit.json` |
| Original blind queries and SQL safeguards replayed | `blind/final-replay-green/`, `blind/replay-acceptance.json` |

The retained source audit covers 2,155 distinct laws plus three byte-identical
copies from separate acquisition receipts. Its 2,158 test inputs and 56,382
section rows are validation observations, not a deduplicated publication.
The archive census and scope are recorded in `corpus-audit/report.md`.

The canonical candidate reuses the exact captures behind the published
119th-Congress law family. Its measured output is 119 law rows: 113 parsed
laws, six unavailable XML sources, and 3,852 native section occurrences.
Every stored section, hierarchy, path, body, and remainder passed comparison
to an independent ElementTree interpretation of the retained XML. Parent
counts, timestamps, URLs, hashes, and occurrence identities also passed.
The existing `law_code_sections` and `table3_records` cells are unchanged.
See `candidate-final.json` and `consumer/candidate-audit.json` for the measured
scope and exact output hashes.

Candidate artifact digest:
`sha256:ba28b9c3cca6c1ef17142cf3ad841a158b2b9e6500d688bca75e8c73dab2be10`.

## Findings addressed and limits

The independent source audit found substantive appropriations outside native
sections and fused XHTML table cells. The final reader retains the remainder
and separates table cells with text boundaries, recording that the original
table structure has been rendered as text. It preserves unknown element text
with observations instead of silently discarding it.

The first blind researcher completed enacted-law and private-law retrieval,
but found an unclassified coverage field and no direct law XML URL. The final
candidate exposes derived coverage and the actual retained acquisition URL.
The original research queries pass again. Their JSON issue arrays differ
only in serialization whitespace after adoption of the source-owned shaper;
the original comparisons and normalization record are retained.

The second blind researcher completed coverage, hierarchy, repeated-label,
appropriation, table, and citation checks without a confirmed implementation
defect. Its conditional pass applies to searching this held sample. The root
review checked both researchers' query results against the canonical data
already compared to source XML. The unavailable citation-resolution dependency
was reported as unavailable; it did not manufacture a citation result.

The local query candidate covers the retained published law family, not all
historical law archives validated by the source audit. PDF processing remains
deferred. Table formatting is flattened, and current legal effect, national
completeness, source freshness beyond the capture dates, and public activation
are outside this qualification. The replay held the published Code relationships
without reacquiring them. Release work must retain the listing and inherited
input lineage, publish the release generation, and verify the deployed consumer
against its resulting publication pin.
