# HRC reader independent review

**APPROVE the pinned final-118 reader, within its documented source-fact scope.** This review does not admit a public generation or an unreviewed edition. The runtime does not run a model: it requires the exact original PDF and the exact independently checked structured asset, then retains that asset privately before building rows.

## Reviewed scope and result

Reviewed `spicy-docs/src/spicy_docs/sources/scorecards/hrc.py`, the literal `NA` change in `hrc_outcomes.py`, and their focused tests. Traced the reader into the shared row builder/validator and the consumer's parser-version and evidence admission checks. The companion JSON receipt pins these files and the private verification artifacts.

One integration blocker was found and fixed during review: `HRCReader` initially lacked the `parser_version` attribute that the consumer reads before acquisition and again during acceptance. The final wrapper exposes the module version through a `ClassVar` and its regression checks the attribute.

Independent real-source replay checked every member name, parenthesized party, state, district, rating and action against the qualified source records. Every item title, target, date, session, roll and ordered bill/amendment citation array remained literal. The selected asset independently replays to unchanged records in retained model observations; no record repair occurred during selection.

## Function trace

| Function | Caller and input | Work and downstream boundary | Result |
| --- | --- | --- | --- |
| `list_scorecards` (`hrc.py:44`) | Wrapper or consumer; bounded source context | Reads the original index through `ScorecardContext.read`; requires the exact known PDF link | Returns only the closed historical edition; changed listing refuses |
| `HRCReader.list_scorecards` (`:66`) | Consumer adapter API | Delegates without changing context or scope | Same historical selection |
| `HRCReader.acquire_scorecard` (`:69`) | Consumer adapter API | Supplies explicitly injected private asset and retention callback to module acquisition | No implicit model or alternative PDF backend |
| `_qualified` (`:78`) | Module acquisition; exact bytes | Checks fixed digest before parsing; verifies source/schema, page/member coverage, unique identities, item/note/legend keys and referenced notes; calls `validate_outcome` | Changed or inconsistent asset refuses before network work |
| `_retain` (`:153`) | Module acquisition; original document and exact asset | Calls private writer and checks canonical UUID4; hides callback text from public exception messages | Opaque observation locator or refusal |
| `acquire_scorecard` (`:165`) | Module or injected wrapper; exact edition/context | Verifies original PDF URL/digest; retains structured input; maps source facts through `Bundle.add`; closes through `Bundle.finish` | Complete source-table bundle for this pinned edition |
| Local `path` (`:195`) | Acquisition mapping loops | Uses physical page, opaque observation UUID and semantic record/field pointer | No private source path or content-derived identifier |
| `validate_outcome` (`hrc_outcomes.py:103`) | Qualification and bounded extraction checks | Literal-value, unique-key and source-scope validation; added `NA` alternative without rewriting values | Accepts the observed literal while malformed near-variants still refuse |

## Data flow and invariants

- **Original acquisition remains separate from derived evidence.** The public capture callback receives the original PDF response. The private callback receives the reviewed structured asset. No synthetic HTTP JSON response or mislabeled Docling observation is introduced (`hrc.py:165–193`).
- **Source identities stay literal.** Member keys namespace the source chamber/state/district/name. Published name, party, state and district values are unchanged; no official member identifiers or member terms are invented (`:272–289`).
- **Historical columns remain score observations.** Bare values, `NA`, `N/A` and caret suffixes survive. The explicit caret legend routes those historical ratings to House-period metrics and supplies its literal note; it does not change a member's chamber or create term records (`:224–235`, `:290–302`).
- **Multiple citations survive.** Every bill and amendment entry has an ordered occurrence ID and source pointer. Singular convenience fields remain NULL when more than one citation exists (`:239–270`). The source Senate amendment-to-amendment item retains both amendment references.
- **Publisher results remain publisher results.** Glyphs are copied directly. There is no inferred official vote, universal scoring formula, weight or metric participation (`:303–312`).
- **Vacancy stays non-member evidence.** Its label, note marker, three literal `N/A` scores and 41 blank cells remain in the retained structured asset. Member, member-rating and member-result tables contain no fake vacancy entity. See [mapping review](vacancy_mapping_review.md).
- **Coverage is explicit.** Inspected row counts are not mislabeled publisher-declared totals. Snapshot disclosure distinguishes typed facts, retained non-member/narrative observations, and prose outside the tabular scope (`:313–322`).

## Edge cases checked

| Scenario | Expected and observed behavior |
| --- | --- |
| Missing asset/callback or changed asset bytes | Refuses before source acquisition |
| HTTP404, empty response or changed PDF | Refuses before private observation retention |
| Missing member/state/House district, repeated item key or missing source note | Complete-scope validation refuses |
| Ordinary member's blank action, or vacancy blank converted to `N/A` | Refuses; the two source cases remain distinct |
| `NA`, `N/A`, `100^` | Exact text retained; no invented numeric conversion or percent sign |
| Callback returns a path, hash or invalid receipt; callback raises private text | Refuses without echoing private content |
| `metadata_only` | Output locators contain opaque IDs, not the source/asset digest or private path |
| Closed 118th edition alongside newer publisher material | `is_current=False`; no current-edition or general historical support claim |

The focused reader/outcome suite passed, and focused Ruff checks passed. The public JSON receipt records the observed result and file hashes.

## Remaining integration boundary

The installed package, host callback wiring, private storage lifecycle, registry selection and public publication still require their own checks. The existing item resolver does not yet inherit the explicit edition Congress when a cosponsorship citation omits per-item Congress; that is a downstream context rule, not a reason to alter the reader's literal NULL fields. Introductory narrative is explicitly outside typed scorecard scope; its inaccurate model prose is not admitted as an exact source transcription. See [frontmatter readback](frontmatter_scope_independent_readback.json).
