# Navigate retained source relationships

The `spicy_regs.relationship_views` module installs read-only SQL views over
already loaded tables. It never acquires sources, rebuilds published data or
looks up targets during connection setup. The serving host calls
`install_relationship_views(connection, available_tables, publication=...)`
before restricting the connection, then uses the returned availability and
metadata for discovery. Views are regular in-memory DuckDB views so request
cursors can see them. The module uses only Python's standard library and the
injected DuckDB connection; it does not import SpicyDocs or pipeline modules.

The registry is the authoritative list of supported arrays. Each entry provides:

- `<name>_occurrences`: every array element, including duplicates, nulls and
  unsupported elements, with the source's full key, zero-based ordinal, field
  pointer, raw element JSON, parsing disposition and publication metadata.
- `<name>_pairs`: distinct valid source/target navigation pairs. These discard
  multiplicity intentionally; return to occurrences for source evidence.
- `<name>_field_states`: the held field's literal value and whether it is SQL
  NULL, JSON null, malformed, a non-array, empty or populated. A non-array never
  becomes a false empty source result. SQL NULL cannot recover the original
  publisher's absent/null distinction when an earlier shaper discarded it.

`target_status=not_checked` means a supported identifier was projected. It does
not assert that a target exists in the selected publication. The separate
lookup views below report `found`, `missing` or `ambiguous` only after an exact
lookup against their declared target inputs. `unsupported`
means this view does not supply a qualified target route. Publication JSON is
provided by the host separately for each input; JSON `null` means it was not
provided, not that immutable source identity was established. Locations point
to held array columns, not to unverified locations in an original capture.

## Supported source observations

| View family | Meaning and limits |
| --- | --- |
| `bill_related_bills` | Directed publisher-related bills. Each occurrence keeps the complete nested relationship-details list. No reciprocal relationship or identical text is inferred. |
| `member_fec_ids` | Source-listed candidate IDs, including presidential IDs, with community-crosswalk capture and roster context. No election cycle or committee authorization is inferred. |
| `meeting_*` | Bills, hearing jackets, committee objects, witnesses and offered documents are independent lists. Full meeting key is Congress/chamber/event ID. Jacket targets retain Congress/chamber. Canceled status stays visible. Witness names create no person identity; document URLs create no acquisition claim. |
| `vote_documents`, `vote_amendments` | Independent native blocks. Nomination suffixes and native Congress survive. PN and held bill shapes have routes; amendment/treaty routing remains unsupported pending positive native specimens. Empty amendment IDs do not suppress document references. |
| `fcc_filing_proceedings` | Held FCC proceeding-name memberships, preserving repetitions. The numeric IDs and participant roles discarded by earlier shapers cannot be reconstructed here. |
| `federal_register_rins`, `federal_register_dockets` | All held values under the complete document-number/publication-date key. Docket target keys remain literal source spellings: no free-text normalizer or unique-target claim. |
| `document_additional_rins`, `proceeding_*`, `comment_period_*` | Every held membership, independently expanded. Shared RINs do not collapse actions or pair different lists by position. |

`house_communication_rins` also preserves every held RIN finding under the full
Congress/type/number key, including source route, field digest, spans and
extraction rule. Older schemas without its held array are unsupported.

Comment references use a separate scalar-field path. `comment_document_references`
keeps the explicit `commentOnDocumentId` observation; its `_pairs` view includes
only nonblank string references. `comment_native_references` preserves the
separate object/original-document namespaces; neither is treated as a document
ID. `comment_reference_field_states` reports the native map's absent/null/empty
states. Legacy rows without that map remain `unread`. The source docket stays
null when the publisher supplied no docket, and there is no prefix inference.

## Target lookup and analytical views

| View | Grain and safeguards |
| --- | --- |
| `agenda_item_editions` | Every exact RIN/edition/URL target stays separate. Duplicate edition rows are ambiguous; no latest-edition selection. |
| `uei_identifiers`, `sam_uei_registrations` | Literal UEIs remain namespaced by source and registrations retain native EFT, including NULL. Shape checks do not adjudicate entity identity. |
| `recipient_sam_entities` | One source recipient row with matching registration count. Grouping registrations before enrichment prevents monetary row multiplication. Original amounts and capture dates remain unchanged; parent/child totals may overlap. |
| `court_*_endpoints` | Native opinion, cluster and docket IDs stay typed. Source edges remain separate from each endpoint lookup. Duplicate exact targets are ambiguous, not silently chosen. No body acquisition or cross-provider court mapping. |
| `section_diff_endpoints`, `section_diff_version_endpoints` | Complete bill/version/provider keys on both sides. Section existence and text-digest agreement are separate. Added/removed sides may be absent. Engine revisions and all version candidates remain visible. |
| `fec_collection_cycles` | One row per collection. `cycle` is the FEC's own `bulk-downloads/<even year>/` directory in the captured URLs (2026 = 2025–2026); NULL with `not_stated` for API, legal, header and other files, and `ambiguous` when captures span two directories. No cycle is read from a file name or query parameter. Join `collection_id` to `fec_source_records`. |
| `fec_relationship_evidence` | Source locator resolves a collection/record companion without multiplying observations. All ambiguous candidates survive. Digest equality compares recorded digests; actual source bytes remain `not_checked`. Explicit empty-list observations remain empty-list observations. |
| `member_vote_party_affiliations` | One row per vote/member with native party candidates under half-open date intervals. Missing ends are not open-ended; gaps stay unknown, overlaps ambiguous, and term party never supplies a silent fallback. Source capture and affiliation ordinals remain in candidates. |
| `org_identity_candidates` | Existing name matches remain pending with matcher features, competing-name counts and original comment date bounds. Candidate IDs are scoped to a publication digest; unversioned sources get no falsely immutable candidate ID. Acting role is unknown; no identity or funds attribution is accepted. |

Offered artifact arrays (`document_artifacts`, `fcc_filing_artifacts`) retain
all native rendition fields and distinguish a URL from acquisition. Native
FR agencies/topics, Congress subject terms and LDA contacted-entity arrays
retain provider namespaces. No RefSpec equivalence is guessed from a matching
label. Empty LDA contacted-entity arrays create no agency edge.

Every advertised view includes schema-derived columns and coverage semantics:
source population is inherited, occurrence counts differ from unique pairs,
legacy schema absence is unsupported, and null/empty/malformed/unread states
remain explicit where the held data supports them. No counts or completeness
measurements are inferred during installation. These are connection views,
not independently published artifacts or an atomic cross-table snapshot.

## Scope of this delivery

This implements the thin held-array portion of T04/T06/T07/T08/T10/T11 and the
T03 explicit-field views. It does not complete target resolution or a public
population qualification. T10 native numeric IDs/participant detail need the
source projection repair. T11 free-text normalization and lifecycle date routing are not added by these
views. T14 entity-level enrichment, T15 selected native endpoints and T21 full
version/section lookup are implemented; population-wide source replay remains
separate. T09 provides companion navigation but does not read or hash evidence
bytes. T16 exposes selected held artifact families, not every source rendition.
T20 exposes native namespaced terms, not reviewed cross-source mappings. T22
exposes candidates without an acceptance/revocation workflow or a qualified
identity truth set. T25 adds explicit coverage semantics without materializing
new aggregate counts. T18 RIN arrays and T19 dated party selection are available
only when their new source columns/tables have actually been published; this
code does not backfill or publish those inputs.

Run focused validation with:

```sh
uv run --frozen --no-sync pytest tests/test_relationship_views*.py
uv run --frozen --no-sync ruff check src/spicy_regs/relationship_views tests/test_relationship_views*.py
```

The retained Senate fixture proves all native document blocks survive alongside
independent empty-ID amendment blocks, including `PN55-25`. The meeting fixture
checks independently retained jackets, witnesses and offered document objects.
See `tests/fixtures/relationship_views/README.md` for capture provenance and
limitations. Synthetic controls cover repeated pairs, malformed/null fields,
complete keys, old schemas and request-cursor visibility. These checks do not
establish publication, source-population completeness or deployed availability.
