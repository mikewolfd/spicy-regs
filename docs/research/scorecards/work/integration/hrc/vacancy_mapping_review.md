# HRC vacancy mapping review

The final 118th Congress PDF includes one explicitly vacant New Jersey district 9 row. Preserve that row as a non-member source observation. Do not insert a person named `Vacant`, a NULL-named synthetic member, or invented member actions to satisfy foreign keys.

The independent [special-row readback](house_special_rows_independent_readback.json) verifies its printed label, footnote marker 11, three literal `N/A` ratings and 41 visibly blank action cells. The blanks remain JSON nulls. They do not mean `N/A`, a recorded vote, or an unreadable mark.

## Current model boundary

The frozen source tables in `spicy-docs/src/spicy_docs/schemas/scorecard_tables.py` have a member occurrence table, member ratings, and member item results. Both dependent tables require a `scorecard_members` key. There is no non-member source-row table or entity-kind discriminator. A synthetic vacancy member would enter downstream member resolution and member counts despite not describing a person.

`scorecard_methodologies` describes source rule sets. Footnote 11 explains why the seat is vacant; it does not itself state a scoring rule. Keep it linked in the retained notes collection instead of inventing methodology semantics. Neither `methodology_text` nor snapshot count fields and completeness prose should substitute for a typed vacancy record.

## Minimal representation within V1

Retain the complete unchanged `vacant_seats` object from the HRC structured outcome alongside the original PDF, model observation, selected-record locator and qualification receipt. The existing outcome schema separates this collection from `members`. The object preserves:

| Source field | Retained representation |
| --- | --- |
| Label, state and district | Exact source strings on the non-member object |
| Footnote marker | Ordered source marker array, linked to separately retained note 11 |
| Three historical rating cells | Three records keyed by the source period labels, each with literal `N/A` |
| Action cells | All 41 distinct source item keys, each with `result_text: null` |

The host can bind the structured input using the existing `SourceEvidenceContext.retain_bytes` or `retain_file` method. This is a retained local derived input, not a fabricated HTTP response. Under `hash_only`, the public receipt contains an opaque capture ID, size and digest; the private retained structured observation supplies replay. That policy does not publish the object or make its values queryable through SQL. Under `metadata_only`, the digest is omitted as usual.

Public source locators must use opaque extraction/observation identities, never private filesystem paths. The source PDF capture and the retained structured input have distinct provenance. Retention failure must refuse the update.

Emit zero vacancy rows in `scorecard_members`, `scorecard_member_ratings` and `scorecard_member_item_results`. Record the non-member vacancy and its typed-table exclusion in the qualification and completeness scope. Completeness checks still require exactly one vacancy in the known source position, its complete three-period rating set and its complete 41-item blank-cell set. Missing vacancy evidence is incomplete acquisition, not permission to omit the row silently.

If consumers require these vacancy values in public SQL now, V1 has no faithful existing-field mapping. Add and review a real non-member row table in a schema revision instead of overloading member or methodology rows. Until then, describe this as retained source evidence outside the typed V1 tables, not full tabular support for non-member source rows.

## Required regression checks

- The source member count excludes the vacancy; downstream identity resolution sees only named members.
- Missing or repeated vacancy, missing/duplicate period or action keys, changed label/location/footnote, and a nonblank action cell refuse.
- Every selected vacancy field matches the unchanged retained model observation and original source readback.
- Hash-only evidence publishes neither the raw PDF nor the structured payload, and no private path appears in table locators or the public journal.
- The source note remains literal and separate from any score interpretation or official congressional data.

This is a mapping recommendation and independent source review. It does not qualify a production reader or authorize publication.
