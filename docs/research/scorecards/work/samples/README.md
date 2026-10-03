# Complete-sample research handoff

This packet supplies the sample and acquisition evidence for task `ss-05kv`.
It recommends implementation candidates; it does not mark any source supported,
create an accepted production snapshot, or authorize a partial edition replacement.

Start with [sample profiles](sample_profiles.json), [measured scope checks](measurements.json),
and [bounded row examples](row_examples.json). The measurements are dated by the
[validation receipt](validation_receipt.json), which identifies the external
source corpus, exact research scripts, hashes and commands. Source bytes and
full extracted member tables remain outside the repository.

The later [PDF qualification receipt](pdf_qualification.json) tests the production
reader interface with retained observations from the existing OvisOCR2/Docling
pipeline. NEA passed complete source reconciliation. Humane failed cell/glyph
fidelity, and C4IP failed a rendered-source name check despite complete page
extraction and matching grade-table dimensions. Those two readers remain
unsupported with the tested observations; their publishers remain available.
This supersedes earlier candidate-selection language as a production readiness
assessment. No output was corrected, registry enabled, or raw source republished.

| Source | What was captured and checked | Implementation decision |
| --- | --- | --- |
| AFL-CIO | Both official 2025 chamber workbooks, complete worksheet grids, both item lists and every linked item description | Select. Use workbooks for the complete member-item matrix; item-page member grids are paginated. |
| LCV | The current response to a 2025 export request, both chambers, complete item index, and full official PDF | Select. Preserve blank annual cells, `na`, literal CSV values and source membership; qualify any full member-item extraction separately. |
| Heritage Action | Current 119th catalogs, all returned item details, live roster and repeated catalog checks | Select current scope. Retain former members from item groups. Refuse historical member-list editions until contamination is resolved. |
| Humane World Action Fund | Full final PDF; complete named Senate chart, its method and legend checked | Select for implementation. House parsing must qualify before whole-edition replacement is enabled. No runtime chamber scope is assumed. |
| NEA | Both complete HTML grade tables, official PDF, and disclosed scored-vote supplement | Select. Undisclosed scoring inputs prevent reproduction, not published-grade retention. |
| U.S. Chamber | Current index and versioned methodology; historical sample PDF request failed | Defer implementation until complete member/component sources are acquired. Keep its observed composite shape in the model. |
| C4IP | Full first-edition PDF, complete Senate Table 4 and House Table 5 grade tables, method pages | Candidate replacement for Chamber. Published grades fit; the unavailable Data Annex and numerical component reproduction remain explicit limits. |

The [capture metadata](capture_additions.json) includes successful and failed
requests. HTTP completion belongs to individual responses; complete disclosed
scope is established separately by table boundaries, workbook structure,
reconciled renditions, publisher pagination metadata, or enumerated detail checks.
The Heritage unpaginated cosponsorship list is an observed current catalog scope,
not proof that no unlisted scoring item exists.

Several observations change parser requirements:

- AFL-CIO's letter item uses its vote interface's `no` value to mean signed, and
  says the template's passed/failed result does not apply. Two identically titled
  H.R. 4 columns coexist with two dated publisher item pages. The workbook has
  no hyperlinks establishing which column belongs to which page; that mapping
  remains an explicit production qualification requirement.
- LCV's chamber export parameter returns both chambers. Its CSV preserves bare
  numbers while HTML shows percent signs; `na` and `N/A` also differ. Three
  listed members have no annual cell for the requested year. Presence and rating
  must remain separate.
- Heritage's historical member page declares the 118th Congress but embeds
  current members. A valid page title alone cannot establish edition identity.
- Humane's PDF text tokens `ü` and `ê` render as ✓ and ★. Publish the checked
  display glyphs through a versioned font-decoding rule and retain the raw
  extraction observations as evidence. `100+` is not `100`, and `••`
  identifies leaders without numerical scores.
- NEA prints `NES` for Pete Ricketts in both renditions. Preserve the source
  spelling; downstream identity resolution must explain any correction.
- C4IP's table defines the set of included members. Do not add a missing senator
  or representative to make it match an assumed roster size.

[Relationship dispositions](relationship_dispositions.json) identify what is
exercised and what remains deferred. [Additional challenges](challenge_dispositions.json)
retain HRC, NTU, AJP and FFRF limits without confusing source-model fit with
acquisition or reproduction support. Numeric member adjustments, historical
PDF variants, inaccessible component ledgers and score reproduction remain
unqualified. Public evidence defaults to hash-only; no redistribution grant was
established in this pass.
