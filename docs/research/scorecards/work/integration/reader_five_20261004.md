# Qualified reader package follow-up

The [source commit](https://github.com/mikewolfd/spicy-docs/commit/7358ba8d80456ee4e8264fb964734683fab9ad0c)
adds NRA-PVF, 21Wilberforce, Drug Policy Action, UAW and AFT readers. Their source
guides describe the qualified renditions and completeness checks. The current
[package receipt](../../../../../vendor/spicy_docs-scorecards.json) pins each
module, the repeatable wheel build and the unchanged shared PDF checks.
The [prior package receipt](../../../../../vendor/spicy_docs-scorecards-d42c3140cd83.json)
retains the previously adopted inputs.

NRA-PVF reports grades for its explicitly disclosed federal incumbents in named
state and election cohorts. Its JSON endpoint supplies navigation; grades come
from HTML. Wyoming's requested cohort contains no disclosed federal incumbents
and refuses rather than reporting a successful empty scorecard. Independent
review corrected orphan-candidate completeness checks and hidden-comment name
handling; the approved reader preserves the qualified native values.
The downstream resolver recognizes exact chamber and district labels while
preserving their source spelling. Election-date prose remains an unqualified
historical term period, with an explicit unresolved reason for name-only links.

21Wilberforce's revised 116th Congress original PDF reports points, grades,
recognition and narrative scoring criteria. Its missing Senate item ordinals
and conflicting chamber labels remain literal source observations. The reader
does not invent a member action ledger or undisclosed component totals.

Drug Policy Action's archived 2016 House list supplies literal letter grades,
including `I`, and explicit source member identities. Its corrected reader refuses new visible grading or member notes that it cannot preserve; the original qualified fields remain identical. The separate Senate route
and other archived renditions remain pending. UAW's original 2019 Senate record
supplies agreement percentages and item glyphs, including its blank vacancy
context. The source does not designate a primary metric, so `is_primary` is null.

AFT’s archived original 111th Congress PDF supplies member contexts, ordered vote results, explicit publisher positions and complete explanatory fragments. It reports no aggregate score. The reader uses shared PDF validation and private observation retention, preserving metric-free results, literal glyphs and repeated citations. Independent native text and Gemini structured outcomes agree after direct original review; earlier refused comparisons remain private evidence.

The [package adoption](reader_adoption_five_20261004.json) records installed
reader replay and consumer checks. The [qualification ledger](integration_qualifications.json)
and [generated queue](integration_progress.md) distinguish qualified scopes
from published editions. Source and model bodies remain in the private campaign
corpus under `hash_only`; public metadata retains hashes and source locators.
The new readers remain disabled for automatic refresh pending qualification of
their live routes and required private PDF assets.

## Remaining work

- Publish qualified retained inputs through the existing scoped replacement
  pipeline. Confirm public-file and hosted MCP readback before advancing the
  publication ledger; preserve all prior editions.
- Complete the separately recorded Citizens for Global Solutions and
  historical-source qualifications before adopting their readers.
- Refresh `scorecard-analysis` against the accepted source generation and pinned
  published congressional data, then report exact unresolved member and item
  reasons.
- Follow the generated queue for every remaining publisher and rendition.
  Original discovery remains dated evidence; a source refusal does not imply
  retirement or deletion.
- Reconcile the hosted FEC release and runtime pins before deploying revised
  MCP guidance. Package adoption and data publication do not establish a
  container deployment.
