# Congressional ecosystem takeaways: local implementation

The ecosystem recommendations are implemented through the existing source and
publication boundaries. They add discovery and exact joins without creating a
second congressional ingestion pipeline. Package and check evidence is retained
beside this record; no production publication or scheduled refresh is implied.

| Takeaway | Implementation | How to check it |
| --- | --- | --- |
| Reuse GovTrack as discovery evidence | A bounded SpicyDocs reader parses commit-pinned publisher metadata and original URLs. SpicyRegs reconciles exact identities and explicit aliases against the census. CSV ratings are not imported. | See [discovery guide](../../../../scorecard-discovery.md) and [reconciliation](govtrack_discovery/reconciliation.json). Missing licensing remains explicit; repository update dates do not become publication dates. |
| Retain the existing member crosswalk | `Legislator` and `members` now retain `votesmart_id`, `bioguide_previous_json`, and `other_names_json`. Source patches, nulls, ordering and date strings remain literal. | Provider source tests and the installed-provider member publication test exercise both current and historical rosters. |
| Resolve historical identities exactly | Resolver v1.2 uses previous IDs and source-dated aliases with historical term context. Conflicts and all candidates remain visible. Undated name patches do not establish a historical match. | See [resolution evidence](../resolution/README.md). No fuzzy matching or first-candidate selection. |
| Qualify identifier dialects | Typed upstream bill/amendment references map to existing canonical IDs. Calendar-year vote IDs resolve through the pinned official vote date and canonical session. Ambiguous or incomplete IDs remain unresolved. | Resolver cases cover repeated votes, conflicting sessions, missing dates and opaque publisher IDs. |
| Preserve independent civic entities and actions | Existing source tables retain unresolved named people, namespaced identifiers, bill-less committee motions, cosponsorship and multiple actions on one bill. | The analysis integration regression verifies these relationships and unchanged publisher facts against immutable inputs. No new state-data dependency or invented floor vote. |
| Reuse existing official publications | The analysis family pins source scorecards plus existing members, terms, bills, amendments and votes. Member vote and cosponsor tables remain available for later comparisons. | Analysis tests verify every partition, input hash, family identity and source snapshot. No Congress acquisition occurs during resolution. |

The new member fields require a rebuilt `members` publication before hosted
resolution can use them. The [local rebuild](member_rebuild/qualification.json)
now reconciles every member, term and affiliation against the original roster
JSON; its immutable generation is available to local scorecard analysis.
Old artifacts fail with the missing column names rather
than silently reducing identity coverage. The operational scorecard registry
remains disabled until publisher-specific qualification is accepted.

PDF readers use the requested shared Docling/OvisOCR2 extraction interface and
private extraction retention. See the SpicyDocs scorecard source guide for each
qualified or unsupported extraction scope. A parser implementation is not proof
that a particular source capture is faithful or complete.

## Verification and delivery boundary

The [integration receipt](ecosystem_validation.json) records the adopted wheel,
source hashes, reproducible replay and repository checks. The wheel recipe pins
its build backend and records the build environment. Its baseline-preservation
check protects existing FEC source tables while adding scorecard readers.

Full-suite validation also exposed older federal input readers writing directly
to the evidence store. They now use shared `retain_bytes` receipts, so their
existing full-retention generations pass the same strict member verification.
The helper has separate full, hash-only and metadata-only tests. This does not
qualify every legacy pipeline's custom event payloads for restricted retention.

These changes are local and uncommitted. Publisher enablement, rebuilding hosted
member data, production publication and scheduled refresh remain separate from
this completed implementation. The broader publisher backlog remains tracked in
[the adapter task manifest](adapter_tasks.json).

The [parallel continuation](parallel_execution_validation.json) records the
subsequent LCV end-to-end qualification, AFP API qualification, IJM access
blocker, installed reader update, and resolver v1.3 item-year matching.
Its raw-source comparisons and newer package checks supplement this checkpoint.
