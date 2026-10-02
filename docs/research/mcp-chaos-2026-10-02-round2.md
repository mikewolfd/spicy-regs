# Blind MCP research round 2, October 2, 2026

Decision: test the released service with fresh research personas, then independently check their answers. A successful query is not proof of a correct answer. Keep source meaning, missing coverage, and unverified checks explicit.

The five personas completed their research without service errors or timeouts. Independent audits found no demonstrated substantive error in their qualified headline answers. They also confirmed a printing-provenance defect below the MCP layer: selected records labeled as public laws reuse enrolled-bill captures. The Congress persona noticed the mismatch and did not treat the labels as independent source evidence.

## Research results

| Question | Result and independent check |
| --- | --- |
| Compare individual contributions to Ohio's 2024 Senate candidates | The reported two-year summary figures and difference agree with independently read Parquet and official FEC CSV. Current/net totals, donors and matching itemized coverage remain unqualified. Some ancillary receipt checks exceeded the audit bound. |
| Find October EPA pesticide participation deadlines | The provisional shortlist's identities and Eastern dates reproduce. Official HTML adds an October 5 Isofetamid objection deadline where structured close fields are empty, and confirms a docket-spelling inconsistency in the publisher notice itself. The persona had flagged the missing deadline and did not claim a complete filing calendar. |
| Identify enacted PFAS provisions and bipartisan backing in the 118th Congress | The selected laws, dates, provisions, vote totals and party breakdowns reproduce. Official law XML and selected official roll calls corroborate them. Sponsorship, package votes and provision-specific support remain distinct. The printing-provenance defect is real. |
| Trace net-neutrality proceedings, filings and litigation | Core chronology and held filing counts reproduce; official GPO HTML corroborates the correction and later cleanup. Historical filings, opinion bodies and current legal status remain incomplete. Some independent court-cluster checks timed out. |
| Compare court references around Loper Bright | All headline counts and percentages reproduce over the selected retained cohort. Citation and parenthetical channels do not measure legal treatment or complete opinion text. Publisher pages returned challenges, and some ancillary metadata remains unverified. |

The prior EPA date and legal-purpose errors did not recur in the exercised workflow. Minor persona bookkeeping errors about tool/description counts are corrected in the local findings; they do not change research conclusions.

## Confirmed printing-provenance defect

Independent checks cover H.R. 2670, H.R. 3935 and H.R. 5009 from the 118th Congress. Their `public-law` / `Public Law` records combine an enrolled BILLS package, `Enrolled-Bill` publisher stage and enrolled digest with an offered distinct PLAW resource. For H.R. 5009, the measured official enrolled XML digest exactly matches both stored labels; the accessible official law XML has a different digest. This establishes different resources, not different legal effect or a demonstrated statutory-text error.

The read-only trace finds two connected causes:

- SpicyDocs maps `public-law` to the enrolled `enr` acquisition suffix.
- SpicyRegs falls back to that derived package and attaches one bulk member to both printing identities. An existing test explicitly expects the substitution.

The selected publication's producer metadata and adopted wheel still contain this behavior. It is not an already-fixed defect waiting for a Worker rollout. The actual law XML in the tested counterexample is refused by the current bill-tree parser; enrolled XML parses successfully. Successful parsing and matching row counts therefore do not establish the right source identity.

The independent reviewer supports correcting source selection, attachment and retained state. The minimum repair should preserve valid enrolled text and honest law-listing metadata, stop the substitution, and replace only proven contaminated records and dependent outputs. Merely changing a label or deploying a server would conceal the problem. Ordinary reruns also preserve complete prior rows, so the repair must explicitly address retained state.

The blind round did not measure the affected population; the implementation follow-up below does. Supporting actual law-section acquisition can follow separately; no compatibility layer or blanket reacquisition is required. SpicyDocs' bills lane owns source policy; SpicyRegs' publication lane owns adoption, scoped replacement and coherent generation publication.

## Follow-up: local red/green correction

The user authorized the repair after the blind round. SpicyDocs branch commit
`11189d72bd29b542874c2081d8dad5e57cd24333` removes the law-to-enrolled suffix
and selects only supported, consistent BILLS resources. The vendored candidate
wheel is built twice from that committed archive with identical bytes; it is
not a registry release or a merge to the provider's main branch.

SpicyRegs uses that source policy when listing printings, planning body reads
and attaching bulk members. Per-package captures must also identify the
selected resource at both requested and resolved URLs. Law listings retain
their offered URLs, labels and dates while acquisition fields stay NULL.

The held-state repair uses the existing scoped replacement mechanism. It
replaces law rows carrying BILLS targets and removes their sections and
incident derived rows, including orphan comparison children. The requested
Congress and bill type bound the repair. Valid enrolled rows and printings
outside that scope remain unchanged; a second run performs no further repair.

The pinned `82c8088d…` inventory measures **4,271** affected law rows, including
**3,047** acquired bodies and **1,224** unacquired listings with false package
metadata. They have **164,270** sections, **2,523** incident comparisons and
**164,738** comparison items. The other inspected derived tables are empty.
The exact keys, source pins and queries are in `repair/inventory/`, under the
evidence root below; private-law listings without BILLS targets are outside
the measured repair.

Validation separates code from publication:

- RED reproduces acquisition of enrolled bytes under a law label, preservation
  of stale complete rows, and attachment of a response from another resource.
- GREEN exercises acquisition, unchanged-status repair, orphan children,
  exact dependent-row removal, enrolled controls, scope limits and idempotence.
  Process-local mutations that disable attachment or held-state protection
  make the regression tests fail.
- The complete pinned versions and comparison-header tables were repaired
  locally using the production shaper and merge. All **225,893** version rows
  remain, offered law facts agree, acquisition claims are cleared, and all
  other printings are logically identical. A second target scan finds no
  remaining repairs. See `repair/real-metadata/receipt.json`.
- The provider gate, consumer gate and review evidence are retained under
  `repair/source/` and `repair/host/`. Review caught and resolved a format field
  that falsely suggested an unacquired rendition had been read.

This is a local correction. Section/member rewriting and coherent generation
publication remain operational work; the public service has not adopted the
repaired data. The local metadata files alone are not a publishable family.

## Method, cost and verification limits

The starting and ending service used Worker `799e61eb-11f5-4fa9-b367-ce5d2194fda9` and container version 29. All prior controls passed both before and after the research: **55 cases and 66 explicit assertions per replay**. Those controls protect existing behavior; they do not establish deployed acceptance of the printing repair.

The personas made **94 completed service calls**, with no tool errors or transport timeouts. Seven calls exceeded ten seconds; the longest was 67.286 seconds. Initial baseline discovery took 71.84 seconds. These are client/session/network measurements after lock acquisition, not isolated execution profiles. Nineteen responses exceeded 32 KiB; broad descriptions and selected nested/text values remain cumbersome. Wire size does not establish token consumption or a particular client's spill behavior.

Each persona was fresh and blind to source code, previous findings and other reports. One used native delegation; the others used isolated OpenCode sessions after the native thread limit. All used the current model at medium effort. This varies personas, not model capability, and does not simulate actual novice users experimentally. The four OpenCode traces contain only the allowed transport and own-response pagination commands. Native execution has retained service wires and its scope attestation.

Five separate auditors wrote fresh SQL over the same pinned public Parquet and checked selected official HTML, XML or CSV. Five direct audit queries were interrupted at the specified bound and were not retried. Their unresolved results remain unverified, not empty. Access challenges were not bypassed. No PDFs were acquired or processed.

Federal Register, CFR and FR docket-link publication pointers advanced during the round. Audits used the exact generations observed by their personas; the unchanged Worker does not imply a common frozen data snapshot. No data, ETL schedule or deployed service was changed by this round.

Evidence is retained locally under `/Users/mikewolfd/Work/corpora/mcp-chaos-2026-10-02/round2/`: persona reports and wires, five audits, baseline and final replays, target snapshots, `phase1-findings.md`, `scout-provenance.md`, `phase2-analysis.md`, `phase3-review.md` and `repair-handoff.md`. Local verification of this write-up is recorded in `documentation-gate/`. Original reports remain unchanged; findings and handoff incorporate audit corrections and reviewer constraints.
