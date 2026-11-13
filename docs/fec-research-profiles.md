# Bounded FEC research outputs

The new profiles extend existing FEC readers, relationship mapping, exact-money
field mappings and financial membership selection. They do not replace the FEC
ingestion path or qualify every retained transaction as current or additive.
Builds read selected originals and write a fresh local generation with source
evidence. Publication uses the existing generation and local-publication routes.

## Committee disclosures

`fec_committee_relationship_profiles` preserves each selected native committee
occurrence. The maintained `api_relationships` mapping retains every affiliation
and sponsor field, missing/null/empty distinctions, multiplicity and unresolved
names. Sponsor-ID and sponsor-object arrays remain separate source witnesses.
Names never become IDs. Source retrieval and filing dates do not establish a
relationship effective date, ownership, a transfer or candidate authorization.

The initial selection names retained OpenFEC pages of the activity-cycle query
in `profile-selection.json`. It is a page selection, not a committee census or
a historical affiliation timeline. Conflicting snapshots remain separate profiles.

## Contributions

`fec_contribution_profiles` and `fec_contribution_decisions` cover recipient
committee `C00921817`, effective filing `1943202`, in cycle 2026. The report
covers October–December 2025. Its native title says Senate; later committee
history also includes a House candidacy. The declared Senate association is
`S6AL00484`, supported by native committee linkage and candidate observations
predating the report. A later House observation fails the period control.
First/last filing dates are not daily authorization dates.

The official report's `most_recent`, `is_amended`, `most_recent_file_number`
and `amendment_chain` must identify the retained filing. Unknown successor files
or changed official amounts refuse. The complete original is decoded by the
source reader and mapped by the existing filing definitions. Native transaction
IDs qualify membership through `select_scope`; repeated keys refuse. The existing
selection tests cover replacements, deletions and equivalent representations.

Only recipient-side Schedule A receipts and matching Schedule B refunds enter
the calculation. Memo entries remain queryable and excluded. Positive receipts,
negative receipt adjustments and refunds remain distinct. Gross positive receipts
plus signed adjustments reproduce reported itemized receipts; refunds reproduce
their matching source report measure. Unitemized amounts remain separate and
are never attributed to named itemized donors. PAC IDs group PAC observations;
individual states and employer labels do not establish people or employer gifts.

The selected report includes September transaction dates. Those records remain
in the report population and explicitly say `outside_report_coverage`; report
coverage is not a transaction-date filter. Missing optional trailing fields
remain missing. The original acquisition time remains unknown. The independently
captured official report supplies reconciliation and effective-report evidence,
not a fabricated historical download time.

The source-reconciled example reports PAC receipts of $12,000 and individual
itemized receipts of $80,188.58, with $500 of individual refunds. These figures
describe this named report. They are not complete-cycle contribution totals.

## Independent spending

`fec_outside_spending_profiles` and `fec_outside_spending_spenders` consume the
publisher's `/schedules/schedule_e/by_candidate/` and
`/schedules/schedule_e/totals/by_candidate/` aggregates for candidate
`S6OH00163`, cycle 2024. Complete observed page/count membership is checked.
Each candidate/cycle/support/oppose group must reconcile exactly to its separate
official total. Repeated native aggregate keys refuse; raw transactions and
alternative captures are never added to the publisher measures.

The [publisher methodology](https://github.com/fecgov/openFEC/blob/develop/webservices/docs.py)
states that these aggregates omit memo items and 24/48-hour notices. The native
aggregate does not give individual election codes or event-date extrema. Those
limits remain explicit; a specific-election request refuses. This is an observed
publisher snapshot, not a frozen all-outside-spending census. Spenders count
distinct source filer IDs, including separate Form 5 identity namespaces; native
transaction counts are not spender counts.

## Official Senate results

`fec_election_workbook_rows` retains every literal row of the official Federal
Elections 2022 workbook, including House, summaries and appendices.
`fec_election_results` interprets the reviewed Senate sheet into separate native
vote-column observations. Full and unexpired terms, primary/general/runoff,
ranked-choice rounds, zero votes, write-ins, source text and footnotes survive.
Candidate IDs stay separate from unresolved write-in/aggregate identities.

The general ballot-line column can reproduce a contest total. Combined-party
totals repeat across party lines and must not be added. Ranked-choice rounds
overlap each other and the reported general column. The appendix is retained
as overlapping source evidence, not an additional set of votes. Other editions
and interpretation of House/appendix layouts remain outside this named scope.

## Build and inspect

Run Python through `uv run` in the coordinated source environment:

```sh
uv run python -m spicy_regs.pipelines.fec_research_profiles \
  --selection /work/profile-selection.json --output-dir /work/new-profiles
uv run python -m spicy_regs.pipelines.fec_election_results \
  --capture /work/workbook-capture.json --output-dir /work/new-results
```

Both commands refuse existing output directories and mismatched input bytes.
Keep failed attempts and prior generations; replay uses the same retained
originals in a new directory. The election evidence records actual reader-code
digests separately from installed package metadata. Coordinated source-branch
proof does not establish a released source wheel or public serving acceptance.

Ordinary queries include:

```sql
SELECT contribution_family, candidate_id, signed_itemized_receipts,
       itemized_refunds, net_itemized_flow, period_start, period_end
FROM fec_contribution_profiles;

SELECT support_oppose, total, reported_spender_count, election_basis
FROM fec_outside_spending_profiles;

SELECT candidate_name, party, votes, source_row, source_column
FROM fec_election_results
WHERE state = 'NY' AND stage = 'general' AND round = 'reported'
  AND row_kind = 'candidate';
```

SpicyGov presents these decisions through its normal metadata and Parquet reader.
Local generation, MCP, private-publication and browser checks establish bounded
local delivery. The coordinator owns package integration, independent review,
public selection and hosted acceptance. Existing `ss-42nr`/`ss-u9s8` correction
limits and the whole-file FEC metadata interpretation binding remain in force.
