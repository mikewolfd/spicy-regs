# Separate campaign-finance and lobbying comparisons

The sector builder consumes accepted `fec_financial_profiles` and the lobbying
lane's qualified `lobbying_quarterly_profiles`, with its native activities.
It verifies exact prerequisite generation and table pins. It neither acquires
lobbying data nor reconstructs quarterly totals. Different inputs require a new
selection and replay.

The initial window is calendar 2025. FEC selection includes committee profiles
whose reported start/end exactly match that year; other periods are explicitly
outside the population. LDA selection retains the lobbying lane's declared
registrants and quarters, including unresolved and missing-quarter limitations.
Each measure is separate: FEC receipts, FEC disbursements, registrant income and
in-house expenses. Known selected LDA amounts are not complete annual totals.
Income and expenses can overlap and are never added together.

`sector_classification_evidence` records versioned curator assignments to existing
NAICS 2022 sectors, whose definitions are owned by RefSpec's
`naics_psc_codes` reader. Code definitions do not classify organizations. The
selected mapping instead cites FEC disclosed organization context plus primary
business descriptions, or LDA native client business descriptions. It preserves
method, applicable financial period, review date and uncertainty. Broad curator
context is not an official NAICS assignment or historical corporate ownership.

The initial comparisons use Health Care and Social Assistance and Other Services
(except Public Administration). MOGA is classified by its trade-association
activity, not automatically as an oil producer because it represents that industry.
Unmapped profiles remain unclassified; competing labels remain uncertain. A
similar GE name is retained as an unresolved proposed association and never joins
money across sources. No cross-source organization identity or causal connection
is inferred.

`sector_financial_comparisons` reports known amounts and denominators for every
source, sector and measure. Null means no qualified measure, not zero. Duplicate
profile IDs, unsupported periods, wrong FEC populations, missing business evidence
and name/issue-code-only classification refuse. Mapping changes move classification
without changing the source financial measures.

`lobbying_issue_financial_observations` is separate from industry classification.
It exposes each selected whole filing at most once per native issue, regardless
of repeated activities. Multi-issue views overlap, do not allocate filing money
by issue, and cannot be added across issues.

```sh
uv run python -m spicy_regs.pipelines.sector_comparisons \
  --selection /work/sector-selection.json --output-dir /work/new-sector-candidate
```

```sql
SELECT sector_label, source_family, measure, known_reported_amount,
       qualified_measure_profiles, unclassified_population_profiles
FROM sector_financial_comparisons
WHERE sector_code IN ('62', '81');
```

The generation and SpicyGov panel preserve source versions, cutoffs, populations,
unknown coverage and classification evidence. Local comparisons do not establish
a national sector total or public acceptance.
