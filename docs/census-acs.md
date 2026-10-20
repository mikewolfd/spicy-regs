# ACS county comparisons

`census_acs_county_observations` compares population and median household income
for a declared county set in one American Community Survey (ACS) five-year
vintage. SpicyDocs owns the original responses, variable metadata, request
partitioning and source release. SpicyRegs reads that verified release and writes
an ordinary generation using the existing source-evidence and publication paths.

The first planned delivery is the 2020–2024 ACS five-year period (`vintage=2024`),
Delaware (`state=10`), counties `001` (Kent), `003` (New Castle) and `005` (Sussex).
This complete declared county set comes from the
[Census Delaware county roster](https://www2.census.gov/geo/docs/reference/codes/files/st10_de_cou.txt),
retained with SHA-256
`adae760bf82bbfdb78d4d5325187ae25a221df00f518c2c20a7aed755684ec11`.
Completeness is checked against that set, never inferred from returned rows.
Other vintages, states, districts and measures are outside this pilot.

Local synthetic source-release, publication and query checks are complete.
Genuine API data capture and public selection remain pending. A local fixture
comparison is not evidence that Census data or this table has been published.
The SpicyDocs source implementation must also be adopted through the repository's
normal dependency packaging before a release; a development `PYTHONPATH` is only
integration evidence.

## Values and identifiers

The selected variables are `B01003_001E/M/EA/MA` (population) and
`B19013_001E/M/EA/MA` (median household income). Population uses persons; income
uses US dollars adjusted to the selected vintage. The full original variable
metadata remains in each row. Margins of error use the same units as their
estimates and the ACS 90% confidence level, as described in the
[Census guidance on estimates and margins of error](https://www2.census.gov/about/training-workshops/2026/2026-03-11-using-acs-estimates-margins-of-error-transcript.pdf).
A ranking of estimates alone does not establish a statistically significant
difference.

Exact decimal values never pass through floating point. Null, empty and Census
sentinel values remain distinct, with the original value and annotation alongside
the interpreted status. The
[Census annotation definitions](https://www.census.gov/data/developers/data-sets/acs-1year/notes-on-acs-estimate-and-annotation-values.html)
define the sentinel meanings and open-ended median intervals. A numeric median
annotation such as `250,000+` marks a lower bound; a numeric median followed by
`-` marks an upper bound. A standalone `-` is not interpreted as a numeric bound.
Unrecognized annotations remain visible. Controlled-estimate margins remain
explicit instead of silently becoming a reported zero.

County codes retain leading zeros. The county GEOID joins the two-character
state code and three-character county code. This composition reuses the County
row exported by RefSpec's maintained Census reader, pinned in
`src/spicy_regs/reference/refspec/census-county-geoid.json`. It defines the code
structure, not the county roster. The original Census `GEO_ID` and the reference
digest remain in every observation.

## Replay and query

The build command reads local retained evidence only. Pass the admitted source
release identity and verifier identity explicitly:

```sh
python -m spicy_regs.pipelines.census_acs \
  --source-release "$ACS_RELEASE" --source-blobs "$ACS_BLOBS" \
  --source-logical-id "$ACS_LOGICAL_ID" --source-digest "$ACS_DIGEST" \
  --verifier-implementation-id "$ACS_VERIFIER" \
  --vintage 2024 --state 10 --counties 001 003 005 \
  --output-dir "$ACS_OUTPUT"
```

Use a fresh output directory for each build. Missing, repeated or mismatched
counties refuse the output. The command emits the ordinary generation and
source-evidence paths and exact generation identity. The generation can use
`sources.publication.publish_generation`, supplying its source-evidence directory;
the build command does not publish it. Source refresh stays in the maintained
SpicyDocs acquisition and replay workflow.

Point the existing local SQL consumer's `SPICY_REGS_DATA_DIR` at the resulting
`generation/` directory. This query shows income comparisons, uncertainty and
the exact source request:

```sql
SELECT county_geoid, name, estimate, margin_of_error, unit,
       estimate_status, margin_status,
       json_extract_string(value_sources_json, '$.B19013_001E.request_url') AS source_url,
       source_release_digest
FROM census_acs_county_observations
WHERE vintage = 2024 AND state_code = '10'
  AND measure = 'median_household_income'
ORDER BY estimate DESC NULLS LAST;
```

The table retains the source record ID, release digest, literal estimate and
margin cells, all annotations, and per-variable response hashes and row/column
locators. The bounded original source archive is retained once through the
existing `CaptureEvidence` owner. No additional downloader, query engine or cache
is introduced.
