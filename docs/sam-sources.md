# SAM notice windows and detailed exclusions

The `sam_sources` pipeline builds ordinary local generations from exact retained
public SAM files. It preserves source rows, repeats, native identifiers, raw
cells and source coordinates. `sam_exclusion_observations` is separate from
`sam_entities.exclusion_status_flag`; the flag describes a registration's
observation time and scope, while the public exclusions file includes parties
without registrations. Use exact UEIs when available, preserve unmatched
parties, and account for multiple registration/EFT rows per UEI. Never infer
name matches or rewrite a registration flag from this table.

`sam_opportunity_observations` is separate from registrations and USAspending
awards. The build selects a declared CGAC agency code and inclusive posted-date
window, reads the complete original, and retains all matching observations.
Notice revisions with the same native ID remain separate. The public file does
not establish all-version history; a rolling filename supplies no snapshot
date, so capture time and source hash identify the observation. Linked documents
remain advertised until independently captured.

Capture one explicitly selected public file through the central raw archive:

```sh
uv run python -m spicy_regs.pipelines.sam_sources acquire \
  --kind exclusions --filename SAM_Exclusions_Public_Extract_V2_26316.ZIP \
  --max-bytes 67108864 --output-dir /owned/capture \
  --capture-out /owned/capture.json
```

For exclusions, the selection JSON is
`{"population":"all-rows-in-public-extract"}`. For a notices file, use
`{"agency_code":"047","posted_from":"2026-11-01","posted_to":"2026-11-07"}`.
Rebuild from the exact captured descriptor without downloading again:

```sh
uv run python -m spicy_regs.pipelines.sam_sources build \
  --kind exclusions --filename SAM_Exclusions_Public_Extract_V2_26316.ZIP \
  --capture /owned/capture.json --selection /owned/selection.json \
  --output-dir /owned/new-build
```

Each generation includes its own population summary. Empty success retains a
zero-row typed table and a summary; malformed, refused or incomplete input
never becomes empty success. Schema, digest and stream failures preserve prior
tables. A build uses a fresh directory and does not select a publication.

The notices CSV has non-UTF-8 bytes. Explicit `--encoding latin-1` provides a
reversible byte mapping; `text_encoding` is part of each row and snapshot
identity. Non-ASCII interpretation remains uncertain. The original bytes and
all cell values survive; no mixed-encoding repair or guessed deadline time zone
is applied. ASCII identifiers/dates can be queried without resolving that
display-text uncertainty. `unqualified_text_fields_json` identifies each affected
native field and `text_interpretation_status` marks diagnostic rows. These fields
are not qualified display names or descriptions. `utf-8-sig` remains the strict default, and an encoding
failure is retained as a refusal.

An exact exclusion lookup reads the reason, agency and dates together:

```sql
SELECT native_id, name_raw, first_name_raw, last_name_raw,
       exclusion_type_raw, excluding_agency_raw, active_date_raw,
       termination_date_raw, additional_comments_raw, uei,
       source_url, source_sha256, member_name, source_locator_json
FROM sam_exclusion_observations
WHERE native_id = ?;
```

Compare generations by native ID and `record_content_sha256`, retaining both
source hashes. A record absent from a later active file is an observed omission;
it is not sufficient evidence of a legal termination. `Indefinite`, blanks,
source comments directing verification with an agency contact, and incomplete
names remain literal facts, rather than invented dates or legal conclusions.

A deadline query can keep ties explicit and retain raw deadlines:

```sql
WITH latest_posting AS (
  SELECT *, dense_rank() OVER (
    PARTITION BY native_id ORDER BY posted_date_raw DESC
  ) AS posting_rank
  FROM sam_opportunity_observations
)
SELECT native_id, title_raw, solicitation_number, office_raw, notice_type_raw,
       posted_date_raw, response_deadline_raw, source_link,
       count(*) OVER (PARTITION BY native_id) AS tied_latest_rows,
       source_sha256, source_locator_json
FROM latest_posting
WHERE posting_rank = 1
  AND try_cast(substr(response_deadline_raw, 1, 10) AS DATE)
      BETWEEN DATE '2026-11-13' AND DATE '2026-12-31'
ORDER BY response_deadline_raw, native_id;
```

This uses the source calendar date. It does not infer a UTC instant from a
deadline without a time zone. Report missing/unrecognized deadlines separately
when reconciling the declared window. Local generation validation and a working
query do not establish shared publication or hosted adoption; the publication
owner performs those steps after integration.

Both SAM families are registered with the maintained `publish-local-generation`
command. Rehearse an exact candidate and its pinned source evidence against a
saved index, using a fresh private output directory:

```sh
uv run publish-local-generation --family sam-exclusions \
  --generation /owned/new-build/generation \
  --evidence /owned/new-build/source-evidence/BUILD_ID/artifact \
  --dry-run --live-index /owned/saved-publication.v2.json \
  --output /owned/exclusions-publication-rehearsal
```

Use `sam-opportunities` for notices. The dry run retains the real publication
guards and writes only a private receipt; missing or mismatched source evidence
and a generation classified as partial refuse. A successful rehearsal preserves
all description qualifications and does not qualify diagnostic Latin-1 text or
establish public adoption.
