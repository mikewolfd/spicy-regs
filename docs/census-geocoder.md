# Census geography resolution

`census_geocoder_lookups` answers what happened to each selected structure address
or coordinate. `census_geocoder_geographies` gives every returned geography, its
candidate, and an exact pointer into the retained original response. The source
reader and retained release belong to SpicyDocs; these ordinary tables and their
local generation belong to SpicyRegs. ACS estimates are a separate source.

The supported version pair is `Public_AR_Census2020` / `Census2020_Census2020`.
Coordinate values use exact decimal strings in longitude/latitude degrees, codes
keep leading zeros, and every row retains the source release pin and original
response digest. Complete response JSON preserves native nulls, empty values,
unknown fields and number spellings. `geography_json` is a derived convenience
field whose decimal numbers use exact strings. County composition reuses the
pinned RefSpec Census guidance; it does not supply a county entity roster.

Read `outcome` first. Only `matched` establishes a single supported candidate.
`ambiguous` preserves all candidates with `unique_assignment=false`. `unmatched`,
`refused` and `conversion_failed` remain in the lookup table and have no geography
rows. Transport errors raise and retain their available failure evidence; they do
not create an empty lookup. No ZIP-centroid approximation is used. Address points
are calculated along address ranges, so native precision does not establish
rooftop accuracy. Do not treat historical district layers as current boundaries.

## Build and query

Create an input JSON array containing one exact pin per retained SpicyDocs lookup:

```json
[{"release":"/absolute/path/to/release","blobs":"/absolute/path/to/blobs",
  "logical_id":"SOURCE_LOGICAL_ID","digest":"sha256:SOURCE_ARTIFACT_DIGEST",
  "verifier_implementation_id":"SOURCE_VERIFIER_IMPLEMENTATION_ID"}]
```

The maintained offline builder admits each release with `GEOCODER_PROFILE`, checks
the complete cohort, retains original responses through `CaptureEvidence`, and
builds the existing generation format. Supply a fresh output directory. Missing,
repeated or changed lookup identities refuse the build. The command performs no
network acquisition and changes no public selection:

```sh
uv run --frozen python -m spicy_regs.pipelines.census_geocoder \
  --inputs "$GEOCODER_INPUTS" --output-dir "$GEOCODER_OUTPUT"
```

Point the ordinary SQL consumer's `SPICY_REGS_DATA_DIR` at the returned `generation`
directory. This answers which county, tract and block the exact selected lookup
returned, with a reproducible source locator:

```sql
SELECT l.address, l.input_x, l.input_y, l.outcome,
       g.layer, g.geoid, g.name, g.x, g.y,
       g.benchmark, g.vintage, g.request_url,
       g.response_sha256, g.json_pointer
FROM census_geocoder_lookups l
LEFT JOIN census_geocoder_geographies g USING (lookup_id)
WHERE g.unique_assignment
  AND g.layer IN ('Counties', 'Census Tracts', 'Census Blocks')
ORDER BY l.lookup_id, g.layer;
```

Inspect nonmatches and refusals independently:

```sql
SELECT address, input_x, input_y, outcome, reason, response_sha256
FROM census_geocoder_lookups
WHERE outcome <> 'matched';
```

Replay uses exact local release inputs without downloading originals again.
Refresh uses an explicit new SpicyDocs acquisition, release, input-pin array and
fresh candidate directory; a repeated request is never silently called unchanged.
The generation can use `sources.publication.publish_generation` with its source
evidence directory. Local in-memory publication rehearsal proves that format;
public publication and selection remain separate acceptance steps.

The 2026-11-13 local genuine cohort resolves the Berkeley structure address and
nearby coordinate to Alameda County (`06` + `001` = `06001`), with separate tract
and block assignments. Native unmatched address and ocean-coordinate observations
are retained too. The source lane's `HANDOFF.md` links exact originals, raw visual
review, SQL output and final pins. Broader geographic/version coverage, batch CSV,
Puerto Rico `addressPR` and ACS estimates remain outside this route. Integration
must adopt the source implementation through normal dependency packaging before
public delivery; a development `PYTHONPATH` proves local coordination only.
