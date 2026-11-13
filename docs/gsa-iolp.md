# Query a declared GSA property and lease snapshot

The `gsa-iolp` family exposes a complete caller-selected Inventory of Owned and
Leased Properties (IOLP) pair. It supplies property observations, lease
observations and a snapshot reconciliation table through the ordinary generation,
publication, managed download and MCP `query_sql` paths. IOLP covers
GSA-controlled property; it is not the government-wide Federal Real Property
Profile (FRPP).

The source is the [GSA IOLP catalog record](https://catalog.data.gov/dataset/inventory-of-owned-and-leased-properties-iolp).
SpicyDocs owns catalog selection, bounded acquisition and complete literal XLSX
reading. This mapping reuses its maintained workbook reader and the existing
SpicyRegs source evidence and Parquet generation builders. No new parser package
or ingestion framework is needed. The selected metadata declares CC0.

## Source meaning

`gsa_iolp_properties` has one row per snapshot and native `Location Code`,
including source-owned and source-leased property. `gsa_iolp_leases` has one row
per snapshot and native `Lease Number`. Several leases can refer to a property.
The `gsa_iolp_property_leases` view links them by exact `snapshot_id` and
`location_code`, retaining unmatched leases. No name or address matching occurs.
The snapshot table records the observed populations and unmatched location count.

Addresses describe the source building. They are not geocoded or standardized.
ZIP codes, regions and congressional district codes remain text with leading
zeros. Source district vintage and coordinate datum are unstated. Coordinates
remain literal decimal degrees. `location_status` separates missing fields,
invalid coordinates, bounded reviewed conflicts and present but unvalidated
locations. A null `location_issue` does not establish geographic correctness.

Area fields retain exact source text in square feet. Available space nulls stay
unknown; a source zero stays zero. Building area on lease rows can repeat across
leases and must not be added as independent leased area. Construction dates stay
literal years. Parsed lease dates retain their source text and parsing status;
they do not establish current occupancy or active/terminated status. Source
`N/A` stays text. `raw_cells_json` and `value_states_json` distinguish omitted
cells, explicit nulls, empty text and values. Unknown columns survive.

The snapshot date is the date in the selected distribution filenames. Catalog
modification and workbook capture times remain separate. `data_as_of` stays null
because the publisher did not establish an effective inventory instant.

## Refresh and replay

Capture only missing selected originals after checking retained holdings. The
source reader accepts the current XLSX distributions even though the metadata
description mentions CSV. Select a complete pair with matching filename dates;
the older XML advertised beside that pair is outside the selected snapshot.

```sh
uv run --frozen python -m spicy_regs.pipelines.gsa_iolp acquire \
  --catalog-url 'https://catalog.data.gov/harvest_record/c3262d4b-9c55-452d-b714-e8a38acdb993/raw' \
  --snapshot-date 2026-11-06 --output-dir acquisition --captures-out captures.json
uv run --frozen python -m spicy_regs.pipelines.gsa_iolp build \
  --captures captures.json --output-dir candidate
```

The build replays exact raw archive originals and acquires nothing. Each build
requires a fresh directory. Required-header changes, repeated/missing keys,
formulas, source errors and byte changes refuse the complete candidate. A
missing address value remains a row with an explicit location state. A
header-only source remains observed-empty; publication guards still apply.

A refresh replaces the selected family with another complete pair; it never
edits the earlier immutable generation or its retained originals. Missing rows
do not prove a disposal, lease termination or publisher deletion. Use
`--read-index` when comparing with an existing publication, and pass the built
generation and evidence to the maintained `publish-local-generation` command.
The coordinator owns integration and shared publication selection. Builds alone
do not make these tables public.

## Consumer examples

These queries run through the normal MCP `query_sql` tool after selecting the
family's managed generation. Each returns original workbook evidence.

```sql
SELECT snapshot_date, location_code, asset_name, ownership,
       street_address, city, state, zip_code,
       rentable_square_feet_raw, area_unit, location_status, source_locator_json
FROM gsa_iolp_properties
WHERE snapshot_date = DATE '2026-11-06' AND state = 'DC' AND ownership = 'owned'
ORDER BY location_code;

SELECT snapshot_date, lease_number, location_code, asset_name,
       street_address, city, state, zip_code,
       lease_effective_date, lease_expiration_date, property_matched,
       property_source_locator_json, lease_source_locator_json
FROM gsa_iolp_property_leases
WHERE snapshot_date = DATE '2026-11-06' AND state = 'CT' AND city = 'NEW LONDON'
ORDER BY lease_number;
```

For the declared November 6 pair, original lease row 2 states `LCT00005`,
location `CT3414`, `2 UNION PLAZA`, `2 UNION PLZ`, New London, Connecticut,
ZIP `06320-6309`, effective July 27, 2019 and expiration July 26, 2034.
Its building rentable area is `7276` square feet and available space is null.
Those are source observations, not a statement that the lease is active today.

## Bounded original review and qualification

The source qualification and native workbook images are retained in
`output/source-completion-20261113/lanes/gsa-property/` at the workspace root.
See `RAW-VISUAL-REVIEW.md`, `HANDOFF.md` and `handoff.json` there for exact source
paths, image locators, tested population, final commits and consumer/failure logs.
The review used LibreOffice rendering of checksum-verified original workbooks,
not regenerated domain tables.

The selected buildings original is SHA-256
`b4218dd3debed861c9f416160ff250a58bd186d5a7215c7e1f41f3f37636dc89`;
leases is
`1e735434fa9b684f2edf189edf604785886bef7034967347dab2c84893b2c36d`.
The measured reconciliation on November 13, 2026 found 7,733 property rows
(1,655 owned and 6,078 leased) and 7,075 lease rows. These measured counts are
controls for this exact pair, not requirements for later snapshots.

The original sample shows apparent location conflicts for `AZ0715` (Arizona
address with Texas coordinates/district) and `SC1001` (South Carolina address
with coordinates near Mississippi). No authoritative corrected values were
established. The mapping retains every original value and marks these exact-byte
observations `source_conflict`; the findings remain unresolved. Other records
remain explicitly unvalidated. The sample does not qualify all geography,
addresses, districts, area measurements or completeness of GSA's upstream
inventory. Public publication and independent coordinator review remain separate
acceptance steps.
