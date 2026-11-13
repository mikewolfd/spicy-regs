# Query GovInfo preservation evidence and native budget files

Use the GovInfo body acquirer for normal document reads. For a metadata-only
read of a selected committee report, hearing or budget package, call
`RetainedGovInfoBodyAcquirer.acquire_preservation` with `CaptureEvidence`.
Select budget granule pages with `RetainedGovInfoDiscoveryReader.granules`.
Use `budget_table_offers` to choose a publisher-stated native file and
`acquire_budget_table_file` to retain it. A package without a readable root
document can still offer a workbook.

Query selected original responses without fetching them again.

```sh
python -m spicy_regs.govinfo_evidence BUDGET-2026-TAB selection.json \
  --budget --sheet hist01z1
```

The selection is a JSON object keyed by `summary`, `mods`, `premis`,
`closing_summary` and, optionally, `file`. Each value has `path`, `sha256`
and `entry`. These are the exact journal path, its prefixed SHA-256 and the
one-based capture entry. For a budget granule walk, `granule_pages` contains
these same references in request order. Use `--page-size` for a walk
captured with a nondefault page size and `--granule-id` for a file belonging
to a stated constituent. Omit `--budget` to query preservation evidence and
an optional selected file for a report or hearing.

The command checks each selected journal and original file before reading
source values. It returns issue dates and publisher update times from
SUMMARY, descriptive dates and relationships from MODS, preservation
objects and events from PREMIS, and separate capture times and source
locators. Optional
`historical_summary_sha256` and `historical_mods_sha256` compare an
observation with an already selected dated state. The comparison reports
exact metadata, current-only metadata, or an unproved historical match.
A PREMIS digest match identifies bytes for one publisher location. Each
source retains its own observation time.

Budget output shows package and granule IDs, fiscal year, root document
formats and native file offers. A fully checked empty listing stays
observed-empty. An unavailable listing has its own status. Missing
continuation pages refuse. The query returns table cells when the file
matches one stated offer, fell inside an unchanged SUMMARY bracket with
a usable update time, and matches the publisher's digest. Otherwise the
result gives the file receipt and the reason the table is held.

Workbook cells keep their positions, values, formulas, number formats and
merged ranges. Printed units and footnotes remain in their original cells.
CSV rows keep their original width and repeated values. The existing
`budget_volumes` table stays at the volume grain.
