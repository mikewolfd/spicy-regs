# Enrich a selected CRS metadata revision

`build_crs_reports` keeps `(report_id, update_date)` as the report revision key.
Its ordinary list refresh preserves qualified detail and source witnesses when
that coordinate and the shared metadata agree. An explicit `detail_limit` adds a
bounded detail pass; the scheduled rollup continues to request list metadata.

```python
from spicy_regs.transforms.build_crs_reports import build_crs_reports

build_crs_reports(output_dir, detail_limit=10)
```

For a prepared local prior, `refresh_list=False` reads only that prior and the
selected detail responses. It requires a positive detail limit. `detail_reports`
names an explicit cohort as `ID@timestamp` strings. Bare IDs require `latest=True`;
unknown IDs, unknown revisions, duplicate picks and malformed timestamps refuse
before inherited receipt restoration. The declared detail limit must admit every
explicit selection; it never silently truncates that cohort. An exact selection
can enrich an older retained revision. Every unselected revision survives.

Supply
`retained_details={report_id: (original_path, "sha256:...")}` to reuse explicitly
pinned original JSON with no origin fallback. Otherwise the configured
api.data.gov key authorizes the live detail route. The detail pass admits at most
100 selected report revisions per call and bounds each response to 2 MiB. Without
`detail_reports`, the existing eligible latest-revision prefix remains supported.
Explicit detail cohorts and list acquisition are separate operations.

The builder admits the complete inherited subject/receipt pair before selecting
or restoring receipts: stored bytes, Parquet uncompressed column bytes and both
member row counts must fit `max_prior_bytes` and `max_prior_rows`. Receipt-held
processing input and restoration scratch are included in that decoded input
budget. The merge still holds the admitted revision table in memory and writes a
replacement; reserve space for that output and recovery. `max_requests` charges
all origin attempts, including retries and redirects, before sending them.
`max_list_bytes` bounds cumulative received list bytes and `max_list_rows` bounds
observed revisions admitted into the merge. Limit failures preserve the prior.
These limits bound work; Parquet decoded bytes are not a measurement of process
memory.

## Prepare a local detail cohort

The installed module command uses `build_crs_reports`, `CaptureEvidence` and the
shared native receipt writer. It requires an explicitly selected local native
table, matching receipts and receipt generation. It writes a separate local
candidate and never uploads, selects a release or activates a schedule.

```bash
python -m spicy_regs.crs_report_operations /work/crs-candidate \
  --operation detail \
  --prior-subject /work/selected/crs_reports.parquet \
  --prior-receipts /work/selected/etl_receipts.parquet \
  --prior-generation-id selected-generation \
  --report IN12418@2026-09-19T16:37:49Z \
  --max-details 1 --max-requests 0 \
  --max-prior-bytes 134217728 --max-prior-rows 100000 \
  --retained-details /work/retained-details.json
```

`retained-details.json` is an array of `{ "report_id": "IN12418", "path":
"/exact/original.json", "sha256": "sha256:..." }` entries. Paths may also be
relative to this manifest. Entries must be unique, and each original must fit
2 MiB and match its digest when read. For multiple reports, repeat `--report`.
Use `--latest` with bare report IDs to choose the newest admitted revision.
`--retry-completed` explicitly rereads a qualified revision. An unchanged repeat
uses the candidate table/receipts and the new generation from
`.government-etl-build.json` as its next prior; it makes no detail request.
`crs-operation.json` records request counts, selected/attempted details and scope.
Its `coverage.prior_inputs` and the `crs-prior-admission` evidence event retain
the admitted subject/receipt SHA256 values, stored bytes, row counts and combined
uncompressed column bytes. These identify the exact inherited inputs without
another full-file read or published local filesystem paths.
A candidate with failed detail attempts remains available for inspection and
retry; the command exits with an error after recording that result.
`coverage.detail_failed` counts failed attempts in this operation;
`coverage.retained_detail_failed` counts all retained revisions whose latest
attempt failed. An unselected historical failure remains in the table and its
receipts without making a healthy selected cohort or unchanged repeat fail.

This local command accepts a native pair, which does not establish a qualified
live discovery cursor. Its detail-only discovery remains `not_requested` and its
cursor remains unknown. A release caller must supply the selected generation's
verified evidence through the normal rollup before a later live refresh. The
normal builder carries that inherited cursor unchanged during detail-only work;
it never derives progress from report dates.

SpicyDocs reads the complete original through the same native checks for live
and retained input. List `version` and item `currentVersion` name the publication
version; when both item spellings are present they must agree. The report ID,
update instant and stated version must match the selected row. A different ID,
advanced detail, contradictory version, or incompatible shared metadata refuses
replacement. Full exact responses enter `CaptureEvidence` and the central raw
archive before parsing. Unknown members stay recoverable through their exact raw
witness rather than a second stored copy of the response.

The table exposes the literal summary and ordered arrays `authors`, `topics`,
`formats` and `related_materials`. Repeated members remain repeated. The actual
JSON uses `formats[].url` and `relatedMaterials[].URL`; the latter becomes native
`related_materials[].url`. Conflicting simultaneous `URL`/`url` values refuse.
Law numbers such as `118-159` and numeric bill numbers use text in the native
identifier column, while the original JSON retains their original types. No
bill/law relationship is inferred from summary prose or a topic label.

CRS `topics` are native `STRUCT(topic)[]` values attached to the selected
`report_id`, `update_date` and publisher `version`. They retain source order,
duplicates, null members, null arrays and empty arrays. A topic label is not a
controlled vocabulary entry or a shared concept identifier; no topic taxonomy
or topic-derived membership is inferred.

`detail_fields` records `missing`, `null`, `empty` or `value` for each promoted
member. `detail_state` identifies the qualified read; `detail_attempt_state`
identifies the latest attempt. A failed retry preserves all qualified values,
field states and qualified witnesses, records the failed attempt separately,
and retries next time. An unchanged successful selected revision needs no new
read. `retry_completed=True` explicitly requests another read of completed rows.
A matched reread may explicitly clear a value with JSON null, an empty string or
an empty array. Dropping a previously stated member, or changing a populated
fact at the same coordinate, refuses replacement. It never joins retained values
from one response to field states or witnesses from another. A new native
revision can supply new content without changing an older row.

The processing receipt keeps separate list, qualified-detail and latest-attempt
witnesses. Same-coordinate list reobservations add their own list witness without
erasing the qualified detail witness. The native field policy retains earlier
report-only and dated-list policies for recovery; new writes use
`government-sources/3`. Local qualification does not select a hosted release.
