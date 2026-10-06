# Capture original selected comments

Install `uv sync --frozen --extra comments-reader`, then use
`scripts/capture_comments_current.py EXPECTED.json OUTPUT --namespace default
--expected-runtime RUNTIME.json`. `EXPECTED.json` selects the complete nullable
string schema and exact table UUID, snapshot ID and schema ID. Generate the
runtime pins with `scripts.capture_comments_current.reader_runtime()`.

The optional PyIceberg reader loads the REST table with its vended file credentials.
It plans the selected snapshot, checks actual live file/delete types, and reads
one planned file at a time through public `ArrowScan`. PyIceberg handles column
IDs, missing nullable columns, delete sequencing and position masks. Unsupported
formats or equality deletes produce a retained refusal. The source retains all
original columns, strings, nulls and repeated rows.

PyIceberg materializes each data file and its applicable delete data before
returning its batches. Feeding the planner's complete task list into that reader
can queue the whole table; this adapter supplies one task at a time. Client
batches and output row groups do not impose a byte limit on a file or record.
Run through a separately admitted whole-process RSS, time and disk watchdog;
refuse oversized files rather than increase limits during a run. Vended credentials
must remain usable for the admitted capture duration; expired credentials refuse
the attempt. Partial files remain private evidence.

The reader verifies fresh start/end identity and original field definitions,
then independently reads the complete exported file. Length-framed UTF-8 field
hashes check every value and ordinal from decoded reader to output, including
nulls and repeats. Physical file/delete counts are observations, not a logical
population. Local population and agency queries operate on the exported logical
rows. Only `RESULT.json` with `status: captured`, plus the successful external
resource result, qualifies the local input. This does not publish or convert it.

See `tests/test_capture_comments_current.py` for actual pre/post Iceberg fixtures,
position-delete duplicates and sequencing, evolved nullable fields, exact original
values, file lifetimes and retained refusal behavior.
