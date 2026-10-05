# Exact rating numbers

`scorecards-etl-ratings-v3` stores `value_number` as `VARCHAR`. It contains an
optional, validated decimal string from the publisher. Signs, leading zeros and
fractional digits survive Arrow, Parquet, DuckDB and MCP without rounding.
`value_text` continues to retain the publisher's presentation, including units,
grades and exclusions. No grade becomes a number.

This restores the numeric representation stated in the
[frozen source model](../../../proposed_schema.json) and the SpicyDocs
`scorecard_member_ratings` description: optional exact decimal text. The typed
serving layer previously used fixed precision. Original ILA detail responses
exceeded its first fractional bound, and original Machinists JSON exceeded the
later bound. Widening Arrow decimals did not solve the serving problem: the
retained DuckDB probe read that representation as an inexact `DOUBLE`.

The private `rating-raw-lexemes-review-v1.json` compares every refused Machinists
rating with the original JSON path, member ID, score-set ID and number token.
Its original response hash is
`19b277b11bdf2128184f9aee3e822c47f635bfe30241d5ca91e1888069571c82`.
The retained `rating-precision-compatibility-v2.json` records the failed wider
decimal probe. These acquisition and failed-publication artifacts remain
separate from publication authority.

Historical `scorecards-etl-v1` and `scorecards-etl-ratings-v2` declarations remain
frozen in `src/spicy_regs/scorecards/historical_rating_policies.json`. Reads
validate their exact declared policies and receipt joins before reconstructing
source rows. A new write uses v3 after reconstruction; casting an old Parquet
file does not migrate its receipts or preserve its subject hashes. Source
identities, `conversion_inputs`, source values and witnesses remain intact.

These historical reads apply to the scorecard migration reader. A saved local
native workspace still requires the installed current policy. Rebuild its
selection through the verified migration path before using v3 with that
workspace. Pinned remote publications and downloaded publication batches keep
their declared historical schemas; their reads do not reinterpret old files.

Weights and item contributions retain their established decimal types. There
is no measured failure requiring their migration. Numeric computation belongs
to an explicit analysis with a stated methodology and precision; the source
layer supplies exact numbers without claiming a universal scoring formula.

New publication must declare the v3 schema and pass exact source replay and
DuckDB readback. An invalid decimal still refuses the entire candidate. The
writer retains that attempt's rows and receipts, validates them, and stops
before carrying prior receipt history. The prior publication remains selected.
