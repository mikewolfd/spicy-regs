# Original comments input capture

`scripts.capture_comments_input` acquires a private, immutable input for later
conversion qualification. It reads the selected legacy comments snapshot through
the attached Iceberg catalog, preserving every original column and value.
It never initializes or writes a catalog table.

The caller supplies captured metadata with `tableUuid`, `snapshotId`, `schemaId`
and `schemas`, and an explicit legacy namespace. The helper requires that exact
identity and nullable string schema before reading. It uses DuckDB's
[`AT (VERSION => snapshot_id)`](https://duckdb.org/docs/current/core_extensions/iceberg/overview)
to read the logical snapshot through Iceberg, including its deletes. It requires
the same current identity and schema on a fresh connection after export. A
changed snapshot, including compaction, refuses this attempt.

```sh
OMP_NUM_THREADS=4 ARROW_IO_THREADS=4 uv run --frozen python -m scripts.capture_comments_input \
  /absolute/path/expected-metadata.json /absolute/path/fresh-owned-output \
  --namespace default
```

Credentials must already be present in the invoking process environment.
The helper does not load an implicit `.env` file. Connection and SQL error text
can contain credentials; refused attempts retain only the failed phase and
exception type. The output must be fresh; existing evidence is never replaced.

The caller must grant the heavy slot, pin the helper and dependencies, admit
available disk and use the reviewed whole-process resource wrapper. DuckDB uses
four threads, 4 GB query memory and 32 GB spill limits. These are query settings,
not operating-system memory or disk ceilings. Source acquisition has its own
whole wall clock; this helper cannot qualify conversion or another workflow's
time bound.

`RESULT.json` reports `captured` only after both identity checks, exact exported
schema, full local population/footer agreement, ID statistics, agency populations
and source byte hash. `comments.parquet` preserves original string values such
as `duplicate_comments='007'`; it contains no mapper normalization. The exported
row order becomes the retained input's order, without asserting a stable ordering
across separate Iceberg reads. Null or duplicate IDs are reported for the later
conversion decision. They do not turn this acquisition result into conversion
approval. `agency-populations.parquet` retains the null agency group too.

Failed attempts retain their output and a refused result. An interrupted attempt
without a successful result remains unqualified. The local fixture tests verify
export, source-value preservation, identity refusal, fresh-connection checks,
output ownership and sanitized failures. Actual hosted snapshot/delete behavior
requires the separately granted source capture run. Native receipt conversion,
catalog commits, ETL priming, documents rollup, mirror publication and production
adoption each require their own complete workflow evidence.
