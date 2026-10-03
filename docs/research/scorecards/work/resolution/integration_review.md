# Independent analysis integration review

No blocking correctness finding remains in the reviewed integration. The test
fixture's `item_kind` spelling was reported and the coordinator changed it to
`item_kind_text`. This review is static; resolver tests were executed separately.

## Patch summary

`ScorecardAnalysisRollup._prime` reads all members of each declared input from
one captured publication index, verifies their bytes and retains exact pins
(`src/spicy_regs/pipelines/rollups/scorecard_analysis.py:23`). Its `build` passes
those bindings to the transform (`:56`). The transform reads Parquet, invokes the
pure resolver and writes both link tables after validating their schemas and
keys (`src/spicy_regs/transforms/build_scorecard_analysis.py:45`). Shared
generation validation now accepts a split-table descriptor digest rather than
inventing a single-file hash (`src/spicy_regs/generations.py:164`).

## Function trace

| Function | Location | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `_run_tables` | `pipelines/rollups/base.py:139` | Captured index → verified generation | Isolates the build when using a public source; carries returned parents into generation creation. |
| `ScorecardAnalysisRollup._prime` | `pipelines/rollups/scorecard_analysis.py:23` | Index + local directory → parents and retained paths | Clears prior bindings first; requires managed inputs; checks warm bytes; downloads every split member. |
| `fetch_member` | `sources/publication.py:344` | Pinned member → checked local file | Downloads through a temporary file; requires matching digest and size before replacement. |
| `table_members` | `sources/publication.py:489` | Table descriptor → pinned members | Retains all declared members and their immutable generation paths. |
| `table_pin` | `sources/publication.py:508` | Managed table → digest, size, family and artifact pin | Hashes the complete split descriptor with deterministic JSON; retains byte hash for single files. |
| `_read` | `transforms/build_scorecard_analysis.py:20` | Explicit member paths → source rows | Refuses missing files and missing required official columns; avoids Hive path-field inference. |
| `_checked_rows` | `transforms/build_scorecard_analysis.py:34` | Resolver rows → validation | Requires exact fields, string/null values and nonempty unique keys. |
| `build_scorecard_analysis` | `transforms/build_scorecard_analysis.py:45` | Verified paths and pins → two Parquet paths | Requires the full input set; builds and validates both output tables before replacing staged outputs. |
| `_check_parents` | `generations.py:164` | Parent records + captured index → validation | Checks digest form, size, paired family/artifact fields and equality with the table's computed pin. |
| `verify_generation_source` | `generations.py:93` | Immutable artifact → verified artifact | Validates the captured index before validating parent pins. |

Paths in the table are relative to `src/spicy_regs/`.

## Data flow and invariants

The captured index binds every input family. `_prime` computes parent pins from
that index, then obtains every member from the same descriptor. The network
helper verifies new bytes; `_prime` independently hashes existing local files.
Only after the whole input loop succeeds are paths and pins stored on the rollup.
A failed later `_prime` cannot reuse bindings from a previous successful call.

The transform requires exactly the declared input mappings and checks required
official columns. The resolver retains pins and source snapshots in all link
rows. Output field and key checks run before staging either published table.
Generation creation subsequently checks parent descriptors against the captured
index. Source ratings remain outside the output family.

The two local path replacements are sequential, but public publication is
atomic at the outer generation pointer. A local replacement failure aborts
generation creation; the code does not promise transactional replacement of
scratch files. This does not violate the public family invariant.

## Tests and edge cases

- `test_analysis_pins_every_partition_and_keeps_publisher_identities`
  (`tests/test_scorecard_analysis.py:67`) covers split-file enumeration, retained
  descriptor pins, member and bill links, generation sealing and a warm read.
- `test_analysis_refuses_inputs_that_do_not_match_the_index` (`:95`) covers
  changed local bytes and missing ownership. It checks that a failed re-prime
  also disables `build`, and produces no output.
- `test_split_parent_pin_rejects_a_changed_member_or_size` (`:113`) changes
  member content, total size and managed ownership. Each path reaches the
  parent validator and must refuse.
- `test_managed_download_refuses_corruption_without_replacing_prior`
  (`tests/test_generation_publication.py:260`) exercises the existing network
  download verifier and preservation of prior bytes. The new integration test
  replaces this helper with a fixture transport; it does not itself exercise
  corrupt network responses.
- The first integration fixture used `item_kind` at
  `tests/test_scorecard_analysis.py:26`. That misspelled source field was reported
  as a test-coverage nit and corrected by the coordinator. Resolver unit tests
  independently exercise the cosponsorship branch with `item_kind_text`.

## Hypothesis checks

H1: downloaded and cached paths are both verified before resolution.
Confirmed by `_prime` and the complete `fetch_member` implementation.

H2: a split-table parent binds every partition rather than one selected file.
Confirmed by `table_pin`, `table_members`, `_check_parents` and descriptor-change
test assertions.

H3: a failed re-prime cannot leave an apparently valid prior binding.
Confirmed by the explicit attribute clearing and the failed-prime build test.

H4: one output failure cannot publish a partial analysis family.
Confirmed by the base-class path from successful `build` to generation creation
and publication. Local scratch replacement is a narrower, nontransactional step.

## Conclusion

VERDICT: APPROVE

The implementation preserves immutable input identity and the separate analysis
family. Changed-path coverage is adequate for this bounded integration. Confidence
is high for the inspected source paths; this review does not establish production
publisher completeness, deployed package adoption or remote publication freshness.
