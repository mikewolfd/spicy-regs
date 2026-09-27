# Citations from selected held fields

`run-rollup-held-citations` reads explicit fields from selected immutable
publications and appends their findings to the existing `document_citations`
table. It publishes through the complete `print-citations` family, carrying
unchanged sibling tables forward. It does not fetch publisher bodies.

The selection manifest names full source keys and exact input generations:

```json
{
  "selections": [
    {"kind": "bill_section", "keys": ["119-hr-10534", "introduced-in-house", "govinfo", "5"]}
  ],
  "input_generations": {"bill_sections": "sha256:<exact selected generation>"}
}
```

Run a local candidate first:

```sh
uv run run-rollup-held-citations --selection selection.json --output-dir output/held-citations
```

The CLI defaults to skipping upload. Its input tables must belong to the named
managed generations. Publication also requires an existing complete
`print-citations` family and the exact prior citation table. A missing required
input stops the run.

| Kind | Full source identity | Literal field |
|---|---|---|
| `bill_section` | `bill_id, version_code, source, seq` | `bill_sections.body` |
| `report_section` | `package_id, part_id, seq` | `report_sections.body` |
| `lobbying_activity` | `filing_uuid, activity_index` | `lobbying_activities.description` |
| `comment_inline` | `comment_id` | `comments.comment` |

Composite `document_key` values are compact JSON lists in the listed order.
Single comment IDs remain literal. Citation spans index the exact UTF-8-decoded
field text; comment offsets index its retained HTML, not rendered text or an
attachment. The field bytes and selection manifest are retained in the normal
source-evidence artifact. The source generation and text digest accompany each
read. MCP checks the current full-key field digest before resolving targets.

Each invocation permits at most 100 selected fields, 4 MiB per field and 32 MiB
total field text. Reads have a bounded timeout. A successful complete field,
including an empty result, replaces findings only for its source kind, full key,
text digest and selected rules. Failed, duplicate-parent, null or capped reads
preserve prior findings. Prior text versions remain distinct. Successful zeroes
and rule versions are checkpoints in the same Parquet artifact as the findings.

Bill numbers require a stated Congress; a document's publication period does
not fill it in. Committee names are excluded from default extraction because
reviewed report text exposed a truncated line-wrapped name. They require explicit
transform-level opt-in and remain unresolved evidence. The reviewed cohort also
contains executive orders and verbose Federal Register references outside the
current grammar. A normalized U.S. Code section key does not identify a specific
statutory note: the original `note` qualifier remains in matched text.

See `research/join-delivery-execution-2026-09-27.md` and its evidence receipts for
the reviewed source scopes, misses, selected publications and delivery status.
These bounded reads do not establish whole-document or corpus recall. FCC body
extraction remains disabled until a retained body route is qualified. Inline
comments require a managed source publication before the publication CLI will
accept them.
