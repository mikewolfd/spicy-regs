# Operate congressional scorecards

SpicyDocs owns publisher parsing and completeness. SpicyRegs selects sources,
replaces complete editions, resolves exact congressional identities and publishes
separate source and analysis families. Retained inputs, prepared data, published
files and hosted readback remain separate observations.

## Use maintained commands

Install the pinned source-reader dependencies. Run `spicy-regs-scorecards --help`
and each phase's `--help` for its arguments. These commands also run as
`python -m spicy_regs.scorecards.operations.cli`.

| Command | Input and result |
| --- | --- |
| `inspect-adapters` | Inspect every installed reader's module or injected class, shared helpers, parser version and code digest. It performs no source requests or extraction. Long functions are review leads, not an automatic qualification failure. |
| `inventory` | Generate the existing publisher queue and API inventory from pinned discovery, qualification, publication and work-policy files. Discovery never promotes support or publication. |
| `qualifications` | Record bounded public metadata from an explicit, pinned private qualification plan. Source and model bodies stay private. |
| `prepare` | Replay a pinned source plan, merge only complete editions with an admitted prior generation, and verify one candidate. It performs no publisher requests or publication. |
| `prepare-analysis` | Build exact links from pinned published scorecard and congressional tables. Conversion of earlier official layouts requires its explicit option. It acquires no congressional source data. |
| `publish` | Reverify a prepared source or analysis candidate and use the existing family publication code. Analysis refuses changed parents. |
| `publish --observe-only` | Recover an uncertain write by observing that the exact candidate is current. It uploads nothing and changes no pointer. Use a fresh output directory, then run readback. |
| `readback` | Download public members independently, verify pins and scope counts, then compare hosted attribution, native values, receipt fields and source receipt origins. |
| `record-publication` | Advance only matching qualified editions after passed public-file and hosted readback. |

The earlier scripts remain thin compatibility entry points. Research directories
hold observations and plans; the installed package owns executable operations.
An installed publish command needs no Git checkout. `--implementation-commit`
optionally requires a clean named checkout. The authenticated result records the
current verifier separately from the candidate's recorded producer packages;
reading an older immutable candidate never rewrites its provenance.

Read-only recovery compares the candidate's logical identity, table and receipt
descriptors with the current family, in addition to its artifact digest. It can
observe an already-published generation even if its parents later advance;
publishing new analysis requires current parents on every conditional pointer
attempt, including retries after an unrelated writer changes the index.

Qualification records use immutable content-hash filenames. The command validates
the full proposed batch before saving records and atomically replacing its ledger.
A failed source or interrupted ledger update preserves the prior ledger and its
referenced evidence. Publication bookkeeping uses the same atomic JSON writer and
admits both current native snapshots and supported historical receipt-only ones.

`prepare` and `prepare-analysis` accept repeated `--local-inputs` roots. They
search only exact member locators, verify size and SHA-256, and link matching
bytes into owned staging. Wrong cache generations are skipped. A corrupt staged
input refuses. Consumers still admit the subject and receipt together. Public
readback deliberately uses the public route to establish availability.

## Refresh and resolve

`run-rollup-scorecards` uses `sources.yaml`. Canonical unattended activation is
bound to the installed `live_qualifications.json`: publisher, reader digest,
parser version, exact edition, completeness and evidence policy must agree.
A new edition or changed reader requires a new source qualification. Retained
plans use their own pinned replay admission. Enabling a reader by availability
or API discovery alone is insufficient.

The scheduled scorecard workflow follows source publication with the existing
`run-rollup-scorecard-analysis` pipeline. The manual analysis workflow provides
the same repair path. Analysis admits current official inputs and refuses
changed parents at publication. A source publication remains valid if later
analysis fails; repair analysis against the now-current parent set. Joins keep
scorecard identifiers, source snapshot identifiers and official identities
separate. Ambiguous, conflicting and unresolved candidates stay visible.

Member matching preserves exact identifier precedence, dated aliases, historical
terms and versioned overrides. Legislative matching keeps bill, amendment,
committee and floor-vote relationships distinct. Candidate order stays stable;
a set now avoids repeated scans when collecting duplicate vote candidates.

## Reuse and resource limits

Source column definitions and identities come directly from SpicyDocs and the
resolver. Native column types remain an explicit SpicyRegs decision. The frozen
pre-refactor fixture checks source fields and key order without duplicating a
second production declaration.

HRC private retention now uses the shared qualified-observation helper. Its
exact PDF, semantic pins, fresh private identity and publisher-specific
completeness checks remain. PDF extraction still uses the injected SpicyDocs
Docling/default infrastructure or reviewed semantic observations; this refactor
adds no OCR engine or model reruns. CPAC's reader supplies its anonymous GraphQL
headers; acquisition does not infer publisher-specific headers.

The direct HTTP loop remains necessary: it captures same-host redirects and
returns completed refusal responses to the publisher context. The existing
`BoundedHttpCapture` rejects redirects and retryable statuses before supplying
that result. Replacing it without a shared transport extension would change
source evidence and recovery behavior. It continues to use the shared captured
response type, bounded payloads and explicit request budgets. Zyte uses the
maintained provider SDK and an explicit publisher selection.

Public readback and publication bookkeeping configure the shared DuckDB resource
helper, use one execution thread, bound memory and spill, and close connections
on failure. Python source reconstruction still materializes prior and merged
rows; this remains the main memory constraint. Preparation does not claim a
streaming corpus rewrite or a lower total process-memory ceiling.

Scoped merging now extends the retained list instead of concatenating another
copy. A Python `tracemalloc` check on preallocated row references measured
16,449,584 peak bytes for concatenation and 8,449,496 for extension, with identical
output length. This measures pointer-array allocation only, not end-to-end ETL
memory. Reproduce with a prior list of one million references, a fresh list of
100 references, and tracing started after allocating inputs; compare
`retained + fresh` with `retained.extend(fresh)` after the same filtering step.

## Release and remaining work

The consumer pins the complete SpicyDocs `0.63.0` wheel with dependency declarations
and lockfile. Older wheels, source qualifications, private inputs and prepared
publication pins remain intact. No new publisher activation is included.

The existing unpublished candidate must be published from its qualified retained
runtime when shared-host capacity and the ongoing higher-priority release allow.
Do not rebuild that candidate to adopt this refactor. Follow publication with
independent public/hosted readback, inventory advancement and analysis refresh.
Code review, merge and the coordinated serving deployment remain separate from
those data operations.

Continue qualification of modern publisher scopes from the existing generated
queue, newest edition first. Publishers inactive before the established cutoff
remain catalogued and deferred. Do not mark an inaccessible source retired
without evidence that the original publisher or scorecard ended. Larger parser
functions need source-specific review before further consolidation; shared
platform helpers already cover RDS/BT50, VoterVoice, Quorum, CapWiz, workbook and
qualified PDF families. Full streaming reconstruction requires a separately
verified replacement for prior-row restoration and complete-family validation.
