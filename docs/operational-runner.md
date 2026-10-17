# Retain a bounded build and continue with a fresh audit

`spicy-regs-run` owns the fixed build → complete independent audit → accounting
sequence for future retained-source runs. It records process births before
releasing work and seals a reusable candidate only after the build child and
its observed descendants close. A later failure leaves that attempt intact.
An explicit continuation creates another attempt and reruns the complete audit.
There is no scheduler, no automatic resume and no OS sandbox.

Install the `operational-runner` extra in the selected runtime using the locked
repository dependencies; its `psutil` dependency supplies creation-time-aware
process checks and signal methods, and the command refuses with that
instruction when the extra is missing. Run installed commands with that
runtime; do not install into accepted historical targets. Preparation and
synthetic tests do not authorize a corpus run, change a source selection, or
qualify an output for publication.

## Request and grant

`spicy-regs-run run --request REQUEST --grant GRANT` takes one request JSON and
one exact grant for its bytes:

```json
{"authorized": true, "request_sha256": "sha256:<hex of the request file bytes>",
 "expires_at": "<ISO-8601 time with a timezone>"}
```

The grant is checked once, at admission; a run that outlasts it is not
aborted between phases. The request has exactly these fields; any other field,
or a field of the wrong shape, is refused before the run root is allocated:

| Field | Meaning |
| --- | --- |
| `version` | `1` |
| `run_root` | absolute, normalized (no `..`), symlink-free path that must not exist yet |
| `python` | absolute launcher; its resolved binary must be a `runtime` pin |
| `inputs`, `code`, `runtime` | lists of exact `{path, bytes, sha256}` pins |
| `activation` | `{prefix, paths, base_paths, origins}`: import paths put first, the interpreter's initial paths, and package origins |
| `offline_single_process` | boolean: apply the Python network/subprocess/write guards |
| `limits` | wall, sampling interval, RSS, output, disk floors, optional scratch/data/evidence bytes |
| `commands` | exactly `build` and `audit`, each `{module, args}`; `{run}` and `{candidate}` are substituted |
| `continue_from` | optional pin of an earlier attempt's `build-completion.json` |

The runner pins `inputs`, `code` and `runtime` at admission, after a fresh
build (before sealing it), and after the audit. Every module loaded by a
command must be one of those pins, checked before and after the command.

## Bytecode binding

Guardian and payload start as `python -I -B -X pycache_prefix=<run>/scratch/pycache`.
`-B` only stops bytecode writes; without the prefix Python would still execute
an existing `__pycache__/*.pyc`, which no pin names. With the prefix pointing at
an empty run-owned directory nothing existing is read and nothing is written, so
the pinned `.py` source is the code that runs, and an offline payload can import
modules after its read guard is installed. `activate` refuses a payload that was
not started this way. Python children of a payload that is allowed to start them
inherit `PYTHONPATH` from the activation paths.

## Future H and D preparation

The maintained `prepare` command translates the existing selection, limits and
inventory metadata. It reads metadata and runtime files, never source archive
bodies. The resulting request and adjacent source configuration are reviewable
preparations. The owner supplies the exact grant separately; this command never
writes a grant.

### What a future real run needs

The historical H and D preparations are read-only evidence. Their installed
runtimes are gone: the clean-room virtual environment named by H's runtime
inventory no longer exists at its recorded paths, and some of the gzip bodies
named by D's capture configuration are no longer at their recorded
`blobs/sha256` paths. A future H or D run therefore needs, beyond the owner's
grant and a free shared resource slot:

1. a newly installed target (a package root containing `spicy_regs`) and
   interpreter that include the maintained runner and adapters and the
   `operational-runner` extra;
2. a newly produced runtime inventory for exactly that interpreter. List every
   file the phase commands open, not only imported modules: the U.S. Code
   builder's code recipe reads every `*.py` file of `spicy_docs`,
   `rulespec_artifacts` and `spicy_regs`, plus `pyproject.toml` and `uv.lock`
   when the target sits in a checkout. The tests derive one by observing the
   real commands with an audit hook under the payload's own launcher flags
   (`tests/operational_helpers.py`, `observed_files`);
3. for D, a new capture configuration whose bodies and records exist at the
   recorded paths in the current corpus catalog.

`prepare` pins every file under `TARGET/spicy_regs` (skipping `__pycache__`) as
code, because the builder, the auditor and the offline guard read the whole
package. It refuses before writing anything when a runtime or input pin names a
missing file, naming the count and the first missing paths. Hashes recorded in an
inventory or preparation are carried, not re-read; the runner verifies them at
admission.

### H

Use the retained `annual1994-maintained-replay-preparation-v4/request.json` as
`H_REQUEST`, a newly produced runtime inventory and an installed target. Keep the
original files read-only:

```sh
spicy-regs-run prepare h-annual --preparation "$H_REQUEST" \
  --runtime-inventory "$RUNTIME_INVENTORY" --python "$PYTHON" --target "$TARGET" \
  --run-root "$FRESH_RUN" --output "$NEW_REQUEST"
spicy-regs-run run --request "$NEW_REQUEST" --grant "$EXACT_GRANT"
```

This replaces H's `guard.py` and historical supervision/accounting imports with
the installed owner and guardian. `spicy_regs.operational_sources h-build` calls
the maintained `uscode_editions.build`; `h-audit` calls the complete maintained
auditor, checks the original conservation counters, source/selection hashes,
generation and before/after reader recipe, and requires the candidate to hold
exactly the builder's closed file set. The selection is read once and checked
against its pin before use.

### D

Use `CFR-held-semantic-runner-v1/capture-bound-v2/executable-request-v3/`
`preflight-config.json` (or a new reviewed one) as `D_CONFIG`:

```sh
spicy-regs-run prepare d-cfr --preparation "$D_CONFIG" \
  --runtime-inventory "$RUNTIME_INVENTORY" --python "$PYTHON" --target "$TARGET" \
  --run-root "$FRESH_RUN" --output "$NEW_REQUEST"
spicy-regs-run run --request "$NEW_REQUEST" --grant "$EXACT_GRANT"
```

The fixed stages are D's preflight then semantic comparison. The comparison code
lives in the package as `spicy_regs.cfr_native_audit`, extracted from D's
`semantic_runner.py`; no external comparator or historical supervisor is a
dependency. Retained capture records spell their fields two ways
(`requested_url`/`resolved_url`/`status_code`/`byte_size` and
`url`/`final_url`/`http_status`|`status`/`bytes`); exactly one spelling of each
field must be present and a record carrying two refuses as ambiguous. Inputs are
bound by content: stored and decoded digests are checked against the
configuration, and recorded inode/ctime/link-count identities are ignored, so
benign corpus maintenance (such as hardlink deduplication) does not invalidate a
configuration. Aggregate, scratch, data and evidence limits retain D's declared
values; candidate/output files are charged as data (including hidden root staging directories, a producer's candidate bytes in
transit) and other records as evidence.

**Owner decision: the historical preflight-acceptance gate is dropped.** The
earlier arrangement made the semantic run require a separately accepted
clean-room preflight and compared a proof with a digest copied from that same
proof. One grant now covers this run's preflight and then its semantic phase, and
the semantic phase refuses unless the preflight record this same run wrote
(`candidate/preflight.json`) is present and exact. There is no
`--preflight-acceptance` argument.

## Offline guard

When `offline_single_process` is true the payload installs the retained Python
guards after activation. Python network, subprocess and link operations are
refused. Python signals may target only the payload itself (`os.kill` of another
process or `os.killpg` is refused). Reads must come from the run root, a pinned
path, a directory, or, on continuation, the previous attempt's already verified
candidate.

Writes are limited to the payload's own scope: `scratch/`, `output/`,
`candidate/` (build phase only), the phase's own `{phase}-imports.json`, and
hidden producer staging directories at the root (names starting with `.`, such as
the builder's `.uscode-editions-*`). The runner's own records (`owner.json`,
`candidate.json`, `request.json`, the `*-child`, `*-release`, `*-terminal` records
and the rest) are not writable, so an offline payload cannot steer a later phase
or forge a closure record. Descriptor-relative opens, removals and renames are
resolved through their directory descriptor, so `shutil.rmtree` and
`TemporaryDirectory` work inside the scope. That judgment applies to calls through
the patched `os.open`/`posix.open`; a function reference captured before the guard
was installed is not covered. The guards observe Python only; native-library I/O
is outside them and they are not an OS sandbox.

Non-offline payloads (the qualification) are not restricted this way. The owner
instead refuses to launch a phase when any of that phase's records already exists
and re-verifies `owner.json` and `candidate.json` against its own memory
immediately before each launch, when no writer exists. A non-offline payload can
still rewrite records of its own running phase; closure trusts a terminal record
only when it names the guardian identity in that phase's child record.

## Retained-population qualification

Qualification has explicit build and audit stages in a request for the
maintained owner. The former combined command is unsupported:

```json
{
  "build": {
    "module": "scripts.qualify_retained_population",
    "args": ["/absolute/selected-plan.json", "{candidate}", "--stage", "build"]
  },
  "audit": {
    "module": "scripts.qualify_retained_population",
    "args": ["/absolute/selected-plan.json", "{run}/output/audit", "--stage", "audit", "--candidate", "{candidate}"]
  }
}
```

Use activation paths for the reviewed checkout's `src` and repository root,
pin the checkout's scripts and runtime dependencies, and require a retained
`localInput` in the selected plan. `offline_single_process` is false for this
qualification because its existing private HTTP/MCP and index subprocess checks
require loopback and children. H/D requests set it true. Neither mode is an OS
sandbox. An enclosing deployment may provide stronger OS isolation separately.

A produced bundle is selected by one maintained rule (`selected_pair`): ordinary
datasets use `<stage>/<dataset>.parquet` with `<stage>/etl_receipts.parquet`, and
`court_opinions` uses the producer's link-selected attempt (`<stage>/court_opinions.parquet`
is a relative link to its immutable `.court-etl/<id>` bundle, whose receipts sit
beside the target). The build stage requires its producer to agree with that rule
and the audit stage reads through it.

`BUILD.json` describes candidate outputs; it does not establish trusted
completion. Only the outer owner's `build-completion.json`, written after child
closure and a complete hash census, supports continuation. The audit stage
re-admits both full bundles and still runs the complete row-reference/oracle,
history, restoration, generation, private upload/readback and serving checks.
Zero candidate-build calls means no selected-prior/current production; independent
reference production remains part of the audit. The audit's `HANDOFF.json`
names both the build's and the audit's phase records with their pins.

## Explicit continuation and recovery

Create another request with a fresh `run_root` and `continue_from` equal to the
exact pin `{"path": "/old/run/build-completion.json", "bytes": ..., "sha256": "sha256:..."}`.
Continuation binds to what produced the candidate: `version`, `python`, `inputs`,
`code`, `runtime`, `activation`, `offline_single_process` and the **build**
command must match the completion exactly. `limits` and the audit command may
change, so a resource failure in the audit can be continued with adjusted limits
and a corrected audit argument without rebuilding. The new request's exact grant
approves that completion's bytes. The owner checks that the previous attempt is
closed, the completion owner, the build terminal, all bound identities and a
fresh complete candidate census. It writes `reused-build.json` in the new
attempt and launches only the audit; the full independent audit always runs. It
never appends to the failed attempt, and editing a failed record cannot turn it
into an admissible completion.

```sh
spicy-regs-run status /absolute/run
spicy-regs-run reconcile /absolute/run
```

`status` only reads. It reports each phase's closure reason and whether the whole
run is closed. A phase is closed when any one holds:

- it was never launched;
- its terminal record says `known_closure: true` and names the same guardian
  identity as the phase's child record (a terminal forged by an earlier phase's
  payload cannot know the future guardian's birth);
- the guardian was never released (no payload can have started); or
- every recorded identity (guardian and observed descendants) no longer matches
  a live process and no live process still belongs to the guardian's process
  group. POSIX keeps a group id reserved while any member lives, so an empty
  group for a dead recorded guardian means nothing from that phase survives. A
  reused id that now leads an unrelated group leaves the run *held*, which fails
  closed.

A guardian that is killed, or a machine that reboots, therefore no longer leaves
a run held forever: after `reconcile` the run is closed even without a terminal
record, and continuation from its sealed build is possible. A continuation
attempt against a run that is not closed is refused with a pointer to
`reconcile`.

`reconcile` is an explicit cleanup action for an exited owner. It uses an
OS-released advisory lock (a concurrent reconcile is refused, a stale lock file
is harmless), signals only matching PID/birth identities, never a group, and
writes `reconciliation.json` (`closed-incomplete-run` or `held`, per-phase
closure, remaining known processes and the root census or the reason it could
not be taken). It never launches a phase or establishes source qualification.
A run that stays held because live members of the guardian's group were never
recorded needs manual inspection; nothing in the runner signals a group.
`run` exits 0 only for `built-and-audited-awaiting-review`, `reconcile` only for
`closed-incomplete-run`; `status` always exits 0.

## Process lifecycle

The owner starts a guardian as a new session leader. Before launching a phase it
refuses if that phase's records already exist, and it checks that `owner.json` and
`candidate.json` still equal what it holds in memory; the audit therefore always
runs on the sealed candidate.

The guardian records its birth, waits for the owner's durable release, starts the
payload, records the payload's birth durably, and only then releases it. The
payload does no activation, import or user work until that release arrives and its
parent is still the guardian; a guardian that dies in between leaves a payload
that exits without having done anything. The guardian always writes a terminal
record: its first termination signal turns every further signal into a no-op, its
cleanup runs uninterrupted, and any cleanup error is recorded with
`known_closure: false`. When the owner stops early it terminates the guardian and
waits up to 30 seconds for that cleanup instead of killing it.

If the guardian is lost without a closed terminal record (for example it is
killed), the owner does not exit and leave its payload running unsupervised. It
reads the phase's child record and terminates the recorded guardian and observed
descendants by exact PID and birth match, never by group, then notes the outcome
under `orphan_cleanup` in `result.json`. When that leaves nothing alive the phase
is closed and a continuation needs no `reconcile`. A live process that was never
recorded, but is still in the guardian's process group, leaves the run held for
manual inspection: `reconcile` also never signals a group.

## Measurements and limits

One resource sampler is shared by owner hashing/accounting and guardian
execution. The absolute monotonic deadline covers all stages. The guardian also
samples the owner, payload and observed descendants, and stops work when the
owner disappears. Limits are reactive observations and can overshoot; configured
thread counts and Python audit hooks are not kernel quotas. The guardian's
environment is minimal and sets `GIT_OPTIONAL_LOCKS=0`, so read-only git queries
by a payload do not refresh a repository index outside the root.

Sampling and the final census are different on purpose. Sampling is an
observation: producers create and delete scratch while it walks, so a vanished
entry is skipped, links are charged without being followed and nothing is
refused by type. The strict closed census runs once every worker has exited.
It hashes regular single-link files and records an in-root file symlink that
resolves to a regular file as `{path, link, target}` (court producers select
their bundle that way); it refuses dangling, out-of-root or directory links,
hardlinks, special files and traversal errors.

The owner hashes unique input/code/runtime pins at admission, after a fresh build
before sealing, and after the audit. A fresh run hashes the candidate at most
twice (at the seal and again after the audit) and `output-files.json` lists it by
the completion pin rather than hashing it a third time. A continuation hashes the
old candidate before reuse and after the audit. Loaded-module checks
independently verify actual imported bytes. `owner_hash_work` records completed
file-hash calls and bytes by selected pin group; the work makes no throughput
claim. Source-specific builder/auditor reads remain additional work.
`output-files.json` lists its excluded final records; the final result has a
bounded reserved charge, avoiding a self-hash cycle. Partial runs and their logs
remain available for review.

## Limitations

- Sampled limits are not OS isolation and can overshoot. Sampling refuses a
  directory it cannot read rather than skipping its bytes.
- Detached descendants that leave the guardian's process group are outside the
  assurance.
- Process identity uses psutil creation times. On Linux, `create_time` can drift
  after clock steps, which fails closed (the process stops matching). macOS is the
  intended platform.
- Durability uses `os.fsync`, which on macOS does not flush the drive cache
  (`F_FULLFSYNC`); a power loss can still lose a record that returned.
- The offline guards observe Python I/O only, judge descriptor-relative calls only
  through the patched `os.open`/`posix.open`, and scope writes as described under
  Offline guard; non-offline payloads (the qualification) are not write-scoped.
- A refusal from `run`, `prepare`, `status` or `reconcile` prints one line
  (`spicy-regs-run: <reason>`) and exits 2; a run that started and failed exits 1.
