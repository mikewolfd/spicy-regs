# Replay native legal references from retained XML

`NativeLegalReferencesRollup` builds `native_legal_references.parquet` and
`native_legal_reference_reads.parquet` from an explicit local manifest. It uses
SpicyDocs' CFR and U.S. Code scanners and the ordinary rollup generation and
source-evidence lifecycle. It makes no acquisition requests.

```sh
uv run --frozen --no-sync python -m spicy_regs.pipelines.rollups.native_legal_references \
  --manifest tests/fixtures/native_legal_references/manifest.json \
  --output-dir output/native-legal-references
```

The default is a local candidate. Upload requires the ordinary explicit rollup
option; a successful build does not claim publication or complete edition
coverage.

## Who owns what

Since SpicyDocs 0.52.0 both tables are SpicyDocs table contracts
(`schemas.native_reference_rows`): their columns, identities and prose, the
row shapers, and the reading of each observation
(`interpretation.native_legal_references`, which types an exact native href
and reads a note's text with the shared citation rules). Its
`docs/decisions.md` entry "The native legal-reference tables have contracts"
states the reading, the measurements and what an importer changes. Both tables
name that rule, `native-legal-reference/003`; rows this repository read before
the reading moved name `/002`.

This repository keeps the manifest, the input pins, the source evidence, the
qualification of complete inputs, the scan loop and the target lookup. The
reading receives the lookup as `resolve`: one call for the whole run to
`citation_resolution.resolve_citations` over the target tables the manifest
pins, so each distinct typed key is read once, and the run's lookup coverage
is journalled as `native-reference-resolution`. Both tables merge through
their contracts, replacing each scope a complete read covers.

## Input selection and evidence

Each `sources` entry requires a local `path`, exact `sha256:` digest,
`source_family` (`ecfr` or `uscode`), `source_record_key`, `source_locator`, and
`edition` (a literal or explicit JSON null). Relative paths resolve against the
manifest directory. The manifest, exact XML bytes and optional target bytes
enter the existing `CaptureEvidence` blob store and journal. Events say
`retained-input`, not a fabricated new HTTP capture.

Optional `targets` map an existing resolver table name to a local Parquet
`path` and exact `sha256:` digest. Reads use a verified copy of those bytes.
Result candidates name the target file pin and the existing resolver's grain.
For example, `law_code_sections` matches classification rows, not hosted U.S.
Code text or proof of current applicability. An unknown source edition stays
unknown even when a target lookup finds a classification row.

Bounds are explicit: at most 100 source inputs, 16 MiB per XML and 64 MiB total
XML, 10,000 selected observations per run, and 64 MiB per optional target
file. Owner scanner depth/text limits remain active. The shared resolver
limits distinct target keys and candidates and records capped/failed lookups.
A parsing, pin or size failure aborts before replacing output tables.

## Rows and corrections

One occurrence row represents one scanner observation, identified by source
scope, input digest and ordinal (`at-joined/1`); several typed references in
one note stay nested candidates of that row. What each observation reads as,
and what stays literal (fragments, subsection paths, unknown namespaces,
part-only CFR findings, `PARAUTH`/`SECAUTH`), is the SpicyDocs reading's and is
described with the contracts' column prose in the data dictionary.

The read table records `complete_selected_shapes`, including successful zero
results. This means the selected owner scanner completed, not that every legal
reference was understood. No current acquisition or global title/edition
completeness is inferred.

After every selected source finishes, replacement retires older occurrences
only in those exact source/edition scopes. Successful empty reads clear that
scope. Other scopes survive; failed or capped source reads clear nothing. Each
explicit replay reruns the current reader and resolution, so an unchanged XML
can resolve against changed selected target bytes. The existing immutable
source-evidence generations preserve earlier captures.

## Qualification

`tests/fixtures/native_legal_references/` pins native fragments copied byte for
byte from SpicyDocs. The retained replay on 2026-09-27 produced 31 USC
observations and two CFR notes; exact rows and a verified local generation are
retained under local
`/Users/mikewolfd/.codex/artifacts/spicy-regs-native-legal-references-20260927/`.
That generation was not uploaded. Tests also cover successful empty correction,
malformed partial XML, digest/byte-bound failure, duplicate scopes, preserved
unrelated scopes, target changes with stable XML, deduplicated lookup, unknown
namespace/href shapes and part-only CFR controls. Synthetic controls do not
extend native source qualification.

## Publication qualification

The 2026-09-27 fixture replay completed the normal rollup lifecycle with upload
skipped. Its immutable generation and source evidence verify under
`/Users/mikewolfd/.codex/artifacts/spicy-regs-native-legal-lifecycle-20260927/verification.json`.
The read snapshot is explicitly empty: this run used local pinned fragments,
not an observed hosted publication. Keep this candidate local.

The USC fixture retains `/us/usc/t5/s423`, but no release point. The CFR fixture
provenance records a parent XML digest, source URL, byte span and appended closing
tag; that parent capture and acquisition receipt have not been admitted as this
candidate's source evidence. Its filename cannot establish enclosing title or
edition. A sampled label does not close that association gap. Before publication,
retain and verify the enclosing publisher input and receipt, preserve the fragment
selection operation, and bind source identity and any stated edition to that
verified evidence. Unknown edition can remain explicit where the verified source
itself leaves it unknown. This is separate from the already verified fidelity of
the retained fragment observations.

### Complete-source qualification added on 2026-09-27

A later scoped replay closes the enclosing-input gap for complete eCFR Title 1
and U.S. Code Title 1, without changing the fragment candidate's status.
The replay and `verification.json` live under
`~/Work/corpora/supply-2026-09-02/receipts/native-legal-full-2026-09-27/`.
They bind the exact full eCFR XML to its historical acquisition receipt and the
USC XML to its validated complete publisher ZIP. The owner validators establish
CFR title identity and USC native `Online@119-103` release identity. CFR's
`requested-as-of:2026-08-10` explicitly identifies the request date; it is not a
printed edition date. Supporting originals and qualification facts enter the
existing source-evidence artifact. Upload remains a separate root-owned step.

Manifest entries may include `qualification.kind` of `ecfr-retained-title` or
`uscode-title-archive`; unsupported kinds, changed input bytes, incorrect archive
membership and contradictory record/edition labels refuse the whole run.
Unqualified fragments still work for local tests but do not gain qualification
merely by passing a scanner. The full-input comparison checks every selected
native href and note/source-credit observation. It found no PARAUTH or SECAUTH
in these two inputs; those forms remain unsupported elsewhere.

Exact native USLM `ref` and XHTML `a` hrefs to USC sections, numbered public laws,
and Statutes at Large pages yield typed candidates. The complete Title 1 ZIP
provides the positive evidence. Subsection tails, fragments, ranges, historical
act locators and unfamiliar namespaces remain unsupported, with the original
href preserved. Matching a held target still does not establish legal effect.

### The first build under `native-legal-reference/003`

Rebuilt on 2026-09-28 through this repository's adopted code from the retained
manifest the published `manifest_sha256` names, with no prior, and compared
with the live generation column by column: every row of both tables moves its
`rule_version` from `/002` to `/003`; 14 U.S. Code source credits'
`target_candidates_json` are re-spelled by `json_column` (an en dash escaped,
the same JSON); 51 text candidates in 31 eCFR notes name citation rules `004`
where the published rows name `003`; nothing else differs, and the lookup
resolves the same 19 occurrences. The republish is the publication lane's.
Receipt: `~/Work/corpora/fork-execution-2026-09-21/regs-adopt-052/native/rebuild-compare.json`.
