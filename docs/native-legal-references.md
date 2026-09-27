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
option and central dictionary/CLI registration; a successful build does not
claim publication or complete edition coverage.

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
scope, input digest and ordinal. Scope hashes source family, record key and
explicit edition; XML path, tag, attributes and ancestors retain source
location. CFR `AUTH` and `SOURCE`, USC hrefs and source credits keep distinct
roles. Interpreted candidates are nested JSON, so several typed references
inside one note do not multiply or erase the native observation.

Exact native USC section hrefs can supply section keys. Fragments, subsection
paths, unknown namespaces and unsupported href forms remain literal rows.
Notes/source credits use only the existing provider's selected legal citation
rules; their findings are labeled partial, with exact text spans, digest and
rule. Unmatched text remains in the complete note. CFR part-only findings stay
`cfr_part` and unsupported for section lookup. Executive orders and historical
compilation shapes are not silently converted into supported targets.

The read table records `complete_selected_shapes`, including successful zero
results. This means the selected owner scanner completed, not that every legal
reference was understood. CFR `PARAUTH`/`SECAUTH` remain explicitly unsupported.
No current acquisition or global title/edition completeness is inferred.

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
