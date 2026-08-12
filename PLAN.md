# SpicyRegs plan

Recorded 2026-08-11 against SpicyRegs `6dbe181ccec7` and RefSpec
`3c1b94ace91f`. Sections 1, 3, 3a and 4 rewritten 2026-08-12 against
`a8938b4`, as records of what was executed and of the owner's decision on how
the payload lands. This file carries the SpicyRegs-owned work: the two
decisions and their execution, the branch reset, the `citations.py` boundary
fixes, the payload landing, the `rkaf_projection.py` boundary freeze, the
editable-install crossing, the canonical-JSON profile rule, and the torn
applicability policy.

The cross-product ownership boundary and its payload rule are REF-024 in
`RefSpec/docs/decisions.md`. This plan cites REF-024 by identifier and does
not restate it.

Ordering, as a constraint of this plan: sections 1 and 5 land before the
first commit of section 4. Section 3 rides in that first landing.

## 1. Two decisions, executed

Both were executed 2026-08-12 on `integrate/payload-prereqs`, branched from
`feat/rkaf-boundary-freeze` at `a8938b4`.

### 1a. URN spelling — respelled

`urn:spicy-regs:` stood at ~7,270 sites, `urn:spicyregs:` at 328 occurrences
over 129 lines in 65 tracked files (re-derived 2026-08-12 at the archive tip,
excluding `RefSpec/`, which carries 66 files of its own and is another
repository's to respell). The 2026-08-11 count recorded here — 209 lines, 341
occurrences, 78 files — was taken against a working tree that also held
untracked build output; the tracked set is what moved. The minority spelling
was the newer v3 writer, so containment decided for `urn:spicy-regs:` and the
minority moved.

Respelled: 11 modules under `src/spicy_regs/`, 3 tools, 9 test modules, both
`policies/` artifacts, both `src/spicy_regs/fixtures/spicyregs-m1-*` fixtures,
and both v3 source-selection ledgers under `tests/fixtures/`.
`document_release_v3_writer._stable_urn` and every `*_SCHEMA_ID` constant in
`document_release_v3.py` mint the new spelling.

Artifact identifiers moved, as the decision accepted:

- `spicyregs-m1-source-fixture-v1.json` reseals to `fixture_digest`
  `sha256:6fa62fa8af16754675cb77981191cdf94b98665e8f5c42609caf7256384e5ca7`.
- `spicyregs-m1-document-release-v1.json` regenerates from it through
  `build_document_release()`; its release id is now
  `urn:spicy-regs:document-release:424021f0c521d1f2e7f19cecea062e18cc1f75a247fac9603ddb5d625d88dae3`.
- Both checked-in v3 release fixtures were rebuilt from their committed
  sources rather than text-substituted: the URNs sit inside digested parquet
  and schema members, so only a rebuild keeps the seal honest. Base artifact
  digest `23d975ee516a24fcf8a4ce5056cfc307ee06461099b577f97a8bb533dddcd34a`,
  mixed `cd65b3e7340b769d0bcf6190624774fa96336f583fba13b3db3010f095971294`.
  The mixed ledger's four `oldDocumentVersionId` values are re-pinned to the
  rebuilt base's document-version ids; the digest pins in
  `tests/test_document_release_v3.py` and
  `tests/test_document_release_v3_incremental_fixture.py` follow.

The base fixture had no rebuild recipe on record and did not reproduce from
one: its parquet members carry 2 and 4 row groups where the current writer
produces 1, so it predates the writer it was built with. It is rebuilt here
with the same parameters the mixed fixture's rebuild test already uses —
`--row-batch-size 2000`, `--compression zstd`, `--memory-limit 512MB`, the
ids in its own `release.json` and `receipts/build.json`, and
`--build-run-id document-release-v3-fixture-v1` at
`2026-08-04T00:00:00Z`/`2026-08-04T00:00:01Z` — so it is reproducible from
now on.

No identifier-format validator or regex constrains this prefix. The only
compiled URN patterns in the tree are `urn:rkaf:*`
(`docpipeline/rkaf_projection.py:193`, `ontology/receipt.py:54`) and
`urn:ref:vocabulary-atlas:*` (`candidate_release.py:30`); nothing splits or
slices a `urn:spicy-regs:` string positionally, and RFC 8141 allows the
hyphen in the namespace identifier.

Two things deliberately keep the old spelling:

- Nine identifiers in dated records under `docs/evidence/` — two receipts and
  three reports from 2026-08-01 and 2026-08-02. Those are what those runs
  actually minted; rewriting them would make a dated record lie. The one
  `docs/` occurrence that stated the *format* rather than a past result
  (`scale-architecture-report-2026-08-04.md:181`) is respelled.
- Gitignored regenerable outputs are not regenerated here. Nineteen files
  under `output/` — in `agency-crosswalk-2026-08-02`,
  `body-retrieval-corpus-2026-08-02`, `date-event-artifact-2026-08-01`,
  `scale-dr-10k-2026-08-05` and `search-holdout-exam-2026-08-01` — predate the
  respell and still carry `urn:spicyregs:`. They regenerate with the new
  spelling on the next build, and no checked-in digest pins them.

The counts above are `git grep` over tracked files. A gitignore-aware search
of the whole worktree agrees on the tracked set, which is how the `output/`
residue was found separately rather than folded into the inventory.

### 1b. Byte versus codepoint offsets — audited, nothing to convert

The page the decision called for.

**What was audited.** Every offset-bearing site in the payload: the evidence
resolver `ontology/llm.py:resolve_exact_evidence_offsets`, the `start_char`/
`end_char` family (22 modules, ~380 sites, led by `docpipeline/source.py`,
`docpipeline/segments.py`, `docpipeline/retrieval.py`, `docpipeline/tag_task.py`,
`ontology/subjects.py`, `ontology/adapters.py`, `ontology/concepts.py`,
`ontology/relation_findings.py`, `ontology/ledger.py`, `rulespec_testbed.py`),
the citation and fragment spans in `docpipeline/rkaf_projection.py`, the
parser spans in `docpipeline/adapters/docling.py`, and the v3 sink
(`document_release_v3.py`, `document_release_v3_writer.py`,
`document_release_v3_verify.py`).

**What is already bytes.** The v3 sink, throughout and provably.
`_segment_utf8` (`document_release_v3_writer.py:707`) encodes to UTF-8 and
splits only on lead bytes; passages carry `normalized_start_utf8_byte` and
`normalized_end_utf8_byte` as `uint64`; `coordinate_scheme` is
`urn:spicy-regs:coordinate:rendition-utf8-byte-slice:1.0`. The verifier closes
it: `document_release_v3_verify.py:763-775` re-slices the normalized *bytes*
and fails on a split code point, and `:905-920` seeks
`byte_offset + startUtf8Byte` into the rendition pack and byte-compares.
Normalization is `rendition.decode("utf-8")` (`writer.py:881`), so the
normalized bytes *are* the rendition bytes and the coordinate's stated target
is the one it addresses.

**What counts code points.** Everything else, and each of them says so in a
constant: `docpipeline/source.py:132` `COORDINATE_UNIT = "unicode-codepoints"`,
`docpipeline/rkaf_projection.py:202` `_COORDINATE_SYSTEM =
"rkaf:unicode-codepoint"`, `enrichment/open_set.py:217`
`"rkaf:coordinateSystem"`, `docpipeline/relation_task.py:179` and
`corpora/relation_exclusion_evaluation_v2.py:179` `"offset_unit": {"const":
"unicode_codepoint"}`, `docpipeline/adapters/docling.py:989`
`unit="unicode-codepoints"`, and `document_release.py:43` (v2)
`COORDINATE_SYSTEM = "unicode-codepoints-half-open"`. Each is proved the same
way the v3 sink proves itself: by re-slicing the `str` it indexes
(`source.py:1759`, `segments.py:1017`, `rkaf_projection.py:281-288`,
`relation_findings.py:263-264`).

**Whether they disagree.** They do not, because no number crosses. The check,
run three ways:

1. No `start_char`-family field exists in any `document_release_v3*` module,
   and no `utf8_byte` field exists anywhere outside them. The two spaces share
   no column, no key, and no dataclass field.
2. The v3 module family imports nothing from `ontology/` or `docpipeline/`.
   The one edge in the other direction is `docpipeline/executor.py:25`, which
   takes `canonical_json_bytes`, `parse_canonical_json`, `require_sha256`,
   `sha256_file`, and `validate_object_key` — five helpers, no offset.
3. Every consumer of `normalized_*_utf8_byte` is inside v3's own verifier or
   its tests.

**What changed.** One site, and it is a declaration rather than a conversion.
`resolve_exact_evidence_offsets` was the only member of the code-point family
that did not name its unit, and it is the one the rest of the family calls.
It now carries `EVIDENCE_OFFSET_UNIT = "unicode-codepoints"` and
`EVIDENCE_OFFSET_INTERVAL = "half-open"`, and `EvidenceOffsetResolution`
records both on the value it returns. Nothing was converted to bytes, because
converting a self-consistent code-point space that never reaches the byte sink
would break every artifact digest in the ontology path to fix a disagreement
that does not exist.

**What the decision therefore means going forward.** "Offsets are bytes" binds
the live sink and anything new that writes into it: v3 is byte-counted and
stays byte-counted, and a new coordinate that reaches a release member is
bytes. It does not retroactively convert the code-point evidence space, which
addresses `str` values that never become release coordinates. The two spaces
stay separate and each keeps stating its unit; a future crossing has to
convert explicitly at the boundary, and there is no such boundary today.

### Landing constraint

Both decisions land before the first commit of section 4. The payload carries
`urn:spicy-regs:` in `src/spicy_regs/ontology/citations.py`,
`src/spicy_regs/ontology/attestations.py`, and
`src/spicy_regs/ontology/relation_findings.py`.

## 2. The branch reset, as a ref move

Executed 2026-08-11: `archive/local-work-2026-08-09` is a tag, and `main`
sits on `origin/main` (`f1fcb8c9c883`) plus this plan. The procedure, kept
for the record:

1. Rename `archive/local-work-2026-08-09` to a tag. An archive that moves is
   not an archive.
2. `git branch -f main origin/main` — no checkout, no `--hard`.
   `git clean -fdx` stays forbidden in that worktree.

Facts already answered on disk:

- `.github/workflows/deploy-mcp.yml` is tracked and byte-identical on
  `origin/main`.
- `mcp-server/api/index.py` and `mcp-server/api/_published.py` revert as a
  pair under the ref move.

Together those two facts mean the deployed function is unaffected by the ref
move. `_published.py`'s eventual destination is a standalone disposition
gating nothing here; it belongs to the pull-request train in section 4.

## 3. `citations.py` boundary fixes — landed

Landed 2026-08-12 on `integrate/payload-prereqs`. These ride in the first
landing of section 4, not as a second round.

- `_CFR_SECTION_CAPTURE` is ported verbatim from SpicySearch
  `identifiers.py:149`. The `_LEFT`/`_RIGHT` guards are not part of the port;
  they fix a different defect, and this module's 25 compiled patterns carry no
  boundary lookbehind at all. The port is scoped to `_CFR_STANDARD`, the one
  expression both regression cases reach — 1 of 25, as priced.
  `_CFR_TITLE_PART` and `_CFR_COMPACT` keep their own section spelling.
- The `:657` phantom loop is fixed with `(?!\d)`. The list expansion guarded
  the next citation with a negative lookahead for "CFR" and a bare `\d+`
  backtracked out from under it: on `'1CFR9,10CFR1'` the engine tried "10",
  failed the lookahead, gave back the "0", and matched "1", publishing a
  phantom `1 CFR 1` beside the two real citations. Refusing to stop mid-number
  leaves the lookahead nothing to backtrack into.
- Both regression cases are tests, both exact. Reproduced before the change:
  `'1CFR9,10CFR1'` returned three citations, `'49 CFR 900.42.'` returned none.
  Both now return exactly what they say, and
  `'40 CFR Parts 60 and 63'`, `'40 CFR 60, 61, and 63.'`, `'40 CFR 60.5-1.'`
  and the Title 3 compilation refusals are unchanged.
- `validate_body_cache` now checks the disk-to-lock direction as well. It
  walked the lock and asked whether each record's file was intact, which says
  nothing about a body on disk that the lock names nowhere — what a partial
  re-draw or an interrupted capture leaves, and what the next reader that
  globs `documents/` takes for corpus.
- The snapshot schema is at version 2, where `visibility` is a required
  artifact field. The pipeline always wrote it, but nothing required it, so a
  reader could not tell "internal" from "the producer forgot", and
  `published.py` never read it at all. `SUPPORTED_FORMAT_VERSIONS = (1, 2)` in
  both readers with the requirement keyed to the version, so a version-1
  snapshot is not held to a rule that postdates its seal.
  `mcp-server/api/_published.py` carries the same change; it is the deployed
  resolver, and leaving it at version 1 would have made it refuse every
  snapshot this build writes. Ten tests reach through
  `materialized/<dataset>/snapshots/<id>/`.

## 3a. `receipt.py` sibling-checkout default — removed

`src/spicy_regs/ontology/receipt.py:2210` defaulted `--rulespec-repo` to
`Path.cwd().parent / "rulespec"`. REF-024 forbids a product-internal path into
a sibling product regardless of destination, and a default is its worst form:
the caller who never passed the flag believes nothing about the layout, so the
wrong directory is read silently and the receipt states a provenance nobody
chose. The flag is required with no default; `build_parser()` is split out of
`main()` so the absence of a default is a fact a test reads rather than a
string a test scans for. `--spicy-repo` keeps `Path.cwd()` — the repository
the code runs inside is not a sibling escape. `build_receipt()` already took
`rulespec_repo` as a required keyword, and the one documented invocation
(`docs/rin-ontology-revision-report.md:228`) already passes the flag.

## 4. The payload lands as direct commits on local `main`

Decided by the product owner 2026-08-12: there is no pull-request train. The
payload lands as direct commits on the local `main`. No pull requests, and
with them nothing that only existed to serve a review: no per-request
blast-radius description (the 87.9% row collapse and the breaking
`comment_periods` schema are still true and still belong in the commit
messages that carry them, but they are not a review preamble), and no
raising of the pymupdf AGPL question inside a pull request. That question is
not answered here; it is unasked, because the venue it was to be asked in
does not exist.

What survives the decision:

- **The order, unchanged and still forced.** `ontology/` lands first: 14 of 18
  `docpipeline/` files import it, and `ontology/` itself carries zero
  `refspec` strings. `build_fr_docket_links.py` lands separately, on its own
  weaker evidence.
- **The REF-024 payload rule, dormant rather than repealed.** No import, path,
  or URN in a payload names an owned product. Nothing is shipping upstream, so
  the rule gates nothing today — a local commit on this repository's own
  `main` is not a crossing. It still governs any future upstream
  contribution, and the two crossings it named stay tracked so the rule has
  something to be true about when that day comes:
  - `src/spicy_regs/ontology/receipt.py:2210` is fixed — see section 3a. It
    was fixed as boundary hygiene the landing wants, not as a gate the landing
    requires.
  - `src/spicy_regs/docpipeline/rkaf_projection.py:2334` still does
    `from refspec import (...)`. It stays tracked under section 6's
    editable-install work, which is where an import of an installed `refspec`
    wheel stops being a sibling source-tree crossing at all.

### The landing, executed

Executed 2026-08-12 in a second worktree, `../spicy-regs-landing`, checked out
on `main` at `c8f5313`. Three commits, nothing pushed, no pull request. The
`integrate/payload-prereqs` worktree and its branches were not touched.

- `7a7c193` `feat(ontology): land the ontology package ahead of everything that
  imports it` — 33 files: the 21 modules of `src/spicy_regs/ontology/` and the
  12 test modules whose only `spicy_regs` imports are `spicy_regs.ontology.*`.
  No payload fixture is reached by those twelve.
  `tests/test_ontology_citations.py` is not among them; it also reads
  `conformance/rulespec-l0.yaml` and walks `tools/`. The message leads with the
  blast radius this section moved out of a review preamble: `rule_targets`
  335,008 → 40,546 rows, 87.9%; `comment_periods` from scalar `proceeding_id`,
  `rin` and `docket_id` to `proceeding_ids_json`, `rins_json` and
  `docket_ids_json` with `opened_by_artifact_ids_json` added, 283,367 → 254,445
  rows. Both are the baseline/candidate pair in
  `docs/evidence/rulespec-stabilization-2026-07-24/paired-receipt.json`, which
  lands in the next commit.
- `246788f` `feat: land the rest of the payload behind ontology/` — 595 files:
  `docpipeline/`, `corpora/`, `transforms/`, `sources/`, `enrichment/`,
  `pipelines/`, `candidate_release.py`, `published.py`, the DocumentRelease v3
  module family, the source-profile builders, `rulespec_testbed.py`,
  `evaluation_boundary.py`, 26 tools, 107 further test modules with 19 test
  fixtures, 56 release fixtures under `fixtures/`, `policies/`,
  `conformance/`, `mcp-server/api/index.py` and `_published.py`, 13 workflows,
  `pyproject.toml` and `uv.lock`, and 256 records under `docs/`.
  `build_fr_docket_links.py` rides here: it stays separable on its own
  evidence, but there is no pull-request train for it to be separate in.
  `tests/test_rkaf_projection_boundary.py` rides here too. This plan's ordering
  constraint puts section 5 before the first commit of section 4, and on
  `integrate/payload-prereqs` the freeze was written first, at `ea56203`, ahead
  of the payload it constrains. On `main` it cannot land before this commit:
  the freeze reads `src/spicy_regs/docpipeline/rkaf_projection.py` by AST, and
  that file arrives here.
- This commit — this record.

Where `main` and the payload both moved a file:

- `main` renamed the public bucket `r2.spicy-regs.dev` to `data.spicy-regs.dev`
  (#150), and that rename was its *only* change to nine files the payload also
  touched. The payload's content landed with the rename applied on top, so
  neither side lost anything: `src/spicy_regs/cli.py`,
  `src/spicy_regs/data_dictionary.py`, `src/spicy_regs/mcp_server.py`,
  `mcp-server/api/index.py`, `.env.example`, `docs/index.md`,
  `docs/changelog.md`, `docs/querying-python.md`, and the skill's
  `references/data-layout.md`. Four occurrences of the old spelling remain and
  are deliberate: three in the sealed
  `docs/migration/spicysearch-product-migration-manifest.json` (two of them
  pinned by `tests/test_document_release.py:658-659`), one in
  `src/spicy_regs/corpora/mixed_real_data.py:47`, and one in
  `scripts/check_rollup_freshness.py:1` that `main`'s own rename had already
  missed.
- `README.md` is `main`'s. The payload's version carried a "Product boundary"
  section naming the four-product split and `policies/`, the
  `build-document-release-from-files` and
  `validate-document-release-distribution` commands, and a "Historical
  managed-vocabulary incubation" section. None of that is on `main`, and none
  of it landed; re-stating it is a separate edit against the newer public
  README.
- The 18 files only `main` changed are untouched: `assets/` (2), eleven
  notebooks, `notebooks/README.md`, `mcp-server/README.md`,
  `src/spicy_regs/generate_analytics.py`, and the skill's two scripts.

Not landed: `.gitmodules` and the `RefSpec` gitlink. `RefSpec/` is a nested
repository and `main` tracks no gitlink for it. The consequence is on record
rather than fixed: `pyproject.toml:147` names a `RefSpec` path `main` does not
carry, so a fresh clone of `main` cannot build the environment until section
6's `[tool.uv.sources]` override is retired.

`.github/workflows/deploy-mcp.yml` is blob `ce1a4b99` on `origin/main`, on
`main` and on `integrate/payload-prereqs`. The deployed function's build is
unchanged by this landing.

The suite at the landed state, `uv run pytest -q -p no:randomly
--continue-on-collection-errors`: 3,009 passed, 8 failed, 50 skipped, 4
deselected, 2 xfailed, 1 collection error, 219s. The same command on
`integrate/payload-prereqs` under the same environment returns 3,008 passed, 8
failed, 51 skipped, 1 collection error — the same eight failures and the same
error, by name, and the same 3,059 collected. The one-test difference is
`test_docpipeline_rkaf_projection.py:1334`, which needs a sibling `rulespec`
checkout and found one beside `../spicy-regs-landing` and none beside the
scratch checkout the comparison ran in; it is a property of where the two
worktrees sit, not of what they contain.
Four failures and the collection error are RefSpec mid-change
(`test_candidate_release_protocol.py`, three in
`test_docpipeline_rkaf_projection.py`, `test_enrichment_reference_runtime.py`);
one is the torn applicability policy of section 8
(`test_source_profile_artifacts.py`); three are the optional `embed` extra
being absent from the environment, and say so —
`ModuleNotFoundError: sentence_transformers`, `torch`, and
`PackageNotFoundError: transformers`. `tests/test_rkaf_projection_boundary.py`,
the section 5 freeze, passes: 3 tests, 0.10s.

## 5. `rkaf_projection.py` boundary freeze

`tests/test_rkaf_projection_boundary.py` carries the freeze, written
2026-08-11 against `src/spicy_regs/docpipeline/rkaf_projection.py` at
`6dbe181ccec7`, before the landing in section 4 moves `ontology/`. It runs in
the default suite: `testpaths = ["tests"]`, no marker, three tests, 0.1s.

The frozen surface, AST-derived from the file: 17 outbound import statements,
16 naming `spicy_regs` and 1 naming `refspec` at line 2334. The 16 reach 13
distinct `spicy_regs` modules across four subpackages — `candidate_release`,
`docpipeline`, `enrichment`, `ontology`. Three statements name
`spicy_regs.docpipeline.source`, two name `spicy_regs.ontology.common`, and
the other 11 modules are named once each. Every remaining import in the file
is standard library; the file carries no relative and no dynamic import.

The freeze records module and imported names, not line numbers or file order,
so imports may move within the file. It records every non-stdlib import, not
only the packages present today, so a new sibling product or third-party
dependency fails it too. One added crossing fails two of the three tests; a
relative or dynamic import fails the third. Each failure states that the list
is the priced boundary for the four-way split and that moving it is a
deliberate change to the freeze and to this section together.

The test shape came from `RefSpec/tests/test_atlas_index.py:490-497` and
DocSpec's `tests/test_package_boundary.py`.

## 6. The editable-install crossing

`pyproject.toml:147` sets `refspec = { path = "RefSpec", editable = true }` —
a sibling source-tree consumption REF-024 forbids. The exact pin at
`pyproject.toml:12` (`refspec==0.1.0.dev0`) is already correct. Delete the
`[tool.uv.sources]` override once the RefSpec package is consumable from an
installed wheel; RefSpec's `PLAN.md` schedules the input resolver that makes
it so. The REF-024 payload rule in section 4 does not cover this crossing —
it governs what ships upstream, and this line never ships. It is also where
`rkaf_projection.py:2334`'s `from refspec import (...)` is answered: an
import of an installed wheel is not a sibling source-tree crossing, so
deleting the override retires both at once.

## 7. Canonical-JSON profiles precede any dedup

Counted 2026-08-11 at the commits in this plan's preamble: the workspace
holds ~33 canonical-JSON implementations — 9 in `src/spicy_regs`, 7 in its
tools, 9 in `RefSpec/src`, 4 in `RefSpec/tools`, 2 in `rulespec/tools`, 2 in
SpicySearch, 2 in DocSpec. At least three profiles exist de facto:
SpicySearch `artifact_protocol.py:127-128` rejects every float and bounds
integers; SpicySearch `canonical.py:213` permits floats and has ~100 call
sites including float-bearing metric reports; SpicySearch
`atlas_search_view.py:279` encodes a third (REF-JSON).

Name the profiles before merging any implementation — RFC 8785 for artifact
identity, a float-permitting profile for reports — then converge each behind
a shared conformance vector set. Deleting `canonical.py:213` as a one-line
dedup would make eight subsystems raise on any float payload.

## 8. The applicability policy is torn against RefSpec's catalog

`policies/profile-resource-applicability-v0.json` (on `main` since the section
4 landing) pins `refspecResourceCatalog` at
`sha256:c0bcce7318ac6fedce2e0c51e77186ef32cf5c9d1039aa2170d7b28422de7dbe`,
while RefSpec's republished `portfolio/resource-catalog-v0.json` states
`catalogDigest`
`sha256:a731fef9a49b3af10813febea52a89a8e69c02b43fe6f938695bba98462b3515`.
The `profileCatalog` half of the policy set agrees; only the RefSpec catalog
moved. SpicySearch's `test_policy_inputs_cross_repository.py:78` suspends on
exactly this tear and converts to a pass when it closes.

Regenerate the applicability policy against RefSpec's catalog after the
validation-cost reset (`RefSpec/plans/validation-cost-reset-plan.md`) stops
moving that repository; regenerating against a moving catalog re-tears it.
