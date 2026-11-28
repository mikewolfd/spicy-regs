# Replay native legal references from retained XML

`NativeLegalReferencesRollup` builds `native_legal_references.parquet` and
`native_legal_reference_reads.parquet` from an explicit local manifest. It uses
SpicyDocs' CFR, U.S. Code, bill, law and Statutes at Large scanners and the ordinary rollup generation and
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
currently name rule `native-legal-reference/004` (adding named readable-text
renditions, `unicode_code_points` character-offset spans beside positional
XPaths, and `statute` extraction); prior builds read before `ss-tdf4` name
`/003`, and rows this repository read before the reading moved name `/002`.

This repository keeps the manifest, the input pins, the source evidence, the
qualification of complete inputs, the scan loop and the target lookup. The
reading receives the lookup as `resolve`: one call for the whole run to
`citation_resolution.resolve_citations` over the target tables the manifest
pins, so each distinct typed key is read once, and the run's lookup coverage
is journalled as `native-reference-resolution`. Both tables merge through
their contracts, replacing each scope a complete read covers.

## Input selection and evidence

Each `sources` entry requires a local `path`, exact `sha256:` digest,
`source_family` (`ecfr`, `uscode`, `bill`, `law` or `statute`), `source_record_key`, `source_locator`, and
`edition` (a literal or explicit JSON null). Relative paths resolve against the
manifest directory. The manifest, exact XML bytes and optional target bytes
enter the existing `CaptureEvidence` blob store and journal. Events say
`retained-input`, not a fabricated new HTTP capture.

Optional `targets` map an existing resolver table name (or `uscode_edition_sections`,
which installs `usc_release_point_sections`) to a local Parquet `path` and exact
`sha256:` digest. Reads use a verified copy of those bytes. Result candidates name
the target file pin and the existing resolver's grain under `selected-target-lookup/2`
(policy `legislative-documents/4`, `list<native-legal-target/4>`). For example,
`usc_release_point_sections` matches sections or range stubs printed at the held OLRC
release point, not Code text or proof of current applicability (consult
`usc_citation_verdicts` for OLRC legal status and 1994–2024 annual-edition presence).
Prior generations (`legislative-documents/1` and `legislative-documents/2` recorded
against `law_code_sections`, and `legislative-documents/3` recorded under
`selected-target-lookup/2` prior to span columns) remain readable and their carried
candidate keys stay linkable. An unknown source edition stays unknown even when a
target lookup finds a release-point section.

Bounds are explicit: at most 100 source inputs, 16 MiB per XML and 64 MiB total
XML, 10,000 selected observations and 10,000 distinct target keys per run
(`NATIVE_MAX_TARGET_KEYS`, matching `MAX_OBSERVATIONS` while leaving
`resolve_citations`'s interactive default at 500), and 64 MiB per optional
target file. Owner scanner depth/text limits remain active. The shared resolver
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

## Retained bill and law adapters

The replay also accepts `source_family` values `bill` and `law`. Bill DTD
`external-xref` elements preserve `legal-doc` and `parsable-cite` as literal
attributes; they never acquire a fabricated href. Bill and law USLM href/ref
observations preserve their original namespace, attributes and positional
ancestry. Each family has its own source/record/edition replacement scope.

For bill DTD references, only an unqualified `external-xref` with
`legal-doc="usc"` and an exact `usc/<title>/<section>` spelling yields a Code
section candidate. Subsection tails, ranges, contradictory document labels and
unknown namespaces remain untyped. In bill/law inputs, GPO USLM `ref` hrefs use
the existing exact USC/public-law/Statutes-at-Large grammar. These additions name
`native-legislative-exact-reference/001`; existing U.S. Code and eCFR readings
remain unchanged. The original XML retains the element text; occurrence rows
retain the native reference attributes rather than claiming a text extraction.

`qualification.kind="bill-dtd-xml"` checks the native bill and printing against
`congress`, `bill_type`, `number` and `package_id`. The source record key is the
complete `BILLS-...` package ID, and `edition` is its validated printing version,
not a date. Other bill XML dialects remain local scanner inputs until an owner
identity validator qualifies them. Inert external DTD declarations are accepted
without loading them; internal entities and malformed XML refuse.

`qualification.kind="law-native-xml"` checks `congress`, `law_kind` (public or
private) and `number` against the law's native metadata and citable form. Its
record key is `<congress>-<public|private>-<number>`. Law `edition` stays null:
approval and processing dates do not establish an edition. `supporting_evidence`
can retain pinned original journals beside either input; native identity
validation alone does not establish historical acquisition time.

Both adapters retain the existing source/total byte, observation and replacement
bounds. Law source credits and unmarked prose are outside the selected forms.
Successful empty scans replace only their exact scope; malformed or capped reads
replace nothing. Adapter qualification covers only the named complete inputs.
Per-table bill/law rebuilds, pre-bulk history and full congressional coverage
require their own evidence; see the Stream E execution ledger. Local source
journals and qualification facts can contain private paths and must pass the
release lane's public-field review before publication.

## Wave 1 declared BILLS/PLAW cohort (`ss-qggu`)

`manifests/native_legal_wave1.json` declares a bounded, reproducible Wave 1
replay cohort (`native-legal-wave1-119th-bills-plaw-v1`) selected strictly from
retained 119th Congress originals in the central raw archive (zero new network
acquisition) and validated under `bill-dtd-xml` and `law-native-xml`:

- **Composition & measured bounds:** 50 complete XML inputs (`<= 75` target,
  `<= 100` pipeline cap) totaling `14,132,042` bytes (`13.48 MiB <= 32 MiB`
  target, `<= 64 MiB` cap) and `7,064` observations (`<= 8,000` target,
  `<= 10,000` cap), yielding `3,401` exact U.S. Code section candidates:
  - 25 `bill` DTD XML packages (`3,158,658` bytes; `1,628` observations;
    `1,250` `native_bill_usc_cite`, `378` `unsupported_href`) spanning Senate
    enrolled (`BILLS-119s5enr`), House engrossed (`eh`), reported (`rh`),
    placed-on-calendar-Senate (`pcs`), and introduced (`ih`) printings.
  - 25 `law` USLM XML enactments (`10,973,384` bytes; `5,436` observations;
    `2,151` `native_section_href`, `593` `native_public_law_href`, `183`
    `native_statute_href`, `2,509` `unsupported_href`) spanning House- and
    Senate-origin 119th Public Laws (`PLAW-119publ1` through `PLAW-119publ108`).
- **Scope-preserving merge:** Replaying `manifests/native_legal_wave1.json`
  against the existing two-scope prior (`uscode` Title 1 `119-103` with `821`
  observations + `ecfr` Title 1 `requested-as-of:2026-08-10` with `60`
  observations) merges by `scope_id` into `7,945` sealed `native_legal_references`
  rows and `52` `native_legal_reference_reads` rows covering all four
  `source_family` values (`bill`, `ecfr`, `law`, `uscode`).
- **Portable replay:** `manifests/native_legal_wave1.json` embeds no local
  machine paths (compatible with `CaptureEvidence`'s published-control check).
  Each source entry records its exact `sha256`, byte count, canonical GovInfo
  `source_locator`, `qualification` block, and central raw archive descriptor
  (`archive_object` plus `archive_encoding` or `archive_zip_member`). Stage the
  verified original XML bytes under `inputs/<filename>` beside a copy of the
  manifest before running `run-rollup-native-legal-references`.

### Raw visual review (2026-11-27, bounded sample)

A bounded visual inspection of rendered original XML/HTML excerpts verified
attribute extraction and `usc_place` separation against retained originals:

- **BILLS DTD XML (3 packages):**
  - `BILLS-119s5enr` (`objects/sha256/df/df4b19ff...`, `sha256:df4b19ff99c27f8f6dcc5098fe82c726e33666c78d65126d1622f611bf8b94c9`,
    L15 chars 2268 & 6743): `<external-xref legal-doc="usc" parsable-cite="usc/8/1226">8 U.S.C. 1226(c)</external-xref>`
    and `usc/8/1225` extract as `native_bill_usc_cite` (`8-1226`, `8-1225`),
    recording the enclosing section without fabricating subsection tails.
  - `BILLS-119hr6644eh` (`objects/sha256/99/99317586...`, `sha256:99317586072b7117889ff6d2bbc9cc888cf52b4931356211788d28a166dd996b`,
    L108 char 22396, L131 char 32754, L266 char 104745): `usc/42/12705d` and
    `usc/42/5302` extract cleanly; `legal-doc="public-law" parsable-cite="pl/117/58"`
    remains `unsupported_href`. **Publisher idiom observed:** congressional bill
    DTD XML places the word `note` outside the tag (`<external-xref legal-doc="usc" parsable-cite="usc/42/8301">42 U.S.C. 8301</external-xref> note)`).
  - `BILLS-119hr8646rh` (`objects/sha256/97/975fef02...gz`, decoded `sha256:975fef02625a869c314d1e2c7b5841039dbf028cd2159b68e92256d536700ffb`,
    L15 chars 7478, 7606, 10477): `usc/7/6924` and `usc/40/121` extract as
    `native_bill_usc_cite`; `pl/115/334` remains `unsupported_href`.
- **PLAW USLM XML (3 enactments):**
  - `PLAW-119publ1.xml` (`119-public-1`, `sha256:ee0e7a5d534411f78dd325405c42d386a1cfcf14f3870c93f5488874bb7acb54`,
    L29, L32, L50), `PLAW-119publ27.xml` (`119-public-27`, `sha256:9c7bf33496c2f65085234dbf7f5cecaa72919520912c43f9f7769264e261d282`,
    L29, L32), and `PLAW-119publ101.xml` (`119-public-101`, `sha256:9f4bc8d57d39bf4d84442f3e68e0fd679a62878ce7c78909e687a70401616a8e`,
    L35, L38, L248): body `<ref href="/us/usc/t8/s1226">` and `/us/usc/t12/s1701x`
    extract as `native_section_href` (`8-1226`, `12-1701x`); `/us/bill/119/...`
    stays `unsupported_href`. **Publisher idiom observed:** GPO USLM `<sidenote>`
    markup assigns a bare section `href` (`/us/usc/t8/s1101`, `/us/usc/t12/s5901`,
    `/us/usc/t12/s1701`) even when the visible sidenote text reads `"8 USC 1101 note"`.
    Under `native-legal-reference/003` (attribute-only extraction, `text` NULL),
    those literal hrefs remain `native_section_href`; `ancestors_json` preserves
    the enclosing `sidenote` element so Wave 4 (`ss-tdf4` / `ss-xpyk`) can
    distinguish sidenote note-labels from body citations once span/text
    extraction lands.
- **OLRC classification `nt new` controls (2 HTML originals):**
  - 119th Cong. 1st Sess. (`objects/sha256/4b/4be913e3...gz`, decoded
    `sha256:4be913e3e1554d4f23b948f9a0cd071857dda3ae15e570d08c4a9d30084f2550`,
    L329): `8 1101 nt new 119-1 1 139 Stat. 3` -> `usc_place = 'note'`.
  - 119th Cong. 2nd Sess. (`objects/sha256/b0/b007f218...gz`, decoded
    `sha256:b007f218b16e0bb8314d85cf86acd8d42dda6b2066c94f40f394360bd6873236`,
    L331): `1 1 nt new 119-74 3 140 Stat. 5` -> `usc_place = 'note'`.
  - In `usc_citation_verdicts` (`usc-release-point-verdict-v2`), both controls
    remain distinct from their printed main sections (`8-1101` and `1-1`) and
    emit `verdict = 'not_checked_non_section_place'`, `reason = 'non_section_place'`,
    `release_point_rows = 0`.
- **Sample limits:** Visual inspection covered 6 legislative XMLs (3 BILLS, 3
  PLAW) and 2 OLRC session HTML tables; it verifies scanner fidelity, manifest
  qualification, and `usc_place` separation for this bounded Wave 1 cohort and
  does not claim exhaustive coverage of all 119th Congress printings.

## Relationship-view integration and Wave 4 seam

- **`usc_citation_verdicts` (`usc-release-point-verdict-v3`):** Keyed by
  `(usc_title, usc_section_key, usc_place)` across `document_citations`,
  `law_code_sections`, `table3_records`, and sealed `native_legal_references`
  (unnesting native Arrow `LIST<STRUCT<...>>` column `target_candidates` via
  `UNNEST(target_candidates)`). Exact native XML section targets
  (`native_section_href`, `native_bill_usc_cite`) attribute to
  `usc_place = 'section'`; text-parsed findings (`partial_text_findings` and
  `document_citations`) attribute to `usc_place = 'unstated'`; classification
  and Table III places (`section`, `note`, `preceding`, `note_preceding`) carry
  verbatim (NULL/blank coalesced to `unstated`). Only `section` and `unstated`
  rows join `usc_release_point_sections` and the SHA-256-pinned vendored OLRC
  oracle tables (`usc_oracle_sections`, `usc_oracle_ranges`,
  `usc_oracle_annual_sections`, `usc_oracle_annual_ranges`) to surface legal
  status (`oracle_status`, `oracle_statuses`, `oracle_status_source`) and
  1994–2024 annual-edition presence (`annual_presence`, `annual_only`,
  `annual_first_year`, `annual_last_year`, `annual_edition_count`, and
  `reason = 'annual_only'`). `note`, `preceding`, and `note_preceding` rows
  emit `not_checked_non_section_place` (`reason = 'non_section_place'`,
  `release_point_rows = 0`, NULL oracle/annual columns).
- **Wave boundaries & handoffs:**
  - **`ss-xpyk` (delivered):** Migrated `native_legal_references` target lookup
    to `selected-target-lookup/2` (`usc_release_point_sections`, policy
    `legislative-documents/3`, `list<native-legal-target/3>`, preserving
    `/1` and `/2` prior-generation readback and carried `law_code_sections`
    navigation) and extended `usc_citation_verdicts` to
    `usc-release-point-verdict-v3` with `usc_oracle_*` legal-status and
    1994–2024 annual-presence enrichment, cross-title and `nt new` negative
    controls, and explicit non-support for automatic letter-suffix repair.
  - **`ss-tdf4` (delivered):** Adds character-offset spans (`text_rendition`,
    `text_offset_unit`, `text_rendition_sha256`, `text_span_start`,
    `text_span_end`, `span_text`, plus per-candidate `rendition_span_start` and
    `rendition_span_end`) beside positional XPath `source_path`, extends native
    scanning and qualification to Statutes at Large (`STATUTE-*.xml`,
    `source_family = 'statute'`, `qualification.kind = 'statute-native-xml'`),
    bumps `NATIVE_LEGAL_REFERENCE_RULE` to `native-legal-reference/004`, and
    advances `native_legal_references` to `legislative-documents/4`
    (`list<native-legal-target/4>`) and `native_legal_reference_reads` to
    `legislative-documents/3` while preserving exact backward-compatible
    restoration of `legislative-documents/1`, `/2`, and `/3` XPath-only bundles.
  - **`ss-2sor` (Wave 4):** Upgrades `document_citations` extraction with
    proposed-section and enclosing-section grammar while maintaining parity
    between `document_citations` and `native_legal_references`.

## Character-offset spans and Statutes at Large (`ss-tdf4`, `native-legal-reference/004`)

### Rendition and offset contract

Every `/004` read and observation records character-offset coordinates in a
named readable-text rendition produced by `spicy_docs.reading.xml_text`
(ported from RefSpec `xml_text` with byte-for-byte parity on USLM 1.0 and
eCFR profiles, and extended to GPO USLM 2.x and Congressional Bill DTD):

- **Offset unit:** `text_offset_unit = 'unicode_code_points'` — 0-based,
  half-open `[start, end)` Python `str` code-point indices into the complete
  decoded rendition string (never UTF-8 byte offsets and never UTF-16 code
  units).
- **Named renditions (`text_rendition`):**
  - `uslm-block-boundaries/1`: U.S. Code (`uscode`), Public/Private Laws
    (`law`), Statutes at Large (`statute`), and USLM bills.
  - `ecfr-block-boundaries/1`: eCFR (`ecfr`).
  - `bill-dtd-block-boundaries/1`: Congressional Bill DTD (`bill`).
- **Traceability beside positional XPath:** `source_path` retains its exact
  namespace-independent positional XPath (`/*[1]/*[2]`), never a byte offset.
  `text_rendition_sha256` binds `[text_span_start, text_span_end)` to the exact
  UTF-8 rendition string, `span_text` records the exact slice
  `rendition_text[text_span_start:text_span_end]`, and each typed candidate in
  `target_candidates` carries `rendition_span_start` / `rendition_span_end`
  projected through `source_map` (so multi-block eCFR `<AUTH>`/`<SOURCE>` notes
  with inserted `\n\n` boundaries still slice the exact cited text). Empty or
  self-closing elements carry NULL offsets/slice text while preserving
  `source_path` and rendition metadata.
- **Backward compatibility:** Earlier `/002` and `/003` XPath-only generations
  (`native_legal_references` `legislative-documents/1`, `/2`, and `/3`;
  `native_legal_reference_reads` `legislative-documents/1` and `/2`) restore
  cleanly through `policy_migrations.json` and `recorded_subject`, and
  incremental scope merges NULL-fill the appended span columns for untouched
  prior scopes without mutating their `source_path` or `rule_version`.

### Statutes at Large (`STATUTE-*.xml`) workflow & qualification

- `source_family = 'statute'` scans GPO USLM `<statutesAtLarge>` bulkdata XML
  (`https://www.govinfo.gov/bulkdata/STATUTE/{volume}/STATUTE-{volume}.xml`) via
  `spicy_docs.sources.govinfo.native_references.scan_statute_references`.
- `qualification.kind = 'statute-native-xml'` validates the requested integer
  `volume` against root `<statutesAtLarge><meta><volume>` via
  `validate_statutes_at_large_xml` (supporting multi-Congress early volumes
  such as Volume 1), requires `source_record_key = 'STATUTE-{volume}'`,
  `source_locator = statutes_at_large_xml_locator(...)`, and `edition = null`.
- Missing volumes (`404`/`410`), wrong volumes (`UslmIdentityError`), wrong
  roots, malformed XML, internal DTD entities, and byte/observation overruns
  refuse fail-closed without touching prior scopes.

### Raw visual review & independent span oracle (2026-11-27)

Verified three complete GovInfo bulkdata volumes spanning 1789, 1951, and 2023
(7,454 observations total) against an independent ElementTree positional XPath
walk and code-point slice check (**0 mismatches** across all 7,454 rows):

- **`STATUTE-1.xml`** (`5,416,088` bytes, UTF-8 BOM + CRLF, Last-Modified
  `2026-04-09 19:35:25 GMT`, `sha256:3493b9f7feafe4fd8e4791a450d5dd3dd4fe1b4dfa798ec75242673e825482c6`,
  Congresses 1–5, 776 `<page>` identifiers): `3,657,547` code points
  (`text_rendition_sha256 = sha256:340f621a...`); `209` `<ref class="footnoteRef">`
  observations (0 with `href`; all `unsupported_href`, e.g., `idx=0`
  `xpath=/*[1]/*[3]/*[1]/*[3]/*[1]/*[2]/*[2]/*[3]`, `span=(158958, 158961)`,
  `span_text='(a)'`).
- **`STATUTE-65.xml`** (`9,962,266` bytes, UTF-8 BOM + CRLF, Last-Modified
  `2026-02-11 17:58:02 GMT`, `sha256:d6c73bf6470d2ecee9e6fa2867afae0218495021051248af38c36c59b0c6ee66`,
  82nd Congress, 1,069 `<page>` identifiers): `4,420,767` code points
  (`text_rendition_sha256 = sha256:13425cec...`); `4,261` observations (`1,761`
  `native_statute_href`, `945` `native_section_href`, `1,555` `unsupported_href`):
  - L923 byte 289812 (`idx=2`, `native_statute_href` -> `63-612`): `span=(103381, 103393)`, `span_text='63 Stat. 612'`.
  - L923 (`idx=3`, `native_section_href` -> `50-847`, Unicode `§`): `span=(103396, 103428)`, `span_text='50 U. S. C., Sup. IV. app. § 847'`.
  - L1380 byte 325320 (`idx=39`, nested `<i>et seq</i>`, `unsupported_href` `/us/usc/t46/s1245/1111`): `span=(123013, 123055)`, `span_text='46 U. S. C. § 1245; Sup. IV, § 1111 et seq'`.
- **`STATUTE-137.xml`** (`12,487,295` bytes, Last-Modified `2026-04-15 18:47:51 GMT`,
  `sha256:ef857f483fbf65abda953d041266bfb6c697fba2defad05c5c6e54c7f2592f5f`,
  118th Congress, 1,439 `<page>` identifiers): `4,190,536` code points
  (`text_rendition_sha256 = sha256:0c9e0b92...`); `2,984` observations (`1,269`
  `native_section_href`, `308` `native_public_law_href`, `206`
  `native_statute_href`, `1,201` `unsupported_href`):
  - L1509 byte 118393 (`idx=7`, body `native_section_href` -> `50-1622`): `span=(25504, 25518)`, `span_text='50 U.S.C. 1622'`.
  - L2114 (`idx=29`, Unicode en-dash `–`, `native_public_law_href` -> `117-public-159`): `span=(52184, 52202)`, `span_text='Public Law 117–159'`.
  - L2672 byte 424210 (`idx=218`, `native_statute_href` -> `72-1263`): `span=(146805, 146818)`, `span_text='72 Stat. 1263'`.
  - Repeated identical citation in the same `<content>` element (`15 U.S.C. 18a`, L12667+): `idx=1212` `span=(965454, 965467)` and `idx=1213` `span=(965605, 965618)` retain distinct XPaths and exact disjoint offsets.
  - Sidenote note-label visibility (`idx=3`, `href='/us/usc/t50/s3161'`): `span=(21992, 22008)`, `span_text='50 USC 3161 note'` now exposes the literal `note` text alongside `ancestors_json`.
- **Sample limits & offline test fixtures:** Declared coverage covers these
  three verified volumes (and offline bounded excerpts `statute-65-excerpt.xml`
  and `statute-137-excerpt.xml` pinned in `spicy-docs` / `spicy-regs` test
  fixtures); Volumes 139+ are not yet published in GovInfo STATUTE bulkdata
  (returning HTTP 404 -> `UslmSourceUnavailableError`), and volumes exceeding
  the 16 MiB per-XML manifest cap (such as `STATUTE-135.xml` at 22.1 MiB)
  refuse cleanly unless bounds are explicitly raised.
