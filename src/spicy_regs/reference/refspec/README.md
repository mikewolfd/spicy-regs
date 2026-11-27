# RefSpec vendored bytes: agency projection, agency registry view, U.S. Code oracle and act sections

These files are RefSpec's and must never be edited in place. A new RefSpec
release is a new copy, with new digests pinned in `spicy_regs/ontology/agencies.py`.
The loader there reads the tables and refuses any bytes whose sha256 is not the
pinned one.

Bytes are pinned; logical digests are compared. A RefSpec lock change (a new
pyarrow, say) can move a file's bytes without changing its rows. Such a copy is
re-vendored with new byte pins only after its logical digest is shown equal to
the one recorded here. A changed logical digest is a new release, which needs
its own review.

## Agency registry view (REF-072 / REF-075 / REF-077, `agency-registry-view/`)

The release `agency-registry-2026-09-26` unifies REF-038 and four owner-decided batches into one
digest-pinned directory (`schemaVersion` 1.4, `docs/decisions.md#ref-072`, `#ref-075`, `#ref-077`):

- REF-038 agency projection (derived inside View 1.4, row-for-row and schema-for-schema equal to
  the retired standalone Oct 3 / Aug 21 copy): 321 resolved Regulations.gov codes and 10 documented
  abstentions (`agencyProjectionDigest`
  `sha256:0dd32dd5320a0d1553f10818a7a31e95e2c3b9b6567df224dd359a8038e14db4`);
- batch 1 (2026-09-26, 26 decisions): 13 identity bridges from Federal Register agencies to eCFR or
  Federal Hierarchy organizations; 9 dated successions (5 renames and 4 splits, 14 event rows);
  4 recorded non-emissions;
- succession batch (2026-10-03, 1 decision): Food and Nutrition Service (FR 200) renamed Food and
  Nutrition Administration (FR 625), effective 2026-06-24;
- alias batch 1 (2026-11-08, REF-075, 70 decisions): 63 adopted aliases + 7 rejected aliases
  (`aliasRejectedByOwner` non-emissions);
- alias batch 2 (2026-11-25, REF-077, 94 decisions): 94 adopted federal sub-unit and statutory name
  aliases, bringing total adopted aliases to 157 across 191 decided items;
- derived tables rebuilt and checked by RefSpec's verifier: `current-successors` (15 rows),
  `organizations` (all 472 pinned Federal Register agencies), and REF-038's `projection` (321 rows)
  and `projection-unresolved` (10 rows).

Vendored from RefSpec `main` (REF-077, `a3e7f5a6` / PR #2 `96c62e00`, `schemaVersion` 1.4), built
and verified with `uv run --frozen python tools/build_agency_registry_view.py --verify <dir>
--expected-manifest-sha256 sha256:c89935debe320e35aaf748ea0519457c1618b49e13d79ddb33cbc30051b5a118`
on RefSpec's frozen lock (pyarrow 25.0.1). It replaced both the Oct 3 `f1c23b91` schema 1.2 view
(manifest `0b39812d…`) and the three separate legacy standalone REF-038 files
(`agency-projection.parquet` `c9ec0fde…`, `agency-projection-unresolved.parquet` `e32e814c…`, and
top-level `view-manifest.json` `991acd29…`). The two REF-038 tables inside View 1.4 have identical
rows and schemas to the retired standalone files (bytes moved only from pyarrow 23.0.0 to 25.0.1).

| File | Bytes | Rows | sha256 |
| --- | ---: | ---: | --- |
| `view-manifest.json` (the single pin RefSpec's build tool names) | 4,578 | | `c89935debe320e35aaf748ea0519457c1618b49e13d79ddb33cbc30051b5a118` |
| `tables/agency-registry-bridges.parquet` | 10,541 | 13 | `2e33905b475c6a1adf27960ecf170a1b4c82df2baf20ac13df9307bb687dd898` |
| `tables/agency-registry-events.parquet` | 22,251 | 15 | `fce2b80a184fd0de98e2d15ba2d3a68d47f84eb9b9ba1c2678bc8787af3e0c07` |
| `tables/agency-registry-non-emissions.parquet` | 10,995 | 11 | `7819b99bd132cd48a08f4c918e8faf20ae78cac8c0ec9ad41a6cb6327b8f2f3e` |
| `tables/agency-registry-current-successors.parquet` | 1,126 | 15 | `857abd0ca7c42a50bab7225d1036df2e498d91e1840a2007440fd10acdf97160` |
| `tables/agency-registry-aliases.parquet` | 50,011 | 157 | `74bba3bdccaf3e2b6111b8320d695f74c777d3a405de73e04a0475c28df834a9` |
| `tables/agency-registry-organizations.parquet` | 21,892 | 472 | `426f3082277f07f8b75d3e154d3065d6f478f09de789393ecdcd647116d76177` |
| `tables/agency-projection.parquet` | 66,304 | 321 | `9b0296b69a8d009199607929841fe8dc9a20f528319988e8d70eaaeab88c5303` |
| `tables/agency-projection-unresolved.parquet` | 6,631 | 10 | `7aa01a441ba254575aac427d72f0c593ace5a1f3d177b6bc8f927c1967a43ca6` |

Provenance, from `agency-registry-view/view-manifest.json`:

- `viewId`: `urn:ref:agency-registry-view:b23e3bb6b673608095358cb947b70a97c67a25c43642319fa892046ebb052ca3`
- `digest` (logical content, the one compared across lock changes):
  `sha256:d66455a02bb32730e509075c0c9d9556bd574b226f453d4a1beeeecce7235ab8`
- `release.agencyProjectionDigest`: `sha256:0dd32dd5320a0d1553f10818a7a31e95e2c3b9b6567df224dd359a8038e14db4`
- `release.frRosterReleaseDigest`: `sha256:0ab874f045462fa665b9a2ef48aa34f373570dc710bcc8fb1b63db0ad8c7b5e6`
- `release.nameKeyVersion`: `"1"`
- `release.candidatesDigests`:
  `plans/agency-registry-batch-1-candidates.json` `sha256:7fe88a9167a9363f5c2bfdcd3911953b7323abe1564d40586991c9612f95f4bc`,
  `plans/agency-registry-succession-batch-candidates.json` `sha256:4d29ffd7c936159dbff13d9b228845e50d134a567a8ff6b60a3d9e2f7f7921cc`,
  `plans/agency-registry-alias-batch-candidates.json` `sha256:f27a8b66b414ed8c75153340997623b589c5fe1820d75e60ec73953d361c0b82`,
  `plans/agency-registry-alias-batch-2-candidates.json` `sha256:0cb4bfbf239d4b2f8bb555f944484b4c47ad18d9df1dd6a1d505ff442953766d`
- `release.sourceReleaseDigest`: `sha256:2ae595b91e4c722da61b5fd90b435d0e0b534a4b9321723be7817c8f52007801`
- `canonicalPayloadDigest`: `sha256:25613f36e96b15212201fb54150f326377422fa37b9216779bc1d6a90f305abd`

`lookup_agency` replies carry this identity in `registry_evidence.publication`: `view_id`,
`schema_version`, `manifest_sha256`, `release`, `digest`, `counts`, and `table_sha256` (all eight
tables). Under `namespace="name"`, free-text strings are keyed with RefSpec's published `name_key`
(`version "1"`) and matched strictly against the 157 adopted aliases and 472 pinned FR
organizations; owner-rejected aliases return `unmatched` with their `aliasRejectedByOwner`
non-emission row.

`agency_code_for_fr_agencies` reads the bridges, the current successors and the events'
stated parents, but not the events' effective dates: every document takes today's lineage,
whatever its own date. A current successor's code wins over the original's own (the owner's rule,
round 5): FR 200, which REF-038 codes FNS, is FNA. It never reads the non-emissions: the
Export-Import Bank, Udall Foundation, IBWC, CISA, and rejected-alias items leave REF-038's codes as
they are.

## U.S. Code section oracle and Table III act sections (`usc-section-oracle/`, `usc-act-index/`)

Owner answers of 2026-10-31 (layer2-citations Q2 and Q4): vendor RefSpec's six digest-pinned
OLRC oracle tables and its Table III act-sections table as reference SQL tables. Each file is
served as a view of the same name (`relationship_views/reference.py`, which pins every sha256
below and refuses other bytes); `tests/test_reference_tables.py` holds the bytes, these pins and
this table equal.

Copied from RefSpec `f8857c8669a412bd6c254aa0b3a23cf05a52e04d` (origin/main, 2026-10-31), where
the bytes are tracked in git:

- the oracle, `research/evidence/usc-section-oracle-2026-08-24/` (generation 2). Its README's
  Files table and `refspec.registry.usc_section_oracle._ORACLE_PINS` state these six digests.
  Sources: OLRC release point 119-102 USLM XML and the annual historical archives 1994-2024.
  RefSpec's `_section_key` orders range bounds as (stem, rest as text), which misorders
  `698v-4`/`698v-10` and `2689s`/`2689aa`; these tables carry the bounds as printed, so a reader
  orders them itself (`identifiers.section_order_sql`).
- the act sections, `portfolio/distributions/usc-act-index-2026-08-22/` (`receipt.json`,
  `outputs`): OLRC Table III bulk member `fulldump@119-73.xml`, release point 119-73, which the
  publisher no longer serves. Its popular-names and quarantine tables are not vendored.

| View | File | Bytes | Rows | sha256 |
| --- | --- | ---: | ---: | --- |
| `usc_oracle_sections` | `usc-section-oracle/usc-oracle-sections.parquet` | 303,565 | 59,364 | `bbb450afb00cc7a28e2fcab7943d47207e8c1899f1e83209bd92d893fe39daac` |
| `usc_oracle_ranges` | `usc-section-oracle/usc-oracle-ranges.parquet` | 40,666 | 1,751 | `070226e36d324a15226bd84ce9f5e4a297b43ff83365dba37ed281d3186cd736` |
| `usc_oracle_annual_sections` | `usc-section-oracle/usc-oracle-annual-sections.parquet` | 7,594,553 | 1,572,225 | `4757327d2bbcd5258d0fc51f0658df7fc9999c21d5a40dbe0c2d466cf25e7323` |
| `usc_oracle_annual_ranges` | `usc-section-oracle/usc-oracle-annual-ranges.parquet` | 178,394 | 49,960 | `c8cd96e1d13e4f429e1024f3a9fe25b8527036eae0adfd9ad29d9656522b43a3` |
| `usc_oracle_subsections` | `usc-section-oracle/usc-oracle-subsections.parquet` | 1,100,246 | 160,209 | `d7502945223eed92bdeb65e6a73cf52240febefe9814180675fc4df512cd08ed` |
| `usc_oracle_chapters` | `usc-section-oracle/usc-oracle-chapters.parquet` | 11,668 | 2,905 | `031ae7f3435f7ba2969bdf2c94b699170a63fe3183dd289ca23fa0ce45e90cb3` |
| `usc_act_sections` | `usc-act-index/usc-act-sections.parquet` | 2,213,252 | 302,156 | `9040d4460e90704aef911b4d4f704e9f432cebd7b58717f31f090a3008ca6159` |

## CCN definition for hospital reports

`ccn-definition.json` records RefSpec's unchanged CCN definition from
`1e110da25124fa2e6dd5355500d71f49b398a603` (`src/refspec/registry/nppes_npi_identifiers.py`, module SHA-256
`86e4096408c2e2803def559c56500fd4d6fb427b0f1c43038e50653cc8ab1bd4`). The thin adapter
`ontology/ccn.py` verifies the artifact SHA-256 `143472e11b719cd424b449704a2c5c40f02330cc0a9df547abde3aba0eb48f87` before applying it.
RefSpec remains the definition owner under REF-024; this is a local reference
artifact adoption, not a RefSpec public release. The full RefSpec package pins
SpicyDocs 0.54.0, incompatible with this receiver's adopted provider. The owned
receiver environment uses Python 3.12, which satisfies RefSpec's Python bound.

The artifact records the exact Python regex, flags, full-match behavior,
`str.strip` normalization, messages and CMS authority references. Python `\d`
continues to accept Unicode decimal digits in the first two positions; the
suffix remains ASCII uppercase alphanumeric. The hospital consumer still
requires six characters and rejects normalized values that differ from native
input. Tests retain the old function as an independent oracle, with no declared
verdict divergences. Retained provider IDs and mutation evidence are recorded
in the final package handoff.

## U.S. Code disposition tables (`usc-disposition-tables/`, `ss-iorw`)

Vendored from RefSpec (`fork/main` `facc0eb5`, `research/evidence/usc-disposition-tables-2026-08-23/`,
`usc-disposition-tables-2026-11-18/`, and `usc-disposition-tables-2026-11-18-complete/`). All 9 tables across 8
recodification families (Titles 10 ROPMA, 31, 34 [editorial reclassification], 41, 46 / 46 App., 49 / 49 App., 49
positive-law companion, 51, and 54) are SHA-256-pinned and verified at load time by
`spicy_regs.ontology.usc_dispositions`.

| File | Bytes | Rows | sha256 |
| --- | ---: | ---: | --- |
| `usc-disposition-tables/usc-1996-title10-ropma-disposition.parquet` | 9,180 | 371 | `5a08734b3c8ea948340203e0e3c67a256a6a62c95ece863a1328a662691eace6` |
| `usc-disposition-tables/usc-1994-title31-disposition.parquet` | 21,251 | 1,563 | `febacca0f59193fc4524ce780d29ba5440d8ee47f8041f133fda161d0c765cd5` |
| `usc-disposition-tables/usc-2017-title34-disposition.parquet` | 21,329 | 1,002 | `17e063ddd410d39c135e4d14550dceadc62707a811dc9e7bd2f63999014dd047` |
| `usc-disposition-tables/usc-2011-title41-disposition.parquet` | 9,706 | 549 | `327de9b607eaf74679c21af8a6cf27086c7bf5aaf19c01dbcf3005959fa3a51b` |
| `usc-disposition-tables/usc-2006-title46-disposition.parquet` | 29,320 | 2,169 | `2b6a315482a8b8ba142360ea3303e55e6e8e07d632ebe4c3a1a24d38a9efca84` |
| `usc-disposition-tables/usc-1994-title49-disposition.parquet` | 37,672 | 3,102 | `8403212c0193b3361accf7ff4be238420634beb5aa5740d78b9960fef5b2aedd` |
| `usc-disposition-tables/usc-1994-title49-positivelaw-disposition.parquet` | 4,624 | 49 | `6387ca0487ec234ae04811e90d1490e9bf93407d1b3dd5f3fa40836e3bc21510` |
| `usc-disposition-tables/usc-2010-title51-disposition.parquet` | 8,074 | 345 | `507bdd8da5dd9a1ff6b5f395193408e1f36120803637ee367ddfecc3792d7c0a` |
| `usc-disposition-tables/usc-2014-title54-disposition.parquet` | 17,061 | 626 | `49e79253f9240b75edd85afa3d8fe9b513e231e060434d4fea0d6bab3ac60034` |

## Act index popular names, quarantine, and source credits (`usc-act-index/`, `ss-njjd`)

Vendored from RefSpec (`fork/main` `facc0eb5`, `output/usc-act-index-2026-08-22/` and
`output/usc-source-credit-index-2026-08-02/`), complementing the already-vendored `usc-act-sections.parquet`
(`9040d4460e90704aef911b4d4f704e9f432cebd7b58717f31f090a3008ca6159`). All four files are SHA-256-pinned and
verified at load time by `spicy_regs.ontology.act_resolution`.

| File | Bytes | Rows | sha256 |
| --- | ---: | ---: | --- |
| `usc-act-index/usc-popular-names.parquet` | 563,299 | 20,865 | `603d5b072133d8fe6802736aeaa70b9fb9832e4fb996158a083fae3ce1026a9a` |
| `usc-act-index/usc-act-sections.parquet` | 2,213,252 | 302,156 | `9040d4460e90704aef911b4d4f704e9f432cebd7b58717f31f090a3008ca6159` |
| `usc-act-index/quarantine.parquet` | 369,783 | 35,805 | `a89305172a27e682488e05e8fc82d8b0521d20a8fcee7d97a00331e8527acfac` |
| `usc-act-index/usc-source-credits.parquet` | 60,840 | 3,721 | `d377545fe60d592a120bda30dffba665380cf82f826ae06cb327a581f0af9d8a` |
