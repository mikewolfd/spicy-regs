# RefSpec agency projection and agency registry view (vendored bytes)

These files are RefSpec's and must never be edited in place. A new RefSpec
release is a new copy, with new digests pinned in `spicy_regs/ontology/agencies.py`.
The loader there reads the tables and refuses any bytes whose sha256 is not the
pinned one.

Bytes are pinned; logical digests are compared. A RefSpec lock change (a new
pyarrow, say) can move a file's bytes without changing its rows. Such a copy is
re-vendored with new byte pins only after its logical digest is shown equal to
the one recorded here. A changed logical digest is a new release, which needs
its own review.

## REF-038 agency projection

Vendored from RefSpec (`spicy-stack/RefSpec`, directory `output/parquet-view/`, which is
gitignored).

- Built 2026-08-21 by RefSpec's parquet-view (implementation 3.2, pyarrow 23.0.0) from the
  REF-038 inputs.
- Build identity (`view-manifest.json` `input`): `distributionId`
  `urn:ref:atlas:distribution:3.1-full-development:3310ba015fc7f8ff19ce0ac45e236f8ff7d05e5c2b1486984515cb9214a98b95`,
  `manifestSha256` `sha256:f6f0f6fc2dcf901228ed8ba7cfc69d35e9189364b89ce95198dac4589cd853ae`.
- REF-038 landed at RefSpec 6535f570 (2026-08-16). The RefSpec owner states that every
  later commit yields the same rows. At dev21 (41e2bf9d) the two tables rebuild as
  `9b0296b6…` and `7aa01a44…`, with identical rows and schemas: RefSpec's lock moved
  pyarrow from 23.0.0 to 25.0.1 (workspace status note of 2026-09-26). Only the writer
  moved, so this copy stays.
- RefSpec 0.1.0.dev19 (739a3b3c) publishes the same reverse rule as
  `reverse_agency_projection()`.

Decision: REF-038 — a reviewed one-way projection from Regulations.gov agency
codes onto organizations in the Federal Register, eCFR and Federal Hierarchy
rosters.

| File | Bytes | sha256 |
| --- | ---: | --- |
| `agency-projection.parquet` | 66,304 | `c9ec0fde1bf5fda17402983880bc091e9caa417845178f232214606e264c049f` |
| `agency-projection-unresolved.parquet` | 6,631 | `e32e814c3c7489d82df6dbdbc00fd6e16694628c08e7300a9473bcaa0659b065` |
| `view-manifest.json` | 6,442 | `991acd29368b17fb66eea770f36c71385c5faee1a368008a4240281e4c8536d8` |

Provenance, from `view-manifest.json`:

- `agencyProjection.digest`: `sha256:0dd32dd5320a0d1553f10818a7a31e95e2c3b9b6567df224dd359a8038e14db4`
- `agencyProjection.decision`: `REF-038` (status `emitted`)
- `canonicalPayloadDigest`: `sha256:52e5b82ca208f1eae79a1190faaa5e981e14c504e5f9358e1644293e063389a5`

## Agency registry view (REF-072, `agency-registry-view/`)

The release `agency-registry-2026-09-26` records two batches the owner decided (decision record
`docs/decisions.md#ref-072`):

- batch 1, 26 decisions: 13 identity bridges from Federal Register agencies to eCFR or Federal
  Hierarchy organizations; 9 dated successions (5 renames and 4 splits, one event row per result);
  4 recorded non-emissions, which add nothing;
- the succession batch (2026-10-03): the Food and Nutrition Service (FR 200) renamed the Food and
  Nutrition Administration (FR 625).

That is 13 bridges, 10 successions in 15 event rows, and 4 non-emissions. Schema 1.2 adds the
current-successors table: each original's results that no later event replaced, sealed by RefSpec,
15 rows, so spicy-regs reads it instead of walking the events itself.

Vendored from RefSpec `f1c23b91` (branch `chaos/2026-10-03-fns-fna-succession`, view `schemaVersion`
1.2), as built by `uv run --frozen python tools/build_agency_registry_view.py --output <dir>` on
RefSpec's own frozen lock (pyarrow 25.0.1, as the manifest's `construction` names). Its manifest is
the pin that tool names. To check a copy, run that tool with `--verify <dir>
--expected-manifest-sha256 sha256:<pin>`: it checks members, schemas, counts and bytes against the
manifest, recomputes the logical digest from the rows, and compares the view with the release it
rebuilds from RefSpec's pinned rosters.

It replaced the `cd78e476` 1.1 copy (manifest `c7dc9310…`, events `72f35636…`, 14 rows, logical
digest `777c6100…`); the bridges and non-emissions kept their bytes. Schema 1.1 had added
`original_parents` to the events table, replacing the dev21 (`41e2bf9d`) 1.0 copy (manifest
`77b357cc…`).

| File | Bytes | Rows | sha256 |
| --- | ---: | ---: | --- |
| `view-manifest.json` (the pin RefSpec's build tool names) | 2,630 | | `0b39812de31930f267dea2d7d51039d71b0a1852606387d5aba64aa75d25ec17` |
| `tables/agency-registry-bridges.parquet` | 10,541 | 13 | `2e33905b475c6a1adf27960ecf170a1b4c82df2baf20ac13df9307bb687dd898` |
| `tables/agency-registry-events.parquet` | 22,251 | 15 | `fce2b80a184fd0de98e2d15ba2d3a68d47f84eb9b9ba1c2678bc8787af3e0c07` |
| `tables/agency-registry-non-emissions.parquet` | 9,500 | 4 | `da863e467f00f16a6b7a9ff1a3e1182fb8e7488f8b7d33f0d7f0c403a0663e24` |
| `tables/agency-registry-current-successors.parquet` | 1,126 | 15 | `857abd0ca7c42a50bab7225d1036df2e498d91e1840a2007440fd10acdf97160` |

Provenance, from `view-manifest.json`:

- `digest` (logical content, the one compared across lock changes):
  `sha256:05a360a409418313a3df9c24b644e9e4f24e1a69eea9942d5e7b143473c512e5`
- `release.candidatesDigests` (the candidates the owner decided on, one digest per batch's
  candidates file; until schema 1.2 a single `candidatesDigest`):
  `plans/agency-registry-batch-1-candidates.json` `sha256:7fe88a9167a9363f5c2bfdcd3911953b7323abe1564d40586991c9612f95f4bc`,
  `plans/agency-registry-succession-batch-candidates.json` `sha256:4d29ffd7c936159dbff13d9b228845e50d134a567a8ff6b60a3d9e2f7f7921cc`
- `release.sourceReleaseDigest`: `sha256:dc639b30526052a4cf1fa9162a201bce6476d82ec67f581e2053091a25aa367b`
- `canonicalPayloadDigest`: `sha256:410f55073ee99652644099a4e6cbd56095b8d8998a60a2b45d00d39c37f3cdba`

`lookup_agency` replies carry this identity in `registry_evidence.publication`: `view_id`,
`manifest_sha256`, `release` (the manifest's object as written, so `candidatesDigests`, a map from
candidates file to digest, where replies before schema 1.2 had `candidatesDigest`), `digest`, and
`table_sha256` (one entry per table, `current-successors` included).

`agency_code_for_fr_agencies` reads the bridges, the current successors and the events'
stated parents, but not the events' effective dates: every document takes today's lineage,
whatever its own date. A current successor's code wins over the original's own (the owner's rule,
round 5): FR 200, which REF-038 codes FNS, is FNA. It never reads the non-emissions: the
Export-Import Bank, Udall Foundation, IBWC and CISA items leave REF-038's codes as they are.
