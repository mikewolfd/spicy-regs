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

The release `agency-registry-2026-09-26` records agency registry batch 1, the
owner's 26 decisions (decision record `docs/decisions.md#ref-072`):

- 13 identity bridges from Federal Register agencies to eCFR or Federal
  Hierarchy organizations;
- 9 dated successions: 5 renames and 4 splits, one event row per result, 14 rows;
- 4 recorded non-emissions, which add nothing.

Vendored from RefSpec `cd78e476` (view `schemaVersion` 1.1), as built by
`uv run --frozen python tools/build_agency_registry_view.py --output <dir>` on
RefSpec's own frozen lock (pyarrow 25.0.1, as the manifest's `construction`
names). It was built twice, byte-identical, and its manifest is the pin that
tool names. To check a copy, run that tool with `--verify <dir>
--expected-manifest-sha256 sha256:<pin>`: it checks members, schemas, counts and
bytes against the manifest, recomputes the logical digest from the rows, and
compares the view with the release it rebuilds from RefSpec's pinned rosters.

Schema 1.1 adds `original_parents` to the events table: each original's roster
parent, aligned with `originals`, null for a top-level organization. It replaced
the dev21 (`41e2bf9d`) 1.0 copy (manifest `77b357cc…`, events `09e35a12…`,
logical digest `9bc9eb03…`); the bridges and non-emissions kept their bytes, and
the release's claims did not move.

| File | Bytes | Rows | sha256 |
| --- | ---: | ---: | --- |
| `view-manifest.json` (the pin RefSpec's build tool names) | 2,059 | | `c7dc9310f9c11cd346245d7cf882f9eaf69b70b25f59841ae6004dca4944866e` |
| `tables/agency-registry-bridges.parquet` | 10,541 | 13 | `2e33905b475c6a1adf27960ecf170a1b4c82df2baf20ac13df9307bb687dd898` |
| `tables/agency-registry-events.parquet` | 19,642 | 14 | `72f35636f9b5c93d708a364352122fb724e872f619a518c5e49ebb19518322b7` |
| `tables/agency-registry-non-emissions.parquet` | 9,500 | 4 | `da863e467f00f16a6b7a9ff1a3e1182fb8e7488f8b7d33f0d7f0c403a0663e24` |

Provenance, from `view-manifest.json`:

- `digest` (logical content, the one compared across lock changes):
  `sha256:777c610051c9fdcbab8e9cdb7d382dce20c4565a484b241ed784247efefc46a4`
- `release.candidatesDigest` (the candidates the owner decided on):
  `sha256:7fe88a9167a9363f5c2bfdcd3911953b7323abe1564d40586991c9612f95f4bc`
- `release.sourceReleaseDigest`: `sha256:69001a4381ddf35cdba6d44fa52f579d7dfa07627c4c74da3dde2b0d8a39f8f0`
- `canonicalPayloadDigest`: `sha256:8d2c8505a7530031c9c042d79c5c2ffd00b468a5e0fe8cc533432e862f7ef6b2`

`agency_code_for_fr_agencies` reads the bridges and events, but not the events'
effective dates: every document takes today's lineage, whatever its own date. It
never reads the non-emissions: the Export-Import Bank, Udall Foundation, IBWC and
CISA items leave REF-038's codes as they are.
