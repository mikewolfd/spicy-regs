# RefSpec REF-038 agency projection (vendored bytes)

These three files are RefSpec's and must never be edited in place; a new RefSpec
release is a new copy of this directory, with new digests pinned in
`spicy_regs/ontology/agencies.py`.

Vendored from: RefSpec (`spicy-stack/RefSpec`, directory `output/parquet-view/`, which is
gitignored).

- Built 2026-08-21 by RefSpec's parquet-view (implementation 3.2) from the REF-038 inputs.
- Build identity (`view-manifest.json` `input`): `distributionId`
  `urn:ref:atlas:distribution:3.1-full-development:3310ba015fc7f8ff19ce0ac45e236f8ff7d05e5c2b1486984515cb9214a98b95`,
  `manifestSha256` `sha256:f6f0f6fc2dcf901228ed8ba7cfc69d35e9189364b89ce95198dac4589cd853ae`.
- REF-038 landed at RefSpec 6535f570 (2026-08-16); the RefSpec owner states that every later
  commit, including dev18 d4c965a1 and dev19 739a3b3c, yields the same rows. Not rebuilt here.
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

The loader `spicy_regs.ontology.agencies` reads `agency-projection.parquet`
from here and refuses any bytes whose sha256 is not the pinned one above.
