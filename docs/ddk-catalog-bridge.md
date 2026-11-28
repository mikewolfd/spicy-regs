# Zero-Copy DDK Catalog & Iceberg v2 Bridge (`ddk-catalogs/spicy-regs/`)

`spicy-regs` exposes an additive, zero-copy [Data Design Kit (DDK)](https://github.com/mikewolfd/data-design-kit)
catalog and Apache Iceberg v2 table-metadata bridge over existing immutable Cloudflare R2 Parquet
generations (`generations/<family>/<sha256>/`) and materialized rulemaking snapshots
(`materialized/rulemaking/snapshots/<snapshot_id>/`).

The bridge is implemented in `src/spicy_regs/ddk_bridge.py` (`publish-ddk-bridge` /
`python -m spicy_regs.ddk_bridge`) and isolated behind the optional `ddk-bridge` dependency group
(`uv sync --group ddk-bridge`) so base CLI and MCP deployments remain lean and independent of `ddk`.

---

## 1. Additive Shadow R2 Layout

All bridge artifacts live inside the existing verified bucket `spicy-regs` (publicly served at
`https://data.spicygov.ai`) under one isolated prefix, aligned with sibling `congressional-tech`'s
`ddk-catalogs/congressional-tech/` layout:

```text
s3://spicy-regs/
├── publication.v2.json                                      [UNTOUCHED — existing live index]
├── explorer-metadata.v1.json                                [UNTOUCHED — existing explorer metadata]
├── pins/archive-releases.json                               [UNTOUCHED — outbound CT pin]
├── generations/<family>/<gen_sha256>/
│   ├── artifact.json                                        [UNTOUCHED — existing generation root]
│   ├── <table>.parquet                                      [UNTOUCHED — zero-copy referenced]
│   └── <table>/<col>=<val>/part-NNNNNN.parquet              [UNTOUCHED — zero-copy referenced]
├── materialized/rulemaking/
│   ├── latest.json                                          [UNTOUCHED — existing rulemaking pointer]
│   └── snapshots/<snapshot_id>/
│       ├── manifest.json                                    [UNTOUCHED — existing snapshot manifest]
│       └── <table>.parquet                                  [UNTOUCHED — zero-copy referenced]
└── ddk-catalogs/
    └── spicy-regs/                                          [NEW — 100% owned by ddk_bridge]
        ├── current.json                                     [CAS pointer; Cache-Control: no-store]
        └── releases/
            └── <release_id>/                                [64-hex content-addressed, create-only]
                ├── catalog/
                │   ├── catalog.ddk.json                     [immutable; canonical DDK catalog]
                │   ├── catalog.lock.json                    [immutable; SHA-256 lock + Iceberg pins]
                │   ├── catalog.report.json                  [immutable; decision == "passed"]
                │   ├── discovery.graph.jsonld               [immutable; JSON-LD knowledge graph]
                │   ├── discovery.dcat.json                  [immutable; W3C DCAT-AP catalog]
                │   └── release.manifest.json                [immutable; max-age=31536000, immutable]
                └── iceberg/
                    └── <table_name>/
                        └── metadata/
                            └── <release_id[:16]>.metadata.json  [immutable; Iceberg v2 metadata]
```

> **Canonical DDK filenames:** DDK's `load_catalog_artifacts` fails closed on extra files inside
> `catalog/`. The bridge therefore emits canonical DDK filenames (`catalog.lock.json` and
> `discovery.graph.jsonld`) rather than legacy aliases (`ddk.lock.json`, `catalog.jsonld`).
>
> **Iceberg v2 Wave 1a scope:** Wave 1 emits verified Iceberg v2 table metadata JSON descriptors
> (`iceberg/<table>/metadata/<release_id[:16]>.metadata.json`) whose `icebergSnapshot.metadataLocation`
> resolves HTTP 200 identically to its uploaded `remote_key`. Emitting Avro `manifest-list` /
> `manifest` files for external `iceberg_scan` SQL engines is tracked separately as a Wave 1b
> follow-up.

---

## 2. Pilot Wave Scope & Determinism

Wave 1 covers the 8 pilot families enrolled in `data_dictionary/odcs/manifest.json`:

- Legislative (`spicycongress` domain): `amendments`, `bill-family`, `committee-rosters`,
  `members`, `nominations`, `roll-call-votes`
- Regulatory (`spicygov` domain): `federal-register`, `rulemaking`

Key mapping guarantees:

- **Internal artifacts excluded:** `rulemaking`'s internal `etl_receipts.parquet`
  (`visibility == "internal"`) is excluded from the public catalog; the remaining public tables
  must match `data_dictionary/odcs/families/rulemaking.odcs.json` 1-to-1.
- **Partitioned & multi-part tables:** Split tables (such as `bill_sections` partitioned by
  `congress`) map directly from `publication.v2.json` members with exact row and byte-size sum
  verification. If a future split family (such as `fec-query`) stores multiple `part-NNNNNN.parquet`
  files under the same partition tuple, the collector automatically appends a deterministic `_part`
  tie-breaker key derived from the filename stem so DDK's unique-partition-tuple invariant holds.
- **Strict Arrow parity:** Every table validates against `odcs_contracts.DDK_ARROW_EXACT_PROFILE`
  (`urn:ddk:odcs:arrow-declaration:3`) with `normalize_arrow_encodings=False` and
  `check_column_order=True`. Readback passes exact `pyarrow.Schema` objects from
  `spicy_regs.odcs_contracts.odcs_schema_to_pyarrow` to preserve DuckDB's `"element"` list child
  field name.
- **Deterministic `release_id`:** `observed_at` is derived deterministically from the maximum
  upstream `publishedAt` / `asserted_at` timestamp across the bridged families, so repeated runs
  over unchanged upstream inputs produce the exact same `release_id`.

---

## 3. Zero-Copy, CAS, Transport & Retention Invariants

1. **Zero Parquet uploads (`Boto3BridgeStorageAdapter`):**
   - Wraps `spicy_regs.sources.r2.verified_target_client()` (enforcing `VERIFIED_BUCKET == "spicy-regs"`
     and `VERIFIED_ENDPOINT`).
   - Hard-refuses any `.parquet` PUT (`CATALOG-ZERO-COPY-VIOLATION`) and any write outside
     `ddk-catalogs/spicy-regs/` (`CATALOG-WRITE-OUTSIDE-PREFIX`) before calling S3.
   - Strictly additive (`delete_object` is always refused).
2. **CAS & Cache-Control discipline:**
   - `ddk-catalogs/spicy-regs/current.json` requires `IfNoneMatch="*"` (bootstrap) or
     `IfMatch=<live_etag>` (update) and always sets `Cache-Control: no-store`. Cloudflare edge
     Cache Rules for `ddk-catalogs/spicy-regs/current.json` must respect origin `no-store`; when
     `purge_pointer_cache=True`, `publish_bridge` also invokes `cloudflare.purge_urls` immediately
     after CAS pointer commit and before post-commit anonymous HTTP readback.
   - Every object under `ddk-catalogs/spicy-regs/releases/<release_id>/*` requires create-only
     `IfNoneMatch="*"` and sets `Cache-Control: public, max-age=31536000, immutable`.
   - On HTTP 412/409 conflicts, immutable artifacts are checked for byte/SHA-256 identity and
     reused if identical; pointer conflicts abort cleanly (`CATALOG-REMOTE-CONFLICT`).
3. **Explicit `User-Agent` & Range readback (`AnonymousPublicFetcher`):**
   - Sends `User-Agent: spicy-regs-ddk-bridge/1.0`, `Cache-Control: no-cache`, and
     `Accept-Encoding: identity` (refusing 3xx redirects) so Cloudflare edge WAF on
     `https://data.spicygov.ai` never returns HTTP 403 Error 1010.
   - Exposes `.user_agent` so `ddk.publication.verify_http_consumer_readback` propagates the
     explicit header to O(KiB) Range Parquet footer reads (`verify_member_bytes=False`,
     `schema_readback_mode="range"` / `"auto"`), avoiding multi-GB RAM buffering on live publishes.
4. **R2 retention protection (`src/spicy_regs/generation_retention.py`):**
   - `_ddk_bridge_pins()` reads `ddk-catalogs/spicy-regs/current.json` -> `release.manifest.json`
     and pins every referenced `generations/<family>/<sha256>/` digest (plus its transitive
     managed parents) against weekly R2 pruning, failing closed if `current.json` exists but its
     manifest is unreadable or digest-mismatched.
   - `ddk_release_lock_pins()` resolves consumer `ddk-release-lock-pins` v1 documents (`bound` +
     `retained` releases) into the same protected keep set, verifying both `release_manifest_sha256`
     and `lock_sha256`.

---

## 4. Rollback & Invariance Bounds

- **Production invariance:** Existing `publication.v2.json`, `generations/<family>/<sha256>/artifact.json`,
  `materialized/rulemaking/latest.json`, `pins/archive-releases.json`, `data.spicygov.ai`, and
  `mcp.spicygov.ai` are never modified by bridge execution.
- **Instant rollback:** Because the shadow bridge is strictly additive under
  `ddk-catalogs/spicy-regs/`, rolling back requires zero changes to any existing generation or
  live index: either pause/revert the bridge publisher, repoint `ddk-catalogs/spicy-regs/current.json`
  via CAS to the previous `release_id`, or remove `ddk-catalogs/spicy-regs/` entirely with zero
  impact on existing consumers.

---

## 5. Immutable `ddk-release-lock-pins` v1 Cross-Repo Replacement Plan

Today, cross-repo dataset pinning uses two ad-hoc JSON schemas tightly coupled to bucket retention:

| Direction | Legacy Repo File | Live Pin URL | Current Limitation |
| --- | --- | --- | --- |
| `congressional-tech` -> `spicy-regs` | `congressional-tech/model/jobs/spicy-regs/spicy-regs.json` | `https://congressional-data.spicygov.ai/viewer-publications/pins/spicy-generations.json` | Manual per-table SHA-256 dict with stale `r2.dev` URLs; no ODCS/contract lock |
| `spicy-regs` -> `congressional-tech` | `spicy-regs/src/spicy_regs/archive_release_pins.json` | `https://data.spicygov.ai/pins/archive-releases.json` | Separate ad-hoc JSON format; no catalog/lock digest binding |

### Target Document (`format: "ddk-release-lock-pins"`, `version: 1`)

Each consumer publishes its own lock-pin document in **its own** R2 bucket (preserving strict
single-bucket write isolation); peer retention reads it over HTTPS:

```json
{
  "format": "ddk-release-lock-pins",
  "version": 1,
  "updated_at": "2026-11-28T06:00:00Z",
  "catalogs": {
    "spicy-regs": {
      "catalog_id": "urn:spicy-regs:catalog:public",
      "audience": "public",
      "catalog_base_url": "https://data.spicygov.ai/ddk-catalogs/spicy-regs",
      "bound": {
        "release_id": "<64-hex release_id>",
        "release_manifest_sha256": "<64-hex release.manifest.json sha256>",
        "lock_sha256": "<64-hex catalog.lock.json sha256>"
      },
      "retained": [
        {
          "release_id": "<64-hex active release_id>",
          "release_manifest_sha256": "<64-hex sha256>",
          "lock_sha256": "<64-hex sha256>"
        },
        {
          "release_id": "<64-hex prior release_id retained across deploy rollout>",
          "release_manifest_sha256": "<64-hex sha256>",
          "lock_sha256": "<64-hex sha256>"
        }
      ]
    }
  }
}
```

### Cryptographic Verification Chain

1. Pinned builds never read mutable `current.json` directly: fetch
   `{catalog_base_url}/releases/{bound.release_id}/catalog/release.manifest.json` and assert
   `sha256(bytes) == bound.release_manifest_sha256`.
2. Fetch `catalog.lock.json`, assert `sha256(bytes) == bound.lock_sha256`, and run
   `ddk.publication.verify_remote_release_lock_pin(...)`.
3. Resolve every table's Parquet URL(s), `sha256`, byte size, row count, and ODCS schema directly
   from the verified `release.manifest.json` and `catalog.ddk.json`, and bind DDK peer
   `imports.<alias>.lockSha256 = bound.lock_sha256`.

### Retention-Safe Phased Cutover

- **Phase 1 (Shadow + Retention Guard):** `spicy-regs` publishes `ddk-catalogs/spicy-regs/current.json`
  and `generation_retention.py` protects both live `current.json` and `--ddk-lock-pins` sources;
  legacy `pins/archive-releases.json` and `pins/spicy-generations.json` stay untouched.
- **Phase 2 (CT -> `spicy-regs` cutover):** `congressional-tech` switches `spicy-regs.json` to a
  `ddk-release-lock-pins` v1 binding (retiring stale `r2.dev` URLs) and dual-emits
  `pins/spicy-generations.json` for 2 consecutive weekly `generation_retention.py` runs before
  retiring `pins/spicy-generations.json`.
- **Phase 3 (`spicy-regs` -> CT cutover):** Once `congressional-tech` publishes live
  `ddk-catalogs/congressional-tech/current.json`, CT `maintenance.json` reads
  `https://data.spicygov.ai/pins/ddk-release-locks.json` alongside `pins/archive-releases.json`,
  `archive_release_pins.json` migrates (keeping the two-release overlap rule in `retained[]`), and
  `pins/archive-releases.json` retires after 2 clean CT maintenance cycles.

---

## 6. CLI Usage

```bash
# 1. Read-only live dry-run (zero R2 writes, no write credentials needed)
uv run --group ddk-bridge publish-ddk-bridge \
  --families pilot \
  --dry-run \
  --output /tmp/ddk-bridge-dry-run.json

# 2. Live shadow publish with CAS precondition (re-reads live current.json, or "none" on bootstrap)
uv run --group ddk-bridge publish-ddk-bridge \
  --families pilot \
  --publish \
  --expect-prior-release-id auto

# 3. Standalone anonymous HTTP consumer readback (Range mode by default)
uv run --group ddk-bridge publish-ddk-bridge \
  --verify-live \
  --public-base-url https://data.spicygov.ai
```
