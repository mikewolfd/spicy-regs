# Offline shared source catalog

The source catalog joins retained corpus manifests, reviewed source bindings,
publisher discovery observations, and selected table lineage into a disposable
SQLite index. It preserves unknown mappings and missing-byte records. Building
or exporting the catalog performs no network requests and changes no source data.

Run from this repository with the package installed or `PYTHONPATH=src`:

```sh
python -m spicy_regs.source_catalog build --corpus ../../corpora/spicyregs --output ../../corpora/spicyregs/catalog/index.sqlite --discovery ../../corpora/spicyregs/metadata/catalog-discovery/20261010-retained/discovery.json
python -m spicy_regs.source_catalog validate --index ../../corpora/spicyregs/catalog/index.sqlite
python -m spicy_regs.source_catalog query --index ../../corpora/spicyregs/catalog/index.sqlite --limit 100
python -m spicy_regs.source_catalog gap-report --index ../../corpora/spicyregs/catalog/index.sqlite
python -m spicy_regs.source_catalog matrix --index ../../corpora/spicyregs/catalog/index.sqlite
python -m spicy_regs.source_catalog export --index ../../corpora/spicyregs/catalog/index.sqlite --output /path/to/public-export
python -m spicy_regs.source_catalog export --index ../../corpora/spicyregs/catalog/index.sqlite --output /path/to/internal-export --internal
```

`--config` overrides the packaged `config/registry.v1.json`. Collections use
explicit `id`, `title`, `source_names` and `families` bindings. Routes use `id`,
`collection_id`, `delivery_method`, `lifecycle`, `custodian`, `content_behavior`
and optional `access_url` and `periods`. Planned work, observations, relationships
and period assertions remain durable configuration. Titles never determine
whether two collections merge. Duplicate source or family bindings refuse import.

From the workspace root, recreate the dated discovery input offline with
`uv run --project spicy-docs --frozen python scripts/prepare-source-catalog-discovery.py`.
It verifies the retained research captures against their decoded hashes and
reuses the maintained SpicyDocs readers. The path above selects that exact
observation set; repeat `--discovery` for additional retained snapshots. Omitting
the flag builds only the corpus and reviewed registry. It does not discover
publisher catalogs automatically. A refresh requires an explicit acquisition
command from the [SpicyDocs guide](../../spicy-docs/docs/sources/source-catalog.md).

Repeat `--discovery FILE` to import retained `spicy-source-discovery` version 1
observations from SpicyDocs. Dataset and route IDs remain source-owned. Failed,
partial and empty observations remain evidence. Discovery items count as known
items until an explicit retained object proves possession. No completeness ratio
is calculated. Period assertions retain their basis; a requested date window does
not establish complete historical coverage.

The importer reads current artifact roots and their member manifests directly.
It does not trust the older `metadata/source-members.sqlite` cache as an authority
for current membership. Member IDs include artifact digest, manifest locator and
ordinal, preserving repeated member names. Decoded SHA-256 identities deduplicate
content while every alias remains queryable. Presence checks recognize original
and lossless gzip storage through the corpus object's reference convention.
Retained verification is reported as manifest evidence with a presence check;
this operation does not rehash every large body or claim fresh byte verification.

Every read metadata input and the importer/exporter implementation are pinned by
SHA-256. A deterministic digest of the indexed facts also binds possession and
encoded storage sizes to the generation; the builder rechecks retained-file
presence, sizes and modification times before activation. The builder checks those pins again before activation, checks SQLite
integrity and references, and replaces the active database atomically. A failed
build leaves the previous index usable and writes its diagnostic reconciliation
receipt beside the index. Query and validation open SQLite read-only. Large
artifact-member arrays stream incrementally with strict top-level JSON and
end-of-document validation. File exports use bounded pages. Shared discovery
observations retain the original document once; collection links reference that
observation instead of copying the entire publisher catalog.
Internally, `observation_ref` retains the original logical observation ID.
Its stored snapshot evidence key is `snapshot:` followed by `stable(observation_ref)`;
the snapshot data preserves the original ID. Collection observations and raw
snapshots therefore cannot overwrite each other in the evidence table.

Exports share one generation. The public manifest references generation-specific
collection details and file pages. Public fields come from a fixed allowlist;
raw evidence, original local paths and URL queries stay in the internal export.
The public privacy validator rejects internal paths and credential markers.
Internal roots carry `audience: "internal"`; never place them under the website's
public directory. Export partitions refuse changed content under an existing
generation path. The current manifest switches only after all partitions validate.
Use the previous export's manifest to roll back a local serving selection.

The selected table generation comes from the pinned corpus catalog, not a fresh
publication fetch. Serving consumers must compare it with their current
publication to identify drift. Planned routes do not rewrite historical inputs.
The matrix reports explicit route assignments and unresolved comparison states;
matching names, file totals or date bounds never imply equivalent routes.

Reviewed route choices live in the registry's `planned` records. Each choice
names a destination, selected route or upstream tables, scope, reason, and any
required baseline. These are acquisition and integration decisions; they do not
change an existing processing receipt or automatically switch a pipeline's
input. A historical baseline remains required until a separate replay proves
that the selected originals replace it.

The `matrix` command includes each route's `relationships` and
`selection_policies`. A comparison applies only to its stated identifiers,
period, capture and fields. Matching IDs can coexist with differing fields;
documented publisher reach is separate from observed local holdings. A route
without comparison evidence still reports its relationship as unresolved.
The dated [relationship qualification report](../../docs/source-route-qualification-2026-10-10.md)
links the retained comparisons and destination decisions.

The index retains source-artifact declarations, processing receipt references,
parent table dependencies, retained baselines, external references and full
internal evidence. It does not scan every processing receipt row or silently bind
unassigned files to routes. Use exact retained evidence to resolve those links;
unknown linkage and source-only replay remain explicit.
