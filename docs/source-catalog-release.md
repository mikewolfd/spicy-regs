# Release selected source-catalog results

Release each qualified destination independently using the existing generation
publisher. The preflight script pins existing files and names unresolved checks;
it never uploads data, advances a pointer, or certifies deployed availability.
The coordinator owns shared package changes and serialized publication.

Prepare an explicit selection JSON with format
`spicy-source-catalog-release-selection`, version `1`, and `candidates`.
Each candidate names its task, complete publication family, qualification file
and SHA-256, subject paths/digests/row counts, shared receipt path/digest/generation,
and required downstream checks. Unregistered historical datasets also supply a
pinned proposed `DatasetPolicy` descriptor. Registration remains a release
prerequisite. A catalog selection names its public export directory and exact
manifest SHA-256. Keep this operational selection private because it names local
paths.

`receipts.generation_id` names the publishing bundle's label.
`receipts.original_generation_ids`, when supplied, pins the distinct original
row generations expected in that receipt file. Omit it only when every row uses
the publishing label. Preserve original row generations when carrying history;
the maintained admission checks allow them to differ from the publishing label.

Inspect locator matches before treating them as private metadata. Publisher text
can contain literal filesystem links, and JSON escapes can resemble Windows
paths. Preserve publisher text. A reviewed `literal_source_exceptions` file may
name exact string hashes, dataset, field and occurrence counts, with a pinned
independent source proof. Its format is `spicy-qualified-public-source-literals`,
version `1`; each entry has `kind` (`subject` or `receipt`), `dataset`, `field`,
`sha256` and `occurrences`. The scan keeps these exact qualified values visible
as source literals and still refuses new private values or changed counts. The
exception records are private release evidence and stay outside the public
export. They do not replace independent source qualification or generation
admission.

```sh
.venv/bin/python scripts/prepare_source_catalog_release.py \
  --selection /absolute/path/selection.json \
  --publication /absolute/path/publication.v2.json \
  --website /absolute/path/spicygov \
  --output /absolute/path/new-preflight.json
```

Capture the current public publication immediately before release. Match its
member digest to retained local bytes before fetching anything. The preflight
checks selected byte hashes, schemas, counts, policy registration, complete family
membership, receipt generation and local filesystem paths in receipts. Catalog
checks use the website's maintained public-field validator and follow every
selected detail/page reference within the selected generation. These checks
complement full generation admission and independent source/receipt binding.

Before packaging, freeze the installed packages, policy descriptors and shared
implementation. Preserve original qualification and successful candidate bytes.
Resolve private receipt paths through a reviewed source/witness representation
with fresh receipt construction and binding verification; editing already hashed
receipts invalidates their qualification. Use `generations.build_generation`
with exact complete-family members, receipt policies, receipt generation and the
captured publication. Existing families retain every unchanged sibling and
required receipt dataset. A partial table repair cannot publish a partial family.
Record the resulting artifact digest, root/member hashes and installed code
identities in the release packet.

Hold each affected join to its actual selected inputs, including parent
multiplicity. Agenda joins use `rin` and, where declared, `agenda_edition`.
Table III placements join on `act_key`, `usc_title`, `usc_section_key` and
`usc_place`. Historical snapshot sections resolve `snapshot_id`; Code structure
resolves `edition_id`. Source-period scope and structure-only Code limits remain
visible in metadata. Refresh explanatory catalog bindings through its owner.

Publish through `sources.publication.publish_generation` with the captured
`prior_index` and `exact_prior=True`. Its maintained gates verify local and
uploaded bytes and compare-and-swap the pointer. Preserve refusal evidence. If a
transport error follows the pointer request, read the stored index before retrying;
the write may have succeeded. Read the public index and exact immutable subjects,
receipts and root anonymously after publication. Run the affected joins and
existing generation audit, and verify the deployed consumer reader understands
the selected schemas and receipt fields. Public availability remains unproved
until this readback succeeds.

For the website, create a public export selection with format
`spicy-public-source-catalog-selection`, version `1`, the selected `generation`,
and every referenced JSON file's `path`, `bytes`, and `sha256`. Pin the selection
file itself. Stage only these public files:

```sh
node /absolute/path/spicygov/scripts/stage-source-catalog.mjs \
  /absolute/path/public-export /absolute/path/public-selection.json \
  sha256:EXACT_SELECTION_DIGEST /absolute/path/new-public-stage
```

The staging tool refuses existing output directories, changed bytes, symlinks,
unreferenced members, mixed generations and internal public fields. Integrate
the verified stage into the deployment artifact through the coordinator's
workflow step; generated local staging files are gitignored and cannot reach CI
without this step. Run website tests/build and check deployed manifest, all
selected references and representative Sources navigation against exact published
table identities. Catalog generation and table publication identities are separate
pins; refresh the catalog after table selection when needed.

Retain the before-publication index, exact prior immutable member locations and
previous website deployment identity. Rollback reselects a verified prior family
against the current index through the maintained conditional pointer process;
it must preserve unrelated families, receipt ownership and membership. A
membership or policy incompatibility requires an explicit reviewed migration.
Do not write a saved whole index over intervening publications. Redeploy the
previous verified public website artifact separately and record anonymous
readback. Keep all prior bytes until the retention owner verifies eligibility.

The initial lane A evidence under
`metadata/source-catalog-program/20261010/A/` distinguishes exact candidate pins,
live conservation, independent reviews, proposed publication and deployed
readback. Task completion follows the workstream plan's user outcome, rather
than the existence of a locally qualified file.
