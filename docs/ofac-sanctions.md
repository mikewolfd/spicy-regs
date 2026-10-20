# OFAC sanctions observations

`ofac_entity_observations` retains the entities in explicitly selected Office
of Foreign Assets Control (OFAC) enhanced exports. `ofac_snapshots` retains
the corresponding publication metadata and reference dictionaries once per
snapshot. The selected download (`sdn` or `consolidated`) and each entity's
stated list memberships are separate fields. The lists can have different
publisher update dates.

SpicyDocs acquires the official compressed original, validates the selected
ZIP member and streams its XML through the existing source-release workflow.
The mapper reads an admitted release once and writes bounded Parquet batches.
It retains original entity and identity IDs as different strings, primary
names, ordered aliases, addresses, identifiers, programs, relationships and
unknown fields. Dictionary codes remain the publisher's codes; this pipeline
introduces no cross-source identifier mappings or vocabulary normalization.

The source trees preserve expanded XML names, ordered attributes, namespace
declarations, text and children. The original ZIP remains the authority for
lexical XML, comments and container whitespace. Sparse addresses remain sparse.
A year-only birthdate is not converted to January 1. A document number keeps
its leading zeros. A source relationship label does not imply an ownership
percentage or a legal conclusion.

Choose prior and new admitted releases explicitly on refresh; the new
generation contains their observations together. A snapshot is identified
by its source-release digest, so another capture of unchanged bytes remains
a distinct observation. Compare entities by selected list and native entity
ID within that history. `entity_content_sha256` excludes positional XML paths,
so inserting an earlier row does not change the fingerprints of later rows.
It still detects literal text, whitespace, ordering and attribute changes.
An absent later observation is not a legal delisting event. This integration
does not reconstruct history before the selected captures or resolve names
to real-world identities.

Use a selection JSON with `sources` containing each admitted release's
`release`, `blobs`, `logicalId`, `artifactDigest` and
`verifierImplementationId`. Its `archives` object maps each original
`sha256:…` digest to the retained ZIP path:

```sh
python -m spicy_regs.pipelines.ofac \
  --selection "$OFAC_SELECTION" --output-dir "$OFAC_OUTPUT"
```

The builder requires fresh output paths and refuses failed, discarded,
incomplete, repeated or contradictory selected observations. It stages both
tables before generation admission. Original ZIPs are retained once per
distinct content digest through the existing evidence writer.

An exact identifier query retains the publisher context:

```sql
SELECT e.selected_list, e.entity_id, e.identity_id, e.primary_names_json,
       e.list_memberships_json, e.programs_json, e.source_locator_json,
       s.data_as_of_raw, s.observed_at, s.source_release_digest
FROM ofac_entity_observations e
JOIN ofac_snapshots s USING (snapshot_id, selected_list)
WHERE e.selected_list = 'sdn' AND e.entity_id = '36'
ORDER BY s.observed_at;
```

The result establishes what the selected source reported. It is not a fuzzy
name match, a current legal determination or a claim of complete historical
coverage. Local qualification and public selection have separate receipts.
