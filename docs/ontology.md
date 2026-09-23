# Metadata ontology and Rulespec Level-0 mapping

The rule-identity spine (`rule_targets`) links dockets, CFR references and
RINs into one normalized graph of what a rulemaking is and what it cites. This
document is the human layer of that ontology: what the carrier means, how its
compact values expand to Rulespec identifier IRIs, and the Level-0 mapping
Rulespec's `tools/l0_mapping_audit.py` audits.

Re-authored 2026-09-21 after the fork strip removed the docpipeline and
JSON-LD projection program an earlier revision of this document described.
What survives in this tree is the citation and identity normalization package
(`src/spicy_regs/ontology/`) and the `rule_targets` carrier below; the
authority-edges, concept-tagging and document-projection tables that document
mapped no longer exist.

## The surviving ontology package

- `ontology/citations.py` — the identifier readers the rulemaking tables use.
  `parse_cfr_citation` reads the Federal Register's structured CFR objects
  (the only form it states) and returns `CfrCitation` values;
  `canonical_cfr_iri` expands one; `normalize_rin` accepts only the published
  RIN key `\d{4}-[A-Z]{2}\d{2}` and returns `None` otherwise;
  `normalize_regsgov_identifier` uppercases and keeps the agency-issued hyphen
  or underscore separators, and accepts no value whose syntax the publisher
  never issues. The prose citation grammar that lived here had no production
  caller and was deleted (fork delivery decision 18); SpicyDocs'
  `interpretation.citation_grammar` is the data-side grammar.
- `ontology/rins.py` — usable RIN sets from proceedings rows: the complete
  `rins_json` list, or the one known legacy scalar. A malformed non-NULL list
  is reported, never silently replaced.
- `ontology/federal_register.py` — dated FR record keys
  (`federal_register_source_record_id` under SpicyDocs' own classification);
  `FederalRegisterIndex`, built once per generation, which resolves
  number-only references against the held generation and retains ambiguity
  (the literal number first, then SpicyDocs' folded and unpadded comparison
  key, with the method in each reference's status); and
  `linked_docket_id`, which reads a Federal Register docket value through its
  label with SpicyDocs' `normalize_docket_reference`.
- `ontology/common.py` — storage and provenance shared by the ontology
  rollups: the attestation columns (`method`, `actor_id`, `run_id`,
  `asserted_at`, `supersedes_id`), `RunContext`, canonical JSON, the
  counting JSON readers, and `eastern_day`, the one day rule of the
  rulemaking tables: an instant falls on its Eastern calendar day, and a
  date-only value (or a bare UTC midnight) keeps its date.

## The rule_targets carrier

`transforms/build_rule_targets.py` builds `rule_targets.parquet`
(`ACTOR_ID = spicy-regs:rule-targets:v3`): one row per observed rule-identity
edge, with `docket_id`, `cfr_ref` (+ `cfr_title`/`cfr_part`/`cfr_section`),
`rin`, the `source` class the edge came from, `first_seen`/`last_seen` as
Eastern days, and `fr_references_json` retaining each literal number-only
observation with the status that says how it resolved. The four source classes are
`fr_cfr_ref` (a CFR reference read off the Federal Register's own citation),
`docket_rin` (the docket's stated RIN), `document_rin` (a document's stated
RIN) and `document_fr_doc` (a document-to-docket resolution). Ambiguous or
missing references emit an observation row with null targets rather than an
invented edge.

The carrier is flat Apache Parquet, not JSON-LD: compact identifiers and enum
values expand deterministically to Rulespec terms, but the tables claim
Rulespec L0 vocabulary mapping only — no L1–L4 parsing, shape, constraint or
runtime conformance.

## Identifier expansion

Parquet keeps the compact join keys users already query. Expand them as
follows; every scheme is the one `ontology/citations.py` mints:

| Carrier value | Rulespec scheme | Canonical identifier |
| --- | --- | --- |
| CFR `40-60` / `40-60.1` | `rkaf:us-cfr` | `urn:rkaf:us:cfr:40:60` / `urn:rkaf:us:cfr:40:60.1` |
| U.S.C. title `42` + section `7401` | `rkaf:us-usc` | `urn:rkaf:us:usc:42:7401` |
| RIN `2060-AV16` | `rkaf:us-rin` | `urn:rkaf:us:rin:2060-AV16` (identity of a durable Regulatory Agenda item, never a Proceeding) |
| FR document `2024-00366` | `rkaf:us-frdoc` | `urn:rkaf:us:frdoc:2024-00366` |
| regulations.gov id `EPA-HQ-OAR-2021-0317` | `rkaf:us-regsgov` | `urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317` |

For local carrier provenance, `actor_id` values expand beneath
`urn:spicy-regs:actor:` after percent-encoding the stored value, and `run_id`
values beneath `urn:spicy-regs:run:`. An absent compact value means the
mapped relationship is absent; consumers must not mint an identifier for a
null.

The Federal Register distinction is deliberate. Rulespec's `rkaf:us-frdoc`
lexical space accepts only `YYYY-NNNNN`, while many real rows use legacy or
correction forms; the normative fallback is the permanent
federalregister.gov document URL as immutable identity, and a nonmatching
value is never labeled `rkaf:us-frdoc`.

## Anchor semantics: what an offset addresses and what a digest covers

Cross-project evidence references only translate mechanically when both sides
agree on the unit, the interval and the exact bytes each digest covers.
Rulespec's `rkaf:FragmentIdentityScheme` (Core §4.2) states the same
semantics this section states; the two documents must stay in agreement.

**Offsets are Python unicode codepoints over half-open `[start, end)`
intervals.** Not bytes, not UTF-16 code units, and never an inclusive end. A
section sign, an em dash, a curly quote and an astral emoji are each exactly
one codepoint, so `region.text == field_text[start_char:end_char]` holds for
every record — and a producer refuses to emit a record where it does not.
Consecutive sibling regions abut: one region's `end_char` is the next one's
`start_char`, and they share no codepoint. Every record carries its own
`coordinate_target` / `coordinate_unit` / `coordinate_interval`, so nothing
has to be inferred. Two targets exist and stay distinct:
`artifact-source-field` means the offsets index one exact field of one
artifact, and `adapter-parsed-text` means they index text a parser built,
which is graded `parser-derived` and never `source-exact`.

**Each digest names its own scope.** A content digest covers the canonical
JSON of exactly the declared values, in declaration order, nulls included;
it is not a digest of a concatenation, and a fragment digest covers exactly
the codepoints the interval names.

## Carrier mapping

The fenced block below is normative and machine-audited by Rulespec's
`tools/l0_mapping_audit.py`.

```yaml rkaf-l0-mapping
rulespec_version: "sha256:2d82f1fca983462089b6a078ba4918bc1f5f0385cde176afe15c97ded471a6d7"
mappings:
  - table: rule_targets
    column: docket_id
    subject_type: https://rulespec.org/ns/v1#Docket
    term: https://rulespec.org/ns/v1#hasDocketIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:regsgov:{docket_id}"
      identifier_scheme: https://rulespec.org/ns/v1#us-regsgov
    samples:
      - input:
          docket_id: EPA-HQ-OAR-2021-0317
        output: urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317
  - table: rule_targets
    column: cfr_ref
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasRegulatoryIdentifier
    direction: forward
    value_kind: iri
    transform:
      pattern: '^([1-9][0-9]*)-([0-9]+(?:\.[0-9]+[a-z]{0,3}(?:-[0-9a-z]+)*)?)$'
      replacement: 'urn:rkaf:us:cfr:\1:\2'
      identifier_scheme: https://rulespec.org/ns/v1#us-cfr
    samples:
      - input:
          cfr_ref: 40-60
        output: urn:rkaf:us:cfr:40:60
  - table: rule_targets
    column: rin
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaItem
    term: https://rulespec.org/ns/v1#hasAgendaItemIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:rin:{rin}"
      identifier_scheme: https://rulespec.org/ns/v1#us-rin
    samples:
      - input:
          rin: 2060-AV16
        output: urn:rkaf:us:rin:2060-AV16
```

The mapping claims the docket, CFR and RIN edges of `rule_targets` only.
`fr_references_json`, the attestation columns and `source` are carrier
provenance and are not mapped to Rulespec terms. A null `docket_id`, `cfr_ref`
or `rin` leaves its relationship absent.
