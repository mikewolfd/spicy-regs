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
  RIN key `\d{4}-[A-Z]{2}\d{2}` and returns `None` otherwise, and
  `action_evidence_rin` is the same less the X-pattern codes
  (`\d{4}-X[A-Z]\d{2}`, NOAA's 0648-X… and every agency's), which stay recorded
  but never decide action evidence (fork delivery decision 61);
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
  key, with the method in each reference's status), and which reads the
  `fr_docket_links` table once for every stage that shares it; and
  `linked_docket_ids`, which reads every docket a Federal Register docket value
  names, behind a label or in a list, with SpicyDocs' `normalize_docket_references`.
- `ontology/common.py` — storage and provenance shared by the ontology
  rollups: the attestation columns (`method`, `actor_id`, `run_id`,
  `asserted_at`, `supersedes_id`), `RunContext`, canonical JSON, the
  counting JSON readers, and `eastern_day`, the one day rule of the
  rulemaking tables (SpicyDocs' `regulations_gov_day`): an instant falls on
  its Eastern calendar day, and a date-only value (or a bare UTC midnight)
  keeps its date.

## The rule_targets carrier

`transforms/build_rule_targets.py` builds `rule_targets.parquet` (versioned by
the builder's `ACTOR_ID` constant, one bump per published row change): one row
per observed rule-identity edge, with `docket_id`, `cfr_ref` (+ `cfr_title`/`cfr_part`/`cfr_section`),
`rin`, the `source` class the edge came from, `first_seen`/`last_seen` as
Eastern days, and `fr_references_json` retaining each literal number-only
observation with the status that says how it resolved. The five source classes are
`fr_cfr_ref` (a CFR reference read off the Federal Register's own citation),
`docket_rin` (the docket's stated RIN), `document_rin` (a document's stated
RIN), `document_fr_doc` (a document-to-docket resolution) and
`docket_document_cites_action_notice` (below). Ambiguous or
missing references emit an observation row with null targets rather than an
invented edge.

A `docket_document_cites_action_notice` row says that one of the docket's own
Regulations.gov documents cites, by its `fr_doc_num`, a Federal Register
document that is action evidence: it states a RIN or a rule stage. It is the
citation that makes a docket an action docket (fork delivery decision 32), and
it is a relationship, not a rule target: `cfr_ref` and `rin` are null, so the
row folds to one per docket. Each entry of `fr_references_json` is one
citation: the citing document (`evidence_id`), its literal number
(`document_number`) and the one notice it resolves to (`candidate_ids`). The
edge unites nothing (decision 33 as amended). When the docket's proceeding
lists the notice in `fr_document_ids_json`, the two are one proceeding, as
they are when the notice names no trusted, non-catch-all docket and its citing
documents in such dockets all lie in that one proceeding (decision 56);
otherwise they are related by citation only, and the notice keeps its own
proceeding. On the R5 re-audit's parents,
55,183 dockets cite 103,621 action notices in 116,642 pairs, 47,699 of them
across proceedings. Uniting those would fuse 58,286 of 268,159 proceedings,
the largest into one of 5,920 dockets; catch-all `*_FRDOC_0001` dockets and
omnibus notices do most of the gluing (receipt `typed-citation-join-2026-09-26/`).

`proceedings.fr_document_joins_json` says how each Register document the
proceeding holds joined it, one entry per `fr_document_ids_json` id in the
same order: `{fr_document_id, joined_by}`, where `joined_by` is
`fr_docket_link` (through its own docket link, action evidence or not),
`fr_copy` (through its Regulations.gov copies), `specific_rin` (through a RIN
that only this docketed proceeding holds) or `fr_document` (the Register
document a docket-less proceeding is). A `specific_rin` entry adds
`joined_rins`, every RIN of the document that points here, and
`holder_sources`, the docket-side evidence that holds them, by name:
`docket_rin`, `document_rin` or `rule_targets:document_fr_doc`, the last a
copy's RINs on its docket. A RIN the proceeding has only through a Register
document it took in, by a docket link (which `rule_targets` restates as
`fr_cfr_ref` rows) or by such an attachment, holds nothing (decision 56 and the
owner's rulings on it). Each event in
`stage_events_json` carries the same `joined_by`, or `docket` for a
Regulations.gov document of the proceeding's own docket, and nothing more.

The carrier is flat Apache Parquet, not JSON-LD: compact identifiers and enum
values expand deterministically to Rulespec terms, but the tables claim
Rulespec L0 vocabulary mapping only — no L1–L4 parsing, shape, constraint or
runtime conformance.

## Rulemaking lifecycles

`transforms/build_lifecycles.py` and `build_agency_lifecycle_stats.py` are the
rulemaking dataset's last stages (owner decisions 54–56c, with 54a–54d and 55a;
each table versioned by its builder's actor constant). `lifecycle_events` holds
each docketed proceeding's stage events collapsed to one per document: a
Regulations.gov copy of a Register document (its own `fr_doc_num` resolves to
that one document, decision 60) is that document, dated by the Register and
staged as proceedings staged it. A document that states no number keeps
Regulations.gov's type and its upload day. Its `source` says who typed the
stage (`federal_register`, which types its copies wherever it states a type;
`regulations_gov`; or `unified_agenda` for a withdrawal the Agenda completed),
`dated_by` whose day it carries, and `anchor_role` the documents the lifecycle
anchors on. An event dated after the run's own Eastern day is left out.

`rulemaking_lifecycles` has one row per docketed proceeding; a docket-less one
is a single Register document that cannot pair. The proposal is the earliest
proposed event the Register dates; one dated only by its Regulations.gov upload
anchors only when the Register dates none that could be its publication (54d:
an upload day is when a document was posted, and agencies post unnumbered
"display" copies of a proposal on the public-inspection day, before the
Register's copy; the upload stays an event, anchoring nothing). No window: the
Register's proposal is time zero however far it follows, except where the
proceeding's own events say it is a later proposal, so the upload keeps its
anchor: a final between the two, or a Register proposal that is itself a
comment-period extension or a correction. It pairs with the earliest final
strictly after it (`finalized`). Without one, a final on the proposal's day makes a `companion`
only when the Register dates both (54b); a same-day final dated by its
Regulations.gov upload beside a Register proposal pairs nothing and makes
nothing. A proposal dated only by its upload with a final that day is an
`upload_pair` (54c): the day is when both were uploaded, so like a companion it
has no survival outcome. Else a Register or Agenda withdrawal on or after the
proposal makes it `withdrawn`, and anything else is `open`. With no proposal the
earliest final anchors a `final_without_observed_proposal`; with neither the row
is `no_anchor`. Finals before the proposal are counted in
`finals_before_proposal`, never paired. `outcome` and `duration_days` are the
survival columns: `final`, `withdrawn` (a competing outcome, never later final;
54a), or `censored` at `censor_date`, the generation's last event day, capped at
the run's day and also stated in the Parquet metadata. A proceeding's
`specific_rins_json` are the RINs its docket-side evidence alone holds
(`ontology/rins`, the same `specific_rin_holders` proceedings attaches by);
`open_signal`, `agenda_priority` and `agenda_major` read those RINs' latest
Unified Agenda entry that `agenda_item_proceedings` links to it.
`routine_family` states its five families, their agencies and their keep-out
phrases once, in `ROUTINE_FAMILIES`, and reads them at time zero from the
anchor's own title: the proposal's, the final's without a proposal, and only
with no anchor the proceeding's (55a); a pesticide-petition receipt is the
tolerance family by its own title. `anchored_by_specific_rin` flags a proposal
or final anchor that joined by specific RIN (decision 56c), and
`pre_2008_coverage` a lifecycle anchored before Regulations.gov's coverage; an
upload pair's day bounds the rule only from above, so it is true before 2008 and
NULL after.

A known limit: a Regulations.gov document whose stated Register number resolves
to no Register row is dated by its upload, which for a legacy document can be
years after the rule. On snapshot_9b2c770e's inputs 262 lifecycles anchor on
such a document whose stated number is more than a year older than its posting,
74 of them upload pairs that 54c keeps out of survival (the review counted 265
before 54b); no rule re-dates them. A second limit: a Regulations.gov document
that states no Register number is typed by Regulations.gov and dated by its
upload (decision 60: a copy is stated by its own `fr_doc_num` alone). On
snapshot_62318069's inputs 22,334 of 25,710 upload-dated events were such
unnumbered documents and 515 of them since-2015 survival anchors (352 open,
among them notices the Register files under another type); a title that names
a Register number is not read as the document's identity (a 30-day paperwork
notice cites the 60-day one; a correction cites its original), so they keep
Regulations.gov's type. 54d re-anchors only the 334 whose proceeding also holds
a Register-dated proposal that could be their publication.

`agency_lifecycle_stats` gives the Aalen-Johansen cumulative incidence of a
final, withdrawal competing (54a), over the lifecycles with an outcome, per
agency and for all agencies (`agency_code` NULL), overall, routine, non-routine
and per family: the first day it reaches each quartile, NULL where it never
does, with a 95% interval from the infinitesimal-jackknife log-log band (R
survival's multi-state `survfit`). The Parquet metadata states the conventions
exactly. A cell under 30 rules keeps its row, `suppressed`, with no estimates.

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
