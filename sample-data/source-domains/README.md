# Documented column domains

GSA, reginfo.gov, GPO (BILLSTATUS), the Senate LDA office, the FEC and GAO
decide what a set of published columns may hold — `document_type`, `docket_type`, the
Unified Agenda's `rule_stage`, `priority_category`, `rin_status` and `major`,
`bill_actions.action_type`, LDA `general_issue_code` and `filing_type`, and the
FEC committee type, designation, filing frequency and organization type codes,
and GAO's topics on `gao_recommendations.topics`;
our builders copy those strings through untouched. So this directory keeps both halves of the claim: the
publishers' own documents, byte for byte, and one dated observation of what our
published tables actually hold. `scripts/check_source_domain_drift.py` compares
them in both directions and fails on every disagreement nobody has recorded a
reason for.

## The check just failed

Run it and read the lines it marks `UNRECORDED`:

```
uv run python scripts/check_source_domain_drift.py
```

Each names one column and one value the two halves disagree about, in one of two
directions:

- **undocumented-value** — our data carries a value the publisher's list omits.
  Anyone switching on the documented list mishandles those rows.
- **unobserved-value** — the publisher documents a value no row carries. Either
  the observation is bounded (part of a table need not reach every documented
  value) or the publisher dropped or never used the value.

Neither is automatically a bug. Read the publisher's document, decide which it
is, then either record the finding in `ACCEPTED_DOMAIN_FINDINGS`
(`src/spicy_regs/sources/source_domains.py`) with the reason it stands, or fix
what produced it. The ledger is closed both ways: an unrecorded finding fails,
and so does a recorded finding the data stopped producing, because an exception
nothing exercises is a claim nobody checked.

## The pinned publisher documents

| File | Bytes | SHA-256 | Publisher URL |
|---|---|---|---|
| `regulations-gov-openapi-v4-2026-08-03.yaml` | 60,826 | `be43c866f5ca424a456bde36ea03cb9326c454ef4e1894a13df80b6dc6e22488` | `https://open.gsa.gov/api/regulationsgov/v4/openapi.yaml` |
| `reginfo-rin-data-ver10262011.xsd` | 22,730 | `94fdcf4b382830cc44b9956c00439dc20a9643de402c298cee71293a14153b24` | `https://www.reginfo.gov/public/xml/REGINFO_XML_Ver10262011.xsd` |
| `billstatus-xml-user-guide-2026-08-03.md` | 38,802 | `a10909696b2ed2244d75c76e75fa32bc3e4eb926deab7e4e00592a6a01c3ad3a` | `https://raw.githubusercontent.com/usgpo/bill-status/master/BILLSTATUS-XML_User_User-Guide.md` |
| `lda-general-issue-codes-2026-07-30.json` | 3,596 | `e1820ef17f3e63048ae50e526c2f56e507b2cf60d720fc227c76ee7c3610d5bf` | `https://lda.gov/api/v1/constants/filing/lobbyingactivityissues/` |
| `lda-filing-types-2026-07-30.json` | 2,803 | `49fbd39383b0be63fb474878aa229d4e397880a30c2e0dac1a0905bc660a3149` | `https://lda.gov/api/v1/constants/filing/filingtypes/` |
| `fec-committee-master-file-description-2026-08-03.html` | 29,343 | `dda49be2e360d39bb1b7dcbc53239e627109a26fbaefe172688aca84abc4ff66` | `https://www.fec.gov/campaign-finance-data/committee-master-file-description/` |
| `fec-committee-type-code-descriptions-2026-08-03.html` | 28,121 | `84e9f16628fd2475750cd89a3947f2c737a5f66c8ced04aea6b1118ac2aecaa4` | `https://www.fec.gov/campaign-finance-data/committee-type-code-descriptions/` |
| `fec-party-code-descriptions-2026-08-03.html` | 29,578 | `e17420381df0e5709449a8c9702600fde97503ea378ef357beef4c40ed6a6b09` | `https://www.fec.gov/campaign-finance-data/party-code-descriptions/` |
| `gao-topics-2026-08-15.html` | 122,070 | `9aa9e7f185b9433236f512a6f694f6c9cf57f109bba3e9ea99ac42de19180096` | `https://www.gao.gov/topics` |
| `gao-science-and-technology-2026-09-01.html` | 148,620 | `98391cad16eba43e48017782765088d8720116ba988e1591d85215804906d0cd` | `https://www.gao.gov/topics/science-and-technology` |

The six added on 2026-10-30 are copied byte for byte from RefSpec's pinned test
fixtures at `f8857c86` (the digests above equal RefSpec's own pins). The guide
is read with spicy-docs' `read_billstatus_guide`, and the LDA responses are
plain JSON. The FEC pages are prose HTML tables, so they are read through
RefSpec's parser rather than a second one: RefSpec's parse is vendored as
`src/spicy_regs/reference/refspec/fec-committee-codes.json`, and each list in it
is used only when the source digest RefSpec recorded equals the pinned page's.

The two GAO pages (added 2026-10-31, owner answer Q8) are copied byte for byte
from RefSpec's `tests/fixtures/gao_published_topics/` at `f8857c86`, whose pins
they equal. RefSpec's `parse_gao_published_topics` and `parse_gao_topic_page`
read them, and their output is vendored the same way as
`src/spicy_regs/reference/refspec/gao-published-topics.json`. The domain is
both pages together: the browse listing names 30 topics, and GAO's own
Science and Technology page states a 31st live term the listing omits (61
recommendations carry it). `GAO Mission and Operations` is listed but carried
by no recommendation; the ledger records it. `gao_reports.topics` (topics mixed
with keywords) and `gao_decisions.topics` (decision types) are not this list;
CBO and CRS taxonomies wait for their archive routes.

`documented-enumeration-capture-manifest-v1.json` binds each capture to its
digest, byte length, publisher URL and capture time.
`spicy_regs.sources.source_domains.read_capture` recomputes the digest and length
on every read, and every documented value is parsed out of those bytes on each
run rather than transcribed — a transcription rots in silence, a parse against a
pinned digest cannot.

The pin proves the capture has not changed since someone pinned it, so no value
can be edited into or out of a publisher's document without the check failing. It
proves nothing about what the publisher serves today, because nothing here
refetches `source_url`. Both halves of the comparison are files in this
repository, so the verdict is a function of the tree — a lock on a dated finding,
not a live drift detector. The lock is the useful part: it fails the moment
someone moves one half and leaves the ledger behind.

### regulations.gov API v4: three enums

The whole 60 KB document holds exactly three `enum` keys, in one block under a
`#Regulations.gov documentation` comment: `DocumentType` (5 values, lines
893-898), `DocketType` (2, lines 902-904) and `SubmitterType` (3, lines 908-911).
Everything else the API categorizes by — `subtype`, `category`,
`organizationType` — it calls agency-specific, so no closed list exists to check
against. Two quirks are load-bearing, and tests pin both: `- Nonrulemaking `
carries a trailing space that a YAML plain scalar drops, and `Supporting &
Related Material` carries the literal ampersand the API returns.

### reginfo.gov Unified Agenda RIN data: twenty sentences

This XSD holds no `xs:enumeration` at all. Every controlled field is
`<xs:restriction base="xs:string"/>` with no facets, and states its value list
only as `xs:documentation` prose of the form *One of the following options: "…",
"…"*. Five of those twenty sentences are read here: `RULE_STAGE` (line 66),
`PRIORITY_CATEGORY` (line 50), `RIN_STATUS` (line 58), `MAJOR` (line 74) and
`RELATED_RINType/RIN_RELATION` (line 526), observed over every element of the
`related_rins` list as `related_rins.rin_relation`. The
publisher's own prose repeats itself — `PRIORITY_CATEGORY` lists `Not Major`
twice — so a literal duplicate folds into one value and the raw count is kept
beside the distinct one rather than thrown away.

## What is not covered

`TTBL_ACTION` (line 443) is a decided exclusion. It names 34 timetable actions;
the same snapshot's `timetable_json` carries 1,139 distinct actions over 10,533
entries. The publisher's own data treats that field as free text, so checking
against its list would report a thousand findings and gate nothing. Its sentence
also opens *One of the following:* with unquoted values, so the XSD reader could
not parse it anyway — the reason and the parser's reach happen to coincide, and
only the reason justifies anything.

`federal_register.document_type` has no pinned document stating its list. The FR
API documentation page is not captured here, and a domain nobody published is not
a documented domain.

`comments.document_type` is now checked against `DocumentType`.
`comments.category` is not covered, by decision: it is not the `SubmitterType`
list the earlier note assumed but an agency-specific category (409 distinct
values on 1.86M of 26.4M rows, measured 2026-10-30), which no publisher list
documents.

Two lists are **labels only**: the dictionary shows their labels and nothing
gates on them. The BILLSTATUS action codes (guide section 3) because the guide
says "a complete, authoritative list of action codes does not exist" and that
codes were reused over the years (164 distinct live codes, 32 of the guide's 36,
on 529,762 of 694,998 coded rows); every label carries that caveat. FEC `party`
on both history tables because 141 and 139 off-list values (0.29% and 0.30% of
rows) are case variants and junk, so a gate would be a ledger of exceptions.
Since 2026-10-31 the history builders fold a case variant of exactly one code
(`Rep`, `Dem`, `dem`, `rep`: 92 committee and 13 candidate rows) to that code and
keep every other off-list value as filed; each receipt keeps the original in
`conversion_inputs` and the reason in `conversion_diagnostics`. Published rows
change at the next build of those families.
LDA covered government entities have no captured list at all.

Every documented list, gated or not, reaches `describe_table` as the table's
`metadata.documented_values` with `detail=true`, each column marked `checked`
or not, with its source URL, locator and any caveat. `values` is the
publisher's list, not the column's domain: a checked column also carries the
ledger's accepted disagreements as of the snapshot's `observed_at` —
`observed_off_list` (values the data carries that the list omits, with the
reason, e.g. `Public Submission` on 99.998% of comments rows) and `unobserved`
(listed values no row carries).

## The observation

`observed-domain-snapshot-2026-10-30.json` is derived, not captured: for each
gated column it holds the distinct values with their row support, the null
count, and the row count of the table they came from.
`scripts/check_source_domain_drift.py --observe --write-snapshot` generates it,
so the code that writes it is the code that reads it back.

It names its inputs: the published tables it read at
`https://data.spicygov.ai`, as of publication index
`sha256:72977cb667a8bd16fe014c08230b051da77da014d7b60c692d84c924cce1acc0`
(2026-10-31T07:37:10Z; re-observed whole when `gao_recommendations` joined, since
`bill_actions` and `documents` had advanced a generation; every finding but the
new GAO one, and its row support, came back unchanged). It read
them remotely, one column per query, so nothing was downloaded (`comments` alone
is 9.7 GB); each source therefore records the index's pin for that member
(`"digest_basis": "publication-index"`), and the observation refused to proceed
unless each file's parquet footer row count equalled the index's. That basis
pins identity by reference, not by measurement: the digest is the index's
statement about the member, never computed from the bytes read, and the footer
row count is the only independent binding between the two. A fragment of
that index (`published-index-fragment-2026-10-30.json`) is checked in beside the
snapshot so the recorded URLs stay verifiable offline. The per-value counts were
re-derived for every table but `comments` with the DuckDB CLI over the same URLs
and agreed exactly, and `comments.document_type` with a separate CLI run (both on
the 2026-10-31T01:59Z observation); `gao_recommendations.topics` was re-derived
the same way on this one and agreed. It superseded the 2026-09-22 file
(`documents` and `dockets` from the fork's r2.dev bucket; git history keeps it),
and its `unified_agenda` read was dropped in favour of that table's own dated
file below, so each table is pinned by one digest.

The observation run took 167 s and 0.94 GB peak resident memory. This is a dated
lock, not live validation: a value a later generation adds fails nothing until
someone re-observes.

`observed-domain-snapshot-2026-10-30-unified-agenda.json` observes
`unified_agenda` whole: its five domains, `RIN_RELATION` included, over all 60
retained editions (241,726 rows). A `related_rins.rin_relation` domain counts
the 28,720 list elements, so its `row_count` is elements and its source's
`row_count` table rows. It was observed from the source-only replay built on the
branch that added the column, before that generation was published, so it
records no publisher URL (`--unpublished`); its digest names that local build,
whose other columns equal the live table's both ways. Its `producer_revision`
names the rebased commit; the build ran at the pre-rebase `ad3fc0e7`, whose tree
differs only in generation-retention files. The pin holds only if that exact
replay (two builds, byte-identical) is what ships; an incremental rollup writes
different bytes and counts. Nothing in the gate re-reads published bytes, so
re-pin it as part of publishing whichever generation ships:

```
scripts/check_source_domain_drift.py --observe --data-dir <dir with unified_agenda.parquet> \
    --snapshot observed-domain-snapshot-2026-10-30-unified-agenda.json \
    --observed-at <ISO-8601> --producer-revision <sha> --write-snapshot
```

Each domain lives in exactly one dated file; the check refuses a domain two
files observe and a declared domain no file observes.

## Refreshing it

Two manual steps, neither of them scheduled. `tests/test_source_domain_drift.py`
runs the offline comparison in CI on every push, so a *changed* file here fails
immediately; a *stale* one does not, because staleness is invisible to a check
whose two inputs are both checked in.

Re-observe against a fresh download of the published tables:

```
SPICY_REGS_R2_URL=https://data.spicygov.ai \
uv run --frozen python scripts/check_source_domain_drift.py --observe \
    --observed-at <ISO-8601> --producer-revision "publication.v2.json sha256:<index digest>" --write-snapshot
```

Add `--data-dir <dir>` to read local copies instead and record their own
digests. The r2.dev host answers 429 to the remote column reads. The URLs the
observation records resolve through the publication index of that host; if a table's generation advanced, refresh the checked-in index fragment
too, or the URL assertions fail.

Re-pin a capture by refetching it from the `source_url` its manifest entry names,
then recording the digest and byte length of what came back. Either step surfaces
a real change as a failed test, which is the point of doing them.
