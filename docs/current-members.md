# Current member preparation

The member build reads both complete community-crosswalk rosters from the
reviewed fork commit named in `src/spicy_regs/sources/member_rosters.json`.
Each request uses that immutable URL and checks the response size and digest.
SpicyDocs owns transport, parsing, byte limits and source refusals. The source
evidence records the requested URL, capture and selected input pin.

These inputs were generated with the community repository's unchanged JSON
serializer from the approved term-date patch. The patch changes the documented
term dates and the dependent party-affiliation end date. Names, identifiers,
record order and term membership retain the reviewed source values. See
[the upstream draft correction](https://github.com/unitedstates/congress-legislators/pull/1067)
and the generated fork commit in the selection file. The fork inputs remain a
community crosswalk with documented corrections; they do not establish an
independently complete official congressional roster.

`scripts/prepare_members_native.py --work NEW_DIRECTORY` prepares the first
current native generation from these complete source files. It uses the
maintained member builder, field policies, receipt writer, source evidence and
generation admission. The captured published member artifact remains the
prior-generation input. Its old processing rows are not used to manufacture
new source facts. The outputs are `members`, `member_terms`,
`member_party_affiliations` and their shared receipts. The command restores
each processing table through the selected native reader and writes
`members-prepared.json` in its private work directory. It performs no
publication.

The ordinary scheduled member build continues to select receipt-backed native
priors. Publication of the prepared generation requires the exact reviewed
current code and source wheel, compatible readers, source and field checks,
the captured member-family entry, and an admitted run. The shared publisher
checks the complete artifact and evidence before changing that family's
pointer. Other family updates can proceed independently.

The source wheel's additional scorecard catalog entries stay disabled and
unqualified. This member preparation does not adopt the separate scorecard
numeric-schema transition or receipt-history changes. See the named member
tests and `docs/native-generation-lifecycle.md` for source and receipt checks.
