# Follow disclosed FARA relationships to filing evidence

The FARA pipeline builds ordinary local tables from exact retained production
exports, admitting each selected source release before writing its generation.
It retains every listed historical filing for the caller's selected native
registration numbers. The selected PDFs are separate original captures.

```sh
uv run --frozen python -m spicy_regs.pipelines.fara acquire \
  --registrant 6335 --registrant 7649 \
  --document-url https://efile.fara.gov/docs/6335-Exhibit-AB-20160224-3.pdf \
  --output-dir /new/acquisition --captures-out /new/captures.json
uv run --frozen python -m spicy_regs.pipelines.fara build \
  --captures /new/captures.json --output-dir /new/candidate
```

Reuse matching originals and acquisition descriptors before acquiring again.
Builds read the central raw archive; `SPICYGOV_RAW_ROOT` selects an explicit
alternative. Each build needs a fresh output directory. HTTP refusal or an
invalid source shape leaves failed evidence and no table generation. The source
reader and source-release profile must be included in the installed SpicyDocs
package; source-checkout qualification does not prove wheel adoption.

The generation contains [registrants](tables/fara_registrants.md),
[foreign principals](tables/fara_foreign_principals.md),
[filings](tables/fara_filings.md) and [population accounting](tables/fara_snapshots.md).
Native registration numbers and publisher PDF filenames remain text. Separate
principal entity IDs are not supplied; the relationship observation ID identifies
the exact source row. Literal native fields, source coordinates, original hashes
and source-release digests remain queryable.

`registration_status` and `relationship_status` describe termination-date
evidence. `no_termination_reported` preserves an absent or blank termination
value without claiming independently confirmed current activity. `terminated`
requires a parseable termination date; malformed dates remain `unresolved`.
Date diagnostics distinguish absent, blank and unparsed source values. Filings
dated after capture are preserved with `date_after_capture=true`.

This ordinary SQL example traces a historical relationship to its retained PDF:

```sql
SELECT r.registration_number, r.registrant_name,
       p.principal_name, p.country, p.relationship_status,
       f.document_type, f.stamped_date, f.document_url,
       f.association_status, f.document_sha256,
       f.document_review_status, f.document_review_json,
       p.source_locator_json AS relationship_source,
       f.source_locator_json AS filing_source
FROM fara_registrants r
JOIN fara_foreign_principals p USING (registration_number)
JOIN fara_filings f ON f.foreign_principal_observation_id=p.source_record_id
WHERE r.registration_number='6335'
  AND f.document_status='retained';
```

A filing joins to a principal observation only when registration number,
principal label and country match exactly and uniquely within the selected
export. `export_label_unique` means that limited source-label association.
Duplicate labels remain `ambiguous_label`; other nonempty labels remain
`unresolved_label`; filings without a principal label remain `registrant_only`.
No fuzzy matching, identity equivalence, date-based attribution, financial
interpretation or activity extraction is performed. Registrant-level filings
must not be joined to every principal and presented as principal-specific facts.

The bounded November 13, 2026 review found that the selected `7649` Exhibit AB
metadata names “State of Israel via Havas Media Germany GmbH,” while PDF page 1
names “HAVAS Media Network.” `document_review_status=principal_label_differs`
keeps this unresolved source difference visible without changing either value.
The `6335` sample supports the disclosed registration/principal label and its
historical filing. Neither sample qualifies all filing pages or other documents.
Optional review records in a capture selection bind the note, original digest,
page and viewed-image digest; they remain human review findings, separate from
structured publisher facts.

Inspect `fara_snapshots` for selected, omitted and observed-empty populations.
Unselected PDF links have `document_status=not_selected`, with null body hash
and size. A complete local generation and a private publication rehearsal do
not establish public delivery. The coordinator owns independent review,
SpicyDocs wheel adoption, catalog registration and publication selection.
