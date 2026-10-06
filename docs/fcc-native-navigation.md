# Native FCC navigation and regulatory date evidence

The FCC filing writer retains the literal values of `proceedings`, `filers`,
`authors`, `lawfirms`, `bureaus` and `documents` in `native_fields_json`. Keys
absent from the source remain absent; explicit nulls, empty arrays, unsupported
shapes and repeated elements remain distinct. `native_fields_sha256` hashes the
canonical selected-field serialization. It is not the response-body digest.
The capture journal or replay receipt supplies the latter evidence.

Existing display columns retain their meaning. An older filing not reread has
SQL NULL in both new fields. A freshly read correction replaces its whole
selected-field map, so a new empty array cannot inherit a prior participant or
artifact. The incremental writer aligns an older Parquet schema before merging.

`fcc_native_observations` expands the main native arrays independently, keeping
source ordinals, repeats and null elements. The original selected fields remain
in the shared receipts. Name-only participants remain role observations; bureau
codes do not become person IDs. `fcc_native_proceeding_links` requires
both native proceeding name and ID to match the selected FCC population and
reports duplicate targets as ambiguous. It does not search other docket systems.

Document occurrences expose offered URLs, filenames and descriptions. No MIME
type or file size is inferred from the filename. `acquisition_status=not_checked`
and a null retained digest mean this projection has not inspected capture or
attachment storage. Alternative documents and repeated URLs remain separate.
Redirect and acquired-body qualification still require attachment receipts.

The existing FR docket-link builder now retains a zero-based source ordinal
and adds the installed SpicyDocs normalizer's candidate spellings beside each
literal docket value. The rule label includes the interpretation module's
SHA-256. A candidate is not unique identity: normalization can collapse distinct
spellings, and excluded historical mentions and SEC file numbers stay visible
in the original field.

`lifecycle_date_evidence` routes event document keys using `dated_by`, separately
from the source that supplied the stage. Register keys keep the publication date
as part of identity. Neither a posting date nor stage source can substitute for
that date. Target counts establish selected-population existence only.

Validation uses retained FCC response fixtures with byte digests in
`tests/fixtures/fcc_native/receipts.json`, retained FR input, and explicit
synthetic unsupported/correction controls. Qualified local output and bounded
public target lookup receipts are under
`/Users/mikewolfd/.codex/artifacts/spicy-regs-fcc-native-20260927/`.
Those candidates are cohorts, not complete publication families. Full retained
backfill, publication, deployment and attachment acquisition remain separate.

## Recorded extraction results in main filings

`government-sources/3` adds `generation_id` and native `extraction_results`
to `fcc_filings`. The shared writer binds each row to its selected build
generation and retains the original producer row in the matching receipts.
Earlier `/1` and `/2` subject/receipt pairs restore under their exact historical
policies before rewriting; changing a policy label alone is insufficient.

Each result retains its URL, status, body digest, page count, error, and position
in the latest recorded result list. `offered_ordinals` contains every document
position whose literal `src` equals that URL. Repeated offers stay separate;
filename equality supplies no match. Null and empty result lists remain distinct.
The ordinal describes this retained list, not a lifetime attempt count.

`fcc_document_extraction_results` expands those main fields into one row per
result and matching offered position. Unmatched results have a null offered
position. Both `fcc_filing_artifacts` and `fcc_native_observations` expose the
offered URL alongside its descriptors and source position.

A recorded `ok` status does not prove retained PDF custody or accessible text.
The main fields report `capture_status=unverified` when a digest is recorded
and `text_access_status=unverified`. A valid digest is a recorded identifier;
it is not a byte-verification result. Malformed URLs and digest strings remain
inspectable with explicit status. Private capture attempts do not qualify a
public filing merely because their URLs match.

Rebuild only the FCC filings family from its selected retained producer input
or exact subject/receipt restoration. Then publish the matching subject and
receipt generation before publishing navigation metadata. New result routes
remain unavailable against earlier main schemas.
