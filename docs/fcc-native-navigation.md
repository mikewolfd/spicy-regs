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

`fcc_native_field_states` reports those states. `fcc_native_observations` expands
each role independently, keeping source ordinals and raw elements. Name-only
participants remain role observations; a bureau ID stays in the raw bureau
object rather than becoming a person ID. `fcc_native_proceeding_links` requires
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
