# NPPES provider observations

`transforms.build_nppes_providers.build_observations` writes a local Parquet
candidate from SpicyDocs' completely checked cohort result. Every occurrence
keeps the original fields in `fields_json`, source URL/member/SHA-256, record
number and byte coordinates. Convenience identity/name/lifecycle columns retain
literal strings. An absent sidecar column is null; a present blank cell is `""`.
Sidecar occurrences remain separate rows. No shared table is registered or
selected while source qualification is held.

The maintained example `examples/nppes_provider_lookup.py` uses RefSpec's NPI
validator, writes the local candidate, and runs an ordinary DuckDB left join.
Its request JSON has `request_id`, `identifier_kind` and `identifier` fields.
Only `identifier_kind == "NPI"` joins to NPI; a CMS Certification Number (CCN)
remains a different identifier even when its characters resemble an NPI.
Unmatched requests survive. Registration alone establishes no licensure,
Medicare participation or shared-address affiliation.

Run with the coordinated source trees available, reusing the existing runtime:

```sh
PYTHONPATH="$DOCS/src:$REGS/src:$REFSPEC/src" \
  uv run --no-sync --project "$REGS_RUNTIME" python \
  "$REGS/examples/nppes_provider_lookup.py" \
  --csv "$ORIGINAL_EXCERPT" --header "$ORIGINAL_HEADER" \
  --sha256 "$EXCERPT_SHA256" --bytes "$EXCERPT_BYTES" \
  --source-url "$EXACT_ARCHIVE_URL" --member "$EXACT_MEMBER" \
  --requests "$REQUESTS_JSON" --output "$NEW_LOCAL_PARQUET"
```

This example labels its product `weekly-changes-excerpt`. The raw observations
are usable within that scope; unmatched means not observed in the excerpt,
never nonexistent in NPPES. RefSpec remains an example-time identifier provider,
not a copied validator or a new mandatory SpicyRegs package dependency.
Coordinated SpicyDocs package adoption belongs to integration.

## Main-record lifecycle candidate

`apply_main_release` accepts complete main CSV members, explicit data periods,
a fixed cohort and exact predecessor digest. A monthly replaces the cohort's
main records. A weekly must immediately follow the prior covered period and
replaces each changed row in full. Sparse deactivation clears current names,
addresses and entity type; reactivation restores only newly disclosed values.
Unchanged rows keep the input pin that actually supplied them. The prior
snapshot remains immutable on success or refusal.

This candidate is explicitly `main-records-only`. It refuses an excerpt as a
baseline, an unknown/gapped/overlapping predecessor, a changed header/cohort,
or any sidecar application. Sidecar omission/removal rules, combined lifecycle
dates and actual monthly/update history need the exact V2 source documentation
and originals. Synthetic lifecycle tests establish mechanics only. Source ticket
`ss-jknq` stays held; no current directory, archive completeness, source-backed
organization/lifecycle population or public delivery is claimed.
