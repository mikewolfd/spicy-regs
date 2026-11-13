# Bill title history, action context and older API reads

Query `congress_bills.title_history` for the titles attached to each selected
BILLSTATUS original. The array keeps source order and repeated entries, along
with the title type label, literal type code, legacy parent type and any stated
chamber, text-version or update field. `congress_bills.calendar_numbers` holds
bill-level calendar entries. The authority statement, source creation date and
origin chamber code appear as literal fields on the same row. Missing groups
read as NULL. A group the publisher explicitly left empty reads as an empty
list.

`bill_actions.action_committees`, `action_calendar_numbers`, and
`action_recorded_votes` hold relationships the source attached to that action.
Join the action and bill on `bill_id`. Do not use the bill-level calendar to
fill an action calendar. The source can state the same committee or vote in
several actions. The action index is the position in the selected source list.

## Select older bills explicitly

The bill-family build accepts space-separated modern bill IDs in
`BILL_FAMILY_CHILD_BILL_IDS` and a local durable
`BILL_FAMILY_CHILD_CHECKPOINT_DIR`. For example, the qualified selection used
`107-hr-3162`. Supply the existing Congress.gov credential and set
`BILL_FAMILY_MAX_VERSION_FETCHES` to cover the selected details and child pages.
This mode takes exact bills in Congresses 82–107 and reads their parent
details, summaries and actions. Its per-route page bound is eight. Historic
numbering outside that range requires a separate source and identity decision.

Each `congress_bills.child_routes` entry records the parent observation time,
advertised locator and count, child outcome, and any observed page pins.
`populated` and `requested-empty` come from a checked terminal page.
`unavailable` means the first requested locator answered HTTP 404 or 410.
`refused` includes incomplete pagination, incompatible responses and request
budget exhaustion. A refused walk can have partial page pins. Its rows do not
replace the last completed list. A credential refusal stops the build.
`unrequested` names a missing link or an endpoint outside the selected slice.
The inventory includes titles, committees, related bills, cosponsors and text
metadata, all intentionally unrequested for this slice. The separate subject
enrichment continues to own subjects.

Reuse the same checkpoint directory to retry an interrupted read or replay a
completed one. The reader checks each saved page against its digest and preserves
its original observation time. Select a new checkpoint directory to take a fresh
set of child observations. Parent dates remain source dates. Action rows carry
their own API page digest, observation time and position. Use the summary's ETL
receipt to inspect the same fields for a publisher summary.

## Read existing printing evidence

Select a row's `bill_id`, `version_code` and `printing_id` in `bill_versions`.
Use that exact identity to select the matching accepted `bill_versions` row
in `etl_receipts`, then inspect its `processing_json`. The retained
`raw_source_row` keeps `offered_formats_json`, `observed_at`, `sha256`,
`requested_url` and `resolved_url`. Local Python consumers can decode the
lossless evidence with `spicy_regs.etl_receipts.decode_exact_json`; the public
processing-evidence view remains queryable through `query_sql`.

The offered formats hold the publisher's exact URLs and type labels. The
capture time and digest describe an acquired body. A listing can offer formats
while its body capture fields are NULL. The former `read_receipt_fields` tool
is retired; this change uses the existing processing evidence and does not
restore it.
