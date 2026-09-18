# Unified Agenda source excerpts

These are exact RIN_INFO byte slices from retained reginfo.gov edition exports.
`pins.json` records each full input hash, zero-based half-open byte span, and
excerpt hash. Tests add only a document wrapper around the unchanged slice.
The two 2004 excerpts retain the publisher's invalid 0x19 control byte.

Source URL: `https://www.reginfo.gov/public/do/XMLViewFileAction?f=REGINFO_RIN_DATA_{edition}.xml`.
Retained collection: RefSpec `output/registry-real-data-sources/unified-agenda-editions`.
The first records of 199510 and 202510 cover older and current field shapes;
the 2004 records prove strict refusal without silently repairing source text.
