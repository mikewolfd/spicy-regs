# Retained FEC agency originals

These unchanged public source fixtures come from SpicyDocs' `tests/fixtures/agency_reports/`.
`sources.json` records each original URL, byte size and SHA-256. The fixtures cover
FEC's native 2010 FOIA XML and the Oversight.gov Data Act report page. Tests use
an explicit fixture observation time; they do not assert a new source retrieval.

The consumer tests compare every report field, XML element, HTML body and asset
to the native parser's output, through both direct retained inputs and verified
source releases. They test transfer into the existing observation tables; source
parser correctness remains owned by SpicyDocs.
