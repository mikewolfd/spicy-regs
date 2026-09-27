# Relationship-view regression inputs

`senate-vote-119-1-00522.xml` is copied unchanged from SpicyDocs' retained native
fixture. Original bytes SHA-256:
`418eb3d0635cf1f1f4e8565af24b794d24b1436dd282648742e3f1dbb752803e`.
Its independent document and amendment blocks test that an empty amendment ID
cannot suppress a document or establish a positional pairing.

`meeting-119-house-119003.json` retains the complete `committeeMeeting` object
from SpicyDocs' native `congress-committee-meeting-detail-119003.json` fixture;
request metadata is omitted. Original response SHA-256:
`a43cb372471870d0926748f542eafcb24756c206b6e013621d1cc31c1cc9a69f`.
The test explicitly projects the existing held-array shapes using only stdlib
JSON/XML readers. It tests relationship SQL, not a replacement provider parser.
Neither fixture is a population-completeness claim or a fresh source capture.
