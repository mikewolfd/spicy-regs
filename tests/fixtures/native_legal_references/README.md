# Retained native legal-reference inputs

These files are byte-identical copies of SpicyDocs 0.47.0 fixtures:
`tests/fixtures/uscode/title-05-s423.xml` and
`tests/fixtures/cfr/ecfr-authority-title1-part18.xml`. The provider fixture
READMEs retain origin/excerpt provenance. They are native XML fragments, not
complete editions. `manifest.json` pins these exact bytes; edition is explicitly
unknown and the CFR filename does not establish title metadata.

The USC fragment contains native reference attributes and a source credit.
The CFR fragment separately states AUTH and SOURCE. No PARAUTH/SECAUTH shape,
complete title, or source acquisition is qualified by these fixtures. Tests
label additional malformed/empty/unknown inputs as synthetic controls.

Complete retained inputs added for identity qualification:

- `full-ecfr-title1.xml` is the unchanged 477,387-byte Title 1 response
  (`fe18aad18e3b6e8fde18478d1f64d946bb9963bb74f1c164183627663c695c72`)
  from RefSpec's 2026-08-24 retained capture, requested as of 2026-08-10.
- `title01-119-103.zip` is the unchanged OLRC title archive already retained
  by SpicyDocs under `tests/fixtures/uscode/xml_usc01@119-103.zip`.
  The owner archive reader verifies its complete XML member and native release
  point. It also provides native USLM and XHTML href qualification cases.

Acquisition receipts and full replay comparisons are retained in
`~/Work/corpora/supply-2026-09-02/receipts/native-legal-full-2026-09-27/`.
Test receipt mutations are explicitly constructed controls; they are not new
publisher responses.
