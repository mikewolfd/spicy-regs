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
