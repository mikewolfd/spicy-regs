# Independent merged-wheel review

**APPROVE the exact candidate wheel for the reviewed preservation and default extraction boundaries.** This review does not deploy the package or requalify remote data generations.

The wheel SHA-256 is `670d168db3ca15609485e363f2179a3fa6ad73572b4aadd4245e01d11c421168`. Both builds are byte-identical. Every packaged runtime file equals its archived source. The package retains every baseline file and all baseline extraction exports. Only the declared overlay changes runtime files. The newer bill committee activities, CBO feed items and GAO decisions remain registered; all earlier table definitions remain identical except the three explicitly added member identity fields. Dependency metadata changes only the requested Docling extra pin.

The retained derivation implementation computes a new code digest because adopted `api.py`/`model.py` changed and the static import graph now reaches `docling.py`, `docling_assets.py` and `docling_markdown.py`. Its original test correctly failed on the old digest and module list. The test-only patch records the new digest `239e046697f1b6f1583b080c2f7123783370c2e1c23f96a252bdcf159a61fe21` and those added modules. It changes only `first/source/tests/test_body_text_derivation.py`; wheel and runtime bytes remain unchanged.

Retaining derivation version `001` is justified for the default rendition path: `body_text` explicitly constructs `DocumentExtractor(NativeText())`, still at 200 DPI. It never selects the newly optional Docling strategy. Comparing the full `BodyText` dataclasses from the two exact wheels in separate processes found identical output for eight retained fixtures, including 21 PDF pages and the HTM/TXT/XML/USLM branches. Custom regional extraction and OCR behavior are outside this equivalence claim.

After the test-only repin, the focused archived suite against the built wheel passed: **129 passed, three skipped, two deselected**. The skips are existing PDF/XML cases without matching XML fixtures; the deselected cases require live acquisition. The consumer print refresh key uses the retained derivation version, rather than this code digest or package version. No stored generation, seal or runtime file was changed by this review.

See `independent-wheel-review.json` for commands, artifact hashes and limits, `body-text-equivalence.json` for fixture/output hashes, and `derivation-test-repin.patch` for the repeatable archive-only adjustment. Run the patch only on an unmodified archived test copy; the retained first/source copy already contains it.
