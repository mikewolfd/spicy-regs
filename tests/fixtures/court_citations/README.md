`derived-text-cohort.json` retains the complete three-row published court PDF
extraction cohort pinned in the file. Text is derived by the recorded extractor,
not native CourtListener CSV text. Source PDF digests, URLs, native SHA-1 checks
and parent opinion pins remain with each row. Original PDFs and acquisition
receipts are under `~/.codex/artifacts/spicy-regs-court-cohort-20260927/`.

The bounded review checked every emitted supported citation against this exact
text. It found a governor's proclamation `No. 20-50` misclassified by the generic
case-docket rule, so this adapter excludes that rule explicitly. State reporter
citations, short-form citations and other unrecognized forms remain outside this
qualification. A completed zero-result body does not imply it cites no law.
