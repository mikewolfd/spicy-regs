# Join every observed RIN

A Federal Register record can name several Regulation Identifier Numbers (RINs).
Its `regulation_id_numbers_json` preserves the complete source array; the scalar
`rin` is only the first element for compatibility. Use the array for complete
joins and keep both `document_number` and `publication_date` in the result.

This DuckDB query joins each usable source RIN to House communications. It
deduplicates repeated RINs within one dated record and leaves the underlying
source strings, including placeholders such as `Not Assigned`, unchanged.

```sql
WITH fr_rins AS (
    SELECT DISTINCT
        fr.document_number,
        fr.publication_date,
        upper(trim(json_extract_string(item.value, '$'))) AS rin
    FROM federal_register fr,
         json_each(fr.regulation_id_numbers_json) item
    WHERE item.type = 'VARCHAR'
      AND regexp_full_match(
          upper(trim(json_extract_string(item.value, '$'))),
          '[0-9]{4}-[A-Z]{2}[0-9]{2}'
      )
)
SELECT h.communication_id, fr.document_number, fr.publication_date, fr.rin
FROM house_communications h
JOIN fr_rins fr ON upper(trim(h.rin)) = fr.rin
ORDER BY h.communication_id, fr.publication_date, fr.document_number, fr.rin;
```

One communication can match several dated Federal Register records, and one
record can match several communications. RIN equality supports discovery; it
does not by itself prove that the records describe the same action or a legally
effective rule. Missing or unusable source values do not become guessed links.

`proceedings.rins_json` retains every usable RIN already gathered from its docket,
document, Federal Register and rule-target evidence. The old scalar `rin` remains
set only when exactly one is observed. RIN equality still does not merge distinct
proceedings. Comment periods inherit the complete set from linked proceedings.
Legacy proceedings without the array can supply only their known scalar; rebuild
them to recover previously discarded multi-RIN sets. Original source observations
remain available in the input tables, including values that fail the RIN grammar.

Native record `2026-17334@2026-08-25` states both `3206-AO36` and `3206-AO80`.
The retained replay and tests check both identities without selecting just the
first. This correction preserves observations; it does not qualify the existing
proceeding or comment-period interpretation rules.
