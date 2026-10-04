"""Native legislative subject readers selected only for receipt-migrated inputs.

The integration owner switches the global view registry with its table catalog.
Receipt metadata is obtained separately through verified receipt reads.
"""

from .sql_views import SQLView

KEYS = ("bill_id", "from_version_code", "from_printing_id", "to_version_code", "to_printing_id")


def endpoints(_pins):
    parts = []
    for side in ("from", "to"):
        parts.append(f"""SELECT d.*, '{side}' AS endpoint_side,
            d.{side}_printing_id AS endpoint_printing_id,
            d.{side}_element_id AS endpoint_element_id,
            d.{side}_body_version_id AS endpoint_body_version_id,
            coalesce(t.n,0) AS target_count,
            CASE WHEN d.{side}_element_id IS NULL OR d.{side}_element_id=''
                 OR d.{side}_printing_id IS NULL THEN 'unsupported'
                 WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
            CASE WHEN t.n IS NULL OR d.{side}_body_version_id IS NULL OR t.body_version_id IS NULL THEN 'unavailable'
                 WHEN t.n<>1 THEN 'ambiguous' WHEN d.{side}_body_version_id=t.body_version_id THEN 'matches'
                 ELSE 'mismatch' END AS body_version_status
            FROM section_diff_items d LEFT JOIN (
                SELECT bill_id,version_code,printing_id,element_id,count(*) AS n,min(body_version_id) AS body_version_id
                FROM bill_sections GROUP BY bill_id,version_code,printing_id,element_id
            ) t ON d.bill_id=t.bill_id AND d.{side}_version_code=t.version_code
                AND d.{side}_printing_id=t.printing_id AND d.{side}_element_id=t.element_id""")
    return " UNION ALL ".join(parts)


def version_endpoints(_pins):
    parts = []
    for side in ("from", "to"):
        parts.append(f"""SELECT d.*, '{side}' AS endpoint_side,
            d.{side}_printing_id AS endpoint_printing_id,
            coalesce(t.n,0) AS target_count,
            CASE WHEN d.{side}_printing_id IS NULL THEN 'unsupported'
                 WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
            t.candidates AS candidate_versions
            FROM section_diffs d LEFT JOIN (
                SELECT bill_id,version_code,printing_id,count(*) AS n,
                    list(struct_pack(printing_id:=printing_id,body_version_id:=body_version_id)) AS candidates
                FROM bill_versions GROUP BY bill_id,version_code,printing_id
            ) t ON d.bill_id=t.bill_id AND d.{side}_version_code=t.version_code
                AND d.{side}_printing_id=t.printing_id""")
    return " UNION ALL ".join(parts)


NATIVE_DIFF_VIEWS = (
    SQLView("section_diff_version_endpoints", {
        "section_diffs": KEYS,
        "bill_versions": ("bill_id", "version_code", "printing_id", "body_version_id"),
    }, version_endpoints, "Both stable printing endpoints. Candidate versions remain a native list; multiple matches stay ambiguous.",
       (*KEYS, "endpoint_side")),
    SQLView("section_diff_endpoints", {
        "section_diff_items": (*KEYS, "seq", "from_element_id", "to_element_id", "from_body_version_id", "to_body_version_id"),
        "bill_sections": ("bill_id", "version_code", "printing_id", "element_id", "body_version_id"),
    }, endpoints, "Each correspondence resolves independently by printing and element. Body-version agreement is checked separately.",
       (*KEYS, "seq", "endpoint_side")),
)
