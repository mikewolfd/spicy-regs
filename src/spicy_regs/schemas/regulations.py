"""Concrete `RecordType` instances for the regulations.gov data shapes.

Each entry pairs a name, S3 path pattern, Parquet schema, dedup key, and an
extract function that maps a raw regulations.gov JSON payload to a flat record
dict. The pipeline addresses these by their dict key (``"dockets"`` etc.); the
key matches ``RecordType.name`` so staging paths and merge logic stay stable.
"""

from json import dumps as json_dumps

import polars as pl

from spicy_regs.schemas.base import RecordType


def _extract_comment(d: dict) -> dict:
    """SpicyDocs' comment extract, which the source repair also reads through, plus the host's PDF results column,
    in the published column order.

    One extract means a repair never clears a field the ETL filled. The text columns are left None and filled
    downstream: the run-pipeline ETL fills them inline from Mirrulations' pre-extracted text
    (transforms.EnrichCommentText), with the PDF text-extraction step (spicy_regs.enrich_pdf) as the backfill for
    attachments not yet extracted upstream. Imported here, not at module level: base CLI and MCP installs lack it.
    """
    from spicy_docs.schemas.regulations import COMMENT as SOURCE_COMMENT

    row = {**SOURCE_COMMENT.extract(d), "pdf_extraction_results_json": None}
    return {column: row[column] for column in COMMENT.schema}


def _extract_document(d: dict, *, attachment_relationship=None) -> dict:
    """Map a regulations.gov document payload to a flat record, keeping every file rendition in attachments_json."""
    attrs = d.get("data", {}).get("attributes", {})
    related = None
    if attachment_relationship is not None:
        from spicy_docs.sources.regulations_gov.attachment_records import attachment_records_json

        related = attachment_records_json(d, attachment_relationship)

    # Each fileFormats entry is one downloadable rendition of the document
    # (e.g. content.pdf), carrying its own URL, format, and byte size. Keep the
    # full list — the single file_url below is retained for backward compat.
    attachments = [
        {"url": f["fileUrl"], "format": f.get("format"), "size": f.get("size")}
        for f in attrs.get("fileFormats") or []
        if f.get("fileUrl")
    ]

    return {
        "document_id": d.get("data", {}).get("id"),
        "docket_id": (v.strip('"') if (v := attrs.get("docketId")) else v),
        "agency_code": attrs.get("agencyId"),
        "title": attrs.get("title"),
        "document_type": attrs.get("documentType"),
        "posted_date": attrs.get("postedDate"),
        "modify_date": attrs.get("modifyDate"),
        "comment_start_date": attrs.get("commentStartDate"),
        "comment_end_date": attrs.get("commentEndDate"),
        "file_url": attachments[0]["url"] if attachments else None,
        "attachments_json": json_dumps(attachments) if attachments else None,
        "attachment_records_json": related,
        "fr_doc_num": attrs.get("frDocNum"),
        "withdrawn": attrs.get("withdrawn"),
        "reason_withdrawn": attrs.get("reasonWithdrawn"),
        "additional_rins": (json_dumps(rins) if (rins := attrs.get("additionalRins")) else None),
        # Populated out-of-band by the PDF text-extraction step
        # (spicy_regs.enrich_pdf); the raw JSON has no text layer.
        "text_content": None,
        "text_extraction_status": None,
        "pdf_extraction_results_json": None,
    }


DOCKET = RecordType(
    name="dockets",
    path_pattern="/docket/",
    schema={
        "docket_id": pl.Utf8,
        "agency_code": pl.Utf8,
        "title": pl.Utf8,
        "docket_type": pl.Utf8,
        "modify_date": pl.Utf8,
        "abstract": pl.Utf8,
        "rin": pl.Utf8,
    },
    dedup_key="docket_id",
    extract=lambda d: {
        "docket_id": (v.strip('"') if (v := d.get("data", {}).get("id")) else v),
        "agency_code": d.get("data", {}).get("attributes", {}).get("agencyId"),
        "title": d.get("data", {}).get("attributes", {}).get("title"),
        "docket_type": d.get("data", {}).get("attributes", {}).get("docketType"),
        "modify_date": d.get("data", {}).get("attributes", {}).get("modifyDate"),
        "abstract": d.get("data", {}).get("attributes", {}).get("dkAbstract"),
        "rin": d.get("data", {}).get("attributes", {}).get("rin"),
    },
)


DOCUMENT = RecordType(
    name="documents",
    path_pattern="/documents/",
    schema={
        "document_id": pl.Utf8,
        "docket_id": pl.Utf8,
        "agency_code": pl.Utf8,
        "title": pl.Utf8,
        "document_type": pl.Utf8,
        "posted_date": pl.Utf8,
        "modify_date": pl.Utf8,
        "comment_start_date": pl.Utf8,
        "comment_end_date": pl.Utf8,
        "file_url": pl.Utf8,
        "attachments_json": pl.Utf8,
        "attachment_records_json": pl.Utf8,
        "fr_doc_num": pl.Utf8,
        "withdrawn": pl.Utf8,
        "reason_withdrawn": pl.Utf8,
        "additional_rins": pl.Utf8,
        # Text extracted from the document's PDF rendition, plus the outcome
        # of that extraction ("ok"/"empty"/"encrypted"/"error"/None if not yet run;
        # documents never take Mirrulations' "derived" comment text).
        "text_content": pl.Utf8,
        "text_extraction_status": pl.Utf8,
        # Latest per-URL PDF attempt; independent of retained or derived text.
        "pdf_extraction_results_json": pl.Utf8,
    },
    dedup_key="document_id",
    extract=_extract_document,
)


COMMENT = RecordType(
    name="comments",
    path_pattern="/comments/",
    schema={
        "comment_id": pl.Utf8,
        "docket_id": pl.Utf8,
        "comment_on_document_id": pl.Utf8,
        "comment_on_object_id": pl.Utf8,
        "original_document_id": pl.Utf8,
        "comment_reference_values_json": pl.Utf8,
        "agency_code": pl.Utf8,
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "organization": pl.Utf8,
        "category": pl.Utf8,
        "title": pl.Utf8,
        "comment": pl.Utf8,
        "document_type": pl.Utf8,
        "posted_date": pl.Utf8,
        "modify_date": pl.Utf8,
        "receive_date": pl.Utf8,
        "attachments_json": pl.Utf8,
        # Text of the comment's attachment(s), plus where it came from: "derived"
        # (Mirrulations' own extraction) or our PDF outcome ("ok"/"empty"/
        # "encrypted"/"error"); None if neither has run. The results column holds
        # the derived provenance object or the per-URL PDF attempts.
        "text_content": pl.Utf8,
        "text_extraction_status": pl.Utf8,
        "pdf_extraction_results_json": pl.Utf8,
        # Appended, as the contract appends them and the catalog's ADD COLUMN does: the agency's submitter class and
        # campaign count, as stated (NULL unread, 0 a stated zero).
        "subtype": pl.Utf8,
        "duplicate_comments": pl.Int32,
    },
    dedup_key="comment_id",
    extract=_extract_comment,
)


# Registry keyed by record-type name. Order matters: it drives the default
# set of data types the pipeline processes.
RECORD_TYPES: dict[str, RecordType] = {
    "dockets": DOCKET,
    "documents": DOCUMENT,
    "comments": COMMENT,
}
