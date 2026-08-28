"""Stable public Federal Register source columns and their faithful projection."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

FEDERAL_REGISTER_COLUMNS: tuple[str, ...] = (
    "document_number",
    "title",
    "abstract",
    "document_type",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agencies_json",
    "agency_slugs",
    "docket_ids_json",
    "regulation_id_numbers_json",
    "cfr_references_json",
    "topics_json",
    "html_url",
    "pdf_url",
    "body_html_url",
    "volume",
    "start_page",
    "end_page",
    "subtype",
    "executive_order_number",
    "modify_date",
)

def _text(value: object) -> str | None:
    return None if value is None else str(value)


def project_federal_register_document(document: Mapping[str, Any]) -> dict[str, str | None]:
    """Project one exact API record onto the stable public source columns."""

    agencies = document.get("agencies") or []
    agency_slugs = ",".join(
        str(agency["slug"])
        for agency in agencies
        if isinstance(agency, Mapping) and agency.get("slug")
    )
    return {
        "document_number": _text(document.get("document_number")),
        "title": _text(document.get("title")),
        "abstract": _text(document.get("abstract")),
        "document_type": _text(document.get("type")),
        "publication_date": _text(document.get("publication_date")),
        "effective_on": _text(document.get("effective_on")),
        "comments_close_on": _text(document.get("comments_close_on")),
        "signing_date": _text(document.get("signing_date")),
        "agencies_json": json.dumps(agencies),
        "agency_slugs": agency_slugs or None,
        "docket_ids_json": json.dumps(document.get("docket_ids") or []),
        "regulation_id_numbers_json": json.dumps(
            document.get("regulation_id_numbers") or []
        ),
        "cfr_references_json": json.dumps(document.get("cfr_references") or []),
        "topics_json": json.dumps(document.get("topics") or []),
        "html_url": _text(document.get("html_url")),
        "pdf_url": _text(document.get("pdf_url")),
        "body_html_url": _text(document.get("body_html_url")),
        "volume": _text(document.get("volume")),
        "start_page": _text(document.get("start_page")),
        "end_page": _text(document.get("end_page")),
        "subtype": _text(document.get("subtype")),
        "executive_order_number": _text(document.get("executive_order_number")),
        # The API exposes no update instant; preserve the public null column.
        "modify_date": None,
    }


__all__ = [
    "FEDERAL_REGISTER_COLUMNS",
    "project_federal_register_document",
]
