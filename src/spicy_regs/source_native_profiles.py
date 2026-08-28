"""Source profiles composed explicitly at SpicyRegs entry points."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any, Final, cast

from spicy_regs import federal_register_source_native as federal_register
from spicy_regs import regulations_gov_source_native as regulations_gov
from spicy_regs.source_native_profile import SourceNativeProfile

FEDERAL_REGISTER_ACQUISITION_POLICY_ID: Final = (
    "urn:spicy-regs:acquisition:federal-register-paginated"
)
FEDERAL_REGISTER_ACQUISITION_POLICY_VERSION: Final = "1.0"
FEDERAL_REGISTER_SOURCE_SCHEMA_KEY: Final = (
    "schemas/federal-register-document-1.0.schema.json"
)


def _federal_register_record_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> None:
    del query_scope
    if (
        not isinstance(page_window, tuple)
        or len(page_window) != 2
        or not all(isinstance(value, date) for value in page_window)
    ):
        raise federal_register.FederalRegisterSourceError(
            "Federal Register page lacks a validated date window"
        )
    typed_window = cast(tuple[date, date], page_window)
    record_date = date.fromisoformat(str(record["publication_date"]))
    if not typed_window[0] <= record_date <= typed_window[1]:
        raise federal_register.FederalRegisterSourceError(
            "Federal Register result falls outside its date window"
        )


FEDERAL_REGISTER_PROFILE: Final = SourceNativeProfile(
    name="Federal Register",
    source_system_id=federal_register.SOURCE_SYSTEM_ID,
    source_system_version=federal_register.SOURCE_SYSTEM_VERSION,
    acquisition_policy_id=FEDERAL_REGISTER_ACQUISITION_POLICY_ID,
    acquisition_policy_version=FEDERAL_REGISTER_ACQUISITION_POLICY_VERSION,
    scope_id=federal_register.SCOPE_ID,
    source_schema_key=FEDERAL_REGISTER_SOURCE_SCHEMA_KEY,
    source_schema=federal_register.FEDERAL_REGISTER_DOCUMENT_SCHEMA,
    record_stem="federal-register",
    max_traversals=federal_register.MAX_RECONCILIATION_TRAVERSALS,
    source_state_scope="observed-crawl",
    traversal_acceptance="stable-consecutive-traversals",
    acquisition_policy=federal_register.federal_register_acquisition_policy,
    validate_query_scope=federal_register.federal_register_query_scope,
    parse_page_response=federal_register.parse_page_response,
    next_page=federal_register.federal_register_next_page_url,
    traversal_check=federal_register.FederalRegisterTraversalCheck,
    classify_record=federal_register.classify_document,
    wrap_record=federal_register.source_record,
    record_digest=federal_register.source_record_digest,
    rendition_rows=federal_register.rendition_rows,
    source_schema_declaration=federal_register.source_schema_declaration,
    source_schema_digest=federal_register.source_schema_digest,
    validate_record_scope=_federal_register_record_scope,
    records_included=federal_register.federal_register_records_included,
    acquisition_check=federal_register.FederalRegisterAcquisitionCheck,
    page_window=federal_register.federal_register_request_window,
)

REGULATIONS_GOV_DOCUMENT_PROFILE: Final = SourceNativeProfile(
    name="Regulations.gov documents",
    source_system_id=regulations_gov.DOCUMENT_SOURCE_SYSTEM_ID,
    source_system_version=regulations_gov.SOURCE_SYSTEM_VERSION,
    acquisition_policy_id=regulations_gov.DOCUMENT_ACQUISITION_POLICY_ID,
    acquisition_policy_version=regulations_gov.ACQUISITION_POLICY_VERSION,
    scope_id=regulations_gov.DOCUMENT_SCOPE_ID,
    source_schema_key=regulations_gov.DOCUMENT_SOURCE_SCHEMA_KEY,
    source_schema=regulations_gov.REGULATIONS_GOV_DOCUMENT_SCHEMA,
    record_stem=regulations_gov.DOCUMENT_RECORD_STEM,
    max_traversals=regulations_gov.MAX_TRAVERSALS,
    source_state_scope="complete-snapshot",
    traversal_acceptance="source-enumeration",
    acquisition_policy=regulations_gov.document_acquisition_policy,
    validate_query_scope=regulations_gov.regulations_gov_document_query_scope,
    parse_page_response=regulations_gov.parse_document_page_response,
    next_page=regulations_gov.regulations_gov_next_page_url,
    traversal_check=regulations_gov.RegulationsGovTraversalCheck,
    classify_record=regulations_gov.classify_document,
    wrap_record=regulations_gov.document_source_record,
    record_digest=regulations_gov.document_source_record_digest,
    rendition_rows=regulations_gov.document_rendition_rows,
    source_schema_declaration=regulations_gov.document_source_schema_declaration,
    source_schema_digest=regulations_gov.document_source_schema_digest,
    validate_record_scope=regulations_gov.validate_document_record_scope,
    records_included=regulations_gov.document_records_included,
    acquisition_check=lambda: regulations_gov.MirrulationsAcquisitionCheck(
        regulations_gov.DOCUMENT_COLLECTION
    ),
    page_window=regulations_gov.parse_mirrulations_request,
)

REGULATIONS_GOV_DOCKET_PROFILE: Final = SourceNativeProfile(
    name="Regulations.gov dockets",
    source_system_id=regulations_gov.DOCKET_SOURCE_SYSTEM_ID,
    source_system_version=regulations_gov.SOURCE_SYSTEM_VERSION,
    acquisition_policy_id=regulations_gov.DOCKET_ACQUISITION_POLICY_ID,
    acquisition_policy_version=regulations_gov.ACQUISITION_POLICY_VERSION,
    scope_id=regulations_gov.DOCKET_SCOPE_ID,
    source_schema_key=regulations_gov.DOCKET_SOURCE_SCHEMA_KEY,
    source_schema=regulations_gov.REGULATIONS_GOV_DOCKET_SCHEMA,
    record_stem=regulations_gov.DOCKET_RECORD_STEM,
    max_traversals=regulations_gov.MAX_TRAVERSALS,
    source_state_scope="complete-snapshot",
    traversal_acceptance="source-enumeration",
    acquisition_policy=regulations_gov.docket_acquisition_policy,
    validate_query_scope=regulations_gov.regulations_gov_docket_query_scope,
    parse_page_response=regulations_gov.parse_docket_page_response,
    next_page=regulations_gov.regulations_gov_next_page_url,
    traversal_check=regulations_gov.RegulationsGovTraversalCheck,
    classify_record=regulations_gov.classify_docket,
    wrap_record=regulations_gov.docket_source_record,
    record_digest=regulations_gov.docket_source_record_digest,
    rendition_rows=regulations_gov.docket_rendition_rows,
    source_schema_declaration=regulations_gov.docket_source_schema_declaration,
    source_schema_digest=regulations_gov.docket_source_schema_digest,
    validate_record_scope=regulations_gov.validate_docket_record_scope,
    records_included=regulations_gov.docket_records_included,
    acquisition_check=lambda: regulations_gov.MirrulationsAcquisitionCheck(
        regulations_gov.DOCKET_COLLECTION
    ),
    page_window=regulations_gov.parse_mirrulations_request,
)

REGULATIONS_GOV_COMMENT_PROFILE: Final = SourceNativeProfile(
    name="Regulations.gov comments",
    source_system_id=regulations_gov.COMMENT_SOURCE_SYSTEM_ID,
    source_system_version=regulations_gov.SOURCE_SYSTEM_VERSION,
    acquisition_policy_id=regulations_gov.COMMENT_ACQUISITION_POLICY_ID,
    acquisition_policy_version=regulations_gov.ACQUISITION_POLICY_VERSION,
    scope_id=regulations_gov.COMMENT_SCOPE_ID,
    source_schema_key=regulations_gov.COMMENT_SOURCE_SCHEMA_KEY,
    source_schema=regulations_gov.REGULATIONS_GOV_COMMENT_SCHEMA,
    record_stem=regulations_gov.COMMENT_RECORD_STEM,
    max_traversals=regulations_gov.MAX_TRAVERSALS,
    source_state_scope="complete-snapshot",
    traversal_acceptance="source-enumeration",
    acquisition_policy=regulations_gov.comment_acquisition_policy,
    validate_query_scope=regulations_gov.regulations_gov_comment_query_scope,
    parse_page_response=regulations_gov.parse_comment_page_response,
    next_page=regulations_gov.regulations_gov_next_page_url,
    traversal_check=regulations_gov.RegulationsGovTraversalCheck,
    classify_record=regulations_gov.classify_comment,
    wrap_record=regulations_gov.comment_source_record,
    record_digest=regulations_gov.comment_source_record_digest,
    rendition_rows=regulations_gov.comment_rendition_rows,
    source_schema_declaration=regulations_gov.comment_source_schema_declaration,
    source_schema_digest=regulations_gov.comment_source_schema_digest,
    validate_record_scope=regulations_gov.validate_comment_record_scope,
    records_included=regulations_gov.comment_records_included,
    acquisition_check=lambda: regulations_gov.MirrulationsAcquisitionCheck(
        regulations_gov.COMMENT_COLLECTION
    ),
    page_window=regulations_gov.parse_mirrulations_request,
    observation_version=regulations_gov.comment_observation_version,
    refuse_equal_observation_versions=True,
)


__all__ = [
    "FEDERAL_REGISTER_ACQUISITION_POLICY_ID",
    "FEDERAL_REGISTER_ACQUISITION_POLICY_VERSION",
    "FEDERAL_REGISTER_PROFILE",
    "FEDERAL_REGISTER_SOURCE_SCHEMA_KEY",
    "REGULATIONS_GOV_DOCUMENT_PROFILE",
    "REGULATIONS_GOV_DOCKET_PROFILE",
    "REGULATIONS_GOV_COMMENT_PROFILE",
]
