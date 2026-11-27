"""Export deterministic ODCS v3.2.0 DataContracts and enforce Parquet Arrow schema parity.

This module bridges ``spicy-regs`` metadata (``table_metadata.json`` and ``table_joins.json``)
and verified Parquet Arrow footers to ODCS v3.2.0 DataContract JSON documents tagged with
their target DDK domain (``spicycongress``, ``spicygov``, ``RefSpec``, or ``SpicyEntities``)
and verified canonical RefSpec / RKAF URN mappings (``urn:rkaf:us:...``, ``urn:ref:...``).

It also implements strict Arrow schema parity checks interoperable with
``ddk.publication.verify_parquet_schema_parity`` (identical contract normalization,
drift diagnostics, and summary structure) without rewriting any ETL pipelines or
requiring ``ddk`` at runtime.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
import io
import json
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.identifiers import RIN_PATTERN
from spicy_regs.native_types import arrow_type as parse_duckdb_arrow_type

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TABLE_METADATA_PATH = Path(__file__).with_name("table_metadata.json")
DEFAULT_TABLE_JOINS_PATH = Path(__file__).with_name("table_joins.json")
DEFAULT_ODCS_DIR = REPO_ROOT / "data_dictionary" / "odcs"
DEFAULT_ODCS_MANIFEST_PATH = DEFAULT_ODCS_DIR / "manifest.json"

ODCS_API_VERSION = "v3.2.0"
ODCS_CONTRACT_VERSION = "1.0.0"
ODCS_MANIFEST_FORMAT = "spicy-regs-odcs-manifest"
ODCS_MANIFEST_VERSION = 1
DDK_ARROW_EXACT_PROFILE = "urn:ddk:odcs:arrow-declaration:3"

#: Target DDK platform / registry domains.
DDK_DOMAINS: tuple[str, ...] = ("spicycongress", "spicygov", "RefSpec", "SpicyEntities")

#: Valid DDK conceptKind values (per DDK catalog.schema.json).
DDK_CONCEPT_KINDS: frozenset[str] = frozenset({"identifier", "attribute"})

#: Verified canonical RefSpec / RKAF URN prefixes and Atlas resource schemes.
#: Never emit a ``vendor: "refspec", property: "urn"`` value outside this verified set.
CANONICAL_REFSPEC_URNS: frozenset[str] = frozenset(
    {
        "urn:rkaf:us:cfr:",
        "urn:rkaf:us:eo:",
        "urn:rkaf:us:frdoc:",
        "urn:rkaf:us:pl:",
        "urn:rkaf:us:regsgov:",
        "urn:rkaf:us:rin:",
        "urn:rkaf:us:usc:",
        "urn:ref:federal-register-agency:",
        "urn:ref:regulations-gov-agency:",
        "urn:ref:atlas-resource-scheme:federal-register-api-topics",
        "urn:ref:atlas-resource-scheme:federal-register-thesaurus-2025",
        "urn:ref:atlas-resource-scheme:uei-authority",
        "urn:ref:atlas-resource-scheme:nppes-npi-authority",
    }
)

#: Eight pilot-wave families (6 legislative families pinned by congressional-tech + federal-register + rulemaking).
PILOT_FAMILIES: tuple[str, ...] = (
    "amendments",
    "bill-family",
    "committee-rosters",
    "members",
    "nominations",
    "roll-call-votes",
    "federal-register",
    "rulemaking",
)

#: Canonical family -> sorted table names registry for deterministic offline export across the full catalog.
#: Includes all 92 publication.v2.json families, the materialized rulemaking snapshot, and supported local families.
FAMILY_TABLES: dict[str, tuple[str, ...]] = {
    "administration-policy": ("administration_policy_bill_links", "administration_policy_statements"),
    "agency-monthly-volume": ("agency_monthly_volume",),
    "agency-stats": ("agency_stats",),
    "amendments": ("amendments",),
    "bill-family": (
        "bill_actions",
        "bill_committee_activities",
        "bill_committees",
        "bill_cosponsors",
        "bill_family_document_file_outcomes",
        "bill_publisher_summaries",
        "bill_sections",
        "bill_summaries",
        "bill_versions",
        "bill_vote_references",
        "cbo_cost_estimates",
        "cbo_estimate_cards",
        "cbo_feed_items",
        "congress_bills",
        "diff_summaries",
        "financial_changes",
        "public_activity_events",
        "section_classifications",
        "section_diff_items",
        "section_diffs",
    ),
    "bill-subjects": ("bill_subjects",),
    "census-acs": (
        "census_acs_congressional_district_observations",
        "census_acs_county_observations",
    ),
    "census-geocoder": ("census_geocoder_geographies", "census_geocoder_lookups"),
    "cfr-sections": ("cfr_sections",),
    "cms-medicaid": ("cms_medicaid_drug_spending",),
    "cms-medicare-provider": ("cms_medicare_provider",),
    "cms-open-payments": ("cms_open_payments_summaries",),
    "cms-part-b": ("cms_part_b_drug_spending",),
    "cms-part-d": ("cms_part_d_drug_spending",),
    "comment-attributes": ("comment_attributes",),
    "comments": ("comments", "comments_index"),
    "committee-meetings": ("committee_meetings", "committee_meetings_detail_reads"),
    "committee-reports": (
        "committee_report_cbo_letters",
        "committee_report_read_outcomes",
        "committee_reports",
        "committee_reports_document_file_outcomes",
        "hearing_bill_links",
        "hearing_transcripts",
        "report_sections",
    ),
    "committee-rosters": ("committee_assignments", "committee_directory_entries", "committees"),
    "congress-sessions": ("congress_party_control", "congress_sessions"),
    "court-citations": ("court_citation_map", "court_citations", "court_parentheticals"),
    "court-docket-groups": ("court_docket_groups",),
    "court-opinion-clusters": ("court_opinion_clusters",),
    "court-opinion-pdf-extractions": ("court_opinion_pdf_extractions",),
    "court-opinions": ("court_opinions",),
    "courtlistener": ("court_dockets",),
    "crs-reports": ("crs_reports",),
    "discovery-signals": ("discovery_signals",),
    "docket-attributes": ("docket_attributes",),
    "dockets": ("dockets",),
    "document-attributes": ("document_attributes",),
    "documents": ("documents",),
    "ecfr-snapshots": ("ecfr_snapshot_sections", "ecfr_snapshots"),
    "executive-disclosures": (
        "executive_disclosure_facts",
        "executive_disclosure_filings",
        "executive_disclosure_pages",
    ),
    "executive-orders": ("executive_order_observations",),
    "fara": ("fara_filings", "fara_foreign_principals", "fara_registrants", "fara_snapshots"),
    "fcc-filings": ("fcc_filings",),
    "fcc-proceedings": ("fcc_proceedings",),
    "fec-candidate-history": ("fec_candidate_history",),
    "fec-committee-history": ("fec_committee_history",),
    "fec-committees": ("fec_committees",),
    "fec-election-results": ("fec_election_results", "fec_election_workbook_rows"),
    "fec-financial-profiles": ("fec_financial_profiles",),
    "fec-observations": ("fec_collections", "fec_relationships", "fec_source_records"),
    "fec-query": (
        "fec_account_transfers",
        "fec_agency_mapping_dispositions",
        "fec_agency_report_documents",
        "fec_agency_report_text",
        "fec_agency_reports",
        "fec_allocated_disbursements",
        "fec_allocation_bases",
        "fec_api_response_controls",
        "fec_audit_findings",
        "fec_bundled_contributions",
        "fec_candidate_api_observations",
        "fec_collection_selection",
        "fec_committee_master_observations",
        "fec_committee_observations",
        "fec_communication_costs",
        "fec_contribution_aggregates",
        "fec_coordinated_party_expenditures",
        "fec_debts",
        "fec_disbursements",
        "fec_electioneering_communications",
        "fec_filing_definition_evidence",
        "fec_filing_definitions",
        "fec_filing_header_associations",
        "fec_filing_links",
        "fec_filing_report_observations",
        "fec_filing_text_observations",
        "fec_filings",
        "fec_historical_ie_statistics",
        "fec_inaugural_donations",
        "fec_independent_expenditures",
        "fec_intercommittee_transactions",
        "fec_legal_documents",
        "fec_legal_events",
        "fec_legal_matters",
        "fec_legal_parties",
        "fec_loan_guarantors",
        "fec_loan_terms",
        "fec_loans",
        "fec_lobbyist_registrations",
        "fec_oversight_recommendations",
        "fec_postgres_committee_history_observations",
        "fec_quality_notices",
        "fec_receipts",
        "fec_record_evidence",
        "fec_registration_statements",
        "fec_report_metrics",
        "fec_reported_financial_summaries",
        "fec_research_context_dispositions",
        "fec_research_document_observations",
        "fec_research_filing_feed_items",
        "fec_research_meeting_observations",
        "fec_research_response_outcomes",
        "fec_research_source_pages",
        "fec_retained_csv_observations",
    ),
    "fec-research-profiles": (
        "fec_committee_relationship_profiles",
        "fec_contribution_decisions",
        "fec_contribution_profiles",
        "fec_outside_spending_profiles",
        "fec_outside_spending_spenders",
    ),
    "fec-source-catalog": ("fec_source_catalog",),
    "federal-register": ("federal_register",),
    "feed-summary": ("feed_summary",),
    "fjc-judicial-careers": ("fjc_appointments", "fjc_courts", "fjc_judges", "fjc_service_terms", "fjc_snapshots"),
    "foreign-assistance": ("foreign_assistance_observations", "foreign_assistance_snapshots"),
    "fr-docket-links": ("fr_docket_links",),
    "gao-recommendations": ("gao_recommendations",),
    "gao-reports": ("gao_decisions", "gao_reports"),
    "grants-opportunities": ("grants_opportunity_observations",),
    "grants-status": ("grants_status_observations", "grants_status_queries"),
    "gsa-iolp": ("gsa_iolp_leases", "gsa_iolp_properties", "gsa_iolp_snapshots"),
    "historical-vetoes": ("historical_veto_administrations", "historical_veto_snapshots"),
    "hospital-cost-reports": ("hospital_cost_report_cells", "hospital_cost_report_measures", "hospital_cost_reports"),
    "house-communications": (
        "house_communications",
        "house_communications_detail_reads",
        "house_record_enrichment_results",
    ),
    "house-disbursement-pages": ("house_disbursement_pdf_pages", "house_disbursement_pdf_rows"),
    "house-disbursements": ("house_disbursements", "house_disbursements_summary"),
    "house-disclosures": (
        "house_disclosure_amendment_links",
        "house_disclosure_filings",
        "house_disclosure_pages",
        "house_disclosure_reports",
        "house_disclosure_transactions",
    ),
    "irs-527": (
        "irs527_contributions",
        "irs527_directors",
        "irs527_eains",
        "irs527_expenditures",
        "irs527_form8871",
        "irs527_form8872",
        "irs527_related_entities",
        "irs527_transmissions",
    ),
    "irs-form990": ("irs990_filings", "irs990_grants", "irs990_officers"),
    "laws": ("law_code_sections", "law_sections", "laws", "laws_document_file_outcomes", "table3_records"),
    "lobbying-filings": (
        "lobbying_activities",
        "lobbying_activity_lobbyists",
        "lobbying_affiliated_organizations",
        "lobbying_clients",
        "lobbying_conviction_disclosures",
        "lobbying_filings",
        "lobbying_foreign_entities",
        "lobbying_registrants",
    ),
    "lobbying-quarterly-profiles": ("lobbying_quarterly_profiles",),
    "member-vote-terms": ("member_vote_terms",),
    "members": ("member_party_affiliations", "member_terms", "members"),
    "native-legal-references": (
        "native_legal_reference_reads",
        "native_legal_references",
        "native_legal_references_document_file_outcomes",
    ),
    "nominations": ("nominations", "nominations_detail_reads"),
    "ofac-sanctions": ("ofac_entity_observations", "ofac_snapshots"),
    "oira-meetings": ("oira_meetings",),
    "oira-reviews": ("oira_reviews",),
    "org-committee-links": ("org_committee_links",),
    "press-releases": ("press_releases",),
    "print-citations": (
        "bill_committee_actions",
        "budget_volumes",
        "document_citation_read_outcomes",
        "document_citations",
        "house_activity_reports",
        "print_citations_document_file_outcomes",
    ),
    "record-issues": ("record_issues",),
    "roll-call-votes": ("member_votes", "roll_call_votes"),
    "rulemaking": (
        "agency_lifecycle_stats",
        "agenda_item_proceedings",
        "comment_periods",
        "lifecycle_events",
        "proceedings",
        "regulatory_agenda_items",
        "rule_targets",
        "rulemaking_lifecycles",
    ),
    "sam-entities": ("sam_entities",),
    "sam-exclusions": ("sam_exclusion_observations", "sam_exclusion_snapshots"),
    "sam-opportunities": ("sam_opportunity_observations", "sam_opportunity_snapshots"),
    "scorecard-analysis": ("scorecard_action_comparisons", "scorecard_item_links", "scorecard_member_links"),
    "scorecards": (
        "scorecard_items",
        "scorecard_member_item_results",
        "scorecard_member_ratings",
        "scorecard_members",
        "scorecard_methodologies",
        "scorecard_metric_components",
        "scorecard_metric_items",
        "scorecard_metrics",
        "scorecard_publishers",
        "scorecard_snapshots",
        "scorecards",
    ),
    "sec-edgar": ("sec_fact_scopes", "sec_filings", "sec_issuer_sources", "sec_reported_facts"),
    "sector-comparisons": (
        "lobbying_issue_financial_observations",
        "sector_classification_evidence",
        "sector_financial_comparisons",
    ),
    "senate-disbursements": ("senate_disbursement_pages", "senate_disbursements"),
    "senate-disclosures": ("senate_disclosure_reports", "senate_disclosure_transactions"),
    "senate-expenditures": ("senate_expenditures", "senate_expenditures_document_file_outcomes"),
    "table3-history": ("table3_historical_records",),
    "treasury-fiscal": (
        "treasury_debt_observations",
        "treasury_statement_classification_cells",
        "treasury_statement_periods",
    ),
    "treaties": ("treaties",),
    "unified-agenda": ("unified_agenda",),
    "usaspending-awards": ("usaspending_archive_members", "usaspending_awards", "usaspending_transactions"),
    "usaspending-recipients": ("usaspending_recipients",),
    "uscode-editions": ("uscode_edition_sections", "uscode_edition_structure", "uscode_editions"),
    "white-house-actions": ("white_house_action_versions",),
}

#: Target DDK primary domain and secondary domain tags for each family.
FAMILY_DOMAIN_ASSIGNMENTS: dict[str, tuple[str, tuple[str, ...]]] = {
    # Legislative / Congress families -> spicycongress
    "amendments": ("spicycongress", ()),
    "bill-family": ("spicycongress", ("RefSpec",)),
    "bill-subjects": ("spicycongress", ("RefSpec",)),
    "committee-meetings": ("spicycongress", ()),
    "committee-reports": ("spicycongress", ()),
    "committee-rosters": ("spicycongress", ("SpicyEntities",)),
    "congress-sessions": ("spicycongress", ()),
    "historical-vetoes": ("spicycongress", ("spicygov",)),
    "house-communications": ("spicycongress", ()),
    "house-disbursement-pages": ("spicycongress", ("spicygov",)),
    "house-disbursements": ("spicycongress", ("spicygov",)),
    "house-disclosures": ("spicycongress", ("SpicyEntities",)),
    "laws": ("spicycongress", ("RefSpec",)),
    "member-vote-terms": ("spicycongress", ("SpicyEntities",)),
    "members": ("spicycongress", ("SpicyEntities",)),
    "native-legal-references": ("spicycongress", ("RefSpec",)),
    "nominations": ("spicycongress", ("spicygov",)),
    "press-releases": ("spicycongress", ()),
    "print-citations": ("spicycongress", ("RefSpec",)),
    "record-issues": ("spicycongress", ()),
    "roll-call-votes": ("spicycongress", ()),
    "scorecard-analysis": ("spicycongress", ()),
    "scorecards": ("spicycongress", ()),
    "senate-disbursements": ("spicycongress", ("spicygov",)),
    "senate-disclosures": ("spicycongress", ("SpicyEntities",)),
    "senate-expenditures": ("spicycongress", ("spicygov",)),
    "treaties": ("spicycongress", ()),
    # Reference & legal text / geographic authority families -> RefSpec
    "census-acs": ("RefSpec", ("spicygov",)),
    "census-geocoder": ("RefSpec", ("spicygov",)),
    "cfr-sections": ("RefSpec", ("spicygov",)),
    "ecfr-snapshots": ("RefSpec", ("spicygov",)),
    "table3-history": ("RefSpec", ("spicycongress",)),
    "uscode-editions": ("RefSpec", ("spicycongress",)),
    # Entity registry & cross-system identity resolution families -> SpicyEntities
    "fara": ("SpicyEntities", ("spicygov",)),
    "ofac-sanctions": ("SpicyEntities", ("spicygov",)),
    "org-committee-links": ("SpicyEntities", ("spicygov",)),
    "sam-entities": ("SpicyEntities", ("spicygov",)),
    "sam-exclusions": ("SpicyEntities", ("spicygov",)),
    "usaspending-recipients": ("SpicyEntities", ("spicygov",)),
    # Regulatory / executive / courts / campaign finance / spending -> spicygov
    "administration-policy": ("spicygov", ("spicycongress",)),
    "agency-monthly-volume": ("spicygov", ()),
    "agency-stats": ("spicygov", ()),
    "cms-medicaid": ("spicygov", ()),
    "cms-medicare-provider": ("spicygov", ("SpicyEntities",)),
    "cms-open-payments": ("spicygov", ("SpicyEntities",)),
    "cms-part-b": ("spicygov", ()),
    "cms-part-d": ("spicygov", ()),
    "comment-attributes": ("spicygov", ()),
    "comments": ("spicygov", ()),
    "court-citations": ("spicygov", ("RefSpec",)),
    "court-docket-groups": ("spicygov", ()),
    "court-opinion-clusters": ("spicygov", ()),
    "court-opinion-pdf-extractions": ("spicygov", ()),
    "court-opinions": ("spicygov", ()),
    "courtlistener": ("spicygov", ()),
    "crs-reports": ("spicygov", ("spicycongress",)),
    "discovery-signals": ("spicygov", ()),
    "docket-attributes": ("spicygov", ()),
    "dockets": ("spicygov", ()),
    "document-attributes": ("spicygov", ()),
    "documents": ("spicygov", ()),
    "executive-disclosures": ("spicygov", ("SpicyEntities",)),
    "executive-orders": ("spicygov", ("RefSpec",)),
    "fcc-filings": ("spicygov", ()),
    "fcc-proceedings": ("spicygov", ()),
    "fec-candidate-history": ("spicygov", ("SpicyEntities",)),
    "fec-committee-history": ("spicygov", ("SpicyEntities",)),
    "fec-committees": ("spicygov", ("SpicyEntities",)),
    "fec-election-results": ("spicygov", ("spicycongress",)),
    "fec-financial-profiles": ("spicygov", ("SpicyEntities",)),
    "fec-observations": ("spicygov", ()),
    "fec-query": ("spicygov", ("SpicyEntities",)),
    "fec-research-profiles": ("spicygov", ("SpicyEntities",)),
    "fec-source-catalog": ("spicygov", ()),
    "federal-register": ("spicygov", ("RefSpec",)),
    "feed-summary": ("spicygov", ()),
    "fjc-judicial-careers": ("spicygov", ("SpicyEntities",)),
    "foreign-assistance": ("spicygov", ()),
    "fr-docket-links": ("spicygov", ("RefSpec",)),
    "gao-recommendations": ("spicygov", ("spicycongress",)),
    "gao-reports": ("spicygov", ("spicycongress",)),
    "grants-opportunities": ("spicygov", ()),
    "grants-status": ("spicygov", ()),
    "gsa-iolp": ("spicygov", ()),
    "hospital-cost-reports": ("spicygov", ("SpicyEntities",)),
    "irs-527": ("spicygov", ("SpicyEntities",)),
    "irs-form990": ("spicygov", ("SpicyEntities",)),
    "lobbying-filings": ("spicygov", ("SpicyEntities",)),
    "lobbying-quarterly-profiles": ("spicygov", ("SpicyEntities",)),
    "oira-meetings": ("spicygov", ()),
    "oira-reviews": ("spicygov", ()),
    "rulemaking": ("spicygov", ("RefSpec",)),
    "sam-opportunities": ("spicygov", ()),
    "sec-edgar": ("spicygov", ("SpicyEntities",)),
    "sector-comparisons": ("spicygov", ("SpicyEntities",)),
    "treasury-fiscal": ("spicygov", ()),
    "unified-agenda": ("spicygov", ("RefSpec",)),
    "usaspending-awards": ("spicygov", ("SpicyEntities",)),
    "white-house-actions": ("spicygov", ()),
}

_TABLE_TO_FAMILY: dict[str, str] = {table: family for family, tables in FAMILY_TABLES.items() for table in tables}

#: Regex constraints from ODCS v3.2.0 JSON Schema and safe filesystem/manifest checks.
_STABLE_ID_RE = re.compile(r"^[^\s.#/\\@!%&^]+$")
_SHORTHAND_REF_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*(\.[A-Za-z_][A-Za-z0-9_\-]*)+$")
_VENDOR_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_FAMILY_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_DECIMAL_RE = re.compile(r"^(?:decimal|decimal128|decimal256|numeric)\s*\(\s*(\d+)\s*,\s*(-?\d+)\s*\)$", re.IGNORECASE)
_FIXED_BIN_RE = re.compile(r"^(?:fixed|fixed_size_binary)\s*[\[(]\s*(\d+)\s*[\])]$", re.IGNORECASE)

_VALID_ODCS_LOGICAL_TYPES = frozenset(
    {
        "string",
        "date",
        "timestamp",
        "time",
        "number",
        "integer",
        "object",
        "array",
        "boolean",
        "map",
        "vector",
    }
)


class ODCSContractError(ValueError):
    """Raised when ODCS contract generation, validation, or Arrow schema parity fails."""


@dataclass(frozen=True)
class RefSpecMapping:
    """Verified RefSpec facet / optional canonical URN + DDK concept mapping attached to a property.

    ``refspec_urn`` MUST either be ``None`` (when no canonical RefSpec / RKAF URN exists for the
    column) or a verified entry in :data:`CANONICAL_REFSPEC_URNS`. ``ddk_concept_kind`` MUST be
    ``"identifier"`` or ``"attribute"`` per DDK's ``catalog.schema.json``.
    """

    refspec_urn: str | None
    refspec_facet: str
    ddk_concept: str
    ddk_concept_kind: str = "identifier"
    pattern: str | None = None
    authority_url: str | None = None
    authority_description: str | None = None

    def __post_init__(self) -> None:
        if self.refspec_urn is not None and self.refspec_urn not in CANONICAL_REFSPEC_URNS:
            raise ValueError(f"Unverified RefSpec URN {self.refspec_urn!r} is not in CANONICAL_REFSPEC_URNS")
        if self.ddk_concept_kind not in DDK_CONCEPT_KINDS:
            raise ValueError(f"Invalid DDK conceptKind {self.ddk_concept_kind!r}; expected one of {DDK_CONCEPT_KINDS}")


_CONGRESS_SUBJECTS = frozenset({"congress", "oversight"})
_FEC_SUBJECTS = frozenset({"campaign_finance"})
_LOBBYING_SUBJECTS = frozenset({"lobbying"})
_COURT_SUBJECTS = frozenset({"courts"})

#: Canonical RIN pattern sourced directly from ``spicy_regs.identifiers.RIN_PATTERN``.
_RIN_FULL_PATTERN = f"^{RIN_PATTERN}$"
_AGENDA_ITEM_RIN_PATTERN = f"^urn:rkaf:us:rin:{RIN_PATTERN}$"

#: Verified RefSpec / RKAF / DDK semantic mappings for typed identifiers, agencies, legal references, and vocabularies.
#: Only columns with an actual canonical RefSpec / RKAF URN in RefSpec or spicy-docs emit ``refspec_urn``.
_UNCONDITIONAL_REFSPEC: dict[str, RefSpecMapping] = {
    "bill_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legislative:concept:bill-id",
        pattern=r"^[0-9]+-(hr|s|hjres|sjres|hconres|sconres|hres|sres)-[0-9]+$",
    ),
    "amended_bill_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legislative:concept:bill-id",
        pattern=r"^[0-9]+-(hr|s|hjres|sjres|hconres|sconres|hres|sres)-[0-9]+$",
    ),
    "amendment_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legislative:concept:amendment-id",
    ),
    "amended_amendment_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legislative:concept:amendment-id",
    ),
    "bioguide_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:legislative:concept:bioguide-id",
        pattern=r"^[A-Z][0-9]{6}$",
    ),
    "sponsor_bioguide_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:legislative:concept:bioguide-id",
        pattern=r"^[A-Z][0-9]{6}$",
    ),
    "bioguide_previous": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:legislative:concept:bioguide-id",
    ),
    "vote_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:legislative:concept:roll-call-id",
        pattern=r"^[0-9]+-(house|senate)-[1-2]-[0-9]+$",
    ),
    "nomination_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:legislative:concept:nomination-id",
    ),
    "law_id": RefSpecMapping(
        refspec_urn="urn:rkaf:us:pl:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legislative:concept:public-law",
    ),
    "public_law_number": RefSpecMapping(
        refspec_urn="urn:rkaf:us:pl:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legislative:concept:public-law",
    ),
    "executive_order_number": RefSpecMapping(
        refspec_urn="urn:rkaf:us:eo:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:regulatory:concept:executive-order-number",
    ),
    "rin": RefSpecMapping(
        refspec_urn="urn:rkaf:us:rin:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:rin",
        pattern=_RIN_FULL_PATTERN,
    ),
    "regulation_id_numbers": RefSpecMapping(
        refspec_urn="urn:rkaf:us:rin:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:rin",
    ),
    "additional_rins": RefSpecMapping(
        refspec_urn="urn:rkaf:us:rin:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:rin",
    ),
    "regulations_dot_gov_regulation_id_number": RefSpecMapping(
        refspec_urn="urn:rkaf:us:rin:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:rin",
        pattern=_RIN_FULL_PATTERN,
    ),
    "agenda_item_id": RefSpecMapping(
        refspec_urn="urn:rkaf:us:rin:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:agenda-item-id",
        pattern=_AGENDA_ITEM_RIN_PATTERN,
    ),
    "proceeding_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:rulemaking-id",
    ),
    "docket_id": RefSpecMapping(
        refspec_urn="urn:rkaf:us:regsgov:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:docket-id",
    ),
    "docket_ids": RefSpecMapping(
        refspec_urn="urn:rkaf:us:regsgov:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:docket-id",
    ),
    "regulations_dot_gov_docket_id": RefSpecMapping(
        refspec_urn="urn:rkaf:us:regsgov:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:docket-id",
    ),
    "regulations_dot_gov_document_id": RefSpecMapping(
        refspec_urn="urn:rkaf:us:regsgov:",
        refspec_facet="urn:ref:facet:administrative-process-stage",
        ddk_concept="urn:ddk:regulatory:concept:document-id",
    ),
    "fr_doc_num": RefSpecMapping(
        refspec_urn="urn:rkaf:us:frdoc:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:regulatory:concept:federal-register-document-number",
    ),
    "agency_code": RefSpecMapping(
        refspec_urn="urn:ref:regulations-gov-agency:",
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:regulatory:concept:agency-code",
        authority_description="RefSpec REF-038 Regulations.gov agency projection",
    ),
    "regulations_dot_gov_agency_id": RefSpecMapping(
        refspec_urn="urn:ref:regulations-gov-agency:",
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:regulatory:concept:agency-code",
        authority_description="RefSpec REF-038 Regulations.gov agency projection",
    ),
    "cfr_references": RefSpecMapping(
        refspec_urn="urn:rkaf:us:cfr:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:regulatory:concept:cfr-reference",
    ),
    "cfr_citations": RefSpecMapping(
        refspec_urn="urn:rkaf:us:cfr:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:regulatory:concept:cfr-reference",
    ),
    "cfr_uris": RefSpecMapping(
        refspec_urn="urn:rkaf:us:cfr:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:regulatory:concept:cfr-reference",
    ),
    "cfr_ref": RefSpecMapping(
        refspec_urn="urn:rkaf:us:cfr:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:regulatory:concept:cfr-reference",
    ),
    "usc_key": RefSpecMapping(
        refspec_urn="urn:rkaf:us:usc:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legal:concept:usc-section",
    ),
    "usc_section_key": RefSpecMapping(
        refspec_urn="urn:rkaf:us:usc:",
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:legal:concept:usc-section",
    ),
    "package_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:gpo:concept:package-id",
    ),
    "uei": RefSpecMapping(
        refspec_urn="urn:ref:atlas-resource-scheme:uei-authority",
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:entity:concept:uei",
        pattern=r"^[A-Z0-9]{12}$",
    ),
    "ein": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:entity:concept:ein",
        pattern=r"^[0-9]{9}$",
    ),
    "cik": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:corporate:concept:sec-cik",
        pattern=r"^[0-9]{10}$",
    ),
    "npi": RefSpecMapping(
        refspec_urn="urn:ref:atlas-resource-scheme:nppes-npi-authority",
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:healthcare:concept:npi",
        pattern=r"^[0-9]{10}$",
    ),
    "fec_ids": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:fec:concept:candidate-id",
    ),
    "reporting_committee_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:fec:concept:committee-id",
        pattern=r"^C[0-9]{8}$",
    ),
    "candidate_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:fec:concept:candidate-id",
    ),
    "candidate_native_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:fec:concept:candidate-id",
    ),
    "cluster_id": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:legal-location",
        ddk_concept="urn:ddk:judicial:concept:courtlistener-cluster-id",
    ),
    "county_geoid": RefSpecMapping(
        refspec_urn=None,
        refspec_facet="urn:ref:facet:entity",
        ddk_concept="urn:ddk:geography:concept:county-geoid",
    ),
}


def canonical_json_bytes(document: Any) -> bytes:
    """Serialize any JSON structure to one deterministic, byte-reproducible UTF-8 representation."""
    return json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"


def _sha256_bytes(data: bytes) -> str:
    return sha256(data).hexdigest()


def _stable_id(raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", raw).strip("_")
    return cleaned or "id"


def _validate_family_slug(family: str) -> str:
    if not isinstance(family, str) or not _FAMILY_SLUG_RE.fullmatch(family):
        raise ODCSContractError(
            f"Invalid family slug {family!r}; expected lowercase alphanumeric and hyphens matching ^[a-z0-9][a-z0-9-]*$"
        )
    return family


def _safe_contained_path(base_dir: Path, rel_path_str: str, *, label: str) -> Path:
    """Resolve a relative POSIX path strictly within ``base_dir``, rejecting traversal and symlinks."""
    if not isinstance(rel_path_str, str) or not rel_path_str or "\\" in rel_path_str:
        raise ODCSContractError(f"{label} must be a non-empty relative POSIX path, got {rel_path_str!r}")
    rel = PurePosixPath(rel_path_str)
    if rel.is_absolute() or any(part in ("", ".", "..") for part in rel.parts):
        raise ODCSContractError(f"{label} escapes containment or contains unsafe segments: {rel_path_str!r}")
    if base_dir.is_symlink():
        raise ODCSContractError(f"ODCS contracts directory must not be a symlink: {base_dir}")
    resolved_base = base_dir.resolve()
    current = base_dir
    for part in rel.parts:
        current = current / part
        if current.is_symlink():
            raise ODCSContractError(f"{label} must not traverse or point to a symlink: {current}")
    resolved_target = current.resolve(strict=False)
    if not resolved_target.is_relative_to(resolved_base):
        raise ODCSContractError(f"{label} resolves outside {resolved_base}: {rel_path_str!r}")
    return current


def family_for_table(table: str) -> str | None:
    """Return the canonical publication family owning ``table``, or ``None`` if virtual/unassigned."""
    return _TABLE_TO_FAMILY.get(table)


def domain_for_family(family: str, *, table_metadata: Mapping[str, Any] | None = None) -> tuple[str, tuple[str, ...]]:
    """Return ``(primary_ddk_domain, secondary_ddk_domains)`` for a publication family."""
    if family in FAMILY_DOMAIN_ASSIGNMENTS:
        return FAMILY_DOMAIN_ASSIGNMENTS[family]
    if table_metadata:
        subjects = {
            table_metadata[t].get("subject")
            for t in FAMILY_TABLES.get(family, ())
            if t in table_metadata and table_metadata[t].get("subject")
        }
        if subjects == {"congress"}:
            return ("spicycongress", ())
        if subjects == {"reference"}:
            return ("RefSpec", ())
    return ("spicygov", ())


def resolve_refspec_mapping(
    table_name: str,
    column_name: str,
    table_meta: Mapping[str, Any],
) -> tuple[RefSpecMapping | None, dict[str, Any] | None]:
    """Return ``(RefSpecMapping | None, documented_values_entry | None)`` for one column."""
    subject = table_meta.get("subject")
    documented = (table_meta.get("documented_values") or {}).get(column_name)

    if column_name in _UNCONDITIONAL_REFSPEC:
        return _UNCONDITIONAL_REFSPEC[column_name], documented

    # Context-sensitive identifier / agency / vocabulary mappings.
    if column_name in ("system_code", "parent_system_code", "committee_system_code") and subject in _CONGRESS_SUBJECTS:
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:legislative:concept:committee-code",
            ),
            documented,
        )
    if column_name in ("event_id", "mods_event_id") and subject in _CONGRESS_SUBJECTS:
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:administrative-process-stage",
                ddk_concept="urn:ddk:legislative:concept:event-id",
            ),
            documented,
        )
    if column_name == "document_number" and (
        table_name in ("federal_register", "fr_docket_links", "executive_order_observations") or subject == "rulemaking"
    ):
        return (
            RefSpecMapping(
                refspec_urn="urn:rkaf:us:frdoc:",
                refspec_facet="urn:ref:facet:legal-location",
                ddk_concept="urn:ddk:regulatory:concept:federal-register-document-number",
            ),
            documented,
        )
    # urn:ref:federal-register-agency: applies ONLY to scalar numeric Federal Register agency ID columns,
    # never to agency_slugs or nested agencies struct arrays.
    if column_name == "fr_agency_id":
        return (
            RefSpecMapping(
                refspec_urn="urn:ref:federal-register-agency:",
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:regulatory:concept:federal-register-agency",
                authority_description="RefSpec REF-038 / REF-072 Federal Register agency registry",
            ),
            documented,
        )
    if column_name in ("agencies", "agency_slugs") and table_name == "federal_register":
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:regulatory:concept:federal-register-agency",
                ddk_concept_kind="attribute",
            ),
            documented,
        )
    if column_name == "topics" and table_name == "federal_register":
        return (
            RefSpecMapping(
                refspec_urn="urn:ref:atlas-resource-scheme:federal-register-api-topics",
                refspec_facet="urn:ref:facet:general-subject",
                ddk_concept="urn:ddk:regulatory:concept:federal-register-topic",
                ddk_concept_kind="attribute",
                authority_description="RefSpec Atlas Federal Register API topics scheme",
            ),
            documented,
        )
    if column_name in ("subjects", "policy_area") and subject == "congress":
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:general-subject",
                ddk_concept="urn:ddk:legislative:concept:crs-subject",
                ddk_concept_kind="attribute",
            ),
            documented,
        )
    if (
        column_name in ("document_id", "proposal_document_id", "final_document_id", "withdrawal_document_id")
        and subject == "rulemaking"
    ):
        return (
            RefSpecMapping(
                refspec_urn="urn:rkaf:us:regsgov:",
                refspec_facet="urn:ref:facet:administrative-process-stage",
                ddk_concept="urn:ddk:regulatory:concept:document-id",
            ),
            documented,
        )
    if column_name == "comment_id" and subject == "rulemaking":
        return (
            RefSpecMapping(
                refspec_urn="urn:rkaf:us:regsgov:",
                refspec_facet="urn:ref:facet:administrative-process-stage",
                ddk_concept="urn:ddk:regulatory:concept:comment-id",
            ),
            documented,
        )
    if column_name == "committee_id" and (subject in _FEC_SUBJECTS or table_name == "org_committee_links"):
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:fec:concept:committee-id",
                pattern=r"^C[0-9]{8}$",
            ),
            documented,
        )
    if column_name == "registrant_id" and subject in _LOBBYING_SUBJECTS:
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:lobbying:concept:lda-registrant-id",
            ),
            documented,
        )
    if column_name == "client_id" and subject in _LOBBYING_SUBJECTS:
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:lobbying:concept:lda-client-id",
            ),
            documented,
        )
    if column_name == "court_id" and subject in _COURT_SUBJECTS:
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:judicial:concept:courtlistener-court-id",
            ),
            documented,
        )
    if column_name in ("provider_number", "ccn") and table_name.startswith("hospital_cost_report"):
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:entity",
                ddk_concept="urn:ddk:healthcare:concept:ccn",
                authority_description="RefSpec CMS Certification Number definition",
            ),
            documented,
        )
    if documented and isinstance(documented.get("domain"), str):
        domain_slug = documented["domain"]
        return (
            RefSpecMapping(
                refspec_urn=None,
                refspec_facet="urn:ref:facet:code-list-value",
                ddk_concept=f"urn:ddk:common:concept:{domain_slug}",
                ddk_concept_kind="attribute",
                authority_url=documented.get("source_url"),
                authority_description=documented.get("locator"),
            ),
            documented,
        )
    return None, documented


def _normalize_arrow_type(dtype: pa.DataType) -> pa.DataType:
    """Normalize Arrow list element names to Parquet's canonical ``element`` field."""
    if pa.types.is_list(dtype):
        return pa.list_(
            pa.field("element", _normalize_arrow_type(dtype.value_type), nullable=dtype.value_field.nullable)
        )
    if pa.types.is_struct(dtype):
        return pa.struct([pa.field(f.name, _normalize_arrow_type(f.type), nullable=f.nullable) for f in dtype])
    return dtype


def arrow_type_to_odcs_property(
    dtype: pa.DataType,
    *,
    name: str | None = None,
    nullable: bool = True,
    duckdb_type: str | None = None,
) -> dict[str, Any]:
    """Convert a PyArrow DataType into an ODCS v3.2.0 property/item dict.

    Refuses Arrow types that cannot round-trip losslessly with ODCS v3.2.0 and DDK
    (such as ``large_list``, ``date64``, ``time32``, non-``us`` ``timestamp``/``time64``,
    and ``string_view``) instead of silently coercing them.

    Produces declarations compatible simultaneously with:
    1. Official ODCS v3.2.0 JSON Schema,
    2. ``ddk.publication.verify_parquet_schema_parity`` (``odcs_schema_to_pyarrow``), and
    3. ``ddk.arrow_mapping.map_declaration(..., profile='urn:ddk:odcs:arrow-declaration:3')``.
    """
    prop: dict[str, Any] = {}
    if name is not None:
        prop["name"] = name

    if hasattr(pa.types, "is_string_view") and pa.types.is_string_view(dtype):
        raise ODCSContractError(f"Arrow string_view type cannot round-trip losslessly to ODCS v3.2.0: {dtype!r}")
    if pa.types.is_large_list(dtype):
        raise ODCSContractError(f"Arrow large_list type cannot round-trip losslessly to ODCS v3.2.0: {dtype!r}")
    if pa.types.is_date64(dtype):
        raise ODCSContractError(f"Arrow date64 type cannot round-trip losslessly to ODCS v3.2.0: {dtype!r}")
    if pa.types.is_time32(dtype):
        raise ODCSContractError(f"Arrow time32 type cannot round-trip losslessly to ODCS v3.2.0: {dtype!r}")

    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        prop["logicalType"] = "string"
        prop["physicalType"] = "LargeUtf8" if pa.types.is_large_string(dtype) else "Utf8"
    elif pa.types.is_boolean(dtype):
        prop["logicalType"] = "boolean"
        prop["physicalType"] = "Boolean"
    elif pa.types.is_date32(dtype):
        prop["logicalType"] = "date"
        prop["logicalTypeOptions"] = {"format": "yyyy-MM-dd"}
        prop["physicalType"] = "Date32"
    elif pa.types.is_time64(dtype):
        if dtype.unit != "us":
            raise ODCSContractError(
                f"Arrow time64 unit {dtype.unit!r} cannot round-trip losslessly through DDK (only 'us' is lossless)"
            )
        prop["logicalType"] = "time"
        prop["logicalTypeOptions"] = {"format": "HH:mm:ss.SSSSSS", "timezone": False}
    elif pa.types.is_timestamp(dtype):
        if dtype.unit != "us":
            raise ODCSContractError(
                f"Arrow timestamp unit {dtype.unit!r} cannot round-trip losslessly through DDK (only 'us' is lossless)"
            )
        if dtype.tz:
            prop["logicalType"] = "timestamp"
            prop["logicalTypeOptions"] = {
                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
                "timezone": True,
                "defaultTimezone": str(dtype.tz),
            }
        else:
            prop["logicalType"] = "timestamp"
            prop["logicalTypeOptions"] = {
                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                "timezone": False,
            }
    elif pa.types.is_int8(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "i8"}
        prop["physicalType"] = "Int8"
    elif pa.types.is_int16(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "i16"}
        prop["physicalType"] = "Int16"
    elif pa.types.is_int32(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "i32"}
        prop["physicalType"] = "Int32"
    elif pa.types.is_int64(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "i64"}
        prop["physicalType"] = "Int64"
    elif pa.types.is_uint8(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "u8"}
        prop["physicalType"] = "UInt8"
    elif pa.types.is_uint16(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "u16"}
        prop["physicalType"] = "UInt16"
    elif pa.types.is_uint32(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "u32"}
        prop["physicalType"] = "UInt32"
    elif pa.types.is_uint64(dtype):
        prop["logicalType"] = "integer"
        prop["logicalTypeOptions"] = {"format": "u64"}
        prop["physicalType"] = "UInt64"
    elif pa.types.is_float32(dtype):
        prop["logicalType"] = "number"
        prop["logicalTypeOptions"] = {"format": "f32"}
        prop["physicalType"] = "Float32"
    elif pa.types.is_float64(dtype):
        prop["logicalType"] = "number"
        prop["logicalTypeOptions"] = {"format": "f64"}
        prop["physicalType"] = "Float64"
    elif pa.types.is_decimal128(dtype):
        prop["logicalType"] = "number"
        prop["physicalType"] = f"decimal128({dtype.precision},{dtype.scale})"
    elif pa.types.is_decimal256(dtype):
        prop["logicalType"] = "number"
        prop["physicalType"] = f"decimal256({dtype.precision},{dtype.scale})"
    elif pa.types.is_binary(dtype):
        prop["logicalType"] = "string"
        prop["logicalTypeOptions"] = {"format": "binary"}
        prop["physicalType"] = "binary"
    elif pa.types.is_large_binary(dtype):
        prop["logicalType"] = "string"
        prop["logicalTypeOptions"] = {"format": "binary"}
        prop["physicalType"] = "large_binary"
    elif pa.types.is_fixed_size_binary(dtype):
        prop["logicalType"] = "string"
        prop["logicalTypeOptions"] = {"format": "binary"}
        prop["physicalType"] = f"fixed[{dtype.byte_width}]"
    elif pa.types.is_list(dtype):
        prop["logicalType"] = "array"
        prop["items"] = arrow_type_to_odcs_property(
            dtype.value_type,
            name=None,
            nullable=dtype.value_field.nullable,
        )
    elif pa.types.is_struct(dtype):
        prop["logicalType"] = "object"
        prop["properties"] = [
            arrow_type_to_odcs_property(child.type, name=child.name, nullable=child.nullable) for child in dtype
        ]
    else:
        raise ODCSContractError(f"Unsupported Arrow data type for ODCS v3.2.0 export: {dtype!r}")

    if not nullable:
        prop["required"] = True

    if duckdb_type is not None:
        prop["customProperties"] = [
            {"vendor": "spicy-regs", "property": "duckdbType", "value": duckdb_type},
        ]
    return prop


def odcs_property_to_pyarrow(prop: Mapping[str, Any]) -> pa.DataType:
    """Convert an ODCS v3.2.0 property/item dict to its exact PyArrow DataType."""
    name = prop.get("name", "<item>")
    logical = prop.get("logicalType")
    physical = str(prop.get("physicalType") or "").strip()
    physical_upper = physical.upper()
    opts = prop.get("logicalTypeOptions") if isinstance(prop.get("logicalTypeOptions"), Mapping) else {}

    if logical is None and prop.get("properties"):
        logical = "object"

    try:
        if logical == "string":
            if opts.get("format") == "binary":
                if physical == "large_binary":
                    return pa.large_binary()
                if physical == "binary":
                    return pa.binary()
                match = _FIXED_BIN_RE.match(physical)
                if match:
                    width = int(match.group(1))
                    if width <= 0:
                        raise ODCSContractError(f"Field {name!r} fixed binary width must be positive, got {width}")
                    return pa.binary(width)
                return pa.binary()
            if physical == "LargeUtf8":
                return pa.large_string()
            return pa.string()
        if logical == "boolean":
            return pa.bool_()
        if logical == "date":
            return pa.date32()
        if logical == "time":
            fmt = str(opts.get("format") or "")
            return pa.time64("ns" if ".SSSSSSSSS" in fmt else "us")
        if logical == "timestamp":
            fmt = str(opts.get("format") or "")
            unit = "ns" if ".SSSSSSSSS" in fmt else "us"
            tz_opt = opts.get("timezone")
            if tz_opt is False or (
                tz_opt is not True and physical_upper in {"TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP WITHOUT TIME ZONE"}
            ):
                return pa.timestamp(unit, tz=None)
            default_tz = opts.get("defaultTimezone")
            tz = default_tz if isinstance(default_tz, str) and default_tz else "UTC"
            return pa.timestamp(unit, tz=tz)
        if logical == "integer":
            fmt = str(opts.get("format") or "").lower()
            if fmt == "i8" or physical_upper in {"TINYINT", "INT8"}:
                return pa.int8()
            if fmt == "i16" or physical_upper in {"SMALLINT", "INT16", "SHORT"}:
                return pa.int16()
            if fmt == "i32" or physical_upper in {"INTEGER", "INT", "INT32"}:
                return pa.int32()
            if fmt == "u8" or physical_upper in {"UTINYINT", "UINT8"}:
                return pa.uint8()
            if fmt == "u16" or physical_upper in {"USMALLINT", "UINT16"}:
                return pa.uint16()
            if fmt == "u32" or physical_upper in {"UINTEGER", "UINT32"}:
                return pa.uint32()
            if fmt == "u64" or physical_upper in {"UBIGINT", "UINT64"}:
                return pa.uint64()
            return pa.int64()
        if logical == "number":
            match = _DECIMAL_RE.match(physical)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                if physical.lower().startswith("decimal256") or precision > 38:
                    return pa.decimal256(precision, scale)
                return pa.decimal128(precision, scale)
            fmt = str(opts.get("format") or "").lower()
            if fmt == "f32" or physical_upper in {"FLOAT", "REAL", "FLOAT32"}:
                return pa.float32()
            return pa.float64()
        if logical == "object":
            children = prop.get("properties")
            if not isinstance(children, list) or not children:
                raise ODCSContractError(f"ODCS object property {name!r} has no declared child properties")
            return pa.struct([odcs_field_to_pyarrow(child) for child in children])
        if logical == "array":
            items = prop.get("items")
            if isinstance(items, Mapping) and (items.get("logicalType") or items.get("properties")):
                elem_type = odcs_property_to_pyarrow(items)
                elem_nullable = not bool(items.get("required", False))
                return pa.list_(pa.field("element", elem_type, nullable=elem_nullable))
            raise ODCSContractError(f"ODCS array property {name!r} has no declared items type")
    except (pa.ArrowException, ValueError, KeyError, TypeError) as exc:
        if isinstance(exc, ODCSContractError):
            raise
        raise ODCSContractError(f"Invalid Arrow type specification for ODCS field {name!r}: {exc}") from exc
    raise ODCSContractError(f"ODCS property {name!r} uses unsupported logicalType {logical!r}")


def odcs_field_to_pyarrow(prop: Mapping[str, Any]) -> pa.Field:
    """Convert an ODCS property dict into a PyArrow Field."""
    name = prop.get("name")
    if not isinstance(name, str) or not name:
        raise ODCSContractError("Every ODCS property requires a non-empty string name")
    return pa.field(name, odcs_property_to_pyarrow(prop), nullable=not bool(prop.get("required", False)))


def odcs_schema_to_pyarrow(schema_obj: Mapping[str, Any]) -> pa.Schema:
    """Convert an ODCS v3.2.0 SchemaObject dict into a PyArrow Schema."""
    props = schema_obj.get("properties") if isinstance(schema_obj, Mapping) else None
    if not isinstance(props, list) or not props:
        raise ODCSContractError("ODCS schema object declares no properties")
    return pa.schema([odcs_field_to_pyarrow(prop) for prop in props])


def contract_declared_schemas(contract: Mapping[str, Any]) -> dict[str, pa.Schema]:
    """Extract ``{table_name: pa.Schema}`` from an ODCS v3.2.0 DataContract dict."""
    schema_objs = contract.get("schema")
    if not isinstance(schema_objs, list) or not schema_objs:
        raise ODCSContractError("ODCS DataContract declares no schema objects")
    declared: dict[str, pa.Schema] = {}
    for obj in schema_objs:
        name = obj.get("name") if isinstance(obj, Mapping) else None
        if not isinstance(name, str) or not name:
            raise ODCSContractError("ODCS schema object missing non-empty 'name'")
        if name in declared:
            raise ODCSContractError(f"ODCS DataContract repeats schema object {name!r}")
        declared[name] = odcs_schema_to_pyarrow(obj)
    return declared


def _resolve_expected_arrow_schema(expected: Any) -> pa.Schema:
    """Normalize a PyArrow Schema, ODCS SchemaObject dict, or DDK Arrow mapping into ``pa.Schema``."""
    if isinstance(expected, pa.Schema):
        return expected
    if isinstance(expected, Mapping):
        if "properties" in expected:
            return odcs_schema_to_pyarrow(expected)
        if "fields" in expected:
            return _ddk_fields_to_pyarrow(expected)
    raise ODCSContractError(
        "Expected declared schema must be a pyarrow.Schema, ODCS SchemaObject dict ('properties'), "
        "or DDK Arrow schema dict ('fields')"
    )


def _ddk_fields_to_pyarrow(schema_dict: Mapping[str, Any]) -> pa.Schema:
    aliases = {
        "Utf8": pa.string(),
        "LargeUtf8": pa.large_string(),
        "Boolean": pa.bool_(),
        "Date32": pa.date32(),
        "Binary": pa.binary(),
        "LargeBinary": pa.large_binary(),
        "Float32": pa.float32(),
        "Float64": pa.float64(),
        "Int8": pa.int8(),
        "Int16": pa.int16(),
        "Int32": pa.int32(),
        "Int64": pa.int64(),
        "UInt8": pa.uint8(),
        "UInt16": pa.uint16(),
        "UInt32": pa.uint32(),
        "UInt64": pa.uint64(),
    }

    def dtype(value: Any) -> pa.DataType:
        if isinstance(value, str):
            if value not in aliases:
                raise ODCSContractError(f"Unsupported DDK Arrow alias {value!r}")
            return aliases[value]
        if isinstance(value, Mapping) and len(value) == 1:
            kind, detail = next(iter(value.items()))
            try:
                if kind == "Struct":
                    return pa.struct([field(child) for child in detail])
                if kind == "List":
                    return pa.list_(field(detail))
                if kind == "FixedSizeBinary":
                    return pa.binary(int(detail))
                if kind == "Decimal128":
                    return pa.decimal128(int(detail[0]), int(detail[1]))
                if kind == "Decimal256":
                    return pa.decimal256(int(detail[0]), int(detail[1]))
                units = {"Microsecond": "us", "Nanosecond": "ns"}
                if kind == "Time64":
                    return pa.time64(units[detail])
                if kind == "Timestamp":
                    return pa.timestamp(units[detail[0]], detail[1])
            except (pa.ArrowException, KeyError, ValueError, TypeError, IndexError) as exc:
                if isinstance(exc, ODCSContractError):
                    raise
                raise ODCSContractError(f"Invalid DDK Arrow type declaration {value!r}: {exc}") from exc
        raise ODCSContractError(f"Unsupported DDK Arrow type declaration {value!r}")

    def field(value: Mapping[str, Any]) -> pa.Field:
        if not isinstance(value, Mapping) or "name" not in value or "data_type" not in value:
            raise ODCSContractError(f"Invalid DDK Arrow field descriptor: {value!r}")
        return pa.field(str(value["name"]), dtype(value["data_type"]), nullable=bool(value.get("nullable", True)))

    fields = schema_dict.get("fields")
    if not isinstance(fields, list):
        raise ODCSContractError("DDK Arrow schema dict 'fields' must be a list")
    return pa.schema([field(item) for item in fields])


def _resolve_actual_arrow_schema(actual: Any) -> pa.Schema:
    """Read an actual ``pa.Schema`` from a PyArrow Schema, Parquet path, Parquet bytes, or Arrow IPC bytes."""
    if isinstance(actual, pa.Schema):
        return actual
    if isinstance(actual, (str, Path)):
        path = Path(actual)
        if not path.is_file():
            raise ODCSContractError(f"Parquet file missing for schema parity check: {path}")
        try:
            return pq.read_schema(path)
        except Exception as exc:
            raise ODCSContractError(f"Could not read Parquet schema from {path}: {exc}") from exc
    if isinstance(actual, (bytes, bytearray, memoryview)):
        raw = bytes(actual)
        try:
            if raw[:4] == b"PAR1":
                return pq.read_schema(pa.BufferReader(raw))
            return pa.ipc.read_schema(pa.py_buffer(raw))
        except Exception as exc:
            raise ODCSContractError(f"Could not read Arrow/Parquet schema from bytes: {exc}") from exc
    raise ODCSContractError(f"Unsupported actual schema input type: {type(actual).__name__}")


def _schema_fingerprint(schema: pa.Schema) -> str:
    canonical = [[f.name, str(f.type), bool(f.nullable)] for f in schema]
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(payload).hexdigest()


def parquet_schema_diff(
    expected: Any,
    actual: Any,
    *,
    check_order: bool = True,
    check_nullability: bool = False,
) -> list[str]:
    """Compare an expected schema contract against an actual Parquet/Arrow schema.

    Interoperates identically with ``ddk.publication.parquet_schema_diff``.
    """
    exp_schema = _resolve_expected_arrow_schema(expected)
    act_schema = _resolve_actual_arrow_schema(actual)

    expected_fields = {f.name: f for f in exp_schema}
    actual_fields = {f.name: f for f in act_schema}
    issues: list[str] = []

    if missing := [f.name for f in exp_schema if f.name not in actual_fields]:
        issues.append(f"missing columns {missing}")
    if extra := [f.name for f in act_schema if f.name not in expected_fields]:
        issues.append(f"unexpected columns {extra}")
    if mismatched := [
        f"{f.name} (expected {expected_fields[f.name].type}, got {f.type})"
        for f in act_schema
        if f.name in expected_fields and f.type != expected_fields[f.name].type
    ]:
        issues.append(f"type mismatch on {mismatched}")
    if check_nullability:
        if null_diffs := [
            f"{f.name} (expected nullable={expected_fields[f.name].nullable}, got {f.nullable})"
            for f in act_schema
            if f.name in expected_fields and f.nullable != expected_fields[f.name].nullable
        ]:
            issues.append(f"nullability mismatch on {null_diffs}")
    if not issues and check_order and list(act_schema.names) != list(exp_schema.names):
        issues.append(f"column order differs (expected {list(exp_schema.names)}, got {list(act_schema.names)})")
    return issues


def _normalize_schema_table_map(items: Mapping[str, Any], kind: str) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for raw_name, value in items.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise ODCSContractError(f"{kind} table name must be a non-empty string")
        table_name = raw_name.removesuffix(".parquet")
        if table_name in normalized:
            raise ODCSContractError(f"Duplicate {kind} table entry after .parquet normalization: {table_name!r}")
        normalized[table_name] = value
    return normalized


def _expand_schema_candidate_items(table_name: str, candidate: Any) -> list[tuple[str, Any]]:
    if isinstance(candidate, (str, Path)):
        path = Path(candidate)
        if path.is_dir():
            files = sorted(path.rglob("*.parquet"))
            if not files:
                raise ODCSContractError(
                    f"Partitioned table {table_name!r} directory {path} contains no Parquet member files"
                )
            return [(p.relative_to(path).as_posix(), p) for p in files]
        if not path.is_file():
            raise ODCSContractError(f"Table {table_name!r} Parquet file is missing: {path}")
        return [(path.name, path)]
    if isinstance(candidate, (bytes, bytearray, memoryview)):
        return [(f"{table_name}[0]", bytes(candidate))]
    if isinstance(candidate, Sequence):
        items = list(candidate)
        if not items:
            raise ODCSContractError(f"Table {table_name!r} has an empty Parquet candidate sequence")
        return [
            (str(item) if isinstance(item, (str, Path)) else f"{table_name}[{idx}]", item)
            for idx, item in enumerate(items)
        ]
    return [(table_name, candidate)]


def verify_parquet_schema_parity(
    declared_schemas: Mapping[str, Any],
    candidates: Mapping[str, Any],
    *,
    check_order: bool = True,
    check_nullability: bool = False,
    allow_subset_tables: bool = False,
) -> dict[str, dict[str, Any]]:
    """Verify every candidate table/shard strictly matches its declared Arrow or ODCS schema.

    Matches ``ddk.publication.verify_parquet_schema_parity`` semantics and return shape.
    """
    if not isinstance(declared_schemas, Mapping) or not isinstance(candidates, Mapping):
        raise ODCSContractError("declared_schemas and candidates must both be mappings")

    norm_declared = _normalize_schema_table_map(declared_schemas, "declared")
    norm_candidates = _normalize_schema_table_map(candidates, "candidate")

    if unexpected := sorted(set(norm_candidates) - set(norm_declared)):
        raise ODCSContractError(
            f"Candidate generation declares unexpected table(s) {unexpected!r}; "
            f"declared tables are {sorted(norm_declared)}"
        )
    if not allow_subset_tables:
        if missing_tables := sorted(set(norm_declared) - set(norm_candidates)):
            raise ODCSContractError(f"Candidate generation missing declared table(s): {missing_tables!r}")

    summary: dict[str, dict[str, Any]] = {}
    for table_name in sorted(norm_candidates):
        expected_schema = _resolve_expected_arrow_schema(norm_declared[table_name])
        items = _expand_schema_candidate_items(table_name, norm_candidates[table_name])
        for label, item in items:
            actual_schema = _resolve_actual_arrow_schema(item)
            if issues := parquet_schema_diff(
                expected_schema,
                actual_schema,
                check_order=check_order,
                check_nullability=check_nullability,
            ):
                raise ODCSContractError(
                    f"Schema parity check failed for {table_name!r} ({label}): " + "; ".join(issues)
                )
        summary[table_name] = {
            "files_checked": len(items),
            "columns": list(expected_schema.names),
            "schema_sha256": _schema_fingerprint(expected_schema),
        }
    return summary


def load_table_metadata(path: Path = DEFAULT_TABLE_METADATA_PATH) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    return json.loads(raw), _sha256_bytes(raw)


def load_table_joins(path: Path = DEFAULT_TABLE_JOINS_PATH) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    return json.loads(raw), _sha256_bytes(raw)


def _metadata_arrow_schema(table_meta: Mapping[str, Any]) -> pa.Schema:
    fields: list[pa.Field] = []
    for col in table_meta["columns"]:
        dtype = _normalize_arrow_type(parse_duckdb_arrow_type(col["column_type"]))
        fields.append(pa.field(col["column_name"], dtype, nullable=True))
    return pa.schema(fields)


def _build_table_relationships(
    table_name: str,
    joins: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build ODCS v3.2.0 RelationshipSchemaLevel entries from ``table_joins.json`` for ``table_name``."""
    relationships: list[dict[str, Any]] = []
    seen_ids: dict[str, int] = {}
    table_joins = [j for j in joins if j.get("child") == table_name]
    table_joins.sort(
        key=lambda j: (
            str(j.get("parent", "")),
            tuple(j.get("child_columns") or ()),
            tuple(j.get("parent_columns") or ()),
            str(j.get("kind", "")),
        )
    )
    for join in table_joins:
        child_cols = list(join.get("child_columns") or ())
        parent = str(join.get("parent") or "")
        parent_cols = list(join.get("parent_columns") or ())
        if not child_cols or not parent or len(child_cols) != len(parent_cols):
            continue
        base_id = _stable_id(f"fk-{table_name}-{'_'.join(child_cols)}-to-{parent}-{'_'.join(parent_cols)}")
        count = seen_ids.get(base_id, 0) + 1
        seen_ids[base_id] = count
        rel_id = base_id if count == 1 else f"{base_id}_{count}"

        from_ref: str | list[str] = (
            f"{table_name}.{child_cols[0]}" if len(child_cols) == 1 else [f"{table_name}.{col}" for col in child_cols]
        )
        to_ref: str | list[str] = (
            f"{parent}.{parent_cols[0]}" if len(parent_cols) == 1 else [f"{parent}.{col}" for col in parent_cols]
        )

        custom_props: list[dict[str, Any]] = [
            {"vendor": "spicy-regs", "property": "joinKind", "value": join.get("kind", "unmeasured")},
            {
                "vendor": "spicy-regs",
                "property": "expectedCardinality",
                "value": join.get("expected_cardinality", "unspecified"),
            },
            {"vendor": "spicy-regs", "property": "baselineKeys", "value": int(join.get("baseline_keys", 0))},
            {"vendor": "spicy-regs", "property": "baselineMissing", "value": int(join.get("baseline_missing", 0))},
        ]
        if join.get("floor_pct") is not None:
            custom_props.append({"vendor": "spicy-regs", "property": "floorPct", "value": join["floor_pct"]})
        if target_family := family_for_table(parent):
            custom_props.append({"vendor": "spicy-regs", "property": "targetFamily", "value": target_family})
        if join.get("measured_via"):
            custom_props.append({"vendor": "spicy-regs", "property": "measuredVia", "value": join["measured_via"]})
        if join.get("child_filter"):
            custom_props.append({"vendor": "spicy-regs", "property": "childFilter", "value": join["child_filter"]})
        if join.get("reason"):
            custom_props.append({"vendor": "spicy-regs", "property": "reason", "value": str(join["reason"])})

        relationships.append(
            {
                "id": rel_id,
                "type": "foreignKey",
                "from": from_ref,
                "to": to_ref,
                "customProperties": custom_props,
            }
        )
    return relationships


def build_table_schema_object(
    table_name: str,
    table_meta: Mapping[str, Any],
    *,
    joins: Sequence[Mapping[str, Any]] = (),
    verified_parquet_schema: pa.Schema | None = None,
) -> dict[str, Any]:
    """Build one ODCS v3.2.0 SchemaObject from ``table_metadata`` (+ optional verified Parquet Arrow footer)."""
    metadata_schema = _metadata_arrow_schema(table_meta)
    if verified_parquet_schema is not None:
        issues = parquet_schema_diff(metadata_schema, verified_parquet_schema, check_order=True)
        if issues:
            raise ODCSContractError(
                f"Parquet footer disagrees with table_metadata.json for {table_name!r}: " + "; ".join(issues)
            )
        effective_schema = _normalize_arrow_type_schema(verified_parquet_schema)
    else:
        effective_schema = metadata_schema

    col_meta_by_name = {col["column_name"]: col for col in table_meta["columns"]}
    identity_cols = list(table_meta.get("identity_columns") or ())
    identity_pos = {col: idx for idx, col in enumerate(identity_cols, start=1)}

    properties: list[dict[str, Any]] = []
    for field in effective_schema:
        col_meta = col_meta_by_name[field.name]
        prop = arrow_type_to_odcs_property(
            field.type,
            name=field.name,
            nullable=field.nullable,
            duckdb_type=col_meta["column_type"],
        )
        if field.name in identity_pos:
            prop["primaryKey"] = True
            prop["primaryKeyPosition"] = identity_pos[field.name]
        if description := col_meta.get("description"):
            prop["description"] = str(description)

        custom_props: list[dict[str, Any]] = list(prop.get("customProperties", ()))
        auth_defs: list[dict[str, Any]] = []

        mapping, documented = resolve_refspec_mapping(table_name, field.name, table_meta)
        if mapping is not None:
            if mapping.refspec_urn is not None:
                custom_props.append({"vendor": "refspec", "property": "urn", "value": mapping.refspec_urn})
            custom_props.extend(
                [
                    {"vendor": "refspec", "property": "facet", "value": mapping.refspec_facet},
                    {"vendor": "ddk", "property": "concept", "value": mapping.ddk_concept},
                    {"vendor": "ddk", "property": "conceptKind", "value": mapping.ddk_concept_kind},
                ]
            )
            if mapping.pattern and prop.get("logicalType") == "string":
                opts = dict(prop.get("logicalTypeOptions") or {})
                opts.setdefault("pattern", mapping.pattern)
                prop["logicalTypeOptions"] = opts
            if mapping.authority_url:
                entry: dict[str, Any] = {"type": "taxonomy", "url": mapping.authority_url}
                if mapping.authority_description:
                    entry["description"] = mapping.authority_description
                auth_defs.append(entry)
            elif mapping.authority_description:
                custom_props.append(
                    {"vendor": "refspec", "property": "authority", "value": mapping.authority_description}
                )
        if documented and isinstance(documented.get("domain"), str):
            custom_props.append({"vendor": "spicy-regs", "property": "documentedDomain", "value": documented["domain"]})
            values = [v["value"] for v in documented.get("values") or () if isinstance(v, Mapping) and "value" in v]
            if values:
                custom_props.append({"vendor": "spicy-regs", "property": "documentedValues", "value": values})

        if custom_props:
            prop["customProperties"] = custom_props
        if auth_defs:
            prop["authoritativeDefinitions"] = auth_defs
        properties.append(prop)

    schema_obj: dict[str, Any] = {
        "id": _stable_id(table_name),
        "name": table_name,
        "physicalName": f"{table_name}.parquet",
        "logicalType": "object",
        "physicalType": "parquet",
        "businessName": str(table_meta.get("label") or table_name.replace("_", " ")),
        "description": str(table_meta.get("summary") or f"Published spicy-regs table {table_name}."),
        "properties": properties,
    }
    if grain := table_meta.get("grain"):
        schema_obj["dataGranularityDescription"] = str(grain)

    tags: list[str] = []
    if subject := table_meta.get("subject"):
        tags.append(f"subject:{subject}")
    if kind := table_meta.get("kind"):
        tags.append(f"coverage:{kind}")
    if tags:
        schema_obj["tags"] = tags

    relationships = _build_table_relationships(table_name, joins)
    if relationships:
        schema_obj["relationships"] = relationships

    table_custom: list[dict[str, Any]] = [
        {"vendor": "ddk", "property": "arrowSchemaProfile", "value": DDK_ARROW_EXACT_PROFILE},
        {"vendor": "spicy-regs", "property": "arrowSchemaSha256", "value": _schema_fingerprint(effective_schema)},
        {
            "vendor": "spicy-regs",
            "property": "parquetFooterVerified",
            "value": verified_parquet_schema is not None,
        },
    ]
    if identity_cols:
        table_custom.append({"vendor": "ddk", "property": "rowIdentity", "value": identity_cols})
    schema_obj["customProperties"] = table_custom
    return schema_obj


def _normalize_arrow_type_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([pa.field(f.name, _normalize_arrow_type(f.type), nullable=f.nullable) for f in schema])


def build_family_contract(
    family: str,
    table_names: Sequence[str],
    table_metadata: Mapping[str, Any],
    *,
    joins: Sequence[Mapping[str, Any]] = (),
    source_metadata_sha256: str | None = None,
    source_joins_sha256: str | None = None,
    verified_parquet_schemas: Mapping[str, pa.Schema] | None = None,
) -> dict[str, Any]:
    """Generate one deterministic ODCS v3.2.0 DataContract document for ``family``."""
    _validate_family_slug(family)
    ordered_tables = sorted(dict.fromkeys(table_names))
    if not ordered_tables:
        raise ODCSContractError(f"Family {family!r} has no declared tables for ODCS contract export")

    missing_meta = [t for t in ordered_tables if t not in table_metadata]
    if missing_meta:
        raise ODCSContractError(
            f"Family {family!r} references table(s) absent from table_metadata.json: {missing_meta}"
        )

    primary_domain, secondary_domains = domain_for_family(family, table_metadata=table_metadata)
    is_pilot = family in PILOT_FAMILIES
    subjects = sorted({str(table_metadata[t]["subject"]) for t in ordered_tables if table_metadata[t].get("subject")})

    schema_objects = [
        build_table_schema_object(
            t,
            table_metadata[t],
            joins=joins,
            verified_parquet_schema=(verified_parquet_schemas or {}).get(t),
        )
        for t in ordered_tables
    ]

    tags = [
        f"ddk-domain:{primary_domain}",
        *(f"ddk-secondary-domain:{sec}" for sec in secondary_domains),
        f"family:{family}",
        *(["pilot-wave"] if is_pilot else []),
        *(f"subject:{subj}" for subj in subjects),
    ]

    custom_properties: list[dict[str, Any]] = [
        {"vendor": "ddk", "property": "targetDomain", "value": primary_domain},
        {"vendor": "ddk", "property": "secondaryDomains", "value": list(secondary_domains)},
        {"vendor": "ddk", "property": "arrowSchemaProfile", "value": DDK_ARROW_EXACT_PROFILE},
        {"vendor": "spicy-regs", "property": "family", "value": family},
        {"vendor": "spicy-regs", "property": "pilotWave", "value": is_pilot},
    ]
    if source_metadata_sha256:
        custom_properties.append(
            {"vendor": "spicy-regs", "property": "tableMetadataSha256", "value": source_metadata_sha256}
        )
    if source_joins_sha256:
        custom_properties.append({"vendor": "spicy-regs", "property": "tableJoinsSha256", "value": source_joins_sha256})

    contract: dict[str, Any] = {
        "apiVersion": ODCS_API_VERSION,
        "kind": "DataContract",
        "id": f"urn:spicy-regs:odcs:family:{family}",
        "version": ODCS_CONTRACT_VERSION,
        "name": f"spicy-regs {family}",
        "status": "active",
        "domain": primary_domain,
        "dataProduct": primary_domain,
        "description": {
            "purpose": (
                f"Deterministic ODCS v3.2.0 DataContract for spicy-regs publication family '{family}' "
                f"({len(ordered_tables)} table(s)), targeting DDK domain '{primary_domain}'."
            ),
            "usage": "Verify Parquet Arrow schema parity before publishing or consuming generations.",
        },
        "tags": tags,
        "schema": schema_objects,
        "customProperties": custom_properties,
    }
    return contract


def validate_odcs_v320_contract(
    contract: Mapping[str, Any],
    *,
    json_schema_path: Path | None = None,
) -> list[str]:
    """Validate an ODCS v3.2.0 DataContract structurally (and optionally against ``odcs-json-schema.json``)."""
    errors: list[str] = []
    if not isinstance(contract, Mapping):
        return ["Contract must be a JSON object"]

    if contract.get("apiVersion") != ODCS_API_VERSION:
        errors.append(f"apiVersion must be {ODCS_API_VERSION!r}, got {contract.get('apiVersion')!r}")
    if contract.get("kind") != "DataContract":
        errors.append(f"kind must be 'DataContract', got {contract.get('kind')!r}")
    for req in ("id", "version"):
        if not isinstance(contract.get(req), str) or not contract[req]:
            errors.append(f"required string field {req!r} is missing or empty")
    domain = contract.get("domain")
    if domain not in DDK_DOMAINS:
        errors.append(f"domain must be one of {DDK_DOMAINS}, got {domain!r}")

    def check_custom_props(items: Any, path: str) -> None:
        if items is None:
            return
        if not isinstance(items, list):
            errors.append(f"{path}.customProperties must be a list")
            return
        for idx, item in enumerate(items):
            if not isinstance(item, Mapping):
                errors.append(f"{path}.customProperties[{idx}] must be an object")
                continue
            extra_keys = set(item) - {"id", "property", "value", "description", "vendor"}
            if extra_keys:
                errors.append(f"{path}.customProperties[{idx}] has invalid keys {sorted(extra_keys)}")
            if not isinstance(item.get("property"), str) or not item["property"]:
                errors.append(f"{path}.customProperties[{idx}] missing property name")
            if "value" not in item:
                errors.append(f"{path}.customProperties[{idx}] missing value")
            vendor = item.get("vendor")
            if vendor is not None and (not isinstance(vendor, str) or not _VENDOR_RE.fullmatch(vendor)):
                errors.append(f"{path}.customProperties[{idx}] invalid vendor {vendor!r}")
            if vendor == "ddk" and item.get("property") == "conceptKind":
                if item.get("value") not in DDK_CONCEPT_KINDS:
                    errors.append(
                        f"{path}.customProperties[{idx}] invalid DDK conceptKind {item.get('value')!r}; "
                        f"expected one of {sorted(DDK_CONCEPT_KINDS)}"
                    )
            if vendor == "refspec" and item.get("property") == "urn":
                if item.get("value") not in CANONICAL_REFSPEC_URNS:
                    errors.append(f"{path}.customProperties[{idx}] unverified RefSpec URN {item.get('value')!r}")

    check_custom_props(contract.get("customProperties"), "$")

    schemas = contract.get("schema")
    if not isinstance(schemas, list) or not schemas:
        errors.append("schema must be a non-empty list")
        return errors

    seen_table_names: set[str] = set()
    seen_table_ids: set[str] = set()

    def check_prop(prop: Any, path: str, *, require_name: bool) -> None:
        if not isinstance(prop, Mapping):
            errors.append(f"{path} must be an object")
            return
        if require_name:
            name = prop.get("name")
            if not isinstance(name, str) or not name:
                errors.append(f"{path}.name must be a non-empty string")
        lt = prop.get("logicalType")
        if lt not in _VALID_ODCS_LOGICAL_TYPES:
            errors.append(f"{path}.logicalType invalid: {lt!r}")
        check_custom_props(prop.get("customProperties"), path)
        if lt == "array":
            if "items" not in prop:
                errors.append(f"{path} array missing items")
            else:
                check_prop(prop["items"], f"{path}.items", require_name=False)
        elif lt == "object":
            children = prop.get("properties")
            if not isinstance(children, list) or not children:
                errors.append(f"{path} object missing non-empty properties")
            else:
                for cidx, child in enumerate(children):
                    check_prop(child, f"{path}.properties[{cidx}]", require_name=True)

    for tidx, obj in enumerate(schemas):
        tpath = f"$.schema[{tidx}]"
        if not isinstance(obj, Mapping):
            errors.append(f"{tpath} must be an object")
            continue
        tname = obj.get("name")
        tid = obj.get("id")
        if not isinstance(tname, str) or not tname:
            errors.append(f"{tpath}.name must be a non-empty string")
        elif tname in seen_table_names:
            errors.append(f"duplicate schema object name {tname!r}")
        else:
            seen_table_names.add(tname)
        if tid is not None:
            if not isinstance(tid, str) or not _STABLE_ID_RE.fullmatch(tid):
                errors.append(f"{tpath}.id invalid StableId {tid!r}")
            elif tid in seen_table_ids:
                errors.append(f"duplicate schema object id {tid!r}")
            else:
                seen_table_ids.add(tid)
        if obj.get("logicalType") != "object":
            errors.append(f"{tpath}.logicalType must be 'object'")
        props = obj.get("properties")
        if not isinstance(props, list) or not props:
            errors.append(f"{tpath}.properties must be a non-empty list")
        else:
            col_names: set[str] = set()
            for pidx, prop in enumerate(props):
                check_prop(prop, f"{tpath}.properties[{pidx}]", require_name=True)
                pname = prop.get("name") if isinstance(prop, Mapping) else None
                if isinstance(pname, str):
                    if pname in col_names:
                        errors.append(f"{tpath} repeats column {pname!r}")
                    col_names.add(pname)
        check_custom_props(obj.get("customProperties"), tpath)
        rels = obj.get("relationships")
        if rels is not None:
            if not isinstance(rels, list):
                errors.append(f"{tpath}.relationships must be a list")
            else:
                seen_rel_ids: set[str] = set()
                for ridx, rel in enumerate(rels):
                    rpath = f"{tpath}.relationships[{ridx}]"
                    if not isinstance(rel, Mapping):
                        errors.append(f"{rpath} must be an object")
                        continue
                    extra_rel = set(rel) - {"id", "type", "from", "to", "customProperties"}
                    if extra_rel:
                        errors.append(f"{rpath} has unsupported keys {sorted(extra_rel)}")
                    rid = rel.get("id")
                    if rid is not None:
                        if not isinstance(rid, str) or not _STABLE_ID_RE.fullmatch(rid):
                            errors.append(f"{rpath}.id invalid StableId {rid!r}")
                        elif rid in seen_rel_ids:
                            errors.append(f"{tpath} duplicates relationship id {rid!r}")
                        else:
                            seen_rel_ids.add(rid)
                    if rel.get("type", "foreignKey") != "foreignKey":
                        errors.append(f"{rpath}.type must be 'foreignKey'")
                    for side in ("from", "to"):
                        val = rel.get(side)
                        if isinstance(val, str):
                            if not _SHORTHAND_REF_RE.fullmatch(val):
                                errors.append(f"{rpath}.{side} invalid ShorthandReference {val!r}")
                        elif isinstance(val, list) and val:
                            for entry in val:
                                if not isinstance(entry, str) or not _SHORTHAND_REF_RE.fullmatch(entry):
                                    errors.append(f"{rpath}.{side} invalid ShorthandReference {entry!r}")
                        else:
                            errors.append(f"{rpath}.{side} missing or empty")
                    if type(rel.get("from")) is not type(rel.get("to")):
                        errors.append(f"{rpath} 'from' and 'to' must both be strings or both be arrays")
                    elif isinstance(rel.get("from"), list) and len(rel["from"]) != len(rel["to"]):
                        errors.append(f"{rpath} composite 'from' and 'to' lengths differ")
                    check_custom_props(rel.get("customProperties"), rpath)

    # Verify round-trip to PyArrow Schema succeeds for every table.
    if not errors:
        try:
            contract_declared_schemas(contract)
        except ODCSContractError as exc:
            errors.append(str(exc))

    if json_schema_path is not None:
        import jsonschema

        schema = json.loads(json_schema_path.read_text(encoding="utf-8"))
        validator = jsonschema.Draft201909Validator(schema)
        for err in validator.iter_errors(contract):
            errors.append(f"JSON Schema ({err.json_path}): {err.message}")

    return errors


def _count_refspec_urn_columns(contract: Mapping[str, Any]) -> int:
    """Count columns carrying a verified canonical RefSpec / RKAF URN."""
    total = 0
    for table in contract.get("schema", ()):
        for prop in table.get("properties", ()):
            if any(
                cp.get("vendor") == "refspec" and cp.get("property") == "urn" for cp in prop.get("customProperties", ())
            ):
                total += 1
    return total


def _count_refspec_faceted_columns(contract: Mapping[str, Any]) -> int:
    """Count columns carrying a RefSpec facet or canonical URN mapping."""
    total = 0
    for table in contract.get("schema", ()):
        for prop in table.get("properties", ()):
            if any(cp.get("vendor") == "refspec" for cp in prop.get("customProperties", ())):
                total += 1
    return total


def tracked_catalog_gaps(
    table_metadata: Mapping[str, Any],
    *,
    published_index: Mapping[str, Any] | None = None,
    rulemaking_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an explicit, never-silently-suppressed inventory of non-live or legacy catalog gaps."""
    from spicy_regs.local_publication import LOCAL_FAMILIES, _family_schemas

    child_views = sorted(t for t, meta in table_metadata.items() if meta.get("view_role") == "child_query")
    shared_processing = sorted(t for t in table_metadata if t == "etl_receipts")
    unmapped_tables = sorted(
        t for t in table_metadata if t not in _TABLE_TO_FAMILY and t not in child_views and t not in shared_processing
    )
    local_without_inline_schema = sorted(fam for fam in LOCAL_FAMILIES if _family_schemas(fam) is None)

    gaps: dict[str, Any] = {
        "derived_child_views": child_views,
        "shared_processing_tables": shared_processing,
        "unassigned_metadata_tables": unmapped_tables,
        "local_families_without_inline_arrow_schema": local_without_inline_schema,
    }

    if published_index is not None:
        pub_tables: set[str] = set()
        receipt_only_families: list[str] = []
        for fam, entry in sorted((published_index.get("families") or {}).items()):
            tables = entry.get("tables") or {}
            if not tables:
                receipt_only_families.append(fam)
            for key in tables:
                pub_tables.add(key.removesuffix(".parquet"))
        if rulemaking_manifest is not None:
            for art_name, art in (rulemaking_manifest.get("artifacts") or {}).items():
                if isinstance(art, Mapping) and art.get("visibility") == "public" and art_name.endswith(".parquet"):
                    pub_tables.add(art_name.removesuffix(".parquet"))

        excluded = set(child_views) | set(shared_processing)
        unpublished_supported = sorted(t for t in table_metadata if t not in pub_tables and t not in excluded)
        unknown_published = sorted(t for t in pub_tables if t not in table_metadata)
        gaps["receipt_only_published_families"] = receipt_only_families
        gaps["unpublished_supported_tables"] = unpublished_supported
        gaps["published_tables_missing_metadata"] = unknown_published

    return gaps


def resolve_families_to_export(
    *,
    scope: str = "pilot",
    families: Sequence[str] | None = None,
    table_metadata: Mapping[str, Any] | None = None,
    published_index: Mapping[str, Any] | None = None,
) -> dict[str, tuple[str, ...]]:
    """Resolve ``{family: (table_name, ...)}`` to export for ``scope`` or explicit ``families``."""
    family_map: dict[str, tuple[str, ...]] = dict(FAMILY_TABLES)
    if published_index is not None:
        for fam, entry in sorted((published_index.get("families") or {}).items()):
            _validate_family_slug(fam)
            tables = tuple(sorted(k.removesuffix(".parquet") for k in (entry.get("tables") or {})))
            if tables:
                family_map[fam] = tables

    if families:
        for fam in families:
            _validate_family_slug(fam)
        unknown = [f for f in families if f not in family_map]
        if unknown:
            raise ODCSContractError(f"Unknown family name(s) requested for ODCS export: {unknown}")
        selected = sorted(dict.fromkeys(families))
    elif scope == "pilot":
        selected = sorted(PILOT_FAMILIES)
    elif scope == "all":
        selected = sorted(family_map)
    else:
        raise ODCSContractError(f"Unknown export scope {scope!r}; expected 'pilot' or 'all'")

    resolved: dict[str, tuple[str, ...]] = {}
    for fam in selected:
        tables = family_map[fam]
        if table_metadata is not None:
            tables = tuple(t for t in tables if t in table_metadata)
        if tables:
            resolved[fam] = tables
    return resolved


def build_odcs_bundle(
    *,
    scope: str = "pilot",
    families: Sequence[str] | None = None,
    metadata_path: Path = DEFAULT_TABLE_METADATA_PATH,
    joins_path: Path = DEFAULT_TABLE_JOINS_PATH,
    verified_parquet_schemas: Mapping[str, pa.Schema] | None = None,
    published_index: Mapping[str, Any] | None = None,
    rulemaking_manifest: Mapping[str, Any] | None = None,
    json_schema_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    """Build deterministic ODCS v3.2.0 contracts and their manifest bytes in memory."""
    table_metadata, metadata_sha = load_table_metadata(metadata_path)
    joins_doc, joins_sha = load_table_joins(joins_path)
    joins = joins_doc.get("joins") or []

    selected = resolve_families_to_export(
        scope=scope,
        families=families,
        table_metadata=table_metadata,
        published_index=published_index,
    )

    file_bytes: dict[str, bytes] = {}
    contract_entries: dict[str, dict[str, Any]] = {}

    for fam in sorted(selected):
        tables = selected[fam]
        contract = build_family_contract(
            fam,
            tables,
            table_metadata,
            joins=joins,
            source_metadata_sha256=metadata_sha,
            source_joins_sha256=joins_sha,
            verified_parquet_schemas=verified_parquet_schemas,
        )
        errors = validate_odcs_v320_contract(contract, json_schema_path=json_schema_path)
        if errors:
            raise ODCSContractError(
                f"Generated ODCS v3.2.0 contract for {fam!r} failed validation: " + "; ".join(errors)
            )
        raw = canonical_json_bytes(contract)
        rel_path = f"families/{fam}.odcs.json"
        file_bytes[rel_path] = raw
        primary_domain, secondary_domains = domain_for_family(fam, table_metadata=table_metadata)
        rel_count = sum(len(obj.get("relationships") or ()) for obj in contract["schema"])
        contract_entries[fam] = {
            "family": fam,
            "path": rel_path,
            "sha256": _sha256_bytes(raw),
            "contract_id": contract["id"],
            "domain": primary_domain,
            "secondary_domains": list(secondary_domains),
            "pilot_wave": fam in PILOT_FAMILIES,
            "tables": list(tables),
            "table_count": len(tables),
            "relationship_count": rel_count,
            "refspec_urn_columns": _count_refspec_urn_columns(contract),
            "refspec_mapped_columns": _count_refspec_faceted_columns(contract),
        }

    manifest: dict[str, Any] = {
        "format": ODCS_MANIFEST_FORMAT,
        "version": ODCS_MANIFEST_VERSION,
        "odcs_api_version": ODCS_API_VERSION,
        "arrow_profile": DDK_ARROW_EXACT_PROFILE,
        "scope": scope if not families else "custom",
        "source_digests": {
            "table_metadata_sha256": metadata_sha,
            "table_joins_sha256": joins_sha,
        },
        "pilot_families": list(PILOT_FAMILIES),
        "enrolled_families": sorted(contract_entries),
        "contracts": contract_entries,
        "tracked_catalog_gaps": tracked_catalog_gaps(
            table_metadata,
            published_index=published_index,
            rulemaking_manifest=rulemaking_manifest,
        ),
    }
    file_bytes["manifest.json"] = canonical_json_bytes(manifest)
    return manifest, file_bytes


def export_odcs_contracts(
    out_dir: Path = DEFAULT_ODCS_DIR,
    *,
    scope: str = "pilot",
    families: Sequence[str] | None = None,
    metadata_path: Path = DEFAULT_TABLE_METADATA_PATH,
    joins_path: Path = DEFAULT_TABLE_JOINS_PATH,
    verified_parquet_schemas: Mapping[str, pa.Schema] | None = None,
    published_index: Mapping[str, Any] | None = None,
    rulemaking_manifest: Mapping[str, Any] | None = None,
    json_schema_path: Path | None = None,
) -> dict[str, Any]:
    """Write deterministic ODCS v3.2.0 DataContracts and ``manifest.json`` to ``out_dir``.

    Stale ``families/*.odcs.json`` files are pruned only on full-scope exports (when ``families``
    is omitted), never on single/subset ``--family`` exports.
    """
    if out_dir.is_symlink():
        raise ODCSContractError(f"ODCS output directory must not be a symlink: {out_dir}")
    manifest, file_bytes = build_odcs_bundle(
        scope=scope,
        families=families,
        metadata_path=metadata_path,
        joins_path=joins_path,
        verified_parquet_schemas=verified_parquet_schemas,
        published_index=published_index,
        rulemaking_manifest=rulemaking_manifest,
        json_schema_path=json_schema_path,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    families_dir = out_dir / "families"
    if families_dir.is_symlink():
        raise ODCSContractError(f"ODCS families directory must not be a symlink: {families_dir}")
    families_dir.mkdir(parents=True, exist_ok=True)
    expected_rel = set(file_bytes)
    # Prune stale contract files only during full-scope exports, never on --family subset exports.
    if not families:
        for existing in sorted(families_dir.glob("*.odcs.json")):
            if existing.is_symlink():
                raise ODCSContractError(f"Refusing to prune symlinked contract file: {existing}")
            rel = f"families/{existing.name}"
            if rel not in expected_rel:
                existing.unlink()
    for rel_path, raw in sorted(file_bytes.items()):
        target = _safe_contained_path(out_dir, rel_path, label=f"ODCS output path {rel_path!r}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    return manifest


def check_odcs_contracts(
    out_dir: Path = DEFAULT_ODCS_DIR,
    *,
    scope: str = "pilot",
    families: Sequence[str] | None = None,
    metadata_path: Path = DEFAULT_TABLE_METADATA_PATH,
    joins_path: Path = DEFAULT_TABLE_JOINS_PATH,
    json_schema_path: Path | None = None,
) -> list[str]:
    """Verify committed ODCS v3.2.0 contracts match a fresh deterministic export byte-for-byte."""
    if out_dir.is_symlink():
        return [f"ODCS contracts directory must not be a symlink: {out_dir}"]
    _, expected_files = build_odcs_bundle(
        scope=scope,
        families=families,
        metadata_path=metadata_path,
        joins_path=joins_path,
        json_schema_path=json_schema_path,
    )
    errors: list[str] = []
    for rel_path, expected_raw in sorted(expected_files.items()):
        try:
            actual_path = _safe_contained_path(out_dir, rel_path, label=f"ODCS file {rel_path!r}")
        except ODCSContractError as exc:
            errors.append(str(exc))
            continue
        if not actual_path.is_file():
            errors.append(f"Missing committed ODCS file: {rel_path}")
            continue
        actual_raw = actual_path.read_bytes()
        if actual_raw != expected_raw:
            errors.append(
                f"Committed ODCS file {rel_path} drifted from fresh export "
                f"(expected sha256 {_sha256_bytes(expected_raw)[:12]}, got {_sha256_bytes(actual_raw)[:12]})"
            )
    families_dir = out_dir / "families"
    if not families and families_dir.is_dir():
        for existing in sorted(families_dir.glob("*.odcs.json")):
            rel = f"families/{existing.name}"
            if rel not in expected_files:
                errors.append(f"Unexpected extra ODCS contract file: {rel}")
    return errors


def _declared_contract_family(contract: Mapping[str, Any]) -> str | None:
    for cp in contract.get("customProperties", ()):
        if isinstance(cp, Mapping) and cp.get("vendor") == "spicy-regs" and cp.get("property") == "family":
            val = cp.get("value")
            return val if isinstance(val, str) else None
    return None


def load_enrolled_family_contract(
    family: str,
    *,
    contracts_dir: Path = DEFAULT_ODCS_DIR,
) -> tuple[dict[str, Any], Path] | None:
    """Load and verify the enrolled ODCS v3.2.0 DataContract for ``family`` from ``contracts_dir``.

    Fails closed on symlinks, path traversal, missing/malformed SHA-256 digests, mismatched
    manifest or contract family identities, corrupt JSON, or invalid ODCS v3.2.0 structure.
    Returns ``None`` only when ``manifest.json`` is absent or does not enroll ``family``.
    """
    _validate_family_slug(family)
    if contracts_dir.is_symlink():
        raise ODCSContractError(f"ODCS contracts directory must not be a symlink: {contracts_dir}")
    manifest_path = contracts_dir / "manifest.json"
    if manifest_path.is_symlink():
        raise ODCSContractError(f"ODCS manifest must not be a symlink: {manifest_path}")
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ODCSContractError(f"Failed to read ODCS manifest {manifest_path}: {exc}") from exc
    if not isinstance(manifest, Mapping):
        raise ODCSContractError(f"ODCS manifest {manifest_path} must be a JSON object")
    if manifest.get("format") != ODCS_MANIFEST_FORMAT:
        raise ODCSContractError(f"Unrecognized ODCS manifest format in {manifest_path}: {manifest.get('format')!r}")
    contracts = manifest.get("contracts")
    if not isinstance(contracts, Mapping):
        raise ODCSContractError(f"ODCS manifest {manifest_path} 'contracts' must be a JSON object")
    entry = contracts.get(family)
    if entry is None:
        return None
    if not isinstance(entry, Mapping):
        raise ODCSContractError(f"ODCS manifest entry for {family!r} must be a JSON object")
    if entry.get("family") != family:
        raise ODCSContractError(
            f"ODCS manifest entry for {family!r} declares mismatched family {entry.get('family')!r}"
        )

    rel_path = entry.get("path")
    contract_path = _safe_contained_path(contracts_dir, rel_path, label=f"Enrolled ODCS contract path for {family!r}")
    if not contract_path.is_file():
        raise ODCSContractError(f"Enrolled ODCS contract for {family!r} is missing: {contract_path}")

    expected_sha = entry.get("sha256")
    if not isinstance(expected_sha, str) or not _HEX64_RE.fullmatch(expected_sha):
        raise ODCSContractError(
            f"Enrolled ODCS contract manifest entry for {family!r} requires a valid 64-hex sha256 digest"
        )
    try:
        raw = contract_path.read_bytes()
    except OSError as exc:
        raise ODCSContractError(f"Failed to read enrolled ODCS contract {contract_path}: {exc}") from exc
    actual_sha = _sha256_bytes(raw)
    if actual_sha != expected_sha:
        raise ODCSContractError(
            f"Enrolled ODCS contract for {family!r} failed SHA-256 check "
            f"(expected {expected_sha[:12]}, got {actual_sha[:12]})"
        )
    try:
        contract = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ODCSContractError(f"Enrolled ODCS contract {contract_path} is not valid JSON: {exc}") from exc
    if not isinstance(contract, dict):
        raise ODCSContractError(f"Enrolled ODCS contract {contract_path} must be a JSON object")
    if errors := validate_odcs_v320_contract(contract):
        raise ODCSContractError(f"Enrolled ODCS contract for {family!r} is invalid: " + "; ".join(errors))
    declared_family = _declared_contract_family(contract)
    if declared_family != family:
        raise ODCSContractError(
            f"Enrolled ODCS contract {contract_path} declares family {declared_family!r}, not the expected {family!r}"
        )
    return contract, contract_path


class _HTTPRangeFile(io.RawIOBase):
    """Minimal seekable HTTP range reader for Parquet footers (reads only a few KiB per table)."""

    def __init__(self, client: Any, url: str) -> None:
        self._client = client
        self._url = url
        head = client.head(url)
        head.raise_for_status()
        raw_len = head.headers.get("content-length")
        if raw_len is None:
            raise ODCSContractError(f"Missing Content-Length header for remote Parquet {url}")
        try:
            size = int(raw_len)
        except ValueError as exc:
            raise ODCSContractError(f"Invalid Content-Length header {raw_len!r} for remote Parquet {url}") from exc
        if size <= 0:
            raise ODCSContractError(f"Non-positive Content-Length {size} for remote Parquet {url}")
        self._size = size
        self._pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence!r}")
        if new_pos < 0:
            raise ValueError(f"Negative seek position: {new_pos}")
        self._pos = new_pos
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size == 0 or self._pos >= self._size:
            return b""
        end = self._size - 1 if size < 0 else min(self._size - 1, self._pos + size - 1)
        expected_max = end - self._pos + 1
        response = self._client.get(self._url, headers={"Range": f"bytes={self._pos}-{end}"})
        response.raise_for_status()
        if response.status_code != 206:
            raise ODCSContractError(
                f"Expected HTTP 206 Partial Content for range read of {self._url}, got {response.status_code}"
            )
        data = response.content
        if len(data) > expected_max:
            raise ODCSContractError(
                f"Remote range response for {self._url} returned {len(data)} bytes, exceeding requested {expected_max}"
            )
        self._pos += len(data)
        return data


def read_remote_parquet_schema(url: str, *, client: Any | None = None) -> pa.Schema:
    """Read a remote Parquet file's Arrow schema via HTTP range requests without downloading row groups."""
    import httpx

    headers = {"User-Agent": "spicy-regs-odcs-verifier/1.0"}
    if client is not None:
        return pq.read_schema(_HTTPRangeFile(client, url))
    with httpx.Client(headers=headers, timeout=30.0) as local_client:
        return pq.read_schema(_HTTPRangeFile(local_client, url))


def audit_parquet_parity(
    *,
    scope: str = "pilot",
    families: Sequence[str] | None = None,
    parquet_dir: Path | None = None,
    r2_base_url: str | None = None,
    metadata_path: Path = DEFAULT_TABLE_METADATA_PATH,
    joins_path: Path = DEFAULT_TABLE_JOINS_PATH,
    check_order: bool = True,
    http_client: Any | None = None,
) -> dict[str, Any]:
    """Audit ODCS v3.2.0 contracts against local Parquet files or live public R2 Parquet footers.

    Explicitly reports verified tables, any schema drift gaps, and tracked catalog gaps
    without silently suppressing any mismatch or HTTP error.
    """
    import httpx

    if parquet_dir is not None and not parquet_dir.is_dir():
        raise ODCSContractError(f"Parquet directory does not exist: {parquet_dir}")

    published_index: dict[str, Any] | None = None
    rulemaking_manifest: dict[str, Any] | None = None
    remote_urls: dict[str, list[tuple[str, str]]] = {}

    owned_client = None
    active_client = http_client
    if r2_base_url is not None and active_client is None:
        owned_client = httpx.Client(headers={"User-Agent": "spicy-regs-odcs-verifier/1.0"}, timeout=30.0)
        active_client = owned_client

    try:
        if r2_base_url is not None:
            base = r2_base_url.rstrip("/")
            pub_resp = active_client.get(f"{base}/publication.v2.json")
            pub_resp.raise_for_status()
            published_index = pub_resp.json()
            for fam, entry in (published_index.get("families") or {}).items():
                prefix = entry["prefix"]
                for tkey, tdesc in (entry.get("tables") or {}).items():
                    tname = tkey.removesuffix(".parquet")
                    if "members" in tdesc and tdesc["members"]:
                        rels = [tdesc["members"][0]["key"]]
                        if len(tdesc["members"]) > 1:
                            rels.append(tdesc["members"][-1]["key"])
                    else:
                        rels = [tkey]
                    remote_urls[tname] = [(rel, f"{base}/{prefix}/{rel}") for rel in rels]

            needs_rulemaking = (families is None) or ("rulemaking" in families)
            rm_ptr = active_client.get(f"{base}/materialized/rulemaking/latest.json")
            if rm_ptr.status_code == 404 and not needs_rulemaking:
                rulemaking_manifest = None
            else:
                rm_ptr.raise_for_status()
                ptr_doc = rm_ptr.json()
                manifest_key = ptr_doc.get("manifest_key")
                if not isinstance(manifest_key, str) or not manifest_key:
                    raise ODCSContractError("Rulemaking latest.json pointer missing valid 'manifest_key'")
                rm_resp = active_client.get(f"{base}/{manifest_key}")
                rm_resp.raise_for_status()
                rulemaking_manifest = rm_resp.json()
                for art_name, art in (rulemaking_manifest.get("artifacts") or {}).items():
                    if isinstance(art, Mapping) and art.get("visibility") == "public" and art_name.endswith(".parquet"):
                        tname = art_name.removesuffix(".parquet")
                        remote_urls[tname] = [(art_name, f"{base}/{art['remote_key']}")]

        manifest, file_bytes = build_odcs_bundle(
            scope=scope,
            families=families,
            metadata_path=metadata_path,
            joins_path=joins_path,
            published_index=published_index,
            rulemaking_manifest=rulemaking_manifest,
        )

        verified_tables: dict[str, dict[str, Any]] = {}
        drift_gaps: list[dict[str, Any]] = []
        missing_parquet_tables: list[dict[str, str]] = []

        for fam, entry in sorted(manifest["contracts"].items()):
            contract = json.loads(file_bytes[entry["path"]])
            declared = contract_declared_schemas(contract)
            for tname, expected_schema in sorted(declared.items()):
                candidates: list[tuple[str, Any]] = []
                fetch_failed = False
                if parquet_dir is not None:
                    direct = parquet_dir / f"{tname}.parquet"
                    fam_direct = parquet_dir / fam / f"{tname}.parquet"
                    part_dir = parquet_dir / tname
                    fam_part_dir = parquet_dir / fam / tname
                    if direct.is_file():
                        candidates = [(direct.name, direct)]
                    elif fam_direct.is_file():
                        candidates = [(f"{fam}/{fam_direct.name}", fam_direct)]
                    elif part_dir.is_dir():
                        candidates = [
                            (p.relative_to(parquet_dir).as_posix(), p) for p in sorted(part_dir.rglob("*.parquet"))
                        ]
                    elif fam_part_dir.is_dir():
                        candidates = [
                            (p.relative_to(parquet_dir).as_posix(), p) for p in sorted(fam_part_dir.rglob("*.parquet"))
                        ]
                elif tname in remote_urls:
                    for rel, url in remote_urls[tname]:
                        try:
                            candidates.append((rel, read_remote_parquet_schema(url, client=active_client)))
                        except Exception as exc:
                            fetch_failed = True
                            drift_gaps.append(
                                {"family": fam, "table": tname, "member": rel, "issues": [f"read_error: {exc}"]}
                            )

                if not candidates and not fetch_failed:
                    missing_parquet_tables.append({"family": fam, "table": tname})
                    continue

                table_failed = fetch_failed
                for label, actual in candidates:
                    try:
                        issues = parquet_schema_diff(expected_schema, actual, check_order=check_order)
                    except ODCSContractError as exc:
                        table_failed = True
                        drift_gaps.append(
                            {"family": fam, "table": tname, "member": label, "issues": [f"read_error: {exc}"]}
                        )
                        continue
                    if issues:
                        table_failed = True
                        drift_gaps.append({"family": fam, "table": tname, "member": label, "issues": issues})
                if not table_failed and candidates:
                    verified_tables[tname] = {
                        "family": fam,
                        "domain": entry["domain"],
                        "files_checked": len(candidates),
                        "column_count": len(expected_schema),
                        "schema_sha256": _schema_fingerprint(expected_schema),
                    }
    finally:
        if owned_client is not None:
            owned_client.close()

    return {
        "odcs_api_version": ODCS_API_VERSION,
        "scope": manifest["scope"],
        "families_checked": len(manifest["contracts"]),
        "verified_table_count": len(verified_tables),
        "drift_gap_count": len(drift_gaps),
        "missing_parquet_count": len(missing_parquet_tables),
        "verified_tables": verified_tables,
        "drift_gaps": drift_gaps,
        "missing_parquet_tables": missing_parquet_tables,
        "tracked_catalog_gaps": manifest["tracked_catalog_gaps"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="export-odcs-contracts",
        description="Export deterministic ODCS v3.2.0 DataContracts and verify Parquet Arrow schema parity.",
    )
    parser.add_argument(
        "--scope",
        choices=("pilot", "all"),
        default="pilot",
        help="Family scope to export or check (default: pilot = 8 pilot-wave families).",
    )
    parser.add_argument(
        "--family",
        action="append",
        default=[],
        help="Specific family to export/check (repeatable; overrides --scope).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_ODCS_DIR,
        help=f"Output directory for ODCS contracts and manifest.json (default: {DEFAULT_ODCS_DIR}).",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_TABLE_METADATA_PATH,
        help="Path to table_metadata.json.",
    )
    parser.add_argument(
        "--joins",
        type=Path,
        default=DEFAULT_TABLE_JOINS_PATH,
        help="Path to table_joins.json.",
    )
    parser.add_argument(
        "--json-schema",
        type=Path,
        default=None,
        help="Optional path to official ODCS v3.2.0 odcs-json-schema.json for Draft 2019-09 validation.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify committed ODCS files match a fresh deterministic export without writing.",
    )
    parser.add_argument(
        "--verify-parquet-dir",
        type=Path,
        default=None,
        help="Verify ODCS contracts against local Parquet files under this directory.",
    )
    parser.add_argument(
        "--verify-r2",
        action="store_true",
        help="Verify ODCS contracts against live public R2 Parquet footers (read-only).",
    )
    parser.add_argument(
        "--r2-base-url",
        default="https://data.spicygov.ai",
        help="Base public URL for --verify-r2 (default: https://data.spicygov.ai).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    families = args.family or None

    if args.verify_parquet_dir is not None or args.verify_r2:
        try:
            report = audit_parquet_parity(
                scope=args.scope,
                families=families,
                parquet_dir=args.verify_parquet_dir,
                r2_base_url=args.r2_base_url if args.verify_r2 else None,
                metadata_path=args.metadata,
                joins_path=args.joins,
            )
        except Exception as exc:
            print(f"ERROR: Parquet schema parity verification failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(report, indent=2, sort_keys=True))
        if report["drift_gap_count"] > 0 or report["verified_table_count"] == 0 or report["missing_parquet_count"] > 0:
            return 1
        return 0

    if args.check:
        errors = check_odcs_contracts(
            args.out_dir,
            scope=args.scope,
            families=families,
            metadata_path=args.metadata,
            joins_path=args.joins,
            json_schema_path=args.json_schema,
        )
        if errors:
            for err in errors:
                print(f"ERROR: {err}", file=sys.stderr)
            return 1
        print(f"OK: ODCS v3.2.0 contracts in {args.out_dir} are up to date and schema-valid.")
        return 0

    manifest = export_odcs_contracts(
        args.out_dir,
        scope=args.scope,
        families=families,
        metadata_path=args.metadata,
        joins_path=args.joins,
        json_schema_path=args.json_schema,
    )
    print(
        json.dumps(
            {
                "status": "exported",
                "out_dir": str(args.out_dir),
                "odcs_api_version": manifest["odcs_api_version"],
                "scope": manifest["scope"],
                "enrolled_families": manifest["enrolled_families"],
                "contract_count": len(manifest["contracts"]),
                "table_count": sum(c["table_count"] for c in manifest["contracts"].values()),
                "tracked_catalog_gaps": manifest["tracked_catalog_gaps"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
