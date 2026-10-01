"""Retained registration, lobbyist, quality-notice and filing observations.

This consumer does not replace candidate/committee histories or relationship
mappings. Names stay literal; image numbers and source row IDs are never filing
numbers. API filing references do not establish amendment replacement scope.
"""

from dataclasses import dataclass
from datetime import datetime
import json
import re
from urllib.parse import quote, urlsplit

import pyarrow as pa

from .fec_bulk_financial import financial_date, pairs
from .fec_query import CollectionSelection, IDENTITY_VERSION, observation_id, record_evidence
from .fec_relationships import _id_status

MAPPING_VERSION = "fec-identity-observations/1"
STATEMENTS = "fec_registration_statements"
LOBBYISTS = "fec_lobbyist_registrations"
NOTICES = "fec_quality_notices"
FILINGS = "fec_filings"
LINKS = "fec_filing_links"
COMMON = (
    "record_id identity_version mapping_version mapping_status mapping_reason_json collection_id source_record_id "
    "source_sha256 source_locator_json source_pointer source_authority selection_evidence_sha256 source_namespace "
    "filing_key filing_link_status native_field_states_json current_record_status"
).split()
STATEMENT_FIELDS = pairs(
    "committee_id:COMMITTEE_ID committee_name:COMMITTEE_NAME committee_street_1:COMMITTEE_STREET_1 "
    "committee_street_2:COMMITTEE_STREET_2 committee_city:COMMITTEE_CITY committee_state:COMMITTEE_STATE "
    "committee_zip:COMMITTEE_ZIP affiliated_committee_name:AFFILIATED_COMMITTEE_NAME "
    "filed_committee_type:FILED_COMMITTEE_TYPE filed_committee_designation:FILED_COMMITTEE_DESIGNATION "
    "filing_frequency:FILING_FREQUENCY organization_type:ORGANIZATION_TYPE treasurer_name:TREASURER_NAME "
    "committee_email:COMMITTEE_EMAIL committee_web_url:COMMITTEE_WEB_URL beginning_image_number:BEGIN_IMAGE_NUMBER"
)
CANDIDATE_FIELDS = pairs(
    "candidate_id:CANDIDATE_ID candidate_name:CANDIDATE_NAME party:PARTY party_code:PARTY_CODE "
    "office:CANDIDATE_OFFICE office_code:CANDIDATE_OFFICE_CODE office_state:CANDIDATE_OFFICE_STATE "
    "office_state_code:CANDIDATE_OFFICE_STATE_CODE office_district:CANDIDATE_OFFICE_DISTRICT "
    "candidate_city:CITY candidate_state:STATE candidate_zip:ZIP election_year:ELECTION_YEAR "
    "report_year:REPORT_YEAR beginning_image_number:BEGIN_IMAGE_NUMBER"
)
LOBBYIST_FIELDS = pairs(
    "committee_id:Committee_Id committee_name:Committee_Name image_url:Link_Image lobbyist_indicator:Is_Lobbyist"
)
NOTICE_FIELDS = pairs(
    "committee_id:committee_id committee_name:committee_name filed_committee_type:filed_cmte_tp_desc filings_url:filings_url"
)
FILING_FIELDS = pairs(
    "report_number:file_number source_record_identifier:sub_id native_filer_id:committee_id filer_name:committee_name "
    "candidate_id:candidate_id candidate_name:candidate_name form_type:form_type form_category:form_category "
    "document_description:document_description document_type:document_type document_type_full:document_type_full "
    "report_type:report_type report_type_full:report_type_full report_year:report_year reported_cycle:cycle "
    "election_year:election_year means_filed:means_filed beginning_image_number:beginning_image_number "
    "ending_image_number:ending_image_number fec_file_id:fec_file_id amendment_indicator:amendment_indicator "
    "amendment_version:amendment_version amendment_number:amendment_number is_amended:is_amended most_recent:most_recent "
    "pages:pages office:office state:state party:party treasurer_name:treasurer_name "
    "fec_url:fec_url csv_url:csv_url html_url:html_url pdf_url:pdf_url"
)
FILING_DATES = (
    "receipt_date",
    "filed_date",
    "coverage_start_date",
    "coverage_end_date",
    "update_date",
    "load_timestamp",
)


def _schema(fields, dates=(), booleans=()):
    names = list(dict.fromkeys(COMMON + list(fields)))
    names += [f + suffix for f in dates for suffix in ("_raw", "_status")]
    return pa.schema(
        [(n, pa.string()) for n in names]
        + [("source_cycle", pa.int32())]
        + [(n, pa.date32()) for n in dates]
        + [(n, pa.bool_()) for n in booleans]
    )


SCHEMAS = {
    STATEMENTS: _schema(
        [k for k, _ in STATEMENT_FIELDS + CANDIDATE_FIELDS]
        + ["form_type", "committee_id_status", "candidate_id_status", "affiliation_status"],
        ("receipt_date",),
    ),
    LOBBYISTS: _schema(
        [k for k, _ in LOBBYIST_FIELDS] + ["committee_id_status", "lobbyist_status", "image_body_status"],
        ("filed_date",),
        ("is_lobbyist",),
    ),
    NOTICES: _schema(
        [k for k, _ in NOTICE_FIELDS] + ["committee_id_status", "notice_kind", "notice_scope", "exclusion_status"],
        ("first_receipt_date",),
    ),
    FILINGS: _schema(
        [k for k, _ in FILING_FIELDS]
        + [
            "filer_entity_type",
            "filer_id_status",
            "reporting_committee_id",
            "candidate_id_status",
            "pdf_body_status",
            "linked_original_body_status",
            "query_completeness",
            "amendment_chain_json",
            "amendment_scope_status",
            "financial_fields_status",
        ],
        FILING_DATES,
    ),
    LINKS: _schema(
        [
            "source_filing_record_id",
            "target_filing_key",
            "native_target_file_number",
            "relation_type",
            "target_resolution_status",
            "replacement_mode",
            "membership_completeness",
            "reference_status",
        ]
    ),
}


@dataclass(frozen=True)
class RegistryMapping:
    key: str
    table: str
    route_pattern: str
    text_fields: tuple[tuple[str, str], ...]
    date_field: str
    native_date_field: str
    form_type: str | None = None


FORM1 = RegistryMapping(
    "form1",
    STATEMENTS,
    r"/bulk-downloads/(\d{4})/Form1Filer_\1\.csv",
    STATEMENT_FIELDS,
    "receipt_date",
    "RECEIPT_DATE",
    "F1",
)
FORM2 = RegistryMapping(
    "form2",
    STATEMENTS,
    r"/bulk-downloads/(\d{4})/Form2Filer_\1\.csv",
    CANDIDATE_FIELDS,
    "receipt_date",
    "RECEIPT_DATE",
    "F2",
)
LOBBYIST = RegistryMapping(
    "lobbyist", LOBBYISTS, r"/bulk-downloads/data\.fec\.gov/lobbyist\.csv", LOBBYIST_FIELDS, "filed_date", "Date_Filed"
)
QUALITY_NOTICE = RegistryMapping(
    "false-fictitious-notice",
    NOTICES,
    r"/bulk-downloads/data\.fec\.gov/FalseFictitiousFilings\.csv",
    NOTICE_FIELDS,
    "first_receipt_date",
    "first_receipt_dt",
)
REGISTRY_MAPPINGS = (FORM1, FORM2, LOBBYIST, QUALITY_NOTICE)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _state(obj, key):
    if key not in obj:
        return "source_missing"
    if obj[key] is None:
        return "source_null"
    if obj[key] == "" or obj[key] == []:
        return "source_empty"
    return "reported"


def _scalar(value):
    if value is None or isinstance(value, str):
        return value
    if type(value) is int:
        return str(value)
    if type(value) is bool:
        return "true" if value else "false"
    raise ValueError("Identity metadata scalar must retain text, integer, boolean or null")


def _base(table, row, selection, namespace, pointer=""):
    if row["collection_id"] != selection.collection_id or row["source_sha256"] != selection.source_sha256:
        raise ValueError("Identity observation differs from pinned collection selection")
    locator = json.loads(row["source_locator_json"])
    if pointer:
        locator["typed_subrecord_pointer"] = pointer
    addressed = {**row, "source_locator_json": _json(locator)}
    result = dict.fromkeys(SCHEMAS[table].names)
    result.update(
        record_id=observation_id(table, addressed, selection.source_authority),
        identity_version=IDENTITY_VERSION,
        mapping_version=MAPPING_VERSION,
        mapping_status="mapped",
        mapping_reason_json="{}",
        collection_id=row["collection_id"],
        source_record_id=row["source_record_id"],
        source_sha256=row["source_sha256"],
        source_locator_json=row["source_locator_json"],
        source_pointer=pointer,
        source_authority=selection.source_authority,
        selection_evidence_sha256=selection.selection_evidence_sha256,
        source_namespace=namespace,
        source_cycle=selection.source_cycle,
        filing_key=None,
        filing_link_status="unresolved",
        current_record_status="unqualified",
    )
    return result


def _finish(result, row, selection, table, problems):
    result["mapping_status"] = "partial" if problems else "mapped"
    result["mapping_reason_json"] = _json(problems)
    return result, record_evidence(table, result["record_id"], row, selection.source_generation_pin)


def registry_mapping_for(row):
    path = urlsplit(row["source_url"]).path
    choices = [m for m in REGISTRY_MAPPINGS if re.fullmatch(m.route_pattern, path)]
    if len(choices) != 1:
        raise ValueError("Source route has no supported native registry mapping")
    return choices[0]


def map_registry(row, selection: CollectionSelection, mapping: RegistryMapping, *, date_year_bounds=None):
    """Map an already named native CSV row; return None for the exact header.

    A Form2 REPORT_YEAR provides a receipt-year witness. Other two-digit dates
    stay unresolved unless the caller supplies separately evidenced year bounds.
    Source cycle alone is never such evidence.
    """
    if registry_mapping_for(row) != mapping:
        raise ValueError("Registry mapping differs from source route")
    if mapping.form_type:
        route_cycle = int(urlsplit(row["source_url"]).path.split("/")[2])
        if selection.source_cycle != route_cycle:
            raise ValueError("Registry source cycle differs from pinned selection")
    result = _base(mapping.table, row, selection, "fec-bulk-" + mapping.key)
    native = json.loads(row["metadata_json"])
    required = {k for _, k in mapping.text_fields} | {mapping.native_date_field}
    if native.get("kind") == "row" and isinstance(native.get("fields"), list):
        if len(native["fields"]) != len(required) or set(native["fields"]) != required:
            raise ValueError("Registry source header differs from declared mapping fields")
        return None, []
    if not isinstance(native, dict) or not required <= native.keys():
        raise ValueError("Registry record lacks source-owned named fields")
    if any(v is not None and not isinstance(v, str) for k, v in native.items() if k in required):
        raise ValueError("Native registry values must remain source text or null")
    result.update({k: native[v] for k, v in mapping.text_fields})
    result["native_field_states_json"] = _json({k: _state(native, k) for k in sorted(required)})
    bounds = date_year_bounds
    if mapping == FORM2:
        raw_year = native["REPORT_YEAR"]
        if raw_year is not None and re.fullmatch(r"[0-9]{4}", raw_year):
            bounds = (int(raw_year), int(raw_year))
    value, status = financial_date(native[mapping.native_date_field], "DD-MON-YY", year_bounds=bounds)
    result.update(
        {
            mapping.date_field: value,
            mapping.date_field + "_raw": native[mapping.native_date_field],
            mapping.date_field + "_status": status,
        }
    )
    problems = (
        {} if status in {"exact_with_year_bounds", "source_empty", "source_null"} else {mapping.date_field: status}
    )
    if mapping.form_type:
        result["form_type"] = mapping.form_type
        result["committee_id_status"] = _id_status(result.get("committee_id"), "committee")
        result["candidate_id_status"] = _id_status(result.get("candidate_id"), "candidate")
        if mapping == FORM1:
            affiliation = result["affiliated_committee_name"]
            result["affiliation_status"] = (
                "source_reported_none"
                if isinstance(affiliation, str) and affiliation.strip().upper() == "NONE"
                else "source_name_only"
                if affiliation
                else _state(native, "AFFILIATED_COMMITTEE_NAME")
            )
        result["filing_link_status"] = "unresolved_no_report_number"
    else:
        result["committee_id_status"] = _id_status(result["committee_id"], "committee")
    if mapping == LOBBYIST:
        flag = native["Is_Lobbyist"]
        result["is_lobbyist"] = {"Y": True, "N": False}.get(flag)
        result["lobbyist_status"] = (
            "reported_yes" if flag == "Y" else "reported_no" if flag == "N" else _state(native, "Is_Lobbyist")
        )
        if flag not in {"Y", "N", "", None}:
            result["lobbyist_status"] = "unsupported_source_code"
            problems["Is_Lobbyist"] = "unsupported_source_code"
        result["image_body_status"] = "deferred_pdf_link_not_opened"
    if mapping == QUALITY_NOTICE:
        result.update(
            notice_kind="publisher_false_fictitious_filings_list",
            notice_scope="source_listed_committee",
            exclusion_status="no_automatic_exclusion",
        )
    return _finish(result, row, selection, mapping.table, problems)


def filing_key(raw, authority="official-fec"):
    """Use the exact API file-number namespace, including negative paper IDs.

    Zero is an absent/sentinel identifier. A source sub_id, image number, filename
    or FEC-prefixed string cannot substitute for the native file_number field.
    """
    if raw is None or raw == "":
        return None
    text = str(raw) if type(raw) is int else raw
    if not isinstance(text, str) or not re.fullmatch(r"-?(?:0|[1-9][0-9]*)", text):
        raise ValueError("Filing file_number must be an exact signed integer")
    if int(text) == 0:
        return None
    return "urn:fec:filing:" + quote(authority, safe="") + ":openfec-file-number:" + text


def _filing_route(row):
    parsed = urlsplit(row["source_url"])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "api.open.fec.gov"
        or not (
            parsed.path in {"/v1/filings/", "/v1/efile/filings/"}
            or re.fullmatch(r"/v1/committee/C[0-9]{8}/filings/", parsed.path)
        )
    ):
        raise ValueError("Unsupported filing metadata API route")
    return parsed.path


def _api_date(native, field):
    raw = native.get(field)
    if field not in native:
        return None, "source_missing"
    if raw is None or raw == "":
        return financial_date(raw, "YYYY-MM-DD")
    if isinstance(raw, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", raw):
        try:
            return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S").date(), "exact_date_component"
        except ValueError:
            return None, "invalid_date"
    return financial_date(raw, "YYYY-MM-DD")


def map_filing_metadata(row, selection: CollectionSelection):
    """Return a filing observation and literal source-reference links.

    Response controls return empty tables. References preserve source field and
    ordinal; an amendment_chain member does not imply a direction or replacement.
    """
    _filing_route(row)
    result = _base(FILINGS, row, selection, "fec-openfec-file-number")
    tables = {FILINGS: [], LINKS: []}
    evidence = []
    wrapped = json.loads(row["metadata_json"])
    if wrapped.get("kind") == "api-response-field":
        return tables, evidence
    if wrapped.get("kind") == "api-record-observation" and isinstance(wrapped.get("metadata"), dict):
        native = wrapped["metadata"]
        prefix = "/metadata"
    elif row.get("profile") == "filing" and "kind" not in wrapped:
        native = wrapped
        prefix = ""
    else:
        raise ValueError("Expected a source-owned filing result or response control")
    result["source_pointer"] = prefix
    fields = (
        {v for _, v in FILING_FIELDS}
        | set(FILING_DATES)
        | {
            "amendment_chain",
            "amends_file",
            "amended_by",
            "previous_file_number",
            "most_recent_file_number",
            "most_recent_filing",
        }
    )
    result.update({k: _scalar(native.get(v)) for k, v in FILING_FIELDS})
    result["native_field_states_json"] = _json({k: _state(native, k) for k in sorted(fields)})
    result["filing_key"] = filing_key(native.get("file_number"), selection.source_authority)
    result["filing_link_status"] = "native_file_number" if result["filing_key"] else "unresolved_no_file_number"
    result.update(
        query_completeness="not-asserted",
        amendment_scope_status="unqualified",
        financial_fields_status="separate_financial_mapping_required",
        pdf_body_status="deferred_pdf" if native.get("pdf_url") else _state(native, "pdf_url"),
        linked_original_body_status="not_requested",
        amendment_chain_json=_json(native["amendment_chain"]) if "amendment_chain" in native else None,
    )
    filer = result["native_filer_id"]
    if _id_status(filer, "committee") == "source_id_shape":
        result.update(filer_entity_type="committee", filer_id_status="source_id_shape", reporting_committee_id=filer)
    elif _id_status(filer, "candidate") == "source_id_shape":
        result.update(filer_entity_type="candidate", filer_id_status="source_id_shape", reporting_committee_id=None)
    else:
        result.update(
            filer_entity_type="unresolved", filer_id_status="not_reported" if not filer else "invalid_source_id_shape"
        )
    result["candidate_id_status"] = _id_status(result.get("candidate_id"), "candidate")
    problems = {}
    for field in FILING_DATES:
        result[field], result[field + "_status"] = _api_date(native, field)
        result[field + "_raw"] = _scalar(native.get(field))
        if result[field + "_status"] not in {
            "exact",
            "exact_date_component",
            "source_null",
            "source_empty",
            "source_missing",
        }:
            problems[field] = result[field + "_status"]
    result, links = _finish(result, row, selection, FILINGS, problems)
    tables[FILINGS].append(result)
    evidence.extend(links)
    references = []
    for field in ("previous_file_number", "most_recent_file_number", "most_recent_filing", "amends_file", "amended_by"):
        if field in native and native[field] is not None and native[field] != "":
            references.append((field, native[field], prefix + "/" + field))
    chain = native.get("amendment_chain")
    if chain is not None:
        if not isinstance(chain, list):
            raise ValueError("API amendment_chain must be array or null")
        references.extend(
            ("amendment_chain_member", value, f"{prefix}/amendment_chain/{i}") for i, value in enumerate(chain)
        )
    for relation, value, pointer in references:
        link = _base(LINKS, row, selection, "fec-openfec-file-number", pointer)
        target = filing_key(value, selection.source_authority)
        link.update(
            filing_key=result["filing_key"],
            filing_link_status=result["filing_link_status"],
            source_filing_record_id=result["record_id"],
            target_filing_key=target,
            native_target_file_number=_scalar(value),
            relation_type=relation,
            target_resolution_status="native_reference_unresolved" if target else "unresolved_no_file_number",
            replacement_mode="unknown",
            membership_completeness="unqualified",
            reference_status="self_reference"
            if target is not None and target == result["filing_key"]
            else "reported_reference",
            native_field_states_json=_json({"referenced_file_number": "source_null" if value is None else "reported"}),
        )
        link, links = _finish(link, row, selection, LINKS, {})
        tables[LINKS].append(link)
        evidence.extend(links)
    return tables, evidence
