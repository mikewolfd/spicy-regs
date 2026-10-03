"""Typed legal observations from retained, source-owned OpenFEC metadata.

Matter keys identify native namespaces; record keys retain each observation and
child occurrence. Repeated parties are assertions, not resolved people. Amounts
are reported penalty/payment states, never a summable legal total. This module
reads neither linked bodies nor PDF bytes.
"""

from datetime import datetime
from decimal import Decimal
import hashlib
import json
import re
from urllib.parse import quote, urljoin, urlsplit

import pyarrow as pa

from .fec_query import (
    AMOUNT_TYPE,
    IDENTITY_VERSION,
    CollectionSelection,
    exact_amount,
    observation_id,
    record_evidence,
)

MAPPING_VERSION = "fec-retained-legal/3"
MATTERS = "fec_legal_matters"
PARTIES = "fec_legal_parties"
EVENTS = "fec_legal_events"
DOCUMENTS = "fec_legal_documents"
FINDINGS = "fec_audit_findings"
TABLES = (MATTERS, PARTIES, EVENTS, DOCUMENTS, FINDINGS)
_TYPES = {
    "advisory_opinions": ("advisory_opinion", "no"),
    "admin_fines": ("administrative_fine", "no"),
    "murs": ("enforcement", "no"),
    "adrs": ("alternative_dispute_resolution", "no"),
    "rulemakings": ("rulemaking", "rm_no"),
    "audit": ("audit", "audit_case_id"),
}
_COMMON = (
    "record_id identity_version mapping_version mapping_status mapping_reason_json collection_id source_record_id "
    "source_sha256 source_locator_json source_pointer source_authority selection_evidence_sha256 "
    "matter_id matter_record_id query_completeness"
).split()
_MATTER_TEXT = (
    "matter_type source_namespace native_matter_id native_document_id title title_status description description_status "
    "status status_status source_url source_url_status committee_id candidate_id audit_id reported_cycle_status "
    "is_pending_status is_published_status current_state_status collection_states_json native_facts_json "
    "final_determination_amount_status final_determination_amount_raw payment_amount_status payment_amount_raw "
    "reason_to_believe_fine_amount_status reason_to_believe_fine_amount_raw "
    "treasury_referral_amount_status treasury_referral_amount_raw currency"
).split()
# Scalar source assertions belong in the matter row. Structured collections
# remain separate from these fields for ordered child queries.
_FACT_TEXT = (
    "challenge_outcome civil_penalty_payment_status report_type mur_type "
    "committee_description committee_designation committee_type rm_id rm_number"
).split()
_FACT_TYPES: dict[str, pa.DataType] = {**dict.fromkeys(_FACT_TEXT, pa.string()), "report_year": pa.int32(), "is_open_for_comment": pa.bool_()}
_MATTER_TEXT = [*_MATTER_TEXT, *(name + "_status" for name in _FACT_TYPES)]
_PARTY_TEXT = (
    "role role_status reported_name reported_name_status native_entity_id native_entity_id_status "
    "entity_namespace entity_type identity_resolution_status document_record_id"
).split()
_EVENT_TEXT = (
    "event_type event_date_raw event_date_status date_precision description description_status "
    "actor_name vote_type respondent_name related_document_record_id amount_raw amount_status amount_kind currency"
).split()
_DOC_TEXT = (
    "document_id native_document_id document_identity_status parent_document_record_id title title_status "
    "category category_status document_type filename url url_status document_date_raw document_date_status "
    "date_precision media_type body_status content_sha256 length_raw source_association_kind"
).split()
_FINDING_TEXT = (
    "finding_kind finding_status primary_category_id primary_category_name category_id category_name "
    "text disposition amount_raw amount_status currency supporting_document_record_id"
).split()


def _schema(text_fields, typed=()):
    return pa.schema([(k, pa.string()) for k in _COMMON + text_fields] + list(typed))


SCHEMAS = {
    MATTERS: _schema(
        _MATTER_TEXT,
        [("reported_cycle", pa.int32()), ("is_pending", pa.bool_()), ("is_published", pa.bool_())]
        + list(_FACT_TYPES.items())
        + [
            (k, AMOUNT_TYPE)
            for k in (
                "final_determination_amount",
                "payment_amount",
                "reason_to_believe_fine_amount",
                "treasury_referral_amount",
            )
        ],
    ),
    PARTIES: _schema(_PARTY_TEXT),
    EVENTS: _schema(_EVENT_TEXT, [("event_date", pa.date32()), ("amount", AMOUNT_TYPE)]),
    DOCUMENTS: _schema(_DOC_TEXT, [("document_date", pa.date32())]),
    FINDINGS: _schema(_FINDING_TEXT, [("amount", AMOUNT_TYPE)]),
}


def _json(value):
    # Source-owned decimal tokens are already strings. Decimal is only used to
    # prevent binary float decoding in a future exact JSON input representation.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False)


def _state(obj, key):
    if key not in obj:
        return "source_missing"
    value = obj[key]
    if value is None:
        return "source_null"
    if value == "" or value == [] or value == {}:
        return "source_empty"
    return "reported"


def _scalar(value):
    if value is None or isinstance(value, str):
        return value
    if type(value) is int or isinstance(value, Decimal):
        return str(value)
    if type(value) is bool:
        return "true" if value else "false"
    raise ValueError("Legal scalar must preserve text, integer, exact decimal, boolean or null")


def _typed_scalar(obj, key, dtype):
    """Interpret a known primitive without rounding numbers or guessing flags."""
    state = _state(obj, key)
    if state != "reported":
        return None, state
    raw = obj[key]
    if pa.types.is_string(dtype):
        return (raw, "reported") if isinstance(raw, str) else (None, "unsupported_spelling")
    if pa.types.is_boolean(dtype):
        if type(raw) is bool:
            return raw, "reported"
        if isinstance(raw, str) and raw.lower() in {"true", "false"}:
            return raw.lower() == "true", "parsed"
        return None, "unsupported_spelling"
    if type(raw) is int:
        value = raw
    elif isinstance(raw, str) and re.fullmatch(r"[+-]?[0-9]+", raw):
        value = int(raw)
    else:
        return None, "unsupported_spelling"
    return (value, "parsed") if -(2**31) <= value < 2**31 else (None, "overflow")


def _first_key(obj, keys):
    return next((key for key in keys if key in obj), keys[0])


def _date(obj, key):
    raw = obj.get(key)
    state = _state(obj, key)
    if state != "reported":
        return None, state, None
    if not isinstance(raw, str):
        return None, "unsupported_spelling", None
    fmt = "%Y-%m-%d" if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw) else "%Y-%m-%dT%H:%M:%S"
    if fmt.endswith("%S") and not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", raw):
        return None, "unsupported_spelling", None
    try:
        return datetime.strptime(raw, fmt).date(), "exact" if fmt == "%Y-%m-%d" else "exact_date_component", "day"
    except ValueError:
        return None, "invalid_date", None


def _money(obj, key):
    state = _state(obj, key)
    raw = obj.get(key)
    text = str(raw) if type(raw) is int or isinstance(raw, Decimal) else raw
    value, status = exact_amount(text)
    return value, state if state != "reported" else status, _scalar(raw)


def _url(obj, key):
    raw = obj.get(key)
    status = _state(obj, key)
    if status != "reported":
        return raw, status
    if not isinstance(raw, str):
        raise ValueError("Legal document URL must remain source text")
    url = urljoin("https://www.fec.gov", raw)
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        return None, "unsupported_url"
    return url, "source_absolute" if urlsplit(raw).scheme else "resolved_relative_to_fec"


def _list(obj, key):
    value = obj.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"Legal {key} must be a source array or null")
    return value


def _key(authority, namespace, value):
    if not isinstance(value, str) or not value:
        raise ValueError("Legal matter requires a native nonempty identifier")
    return "urn:fec:" + ":".join(quote(part, safe="") for part in (authority, namespace, value))


def _identifier(obj, key):
    value = obj.get(key)
    if value is not None and not isinstance(value, str) and type(value) is not int:
        raise ValueError("Legal native identifier must remain text or integer")
    return _scalar(value)


def _route_kind(row):
    parsed = urlsplit(row["source_url"])
    if parsed.scheme != "https" or parsed.hostname != "api.open.fec.gov":
        raise ValueError("Legal API mapping requires the retained official API source URL")
    if parsed.path in {"/v1/legal/search/", "/v1/rulemaking/search/", "/v1/audit-case/"}:
        return parsed.path
    if re.fullmatch(r"/v1/legal/docs/murs/[^/]+/?", parsed.path):
        return parsed.path
    raise ValueError("Unsupported retained legal API route")


def legal_source_kind(row):
    """Classify controls separately; their repeated result arrays are not data rows."""
    _route_kind(row)
    wrapped = json.loads(row["metadata_json"], parse_float=Decimal)
    if not isinstance(wrapped, dict):
        raise ValueError("Legal source metadata must be an object")
    if wrapped.get("kind") == "api-response-field":
        return "response_control"
    if wrapped.get("kind") == "api-record-observation":
        if not isinstance(wrapped.get("metadata"), dict):
            raise ValueError("Legal API result lacks source-owned metadata")
        return "wrapped_record"
    if row.get("profile") == "legal" and "kind" not in wrapped:
        return "native_legal_record"
    raise ValueError("Expected a source-owned legal record or API response control")


def map_legal(row, selection: CollectionSelection):
    """Return typed table rows and exact parent evidence for one observation.

    API response controls return empty tables. Callers retain their disposition
    and original source rows. Child ``source_pointer`` values are relative to the
    retained ``metadata_json`` column, while evidence resolves the unchanged
    parent locator. No capture is selected as the current legal state.
    """
    if row["collection_id"] != selection.collection_id or row["source_sha256"] != selection.source_sha256:
        raise ValueError("Legal observation differs from its pinned selection")
    # Validate source identity even when this row is only a response control.
    observation_id(MATTERS, row, selection.source_authority)
    kind = legal_source_kind(row)
    tables = {table: [] for table in TABLES}
    evidence = []
    if kind == "response_control":
        return tables, evidence
    wrapped = json.loads(row["metadata_json"], parse_float=Decimal)
    native = wrapped["metadata"] if kind == "wrapped_record" else wrapped
    base_pointer = "/metadata" if kind == "wrapped_record" else ""
    route = _route_kind(row)
    native_type = "audit" if route == "/v1/audit-case/" else native.get("type")
    if native_type not in _TYPES:
        raise ValueError("Unsupported legal matter type")
    if route == "/v1/rulemaking/search/" and native_type != "rulemakings":
        raise ValueError("Rulemaking result type differs from source route")
    if route.startswith("/v1/legal/docs/murs/") and native_type != "murs":
        raise ValueError("Enforcement detail type differs from source route")
    matter_type, id_field = _TYPES[native_type]
    native_id = _identifier(native, id_field)
    if route.startswith("/v1/legal/docs/murs/") and route.rstrip("/").rsplit("/", 1)[-1] != native_id:
        raise ValueError("Legal detail matter identifier differs from source route")
    matter_id = _key(selection.source_authority, native_type + ":" + id_field, native_id)
    matter_record_id = observation_id(MATTERS, row, selection.source_authority)

    def add(table, pointer, values):
        locator = json.loads(row["source_locator_json"])
        if pointer != base_pointer:
            locator["typed_subrecord_pointer"] = pointer
        addressed = {**row, "source_locator_json": _json(locator)}
        record_id = observation_id(table, addressed, selection.source_authority)
        result = dict.fromkeys(SCHEMAS[table].names)
        result.update(
            record_id=record_id,
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
            matter_id=matter_id,
            matter_record_id=matter_record_id,
            query_completeness="not-asserted",
            **values,
        )
        problems = {
            k: v
            for k, v in result.items()
            if k.endswith("_status")
            and v
            in {
                "unsupported_spelling",
                "invalid_date",
                "unsupported_url",
                "excess_precision",
                "overflow",
                "invalid_decimal",
            }
        }
        if problems:
            result.update(mapping_status="partial", mapping_reason_json=_json(problems))
        tables[table].append(result)
        evidence.extend(record_evidence(table, record_id, row, selection.source_generation_pin))
        return record_id

    title_key = _first_key(native, ("title", "name", "mur_name", "committee_name"))
    description_key = _first_key(native, ("summary", "description"))
    status_key = _first_key(native, ("status", "case_status"))
    matter_url, url_status = _url(native, "url")
    amounts = {}
    for field in (
        "final_determination_amount",
        "payment_amount",
        "reason_to_believe_fine_amount",
        "treasury_referral_amount",
    ):
        amounts[field], amounts[field + "_status"], amounts[field + "_raw"] = _money(native, field)
    collections = {
        k: _state(native, k)
        for k in (
            "entities",
            "rm_entities",
            "participants",
            "respondents",
            "complainants",
            "complainant",
            "documents",
            "commission_votes",
            "dispositions",
            "primary_category_list",
            "key_documents",
            "no_tier_documents",
        )
    }
    # Keep only structured collections here. Scalars are mapped below; the
    # complete object, including unknown keys, remains in fec_source_records.
    facts = {
        k: native[k]
        for k in (
            "ao_citations",
            "aos_cited_by",
            "regulatory_citations",
            "statutory_citations",
            "citations",
            "subjects",
            "subject",
            "election_cycles",
            "non_monetary_terms",
            "non_monetary_terms_respondents",
        )
        if k in native
    }
    scalars = {}
    for name, dtype in {
        "reported_cycle": pa.int32(),
        "is_pending": pa.bool_(),
        "is_published": pa.bool_(),
        **_FACT_TYPES,
    }.items():
        native_name = {"reported_cycle": "cycle", "is_published": "published_flg"}.get(name, name)
        if name == "rm_id" and type(native.get(native_name)) is int:
            # Alternate native identifiers need their exact spelling, not a
            # numerical measure or an assumed equivalence to the matter number.
            scalars[name], scalars[name + "_status"] = str(native[native_name]), "reported"
        else:
            scalars[name], scalars[name + "_status"] = _typed_scalar(native, native_name, dtype)
    add(
        MATTERS,
        base_pointer,
        dict(
            matter_type=matter_type,
            source_namespace="fec-openfec-" + native_type + "-" + id_field,
            native_matter_id=native_id,
            native_document_id=_scalar(native.get("doc_id")),
            title=_scalar(native.get(title_key)),
            title_status=_state(native, title_key),
            description=_scalar(native.get(description_key)),
            description_status=_state(native, description_key),
            status=_scalar(native.get(status_key)),
            status_status=_state(native, status_key),
            source_url=matter_url,
            source_url_status=url_status,
            committee_id=_scalar(native.get("committee_id")),
            candidate_id=_scalar(native.get("candidate_id")),
            audit_id=_scalar(native.get("audit_id")),
            current_state_status="unqualified",
            collection_states_json=_json(collections),
            native_facts_json=_json(facts),
            currency="USD" if native_type == "admin_fines" else None,
            **amounts,
            **scalars,
        ),
    )

    def party(obj, pointer, role=None, name_key="name", id_key="entity_id", namespace=None, document=None):
        add(
            PARTIES,
            pointer,
            dict(
                role=_scalar(obj.get("role")) if role is None else role,
                role_status=_state(obj, "role") if role is None else "source_field_role",
                reported_name=_scalar(obj.get(name_key)),
                reported_name_status=_state(obj, name_key),
                native_entity_id=_scalar(obj.get(id_key)),
                native_entity_id_status=_state(obj, id_key),
                entity_namespace=namespace,
                entity_type=_scalar(obj.get("type")),
                identity_resolution_status="native_id_unresolved" if obj.get(id_key) else "source_name_only",
                document_record_id=document,
            ),
        )

    for field in ("entities", "rm_entities", "participants"):
        for i, item in enumerate(_list(native, field)):
            if not isinstance(item, dict):
                raise ValueError("Legal role assertion must be an object")
            party(item, f"{base_pointer}/{field}/{i}")
    for field, role in (
        ("respondents", "Respondent"),
        ("complainants", "Complainant"),
        ("complainant", "Complainant"),
        ("requestor_names", "Requestor"),
        ("representative_names", "Counsel/Representative"),
        ("commenter_names", "Commenter"),
        ("counsel_names", "Counsel"),
        ("petitioner_names", "Petitioner"),
        ("witness_names", "Witness"),
    ):
        for i, name in enumerate(_list(native, field)):
            party({"name": name}, f"{base_pointer}/{field}/{i}", role=role)
    if native_type in {"admin_fines", "audit"}:
        party(
            native,
            base_pointer + "/committee_id",
            "Subject committee",
            "committee_name" if native_type == "audit" else "name",
            "committee_id",
            "fec-committee-id",
        )
    if native_type == "audit" and native.get("candidate_id"):
        party(
            native,
            base_pointer + "/candidate_id",
            "Associated candidate",
            "candidate_name",
            "candidate_id",
            "fec-candidate-id",
        )

    def event(obj, pointer, event_type, date_key, description_key="description", amount_key="amount", document=None):
        value, status, precision = _date(obj, date_key)
        amount, amount_status, amount_raw = _money(obj, amount_key)
        return add(
            EVENTS,
            pointer,
            dict(
                event_type=event_type,
                event_date=value,
                event_date_status=status,
                date_precision=precision,
                event_date_raw=_scalar(obj.get(date_key)),
                description=_scalar(obj.get(description_key)),
                description_status=_state(obj, description_key),
                actor_name=_scalar(obj.get("commissioner_name")),
                vote_type=_scalar(obj.get("vote_type")),
                respondent_name=_scalar(obj.get("respondent")),
                related_document_record_id=document,
                amount=amount,
                amount_status=amount_status,
                amount_raw=amount_raw,
                amount_kind="reported_penalty" if amount_key == "penalty" else None,
                currency="USD" if amount_key == "penalty" else None,
            ),
        )

    for field in (
        "open_date",
        "close_date",
        "request_date",
        "issue_date",
        "challenge_receipt_date",
        "civil_penalty_due_date",
        "final_determination_date",
        "petition_court_decision_date",
        "petition_court_filing_date",
        "reason_to_believe_action_date",
        "treasury_referral_date",
        "admin_close_date",
        "comment_close_date",
        "calculated_comment_close_date",
        "far_release_date",
    ):
        if field in native:
            event(native, base_pointer + "/" + field, field.removesuffix("_date"), field)
    for field in ("fr_publication_dates", "hearing_dates", "vote_dates"):
        for i, value in enumerate(_list(native, field)):
            event({"date": value}, f"{base_pointer}/{field}/{i}", field.removesuffix("_dates"), "date")
    for field, date_key, text_key in (
        ("commission_votes", "vote_date", "action"),
        ("dispositions", "disposition_date", "disposition_description"),
    ):
        for i, item in enumerate(_list(native, field)):
            if not isinstance(item, dict):
                raise ValueError("Legal event must be an object")
            description_key = _first_key(item, (text_key, "disposition"))
            event(
                item,
                f"{base_pointer}/{field}/{i}",
                field.removesuffix("s"),
                date_key,
                description_key,
                "penalty" if field == "dispositions" else "amount",
            )
            if "respondent" in item:
                party(item, f"{base_pointer}/{field}/{i}/respondent", "Disposition respondent", "respondent")

    def document(item, pointer, association, parent=None):
        if not isinstance(item, dict):
            raise ValueError("Legal document metadata must be an object")
        id_key = _first_key(item, ("document_id", "doc_id"))
        doc_native_id = _identifier(item, id_key)
        doc_id = (
            _key(selection.source_authority, native_type + ":document:" + native_id, doc_native_id)
            if doc_native_id
            else None
        )
        title_key = _first_key(item, ("description", "doc_description"))
        category_key = _first_key(item, ("category", "doc_category_label"))
        date_key = _first_key(item, ("document_date", "date", "doc_date"))
        url, url_status = _url(item, "url")
        value, status, precision = _date(item, date_key)
        pdf = url is not None and urlsplit(url).path.lower().endswith(".pdf")
        doc_record_id = add(
            DOCUMENTS,
            pointer,
            dict(
                document_id=doc_id,
                native_document_id=doc_native_id,
                document_identity_status="native_document_observation_no_edition_claim"
                if doc_id
                else "source_occurrence_only",
                parent_document_record_id=parent,
                title=_scalar(item.get(title_key)),
                title_status=_state(item, title_key),
                category=_scalar(item.get(category_key)),
                category_status=_state(item, category_key),
                document_type=_scalar(item.get("doc_type_label")),
                filename=_scalar(item.get("filename")),
                url=url,
                url_status=url_status,
                document_date=value,
                document_date_status=status,
                document_date_raw=_scalar(item.get(date_key)),
                date_precision=precision,
                media_type="application/pdf" if pdf else None,
                body_status="deferred_pdf" if pdf else "not_requested",
                content_sha256=None,
                length_raw=_scalar(item.get("length")),
                source_association_kind=association,
            ),
        )
        if date_key in item:
            event(item, pointer + "/" + date_key, "document_date", date_key, title_key, document=doc_record_id)
        for i, person in enumerate(_list(item, "doc_entities")):
            party(person, f"{pointer}/doc_entities/{i}", document=doc_record_id)
        for i, group in enumerate(_list(item, "level_2_labels")):
            for j, child in enumerate(_list(group, "level_2_docs")):
                document(
                    child, f"{pointer}/level_2_labels/{i}/level_2_docs/{j}", "nested_rulemaking_document", doc_record_id
                )
        return doc_record_id

    for field in ("documents", "key_documents", "no_tier_documents"):
        for i, item in enumerate(_list(native, field)):
            document(item, f"{base_pointer}/{field}/{i}", field)
    audit_document = None
    if native_type == "audit" and "link_to_report" in native:
        audit_document = document(
            {"url": native["link_to_report"]}, base_pointer + "/link_to_report", "audit_report_reference"
        )
    for i, primary in enumerate(_list(native, "primary_category_list")):
        children = _list(primary, "sub_category_list")
        for j, child in enumerate(children or [None]):
            item = child if child is not None else primary
            pointer = f"{base_pointer}/primary_category_list/{i}" + (
                f"/sub_category_list/{j}" if child is not None else ""
            )
            cid = _scalar(item.get("sub_category_id" if child is not None else "primary_category_id"))
            name = _scalar(item.get("sub_category_name" if child is not None else "primary_category_name"))
            no_findings = child is not None and cid == "0" and name == "No Findings or Issues"
            add(
                FINDINGS,
                pointer,
                dict(
                    finding_kind="reported_audit_category",
                    finding_status="explicit_no_findings_or_issues" if no_findings else "reported_category",
                    primary_category_id=_scalar(primary.get("primary_category_id")),
                    primary_category_name=_scalar(primary.get("primary_category_name")),
                    category_id=cid,
                    category_name=name,
                    text=None,
                    disposition=None,
                    amount=None,
                    amount_raw=None,
                    amount_status="source_missing",
                    currency=None,
                    supporting_document_record_id=audit_document,
                ),
            )
    return tables, evidence


def legal_selection_digest(collection):
    """Pin the exact retained collection selection used by a mapping receipt."""
    return "sha256:" + hashlib.sha256(_json(collection).encode()).hexdigest()
