"""Typed retained FEC observations, separate from qualified financial totals.

Source decoding stays with SpicyDocs. These mappings consume the verified
``fec_source_records`` fields and keep physical source observations distinct.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re

import pyarrow as pa

from spicy_regs.fec_versions import INDIVIDUAL_RECEIPT_MAPPING_VERSION as RECEIPT_MAPPING_VERSION

IDENTITY_VERSION = "fec-typed-observation/1"
VALUE_MAPPING_VERSION = "fec-exact-financial-values/2"
#: The year FEC's first two-year election cycle ended (1975-1976); FEC began administering the law in 1975.
FIRST_FEC_CYCLE = 1976
AMOUNT_TYPE = pa.decimal128(38, 9)
RECEIPT_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "record_id identity_version mapping_version value_mapping_version mapping_status mapping_reason_json collection_id source_record_id "
            "source_sha256 source_locator_json "
            "source_authority selection_evidence_sha256 source_representation_role correction_operation "
            "correction_applicability_status current_record_status reporting_committee_id contributor_name contributor_type "
            "contributor_city contributor_state contributor_zip employer occupation date_status amount_status currency "
            "amount_raw transaction_date_raw transaction_date_status "
            "amount_kind transaction_type transaction_id source_record_identifier source_namespace report_number report_type "
            "image_number amendment_indicator memo_indicator memo_text other_native_id transaction_election filing_key "
            "filing_link_status"
        ).split()
    ]
    + [("source_cycle", pa.int32()), ("transaction_date", pa.date32()), ("amount", AMOUNT_TYPE)]
)
EVIDENCE_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "target_table target_record_id target_generation_scope witness_generation_scope witness_generation_pin "
            "role endpoint_kind collection_id source_record_id context_column context_pointer witness_sha256 witness_locator_json"
        ).split()
    ]
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_AMOUNT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)\Z")
_INDIV_FIELDS = frozenset(
    "CMTE_ID AMNDT_IND RPT_TP TRANSACTION_PGI IMAGE_NUM TRANSACTION_TP ENTITY_TP NAME CITY "
    "STATE ZIP_CODE EMPLOYER OCCUPATION TRANSACTION_DT TRANSACTION_AMT OTHER_ID TRAN_ID "
    "FILE_NUM MEMO_CD MEMO_TEXT SUB_ID".split()
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _digest(value):
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError("FEC evidence requires a SHA-256 pin")
    return value


def _text(value):
    if not isinstance(value, str) or not value:
        raise ValueError("FEC evidence identity requires a nonempty string")
    return value


def exact_amount(raw):
    """Return an exact DECIMAL(38,9) value and explicit missing/refusal status.

    No floats, exponent syntax, silent rounding or overflow are admitted by this
    bulk-text mapping. A leading decimal point is valid retained FEC spelling
    (for example .01); a leading zero is not required. Native text and its scale
    remain in the source observation.
    """
    if raw is None:
        return None, "source_null"
    if raw == "":
        return None, "source_empty"
    if not isinstance(raw, str) or not _AMOUNT.fullmatch(raw):
        return None, "unsupported_spelling"
    try:
        value = Decimal(raw)
        with localcontext() as context:
            context.prec = max(50, len(raw) + 10)
            typed = value.quantize(Decimal("0.000000001"))
        if typed != value:
            return None, "excess_precision"
        if typed.copy_abs() >= Decimal("1e29"):
            return None, "overflow"
        return typed, "exact"
    except InvalidOperation:
        return None, "invalid_decimal"


def bulk_date(raw):
    """Read the selected bulk MMDDYYYY format without inventing a date."""
    if raw is None:
        return None, "source_null"
    if raw == "":
        return None, "source_empty"
    if not isinstance(raw, str) or re.fullmatch(r"[0-9]{8}", raw) is None:
        return None, "unsupported_spelling"
    try:
        return datetime.strptime(raw, "%m%d%Y").date(), "exact"
    except ValueError:
        return None, "invalid_date"


def observation_id(table, row, authority):
    """Stable source-observation identity; definition changes do not change it."""
    locator = json.loads(row["source_locator_json"])
    for key in ("collection_id", "source_record_id"):
        if locator.get(key) != _text(row[key]):
            raise ValueError("FEC locator differs from its source observation")
    # This is interpretation evidence, not part of the physical source address.
    physical = {k: v for k, v in locator.items() if k != "field_mapping"}
    identity = [
        IDENTITY_VERSION,
        _text(table),
        _text(authority),
        row["collection_id"],
        row["source_record_id"],
        _digest(row["source_sha256"]),
        physical,
    ]
    return "sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest()


def record_evidence(table, record_id, row, generation_pin):
    """Link to a record or an existing collection dictionary, with no fake rows.

    The target is in the containing typed generation (self); the witnesses are in
    an already sealed source generation (external). Only the external pin is stored.
    Endpoint cardinality and actual byte reachability are checked by release validation.
    """
    common = dict(
        target_table=_text(table),
        target_record_id=_digest(record_id),
        target_generation_scope="self",
        witness_generation_scope="external",
        witness_generation_pin=_digest(generation_pin),
    )
    locator = json.loads(row["source_locator_json"])
    for key in ("collection_id", "source_record_id"):
        if locator.get(key) != _text(row[key]):
            raise ValueError("FEC evidence locator differs from its source observation")
    result = [
        {
            **common,
            "role": "primary",
            "endpoint_kind": "source_record",
            "collection_id": row["collection_id"],
            "source_record_id": row["source_record_id"],
            "context_column": None,
            "context_pointer": None,
            "witness_sha256": _digest(row["source_sha256"]),
            "witness_locator_json": _json(locator),
        }
    ]
    definition = locator.get("field_mapping")
    if definition is None:
        return result
    if definition.get("kind") == "official-html-dictionary":
        if definition.get("definitions_pointer") != "/tableFieldDefinitions":
            raise ValueError("FEC dictionary endpoint must identify the retained collection definitions")
        result.append(
            {
                **common,
                "role": "field_definition",
                "endpoint_kind": "collection_context",
                "collection_id": _text(definition["definitions_collection_id"]),
                "source_record_id": None,
                "context_column": "collection_outcome_json",
                "context_pointer": _text(definition["definitions_pointer"]),
                "witness_sha256": _digest(definition["source_sha256"]),
                "witness_locator_json": _json(definition["table_locator"]),
            }
        )
    else:
        header_locator = definition["source_locator"]
        if any(header_locator.get(k) != _text(definition[k]) for k in ("collection_id", "source_record_id")):
            raise ValueError("FEC header locator differs from its source observation")
        result.append(
            {
                **common,
                "role": "field_definition",
                "endpoint_kind": "source_record",
                "collection_id": definition["collection_id"],
                "source_record_id": definition["source_record_id"],
                "context_column": None,
                "context_pointer": None,
                "witness_sha256": _digest(definition["source_sha256"]),
                "witness_locator_json": _json(header_locator),
            }
        )
    return result


@dataclass(frozen=True)
class CollectionSelection:
    """A reviewed collection mapping; operation comes from collection evidence."""

    collection_id: str
    source_sha256: str
    source_generation_pin: str
    source_authority: str
    source_cycle: int | None
    representation_role: str
    selection_evidence_sha256: str

    def __post_init__(self):
        for value in (self.source_sha256, self.source_generation_pin, self.selection_evidence_sha256):
            _digest(value)
        _text(self.collection_id)
        _text(self.source_authority)
        if self.source_cycle is not None and (
            type(self.source_cycle) is not int
            or not FIRST_FEC_CYCLE <= self.source_cycle <= 9998
            or self.source_cycle % 2
        ):
            raise ValueError("Financial selection requires an explicit FEC cycle-ending year")
        if self.representation_role not in {"snapshot", "insertion", "deletion", "other_correction", "unknown"}:
            raise ValueError("Unsupported FEC collection representation role")


# Retained trial scripts use this name. The pinned collection semantics also
# apply to other financial tables; keep their mappings separate below.
ReceiptSelection = CollectionSelection


def observation_fields(table, row, selection: CollectionSelection, mapping_version):
    """Common typed fields, with no inferred filing or current-record status."""
    if row["collection_id"] != selection.collection_id or row["source_sha256"] != selection.source_sha256:
        raise ValueError("Financial row differs from its pinned selected collection")
    return dict(
        record_id=observation_id(table, row, selection.source_authority),
        identity_version=IDENTITY_VERSION,
        mapping_version=mapping_version,
        value_mapping_version=VALUE_MAPPING_VERSION,
        collection_id=row["collection_id"],
        source_record_id=row["source_record_id"],
        source_sha256=row["source_sha256"],
        source_locator_json=_json(json.loads(row["source_locator_json"])),
        source_authority=selection.source_authority,
        source_cycle=selection.source_cycle,
        selection_evidence_sha256=selection.selection_evidence_sha256,
        source_representation_role=selection.representation_role,
        correction_operation=selection.representation_role
        if selection.representation_role in {"insertion", "deletion", "other_correction"}
        else "none"
        if selection.representation_role == "snapshot"
        else "unknown",
        correction_applicability_status="unqualified",
        current_record_status="unqualified",
        filing_key=None,
        filing_link_status="unresolved",
    )


def bulk_receipt(row, selection: CollectionSelection):
    """Map one individual-contribution bulk observation; no amendment selection.

    The caller must prove that the selected collection is the individual-receipt
    layout. Identical headers in other transaction files do not prove its grain.
    """
    if selection.source_cycle is None:
        raise ValueError("Individual bulk receipts require their explicitly selected source cycle")
    common = observation_fields("fec_receipts", row, selection, RECEIPT_MAPPING_VERSION)
    native = json.loads(row["metadata_json"])
    if not isinstance(native, dict) or not _INDIV_FIELDS <= native.keys():
        raise ValueError("Receipt observation lacks the qualified individual-contribution fields")
    if any(v is not None and not isinstance(v, str) for k, v in native.items() if k in _INDIV_FIELDS):
        raise ValueError("Bulk receipt native fields must remain text or source null")
    amount, amount_status = exact_amount(native["TRANSACTION_AMT"])
    date, date_status = bulk_date(native["TRANSACTION_DT"])
    refusals = {
        name: status
        for name, status in (("amount", amount_status), ("transaction_date", date_status))
        if status not in {"exact", "source_null", "source_empty"}
    }
    unknown = sorted(native.keys() - _INDIV_FIELDS)
    result = dict(
        **common,
        mapping_status="partial" if refusals else "mapped",
        mapping_reason_json=_json({"field_refusals": refusals, "uninterpreted_native_fields": unknown}),
        reporting_committee_id=native["CMTE_ID"],
        contributor_name=native["NAME"],
        contributor_type=native["ENTITY_TP"],
        contributor_city=native["CITY"],
        contributor_state=native["STATE"],
        contributor_zip=native["ZIP_CODE"],
        employer=native["EMPLOYER"],
        occupation=native["OCCUPATION"],
        transaction_date=date,
        date_status=date_status,
        transaction_date_raw=native["TRANSACTION_DT"],
        transaction_date_status=date_status,
        amount=amount,
        amount_status=amount_status,
        amount_raw=native["TRANSACTION_AMT"],
        currency="USD",
        amount_kind="reported_receipt",
        transaction_type=native["TRANSACTION_TP"],
        transaction_id=native["TRAN_ID"],
        source_record_identifier=native["SUB_ID"],
        source_namespace="fec-bulk-individual-contributions",
        report_number=native["FILE_NUM"],
        report_type=native["RPT_TP"],
        image_number=native["IMAGE_NUM"],
        amendment_indicator=native["AMNDT_IND"],
        memo_indicator=native["MEMO_CD"],
        memo_text=native["MEMO_TEXT"],
        other_native_id=native["OTHER_ID"],
        transaction_election=native["TRANSACTION_PGI"],
    )
    return result, record_evidence("fec_receipts", result["record_id"], row, selection.source_generation_pin)
