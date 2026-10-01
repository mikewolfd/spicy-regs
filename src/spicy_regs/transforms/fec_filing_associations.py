"""Evidence-backed filing associations that preserve financial row counts.

Native file numbers and API-reported original URLs are separate evidence paths.
An HDR field, archive member name, image number or source sub_id supplies neither.
These associations do not select amendments, current filings or money totals.
"""

from collections import defaultdict
import hashlib
import json

import pyarrow as pa

from spicy_regs.relationship_views.fec_filing_associations import (
    FILE_NUMBER_FILER_FIELDS,
    FILE_NUMBER_MAPPINGS,
    POLICY_VERSION,
    TARGET_IDENTITY_VERSION,
    TARGET_MAPPING_VERSION,
)

from .fec_filing_financial import validate_filing_header
from .fec_identity_observations import filing_key
from .fec_query import CollectionSelection, _digest, _json, _text, observation_id


ASSOCIATION_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "record_id policy_version target_table target_record_id source_generation_pin "
            "collection_id source_record_id source_sha256 source_authority source_locator_json "
            "association_basis association_status referenced_filing_key filing_key "
            "selection_evidence_sha256 namespace_evidence_sha256 report_number_raw "
            "header_record_id header_locator_json request_url resolved_url "
            "replacement_mode membership_completeness current_record_status"
        ).split()
    ]
    + [("filing_observation_count", pa.int64()), ("filing_observation_ids", pa.list_(pa.string()))]
)


def _index(filings):
    by_key, by_url = defaultdict(list), defaultdict(list)
    for row in filings:
        if row.get("filing_key") is not None:
            by_key[row["filing_key"]].append(row)
        if row.get("fec_url"):
            by_url[row["fec_url"]].append(row)
    return by_key, by_url


def _resolve(targets, authority):
    """Keep all target observations; never choose a first/latest capture."""
    if not targets:
        return None, "unresolved_target_not_retained"
    if len({r["record_id"] for r in targets}) != len(targets):
        return None, "unresolved_target_observation_ambiguous"
    keys = set()
    for row in targets:
        try:
            key = filing_key(row.get("report_number"), authority)
        except ValueError:
            return None, "unresolved_target_identity_conflict"
        if (
            key is None
            or key != row.get("filing_key")
            or row.get("source_authority") != authority
            or row.get("source_namespace") != "fec-openfec-file-number"
            or row.get("identity_version") != TARGET_IDENTITY_VERSION
            or row.get("mapping_version") != TARGET_MAPPING_VERSION
        ):
            return None, "unresolved_target_identity_conflict"
        keys.add(key)
    if len(keys) != 1 or any(
        len({r.get(field) for r in targets if r.get(field) not in (None, "")}) > 1
        for field in ("native_filer_id", "form_type")
    ):
        return None, "unresolved_target_identity_conflict"
    return keys.pop(), "resolved_native_filing_key"


def _base(row, table, generation, evidence, basis):
    locator = json.loads(row["source_locator_json"])
    if any(locator.get(k) != _text(row[k]) for k in ("collection_id", "source_record_id")):
        raise ValueError("Filing association locator differs from its source observation")
    identity = [
        POLICY_VERSION,
        TARGET_IDENTITY_VERSION,
        TARGET_MAPPING_VERSION,
        _digest(generation),
        _text(table),
        _text(row["record_id"]),
        observation_id(table, row, row["source_authority"]),
        basis,
    ]
    return dict(
        record_id="sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest(),
        policy_version=POLICY_VERSION,
        target_table=table,
        target_record_id=row["record_id"],
        source_generation_pin=generation,
        collection_id=_text(row["collection_id"]),
        source_record_id=row["source_record_id"],
        source_sha256=_digest(row["source_sha256"]),
        source_authority=_text(row["source_authority"]),
        source_locator_json=_json(locator),
        association_basis=basis,
        association_status="unresolved",
        referenced_filing_key=None,
        filing_key=None,
        selection_evidence_sha256=_digest(evidence),
        namespace_evidence_sha256=None,
        report_number_raw=row.get("report_number"),
        header_record_id=None,
        header_locator_json=None,
        request_url=None,
        resolved_url=None,
        replacement_mode="unknown",
        membership_completeness="unqualified",
        current_record_status="unqualified",
        filing_observation_count=0,
        filing_observation_ids=[],
    )


def _targets(result, targets, filer=None):
    key, status = _resolve(targets, result["source_authority"])
    if (
        key is not None
        and filer not in (None, "")
        and any(row.get("native_filer_id") not in (None, "", filer) for row in targets)
    ):
        key, status = None, "unresolved_filer_identity_conflict"
    result.update(
        filing_key=key,
        association_status=status,
        filing_observation_count=len(targets),
        filing_observation_ids=sorted(r["record_id"] for r in targets),
    )


def associate_filing_numbers(rows, *, table, filings, source_generation_pin, namespace_evidence):
    """Associate a bounded typed batch using qualified native file-number fields.

    namespace_evidence maps supported mapper namespaces to retained official
    definition pins. Release validation verifies those bytes and their meaning;
    a digest's spelling alone is not evidence. Missing or invalid IDs stay visible.
    """
    by_key, _ = _index(filings)
    result = []
    for row in rows:
        item = _base(row, table, source_generation_pin, row["selection_evidence_sha256"], "native-file-number")
        namespace = row.get("source_namespace")
        proof = namespace_evidence.get(namespace)
        if row["source_authority"] != "official-fec":
            item["association_status"] = "unresolved_source_authority"
        elif namespace not in FILE_NUMBER_MAPPINGS or row.get("mapping_version") != FILE_NUMBER_MAPPINGS[namespace]:
            item["association_status"] = "unresolved_native_namespace"
        elif proof is None:
            item["association_status"] = "unresolved_namespace_evidence"
        else:
            item["namespace_evidence_sha256"] = _digest(proof)
            try:
                item["referenced_filing_key"] = filing_key(row.get("report_number"), row["source_authority"])
            except ValueError:
                item["association_status"] = "unresolved_invalid_file_number"
            else:
                if item["referenced_filing_key"] is None:
                    item["association_status"] = "unresolved_no_file_number"
                else:
                    filer = row.get(FILE_NUMBER_FILER_FIELDS[namespace])
                    _targets(item, by_key.get(item["referenced_filing_key"], []), filer)
        result.append(item)
    return result


def associate_filing_headers(
    headers, *, scopes, authorities, filings, source_generation_pin, selection_evidence_sha256
):
    """Associate each selected physical header with an API-reported original URL.

    scopes are the exact retained source-reader selections, keyed by collection.
    Archive member names and HDR report/amendment fields never establish identity.
    The physical header endpoint remains usable when no logical filing can resolve.
    """
    _, by_url = _index(filings)
    result = []
    for header in headers:
        cid = header["collection_id"]
        scope, authority = scopes[cid], authorities[cid]
        selection = CollectionSelection(
            cid, header["source_sha256"], source_generation_pin, authority, None, "unknown", selection_evidence_sha256
        )
        validate_filing_header(header, header, selection)
        capture = scope["capture"]
        locator = json.loads(header["source_locator_json"])
        if capture["responseSha256"] != header["source_sha256"] or scope.get("member") != locator.get("member"):
            raise ValueError("Header association scope differs from the exact original/member")
        item = _base(
            {**header, "record_id": header["source_record_id"], "source_authority": authority},
            "fec_source_records",
            source_generation_pin,
            selection_evidence_sha256,
            "native-api-original-url",
        )
        item.update(
            header_record_id=header["source_record_id"],
            header_locator_json=header["source_locator_json"],
            request_url=capture["requestUrl"],
            resolved_url=capture.get("resolvedUrl"),
        )
        if authority != "official-fec":
            item["association_status"] = "unresolved_source_authority"
        elif scope.get("member") is not None:
            item["association_status"] = "unresolved_archive_member_identity"
        elif capture.get("resolvedUrl", capture["requestUrl"]) != capture["requestUrl"]:
            item["association_status"] = "unresolved_redirect_identity"
        else:
            targets = by_url.get(capture["requestUrl"], [])
            _targets(item, targets)
            if not targets:
                item["association_status"] = "unresolved_no_native_url_witness"
            item["referenced_filing_key"] = item["filing_key"]
        result.append(item)
    return result
