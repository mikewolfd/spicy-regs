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


def iter_filing_number_associations(rows, *, table, filings, source_generation_pin, namespace_evidence):
    """Associate a bounded typed batch using qualified native file-number fields.

    namespace_evidence maps supported mapper namespaces to retained official
    definition pins. Release validation verifies those bytes and their meaning;
    a digest's spelling alone is not evidence. Missing or invalid IDs stay visible.
    """
    by_key, _ = _index(filings)
    yield from _number_associations(rows, table, by_key, source_generation_pin, namespace_evidence)


def _number_associations(rows, table, by_key, source_generation_pin, namespace_evidence):
    """Apply the same number rule with one selected metadata population."""
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
        yield item


def associate_filing_numbers(rows, **kwargs):
    """Bounded batch compatibility wrapper over the maintained streaming rule."""
    return list(iter_filing_number_associations(rows, **kwargs))


def association_records(rows, *, table, schema, filings, headers, source_generation_pin,
                        target_generation_pin, namespace_evidence, check_resources):
    """Publish one guarded decision per financial/text row in bounded batches.

    Reuse the existing number and header rules. Neither rule chooses a latest
    filing, applies corrections, or changes an amount. The original mapper row
    remains separate so the receipt writer can restore it exactly.
    """
    import duckdb
    from .fec_native_subjects import FIELD_RULES
    from spicy_regs.relationship_views.fec_filing_associations import financial_header_association_sql

    if table not in FIELD_RULES or "filing_key" not in FIELD_RULES[table]["keep"]:
        raise ValueError("Filing associations are inapplicable to this subject")
    _digest(target_generation_pin)
    by_key, _ = _index(filings)
    header_witnesses = defaultdict(list)
    for header in headers:
        header_witnesses[tuple(header.get(k) for k in (
            "record_id", "source_generation_pin", "collection_id", "source_sha256", "source_authority",
            "header_record_id", "header_locator_json"))].append(header)
        if header.get("association_status") == "resolved_native_filing_key":
            candidates = by_key.get(header.get("filing_key"), [])
            key, status = _resolve(candidates, header.get("source_authority"))
            if (status != "resolved_native_filing_key" or key != header.get("filing_key")
                    or sorted(header.get("filing_observation_ids") or []) != sorted(r["record_id"] for r in candidates)
                    or header.get("filing_observation_count") != len(candidates)):
                raise ValueError("Header association differs from selected complete filing witnesses")

    def batches():
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) == 512:
                yield batch
                batch = []
        if batch:
            yield batch

    with duckdb.connect(config={"threads": 1, "memory_limit": "128MiB", "max_temp_directory_size": "0B"}) as con:
        con.register("fec_filing_header_associations", pa.Table.from_pylist(headers, schema=ASSOCIATION_SCHEMA))
        for batch in batches():
            check_resources()
            number_decisions = []
            valid_rows, valid_indices = [], []
            for index, row in enumerate(batch):
                # A missing source coordinate is an unavailable association,
                # never a synthetic source record or an invented witness ID.
                try:
                    _base(row, table, source_generation_pin, row["selection_evidence_sha256"], "native-file-number")
                except (KeyError, ValueError, TypeError, AttributeError):
                    number_decisions.append(dict(record_id=None, filing_key=None, referenced_filing_key=None,
                        association_status="unresolved_source_context", filing_observation_count=None,
                        filing_observation_ids=None))
                else:
                    number_decisions.append(None)
                    valid_rows.append(row)
                    valid_indices.append(index)
            for index, decision in zip(valid_indices, _number_associations(
                    valid_rows, table, by_key, source_generation_pin, namespace_evidence), strict=True):
                number_decisions[index] = decision
            header_decisions = {}
            if any(row.get("filing_header_record_id") is not None for row in batch):
                required = {"filing_header_record_id", "filing_header_locator_json"}
                if not required <= set(schema.names):
                    raise ValueError("Header lookup lacks its retained main identity and locator")
                con.register(table, pa.Table.from_pylist(batch, schema=schema))
                cursor = con.execute(financial_header_association_sql(table, source_generation_pin))
                names = [column[0] for column in cursor.description]
                for values in cursor.fetchall():
                    decision = dict(zip(names, values, strict=True))
                    rid = decision["target_record_id"]
                    if rid in header_decisions:
                        raise ValueError("Repeated source identity prevents a singular header decision")
                    header_decisions[rid] = decision
            for row, decision in zip(batch, number_decisions, strict=True):
                if decision is None:
                    raise ValueError("Selected source row lacks an association decision")
                ids = decision["filing_observation_ids"]
                count = decision["filing_observation_count"]
                if (row.get("filing_header_record_id") is not None
                        and decision["association_status"] != "unresolved_source_context"):
                    decision = header_decisions[row["record_id"]]
                    # Only the exact admitted header occurrence supplies its
                    # observed target population. Absent/ambiguous headers
                    # leave this unknown rather than manufacture zero matches.
                    witnesses = header_witnesses.get((decision.get("association_record_id"), source_generation_pin,
                        row.get("collection_id"), row.get("source_sha256"), row.get("source_authority"),
                        row.get("filing_header_record_id"), row.get("filing_header_locator_json")), [])
                    witness = witnesses[0] if len(witnesses) == 1 else None
                    ids = witness["filing_observation_ids"] if witness is not None else None
                    count = witness["filing_observation_count"] if witness is not None else None
                yield row, dict(
                    filing_key=decision.get("filing_key"),
                    filing_link_status=decision["association_status"],
                    filing_association_status=decision["association_status"],
                    filing_association_policy=POLICY_VERSION,
                    filing_association_source_generation_pin=source_generation_pin,
                    filing_association_target_generation_pin=target_generation_pin,
                    filing_association_record_id=decision.get("association_record_id", decision.get("record_id")),
                    filing_association_referenced_key=decision.get("referenced_filing_key"),
                    filing_association_target_count=count,
                    filing_association_target_record_ids=ids,
                )


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
