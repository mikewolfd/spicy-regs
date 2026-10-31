"""Dispositions and provider response outcomes from sealed retained FEC collection context.

Inputs are native facts in fec_collections, not files at workstation paths.
Context witnesses never become fictional fec_source_records. Only the two tables
that describe the receipts themselves come from here; historical IE statistics,
meetings, the filing feed and source pages are built from their retained originals
(fec_original_tables, ss-j4vz B7-B9), and their collections are dispositioned
"rebuilt_from_original".
"""

from dataclasses import dataclass, field
import hashlib
import json
import re
from urllib.parse import urlsplit

import pyarrow as pa

from .fec_query import _digest
from .fec_context_shape import response_capture_disposition

IDENTITY_VERSION = "fec-research-context/1"
VERSION = "fec-research-context/2"
ROOT = "/receiverDisposition/callerContext/facts"
COMMON = "record_id mapping_version collection_id source_authority source_url source_sha256 context_sha256 source_context_pointer observed_at_json".split()
RESPONSE_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in COMMON + "profile_refusal query_completeness payload_shape outcome_status capture_disposition".split()
    ]
    + [("observed_payload_records", pa.int32())]
)
DISPOSITION_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in "collection_id mapping_status mapping_reason source_url context_sha256 outputs_json".split()
    ]
    + [("source_fact_count", pa.int64())]
)
SCHEMAS = {
    "fec_research_response_outcomes": RESPONSE_SCHEMA,
    "fec_research_context_dispositions": DISPOSITION_SCHEMA,
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _sha(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
        value = "sha256:" + value
    return _digest(value)


@dataclass
class ContextMapping:
    tables: dict = field(default_factory=lambda: {name: [] for name in SCHEMAS})
    evidence: list = field(default_factory=list)


class _Context:
    def __init__(self, collection, source_generation_pin):
        self.generation_pin = _digest(source_generation_pin)
        self.collection_id = collection["collection_id"]
        outcome = json.loads(collection["collection_outcome_json"])
        caller = outcome["receiverDisposition"]["callerContext"]
        self.pin = _digest(caller["pin"]["sha256"])
        self.facts = caller["facts"]
        self.parsing = self.facts.get("parsing", {})
        self.capture = self.facts.get("source_capture", {})
        self.url = self.capture.get("url")
        self.source_sha = _sha(self.capture["sha256"]) if self.capture.get("sha256") else None
        self.authority = (
            self.capture.get("authority") or self.facts.get("source_authority") or urlsplit(self.url or "").hostname
        )
        if not isinstance(self.authority, str):
            self.authority = _json(self.authority)
        self.result = ContextMapping()

    def emit(self, table, pointer, values, *, context=()):
        pointer = ROOT + pointer
        identity = [IDENTITY_VERSION, table, self.collection_id, self.source_sha, self.pin, pointer]
        rid = "sha256:" + hashlib.sha256(_json(identity).encode()).hexdigest()
        row = dict(
            record_id=rid,
            mapping_version=VERSION,
            collection_id=self.collection_id,
            source_authority=self.authority,
            source_url=self.url,
            source_sha256=self.source_sha,
            context_sha256=self.pin,
            source_context_pointer=pointer,
            observed_at_json=_json(self.capture.get("observed_at")),
            **values,
        )
        self.result.tables[table].append(row)
        for p, role in [(pointer, "primary"), *[(ROOT + p, "field_definition") for p in context]]:
            self.result.evidence.append(
                dict(
                    target_table=table,
                    target_record_id=rid,
                    target_generation_scope="self",
                    witness_generation_scope="external",
                    witness_generation_pin=self.generation_pin,
                    role=role,
                    endpoint_kind="collection_context",
                    collection_id=self.collection_id,
                    source_record_id=None,
                    context_column="collection_outcome_json",
                    context_pointer=p,
                    witness_sha256=self.pin,
                    witness_locator_json=_json(dict(context_sha256=self.pin, source_sha256=self.source_sha, pointer=p)),
                )
            )
        return row

    def disposition(self, status, reason):
        count = len(self.parsing.get("facts", self.parsing.get("native_events", [])))
        self.result.tables["fec_research_context_dispositions"].append(
            dict(
                collection_id=self.collection_id,
                mapping_status=status,
                mapping_reason=reason,
                source_url=self.url,
                context_sha256=self.pin,
                source_fact_count=count,
                outputs_json=_json({k: len(v) for k, v in self.result.tables.items() if v}),
            )
        )
        return self.result


def _response_outcome(ctx):
    found = False
    for i, fact in enumerate(ctx.parsing.get("facts", [])):
        if "value" not in fact or "profile_refusal" not in fact:
            continue
        found = True
        value = fact["value"]
        count, shape = None, "other_native_payload"
        if isinstance(value, dict):
            if isinstance(value.get("results"), list):
                count = len(value["results"])
                shape = "results_empty" if count == 0 else "results_present"
            elif isinstance(value.get("docs"), list):
                count, shape = len(value["docs"]), "docs_present"
            elif "message" in value:
                shape = "error_message"
        ctx.emit(
            "fec_research_response_outcomes",
            f"/parsing/facts/{i}",
            dict(
                profile_refusal=fact["profile_refusal"],
                query_completeness=fact.get("query_completeness"),
                payload_shape=shape,
                outcome_status="refused_native_observation_not_qualified_empty_success",
                observed_payload_records=count,
                capture_disposition=response_capture_disposition(fact['profile_refusal'], shape),
            ),
        )
    return ctx.disposition(
        "mapped_refusal_outcome" if found else "reference_documentation",
        "Provider refusal and native payload retained; empty results do not establish successful coverage"
        if found
        else "OpenFEC documentation context contains no qualified source response observation",
    )


def _rebuilt(ctx, table):
    # Owner decision 2026-10-30 (ss-j4vz B7-B9, Q6-Q8): these rows come from the
    # retained original, never from its receipt context.
    return ctx.disposition(
        "rebuilt_from_original", f"Built from the retained original by fec_original_tables as {table}; no context output"
    )


def map_research_context(collection, source_generation_pin):
    """Map supported source context; emit one explicit disposition otherwise."""
    ctx = _Context(collection, source_generation_pin)
    host = urlsplit(ctx.url or "").hostname
    path = urlsplit(ctx.url or "").path
    if ctx.collection_id.startswith("retained-ie-workbook-"):
        return _rebuilt(ctx, "fec_historical_ie_statistics")
    if "complete_records" in ctx.parsing or (host == "api.www.documentcloud.org" and path == "/api/documents/search/"):
        # Owner decision 2026-10-30 (ss-j4vz B10): truncated CSV previews and DocumentCloud
        # search results are not published as tables; their originals stay in the raw store.
        return ctx.disposition("retired_output", "Truncated CSV preview or third-party search results; no table output")
    if host == "www.fec.gov" and path == "/meetings/":
        return _rebuilt(ctx, "fec_research_meeting_observations")
    if (
        ctx.collection_id.startswith(("filing-format-", "retained-financial-reference-"))
        or host == "raw.githubusercontent.com"
    ):
        return ctx.disposition(
            "definition_context",
            "Native layout definitions, source code or methodological documentation; not economic observations",
        )
    if host == "api.open.fec.gov":
        return _response_outcome(ctx)
    if host == "efilingapps.fec.gov" and path == "/rss/generate":
        return _rebuilt(ctx, "fec_research_filing_feed_items")
    if host == "efilingapps.fec.gov" and path == "/registration/softwarelogs.htm":
        return ctx.disposition(
            "reference_documentation",
            "Filing software/vendor directory and developer requirements; retained source documentation, "
            "not report submission or receipt events. Original HTML and native reader refusal remain in context",
        )
    if ctx.parsing.get("status") == "literal-html-bytes":
        return ctx.disposition(
            "requires_native_reader",
            "Only retained HTML bytes are exposed; SpicyDocs must supply native events before semantic mapping",
        )
    if host == "docquery.fec.gov":
        if path in {"/cgi-bin/senate_forms/", "/senate/index.html"}:
            return ctx.disposition(
                "reference_inventory", "Senate filing archive inventory remains source discovery context"
            )
        return _rebuilt(ctx, "fec_research_source_pages")
    if host == "www.oversight.gov" and path.startswith("/reports/federal"):
        return ctx.disposition(
            "reference_inventory",
            "Oversight report index remains reference context; report editions are mapped by the agency owner",
        )
    if host == "www.fec.gov" and "court-case-alphabetical-index" in path:
        return ctx.disposition(
            "reference_inventory",
            "Court-case alphabetical index remains a source inventory, not duplicate case records",
        )
    if host == "www.fec.gov" and "/court-cases/" in path:
        return _rebuilt(ctx, "fec_research_source_pages")
    if host == "www.fec.gov" and "/data/legal/" in path:
        if any(t in path for t in ("/search/", "/advisory-opinions/")):
            return ctx.disposition("reference_inventory", "Legal search/interface page remains discovery context")
        return _rebuilt(ctx, "fec_research_source_pages")
    if host == "www.oversight.gov" and "/reports/" in path:
        return _rebuilt(ctx, "fec_research_source_pages")
    if path.lower().endswith(".pdf"):
        return ctx.disposition("deferred_pdf", "PDF body processing deferred")
    if not ctx.capture or ctx.parsing.get("status") == "physical-line-fallback":
        return ctx.disposition("provenance_context", "Selection/parse disposition; no new domain observation")
    if (
        ctx.collection_id.startswith(("archive-index-", "agency-archive-inventory-"))
        or "sitemap" in path
        or "index.commoncrawl.org" == host
    ):
        return ctx.disposition(
            "acquisition_inventory",
            "Retained archive/member/capture inventory supports discovery, not source dataset rows",
        )
    return ctx.disposition(
        "reference_or_research_context",
        "Retained source documentation, catalog/index or research discovery content; reference-only context is preserved",
    )
