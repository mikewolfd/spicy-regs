"""Typed Federal Register Layer 1 tables from immutable retained API pages.

The SpicyDocs reader owns field classification, source identity and traversal.
This module owns physical types, child rows, bounded offline candidate builds
(``build``) and production streaming R2-blob replay / incremental reconciliation
(``build_federal_register_typed_from_r2`` and ``merge_federal_register_typed``).
Originals remain the evidence for exact source spelling; dates and mixed
numeric/text CFR values each have one typed query column.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import date
import hashlib
import json
from os import getenv
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
import time
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import httpx
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.sources import r2
from spicy_regs.sources.publication import PUBLIC_HEADERS
from spicy_regs.transforms.table_merge import published_prior

DATES = frozenset({"publication_date", "effective_on", "comments_close_on", "signing_date"})
INTEGERS = frozenset({"start_page", "end_page", "volume", "executive_order_number"})
TEXT_LISTS = frozenset({"agency_names", "corrections", "docket_ids", "regulation_id_numbers", "topics"})
JSON_LISTS = frozenset({"agencies", "cfr_references"})
OBJECTS = frozenset({"president", "regulations_dot_gov_info"})
TEXT = frozenset({"abstract", "action", "body_html_url", "correction_of", "disposition_notes",
                  "executive_order_notes", "full_text_xml_url", "html_url", "pdf_url", "subtype", "title", "type"})
VERSION = "federal-register-layer1/1"
LINK_FIELDS = [("source_record_id", pa.string()), ("document_number", pa.string()),
               ("publication_date", pa.date32()), ("source_sha256", pa.string()), ("source_locator", pa.string())]

FAMILY = "federal-register-typed"
DOCUMENTS_TABLE = "federal_register_documents"
AGENCIES_TABLE = "federal_register_agencies"
CFR_REFERENCES_TABLE = "federal_register_cfr_references"
DOCKET_CANDIDATES_TABLE = "federal_register_docket_candidates"
TABLES: tuple[str, str, str, str] = (
    DOCUMENTS_TABLE,
    AGENCIES_TABLE,
    CFR_REFERENCES_TABLE,
    DOCKET_CANDIDATES_TABLE,
)
OUTPUTS: tuple[str, str, str, str] = (
    f"{DOCUMENTS_TABLE}.parquet",
    f"{AGENCIES_TABLE}.parquet",
    f"{CFR_REFERENCES_TABLE}.parquet",
    f"{DOCKET_CANDIDATES_TABLE}.parquet",
)

#: The nine fields omitted by legacy 22-field captures (field_set_id 08fa5de7c61e) vs current 31-field schema 1.3.
LEGACY_22_ABSENT_FIELDS = frozenset({
    "action",
    "correction_of",
    "corrections",
    "disposition_notes",
    "executive_order_notes",
    "full_text_xml_url",
    "president",
    "regulations_dot_gov_info",
    "significant",
})
#: The three fields added in schema 1.3 beyond the 28-field schema 1.2 capture.
SCHEMA_13_ADDED_FIELDS = frozenset({"disposition_notes", "executive_order_notes", "president"})

STREAM_FLUSH_ROWS = 50_000
ROW_GROUP_ROWS = 50_000
BLOB_FETCH_WORKERS = 8
BLOB_FETCH_ATTEMPTS = 4
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}\Z")

_ORDER_BY: dict[str, str] = {
    DOCUMENTS_TABLE: "publication_date DESC, document_number",
    AGENCIES_TABLE: "publication_date DESC, document_number, position",
    CFR_REFERENCES_TABLE: "publication_date DESC, document_number, position",
    DOCKET_CANDIDATES_TABLE: "publication_date DESC, document_number, label_position, candidate_position",
}


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _date(value: str | None) -> date | None:
    if value is None:
        return None
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(f"Noncanonical Federal Register date: {value!r}")
    return parsed


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_sha256(digest: str) -> str:
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise ValueError(f"Malformed Federal Register source_sha256 digest: {digest!r}")
    return digest


def document_schema() -> pa.Schema:
    from spicy_docs.sources.federal_register.native import DOCUMENT_FIELDS

    accounted = DATES | INTEGERS | TEXT_LISTS | JSON_LISTS | OBJECTS | TEXT | {"document_number", "significant"}
    if accounted != DOCUMENT_FIELDS:
        raise ValueError("Federal Register reader fields differ from the typed mapping")
    fields = dict(LINK_FIELDS)
    for name in sorted(DOCUMENT_FIELDS - fields.keys()):
        fields[name] = (pa.date32() if name in DATES else pa.int64() if name in INTEGERS else
                        pa.bool_() if name == "significant" else pa.list_(pa.string())
                        if name in TEXT_LISTS | JSON_LISTS else pa.string())
    fields["member_names"] = pa.list_(pa.string())
    return pa.schema(fields)


def agencies_schema() -> pa.Schema:
    children_common = [*LINK_FIELDS, ("position", pa.int32()), ("member_names", pa.list_(pa.string()))]
    return pa.schema([
        *children_common,
        ("id", pa.int64()),
        ("parent_id", pa.int64()),
        *[(name, pa.string()) for name in ("json_url", "name", "raw_name", "slug", "url")],
    ])


def cfr_references_schema() -> pa.Schema:
    children_common = [*LINK_FIELDS, ("position", pa.int32()), ("member_names", pa.list_(pa.string()))]
    return pa.schema([
        *children_common,
        ("title", pa.int64()),
        *[(name, pa.string()) for name in ("part", "chapter", "citation_url")],
    ])


def docket_candidates_schema() -> pa.Schema:
    return pa.schema([
        *LINK_FIELDS,
        ("label_position", pa.int32()),
        ("candidate_position", pa.int32()),
        ("label", pa.string()),
        ("docket_id", pa.string()),
    ])


def layer1_schemas() -> dict[str, pa.Schema]:
    """Return exact Arrow schemas for all four Federal Register Layer 1 tables."""
    return {
        DOCUMENTS_TABLE: document_schema(),
        AGENCIES_TABLE: agencies_schema(),
        CFR_REFERENCES_TABLE: cfr_references_schema(),
        DOCKET_CANDIDATES_TABLE: docket_candidates_schema(),
    }


def normalized_document(record: dict, sha256: str, locator: str) -> dict:
    from spicy_docs.sources.federal_register.native import classify_document, federal_register_source_record_id

    classified = classify_document(record)
    row = dict(classified)
    for name in DATES:
        row[name] = _date(record.get(name))
    number = record.get("executive_order_number")
    if number is not None:
        if not number.isascii() or not number.isdecimal():
            raise ValueError(f"Unsupported Federal Register executive_order_number: {number!r}")
        row["executive_order_number"] = int(number)
    for name in JSON_LISTS:
        values = record.get(name)
        row[name] = None if values is None else [None if item is None else _json(item) for item in values]
    for name in OBJECTS:
        row[name] = None if record.get(name) is None else _json(record[name])
    return {**row, "source_record_id": federal_register_source_record_id(classified), "source_sha256": sha256,
            "source_locator": locator, "member_names": list(record)}


def _children(record: dict, parent: dict, member: str) -> list[dict]:
    rows = []
    for position, value in enumerate(record.get(member) or []):
        fields: dict[str, Any] = {}
        for name, item in value.items():
            numeric = name in ({"id", "parent_id"} if member == "agencies" else {"title"})
            fields[name] = (None if item is None else int(item)) if numeric else (
                None if item is None else str(item))
        rows.append({**{name: parent[name] for name, _ in LINK_FIELDS}, **fields, "position": position,
                     "member_names": list(value), "source_locator": parent["source_locator"] + f"/{member}/{position}"})
    return rows


def _docket_candidates(record: dict, parent: dict) -> list[dict]:
    from spicy_regs.ontology.federal_register import linked_docket_ids

    candidates = []
    for label_position, label in enumerate(record.get("docket_ids") or []):
        for candidate_position, docket in enumerate(linked_docket_ids(label)):
            candidates.append({
                **{name: parent[name] for name, _ in LINK_FIELDS},
                "label_position": label_position,
                "candidate_position": candidate_position,
                "label": label,
                "docket_id": docket,
            })
    return candidates


def validate_page_field_set(
    response: Mapping[str, Any],
    *,
    sha256: str,
    require_full_field_set: bool = True,
) -> None:
    """Validate that a retained API page matches the current 31-field schema 1.3 set.

    Refuses legacy 22-field or pre-1.3 (28-field) pages and any cursor URL carrying
    a drifted ``fields[]`` parameter.
    """
    from spicy_docs.sources.federal_register import native

    requested_fields = sorted(native.DOCUMENT_FIELDS)
    for cursor_key in ("next_page_url", "previous_page_url"):
        cursor = response.get(cursor_key)
        if isinstance(cursor, str) and cursor:
            params = parse_qs(urlsplit(cursor).query, keep_blank_values=True)
            if "fields[]" in params and sorted(params["fields[]"]) != requested_fields:
                missing = sorted(native.DOCUMENT_FIELDS - set(params["fields[]"]))
                if LEGACY_22_ABSENT_FIELDS <= set(missing):
                    raise ValueError(
                        f"Refusing legacy 22-field Federal Register page {sha256} ({cursor_key} fields[] drift)"
                    )
                raise ValueError(
                    f"Federal Register page {sha256} {cursor_key} field set differs from current 31 fields"
                )
    if not require_full_field_set:
        return
    results = response.get("results") or []
    if not results:
        return
    observed: set[str] = set()
    for record in results:
        if not isinstance(record, Mapping):
            raise ValueError(f"Federal Register page {sha256} contains a non-object result entry")
        observed.update(record.keys())
    unknown = sorted(observed - native.DOCUMENT_FIELDS)
    if unknown:
        raise ValueError(f"Federal Register page {sha256} contains unclassified fields: {unknown}")
    missing_fields = native.DOCUMENT_FIELDS - observed
    if missing_fields:
        if LEGACY_22_ABSENT_FIELDS <= missing_fields:
            raise ValueError(
                f"Refusing legacy 22-field Federal Register page {sha256}: missing fields {sorted(missing_fields)}"
            )
        if SCHEMA_13_ADDED_FIELDS <= missing_fields:
            raise ValueError(
                f"Refusing legacy pre-1.3 Federal Register page {sha256}: missing fields {sorted(missing_fields)}"
            )
        raise ValueError(
            f"Federal Register page {sha256} lacks current 31-field members: {sorted(missing_fields)}"
        )


def iter_typed_page_records(
    page_bytes: bytes,
    sha256: str,
    *,
    selected_keys: set[tuple[str, str]] | None = None,
    require_full_field_set: bool = True,
) -> Iterator[tuple[dict, list[dict], list[dict], list[dict]]]:
    """Verify one immutable API JSON page and yield typed document + child rows.

    Yields ``(document_row, agency_rows, cfr_rows, docket_candidate_rows)`` for each
    distinct dated document in the page (or only those matching ``selected_keys`` when
    provided). Identical same-date repeats within the page collapse; conflicting
    same-date observations refuse. When ``selected_keys`` is provided, every requested
    ``(document_number, publication_date)`` key must be observed in the page.
    """
    from spicy_docs.sources.federal_register import native

    _validate_sha256(sha256)
    if not isinstance(page_bytes, (bytes, bytearray)) or not page_bytes:
        raise ValueError(f"Federal Register page {sha256} is empty")
    if len(page_bytes) > native.MAX_PAGE_BYTES:
        raise ValueError(f"Federal Register page {sha256} exceeds max byte bound ({len(page_bytes)} bytes)")
    actual_sha = "sha256:" + hashlib.sha256(page_bytes).hexdigest()
    if actual_sha != sha256:
        raise ValueError(f"Retained Federal Register original digest differs: expected {sha256}, got {actual_sha}")

    response = native.parse_page_response(bytes(page_bytes))
    validate_page_field_set(response, sha256=sha256, require_full_field_set=require_full_field_set)

    seen_in_page: dict[str, str] = {}
    matched_keys: set[tuple[str, str]] = set()
    for position, record in enumerate(response["results"]):
        if not isinstance(record, dict):
            raise ValueError(f"Federal Register page {sha256} result at /results/{position} is not a JSON object")
        doc_num = record.get("document_number")
        pub_date = record.get("publication_date")
        if selected_keys is not None and (doc_num, pub_date) not in selected_keys:
            continue
        row = normalized_document(record, sha256, f"/results/{position}")
        identity = row["source_record_id"]
        fingerprint = json.dumps(record, sort_keys=True)
        if identity in seen_in_page:
            if seen_in_page[identity] != fingerprint:
                raise ValueError(f"Conflicting Federal Register observations for {identity}")
            continue
        seen_in_page[identity] = fingerprint
        matched_keys.add((row["document_number"], str(pub_date)))
        yield (
            row,
            _children(record, row, "agencies"),
            _children(record, row, "cfr_references"),
            _docket_candidates(record, row),
        )

    if selected_keys is not None:
        missing_keys = selected_keys - matched_keys
        if missing_keys:
            sample = sorted(missing_keys)[:5]
            raise ValueError(
                f"Federal Register blob {sha256} is missing {len(missing_keys)} expected key(s), e.g. {sample!r}"
            )


def _fetch_r2_blob_http(
    client: httpx.Client,
    base_url: str,
    sha256: str,
    *,
    max_bytes: int,
    attempts: int = BLOB_FETCH_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Fetch one immutable R2 source-evidence blob over HTTP with retry/backoff and SHA-256 verification."""
    digest_hex = _validate_sha256(sha256).removeprefix("sha256:")
    url = f"{base_url.rstrip('/')}/source-evidence/blobs/sha256/{digest_hex}"
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            with client.stream("GET", url) as response:
                if response.status_code in _RETRYABLE_STATUSES and attempt + 1 < attempts:
                    sleep(0.5 * (2**attempt))
                    continue
                response.raise_for_status()
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes(chunk_size=262_144):
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"Federal Register R2 blob {sha256} exceeds max byte bound ({max_bytes})")
                    chunks.append(chunk)
                raw = b"".join(chunks)
            actual = "sha256:" + hashlib.sha256(raw).hexdigest()
            if actual != sha256:
                raise ValueError(f"Federal Register R2 blob digest mismatch for {sha256}: got {actual}")
            return raw
        except httpx.TransportError as exc:
            last_error = exc
            if attempt + 1 < attempts:
                sleep(0.5 * (2**attempt))
                continue
            raise
    assert last_error is not None
    raise last_error


def _fetch_r2_blob_s3(s3_client: Any, bucket: str, sha256: str, *, max_bytes: int) -> bytes:
    """Fetch one immutable R2 source-evidence blob via S3 API with byte-bound and SHA-256 verification."""
    digest_hex = _validate_sha256(sha256).removeprefix("sha256:")
    key = f"source-evidence/blobs/sha256/{digest_hex}"
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]
    try:
        raw = body.read(max_bytes + 1)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()
    if len(raw) > max_bytes:
        raise ValueError(f"Federal Register R2 blob {sha256} exceeds max byte bound ({max_bytes})")
    actual = "sha256:" + hashlib.sha256(raw).hexdigest()
    if actual != sha256:
        raise ValueError(f"Federal Register R2 blob digest mismatch for {sha256}: got {actual}")
    return bytes(raw)


def _configure_duckdb(con: Any, spill_dir: Path) -> None:
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory={_sql_quote(str(spill_dir))}")


def _validate_and_load_target_keys(con: Any, fr_path: Path) -> int:
    """Validate pinned ``federal_register.parquet`` and materialize ``_target_keys`` in DuckDB."""
    if not fr_path.is_file():
        raise FileNotFoundError(f"federal_register.parquet not found: {fr_path}")
    fr_sql = _sql_quote(str(fr_path))
    schema = pq.read_schema(fr_path)
    missing_cols = [c for c in ("document_number", "publication_date", "source_sha256") if c not in schema.names]
    if missing_cols:
        raise ValueError(f"federal_register.parquet is missing required columns {missing_cols}")

    bad_identity = con.execute(
        f"""SELECT document_number, publication_date FROM read_parquet({fr_sql})
            WHERE document_number IS NULL OR trim(document_number) = ''
               OR publication_date IS NULL
               OR try_cast(publication_date AS DATE) IS NULL
               OR cast(try_cast(publication_date AS DATE) AS VARCHAR) <> publication_date
            LIMIT 1"""
    ).fetchone()
    if bad_identity is not None:
        raise ValueError(f"federal_register.parquet contains invalid document identity: {bad_identity!r}")

    bad_sha = con.execute(
        f"""SELECT count(*), min(document_number) FROM read_parquet({fr_sql})
            WHERE source_sha256 IS NULL
               OR NOT regexp_full_match(source_sha256, '^sha256:[0-9a-f]{{64}}$')"""
    ).fetchone()
    if bad_sha is not None and bad_sha[0] > 0:
        raise ValueError(
            f"Refusing federal-register-typed build: {bad_sha[0]:,} row(s) in federal_register.parquet "
            f"have NULL or malformed source_sha256 (e.g. {bad_sha[1]!r})"
        )

    con.execute(
        f"""CREATE TEMP TABLE _target_keys AS
            SELECT document_number,
                   CAST(publication_date AS DATE) AS publication_date,
                   publication_date AS publication_date_str,
                   source_sha256
            FROM read_parquet({fr_sql})"""
    )
    total, distinct_keys = con.execute(
        "SELECT count(*), count(DISTINCT (document_number, publication_date)) FROM _target_keys"
    ).fetchone()
    if total != distinct_keys:
        raise ValueError(
            f"federal_register.parquet repeats (document_number, publication_date) keys: {total} rows vs {distinct_keys} keys"
        )
    return int(total)


def _validate_priors(con: Any, prior_paths: Mapping[str, Path], schemas: Mapping[str, pa.Schema]) -> bool:
    """Return True if all four prior files exist and match declared schemas; raise on internal prior corruption."""
    for name in TABLES:
        path = prior_paths[name]
        if not path.is_file():
            return False
        if not pq.read_schema(path).equals(schemas[name], check_metadata=False):
            logger.info("FR typed: prior {} schema differs from current Layer 1 schema; triggering full rebuild", name)
            return False

    docs_sql = _sql_quote(str(prior_paths[DOCUMENTS_TABLE]))
    total, distinct_keys = con.execute(
        f"SELECT count(*), count(DISTINCT (document_number, publication_date)) FROM read_parquet({docs_sql})"
    ).fetchone()
    if total != distinct_keys:
        raise ValueError(
            f"Prior {DOCUMENTS_TABLE}.parquet repeats (document_number, publication_date) keys: "
            f"{total} rows vs {distinct_keys} keys"
        )
    for child in (AGENCIES_TABLE, CFR_REFERENCES_TABLE, DOCKET_CANDIDATES_TABLE):
        child_sql = _sql_quote(str(prior_paths[child]))
        (orphans,) = con.execute(
            f"""SELECT count(*) FROM read_parquet({child_sql}) c
                ANTI JOIN read_parquet({docs_sql}) p
                  ON c.document_number = p.document_number AND c.publication_date = p.publication_date"""
        ).fetchone()
        if orphans:
            raise ValueError(f"Prior {child}.parquet contains {orphans:,} orphan row(s) missing from prior documents")
    return True


def _stream_blobs_to_staging(
    needed_keys_by_sha: Mapping[str, set[tuple[str, str]]],
    staging_paths: Mapping[str, Path],
    schemas: Mapping[str, pa.Schema],
    *,
    fetch_blob: Callable[[str], bytes],
    flush_rows: int = STREAM_FLUSH_ROWS,
    max_workers: int = BLOB_FETCH_WORKERS,
    require_full_field_set: bool = True,
) -> dict[str, int]:
    """Fetch needed R2 blobs with bounded in-flight concurrency and stream all four tables to staging Parquet."""
    if isinstance(flush_rows, bool) or not isinstance(flush_rows, int) or flush_rows < 1:
        raise ValueError(f"flush_rows must be a positive integer, got {flush_rows!r}")
    if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1:
        raise ValueError(f"max_workers must be a positive integer, got {max_workers!r}")

    meta_schemas = {
        name: schema.with_metadata({"layer1.version": VERSION}) for name, schema in schemas.items()
    }
    buffers: dict[str, list[dict]] = {name: [] for name in TABLES}
    counts: dict[str, int] = {name: 0 for name in TABLES}
    writers: dict[str, pq.ParquetWriter] = {}

    def flush(name: str, *, force_empty: bool = False) -> None:
        buf = buffers[name]
        if buf:
            writers[name].write_table(
                pa.Table.from_pylist(buf, schema=meta_schemas[name]),
                row_group_size=ROW_GROUP_ROWS,
            )
            counts[name] += len(buf)
            buf.clear()
        elif force_empty and counts[name] == 0:
            writers[name].write_table(meta_schemas[name].empty_table())

    try:
        for name in TABLES:
            writers[name] = pq.ParquetWriter(staging_paths[name], meta_schemas[name], compression="zstd")

        ordered_digests = sorted(needed_keys_by_sha)
        if ordered_digests:
            workers = min(max_workers, len(ordered_digests))
            max_in_flight = max(workers * 2, 1)
            digest_iter = iter(ordered_digests)
            in_flight: dict[Future[bytes], str] = {}

            with ThreadPoolExecutor(max_workers=workers) as executor:
                def submit_next() -> bool:
                    try:
                        digest = next(digest_iter)
                    except StopIteration:
                        return False
                    in_flight[executor.submit(fetch_blob, digest)] = digest
                    return True

                for _ in range(max_in_flight):
                    if not submit_next():
                        break

                while in_flight:
                    done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in done:
                        digest = in_flight.pop(future)
                        raw_bytes = future.result()
                        for doc_row, ag_rows, cfr_rows, dkt_rows in iter_typed_page_records(
                            raw_bytes,
                            digest,
                            selected_keys=needed_keys_by_sha[digest],
                            require_full_field_set=require_full_field_set,
                        ):
                            buffers[DOCUMENTS_TABLE].append(doc_row)
                            if ag_rows:
                                buffers[AGENCIES_TABLE].extend(ag_rows)
                            if cfr_rows:
                                buffers[CFR_REFERENCES_TABLE].extend(cfr_rows)
                            if dkt_rows:
                                buffers[DOCKET_CANDIDATES_TABLE].extend(dkt_rows)
                        del raw_bytes
                        for name in TABLES:
                            if len(buffers[name]) >= flush_rows:
                                flush(name)
                        submit_next()

        for name in TABLES:
            flush(name, force_empty=True)
    finally:
        for writer in writers.values():
            writer.close()

    return counts


def _write_ordered_table(
    con: Any,
    *,
    name: str,
    schema: pa.Schema,
    fresh_path: Path,
    prior_path: Path | None,
    merged_path: Path,
) -> int:
    """Sort and (optionally) anti-join merge one table in DuckDB, streaming 50,000-row batches to Parquet."""
    meta_schema = schema.with_metadata({"layer1.version": VERSION})
    cols = ", ".join(f'"{col}"' for col in schema.names)
    pcols = ", ".join(f'p."{col}"' for col in schema.names)
    fresh_sql = _sql_quote(str(fresh_path))
    order_by = _ORDER_BY[name]

    if prior_path is not None:
        prior_sql = _sql_quote(str(prior_path))
        query = f"""
            SELECT {cols} FROM (
                SELECT {cols} FROM read_parquet({fresh_sql})
                UNION ALL
                SELECT {pcols}
                FROM read_parquet({prior_sql}) p
                JOIN _carry_keys c
                  ON p.document_number = c.document_number AND p.publication_date = c.publication_date
            )
            ORDER BY {order_by}
        """
    else:
        query = f"SELECT {cols} FROM read_parquet({fresh_sql}) ORDER BY {order_by}"

    reader = con.execute(query).to_arrow_reader(ROW_GROUP_ROWS)
    total = 0
    with pq.ParquetWriter(merged_path, meta_schema, compression="zstd") as writer:
        for batch in reader:
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch]).cast(meta_schema)
            writer.write_table(table, row_group_size=ROW_GROUP_ROWS)
            total += table.num_rows
        if total == 0:
            writer.write_table(meta_schema.empty_table())
    return total


def _reconcile_merged_tables(con: Any, merged_paths: Mapping[str, Path], expected_doc_count: int) -> None:
    """Assert exact 1-to-1 key/SHA parity with ``federal_register.parquet`` and zero orphan child rows."""
    docs_sql = _sql_quote(str(merged_paths[DOCUMENTS_TABLE]))
    total_docs, distinct_docs = con.execute(
        f"SELECT count(*), count(DISTINCT (document_number, publication_date)) FROM read_parquet({docs_sql})"
    ).fetchone()
    if total_docs != distinct_docs:
        raise ValueError(
            f"Merged {DOCUMENTS_TABLE}.parquet repeats (document_number, publication_date) keys: "
            f"{total_docs} rows vs {distinct_docs} keys"
        )
    if total_docs != expected_doc_count:
        raise ValueError(
            f"Merged {DOCUMENTS_TABLE}.parquet row count ({total_docs:,}) differs from "
            f"federal_register.parquet ({expected_doc_count:,})"
        )

    (missing_in_typed,) = con.execute(
        f"""SELECT count(*) FROM (
                SELECT document_number, publication_date FROM _target_keys
                EXCEPT
                SELECT document_number, publication_date FROM read_parquet({docs_sql})
            )"""
    ).fetchone()
    (extra_in_typed,) = con.execute(
        f"""SELECT count(*) FROM (
                SELECT document_number, publication_date FROM read_parquet({docs_sql})
                EXCEPT
                SELECT document_number, publication_date FROM _target_keys
            )"""
    ).fetchone()
    if missing_in_typed or extra_in_typed:
        raise ValueError(
            f"{DOCUMENTS_TABLE}.parquet key set differs from federal_register.parquet: "
            f"{missing_in_typed:,} missing, {extra_in_typed:,} extra"
        )

    (sha_mismatches,) = con.execute(
        f"""SELECT count(*) FROM read_parquet({docs_sql}) d
            JOIN _target_keys t
              ON d.document_number = t.document_number AND d.publication_date = t.publication_date
            WHERE d.source_sha256 <> t.source_sha256"""
    ).fetchone()
    if sha_mismatches:
        raise ValueError(
            f"{DOCUMENTS_TABLE}.parquet has {sha_mismatches:,} row(s) whose source_sha256 differs "
            "from federal_register.parquet"
        )

    for child in (AGENCIES_TABLE, CFR_REFERENCES_TABLE, DOCKET_CANDIDATES_TABLE):
        child_sql = _sql_quote(str(merged_paths[child]))
        (orphans,) = con.execute(
            f"""SELECT count(*) FROM read_parquet({child_sql}) c
                ANTI JOIN read_parquet({docs_sql}) d
                  ON c.document_number = d.document_number AND c.publication_date = d.publication_date"""
        ).fetchone()
        if orphans:
            raise ValueError(f"Merged {child}.parquet contains {orphans:,} orphan row(s) missing from {DOCUMENTS_TABLE}")


def _build_or_merge_typed(
    output_dir: Path,
    *,
    fr_path: Path | None = None,
    public_url: str | None = None,
    fetch_blob: Callable[[str], bytes] | None = None,
    s3_client: Any | None = None,
    bucket: str | None = None,
    download_prior: Callable[[str, Path], bool] | None = None,
    incremental: bool = True,
    force_rebuild: bool = False,
    flush_rows: int = STREAM_FLUSH_ROWS,
    max_workers: int = BLOB_FETCH_WORKERS,
    require_full_field_set: bool = True,
) -> tuple[Path, Path, Path, Path]:
    import duckdb
    from spicy_docs.sources.federal_register import native

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    source_fr = Path(fr_path) if fr_path is not None else (output_dir / "federal_register.parquet")
    schemas = layer1_schemas()

    work_dir = output_dir / f".fr-typed-work-{uuid4().hex}"
    spill_dir = work_dir / "duckdb_tmp"
    prior_dir = work_dir / "prior"
    fresh_dir = work_dir / "fresh"
    merged_dir = work_dir / "merged"
    for directory in (spill_dir, prior_dir, fresh_dir, merged_dir):
        directory.mkdir(parents=True, exist_ok=True)

    prior_paths = {name: prior_dir / f"{name}.parquet" for name in TABLES}
    fresh_paths = {name: fresh_dir / f"{name}.parquet" for name in TABLES}
    merged_paths = {name: merged_dir / f"{name}.parquet" for name in TABLES}
    out_paths = {name: output_dir / f"{name}.parquet" for name in TABLES}

    con = duckdb.connect()
    try:
        _configure_duckdb(con, spill_dir)
        target_count = _validate_and_load_target_keys(con, source_fr)

        use_prior = False
        if incremental and not force_rebuild:
            if download_prior is not None:
                downloaded = all(bool(download_prior(f"{name}.parquet", prior_paths[name])) for name in TABLES)
            else:
                downloaded = False
            if not downloaded and all(out_paths[name].is_file() for name in TABLES):
                for name in TABLES:
                    shutil.copy2(out_paths[name], prior_paths[name])
                downloaded = True
            if downloaded:
                use_prior = _validate_priors(con, prior_paths, schemas)

        needed_keys_by_sha: dict[str, set[tuple[str, str]]] = {}
        if use_prior:
            prior_docs_sql = _sql_quote(str(prior_paths[DOCUMENTS_TABLE]))
            con.execute(
                f"""CREATE TEMP TABLE _needed_rows AS
                    SELECT t.document_number, t.publication_date, t.publication_date_str, t.source_sha256
                    FROM _target_keys t
                    LEFT JOIN (
                        SELECT document_number, publication_date, source_sha256
                        FROM read_parquet({prior_docs_sql})
                    ) p
                      ON t.document_number = p.document_number AND t.publication_date = p.publication_date
                    WHERE p.document_number IS NULL OR p.source_sha256 <> t.source_sha256"""
            )
            for doc_num, pub_date_str, sha256 in con.execute(
                "SELECT document_number, publication_date_str, source_sha256 FROM _needed_rows"
            ).fetchall():
                needed_keys_by_sha.setdefault(sha256, set()).add((doc_num, pub_date_str))
            removed_row = con.execute(
                f"""SELECT count(*) FROM read_parquet({prior_docs_sql}) p
                    ANTI JOIN _target_keys t
                      ON p.document_number = t.document_number AND p.publication_date = t.publication_date"""
            ).fetchone()
            removed_count = int(removed_row[0]) if removed_row is not None else 0
            logger.info(
                "FR typed incremental: {:,} target rows, {:,} row(s) across {:,} changed/new blob(s), {:,} removed",
                target_count,
                sum(len(keys) for keys in needed_keys_by_sha.values()),
                len(needed_keys_by_sha),
                removed_count,
            )
        else:
            for doc_num, pub_date_str, sha256 in con.execute(
                "SELECT document_number, publication_date_str, source_sha256 FROM _target_keys"
            ).fetchall():
                needed_keys_by_sha.setdefault(sha256, set()).add((doc_num, pub_date_str))
            logger.info(
                "FR typed bootstrap: {:,} target rows across {:,} distinct R2 blob(s)",
                target_count,
                len(needed_keys_by_sha),
            )

        if fetch_blob is not None:
            _stream_blobs_to_staging(
                needed_keys_by_sha,
                fresh_paths,
                schemas,
                fetch_blob=fetch_blob,
                flush_rows=flush_rows,
                max_workers=max_workers,
                require_full_field_set=require_full_field_set,
            )
        elif s3_client is not None:
            resolved_bucket = bucket or getenv("R2_BUCKET_NAME", r2.VERIFIED_BUCKET)
            _stream_blobs_to_staging(
                needed_keys_by_sha,
                fresh_paths,
                schemas,
                fetch_blob=lambda digest: _fetch_r2_blob_s3(
                    s3_client, resolved_bucket, digest, max_bytes=native.MAX_PAGE_BYTES
                ),
                flush_rows=flush_rows,
                max_workers=max_workers,
                require_full_field_set=require_full_field_set,
            )
        else:
            resolved_base_url = public_url or getenv("R2_PUBLIC_URL") or r2.VERIFIED_PUBLIC_URL
            limits = httpx.Limits(
                max_connections=max_workers + 4,
                max_keepalive_connections=max_workers + 4,
            )
            headers = {**PUBLIC_HEADERS, "Accept-Encoding": "identity"}
            with httpx.Client(headers=headers, timeout=120.0, limits=limits, follow_redirects=True) as client:
                _stream_blobs_to_staging(
                    needed_keys_by_sha,
                    fresh_paths,
                    schemas,
                    fetch_blob=lambda digest: _fetch_r2_blob_http(
                        client, resolved_base_url, digest, max_bytes=native.MAX_PAGE_BYTES
                    ),
                    flush_rows=flush_rows,
                    max_workers=max_workers,
                    require_full_field_set=require_full_field_set,
                )

        if use_prior:
            fresh_docs_sql = _sql_quote(str(fresh_paths[DOCUMENTS_TABLE]))
            con.execute(
                f"""CREATE TEMP TABLE _fresh_keys AS
                    SELECT document_number, publication_date FROM read_parquet({fresh_docs_sql})"""
            )
            con.execute(
                """CREATE TEMP TABLE _carry_keys AS
                   SELECT t.document_number, t.publication_date
                   FROM _target_keys t
                   ANTI JOIN _fresh_keys f
                     ON t.document_number = f.document_number AND t.publication_date = f.publication_date"""
            )

        written_counts: dict[str, int] = {}
        for name in TABLES:
            written_counts[name] = _write_ordered_table(
                con,
                name=name,
                schema=schemas[name],
                fresh_path=fresh_paths[name],
                prior_path=prior_paths[name] if use_prior else None,
                merged_path=merged_paths[name],
            )

        _reconcile_merged_tables(con, merged_paths, target_count)

        for name in TABLES:
            merged_paths[name].replace(out_paths[name])

        logger.info(
            "FR typed complete: documents={:,}, agencies={:,}, cfr_references={:,}, docket_candidates={:,}",
            written_counts[DOCUMENTS_TABLE],
            written_counts[AGENCIES_TABLE],
            written_counts[CFR_REFERENCES_TABLE],
            written_counts[DOCKET_CANDIDATES_TABLE],
        )
        return (
            out_paths[DOCUMENTS_TABLE],
            out_paths[AGENCIES_TABLE],
            out_paths[CFR_REFERENCES_TABLE],
            out_paths[DOCKET_CANDIDATES_TABLE],
        )
    finally:
        con.close()
        shutil.rmtree(work_dir, ignore_errors=True)


def build_federal_register_typed_from_r2(
    output_dir: Path,
    *,
    fr_path: Path | None = None,
    public_url: str | None = None,
    fetch_blob: Callable[[str], bytes] | None = None,
    s3_client: Any | None = None,
    bucket: str | None = None,
    flush_rows: int = STREAM_FLUSH_ROWS,
    max_workers: int = BLOB_FETCH_WORKERS,
    require_full_field_set: bool = True,
) -> tuple[Path, Path, Path, Path]:
    """Bootstrap all four typed Layer 1 Parquet tables from immutable R2 source-evidence blobs.

    Reads ``federal_register.parquet`` (zero external ``federalregister.gov`` calls),
    fetches every distinct ``source_sha256`` blob from ``source-evidence/blobs/sha256/<hex>``,
    verifies SHA-256 digests and 31-field completeness, streams all four tables with bounded
    memory, and asserts exact 1-to-1 ``(document_number, publication_date)`` key parity.
    """
    return _build_or_merge_typed(
        output_dir,
        fr_path=fr_path,
        public_url=public_url,
        fetch_blob=fetch_blob,
        s3_client=s3_client,
        bucket=bucket,
        download_prior=None,
        incremental=False,
        force_rebuild=True,
        flush_rows=flush_rows,
        max_workers=max_workers,
        require_full_field_set=require_full_field_set,
    )


def merge_federal_register_typed(
    output_dir: Path,
    *,
    fr_path: Path | None = None,
    public_url: str | None = None,
    fetch_blob: Callable[[str], bytes] | None = None,
    s3_client: Any | None = None,
    bucket: str | None = None,
    download_prior: Callable[[str, Path], bool] = published_prior,
    force_rebuild: bool = False,
    flush_rows: int = STREAM_FLUSH_ROWS,
    max_workers: int = BLOB_FETCH_WORKERS,
    require_full_field_set: bool = True,
) -> tuple[Path, Path, Path, Path]:
    """Build or incrementally merge all four typed Layer 1 tables from pinned ``federal_register.parquet``.

    When all four prior published tables are available and match the declared Layer 1
    schemas (unless ``force_rebuild=True``), only newly added or changed ``source_sha256``
    blobs are fetched from R2 ``source-evidence/blobs/sha256/<hex>``, and all four tables
    are reconciled atomically in DuckDB by ``(document_number, publication_date)``.
    """
    return _build_or_merge_typed(
        output_dir,
        fr_path=fr_path,
        public_url=public_url,
        fetch_blob=fetch_blob,
        s3_client=s3_client,
        bucket=bucket,
        download_prior=download_prior,
        incremental=True,
        force_rebuild=force_rebuild,
        flush_rows=flush_rows,
        max_workers=max_workers,
        require_full_field_set=require_full_field_set,
    )


def build(manifest_path: Path, archive_root: Path, output_dir: Path, *, max_records: int = 10000) -> Path:
    """Validate a finite held traversal, then write one source table and ordered children.

    Identical dated records collapse; conflicting observations refuse. Every
    distinct input URL is required exactly once, and every selected page must be
    consumed. A failed validation writes no candidate output directory.
    """
    from spicy_docs.sources.federal_register import native
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    if not 1 <= max_records <= 10000:
        raise ValueError("A Federal Register candidate requires max_records between 1 and 10000")
    destination = output_dir.resolve()
    if destination.is_relative_to(archive_root.resolve()):
        raise ValueError("Federal Register candidate output must be outside the raw archive")
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(output_dir)
    output_dir = destination
    schemas = layer1_schemas()
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    pages = manifest["pages"]
    if not isinstance(pages, list) or not 1 <= len(pages) <= 100:
        raise ValueError("A Federal Register candidate requires 1 to 100 retained pages")
    by_url = {page["url"]: page for page in pages}
    if len(by_url) != len(pages):
        raise ValueError("Repeated Federal Register request cursor in retained manifest")
    requested_fields = sorted(native.DOCUMENT_FIELDS)
    for url in by_url:
        fields = parse_qs(urlsplit(url).query, keep_blank_values=True).get("fields[]", [])
        if sorted(fields) != requested_fields:
            raise ValueError("Federal Register request field set differs from current fields")
    archive = CentralRawArchiveStore(archive_root, create=False)
    used: dict[str, dict] = {}

    def fetch(url: str) -> bytes:
        if url not in by_url:
            raise ValueError(f"Required Federal Register page is not retained in this manifest: {url}")
        item = by_url[url]
        with archive.open(item["sha256"]) as stream:
            raw = stream.read(native.MAX_PAGE_BYTES + 1)
        if "sha256:" + hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("Retained Federal Register original digest differs")
        used[url] = {**item, "bytes": len(raw)}
        return raw

    rows, agencies, cfr, candidates = [], [], [], []
    identities: dict[str, str] = {}
    members: Counter = Counter()
    nulls: Counter = Counter()
    populated: Counter = Counter()
    duplicate_records = 0
    observed_records = 0
    window = None
    for page in native.iter_federal_register_pages(fetch, query_scope=manifest["query_scope"], traversals=1):
        response = native.parse_page_response(page.response_bytes)
        if page.source_cursor is None:
            window = native.federal_register_request_window(page.request_key)
        # The owner's capped probes contain no admitted records; split leaf windows follow them.
        if response["count"] >= native.RESULT_CAP:
            continue
        for position, record in enumerate(response["results"]):
            observed_records += 1
            if observed_records > max_records:
                raise ValueError("Federal Register candidate exceeds max_records")
            if window is not None and not window.published_from.isoformat() <= record["publication_date"] <= window.published_through.isoformat():
                raise ValueError("Federal Register record is outside its requested window")
            if manifest["query_scope"].get("presidentialDocumentType") and record.get("subtype") != "Executive Order":
                raise ValueError("Federal Register record differs from the executive-order predicate")
            sha256 = used[page.request_key]["sha256"]
            row = normalized_document(record, sha256, f"/results/{position}")
            identity, fingerprint = row["source_record_id"], json.dumps(record, sort_keys=True)
            if identity in identities:
                if identities[identity] != fingerprint:
                    raise ValueError(f"Conflicting Federal Register observations for {identity}")
                duplicate_records += 1
                continue
            identities[identity] = fingerprint
            rows.append(row)
            members.update(record.keys())
            nulls.update(name for name, value in record.items() if value is None)
            populated.update(name for name, value in record.items() if value is not None and value != [] and value != {})
            agencies.extend(_children(record, row, "agencies"))
            cfr.extend(_children(record, row, "cfr_references"))
            candidates.extend(_docket_candidates(record, row))
    if set(used) != set(by_url):
        raise ValueError("Retained Federal Register manifest contains unconsumed pages")
    tables = {
        DOCUMENTS_TABLE: (rows, schemas[DOCUMENTS_TABLE]),
        AGENCIES_TABLE: (agencies, schemas[AGENCIES_TABLE]),
        CFR_REFERENCES_TABLE: (cfr, schemas[CFR_REFERENCES_TABLE]),
        DOCKET_CANDIDATES_TABLE: (candidates, schemas[DOCKET_CANDIDATES_TABLE]),
    }
    # Convert before creating the output, so invalid physical values cannot leave a successful-looking candidate.
    converted = {name: pa.Table.from_pylist(data, schema=table_schema) for name, (data, table_schema) in tables.items()}
    receipt = {"version": VERSION, "reader_schema": native.SCHEMA_VERSION, "reader_schema_digest": native.source_schema_digest(),
               "input_manifest_sha256": "sha256:" + hashlib.sha256(manifest_bytes).hexdigest(),
               "requested_fields": requested_fields, "query_scope": manifest["query_scope"],
               "observed_records": observed_records, "duplicate_records": duplicate_records, "rows": len(rows),
               "coverage": {name: {"present": members[name], "null": nulls[name], "populated": populated[name],
                                    "absent": len(rows) - members[name], "unrequested": False} for name in sorted(native.DOCUMENT_FIELDS)},
               "tables": {name: {"rows": len(data), "inputs": list(used.values())} for name, (data, _) in tables.items()}}
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{output_dir.name}-", dir=output_dir.parent) as temporary:
        staged = Path(temporary) / "candidate"
        staged.mkdir()
        for name, table in converted.items():
            pq.write_table(table.replace_schema_metadata({"layer1.version": VERSION}), staged / f"{name}.parquet", compression="zstd")
        (staged / "inputs.json").write_text(json.dumps(receipt, indent=2) + "\n")
        # Reserve without clobbering a competing writer. Rename only the empty
        # directory we just created; no partially written table becomes visible.
        output_dir.mkdir()
        try:
            staged.rename(output_dir)
        except BaseException:
            try:
                output_dir.rmdir()
            except OSError:
                pass  # Preserve another writer's content if it appeared in the reservation.
            raise
    return output_dir / "federal_register_documents.parquet"
