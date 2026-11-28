"""Unit, negative, and hermetic loopback S3/HTTP E2E tests for the zero-copy DDK catalog bridge."""

from __future__ import annotations

from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import threading
from typing import Any
import urllib.error
import urllib.request

from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import ddk_bridge, generation_retention as retention, odcs_contracts
from spicy_regs.sources import cloudflare, publication as pub, r2


# ---------------------------------------------------------------------------
# In-memory CAS S3 + WAF-enforcing loopback HTTP double
# ---------------------------------------------------------------------------


class _LoopbackR2Server:
    """In-memory S3 + anonymous public HTTP server enforcing CAS, Cache-Control, Range, and WAF User-Agent."""

    def __init__(self, *, require_explicit_ua: bool = True) -> None:
        self.require_explicit_ua = require_explicit_ua
        self.objects: dict[str, bytes] = {}
        self.etags: dict[str, str] = {}
        self.content_types: dict[str, str] = {}
        self.cache_controls: dict[str, str] = {}
        self.redirects: dict[str, str] = {}
        self.put_log: list[dict[str, Any]] = []
        self.http_log: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        assert self._httpd is not None
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    def seed(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        cache_control: str = "public, max-age=31536000, immutable",
    ) -> str:
        with self._lock:
            payload = bytes(data)
            etag = sha256(payload).hexdigest()[:32]
            self.objects[key] = payload
            self.etags[key] = etag
            self.content_types[key] = content_type
            self.cache_controls[key] = cache_control
            return etag

    def clear_logs(self) -> None:
        with self._lock:
            self.put_log.clear()
            self.http_log.clear()

    # -- Boto3 S3 client surface --

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        with self._lock:
            if Key not in self.objects:
                raise ClientError(
                    {"Error": {"Code": "404", "Message": "Not Found"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                    "HeadObject",
                )
            return {
                "ETag": f'"{self.etags[Key]}"',
                "ContentLength": len(self.objects[Key]),
                "ContentType": self.content_types[Key],
                "CacheControl": self.cache_controls[Key],
            }

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Range: str | None = None,
        IfMatch: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            if Key not in self.objects:
                raise ClientError(
                    {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                    "GetObject",
                )
            current_etag = self.etags[Key]
            if IfMatch is not None and IfMatch.strip('"') != current_etag:
                raise ClientError(
                    {
                        "Error": {"Code": "PreconditionFailed", "Message": "ETag mismatch"},
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                    },
                    "GetObject",
                )
            raw = self.objects[Key]
            if Range is not None:
                assert Range.startswith("bytes=")
                start_s, end_s = Range.removeprefix("bytes=").split("-", 1)
                start, end = int(start_s), int(end_s)
                raw = raw[start : end + 1]
            return {
                "Body": io.BytesIO(raw),
                "ETag": f'"{current_etag}"',
                "ContentLength": len(raw),
                "ContentType": self.content_types[Key],
            }

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str = "application/octet-stream",
        CacheControl: str = "",
        IfMatch: str | None = None,
        IfNoneMatch: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            exists = Key in self.objects
            if IfNoneMatch == "*" and exists:
                raise ClientError(
                    {
                        "Error": {"Code": "PreconditionFailed", "Message": "Object already exists"},
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                    },
                    "PutObject",
                )
            if IfMatch is not None:
                if not exists or self.etags[Key] != IfMatch.strip('"'):
                    raise ClientError(
                        {
                            "Error": {"Code": "PreconditionFailed", "Message": "ETag mismatch"},
                            "ResponseMetadata": {"HTTPStatusCode": 412},
                        },
                        "PutObject",
                    )
            payload = bytes(Body)
            etag = sha256(payload).hexdigest()[:32]
            self.objects[Key] = payload
            self.etags[Key] = etag
            self.content_types[Key] = ContentType
            self.cache_controls[Key] = CacheControl
            self.put_log.append(
                {
                    "bucket": Bucket,
                    "key": Key,
                    "bytes": len(payload),
                    "content_type": ContentType,
                    "cache_control": CacheControl,
                    "if_match": IfMatch,
                    "if_none_match": IfNoneMatch,
                    "etag": etag,
                }
            )
            return {"ETag": f'"{etag}"'}

    # -- Loopback HTTP server lifecycle --

    def __enter__(self) -> _LoopbackR2Server:
        server_ref = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _check_waf(self, method: str, key: str) -> bool:
                ua = self.headers.get("User-Agent") or ""
                rng = self.headers.get("Range")
                with server_ref._lock:
                    server_ref.http_log.append({"method": method, "key": key, "user_agent": ua, "range": rng})
                if server_ref.require_explicit_ua and (not ua.strip() or ua.startswith("Python-urllib")):
                    body = b"error code: 1010"
                    self.send_response(403)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    if method != "HEAD":
                        self.wfile.write(body)
                    return False
                return True

            def _serve(self, *, head_only: bool) -> None:
                key = self.path.lstrip("/")
                method = "HEAD" if head_only else "GET"
                if not self._check_waf(method, key):
                    return
                with server_ref._lock:
                    if key in server_ref.redirects:
                        target = server_ref.redirects[key]
                        self.send_response(302)
                        self.send_header("Location", target)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    if key not in server_ref.objects:
                        self.send_response(404)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    data = server_ref.objects[key]
                    etag = server_ref.etags[key]
                    ctype = server_ref.content_types[key]
                    cc = server_ref.cache_controls[key]

                rng = self.headers.get("Range")
                if not head_only and rng and rng.startswith("bytes="):
                    spec = rng.removeprefix("bytes=")
                    start_s, end_s = spec.split("-", 1)
                    total = len(data)
                    if start_s == "":
                        suffix = int(end_s)
                        start = max(0, total - suffix)
                        end = total - 1
                    else:
                        start = int(start_s)
                        end = total - 1 if end_s == "" else min(total - 1, int(end_s))
                    chunk = data[start : end + 1]
                    self.send_response(206)
                    self.send_header("Content-Type", ctype)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
                    self.send_header("Content-Length", str(len(chunk)))
                    self.send_header("ETag", f'"{etag}"')
                    self.send_header("Cache-Control", cc)
                    self.end_headers()
                    self.wfile.write(chunk)
                    return

                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("ETag", f'"{etag}"')
                self.send_header("Cache-Control", cc)
                self.end_headers()
                if not head_only:
                    self.wfile.write(data)

            def do_GET(self) -> None:
                self._serve(head_only=False)

            def do_HEAD(self) -> None:
                self._serve(head_only=True)

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)


def _sample_value(dtype: pa.DataType, idx: int) -> Any:
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return f"val-{idx}"
    if pa.types.is_boolean(dtype):
        return idx % 2 == 0
    if pa.types.is_integer(dtype):
        return idx + 1
    if pa.types.is_floating(dtype):
        return float(idx) + 0.5
    if pa.types.is_decimal(dtype):
        from decimal import Decimal

        return Decimal("1.00")
    if pa.types.is_date32(dtype) or pa.types.is_date64(dtype):
        from datetime import date

        return date(2026, 1, 1 + (idx % 20))
    if pa.types.is_timestamp(dtype):
        from datetime import datetime, timezone

        tz = timezone.utc if dtype.tz else None
        return datetime(2026, 1, 1, 12, 0, idx % 60, tzinfo=tz)
    if pa.types.is_time(dtype):
        from datetime import time

        return time(12, 0, idx % 60)
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return f"bin-{idx}".encode()
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return [_sample_value(dtype.value_type, idx)]
    if pa.types.is_struct(dtype):
        return {field.name: _sample_value(field.type, idx) for field in dtype}
    return None


def _parquet_bytes_for_schema(schema: pa.Schema, *, rows: int = 2) -> bytes:
    arrays = []
    for field in schema:
        values = [_sample_value(field.type, i) for i in range(rows)]
        arrays.append(pa.array(values, type=field.type))
    table = pa.Table.from_arrays(arrays, schema=schema)
    buf = io.BytesIO()
    # DuckDB-style Parquet without embedded pandas/arrow schema metadata, matching live R2 files.
    pq.write_table(table, buf, store_schema=False)
    return buf.getvalue()


def _seed_pilot_subset_bucket(
    server: _LoopbackR2Server,
    *,
    families: tuple[str, ...] = ("amendments", "members", "bill-family", "rulemaking"),
    published_at: str = "2026-11-28T05:46:05Z",
    rulemaking_asserted_at: str = "2026-11-27T09:09:06Z",
    zero_row_tables: frozenset[str] = frozenset(
        {"bill_summaries", "diff_summaries", "financial_changes", "section_classifications"}
    ),
    corrupt_table_schema: str | None = None,
) -> dict[str, Any]:
    """Seed realistic pilot families (including split bill_sections, 0-row tables, list columns, and rulemaking)."""
    pub_families: dict[str, Any] = {}
    for idx, family in enumerate(fam for fam in families if fam != "rulemaking"):
        loaded = odcs_contracts.load_enrolled_family_contract(family)
        assert loaded is not None
        contract, _ = loaded
        gen_digest = sha256(f"{family}:{published_at}:{idx}".encode()).hexdigest()
        prefix = f"generations/{family}/{gen_digest}"
        art_doc = {"artifactDigest": f"sha256:{gen_digest}", "spec": {"family": family}}
        server.seed(f"{prefix}/artifact.json", json.dumps(art_doc).encode(), content_type="application/json")

        tables_entry: dict[str, Any] = {}
        for schema_obj in contract["schema"]:
            tname = schema_obj["name"]
            arrow_schema = odcs_contracts.odcs_schema_to_pyarrow(schema_obj)
            if corrupt_table_schema == tname:
                # Flip the first column's Arrow type to simulate unannounced upstream schema drift.
                first = arrow_schema[0]
                drifted_type = pa.string() if pa.types.is_integer(first.type) else pa.int64()
                write_schema = pa.schema([pa.field(first.name, drifted_type, nullable=first.nullable), *list(arrow_schema)[1:]])
            else:
                write_schema = arrow_schema

            if tname == "bill_sections":
                members_list: list[dict[str, Any]] = []
                total_bytes = 0
                total_rows = 0
                for congress in ("118", "119"):
                    raw = _parquet_bytes_for_schema(write_schema, rows=3)
                    rel = f"bill_sections/congress={congress}/part-000000.parquet"
                    server.seed(f"{prefix}/{rel}", raw)
                    members_list.append(
                        {
                            "key": rel,
                            "sha256": f"sha256:{sha256(raw).hexdigest()}",
                            "byteSize": len(raw),
                            "rows": 3,
                            "partition": {"congress": congress},
                        }
                    )
                    total_bytes += len(raw)
                    total_rows += 3
                tables_entry[f"{tname}.parquet"] = {
                    "byteSize": total_bytes,
                    "rows": total_rows,
                    "columns": [[f.name, str(f.type)] for f in arrow_schema],
                    "partitionColumns": ["congress"],
                    "members": members_list,
                }
            else:
                row_count = 0 if tname in zero_row_tables else 2
                raw = _parquet_bytes_for_schema(write_schema, rows=row_count)
                server.seed(f"{prefix}/{tname}.parquet", raw)
                tables_entry[f"{tname}.parquet"] = {
                    "byteSize": len(raw),
                    "rows": row_count,
                    "columns": [[f.name, str(f.type)] for f in arrow_schema],
                    "sha256": f"sha256:{sha256(raw).hexdigest()}",
                }

        pub_families[family] = {
            "prefix": prefix,
            "logicalId": f"urn:spicy-regs:family:{family}",
            "artifactDigest": f"sha256:{gen_digest}",
            "publishedAt": published_at,
            "tables": tables_entry,
        }

    pub_doc = {"format": "spicy-regs-publication", "version": 2, "families": pub_families}
    pub_bytes = json.dumps(pub_doc, sort_keys=True).encode()
    pub_etag = server.seed(pub.INDEX_V2_KEY, pub_bytes, content_type="application/json", cache_control="no-store")

    rm_latest_bytes: bytes | None = None
    if "rulemaking" in families:
        loaded_rm = odcs_contracts.load_enrolled_family_contract("rulemaking")
        assert loaded_rm is not None
        rm_contract, _ = loaded_rm
        snap_id = "snapshot_test_0001"
        snap_prefix = f"materialized/rulemaking/snapshots/{snap_id}"
        rm_artifacts: dict[str, Any] = {}
        for schema_obj in rm_contract["schema"]:
            tname = schema_obj["name"]
            arrow_schema = odcs_contracts.odcs_schema_to_pyarrow(schema_obj)
            raw = _parquet_bytes_for_schema(arrow_schema, rows=2)
            rkey = f"{snap_prefix}/{tname}.parquet"
            server.seed(rkey, raw)
            rm_artifacts[f"{tname}.parquet"] = {
                "remote_key": rkey,
                "sha256": sha256(raw).hexdigest(),
                "bytes": len(raw),
                "rows": 2,
                "visibility": "public",
            }
        # Seed internal etl_receipts.parquet which MUST be excluded from the public DDK catalog!
        internal_raw = _parquet_bytes_for_schema(pa.schema([pa.field("receipt_id", pa.string())]), rows=1)
        internal_key = f"{snap_prefix}/etl_receipts.parquet"
        server.seed(internal_key, internal_raw)
        rm_artifacts["etl_receipts.parquet"] = {
            "remote_key": internal_key,
            "sha256": sha256(internal_raw).hexdigest(),
            "bytes": len(internal_raw),
            "rows": 1,
            "visibility": "internal",
        }
        rm_manifest = {
            "dataset": "rulemaking",
            "format_version": 2,
            "snapshot_id": snap_id,
            "asserted_at": rulemaking_asserted_at,
            "artifacts": rm_artifacts,
        }
        rm_manifest_bytes = json.dumps(rm_manifest, sort_keys=True).encode()
        rm_manifest_key = f"{snap_prefix}/manifest.json"
        server.seed(rm_manifest_key, rm_manifest_bytes, content_type="application/json")
        rm_latest = {
            "dataset": "rulemaking",
            "format_version": 2,
            "snapshot_id": snap_id,
            "manifest_key": rm_manifest_key,
            "manifest_sha256": sha256(rm_manifest_bytes).hexdigest(),
        }
        rm_latest_bytes = json.dumps(rm_latest, sort_keys=True).encode()
        server.seed(pub.SNAPSHOT_POINTER, rm_latest_bytes, content_type="application/json", cache_control="no-store")

    return {"pub_bytes": pub_bytes, "pub_etag": pub_etag, "rm_latest_bytes": rm_latest_bytes}


# ---------------------------------------------------------------------------
# 1. Unit & negative tests (work even without optional ddk-bridge installed)
# ---------------------------------------------------------------------------


def test_lazy_import_missing_ddk_gives_clear_import_error_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = ddk_bridge.importlib.import_module

    def _missing_ddk(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "ddk.publication" or name.startswith("ddk."):
            raise ModuleNotFoundError("No module named 'ddk'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(ddk_bridge.importlib, "import_module", _missing_ddk)
    assert ddk_bridge.ddk_bridge_available() is False
    with pytest.raises(ImportError, match="ddk-bridge"):
        ddk_bridge._load_ddk_publication()


def test_storage_adapter_hard_refuses_parquet_put_and_out_of_prefix_writes() -> None:
    srv = _LoopbackR2Server()
    adapter = ddk_bridge.Boto3BridgeStorageAdapter(srv, bucket="spicy-regs")

    # 1. Any .parquet PUT is hard-refused before touching S3 (both inside & outside bridge prefix).
    for bad_parquet_key in (
        "ddk-catalogs/spicy-regs/releases/" + "a" * 64 + "/members.parquet",
        "generations/amendments/" + "b" * 64 + "/amendments.parquet",
        "generations/bill-family/" + "b" * 64 + "/bill_sections/congress=119/part-000000.parquet",
    ):
        with pytest.raises(Exception) as exc_info:
            adapter.put_bytes(bad_parquet_key, b"PAR1", if_none_match=True)
        assert getattr(exc_info.value, "code", None) == "CATALOG-ZERO-COPY-VIOLATION"

    # 2. Any write outside ddk-catalogs/spicy-regs/ is hard-refused.
    for bad_key in (
        "publication.v2.json",
        "generations/amendments/" + "a" * 64 + "/artifact.json",
        "materialized/rulemaking/latest.json",
        "pins/archive-releases.json",
        "ddk-catalogs/congressional-tech/current.json",
        "ddk-catalogs/spicy-regs/stray.json",
    ):
        with pytest.raises(Exception) as exc_info:
            adapter.put_bytes(bad_key, b"{}", if_none_match=True)
        assert getattr(exc_info.value, "code", None) == "CATALOG-WRITE-OUTSIDE-PREFIX"

    # 3. Path traversal is refused.
    with pytest.raises(ValueError, match="Unsafe key"):
        adapter.put_bytes("ddk-catalogs/spicy-regs/../publication.v2.json", b"{}", if_none_match=True)

    # 4. Delete is always refused.
    with pytest.raises(Exception) as exc_info:
        adapter.delete_object(ddk_bridge.BRIDGE_POINTER_KEY)
    assert getattr(exc_info.value, "code", None) == "CATALOG-READONLY-DELETE"

    assert srv.put_log == []
    assert adapter.put_keys == []


def test_storage_adapter_enforces_cache_control_cas_and_verified_target_client(monkeypatch: pytest.MonkeyPatch) -> None:
    srv = _LoopbackR2Server()
    adapter = ddk_bridge.Boto3BridgeStorageAdapter(srv, bucket="spicy-regs")

    # Pointer write without CAS precondition is refused.
    with pytest.raises(Exception) as exc_info:
        adapter.put_bytes(ddk_bridge.BRIDGE_POINTER_KEY, b'{"release_id": "a"}')
    assert getattr(exc_info.value, "code", None) == "CATALOG-REMOTE-CONFLICT"

    # Pointer bootstrap sets Cache-Control: no-store + IfNoneMatch="*".
    res1 = adapter.put_bytes(
        ddk_bridge.BRIDGE_POINTER_KEY,
        b'{"release_id": "1"}',
        content_type="application/json",
        if_none_match=True,
    )
    assert res1["cacheControl"] == "no-store"
    assert srv.put_log[-1]["cache_control"] == "no-store"
    assert srv.put_log[-1]["if_none_match"] == "*"

    # Stale CAS ETag on pointer update is refused with CATALOG-REMOTE-CONFLICT.
    with pytest.raises(Exception) as exc_info:
        adapter.put_bytes(
            ddk_bridge.BRIDGE_POINTER_KEY,
            b'{"release_id": "2"}',
            content_type="application/json",
            if_match="deadbeef",
        )
    assert getattr(exc_info.value, "code", None) == "CATALOG-REMOTE-CONFLICT"

    # Valid CAS ETag advances pointer with no-store.
    res2 = adapter.put_bytes(
        ddk_bridge.BRIDGE_POINTER_KEY,
        b'{"release_id": "2"}',
        content_type="application/json",
        if_match=res1["etag"],
    )
    assert res2["etag"] != res1["etag"]
    assert srv.put_log[-1]["if_match"] == res1["etag"]

    # Immutable release artifact requires create-only if_none_match=True and sets immutable Cache-Control.
    rel_key = f"{ddk_bridge.BRIDGE_RELEASES_PREFIX}/{'a' * 64}/release.manifest.json"
    with pytest.raises(Exception) as exc_info:
        adapter.put_bytes(rel_key, b"{}", content_type="application/json", if_none_match=False)
    assert getattr(exc_info.value, "code", None) == "CATALOG-REMOTE-CONFLICT"

    rel_res = adapter.put_bytes(rel_key, b"{}", content_type="application/json", if_none_match=True)
    assert rel_res["cacheControl"] == "public, max-age=31536000, immutable"
    assert srv.put_log[-1]["cache_control"] == "public, max-age=31536000, immutable"
    assert srv.put_log[-1]["if_none_match"] == "*"

    # Duplicate create-only release PUT fails with CATALOG-REMOTE-CONFLICT (412).
    with pytest.raises(Exception) as exc_info:
        adapter.put_bytes(rel_key, b"{}", content_type="application/json", if_none_match=True)
    assert getattr(exc_info.value, "code", None) == "CATALOG-REMOTE-CONFLICT"

    # Default constructor delegates to r2.verified_target_client() and rejects mismatched bucket.
    monkeypatch.setattr(r2, "verified_target_client", lambda: (srv, r2.VERIFIED_BUCKET))
    verified_adapter = ddk_bridge.Boto3BridgeStorageAdapter()
    assert verified_adapter.bucket == r2.VERIFIED_BUCKET
    with pytest.raises(ValueError, match="verified target bucket"):
        ddk_bridge.Boto3BridgeStorageAdapter(bucket="wrong-bucket")


def test_anonymous_public_fetcher_sends_user_agent_avoids_waf_403_and_refuses_redirects() -> None:
    with _LoopbackR2Server(require_explicit_ua=True) as srv:
        srv.seed("probe.txt", b"0123456789abcdef", content_type="text/plain")
        srv.redirects["redir.txt"] = f"{srv.base_url}/probe.txt"

        # Bare urllib without custom User-Agent triggers simulated Cloudflare WAF 403 Error 1010.
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{srv.base_url}/probe.txt", timeout=5)
        assert exc_info.value.code == 403

        # AnonymousPublicFetcher sets explicit User-Agent and exposes .user_agent.
        fetcher = ddk_bridge.AnonymousPublicFetcher(srv.base_url, allow_insecure_http=True)
        assert fetcher.user_agent == ddk_bridge.BRIDGE_USER_AGENT
        body, etag = fetcher("probe.txt")
        assert body == b"0123456789abcdef"
        assert etag is not None
        assert fetcher.get_range("probe.txt", 2, 5) == b"2345"
        head = fetcher.head_key("probe.txt")
        assert head["size"] == 16

        # 302 redirect is refused on both GET and HEAD.
        with pytest.raises(Exception) as get_redir:
            fetcher("redir.txt")
        assert getattr(get_redir.value, "code", None) == "CATALOG-REMOTE-REDIRECT"
        with pytest.raises(Exception) as head_redir:
            fetcher.head_key("redir.txt")
        assert getattr(head_redir.value, "code", None) == "CATALOG-REMOTE-REDIRECT"

        # Every fetcher request carried our explicit User-Agent.
        fetcher_calls = [entry for entry in srv.http_log if entry["user_agent"] == ddk_bridge.BRIDGE_USER_AGENT]
        assert len(fetcher_calls) == 5


def test_collector_handles_multipart_split_tiebreaker_internal_exclusion_and_deterministic_observed_at() -> None:
    with _LoopbackR2Server(require_explicit_ua=True) as srv:
        _seed_pilot_subset_bucket(
            srv,
            families=("bill-family", "rulemaking"),
            published_at="2026-11-28T05:46:05Z",
            rulemaking_asserted_at="2026-11-27T09:09:06Z",
        )
        # Mutate bill_sections in publication.v2.json to have TWO part files under congress=119 (fec-query pattern).
        pub_doc = json.loads(srv.objects[pub.INDEX_V2_KEY])
        bf = pub_doc["families"]["bill-family"]
        prefix = bf["prefix"]
        bs = bf["tables"]["bill_sections.parquet"]
        part0 = dict(bs["members"][1])  # congress=119/part-000000.parquet
        part1_raw = srv.objects[f"{prefix}/{part0['key']}"]
        part1_rel = "bill_sections/congress=119/part-000001.parquet"
        srv.seed(f"{prefix}/{part1_rel}", part1_raw)
        bs["members"].append(
            {
                "key": part1_rel,
                "sha256": part0["sha256"],
                "byteSize": len(part1_raw),
                "rows": part0["rows"],
                "partition": {"congress": "119"},
            }
        )
        bs["byteSize"] += len(part1_raw)
        bs["rows"] += part0["rows"]
        srv.seed(pub.INDEX_V2_KEY, json.dumps(pub_doc).encode(), content_type="application/json")

        collection = ddk_bridge.collect_pilot_external_tables(
            families=("bill-family", "rulemaking"),
            public_base_url=srv.base_url,
            allow_insecure_http=True,
        )
        assert collection.observed_at == "2026-11-28T05:46:05Z"
        table_map = {t.table_name: t for t in collection.tables}
        assert "etl_receipts" not in table_map
        assert "rulemaking_lifecycles" in table_map

        # Duplicate congress=119 partition triggered deterministic _part tie-breaker.
        bs_table = table_map["bill_sections"]
        assert bs_table.partition_keys == ("congress", "_part")
        assert [dict(m.partition_values) for m in bs_table.members] == [
            {"congress": "118", "_part": "part-000000"},
            {"congress": "119", "_part": "part-000000"},
            {"congress": "119", "_part": "part-000001"},
        ]


def test_collector_validates_rulemaking_pointer_and_members_json_digest() -> None:
    with _LoopbackR2Server(require_explicit_ua=True) as srv:
        _seed_pilot_subset_bucket(srv, families=("bill-family", "rulemaking"))

        # 1. members.json fallback checks membersDigest when present (passes on exact digest, fails on mismatch).
        pub_doc = json.loads(srv.objects[pub.INDEX_V2_KEY])
        bf = pub_doc["families"]["bill-family"]
        prefix = bf["prefix"]
        bs_members = bf["tables"]["bill_sections.parquet"].pop("members")
        bf["tables"]["bill_sections.parquet"]["layout"] = "split"
        members_payload = json.dumps({"tables": {"bill_sections.parquet": {"members": bs_members}}}).encode()
        srv.seed(f"{prefix}/members.json", members_payload, content_type="application/json")
        bf["membersDigest"] = f"sha256:{sha256(members_payload).hexdigest()}"
        srv.seed(pub.INDEX_V2_KEY, json.dumps(pub_doc).encode(), content_type="application/json")

        ok_col = ddk_bridge.collect_pilot_external_tables(
            families=("bill-family",),
            public_base_url=srv.base_url,
            allow_insecure_http=True,
        )
        assert any(t.table_name == "bill_sections" and len(t.members) == 2 for t in ok_col.tables)

        bf["membersDigest"] = "sha256:" + "0" * 64
        srv.seed(pub.INDEX_V2_KEY, json.dumps(pub_doc).encode(), content_type="application/json")
        with pytest.raises(ddk_bridge.BridgeError, match="members.json SHA-256"):
            ddk_bridge.collect_pilot_external_tables(
                families=("bill-family",),
                public_base_url=srv.base_url,
                allow_insecure_http=True,
            )

        # 2. rulemaking/latest.json rejects mismatched manifest_key, dataset, or format_version.
        rm_orig = json.loads(srv.objects[pub.SNAPSHOT_POINTER])
        for mutated, match in (
            ({**rm_orig, "manifest_key": "other/manifest.json"}, "manifest_key"),
            ({**rm_orig, "dataset": "other"}, "Unexpected dataset"),
            ({**rm_orig, "format_version": 99}, "Unsupported format_version"),
        ):
            srv.seed(pub.SNAPSHOT_POINTER, json.dumps(mutated).encode(), content_type="application/json")
            with pytest.raises(ddk_bridge.BridgeError, match=match):
                ddk_bridge.collect_pilot_external_tables(
                    families=("rulemaking",),
                    public_base_url=srv.base_url,
                    allow_insecure_http=True,
                )


def test_readonly_storage_url_ssrf_and_adapter_get_error_mapping() -> None:
    srv = _LoopbackR2Server()
    srv.seed("ddk-catalogs/spicy-regs/probe.json", b"{}", content_type="application/json")
    adapter = ddk_bridge.Boto3BridgeStorageAdapter(srv, bucket="spicy-regs")

    # 404 and 412 get_bytes mapping.
    with pytest.raises(Exception) as missing_err:
        adapter.get_bytes("ddk-catalogs/spicy-regs/absent.json")
    assert getattr(missing_err.value, "code", None) == "CATALOG-OBJECT-MISSING"

    with pytest.raises(Exception) as mutated_err:
        adapter.get_bytes("ddk-catalogs/spicy-regs/probe.json", if_match="wrong-etag")
    assert getattr(mutated_err.value, "code", None) == "CATALOG-UPSTREAM-MUTATED"

    # PublicHttpReadOnlyStorage always refuses put_bytes.
    fetcher = ddk_bridge.AnonymousPublicFetcher("https://data.spicygov.ai")
    ro = ddk_bridge.PublicHttpReadOnlyStorage(fetcher)
    with pytest.raises(Exception) as ro_err:
        ro.put_bytes(ddk_bridge.BRIDGE_POINTER_KEY, b"{}", if_none_match=True)
    assert getattr(ro_err.value, "code", None) == "CATALOG-READONLY-DRYRUN"

    # SSRF / credential / query / insecure HTTP validation.
    for bad_url in (
        "http://data.spicygov.ai",
        "http://127.0.0.1:8080",
        "https://user:pass@data.spicygov.ai",
        "https://data.spicygov.ai?x=1",
        "https://10.0.0.1",
        "https://169.254.169.254",
    ):
        with pytest.raises(ValueError):
            ddk_bridge.validate_public_base_url(bad_url, allow_insecure_http=False)


# ---------------------------------------------------------------------------
# 2. Hermetic loopback E2E & negative tests requiring optional ddk-bridge
# ---------------------------------------------------------------------------


def test_e2e_loopback_pilot_subset_dry_run_publish_full_and_range_readback() -> None:
    """32-table pilot subset (split bill_sections, list columns, 0-row tables, rulemaking minus etl_receipts)."""
    pytest.importorskip("ddk")
    families = ("amendments", "members", "bill-family", "rulemaking")

    with _LoopbackR2Server(require_explicit_ua=True) as srv:
        seeded = _seed_pilot_subset_bucket(srv, families=families)
        adapter = ddk_bridge.Boto3BridgeStorageAdapter(srv, bucket="spicy-regs")
        fetcher = ddk_bridge.AnonymousPublicFetcher(srv.base_url, allow_insecure_http=True)

        # 1. Dry-run succeeds with ZERO writes and parquet_upload_count == 0.
        dry = ddk_bridge.dry_run_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id="none",
            allow_insecure_http=True,
        )
        assert dry["status"] == "ok"
        assert dry["parquet_upload_count"] == 0
        assert dry["tables"] == 32  # 1 + 3 + 20 + 8 (etl_receipts excluded!)
        assert dry["parquet_referenced_count"] == 33  # bill_sections has 2 partitions
        assert dry["schema_parity_passed"] == 33
        assert dry["metadata_location_matches_remote_key"] == 32
        assert dry["relationship_count"] > 0
        assert srv.put_log == []

        # 2. Publish with Range-mode readback + CAS bootstrap.
        srv.clear_logs()
        pub_receipt = ddk_bridge.publish_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id="none",
            verify_member_bytes=False,
            schema_readback_mode="range",
            purge_pointer_cache=False,
            allow_insecure_http=True,
        )
        assert pub_receipt["status"] == "committed"
        assert pub_receipt["parquet_upload_count"] == 0
        assert pub_receipt["release_id"] == dry["release_id"]
        assert pub_receipt["lock_sha256"] == dry["lock_sha256"]
        assert pub_receipt["metadata_location_matches_remote_key"] == 32

        # Zero .parquet PUTs and every PUT stayed strictly inside ddk-catalogs/spicy-regs/.
        assert srv.put_log
        assert all(not entry["key"].lower().endswith(".parquet") for entry in srv.put_log)
        assert all(entry["key"].startswith(f"{ddk_bridge.BRIDGE_PREFIX}/") for entry in srv.put_log)
        pointer_puts = [e for e in srv.put_log if e["key"] == ddk_bridge.BRIDGE_POINTER_KEY]
        release_puts = [e for e in srv.put_log if e["key"] != ddk_bridge.BRIDGE_POINTER_KEY]
        assert len(pointer_puts) == 1
        assert pointer_puts[0]["cache_control"] == "no-store"
        assert pointer_puts[0]["if_none_match"] == "*"
        assert release_puts
        assert all(e["cache_control"] == "public, max-age=31536000, immutable" for e in release_puts)
        assert all(e["if_none_match"] == "*" for e in release_puts)

        # Existing live pointers remained bit-for-bit untouched.
        assert srv.objects[pub.INDEX_V2_KEY] == seeded["pub_bytes"]
        assert srv.objects[pub.SNAPSHOT_POINTER] == seeded["rm_latest_bytes"]

        # Every icebergSnapshot.metadataLocation resolves HTTP 200 and etl_receipts is absent.
        ptr_doc = json.loads(srv.objects[ddk_bridge.BRIDGE_POINTER_KEY])
        manifest_key = ptr_doc["release_manifest"]["remote_key"]
        assert manifest_key == f"{ddk_bridge.BRIDGE_RELEASES_PREFIX}/{pub_receipt['release_id']}/catalog/release.manifest.json"
        manifest_doc = json.loads(srv.objects[manifest_key])
        assert "etl_receipts" not in {t["table_name"] for t in manifest_doc["logical_tables"]}
        for snap in manifest_doc["iceberg_snapshots"]:
            meta_bytes, _ = fetcher.fetch_url(snap["metadata_location"])
            assert sha256(meta_bytes).hexdigest() == snap["metadata_sha256"]

        # 3. Standalone anonymous readback in FULL-BODY mode (verify_member_bytes=True + pa.Schema).
        full_readback = ddk_bridge.verify_live_bridge_readback(
            public_base_url=srv.base_url,
            families=families,
            fetcher=fetcher,
            expected_release_id=pub_receipt["release_id"],
            expected_lock_sha256=pub_receipt["lock_sha256"],
            verify_member_bytes=True,
            schema_readback_mode="full",
            allow_insecure_http=True,
        )
        assert full_readback["release_id"] == pub_receipt["release_id"]
        assert len(full_readback["verified_members"]) == 33
        assert len(full_readback["verified_iceberg_snapshots"]) == 32
        assert len(full_readback["verified_schemas"]) == 32
        assert full_readback["verified_schemas"]["bill_sections"]["files_checked"] == 2
        assert full_readback["verified_schemas"]["members"]["files_checked"] == 1

        # 4. Standalone anonymous readback in RANGE mode (0 full-body Parquet GETs + explicit User-Agent).
        srv.clear_logs()
        range_readback = ddk_bridge.verify_live_bridge_readback(
            public_base_url=srv.base_url,
            families=families,
            fetcher=fetcher,
            expected_release_id=pub_receipt["release_id"],
            expected_lock_sha256=pub_receipt["lock_sha256"],
            verify_member_bytes=False,
            schema_readback_mode="range",
            allow_insecure_http=True,
        )
        assert range_readback["release_id"] == pub_receipt["release_id"]
        assert len(range_readback["verified_members"]) == 33
        assert all(m["verification"] == "range" for m in range_readback["verified_members"])
        assert range_readback["verified_schemas"] == full_readback["verified_schemas"]
        full_parquet_gets = [
            e for e in srv.http_log if e["method"] == "GET" and e["key"].endswith(".parquet") and e["range"] is None
        ]
        assert full_parquet_gets == []
        assert all(e["user_agent"] == ddk_bridge.BRIDGE_USER_AGENT for e in srv.http_log)


def test_e2e_determinism_idempotent_republish_partial_retry_purge_and_retention_lock_pins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fast 1-family E2E proving byte-identical determinism, partial-upload retry, idempotent re-publish,
    pre-Step-8 cache purge ordering, CAS conflicts, CLI JSON output, and cross-module retention lock pins."""
    pytest.importorskip("ddk")
    families = ("amendments",)

    with _LoopbackR2Server(require_explicit_ua=True) as srv:
        _seed_pilot_subset_bucket(srv, families=families)
        adapter = ddk_bridge.Boto3BridgeStorageAdapter(srv, bucket="spicy-regs")
        fetcher = ddk_bridge.AnonymousPublicFetcher(srv.base_url, allow_insecure_http=True)

        # 1. Fail-fast before ANY upload on invalid verify_member_bytes=False + schema_readback_mode="full".
        with pytest.raises(ValueError, match="verify_member_bytes=True"):
            ddk_bridge.publish_bridge(
                families=families,
                public_base_url=srv.base_url,
                storage=adapter,
                fetcher=fetcher,
                expected_prior_release_id="none",
                verify_member_bytes=False,
                schema_readback_mode="full",
                purge_pointer_cache=False,
                allow_insecure_http=True,
            )
        assert srv.put_log == []

        # 2. Byte-identical determinism across two distinct temp roots (release_id, catalog_sha256, lock_sha256).
        dry1 = ddk_bridge.dry_run_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id="none",
            output_dir=tmp_path / "det_a" / "catalog",
            allow_insecure_http=True,
        )
        dry2 = ddk_bridge.dry_run_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id="none",
            output_dir=tmp_path / "det_b" / "catalog",
            allow_insecure_http=True,
        )
        assert dry1["observed_at"] == dry2["observed_at"]
        assert dry1["release_id"] == dry2["release_id"]
        assert dry1["catalog_sha256"] == dry2["catalog_sha256"]
        assert dry1["lock_sha256"] == dry2["lock_sha256"]

        # 3. Simulate partial Step 4 upload (catalog.ddk.json + catalog.lock.json already uploaded, current.json absent).
        rel_id = dry1["release_id"]
        for fname in ("catalog.ddk.json", "catalog.lock.json"):
            rkey = f"{ddk_bridge.BRIDGE_RELEASES_PREFIX}/{rel_id}/catalog/{fname}"
            srv.seed(rkey, (tmp_path / "det_a" / "catalog" / fname).read_bytes(), content_type="application/json")

        # Track exact ordering of Step 7 pointer PUT -> Cloudflare cache purge -> Step 8 live HTTP GET current.json.
        events: list[tuple[str, str]] = []
        orig_put = srv.put_object
        orig_fetch_url = fetcher.fetch_url

        def _tracked_put(*, Bucket: str, Key: str, **kwargs: Any) -> dict[str, Any]:
            res = orig_put(Bucket=Bucket, Key=Key, **kwargs)
            if Key == ddk_bridge.BRIDGE_POINTER_KEY:
                events.append(("put_pointer", Key))
            return res

        def _tracked_purge(urls: list[str]) -> None:
            for url in urls:
                events.append(("purge", url))

        def _tracked_fetch(url: str, *, byte_range: tuple[int, int] | None = None) -> tuple[bytes, str | None]:
            if url.endswith(f"/{ddk_bridge.BRIDGE_POINTER_KEY}"):
                events.append(("http_get_pointer", url))
            return orig_fetch_url(url, byte_range=byte_range)

        monkeypatch.setattr(srv, "put_object", _tracked_put)
        monkeypatch.setattr(cloudflare, "purge_urls", _tracked_purge)
        monkeypatch.setattr(fetcher, "fetch_url", _tracked_fetch)

        pub1 = ddk_bridge.publish_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id="none",
            verify_member_bytes=False,
            schema_readback_mode="range",
            purge_pointer_cache=True,
            allow_insecure_http=True,
        )
        assert pub1["status"] == "committed"
        assert pub1["release_id"] == rel_id
        assert pub1["lock_sha256"] == dry1["lock_sha256"]
        # Both pre-seeded immutable artifacts were cleanly verified and reused on partial-upload retry!
        assert len(pub1["reused_keys"]) == 2
        assert len(pub1["uploaded_keys"]) == dry1["catalog_upload_count"] - 2
        # Cache purge fired strictly AFTER Step 7 pointer PUT and strictly BEFORE Step 8 live pointer readback!
        pointer_url = f"{srv.base_url}/{ddk_bridge.BRIDGE_POINTER_KEY}"
        assert events == [
            ("put_pointer", ddk_bridge.BRIDGE_POINTER_KEY),
            ("purge", pointer_url),
            ("http_get_pointer", pointer_url),
        ]

        # 4. Idempotent re-publish over unchanged inputs reuses 100% of immutable artifacts cleanly!
        pub_idem = ddk_bridge.publish_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id=rel_id,
            verify_member_bytes=False,
            schema_readback_mode="range",
            purge_pointer_cache=False,
            allow_insecure_http=True,
        )
        assert pub_idem["status"] == "committed"
        assert pub_idem["uploaded_keys"] == []
        assert len(pub_idem["reused_keys"]) == dry1["catalog_upload_count"]
        assert pub_idem["lock_sha256"] == pub1["lock_sha256"]

        # 5. Stale CAS precondition is rejected without mutating live current.json.
        live_before_conflict = srv.objects[ddk_bridge.BRIDGE_POINTER_KEY]
        with pytest.raises(Exception) as cas_err:
            ddk_bridge.publish_bridge(
                families=families,
                public_base_url=srv.base_url,
                storage=adapter,
                fetcher=fetcher,
                expected_prior_release_id="0" * 64,
                observed_at="2026-11-28T06:00:00Z",
                verify_member_bytes=False,
                purge_pointer_cache=False,
                allow_insecure_http=True,
            )
        assert getattr(cas_err.value, "code", None) == "CATALOG-REMOTE-CONFLICT"
        assert srv.objects[ddk_bridge.BRIDGE_POINTER_KEY] == live_before_conflict

        # 6. Advancing via valid CAS expected_prior_release_id commits a second release.
        pub2 = ddk_bridge.publish_bridge(
            families=families,
            public_base_url=srv.base_url,
            storage=adapter,
            fetcher=fetcher,
            expected_prior_release_id=rel_id,
            observed_at="2026-11-28T06:00:00Z",
            verify_member_bytes=False,
            purge_pointer_cache=False,
            allow_insecure_http=True,
        )
        assert pub2["status"] == "committed"
        assert pub2["release_id"] != rel_id

        # 7. Cross-module retention test directly against REAL publish_bridge output!
        expected_gen_digest = json.loads(srv.objects[pub.INDEX_V2_KEY])["families"]["amendments"][
            "artifactDigest"
        ].removeprefix("sha256:")
        live_bridge_pins = retention._ddk_bridge_pins(srv, "spicy-regs")
        assert live_bridge_pins == {
            expected_gen_digest: f"referenced by DDK catalog release {pub2['release_id'][:12]}"
        }

        ptr2 = json.loads(srv.objects[ddk_bridge.BRIDGE_POINTER_KEY])
        man1_key = f"{ddk_bridge.BRIDGE_RELEASES_PREFIX}/{rel_id}/catalog/release.manifest.json"
        lock_pin_doc = {
            "format": retention.DDK_LOCK_PINS_FORMAT,
            "version": 1,
            "updated_at": "2026-11-28T06:00:00Z",
            "catalogs": {
                "spicy-regs": {
                    "catalog_id": retention.DDK_BRIDGE_CATALOG_ID,
                    "audience": "public",
                    "catalog_base_url": f"{srv.base_url}/{ddk_bridge.BRIDGE_PREFIX}",
                    "bound": {
                        "release_id": pub2["release_id"],
                        "release_manifest_sha256": ptr2["release_manifest"]["sha256"],
                        "lock_sha256": pub2["lock_sha256"],
                    },
                    "retained": [
                        {
                            "release_id": pub2["release_id"],
                            "release_manifest_sha256": ptr2["release_manifest"]["sha256"],
                            "lock_sha256": pub2["lock_sha256"],
                        },
                        {
                            "release_id": rel_id,
                            "release_manifest_sha256": sha256(srv.objects[man1_key]).hexdigest(),
                            "lock_sha256": pub1["lock_sha256"],
                        },
                    ],
                }
            },
        }
        lock_pins = retention.ddk_release_lock_pins(
            json.dumps(lock_pin_doc).encode(),
            client=srv,
            bucket="spicy-regs",
        )
        assert lock_pins == {expected_gen_digest: f"DDK release-lock pin (spicy-regs {pub2['release_id'][:12]})"}

        # 8. CLI --dry-run and --verify-live with --output JSON receipt.
        dry_out = tmp_path / "dry.json"
        rc_dry = ddk_bridge.main(
            [
                "--dry-run",
                "--families",
                "amendments",
                "--public-base-url",
                srv.base_url,
                "--observed-at",
                "2026-11-28T06:00:00Z",
                "--allow-insecure-http",
                "--output",
                str(dry_out),
            ]
        )
        assert rc_dry == 0
        assert json.loads(dry_out.read_text())["parquet_upload_count"] == 0

        verify_out = tmp_path / "verify.json"
        rc_verify = ddk_bridge.main(
            [
                "--verify-live",
                "--families",
                "amendments",
                "--public-base-url",
                srv.base_url,
                "--allow-insecure-http",
                "--output",
                str(verify_out),
            ]
        )
        assert rc_verify == 0
        assert json.loads(verify_out.read_text())["release_id"] == pub2["release_id"]


def test_e2e_schema_drift_is_refused_before_any_upload() -> None:
    pytest.importorskip("ddk")
    with _LoopbackR2Server(require_explicit_ua=True) as srv:
        _seed_pilot_subset_bucket(srv, families=("amendments",), corrupt_table_schema="amendments")
        adapter = ddk_bridge.Boto3BridgeStorageAdapter(srv, bucket="spicy-regs")
        fetcher = ddk_bridge.AnonymousPublicFetcher(srv.base_url, allow_insecure_http=True)

        with pytest.raises(Exception) as exc_info:
            ddk_bridge.publish_bridge(
                families=("amendments",),
                public_base_url=srv.base_url,
                storage=adapter,
                fetcher=fetcher,
                expected_prior_release_id="none",
                verify_member_bytes=False,
                purge_pointer_cache=False,
                allow_insecure_http=True,
            )
        assert getattr(exc_info.value, "code", None) == "CATALOG-SCHEMA-PARITY"
        assert srv.put_log == []
