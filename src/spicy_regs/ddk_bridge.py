"""Additive zero-copy DDK catalog and Iceberg v2 bridge over immutable R2 generations.

This module publishes a shadow DDK catalog and Iceberg v2 table-metadata bridge under
``ddk-catalogs/spicy-regs/`` pointing zero-copy to pre-existing immutable Parquet
generations under ``generations/<family>/<sha256>/`` and materialized rulemaking
snapshots under ``materialized/rulemaking/snapshots/<snapshot_id>/``.

Zero Parquet payloads are copied or uploaded, and existing live pointers
(``publication.v2.json``, ``materialized/rulemaking/latest.json``,
``pins/archive-releases.json``, ``data.spicygov.ai``, and ``mcp.spicygov.ai``) remain
strictly read-only and unmodified.

``data-design-kit`` (``ddk``) is imported lazily so base ``spicy-regs`` CLI/MCP
installs require zero ``ddk`` runtime dependency.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import http.client
import importlib
import ipaddress
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tempfile
from typing import Any
import urllib.error
from urllib.parse import urlsplit
import urllib.request

import pyarrow as pa

from spicy_regs import odcs_contracts
from spicy_regs.sources import cloudflare, publication as pub, r2

BRIDGE_PREFIX = "ddk-catalogs/spicy-regs"
BRIDGE_POINTER_KEY = f"{BRIDGE_PREFIX}/current.json"
BRIDGE_RELEASES_PREFIX = f"{BRIDGE_PREFIX}/releases"
BRIDGE_USER_AGENT = "spicy-regs-ddk-bridge/1.0"

DEFAULT_PUBLIC_BASE_URL = r2.VERIFIED_PUBLIC_URL
DEFAULT_BUCKET = r2.VERIFIED_BUCKET
DEFAULT_DATASET = "spicy-regs-pilot"
DEFAULT_CATALOG_ID = "urn:spicy-regs:catalog:public"
DEFAULT_PROJECT_ID = "urn:spicy-regs:project:public"
DEFAULT_AUTHORITY_ID = "urn:spicy-regs:authority:spicygov"
DEFAULT_PRODUCT_ID = "urn:spicy-regs:product:public"
DEFAULT_PURPOSE = "shadow-read"
DEFAULT_AUDIENCE = "public"
DEFAULT_VERSION = "1.0.0"

CACHE_CONTROL_POINTER = "no-store"
CACHE_CONTROL_IMMUTABLE = "public, max-age=31536000, immutable"

DDK_RELEASE_LOCK_PINS_FORMAT = "ddk-release-lock-pins"
DDK_RELEASE_LOCK_PINS_VERSION = 1
#: DDK managed Arrow schema observation profile mapping to ``odcs_contracts.DDK_ARROW_EXACT_PROFILE``.
DDK_MANAGED_SCHEMA_V3 = "urn:ddk:metadata:arrow-odcs:3"

_RESERVED_WRITE_PREFIXES = frozenset({"generations", "materialized", "pins", "source-evidence", "retention"})
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_PART_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_SAFE_PART_WITH_EQ_RE = re.compile(r"^[A-Za-z0-9._=-]{1,128}$")
_SENTINEL: Any = object()

_DDK_INSTALL_HINT = (
    "Optional dependency 'data-design-kit' (ddk) is not installed. "
    "Install the optional 'ddk-bridge' dependency group "
    "(e.g. `uv sync --group ddk-bridge` or `uv run --group ddk-bridge ...`) "
    "to use spicy_regs.ddk_bridge."
)


class BridgeError(RuntimeError):
    """Raised when DDK bridge collection, validation, or policy enforcement fails."""


class _FallbackCatalogError(RuntimeError):
    """Fallback error matching ``ddk.catalog_access.CatalogError`` when ``ddk`` is absent."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def _catalog_error_cls() -> type[Exception]:
    try:
        mod = importlib.import_module("ddk.catalog_access")
        err = getattr(mod, "CatalogError", None)
        if isinstance(err, type) and issubclass(err, Exception):
            return err
    except ImportError:
        pass
    return _FallbackCatalogError


def _catalog_error(code: str, message: str) -> Exception:
    return _catalog_error_cls()(code, message)


def _load_ddk_publication() -> Any:
    """Lazily import ``ddk.publication``, raising ``ImportError`` with an install hint if absent."""
    try:
        mod = importlib.import_module("ddk.publication")
    except ImportError as exc:
        raise ImportError(_DDK_INSTALL_HINT) from exc
    required = (
        "RemoteDestination",
        "ExternalParquetObjectSpec",
        "ExternalTableSpec",
        "build_external_generation_bundle",
        "dry_run_remote_publication",
        "publish_and_verify_remote_publication",
        "verify_http_consumer_readback",
    )
    missing = [name for name in required if not hasattr(mod, name)]
    if missing:
        raise ImportError(
            f"Installed ddk.publication is missing required bridge APIs ({', '.join(missing)}). {_DDK_INSTALL_HINT}"
        )
    return mod


def ddk_bridge_available() -> bool:
    """Return True when the optional ``data-design-kit`` bridge APIs are importable."""
    try:
        _load_ddk_publication()
    except ImportError:
        return False
    return True


def _safe_rel_path(value: str, *, label: str = "key", allow_equals: bool = True) -> str:
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value:
        raise ValueError(f"Unsafe {label}: {value!r}")
    pattern = _SAFE_PART_WITH_EQ_RE if allow_equals else _SAFE_PART_RE
    parts = value.split("/")
    if any(not part or part in {".", ".."} or not pattern.fullmatch(part) for part in parts):
        raise ValueError(f"Unsafe {label} segment in {value!r}")
    return "/".join(parts)


def validate_bridge_prefix(prefix: str) -> str:
    """Validate that the bridge write prefix is isolated outside existing live R2 prefixes."""
    clean = _safe_rel_path(prefix.strip("/"), label="bridge_prefix", allow_equals=False)
    top = clean.split("/", 1)[0]
    if top in _RESERVED_WRITE_PREFIXES or clean.lower().endswith(".parquet"):
        raise ValueError(
            f"bridge_prefix {clean!r} must be isolated outside reserved prefixes {sorted(_RESERVED_WRITE_PREFIXES)!r}"
        )
    return clean


def _is_loopback_ip(ip_obj: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    mapped = getattr(ip_obj, "ipv4_mapped", None)
    if mapped is not None:
        return _is_loopback_ip(mapped)
    return bool(ip_obj.is_loopback)


def _is_disallowed_ip(
    ip_obj: ipaddress.IPv4Address | ipaddress.IPv6Address,
    *,
    allow_loopback: bool = False,
) -> bool:
    mapped = getattr(ip_obj, "ipv4_mapped", None)
    if mapped is not None:
        return _is_disallowed_ip(mapped, allow_loopback=allow_loopback)
    if ip_obj.is_loopback:
        return not allow_loopback
    return bool(
        not ip_obj.is_global
        or ip_obj.is_private
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    )


def _is_loopback_host(host_clean: str, ip_literal: ipaddress.IPv4Address | ipaddress.IPv6Address | None) -> bool:
    if ip_literal is not None:
        return _is_loopback_ip(ip_literal)
    return host_clean == "localhost" or host_clean.endswith(".localhost")


def _validate_http_url(url: str, *, allow_insecure_http: bool = False, allow_path: bool = True) -> str:
    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"URL must be a nonempty string, got {url!r}")
    raw = url.strip()
    if "?" in raw or "#" in raw:
        raise ValueError(f"URL must not contain query or fragment: {url!r}")
    parts = urlsplit(raw)
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise ValueError(f"URL must not contain credentials: {url!r}")
    if parts.query or parts.fragment:
        raise ValueError(f"URL must not contain query or fragment: {url!r}")
    if not allow_path and parts.path not in {"", "/"}:
        raise ValueError(f"public_base_url must not contain a path: {url!r}")
    hostname = parts.hostname
    if not parts.netloc or not hostname:
        raise ValueError(f"URL requires host: {url!r}")

    host_clean = hostname.strip("[]").lower()
    try:
        ip_literal: ipaddress.IPv4Address | ipaddress.IPv6Address | None = ipaddress.ip_address(host_clean)
    except ValueError:
        ip_literal = None

    if ip_literal is not None:
        if _is_disallowed_ip(ip_literal, allow_loopback=allow_insecure_http):
            raise ValueError(f"URL resolves to disallowed IP {ip_literal}: {url!r}")
    elif _is_loopback_host(host_clean, None) and not allow_insecure_http:
        raise ValueError(f"Loopback URL requires allow_insecure_http=True: {url!r}")

    scheme = parts.scheme.lower()
    clean = raw.rstrip("/") if not allow_path else raw
    if scheme == "https":
        return clean
    if scheme == "http":
        if allow_insecure_http and _is_loopback_host(host_clean, ip_literal):
            return clean
        raise ValueError(f"allow_insecure_http only permits loopback http origins, got {url!r}")
    raise ValueError(f"URL must use https (or loopback http with allow_insecure_http=True): {url!r}")


def validate_public_base_url(url: str, *, allow_insecure_http: bool = False) -> str:
    """Validate and normalize the public HTTPS origin (or loopback HTTP when explicitly allowed)."""
    return _validate_http_url(url, allow_insecure_http=allow_insecure_http, allow_path=False)


def _clean_etag(etag: Any) -> str | None:
    if etag is None:
        return None
    text = str(etag).strip()
    if text.startswith("W/"):
        text = text[2:].strip()
    text = text.strip('"')
    return text or None


def _http_status_from_client_error(exc: Exception) -> int | None:
    resp = getattr(exc, "response", None)
    if not isinstance(resp, Mapping):
        return None
    err = resp.get("Error")
    if isinstance(err, Mapping):
        code = str(err.get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return 404
        if code in {"409", "412", "PreconditionFailed", "ConditionalRequestConflict"}:
            return 412
    meta = resp.get("ResponseMetadata")
    if isinstance(meta, Mapping) and isinstance(meta.get("HTTPStatusCode"), int):
        status = meta["HTTPStatusCode"]
        return 412 if status == 409 else status
    return None


def _normalize_dataset_alias(table_name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", table_name.strip().lower()).strip("-")
    if not cleaned:
        raise BridgeError(f"Cannot normalize table name {table_name!r} to a valid DDK dataset alias")
    return cleaned


def _parse_utc_timestamp(value: str, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise BridgeError(f"{label} must be a non-empty ISO-8601 UTC timestamp, got {value!r}")
    text = value.strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BridgeError(f"Invalid ISO-8601 timestamp for {label}: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_utc_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Boto3BridgeStorageAdapter:
    """DDK ``ObjectStoragePort`` over ``r2.verified_target_client()`` enforcing zero-copy and prefix isolation.

    Invariants enforced on every write:
    - Hard-refuses any ``.parquet`` PUT (``CATALOG-ZERO-COPY-VIOLATION``).
    - Hard-refuses any key outside ``ddk-catalogs/spicy-regs/`` (``CATALOG-WRITE-OUTSIDE-PREFIX``).
    - Sets ``Cache-Control: no-store`` and requires CAS (``IfNoneMatch='*'`` or ``IfMatch=<etag>``)
      on ``ddk-catalogs/spicy-regs/current.json``.
    - Sets ``Cache-Control: public, max-age=31536000, immutable`` and enforces create-only
      ``IfNoneMatch='*'`` on all ``ddk-catalogs/spicy-regs/releases/<release_id>/*`` objects.
    - Strictly additive: ``delete_object`` is always refused.
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        bucket: str | None = None,
        bridge_prefix: str = BRIDGE_PREFIX,
        pointer_key: str | None = None,
    ) -> None:
        if client is None:
            verified_client, verified_bucket = r2.verified_target_client()
            if bucket is not None and bucket.strip() != verified_bucket:
                raise ValueError(
                    f"Requested bucket {bucket!r} differs from verified target bucket {verified_bucket!r}"
                )
            self.client = verified_client
            self.bucket = verified_bucket
        else:
            for method_name in ("head_object", "get_object", "put_object"):
                if not callable(getattr(client, method_name, None)):
                    raise TypeError(f"S3/R2 storage client requires callable {method_name}()")
            resolved_bucket = (bucket if bucket is not None else DEFAULT_BUCKET).strip()
            if not resolved_bucket:
                raise ValueError(f"Invalid bucket: {bucket!r}")
            self.client = client
            self.bucket = resolved_bucket

        self.bridge_prefix = validate_bridge_prefix(bridge_prefix)
        self.releases_prefix = f"{self.bridge_prefix}/releases"
        resolved_pointer = (
            _safe_rel_path(pointer_key, label="pointer_key", allow_equals=False)
            if pointer_key is not None
            else f"{self.bridge_prefix}/current.json"
        )
        if not resolved_pointer.startswith(f"{self.bridge_prefix}/") or resolved_pointer.lower().endswith(".parquet"):
            raise ValueError(
                f"pointer_key {resolved_pointer!r} must reside inside {self.bridge_prefix + '/'!r} and not be .parquet"
            )
        self.pointer_key = resolved_pointer
        self.put_keys: list[str] = []
        self.put_records: list[dict[str, Any]] = []

    def head_object(self, key: str) -> dict[str, Any]:
        safe_key = _safe_rel_path(key, allow_equals=True)
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=safe_key)
        except Exception as exc:
            if _http_status_from_client_error(exc) == 404:
                raise _catalog_error("CATALOG-OBJECT-MISSING", f"Remote object missing: {safe_key}") from exc
            raise
        return {
            "key": safe_key,
            "etag": _clean_etag(resp.get("ETag")),
            "size": resp.get("ContentLength"),
            "contentType": resp.get("ContentType"),
        }

    def get_bytes(
        self,
        key: str,
        *,
        byte_range: tuple[int, int] | None = None,
        if_match: str | None = None,
    ) -> bytes:
        safe_key = _safe_rel_path(key, allow_equals=True)
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": safe_key}
        if byte_range is not None:
            start, end = byte_range
            if (
                not isinstance(start, int)
                or isinstance(start, bool)
                or not isinstance(end, int)
                or isinstance(end, bool)
                or start < 0
                or end < start
            ):
                raise ValueError(f"Invalid byte_range for {safe_key!r}: {byte_range!r}")
            kwargs["Range"] = f"bytes={start}-{end}"
        if if_match is not None:
            kwargs["IfMatch"] = _clean_etag(if_match) or if_match
        try:
            resp = self.client.get_object(**kwargs)
        except Exception as exc:
            status = _http_status_from_client_error(exc)
            if status == 404:
                raise _catalog_error("CATALOG-OBJECT-MISSING", f"Remote object missing: {safe_key}") from exc
            if status == 412:
                raise _catalog_error("CATALOG-UPSTREAM-MUTATED", f"Precondition failed reading {safe_key}") from exc
            raise
        body = resp["Body"]
        try:
            return bytes(body.read())
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> dict[str, Any]:
        safe_key = _safe_rel_path(key, allow_equals=True)
        if safe_key.lower().endswith(".parquet"):
            raise _catalog_error(
                "CATALOG-ZERO-COPY-VIOLATION",
                f"Zero-copy DDK bridge forbids uploading Parquet objects: {safe_key!r}",
            )
        prefix_slash = f"{self.bridge_prefix}/"
        if not safe_key.startswith(prefix_slash):
            raise _catalog_error(
                "CATALOG-WRITE-OUTSIDE-PREFIX",
                f"DDK bridge refuses writes outside isolated prefix {prefix_slash!r}: {safe_key!r}",
            )
        if if_match is not None and if_none_match:
            raise ValueError("Cannot specify both if_match and if_none_match")

        clean_if_match = _clean_etag(if_match) if if_match is not None else None
        if safe_key == self.pointer_key:
            if not if_none_match and not clean_if_match:
                raise _catalog_error(
                    "CATALOG-REMOTE-CONFLICT",
                    f"CAS precondition (if_none_match=True or if_match) is required when writing {safe_key!r}",
                )
            cache_control = CACHE_CONTROL_POINTER
        else:
            releases_slash = f"{self.releases_prefix}/"
            if not safe_key.startswith(releases_slash):
                raise _catalog_error(
                    "CATALOG-WRITE-OUTSIDE-PREFIX",
                    f"Non-pointer bridge writes must live under {releases_slash!r}: {safe_key!r}",
                )
            if not if_none_match or clean_if_match is not None:
                raise _catalog_error(
                    "CATALOG-REMOTE-CONFLICT",
                    f"Immutable release artifact {safe_key!r} requires create-only if_none_match=True",
                )
            cache_control = CACHE_CONTROL_IMMUTABLE

        payload = bytes(data)
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": safe_key,
            "Body": payload,
            "ContentType": content_type,
            "CacheControl": cache_control,
        }
        if clean_if_match is not None:
            kwargs["IfMatch"] = clean_if_match
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"

        try:
            resp = self.client.put_object(**kwargs)
        except Exception as exc:
            if _http_status_from_client_error(exc) == 412:
                raise _catalog_error(
                    "CATALOG-REMOTE-CONFLICT",
                    f"CAS precondition failed writing {safe_key}",
                ) from exc
            raise

        etag = _clean_etag(resp.get("ETag"))
        digest = sha256(payload).hexdigest()
        record = {
            "key": safe_key,
            "etag": etag,
            "sha256": digest,
            "bytes": len(payload),
            "content_type": content_type,
            "cache_control": cache_control,
            "if_match": clean_if_match,
            "if_none_match": if_none_match,
        }
        self.put_keys.append(safe_key)
        self.put_records.append(record)
        return {
            "key": safe_key,
            "etag": etag,
            "sha256": digest,
            "bytes": len(payload),
            "cacheControl": cache_control,
        }

    def delete_object(self, key: str) -> None:
        raise _catalog_error(
            "CATALOG-READONLY-DELETE",
            f"DDK bridge storage is strictly additive; refusing delete of {key!r}",
        )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class AnonymousPublicFetcher:
    """No-redirect anonymous HTTPS/HTTP fetcher with explicit ``User-Agent`` propagation.

    Exposes ``.user_agent`` and ``.fetch_url`` matching ``ddk.publication._build_anonymous_http_fetcher``
    so ``ddk.publication.verify_http_consumer_readback`` propagates ``User-Agent`` to Range footer reads.
    """

    def __init__(
        self,
        public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
        *,
        user_agent: str = BRIDGE_USER_AGENT,
        timeout: float = 30.0,
        allow_insecure_http: bool = False,
    ) -> None:
        self.public_base_url = validate_public_base_url(
            public_base_url,
            allow_insecure_http=allow_insecure_http,
        )
        self.user_agent = (user_agent or "").strip() or BRIDGE_USER_AGENT
        self.timeout = float(timeout)
        self.allow_insecure_http = bool(allow_insecure_http)
        self._opener = urllib.request.build_opener(_NoRedirectHandler())

    def url_for_key(self, rel_key: str) -> str:
        safe_key = _safe_rel_path(rel_key, allow_equals=True)
        url = f"{self.public_base_url}/{safe_key}"
        if not url.startswith(self.public_base_url + "/"):
            raise _catalog_error("CATALOG-REMOTE-IO", f"Key {rel_key!r} escaped public_base_url")
        return url

    def _request_headers(self, *, byte_range: tuple[int, int] | None = None) -> dict[str, str]:
        headers = {
            "User-Agent": self.user_agent,
            "Cache-Control": "no-cache",
            "Accept-Encoding": "identity",
        }
        if byte_range is not None:
            start, end = byte_range
            if (
                not isinstance(start, int)
                or isinstance(start, bool)
                or not isinstance(end, int)
                or isinstance(end, bool)
                or start < 0
                or end < start
            ):
                raise ValueError(f"Invalid byte_range: {byte_range!r}")
            headers["Range"] = f"bytes={start}-{end}"
        return headers

    def fetch_url(
        self,
        url: str,
        *,
        byte_range: tuple[int, int] | None = None,
    ) -> tuple[bytes, str | None]:
        clean_url = _validate_http_url(url, allow_insecure_http=self.allow_insecure_http, allow_path=True)
        req = urllib.request.Request(
            clean_url,
            method="GET",
            headers=self._request_headers(byte_range=byte_range),
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                status = getattr(resp, "status", 200)
                if status in {301, 302, 303, 307, 308}:
                    raise _catalog_error("CATALOG-REMOTE-REDIRECT", f"Refusing HTTP {status} redirect for {clean_url}")
                if byte_range is not None and status != 206:
                    raise _catalog_error(
                        "CATALOG-REMOTE-INCOMPLETE",
                        f"Expected HTTP 206 Partial Content for range read of {clean_url}, got HTTP {status}",
                    )
                body = resp.read()
                etag = _clean_etag(resp.headers.get("ETag"))
                return body, etag
        except urllib.error.HTTPError as exc:
            if exc.code in {301, 302, 303, 307, 308}:
                raise _catalog_error(
                    "CATALOG-REMOTE-REDIRECT",
                    f"Refusing HTTP {exc.code} redirect for {clean_url}",
                ) from exc
            err = _catalog_error("CATALOG-REMOTE-IO", f"HTTP {exc.code} fetching {clean_url}")
            setattr(err, "http_status", exc.code)
            raise err from exc
        except urllib.error.URLError as exc:
            raise _catalog_error("CATALOG-REMOTE-IO", f"Failed to fetch {clean_url}: {exc.reason}") from exc

    def head_url(self, url: str) -> dict[str, Any]:
        clean_url = _validate_http_url(url, allow_insecure_http=self.allow_insecure_http, allow_path=True)
        parts = urlsplit(clean_url)
        conn_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(parts.hostname or "", parts.port, timeout=self.timeout)
        target = parts.path or "/"
        try:
            conn.request(
                "HEAD",
                target,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept-Encoding": "identity",
                },
            )
            resp = conn.getresponse()
            resp.read()
            if resp.status in {301, 302, 303, 307, 308}:
                raise _catalog_error("CATALOG-REMOTE-REDIRECT", f"Refusing HTTP {resp.status} redirect on HEAD {clean_url}")
            if resp.status == 404:
                raise _catalog_error("CATALOG-OBJECT-MISSING", f"Remote object missing (HTTP 404): {clean_url}")
            if resp.status not in {200, 206}:
                err = _catalog_error("CATALOG-REMOTE-IO", f"HTTP {resp.status} on HEAD {clean_url}")
                setattr(err, "http_status", resp.status)
                raise err
            raw_len = resp.getheader("Content-Length")
            size = int(raw_len) if raw_len is not None and raw_len.isdigit() else None
            return {
                "url": clean_url,
                "status": resp.status,
                "etag": _clean_etag(resp.getheader("ETag")),
                "size": size,
                "contentType": resp.getheader("Content-Type"),
            }
        except OSError as exc:
            raise _catalog_error("CATALOG-REMOTE-IO", f"Failed HEAD {clean_url}: {exc}") from exc
        finally:
            conn.close()

    def head_key(self, rel_key: str) -> dict[str, Any]:
        safe_key = _safe_rel_path(rel_key, allow_equals=True)
        meta = self.head_url(self.url_for_key(safe_key))
        return {
            "key": safe_key,
            "etag": meta["etag"],
            "size": meta["size"],
            "contentType": meta["contentType"],
        }

    def get_range(self, rel_key: str, start: int, end: int) -> bytes:
        url = self.url_for_key(rel_key)
        try:
            body, _ = self.fetch_url(url, byte_range=(start, end))
        except Exception as exc:
            if getattr(exc, "http_status", None) == 404:
                raise _catalog_error("CATALOG-OBJECT-MISSING", f"Remote object missing: {rel_key}") from exc
            raise
        return body

    def __call__(self, rel_key: str) -> tuple[bytes, str | None]:
        return self.fetch_url(self.url_for_key(rel_key))


class PublicHttpReadOnlyStorage:
    """Read-only ``ObjectStoragePort`` over ``AnonymousPublicFetcher`` for credential-free ``--dry-run``."""

    def __init__(self, fetcher: AnonymousPublicFetcher) -> None:
        self.fetcher = fetcher

    def head_object(self, key: str) -> dict[str, Any]:
        return self.fetcher.head_key(key)

    def get_bytes(
        self,
        key: str,
        *,
        byte_range: tuple[int, int] | None = None,
        if_match: str | None = None,
    ) -> bytes:
        safe_key = _safe_rel_path(key, allow_equals=True)
        if byte_range is not None:
            return self.fetcher.get_range(safe_key, byte_range[0], byte_range[1])
        try:
            body, _ = self.fetcher(safe_key)
        except Exception as exc:
            if getattr(exc, "http_status", None) == 404:
                raise _catalog_error("CATALOG-OBJECT-MISSING", f"Remote object missing: {safe_key}") from exc
            raise
        return body

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> dict[str, Any]:
        raise _catalog_error(
            "CATALOG-READONLY-DRYRUN",
            f"Read-only dry-run storage forbids writes (refused PUT {key!r})",
        )


@dataclass(frozen=True)
class CollectedMember:
    """One pre-existing immutable Parquet file discovered from live R2 manifests."""

    table_name: str
    remote_key: str
    url: str
    sha256: str
    size: int
    rows: int
    partition_values: Mapping[str, str]
    output_name: str


@dataclass(frozen=True)
class CollectedTable:
    """One logical table paired with its committed ODCS contract and immutable Parquet member(s)."""

    family: str
    table_name: str
    domain: str
    contract: Mapping[str, Any]
    contract_path: Path
    contract_sha256: str
    schema_obj: Mapping[str, Any]
    arrow_schema: pa.Schema
    partition_keys: tuple[str, ...]
    members: tuple[CollectedMember, ...]
    generation_digest: str


@dataclass(frozen=True)
class PilotCollection:
    """Deterministic collection of pilot-wave tables, Arrow schemas, relationships, and provenance."""

    families: tuple[str, ...]
    tables: tuple[CollectedTable, ...]
    declared_schemas: Mapping[str, pa.Schema]
    relationships: tuple[dict[str, Any], ...]
    product: dict[str, Any]
    observed_at: str
    public_base_url: str
    bridge_prefix: str
    source_index_sha256: str | None
    source_index_etag: str | None
    rulemaking_snapshot_id: str | None
    rulemaking_latest_sha256: str | None
    rulemaking_manifest_sha256: str | None
    odcs_manifest_sha256: str
    family_generations: Mapping[str, str]

    @property
    def member_count(self) -> int:
        return sum(len(t.members) for t in self.tables)

    @property
    def total_bytes(self) -> int:
        return sum(m.size for t in self.tables for m in t.members)

    @property
    def total_rows(self) -> int:
        return sum(m.rows for t in self.tables for m in t.members)


def _strip_sha256_prefix(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise BridgeError(f"{label} must be a SHA-256 string, got {value!r}")
    digest = value.removeprefix("sha256:")
    if not _HEX64_RE.fullmatch(digest):
        raise BridgeError(f"{label} is not a valid 64-hex lowercase sha256: {value!r}")
    return digest


def _parse_hive_partition(rel_key: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for seg in PurePosixPath(rel_key).parts[1:-1]:
        if "=" not in seg:
            raise BridgeError(f"Split member key {rel_key!r} contains non-Hive segment {seg!r}")
        col, val = seg.split("=", 1)
        if not col or not val or col in parsed:
            raise BridgeError(f"Malformed or duplicate Hive partition segment {seg!r} in {rel_key!r}")
        parsed[col] = val
    return parsed


def _ddk_join_field_type(prop: Mapping[str, Any]) -> str | None:
    """Mirror ``ddk.catalog_access.field_type`` so emitted relationships stay inside DDK's equality profile."""
    logical = prop.get("logicalType")
    physical = str(prop.get("physicalType") or "").lower()
    if physical in {"binary", "bytes", "bytea", "blob", "varbinary", "fixed_size_binary"}:
        return "binary"
    raw_opts = prop.get("logicalTypeOptions")
    opts: Mapping[str, Any] = raw_opts if isinstance(raw_opts, Mapping) else {}
    if logical == "string" and opts.get("format") in {"byte", "binary"}:
        return None
    if logical == "date":
        calendars = [
            item.get("value")
            for item in prop.get("customProperties", ())
            if isinstance(item, Mapping) and item.get("vendor") == "ddk" and item.get("property") == "calendar"
        ]
        if calendars and calendars != ["gregorian"]:
            return None
    if logical in {"integer", "string", "date"}:
        return str(logical)
    return None


def _odcs_primary_key_columns(schema_obj: Mapping[str, Any]) -> tuple[str, ...]:
    primary = [
        prop
        for prop in schema_obj.get("properties", ())
        if isinstance(prop, Mapping) and prop.get("primaryKey") is True and isinstance(prop.get("name"), str)
    ]
    if not primary:
        return ()
    if len(primary) == 1:
        return (primary[0]["name"],)
    return tuple(
        prop["name"]
        for prop in sorted(primary, key=lambda item: int(item.get("primaryKeyPosition", -1)))
    )


def _extract_ddk_relationships(
    tables: Sequence[CollectedTable],
    *,
    joins_path: Path = odcs_contracts.DEFAULT_TABLE_JOINS_PATH,
) -> tuple[dict[str, Any], ...]:
    """Filter verified ``table_joins.json`` edges to DDK-valid same-release catalog relationships."""
    if not joins_path.is_file():
        return ()
    joins_doc = json.loads(joins_path.read_bytes())
    raw_joins = joins_doc.get("joins") if isinstance(joins_doc, Mapping) else None
    if not isinstance(raw_joins, list):
        return ()

    props_by_table: dict[str, dict[str, Mapping[str, Any]]] = {}
    pk_by_table: dict[str, tuple[str, ...]] = {}
    for table in tables:
        props_by_table[table.table_name] = {
            prop["name"]: prop
            for prop in table.schema_obj.get("properties", ())
            if isinstance(prop, Mapping) and isinstance(prop.get("name"), str)
        }
        pk_by_table[table.table_name] = _odcs_primary_key_columns(table.schema_obj)

    relationships: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for join in raw_joins:
        if not isinstance(join, Mapping):
            continue
        child = join.get("child")
        parent = join.get("parent")
        if child not in props_by_table or parent not in props_by_table:
            continue
        child_cols = tuple(join.get("child_columns") or ())
        parent_cols = tuple(join.get("parent_columns") or ())
        if not child_cols or len(child_cols) != len(parent_cols):
            continue
        if not all(isinstance(c, str) and c in props_by_table[child] for c in child_cols):
            continue
        if not all(isinstance(p, str) and p in props_by_table[parent] for p in parent_cols):
            continue

        compatible = True
        for c_col, p_col in zip(child_cols, parent_cols):
            c_prop = props_by_table[child][c_col]
            p_prop = props_by_table[parent][p_col]
            c_type = _ddk_join_field_type(c_prop)
            p_type = _ddk_join_field_type(p_prop)
            if c_type is None or p_type is None or c_type != p_type:
                compatible = False
                break
            if c_prop.get("logicalType") == "string" and p_prop.get("logicalType") == "string":
                raw_c_opts = c_prop.get("logicalTypeOptions")
                raw_p_opts = p_prop.get("logicalTypeOptions")
                c_opts: Mapping[str, Any] = raw_c_opts if isinstance(raw_c_opts, Mapping) else {}
                p_opts: Mapping[str, Any] = raw_p_opts if isinstance(raw_p_opts, Mapping) else {}
                if (
                    ("pattern" in c_opts and "pattern" in p_opts and c_opts["pattern"] != p_opts["pattern"])
                    or ("format" in c_opts and "format" in p_opts and c_opts["format"] != p_opts["format"])
                ):
                    compatible = False
                    break
        if not compatible:
            continue

        rel_id = f"urn:spicy-regs:rel:{child}:{'.'.join(child_cols)}:to:{parent}:{'.'.join(parent_cols)}"
        if rel_id in seen_ids:
            continue
        seen_ids.add(rel_id)

        raw_card = str(join.get("expected_cardinality") or "unspecified")
        parent_pk = pk_by_table[parent]
        if raw_card == "one" and (not parent_pk or parent_cols == parent_pk):
            cardinality = "many_to_one"
        elif raw_card == "many":
            cardinality = "many_to_many"
        else:
            cardinality = "unknown"

        kind = str(join.get("kind") or "unmeasured")
        missing = int(join.get("baseline_missing") or 0)
        floor_pct = join.get("floor_pct")
        if kind == "complete" and missing == 0 and (floor_pct is None or float(floor_pct) >= 100.0):
            resolution = "required"
            unresolved_reason = None
        else:
            resolution = "unresolved_allowed"
            unresolved_reason = {
                "complete": "publisher_defect",
                "scope": "scope_boundary",
                "design": "design_optional",
                "empty": "unmeasured",
                "unmeasured": "unmeasured",
            }.get(kind, "unmeasured")

        reason_text = str(join.get("reason") or "").strip()
        meaning = reason_text or f"Join {child}({', '.join(child_cols)}) to {parent}({', '.join(parent_cols)})."
        rel_doc: dict[str, Any] = {
            "id": rel_id,
            "source": {
                "dataset": _normalize_dataset_alias(child),
                "paths": [[{"field": col}] for col in child_cols],
            },
            "target": {
                "dataset": _normalize_dataset_alias(parent),
                "paths": [[{"field": col}] for col in parent_cols],
            },
            "match": "equal",
            "cardinality": cardinality,
            "scope": "same_release",
            "resolution": resolution,
            "meaning": meaning,
            "nulls": "skip",
        }
        if unresolved_reason is not None:
            rel_doc["unresolvedReason"] = unresolved_reason
        relationships.append(rel_doc)

    return tuple(sorted(relationships, key=lambda item: item["id"]))


def _build_odps_product(
    *,
    tables: Sequence[CollectedTable],
    public_base_url: str,
    bridge_prefix: str,
    observed_at: str,
    source_index_sha256: str | None,
    source_index_etag: str | None,
    rulemaking_snapshot_id: str | None,
    rulemaking_latest_sha256: str | None,
    rulemaking_manifest_sha256: str | None,
    odcs_manifest_sha256: str,
    family_generations: Mapping[str, str],
) -> dict[str, Any]:
    output_ports = [
        {
            "name": _normalize_dataset_alias(table.table_name),
            "version": str(table.contract.get("version") or DEFAULT_VERSION),
            "contractId": str(
                table.contract.get("id") or f"urn:ddk:contract:{_normalize_dataset_alias(table.table_name)}"
            ),
        }
        for table in tables
    ]
    custom_properties: list[dict[str, Any]] = [
        {"property": "zeroCopyBridge", "value": True},
        {"property": "bridgePrefix", "value": bridge_prefix},
        {"property": "sourceIndexUrl", "value": f"{public_base_url}/{pub.INDEX_V2_KEY}"},
        {"property": "odcsManifestSha256", "value": odcs_manifest_sha256},
        {"property": "observedAt", "value": observed_at},
        {"property": "familyGenerations", "value": dict(sorted(family_generations.items()))},
    ]
    if source_index_sha256 is not None:
        custom_properties.append({"property": "sourceIndexSha256", "value": source_index_sha256})
    if source_index_etag is not None:
        custom_properties.append({"property": "sourceIndexEtag", "value": source_index_etag})
    if rulemaking_snapshot_id is not None:
        custom_properties.append({"property": "rulemakingSnapshotId", "value": rulemaking_snapshot_id})
    if rulemaking_latest_sha256 is not None:
        custom_properties.append({"property": "rulemakingLatestSha256", "value": rulemaking_latest_sha256})
    if rulemaking_manifest_sha256 is not None:
        custom_properties.append({"property": "rulemakingManifestSha256", "value": rulemaking_manifest_sha256})

    return {
        "apiVersion": "v1.1.0",
        "kind": "DataProduct",
        "id": DEFAULT_PRODUCT_ID,
        "name": "Spicy Regs Pilot Catalog",
        "version": DEFAULT_VERSION,
        "status": "active",
        "domain": "spicygov",
        "description": {
            "purpose": (
                "Zero-copy DDK catalog and Iceberg v2 metadata bridge over immutable spicy-regs R2 generations "
                "and materialized rulemaking snapshots."
            ),
            "usage": (
                f"Discover tables, ODCS v3.2.0 contracts, relationships, and immutable Parquet URLs from "
                f"{public_base_url}/{bridge_prefix}/current.json."
            ),
            "limitations": (
                "Shadow bridge uploads zero Parquet bytes and leaves publication.v2.json, "
                "materialized/rulemaking/latest.json, and legacy pin pointers untouched. "
                "Wave 1 emits Iceberg v2 table metadata JSON descriptors."
            ),
        },
        "outputPorts": output_ports,
        "customProperties": custom_properties,
    }


def collect_pilot_external_tables(
    *,
    families: Sequence[str] | None = None,
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    bridge_prefix: str = BRIDGE_PREFIX,
    fetcher: AnonymousPublicFetcher | None = None,
    contracts_dir: Path = odcs_contracts.DEFAULT_ODCS_DIR,
    joins_path: Path = odcs_contracts.DEFAULT_TABLE_JOINS_PATH,
    verify_artifact_roots: bool = True,
    observed_at: str | None = None,
    user_agent: str = BRIDGE_USER_AGENT,
    allow_insecure_http: bool = False,
) -> PilotCollection:
    """Collect pilot-wave tables from live R2 manifests and committed ODCS v3.2.0 contracts.

    Supports single-file tables, Hive-partitioned split tables (with automatic ``_part``
    tie-breaker when multiple part files share a partition tuple), and the materialized
    ``rulemaking`` snapshot (excluding internal ``etl_receipts.parquet``).

    Derives a deterministic ``observed_at`` from upstream index/manifest timestamps when not
    explicitly overridden so repeated runs over unchanged upstream state yield the exact same
    DDK ``release_id``.
    """
    clean_base = validate_public_base_url(public_base_url, allow_insecure_http=allow_insecure_http)
    clean_prefix = validate_bridge_prefix(bridge_prefix)
    active_fetcher = fetcher or AnonymousPublicFetcher(
        clean_base,
        user_agent=user_agent,
        allow_insecure_http=allow_insecure_http,
    )

    selected_families = tuple(dict.fromkeys(families if families is not None else odcs_contracts.PILOT_FAMILIES))
    if not selected_families:
        raise BridgeError("collect_pilot_external_tables requires at least one family")

    manifest_path = contracts_dir / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise BridgeError(f"Committed ODCS manifest is missing or symlinked: {manifest_path}")
    odcs_manifest_sha256 = sha256(manifest_path.read_bytes()).hexdigest()

    standard_families = [fam for fam in selected_families if fam != "rulemaking"]
    include_rulemaking = "rulemaking" in selected_families

    pub_v2: Mapping[str, Any] | None = None
    source_index_sha256: str | None = None
    source_index_etag: str | None = None
    if standard_families:
        pub_raw, source_index_etag = active_fetcher(pub.INDEX_V2_KEY)
        source_index_sha256 = sha256(pub_raw).hexdigest()
        pub_v2 = json.loads(pub_raw)
        if not isinstance(pub_v2, Mapping) or not isinstance(pub_v2.get("families"), Mapping):
            raise BridgeError("publication.v2.json is not a valid publication index object")

    collected_tables: list[CollectedTable] = []
    declared_schemas: dict[str, pa.Schema] = {}
    family_generations: dict[str, str] = {}
    upstream_timestamps: list[datetime] = []

    for family in standard_families:
        loaded = odcs_contracts.load_enrolled_family_contract(family, contracts_dir=contracts_dir)
        if loaded is None:
            raise BridgeError(f"Family {family!r} is not enrolled in ODCS contracts at {contracts_dir}")
        contract, contract_path = loaded
        contract_sha256 = sha256(contract_path.read_bytes()).hexdigest()
        domain = str(contract.get("domain") or "spicycongress")

        assert pub_v2 is not None
        fam_entry = pub_v2["families"].get(family)
        if not isinstance(fam_entry, Mapping):
            raise BridgeError(f"Family {family!r} is missing from {pub.INDEX_V2_KEY}")

        artifact_digest = fam_entry.get("artifactDigest")
        gen_digest = _strip_sha256_prefix(artifact_digest, label=f"{family}.artifactDigest")
        expected_prefix = f"generations/{family}/{gen_digest}"
        gen_prefix = fam_entry.get("prefix")
        if gen_prefix != expected_prefix:
            raise BridgeError(
                f"Family {family!r} prefix {gen_prefix!r} disagrees with expected {expected_prefix!r}"
            )
        if verify_artifact_roots:
            art_raw, _ = active_fetcher(f"{gen_prefix}/artifact.json")
            art_doc = json.loads(art_raw)
            if not isinstance(art_doc, Mapping) or art_doc.get("artifactDigest") != artifact_digest:
                raise BridgeError(
                    f"Generation root {gen_prefix}/artifact.json digest disagrees with {pub.INDEX_V2_KEY}"
                )

        if isinstance(fam_entry.get("publishedAt"), str) and fam_entry["publishedAt"].strip():
            upstream_timestamps.append(_parse_utc_timestamp(fam_entry["publishedAt"], label=f"{family}.publishedAt"))
        family_generations[family] = gen_digest

        tables_map = fam_entry.get("tables")
        if not isinstance(tables_map, Mapping):
            raise BridgeError(f"Family {family!r} has invalid 'tables' mapping in {pub.INDEX_V2_KEY}")

        members_fallback_doc: Mapping[str, Any] | None = None
        for schema_obj in contract.get("schema") or ():
            table_name = schema_obj["name"]
            table_key = f"{table_name}.parquet"
            table_meta = tables_map.get(table_key)
            if not isinstance(table_meta, Mapping):
                raise BridgeError(f"Table {table_key!r} from ODCS contract {family!r} is missing in {pub.INDEX_V2_KEY}")

            arrow_schema = odcs_contracts.odcs_schema_to_pyarrow(schema_obj)
            is_split = ("members" in table_meta) or (table_meta.get("layout") == "split")
            if is_split:
                raw_part_cols = table_meta.get("partitionColumns")
                if not isinstance(raw_part_cols, list) or not raw_part_cols or not all(
                    isinstance(c, str) and c for c in raw_part_cols
                ):
                    raise BridgeError(f"Split table {family}/{table_name} missing valid partitionColumns")
                partition_cols = tuple(raw_part_cols)

                raw_members = table_meta.get("members")
                if raw_members is None:
                    if members_fallback_doc is None:
                        mem_raw, _ = active_fetcher(f"{gen_prefix}/members.json")
                        expected_members_digest = fam_entry.get("membersDigest")
                        if expected_members_digest is not None:
                            expected_mem_sha = _strip_sha256_prefix(
                                expected_members_digest,
                                label=f"{family}.membersDigest",
                            )
                            actual_mem_sha = sha256(mem_raw).hexdigest()
                            if actual_mem_sha != expected_mem_sha:
                                raise BridgeError(
                                    f"Family {family!r} members.json SHA-256 {actual_mem_sha[:12]} "
                                    f"disagrees with expected {expected_mem_sha[:12]}"
                                )
                        members_fallback_doc = json.loads(mem_raw)
                    split_tbl = (members_fallback_doc.get("tables") or {}).get(table_key)
                    raw_members = split_tbl.get("members") if isinstance(split_tbl, Mapping) else None
                if not isinstance(raw_members, list) or not raw_members:
                    raise BridgeError(f"Split table {family}/{table_name} declares no members")

                parsed_members: list[tuple[str, dict[str, str], str, int, int]] = []
                for member in raw_members:
                    if not isinstance(member, Mapping):
                        raise BridgeError(f"Malformed split member in {family}/{table_name}")
                    rel_member_key = _safe_rel_path(str(member["key"]), label="member key", allow_equals=True)
                    raw_part = member.get("partition")
                    if isinstance(raw_part, Mapping):
                        pvals = {str(k): str(v) for k, v in raw_part.items()}
                    else:
                        pvals = _parse_hive_partition(rel_member_key)
                    if set(pvals) != set(partition_cols):
                        raise BridgeError(
                            f"Split member {rel_member_key!r} partitions {sorted(pvals)} disagree with {list(partition_cols)}"
                        )
                    m_sha = _strip_sha256_prefix(member.get("sha256"), label=f"{table_name}/{rel_member_key}.sha256")
                    m_size = int(member["byteSize"])
                    m_rows = int(member["rows"] if "rows" in member else member["rowCount"])
                    parsed_members.append((rel_member_key, pvals, m_sha, m_size, m_rows))

                expected_rows = int(table_meta["rows"] if "rows" in table_meta else table_meta["rowCount"])
                expected_size = int(table_meta["byteSize"])
                if sum(item[4] for item in parsed_members) != expected_rows:
                    raise BridgeError(f"Split table {family}/{table_name} member rows do not sum to table rows")
                if sum(item[3] for item in parsed_members) != expected_size:
                    raise BridgeError(f"Split table {family}/{table_name} member bytes do not sum to table byteSize")

                partition_tuples = [tuple((col, item[1][col]) for col in partition_cols) for item in parsed_members]
                needs_part_tiebreaker = len(set(partition_tuples)) < len(partition_tuples)
                if needs_part_tiebreaker:
                    if "_part" in partition_cols:
                        raise BridgeError(f"Split table {family}/{table_name} repeats partition tuples including _part")
                    eff_partition_keys: tuple[str, ...] = (*partition_cols, "_part")
                else:
                    eff_partition_keys = partition_cols

                member_specs: list[CollectedMember] = []
                seen_eff_tuples: set[tuple[tuple[str, str], ...]] = set()
                for idx, (rel_member_key, pvals, m_sha, m_size, m_rows) in enumerate(parsed_members):
                    eff_pvals = dict(pvals)
                    if needs_part_tiebreaker:
                        eff_pvals["_part"] = PurePosixPath(rel_member_key).stem
                    eff_tuple = tuple((col, eff_pvals[col]) for col in eff_partition_keys)
                    if eff_tuple in seen_eff_tuples:
                        raise BridgeError(
                            f"Duplicate partition tuple {eff_tuple!r} on split table {family}/{table_name}"
                        )
                    seen_eff_tuples.add(eff_tuple)
                    remote_key = f"{gen_prefix}/{rel_member_key}"
                    member_specs.append(
                        CollectedMember(
                            table_name=table_name,
                            remote_key=remote_key,
                            url=f"{clean_base}/{remote_key}",
                            sha256=m_sha,
                            size=m_size,
                            rows=m_rows,
                            partition_values=eff_pvals,
                            output_name=f"{table_name}__{idx}",
                        )
                    )
            else:
                eff_partition_keys = ()
                remote_key = f"{gen_prefix}/{table_key}"
                t_sha = _strip_sha256_prefix(table_meta.get("sha256"), label=f"{family}/{table_name}.sha256")
                t_size = int(table_meta["byteSize"])
                t_rows = int(table_meta["rows"] if "rows" in table_meta else table_meta["rowCount"])
                member_specs = [
                    CollectedMember(
                        table_name=table_name,
                        remote_key=remote_key,
                        url=f"{clean_base}/{remote_key}",
                        sha256=t_sha,
                        size=t_size,
                        rows=t_rows,
                        partition_values={},
                        output_name=table_name,
                    )
                ]

            if table_name in declared_schemas:
                raise BridgeError(f"Duplicate logical table {table_name!r} across families")
            declared_schemas[table_name] = arrow_schema
            collected_tables.append(
                CollectedTable(
                    family=family,
                    table_name=table_name,
                    domain=domain,
                    contract=deepcopy(contract),
                    contract_path=contract_path,
                    contract_sha256=contract_sha256,
                    schema_obj=deepcopy(schema_obj),
                    arrow_schema=arrow_schema,
                    partition_keys=eff_partition_keys,
                    members=tuple(member_specs),
                    generation_digest=gen_digest,
                )
            )

    rulemaking_snapshot_id: str | None = None
    rulemaking_latest_sha256: str | None = None
    rulemaking_manifest_sha256: str | None = None
    if include_rulemaking:
        loaded = odcs_contracts.load_enrolled_family_contract("rulemaking", contracts_dir=contracts_dir)
        if loaded is None:
            raise BridgeError(f"Family 'rulemaking' is not enrolled in ODCS contracts at {contracts_dir}")
        rm_contract, rm_contract_path = loaded
        rm_contract_sha256 = sha256(rm_contract_path.read_bytes()).hexdigest()
        rm_domain = str(rm_contract.get("domain") or "spicygov")

        rm_latest_raw, _ = active_fetcher(pub.SNAPSHOT_POINTER)
        rulemaking_latest_sha256 = sha256(rm_latest_raw).hexdigest()
        rm_latest = json.loads(rm_latest_raw)
        if not isinstance(rm_latest, Mapping):
            raise BridgeError(f"{pub.SNAPSHOT_POINTER} must be a JSON object")
        if rm_latest.get("dataset") is not None and rm_latest.get("dataset") != "rulemaking":
            raise BridgeError(f"Unexpected dataset in {pub.SNAPSHOT_POINTER}: {rm_latest.get('dataset')!r}")
        if (
            rm_latest.get("format_version") is not None
            and rm_latest.get("format_version") not in pub.SNAPSHOT_FORMAT_VERSIONS
        ):
            raise BridgeError(
                f"Unsupported format_version in {pub.SNAPSHOT_POINTER}: {rm_latest.get('format_version')!r}"
            )
        snapshot_id = rm_latest.get("snapshot_id")
        manifest_key = rm_latest.get("manifest_key")
        if not isinstance(snapshot_id, str) or not pub.SNAPSHOT_ID.fullmatch(snapshot_id):
            raise BridgeError(f"Invalid rulemaking snapshot_id in {pub.SNAPSHOT_POINTER}: {snapshot_id!r}")
        expected_manifest_key = f"materialized/rulemaking/snapshots/{snapshot_id}/manifest.json"
        if manifest_key != expected_manifest_key:
            raise BridgeError(
                f"Rulemaking manifest_key {manifest_key!r} disagrees with expected {expected_manifest_key!r}"
            )
        rulemaking_snapshot_id = snapshot_id

        rm_manifest_raw, _ = active_fetcher(manifest_key)
        rulemaking_manifest_sha256 = sha256(rm_manifest_raw).hexdigest()
        if rm_latest.get("manifest_sha256") is not None:
            expected_rm_sha = _strip_sha256_prefix(rm_latest["manifest_sha256"], label="rulemaking.manifest_sha256")
            if rulemaking_manifest_sha256 != expected_rm_sha:
                raise BridgeError("Rulemaking manifest.json SHA-256 disagrees with latest.json")
        rm_manifest = json.loads(rm_manifest_raw)
        if not isinstance(rm_manifest, Mapping) or rm_manifest.get("snapshot_id") != snapshot_id:
            raise BridgeError(f"Rulemaking manifest snapshot_id disagrees with {pub.SNAPSHOT_POINTER}")

        for ts_field in ("asserted_at", "published_at"):
            ts_val = rm_manifest.get(ts_field)
            if isinstance(ts_val, str) and ts_val.strip():
                upstream_timestamps.append(_parse_utc_timestamp(ts_val, label=f"rulemaking.{ts_field}"))
                break
        family_generations["rulemaking"] = snapshot_id

        raw_artifacts = rm_manifest.get("artifacts")
        if not isinstance(raw_artifacts, Mapping):
            raw_artifacts = rm_manifest.get("tables")
        if not isinstance(raw_artifacts, Mapping):
            raise BridgeError("Rulemaking manifest.json missing 'artifacts' mapping")

        public_rm_artifacts: dict[str, Mapping[str, Any]] = {}
        for art_name, art in raw_artifacts.items():
            if not isinstance(art_name, str) or not art_name.endswith(".parquet") or not isinstance(art, Mapping):
                continue
            visibility = art.get("visibility", "public")
            if visibility != "public" or art_name == "etl_receipts.parquet":
                continue
            public_rm_artifacts[art_name.removesuffix(".parquet")] = art

        contract_rm_tables = {obj["name"]: obj for obj in rm_contract.get("schema") or ()}
        if set(public_rm_artifacts) != set(contract_rm_tables):
            missing_in_manifest = sorted(set(contract_rm_tables) - set(public_rm_artifacts))
            extra_in_manifest = sorted(set(public_rm_artifacts) - set(contract_rm_tables))
            raise BridgeError(
                f"Public rulemaking tables disagree with rulemaking.odcs.json "
                f"(missing={missing_in_manifest}, extra={extra_in_manifest})"
            )

        expected_rm_prefix = f"materialized/rulemaking/snapshots/{snapshot_id}/"
        for table_name in sorted(contract_rm_tables):
            schema_obj = contract_rm_tables[table_name]
            art = public_rm_artifacts[table_name]
            remote_key = _safe_rel_path(
                str(art.get("remote_key") or f"{expected_rm_prefix}{table_name}.parquet"),
                label="rulemaking remote_key",
                allow_equals=False,
            )
            if not remote_key.startswith(expected_rm_prefix):
                raise BridgeError(
                    f"Rulemaking artifact {table_name!r} remote_key {remote_key!r} outside {expected_rm_prefix!r}"
                )
            art_sha = _strip_sha256_prefix(art.get("sha256"), label=f"rulemaking/{table_name}.sha256")
            art_size = int(art["bytes"] if "bytes" in art else art["byte_size"])
            art_rows = int(art["rows"] if "rows" in art else art["row_count"])
            arrow_schema = odcs_contracts.odcs_schema_to_pyarrow(schema_obj)
            if table_name in declared_schemas:
                raise BridgeError(f"Duplicate logical table {table_name!r} across families")
            declared_schemas[table_name] = arrow_schema
            collected_tables.append(
                CollectedTable(
                    family="rulemaking",
                    table_name=table_name,
                    domain=rm_domain,
                    contract=deepcopy(rm_contract),
                    contract_path=rm_contract_path,
                    contract_sha256=rm_contract_sha256,
                    schema_obj=deepcopy(schema_obj),
                    arrow_schema=arrow_schema,
                    partition_keys=(),
                    members=(
                        CollectedMember(
                            table_name=table_name,
                            remote_key=remote_key,
                            url=f"{clean_base}/{remote_key}",
                            sha256=art_sha,
                            size=art_size,
                            rows=art_rows,
                            partition_values={},
                            output_name=table_name,
                        ),
                    ),
                    generation_digest=snapshot_id,
                )
            )

    if observed_at is not None:
        eff_observed_at = _format_utc_timestamp(_parse_utc_timestamp(observed_at, label="observed_at"))
    elif upstream_timestamps:
        eff_observed_at = _format_utc_timestamp(max(upstream_timestamps))
    else:
        raise BridgeError("Cannot derive deterministic observed_at: no upstream timestamps found and observed_at omitted")

    sorted_tables = tuple(sorted(collected_tables, key=lambda item: item.table_name))
    relationships = _extract_ddk_relationships(sorted_tables, joins_path=joins_path)
    product = _build_odps_product(
        tables=sorted_tables,
        public_base_url=clean_base,
        bridge_prefix=clean_prefix,
        observed_at=eff_observed_at,
        source_index_sha256=source_index_sha256,
        source_index_etag=source_index_etag,
        rulemaking_snapshot_id=rulemaking_snapshot_id,
        rulemaking_latest_sha256=rulemaking_latest_sha256,
        rulemaking_manifest_sha256=rulemaking_manifest_sha256,
        odcs_manifest_sha256=odcs_manifest_sha256,
        family_generations=family_generations,
    )

    return PilotCollection(
        families=selected_families,
        tables=sorted_tables,
        declared_schemas=declared_schemas,
        relationships=relationships,
        product=product,
        observed_at=eff_observed_at,
        public_base_url=clean_base,
        bridge_prefix=clean_prefix,
        source_index_sha256=source_index_sha256,
        source_index_etag=source_index_etag,
        rulemaking_snapshot_id=rulemaking_snapshot_id,
        rulemaking_latest_sha256=rulemaking_latest_sha256,
        rulemaking_manifest_sha256=rulemaking_manifest_sha256,
        odcs_manifest_sha256=odcs_manifest_sha256,
        family_generations=family_generations,
    )


def make_bridge_destination(
    *,
    bucket: str = DEFAULT_BUCKET,
    bridge_prefix: str = BRIDGE_PREFIX,
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    allow_insecure_http: bool = False,
) -> Any:
    """Create the DDK ``RemoteDestination`` targeting ``ddk-catalogs/spicy-regs/``."""
    dp = _load_ddk_publication()
    clean_prefix = validate_bridge_prefix(bridge_prefix)
    clean_base = validate_public_base_url(public_base_url, allow_insecure_http=allow_insecure_http)
    clean_bucket = bucket.strip()
    if not clean_bucket:
        raise ValueError(f"Invalid bucket: {bucket!r}")
    return dp.RemoteDestination(
        owned_prefix=f"s3://{clean_bucket}",
        release_prefix=f"{clean_prefix}/releases",
        pointer_key=f"{clean_prefix}/current.json",
        properties={"public_base_url": clean_base, "bucket": clean_bucket},
    )


def _to_ddk_external_tables(
    collection: PilotCollection,
    *,
    allow_insecure_http: bool = False,
) -> tuple[Any, ...]:
    dp = _load_ddk_publication()
    result: list[Any] = []
    for table in collection.tables:
        members = tuple(
            dp.ExternalParquetObjectSpec(
                table_name=member.table_name,
                remote_key=member.remote_key,
                sha256=member.sha256,
                size=member.size,
                rows=member.rows,
                url=member.url,
                partition_values=dict(member.partition_values),
                output_name=member.output_name,
                allow_insecure_http=allow_insecure_http,
            )
            for member in table.members
        )
        result.append(
            dp.ExternalTableSpec(
                table_name=table.table_name,
                contract=table.contract,
                members=members,
                partition_keys=table.partition_keys,
                contract_object=table.table_name,
                catalog_id=f"urn:spicy-regs:dataset:{_normalize_dataset_alias(table.table_name)}",
                schema_profile=DDK_MANAGED_SCHEMA_V3,
                normalize_arrow_encodings=False,
                check_column_order=True,
            )
        )
    return tuple(result)


def build_pilot_bridge_bundle(
    collection: PilotCollection,
    *,
    output_dir: Path | str,
    bundle_dir: Path | str | None = None,
    bucket: str = DEFAULT_BUCKET,
    dataset: str = DEFAULT_DATASET,
    catalog_id: str = DEFAULT_CATALOG_ID,
    project_id: str = DEFAULT_PROJECT_ID,
    authority: str = DEFAULT_AUTHORITY_ID,
    purpose: str = DEFAULT_PURPOSE,
    audience: str = DEFAULT_AUDIENCE,
    version: str = DEFAULT_VERSION,
    expected_prior_release_id: Any = _SENTINEL,
    expected_pointer: Any | None = None,
    storage: Any | None = None,
    local_parquet_paths: Mapping[str, Path | str] | None = None,
    verify_schema_parity: bool = True,
    synthesize_iceberg: bool = True,
    user_agent: str = BRIDGE_USER_AGENT,
    allow_insecure_http: bool = False,
) -> Any:
    """Assemble a verified DDK catalog + Iceberg v2 + remote publication bundle from a ``PilotCollection``."""
    dp = _load_ddk_publication()
    destination = make_bridge_destination(
        bucket=bucket,
        bridge_prefix=collection.bridge_prefix,
        public_base_url=collection.public_base_url,
        allow_insecure_http=allow_insecure_http,
    )
    ddk_tables = _to_ddk_external_tables(collection, allow_insecure_http=allow_insecure_http)
    eff_prior = ... if expected_prior_release_id is _SENTINEL else expected_prior_release_id
    return dp.build_external_generation_bundle(
        tables=ddk_tables,
        destination=destination,
        output_dir=output_dir,
        bundle_dir=bundle_dir,
        dataset=dataset,
        expected_pointer=expected_pointer,
        expected_prior_release_id=eff_prior,
        catalog_id=catalog_id,
        project_id=project_id,
        version=version,
        authority=authority,
        purpose=purpose,
        observed_at=collection.observed_at,
        audience=audience,
        relationships=collection.relationships,
        product=collection.product,
        exports=("dcat", "graph"),
        verify_schema_parity=verify_schema_parity,
        schema_profile=DDK_MANAGED_SCHEMA_V3,
        normalize_arrow_encodings=False,
        check_column_order=True,
        synthesize_iceberg=synthesize_iceberg,
        storage=storage,
        local_parquet_paths=local_parquet_paths,
        base_url=collection.public_base_url,
        allow_insecure_http=allow_insecure_http,
        user_agent=user_agent,
    )


def _check_iceberg_metadata_locations(bundle: Any, public_base_url: str) -> dict[str, int]:
    pub_base = public_base_url.rstrip("/")
    manifest_doc = json.loads(bundle.plan.manifest_bytes)
    snaps = manifest_doc.get("iceberg_snapshots") or ()
    uploaded_catalog_keys = {
        art["remote_key"]
        for art in (bundle.plan.publish_command.get("catalog_artifacts") or ())
        if isinstance(art, Mapping) and isinstance(art.get("remote_key"), str)
    }
    matched = 0
    for snap in snaps:
        remote_key = snap.get("remote_key")
        meta_loc = snap.get("metadata_location")
        expected = f"{pub_base}/{remote_key.lstrip('/')}" if remote_key else None
        if not expected or meta_loc != expected or remote_key not in uploaded_catalog_keys:
            raise BridgeError(
                f"Iceberg metadata_location {meta_loc!r} disagrees with uploaded remote_key URL {expected!r}"
            )
        matched += 1
    return {"iceberg_snapshots": len(snaps), "metadata_location_matches_remote_key": matched}


def _resolve_live_prior_release_id(
    pointer_key: str,
    *,
    storage: Any | None = None,
    fetcher: AnonymousPublicFetcher | None = None,
) -> str | None:
    raw: bytes | None = None
    if storage is not None:
        try:
            raw = storage.get_bytes(pointer_key)
        except Exception as exc:
            if getattr(exc, "code", None) == "CATALOG-OBJECT-MISSING":
                return None
            raise
    elif fetcher is not None:
        try:
            raw, _ = fetcher(pointer_key)
        except Exception as exc:
            if getattr(exc, "http_status", None) == 404 or getattr(exc, "code", None) == "CATALOG-OBJECT-MISSING":
                return None
            raise
    if raw is None:
        return None
    doc = json.loads(raw)
    if not isinstance(doc, Mapping) or not isinstance(doc.get("release_id"), str):
        raise BridgeError(f"Existing live pointer {pointer_key!r} is missing valid release_id")
    return doc["release_id"]


def dry_run_bridge(
    *,
    families: Sequence[str] | None = None,
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    bridge_prefix: str = BRIDGE_PREFIX,
    bucket: str = DEFAULT_BUCKET,
    storage: Any | None = None,
    fetcher: AnonymousPublicFetcher | None = None,
    contracts_dir: Path = odcs_contracts.DEFAULT_ODCS_DIR,
    joins_path: Path = odcs_contracts.DEFAULT_TABLE_JOINS_PATH,
    output_dir: Path | str | None = None,
    bundle_dir: Path | str | None = None,
    expected_prior_release_id: Any = "auto",
    verify_artifact_roots: bool = True,
    observed_at: str | None = None,
    user_agent: str = BRIDGE_USER_AGENT,
    allow_insecure_http: bool = False,
) -> dict[str, Any]:
    """Run a read-only zero-write dry run against R2 or public HTTPS, verifying strict schema parity."""
    dp = _load_ddk_publication()
    active_fetcher = fetcher or AnonymousPublicFetcher(
        public_base_url,
        user_agent=user_agent,
        allow_insecure_http=allow_insecure_http,
    )
    active_storage = storage if storage is not None else PublicHttpReadOnlyStorage(active_fetcher)
    collection = collect_pilot_external_tables(
        families=families,
        public_base_url=public_base_url,
        bridge_prefix=bridge_prefix,
        fetcher=active_fetcher,
        contracts_dir=contracts_dir,
        joins_path=joins_path,
        verify_artifact_roots=verify_artifact_roots,
        observed_at=observed_at,
        user_agent=user_agent,
        allow_insecure_http=allow_insecure_http,
    )
    pointer_key = f"{collection.bridge_prefix}/current.json"
    if expected_prior_release_id == "auto":
        eff_prior = _resolve_live_prior_release_id(pointer_key, storage=active_storage, fetcher=active_fetcher)
    elif expected_prior_release_id == "none":
        eff_prior = None
    else:
        eff_prior = expected_prior_release_id

    with tempfile.TemporaryDirectory(prefix="spicy-regs-ddk-dryrun-") as tmp_dir:
        cat_out = Path(output_dir) if output_dir is not None else Path(tmp_dir) / "catalog"
        bundle = build_pilot_bridge_bundle(
            collection,
            output_dir=cat_out,
            bundle_dir=bundle_dir,
            bucket=bucket,
            expected_prior_release_id=eff_prior,
            storage=active_storage,
            verify_schema_parity=True,
            synthesize_iceberg=True,
            user_agent=user_agent,
            allow_insecure_http=allow_insecure_http,
        )
        # Schema parity was already verified across every member during bundle assembly;
        # dry_run_remote_publication checks CAS pointer, HEAD size parity, and catalog upload plan.
        dry_receipt = dp.dry_run_remote_publication(
            bundle,
            storage=active_storage,
            expected_prior_release_id=eff_prior,
            verify_schema_parity=False,
            allow_insecure_http=allow_insecure_http,
        )
        iceberg_check = _check_iceberg_metadata_locations(bundle, collection.public_base_url)

    return {
        **dry_receipt,
        "families": list(collection.families),
        "tables": len(collection.tables),
        "parquet_referenced_count": len(dry_receipt["parquet_referenced"]),
        "total_bytes": collection.total_bytes,
        "total_rows": collection.total_rows,
        "observed_at": collection.observed_at,
        "lock_sha256": bundle.lock_sha256,
        "catalog_sha256": bundle.catalog_sha256,
        "relationship_count": len(collection.relationships),
        "schema_parity_passed": len(bundle.schema_parity),
        "schema_parity_total": collection.member_count,
        "catalog_upload_count": len(dry_receipt["catalog_uploads"]),
        "catalog_reused_count": len(dry_receipt["catalog_reused"]),
        "family_generations": dict(collection.family_generations),
        **iceberg_check,
    }


class _PointerCommitHookStorage:
    """Thin ``ObjectStoragePort`` proxy that fires ``on_pointer_commit`` right after ``pointer_key`` PUT succeeds."""

    def __init__(self, inner: Any, *, pointer_key: str, on_pointer_commit: Any) -> None:
        self._inner = inner
        self._pointer_key = pointer_key
        self._on_pointer_commit = on_pointer_commit

    def head_object(self, key: str) -> dict[str, Any]:
        return self._inner.head_object(key)

    def get_bytes(
        self,
        key: str,
        *,
        byte_range: tuple[int, int] | None = None,
        if_match: str | None = None,
    ) -> bytes:
        return self._inner.get_bytes(key, byte_range=byte_range, if_match=if_match)

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> dict[str, Any]:
        result = self._inner.put_bytes(
            key,
            data,
            content_type=content_type,
            if_match=if_match,
            if_none_match=if_none_match,
        )
        if key == self._pointer_key:
            self._on_pointer_commit(result)
        return result

    def delete_object(self, key: str) -> None:
        self._inner.delete_object(key)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def publish_bridge(
    *,
    families: Sequence[str] | None = None,
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    bridge_prefix: str = BRIDGE_PREFIX,
    bucket: str | None = None,
    storage: Boto3BridgeStorageAdapter | Any | None = None,
    fetcher: AnonymousPublicFetcher | None = None,
    contracts_dir: Path = odcs_contracts.DEFAULT_ODCS_DIR,
    joins_path: Path = odcs_contracts.DEFAULT_TABLE_JOINS_PATH,
    output_dir: Path | str | None = None,
    bundle_dir: Path | str | None = None,
    expected_prior_release_id: Any = "auto",
    verify_member_bytes: bool = False,
    schema_readback_mode: str = "auto",
    verify_artifact_roots: bool = True,
    purge_pointer_cache: bool = True,
    observed_at: str | None = None,
    user_agent: str = BRIDGE_USER_AGENT,
    allow_insecure_http: bool = False,
) -> dict[str, Any]:
    """Publish the zero-copy DDK catalog bridge with CAS preconditions and anonymous HTTP readback."""
    if schema_readback_mode not in {"auto", "full", "range"}:
        raise ValueError(
            f"schema_readback_mode must be 'auto', 'full', or 'range', got {schema_readback_mode!r}"
        )
    if not verify_member_bytes and schema_readback_mode == "full":
        raise ValueError("verify_member_bytes=True is required when schema_readback_mode='full'")
    dp = _load_ddk_publication()
    active_storage = (
        storage
        if storage is not None
        else Boto3BridgeStorageAdapter(bucket=bucket, bridge_prefix=bridge_prefix)
    )
    eff_bucket = getattr(active_storage, "bucket", bucket or DEFAULT_BUCKET)
    active_fetcher = fetcher or AnonymousPublicFetcher(
        public_base_url,
        user_agent=user_agent,
        allow_insecure_http=allow_insecure_http,
    )
    collection = collect_pilot_external_tables(
        families=families,
        public_base_url=public_base_url,
        bridge_prefix=bridge_prefix,
        fetcher=active_fetcher,
        contracts_dir=contracts_dir,
        joins_path=joins_path,
        verify_artifact_roots=verify_artifact_roots,
        observed_at=observed_at,
        user_agent=user_agent,
        allow_insecure_http=allow_insecure_http,
    )
    pointer_key = f"{collection.bridge_prefix}/current.json"
    if expected_prior_release_id == "auto":
        eff_prior = _resolve_live_prior_release_id(pointer_key, storage=active_storage, fetcher=active_fetcher)
    elif expected_prior_release_id == "none":
        eff_prior = None
    else:
        eff_prior = expected_prior_release_id

    pointer_url = f"{collection.public_base_url}/{pointer_key}"
    publish_storage: Any = (
        _PointerCommitHookStorage(
            active_storage,
            pointer_key=pointer_key,
            on_pointer_commit=lambda _res: cloudflare.purge_urls([pointer_url]),
        )
        if purge_pointer_cache
        else active_storage
    )

    with tempfile.TemporaryDirectory(prefix="spicy-regs-ddk-publish-") as tmp_dir:
        cat_out = Path(output_dir) if output_dir is not None else Path(tmp_dir) / "catalog"
        bundle = build_pilot_bridge_bundle(
            collection,
            output_dir=cat_out,
            bundle_dir=bundle_dir,
            bucket=eff_bucket,
            expected_prior_release_id=eff_prior,
            storage=active_storage,
            verify_schema_parity=True,
            synthesize_iceberg=True,
            user_agent=user_agent,
            allow_insecure_http=allow_insecure_http,
        )
        iceberg_check = _check_iceberg_metadata_locations(bundle, collection.public_base_url)
        pub_receipt = dp.publish_and_verify_remote_publication(
            bundle,
            storage=publish_storage,
            anonymous_fetch=active_fetcher,
            expected_prior_release_id=eff_prior,
            declared_schemas=collection.declared_schemas,
            schema_check_order=True,
            schema_check_nullability=False,
            schema_readback_mode=schema_readback_mode,
            verify_schema_parity=False,
            verify_member_bytes=verify_member_bytes,
            allow_insecure_http=allow_insecure_http,
            user_agent=active_fetcher.user_agent,
        )

    return {
        **pub_receipt,
        "families": list(collection.families),
        "tables": len(collection.tables),
        "parquet_referenced_count": len(pub_receipt["parquet_referenced"]),
        "total_bytes": collection.total_bytes,
        "total_rows": collection.total_rows,
        "observed_at": collection.observed_at,
        "relationship_count": len(collection.relationships),
        "family_generations": dict(collection.family_generations),
        **iceberg_check,
    }


def load_pilot_declared_schemas(
    *,
    families: Sequence[str] | None = None,
    contracts_dir: Path = odcs_contracts.DEFAULT_ODCS_DIR,
) -> dict[str, pa.Schema]:
    """Load exact ``pa.Schema`` declarations for enrolled families from committed ODCS contracts."""
    selected = tuple(dict.fromkeys(families if families is not None else odcs_contracts.PILOT_FAMILIES))
    schemas: dict[str, pa.Schema] = {}
    for family in selected:
        loaded = odcs_contracts.load_enrolled_family_contract(family, contracts_dir=contracts_dir)
        if loaded is None:
            raise BridgeError(f"Family {family!r} is not enrolled in ODCS contracts at {contracts_dir}")
        contract, _ = loaded
        for table_name, arrow_schema in odcs_contracts.contract_declared_schemas(contract).items():
            if table_name in schemas:
                raise BridgeError(f"Duplicate declared schema for table {table_name!r}")
            schemas[table_name] = arrow_schema
    return schemas


def verify_live_bridge_readback(
    *,
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
    bridge_prefix: str = BRIDGE_PREFIX,
    bucket: str = DEFAULT_BUCKET,
    families: Sequence[str] | None = None,
    declared_schemas: Mapping[str, pa.Schema] | None = None,
    contracts_dir: Path = odcs_contracts.DEFAULT_ODCS_DIR,
    fetcher: AnonymousPublicFetcher | None = None,
    expected_release_id: str | None = None,
    expected_lock_sha256: str | None = None,
    verify_member_bytes: bool = False,
    schema_readback_mode: str = "auto",
    user_agent: str = BRIDGE_USER_AGENT,
    timeout: float = 30.0,
    allow_insecure_http: bool = False,
) -> dict[str, Any]:
    """Independently verify the live anonymous DDK catalog, Iceberg metadata, and Parquet schemas."""
    dp = _load_ddk_publication()
    active_fetcher = fetcher or AnonymousPublicFetcher(
        public_base_url,
        user_agent=user_agent,
        timeout=timeout,
        allow_insecure_http=allow_insecure_http,
    )
    destination = make_bridge_destination(
        bucket=bucket,
        bridge_prefix=bridge_prefix,
        public_base_url=public_base_url,
        allow_insecure_http=allow_insecure_http,
    )
    eff_schemas = (
        dict(declared_schemas)
        if declared_schemas is not None
        else load_pilot_declared_schemas(families=families, contracts_dir=contracts_dir)
    )
    return dp.verify_http_consumer_readback(
        active_fetcher,
        destination,
        expected_release_id=expected_release_id,
        expected_lock_sha256=expected_lock_sha256,
        verify_member_bytes=verify_member_bytes,
        allow_insecure_http=allow_insecure_http,
        declared_schemas=eff_schemas,
        schema_check_order=True,
        schema_check_nullability=False,
        schema_readback_mode=schema_readback_mode,
        user_agent=active_fetcher.user_agent,
        timeout=timeout,
    )


def _parse_families_arg(raw: Sequence[str] | None) -> tuple[str, ...]:
    if not raw:
        return odcs_contracts.PILOT_FAMILIES
    parsed: list[str] = []
    for item in raw:
        for part in item.split(","):
            slug = part.strip()
            if not slug:
                continue
            if slug == "pilot":
                parsed.extend(odcs_contracts.PILOT_FAMILIES)
            else:
                parsed.append(slug)
    return tuple(dict.fromkeys(parsed)) or odcs_contracts.PILOT_FAMILIES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="publish-ddk-bridge",
        description="Zero-copy DDK catalog and Iceberg v2 bridge over immutable spicy-regs R2 generations.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Read-only dry run verifying strict ODCS Parquet schema parity and zero Parquet uploads.",
    )
    mode.add_argument(
        "--publish",
        action="store_true",
        help="Publish additive shadow DDK catalog and Iceberg v2 metadata with CAS preconditions.",
    )
    mode.add_argument(
        "--verify-live",
        action="store_true",
        help="Run standalone anonymous HTTP consumer readback against the live bridge pointer.",
    )
    parser.add_argument(
        "--families",
        action="append",
        default=None,
        help="Family set to bridge: 'pilot' (default, all 8 pilot families) or comma-separated/repeatable slugs.",
    )
    parser.add_argument(
        "--expect-prior-release-id",
        default="auto",
        help="CAS prior release expectation: 'auto' (re-read live pointer), 'none' (bootstrap), or 64-hex release_id.",
    )
    parser.add_argument(
        "--public-base-url",
        default=DEFAULT_PUBLIC_BASE_URL,
        help=f"Public HTTPS origin for anonymous reads (default: {DEFAULT_PUBLIC_BASE_URL}).",
    )
    parser.add_argument(
        "--bridge-prefix",
        default=BRIDGE_PREFIX,
        help=f"Isolated R2 prefix owned by the bridge (default: {BRIDGE_PREFIX}).",
    )
    parser.add_argument(
        "--contracts-dir",
        type=Path,
        default=odcs_contracts.DEFAULT_ODCS_DIR,
        help="Directory holding committed ODCS v3.2.0 contracts and manifest.json.",
    )
    parser.add_argument(
        "--bundle-dir",
        type=Path,
        default=None,
        help="Optional directory to materialize staged current.json, release.manifest.json, and Iceberg metadata.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to write the JSON receipt/summary.",
    )
    parser.add_argument(
        "--observed-at",
        default=None,
        help="Optional ISO-8601 UTC timestamp override (defaults to deterministic max upstream timestamp).",
    )
    parser.add_argument(
        "--user-agent",
        default=BRIDGE_USER_AGENT,
        help=f"Explicit HTTP User-Agent header (default: {BRIDGE_USER_AGENT}).",
    )
    parser.add_argument(
        "--verify-member-bytes",
        action="store_true",
        help="Download full Parquet member bytes during readback instead of O(KiB) Range footer checks.",
    )
    parser.add_argument(
        "--allow-insecure-http",
        action="store_true",
        help="Permit loopback http:// URLs for local tests only.",
    )
    return parser


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted(_json_safe(item) for item in value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    families = _parse_families_arg(args.families)
    try:
        if args.dry_run:
            result = dry_run_bridge(
                families=families,
                public_base_url=args.public_base_url,
                bridge_prefix=args.bridge_prefix,
                contracts_dir=args.contracts_dir,
                bundle_dir=args.bundle_dir,
                expected_prior_release_id=args.expect_prior_release_id,
                observed_at=args.observed_at,
                user_agent=args.user_agent,
                allow_insecure_http=args.allow_insecure_http,
            )
        elif args.publish:
            result = publish_bridge(
                families=families,
                public_base_url=args.public_base_url,
                bridge_prefix=args.bridge_prefix,
                contracts_dir=args.contracts_dir,
                bundle_dir=args.bundle_dir,
                expected_prior_release_id=args.expect_prior_release_id,
                verify_member_bytes=args.verify_member_bytes,
                observed_at=args.observed_at,
                user_agent=args.user_agent,
                allow_insecure_http=args.allow_insecure_http,
            )
        else:
            result = verify_live_bridge_readback(
                public_base_url=args.public_base_url,
                bridge_prefix=args.bridge_prefix,
                families=families,
                contracts_dir=args.contracts_dir,
                verify_member_bytes=args.verify_member_bytes,
                user_agent=args.user_agent,
                allow_insecure_http=args.allow_insecure_http,
            )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    rendered = json.dumps(_json_safe(result), indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
