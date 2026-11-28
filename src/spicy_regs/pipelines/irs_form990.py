"""Discover, stream, carry forward, and build multi-year IRS Form 990 Parquet generations."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
import gc
import hashlib
import json
from pathlib import Path
import re
import resource
import shutil
import sys
from tempfile import TemporaryDirectory
import time
from typing import Any

import pyarrow.parquet as pq

from spicy_regs.source_evidence import CaptureEvidence, JOURNAL, acquisition_evidence
from spicy_regs.transforms.build_irs_form990 import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_QUARANTINED_RATIO,
    DEFAULT_MAX_QUARANTINED_ROWS,
    PARTITIONED_TABLES,
    SCHEMAS,
    build_irs_form990_year,
)

FAMILY = "irs-form990"
DEFAULT_USER_AGENT = "SpicyGov/1.0 (https://spicygov.ai; contact@spicygov.ai)"
MIN_REQUEST_INTERVAL_SECONDS = 1.5
INDEX_PUBLISHER_ID = "irs-form990-index"
INDEX_POLICY_DECISION = "ss-umeo-irs990-annual-index-full"
XML_ARCHIVES_PUBLISHER_ID = "irs-form990-xml-archives"
XML_ARCHIVES_POLICY_DECISION = "ss-umeo-irs990-teos-zip-hash-only"

_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}", re.ASCII)
_PARTITION_YEAR_RE = re.compile(r"submission_year=(20\d{2})", re.ASCII)
_CHUNK_BYTES = 1024 * 1024


def _peak_rss_bytes() -> int:
    """Process peak resident set size in bytes (macOS reports bytes; Linux reports KiB)."""
    ru_maxrss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return ru_maxrss if sys.platform == "darwin" else ru_maxrss * 1024


def _parser_version() -> str:
    from spicy_docs.sources import irs_form990 as source

    digest = hashlib.sha256(Path(source.__file__).read_bytes()).hexdigest()
    return "irs-form990-v2:sha256:" + digest


def _validate_years(years: Sequence[int]) -> list[int]:
    cleaned: list[int] = []
    for year in years:
        if type(year) is not int or not 2011 <= year <= 2099:
            raise ValueError(f"Invalid IRS Form 990 submission year: {year!r}")
        if year not in cleaned:
            cleaned.append(year)
    if not cleaned:
        raise ValueError("Specify at least one IRS Form 990 submission year")
    return sorted(cleaned)


class _RequestPacer:
    """Enforce a strictly sequential minimum delay between upstream HTTP requests."""

    def __init__(
        self,
        min_interval: float = MIN_REQUEST_INTERVAL_SECONDS,
        *,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        if min_interval < 0:
            raise ValueError("min_interval must be non-negative")
        self._min_interval = float(min_interval)
        self._clock = time.monotonic if clock is None else clock
        self._sleeper = time.sleep if sleeper is None else sleeper
        self._last_request: float | None = None

    def wait(self) -> None:
        if self._last_request is not None and self._min_interval > 0:
            elapsed = self._clock() - self._last_request
            remaining = self._min_interval - elapsed
            if remaining > 0:
                self._sleeper(remaining)
        self._last_request = self._clock()


def _check_http_access(status_code: int, url: str) -> None:
    from spicy_docs.transport.credentials import CredentialRefusedError

    if status_code in (401, 403):
        raise CredentialRefusedError(
            f"IRS upstream refused request (HTTP {status_code}) for {url}; aborting without retry or bypass"
        )


def discover(
    years: Sequence[int],
    *,
    transport=None,
    min_interval: float = MIN_REQUEST_INTERVAL_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Fetch the official IRS downloads page once and sequentially HEAD requested year assets."""
    import httpx
    from spicy_docs.sources import irs_form990 as source

    selected_years = _validate_years(years)
    pacer = _RequestPacer(min_interval, clock=clock, sleeper=sleeper)
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}

    with httpx.Client(headers=headers, timeout=60, follow_redirects=False, transport=transport) as client:
        pacer.wait()
        page_response = client.get(source.DOWNLOADS_PAGE_URL)
        _check_http_access(page_response.status_code, source.DOWNLOADS_PAGE_URL)
        if page_response.status_code != 200:
            raise source.IRS990Error(
                f"IRS downloads page returned unexpected HTTP {page_response.status_code}"
            )
        links = source.parse_downloads_page(page_response.content)

        by_year: dict[int, dict[str, list[str]]] = {
            year: {"index": [], "archive": []} for year in selected_years
        }
        for link in links:
            if link.year in by_year:
                by_year[link.year][link.kind].append(link.url)

        discovered_years: dict[str, Any] = {}
        for year in selected_years:
            index_urls = by_year[year]["index"]
            archive_urls = sorted(by_year[year]["archive"])
            if len(index_urls) != 1:
                raise source.IRS990Error(
                    f"IRS downloads page lists {len(index_urls)} index CSVs for submission_year={year}; expected 1"
                )
            if not archive_urls:
                raise source.IRS990Error(
                    f"IRS downloads page lists zero XML archives for submission_year={year}"
                )

            def probe(url: str, *, kind: str) -> dict[str, Any]:
                source.validate_url(url)
                pacer.wait()
                observed_at = datetime.now(UTC).isoformat()
                resp = client.head(url)
                _check_http_access(resp.status_code, url)
                if resp.status_code != 200:
                    raise source.IRS990Error(f"IRS HEAD returned HTTP {resp.status_code} for {url}")
                raw_lengths = resp.headers.get_list("content-length")
                raw_etag = resp.headers.get("etag")
                etag = raw_etag.strip() if isinstance(raw_etag, str) and raw_etag.strip() else None
                content_length: int | None
                if not raw_lengths:
                    if not etag:
                        raise source.IRS990Error(
                            f"IRS HEAD omitted both Content-Length and ETag for {url}"
                        )
                    content_length = None
                else:
                    if len(raw_lengths) != 1:
                        raise source.IRS990Error(
                            f"IRS HEAD returned multiple Content-Length headers for {url}"
                        )
                    raw_length = raw_lengths[0]
                    if not raw_length or not raw_length.isascii() or not raw_length.isdigit():
                        raise source.IRS990Error(
                            f"IRS HEAD returned malformed Content-Length {raw_length!r} for {url}"
                        )
                    content_length = int(raw_length)
                    bound = source.MAX_INDEX_BYTES if kind == "index" else source.MAX_ARCHIVE_BYTES
                    if not 0 < content_length <= bound:
                        raise source.IRS990Error(
                            f"IRS HEAD Content-Length {content_length} is outside allowed {kind} bound (1..{bound}) for {url}"
                        )
                return {
                    "url": url,
                    "content_length": content_length,
                    "etag": etag,
                    "last_modified": resp.headers.get("last-modified"),
                    "observed_at": observed_at,
                }

            discovered_years[str(year)] = {
                "submission_year": year,
                "index": probe(index_urls[0], kind="index"),
                "archives": [probe(url, kind="archive") for url in archive_urls],
            }

    return {
        "format": "spicy-irs-form990-discovery",
        "version": 1,
        "downloads_page_url": source.DOWNLOADS_PAGE_URL,
        "discovered_at": datetime.now(UTC).isoformat(),
        "years": discovered_years,
    }


def _entry_etag(entry: Mapping[Any, Any]) -> str | None:
    etag = entry.get("etag")
    if isinstance(etag, str):
        cleaned = etag.strip()
        if cleaned:
            return cleaned
    return None


def _entry_size(entry: Mapping[Any, Any]) -> int | None:
    """Return positive int size from byte_size/content_length, None if both absent/None, or -1 if invalid."""
    sizes: list[int] = []
    for key in ("byte_size", "content_length"):
        val = entry.get(key)
        if val is not None:
            if type(val) is not int or val <= 0:
                return -1
            sizes.append(val)
    if not sizes:
        return None
    if len(sizes) == 2 and sizes[0] != sizes[1]:
        return -1
    return sizes[0]


def _fingerprint_entry(entry: Mapping[Any, Any]) -> tuple[str, str | None, int | None]:
    url = str(entry.get("url") or entry.get("request_url") or "")
    return (url, _entry_etag(entry), _entry_size(entry))


def _entry_fingerprint_matches(cur_entry: Mapping[Any, Any], old_entry: Mapping[Any, Any]) -> bool:
    cur_url, cur_etag, cur_size = _fingerprint_entry(cur_entry)
    old_url, old_etag, old_size = _fingerprint_entry(old_entry)
    if not cur_url or cur_url != old_url:
        return False
    if not cur_etag or not old_etag or cur_etag != old_etag:
        return False
    if old_size is None or old_size <= 0:
        return False
    if cur_size is None:
        return True
    return cur_size > 0 and cur_size == old_size


def _year_fingerprint_matches(
    discovered_year: Mapping[str, Any],
    prior_year_summary: Mapping[str, Any],
) -> bool:
    """Return True when index + every archive URL, non-empty ETag, and (when present) byte size match."""
    prior_index = prior_year_summary.get("source_index")
    prior_archives = prior_year_summary.get("source_archives")
    cur_index = discovered_year.get("index")
    cur_archives = discovered_year.get("archives")
    if (
        not isinstance(cur_index, Mapping)
        or not isinstance(prior_index, Mapping)
        or not isinstance(cur_archives, Sequence)
        or isinstance(cur_archives, (str, bytes))
        or not isinstance(prior_archives, list)
        or not cur_archives
        or len(cur_archives) != len(prior_archives)
    ):
        return False
    if not _entry_fingerprint_matches(cur_index, prior_index):
        return False
    return all(
        isinstance(cur_item, Mapping)
        and isinstance(old_item, Mapping)
        and _entry_fingerprint_matches(cur_item, old_item)
        for cur_item, old_item in zip(cur_archives, prior_archives, strict=True)
    )


def _discover_prior_partition_years(carry_forward_dir: Path | None) -> list[int]:
    if carry_forward_dir is None or not carry_forward_dir.is_dir():
        return []
    year_sets: dict[str, set[int]] = {}
    for table_name in SCHEMAS:
        table_dir = carry_forward_dir / table_name
        if not table_dir.is_dir():
            year_sets[table_name] = set()
            continue
        found: set[int] = set()
        for child in table_dir.iterdir():
            if child.is_dir():
                match = _PARTITION_YEAR_RE.fullmatch(child.name)
                if match and any(child.glob("part-*.parquet")):
                    found.add(int(match.group(1)))
        year_sets[table_name] = found
    unique_sets = list(year_sets.values())
    if not any(unique_sets):
        return []
    if len({frozenset(s) for s in unique_sets}) != 1:
        raise ValueError(
            f"Asymmetric prior partition years across tables in {carry_forward_dir}: "
            f"{ {k: sorted(v) for k, v in year_sets.items()} }"
        )
    return sorted(unique_sets[0])


def _can_copy_year_partitions(carry_forward_dir: Path | None, year: int) -> bool:
    if carry_forward_dir is None:
        return False
    for table_name, expected_schema in SCHEMAS.items():
        part_dir = carry_forward_dir / table_name / f"submission_year={year}"
        parts = sorted(part_dir.glob("part-*.parquet")) if part_dir.is_dir() else []
        if not parts:
            return False
        for part_path in parts:
            try:
                actual = pq.read_schema(part_path)
            except Exception:
                return False
            if not actual.equals(expected_schema):
                return False
    return True


def _copy_year_partitions(carry_forward_dir: Path, candidate_dir: Path, year: int) -> dict[str, int]:
    rows: dict[str, int] = {}
    for table_name in SCHEMAS:
        src_dir = carry_forward_dir / table_name / f"submission_year={year}"
        parts = sorted(src_dir.glob("part-*.parquet"))
        if not parts:
            raise ValueError(f"Missing partition members for submission_year={year}: {table_name}")
        table_rows = 0
        for src in parts:
            dest = candidate_dir / table_name / f"submission_year={year}" / src.name
            if dest.exists():
                raise ValueError(f"Duplicate partition target for submission_year={year}: {table_name}/{src.name}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dest)
            table_rows += pq.read_metadata(dest).num_rows
        rows[table_name] = table_rows
    return rows


def _receipt_etag(receipt: Mapping[str, Any]) -> str | None:
    if isinstance(receipt.get("etag"), str) and receipt["etag"].strip():
        return receipt["etag"].strip()
    headers = receipt.get("response_headers")
    if isinstance(headers, list):
        for pair in headers:
            if isinstance(pair, (list, tuple)) and len(pair) == 2 and str(pair[0]).lower() == "etag" and pair[1]:
                cleaned = str(pair[1]).strip()
                if cleaned:
                    return cleaned
    return None


def _match_verified_local_blob(store, sha: str, size: int, observed_at: str) -> tuple[Path, str, int, str] | None:
    if not isinstance(sha, str) or not _SHA256_RE.fullmatch(sha) or type(size) is not int or size <= 0:
        return None
    try:
        described = store.describe_blob(sha, size)
    except FileNotFoundError:
        return None
    if described["encoding"] != "raw":
        raise ValueError("Raw original must be stored unwrapped to be read as a file")
    return store.root / described["object"], sha, size, observed_at


def _find_local_raw_match(
    store,
    entry: Mapping[str, Any],
    *,
    prior_years_meta: Mapping[str, Any] | None = None,
) -> tuple[Path, str, int, str] | None:
    """Return ``(blob_path, sha256, byte_size, observed_at)`` for an exact verified local blob.

    Matches either by explicit ``sha256``/``response_sha256`` + ``byte_size`` or, for ``discover()``
    manifests omitting ``sha256``, by verified ``(url, etag, byte_size)`` (or unambiguous ``(url, etag)``
    when ``HEAD`` omitted ``Content-Length``) against ``prior_years_meta`` or ``CentralRawArchiveStore``
    acquisition receipts.
    """
    sha = entry.get("sha256") or entry.get("response_sha256")
    size = _entry_size(entry)
    observed_at = str(entry.get("observed_at") or datetime.now(UTC).isoformat())
    if size is not None and size <= 0:
        return None
    if isinstance(sha, str) and isinstance(size, int):
        return _match_verified_local_blob(store, sha, size, observed_at)

    url = str(entry.get("url") or entry.get("request_url") or "")
    etag = _entry_etag(entry)
    if not url or not etag:
        return None

    # 1. Check prior population summary fingerprints for an unambiguous verified match.
    prior_matches: dict[tuple[str, int], tuple[Path, str, int, str]] = {}
    for year_meta in (prior_years_meta or {}).values():
        if not isinstance(year_meta, Mapping):
            continue
        candidates: list[Any] = []
        if isinstance(year_meta.get("source_index"), Mapping):
            candidates.append(year_meta["source_index"])
        if isinstance(year_meta.get("source_archives"), list):
            candidates.extend(year_meta["source_archives"])
        for item in candidates:
            if not isinstance(item, Mapping):
                continue
            item_url = str(item.get("url") or item.get("request_url") or "")
            item_etag = _entry_etag(item)
            item_size = _entry_size(item)
            item_sha = item.get("sha256") or item.get("response_sha256")
            if (
                item_url == url
                and item_etag == etag
                and item_size is not None
                and item_size > 0
                and (size is None or item_size == size)
                and isinstance(item_sha, str)
            ):
                matched = _match_verified_local_blob(store, item_sha, item_size, observed_at)
                if matched is not None:
                    prior_matches[(item_sha, item_size)] = matched
    if len(prior_matches) == 1:
        return next(iter(prior_matches.values()))
    if len(prior_matches) > 1:
        return None

    # 2. Scan CentralRawArchiveStore acquisition receipts for an unambiguous verified match.
    acquisitions_dir = store.root / "acquisitions" / "sha256"
    if not acquisitions_dir.is_dir():
        return None
    receipt_matches: dict[tuple[str, int], tuple[Path, str, int, str]] = {}
    for receipt_path in sorted(acquisitions_dir.rglob("*")):
        if not receipt_path.is_file():
            continue
        try:
            doc = json.loads(receipt_path.read_bytes())
        except (OSError, ValueError):
            continue
        if not isinstance(doc, Mapping) or doc.get("format") != "spicygov-acquisition-1":
            continue
        receipt = doc.get("receipt")
        if not isinstance(receipt, Mapping):
            continue
        urls = {
            receipt.get(key)
            for key in ("requested_url", "resolved_url", "source_url", "original_location")
            if isinstance(receipt.get(key), str)
        }
        if url not in urls:
            continue
        candidate_size = receipt.get("byte_size")
        if type(candidate_size) is not int or candidate_size <= 0:
            continue
        if size is not None and candidate_size != size:
            continue
        if _receipt_etag(receipt) != etag:
            continue
        candidate_sha = receipt.get("sha256")
        if not isinstance(candidate_sha, str):
            continue
        obs = str(receipt.get("observed_at") or observed_at)
        matched = _match_verified_local_blob(store, candidate_sha, candidate_size, obs)
        if matched is not None:
            receipt_matches[(candidate_sha, candidate_size)] = matched
    if len(receipt_matches) == 1:
        return next(iter(receipt_matches.values()))
    return None


def _validate_manifest_expected_size(
    entry: Mapping[str, Any], *, bound: int, kind: str, url: str
) -> int | None:
    from spicy_docs.sources import irs_form990 as source

    expected_size = _entry_size(entry)
    if expected_size is None:
        return None
    if not 0 < expected_size <= bound:
        raise source.IRS990Error(
            f"IRS {kind} manifest size {expected_size} is outside allowed bound (1..{bound}) for {url}"
        )
    return expected_size


def _check_response_content_length(response, *, bound: int, kind: str, url: str) -> None:
    from spicy_docs.sources import irs_form990 as source

    raw_lengths = response.headers.get_list("content-length")
    if not raw_lengths:
        return
    if len(raw_lengths) != 1:
        raise source.IRS990Error(f"IRS {kind} GET returned multiple Content-Length headers for {url}")
    raw_length = raw_lengths[0]
    if not raw_length or not raw_length.isascii() or not raw_length.isdigit():
        raise source.IRS990Error(
            f"IRS {kind} GET returned malformed Content-Length {raw_length!r} for {url}"
        )
    declared = int(raw_length)
    if not 0 < declared <= bound:
        raise source.IRS990Error(
            f"IRS {kind} GET Content-Length {declared} is outside allowed bound (1..{bound}) for {url}"
        )


def _latest_capture_event(evidence: CaptureEvidence, *, stage: str, url: str) -> dict[str, Any]:
    events = [json.loads(line) for line in (evidence.artifact_dir / JOURNAL).read_text(encoding="utf-8").splitlines()]
    matched = [
        event
        for event in events
        if event.get("event") == "capture"
        and event.get("stage") == stage
        and event.get("requested_url") == url
    ]
    if not matched:
        raise ValueError(f"Downloaded IRS original lacks a capture receipt in evidence journal: {url}")
    return matched[-1]


def _stream_download_full_index(
    index_owner,
    evidence: CaptureEvidence,
    entry: Mapping[str, Any],
    *,
    transport=None,
    pacer: _RequestPacer,
    user_agent: str,
) -> tuple[Path, dict[str, Any]]:
    """Download an annual index CSV under policy='full' directly into CentralRawArchiveStore."""
    import httpx
    from spicy_docs.sources import irs_form990 as source

    url = str(entry.get("url") or entry.get("request_url") or "")
    source.validate_url(url)
    expected_size = _validate_manifest_expected_size(
        entry, bound=source.MAX_INDEX_BYTES, kind="index", url=url
    )
    expected_etag = _entry_etag(entry)
    if expected_size is None and not expected_etag:
        raise source.IRS990Error(f"IRS index manifest omitted both Content-Length and ETag for {url}")

    pacer.wait()
    retained = index_owner.transport(
        transport,
        stage="irs-form990-index",
        max_bytes=source.MAX_INDEX_BYTES,
        response_header_names=("etag", "last-modified"),
    )
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    digest = hashlib.sha256()
    byte_size = 0
    actual_etag: str | None = None
    with httpx.Client(headers=headers, timeout=300, follow_redirects=False, transport=retained) as client:
        with client.stream("GET", url) as response:
            _check_http_access(response.status_code, url)
            if response.status_code != 200:
                raise source.IRS990Error(f"IRS index GET returned HTTP {response.status_code} for {url}")
            _check_response_content_length(response, bound=source.MAX_INDEX_BYTES, kind="index", url=url)
            raw_actual_etag = response.headers.get("etag")
            actual_etag = raw_actual_etag.strip() if isinstance(raw_actual_etag, str) and raw_actual_etag.strip() else None
            if expected_etag and actual_etag and actual_etag != expected_etag:
                raise source.IRS990Error(
                    f"IRS index ETag changed for {url}: expected {expected_etag!r}, got {actual_etag!r}"
                )
            for chunk in response.iter_bytes(_CHUNK_BYTES):
                byte_size += len(chunk)
                if byte_size > source.MAX_INDEX_BYTES:
                    raise source.IRS990Error(
                        f"IRS index stream exceeded bound {source.MAX_INDEX_BYTES} for {url}"
                    )
                digest.update(chunk)

    if byte_size <= 0:
        raise source.IRS990Error(f"IRS index GET returned empty body for {url}")
    sha256 = "sha256:" + digest.hexdigest()
    if expected_size is not None and byte_size != expected_size:
        raise source.IRS990Error(f"IRS index size differs from manifest for {url}: {byte_size} != {expected_size}")

    receipt = _latest_capture_event(evidence, stage="irs-form990-index", url=url)
    if receipt["sha256"] != sha256 or receipt["byte_size"] != byte_size:
        raise source.IRS990Error(f"IRS index receipt digest differs from streamed bytes for {url}")

    described = evidence.store.describe_blob(sha256, byte_size)
    if described["encoding"] != "raw":
        raise ValueError("Raw original must be stored unwrapped to be read as a file")
    blob_path = evidence.store.root / described["object"]
    descriptor = {
        "request_url": url,
        "response_sha256": sha256,
        "byte_size": byte_size,
        "observed_at": receipt["observed_at"],
        **({"etag": actual_etag or expected_etag} if (actual_etag or expected_etag) else {}),
    }
    return blob_path, descriptor


def _stream_download_hash_only_zip(
    zip_owner,
    evidence: CaptureEvidence,
    entry: Mapping[str, Any],
    dest_path: Path,
    *,
    transport=None,
    pacer: _RequestPacer,
    user_agent: str,
) -> dict[str, Any]:
    """Download one TEOS ZIP under policy='hash_only' into a single private temp file."""
    import httpx
    from spicy_docs.sources import irs_form990 as source

    url = str(entry.get("url") or entry.get("request_url") or "")
    source.validate_url(url)
    expected_size = _validate_manifest_expected_size(
        entry, bound=source.MAX_ARCHIVE_BYTES, kind="archive", url=url
    )
    expected_etag = _entry_etag(entry)
    if expected_size is None and not expected_etag:
        raise source.IRS990Error(f"IRS archive manifest omitted both Content-Length and ETag for {url}")

    pacer.wait()
    retained = zip_owner.transport(transport, stage="irs-form990-archive", max_bytes=source.MAX_ARCHIVE_BYTES)
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    digest = hashlib.sha256()
    byte_size = 0
    actual_etag: str | None = None
    with httpx.Client(headers=headers, timeout=600, follow_redirects=False, transport=retained) as client:
        with client.stream("GET", url) as response:
            _check_http_access(response.status_code, url)
            if response.status_code != 200:
                raise source.IRS990Error(f"IRS archive GET returned HTTP {response.status_code} for {url}")
            _check_response_content_length(response, bound=source.MAX_ARCHIVE_BYTES, kind="archive", url=url)
            raw_actual_etag = response.headers.get("etag")
            actual_etag = raw_actual_etag.strip() if isinstance(raw_actual_etag, str) and raw_actual_etag.strip() else None
            if expected_etag and actual_etag and actual_etag != expected_etag:
                raise source.IRS990Error(
                    f"IRS archive ETag changed for {url}: expected {expected_etag!r}, got {actual_etag!r}"
                )
            with dest_path.open("wb") as sink:
                for chunk in response.iter_bytes(_CHUNK_BYTES):
                    byte_size += len(chunk)
                    if byte_size > source.MAX_ARCHIVE_BYTES:
                        raise source.IRS990Error(
                            f"IRS archive stream exceeded bound {source.MAX_ARCHIVE_BYTES} for {url}"
                        )
                    digest.update(chunk)
                    sink.write(chunk)

    if byte_size <= 0:
        raise source.IRS990Error(f"IRS archive GET returned empty body for {url}")
    sha256 = "sha256:" + digest.hexdigest()
    if expected_size is not None and byte_size != expected_size:
        raise source.IRS990Error(f"IRS archive size differs from manifest for {url}: {byte_size} != {expected_size}")

    receipt = _latest_capture_event(evidence, stage="irs-form990-archive", url=url)
    if receipt["sha256"] != sha256 or receipt["byte_size"] != byte_size:
        raise source.IRS990Error(f"IRS archive receipt digest differs from streamed bytes for {url}")

    return {
        "request_url": url,
        "response_sha256": sha256,
        "byte_size": byte_size,
        "observed_at": receipt["observed_at"],
        **({"etag": actual_etag or expected_etag} if (actual_etag or expected_etag) else {}),
    }


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _index_return_type_counts(index_map) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for oid in index_map:
        rtype = index_map.return_type(oid)
        counts[rtype or "unknown"] += 1
    return dict(sorted(counts.items()))


def sync_build(
    manifest: Mapping[str, Any],
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
    carry_forward_generation: Path | None = None,
    carry_forward_summary: Path | None = None,
    carry_forward_unlisted: bool = True,
    max_zips: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_quarantined_rows: int = DEFAULT_MAX_QUARANTINED_ROWS,
    max_quarantined_ratio: float = DEFAULT_MAX_QUARANTINED_RATIO,
    strict_versions: bool = False,
    offline: bool = False,
    min_interval: float = MIN_REQUEST_INTERVAL_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    transport=None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> tuple[Any, Path, dict[str, Any]]:
    """Build a multi-year submission_year-partitioned, receipts-free IRS Form 990 generation.

    Uses dual-tier evidence: ``policy="full"`` for annual index CSVs (reusing exact retained
    blobs in ``CentralRawArchiveStore``) and ``policy="hash_only"`` for sequentially streamed
    TEOS ZIPs (one private temp file at a time, unlinked immediately after Parquet flush;
    already-retained local ZIPs are also reused under ``policy="hash_only"`` so bulk ZIPs are
    not uploaded to R2 evidence). Unchanged closed years from ``carry_forward_generation`` are
    copied forward byte-for-byte (requiring prior summary metadata/fingerprints) so R2 publish
    executes server-side ``copy_object`` without re-downloading. Per-ZIP ``checkpoints/<year>.json``
    files record progress audit state; resume operates at the submission-year boundary via
    ``--carry-forward-generation``. Truncated builds (``max_zips > 0``) seal as ``local-partial``
    so they cannot overwrite live R2.
    """
    from spicy_docs.sources import irs_form990 as source

    if manifest.get("format") != "spicy-irs-form990-discovery" or manifest.get("version") != 1:
        raise ValueError("Unsupported IRS Form 990 discovery manifest")
    manifest_years = manifest.get("years")
    if not isinstance(manifest_years, Mapping) or not manifest_years:
        raise ValueError("IRS Form 990 discovery manifest contains no submission years")
    if type(max_zips) is not int or max_zips < 0:
        raise ValueError("max_zips must be a non-negative integer (0 = all)")

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists() or (output_dir / "candidate").exists():
        raise ValueError("Use a fresh output directory for each IRS Form 990 generation")
    candidate_dir = output_dir / "candidate"
    checkpoints_dir = output_dir / "checkpoints"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    prior_summary_doc: dict[str, Any] = {}
    if carry_forward_summary is not None and Path(carry_forward_summary).is_file():
        prior_summary_doc = json.loads(Path(carry_forward_summary).read_text(encoding="utf-8"))
    prior_years_meta: Mapping[str, Any] = prior_summary_doc.get("years", {}) if isinstance(prior_summary_doc, Mapping) else {}

    started = time.monotonic()
    peak_temp_zip_bytes = 0
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    pacer = _RequestPacer(min_interval, clock=clock, sleeper=sleeper)
    parser_ver = _parser_version()

    try:
        read_snapshot = None
        if read_index is not None:
            from spicy_regs.pipelines.raw_releases import inherit_read_index

            read_snapshot = inherit_read_index(evidence, read_index)
        manifest_copy_path = output_dir / "discovery-manifest.json"
        _write_json_atomic(manifest_copy_path, dict(manifest))
        evidence.retain_control(manifest_copy_path, stage="irs990-discovery-manifest")

        index_owner = evidence.for_source(
            INDEX_PUBLISHER_ID,
            "full",
            parser_version=parser_ver,
            policy_decision_id=INDEX_POLICY_DECISION,
        )
        zip_owner = evidence.for_source(
            XML_ARCHIVES_PUBLISHER_ID,
            "hash_only",
            parser_version=parser_ver,
            policy_decision_id=XML_ARCHIVES_POLICY_DECISION,
        )

        requested_years = _validate_years([int(k) for k in manifest_years])
        used_inputs: list[dict[str, Any]] = []
        years_summary: dict[str, Any] = {}
        all_quarantined: list[dict[str, Any]] = []

        # Carry forward any unlisted closed years already present in carry_forward_generation.
        if carry_forward_unlisted and carry_forward_generation is not None:
            for prior_year in _discover_prior_partition_years(carry_forward_generation):
                if prior_year in requested_years:
                    continue
                prior_year_meta = prior_years_meta.get(str(prior_year))
                if (
                    not isinstance(prior_year_meta, Mapping)
                    or not isinstance(prior_year_meta.get("source_index"), Mapping)
                    or not isinstance(prior_year_meta.get("source_archives"), list)
                    or not prior_year_meta["source_archives"]
                ):
                    raise ValueError(
                        f"Cannot carry forward unlisted closed year {prior_year}: missing prior year "
                        "metadata/fingerprints in carry_forward_summary"
                    )
                if not _can_copy_year_partitions(carry_forward_generation, prior_year):
                    raise ValueError(
                        f"Prior carry-forward year {prior_year} failed schema verification in {carry_forward_generation}"
                    )
                copied_rows = _copy_year_partitions(carry_forward_generation, candidate_dir, prior_year)
                years_summary[str(prior_year)] = {
                    **dict(prior_year_meta),
                    "submission_year": prior_year,
                    "mode": "carried_forward_unlisted",
                    "rows": copied_rows,
                }
                evidence.event(
                    "irs990-year-carried-forward",
                    submission_year=prior_year,
                    mode="unlisted_prior_year",
                    rows=copied_rows,
                )

        for year in requested_years:
            year_key = str(year)
            year_spec = manifest_years[year_key]
            index_entry = year_spec["index"]
            archive_entries = list(year_spec["archives"])
            if max_zips > 0:
                archive_entries = archive_entries[:max_zips]
            if not archive_entries:
                raise ValueError(f"IRS Form 990 submission_year={year} has zero archives selected")

            # Case B: requested year's discovered index + ZIP fingerprints match prior generation identically.
            prior_meta = prior_years_meta.get(year_key)
            if (
                max_zips == 0
                and carry_forward_generation is not None
                and isinstance(prior_meta, Mapping)
                and _year_fingerprint_matches(year_spec, prior_meta)
                and _can_copy_year_partitions(carry_forward_generation, year)
            ):
                copied_rows = _copy_year_partitions(carry_forward_generation, candidate_dir, year)
                years_summary[year_key] = {
                    **dict(prior_meta),
                    "submission_year": year,
                    "mode": "carried_forward",
                    "rows": copied_rows,
                }
                evidence.event(
                    "irs990-year-carried-forward",
                    submission_year=year,
                    mode="fingerprint_matched",
                    rows=copied_rows,
                )
                continue

            # Acquire or reuse the annual index CSV under policy="full".
            index_url = str(index_entry.get("url") or index_entry.get("request_url"))
            source.validate_url(index_url)
            local_index = _find_local_raw_match(evidence.store, index_entry, prior_years_meta=prior_years_meta)
            if local_index is not None:
                index_path, index_sha, index_size, index_obs = local_index
                saved_idx = evidence.retain_file(
                    index_path,
                    stage="irs-form990-index",
                    raw_source=True,
                    original_location=index_url,
                    source_url=index_url,
                    observed_at=index_obs,
                )
                if saved_idx["sha256"] != index_sha:
                    raise ValueError(f"IRS retained index digest differs for {index_url}")
                index_descriptor = {
                    "request_url": index_url,
                    "response_sha256": index_sha,
                    "byte_size": index_size,
                    "observed_at": index_obs,
                    **({"etag": index_entry["etag"]} if index_entry.get("etag") else {}),
                }
            else:
                if offline:
                    raise FileNotFoundError(
                        f"Offline IRS Form 990 build missing retained annual index in raw archive: {index_url}"
                    )
                index_path, index_descriptor = _stream_download_full_index(
                    index_owner,
                    evidence,
                    index_entry,
                    transport=transport,
                    pacer=pacer,
                    user_agent=user_agent,
                )
            used_inputs.append({
                "location": index_descriptor["request_url"],
                "sha256": index_descriptor["response_sha256"],
                "bytes": index_descriptor["byte_size"],
                "asset_kind": "index",
            })

            with index_path.open("rb") as idx_stream:
                index_map = source.load_index_map(idx_stream, submission_year=year)
            index_summary = {
                "url": index_descriptor["request_url"],
                "sha256": index_descriptor["response_sha256"],
                "byte_size": index_descriptor["byte_size"],
                "total_rows": index_map.total_rows,
                "unique_object_ids": len(index_map),
                "duplicate_identical_rows": index_map.duplicate_identical_rows,
                "by_return_type": _index_return_type_counts(index_map),
            }

            archive_dispositions: list[dict[str, Any]] = []
            source_archives_meta: list[dict[str, Any]] = []
            checkpoint_path = checkpoints_dir / f"{year}.json"
            completed_checkpoints: list[dict[str, Any]] = []
            pending_urls = [str(item.get("url") or item.get("request_url")) for item in archive_entries]

            def iter_year_archives(year_index_map) -> Iterator[Any]:
                nonlocal peak_temp_zip_bytes
                with TemporaryDirectory(dir=output_dir, prefix=f".zip-spool-{year}-") as spool_dir:
                    spool = Path(spool_dir)
                    for idx, archive_entry in enumerate(archive_entries):
                        zip_started = time.monotonic()
                        zip_url = str(archive_entry.get("url") or archive_entry.get("request_url"))
                        source.validate_url(zip_url)
                        temp_zip: Path | None = None
                        try:
                            local_zip = _find_local_raw_match(
                                evidence.store, archive_entry, prior_years_meta=prior_years_meta
                            )
                            if local_zip is not None:
                                zip_path, zip_sha, zip_size, zip_obs = local_zip
                                saved_zip = zip_owner.retain_file(
                                    zip_path,
                                    stage="irs-form990-archive",
                                    raw_source=True,
                                    original_location=zip_url,
                                    source_url=zip_url,
                                    observed_at=zip_obs,
                                )
                                if saved_zip["sha256"] != zip_sha:
                                    raise ValueError(f"IRS retained archive digest differs for {zip_url}")
                                archive_descriptor = {
                                    "request_url": zip_url,
                                    "response_sha256": zip_sha,
                                    "byte_size": zip_size,
                                    "observed_at": zip_obs,
                                    **({"etag": archive_entry["etag"]} if archive_entry.get("etag") else {}),
                                }
                                stream_path = zip_path
                                evidence_mode = "hash_only_local_reuse"
                            else:
                                if offline:
                                    raise FileNotFoundError(
                                        f"Offline IRS Form 990 build missing retained archive in raw archive: {zip_url}"
                                    )
                                temp_zip = spool / f"archive-{idx:04d}.zip"
                                archive_descriptor = _stream_download_hash_only_zip(
                                    zip_owner,
                                    evidence,
                                    archive_entry,
                                    temp_zip,
                                    transport=transport,
                                    pacer=pacer,
                                    user_agent=user_agent,
                                )
                                peak_temp_zip_bytes = max(peak_temp_zip_bytes, temp_zip.stat().st_size)
                                stream_path = temp_zip
                                evidence_mode = "hash_only"
                                used_inputs.append({
                                    "location": archive_descriptor["request_url"],
                                    "sha256": archive_descriptor["response_sha256"],
                                    "bytes": archive_descriptor["byte_size"],
                                    "asset_kind": "archive",
                                })

                            source_archives_meta.append({
                                "url": archive_descriptor["request_url"],
                                "sha256": archive_descriptor["response_sha256"],
                                "byte_size": archive_descriptor["byte_size"],
                                "content_length": archive_descriptor["byte_size"],
                                "etag": archive_descriptor.get("etag"),
                                "evidence_mode": evidence_mode,
                            })

                            archive_stream = source.iter_archive_returns(
                                stream_path,
                                index_by_oid=year_index_map,
                                archive_sha256=str(archive_descriptor["response_sha256"]),
                                return_types=("990",),
                                on_anomaly="yield",
                            )
                            for item in archive_stream:
                                if isinstance(item, source.IRS990ParsedReturn):
                                    item.record["source_locator"].update({
                                        "index": index_descriptor,
                                        "archive": archive_descriptor,
                                    })
                                yield item

                            disp = archive_stream.disposition.to_dict()
                            disp["url"] = zip_url
                            disp["evidence_mode"] = evidence_mode
                            archive_dispositions.append(disp)
                            completed_checkpoints.append({
                                "url": zip_url,
                                "sha256": archive_descriptor["response_sha256"],
                                "byte_size": archive_descriptor["byte_size"],
                                "etag": archive_descriptor.get("etag"),
                                "evidence_mode": evidence_mode,
                                "disposition": disp,
                                "elapsed_seconds": round(time.monotonic() - zip_started, 3),
                            })
                            _write_json_atomic(
                                checkpoint_path,
                                {
                                    "format": "spicy-irs-form990-checkpoint",
                                    "version": 1,
                                    "submission_year": year,
                                    "index": index_descriptor,
                                    "completed_zips": completed_checkpoints,
                                    "pending_zips": pending_urls[idx + 1 :],
                                    "peak_rss_bytes": _peak_rss_bytes(),
                                    "updated_at": datetime.now(UTC).isoformat(),
                                },
                            )
                        finally:
                            if temp_zip is not None:
                                temp_zip.unlink(missing_ok=True)

            year_quarantined_path = output_dir / f"quarantined-anomalies-{year}.json"
            year_summary = build_irs_form990_year(
                year,
                iter_year_archives(index_map),
                candidate_dir,
                batch_size=batch_size,
                max_quarantined_rows=max_quarantined_rows,
                max_quarantined_ratio=max_quarantined_ratio,
                strict_versions=strict_versions,
                index_summary=index_summary,
                archive_dispositions=archive_dispositions,
                quarantined_out=year_quarantined_path,
            )
            # Release the per-year compact index map (~150-200 MiB) before the next year or generation admission.
            del index_map
            gc.collect()
            # Attach exact source fingerprints so future runs can carry forward closed years.
            year_summary["mode"] = "built"
            year_summary["source_index"] = {
                "url": index_descriptor["request_url"],
                "sha256": index_descriptor["response_sha256"],
                "byte_size": index_descriptor["byte_size"],
                "content_length": index_descriptor["byte_size"],
                "etag": index_descriptor.get("etag"),
            }
            year_summary["source_archives"] = source_archives_meta
            years_summary[year_key] = year_summary

            if year_quarantined_path.is_file():
                q_doc = json.loads(year_quarantined_path.read_text(encoding="utf-8"))
                all_quarantined.extend(q_doc.get("rows", ()))
                year_quarantined_path.unlink()

            evidence.event(
                "irs990-year-built",
                submission_year=year,
                rows=year_summary["rows"],
                return_version_counts=year_summary["return_version_counts"],
                excluded_by_return_type=year_summary["excluded_by_return_type"],
                duplicate_identical_oids=year_summary["duplicate_identical_oids"],
                quarantined_count=year_summary["quarantined_count"],
                archives_processed=len(source_archives_meta),
            )

        total_rows = {
            table_name: sum(int(y["rows"][table_name]) for y in years_summary.values())
            for table_name in SCHEMAS
        }
        population_summary: dict[str, Any] = {
            "format": "spicy-irs-form990-population-summary",
            "version": 1,
            "family": FAMILY,
            "generated_at": datetime.now(UTC).isoformat(),
            "submission_years": sorted(int(y) for y in years_summary),
            "max_zips": max_zips,
            "truncated": max_zips > 0,
            "rows": total_rows,
            "quarantined_count": len(all_quarantined),
            "telemetry": {
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "peak_rss_bytes": _peak_rss_bytes(),
                "peak_temp_zip_bytes": peak_temp_zip_bytes,
            },
            "years": {str(y): years_summary[str(y)] for y in sorted(int(k) for k in years_summary)},
        }
        summary_path = output_dir / "irs990-population-summary.json"
        _write_json_atomic(summary_path, population_summary)
        evidence.retain_control(summary_path, stage="irs990-population-summary")

        if all_quarantined:
            by_reason = Counter(str(item.get("reason", "unknown")) for item in all_quarantined)
            quarantined_path = output_dir / "quarantined-anomalies.json"
            _write_json_atomic(
                quarantined_path,
                {
                    "format": "spicy-irs-form990-quarantined-anomalies",
                    "version": 1,
                    "count": len(all_quarantined),
                    "by_reason": dict(sorted(by_reason.items())),
                    "rows": all_quarantined,
                },
            )
            evidence.retain_control(quarantined_path, stage="irs990-quarantined-anomalies")

        if used_inputs:
            evidence.declare_used_inputs(used_inputs, scope_id="irs-form990-teos-streaming")

        from spicy_regs.contract_types import described_schema
        from spicy_regs.generations import build_generation

        table_dirs = [candidate_dir / name for name in SCHEMAS]
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=table_dirs,
            expected_keys=tuple(f"{name}.parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            publication_status="local-partial" if max_zips > 0 else "complete-family",
            partitioned=PARTITIONED_TABLES,
            receipt_path=None,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir, population_summary


def acquire(originals, output_dir, *, transport=None):
    """Acquire caller-selected URL/size/ETag descriptors; never crawl or guess a filing URL."""
    from spicy_docs.sources import irs_form990 as source
    from spicy_regs.pipelines.raw_releases import download_original

    captured = []
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        for selected in originals:
            bound = source.MAX_INDEX_BYTES if selected["url"].endswith(".csv") else source.MAX_ARCHIVE_BYTES
            captured.append(
                download_original(
                    evidence,
                    lambda acquirer, store, item=selected: source.acquire_original(acquirer, store=store, **item),
                    stage="irs-original",
                    max_bytes=bound,
                    validate_url=source.validate_url,
                    transport=transport,
                )
            )
    return captured


def validate_selection(selection):
    from spicy_docs.sources import irs_form990 as source

    if set(selection) != {"submission_year", "index", "archive", "object_ids"}:
        raise ValueError("IRS filing selection fields differ")
    year = selection["submission_year"]
    if type(year) is not int or not 2011 <= year <= 2099:
        raise ValueError("IRS selection requires an explicit index year")
    for kind in ("index", "archive"):
        item = selection[kind]
        if set(item) != {"request_url", "response_sha256", "byte_size", "observed_at"}:
            raise ValueError("IRS original descriptor fields differ")
        source.validate_url(item["request_url"])
        if f"/{year}/" not in item["request_url"] or item["request_url"].endswith(".csv") != (kind == "index"):
            raise ValueError("IRS original differs from the selected year or format")
        if kind == "index" and item["request_url"] != f"{source.BASE_URL}{year}/index_{year}.csv":
            raise ValueError("IRS index URL differs from selected year")
        bound = source.MAX_INDEX_BYTES if kind == "index" else source.MAX_ARCHIVE_BYTES
        if type(item["byte_size"]) is not int or not 0 < item["byte_size"] <= bound:
            raise ValueError("IRS original exceeds byte bound")
        if _SHA256_RE.fullmatch(item["response_sha256"]) is None:
            raise ValueError("IRS original digest is malformed")
        if datetime.fromisoformat(item["observed_at"]).utcoffset() is None:
            raise ValueError("IRS capture requires a timezone")
    return selection


def load_records(selection, store):
    """Verify original identities and replay the exact selected membership from the retained index and ZIP."""
    from spicy_docs.sources import irs_form990 as source
    from spicy_regs.pipelines.raw_releases import raw_original

    validate_selection(selection)
    originals = {
        name: raw_original(store, item["response_sha256"], item["byte_size"])
        for name, item in ((name, selection[name]) for name in ("index", "archive"))
    }
    with originals["index"].open("rb") as stream:
        filings = source.read_index(
            stream, submission_year=selection["submission_year"], object_ids=selection["object_ids"]
        )
    with originals["archive"].open("rb") as stream:
        records = source.read_selected_returns(
            stream, filings=filings, archive_sha256=selection["archive"]["response_sha256"]
        )
    for record in records:
        record["source_locator"].update({"index": selection["index"], "archive": selection["archive"]})
    return records, originals


def build(selection, output_dir, *, archive_root=None, read_index=None):
    """Build a submission_year-partitioned generation from an explicit offline selection."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root
    from spicy_regs.contract_types import described_schema
    from spicy_regs.pipelines.raw_releases import inherit_read_index

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists() or (output_dir / "candidate").exists():
        raise ValueError("Use a fresh output directory for each IRS generation")
    store = CentralRawArchiveStore(archive_root or default_raw_archive_root(), create=False)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        records, originals = load_records(selection, store)
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="irs-selection")
        for name, original in originals.items():
            descriptor = selection[name]
            saved = evidence.retain_file(
                original,
                stage="irs-original",
                raw_source=True,
                original_location=descriptor["request_url"],
                source_url=descriptor["request_url"],
                observed_at=descriptor["observed_at"],
            )
            if saved["sha256"] != descriptor["response_sha256"]:
                raise ValueError("IRS retained original differs from selection")
        candidate_dir = output_dir / "candidate"
        summary_path = output_dir / "irs990-population-summary.json"
        build_irs_form990_year(
            selection["submission_year"],
            records,
            candidate_dir,
            max_quarantined_rows=0,
            strict_versions=True,
            summary_out=summary_path,
        )
        evidence.retain_control(summary_path, stage="irs990-population-summary")
        evidence.declare_used_inputs(
            [
                {
                    "location": selection[name]["request_url"],
                    "sha256": selection[name]["response_sha256"],
                    "bytes": selection[name]["byte_size"],
                    "asset_kind": name,
                }
                for name in ("index", "archive")
            ],
            scope_id="irs-form990-selected-filings",
        )
        evidence.event(
            "irs-selected-filings",
            object_ids=selection["object_ids"],
            submission_year=selection["submission_year"],
            coverage="Explicit selected index membership and XML members only; not a full-year or archive census",
        )
        from spicy_regs.generations import build_generation

        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=[candidate_dir / name for name in SCHEMAS],
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            partitioned=PARTITIONED_TABLES,
            receipt_path=None,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def _parse_years_arg(raw: str) -> list[int]:
    items = [part.strip() for part in raw.split(",") if part.strip()]
    if not items or any(not re.fullmatch(r"\d{4}", item) for item in items):
        raise ValueError(f"Invalid --years value: {raw!r}")
    return _validate_years([int(item) for item in items])


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    disc = commands.add_parser("discover", help="Discover official IRS TEOS index and XML archive URLs via sequential HEAD")
    disc.add_argument("--years", required=True, help="Comma-separated submission years (e.g. 2023,2024,2025,2026)")
    disc.add_argument("--output", type=Path, required=True, help="Output JSON path for the pinned discovery manifest")
    disc.add_argument("--min-interval", type=float, default=MIN_REQUEST_INTERVAL_SECONDS)
    disc.add_argument("--user-agent", default=DEFAULT_USER_AGENT)

    sync = commands.add_parser("sync-build", help="Stream multi-year IRS Form 990 ZIPs into a partitioned generation")
    sync.add_argument("--manifest", type=Path, required=True, help="Discovery or pinned local replay manifest JSON")
    sync.add_argument("--output-dir", type=Path, required=True, help="Fresh build output directory")
    sync.add_argument("--archive-root", type=Path, default=None, help="Optional CentralRawArchiveStore root")
    sync.add_argument("--read-index", type=Path, default=None, help="The published index this build read, as `spicy-regs mirror` saved it")
    sync.add_argument(
        "--carry-forward-generation",
        type=Path,
        default=None,
        help="Optional mirrored prior irs-form990 generation directory for closed-year partition carry-forward",
    )
    sync.add_argument(
        "--carry-forward-summary",
        type=Path,
        default=None,
        help="Optional prior irs990-population-summary.json for fingerprint-matched year carry-forward",
    )
    sync.add_argument("--no-carry-forward-unlisted", action="store_true")
    sync.add_argument(
        "--max-zips",
        type=int,
        default=0,
        help="Optional cap on ZIPs processed per year (0 = all; >0 seals a non-publishable local-partial build)",
    )
    sync.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    sync.add_argument("--max-quarantined-rows", type=int, default=DEFAULT_MAX_QUARANTINED_ROWS)
    sync.add_argument("--max-quarantined-ratio", type=float, default=DEFAULT_MAX_QUARANTINED_RATIO)
    sync.add_argument("--strict-versions", action="store_true")
    sync.add_argument("--offline", action="store_true", help="Refuse any network call; require all assets in raw archive")
    sync.add_argument("--min-interval", type=float, default=MIN_REQUEST_INTERVAL_SECONDS)
    sync.add_argument("--user-agent", default=DEFAULT_USER_AGENT)

    capture = commands.add_parser("acquire", help="Acquire explicit URL/size/ETag originals")
    capture.add_argument("--originals", type=Path, required=True, help="JSON list of official URL/size/ETag selections")
    capture.add_argument("--output-dir", type=Path, required=True)
    capture.add_argument("--captures-out", type=Path, required=True)

    replay = commands.add_parser("build", help="Replay an explicit offline selection into a partitioned generation")
    replay.add_argument("--selection", type=Path, required=True)
    replay.add_argument("--output-dir", type=Path, required=True)
    replay.add_argument("--archive-root", type=Path, default=None)
    replay.add_argument("--read-index", type=Path, default=None, help="The published index this build read, as `spicy-regs mirror` saved it")

    args = parser.parse_args(argv)
    if args.command == "discover":
        manifest = discover(
            _parse_years_arg(args.years),
            min_interval=args.min_interval,
            user_agent=args.user_agent,
        )
        _write_json_atomic(args.output, manifest)
        print(json.dumps({
            "manifest": str(args.output),
            "years": sorted(int(y) for y in manifest["years"]),
            "archives": {y: len(spec["archives"]) for y, spec in manifest["years"].items()},
        }, sort_keys=True))
    elif args.command == "sync-build":
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        artifact, evidence_dir, summary = sync_build(
            manifest,
            args.output_dir,
            archive_root=args.archive_root,
            read_index=args.read_index,
            carry_forward_generation=args.carry_forward_generation,
            carry_forward_summary=args.carry_forward_summary,
            carry_forward_unlisted=not args.no_carry_forward_unlisted,
            max_zips=args.max_zips,
            batch_size=args.batch_size,
            max_quarantined_rows=args.max_quarantined_rows,
            max_quarantined_ratio=args.max_quarantined_ratio,
            strict_versions=args.strict_versions,
            offline=args.offline,
            min_interval=args.min_interval,
            user_agent=args.user_agent,
        )
        print(json.dumps({
            "artifactDigest": artifact.pin.artifact_digest,
            "logicalId": artifact.pin.logical_id,
            "sourceEvidence": str(evidence_dir),
            "rows": summary["rows"],
            "submissionYears": summary["submission_years"],
            "telemetry": summary["telemetry"],
        }, sort_keys=True))
    elif args.command == "acquire":
        captured = acquire(json.loads(args.originals.read_text()), args.output_dir)
        args.captures_out.write_text(json.dumps(captured, indent=2) + "\n")
        print(json.dumps({"captures": str(args.captures_out)}))
    else:
        artifact, evidence_dir = build(
            json.loads(args.selection.read_text()),
            args.output_dir,
            archive_root=args.archive_root,
            read_index=args.read_index,
        )
        print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "sourceEvidence": str(evidence_dir)}))


if __name__ == "__main__":
    main()
