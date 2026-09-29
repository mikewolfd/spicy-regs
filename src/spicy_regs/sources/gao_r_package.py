"""GAO reports copied once from the CetiAlphaFive/gao R package, for the reports no route of ours holds.

The R package ``gao`` by Jack T. Rametta (GPL-3.0-or-later, github.com/CetiAlphaFive/gao) ships
``inst/extdata/gao_links.rds``: one row per gao.gov product page, 1922 to 2026, updated daily. The owner approved one
copy (2026-09-28) and accepted the copyleft caveat. It is the lowest-precedence route: it adds only reports no row
holds, and never replaces a cell any route of ours states. A later read by one of our routes takes the row over, and
the package's cells fill only what ours leave NULL.

What the copy takes, and why:
- The key is the page, as every route keys a row: the product URL's last segment, lowercased and percent-decoded.
  The package's own ``report_id`` repeats across pages (``A-51604`` has four), so it becomes ``report_number``.
- A page on Drupal's ``-N`` twin of a page we hold, with the same number (``gao-16-75sp-0``), is that product: held.
- Legal decisions stay out, as in :mod:`spicy_regs.sources.gao_listing`. The package marks all 6,566 B-numbered rows
  ``requester_type = legal_decision`` and no other row, so the B-number decides.
- ``title``, the release date (else the issue date), ``summary`` as ``abstract``, the one GAO topic as ``topics_json``
  and ``agencies_affected`` as ``agencies_json``. The package states no product type. An empty string or R's ``NA``
  is unstated, never a value.

The ``.rds`` was converted once to Parquet outside this project (``~/Work/corpora/gao-r-package-2026-09-29/``,
``convert.py`` and ``receipt.json``: 56,557 rows and 17 columns before and after, every non-null count and every value
equal), so reading it needs nothing this project does not already have. Both files are pinned by digest here.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from spicy_regs.sources.gao_govinfo import report_type

SOURCE = "gao_r_package"
REPOSITORY = "https://github.com/CetiAlphaFive/gao"
#: The package commit copied: "update gao links (+4 new)", 2026-09-26T10:45:43Z.
PACKAGE_COMMIT = "6f612305d3c2ccc285decd18577f44e1452366d1"
RDS_PATH = "inst/extdata/gao_links.rds"
RDS_URL = f"https://raw.githubusercontent.com/CetiAlphaFive/gao/{PACKAGE_COMMIT}/{RDS_PATH}"
#: The exact ``.rds`` at that commit (equal to ``RDS_URL``'s bytes): 3,907,096 bytes.
RDS_SHA256 = "sha256:2979706dca1f9c09e6fce60a70e356034ba0c2cc21765819b1810484bc16de37"
RDS_BYTES = 3_907_096
#: Its one lossless conversion to Parquet, the file this project reads.
PARQUET_SHA256 = "sha256:e7f2bb95cdc4625294bc9987195524c3f68369f60b7efd957a2287b993aef362"
PARQUET_BYTES = 6_763_656
PACKAGE_ROWS = 56_557
LICENSE = "GPL-3.0-or-later"
ATTRIBUTION = "Jack T. Rametta, gao: Access U.S. Government Accountability Office Reports (R package)"
LEGAL_DECISION = "legal_decision"
PRODUCT_PAGE = re.compile(r"https://www\.gao\.gov/products/([^/?#]+)")
_TWIN = re.compile(r"-\d+\Z")


class GaoRPackageError(ValueError):
    """A package row this copy cannot place."""


def _stated(value: object) -> str | None:
    """The package's value, or None where it states nothing: R's ``NA`` reads as None or NaN, and ``""`` is empty."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


def page_id(url: str) -> str:
    """The product page's id as every route keys it: the URL's last segment, percent-decoded and lowercased."""
    match = PRODUCT_PAGE.fullmatch(url.strip())
    if match is None:
        raise GaoRPackageError(f"Package row is not a gao.gov product page: {url}")
    return unquote(match[1]).lower()


def _list_json(value: str | None) -> str | None:
    return None if value is None else json.dumps([part.strip() for part in value.split(";") if part.strip()])


def shape(record: Mapping[str, Any]) -> dict | None:
    """One package row as a ``gao_reports`` row, or None for a legal decision."""
    number = _stated(record.get("report_id"))
    if _stated(record.get("requester_type")) == LEGAL_DECISION or (number or "").upper().startswith("B-"):
        return None
    url = _stated(record.get("url"))
    if url is None:
        raise GaoRPackageError(f"Package row {number} has no url")
    report_id = page_id(url)
    return {
        "report_id": report_id,
        "title": _stated(record.get("title")),
        "report_type": report_type(report_id),
        "published_date": _stated(record.get("released")) or _stated(record.get("published")),
        "abstract": _stated(record.get("summary")),
        "agencies_json": _list_json(_stated(record.get("agencies_affected"))),
        "topics_json": _list_json(_stated(record.get("topics"))),
        "url": url,
        "source": SOURCE,
        "product_type": None,
        "report_number": number,
    }


def held_as(row: Mapping[str, Any], held: Mapping[str, Mapping[str, Any]]) -> str | None:
    """The held row this package row is: the same page, or the base page of a ``-N`` twin with the same number."""
    if row["report_id"] in held:
        return row["report_id"]
    suffix = _TWIN.search(row["report_id"])
    base = row["report_id"][: suffix.start()] if suffix else None
    if base in held and (held[base].get("report_number") or base.upper()) == (row["report_number"] or "").upper():
        return base
    return None


def package_rows(records: Iterable[Mapping[str, Any]]) -> tuple[list[dict], Counter[str]]:
    """Every package row shaped, one per page, and counts of what the copy left out."""
    counts: Counter[str] = Counter()
    rows: dict[str, dict] = {}
    for record in records:
        counts["package_rows"] += 1
        row = shape(record)
        if row is None:
            counts["legal_decisions_left_out"] += 1
            continue
        if row["report_id"] in rows:
            raise GaoRPackageError(f"Package lists page {row['report_id']} twice")
        rows[row["report_id"]] = row
    counts["shaped"] = len(rows)
    return list(rows.values()), counts


def read_package(path: Path) -> list[dict[str, Any]]:
    """The converted package table's rows, after its digest is checked against :data:`PARQUET_SHA256`."""
    import hashlib

    import pyarrow.parquet as pq

    digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != PARQUET_SHA256:
        raise GaoRPackageError(f"{path} is not the reviewed conversion ({digest})")
    rows = pq.read_table(path).to_pylist()
    if len(rows) != PACKAGE_ROWS:
        raise GaoRPackageError(f"{path} holds {len(rows)} rows, not {PACKAGE_ROWS}")
    return rows
