"""GAO's reports and testimony from 1989 to 2008, read from GovInfo's closed GAOREPORTS listing.

GovInfo's ``GAOREPORTS`` collection holds GAO products issued from 1989-11-15 to
2008-09-18 and adds nothing later; on 2026-09-28 every package's
``lastModified`` was 2025-03-07. SpicyDocs' discovery reader walks the collection
route at its largest page size, 17 keyed requests for the whole listing, checking
every page's count and each package id. Each listing row states the package id,
``title``, ``dateIssued`` and ``docClass``, which is everything a history row
starts with.
A separate one-time pass reads each package's MODS
(:mod:`spicy_regs.sources.gao_govinfo_mods`).

Scope: a package is a report when GovInfo classes it ``REPORT`` and its number is
not a B-file number. ``COMPTROLLERDECISION`` packages are bid-protest and
appropriations-law decisions, and gao.gov labels the few ``REPORT`` packages with
B-numbers as decisions too.

Identity follows the table's rule, the gao.gov product id lowercased. GovInfo
writes a report number's ``/`` as ``-``, so ``GAO/HEHS-00-73`` is
``GAOREPORTS-GAO-HEHS-00-73`` and ``RCED/AIMD-94-221FS`` is
``GAOREPORTS-RCED-AIMD-94-221FS``. gao.gov drops ``GAO/`` before a division code
and joins a joint report's division codes: ``hehs-00-73``, ``rcedaimd-94-221fs``,
``t-rcedaimd-95-131``, all checked on gao.gov on 2026-09-28. GovInfo lists some
products twice, as ``GAO-RCED-95-87R`` and ``RCED-95-87R``, or ``GAO-01-1171T``
and ``GAO-01-1171t``. Each such pair maps to one id, and the plainer spelling
wins. A few spellings stay GovInfo's own, such as a dotted number written with
hyphens or a missing hyphen, because nothing in the package id recovers them.

A ``T-`` number, or a number ending in ``T`` after its serial, is testimony, the
label gao.gov and MODS give it. Every other product is typed ``Report``.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

COLLECTION = "GAOREPORTS"
PACKAGE_PREFIX = "GAOREPORTS-"
#: Earlier than any GovInfo ``lastModified``, so the collection route lists every package.
LISTED_SINCE = "1990-01-01T00:00:00Z"
#: The discovery reader's largest page. The walk measured 17 pages on 2026-09-28.
PAGE_SIZE = 1000
MAX_PAGES = 40
#: Attempts per page, retries included; SpicyDocs resets this budget at each page.
REQUESTS_PER_PAGE = 5
SOURCE = "govinfo"
DETAILS_URL = "https://www.govinfo.gov/app/details/{}"
REPORT_CLASS = "REPORT"
DECISION_CLASS = "COMPTROLLERDECISION"

_JOINT = re.compile(r"([a-z]+(?:-[a-z]+)+)(-\d{2}-.+)")
_TESTIMONY = re.compile(r"t-.+|.+-\d+t")


class GaoGovInfoError(ValueError):
    """The GAOREPORTS listing states a package this mapping cannot place."""


class PackageDiscoverySource(Protocol):
    """The collection walk this module needs of SpicyDocs' GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


def _number(package_id: str) -> str:
    if not package_id.startswith(PACKAGE_PREFIX):
        raise GaoGovInfoError(f"GAOREPORTS listing returned a package outside its collection: {package_id}")
    return package_id.removeprefix(PACKAGE_PREFIX).lower()


def _redundant_gao(number: str) -> bool:
    """GovInfo's spelling of the ``GAO/`` citation prefix before a division code."""
    return number.startswith("gao-") and number[4:5].isalpha()


def report_id(package_id: str) -> str:
    """The gao.gov product id for a GAOREPORTS package id; see the module docstring for each rule.

    The result must read as a gao.gov product id under spicy-docs' own grammar
    (``gao_product_url``); a package id with no such reading refuses.
    """
    from spicy_docs.sources.gao.native import GaoProductSourceError, gao_product_url

    number = _number(package_id)
    if _redundant_gao(number):
        number = number[4:]
    prefix = "t-" if number.startswith("t-") else ""
    if joint := _JOINT.fullmatch(number.removeprefix(prefix)):
        number = prefix + joint[1].replace("-", "") + joint[2]
    try:
        gao_product_url(number)
    except GaoProductSourceError as error:
        raise GaoGovInfoError(f"GAOREPORTS package id has no product-id reading: {package_id}") from error
    return number


def report_type(product_id: str) -> str:
    return "Testimony" if _TESTIMONY.fullmatch(product_id) else "Report"


def is_report(package: dict) -> bool:
    """A ``REPORT`` package whose number is not a B-file number; refuses a class GovInfo has not used here."""
    doc_class = package.get("docClass")
    if doc_class not in (REPORT_CLASS, DECISION_CLASS):
        raise GaoGovInfoError(f"GAOREPORTS package {package.get('packageId')} has docClass {doc_class!r}")
    return doc_class == REPORT_CLASS and not _number(package["packageId"]).startswith("b-")


def _plainness(package_id: str) -> tuple[bool, bool, str]:
    """Sort key: no redundant ``GAO-``, then the uppercase spelling, then the id itself."""
    return _redundant_gao(_number(package_id)), package_id != package_id.upper(), package_id


def report_rows(packages: Iterable[dict]) -> tuple[list[dict], Counter[str]]:
    """Shape listed packages into table rows, one per product id, and count what the scope left out."""
    counts: Counter[str] = Counter()
    chosen: dict[str, dict] = {}
    for package in packages:
        counts["listed"] += 1
        if not is_report(package):
            counts["decisions_left_out"] += 1
            continue
        product_id = report_id(package["packageId"])
        held = chosen.get(product_id)
        if held is not None:
            counts["twins_collapsed"] += 1
            if _plainness(held["packageId"]) < _plainness(package["packageId"]):
                continue
        chosen[product_id] = package
    rows = [
        {
            "report_id": product_id,
            "title": package.get("title"),
            "report_type": report_type(product_id),
            "published_date": package.get("dateIssued"),
            "abstract": None,
            "agencies_json": None,
            "topics_json": None,
            "url": DETAILS_URL.format(package["packageId"]),
            "source": SOURCE,
            "product_type": None,
            "report_number": None,
        }
        for product_id, package in chosen.items()
    ]
    counts["rows"] = len(rows)
    return rows, counts


def package_id_of(row: dict) -> str:
    """The GovInfo package a history row was read from, stated by its ``url``."""
    url = row.get("url") or ""
    package_id = url.removeprefix(DETAILS_URL.format(""))
    if row.get("source") != SOURCE or package_id == url or not package_id.startswith(PACKAGE_PREFIX):
        raise GaoGovInfoError(f"{row.get('report_id')} is not a GovInfo history row")
    return package_id


def read_history(reader: PackageDiscoverySource) -> tuple[list[dict], Counter[str]]:
    """Walk the whole collection once and shape it; a refused page or count raises before any row is kept."""
    from spicy_docs.sources.govinfo.discovery import collection_url

    url = collection_url(COLLECTION, LISTED_SINCE, page_size=PAGE_SIZE)
    return report_rows(row for page in reader.packages(url, max_pages=MAX_PAGES) for row in page.records)


def discovery_reader(evidence: CaptureEvidence | None = None) -> Any:
    """SpicyDocs' keyed reader, retaining every page into ``evidence`` when a rollup gives one."""
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader

    from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
    from spicy_regs.sources.retained import RetainedGovInfoDiscoveryReader

    api_key = _resolve_api_key()
    if not api_key:
        raise RuntimeError(f"GAO history needs an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
    budget = PagedJsonBudget(
        max_requests=REQUESTS_PER_PAGE, max_page_bytes=8 * 1024 * 1024, timeout_seconds=60.0, min_request_interval_seconds=0.2
    )
    if evidence is not None:
        return RetainedGovInfoDiscoveryReader(budget=budget, api_key=api_key, evidence=evidence)
    return GovInfoDiscoveryReader(budget=budget, api_key=api_key)

