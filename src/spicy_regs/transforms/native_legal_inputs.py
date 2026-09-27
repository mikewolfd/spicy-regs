"""Bind complete native legal inputs to owner-validated source identities.

Historical capture receipts are retained observations, never new HTTP captures.
Request dates and native release points have deliberately different labels.
"""

from dataclasses import asdict
import hashlib
import json
from typing import Callable

from spicy_docs.sources.cfr import EcfrSelection, ecfr_xml_locator, validate_ecfr_xml
from spicy_docs.sources.uscode import ReleasePoint, TitleSelection, title_xml_locator
from spicy_docs.sources.uscode.archive import read_title_archive


def qualify_source(spec: dict, body: bytes, read: Callable[[dict], bytes], retain: Callable) -> dict | None:
    """Refuse mismatched identity, archive membership or retained capture receipt."""
    selection = spec.get("qualification")
    if selection is None:
        return None
    kind = selection["kind"]
    if kind == "ecfr-retained-title":
        request = EcfrSelection(title=selection["title"], date=selection["requested_date"])
        locator = ecfr_xml_locator(request)
        receipt_body = read(selection["receipt"])
        receipt = json.loads(receipt_body)
        entries = [row for row in receipt["titles"] if row.get("title") == request.title]
        if len(entries) != 1:
            raise ValueError("eCFR acquisition receipt must contain exactly one selected title")
        row = entries[0]
        expected = {
            "url": locator,
            "date": request.date,
            "http_status": 200,
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "ok": True,
        }
        if any(row.get(key) != value for key, value in expected.items()):
            raise ValueError("eCFR acquisition receipt disagrees with retained bytes or selection")
        if spec["source_locator"] != locator or spec["source_record_key"] != f"ecfr/title/{request.title}":
            raise ValueError("eCFR source identity disagrees with qualified selection")
        if spec["edition"] != "requested-as-of:" + request.date or spec["source_family"] != "ecfr":
            raise ValueError("eCFR date must remain explicitly request-based")
        metadata = validate_ecfr_xml(body, selection=request, final_url=locator)
        retain(receipt_body, role="historical-acquisition-receipt")
        facts = {
            "kind": kind,
            "metadata": asdict(metadata),
            "capture_receipt": row,
            "date_basis": "historical canonical request URL; not a printed edition or new acquisition",
        }
    elif kind == "uscode-title-archive":
        request = TitleSelection(ReleasePoint.from_label(selection["release_point"]), selection["title"])
        archive = read(selection["archive"])
        result = read_title_archive(archive, selection=request)
        if result.xml_bytes != body:
            raise ValueError("U.S. Code XML differs from validated complete archive member")
        if spec["source_locator"] != title_xml_locator(request) or spec["source_record_key"] != request.identifier:
            raise ValueError("U.S. Code source identity disagrees with validated archive")
        if spec["edition"] != result.release_point or spec["source_family"] != "uscode":
            raise ValueError("U.S. Code edition differs from native release point")
        retain(archive, role="complete-publisher-title-archive")
        facts = {
            "kind": kind,
            "member": asdict(result.entry),
            "release_point": result.release_point,
            "edition_basis": "native docPublicationName and exact validated ZIP membership",
        }
    else:
        raise ValueError("unsupported native input qualification")
    for receipt in selection.get("supporting_evidence", []):
        retain(read(receipt), role="historical-supporting-evidence")
    return facts
