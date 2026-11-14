"""An independently dated Congress.gov observation of a hearing and its meeting."""

from collections.abc import Mapping, Sequence
from typing import Any

from spicy_docs.schemas.committee_report_tables import CHAMBER_BY_DOCUMENT_TYPE
from spicy_docs.sources.congress.listing import LIST_ROUTES, hearing_detail_event_id, list_route_url
from spicy_docs.sources.govinfo.bodies import PackageIdentity
from spicy_docs.transport.captured import CapturedBodyResponse


def hearing_detail_urls(identity: PackageIdentity) -> tuple[str, ...]:
    chamber = CHAMBER_BY_DOCUMENT_TYPE.get(str(identity.document_type))
    if not identity.package_id.startswith("CHRG-") or chamber is None or identity.number is None:
        raise ValueError("Congress hearing detail requires a hearing's chamber and jacket")
    # Congress.gov files some joint hearings under senate or house (CHRG-119jhrg61392).
    chambers = ("joint", "senate", "house") if identity.document_type == "jhrg" else (chamber,)
    return tuple(list_route_url(
        LIST_ROUTES["hearing-detail"], congress=identity.congress,
        chamber=each, number=int(identity.number), limit=1,
    ) for each in chambers)


def hearing_detail_facts(
    identity: PackageIdentity, records: Sequence[Mapping[str, Any]], capture: CapturedBodyResponse
) -> dict:
    """Keep native meeting field states, including null and missing, with the exact capture."""
    expected = hearing_detail_urls(identity)
    if (
        capture.requested_url not in expected or capture.resolved_url != capture.requested_url
        or capture.method != "GET" or capture.status_code != 200 or capture.content_encoding != "identity"
    ):
        raise ValueError("Congress hearing detail capture differs from the selected route")
    event_id = hearing_detail_event_id(records, congress=identity.congress, jacket=identity.number)
    record = records[0]
    chamber = capture.requested_url.split("/hearing/", 1)[1].split("/")[1]
    if "chamber" in record and str(record["chamber"]).lower() != chamber:
        raise ValueError("hearing detail chamber differs from the GovInfo package")
    meeting = record.get("associatedMeeting")
    if "associatedMeeting" not in record:
        state = "missing"
    elif meeting is None:
        state = "null"
    elif event_id is not None:
        state = "meeting"
    elif not meeting:
        state = "empty"
    else:
        state = "null-event-id" if "eventId" in meeting else "missing-event-id"
    return {
        "package_id": identity.package_id,
        "state": state, "event_id": event_id,
        "associated_meeting": dict(meeting) if isinstance(meeting, Mapping) else meeting,
        "locator": "/hearing/associatedMeeting",
        "sha256": capture.sha256, "source_url": capture.resolved_url, "observed_at": capture.observed_at,
    }
