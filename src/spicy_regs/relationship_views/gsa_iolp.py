"""Native location links within one GSA IOLP snapshot; unmatched leases remain visible."""

from .sql_views import SQLView


def property_leases(_pins):
    return """SELECT l.snapshot_id, l.snapshot_date, l.lease_number, l.location_code,
        l.asset_name, l.street_address, l.city, l.state, l.zip_code,
        l.lease_effective_date, l.lease_expiration_date, l.location_status,
        p.ownership AS property_ownership, p.location_status AS property_location_status,
        p.source_locator_json AS property_source_locator_json,
        l.source_locator_json AS lease_source_locator_json,
        p.location_code IS NOT NULL AS property_matched,
        'Native Location Code link in one declared GSA snapshot; dates do not prove current occupancy or active lease.'
            AS interpretation_limit
        FROM gsa_iolp_leases l LEFT JOIN gsa_iolp_properties p
          ON l.snapshot_id = p.snapshot_id AND l.location_code = p.location_code"""


GSA_IOLP_VIEWS = (
    SQLView(
        "gsa_iolp_property_leases",
        {
            "gsa_iolp_leases": (
                "snapshot_id",
                "snapshot_date",
                "lease_number",
                "location_code",
                "asset_name",
                "street_address",
                "city",
                "state",
                "zip_code",
                "lease_effective_date",
                "lease_expiration_date",
                "location_status",
                "source_locator_json",
            ),
            "gsa_iolp_properties": (
                "snapshot_id",
                "location_code",
                "ownership",
                "location_status",
                "source_locator_json",
            ),
        },
        property_leases,
        "Each GSA IOLP lease with its same-snapshot property by native Location Code. Unmatched leases remain visible.",
        ("snapshot_id", "lease_number"),
        rule_version="gsa-iolp-native-location/1",
        column_descriptions={
            "property_matched": "Whether this lease's native location exists in the same selected property snapshot.",
            "property_ownership": "Owned/leased label of the same-snapshot property, null for unmatched leases.",
            "property_location_status": "Location quality state of the matched property; source conflicts remain explicit.",
            "property_source_locator_json": "Exact original property workbook digest, URL, sheet and row.",
            "lease_source_locator_json": "Exact original lease workbook digest, URL, sheet and row.",
            "interpretation_limit": "Limits on occupancy and current lease-status interpretation.",
        },
    ),
)
