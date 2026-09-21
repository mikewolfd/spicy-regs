"""Usable RIN sets; literal publisher observations stay in their source rows."""

from spicy_regs.ontology.citations import normalize_rin
from spicy_regs.ontology.common import JsonReadStats, parse_json_list


def proceeding_rins(row: dict, stats: JsonReadStats) -> set[str]:
    """Read the complete set, or the one known scalar from a legacy artifact.

    A malformed non-NULL set is reported by the common JSON reader and never
    silently replaced by the convenience scalar. A legacy scalar cannot recover
    multiple observations discarded by an older build; regeneration is required.
    """
    if row.get("rins_json") is None:
        rin = normalize_rin(row.get("rin"))
        return {rin} if rin else set()
    values = parse_json_list(
        row["rins_json"],
        stats=stats,
        table="proceedings",
        row_id=row.get("proceeding_id"),
        column="rins_json",
    )
    return set() if values is None else {rin for value in values if (rin := normalize_rin(value)) is not None}
