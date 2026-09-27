"""Usable RIN sets and the docket-side holders of a RIN; literal publisher observations stay in their source rows."""

from collections import defaultdict
from collections.abc import Iterable, Mapping

from spicy_regs.ontology.citations import action_evidence_rin, normalize_rin
from spicy_regs.ontology.common import JsonReadStats, parse_json_list


def docket_side_holder(rule_targets_source: object, *, feed_docket: bool = False) -> str | None:
    """How a docket holds a ``rule_targets`` row's RIN as its own (decision 56a), by holder name; ``None`` if not.

    A docket's own evidence holds a RIN: ``docket_rin`` (its rin), ``document_rin`` (its
    documents' RINs) and ``rule_targets:document_fr_doc`` (a copy's RINs, written on the
    copy's docket). ``fr_cfr_ref`` restates the RINs of a Register document the docket
    links, which its proceeding takes in and never holds (owner ruling on review 2b). A
    Federal Register feed docket (``catch_all_docket``) holds only its own rin: the
    documents it posts belong to other rulemakings. Proceedings attach by the RINs a single
    docketed proceeding holds this way (decision 56's E), and lifecycles read those same
    specific RINs.
    """
    source = str(rule_targets_source or "")
    if source == "fr_cfr_ref" or (feed_docket and source != "docket_rin"):
        return None
    return source if source in ("docket_rin", "document_rin") else f"rule_targets:{source}"


def specific_rin_holders(rins_by_holder: Mapping[str, Iterable[str]]) -> dict[str, str]:
    """Each specific RIN and the one holder that holds it (decision 56a).

    ``rins_by_holder`` maps each docketed proceeding to the RINs its docket-side evidence
    holds (:func:`docket_side_holder`). A RIN is specific when exactly one of them holds it;
    one two or more hold is nobody's, and an X-pattern code never is (decision 61b).
    Proceedings attach a Register document by these RINs; lifecycles read them as a
    proceeding's specific RINs.
    """
    holders: dict[str, set[str]] = defaultdict(set)
    for holder, rins in rins_by_holder.items():
        for rin in rins:
            if action_evidence_rin(rin):
                holders[rin].add(holder)
    return {rin: next(iter(keys)) for rin, keys in holders.items() if len(keys) == 1}


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
