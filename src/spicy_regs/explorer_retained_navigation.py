"""Explicit main-row relationships in retained publisher collections.

The canonical explorer registry consumes these declarations. Content-bound
observation IDs keep source versions separate.
"""

from spicy_regs.explorer_navigation import array, key, part, route


DIGEST = r"sha256:[a-f0-9]{64}"


def _row(identifier, source, target, components, meaning):
    """Declare every source/target key component; never infer a column match."""
    return array(
        identifier, source, (),
        (route(target, tuple(column for column, _, _ in components),
               tuple(key(part(field, row=True), pattern=pattern) for _, field, pattern in components)),),
        mode="row", meaning=meaning,
    )


def declarations():
    """Portable main-key routes, with no receipt-derived navigation inputs."""
    specs = [
        _row("administration_policy_statement", "administration_policy_bill_links",
             "administration_policy_statements", (("statement_id", "statement_id", DIGEST),),
             "The exact retained statement that lists this bill, including repeated bill mentions."),
        _row("administration_policy_bill", "administration_policy_bill_links", "congress_bills",
             (("bill_id", "bill_id", r"[1-9][0-9]*-(?:hr|s|hres|sres|hjres|sjres|hconres|sconres)-[1-9][0-9]*"),),
             "The bill explicitly listed in the archived metadata; no support or opposition is inferred."),
        _row("ecfr_content_snapshot", "ecfr_snapshot_sections", "ecfr_snapshots",
             (("snapshot_id", "snapshot_id", DIGEST),),
             "The exact dated eCFR selection containing this source node."),
    ]
    for source, identifier in (("uscode_edition_sections", "uscode_content_edition_member"),
                               ("uscode_edition_structure", "uscode_structure_edition_member")):
        specs.append(_row(identifier, source, "uscode_editions", (("edition_id", "edition_id", DIGEST),),
                          "The exact retained archive member and edition observation, including carried appendices."))
    return specs
