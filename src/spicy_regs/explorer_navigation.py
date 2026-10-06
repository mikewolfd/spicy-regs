"""Portable navigation over explicitly named source fields, never inferred column names.

The registry ships with table_joins.json. Python validation/measurement and the
browser consume the same key recipes. Installation and metadata rendering do
not read source rows. Existing source-occurrence SQL views remain available.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy


def part(path: str, *, row: bool = False, transform: str | None = None) -> dict:
    value = {"path": path.split(".") if path else [], "from": "row" if row else "element"}
    if transform:
        value["transform"] = transform
    return value


def key(*parts: dict, separator: str = "", pattern: str = ".+") -> dict:
    return {"parts": list(parts), "separator": separator, "pattern": pattern}


def guard(path: str, *, row: bool = False, values: tuple[str, ...] = (), pattern: str | None = None) -> dict:
    return {**part(path, row=row), **({"values": list(values)} if values else {}),
            **({"pattern": pattern} if pattern else {})}


def route(table: str, columns: tuple[str, ...], keys: tuple[dict, ...], *guards: dict) -> dict:
    return {"table": table, "columns": list(columns), "keys": list(keys), "guards": list(guards)}


def array(identifier: str, source: str, fields: tuple[str, ...], targets: tuple[dict, ...], *,
          meaning: str, receipt_fields: tuple[str, ...] = (), element_path: tuple[str, ...] = (),
          mode: str = "array", candidates: bool = False) -> dict:
    return {"id": identifier, "source": source, "fields": list(fields), "targets": list(targets),
            "mode": mode, "candidates": candidates, "meaning": meaning, "receiptFields": list(receipt_fields),
            "elementPath": list(element_path), "ruleVersion": "source-navigation/1"}


BILL_TYPES = ("hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres")
NUM = r"[1-9][0-9]*"
CONGRESS = key(part("congress"), pattern=NUM)
BILL_ID = key(part(""), pattern=r"[0-9]+-(hr|s|hres|sres|hjres|sjres|hconres|sconres)-[0-9]+")
NOMINATION = route("nominations", ("congress", "citation"), (
    CONGRESS, key(part("number", transform="nomination-citation"), part("part", transform="partition"))),
    guard("number", pattern=NUM), guard("congress", pattern=NUM), guard("part", pattern=r"00|[1-9][0-9]*"))
TREATY = route("treaties", ("congress_received", "number", "suffix"), (
    CONGRESS, key(part("number"), pattern=NUM), key(part("part", transform="partition-value"), pattern=r"[0-9]*")),
    guard("number", pattern=NUM), guard("congress", pattern=NUM), guard("part", pattern=r"00|[1-9][0-9]*"))



def declarations(processing_joins: tuple = ()) -> list[dict]:
    """Explicit source relationships plus formerly receipt-only keys with selected receipt fallbacks."""
    specs = [
        array("meeting_nominations", "committee_meetings", ("nomination_references_json",), (NOMINATION,),
              meaning="Nominations explicitly listed for this meeting; nomination partitions stay distinct."),
        array("meeting_treaties", "committee_meetings", ("treaty_references_json",), (TREATY,),
              meaning="Treaties explicitly listed for this meeting, with their own Congress and partition."),
        array("meeting_bills", "committee_meetings", ("bill_ids", "bill_ids_json"),
              (route("congress_bills", ("bill_id",), (BILL_ID,)),), meaning="Bills explicitly listed for this meeting."),
        array("meeting_committees", "committee_meetings", ("committees", "committees_json"),
              (route("committees", ("system_code",), (key(part("systemCode")),)),),
              meaning="Every publisher-listed committee; independent of meeting bills and witnesses."),
        array("nomination_committees", "nominations", ("committees_json",),
              (route("committees", ("system_code",), (key(part("systemCode")),)),),
              meaning="Committees whose nomination relationship list was read completely."),
        array("nomination_hearings", "nominations", ("hearings_json",),
              (route("hearing_transcripts", ("congress", "chamber", "jacket_number"), (
                  key(part("citation", transform="hearing-congress"), pattern=NUM), key(part("chamber", transform="lower")),
                  key(part("jacketNumber"), pattern=NUM))),),
              meaning="Printed hearings the nomination lists; no meeting or witness pairing is inferred."),
        array("fcc_filing_proceedings", "fcc_filings", ("proceedings",),
              (route("fcc_proceedings", ("name", "id_proceeding"), (key(part("name")), key(part("id_proceeding")))),),
              meaning="Match both the FCC proceeding name and native numeric ID; missing IDs remain unsupported."),
        array("fcc_filing_documents", "fcc_filings", ("documents",),
              (route("@url", ("url",), (key(part("src"), pattern=r"https://[^\s]+"),)),),
              meaning="Document URLs offered by the FCC; offering a URL does not establish capture or extraction.",
              receipt_fields=("native_fields_json",), element_path=("documents",)),
        array("house_communication_record", "house_communications", (),
              (route("record_issues", ("package_id",), (key(part("record_package_id", row=True)),)),),
              meaning="The exact Congressional Record package that supplied this communication.",
              receipt_fields=("record_package_id", "record_granule_id", "record_entry_text"), mode="row"),
        array("member_fec_ids", "members", ("fec_ids", "fec_ids_json"),
              (route("fec_candidate_history", ("candidate_id",), (key(part(""), pattern=r"[HSP][0-9][A-Z0-9]{7}"),)),),
              meaning="Source-asserted candidate IDs; returns retained cycle observations, not an adjudicated person identity."),
    ]
    specs += [
        array("bill_related_bills", "congress_bills", ("related_bills", "related_bills_json"),
              (route("congress_bills", ("bill_id",),
                     (key(part("congress"), part("bill_type", transform="lower"), part("number"), separator="-", pattern=r"[0-9]+-(hr|s|hres|sres|hjres|sjres|hconres|sconres)-[0-9]+"),)),),
              meaning="Publisher-listed related bills. No reciprocal relationship or identical text is inferred."),
        array("meeting_hearing_jackets", "committee_meetings", ("hearing_jackets", "hearing_jackets_json"),
              (route("hearing_transcripts", ("congress", "chamber", "jacket_number"),
                     (key(part("congress", row=True), pattern=NUM), key(part("chamber", row=True)), key(part(""), pattern=NUM))),),
              meaning="Meeting-listed hearing jackets within this meeting's own Congress and chamber."),
        array("meeting_documents", "committee_meetings", ("document_urls", "document_urls_json"),
              (route("@url", ("url",), (key(part(""), pattern=r"https://[^\s]+"),)),),
              meaning="Documents offered for this meeting; no capture, extraction or witness pairing is implied."),
        array("meeting_witness_documents", "committee_meetings", ("witness_documents", "witness_documents_json"),
              (route("@url", ("url",), (key(part("url"), pattern=r"https://[^\s]+"),)),),
              meaning="Publisher-listed witness documents in their source order; a URL does not prove capture."),
    ]
    vote_bill = route("congress_bills", ("bill_id",), (
        key(part("congress"), part("type", transform="bill-type"), part("number"), separator="-"),),
        guard("type", values=("HR", "S", "HRES", "SRES", "HJRES", "SJRES", "HCONRES", "SCONRES", "H.R.", "S.", "H.Res.", "S.Res.", "H.J.Res.", "S.J.Res.", "H.Con.Res.", "S.Con.Res.")),
        guard("congress", pattern=NUM), guard("number", pattern=NUM))
    vote_nomination = route("nominations", ("congress", "citation"), (
        CONGRESS, key(part("number", transform="nomination-citation"), pattern=r"PN[1-9][0-9]*(?:-[1-9][0-9]*)?")),
        guard("type", values=("PN",)))
    vote_treaty = route("treaties", ("treaty_id",), (key(part("number"), pattern=r"[1-9][0-9]*-[1-9][0-9]*"),),
                        guard("type", values=("Treaty Doc.",)))
    specs += [
        array("vote_documents", "roll_call_votes", ("documents", "documents_json"),
              (vote_bill, vote_nomination, vote_treaty), meaning="Each independent source document reference; its own Congress and nomination partition are retained."),
        array("vote_amendments", "roll_call_votes", ("amendments", "amendments_json"),
              (route("amendments", ("amendment_id",), (key(part("congress", row=True), part("number", transform="senate-amendment"), separator="-"),),
                     guard("chamber", row=True, values=("senate",)), guard("congress", row=True, pattern=NUM),
                     {**part("vote_id", row=True, transform="vote-congress"), "sameAs": part("congress", row=True)}, guard("number", pattern=r"S\.Amdt\. [1-9][0-9]*")),),
              meaning="Source-stated Senate amendment numbers; no positional pairing with vote document blocks."),
    ]
    # Resolver-owned routes qualify candidate identities, including ranges and ambiguous targets.
    # Navigate each recorded candidate key rather than reconstructing its original citation.
    from spicy_regs.citation_resolution import ROUTES
    legal_targets = [route(r.table, r.identity, tuple(key(part(c)) for c in r.identity),
                           guard("target_table_selected", values=(r.table,)),
                           guard("target_status", values=("found", "ambiguous")))
                     for r in dict.fromkeys(ROUTES.values())]
    specs.append(array("native_legal_targets", "native_legal_references", ("target_candidates", "target_candidates_json"), tuple(legal_targets),
                       receipt_fields=("target_candidates_json",),
                       candidates=True,
                       meaning="Every recorded candidate identity, including ambiguous matches. Missing, unsupported and unchecked targets remain explicit."))
    specs.append(array("native_legal_read", "native_legal_references", (),
                       (route("@receipt:native_legal_reference_reads", ("scope_id",), (key(part("scope_id", row=True)),)),),
                       meaning="The complete-read scope for this observed legal reference.", mode="row"))
    for source, field, target, column in (("fec_committee_observations", "candidate_ids_json", "fec_candidate_history", "candidate_id"),
                                           ("fec_committee_observations", "sponsor_candidate_ids_json", "fec_candidate_history", "candidate_id")):
        specs.append(array(source + "_" + field, source, (field, field.removesuffix("_json")),
                           (route(target, (column,), (key(part(""), pattern=r"[HSP][0-9A-Z]{8}"),)),),
                           meaning="Source-reported candidate relationship; all retained cycle observations remain available."))
    from spicy_regs.subject_catalog import descriptors
    policies = descriptors()
    for join in processing_joins:
        if not join.child.startswith("fec_"):
            continue
        # These are existing canonical declarations, not inferred matching names.
        guards = ()
        if join.child == "fec_record_evidence" and "target_record_id" in join.child_columns:
            guards = (guard("target_table", row=True, values=(join.parent,)),)
        specs.append(array("receipt_" + join.child + "_" + "_".join(join.child_columns) + "_" + join.parent,
                           join.child, (), (route("@receipt:" + join.parent if policies.get(join.parent, {}).get("receipt_only") else join.parent, join.parent_columns,
                                                tuple(key(part(c, row=True)) for c in join.child_columns), *guards),),
                           meaning=join.reason or "Retained source-record relationship; no current-record or financial-total qualification.",
                           receipt_fields=join.child_columns, mode="row"))
    # These canonical collection relationships enumerate the typed observations.
    # A stored native filing_key is already the retained mapper's decision; browser
    # navigation keeps every metadata observation and makes no latest/current selection.
    filing_sources = {j.child for j in processing_joins if j.parent == "fec_collections"}
    for source in sorted(filing_sources):
        if source == "fec_filings":
            continue
        specs.append(array("native_filing_" + source, source, (),
                           (route("fec_filings", ("filing_key",),
                                  (key(part("filing_key", row=True), pattern=r"urn:fec:filing:official-fec:openfec-file-number:-?[1-9][0-9]*"),)),),
                           meaning="All retained filing metadata with this mapper-qualified native key. This does not select a current amendment or combine financial amounts.", mode="row"))
    return validate_navigation(specs)


def scalar(value):
    return str(value) if isinstance(value, (str, int)) and not isinstance(value, bool) else None


def at(value, path):
    for name in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(name)
    return value


def word(recipe, element, row):
    value = at(row if recipe.get("from") == "row" else element, recipe["path"])
    transform = recipe.get("transform")
    if transform == "partition":
        if value in (None, "", "00", 0):
            return ""
        value = scalar(value)
        return "-" + value if value and re.fullmatch(NUM, value) else None
    if transform == "partition-value":
        return "" if value in (None, "", "00", 0) else scalar(value)
    value = scalar(value)
    if value is None:
        return None
    if transform == "vote-congress":
        match = re.fullmatch(r"([1-9][0-9]*)-senate-[12]-[1-9][0-9]*", value)
        return match[1] if match else None
    if transform == "hearing-congress":
        match = re.fullmatch(r"[SH]\.Hrg\. ([1-9][0-9]*)-[0-9]+", value)
        return match[1] if match else None
    if transform == "lower":
        return value.lower()
    if transform == "bill-type":
        return value.lower().replace(".", "")
    if transform == "nomination-citation":
        return "PN" + value
    if transform == "senate-amendment":
        return "samdt-" + value.removeprefix("S.Amdt. ")
    return value


def target_keys(target, element, row):
    for check in target["guards"]:
        value = word(check, element, row)
        if value is None or "sameAs" in check and value != word(check["sameAs"], element, row) or "values" in check and value not in check["values"] or "pattern" in check and not re.fullmatch(check["pattern"], value):
            return None
    values = []
    for recipe in target["keys"]:
        parts = [word(p, element, row) for p in recipe["parts"]]
        if None in parts:
            return None
        value = recipe["separator"].join(parts)
        if not re.fullmatch(recipe["pattern"], value):
            return None
        values.append(value)
    return values


def published_navigation(specs, schemas):
    """Resolve field aliases against this publication, retaining missing-target explanations."""
    result = []
    for original in specs:
        if original["source"] not in schemas:
            continue
        spec = deepcopy(original)
        fields = {name for name, _ in schemas[spec["source"]]}
        spec["field"] = next((name for name in spec["fields"] if name in fields), None)
        row_fields = {p["path"][0] for t in spec["targets"] for p in
                      [*t["guards"], *(p for k in t["keys"] for p in k["parts"])]
                      if p["from"] == "row" and p["path"]}
        spec["available"] = ((spec["mode"] == "row" and row_fields <= fields)
                             or spec["field"] is not None or bool(spec["receiptFields"]))
        if not spec["available"] and not spec["receiptFields"]:
            spec["unavailableReason"] = "These source references have not been published yet."
        for target in spec["targets"]:
            if target["table"].startswith("@receipt:"):
                dataset = target["table"].removeprefix("@receipt:")
                if set(target["columns"]) <= {c[0] for c in schemas.get(dataset, [])}:
                    # Older publications can still expose the exact declared evidence table.
                    target["table"] = dataset
            target["available"] = target["table"].startswith("@") or set(target["columns"]) <= {c[0] for c in schemas.get(target["table"], [])}
            if not target["available"]:
                target["unavailableReason"] = "The target table or its complete key is not published."
        result.append(spec)
    return result


def validate_navigation(specs):
    """Reject malformed keys at generation time, before clients receive them."""
    ids = set()
    for spec in specs:
        if spec["id"] in ids or spec["mode"] not in {"row", "array"}:
            raise ValueError("Invalid or duplicate navigation declaration")
        ids.add(spec["id"])
        for target in spec["targets"]:
            if not target["columns"] or len(target["columns"]) != len(target["keys"]):
                raise ValueError("Navigation requires every component of the target key")
            for recipe in [*target["guards"], *(g["sameAs"] for g in target["guards"] if "sameAs" in g), *(p for k in target["keys"] for p in k["parts"])]:
                if recipe["from"] not in {"row", "element"} or not all(isinstance(p, str) for p in recipe["path"]):
                    raise ValueError("Invalid navigation field path")
                if recipe.get("transform") not in {None, "lower", "bill-type", "nomination-citation", "partition", "partition-value", "senate-amendment", "hearing-congress", "vote-congress"}:
                    raise ValueError("Unknown navigation transform")
            for check in [*target["guards"], *target["keys"]]:
                if "pattern" in check:
                    re.compile(check["pattern"])
    return specs


def array_sql_relationships():
    """Compile new Congress list relationships from the same recipes shipped to browsers."""
    from spicy_regs.relationship_views.core import ArrayRelationship
    # Keep base MCP imports free of the ETL-only Arrow dependency.
    identities = {"committee_meetings": ("congress", "chamber", "event_id"), "nominations": ("congress", "citation")}

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    def expression(p):
        if p["from"] == "row":
            if len(p["path"]) != 1:
                raise ValueError("SQL row navigation requires a scalar field")
            result = 's."' + p["path"][0] + '"'
        else:
            result = "json_extract_string(e.value, " + literal("$." + ".".join(p["path"])) + ")"
        transform = p.get("transform")
        if transform == "nomination-citation":
            return "'PN' || " + result
        if transform in {"partition", "partition-value"}:
            empty = "''"
            prefix = "'-' || " if transform == "partition" else ""
            return f"CASE WHEN {result} IS NULL OR {result} IN ('','00','0') THEN {empty} ELSE {prefix}{result} END"
        if transform == "lower":
            return f"lower({result})"
        if transform == "hearing-congress":
            return f"regexp_extract({result}, '[SH]\\.Hrg\\. ([1-9][0-9]*)-[0-9]+', 1)"
        if transform:
            raise ValueError("Unsupported SQL navigation transform")
        return result

    specs = []
    for spec in declarations():
        if spec["id"] not in {"meeting_nominations", "meeting_treaties", "nomination_committees", "nomination_hearings"}:
            continue
        target = spec["targets"][0]
        values, checks = [], ["e.type = 'OBJECT'"]
        for check in target["guards"]:
            expr = expression(check)
            if "pattern" in check:
                checks.append(f"regexp_full_match({expr}, {literal(check['pattern'])})")
            if "values" in check:
                checks.append(expr + " IN (" + ",".join(map(literal, check["values"])) + ")")
        for recipe in target["keys"]:
            expr = (" || " + literal(recipe["separator"]) + " || ").join(expression(p) for p in recipe["parts"])
            checks.append(f"regexp_full_match({expr}, {literal(recipe['pattern'])})")
            values.append(expr)
        target_expression = " || ':' || ".join(values)
        specs.append(ArrayRelationship(spec["id"], spec["source"], identities[spec["source"]], spec["fields"][0],
                                       target["table"], target_expression, " AND ".join(checks), spec["meaning"],
                                       rule_version=spec["ruleVersion"]))
    return tuple(specs)
