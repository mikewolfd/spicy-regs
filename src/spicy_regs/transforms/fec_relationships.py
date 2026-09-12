"""Source-reported FEC relationship observations; no name matching or publication.

An empty value is an observation, not an edge. Names never become identifiers.
Cycle selects bulk observations; current API records retain cycle=None. These
rows do not replace the heuristic org_committee_links table. Callers verify
retained inputs once per source and supply exact row/field or byte references.
"""

import json
import re
from pathlib import Path

import pyarrow as pa

from spicy_regs.transforms.parquet_rows import write_rows

COLUMNS = (
    "subject_id",
    "subject_type",
    "subject_id_status",
    "relationship_type",
    "object_id",
    "object_type",
    "object_id_status",
    "object_name",
    "value_status",
    "source_family",
    "cycle",
    "candidate_election_year",
    "source_sha256",
    "source_url",
    "observed_at",
    "source_locator_json",
    "source_fields_json",
)
SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])
_ID = {"candidate": r"[HPS][A-Za-z0-9]{8}", "committee": r"C[0-9]{8}"}


def _id_status(value, kind):
    if value in (None, ""):
        return "not_reported"
    return "source_id_shape" if kind in _ID and re.fullmatch(_ID[kind], value) else "invalid_source_id_shape"


def _observation(
    row,
    evidence,
    family,
    cycle,
    subject,
    subject_type,
    relation,
    object_type,
    *,
    object_id=None,
    name=None,
    fields=(),
    value_status=None,
    election_year=None,
):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", evidence["sha256"]) or not evidence["locator"]:
        raise ValueError("relationship requires a source digest and locator")
    if value_status is None:
        value_status = (
            "reported" if object_id or name else "null" if object_id is None and name is None else "empty_string"
        )
        if not object_id and isinstance(name, str) and name.strip().upper() == "NONE":
            value_status = "reported_none"
    return dict(
        zip(
            COLUMNS,
            (
                subject,
                subject_type,
                _id_status(subject, subject_type),
                relation,
                object_id,
                object_type,
                _id_status(object_id, object_type),
                name,
                value_status,
                family,
                str(cycle) if cycle is not None else None,
                str(election_year) if election_year is not None else None,
                evidence["sha256"],
                evidence["url"],
                evidence["observed_at"],
                json.dumps({**evidence["locator"], "fields": list(fields)}, sort_keys=True),
                json.dumps({k: row[k] for k in fields if k in row}, sort_keys=True),
            ),
            strict=True,
        )
    )


def bulk_relationships(family: str, row: dict, *, evidence: dict, cycle: int):
    """Map every selected bulk row, preserving blanks, source codes and dates."""

    def emit(*args, **kwargs):
        return _observation(row, evidence, family, cycle, *args, **kwargs)

    if family == "linkage":
        yield emit(
            row["CAND_ID"],
            "candidate",
            {"P": "principal_campaign_committee", "A": "authorized_committee"}.get(
                row["CMTE_DSGN"], "candidate_committee_link"
            ),
            "committee",
            object_id=row["CMTE_ID"],
            fields=tuple(row),
            election_year=row["CAND_ELECTION_YR"],
        )
    elif family == "candidate_master":
        yield emit(
            row["CAND_ID"],
            "candidate",
            "principal_campaign_committee",
            "committee",
            object_id=row["CAND_PCC"],
            fields=("CAND_ID", "CAND_PCC", "CAND_ELECTION_YR"),
            election_year=row["CAND_ELECTION_YR"],
        )
    elif family == "committee_master":
        for relation, kind, field, is_name in (
            ("connected_organization", "organization", "CONNECTED_ORG_NM", True),
            ("committee_candidate", "candidate", "CAND_ID", False),
        ):
            yield emit(
                row["CMTE_ID"],
                "committee",
                relation,
                kind,
                **{"name" if is_name else "object_id": row[field]},
                fields=("CMTE_ID", field, "CMTE_DSGN", "CMTE_TP", "ORG_TP"),
            )
    elif family == "form1_bulk":
        yield emit(
            row["COMMITTEE_ID"],
            "committee",
            "affiliated_committee_or_connected_organization",
            "unresolved",
            name=row["AFFILIATED_COMMITTEE_NAME"],
            fields=(
                "COMMITTEE_ID",
                "AFFILIATED_COMMITTEE_NAME",
                "RECEIPT_DATE",
                "BEGIN_IMAGE_NUMBER",
                "FILED_COMMITTEE_TYPE",
                "FILED_COMMITTEE_DESIGNATION",
                "ORGANIZATION_TYPE",
            ),
        )
    elif family == "leadership":
        yield emit(
            row["Committee_Id"],
            "committee",
            "leadership_pac_sponsor",
            "candidate",
            name=row["Sponsor_Name"],
            fields=("Committee_Id", "Sponsor_Name", "Link_Image", "Coverage_End_Date"),
        )
    else:
        raise ValueError("unsupported FEC bulk relationship family")


def api_relationships(row: dict, *, evidence: dict):
    """Preserve current source assertions and exact array elements, not an ontology.

    Type columns name source identifier/field roles. Source codes and labels qualify
    them; a C-prefixed filer need not be a committee. The pinned parent record keeps
    complete arrays, including cycles: current fields do not establish history or
    authorization. An array locator selects the literal element stored inline.
    """
    common = (
        "committee_id",
        "last_f1_date",
        "last_file_date",
        "designation",
        "committee_type",
        "committee_type_full",
        "organization_type",
        "organization_type_full",
    )
    context = {field: row[field] for field in common if field in row}
    for field, relation, kind in (
        ("affiliated_committee_name", "affiliated_committee_or_connected_organization", "unresolved"),
        ("candidate_ids", "committee_candidate", "candidate"),
        ("sponsor_candidate_ids", "leadership_pac_sponsor", "candidate"),
        ("sponsor_candidate_list", "leadership_pac_sponsor", "candidate"),
    ):
        value = row.get(field)
        is_array = field != "affiliated_committee_name"
        if is_array and value is not None and not isinstance(value, list):
            raise ValueError("expected source relationship array")
        status = "missing_field" if field not in row else "null" if value is None else None
        if is_array and value == []:
            status = "empty_list"
        populated = is_array and bool(value)
        for index, item in enumerate(value if populated else [None if is_array else value]):
            if field == "sponsor_candidate_list":
                if item is not None and not isinstance(item, dict):
                    raise ValueError("expected source sponsor object")
                target = {
                    "object_id": (item or {}).get("sponsor_candidate_id"),
                    "name": (item or {}).get("sponsor_candidate_name"),
                }
            else:
                target = {"object_id" if is_array else "name": item}
            if any(v is not None and not isinstance(v, str) for v in target.values()):
                raise ValueError("expected source relationship text")
            locator = {**evidence["locator"], "array_index": index if populated else None}
            if is_array:
                locator["array_field"] = field
            source = {**context, **({field: item if populated else value} if field in row else {})}
            yield _observation(
                source,
                {**evidence, "locator": locator},
                "committee_api_current",
                None,
                row["committee_id"],
                "committee",
                relation,
                kind,
                **target,
                fields=(*common, field),
                value_status=status,
            )


def statement_relationships(record: dict, *, version: str, evidence: dict):
    """Read Forms 1/1S/2/2S positions from official 8.3 and 8.4 workbooks.

    F1 line 6 moved two positions in 8.4. Its ORG/AFF/JFR/LPS codes give
    distinct reported roles. Extra and unsupported fields stay in the original;
    unsupported versions refuse instead of borrowing the latest layout.
    """
    if version not in ("8.3", "8.4"):
        raise ValueError("unsupported FEC statement version")
    row = record["fields"]
    form = row["0"]
    if form not in ("F1N", "F1A", "F1S", "F2N", "F2A", "F2S"):
        raise ValueError("unsupported FEC statement record")
    evidence = {**evidence, "locator": {**evidence["locator"], "version": version, "record_type": form}}

    def g(n):
        return row.get(str(n))

    subject_type = "candidate" if form.startswith("F2") else "committee"

    def emit(relation, kind, id_position, name_positions, positions, year=None):
        names = [g(n) for n in name_positions]
        name = names[0] if len(names) == 1 else " ".join(n for n in names if n) if any(names) else None
        return _observation(
            row,
            evidence,
            "original_statement",
            None,
            g(1),
            subject_type,
            relation,
            kind,
            object_id=g(id_position),
            name=name,
            fields=tuple(str(n) for n in dict.fromkeys((0, 1, id_position, *name_positions, *positions))),
            election_year=year,
        )

    if form.startswith("F2"):
        starts = (
            [(2, "authorized_committee")]
            if form == "F2S"
            else [(23, "principal_campaign_committee"), (30, "authorized_committee")]
        )
        for start, role in starts:
            positions = (*range(start, start + 7), 22, 42) if form != "F2S" else range(start, start + 7)
            yield emit(role, "committee", start, [start + 1], positions, g(22) if form != "F2S" else None)
        return
    if form != "F1S":
        role = {
            "A": "principal_campaign_candidate",
            "B": "authorized_candidate",
            "C": "support_or_oppose_candidate",
        }.get(g(21), "committee_candidate")
        yield emit(role, "candidate", 22, range(23, 28), (14, 20, 21, 28, 29, 30, 31))
    else:
        yield emit("joint_fundraising_participant", "committee", 3, [2], (2, 3))
    start = 4 if form == "F1S" else 37 if version == "8.3" else 39
    code = g(start + 13)
    role, kind = {
        "ORG": ("connected_organization", "organization"),
        "AFF": ("affiliated_committee", "committee"),
        "JFR": ("joint_fundraising_participant", "committee"),
        "LPS": ("leadership_pac_sponsor", "candidate"),
    }.get(code, ("reported_affiliation", "unresolved"))
    # Preserve conflicting committee/candidate IDs as separate reported targets.
    candidate = bool(g(start + 2)) or any(g(n) for n in range(start + 3, start + 8)) or code == "LPS"
    if g(start) or g(start + 1) or not candidate:
        yield emit(role, "committee" if g(start) else kind, start, [start + 1], range(start, start + 14))
    if candidate:
        yield emit(role, "candidate", start + 2, range(start + 3, start + 8), range(start, start + 14))


def write_fec_relationship_rows(records, destination: Path, *, batch_size: int = 2_000) -> Path:
    """Write explicit observations only; caller owns completeness and admission."""

    def checked():
        for row in records:
            if set(row) != set(COLUMNS):
                raise ValueError("relationship observation columns differ from the output schema")
            yield row

    return write_rows(checked(), destination, SCHEMA, batch_size=batch_size)
