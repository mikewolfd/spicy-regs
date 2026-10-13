"""Exact main-field domain keys; no inferred identities or first/latest choice."""
import duckdb
import pytest

from spicy_regs.explorer_domain_navigation import declarations
from spicy_regs.explorer_navigation import scalar, target_keys, validate_navigation
from spicy_regs.relationship_views.fec_typed import filing_reference_resolution

H = "sha256:" + "a" * 64
OTHER = "sha256:" + "b" * 64
FILING = "urn:fec:filing:official-fec:openfec-file-number:22"
SOURCE_FILING = "urn:fec:filing:official-fec:openfec-file-number:11"


def spec(name):
    return next(s for s in declarations() if s["id"] == name)


def forward(name, source, targets):
    target = spec(name)["targets"][0]
    keys = target_keys(target, {}, source)
    return [] if keys is None else [row for row in targets
                                   if [scalar(row.get(c)) for c in target["columns"]] == keys]


def reverse(name, target, sources):
    recipe = spec(name)["targets"][0]
    keys = [scalar(target.get(c)) for c in recipe["columns"]]
    return [row for row in sources if target_keys(recipe, {}, row) == keys]


def test_domain_registry_uses_only_main_rows_and_has_no_duplicate_affiliation_route():
    specs = validate_navigation(declarations())
    assert len(specs) == len({s["id"] for s in specs}) == 13
    assert all(s["mode"] == "row" and s["receiptFields"] == [] and s["fields"] == [] for s in specs)
    assert all(p["from"] == "row" for s in specs for t in s["targets"] for k in t["keys"] for p in k["parts"])
    assert not any(s["source"] == "member_party_affiliations" for s in specs)


@pytest.mark.parametrize("name,source,target,reference", [
    ("fec_audit_finding_matter", {"matter_record_id": H, "matter_id": "official-fec:audit:1"},
     {"record_id": H, "matter_id": "official-fec:audit:1"}, "matter_record_id"),
    ("fec_audit_finding_document", {"supporting_document_record_id": H, "matter_record_id": OTHER},
     {"record_id": H, "matter_record_id": OTHER}, "supporting_document_record_id"),
    ("fec_recommendation_report", {"report_id": H}, {"report_id": H}, "report_id"),
    ("fec_document_parent", {"parent_document_record_id": H, "matter_record_id": OTHER},
     {"record_id": H, "matter_record_id": OTHER}, "parent_document_record_id"),
    ("fec_event_document", {"related_document_record_id": H, "matter_record_id": OTHER},
     {"record_id": H, "matter_record_id": OTHER}, "related_document_record_id"),
    ("fec_party_document", {"document_record_id": H, "matter_record_id": OTHER},
     {"record_id": H, "matter_record_id": OTHER}, "document_record_id"),
])
def test_source_constructed_fec_references_preserve_repetitions_and_absence(name, source, target, reference):
    repeated = [dict(target, observation="first"), dict(target, observation="second")]
    assert forward(name, source, repeated) == repeated
    assert reverse(name, target, [source, dict(source), {**source, reference: None}]) == [source, source]
    for unavailable in (None, "", "a name", "sha256:short"):
        assert forward(name, {**source, reference: unavailable}, repeated) == []
    if "matter_record_id" in target:
        assert forward(name, source, [{**target, "matter_record_id": H}]) == []


def resolve_filings(targets, *, repeated_source=False):
    """Run the maintained actual conflict policy; do not hand-author approval."""
    with duckdb.connect() as c:
        c.execute("CREATE TABLE fec_filings(record_id VARCHAR, filing_key VARCHAR, source_authority VARCHAR, "
                  "source_namespace VARCHAR, report_number VARCHAR, native_filer_id VARCHAR, form_type VARCHAR)")
        source = (H, SOURCE_FILING, "official-fec", "fec-openfec-file-number", "11", "C00000001", "F3")
        c.executemany("INSERT INTO fec_filings VALUES (?,?,?,?,?,?,?)", [source, *([source] if repeated_source else []), *targets])
        c.execute("CREATE TABLE fec_filing_links(record_id VARCHAR, source_filing_record_id VARCHAR, filing_key VARCHAR, "
                  "target_filing_key VARCHAR, target_resolution_status VARCHAR, source_authority VARCHAR, "
                  "source_namespace VARCHAR, filing_link_status VARCHAR)")
        c.execute("INSERT INTO fec_filing_links VALUES (?,?,?,?,?,?,?,?)", [OTHER, H, SOURCE_FILING, FILING,
                  "native_reference_unresolved", "official-fec", "fec-openfec-file-number", "native_file_number"])
        cursor = c.execute(filing_reference_resolution())
        names = [d[0] for d in cursor.description]
        values = cursor.fetchone()
        assert values is not None
        result = dict(zip(names, values, strict=True))
    return result


def filing(record_id=OTHER, authority="official-fec", report_number="22"):
    return (record_id, FILING, authority, "fec-openfec-file-number", report_number, "C00000001", "F3")


def test_filing_target_uses_actual_resolver_and_preserves_all_logical_version_observations():
    result = resolve_filings([filing(), filing("sha256:" + "c" * 64)])
    assert result["target_resolution_status"] == "resolved_native_filing_key"
    assert result["target_observation_count"] == 2
    targets = [{"filing_key": FILING, "source_authority": "official-fec", "source_namespace": "fec-openfec-file-number",
                "record_id": x[0]} for x in [filing(), filing("sha256:" + "c" * 64)]]
    assert forward("fec_filing_reference_target", result, targets) == targets
    assert reverse("fec_filing_reference_target", targets[0], [result, dict(result)]) == [result, result]
    assert forward("fec_filing_reference_target", {**result, "target_resolution_status": "native_reference_unresolved"}, targets) == []


@pytest.mark.parametrize("targets,repeated_source,status", [
    ([], False, "unresolved_target_not_retained"),
    ([filing(), filing()], False, "unresolved_target_observation_ambiguous"),
    ([filing(), filing("sha256:" + "c" * 64, authority="third-party")], False, "unresolved_target_identity_conflict"),
    ([filing(), filing("sha256:" + "c" * 64, report_number="23")], False, "unresolved_target_identity_conflict"),
    ([filing()], True, "unresolved_source_observation_ambiguous"),
])
def test_actual_filing_conflict_or_missing_resolution_refuses_target_navigation(targets, repeated_source, status):
    result = resolve_filings(targets, repeated_source=repeated_source)
    assert result["target_resolution_status"] == status
    assert target_keys(spec("fec_filing_reference_target")["targets"][0], {}, result) is None


def test_filing_source_requires_exact_observation_native_key_and_authority():
    row = resolve_filings([filing()])
    target = {"record_id": H, "filing_key": SOURCE_FILING, "source_authority": "official-fec",
              "source_namespace": "fec-openfec-file-number"}
    assert forward("fec_filing_reference_source", row, [target, dict(target)]) == [target, target]
    assert reverse("fec_filing_reference_source", target, [row, dict(row)]) == [row, row]
    for changes in ({"filing_link_status": "unresolved"}, {"source_authority": "third-party"},
                    {"source_namespace": "image-number"}, {"filing_key": "11"}, {"source_filing_record_id": None}):
        assert forward("fec_filing_reference_source", {**row, **changes}, [target]) == []
    assert forward("fec_filing_reference_source", row, [{**target, "filing_key": FILING}]) == []


@pytest.mark.parametrize("side", ["from", "to"])
def test_diff_endpoint_preserves_printing_element_and_body_scope_in_both_directions(side):
    row = {"bill_id": "119-hr-1", side + "_version_code": "ih", side + "_printing_id": "printing-1",
           side + "_element_id": "e1", side + "_body_version_id": "body-1"}
    target = {"bill_id": "119-hr-1", "version_code": "ih", "printing_id": "printing-1",
              "element_id": "e1", "body_version_id": "body-1"}
    name = "section_diff_" + side + "_section"
    assert forward(name, row, [target, dict(target)]) == [target, target]
    assert reverse(name, target, [row, dict(row)]) == [row, row]
    for field in ("bill_id", "version_code", "printing_id", "element_id", "body_version_id"):
        assert forward(name, row, [{**target, field: "wrong"}]) == []
    assert forward(name, {**row, side + "_body_version_id": None}, [target]) == []
    assert forward(name, {**row, side + "_element_id": ""}, [target]) == []


def test_law_parent_uses_same_law_and_explicit_nonnegative_position():
    row = {"law_id": "119-1", "seq": 4, "parent_seq": 0, "is_quoted": True}
    target = {"law_id": "119-1", "seq": 0}
    assert forward("law_section_parent", row, [target]) == [target]
    assert reverse("law_section_parent", target, [row, dict(row)]) == [row, row]
    assert forward("law_section_parent", row, [{**target, "law_id": "118-1"}]) == []
    for parent in (None, "", -1, False, "01", 1.0):
        assert forward("law_section_parent", {**row, "parent_seq": parent}, [target]) == []


def test_printed_action_document_and_mention_require_exact_body_kind_bill_and_offsets():
    row = {"document_kind": "govinfo_package", "document_key": "CRPT-118hrpt1", "body_version_id": "body-1",
           "bill_id": "118-hr-1", "mention_span_start": 0, "mention_span_end": 12}
    document = {"package_id": row["document_key"], "body_version_id": "body-1"}
    mention = {"document_kind": row["document_kind"], "document_key": row["document_key"],
               "body_version_id": "body-1", "cite_kind": "bill_number", "target_key": row["bill_id"],
               "span_start": 0, "span_end": 12}
    for name, target in (("printed_committee_action_document", document), ("printed_committee_action_mention", mention)):
        assert forward(name, row, [target, dict(target)]) == [target, target]
        assert reverse(name, target, [row, dict(row)]) == [row, row]
        assert forward(name, {**row, "document_kind": "budget_volume"}, [target]) == []
        assert forward(name, {**row, "body_version_id": None}, [target]) == []
        assert forward(name, row, [{**target, "body_version_id": "wrong"}]) == []
    for field in ("document_kind", "document_key", "cite_kind", "target_key", "span_start", "span_end"):
        assert forward("printed_committee_action_mention", row, [{**mention, field: "wrong"}]) == []
    assert forward("printed_committee_action_mention", {**row, "mention_span_start": -1}, [mention]) == []


def test_filing_target_matches_authority_and_namespace_even_after_a_resolution_label():
    # The resolver counts conflicts within a native key. The portable target
    # also compares authority/namespace, so a single foreign assertion cannot
    # pass solely because its own population had no internal conflict.
    result = resolve_filings([filing(authority="third-party")])
    foreign = {"filing_key": FILING, "source_authority": "third-party", "source_namespace": "fec-openfec-file-number"}
    assert forward("fec_filing_reference_target", result, [foreign]) == []
    result = resolve_filings([filing()])
    assert forward("fec_filing_reference_target", result, [{**foreign, "source_authority": "official-fec",
                                                           "source_namespace": "another-file-number-system"}]) == []
