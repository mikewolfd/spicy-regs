"""The documented-versus-observed enumeration gate.

Three kinds of test, in the shape the data-dictionary check already uses: the
gate passes on the committed inputs, the gate *fires* when an input breaks, and
the pinned publisher facts are what the pinned publisher bytes actually say.

The last kind is the point of checking the captures in rather than transcribing
them. Every documented value below is re-derived from
``sample-data/source-domains/`` on each run, so these assertions state something
about the publisher's own document, not about a list somebody typed.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from scripts.check_source_domain_drift import observe, table_urls
from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.sources.publication import parse_index, single_member
from spicy_regs.sources.source_domains import (
    ACCEPTED_DOMAIN_FINDINGS,
    DEFAULT_SOURCE_DOMAIN_DIR,
    DOMAIN_KEYS,
    FINDING_KINDS,
    OBSERVED_SNAPSHOT_FILENAMES,
    UNDOCUMENTED,
    SourceDomainError,
    documented_domains,
    documented_values,
    domain_findings,
    fec_code_values,
    label_only_domains,
    lda_constant_values,
    load_capture_manifest,
    load_observed_snapshot,
    load_observed_snapshots,
    merged_domains,
    openapi_schema_enum,
    read_capture,
    stale_accepted_findings,
    unrecorded_findings,
    xsd_documented_options,
)

ROOT = Path(__file__).resolve().parents[1]
DOMAIN_DIR = ROOT / DEFAULT_SOURCE_DOMAIN_DIR
TOOL = ROOT / "scripts" / "check_source_domain_drift.py"

#: The host the dated snapshot recorded its URLs under, resolved through the publication
#: index: the data.spicygov.ai domain, since the bucket's r2.dev host answered 429 to the
#: column reads of a remote observation (2026-10-30).
FORK_PUBLIC_URL = "https://data.spicygov.ai"

#: The live publication index is a network fetch, so a fragment of it is checked
#: in beside the snapshot for the offline URL assertions.
PUBLISHED_INDEX_FRAGMENT = "published-index-fragment-2026-10-30.json"


def _published_index() -> dict:
    return parse_index((DOMAIN_DIR / PUBLISHED_INDEX_FRAGMENT).read_bytes())


@pytest.fixture(scope="module")
def documented():
    return documented_domains(DOMAIN_DIR)


@pytest.fixture(scope="module")
def snapshot():
    return load_observed_snapshot(DOMAIN_DIR)


@pytest.fixture(scope="module")
def agenda_snapshot():
    return load_observed_snapshot(DOMAIN_DIR, filename=OBSERVED_SNAPSHOT_FILENAMES[1])


@pytest.fixture(scope="module")
def observed():
    """Every dated snapshot's domains: the register the gate diffs."""
    return merged_domains(load_observed_snapshots(DOMAIN_DIR))


@pytest.fixture(scope="module")
def findings(documented, observed):
    return domain_findings(documented, observed)


# --- the gate itself --------------------------------------------------------


def test_every_finding_is_recorded(findings):
    """The gate's pass condition: the ledger accounts for every disagreement."""

    unrecorded = unrecorded_findings(findings)
    assert unrecorded == (), [finding.identifier for finding in unrecorded]


def test_the_ledger_holds_nothing_stale(findings):
    """The other half: delete an accepted finding once the data stops producing it.

    With the test above this is set equality in both directions — ledger ⊇ findings
    and ledger ⊆ findings — so a third assertion of the same thing would add nothing.
    """

    assert stale_accepted_findings(findings) == ()


def test_every_accepted_finding_states_a_reason():
    for entry in ACCEPTED_DOMAIN_FINDINGS:
        assert entry.kind in FINDING_KINDS
        assert entry.domain_key in DOMAIN_KEYS
        assert len(entry.reason) > 60, entry


def test_the_findings_are_the_ones_this_snapshot_carries(findings):
    """Pinned so a silent change in either half shows up as a number, not a shrug.

    This list deliberately transcribes the ledger a second time. The set-equality
    tests above compare the ledger against the findings, so both pass when someone
    edits the ledger and the snapshot to agree on something wrong. This one states
    them independently of both, and catches that. Kept for that reason. The 17
    listed action types no action carries are counted rather than spelled out.
    """

    identifiers = sorted(
        finding.identifier
        for finding in findings
        if not (finding.domain_key == "billstatus-action-type" and finding.kind == "unobserved-value")
    )
    assert identifiers == [
        "billstatus-action-type/undocumented-value/Veto",
        "fec-filing-frequency/unobserved-value/W",
        "fec-organization-type/undocumented-value/H",
        "fec-organization-type/undocumented-value/I",
        "regulations-gov-comment-document-type/undocumented-value/Public Submission",
        "regulations-gov-comment-document-type/unobserved-value/Notice",
        "regulations-gov-comment-document-type/unobserved-value/Proposed Rule",
        "regulations-gov-comment-document-type/unobserved-value/Rule",
        "regulations-gov-document-type/undocumented-value/Public Submission",
        "unified-agenda-priority-category/undocumented-value/Section 3(f)(1) Significant",
        "unified-agenda-priority-category/unobserved-value/Not Major",
        "unified-agenda-rin-relation/undocumented-value/Merged with",
        "unified-agenda-rin-relation/undocumented-value/Previously report",
        "unified-agenda-rin-relation/undocumented-value/Related t",
        "unified-agenda-rin-relation/unobserved-value/Merge with",
        "unified-agenda-rin-status/undocumented-value/First Time Published in The Unified Agenda",
        "unified-agenda-rin-status/undocumented-value/Previously Published in The Unified Agenda",
        "unified-agenda-rin-status/unobserved-value/First time published in the Unified Agenda",
        "unified-agenda-rin-status/unobserved-value/Previously published in the Unified Agenda",
        "unified-agenda-rule-stage/unobserved-value/No Stage",
    ]
    unobserved_types = [f for f in findings if f.domain_key == "billstatus-action-type" and f.kind == "unobserved-value"]
    assert len(unobserved_types) == 17


def test_the_lda_and_fec_codes_the_tickets_named_agree_with_their_registries(findings, snapshot):
    """LDA issue codes 79/79 and filing types 50/50 observed with no finding; FEC committee type 16/16."""

    drifted = {finding.domain_key for finding in findings}
    assert {"lda-general-issue-code", "lda-filing-type", "fec-committee-type", "fec-committee-designation"}.isdisjoint(
        drifted
    )
    assert len(snapshot.domains["lda-general-issue-code"].value_counts) == 79
    assert len(snapshot.domains["lda-filing-type"].value_counts) == 50


def test_public_submission_carries_its_row_support(findings):
    """The finding that matters most is a count, not an adjective."""

    support = {one.domain_key: one.row_count for one in findings if one.value == "Public Submission"}
    assert support == {"regulations-gov-document-type": 374, "regulations-gov-comment-document-type": 26_417_470}
    assert {one.kind for one in findings if one.value == "Public Submission"} == {UNDOCUMENTED}


def test_two_domains_agree_completely(findings):
    """Some columns agree exactly, and a gate that reports drift everywhere gates nothing."""

    drifted = {finding.domain_key for finding in findings}
    assert "regulations-gov-docket-type" not in drifted
    assert "unified-agenda-major" not in drifted


# --- the gate fires ---------------------------------------------------------


def test_an_undocumented_value_that_is_not_recorded_fails(documented, observed):
    """Flip one observed value to something nobody documented: the gate must catch it."""

    domain = observed["unified-agenda-major"]
    mutated = dict(observed)
    mutated["unified-agenda-major"] = replace(domain, value_counts=(("Undecided", 865), ("Yes", 309), ("No", 2780)))
    unrecorded = unrecorded_findings(domain_findings(documented, mutated))
    assert [finding.identifier for finding in unrecorded] == [
        "unified-agenda-major/undocumented-value/Undecided",
        "unified-agenda-major/unobserved-value/Undetermined",
    ]


def test_a_documented_value_falling_out_of_the_data_fails(documented, observed):
    """Drop a value the publisher documents and we observe: that is news, not silence."""

    domain = observed["regulations-gov-docket-type"]
    mutated = dict(observed)
    mutated["regulations-gov-docket-type"] = replace(domain, value_counts=(("Nonrulemaking", 213858),))
    unrecorded = unrecorded_findings(domain_findings(documented, mutated))
    assert [finding.identifier for finding in unrecorded] == ["regulations-gov-docket-type/unobserved-value/Rulemaking"]


def test_a_ledger_entry_the_data_stopped_producing_fails(documented, observed):
    """The reverse: 'Public Submission' disappearing must fail loudly, not pass quietly."""

    domain = observed["regulations-gov-document-type"]
    kept = tuple(one for one in domain.value_counts if one[0] != "Public Submission")
    mutated = dict(observed)
    mutated["regulations-gov-document-type"] = replace(domain, value_counts=kept)
    stale = stale_accepted_findings(domain_findings(documented, mutated))
    assert [entry.value for entry in stale] == ["Public Submission"]


def test_a_cut_off_relation_the_data_stopped_producing_fails(documented, observed):
    """Repairing "Related t" upstream without deleting its ledger entry must fail, not pass quietly."""

    domain = observed["unified-agenda-rin-relation"]
    mutated = dict(observed)
    mutated["unified-agenda-rin-relation"] = replace(
        domain, value_counts=tuple(one for one in domain.value_counts if one[0] != "Related t")
    )
    stale = stale_accepted_findings(domain_findings(documented, mutated))
    assert [(entry.kind, entry.value) for entry in stale] == [(UNDOCUMENTED, "Related t")]


def test_a_domain_two_snapshots_observe_is_refused(snapshot, agenda_snapshot):
    with pytest.raises(SourceDomainError, match="more than one snapshot"):
        merged_domains((snapshot, agenda_snapshot, agenda_snapshot))


def test_a_domain_observed_on_the_wrong_column_fails(documented, observed):
    domain = observed["unified-agenda-major"]
    mutated = dict(observed)
    mutated["unified-agenda-major"] = replace(domain, column="rule_stage")
    with pytest.raises(SourceDomainError, match="observed on unified_agenda.rule_stage"):
        domain_findings(documented, mutated)


def test_an_unobserved_domain_fails(documented, observed):
    mutated = {key: value for key, value in observed.items() if key != "unified-agenda-major"}
    with pytest.raises(SourceDomainError, match="domain coverage differs"):
        domain_findings(documented, mutated)


# --- the pinned publisher bytes ---------------------------------------------


def test_captures_verify_against_their_pins():
    captures = load_capture_manifest(DOMAIN_DIR)
    assert sorted(captures) == [
        "billstatus-user-guide",
        "fec-committee-master-file",
        "fec-committee-type-codes",
        "fec-party-codes",
        "lda-filing-types",
        "lda-general-issue-codes",
        "reginfo-rin-data-xsd",
        "regulations-gov-openapi-v4",
    ]
    for capture in captures.values():
        assert len(read_capture(capture, root=DOMAIN_DIR)) == capture.byte_length
        assert capture.source_url.startswith("https://")


def test_a_mutated_capture_is_refused(tmp_path):
    """Edit one byte of the XSD and the pin refuses it, so nobody can retype a publisher's list."""

    shutil.copytree(DOMAIN_DIR, tmp_path / "domains")
    target = tmp_path / "domains" / "reginfo-rin-data-ver10262011.xsd"
    target.write_bytes(target.read_bytes().replace(b"No Stage", b"No stage"))
    with pytest.raises(SourceDomainError, match="hashes to"):
        documented_domains(tmp_path / "domains")


def test_regulations_gov_document_type_is_the_five_documented_values(documented):
    domain = documented["regulations-gov-document-type"]
    assert domain.values == ("Notice", "Rule", "Proposed Rule", "Supporting & Related Material", "Other")
    assert domain.locator.endswith("components.schemas.DocumentType.enum (lines 893-898)")
    assert "Public Submission" not in domain.values


def test_the_docket_type_enums_trailing_space_is_not_part_of_the_value(documented):
    """The capture writes ``- Nonrulemaking `` with a trailing space; a YAML plain scalar drops it."""

    assert documented["regulations-gov-docket-type"].values == ("Rulemaking", "Nonrulemaking")
    raw = (DOMAIN_DIR / "regulations-gov-openapi-v4-2026-08-03.yaml").read_text(encoding="utf-8")
    assert "        - Nonrulemaking \n" in raw


def test_unified_agenda_domains_come_from_documentation_prose_not_enumerations(documented):
    xsd = (DOMAIN_DIR / "reginfo-rin-data-ver10262011.xsd").read_bytes()
    assert b"xs:enumeration" not in xsd
    assert documented["unified-agenda-rule-stage"].values == (
        "Prerule Stage",
        "Proposed Rule Stage",
        "Final Rule Stage",
        "Long-Term Actions",
        "Completed Actions",
        "No Stage",
    )
    assert documented["unified-agenda-rin-status"].values == (
        "First time published in the Unified Agenda",
        "Previously published in the Unified Agenda",
    )
    assert documented["unified-agenda-major"].values == ("Yes", "No", "Undetermined")
    assert documented["unified-agenda-rin-relation"].values == (
        "Merge with",
        "Split from",
        "Previously reported as",
        "Duplicate of",
        "Related to",
    )


def test_the_priority_category_sentence_lists_not_major_twice(documented):
    """The publisher's own duplicate folds to one value; the raw count keeps the evidence."""

    domain = documented["unified-agenda-priority-category"]
    assert domain.values == (
        "Economically Significant",
        "Other Significant",
        "Substantive, Nonsignificant",
        "Routine and Frequent",
        "Info./Admin./Other",
        "Not Major",
    )
    assert (len(domain.values), domain.raw_option_count) == (6, 7)


def test_a_documented_list_that_grows_is_refused(tmp_path):
    """Counts are pinned per domain, so a publisher adding a value cannot land unnoticed."""

    shutil.copytree(DOMAIN_DIR, tmp_path / "domains")
    target = tmp_path / "domains" / "regulations-gov-openapi-v4-2026-08-03.yaml"
    target.write_bytes(
        target.read_bytes().replace(b"        - Other\n", b"        - Other\n        - Public Submission\n")
    )
    manifest_path = tmp_path / "domains" / "documented-enumeration-capture-manifest-v1.json"
    _repin(manifest_path, "regulations-gov-openapi-v4", target)
    with pytest.raises(SourceDomainError, match="parses 6 values from 6 options, pinned at 5"):
        documented_domains(tmp_path / "domains")


def test_an_openapi_schema_whose_enum_is_not_a_list_is_refused():
    payload = b"components:\n  schemas:\n    DocumentType:\n      type: string\n      enum:\n        bogus: 1\n"
    with pytest.raises(SourceDomainError, match="states no enum member list"):
        openapi_schema_enum(payload, "DocumentType")


def test_an_openapi_schema_that_is_absent_is_refused():
    payload = b"components:\n  schemas:\n    DocketType:\n      enum:\n        - Rulemaking\n"
    with pytest.raises(SourceDomainError, match="declares no components.schemas.DocumentType"):
        openapi_schema_enum(payload, "DocumentType")


def test_an_openapi_capture_that_is_not_yaml_is_refused():
    with pytest.raises(SourceDomainError, match="not well-formed YAML"):
        openapi_schema_enum(b"components:\n  schemas:\n   - [unclosed\n", "DocumentType")


def test_an_xsd_documentation_string_without_an_option_list_is_refused():
    payload = (
        b'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
        b'<xs:element name="MAJOR"><xs:annotation><xs:documentation>Free text.</xs:documentation>'
        b"</xs:annotation></xs:element></xs:schema>"
    )
    with pytest.raises(SourceDomainError, match="does not state an option list"):
        xsd_documented_options(payload, "MAJOR")


def test_an_xsd_with_a_doctype_is_refused():
    payload = b'<!DOCTYPE schema [<!ENTITY x "y">]>\n<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"/>'
    with pytest.raises(SourceDomainError, match="DOCTYPE"):
        xsd_documented_options(payload, "MAJOR")


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">&x;</xs:schema>', "malformed"),
        (b'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">', "malformed"),
        (b"<a>" + b" " * (1024 * 1024) + b"</a>", "within max_bytes"),
        (b"<a>" * 300 + b"</a>" * 300, "nesting depth"),
    ],
)
def test_an_xsd_the_shared_reader_refuses_is_a_source_domain_error(payload, reason):
    """spicy-docs' bounded reader: an undefined entity, broken XML, an oversize or over-deep document all refuse."""
    with pytest.raises(SourceDomainError, match=reason):
        xsd_documented_options(payload, "MAJOR")


# --- the BILLSTATUS guide, the LDA constants, the FEC lists ----------------


@pytest.fixture(scope="module")
def labels_only():
    return label_only_domains(DOMAIN_DIR)


def test_bill_action_types_are_the_guides_twenty_six_and_carry_no_invented_label(documented):
    """Section 4 states the type text alone, so the value is its own meaning and no label is made up."""

    domain = documented["billstatus-action-type"]
    assert len(domain.values) == 26
    assert domain.values[:2] == ("Actions by the President", "Actions Related to President")
    assert domain.labels == {}


def test_action_codes_are_labels_only_and_carry_the_guides_own_caveat(labels_only):
    """The guide says no complete list exists and codes were reused, so the labels say so and gate nothing."""

    domain = labels_only["billstatus-action-code"]
    assert len(domain.values) == 36
    assert domain.labels["B00100"] == "Sponsor introductory remarks on measure"
    assert "a complete, authoritative list of action codes does not exist" in (domain.caveat or "")
    assert "re-used over the years" in (domain.caveat or "")
    assert "billstatus-action-code" not in DOMAIN_KEYS


def test_lda_lists_are_the_publishers_json_read_directly(documented):
    """Re-derived from the pinned bytes with ``json`` alone: 79 issue codes (not the ticket's 80), 50 filing types."""

    for key, filename, count in (
        ("lda-general-issue-code", "lda-general-issue-codes-2026-07-30.json", 79),
        ("lda-filing-type", "lda-filing-types-2026-07-30.json", 50),
    ):
        items = json.loads((DOMAIN_DIR / filename).read_bytes())
        assert documented[key].values == tuple(item["value"] for item in items)
        assert dict(documented[key].labels) == {item["value"]: item["name"] for item in items}
        assert len(items) == count
    assert documented["lda-filing-type"].labels["RR"] == "Registration"


@pytest.mark.parametrize(
    "payload",
    [b'{"value": "ACC"}', b'[{"value": "ACC"}]', b'[{"value": "ACC", "name": "A"}, {"value": "ACC", "name": "B"}]'],
)
def test_a_drifted_lda_constants_response_is_refused(payload):
    with pytest.raises(SourceDomainError):
        lda_constant_values(payload)


def test_fec_lists_come_from_refspecs_parse_of_the_pinned_pages(documented, labels_only):
    assert documented["fec-committee-designation"].values == ("A", "B", "D", "J", "P", "U")
    assert len(documented["fec-committee-type"].values) == 16
    assert len(labels_only["fec-committee-party"].values) == 95
    assert labels_only["fec-candidate-party"].labels["REP"] == "Republican Party"


def test_every_vendored_fec_code_and_label_is_on_its_pinned_page(documented, labels_only):
    """The digest match is the vendored file's own claim; this reads the pinned bytes themselves.

    A one-way presence check (each code and label occurs in the page's text), not a second parser.
    """

    def page_text(capture) -> str:
        text = re.sub(r"<[^>]+>", " ", html.unescape(read_capture(capture, root=DOMAIN_DIR).decode("utf-8")))
        return " ".join(text.split())

    captures = load_capture_manifest(DOMAIN_DIR)
    fec = [domain for domain in (*documented.values(), *labels_only.values()) if domain.capture_key.startswith("fec-")]
    assert len(fec) == 6
    for domain in fec:
        text = page_text(captures[domain.capture_key])
        for code in domain.values:
            label = " ".join(domain.labels[code].split())
            assert re.search(rf"(?<![A-Za-z0-9]){re.escape(code)}(?![A-Za-z0-9])", text), (domain.key, code)
            assert label in text, (domain.key, code, label)


def test_a_vendored_fec_list_parsed_from_other_bytes_is_refused(tmp_path):
    """The derived list is not its own witness: its recorded source digest must be the pinned capture's."""

    capture = load_capture_manifest(DOMAIN_DIR)["fec-committee-master-file"]
    vendored = json.loads((ROOT / "src/spicy_regs/reference/refspec/fec-committee-codes.json").read_bytes())
    vendored["resources"]["committeeDesignation"]["source_sha256"] = "sha256:" + "0" * 64
    lists = tmp_path / "fec-committee-codes.json"
    lists.write_text(json.dumps(vendored), encoding="utf-8")
    with pytest.raises(SourceDomainError, match="not the pinned capture"):
        fec_code_values(capture, "committeeDesignation", lists=lists)


def test_documented_values_mark_which_lists_gate():
    """What the dictionary carries: the checked gated list and the unchecked labels, each with its source."""

    values = documented_values(DOMAIN_DIR)
    filing_type = values["lobbying_filings"]["filing_type"]
    assert filing_type["checked"] is True
    assert {"value": "RR", "label": "Registration"} in filing_type["values"]
    party = values["fec_committee_history"]["party"]
    assert party["checked"] is False
    assert party["source_url"] == "https://www.fec.gov/campaign-finance-data/party-code-descriptions/"
    assert "caveat" in values["bill_actions"]["action_code"]
    assert values["bill_actions"]["action_type"]["values"][0] == {"value": "Actions by the President"}


def test_documented_values_carry_the_accepted_disagreements():
    """A checked list is the publisher's, not the column's: the dominant off-list value must ride along."""

    values = documented_values(DOMAIN_DIR)
    comment_type = values["comments"]["document_type"]
    assert "Public Submission" not in {item["value"] for item in comment_type["values"]}
    assert [item["value"] for item in comment_type["observed_off_list"]] == ["Public Submission"]
    assert set(comment_type["unobserved"]) == {"Notice", "Rule", "Proposed Rule"}
    action_type = values["bill_actions"]["action_type"]
    assert [item["value"] for item in action_type["observed_off_list"]] == ["Veto"]
    assert "Actions Related to Veto" in action_type["unobserved"]
    assert "Section 3(f)(1) Significant" in {
        item["value"] for item in values["unified_agenda"]["priority_category"]["observed_off_list"]
    }
    lda = values["lobbying_filings"]["filing_type"]
    assert (lda["observed_off_list"], lda["unobserved"]) == ([], [])
    assert {"observed_off_list", "unobserved", "observed_at"}.isdisjoint(values["bill_actions"]["action_code"])


# --- the observed snapshot's own provenance ---------------------------------


def test_every_domain_names_a_real_published_column(documented, labels_only):
    schemas = expected_schemas()
    for domain in (*documented.values(), *labels_only.values()):
        columns = dict(schemas[domain.table])
        column, _, member = domain.column.partition(".")
        assert column in columns, f"{domain.table}.{column} is not a published column"
        if member:
            assert f"{member} VARCHAR" in columns[column] and columns[column].endswith("[]"), columns[column]


def test_the_snapshot_states_where_its_rows_came_from(snapshot):
    assert snapshot.observed_at == "2026-10-31T01:59:08Z"
    assert snapshot.producer_revision == (
        "publication.v2.json sha256:74157933fbcbeb9b54adae90442b24af81601abe330bc92a9e104e58666f1fb5"
    )
    sources = {str(one["table"]): one for one in snapshot.sources}
    # unified_agenda is observed whole in its own dated file.
    assert sorted(sources) == [
        "bill_actions",
        "comments",
        "dockets",
        "documents",
        "fec_committee_history",
        "lobbying_activities",
        "lobbying_filings",
    ]
    index = _published_index()
    for table, source in sources.items():
        # The recorded URL is a string in a checked-in data file; the expected one
        # resolves through the publisher's own index (checked in as a fragment).
        # Two origins, so this fails when the corpus moves or a generation
        # advances and nobody re-pins the snapshot — exactly how this file first
        # shipped naming a host we never served.
        member = single_member(index, f"{table}.parquet")
        assert source["publisher_url"] == f"{FORK_PUBLIC_URL}/{member.path}"
        # Read remotely, so the digest is the index's pin for that member, and says so.
        assert source["digest_basis"] == "publication-index"
        assert (source["bytes_digest"], source["byte_length"], source["row_count"]) == (
            member.sha256,
            member.byte_size,
            member.rows,
        )
    assert sources["documents"]["row_count"] == 2_004_396
    assert sources["bill_actions"]["row_count"] == 932_877


@pytest.mark.parametrize("filename", OBSERVED_SNAPSHOT_FILENAMES)
def test_the_snapshot_row_counts_add_up(filename):
    """Every row of a domain's table is either a value or a null; nothing is dropped.

    The file states each table's total twice — once per domain and once per source —
    so require the two to agree. A hand-edit of one half of the observation must
    match the other half before it can pass.
    """

    snapshot = load_observed_snapshot(DOMAIN_DIR, filename=filename)
    sources = {str(one["table"]): one for one in snapshot.sources}
    for domain in snapshot.domains.values():
        assert sum(count for _, count in domain.value_counts) + domain.null_count == domain.row_count
        if "." not in domain.column:  # a list-member domain counts list elements, not table rows
            assert domain.row_count == sources[domain.table]["row_count"]


def test_the_nullable_domains_are_the_measured_ones(snapshot):
    """Nulls are not findings, so their counts are pinned here instead of passing unseen."""

    nulls = {key: domain.null_count for key, domain in snapshot.domains.items() if domain.null_count}
    assert nulls == {
        "billstatus-action-type": 1_093,
        "fec-committee-designation": 79,
        "fec-committee-type": 42,
        "fec-organization-type": 209_616,
    }


def test_the_agenda_snapshot_observes_the_whole_table_of_an_unpublished_build(agenda_snapshot):
    """unified_agenda is observed over all 60 retained editions (241,726 rows), RIN_RELATION over its list elements.

    The RIN_RELATION counts equal the independent lxml parse in work/agenda-related-rins-20261030 (28,720
    related RINs); the scalar columns equal the live table's, since the build equals live on every one of
    them. The build is not published yet, so no publisher URL is recorded; re-pin once it is.
    """

    assert sorted(agenda_snapshot.domains) == [key for key in sorted(DOMAIN_KEYS) if key.startswith("unified-agenda-")]
    relation = agenda_snapshot.domains["unified-agenda-rin-relation"]
    assert relation.column == "related_rins.rin_relation"
    assert dict(relation.value_counts) == {
        "Related to": 21_248,
        "Previously reported as": 3_099,
        "Merged with": 2_283,
        "Split from": 1_676,
        "Duplicate of": 410,
        "Related t": 3,
        "Previously report": 1,
    }
    assert sum(count for _, count in relation.value_counts) + relation.null_count == relation.row_count == 28_720
    assert dict(agenda_snapshot.domains["unified-agenda-priority-category"].value_counts)[
        "Section 3(f)(1) Significant"] == 833
    assert "No Stage" not in dict(agenda_snapshot.domains["unified-agenda-rule-stage"].value_counts)
    [source] = agenda_snapshot.sources
    assert source["publisher_url"] is None and source["table"] == "unified_agenda"
    assert source["row_count"] == 241_726 and str(source["bytes_digest"]).startswith("sha256:")


def test_a_list_member_domain_is_observed_over_its_elements(tmp_path, documented):
    import pyarrow as pa
    import pyarrow.parquet as pq

    related = pa.list_(pa.struct([("rin", pa.string()), ("rin_relation", pa.string())]))
    rows = [
        {"related_rins": [{"rin": "1", "rin_relation": "Related to"}, {"rin": "2", "rin_relation": None}]},
        {"related_rins": None},
        {"related_rins": [{"rin": "1", "rin_relation": "Related to"}]},
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([("related_rins", related)])), tmp_path / "unified_agenda.parquet")
    key = "unified-agenda-rin-relation"
    snapshot = observe(tmp_path, {key: documented[key]}, base_url=None, observed_at="t", producer_revision="r")
    domain = snapshot.domains[key]
    assert (domain.value_counts, domain.null_count, domain.row_count) == ((("Related to", 2),), 1, 3)
    assert snapshot.sources[0]["row_count"] == 3 and snapshot.sources[0]["publisher_url"] is None


def test_nulls_occur_only_where_the_xsd_makes_the_element_optional(observed):
    """PRIORITY_CATEGORY, RIN_STATUS and MAJOR are minOccurs="0" in the XSD, so a null there is documented, not drift.

    MAJOR is absent from most of 199510 and 199604 (9,830 of its 9,879 nulls); the rest are single records.
    """

    nulls = {key: domain.null_count for key, domain in observed.items() if domain.null_count and domain.table == "unified_agenda"}
    assert nulls == {
        "unified-agenda-priority-category": 15,
        "unified-agenda-rin-status": 2,
        "unified-agenda-major": 9_879,
    }
    xsd = (DOMAIN_DIR / "reginfo-rin-data-ver10262011.xsd").read_text()
    for element in ("PRIORITY_CATEGORY", "RIN_STATUS", "MAJOR"):
        assert f'<xs:element name="{element}" minOccurs="0">' in xsd


def test_the_snapshot_file_is_small_enough_to_read():
    """A domain snapshot is a summary, not a copy of the table it summarizes."""

    for filename in OBSERVED_SNAPSHOT_FILENAMES:
        assert (DOMAIN_DIR / filename).stat().st_size < 32_768


# --- the tool ---------------------------------------------------------------


def test_the_tool_passes_on_the_committed_inputs():
    result = subprocess.run([sys.executable, str(TOOL)], capture_output=True, text=True, cwd=ROOT)
    assert result.returncode == 0, result.stderr
    assert "15 documented domains" in result.stdout
    assert "37 findings" in result.stdout
    assert "UNRECORDED" not in result.stdout


def test_the_recorded_urls_resolve_through_the_pinned_index(snapshot):
    """Close the loop on the writer: what ``--observe`` recorded must equal what the pinned index fragment resolves.

    The live index is a network fetch, so a fragment of it is checked in beside
    the snapshot. The snapshot's URL is a string in a checked-in data file and
    the fragment is a captured index entry — two origins, so a moved host or an
    advanced generation fails here until the snapshot is re-pinned.
    """

    index = _published_index()
    for source in snapshot.sources:
        key = single_member(index, f"{source['table']}.parquet").path
        assert source["publisher_url"] == f"{FORK_PUBLIC_URL}/{key}"
    urls = table_urls(index, FORK_PUBLIC_URL, sorted({str(one["table"]) for one in snapshot.sources}))
    assert urls["documents"] == (
        f"{FORK_PUBLIC_URL}/generations/documents/"
        "5b9f0a4a28786da6672edee9be79b0a4872e5e629f311640895f0ea6207e2f71/documents.parquet"
    )


def test_the_tool_refuses_to_write_a_snapshot_without_provenance():
    result = subprocess.run(
        [sys.executable, str(TOOL), "--observe", "--data-dir", ".", "--write-snapshot"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 2
    assert "a snapshot states its provenance" in result.stderr


def _repin(manifest_path: Path, key: str, capture_path: Path) -> None:
    """Re-pin one capture's digest and length so a mutation test reaches the parser."""

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    for entry in payload["captures"]:
        if entry["key"] == key:
            body = capture_path.read_bytes()
            entry["bytes_digest"] = "sha256:" + hashlib.sha256(body).hexdigest()
            entry["byte_length"] = len(body)
    manifest_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
