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
import json
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
    domain_findings,
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
#: index: the bucket's r2.dev host, which stays enabled beside the data.spicygov.ai domain.
FORK_PUBLIC_URL = "https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev"

#: The live publication index is a network fetch, so a fragment of it is checked
#: in beside the snapshot for the offline URL assertions.
PUBLISHED_INDEX_FRAGMENT = "published-index-fragment-2026-09-22.json"


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


def test_the_findings_are_the_twelve_the_snapshots_carry(findings):
    """Pinned so a silent change in either half shows up as a number, not a shrug.

    This list deliberately transcribes the ledger a second time. The set-equality
    tests above compare the ledger against the findings, so both pass when someone
    edits the ledger and the snapshot to agree on something wrong. This one states
    the twelve independently of both, and catches that. Kept for that reason.
    """

    identifiers = sorted(finding.identifier for finding in findings)
    assert identifiers == [
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


def test_public_submission_carries_its_row_support(findings):
    """The finding that matters most is a count, not an adjective."""

    (finding,) = [one for one in findings if one.value == "Public Submission"]
    assert finding.kind == UNDOCUMENTED
    assert finding.row_count == 374


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
    assert sorted(captures) == ["reginfo-rin-data-xsd", "regulations-gov-openapi-v4"]
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


# --- the observed snapshot's own provenance ---------------------------------


def test_every_domain_names_a_real_published_column(documented):
    schemas = expected_schemas()
    for domain in documented.values():
        columns = dict(schemas[domain.table])
        column, _, member = domain.column.partition(".")
        assert column in columns, f"{domain.table}.{column} is not a published column"
        if member:
            assert f"{member} VARCHAR" in columns[column] and columns[column].endswith("[]"), columns[column]


def test_the_snapshot_states_where_its_rows_came_from(snapshot):
    assert snapshot.observed_at == "2026-09-22T16:50:25Z"
    assert snapshot.producer_revision == "ae1c4e108bf764b2d15370c11832cfeff11487d1"
    sources = {str(one["table"]): one for one in snapshot.sources}
    assert sorted(sources) == ["dockets", "documents"]  # unified_agenda is observed whole in its own file
    index = _published_index()
    for table, source in sources.items():
        # The recorded URL is a string in a checked-in data file; the expected one
        # resolves through the publisher's own index (checked in as a fragment).
        # Two origins, so this fails when the corpus moves or a generation
        # advances and nobody re-pins the snapshot — exactly how this file first
        # shipped naming a host we never served.
        key = single_member(index, f"{table}.parquet").path
        assert source["publisher_url"] == f"{FORK_PUBLIC_URL}/{key}"
        assert str(source["bytes_digest"]).startswith("sha256:")
        assert int(source["byte_length"]) > 0
    assert sources["documents"]["row_count"] == 2_001_531


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

    nulls = {key: domain.null_count for key, domain in observed.items() if domain.null_count}
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
        assert (DOMAIN_DIR / filename).stat().st_size < 16_384


# --- the tool ---------------------------------------------------------------


def test_the_tool_passes_on_the_committed_inputs():
    result = subprocess.run([sys.executable, str(TOOL)], capture_output=True, text=True, cwd=ROOT)
    assert result.returncode == 0, result.stderr
    assert "7 documented domains" in result.stdout
    assert "12 findings" in result.stdout
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
    assert urls["documents"] == f"{FORK_PUBLIC_URL}/documents.parquet"


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
