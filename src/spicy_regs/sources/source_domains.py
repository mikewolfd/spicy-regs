"""What publishers say a column may hold, and where our data disagrees.

A *source domain* is the controlled value list a publisher documents for one
column of one published table: regulations.gov's ``documentType`` enum, the
Unified Agenda's ``RULE_STAGE`` list, the BILLSTATUS guide's action types, the
LDA constants endpoints, the FEC committee master file's codes. GSA,
reginfo.gov, GPO, the Senate LDA office and the FEC decide those lists. This module holds their half of the claim and the machinery to
hold our data to it.

Why it exists. ``build_federal_register`` and ``build_unified_agenda`` copy the
publisher's strings verbatim, so a column holds whatever the publisher emitted,
and until now nothing compared that against what the publisher said it would
emit. The two directions of disagreement mean different things:

* an **undocumented** value (observed, not documented) says the documented
  enumeration is incomplete. A consumer that switches on the documented list
  silently mishandles those rows.
* an **unobserved** value (documented, not observed) says either the observation
  is bounded — part of a table need not exercise every documented value — or the
  publisher retired a value in silence.

Neither is automatically an error, and neither is automatically fine, so both
land in :data:`ACCEPTED_DOMAIN_FINDINGS`, a ledger closed in both directions:
every finding needs a recorded reason, and a ledger entry the data stops
producing fails as loudly as an unrecorded finding does. That is what keeps the
ledger from becoming the place drift goes to be forgotten.

Nobody transcribes the documented half. Both publisher documents sit in
``sample-data/source-domains/`` as exact bytes, bound to their SHA-256 digest,
byte length, publisher URL and capture time by
``documented-enumeration-capture-manifest-v1.json``, and every documented value
is *parsed out of those bytes* on each run. A hand-typed list rots in silence; a
parse against pinned bytes cannot.

What the digest pin proves, and what it does not. It proves the capture has not
changed since someone pinned it, so no value can be edited into or out of the
publisher's document without the pin failing. It proves nothing about what the
publisher serves today, because nothing here refetches ``source_url`` —
re-pinning a capture is a deliberate manual act, and that act, not this module,
surfaces a change in the publisher's own enumeration. Both halves of the
comparison are therefore checked-in files, and the verdict is a function of the
tree: a lock on a dated finding rather than a live drift detector. The lock is
the useful part, because it fails when someone moves one half and leaves the
ledger behind.

Note what the reginfo.gov XSD does *not* do: it declares every one of these
elements an unrestricted ``xs:string`` and states the controlled list only in an
``xs:documentation`` sentence. The file carries no ``xs:enumeration`` anywhere.
So the "documented" domain for the Unified Agenda is publisher prose, read by a
parser that refuses any sentence it does not recognise.

Labels. Where the publisher states a meaning beside each value (an LDA issue
code's name, an FEC code's description, a BILLSTATUS action code's text), the
domain carries it, and ``spicy-regs-dict generate`` writes it into the
generated dictionary as the column's ``documented_values``. Some lists are
labels only, never a gate (:data:`_LABEL_ONLY_DECLARATIONS`): the publisher
says the list is incomplete, or the off-list share is noise, so a gate would be
a ledger of exceptions that gates nothing. Those are neither observed nor
diffed.

The FEC states its codes only as prose HTML tables. Those pages are pinned
here as exact bytes like every other capture, but they are read through
RefSpec's parser rather than a second one written here: the code lists RefSpec
parsed from the same bytes are vendored at
``spicy_regs/reference/refspec/fec-committee-codes.json``, and each list is
used only when the digest RefSpec recorded for its source equals the pinned
capture's, so the derived list never vouches for itself.

Parsers here are pure — bytes in, values out, no network and no I/O beyond the
manifest, snapshot and vendored-list reads — and every document parse is
memoized on those bytes, because one capture states the domain of several
columns.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from functools import lru_cache
from pathlib import Path
from typing import Any
from xml.etree.ElementTree import Element

import yaml
from spicy_docs.reading.xml import parse_xml
from spicy_docs.sources.congress.billstatus_codes import BillStatusGuideError, read_billstatus_guide

DEFAULT_SOURCE_DOMAIN_DIR = Path("sample-data/source-domains")
CAPTURE_MANIFEST_FILENAME = "documented-enumeration-capture-manifest-v1.json"
OBSERVED_SNAPSHOT_FILENAME = "observed-domain-snapshot-2026-10-30.json"
#: Each dated observation covers its own domains, so one source digest pins every domain of its table, and
#: together the files cover every declaration once. The 2026-10-30 file observes every published table but
#: unified_agenda, read remotely column by column; unified_agenda is observed whole (all 60 retained editions,
#: the five domains including RIN_RELATION) in its own dated file.
OBSERVED_SNAPSHOT_FILENAMES = (
    OBSERVED_SNAPSHOT_FILENAME,
    "observed-domain-snapshot-2026-10-30-unified-agenda.json",
)
#: RefSpec's parse of the FEC HTML captures; see the module docstring.
FEC_CODE_LISTS = Path(__file__).resolve().parents[1] / "reference" / "refspec" / "fec-committee-codes.json"

CAPTURE_MANIFEST_FORMAT_VERSION = "spicyregs-source-domains/capture-v1"
OBSERVED_SNAPSHOT_FORMAT_VERSION = "spicyregs-source-domains/observed-v1"

_XSD_NS = "{http://www.w3.org/2001/XMLSchema}"


class SourceDomainError(ValueError):
    """A capture failed verification, or a publisher document drifted from its captured shape."""


# --- pinned publisher captures ----------------------------------------------


@dataclass(frozen=True)
class DocumentedEnumerationCapture:
    """One exact publisher document that states a controlled value list."""

    key: str
    publisher: str
    states: str
    path: str
    media_type: str
    bytes_digest: str
    byte_length: int
    source_url: str
    observed_at: str
    publisher_revision: str


# A manifest entry states exactly these fields. Deriving the set from the dataclass
# beats repeating it here: a field added to one cannot go missing from the other.
_CAPTURE_FIELDS = frozenset(field.name for field in fields(DocumentedEnumerationCapture))


def load_capture_manifest(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> dict[str, DocumentedEnumerationCapture]:
    """Read the capture manifest, keyed by capture key. The captures stay unread until :func:`read_capture`."""

    path = Path(root) / CAPTURE_MANIFEST_FILENAME
    try:
        manifest = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise SourceDomainError(f"capture manifest {path} is unreadable: {error}") from error
    if not isinstance(manifest, Mapping) or manifest.get("format_version") != CAPTURE_MANIFEST_FORMAT_VERSION:
        raise SourceDomainError(f"capture manifest {path} is not {CAPTURE_MANIFEST_FORMAT_VERSION}")
    captures: dict[str, DocumentedEnumerationCapture] = {}
    for entry in manifest.get("captures") or ():
        if not isinstance(entry, Mapping) or set(entry) != _CAPTURE_FIELDS:
            raise SourceDomainError(f"capture manifest {path} carries a drifted capture entry")
        key = str(entry["key"])
        if key in captures:
            raise SourceDomainError(f"capture manifest {path} repeats capture key {key!r}")
        captures[key] = DocumentedEnumerationCapture(
            key=key,
            publisher=str(entry["publisher"]),
            states=str(entry["states"]),
            path=str(entry["path"]),
            media_type=str(entry["media_type"]),
            bytes_digest=str(entry["bytes_digest"]),
            byte_length=int(entry["byte_length"]),
            source_url=str(entry["source_url"]),
            observed_at=str(entry["observed_at"]),
            publisher_revision=str(entry["publisher_revision"]),
        )
    if not captures:
        raise SourceDomainError(f"capture manifest {path} names no captures")
    return captures


def read_capture(capture: DocumentedEnumerationCapture, *, root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> bytes:
    """Return the capture's bytes, refusing any file whose digest or length differs."""

    path = Path(root) / capture.path
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise SourceDomainError(f"capture {capture.key} is missing at {path}: {error}") from error
    if len(payload) != capture.byte_length:
        raise SourceDomainError(f"capture {capture.key} is {len(payload)} bytes, pinned at {capture.byte_length}")
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    if digest != capture.bytes_digest:
        raise SourceDomainError(f"capture {capture.key} hashes to {digest}, pinned at {capture.bytes_digest}")
    return payload


# --- parsing the regulations.gov OpenAPI document ---------------------------


# Memoized on the exact bytes, so a changed capture is a different cache entry and
# never a stale hit. Parsing this 60 KB document costs 35 ms — 97% of the module's
# runtime — and one capture states the domain of several columns, so parsing it once
# per capture rather than once per column is the whole cost of building the register.
@lru_cache(maxsize=4)
def _parse_yaml(payload: bytes) -> Mapping[str, Any]:
    try:
        document = yaml.safe_load(payload)
    except yaml.YAMLError as error:
        raise SourceDomainError(f"openapi capture is not well-formed YAML: {error}") from error
    if not isinstance(document, Mapping):
        raise SourceDomainError("openapi capture is not a YAML mapping")
    return document


def openapi_schema_enum(payload: bytes, schema_name: str) -> tuple[str, ...]:
    """Return the ``enum`` members of one ``components.schemas`` entry, in source order.

    ``yaml.safe_load`` constructs no arbitrary Python objects, the only loader
    worth pointing at a fetched document. It also settles the capture's one
    whitespace quirk unaided: ``- Nonrulemaking `` is a plain scalar, and a
    plain scalar drops its trailing space. Nothing else is normalized —
    ``Supporting & Related Material`` keeps the literal ampersand the API
    returns.
    """

    document = _parse_yaml(payload)
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, Mapping) else None
    if not isinstance(schemas, Mapping) or schema_name not in schemas:
        raise SourceDomainError(f"openapi capture declares no components.schemas.{schema_name}")
    schema = schemas[schema_name]
    members = schema.get("enum") if isinstance(schema, Mapping) else None
    if not isinstance(members, list) or not members:
        raise SourceDomainError(f"openapi schema {schema_name!r} states no enum member list")
    for member in members:
        if not isinstance(member, str):
            raise SourceDomainError(f"openapi schema {schema_name!r} enum carries a non-string member {member!r}")
    if len(set(members)) != len(members):
        raise SourceDomainError(f"openapi schema {schema_name!r} repeats an enum member")
    return tuple(members)


# --- parsing the reginfo.gov Unified Agenda XSD -----------------------------

# Every controlled Unified Agenda field is declared ``<xs:restriction
# base="xs:string"/>`` with no facets; its value list lives only in the element's
# xs:documentation sentence. The values are double-quoted, which is what keeps
# "Substantive, Nonsignificant" one value despite its comma.
_XSD_OPTIONS_PREFIX = re.compile(r"^One of the following options:\s*")
_XSD_QUOTED_OPTION = re.compile(r'"([^"]+)"')

#: The pinned capture is 22,730 bytes; a document forty times that is not the one pinned.
_MAX_XSD_BYTES = 1024 * 1024


# Memoized like the YAML parse above and for the same reason: four documented
# domains come out of this one document. Callers only read the tree. spicy-docs'
# reader refuses any DOCTYPE, where entity-expansion and XXE payloads live, and
# every entity declaration or reference; this publisher emits none.
@lru_cache(maxsize=4)
def _parse_xsd(payload: bytes) -> Element:
    return parse_xml(payload, max_bytes=_MAX_XSD_BYTES, error_type=SourceDomainError, label="reginfo XSD")


def xsd_documented_options(payload: bytes, element_name: str) -> tuple[tuple[str, ...], int]:
    """Return one element's documented options and the raw count its sentence lists.

    The sentence is publisher prose, and this publisher's prose repeats itself:
    ``PRIORITY_CATEGORY`` lists ``Not Major`` twice. A literal duplicate folds
    into one value — inventing a second meaning for it would be worse — and the
    raw count comes back alongside, so a caller can pin both numbers.
    """

    root = _parse_xsd(payload)
    matches = [element for element in root.iter(f"{_XSD_NS}element") if element.get("name") == element_name]
    if len(matches) != 1:
        raise SourceDomainError(f"reginfo XSD declares element {element_name!r} {len(matches)} times, expected once")
    documentation = matches[0].find(f"{_XSD_NS}annotation/{_XSD_NS}documentation")
    if documentation is None or not (documentation.text or "").strip():
        raise SourceDomainError(f"reginfo XSD element {element_name!r} carries no documentation string")
    sentence = (documentation.text or "").strip()
    if _XSD_OPTIONS_PREFIX.match(sentence) is None:
        raise SourceDomainError(f"reginfo XSD element {element_name!r} does not state an option list: {sentence!r}")
    raw = [value.strip() for value in _XSD_QUOTED_OPTION.findall(sentence)]
    if not raw:
        raise SourceDomainError(f"reginfo XSD element {element_name!r} quotes no options")
    folded: list[str] = []
    for value in raw:
        if value not in folded:
            folded.append(value)
    return tuple(folded), len(raw)


# --- parsing the BILLSTATUS user guide, the LDA constants and the FEC lists ---


@lru_cache(maxsize=2)
def _parse_guide(payload: bytes):
    try:
        return read_billstatus_guide(payload)
    except BillStatusGuideError as error:
        raise SourceDomainError(f"BILLSTATUS guide capture does not read: {error}") from error


def guide_table_values(payload: bytes, section: str) -> tuple[tuple[str, ...], dict[str, str]]:
    """One numbered guide table's first-column values and, for a two-column table, their stated text.

    spicy-docs' reader keeps the guide's literal cells; a table that states only
    the value (section 4, action types) carries no label, and none is invented.
    """

    tables = [table for table in _parse_guide(payload).tables if table.section_number == section]
    if len(tables) != 1:
        raise SourceDomainError(f"BILLSTATUS guide states section {section} {len(tables)} times, expected once")
    (table,) = tables
    values = tuple(row.cells[0].value for row in table.rows)
    if len(set(values)) != len(values) or not all(values):
        raise SourceDomainError(f"BILLSTATUS guide section {section} repeats or blanks a value")
    labels = {row.cells[0].value: row.cells[-1].value for row in table.rows} if len(table.header.cells) == 2 else {}
    return values, labels


def lda_constant_values(payload: bytes) -> tuple[tuple[str, ...], dict[str, str]]:
    """An LDA constants response: a JSON list of exactly ``{"value", "name"}`` objects, in publisher order."""

    try:
        items = json.loads(payload)
    except ValueError as error:
        raise SourceDomainError(f"LDA constants capture is not JSON: {error}") from error
    if not isinstance(items, list) or not items:
        raise SourceDomainError("LDA constants capture is not a nonempty JSON list")
    for item in items:
        if not isinstance(item, Mapping) or set(item) != {"value", "name"} or not all(
            isinstance(item[field], str) and item[field] for field in ("value", "name")
        ):
            raise SourceDomainError(f"LDA constants capture carries a drifted item {item!r}")
    values = tuple(item["value"] for item in items)
    if len(set(values)) != len(values):
        raise SourceDomainError("LDA constants capture repeats a value")
    return values, {item["value"]: item["name"] for item in items}


def fec_code_values(
    capture: DocumentedEnumerationCapture, resource: str, *, lists: Path = FEC_CODE_LISTS
) -> tuple[tuple[str, ...], dict[str, str]]:
    """RefSpec's parse of one FEC code list, accepted only from the exact capture pinned here."""

    try:
        vendored = json.loads(lists.read_bytes())
        entry = vendored["resources"][resource]
        codes = entry["codes"]
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SourceDomainError(f"vendored FEC code lists {lists} do not state {resource!r}: {error}") from error
    if (entry.get("source_sha256"), entry.get("source_byte_length")) != (capture.bytes_digest, capture.byte_length):
        raise SourceDomainError(
            f"vendored FEC list {resource!r} was parsed from {entry.get('source_sha256')}, "
            f"not the pinned capture {capture.key} ({capture.bytes_digest})"
        )
    if not isinstance(codes, Mapping) or not codes:
        raise SourceDomainError(f"vendored FEC list {resource!r} states no codes")
    return tuple(codes), dict(codes)


# --- the register -----------------------------------------------------------


@dataclass(frozen=True)
class DocumentedDomain:
    """One published column, the values its publisher documents, and where it says so.

    ``column`` names a scalar column, or ``list_column.member`` for one member of a list-of-struct column,
    observed over every list element.
    """

    key: str
    table: str
    column: str
    capture_key: str
    locator: str
    values: tuple[str, ...]
    raw_option_count: int
    #: The publisher's own text beside each value; empty when it states the values alone.
    labels: Mapping[str, str]
    #: What a reader must know before trusting a label, in the publisher's words; ``None`` when nothing.
    caveat: str | None
    source_url: str


@dataclass(frozen=True)
class _DomainDeclaration:
    key: str
    table: str
    column: str
    capture_key: str
    locator: str
    element: str
    expected_values: int
    expected_raw_options: int
    caveat: str | None = None


# The gated domains: each is observed, diffed both ways, and held to the ledger.
# What is absent, and why — so a reader takes the set for a decision and knows
# which hole is not one:
#
# * ``TTBL_ACTION`` (XSD L441-448) is a decided exclusion. The publisher's own
#   data treats the field as free text — the 2026-08-03 observation's
#   ``timetable_json`` held 1,139 distinct actions over 10,533 entries against
#   34 documented ones — so checking against its list would report a thousand
#   findings and gate nothing. Its sentence also opens "One of the following:"
#   with unquoted values, which :func:`xsd_documented_options` could not read
#   anyway: the exclusion and the parser's reach coincide here, and only the
#   first of the two is a reason.
# * ``federal_register.document_type`` is absent because no pinned publisher
#   document states its list. The FR API documentation page is not captured
#   here, and a domain nobody published is not a documented domain.
# * ``comments.category`` is not the OpenAPI ``SubmitterType`` list
#   (Individual, Organization, Anonymous) the 2026-09-22 note assumed: measured
#   2026-10-30 it holds agency-specific submitter categories ("Individual
#   Consumer", "Drug Industry - C0022", ...) on 1.86M of 26.4M rows, which the
#   API calls agency-specific. No publisher list documents it, so it is not a
#   documented domain.
# * ``comments.document_type`` is checked against the same ``DocumentType``
#   list as ``documents``; see ``regulations-gov-comment-document-type``.
# * ``bill_actions.action_code`` and FEC ``party`` are labels only
#   (:data:`_LABEL_ONLY_DECLARATIONS`), for the reasons stated there.
# * ``origin_chamber`` and ``bill_committees.chamber`` have no publisher list:
#   the BILLSTATUS guide's chamber column belongs to its summary-version table.
# * LDA covered government entities: no capture states their list (live data
#   holds 255 distinct ids), so there is nothing to check them against.
_DECLARATIONS: tuple[_DomainDeclaration, ...] = (
    _DomainDeclaration(
        key="regulations-gov-document-type",
        table="documents",
        column="document_type",
        capture_key="regulations-gov-openapi-v4",
        locator="components.schemas.DocumentType.enum (lines 893-898)",
        element="DocumentType",
        expected_values=5,
        expected_raw_options=5,
    ),
    _DomainDeclaration(
        key="regulations-gov-docket-type",
        table="dockets",
        column="docket_type",
        capture_key="regulations-gov-openapi-v4",
        locator="components.schemas.DocketType.enum (lines 902-904)",
        element="DocketType",
        expected_values=2,
        expected_raw_options=2,
    ),
    _DomainDeclaration(
        key="unified-agenda-rule-stage",
        table="unified_agenda",
        column="rule_stage",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/RULE_STAGE xs:documentation (line 66)",
        element="RULE_STAGE",
        expected_values=6,
        expected_raw_options=6,
    ),
    _DomainDeclaration(
        key="unified-agenda-priority-category",
        table="unified_agenda",
        column="priority_category",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/PRIORITY_CATEGORY xs:documentation (line 50)",
        element="PRIORITY_CATEGORY",
        expected_values=6,
        expected_raw_options=7,
    ),
    _DomainDeclaration(
        key="unified-agenda-rin-status",
        table="unified_agenda",
        column="rin_status",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/RIN_STATUS xs:documentation (line 58)",
        element="RIN_STATUS",
        expected_values=2,
        expected_raw_options=2,
    ),
    _DomainDeclaration(
        key="unified-agenda-major",
        table="unified_agenda",
        column="major",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/MAJOR xs:documentation (line 74)",
        element="MAJOR",
        expected_values=3,
        expected_raw_options=3,
    ),
    _DomainDeclaration(
        key="unified-agenda-rin-relation",
        table="unified_agenda",
        column="related_rins.rin_relation",
        capture_key="reginfo-rin-data-xsd",
        locator="RELATED_RINType/RIN_RELATION xs:documentation (line 526)",
        element="RIN_RELATION",
        expected_values=5,
        expected_raw_options=5,
    ),
    _DomainDeclaration(
        key="regulations-gov-comment-document-type",
        table="comments",
        column="document_type",
        capture_key="regulations-gov-openapi-v4",
        locator="components.schemas.DocumentType.enum (lines 893-898)",
        element="DocumentType",
        expected_values=5,
        expected_raw_options=5,
    ),
    _DomainDeclaration(
        key="billstatus-action-type",
        table="bill_actions",
        column="action_type",
        capture_key="billstatus-user-guide",
        locator="section 4, Actions Type Element Possible Values (lines 498-532)",
        element="4",
        expected_values=26,
        expected_raw_options=26,
    ),
    _DomainDeclaration(
        key="lda-general-issue-code",
        table="lobbying_activities",
        column="general_issue_code",
        capture_key="lda-general-issue-codes",
        locator="constants/filing/lobbyingactivityissues/ (value, name)",
        element="value",
        expected_values=79,
        expected_raw_options=79,
    ),
    _DomainDeclaration(
        key="lda-filing-type",
        table="lobbying_filings",
        column="filing_type",
        capture_key="lda-filing-types",
        locator="constants/filing/filingtypes/ (value, name)",
        element="value",
        expected_values=50,
        expected_raw_options=50,
    ),
    _DomainDeclaration(
        key="fec-committee-type",
        table="fec_committee_history",
        column="committee_type",
        capture_key="fec-committee-type-codes",
        locator="committee type code table, read by RefSpec parse_committee_type_codes",
        element="committeeType",
        expected_values=16,
        expected_raw_options=16,
    ),
    _DomainDeclaration(
        key="fec-committee-designation",
        table="fec_committee_history",
        column="designation",
        capture_key="fec-committee-master-file",
        locator="CMTE_DSGN row, read by RefSpec parse_committee_designation_codes",
        element="committeeDesignation",
        expected_values=6,
        expected_raw_options=6,
    ),
    _DomainDeclaration(
        key="fec-filing-frequency",
        table="fec_committee_history",
        column="filing_frequency",
        capture_key="fec-committee-master-file",
        locator="CMTE_FILING_FREQ row, read by RefSpec parse_filing_frequency_codes",
        element="filingFrequency",
        expected_values=6,
        expected_raw_options=6,
    ),
    _DomainDeclaration(
        key="fec-organization-type",
        table="fec_committee_history",
        column="organization_type",
        capture_key="fec-committee-master-file",
        locator="ORG_TP row, read by RefSpec parse_organization_type_codes",
        element="organizationType",
        expected_values=6,
        expected_raw_options=6,
    ),
)

#: The guide's own words on its action-code table (lines 453-455), carried beside every label.
_ACTION_CODE_CAVEAT = (
    "The publisher calls this list representational and 'provided as a courtesy; a complete, authoritative list of "
    "action codes does not exist. In general, expect action codes when source system is 2 (House floor) or 9 "
    "(Library of Congress).' It adds that 'some codes may have been used and re-used over the years for different "
    "purposes', so a label is the guide's current text for the code, not a statement of what an older action meant."
)

# Labels only: never observed, never diffed, never in the ledger. A list belongs
# here only when its publisher says it is incomplete or the off-list share is
# noise, because then a gate is a ledger of exceptions that gates nothing (the
# TTBL_ACTION argument above).
#
# * ``bill_actions.action_code``: the guide says no complete list exists
#   (line 453). Measured 2026-10-30 on bill family f719442c: 164 distinct codes,
#   32 of the guide's 36 among them, covering 529,762 of 694,998 coded rows.
#   spicy-docs ``interpretation/bill_actions.py`` already refuses to read this
#   list as a vocabulary for the same reason.
# * FEC ``party`` on both history tables: 141 distinct off-list values on
#   fec_committee_history (857 of 298,596 rows, 0.29%) and 139 on
#   fec_candidate_history (393 of 130,885): case variants (Rep, Dem, dem), GOP,
#   NAT and junk ('  0', '.'). Folding case is an owner decision, not a gate.
_LABEL_ONLY_DECLARATIONS: tuple[_DomainDeclaration, ...] = (
    _DomainDeclaration(
        key="billstatus-action-code",
        table="bill_actions",
        column="action_code",
        capture_key="billstatus-user-guide",
        locator="section 3, Action Code Element Possible Values (lines 449-497)",
        element="3",
        expected_values=36,
        expected_raw_options=36,
        caveat=_ACTION_CODE_CAVEAT,
    ),
    _DomainDeclaration(
        key="fec-committee-party",
        table="fec_committee_history",
        column="party",
        capture_key="fec-party-codes",
        locator="party code table, read by RefSpec parse_party_codes",
        element="party",
        expected_values=95,
        expected_raw_options=95,
    ),
    _DomainDeclaration(
        key="fec-candidate-party",
        table="fec_candidate_history",
        column="party",
        capture_key="fec-party-codes",
        locator="party code table, read by RefSpec parse_party_codes",
        element="party",
        expected_values=95,
        expected_raw_options=95,
    ),
)

DOMAIN_KEYS: tuple[str, ...] = tuple(declaration.key for declaration in _DECLARATIONS)


def documented_domains(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> dict[str, DocumentedDomain]:
    """Parse every gated domain out of the pinned publisher captures.

    Counts are pinned per domain: a publisher that adds or drops a documented
    value changes the capture, which changes its digest, which this refuses to
    read until someone pins the new capture deliberately.
    """

    return _parse_declarations(_DECLARATIONS, root)


def label_only_domains(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> dict[str, DocumentedDomain]:
    """Parse the labels-only domains: the dictionary shows them, and nothing gates on them."""

    return _parse_declarations(_LABEL_ONLY_DECLARATIONS, root)


def _parse_declarations(
    declarations: Sequence[_DomainDeclaration], root: Path | str
) -> dict[str, DocumentedDomain]:
    captures = load_capture_manifest(root)
    payloads: dict[str, bytes] = {}
    domains: dict[str, DocumentedDomain] = {}
    for declaration in declarations:
        capture = captures.get(declaration.capture_key)
        if capture is None:
            raise SourceDomainError(f"domain {declaration.key} names unpinned capture {declaration.capture_key!r}")
        if capture.key not in payloads:
            payloads[capture.key] = read_capture(capture, root=root)
        payload = payloads[capture.key]
        labels: dict[str, str] = {}
        if capture.media_type == "application/yaml":
            values = openapi_schema_enum(payload, declaration.element)
            raw_options = len(values)
        elif capture.media_type == "application/xml":
            values, raw_options = xsd_documented_options(payload, declaration.element)
        else:
            if capture.media_type == "text/markdown":
                values, labels = guide_table_values(payload, declaration.element)
            elif capture.media_type == "application/json":
                values, labels = lda_constant_values(payload)
            elif capture.media_type == "text/html":
                values, labels = fec_code_values(capture, declaration.element)
            else:
                raise SourceDomainError(f"capture {capture.key} has unsupported media type {capture.media_type!r}")
            raw_options = len(values)
        if len(values) != declaration.expected_values or raw_options != declaration.expected_raw_options:
            raise SourceDomainError(
                f"domain {declaration.key} parses {len(values)} values from {raw_options} options, "
                f"pinned at {declaration.expected_values} from {declaration.expected_raw_options}"
            )
        domains[declaration.key] = DocumentedDomain(
            key=declaration.key,
            table=declaration.table,
            column=declaration.column,
            capture_key=capture.key,
            locator=f"{capture.path} {declaration.locator}",
            values=values,
            raw_option_count=raw_options,
            labels=labels,
            caveat=declaration.caveat,
            source_url=capture.source_url,
        )
    return domains


def documented_values(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> dict[str, dict[str, dict[str, Any]]]:
    """Per table and column, the publisher's documented values for the generated dictionary.

    ``checked`` says whether ``check_source_domain_drift.py`` holds the column
    to the list (a dated lock re-pinned by hand, not a live check) or whether
    the list is labels only. ``values`` keep the publisher's order.
    """

    result: dict[str, dict[str, dict[str, Any]]] = {}
    for checked, domains in ((True, documented_domains(root)), (False, label_only_domains(root))):
        for domain in domains.values():
            entry: dict[str, Any] = {
                "domain": domain.key,
                "checked": checked,
                "source_url": domain.source_url,
                "locator": domain.locator,
                **({"caveat": domain.caveat} if domain.caveat else {}),
                "values": [
                    {"value": value, **({"label": domain.labels[value]} if value in domain.labels else {})}
                    for value in domain.values
                ],
            }
            result.setdefault(domain.table, {})[domain.column] = entry
    return result


# --- the observed half ------------------------------------------------------


@dataclass(frozen=True)
class ObservedDomain:
    """The distinct values one published column actually carries, with row support.

    For a ``list_column.member`` domain the counts and ``row_count`` are list elements, not table rows.
    """

    key: str
    table: str
    column: str
    value_counts: tuple[tuple[str, int], ...]
    null_count: int
    row_count: int


@dataclass(frozen=True)
class ObservedSnapshot:
    """One dated observation of every domain column, and the tables it came from."""

    observed_at: str
    producer_revision: str
    sources: tuple[Mapping[str, object], ...]
    domains: dict[str, ObservedDomain]


def load_observed_snapshot(
    root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR,
    *,
    filename: str = OBSERVED_SNAPSHOT_FILENAME,
) -> ObservedSnapshot:
    """Read the checked-in observed snapshot."""

    path = Path(root) / filename
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise SourceDomainError(f"observed snapshot {path} is unreadable: {error}") from error
    if not isinstance(payload, Mapping) or payload.get("format_version") != OBSERVED_SNAPSHOT_FORMAT_VERSION:
        raise SourceDomainError(f"observed snapshot {path} is not {OBSERVED_SNAPSHOT_FORMAT_VERSION}")
    domains: dict[str, ObservedDomain] = {}
    for entry in payload.get("domains") or ():
        key = str(entry["key"])
        if key in domains:
            raise SourceDomainError(f"observed snapshot {path} repeats domain key {key!r}")
        domains[key] = ObservedDomain(
            key=key,
            table=str(entry["table"]),
            column=str(entry["column"]),
            value_counts=tuple((str(value), int(count)) for value, count in entry["value_counts"]),
            null_count=int(entry["null_count"]),
            row_count=int(entry["row_count"]),
        )
    if not domains:
        raise SourceDomainError(f"observed snapshot {path} observes no domains")
    return ObservedSnapshot(
        observed_at=str(payload["observed_at"]),
        producer_revision=str(payload["producer_revision"]),
        sources=tuple(payload.get("sources") or ()),
        domains=domains,
    )


def load_observed_snapshots(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> tuple[ObservedSnapshot, ...]:
    """Every checked-in dated observation, in :data:`OBSERVED_SNAPSHOT_FILENAMES` order."""

    return tuple(load_observed_snapshot(root, filename=filename) for filename in OBSERVED_SNAPSHOT_FILENAMES)


def merged_domains(snapshots: Sequence[ObservedSnapshot]) -> dict[str, ObservedDomain]:
    """The snapshots' domains as one register, refusing a domain two snapshots both observe."""

    merged: dict[str, ObservedDomain] = {}
    for snapshot in snapshots:
        for key, domain in snapshot.domains.items():
            if key in merged:
                raise SourceDomainError(f"domain {key!r} is observed by more than one snapshot")
            merged[key] = domain
    return merged


# --- the diff ---------------------------------------------------------------

UNDOCUMENTED = "undocumented-value"
UNOBSERVED = "unobserved-value"
FINDING_KINDS = (UNDOCUMENTED, UNOBSERVED)


@dataclass(frozen=True)
class DomainFinding:
    """One value that the publisher's list and our data disagree about."""

    domain_key: str
    kind: str
    value: str
    row_count: int | None

    @property
    def identifier(self) -> str:
        return f"{self.domain_key}/{self.kind}/{self.value}"


def domain_findings(
    documented: Mapping[str, DocumentedDomain],
    observed: Mapping[str, ObservedDomain],
) -> tuple[DomainFinding, ...]:
    """Diff each documented domain against its observation, in both directions."""

    if set(documented) != set(observed):
        missing = sorted(set(documented) - set(observed))
        extra = sorted(set(observed) - set(documented))
        raise SourceDomainError(f"domain coverage differs; unobserved={missing} undeclared={extra}")
    findings: list[DomainFinding] = []
    for key in sorted(documented):
        declared = documented[key]
        seen = observed[key]
        if (seen.table, seen.column) != (declared.table, declared.column):
            raise SourceDomainError(
                f"domain {key} is declared on {declared.table}.{declared.column} "
                f"but observed on {seen.table}.{seen.column}"
            )
        counts = dict(seen.value_counts)
        for value, count in seen.value_counts:
            if value not in declared.values:
                findings.append(DomainFinding(key, UNDOCUMENTED, value, count))
        for value in declared.values:
            if value not in counts:
                findings.append(DomainFinding(key, UNOBSERVED, value, None))
    return tuple(findings)


# --- the closed ledger of accepted findings ---------------------------------


@dataclass(frozen=True)
class AcceptedFinding:
    """One recorded finding: what it is, and why it is a finding rather than an error."""

    domain_key: str
    kind: str
    value: str
    reason: str


#: Why each listed action type no action carries is breadth, not drift; one reason for all 17.
_GUIDE_ACTION_TYPE_UNOBSERVED = (
    "The guide's section 4 lists 26 type strings: older descriptive ones (Chamber Actions, House Actions, "
    "Introduced in Senate, ...) beside Congress.gov's condensed ones (IntroReferral, Floor, Committee, ...). The "
    "2026-10-30 observation of bill family f719442c carries 10 types, 9 of them listed and all condensed; a listed "
    "type no action in this corpus carries is the list's breadth, not a retired value."
)
_UNOBSERVED_ACTION_TYPES = (
    "Actions by the President",
    "Actions Related to President",
    "Actions Related to Veto",
    "Amending Actions",
    "Chamber Actions",
    "Chamber Consideration Actions",
    "Committee Actions",
    "Conference Actions",
    "ForwardY-N",
    "House Actions",
    "Introduced in House",
    "Introduced in Senate",
    "Procedural",
    "Senate Actions",
    "Significant Actions",
    "Unknown",
    "VARIES",
)

# Every finding the checked-in snapshots produce, each with the reason it stands
# recorded rather than fixed. The ledger is closed in both directions: an
# unrecorded finding fails the gate, and so does a recorded finding the data no
# longer produces, because an exception nothing exercises is a claim nobody
# checked. The captures and their pins live in sample-data/source-domains/.
ACCEPTED_DOMAIN_FINDINGS: tuple[AcceptedFinding, ...] = (
    AcceptedFinding(
        domain_key="regulations-gov-document-type",
        kind=UNDOCUMENTED,
        value="Public Submission",
        reason=(
            "regulations.gov returns this documentType on 374 document rows and labels it in its own web UI, "
            "but the pinned v4 OpenAPI DocumentType enum lists only five values and does not include it. "
            "The publisher's documentation is incomplete; the data is not wrong."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNDOCUMENTED,
        value="First Time Published in The Unified Agenda",
        reason=(
            "Title-cased in the data, sentence-cased in the schema documentation "
            '("First time published in the Unified Agenda"). Same value, different bytes: an exact-match '
            "consumer built from the documentation matches no row at all."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNDOCUMENTED,
        value="Previously Published in The Unified Agenda",
        reason=(
            "The other half of the same casing drift; the schema documents "
            '"Previously published in the Unified Agenda".'
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNOBSERVED,
        value="First time published in the Unified Agenda",
        reason="The sentence-cased documented spelling occurs on no row; the data carries the title-cased form.",
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNOBSERVED,
        value="Previously published in the Unified Agenda",
        reason="The sentence-cased documented spelling occurs on no row; the data carries the title-cased form.",
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rule-stage",
        kind=UNOBSERVED,
        value="No Stage",
        reason=(
            "Documented, yet no row of any of the 60 retained editions (199510-202510, 241,726 rows) carries it: "
            "the publisher lists a stage its exports never use. Kept in the documented domain because the "
            "pinned XSD still states it."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-priority-category",
        kind=UNOBSERVED,
        value="Not Major",
        reason=(
            "Documented, yet no row of the 60 retained editions carries it, like No Stage. The XSD "
            "documentation also lists this value twice, which is the publisher's own typo rather than two "
            "categories."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-priority-category",
        kind=UNDOCUMENTED,
        value="Section 3(f)(1) Significant",
        reason=(
            "EO 14094 (April 2023) renamed the EO 12866 section 3(f)(1) category, and editions 202304-202410 "
            "carry it on 833 rows; the pinned XSD (version 10262011) predates the order. 202504 and later "
            "return to Economically Significant after EO 14148 revoked 14094."
        ),
    ),
    # RIN_RELATION, observed over all 28,720 related RINs of the 60 retained editions (owner Q15: published as
    # written). The four findings are the action list's C11.
    AcceptedFinding(
        domain_key="unified-agenda-rin-relation",
        kind=UNDOCUMENTED,
        value="Merged with",
        reason=(
            'The XSD documents "Merge with"; the data writes "Merged with" on 2,283 related RINs. Same '
            "relation, different bytes: an exact-match consumer built from the documentation matches none."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-relation",
        kind=UNOBSERVED,
        value="Merge with",
        reason="The documented spelling occurs on no related RIN in 60 editions; the data carries \"Merged with\".",
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-relation",
        kind=UNDOCUMENTED,
        value="Related t",
        reason=(
            'A publisher cut-off of "Related to": 1125-AB08 in 202004, 202010 and 202104, the same entry that '
            'states the related RIN " 1615-NYD". Kept as written (owner Q15), never repaired to a guess.'
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-relation",
        kind=UNDOCUMENTED,
        value="Previously report",
        reason=(
            'A publisher cut-off of "Previously reported as": 3150-AH33 in 200310, beside the malformed '
            'related RIN "d as AF61". Kept as written (owner Q15).'
        ),
    ),
    AcceptedFinding(
        domain_key="regulations-gov-comment-document-type",
        kind=UNDOCUMENTED,
        value="Public Submission",
        reason=(
            "The documentType of a comment itself (26,417,470 of 26,418,078 comments rows on 2026-10-30), which "
            "regulations.gov returns and labels but its v4 OpenAPI DocumentType enum omits; the same gap as on "
            "documents."
        ),
    ),
    *(
        AcceptedFinding(
            domain_key="regulations-gov-comment-document-type",
            kind=UNOBSERVED,
            value=value,
            reason=(
                "comments holds what was submitted to a docket: Public Submission plus 608 rows typed Other or "
                "Supporting & Related Material. An agency's own notices and rules are documents rows, not comments."
            ),
        )
        for value in ("Notice", "Rule", "Proposed Rule")
    ),
    AcceptedFinding(
        domain_key="billstatus-action-type",
        kind=UNDOCUMENTED,
        value="Veto",
        reason=(
            "212 actions (veto messages laid before the House or received in the Senate, 2007-2026) carry type "
            "Veto, which the guide's section 4 omits; it lists only 'Actions Related to Veto', which no row "
            "carries. Congress.gov emits the type; the guide's list is behind it."
        ),
    ),
    *(
        AcceptedFinding(domain_key="billstatus-action-type", kind=UNOBSERVED, value=value,
                        reason=_GUIDE_ACTION_TYPE_UNOBSERVED)
        for value in _UNOBSERVED_ACTION_TYPES
    ),
    AcceptedFinding(
        domain_key="fec-filing-frequency",
        kind=UNOBSERVED,
        value="W",
        reason=(
            "W (Waived) is documented on the committee master file page but carried by no fec_committee_history "
            "row on 2026-10-30; the FEC states no date for the code, so it may be historical or rare."
        ),
    ),
    *(
        AcceptedFinding(
            domain_key="fec-organization-type",
            kind=UNDOCUMENTED,
            value=value,
            reason=(
                f"{rows} committee-cycle rows ({cycles}, all designation U) carry ORG_TP {value}, which the "
                "2026-08-03 committee master file description does not list. Retained as the FEC states it; "
                "its meaning is undocumented."
            ),
        )
        for value, rows, cycles in (("H", 26, "cycles 2014-2020"), ("I", 20, "cycles 2014-2026"))
    ),
)


def unrecorded_findings(findings: Sequence[DomainFinding]) -> tuple[DomainFinding, ...]:
    """Findings missing from the ledger — the gate's failure condition."""

    recorded = {(entry.domain_key, entry.kind, entry.value) for entry in ACCEPTED_DOMAIN_FINDINGS}
    return tuple(finding for finding in findings if (finding.domain_key, finding.kind, finding.value) not in recorded)


def stale_accepted_findings(findings: Sequence[DomainFinding]) -> tuple[AcceptedFinding, ...]:
    """Ledger entries the data no longer produces — the other half of the gate."""

    observed = {(finding.domain_key, finding.kind, finding.value) for finding in findings}
    return tuple(
        entry for entry in ACCEPTED_DOMAIN_FINDINGS if (entry.domain_key, entry.kind, entry.value) not in observed
    )
