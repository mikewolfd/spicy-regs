"""Read native metadata from bounded, checksum-verified original source bytes.

The caller owns the byte limit and checksum verification. This module reads
primary structured records and exact Federal Register issue titles, never linked
subjects or arbitrary dates in prose. Results are
rebuildable observations, not evidence of dataset completeness. XML external DTD
names are accepted without fetching them; entities and internal DTDs are refused.
"""

from __future__ import annotations

import json
import html
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Never
from xml.etree.ElementTree import Element, TreeBuilder
from xml.parsers import expat

SUPPORTED_DATASETS = frozenset(f"family:{name}" for name in (
    "comments-mirror", "lobbying-filings", "bill-family", "roll-call-votes",
    "house-communications", "federal-register", "committee-meetings",
))
_SCOPE = "Observed primary native records only; no completeness or acquisition-date inference."
_BILL_TYPES = {"hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"}
_MODS = "{http://www.loc.gov/mods/v3}"
_MAX_RECORDS = 10_000
_REGISTER_TITLE = re.compile(
    r"^Federal Register,\s*Volume\s+\d+\s+Issue\s+\d+\s+"
    r"\((Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})\)$"
)


def reader_inputs(dataset_id: str) -> tuple[Path, ...]:
    """Interpretation dependencies used by the catalog's source-owned reader."""
    paths = [Path(__file__)]
    if dataset_id == 'family:federal-register':
        from spicy_docs.reading import xml_tree
        from spicy_docs.sources.federal_register import issues, native
        from spicy_docs.sources.govinfo import mods
        paths.extend(Path(module.__file__) for module in (native, issues, mods, xml_tree))
    return tuple(paths)


def _register_html(raw: bytes) -> dict:
    """Read the issue date only from the publisher's exact HTML title shape."""
    title = re.search(br"<title(?:\s[^>]*)?>\s*([^<]{1,512})\s*</title>", raw[:65536], re.I)
    if not title:
        return _result("unsupported")
    try:
        match = _REGISTER_TITLE.fullmatch(html.unescape(title.group(1).decode("utf-8").strip()))
        if not match:
            return _result("unsupported")
        months = {name: number for number, name in enumerate(("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"), 1)}
        weekdays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
        published = date(int(match[4]), months[match[2]], int(match[3]))
        if weekdays[published.weekday()] != match[1]:
            return _result("unsupported")
    except (UnicodeDecodeError, ValueError, KeyError):
        return _result("unsupported")
    facts = _Facts("federal-register")
    facts.record_count = 1
    facts.result["scope"] = "Federal Register issue title date; no completeness inference."
    facts.period("publication-date", published.isoformat(), "/title")
    return facts.finish()


def _result(outcome: str, **extra: Any) -> dict:
    return {"outcome": outcome, "dataset_id": None, "native_id": None, "primary_records": 0,
            "source_version": None, "periods": [], "evidence": [], "scope": _SCOPE, **extra}


def _integer(value: Any, maximum: int = 9999) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("native integer has the wrong type")
    if not re.fullmatch(r"[0-9]+", str(value)) or not 1 <= int(value) <= maximum:
        raise ValueError("native integer is out of range")
    return str(int(value))


def _string(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError("native identity has the wrong type or length")
    return value.strip()


def _date(value: Any) -> str:
    value = _string(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return date.fromisoformat(value).isoformat()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value):
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value[:10]  # Publisher's calendar date, without timezone conversion.
    raise ValueError("native date is not an ISO date or timestamp")


class _Facts:
    """Keep every field locator while selecting only unambiguous file identities."""

    def __init__(self, family: str):
        self.result = _result("ok", dataset_id=f"family:{family}")
        self.identities: set[tuple[str, str]] = set()
        self.versions: set[str] = set()
        self.record_count = 0
        self.periods: set[tuple[str, str]] = set()

    def evidence(self, field: str, value: Any, rule: str, target: str, **extra: Any) -> None:
        self.result["evidence"].append({"origin": "content", "field": field, "locator": field,
                                        "value": value, "rule": rule, "target": target, **extra})

    def identity(self, value: str, field: str, namespace: str, **extra: Any) -> None:
        self.record_count += 1
        self.identities.add((namespace, value))
        self.evidence(field, value, "primary-native-identity", "native_id", namespace=namespace, **extra)

    def version(self, value: Any, field: str) -> None:
        if value is not None:
            value = _string(value)
            self.versions.add(value)
            self.evidence(field, value, "native-document-version", "source_version")

    def period(self, kind: str, value: Any, field: str, *, numeric: bool = False) -> str:
        label = _integer(value, 999 if kind == "congress" else 9999) if numeric else _date(value)
        if kind == "filing-year" and len(label) != 4:
            raise ValueError("filing year must contain four digits")
        rule = f"primary-native-{kind}"
        if (kind, label) not in self.periods:
            self.periods.add((kind, label))
            self.result["periods"].append({"kind": kind, "label": label, "origin": "content",
                                           "field": field, "rule": rule,
                                           "basis": f"Original {field}; observed record, not completeness"})
        self.evidence(field, value, rule, "period", kind=kind, label=label)
        return label

    def finish(self) -> dict:
        self.result["primary_records"] = self.record_count
        if self.record_count == 1 and len(self.identities) == 1:
            self.result["native_id"] = next(iter(self.identities))[1]
            if len(self.versions) == 1:
                self.result["source_version"] = next(iter(self.versions))
        return self.result


def _bulk_download_receipt(value: dict) -> bool:
    """Recognize the local selection/download record, including its retained body."""
    selection, download = value.get("selection"), value.get("download")
    if not isinstance(selection, dict) or not isinstance(download, dict):
        return False
    digest, size = download.get("sha256"), download.get("bytes")
    response = download.get("response")
    return (
        isinstance(digest, str) and re.fullmatch(r"sha256:[a-f0-9]{64}", digest) is not None
        and isinstance(size, int) and not isinstance(size, bool) and size >= 0
        and selection.get("bytes") == size and not isinstance(selection.get("bytes"), bool)
        and download.get("blob_path") == "sha256/" + digest.removeprefix("sha256:")
        and isinstance(download.get("url"), str) and bool(download["url"])
        and selection.get("url") == download["url"]
        and all(isinstance(selection.get(k), str) and selection[k] for k in ("dataset", "filename"))
        and isinstance(download.get("downloaded"), bool) and isinstance(download.get("reused"), bool)
        and download["downloaded"] != download["reused"]
        and isinstance(response, dict)
        and all(isinstance(response.get(k), str) and response[k] for k in ("resolved_url", "observed_at"))
    )


def _receipt(value: Any) -> bool:
    """Recognize acquisition records by their digest, location, and request facts."""
    if not isinstance(value, dict):
        return False
    if _bulk_download_receipt(value):
        return True
    digest = value.get("sha256", value.get("body_sha256"))
    if not isinstance(digest, str) or not re.fullmatch(r"(?:sha256:)?[a-fA-F0-9]{64}", digest):
        return False
    size = value.get("byte_size", value.get("bytes", value.get("body_bytes")))
    location = any(isinstance(value.get(k), str) for k in ("body_path", "path", "blob_member", "local_path"))
    request = any(isinstance(value.get(k), str) for k in ("requested_url", "resolved_url", "url", "source_url"))
    acquisition = any(k in value for k in ("observed_at", "acquired_at", "status_code", "http_status", "request_headers"))
    return isinstance(size, int) and not isinstance(size, bool) and size >= 0 and location and request and acquisition


def _pairs(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _json(raw: bytes) -> Any:
    def invalid_constant(_: str) -> None:
        raise ValueError("non-finite JSON number")
    return json.loads(raw, object_pairs_hook=_pairs, parse_constant=invalid_constant)


def _records(value: Any, field: str) -> list[tuple[dict, str]]:
    if isinstance(value, dict):
        return [(value, field)]
    if not isinstance(value, list) or len(value) > _MAX_RECORDS:
        raise ValueError("native record list has the wrong shape or exceeds the record limit")
    records = []
    for i, record in enumerate(value):
        if not isinstance(record, dict):
            raise ValueError("native record list has the wrong shape or exceeds the record limit")
        records.append((record, f"{field}/{i}"))
    return records


def _json_family(value: dict, dataset_id: str | None) -> tuple[str, list[tuple[dict, str]], str] | None:
    # Distinct publisher wrappers take priority over generic results lists.
    for key, family in (("houseCommunication", "house-communications"), ("houseCommunications", "house-communications"),
                        ("committeeMeeting", "committee-meetings"), ("committeeMeetings", "committee-meetings"),
                        ("bill", "bill-family"), ("bills", "bill-family")):
        if key in value:
            return family, _records(value[key], f"/{key}"), "api"
    data = value.get("data")
    if isinstance(data, dict) and data.get("type") == "comments":
        return "comments-mirror", [(data, "/data")], "api"
    if isinstance(data, list) and data and all(isinstance(v, dict) and v.get("type") == "comments" for v in data):
        return "comments-mirror", _records(data, "/data"), "api"
    if isinstance(value.get("packageId"), str) and value["packageId"].startswith(("BILLS-", "BILLSTATUS-")):
        return "bill-family", [(value, "")], "govinfo"
    records = _records(value["results"], "/results") if "results" in value else [(value, "")]
    if records and all("filing_uuid" in record and "filing_year" in record for record, _ in records):
        return "lobbying-filings", records, "api"
    # FR field projections may omit type; require an established family for
    # those reduced records, then let the native reader validate both fields.
    if records and all("document_number" in record and "publication_date" in record
                       and ("type" in record or dataset_id == "family:federal-register")
                       for record, _ in records):
        return "federal-register", records, "api"
    return None


def _bill_identity(facts: _Facts, record: dict, field: str, style: str,
                   fields: dict[str, str] | None = None, congress_value: Any = None) -> None:
    fields = fields or {}
    def path(key: str) -> str:
        return fields.get(key, field + "/" + key)
    congress = facts.period("congress", record.get("congress"), path("congress"), numeric=True)
    if congress_value is not None:
        facts.result["evidence"][-1]["value"] = congress_value
    kind = _string(record.get("billType" if style == "govinfo" else "type")).lower()
    number = _integer(record.get("billNumber" if style == "govinfo" else "number"), 999999)
    if kind not in _BILL_TYPES:
        raise ValueError("unsupported native bill type")
    identity = f"{kind}{number}-{congress}"
    namespace = "congress-bill"
    if style == "govinfo":
        identity = _string(record.get("packageId"))
        version = record.get("billVersion")
        expected = f"BILLS-{congress}{kind}{number}{_string(version)}" if identity.startswith("BILLS-") else f"BILLSTATUS-{congress}{kind}{number}"
        if identity != expected:
            raise ValueError("native bill package and subject fields disagree")
        namespace = "govinfo-package"
        facts.version(version, path("billVersion"))
        if record.get("dateIssued") is not None:
            facts.period("issue-date", record["dateIssued"], path("dateIssued"))
    facts.identity(identity, path("packageId") if style == "govinfo" else field, namespace,
                   source_fields=[path(k) for k in ("congress", "billType", "billNumber")]
                   if style == "govinfo" else [path(k) for k in ("congress", "type", "number")])


def _extract_json(family: str, records: list[tuple[dict, str]], style: str) -> dict:
    if not records:
        return _result("unsupported", reason="No primary native records in response")
    facts = _Facts(family)
    for record, field in records:
        if family == "bill-family":
            _bill_identity(facts, record, field, style)
        elif family == "lobbying-filings":
            identity = _string(record.get("filing_uuid"))
            if not re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", identity):
                raise ValueError("invalid native filing UUID")
            facts.identity(identity, field + "/filing_uuid", "lda-filing")
            facts.period("filing-year", record.get("filing_year"), field + "/filing_year", numeric=True)
        elif family == "federal-register":
            from spicy_docs.sources.federal_register.native import classify_document
            # Validate only the fields this inventory consumes. SpicyDocs owns
            # historical split IDs and canonical publication dates; raw bytes
            # retain the complete record and are never normalized here.
            native = classify_document({key: record.get(key) for key in ('document_number', 'publication_date')})
            facts.identity(native['document_number'], field + "/document_number", "federal-register-document")
            facts.period("publication-date", native['publication_date'], field + "/publication_date")
        elif family == "comments-mirror":
            facts.identity(_string(record.get("id")), field + "/id", "regulations-comment")
            attributes = record.get("attributes")
            if not isinstance(attributes, dict):
                raise ValueError("comment attributes are not an object")
            if attributes.get("postedDate") is not None:
                facts.period("record-posted-date", attributes["postedDate"], field + "/attributes/postedDate")
        elif family == "house-communications":
            congress = facts.period("congress", record.get("congress"), field + "/congress", numeric=True)
            number = _integer(record.get("number"), 99999999)
            kind = record.get("communicationType")
            if not isinstance(kind, dict) or not re.fullmatch(r"[A-Za-z]{1,8}", _string(kind.get("code"))):
                raise ValueError("invalid communication type")
            if record.get("chamber") != "House":
                raise ValueError("house communication has no House chamber")
            facts.identity(f"{congress}:{kind['code'].lower()}:{number}", field, "house-communication",
                           source_fields=[field + "/" + key for key in ("congress", "communicationType/code", "number")])
            if record.get("congressionalRecordDate") is not None:
                facts.period("congressional-record-date", record["congressionalRecordDate"], field + "/congressionalRecordDate")
        elif family == "committee-meetings":
            congress = facts.period("congress", record.get("congress"), field + "/congress", numeric=True)
            event = _integer(record.get("eventId"), 999999999)
            chamber = _string(record.get("chamber")).lower()
            if chamber not in {"house", "senate", "joint"}:
                raise ValueError("invalid committee meeting chamber")
            facts.identity(f"{congress}:{chamber}:{event}", field, "congress-committee-meeting",
                           source_fields=[field + "/" + key for key in ("congress", "chamber", "eventId")])
            if record.get("date") is not None:
                facts.period("meeting-date", record["date"], field + "/date")
    return facts.finish()


def _xml(raw: bytes) -> Element:
    builder = TreeBuilder()
    parser = expat.ParserCreate(namespace_separator="}")
    depth = nodes = 0

    def name(value: str) -> str:
        return "{" + value if "}" in value else value

    def start(tag: str, attributes: dict) -> None:
        nonlocal depth, nodes
        depth += 1
        nodes += 1
        if depth > 128 or nodes > 100_000:
            raise ValueError("XML structure exceeds parser limits")
        builder.start(name(tag), {name(k): v for k, v in attributes.items()})

    def end(tag: str) -> None:
        nonlocal depth
        builder.end(name(tag))
        depth -= 1

    def doctype(_: str, _system: str | None, _public: str | None, internal: bool) -> None:
        if internal:
            raise ValueError("XML internal DTD is unsupported")

    def entity(*_: Any) -> Never:
        raise ValueError("XML entity declaration or external entity is unsupported")

    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = builder.data
    parser.StartDoctypeDeclHandler = doctype
    parser.EntityDeclHandler = entity
    parser.ExternalEntityRefHandler = entity
    parser.Parse(raw, True)
    return builder.close()


def _child(parent: Element, name: str) -> Element:
    children = parent.findall(name)
    if len(children) != 1:
        raise ValueError("native XML field is missing or repeated")
    return children[0]


def _text(parent: Element, name: str) -> str:
    node = _child(parent, name)
    if len(node):
        raise ValueError("native XML scalar contains nested elements")
    return _string(node.text)


def _register_mods(raw: bytes, root: Element) -> dict:
    """Use the source-owned issue reader; granule/host dates stay separate."""
    from spicy_docs.sources.federal_register.issues import read_fr_issue

    access_ids = root.findall(f"{_MODS}extension/{_MODS}accessId")
    if len(access_ids) != 1:
        raise ValueError("ambiguous MODS primary subject")
    package_id = _string(access_ids[0].text)
    if re.fullmatch(r"FR-\d{4}-\d{2}-\d{2}", package_id) is None:
        return _result("unsupported")
    issue = read_fr_issue(raw, package_id=package_id)
    facts = _Facts("federal-register")
    facts.result["scope"] = (
        "Federal Register issue metadata only; no rendition acquisition or completeness inference."
    )
    facts.identity(issue["package_id"], "/mods/extension/accessId", "govinfo-package")
    facts.period("publication-date", issue["publication_date"], "/mods/originInfo/dateIssued")
    return facts.finish()


def _extract_xml(root: Element, raw: bytes) -> dict:
    if root.tag in {"bill", "resolution", "billStatus"}:
        facts = _Facts("bill-family")
        if root.tag == "billStatus":
            bill = _child(root, "bill")
            _bill_identity(facts, {key: _text(bill, key) for key in ("congress", "type", "number")}, "/billStatus/bill", "api")
        else:
            form = _child(root, "form")
            congress_text = _text(form, "congress")
            congress = re.fullmatch(r"([0-9]+)(?:st|nd|rd|th) CONGRESS", congress_text)
            number = re.fullmatch(r"(H\.?\s*(?:R\.?|RES\.?|J\.?\s*RES\.?|CON\.?\s*RES\.?)|S\.?(?:\s*(?:RES\.?|J\.?\s*RES\.?|CON\.?\s*RES\.?))?)\s+([0-9]+)", _text(form, "legis-num"), re.I)
            if not congress or not number:
                raise ValueError("unsupported bill form identity")
            kind = re.sub(r"[^a-z]", "", number[1].lower())
            field = f"/{root.tag}/form"
            _bill_identity(facts, {"congress": congress[1], "type": kind, "number": number[2]}, field, "api",
                           fields={"type": field + "/legis-num", "number": field + "/legis-num"},
                           congress_value=congress_text)
            facts.version(root.get(f"{root.tag}-stage"), f"/{root.tag}/@{root.tag}-stage")
        return facts.finish()
    if root.tag == _MODS + "mods":
        extensions = root.findall(_MODS + "extension")
        collections = [node.text for ext in extensions for node in ext.findall(_MODS + "collectionCode")]
        if collections == ["FR"]:
            return _register_mods(raw, root)
        if collections != ["BILLS"]:
            return _result("unsupported")
        subjects = [ext for ext in extensions if ext.find(_MODS + "accessId") is not None]
        if len(subjects) != 1:
            raise ValueError("ambiguous MODS primary subject")
        ext = subjects[0]
        record = {target: _text(ext, _MODS + source) for source, target in (
            ("congress", "congress"), ("docClass", "billType"), ("billNumber", "billNumber"),
            ("billVersion", "billVersion"), ("accessId", "packageId"))}
        facts = _Facts("bill-family")
        field = f"/mods/extension[{extensions.index(ext) + 1}]"
        _bill_identity(facts, record, field, "govinfo",
                       fields={"billType": field + "/docClass", "packageId": field + "/accessId"})
        return facts.finish()
    if root.tag in {"rollcall-vote", "roll_call_vote"}:
        facts = _Facts("roll-call-votes")
        house = root.tag == "rollcall-vote"
        record = _child(root, "vote-metadata") if house else root
        field = "/rollcall-vote/vote-metadata" if house else "/roll_call_vote"
        congress = facts.period("congress", _text(record, "congress"), field + "/congress", numeric=True)
        session = _text(record, "session")
        if house:
            if not re.fullmatch(r"(?:1st|2nd)", session):
                raise ValueError("invalid House vote session")
            session = session[0]
        else:
            session = _integer(session, 2)
        number_key = "rollcall-num" if house else "vote_number"
        number = _integer(_text(record, number_key), 999999)
        chamber = "house" if house else "senate"
        facts.identity(f"{congress}:{session}:{chamber}:{number}", field, "congress-roll-call",
                       source_fields=[field + "/" + key for key in ("congress", "session", number_key)])
        date_key = "action-date" if house else "vote_date"
        value = _text(record, date_key)
        fmt = "%d-%b-%Y" if house else "%B %d, %Y, %I:%M %p"
        parsed = datetime.strptime(" ".join(value.split()), fmt).date().isoformat()
        facts.period("vote-date", parsed, field + "/" + date_key)
        facts.result["evidence"][-1]["value"] = value
        return facts.finish()
    return _result("unsupported")


def extract_metadata(dataset_id: str | None, original_bytes: bytes) -> dict:
    """Extract exact field observations; unrecognized formats remain unsupported.

    ``dataset_id=None`` permits strict native-shape detection. An explicit dataset
    never changes to a different family. Clear acquisition-receipt JSON/JSONL is
    classified independently of family and contributes no native source periods.
    Invalid recognized fields return an error without partial metadata. Evidence
    is bounded to 10,000 primary records; linked records are never traversed.
    """
    if not isinstance(original_bytes, bytes):
        raise TypeError("original_bytes must be bytes")
    raw = original_bytes.removeprefix(b"\xef\xbb\xbf").lstrip()
    if not raw:
        return _result("unsupported")
    try:
        if raw.startswith((b"{", b"[")):
            try:
                value = _json(raw)
            except json.JSONDecodeError:
                # A journal is a receipt only when *every* nonempty line qualifies.
                lines = [line for line in raw.splitlines() if line.strip()]
                if 1 < len(lines) <= _MAX_RECORDS and all(_receipt(_json(line)) for line in lines):
                    return _result("ok", role="acquisition-receipt")
                raise
            if _receipt(value):
                return _result("ok", role="acquisition-receipt")
            if not isinstance(value, dict) or "error" in value or "errors" in value:
                return _result("unsupported")
            if dataset_id is not None and dataset_id not in SUPPORTED_DATASETS:
                return _result("unsupported")
            detected = _json_family(value, dataset_id)
            if detected is None:
                return _result("unsupported")
            family, records, style = detected
            if dataset_id is not None and dataset_id != f"family:{family}":
                return _result("unsupported", reason="Native shape conflicts with the supplied dataset")
            return _extract_json(family, records, style)
        if raw.startswith(b"<"):
            # Only the exact Federal Register issue title is a reviewed HTML date source.
            if re.search(br"<\s*(?:!doctype\s+html|html)(?:\s|>)", raw[:2048], re.I):
                return _register_html(raw) if dataset_id == "family:federal-register" else _result("unsupported")
            if dataset_id is not None and dataset_id not in SUPPORTED_DATASETS:
                return _result("unsupported")
            result = _extract_xml(_xml(raw), raw)
            if result["dataset_id"] is not None and dataset_id is not None and result["dataset_id"] != dataset_id:
                return _result("unsupported", reason="Native shape conflicts with the supplied dataset")
            return result
        return _result("unsupported")
    except (ValueError, TypeError, KeyError, RecursionError, expat.ExpatError) as error:
        # Parser diagnostics are safe summaries, not fragments of source contents.
        return _result("error", error=type(error).__name__, reason="Malformed or unsupported native structured metadata")
