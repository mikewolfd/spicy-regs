"""Complete CFR native-event comparison for the fixed retained selection.

Extracted from D executable-request-v3 semantic_runner.py; historical supervisor,
worker, grant wrappers and CLI are not runtime dependencies. Retained capture
records use two field spellings (requested_url/resolved_url/status_code/byte_size
and url/final_url/http_status|status/bytes); exactly one spelling of each field
must be present, so a record carrying two refuses as ambiguous. Inputs are bound
by content (stored and decoded digests), never by filesystem identity. Native
mapping is checked independently; XML decoding is shared Expat, and
historical/grammar qualification stays separate.
"""

from dataclasses import asdict
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
from xml.parsers import expat

MAX_BODY = 2 * 1024 * 1024
MAX_RECORD = 65536
MAX_ELEMENTS = 100000
MAX_DEPTH = 64
MAX_OUTPUT = 16 * 1024 * 1024


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def pairs(items):
    d = {}
    for k, v in items:
        require(k not in d, "duplicate JSON key")
        d[k] = v
    return d


def decode(raw):
    def constant(_):
        raise ValueError("nonfinite JSON")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def checksum(value):
    require(type(value) is str, "checksum type")
    require(re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", value) is not None, "checksum format")
    return value if value.startswith("sha256:") else "sha256:" + value


CAPTURE_ALIASES = {
    "requested_url": ("requested_url", "url"),
    "resolved_url": ("resolved_url", "final_url"),
    "status_code": ("status_code", "http_status", "status"),
    "byte_size": ("byte_size", "bytes"),
}


def one(record, name):
    """The single present spelling of a logical capture field."""
    found = [record[key] for key in CAPTURE_ALIASES.get(name, (name,)) if key in record]
    require(found, "capture missing " + name)
    require(len(found) == 1, "capture ambiguous " + name)
    return found[0]


def capture_association(raw, body_spec, context):
    require(len(raw) <= MAX_RECORD, "capture record bound")
    r = decode(raw)
    require(type(r) is dict, "capture object required")
    url = one(r, "requested_url")
    final = one(r, "resolved_url")
    status = one(r, "status_code")
    mime = one(r, "content_type")
    acquired = one(r, "observed_at")
    sha = checksum(one(r, "sha256"))
    size = one(r, "byte_size")
    require(type(status) is int and type(size) is int, "capture numeric types")
    require(all(type(x) is str and x for x in (url, final, mime, acquired)), "capture scalar types")
    require(
        sha == checksum(body_spec["decoded_sha256"]) and size == body_spec["decoded_bytes"],
        "capture/body binding differs",
    )
    expected = context["expected_final_url"]
    require(type(expected) is str and url == expected and final == expected, "capture route differs")
    if context["kind"] == "failure":
        require(status == 404 and mime.split(";", 1)[0].strip().lower() == "text/html", "expected404 differs")
    else:
        require(
            status == 200 and mime.split(";", 1)[0].strip().lower() in ("application/xml", "text/xml"),
            "successfulXML capture differs",
        )
    return {
        "source_record": r,
        "requested_url": url,
        "final_url": final,
        "status": status,
        "content_type": mime,
        "observed_at": acquired,
        "decoded_sha256": sha,
        "decoded_bytes": size,
    }


def independent_events(body):
    """Complete expanded names/attributes/namespaces and mixed text in order.

    Shares Expat XML decoding with maintained trees; traversal and bounds are
    independent. This is mapping fidelity, not independent XML engine proof.
    """
    require(type(body) is bytes and len(body) <= MAX_BODY, "XML byte bound")
    parser = expat.ParserCreate(namespace_separator="}")
    parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)
    events = []
    depth = 0
    count = 0
    chars = 0
    decl = []

    def expanded(name):
        return "{" + name if "}" in name else name

    def ns(prefix, uri):
        decl.append([prefix or "", uri or ""])

    def start(name, attrs):
        nonlocal depth, count
        depth += 1
        count += 1
        require(depth <= MAX_DEPTH and count <= MAX_ELEMENTS, "XML structural bound")
        require(len(attrs) <= 64, "XML attribute bound")
        events.append(["start", expanded(name), [[expanded(k), v] for k, v in attrs.items()], list(decl)])
        decl.clear()

    def end(name):
        nonlocal depth
        events.append(["end", expanded(name)])
        depth -= 1

    def text(value):
        nonlocal chars
        if depth == 0:
            return
        chars += len(value)
        require(chars <= MAX_BODY, "XML decoded text bound")
        if events and events[-1][0] == "text":
            events[-1][1] += value
        else:
            events.append(["text", value])

    def forbidden(*_):
        raise ValueError("DTD/entity declarations forbidden")

    parser.StartNamespaceDeclHandler = ns
    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = text
    parser.StartDoctypeDeclHandler = forbidden
    parser.EntityDeclHandler = forbidden
    parser.ExternalEntityRefHandler = forbidden
    parser.Parse(body, True)
    require(count > 0 and depth == 0, "incomplete XML")
    return events


def maintained_events(node):
    result = [["start", node.name, [list(x) for x in node.attributes], [list(x) for x in node.namespace_declarations]]]
    for item in node.content:
        if isinstance(item, str):
            if result[-1][0] == "text":
                result[-1][1] += item
            else:
                result.append(["text", item])
        else:
            result.extend(maintained_events(item))
    result.append(["end", node.name])
    return result


def observe(body, capture_raw, body_spec, context, backend):
    association = capture_association(capture_raw, body_spec, context)
    require(len(body) == body_spec["decoded_bytes"] and len(body) <= MAX_BODY, "decoded body size differs")
    if context["kind"] == "failure":
        return {"kind": "failure", "capture": association, "native": None, "status": "retained404-failure"}
    require(context["kind"] in ("mods", "annual"), "context kind differs")
    require(type(context["year"]) is int and context["year"] in (2023, 2025), "edition context differs")
    expected = independent_events(body)
    mapped, summary = backend.read(body, context, association["final_url"])
    actual = maintained_events(mapped)
    require(actual == expected, "complete native mapping differs")
    return {
        "kind": context["kind"],
        "capture": association,
        "native_events": expected,
        "typed_summary": summary,
        "status": "complete-native-mapping-compared",
        "independence": "Independent traversal/bounds; shared Expat decoder. Derived annual grammar and section text not independently qualified.",
    }


def bounded(pin, limit=MAX_RECORD):
    """Read one small pinned file; its bytes and digest are the binding, not its inode."""
    require(type(pin["bytes"]) is int and 0 <= pin["bytes"] <= limit, "file byte bound")
    path = Path(pin["path"])
    require(path.is_file(), "source file/type differs")
    with path.open("rb") as f:
        size = os.fstat(f.fileno()).st_size
        raw = f.read(pin["bytes"] + 1)
        require(size == pin["bytes"] == len(raw) and os.fstat(f.fileno()).st_size == size, "file drift/bound")
    require(digest(raw) == checksum(pin["sha256"]), "file pin differs: " + pin["path"])
    return raw


def read_admitted(spec):
    """Stored and decoded bytes are both checked against their recorded digests."""
    stored = bounded(spec["stored"], MAX_BODY)
    require(0 <= spec["decoded_bytes"] <= MAX_BODY, "decoded bound")
    if spec["encoding"] == "gzip":
        with gzip.GzipFile(fileobj=io.BytesIO(stored)) as f:
            body = f.read(spec["decoded_bytes"] + 1)
    else:
        require(spec["encoding"] == "identity", "unsupported encoding")
        body = stored
    require(len(body) == spec["decoded_bytes"], "decoded size differs")
    require(digest(body) == checksum(spec["decoded_sha256"]), "decoded hash differs")
    return body


def run(config, backend):
    specs = config["bodies"]
    contexts = config["contexts"]
    require(type(specs) is dict and len(specs) == 5 and len(contexts) == 6, "complete input/context scope differs")
    require(
        len({c["id"] for c in contexts}) == 6 and all(c["body"] in specs for c in contexts),
        "context ids/references differ",
    )
    require(sum(s["decoded_bytes"] for s in specs.values()) <= 8 * 1024 * 1024, "aggregate decode bound")
    output = {}
    records = {c["id"]: bounded(c["capture"]) for c in contexts}
    for key, spec in specs.items():
        body = read_admitted(spec)
        selected = [c for c in contexts if c["body"] == key]
        require(bool(selected), "unobserved body")
        for c in selected:
            output[c["id"]] = observe(body, records[c["id"]], spec, c, backend)
        del body
    require(len(output) == 6, "duplicate/missing context id")
    # Compare full source event lists across editions/versions without coercion.
    differences = []
    for left, right in config["comparisons"]:
        a = output[left]["native_events"]
        b = output[right]["native_events"]
        rows = []
        for i in range(max(len(a), len(b))):
            x = a[i] if i < len(a) else None
            y = b[i] if i < len(b) else None
            if x != y:
                rows.append({"event_position": i + 1, "left": x, "right": y})
        differences.append({"left": left, "right": right, "differences": rows})
    result = {
        "status": "native-observations-and-comparisons-complete",
        "observations": output,
        "comparisons": differences,
        "custody_acceptance": config["custody_acceptance"],
        "body_specs": specs,
        "runtime_closure_accepted": False,
        "historical_selection_qualified": False,
        "limits": "Complete mapped native events and maintained typed views; grammar/section-body text and historical discovery are separate.",
    }
    raw = (json.dumps(result, ensure_ascii=False) + "\n").encode()
    require(len(raw) <= MAX_OUTPUT, "output bound")
    return raw


def stored_pin(stored):
    return {"path": stored["path"], "bytes": stored["bytes"], "sha256": checksum(stored["sha256"])}


def verify_custody(config):
    proofs = config["custody_acceptance"]
    require(len(proofs) == 3, "custody proof scope differs")
    data = []
    for proof in proofs:
        data.append(decode(bounded(proof)))
    acceptance, _, result = data
    require(acceptance["state"] == "accepted-exact-current-stored-decoded-original-custody", "custody not accepted")
    require(acceptance["handoff"] == proofs[1] and acceptance["result"] == proofs[2], "custody chain differs")
    accepted = result["gzip_objects"]
    gzip_specs = [s for s in config["bodies"].values() if s["encoding"] == "gzip"]
    require(len(gzip_specs) == 4 and len(accepted) == 4, "gzip custody scope differs")
    require(
        len({s["decoded_sha256"] for s in gzip_specs}) == 4
        and {s["decoded_sha256"] for s in gzip_specs} == {x["decoded_sha256"] for x in accepted},
        "gzip custody coverage differs",
    )
    require(len({s["stored"]["path"] for s in config["bodies"].values()}) == 5, "distinct body paths required")
    for spec in gzip_specs:
        matches = [x for x in accepted if x["decoded_sha256"] == spec["decoded_sha256"]]
        require(len(matches) == 1, "gzip custody match differs")
        item = matches[0]
        require(
            stored_pin(spec["stored"]) == stored_pin(item["stored"]) and spec["decoded_bytes"] == item["decoded_bytes"],
            "gzip custody binding differs",
        )
        aliases = {(x["manifest"], x["selector"]) for x in spec["aliases"]}
        require(
            aliases == {(x["manifest"], x["selector"]) for x in item["all_manifest_aliases"]}, "gzip aliases differ"
        )


def maintained_backend():
    from spicy_docs.reading.xml_tree import read_xml_tree
    from spicy_docs.sources.govinfo.mods import parse_govinfo_mods
    from spicy_docs.sources.cfr.edition import parse_annual_cfr_edition
    from spicy_docs.sources.cfr.annual import validate_annual_cfr_xml, scan_annual_cfr_sections
    from spicy_docs.sources.cfr.models import AnnualCfrSelection

    class Maintained:
        @staticmethod
        def read(body, c, url):
            selection = AnnualCfrSelection(c["year"], 1, 1)
            if c["kind"] == "mods":
                metadata = parse_govinfo_mods(body, max_bytes=MAX_BODY, max_elements=MAX_ELEMENTS)
                edition = parse_annual_cfr_edition(body, selection=selection, final_url=url, max_bytes=MAX_BODY)
                return metadata.package.element, {
                    "edition": asdict(edition),
                    "edition_type": edition.edition_type.value,
                    "constituents": len(metadata.constituents),
                }
            tree = read_xml_tree(
                body,
                expected_root="CFRDOC",
                error_type=ValueError,
                label="annualCFR",
                max_bytes=MAX_BODY,
                max_elements=MAX_ELEMENTS,
                max_depth=MAX_DEPTH,
            )
            try:
                native = asdict(validate_annual_cfr_xml(body, identity=selection, final_url=url, max_bytes=MAX_BODY))
            except ValueError as error:
                native = {"status": "identity-refused", "error": str(error)}
            sections = scan_annual_cfr_sections(body, max_bytes=MAX_BODY)
            return tree.element, {"identity": native, "sections": [asdict(s) for s in sections]}

    return Maintained
