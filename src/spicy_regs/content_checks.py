"""Credential and body-shape checks over stored response bytes, in one streaming pass per object.

Audits of retained evidence missed keys (compressed bytes, chunk boundaries,
spellings the scrubber never writes) and accepted HTTP 200 HTML as XML. A
``Scan`` decodes gzip, finds credential parameters and configured key values
across chunk boundaries, and sniffs what the body actually is; ``expected_kind``
says what it was requested as. Values are never kept, only names, offsets and
lengths.
"""

from __future__ import annotations

import os
import re
import zlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

CHUNK = 1024 * 1024
SNIFF_BYTES = 4096
HIT_SAMPLES = 20
def _redacted() -> tuple[str, ...]:
    """Values written in place of a key, lower-cased: a parameter carrying one of these holds no secret.

    Besides the owners' own scrub marks, api.data.gov routes (SAM's extract trigger and
    its entity pages) state download links with the publisher's literal placeholder,
    which the SpicyDocs reader re-injects; it is a documented template, not a key.
    """
    from spicy_docs.sources.sam_extract import API_KEY_PLACEHOLDER

    return ("redacted", "demo_key", API_KEY_PLACEHOLDER.lower())


REDACTED = _redacted()


def _credential_names() -> tuple[str, ...]:
    """SpicyDocs' credential parameters plus header/JSON spellings of an API key, longest first."""
    from spicy_docs.transport.credentials import CREDENTIAL_PARAMETERS

    names = {name.lower() for name in CREDENTIAL_PARAMETERS} | {"api-key", "apikey", "x-api-key"}
    return tuple(sorted(names, key=lambda name: (-len(name), name)))


#: ``name=value``, ``name%3Dvalue`` (a URL inside a URL) and ``name: value`` / ``"name": "value"`` (headers,
#: JSON), case-insensitive. Python and DuckDB's RE2 read the same text, so file and cell scans cannot drift.
CREDENTIAL_PATTERN = (
    r"(?i)(" + "|".join(re.escape(name) for name in _credential_names()) + r")[\"']?\s*(?:=|%3d|:)\s*[\"']?"
    r"([A-Za-z0-9._~+/%-]{8,256})"
)
_CREDENTIAL = re.compile(CREDENTIAL_PATTERN.encode())
_CREDENTIAL_TEXT = re.compile(CREDENTIAL_PATTERN)
_PROLOG = re.compile(rb"\s*(?:<\?.*?\?>|<!--.*?-->|<!doctype\s+([A-Za-z]+)[^>]*>)", re.I | re.S)
_ELEMENT = re.compile(rb"\s*<(?:[A-Za-z_][\w.-]*:)?([A-Za-z_][\w.-]*)")
#: HTML elements an error or warning page can open with (a PHP warning opens with ``<br />``); no XML payload
#: this repository reads has one of these as its document element.
_HTML_ROOTS = frozenset(b"""a b body br center div em font form h1 h2 h3 h4 h5 h6 head hr html i iframe img li link meta
    noscript ol p pre script span strong style title u ul""".split())
#: Compression a response body can carry that ``Scan`` does not decode; each is reported as a limit.
_UNDECODED = ((b"BZh", "bzip2"), (b"\xfd7zXZ\x00", "xz"), (b"\x28\xb5\x2f\xfd", "zstd"))
_SUFFIX_KINDS = {".json": "json", ".xml": "xml", ".htm": "html", ".html": "html", ".shtml": "html", ".pdf": "pdf",
                 ".zip": "zip"}
_SECRET_NAME = re.compile(r"KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|_GOV$", re.I)


def sniff(head: bytes) -> str:
    """Classify leading bytes: html, xml, json, pdf, zip, gzip, text, binary or empty.

    Looks past a BOM, an XML declaration, comments and a DOCTYPE, so an XHTML
    error page behind an XML prolog is HTML. SpicyDocs' acquisition check
    (``validate_body_prefix``) reads only the first tag, which that page passes.
    """
    text = head.removeprefix(b"\xef\xbb\xbf").lstrip()
    if not text:
        return "empty"
    for magic, kind in ((b"%PDF", "pdf"), (b"PK\x03\x04", "zip"), (b"\x1f\x8b", "gzip"), *_UNDECODED):
        if text.startswith(magic):
            return kind
    if text[:1] in (b"{", b"["):
        return "json"
    if not text.startswith(b"<"):
        return "binary" if b"\x00" in text else "text"
    position, doctype = 0, b""
    while match := _PROLOG.match(text, position):
        doctype, position = doctype or (match.group(1) or b""), match.end()
    element = _ELEMENT.match(text, position)
    root = element.group(1).lower() if element else b""
    return "html" if doctype.lower() == b"html" or root in _HTML_ROOTS else "xml"


def expected_kind(url: str, content_type: str | None) -> str | None:
    """What a capture was requested as: a ``format=`` parameter, then the path suffix, then a JSON/XML media type.

    ``None`` when none of them says; the HTML-where-data-was-expected check applies only where one does.
    """
    parts = urlsplit(url)
    formats = [v.lower() for k, values in parse_qs(parts.query).items() if k.lower() == "format" for v in values]
    if formats and formats[0] in ("json", "xml"):
        return formats[0]
    suffix = Path(parts.path).suffix.lower()
    if suffix in _SUFFIX_KINDS:
        return _SUFFIX_KINDS[suffix]
    media = (content_type or "").split(";", 1)[0].strip().lower()
    if media.endswith(("/json", "+json")):
        return "json"
    if media.endswith(("/xml", "+xml")) and "xhtml" not in media:
        return "xml"
    return None


class Scan:
    """One pass over one object's content: credential search and a leading-bytes sniff.

    Gzip, detected by its magic whatever the journal says, is decoded in
    bounded steps first, since compression hides literal bytes. A match is
    counted once even across chunks: one touching a buffer's end waits for the
    next chunk, and one starting before the last counted match's end is the
    carried tail seen again.
    """

    def __init__(self, secrets: Mapping[str, bytes]):
        self.secrets = secrets
        self._tail_size = max([1024, *(len(value) for value in secrets.values())])
        self._head, self._tail, self._offset, self._counted = bytearray(), b"", 0, 0
        self._decoder: Any = None
        self._started = False
        self.encoding = "identity"
        self.pattern_count, self.pattern_hits = 0, []
        self.configured: set[str] = set()
        self.decoded_bytes = 0
        self.kind: str | None = None

    def update(self, chunk: bytes) -> None:
        if not self._started:
            self._started = True
            if chunk[:2] == b"\x1f\x8b":
                self._decoder, self.encoding = zlib.decompressobj(31), "gzip"
        for data in self._decoded(chunk):
            self._scan(data, final=False)

    def _decoded(self, chunk: bytes) -> Iterator[bytes]:
        """Decoded bytes in bounded steps; bytes after one gzip member start the next member."""
        if self._decoder is None:
            yield chunk
            return
        try:
            while chunk:
                if self._decoder.eof:
                    self._decoder = zlib.decompressobj(31)
                yield self._decoder.decompress(chunk, CHUNK)
                chunk = self._decoder.unconsumed_tail or (self._decoder.unused_data if self._decoder.eof else b"")
        except zlib.error:
            self._decoder, self.encoding = None, "gzip-undecodable"

    def _scan(self, data: bytes, *, final: bool) -> None:
        if len(self._head) < SNIFF_BYTES:
            self._head += data[: SNIFF_BYTES - len(self._head)]
        buffer = self._tail + data
        start = self._offset - len(self._tail)
        for match in _CREDENTIAL.finditer(buffer):
            if start + match.start() < self._counted or (match.end() == len(buffer) and not final):
                continue
            self._counted = start + match.end()
            lowered = match.group(2).lower()
            if lowered.decode("ascii", "replace") in REDACTED or lowered.startswith(b"%3c"):
                continue
            self.pattern_count += 1
            if len(self.pattern_hits) < HIT_SAMPLES:
                self.pattern_hits.append({"offset": start + match.start(), "name": match.group(1).decode().lower(),
                                          "value_length": len(match.group(2))})
        for name, secret in self.secrets.items():
            if name not in self.configured and secret in buffer:
                self.configured.add(name)
        self._tail = buffer[-self._tail_size:]
        self._offset += len(data)
        self.decoded_bytes += len(data)

    def close(self) -> None:
        """Flush the decoder, count matches that waited for more bytes, and sniff the head."""
        if self._decoder is not None:
            try:
                rest = self._decoder.flush()
            except zlib.error:
                rest, self.encoding = b"", "gzip-undecodable"
            self._scan(rest, final=False)
        self._scan(b"", final=True)
        self.kind = sniff(bytes(self._head))
        self._head, self._tail = bytearray(), b""


def redact(text: str, secrets: Iterable[str]) -> str:
    """``text`` with configured values and every credential parameter's value replaced; redact before truncating."""
    for secret in secrets:
        text = text.replace(secret, "<redacted>")
    return _CREDENTIAL_TEXT.sub(lambda m: m.group(0)[: m.start(2) - m.start(0)] + "<redacted>", text)


def scan_bytes(raw: bytes, secrets: Mapping[str, bytes]) -> Scan:
    """A closed ``Scan`` over one in-memory object."""
    scan = Scan(secrets)
    scan.update(raw)
    scan.close()
    return scan


def configured_secrets(env_files: Sequence[Path] = (), environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Key values to search for literally, labelled ``origin:NAME``, one label per distinct value of 16+ characters.

    Every value in a named env file counts except URLs and paths: naming the
    file says it holds secrets, and the data.gov key there is ``API_GOV``,
    which no name pattern for keys would pick out. From the process
    environment only secret-named settings count, ``*_GOV`` included.
    """
    from dotenv import dotenv_values

    environment = os.environ if environ is None else environ
    candidates = [("env", name, value) for name, value in environment.items() if _SECRET_NAME.search(name)]
    for path in env_files:
        candidates += [(str(path), name, value) for name, value in dotenv_values(path).items()
                       if value and "://" not in value and not value.startswith(("/", "~"))]
    found: dict[str, str] = {}
    for origin, name, value in candidates:
        if len(value) >= 16 and value not in found.values():
            found[f"{origin}:{name}"] = value
    return found
