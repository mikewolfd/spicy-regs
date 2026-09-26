"""Parse the fork output ledger's rows, audit phrases and destination into the MCP's qualification record.

The ledger is dated Markdown a person maintains under ``docs/research/``; the
MCP image installs only the package, so the server cannot read it. ``spicy-regs-dict
generate`` writes ``table_qualification.json`` beside ``table_metadata.json`` and
``check`` refuses a copy that differs from a fresh build.
``scripts/check_ledger_pins.py`` reads the same rows through this module.
Standard library only, because the MCP image imports nothing heavier.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

LEDGER_NAME = "docs/research/fork-output-ledger-2026-09-21.md"
LEDGER = Path(__file__).resolve().parents[2] / LEDGER_NAME
RECORD = Path(__file__).with_name("table_qualification.json")
RECORD_FORMAT = "spicy-regs-table-qualification"

_ROW = re.compile(r"\| T\d")
_CODE = re.compile(r"`([^`]+)`")
_DESTINATION = re.compile(r"^Public data destination: `(https://[^`]+)`", re.M)
_PIN = r"(?:snapshot_)?[0-9a-f]{8}"
_DATE = r"\d{4}-\d{2}-\d{2}"
#: The ledger's audited-generation phrases, in a closed vocabulary: each word
#: names an audit outcome, so a match may be reported as an audited generation.
#: "Published at" is a publication statement, not an audit, and is not matched.
_AUDIT = re.compile(
    rf"\b(?P<word>qualified|FAILED) at `(?P<pin>{_PIN})…` \((?P<date>{_DATE})\)"
    rf"|current `(?P<current>{_PIN})…` is (?P<state>PARTIAL|FAILED) \((?P<since>{_DATE})\)"
    rf"|verified at table digest `(?P<digest>[0-9a-f]{{8}})…` "
    rf"\((?P<checked>{_DATE}); ETag `(?P<etag>[0-9a-f]{{8}})…`\)"
)
#: Anything that starts one of those phrases, or an all-caps word used like one,
#: so a slipped or new spelling fails the build instead of reading as no audit.
_AUDIT_MENTION = re.compile(
    r"\bqualified(?: [a-z]+){0,3} at `|\b[A-Z][A-Z-]{3,} at `|current `[^`]*` is |verified at table digest"
)


def ledger_destination(text: str) -> str:
    """The public base URL the ledger states it describes; exactly one is required."""
    found = _DESTINATION.findall(text)
    if len(found) != 1:
        raise ValueError(f"the ledger states {len(found)} public data destinations, not 1; pass --index-url")
    return found[0].rstrip("/")


def ledger_rows(text: str) -> Iterator[tuple[str, list[str], str]]:
    """Yield ``(task, tables, delivery state)`` for each ``| T<n>`` table row."""
    for line in text.splitlines():
        if not _ROW.match(line):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|", 3)]
        if len(cells) != 4:
            raise ValueError(f"Ledger row is not Task | Producer | Output | Delivery state: {line}")
        tables = [name for name in _CODE.findall(cells[2]) if "<" not in name and "/" not in name]
        yield cells[0], tables, cells[3]


def audits(state: str) -> list[dict[str, str]]:
    """Each audited-generation phrase in a delivery state, in row order, with its disposition word verbatim.

    ``pin_kind`` names what the pin abbreviates: a family ``artifact`` digest, a
    rulemaking ``snapshot``, or a base object's ``table`` digest (whose ETag is kept).
    """
    found = []
    for match in _AUDIT.finditer(state):
        pin = match["pin"] or match["current"] or match["digest"]
        kind = "table" if match["digest"] else "snapshot" if pin.startswith("snapshot_") else "artifact"
        audit = {
            "disposition": match["word"] or match["state"] or "verified",
            "pin": pin,
            "pin_kind": kind,
            "date": match["date"] or match["since"] or match["checked"],
        }
        found.append(audit | {"etag": match["etag"]} if match["etag"] else audit)
    return found


def qualification_record(text: str) -> dict:
    """The ledger's destination and, per row naming a table, its audits and verbatim delivery state.

    A row whose audit wording falls outside the recognized phrases refuses, so a
    slip cannot silently read as "no audit recorded".
    """
    rows = []
    for task, tables, state in ledger_rows(text):
        if not tables:
            continue
        found = audits(state)
        if len(_AUDIT_MENTION.findall(state)) != len(found):
            raise ValueError(f"Ledger row {task} ({', '.join(tables)}) states an audit outside the recognized phrases")
        names = [name.removesuffix(".parquet") for name in tables]
        rows.append({"task": task, "tables": names, "audits": found, "statement": state})
    return {
        "format": RECORD_FORMAT,
        "version": 1,
        "ledger": LEDGER_NAME,
        "destination": ledger_destination(text),
        "rows": rows,
    }
