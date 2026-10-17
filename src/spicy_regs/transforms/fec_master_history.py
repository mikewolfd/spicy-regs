"""The FEC bulk master histories (candidate and committee): acquire a selection, then build only from a selection.

Owner decision 53: each table is the whole population, one file per two-year cycle from 1980 through the current
cycle (the even year at or after today) plus the master's header file, about 13 MB in 25 requests for the committee
master. A history is never merged with a prior and a repeated (identity, cycle) refuses.

Two routes, and only these:

* **Refresh** (``capture_dir``) downloads the header and every cycle through SpicyDocs' FEC client into a new
  ``<capture_dir>/fec-<label>-master-XXXX/`` attempt: ``blobs/`` holds each original and ``selection.json`` records
  every capture as it lands, ``complete: true`` after the last, or a scrubbed ``failure``. Old URLs are never assumed
  immutable, so a refresh always downloads; a failed or refused attempt stays on disk.
* **Rebuild** (``selection``) reads a retained, complete ``selection.json`` and its blobs with no client and no HTTP.
  It is how a mapping change, or a parser failure after a good download, is re-run without touching the publisher.

Either way the table is built from a selection through SpicyDocs' master readers, which verify every file by digest
and size, check the header, the URL for the cycle and the row width. The selection must name exactly the cycles 1980
through its closing cycle, once each; a missing, extra, duplicate or reordered cycle refuses before any file is read.
The output replaces the destination only after full consumption (``write_rows``).

No parse cache: a cache keyed on digest, parser and settings would add an invalidation rule to guard a parse of at
most a dozen MB, and most of a rebuild is the shared writer's per-row extra-field check, which is the better lever.
The "one changed original invalidates only its cache" probe is vacuous without a cache and was not run.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import mkdtemp
from typing import TYPE_CHECKING

import pyarrow as pa
from loguru import logger

from spicy_regs.transforms.parquet_rows import write_rows

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

SELECTION_FORMAT = "fec-master-selection-v1"
#: cm26, the largest committee file, is 0.87 MB; a file past this is not a master.
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_HEADER_BYTES = 4 * 1024
#: Response fact -> SpicyDocs capture field, kept when the publisher stated them as strings.
_FACTS = {
    "resolved_url": "resolvedUrl",
    "media_type": "mediaType",
    "via": "via",
    "etag": "etag",
    "last_modified": "lastModified",
    "content_encoding": "contentEncoding",
    "content_length": "contentLength",
}


def current_cycle(today: date) -> int:
    """The two-year cycle ``today`` falls in, named by its closing even year."""
    return today.year + today.year % 2


@dataclass(frozen=True)
class MasterFamily:
    """What differs between the candidate and committee masters; everything else is shared."""

    table: str  # "fec_candidate_history"
    label: str  # "candidate": names the stage "fec-candidate-master" and the refusal text
    header_url: str
    cycles: Callable[[int], tuple[int, ...]]
    url: Callable[[int], str]
    rows: Callable[..., Iterator[dict]]
    project: Callable[[dict], dict]
    columns: tuple[str, ...]
    identity: str  # "candidate_id" / "committee_id"

    @property
    def stage(self) -> str:
        return f"fec-{self.label}-master"

    @property
    def output(self) -> str:
        return f"{self.table}.parquet"


def build_master_history(
    family: MasterFamily,
    output_dir: Path,
    *,
    capture_dir: Path | None = None,
    selection: Path | None = None,
    evidence: CaptureEvidence | None = None,
    today: date | None = None,
) -> Path:
    """Write the whole history table, from a fresh download into ``capture_dir`` or from a retained ``selection``."""
    if capture_dir is not None and selection is None:
        retained = _acquire(family, Path(capture_dir), today=today or date.today(), evidence=evidence)
    elif selection is not None and capture_dir is None:
        if evidence is not None:
            raise ValueError("A selection rebuild makes no acquisition; pass no evidence")
        retained = Path(selection)
    else:
        raise ValueError("Pass capture_dir to refresh or selection to rebuild a retained FEC master selection")
    output_dir.mkdir(parents=True, exist_ok=True)
    out, counts = _rebuild(family, retained, output_dir)
    if evidence is not None:
        evidence.event("cycle-rows", stage=family.stage, rows_by_cycle={str(k): v for k, v in counts.items()})
    logger.info(
        "FEC {} history: {:,} rows over {} cycles; {}",
        family.label,
        sum(counts.values()),
        len(counts),
        json.dumps({c: n for c, n in list(counts.items())[-2:]}),
    )
    return out


def _capture(result: dict, representation: str) -> dict:
    """SpicyDocs' capture facts for one downloaded original, as its readers take them."""
    facts = result.get("response") or {}
    if not result.get("downloaded") or not facts.get("observed_at"):
        raise ValueError(f"FEC bulk original was not freshly downloaded: {result['url']}")
    return {
        "requestUrl": result["url"],
        "observedAt": facts["observed_at"],
        "responseSha256": result["sha256"],
        "byteSize": result["bytes"],
        "representation": representation,
        **{name: facts[key] for key, name in _FACTS.items() if isinstance(facts.get(key), str)},
    }


def write_json(path: Path, value: dict) -> None:
    """Replace ``path`` whole and durably (temporary file, fsync, rename), so a kill never leaves a half-written file."""
    text = json.dumps(value, indent=2) + "\n"
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _acquire(family: MasterFamily, capture_dir: Path, *, today: date, evidence: CaptureEvidence | None) -> Path:
    """Download the header and every cycle into a new attempt; return its ``selection.json``."""
    from spicy_docs.sources.fec.client import FecClient
    from spicy_docs.transport.credentials import failure_reason

    through = current_cycle(today)
    cycles = family.cycles(through)
    capture_dir.mkdir(parents=True, exist_ok=True)
    attempt = Path(mkdtemp(prefix=family.stage + "-", dir=capture_dir))
    manifest = attempt / "selection.json"
    state: dict = {
        "format": SELECTION_FORMAT,
        "family": family.table,
        "through_cycle": through,
        "complete": False,
        "header": None,
        "cycles": [],
    }
    write_json(manifest, state)
    transport = None
    if evidence is not None:
        evidence.event("selection", stage=family.stage, cycles=[cycles[0], cycles[-1]])
        transport = evidence.transport(stage=family.stage + "-response", max_bytes=MAX_FILE_BYTES)
    try:
        with FecClient(store=attempt / "blobs", max_requests=(len(cycles) + 1) * 4, transport=transport) as client:
            state["header"] = _capture(client.download(family.header_url, max_bytes=MAX_HEADER_BYTES), "opaque")
            write_json(manifest, state)
            for cycle in cycles:
                capture = _capture(client.download(family.url(cycle), max_bytes=MAX_FILE_BYTES), "zip")
                state["cycles"].append({"cycle": cycle, **capture})
                write_json(manifest, state)
        state["complete"] = True
    except Exception as error:
        state["failure"] = failure_reason(error)
        raise
    finally:
        write_json(manifest, state)
    return manifest


def _rebuild(family: MasterFamily, selection: Path, output_dir: Path) -> tuple[Path, dict[int, int]]:
    """Build the table from a complete retained selection; no client is constructed."""
    from rulespec_artifacts import LocalBlobSource

    try:
        value = json.loads(selection.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"FEC master selection is missing or unreadable: {selection}") from error
    if not isinstance(value, dict) or value.get("format") != SELECTION_FORMAT or value.get("family") != family.table:
        raise ValueError(f"{selection} is not a {family.table} master selection")
    if value.get("complete") is not True:
        raise ValueError("FEC master selection is incomplete; refresh to acquire a complete one")
    entries = value.get("cycles")
    if not isinstance(entries, list):
        raise ValueError("FEC master selection omitted its cycles")
    through = value.get("through_cycle")
    if type(through) is not int:
        raise ValueError("FEC master selection omitted its closing cycle")
    expected = family.cycles(through)
    if [e.get("cycle") if isinstance(e, dict) else None for e in entries] != list(expected):
        raise ValueError(
            f"FEC {family.label} master selection must name every cycle {expected[0]} through {expected[-1]} once"
        )
    header = value.get("header")
    blobs = LocalBlobSource(selection.parent / "blobs")
    counts: dict[int, int] = {}

    def rows() -> Iterator[dict]:
        for entry in entries:
            cycle = entry["cycle"]
            counts[cycle] = 0
            # A file's rows all carry its own cycle, so a repeat can only happen inside one file.
            seen: set[str | None] = set()
            capture = {name: fact for name, fact in entry.items() if name != "cycle"}
            for row in family.rows(capture=capture, header_capture=header, blob_source=blobs, cycle=cycle):
                shaped = family.project(row)
                identity = shaped[family.identity]
                if identity in seen:
                    raise ValueError(f"FEC {family.label} master repeats {identity} in cycle {cycle}")
                seen.add(identity)
                counts[cycle] += 1
                yield shaped

    schema = pa.schema([(column, pa.string()) for column in family.columns])
    return write_rows(rows(), output_dir / family.output, schema), counts
