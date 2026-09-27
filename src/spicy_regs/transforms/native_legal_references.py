"""Read bounded pinned XML selections into source occurrences and complete-read scopes.

Scanners own XML reading. One row remains one scanner observation; optional
interpretation/lookup results are nested candidates, never multiplied rows.
Every requested source must finish before outputs are replaced. Retained bytes
and manifest enter the existing CaptureEvidence artifact, not a second journal.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from spicy_docs.interpretation.citations import find_citations
from spicy_docs.schemas.native_reference_rows import shape_ecfr_note, shape_uscode_reference, shape_uscode_source_credit
from spicy_docs.schemas.tables import usc_section_key
from spicy_docs.sources.cfr.authority import scan_ecfr_authority_notes
from spicy_docs.sources.uscode.references import scan_uscode_references

from spicy_regs.citation_resolution import ROUTES, resolve_citations
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.table_merge import merge_table, prior_scratch_path

RULE = "native-legal-reference/001"
REFERENCE_COLUMNS = (
    "scope_id",
    "source_family",
    "source_record_key",
    "edition",
    "input_sha256",
    "source_locator",
    "occurrence_index",
    "source_path",
    "element_tag",
    "attributes_json",
    "ancestors_json",
    "observation_kind",
    "href",
    "text",
    "text_runs_json",
    "cfr_title",
    "cfr_part",
    "interpretation_status",
    "target_candidates_json",
    "rule_version",
)
READ_COLUMNS = (
    "scope_id",
    "source_family",
    "source_record_key",
    "edition",
    "input_sha256",
    "source_locator",
    "source_bytes",
    "occurrence_count",
    "read_status",
    "selected_shapes_json",
    "unsupported_shapes_json",
    "manifest_sha256",
    "rule_version",
)
SCHEMAS = {"native_legal_references": REFERENCE_COLUMNS, "native_legal_reference_reads": READ_COLUMNS}
OUTPUTS = tuple(name + ".parquet" for name in SCHEMAS)
KINDS = ("usc_section", "cfr_section", "public_law", "statutes_at_large", "federal_register_cite")
_HREF = re.compile(r"/us/usc/t([1-9][0-9]*[aA]?)/s([0-9]+[A-Za-z0-9–—-]*)")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _pinned(spec: dict, base: Path, maximum: int) -> bytes:
    path = base / spec["path"]
    if path.stat().st_size > maximum:
        raise ValueError("retained input exceeds byte bound")
    with path.open("rb") as stream:
        body = stream.read(maximum + 1)
    if len(body) > maximum or _digest(body) != spec["sha256"]:
        raise ValueError("retained input digest or byte bound differs")
    return body


def _retain(evidence: CaptureEvidence, body: bytes, **fields: Any) -> None:
    # A retained input is not a newly observed HTTP response. Use the shared
    # store and journal with an honest event kind; artifact admission verifies bytes.
    evidence.store.put_blob(_digest(body), len(body), [body])
    evidence.event("retained-input", sha256=_digest(body), byte_size=len(body), **fields)


def _interpret(row: dict) -> tuple[str, str]:
    key = row["scope_id"] + ":" + row["occurrence_index"]
    common = {"document_kind": row["source_family"], "document_key": key, "source_record_key": row["source_record_key"]}
    candidates = []
    if row["observation_kind"] == "native_reference":
        href = row.get("href")
        match = _HREF.fullmatch(href or "")
        # Exact section hrefs only. Subsections, fragments and other namespaces
        # remain literal observations, not silently shortened identifiers.
        if not match or row["element_tag"] not in {"ref", "{http://xml.house.gov/schemas/uslm/1.0}ref"}:
            return "unsupported_href", _json([])
        candidates.append(
            {
                **common,
                "occurrence_key": key + ":href",
                "cite_kind": "usc_section",
                "target_key": match[1].upper() + "-" + str(usc_section_key(match[2])),
                "matched_text": href,
                "target_resolved": True,
                "derivation_rule": "native-usc-section-href/001",
            }
        )
        status = "native_section_href"
    else:
        text = row.get("text") or ""
        for index, finding in enumerate(find_citations(text, kinds=KINDS)):
            candidates.append(
                {
                    **common,
                    "occurrence_key": key + ":" + str(index),
                    "cite_kind": (
                        "cfr_part"
                        if finding.kind == "cfr_section" and re.fullmatch(r"[0-9]+-[0-9]+", finding.target_key)
                        else finding.kind
                    ),
                    "target_key": finding.target_key,
                    "target_resolved": finding.target_resolved,
                    "matched_text": finding.matched_text,
                    "span_start": finding.span_start,
                    "span_end": finding.span_end,
                    "text_sha256": _digest(text.encode()),
                    "derivation_rule": finding.target_rule,
                    "derivation_version": finding.rule_version,
                }
            )
        status = "partial_text_findings" if candidates else "no_qualified_text_findings"
    return status, _json(candidates)


def build_native_legal_references(
    manifest: Path,
    output_dir: Path,
    *,
    evidence: CaptureEvidence,
    download_prior: Callable[[str, Path], bool] = lambda *_: False,
) -> tuple[Path, ...]:
    """Replay explicit complete inputs; a failed/capped scan cannot clear earlier findings.

    Bounds: at most 100 source records, 16 MiB per XML, 64 MiB total XML,
    10,000 selected observations per run. Optional target Parquet inputs are
    exact byte-pinned and bounded to 64 MiB each. No network acquisition occurs.
    Scope is family/record/edition; input digest and rule revisions replace that
    scope after a complete read, including zero. Other scopes survive the merge.
    """
    import duckdb

    raw = manifest.read_bytes()
    if len(raw) > 1024 * 1024:
        raise ValueError("manifest exceeds byte bound")
    selection = json.loads(raw)
    sources = selection["sources"]
    if not isinstance(sources, list) or not 1 <= len(sources) <= 100:
        raise ValueError("manifest needs 1..100 sources")
    _retain(evidence, raw, role="selection-manifest")
    rows, reads, scopes = [], [], set()
    total = 0
    with duckdb.connect() as cursor:
        snapshots = {}
        targets = selection.get("targets", {})
        allowed = {route.table for route in ROUTES.values()}
        for table, spec in targets.items():
            if table not in allowed:
                raise ValueError("unsupported target table")
            body = _pinned(spec, manifest.parent, 64 * 1024 * 1024)
            _retain(evidence, body, role="target", table=table)
            # Read the verified copy, never reopen mutable manifest input paths.
            path = output_dir / ".targets" / (_digest(body).split(":")[1] + ".parquet")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            escaped = str(path).replace("'", "''")
            cursor.execute(f"CREATE VIEW \"{table}\" AS SELECT * FROM read_parquet('{escaped}')")
            snapshots[table] = {"status": "retained_file", "sha256": _digest(body)}
        for spec in sources:
            family, record, edition = spec["source_family"], spec["source_record_key"], spec["edition"]
            if family not in {"ecfr", "uscode"} or not isinstance(record, str) or not record:
                raise ValueError("source family and nonblank record identity required")
            if edition is not None and (not isinstance(edition, str) or not edition):
                raise ValueError("edition must be a nonblank literal or explicit null")
            if not isinstance(spec["source_locator"], str) or not spec["source_locator"]:
                raise ValueError("source locator required")
            scope = _digest(_json([family, record, edition]).encode())
            if scope in scopes:
                raise ValueError("repeated source/edition scope in one manifest")
            scopes.add(scope)
            body = _pinned(spec, manifest.parent, 16 * 1024 * 1024)
            total += len(body)
            if total > 64 * 1024 * 1024:
                raise ValueError("total XML exceeds byte bound")
            _retain(
                evidence,
                body,
                role="native-xml",
                source_family=family,
                source_record_key=record,
                edition=edition,
                source_locator=spec["source_locator"],
            )
            context = {
                "source_record_key": record,
                "input_sha256": _digest(body),
                "source_locator": spec["source_locator"],
                "edition": edition,
            }
            fresh = []

            def admit(observation, shaper):
                if len(rows) + len(fresh) >= 10_000:
                    raise ValueError("selected observation bound exceeded")
                fresh.append(shaper(observation, occurrence_index=len(fresh), **context))

            try:
                if family == "ecfr":
                    scan_ecfr_authority_notes(
                        body,
                        on_authority=lambda o: admit(o, shape_ecfr_note),
                        on_source=lambda o: admit(o, shape_ecfr_note),
                        max_observations=10_000,
                    )
                else:
                    scan_uscode_references(
                        body,
                        on_reference=lambda o: admit(o, shape_uscode_reference),
                        on_source_credit=lambda o: admit(o, shape_uscode_source_credit),
                        max_observations=10_000,
                    )
            except Exception as error:
                evidence.refusal(error, stage="native-legal-reference:" + scope)
                raise
            for row in fresh:
                row.update(scope_id=scope, source_family=family, rule_version=RULE)
                row["interpretation_status"], row["target_candidates_json"] = _interpret(row)
            rows.extend(fresh)
            reads.append(
                {
                    **context,
                    "scope_id": scope,
                    "source_family": family,
                    "source_bytes": str(len(body)),
                    "occurrence_count": str(len(fresh)),
                    "read_status": "complete_selected_shapes",
                    "selected_shapes_json": _json(["AUTH", "SOURCE"] if family == "ecfr" else ["href", "sourceCredit"]),
                    "unsupported_shapes_json": _json(["PARAUTH", "SECAUTH"] if family == "ecfr" else []),
                    "manifest_sha256": _digest(raw),
                    "rule_version": RULE,
                }
            )
            evidence.event("native-reference-read", **reads[-1])
        # Resolve each distinct typed key once per selected target snapshot.
        candidates = [candidate for row in rows for candidate in json.loads(row["target_candidates_json"])]
        source_digests = {
            (row["source_family"], row["scope_id"] + ":" + row["occurrence_index"]): _digest(
                (row.get("text") or "").encode()
            )
            for row in rows
        }
        resolved = resolve_citations(cursor, candidates, snapshots, source_digests=source_digests)
        by_occurrence = {}
        for candidate in resolved["occurrences"]:
            by_occurrence.setdefault(candidate["document_key"], []).append(candidate)
        for row in rows:
            row["target_candidates_json"] = _json(
                by_occurrence.get(row["scope_id"] + ":" + row["occurrence_index"], [])
            )
        evidence.event("native-reference-resolution", **resolved["coverage"])
    output_dir.mkdir(parents=True, exist_ok=True)
    # Local replay uses its last complete output as prior; isolated rollup builds
    # instead obtain the selected published generation through download_prior.
    for name in SCHEMAS:
        local_prior = output_dir / (name + ".parquet")
        if local_prior.exists():
            shutil.copyfile(local_prior, prior_scratch_path(output_dir, name))
    return tuple(
        merge_table(
            output_dir,
            name=name,
            columns=columns,
            identity=("scope_id", "input_sha256", "occurrence_index")
            if name == "native_legal_references"
            else ("scope_id",),
            version_column=None,
            rows=rows if name == "native_legal_references" else reads,
            remote_key=name + ".parquet",
            download_prior=download_prior,
            replace_parents=("scope_id", scopes),
        )
        for name, columns in SCHEMAS.items()
    )
