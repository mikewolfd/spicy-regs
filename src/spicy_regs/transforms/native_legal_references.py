"""Read bounded pinned XML selections into source occurrences and complete-read scopes.

spicy-docs owns the two tables' contracts, the row shaping and the reading of
each observation (``interpretation.native_legal_references``); this host owns
the manifest, the pins, the evidence, the qualification, the scan loop and the
target lookup the reading is given. One row remains one scanner observation;
lookup results are nested candidates, never multiplied rows. Every requested
source must finish before outputs are replaced. Retained bytes and manifest
enter the existing CaptureEvidence artifact, not a second journal.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from spicy_docs.interpretation.native_legal_references import interpret_native_references
from spicy_docs.schemas.native_reference_rows import (
    NATIVE_LEGAL_REFERENCE_READS,
    NATIVE_LEGAL_REFERENCES,
    native_reference_scope_id,
    shape_ecfr_note,
    shape_native_reference_read,
    shape_uscode_reference,
    shape_uscode_source_credit,
)
from spicy_docs.sources.cfr.authority import scan_ecfr_authority_notes
from spicy_docs.sources.uscode.references import scan_uscode_references

from spicy_regs.citation_resolution import ROUTES, resolve_citations
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

#: The two tables, as spicy-docs contracts them: columns, identities and the rule their rows name.
CONTRACTS = (NATIVE_LEGAL_REFERENCES, NATIVE_LEGAL_REFERENCE_READS)
OUTPUTS = tuple(contract.name + ".parquet" for contract in CONTRACTS)


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
            scope = native_reference_scope_id(family, record, edition)
            if scope in scopes:
                raise ValueError("repeated source/edition scope in one manifest")
            scopes.add(scope)
            body = _pinned(spec, manifest.parent, 16 * 1024 * 1024)
            from spicy_regs.transforms.native_legal_inputs import qualify_source

            facts = qualify_source(
                spec,
                body,
                lambda pinned: _pinned(pinned, manifest.parent, 16 * 1024 * 1024),
                lambda payload, **fields: _retain(evidence, payload, **fields),
            )
            if facts is not None:
                _retain(evidence, _json(facts).encode(), role="native-source-qualification", scope_id=scope)
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
                extra = (
                    {"title": str(facts["metadata"]["title"])}
                    if (family == "ecfr" and facts is not None and facts["metadata"]["title"] is not None)
                    else {}
                )
                fresh.append(shaper(observation, occurrence_index=len(fresh), **context, **extra))

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
            rows.extend(fresh)
            reads.append(
                shape_native_reference_read(
                    source_family=family,
                    source_bytes=len(body),
                    occurrence_count=len(fresh),
                    manifest_sha256=_digest(raw),
                    **context,
                )
            )
            evidence.event("native-reference-read", **reads[-1])
        # spicy-docs reads every observation; the lookup is this repository's, over the target tables the manifest
        # pinned, called once for the run so each distinct typed key is read once.
        coverage: dict[str, Any] = {}

        def lookup(candidates: list[dict[str, Any]], texts: dict[tuple[str, str], str]) -> list[dict[str, Any]]:
            resolved = resolve_citations(cursor, candidates, snapshots, source_digests=texts)
            coverage.update(resolved["coverage"])
            return resolved["occurrences"]

        rows = interpret_native_references(rows, resolve=lookup)
        evidence.event("native-reference-resolution", **coverage)
    output_dir.mkdir(parents=True, exist_ok=True)
    # Local replay uses its last complete output as prior; isolated rollup builds
    # instead obtain the selected published generation through download_prior.
    for contract in CONTRACTS:
        local_prior = output_dir / (contract.name + ".parquet")
        if local_prior.exists():
            shutil.copyfile(local_prior, prior_scratch_path(output_dir, contract.name))
    return tuple(
        merge_contract_table(
            output_dir,
            contract.name,
            rows if contract is NATIVE_LEGAL_REFERENCES else reads,
            download_prior=download_prior,
            replace_parents=("scope_id", scopes),
        )
        for contract in CONTRACTS
    )
