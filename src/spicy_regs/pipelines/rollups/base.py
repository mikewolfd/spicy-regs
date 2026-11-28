"""Base class for decoupled, per-rollup pipelines, each on its own cron.

Each rollup (``feed_summary``, ``agency_stats``, ``discovery_signals``, ...)
is materialized by its own :class:`RollupPipeline` subclass with its own console
entry (``run-rollup-*``) and GitHub Actions workflow, so it can be re-run or
backfilled alone and a failure stays isolated to one artifact. ``run()`` reads
top-to-bottom as the data flow: prime the base tables this rollup reads from R2
(skipping any already present locally), materialize the artifact(s) via a
transform, retain one complete immutable generation, then — unless
``skip_upload`` is set — verify uploaded bytes before atomically replacing its
public pointer.

**Contract:** a rollup reads only tables no other rollup *derives* — the ETL's
published base tables (``dockets``, ``documents``, ``comments_index``) and the
published output of an **ingest** rollup, which fetches from a publisher and
has no upstream dependency inside this repository. Reading a derived rollup's
output stays forbidden, because that is the read that makes two pipelines race,
whereas an ingest table's content comes from outside, so the worst a stale copy
costs is one cron's lag, which the offsets already handle. A best-effort
merge-time join whose absence must not fail the run is declared in
``soft_inputs``, not ``inputs``: it primes nothing, fails nothing, and leaves
the linkage columns NULL so the rollup still publishes everything its own
source establishes — it exists so the read is *stated* rather than buried in a
transform, and so ``tests/test_hosted_rollups.py`` can hold each entry to being
an ingest rollup's output whose cron runs before this one's. A read that does
fetch the table is recorded like any other: every table of another family the
build fetches through :func:`spicy_regs.sources.r2.download_from_r2`, soft or
undeclared, is a parent of the generation at the bytes fetched; a read that
found nothing records nothing, and a read that bypasses the helper is not
recorded. The family's own prior is never a parent: the generation's
prior-generation input names it. Multi-output
rollups declare ``outputs`` instead of ``output`` and return a tuple of paths
from ``build()``; every declared output must be present, including successful
empty tables, and each goes through the same per-file shrink guard on upload.
Uploads use immutable generation paths, and ``publication.v2.json`` changes only
after the full family passes local schema checks, shrink checks and remote byte
verification; readers capture that index once per operation. The old bare
Parquet URLs are not rewritten by this path.
"""

import hashlib
import json
import os
import re
from abc import abstractmethod
from collections.abc import Mapping
from contextlib import nullcontext
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Annotated, Any, ClassVar
from uuid import uuid4

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.env_values import flag_env
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.source_evidence import JOURNAL, CaptureEvidence
from spicy_regs.sources import r2
from spicy_regs.sources.publication import PublicationError


class Unchanged(Exception):
    """The builder found no new table data; the captured prior supplies the table bytes.

    This is not permission to discard new observations or skip the selection checks.
    """


_SELECTION_EVENT = "rollup-selection"
_CARRIED_EVIDENCE_EVENT = "rollup-carried-evidence"
_BOOKKEEPING_EVENTS = frozenset({"run", "lineage", "build-outcome", _SELECTION_EVENT, _CARRIED_EVIDENCE_EVENT})


def _journal_events(raw: bytes) -> list[dict]:
    return [json.loads(line) for line in raw.splitlines()]


def _evidence_records(events: list[dict]) -> list[tuple[bool, bytes]]:
    """Ignore observation-instance IDs/clocks, not source versions, status, or selection."""
    from rulespec_artifacts import canonical_json_bytes

    records = []
    for event in events:
        name = event["event"]
        if name in _BOOKKEEPING_EVENTS:
            continue
        record = {key: value for key, value in event.items() if key != "recorded_at"}
        if name in {"capture", "retained-file"}:
            record.pop("capture_id", None)
        if name == "capture":
            for key in ("observed_at", "archive_receipt_key"):
                record.pop(key, None)
            if "response_headers" in record:
                record["response_headers"] = [
                    item for item in record["response_headers"] if item[0].lower() != "date"
                ]
        if name == "retained-file" and "redirect_capture_id" in record:
            # The official and redirect URLs plus the body pin are kept.
            # A redirect adapter copies the capture's fresh ID and clock.
            record.pop("redirect_capture_id")
            record.pop("observed_at", None)
        status = record.get("status_code")
        refused = (
            name in {"refusal", "capture-incomplete"}
            or record.get("source_validation") == "refused"
            or (isinstance(status, int) and status >= 400)
        )
        records.append((refused, canonical_json_bytes(record)))
    return records


class _RollupEvidence(CaptureEvidence):
    """Keep inherited checkpoint queries working after an evidence-only publication."""

    prior_root: dict | None = None
    _carried_events: list[dict] | None = None

    def _record_prior(self, prior_index, raw, root, *, public_url):
        super()._record_prior(prior_index, raw, root, public_url=public_url)
        self.prior_root = root

    def prior_events(self) -> list[dict]:
        from spicy_regs.sources.publication import load_evidence_journal

        if self._prior_evidence is None:
            return []
        if self._prior_events is None:
            url, pin = self._prior_evidence
            if url is None:
                return []
            self._prior_events = _journal_events(load_evidence_journal(url, pin, predating_ok=True))
        return self._prior_events

    def _carried_pin(self) -> dict | None:
        return next(
            (event["pin"] for event in reversed(self.prior_events()) if event["event"] == _CARRIED_EVIDENCE_EVENT),
            None,
        )

    def checkpoint_pin(self) -> dict | None:
        if self._prior_evidence is None or not self.prior_events():
            return None
        return self._carried_pin() or {
            key: self._prior_evidence[1][key] for key in ("logicalId", "artifactDigest")
        }

    def inherited_event(self, event: str, **match) -> dict | None:
        from spicy_regs.sources.publication import load_evidence_journal

        found = super().inherited_event(event, **match)
        if found is not None or self._prior_evidence is None:
            return found
        carried = self._carried_pin()
        url, _ = self._prior_evidence
        if carried is None or url is None:
            return None
        if self._carried_events is None:
            self._carried_events = _journal_events(load_evidence_journal(url, carried))
        return next(
            (row for row in reversed(self._carried_events)
             if row["event"] == event and all(row.get(key) == value for key, value in match.items())),
            None,
        )


class ResumeRefusedError(PublicationError):
    """Retained candidate resume was refused fail-closed without running source fetches or mutating R2."""


RESUME_CANDIDATES_DIR = "resume-candidates"
RESUME_BINDING_FORMAT = "spicy-regs-rollup-candidate-binding"
RESUME_BINDING_VERSION = 1
RESUME_GENERATION_ENV = "SPICY_REGS_RESUME_GENERATION"
RESUME_REQUIRE_ENV = "SPICY_REGS_RESUME_REQUIRE"
_BINDING_LIMIT = 1024 * 1024
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_BINDING_KEYS = frozenset({
    "format",
    "version",
    "family",
    "pipeline_class",
    "generation_directory",
    "generation_pin",
    "evidence_directory",
    "expected_keys",
    "added_tables",
    "receipt_only_tables",
    "partitioned",
    "retain_source_evidence",
    "validation_identity",
    "invocation_scope",
    "soft_inputs",
    "remote_inputs",
    "deferred_failure",
})

#: Non-secret environment variables that scope rollup build outputs.
ROLLUP_SCOPE_ENV_VARS: tuple[str, ...] = (
    "ADMINISTRATION_POLICY_FORCE_PUBLISH",
    "AMENDMENTS_MAX_DETAILS",
    "AMENDMENTS_SINCE",
    "AMENDMENTS_UNTIL",
    "BILL_FAMILY_BILL_TYPES",
    "BILL_FAMILY_CBO_ARCHIVE_MANIFEST",
    "BILL_FAMILY_CHILD_BILL_IDS",
    "BILL_FAMILY_CHILD_CHECKPOINT_DIR",
    "BILL_FAMILY_CONGRESSES",
    "BILL_FAMILY_MAX_VERSION_FETCHES",
    "BILL_SUBJECTS_DEADLINE_MINUTES",
    "BILL_SUBJECTS_MAX",
    "CFR_REPLACE_ALL",
    "COMMITTEE_REPORTS_REREAD",
    "COMMITTEE_REPORTS_RETAINED_BUNDLE",
    "COMMITTEE_REPORTS_RETAINED_BUNDLE_ROOT",
    "COMMITTEE_REPORTS_RETAINED_RAW_ARCHIVE",
    "COMMITTEE_REPORTS_SINCE",
    "COMMITTEE_REPORTS_UNTIL",
    "COMMITTEE_ROSTERS_DIRECTORY_REREAD",
    "COURTLISTENER_BULK_DIR",
    "COURT_CLUSTERS_MAX_REQUESTS",
    "CRS_REPORTS_RECONCILE",
    "CRS_REPORTS_SINCE",
    "FCC_PROCEEDINGS",
    "FCC_SINCE",
    "FEC_COMMITTEES_FULL_WALK",
    "FR_BODIES_MAX_NEW",
    "FR_BODIES_RETRY_FAILED",
    "FR_BODIES_SINCE",
    "FR_SINCE",
    "FR_TYPED_FORCE_REBUILD",
    "FR_UNTIL",
    "GAO_ALLOW_MASS_CLOSE_REASON",
    "GAO_DECISION_PAGES",
    "GAO_GOVINFO_HISTORY",
    "GAO_GOVINFO_MODS",
    "GAO_LISTING_RUN",
    "GAO_MAJOR_RULE_LETTERS",
    "GAO_MAJOR_RULE_OLD_INDEX_RUN",
    "GAO_MAJOR_RULE_RUN",
    "GAO_PRODUCT_PAGES",
    "GAO_PRODUCT_PAGES_UNDO",
    "GEMINI_MODEL",
    "LDA_FILING_YEAR",
    "LDA_POOL_PASSES",
    "LDA_RETAINED_MANIFEST",
    "LDA_RETAINED_MANIFEST_SHA256",
    "LDA_SINCE",
    "LDA_UNTIL",
    "PERSON_IDENTITY_DECISIONS_LOG",
    "PERSON_IDENTITY_OWNER_APPROVAL",
    "PRINT_CITATIONS_SINCE",
    "ROLL_CALL_CHAMBERS",
    "ROLL_CALL_MAX_VOTES",
    "SAM_INGEST_MODE",
    "SAM_MAX_RECORDS",
    "SAM_SINCE_YEAR",
    "SAM_UNTIL_YEAR",
    "SENATE_EXPENDITURES_SINCE",
    "SPICY_REGS_CONGRESSES",
    "SPICY_REGS_REQUIRE_BULK",
    "TREASURY_MTS_ISSUE_DATE",
    "USASPENDING_EVERY_FUNDED",
)


def file_scope_digest(path: Path | None) -> str | None:
    """Return a content SHA-256 for a regular file scope input, or its path string when not a file."""
    if path is None:
        return None
    if path.is_file():
        with path.open("rb") as stream:
            return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    return str(path)


def _resolve_contained_path(root: Path, relative: Any, *, label: str, expect_dir: bool = True) -> Path:
    """Resolve a relative path strictly inside ``root`` without symlinks or ``..`` traversal."""
    if not isinstance(relative, str) or not relative or relative.startswith(("/", "\\")):
        raise ResumeRefusedError(f"Invalid {label} path in resume binding")
    rel = Path(relative)
    if rel.is_absolute() or rel.drive or not rel.parts or any(part in ("", ".", "..") for part in rel.parts):
        raise ResumeRefusedError(f"Invalid {label} path in resume binding")
    current = root
    for part in rel.parts:
        current = current / part
        if current.is_symlink():
            raise ResumeRefusedError(f"Symlink refused in resume binding {label}: {relative}")
    root_resolved = root.resolve()
    resolved = current.resolve()
    if resolved == root_resolved or not resolved.is_relative_to(root_resolved):
        raise ResumeRefusedError(f"Resume binding {label} escapes output directory: {relative}")
    if expect_dir:
        if not current.is_dir():
            raise ResumeRefusedError(f"Resume binding {label} directory is missing: {relative}")
    elif not current.is_file():
        raise ResumeRefusedError(f"Resume binding {label} file is missing: {relative}")
    return current


class RollupPipeline(Pipeline):
    """A single materialized rollup, run standalone from the base tables on R2."""

    #: Base-table Parquet files this rollup reads (R2 remote keys, e.g.
    #: ``"documents.parquet"``). Primed from R2 before the build, and a
    #: missing one fails the run.
    inputs: ClassVar[tuple[str, ...]] = ()

    #: Published tables this rollup reads **best-effort** at merge time, to
    #: fill columns its own source does not carry (R2 remote keys). Not
    #: primed and never fatal: absence leaves those columns NULL and the
    #: rollup still publishes everything its own source establishes; a read
    #: that fetches one is recorded as a parent (see the module note). Each
    #: entry must name an *ingest* rollup's output whose cron runs before
    #: this one's — ``tests/test_hosted_rollups.py`` holds both.
    soft_inputs: ClassVar[tuple[str, ...]] = ()
    #: Public objects the transform reads over HTTP instead of downloading (for
    #: Parquet projection pushdown). Their storage versions are recorded as
    #: parents and must not change while the rollup builds.
    remote_inputs: ClassVar[tuple[str, ...]] = ()

    #: Zero or more artifacts a multi-output rollup writes and publishes (R2
    #: remote keys, same convention as ``output``). Leave empty and declare
    #: ``output`` directly for the (default) single-output case; declaring
    #: ``outputs`` instead picks up the ``output`` property below, which
    #: reads as ``outputs[0]`` for anything that only needs one key (e.g. the
    #: freshness checker).
    outputs: ClassVar[tuple[str, ...]] = ()

    #: A partial writer can update this existing complete family. Unchanged
    #: siblings are carried forward from its captured generation, never rebuilt.
    publication_family: ClassVar[str | None] = None
    generation_tables: ClassVar[bool] = True
    retain_source_evidence: ClassVar[bool] = False
    #: Explicit policies for every output. Builders write one shared receipt member in build_dir.
    receipt_policies: ClassVar[tuple] = ()
    #: Tables this rollup adds to its published family: the explicit migration publication requires before a
    #: family's table set may grow. Inert once they are published; never a way to drop one.
    added_tables: ClassVar[tuple[str, ...]] = ()
    receipt_only_tables: ClassVar[tuple[str, ...]] = ()
    #: Exact generation table keys exempt from the publication byte-size guard for this rollup.
    allow_shrink_tables: frozenset[str] = frozenset()
    #: Outputs stored as several files, by key, to their partition columns. The build returns each as a directory
    #: named for the table, holding ``<col>=<value>/part-NNNNNN.parquet`` (multi-file design §4.2).
    partitioned: ClassVar[Mapping[str, tuple[str, ...]]] = {}

    #: The single artifact this rollup writes and publishes (e.g.
    #: ``"feed_summary.parquet"``). Its R2 remote key is the same filename.
    #: Single-output rollups set this directly, which — because Python
    #: resolves a class attribute from the most-derived class first —
    #: shadows the ``output`` property below entirely; it never runs for them.
    output: ClassVar[str]

    @property
    def output(self) -> str:  # noqa: F811 — intentional redefinition, see docstring above
        """``outputs[0]``, for callers that only need this rollup's first/only key."""
        if self.outputs:
            return self.outputs[0]
        raise AttributeError(f"{type(self).__name__} must set 'output' or 'outputs'")

    def __init__(
        self,
        *,
        output_dir: Path | None = None,
        skip_upload: bool = True,
        resume: bool = False,
        require_resume_candidate: bool = False,
        allow_shrink_tables: frozenset[str] | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.skip_upload = skip_upload
        self.resume = resume
        self.require_resume_candidate = require_resume_candidate
        if allow_shrink_tables is not None:
            self.allow_shrink_tables = allow_shrink_tables
        self.source_evidence: CaptureEvidence | None = None
        #: A failure ``build`` met and built around (one source refused, the rest was read): ``run`` publishes the
        #: generation and then raises it, so the run still fails.
        self.deferred_failure: BaseException | None = None
        self.receipt_generation_id = uuid4().hex

    def invocation_scope(self) -> dict[str, Any]:
        """Deterministic, secret-free fingerprint of build-affecting invocation parameters."""
        env_scope: dict[str, str] = {}
        for name in ROLLUP_SCOPE_ENV_VARS:
            raw = getenv(name)
            if raw is not None and raw.strip():
                env_scope[name] = raw.strip()
        return {"env": env_scope}

    def run(self, *, read_operation=None) -> None:
        self._conversion_read_operation = read_operation
        if not self.generation_tables:
            # Docket search is a browser gzip object, not a table family. Keep
            # its existing single-object path until that consumer is migrated.
            output_dir = self.output_dir or (Path.cwd() / "output")
            output_dir.mkdir(parents=True, exist_ok=True)
            self._prime(output_dir)
            built = self.build(output_dir)
            if isinstance(built, tuple) or built.name != self.output or self.outputs:
                raise ValueError("Legacy object rollups must produce exactly their declared single output")
            if not self.skip_upload:
                r2.upload_file(built, remote_key=self.output)
            logger.info("Legacy unversioned object retained: {}", built)
            return

        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        resume_requested = self.resume or flag_env(RESUME_GENERATION_ENV)
        require_candidate = self.require_resume_candidate or flag_env(RESUME_REQUIRE_ENV)
        if resume_requested and self._try_resume_generation(output_dir, require_candidate=require_candidate):
            return

        if self.retain_source_evidence:
            self.source_evidence = _RollupEvidence(output_dir, self.publication_family or self.name)
        self._resume_checked = True
        try:
            self._run_tables(output_dir)
            if self.deferred_failure is not None:
                raise self.deferred_failure
        except BaseException as error:
            if self.source_evidence:
                self.source_evidence.finish(error)
            raise
        else:
            if self.source_evidence:
                self.source_evidence.finish()
        finally:
            self._resume_checked = False

    def generation_schemas(self):
        """The schemas this producer declares for generation admission."""
        from spicy_regs.data_dictionary import expected_schemas

        return expected_schemas()

    def generation_kwargs(self, out_paths):
        """Additional shared generation admission arguments supplied by an adopter."""
        return {}

    def _validate_resumed_candidate(self, gen_dir: Path, artifact) -> None:
        """Subclass hook for producer-specific gates that must hold before resuming a candidate."""

    def _verify_live_resume_inputs(
        self,
        *,
        output_dir: Path,
        public_url: str,
        live_index: dict,
        family: str,
        spec: Mapping,
        binding: Mapping,
        expected_keys: list[str],
        managed_receipt_parents: list[tuple[str, str, Mapping]],
        check_receipt_pins: bool,
    ) -> None:
        """Verify live prior, managed parents/receipts, soft/remote inputs, and receipt pins."""
        from spicy_regs.sources import publication

        live_family = live_index["families"].get(family)
        captured_snapshot = spec["readSnapshot"]
        captured_prior = captured_snapshot.get("families", {}).get(family) if captured_snapshot else None
        if publication._generation_entry(live_family) != publication._generation_entry(captured_prior):
            raise ResumeRefusedError(
                f"Live family {family!r} moved since retained candidate was built"
            )

        try:
            publication._assert_parents_current(live_index, spec.get("parents", {}))
        except PublicationError as exc:
            raise ResumeRefusedError(f"Managed parent changed since retained candidate was built: {exc}") from exc

        for receipt_key, parent_family, receipt_pin in managed_receipt_parents:
            live_parent = live_index["families"].get(parent_family)
            live_receipt = (live_parent or {}).get("etlReceipts")
            if (
                not isinstance(live_receipt, Mapping)
                or live_receipt.get("sha256") != receipt_pin["sha256"]
                or live_receipt.get("byteSize") != receipt_pin["byteSize"]
            ):
                raise ResumeRefusedError(
                    f"Managed parent receipts changed since retained candidate was built: {receipt_key}"
                )

        live_soft = self._observe_soft_inputs(live_index)
        if live_soft != binding["soft_inputs"]:
            raise ResumeRefusedError("Soft inputs changed since retained candidate was built")

        try:
            with (
                publication.with_snapshot(public_url, live_index),
                TemporaryDirectory(prefix=".resume-probe-", dir=output_dir) as probe_dir,
            ):
                live_remote = self._observe_remote_inputs(Path(probe_dir), public_url)
        except Exception as exc:
            raise ResumeRefusedError(f"Remote inputs could not be verified for resume: {exc}") from exc
        if live_remote != binding["remote_inputs"]:
            raise ResumeRefusedError("Remote inputs changed since retained candidate was built")

        if check_receipt_pins:
            try:
                publication.require_live_receipt_pins(
                    dict(live_index),
                    [key.removesuffix(".parquet") for key in expected_keys if key not in self.receipt_only_tables],
                )
            except PublicationError as exc:
                raise ResumeRefusedError(f"Live receipt archive pin requirement failed: {exc}") from exc

    def _observe_soft_inputs(self, index: Mapping) -> dict[str, dict | None]:
        """Current live table pin (or ``None``) for every declared soft input in ``index``."""
        from spicy_regs.sources import publication

        return {
            key: publication.table_pin(index, key) if publication.table_owner(index, key) is not None else None
            for key in self.soft_inputs
        }

    def _pipeline_class_name(self) -> str:
        return f"{type(self).__module__}.{type(self).__qualname__}"

    def _write_candidate_binding(
        self,
        *,
        output_dir: Path,
        family: str,
        destination: Path,
        artifact,
        expected_keys: tuple[str, ...],
        partitioned: Mapping[str, tuple[str, ...]],
        soft_inputs: Mapping[str, dict | None],
        remote_inputs: Mapping[str, dict],
        invocation_scope: Mapping,
    ) -> Path:
        """Persist non-hidden, non-authoritative build-scope binding before upload starts."""
        from rulespec_artifacts import canonical_json_bytes
        from spicy_regs.generations import validation_identity

        digest_hex = artifact.pin.artifact_digest.removeprefix("sha256:")
        binding_dir = output_dir / RESUME_CANDIDATES_DIR / family
        binding_dir.mkdir(parents=True, exist_ok=True)
        binding_path = binding_dir / f"{digest_hex}.json"
        deferred = (
            {"type": type(self.deferred_failure).__name__, "message": str(self.deferred_failure)}
            if self.deferred_failure is not None
            else None
        )
        payload = {
            "format": RESUME_BINDING_FORMAT,
            "version": RESUME_BINDING_VERSION,
            "family": family,
            "pipeline_class": self._pipeline_class_name(),
            "generation_directory": destination.relative_to(output_dir).as_posix(),
            "generation_pin": artifact.pin.as_dict(),
            "evidence_directory": (
                self.source_evidence.artifact_dir.relative_to(output_dir).as_posix()
                if self.source_evidence is not None
                else None
            ),
            "expected_keys": list(expected_keys),
            "added_tables": list(self.added_tables),
            "receipt_only_tables": list(self.receipt_only_tables),
            "partitioned": {key: list(cols) for key, cols in sorted(partitioned.items())},
            "retain_source_evidence": bool(self.retain_source_evidence),
            "validation_identity": validation_identity(),
            "invocation_scope": dict(invocation_scope),
            "soft_inputs": dict(soft_inputs),
            "remote_inputs": dict(remote_inputs),
            "deferred_failure": deferred,
        }
        raw = canonical_json_bytes(payload)
        temporary = binding_dir / f".{digest_hex}.{os.getpid()}.{uuid4().hex}.tmp"
        try:
            temporary.write_bytes(raw)
            temporary.replace(binding_path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return binding_path

    def _try_resume_generation(self, output_dir: Path, *, require_candidate: bool) -> bool:
        """Discover, cryptographically verify, and resume a retained candidate before any fetches or ``build()``.

        Returns ``True`` when a retained candidate was resumed/reconciled, ``False`` when no candidate exists
        and ``require_candidate`` is ``False`` (caller falls through to a fresh build). Any validation,
        identity, input, or live-prior drift raises :class:`ResumeRefusedError` fail-closed.
        """
        from rulespec_artifacts import ArtifactPin, LocalMemberSource, iter_member_descriptors
        from spicy_regs.generations import admit_generation_for_publication, implementation_id, validation_identity
        from spicy_regs.selected_generations import remember_generation
        from spicy_regs.source_evidence import INPUT_ROLE, verify_evidence
        from spicy_regs.sources import publication

        candidate_families = tuple(dict.fromkeys(fam for fam in (self.publication_family, self.name) if fam))
        bindings: list[Path] = []
        for fam in candidate_families:
            fam_dir = output_dir / RESUME_CANDIDATES_DIR / fam
            if fam_dir.is_symlink():
                raise ResumeRefusedError(f"Symlinked resume candidate directory refused: {fam_dir}")
            if fam_dir.is_dir():
                for candidate in sorted(fam_dir.glob("*.json")):
                    if candidate.is_symlink() or not candidate.is_file():
                        raise ResumeRefusedError(f"Invalid resume candidate binding entry: {candidate}")
                    bindings.append(candidate)

        if not bindings:
            if require_candidate:
                raise ResumeRefusedError(f"No retained generation candidate found for {self.name} in {output_dir}")
            logger.info("Rollup {}: no retained generation candidate found; running fresh build", self.name)
            return False
        if len(bindings) > 1:
            names = ", ".join(path.name for path in bindings)
            raise ResumeRefusedError(
                f"Multiple retained generation candidates found for {self.name} ({names}); "
                "clean output_dir before retrying"
            )

        binding_path = bindings[0]
        try:
            if binding_path.stat().st_size > _BINDING_LIMIT:
                raise ResumeRefusedError(f"Resume binding exceeds byte limit: {binding_path.name}")
            binding = json.loads(binding_path.read_bytes())
        except ResumeRefusedError:
            raise
        except (OSError, ValueError, TypeError) as exc:
            raise ResumeRefusedError(f"Unreadable resume binding {binding_path.name}: {exc}") from exc

        if not isinstance(binding, dict) or set(binding) != _BINDING_KEYS:
            raise ResumeRefusedError(f"Malformed resume binding keys in {binding_path.name}")
        if binding["format"] != RESUME_BINDING_FORMAT or binding["version"] != RESUME_BINDING_VERSION:
            raise ResumeRefusedError(f"Unsupported resume binding format/version in {binding_path.name}")

        family = binding["family"]
        if not isinstance(family, str) or family not in candidate_families or binding_path.parent.name != family:
            raise ResumeRefusedError(f"Resume binding family mismatch: {family!r}")
        if binding["pipeline_class"] != self._pipeline_class_name():
            raise ResumeRefusedError(
                f"Resume binding pipeline class {binding['pipeline_class']!r} != {self._pipeline_class_name()!r}"
            )
        if binding["added_tables"] != list(self.added_tables):
            raise ResumeRefusedError("Resume binding added_tables differs from current pipeline declaration")
        if binding["receipt_only_tables"] != list(self.receipt_only_tables):
            raise ResumeRefusedError("Resume binding receipt_only_tables differs from current pipeline declaration")
        if binding["retain_source_evidence"] is not bool(self.retain_source_evidence):
            raise ResumeRefusedError("Resume binding retain_source_evidence differs from current pipeline declaration")

        own_keys = [key for key in (self.outputs or (self.output,)) if key not in self.receipt_only_tables]
        own_partitioned = {key: list(cols) for key, cols in sorted(self.partitioned.items())}
        expected_keys = binding["expected_keys"]
        partitioned = binding["partitioned"]
        if not isinstance(expected_keys, list) or not all(isinstance(k, str) for k in expected_keys):
            raise ResumeRefusedError("Invalid expected_keys in resume binding")
        if not isinstance(partitioned, dict):
            raise ResumeRefusedError("Invalid partitioned mapping in resume binding")
        if self.publication_family is None:
            if expected_keys != own_keys or partitioned != own_partitioned:
                raise ResumeRefusedError("Resume binding outputs/partitioned differ from current pipeline declaration")
        else:
            if not set(own_keys) <= set(expected_keys) or any(
                partitioned.get(k) != v for k, v in own_partitioned.items()
            ):
                raise ResumeRefusedError("Resume binding partial-writer outputs differ from current declaration")

        if not isinstance(binding["soft_inputs"], dict) or not isinstance(binding["remote_inputs"], dict):
            raise ResumeRefusedError("Invalid soft_inputs/remote_inputs in resume binding")

        deferred_raw = binding["deferred_failure"]
        deferred_error: RuntimeError | None = None
        if deferred_raw is not None:
            if (
                not isinstance(deferred_raw, dict)
                or set(deferred_raw) != {"type", "message"}
                or not isinstance(deferred_raw["type"], str)
                or not isinstance(deferred_raw["message"], str)
            ):
                raise ResumeRefusedError("Invalid deferred_failure descriptor in resume binding")
            deferred_error = RuntimeError(
                f"Resumed generation carries deferred build failure ({deferred_raw['type']}): "
                f"{deferred_raw['message']}"
            )

        if binding["validation_identity"] != validation_identity():
            raise ResumeRefusedError("Producer/runtime validation_identity changed since retained candidate was built")
        if binding["invocation_scope"] != self.invocation_scope():
            raise ResumeRefusedError("Invocation scope changed since retained candidate was built")

        pin_raw = binding["generation_pin"]
        if (
            not isinstance(pin_raw, dict)
            or set(pin_raw) != {"logicalId", "artifactDigest"}
            or not isinstance(pin_raw["logicalId"], str)
            or not pin_raw["logicalId"]
            or not isinstance(pin_raw["artifactDigest"], str)
            or _DIGEST.fullmatch(pin_raw["artifactDigest"]) is None
        ):
            raise ResumeRefusedError("Invalid generation_pin in resume binding")
        digest_hex = pin_raw["artifactDigest"].removeprefix("sha256:")
        if binding_path.stem != digest_hex:
            raise ResumeRefusedError("Resume binding filename does not match generation_pin digest")

        gen_dir = _resolve_contained_path(output_dir, binding["generation_directory"], label="generation_directory")
        if gen_dir.name != digest_hex:
            raise ResumeRefusedError("Resume binding generation_directory does not match generation_pin digest")

        expected_pin = ArtifactPin(pin_raw["logicalId"], pin_raw["artifactDigest"])
        read_operation = getattr(self, "_conversion_read_operation", None)
        try:
            if read_operation is not None:
                artifact, _ = read_operation.admit_generation(gen_dir, expected_pin=expected_pin)
            else:
                artifact = admit_generation_for_publication(gen_dir, expected_pin=expected_pin)
        except Exception as exc:
            raise ResumeRefusedError(f"Retained generation failed cryptographic admission: {exc}") from exc

        if artifact.root.get("producer", {}).get("implementationId") != implementation_id():
            raise ResumeRefusedError("Producer implementationId changed since retained candidate was built")
        spec = artifact.root["spec"]
        if spec["family"] != family or set(spec["tables"]) != set(expected_keys):
            raise ResumeRefusedError("Admitted generation family/tables differ from resume binding")
        if not self.skip_upload and spec["publicationStatus"] != "complete-family":
            raise ResumeRefusedError("Cannot resume a local-partial generation for live publication")

        declared_evidence = [item for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE]
        evidence_dir: Path | None = None
        if self.retain_source_evidence:
            if len(declared_evidence) != 1:
                raise ResumeRefusedError("Admitted generation is missing its declared source-evidence pin")
            evidence_dir = _resolve_contained_path(
                output_dir, binding["evidence_directory"], label="evidence_directory"
            )
            ev_pin = ArtifactPin(declared_evidence[0]["logicalId"], declared_evidence[0]["artifactDigest"])
            try:
                ev_artifact = verify_evidence(evidence_dir, expected_pin=ev_pin)
            except Exception as exc:
                raise ResumeRefusedError(f"Retained source evidence failed verification: {exc}") from exc
            if ev_artifact.root["spec"].get("outcome") != "build-complete" or ev_artifact.root["spec"].get("family") != family:
                raise ResumeRefusedError("Retained source evidence is not a build-complete artifact for this family")
        else:
            if binding["evidence_directory"] is not None or declared_evidence:
                raise ResumeRefusedError("Unexpected source evidence attached to non-evidence rollup resume binding")

        parents_spec = spec.get("parents", {})
        captured_snapshot = spec["readSnapshot"]
        managed_receipt_parents: list[tuple[str, str, Mapping]] = []
        for parent_key, parent in parents_spec.items():
            if "family" not in parent and "sha256" in parent:
                if parent_key.endswith("/etl_receipts.parquet"):
                    dataset = parent_key.removesuffix("/etl_receipts.parquet")
                    companion = parents_spec.get(f"{dataset}.parquet")
                    if isinstance(companion, Mapping) and isinstance(companion.get("family"), str):
                        managed_receipt_parents.append((parent_key, companion["family"], parent))
                        continue
                    receipt_owners = [
                        fam_name
                        for fam_name, fam_entry in (captured_snapshot or {}).get("families", {}).items()
                        if dataset in fam_entry.get("etlReceipts", {}).get("datasets", ())
                    ]
                    if len(receipt_owners) == 1:
                        managed_receipt_parents.append((parent_key, receipt_owners[0], parent))
                        continue
                local_parent = _resolve_contained_path(
                    output_dir, parent_key, label=f"parent {parent_key}", expect_dir=False
                )
                with local_parent.open("rb") as stream:
                    actual_sha = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
                if actual_sha != parent["sha256"] or local_parent.stat().st_size != parent["byteSize"]:
                    raise ResumeRefusedError(f"Unmanaged local parent changed since candidate build: {parent_key}")

        self._validate_resumed_candidate(gen_dir, artifact)

        public_url = getenv("R2_PUBLIC_URL")
        if self.skip_upload:
            if public_url:
                with publication.snapshot(public_url) as live_index:
                    source = (
                        read_operation.verified_source(gen_dir)[1]
                        if read_operation is not None
                        else LocalMemberSource(gen_dir)
                    )
                    expected_entry = publication.generation_entry(
                        artifact, list(iter_member_descriptors(artifact, source))
                    )
                    live_family = live_index["families"].get(family)
                    if publication._generation_entry(live_family) != expected_entry:
                        self._verify_live_resume_inputs(
                            output_dir=output_dir,
                            public_url=public_url,
                            live_index=live_index,
                            family=family,
                            spec=spec,
                            binding=binding,
                            expected_keys=expected_keys,
                            managed_receipt_parents=managed_receipt_parents,
                            check_receipt_pins=False,
                        )
            elif self.remote_inputs or binding["remote_inputs"]:
                try:
                    observed_remote = self._observe_remote_inputs(output_dir, None)
                except Exception as exc:
                    raise ResumeRefusedError(f"Remote inputs could not be verified for resume: {exc}") from exc
                if observed_remote != binding["remote_inputs"]:
                    raise ResumeRefusedError("Remote inputs changed since retained candidate was built")
            logger.info("Verified retained local generation {}; upload skipped (resumed)", gen_dir)
            remember_generation(output_dir, gen_dir, artifact)
            if deferred_error is not None:
                self.deferred_failure = deferred_error
                raise deferred_error
            return True

        r2.require_credentials("Generation publication")
        if not public_url:
            raise RuntimeError("Generation publication requires R2_PUBLIC_URL")
        client = r2.get_r2_client()
        bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
        live_index, _ = publication._stored_index(client, bucket)

        if read_operation is not None:
            artifact, source = read_operation.verified_source(gen_dir)
        else:
            source = LocalMemberSource(gen_dir)
        expected_entry = publication.generation_entry(artifact, list(iter_member_descriptors(artifact, source)))
        live_family = live_index["families"].get(family)

        from spicy_regs.sources.cloudflare import purge_urls

        if publication._generation_entry(live_family) == expected_entry:
            logger.info(
                "Rollup {}: retained generation {} is already live on R2; reconciling without re-upload",
                self.name,
                artifact.pin.artifact_digest,
            )
            purge_urls([f"{public_url.rstrip('/')}/{publication.INDEX_V2_KEY}"])
            remember_generation(output_dir, gen_dir, artifact)
            if deferred_error is not None:
                self.deferred_failure = deferred_error
                raise deferred_error
            binding_path.unlink(missing_ok=True)
            logger.info("Done!")
            return True

        self._verify_live_resume_inputs(
            output_dir=output_dir,
            public_url=public_url,
            live_index=live_index,
            family=family,
            spec=spec,
            binding=binding,
            expected_keys=expected_keys,
            managed_receipt_parents=managed_receipt_parents,
            check_receipt_pins=True,
        )

        prior_for_publish = captured_snapshot if captured_snapshot else publication.empty_index()
        publish = publication.publish_generation
        arguments: dict[str, Any] = {}
        if read_operation is not None:
            publish = publication._publish_verified_generation
            arguments = {
                "artifact": artifact,
                "source": source,
                "local": gen_dir,
                "upload_member": lambda prefix, key: publication._put_immutable(
                    client, bucket, prefix + "/" + key, gen_dir / key
                ),
            }
        else:
            arguments = {"directory": gen_dir}
        logger.info("Rollup {}: resuming publication of verified generation {}", self.name, gen_dir)
        publish(
            **arguments,
            client=client,
            bucket=bucket,
            prior_index=prior_for_publish,
            evidence_directories=(evidence_dir,) if evidence_dir is not None else (),
            added_tables=frozenset(self.added_tables),
            receipt_only_tables=frozenset(self.receipt_only_tables),
            allow_shrink_tables=frozenset(self.allow_shrink_tables),
        )
        purge_urls([f"{public_url.rstrip('/')}/{publication.INDEX_V2_KEY}"])
        remember_generation(output_dir, gen_dir, artifact)
        if deferred_error is not None:
            self.deferred_failure = deferred_error
            raise deferred_error
        binding_path.unlink(missing_ok=True)
        logger.info("Done!")
        return True

    def _selection_digest(self, invocation_scope: Mapping, soft_inputs: Mapping, remote_inputs: Mapping) -> str:
        from rulespec_artifacts import canonical_json_bytes

        scope = {
            "pipeline": self._pipeline_class_name(),
            "scope": invocation_scope,
            "softInputs": soft_inputs,
            "remoteInputs": remote_inputs,
        }
        return "sha256:" + hashlib.sha256(canonical_json_bytes(scope)).hexdigest()

    def _compare_evidence(self, selection: str, *, early: bool) -> bool:
        """Compare the current journal to the pinned prior, never to a mutable local cache."""
        evidence = self.source_evidence
        if (
            not isinstance(evidence, _RollupEvidence)
            or self.deferred_failure is not None
            or (early and evidence._used_input_scope is not None)
        ):
            return False
        current = _evidence_records(_journal_events((evidence.artifact_dir / JOURNAL).read_bytes()))
        prior = evidence.prior_events()
        if not any(
            event["event"] == _SELECTION_EVENT and event.get("version") == 1 and event.get("sha256") == selection
            for event in prior
        ):
            return False
        previous = _evidence_records(prior)
        # An early-return builder did not replay the whole previous build. Its
        # observations are additions to inherited coverage, not a replacement.
        if not early:
            return current == previous
        # In particular, do not hide a refusal that stopped recurring.
        if [record for refused, record in current if refused] != [record for refused, record in previous if refused]:
            return False
        previous_iter = iter(previous)
        return all(record in previous_iter for record in current)

    def _report_unchanged(self, prior_index: dict, family: str, parents: Mapping, started: float) -> None:
        from spicy_regs.sources import publication

        if not self.skip_upload:
            r2.require_credentials("Generation unchanged verification")
            live_index, _ = publication._stored_index(r2.get_r2_client(), getenv("R2_BUCKET_NAME", "spicy-regs"))
            publication._assert_family_unchanged(live_index, prior_index, family, exact=True)
            publication._assert_parents_current(live_index, parents)
            if self._observe_soft_inputs(live_index) != self._observe_soft_inputs(prior_index):
                raise publication.PublicationError("Soft inputs moved while checking for unchanged outputs")
        prior = prior_index["families"][family]
        logger.info(
            "Rollup {}: zero delta; skipped generation publish ({} table bytes not staged, {:.3f}s)",
            self.name, sum(table["byteSize"] for table in prior["tables"].values()), monotonic() - started,
        )

    def _carry_unchanged(
        self, signal: Unchanged, build_dir: Path, expected_keys: tuple[str, ...], *,
        prior_index: dict, family: str, parents: Mapping, selection: str, public_url: str | None, started: float,
    ) -> tuple[Path, ...] | None:
        """Keep the pinned tables when an early-return builder has new evidence to publish."""
        from spicy_regs.generations import implementation_id
        from spicy_regs.sources import publication

        evidence = self.source_evidence
        if not isinstance(evidence, _RollupEvidence) or (self.skip_upload and evidence.prior_root is None):
            # Read-only/ETL delegation, or an offline probe: the builder
            # supplied no candidate and there is no pinned source baseline.
            logger.info("Rollup {}: {}; no candidate built", self.name, signal)
            return None
        prior = prior_index["families"].get(family)
        if (
            prior is None or public_url is None or prior.get("etlReceipts")
            or self.receipt_policies or set(expected_keys) != set(prior["tables"])
        ):
            raise publication.PublicationError("Unchanged hint cannot carry a different table or receipt family")
        if evidence.prior_root is None:
            raise publication.PublicationError("Unchanged hint has no verified prior root")
        if (
            evidence.prior_root["producer"]["implementationId"] == implementation_id()
            and evidence.prior_root["spec"].get("parents", {}) == parents
            and self._compare_evidence(selection, early=True)
        ):
            evidence.seal(outcome="build-complete")
            self._report_unchanged(prior_index, family, parents, started)
            return None
        pin = evidence.checkpoint_pin()
        if pin is not None:
            evidence.event(_CARRIED_EVIDENCE_EVENT, pin=pin)
        directory = build_dir / ".unchanged-tables"
        directory.mkdir()
        for key in expected_keys:
            r2.download_members(key, directory)
        return tuple(directory / (key.removesuffix(".parquet") if key in self.partitioned else key)
                     for key in expected_keys)

    def _run_tables(self, output_dir: Path) -> None:
        from rulespec_artifacts import LocalMemberSource, iter_member_descriptors, publish_directory_no_replace
        from spicy_regs.generations import build_generation, output_keys, verify_generation
        from spicy_regs.sources import publication

        started = monotonic()
        read_operation = getattr(self, "_conversion_read_operation", None)

        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        if not getattr(self, "_resume_checked", False):
            resume_requested = self.resume or flag_env(RESUME_GENERATION_ENV)
            require_candidate = self.require_resume_candidate or flag_env(RESUME_REQUIRE_ENV)
            if resume_requested and self._try_resume_generation(output_dir, require_candidate=require_candidate):
                return
        public_url = getenv("R2_PUBLIC_URL")
        context = publication.snapshot(public_url) if public_url else nullcontext(publication.empty_index())
        with context as prior_index:
            publication.require_live_receipt_pins(
                dict(prior_index),
                [
                    key.removesuffix(".parquet")
                    for key in (self.outputs or (self.output,))
                    if key not in self.receipt_only_tables
                ],
            )
            soft_inputs = self._observe_soft_inputs(prior_index)
            invocation_scope = self.invocation_scope()
            if self.source_evidence:
                self.source_evidence.inherit(prior_index, public_url=public_url)
            # Bespoke ingest builders also cache priors. An isolated build
            # directory covers all of them without deleting user evidence.
            build_dir = output_dir
            if public_url:
                build_dir = output_dir / ".builds" / uuid4().hex
                build_dir.mkdir(parents=True)
            with r2.recorded_reads() as reads:
                parents = self._prime(build_dir, prior_index)
                remote = self._observe_remote_inputs(build_dir, public_url)
                logger.info("Building rollup {}...", self.output)
                self._defer_native_selection = True
                unchanged = None
                try:
                    built = self.build(build_dir)
                except Unchanged as signal:
                    unchanged = signal
                    built = ()
                finally:
                    self._defer_native_selection = False
            if self._observe_remote_inputs(build_dir, public_url) != remote:
                raise publication.PublicationError("A remote input changed while the rollup built; rebuild")
            if self.invocation_scope() != invocation_scope:
                raise publication.PublicationError("Invocation scope changed while the rollup built; rebuild")
            # Every other family's table the build fetched is a parent, read soft or undeclared included.
            parents |= remote | {key: read for key, read in reads.items()
                                 if key not in parents and not self._own(key, read.get("family"))}
            family = self.publication_family or self.name
            prior = prior_index["families"].get(family)
            selection = self._selection_digest(invocation_scope, soft_inputs, remote)
            if self.source_evidence is not None:
                self.source_evidence.event(_SELECTION_EVENT, version=1, sha256=selection)
                if self.deferred_failure is not None:
                    self.source_evidence.refusal(self.deferred_failure, stage="rollup-deferred-failure")
            out_paths = built if isinstance(built, tuple) else (built,)
            expected_keys = tuple(
                key for key in (self.outputs or (self.output,)) if key not in self.receipt_only_tables
            )
            if unchanged is not None:
                carried_paths = self._carry_unchanged(
                    unchanged, build_dir, expected_keys, prior_index=prior_index, family=family, parents=parents,
                    selection=selection, public_url=public_url, started=started,
                )
                if carried_paths is None:
                    return
                out_paths = carried_paths
            if len(out_paths) != len(expected_keys) or set(output_keys(out_paths, self.partitioned)) != set(
                expected_keys
            ):
                raise publication.PublicationError("Build outputs differ from its declared complete set")
            carried_forward = {
                key: prior["artifactDigest"] for key in expected_keys if key not in self.partitioned
            } if unchanged is not None and prior is not None else {}
            partitioned = dict(self.partitioned)
            publication_status = "complete-family"
            if self.publication_family:
                prior = prior_index["families"].get(self.publication_family)
                if prior is None:
                    if not self.skip_upload:
                        raise publication.PublicationError(
                            f"Publish a complete {self.publication_family} generation before its partial writer"
                        )
                    logger.warning("Retaining a local partial candidate; no complete publication family exists")
                    publication_status = "local-partial"
                    family = self.name
                else:
                    family = self.publication_family
                    expected_keys = tuple(prior["tables"])
                    written = set(output_keys(out_paths, self.partitioned))
                    if not written <= set(expected_keys):
                        raise publication.PublicationError("Partial writer output is outside its existing family")
                    for key in expected_keys:
                        if key in written:
                            continue
                        descriptor = prior["tables"][key]
                        if "members" in descriptor:
                            # A split sibling is carried member for member, each checked against its pin, and is
                            # not declared carried whole: publication copies every unchanged member server-side.
                            r2.download_members(key, build_dir)
                            out_paths += (build_dir / key.removesuffix(".parquet"),)
                            partitioned[key] = tuple(descriptor["partitionColumns"])
                            continue
                        path = build_dir / key
                        if not r2.download(key, path):
                            raise publication.PublicationError(f"Missing carried-forward member: {key}")
                        out_paths += (path,)
                        carried_forward[key] = prior["artifactDigest"]
        generations = output_dir / "generations"
        generations.mkdir(exist_ok=True)
        with TemporaryDirectory(prefix=".generation-", dir=output_dir) as staging:
            directory = Path(staging) / "artifact"
            artifact = build_generation(
                directory,
                family=family,
                files=out_paths,
                expected_keys=expected_keys,
                schemas=self.generation_schemas(),
                read_snapshot=prior_index,
                carried_forward=carried_forward,
                publication_status=publication_status,
                inputs=self.source_evidence.inputs() if self.source_evidence else (),
                parents=parents,
                partitioned=partitioned,
                read_operation=read_operation,
                canonical_root=generations if read_operation is not None else None,
                **self.generation_kwargs(out_paths),
                **(
                    {
                        "receipt_path": build_dir / "etl_receipts.parquet",
                        "receipt_policies": self.receipt_policies,
                        "receipt_generation_id": self.receipt_generation_id,
                    }
                    if self.receipt_policies
                    else {}
                ),
            )
            destination = generations / artifact.pin.artifact_digest.removeprefix("sha256:")
            evidence = self.source_evidence
            prior_root = evidence.prior_root if isinstance(evidence, _RollupEvidence) else None
            # An owned conversion needs its retained candidate even when public tables happen to match.
            if (
                unchanged is None and prior is not None and prior_root is not None and read_operation is None
                and not invocation_scope.get("administration_policy", {}).get("force_publish")
            ):
                source = LocalMemberSource(directory)
                candidate = publication.generation_entry(artifact, iter_member_descriptors(artifact, source))
                if (
                    candidate["tables"] == prior["tables"]
                    and candidate.get("etlReceipts") == prior.get("etlReceipts")
                    and prior_root["producer"] == artifact.root["producer"]
                    and prior_root["spec"]["packages"] == artifact.root["spec"]["packages"]
                    and prior_root["spec"].get("etlReceipts") == artifact.root["spec"].get("etlReceipts")
                    and prior_root["spec"].get("parents", {}) == parents
                    and self._compare_evidence(selection, early=False)
                ):
                    self._report_unchanged(prior_index, family, parents, started)
                    return
            if destination.exists():
                if read_operation is None:
                    artifact = verify_generation(destination, expected_pin=artifact.pin)
                else:
                    artifact, _, _ = read_operation.admit_generation(destination, expected_pin=artifact.pin)
            else:
                publish_directory_no_replace(directory, destination)
        from spicy_regs.source_catalog.input_proof import save_input_proof
        from spicy_regs.source_evidence import EvidenceMemberSource

        evidence = ((EvidenceMemberSource(self.source_evidence.artifact_dir),
                     self.source_evidence.artifact),) if self.source_evidence else ()
        save_input_proof(
            output_dir / "generation-inputs" / family / artifact.pin.artifact_digest.removeprefix("sha256:"),
            artifact, LocalMemberSource(destination), evidence,
        )
        binding_path = self._write_candidate_binding(
            output_dir=output_dir,
            family=family,
            destination=destination,
            artifact=artifact,
            expected_keys=expected_keys,
            partitioned=partitioned,
            soft_inputs=soft_inputs,
            remote_inputs=remote,
            invocation_scope=invocation_scope,
        )
        if self.skip_upload:
            logger.info("Verified local generation {}; upload skipped", destination)
        else:
            r2.require_credentials("Generation publication")
            if not public_url:
                raise RuntimeError("Generation publication requires R2_PUBLIC_URL")
            publish = publication.publish_generation
            arguments: dict[str, Any] = {}
            if read_operation is not None:
                artifact, source = read_operation.verified_source(destination)
                publish = publication._publish_verified_generation
                arguments = {"artifact": artifact, "source": source, "local": destination,
                             "upload_member": lambda prefix, key: publication._put_immutable(
                                 r2.get_r2_client(), getenv("R2_BUCKET_NAME", "spicy-regs"),
                                 prefix + "/" + key, destination / key)}
            else:
                arguments = {"directory": destination}
            publish(
                **arguments,
                client=r2.get_r2_client(),
                bucket=getenv("R2_BUCKET_NAME", "spicy-regs"),
                prior_index=prior_index,
                evidence_directories=(self.source_evidence.artifact_dir,) if self.source_evidence else (),
                added_tables=frozenset(self.added_tables),
                receipt_only_tables=frozenset(self.receipt_only_tables),
                allow_shrink_tables=frozenset(self.allow_shrink_tables),
            )
            from spicy_regs.sources.cloudflare import purge_urls

            purge_urls([f"{public_url.rstrip('/')}/{publication.INDEX_V2_KEY}"])
        from spicy_regs.selected_generations import remember_generation

        remember_generation(output_dir, destination, artifact)
        if not self.skip_upload and self.deferred_failure is None:
            binding_path.unlink(missing_ok=True)
        logger.info("Done!")

    def _own(self, key: str, family: str | None) -> bool:
        """Whether ``key`` is this rollup's own table: its prior is a prior-generation input, never a parent."""
        return family == (self.publication_family or self.name) or key in (self.outputs or (self.output,))

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        """Download each required base table from R2 unless already local; return the parents read.

        A missing base table is fatal: a rollup built from absent inputs would
        publish a truncated artifact that the upload shrink-guard would then
        (correctly) reject — better to fail loudly here with a clear message.
        Each parent records the digest and size of the bytes read, plus its
        family and generation when a managed family published it. A verified
        managed download reuses its descriptor; any other file is hashed once.
        The rollup's own table is primed but is no parent (:meth:`_own`).
        """
        if snapshot is not None:
            from spicy_regs.sources import publication

            publication.require_live_receipt_pins(
                dict(snapshot),
                [
                    key.removesuffix(".parquet")
                    for key in (self.outputs or (self.output,))
                    if key not in self.receipt_only_tables
                ],
            )
        return self._prime_tables(self.inputs, output_dir, snapshot)

    def _prime_tables(self, keys: tuple[str, ...], output_dir: Path, snapshot: Mapping | None) -> dict[str, dict]:
        """:meth:`_prime` for ``keys``: each published table downloaded, pinned and returned as a parent."""
        from spicy_regs.sources import publication

        parents = {}
        for remote_key in keys:
            local = output_dir / remote_key
            downloaded = False
            if local.exists():
                logger.info("Using local {} (skipping download)", remote_key)
            elif not r2.download(remote_key, local):
                raise RuntimeError(
                    f"Rollup {self.output!r}: required base table {remote_key!r} "
                    f"not found on R2 and not present locally in {output_dir}"
                )
            else:
                downloaded = True
            owner = publication.table_owner(snapshot or publication.empty_index(), remote_key)
            if self._own(remote_key, owner[0] if owner else None):
                continue
            descriptor = owner[1]["tables"][remote_key] if owner else None
            if downloaded and descriptor:
                parent = {"sha256": descriptor["sha256"], "byteSize": descriptor["byteSize"]}
            else:
                with local.open("rb") as stream:
                    parent = {
                        "sha256": "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest(),
                        "byteSize": local.stat().st_size,
                    }
            if owner:
                parent |= {"family": owner[0], "artifactDigest": owner[1]["artifactDigest"]}
            parents[remote_key] = parent
        return parents

    def _observe_remote_inputs(self, output_dir: Path, public_url: str | None) -> dict[str, dict]:
        """The storage version of each remote input, or the digest of a local copy that replaces it."""
        from spicy_regs.sources import publication

        observed = {}
        index: dict | None = None
        for remote_key in self.remote_inputs:
            local = output_dir / remote_key
            if local.exists():
                with local.open("rb") as stream:
                    observed[remote_key] = {
                        "sha256": "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest(),
                        "byteSize": local.stat().st_size,
                    }
                continue
            if not public_url:
                raise RuntimeError(f"Rollup {self.output!r}: {remote_key} needs R2_PUBLIC_URL or a local copy")
            if index is None:
                index = publication.current_index(public_url)
            member = publication.single_member(index, remote_key)
            version = r2.public_object_version(f"{public_url.rstrip('/')}/{member.path}")
            if version is None:
                raise RuntimeError(f"Rollup {self.output!r}: remote input {remote_key!r} is missing")
            if publication.table_owner(index, remote_key) is not None:
                observed[remote_key] = publication.table_pin(index, remote_key)
            else:
                observed[remote_key] = {"etag": version["etag"], "byteSize": version["bytes"]}
        return observed

    @abstractmethod
    def build(self, output_dir: Path) -> Path | tuple[Path, ...]:
        """Materialize the rollup into ``output_dir``.

        Return the single artifact's path, or — for a rollup that declares
        ``outputs`` — a tuple of paths, one per declared key, in any order.
        """
        ...


def make_rollup_app(pipeline_cls: type[RollupPipeline]) -> App:
    """Build the ``run-rollup-*`` cyclopts CLI for one rollup pipeline.

    All rollups share the same two flags, so each module just does::

        app = make_rollup_app(FeedSummaryRollup)
    """
    app = App(
        name=f"run-rollup-{pipeline_cls.name}",
        help=(pipeline_cls.__doc__ or "").strip().splitlines()[0] if pipeline_cls.__doc__ else "",
    )

    @app.default
    def main(
        *,
        output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
        skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
        resume: Annotated[
            bool, Parameter(help="Resume a verified retained generation candidate before rebuilding")
        ] = False,
    ) -> None:
        load_dotenv()
        pipeline_cls(output_dir=output_dir, skip_upload=skip_upload, resume=resume).run()

    return app
