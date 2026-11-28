"""Hosted refreshes of the source families that were refreshed by hand.

Each rollup acquires through its family pipeline's own capture functions with the run's source evidence, so
every downloaded byte is in that evidence and is uploaded to R2 before the family pointer moves: a hosted
runner has no shared raw archive, and its evidence is where its downloads are kept. A run that finds nothing
the published generation lacks raises :class:`Unchanged` and publishes nothing.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
import hashlib
from os import getenv
from pathlib import Path

from loguru import logger

from spicy_regs.env_values import flag_env
from spicy_regs.pipelines import (
    administration_policy, congress_sessions, executive_orders, fec_financial_profiles, fec_research_profiles,
    grants_opportunities, house_disbursements, ofac, sector_comparisons, treasury_fiscal,
)
from spicy_regs.pipelines.raw_releases import (
    journal_snapshot_releases, published_before, restore_originals, snapshot_readers,
)
from spicy_regs.pipelines.rollups.base import RollupPipeline, Unchanged, make_rollup_app
from spicy_regs.schemas.house_disbursements import PARTITION_COLUMNS as HOUSE_PARTITIONS, TABLES as HOUSE_TABLES
from spicy_regs.sources import publication, r2, retained_collections
from spicy_regs.transforms import (
    build_congress_sessions, build_executive_orders, build_fec_financial_profiles, build_grants_opportunities,
    build_ofac, build_treasury_fiscal, sector_comparisons as sector_comparisons_transform,
)

#: How many days back a Grants.gov run looks for the newest daily export before it reports the source as stalled.
GRANTS_LOOKBACK_DAYS = 7


class SourceRefreshRollup(RollupPipeline):
    """A rollup whose build acquires its own source and reads its own published generation."""

    retain_source_evidence = True
    #: An injected HTTP transport for the source requests; ``None`` reaches the publisher.
    transport = None

    @property
    def evidence(self):
        """The run's source evidence, which ``run`` opens before it calls ``build``."""
        if self.source_evidence is None:
            raise RuntimeError(f"{self.name}: build needs the source evidence run() opens")
        return self.source_evidence

    def published_tables(self, directory: Path) -> list[Path]:
        """This family's published tables, each fetched against its pin into ``directory``."""
        directory.mkdir()
        paths = [directory / key for key in (self.outputs or (self.output,))]
        for path in paths:
            if not r2.download(path.name, path):
                raise RuntimeError(f"{self.name}: published {path.name} is required to continue the family")
        return paths


class OfacSanctionsRollup(SourceRefreshRollup):
    """Publish the current OFAC SDN and consolidated exports as the family's two snapshots."""

    name = "ofac-sanctions"
    outputs = tuple(f"{table}.parquet" for table in build_ofac.SCHEMAS)

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        from spicy_docs.sources.ofac_profile import OFAC_PROFILE, iter_retained_pages

        evidence = self.evidence
        scopes = ofac.capture(evidence, ("sdn", "consolidated"), transport=self.transport)
        if all(published_before(evidence, ofac.STAGE, scope["capture"]["response_sha256"]) for scope in scopes):
            raise Unchanged("both exports are the published ones")
        readers, _ = snapshot_readers(OFAC_PROFILE, iter_retained_pages, scopes, output_dir / "source-releases",
                                      store=evidence.store)
        journal_snapshot_releases(evidence, readers, ofac.RELEASE_EVENT)
        return tuple(build_ofac.build_ofac(readers, output_dir))


class GrantsOpportunitiesRollup(SourceRefreshRollup):
    """Publish the newest Grants.gov daily export as the family's one snapshot."""

    name = "grants-opportunities"
    output = build_grants_opportunities.OUTPUT

    def newest_export(self, today: date) -> str | None:
        """The newest export date the publisher holds within the lookback, newest first."""
        days = (today - timedelta(days=back) for back in range(GRANTS_LOOKBACK_DAYS))
        return next((day.isoformat() for day in days
                     if grants_opportunities.stated_version(day.isoformat(), transport=self.transport)), None)

    def build(self, output_dir: Path) -> Path:
        from spicy_docs.sources.grants_gov_profile import GRANTS_GOV_PROFILE, iter_retained_pages

        evidence = self.evidence
        newest = self.newest_export(datetime.now(UTC).date())
        if newest is None:
            raise RuntimeError(f"Grants.gov holds no daily export from the last {GRANTS_LOOKBACK_DAYS} days")
        published = evidence.inherited_event(grants_opportunities.RELEASE_EVENT)
        if published and newest <= published["collection_outcome"]["requestedScope"]["snapshot_date"]:
            raise Unchanged(f"the newest export, {newest}, is published")
        scopes = grants_opportunities.capture(evidence, [newest], transport=self.transport)
        readers, _ = snapshot_readers(GRANTS_GOV_PROFILE, iter_retained_pages, scopes,
                                      output_dir / "source-releases", store=evidence.store)
        journal_snapshot_releases(evidence, readers, grants_opportunities.RELEASE_EVENT)
        return build_grants_opportunities.build_grants_opportunities(readers, output_dir)


class AdministrationPolicyRollup(SourceRefreshRollup):
    """Rebuild administration-policy statements from the community archive's current commit."""

    name = "administration-policy"
    outputs = tuple(f"{table}.parquet" for table in retained_collections.SCHEMAS)
    force_publish: bool = False
    _live_has_etl_receipts: bool = False

    def invocation_scope(self) -> dict:
        scope = super().invocation_scope()
        scope["administration_policy"] = {
            "force_publish": bool(self.force_publish or flag_env("ADMINISTRATION_POLICY_FORCE_PUBLISH"))
        }
        return scope

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        parents = super()._prime(output_dir, snapshot)
        family = (snapshot or {}).get("families", {}).get(self.name, {})
        self._live_has_etl_receipts = "etlReceipts" in family
        return parents

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.evidence
        force_publish = self.force_publish or flag_env("ADMINISTRATION_POLICY_FORCE_PUBLISH")
        statements = next(path for path in self.published_tables(output_dir / "published")
                          if path.stem == administration_policy.STATEMENTS)
        published = administration_policy.published_sources(statements)
        commit = administration_policy.head_commit()
        evidence.event("administration-policy-head", remote=administration_policy.REMOTE, commit=commit)
        selection = administration_policy.capture(evidence, commit, (), published=published,
                                                  transport=self.transport)
        if all(source["commit"] == published[source["administration"]]["commit"] for source in selection["sources"]):
            if not force_publish and not self._live_has_etl_receipts:
                raise Unchanged(f"no archive file differs at {commit[:12]}")
            reason = "force_publish was requested" if force_publish else "the live family still carries etlReceipts"
            logger.info("Rollup {}: bypassing unchanged check at {} because {}", self.name, commit[:12], reason)
        built = administration_policy.candidate(selection, output_dir, evidence, store=evidence.store, retain=False)
        return tuple(output_dir / "candidate" / built["outputs"][table]["path"]
                     for table in retained_collections.SCHEMAS)


class TreasuryFiscalRollup(SourceRefreshRollup):
    """Add each newly listed Debt to the Penny day (and optional reviewed MTS issue) to the Treasury generation.

    The published rows name every capture behind them, and published evidence holds those bytes, so a run
    needs no capture file from an earlier one. A new monthly statement needs its reviewed estimate entries
    in ``treasury_units.json`` and is selected via ``TREASURY_MTS_ISSUE_DATE`` (workflow ``mts_issue_date``).
    """

    name = "treasury-fiscal"
    outputs = tuple(f"{table}.parquet" for table in build_treasury_fiscal.SCHEMAS)
    mts_issue_date: str | None = None

    def invocation_scope(self) -> dict:
        scope = super().invocation_scope()
        mts_raw = (self.mts_issue_date or getenv("TREASURY_MTS_ISSUE_DATE") or "").strip() or None
        scope["treasury_fiscal"] = {"mts_issue_date": mts_raw}
        return scope

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.evidence
        published = treasury_fiscal.published_selection(self.published_tables(output_dir / "published"))
        debt_days = [scope["selection"]["record_date"] for scope in published["releases"]
                     if scope["selection"]["dataset"] == "debt_to_penny"]
        listed = treasury_fiscal.debt_dates_after(evidence, date.fromisoformat(max(debt_days)),
                                                  transport=self.transport)
        mts_raw = (self.mts_issue_date or getenv("TREASURY_MTS_ISSUE_DATE") or "").strip()
        mts_dates = [date.fromisoformat(mts_raw)] if mts_raw else []
        published_reports = set(published["reports"])
        new_mts_dates = [day for day in mts_dates if day.isoformat() not in published_reports]
        if not listed and not new_mts_dates:
            if mts_dates:
                raise Unchanged(f"no debt date is listed after {max(debt_days)} and MTS {mts_raw} is already published")
            raise Unchanged(f"no debt date is listed after {max(debt_days)}")
        selections = [("debt_to_penny", day) for day in listed]
        for day in new_mts_dates:
            selections.extend([("mts_table_1", day), ("mts_table_2", day)])
        added = treasury_fiscal.capture(evidence, selections, new_mts_dates, transport=self.transport)
        held = [capture["response_sha256"] for scope in published["releases"] for capture in scope["captures"]]
        held += [report["sha256"] for report in published["reports"].values()]
        restore_originals(evidence.store, held, getenv("R2_PUBLIC_URL", r2.VERIFIED_PUBLIC_URL),
                          max_bytes=treasury_fiscal.MAX_REPORT_BYTES)
        selection = treasury_fiscal.merge_selections([published, added])
        readers, inputs = treasury_fiscal.selected_inputs(selection, output_dir / "source-releases",
                                                          store=evidence.store)
        files, _ = treasury_fiscal.retain_and_build(readers, output_dir, evidence, **inputs)
        return tuple(files)


class ExecutiveOrdersRollup(SourceRefreshRollup):
    """Publish every executive order the Federal Register API serves, keeping the published rows' bodies."""

    name = "executive-orders"
    output = build_executive_orders.OUTPUT
    #: An injected listing fetch (URL to bytes); ``None`` uses the Federal Register owner's client.
    fetch = None

    def build(self, output_dir: Path) -> Path:
        evidence = self.evidence
        (table,) = self.published_tables(output_dir / "published")
        prior = executive_orders.prior_rows(table)
        selection = executive_orders.capture(evidence, prior=prior, results=output_dir / "body-captures",
                                             through=datetime.now(UTC).date(), fetch=self.fetch,
                                             transport=self.transport)
        if "bodies" not in selection:
            raise Unchanged(f"the API lists the {len(prior)} published orders unchanged")
        return executive_orders.retain_and_build(selection, output_dir, evidence, prior=prior)


class HouseDisbursementsRollup(SourceRefreshRollup):
    """Rebuild the House ledger when the House lists a CSV the published generation lacks.

    The published originals are restored from R2 evidence (0.92 GB for 2016 to mid-2026) rather than downloaded
    from the House again; every published quarter is normalized again with the new ones.
    """

    name = house_disbursements.FAMILY
    outputs = tuple(f"{table}.parquet" for table in HOUSE_TABLES.values())
    partitioned = {key: HOUSE_PARTITIONS for key in outputs}

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        from spicy_docs.sources.house_disbursements import HouseDisbursementsBudget

        evidence = self.evidence
        listed = house_disbursements.listed_links(evidence, transport=self.transport)
        # The published rows, not the prior evidence's journal, say which files are published: a journal event read
        # back here would have to be handed on by every native conversion (native_conversion.INHERITED_EVENTS).
        for key in self.outputs:
            r2.download_members(key, output_dir / "published")
        files = house_disbursements.published_files(output_dir / "published")
        added = house_disbursements.additions(listed, files)
        if not added:
            raise Unchanged("the House lists no CSV the published generation lacks")
        restore_originals(evidence.store, [item["source_sha256"] for item in files],
                          getenv("R2_PUBLIC_URL", r2.VERIFIED_PUBLIC_URL), max_bytes=HouseDisbursementsBudget().max_bytes,
                          transport=self.transport)
        return house_disbursements.refresh(files, added, output_dir, store=evidence.store, evidence=evidence,
                                           transport=self.transport)


class FecFinancialProfilesRollup(SourceRefreshRollup):
    """Publish the FEC's current candidate and committee summary CSVs of a cycle as the family's profiles.

    FEC overwrites both files every day, so the day's bytes are captured and their evidence sent to R2 before
    anything parses them: a refused build keeps them. A run whose two downloads are the published bytes
    publishes nothing; a file that lost more than ``MAX_ROW_LOSS`` of its published rows refuses.
    """

    name = fec_financial_profiles.FAMILY
    output = build_fec_financial_profiles.OUTPUT
    #: The one cycle the family holds (``select`` builds from one cycle's pair).
    cycle = 2026

    def invocation_scope(self) -> dict:
        scope = super().invocation_scope()
        scope["fec_financial_profiles"] = {"cycle": self.cycle}
        return scope

    def build(self, output_dir: Path) -> Path:
        evidence = self.evidence
        captures = fec_financial_profiles.acquire([self.cycle], output_dir / "capture", transport=self.transport,
                                                  publish=not self.skip_upload)["captures"]
        if all(published_before(evidence, fec_financial_profiles.STAGE, item["response_sha256"]) for item in captures):
            raise Unchanged(f"both {self.cycle} summary files are the published ones")
        (published,) = self.published_tables(output_dir / "published")
        return fec_financial_profiles.profiles_table(evidence, captures, output_dir, published=published)


class CongressSessionsRollup(SourceRefreshRollup):
    """Publish reconciled House and Senate session dates and party control when any of the three pages changes."""

    name = congress_sessions.FAMILY
    outputs = tuple(f"{table}.parquet" for table in build_congress_sessions.SCHEMAS)

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.evidence
        selection = congress_sessions.capture(evidence, transport=self.transport)
        if all(
            published_before(evidence, congress_sessions.STAGE, source["sha256"])
            for source in selection["sources"].values()
        ):
            raise Unchanged("all three official House and Senate history pages are the published ones")
        return congress_sessions.build_tables_from_selection(
            selection, output_dir, store=evidence.store, evidence=evidence, retain=False
        )


class SectorComparisonsRollup(SourceRefreshRollup):
    """Rebuild sector comparisons when published FEC profiles, LDA profiles, LDA activities, or mappings change."""

    name = sector_comparisons.FAMILY
    inputs = (
        "fec_financial_profiles.parquet",
        "lobbying_quarterly_profiles.parquet",
        "lobbying_activities.parquet",
    )
    outputs = tuple(f"{table}.parquet" for table in sector_comparisons_transform.SCHEMAS)

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        parents = super()._prime(output_dir, snapshot)
        index = snapshot or publication.empty_index()
        self._primed_parents = parents
        self._primed_cutoffs = {
            key: owner[1].get("publishedAt")
            for key in self.inputs
            if (owner := publication.table_owner(index, key)) is not None
        }
        return parents

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.evidence
        config = sector_comparisons.load_rollup_selection()
        parents = getattr(self, "_primed_parents", None)
        if parents is None:
            raise RuntimeError("sector-comparisons: build requires primed upstream tables")
        cutoffs = getattr(self, "_primed_cutoffs", {})
        fec_pin = parents["fec_financial_profiles.parquet"]
        lda_pin = parents["lobbying_quarterly_profiles.parquet"]
        act_pin = parents["lobbying_activities.parquet"]
        fallback_cutoff = f"{config['mapping']['observed_at']}T00:00:00Z"
        scope = {
            "fec": {
                "cutoff": cutoffs.get("fec_financial_profiles.parquet") or fallback_cutoff,
                "generation": fec_pin.get("artifactDigest") or fec_pin["sha256"],
                "population": config["populations"]["fec"],
                "table_sha256": fec_pin["sha256"],
            },
            "lda": {
                "cutoff": cutoffs.get("lobbying_quarterly_profiles.parquet") or fallback_cutoff,
                "generation": lda_pin.get("artifactDigest") or lda_pin["sha256"],
                "population": config["populations"]["lda"],
                "table_sha256": lda_pin["sha256"],
            },
            "period_start": config["period_start"],
            "period_end": config["period_end"],
        }
        selection_bytes = sector_comparisons.json_bytes({"scope": scope, "mapping": config["mapping"]})
        selection_sha256 = "sha256:" + hashlib.sha256(selection_bytes).hexdigest()
        act_sha256 = act_pin["sha256"]
        if evidence.inherited_event(
            "sector-comparison-inputs",
            selection_sha256=selection_sha256,
            lda_activities_sha256=act_sha256,
        ):
            raise Unchanged(
                "FEC profiles, LDA profiles, LDA activities, and curated sector mapping are the published ones"
            )
        restore_originals(
            evidence.store,
            [capture["sha256"] for capture in config["classification_captures"]],
            getenv("R2_PUBLIC_URL", r2.VERIFIED_PUBLIC_URL),
            max_bytes=fec_research_profiles.MAX_ORIGINAL_BYTES,
            transport=self.transport,
        )
        evidence.event(
            "sector-comparison-inputs",
            selection_sha256=selection_sha256,
            fec_generation=scope["fec"]["generation"],
            fec_table_sha256=scope["fec"]["table_sha256"],
            lda_generation=scope["lda"]["generation"],
            lda_table_sha256=scope["lda"]["table_sha256"],
            lda_activities_sha256=act_sha256,
            mapping_version=config["mapping"]["version"],
        )
        return sector_comparisons.build_tables_from_paths(
            evidence,
            scope=scope,
            mapping=config["mapping"],
            classification_captures=config["classification_captures"],
            fec_path=output_dir / "fec_financial_profiles.parquet",
            lda_path=output_dir / "lobbying_quarterly_profiles.parquet",
            lda_activities_path=output_dir / "lobbying_activities.parquet",
            output_dir=output_dir,
            fec_expected_hash=fec_pin["sha256"],
            lda_expected_hash=lda_pin["sha256"],
            lda_activities_expected_hash=act_sha256,
        )


ofac_sanctions_app = make_rollup_app(OfacSanctionsRollup)
grants_opportunities_app = make_rollup_app(GrantsOpportunitiesRollup)
administration_policy_app = make_rollup_app(AdministrationPolicyRollup)
treasury_fiscal_app = make_rollup_app(TreasuryFiscalRollup)
executive_orders_app = make_rollup_app(ExecutiveOrdersRollup)
house_disbursements_app = make_rollup_app(HouseDisbursementsRollup)
fec_financial_profiles_app = make_rollup_app(FecFinancialProfilesRollup)
congress_sessions_app = make_rollup_app(CongressSessionsRollup)
sector_comparisons_app = make_rollup_app(SectorComparisonsRollup)
