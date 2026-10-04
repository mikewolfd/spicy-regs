"""Re-read explicit fields from pinned publications into the shared citation table."""

import hashlib
import json
import os
from pathlib import Path

from cyclopts import App
import duckdb
from dotenv import load_dotenv

from spicy_regs.citation_sources import TEXT_SOURCES
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.legislative_rollups import family_policies, _selected_prior
from spicy_regs.legislative_receipts import FILE_POLICY, FILE_STATES, migrate_outputs, restore_prior
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, combine_receipts, read_attempts, rebind_receipt, select_receipts
from spicy_regs.native_types import described_schema
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from spicy_regs.sources import publication
from spicy_regs.transforms.held_citations import build_held_citations, parse_selections


class HeldCitationsRollup(RollupPipeline):
    """Explicit bounded field selection; reuse print-citations publication and source evidence."""

    name = "held-citations"
    publication_family = "print-citations"
    inputs = ("document_citations.parquet",)
    output = "document_citations.parquet"
    outputs = ("house_activity_reports.parquet", "budget_volumes.parquet",
               "bill_committee_actions.parquet", "document_citations.parquet")
    receipt_policies = family_policies(*(key.removesuffix(".parquet") for key in outputs))
    retain_source_evidence = True

    def __init__(self, *, selection: Path, **kwargs):
        super().__init__(**kwargs)
        if selection.stat().st_size > 64 * 1024:
            raise ValueError("Selection manifest exceeds 64 KiB")
        body = selection.read_bytes()
        manifest = json.loads(body)
        if not isinstance(manifest, dict) or set(manifest) != {"selections", "input_generations"}:
            raise ValueError("Selection manifest requires selections and input_generations")
        self.selections = parse_selections(manifest["selections"])
        self.expected = manifest["input_generations"]
        tables = {TEXT_SOURCES[s.kind].table for s in self.selections}
        if not isinstance(self.expected, dict) or set(self.expected) != tables:
            raise ValueError("Every selected source table needs exactly one input generation")
        self.selection_sha256 = "sha256:" + hashlib.sha256(body).hexdigest()
        self.selection_body = body
        self.source_members = {}
        self.input_pins = {}

    def _observe_remote_inputs(self, output_dir, public_url):
        if not public_url:
            raise ValueError("Held citation extraction requires R2_PUBLIC_URL and explicit input generations")
        index = publication.current_index(public_url)
        parents = {}
        for table, expected in self.expected.items():
            key = table + ".parquet"
            owner = publication.table_owner(index, key)
            if owner is None or owner[1]["artifactDigest"] != expected:
                raise ValueError(f"{table}: requested input generation is not selected; rebuild the selection")
            member = publication.single_member(index, key)
            pin = {"family": owner[0], "artifactDigest": expected,
                   "sha256": member.sha256, "byteSize": member.byte_size}
            parents[key] = pin
            self.input_pins[table] = pin
            self.source_members[table] = public_url.rstrip("/") + "/" + member.path
        return parents

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}

    def build(self, output_dir):
        if not self.source_members:
            self._observe_remote_inputs(output_dir, os.environ.get("R2_PUBLIC_URL"))
        if self.source_evidence:
            self.source_evidence.retain_bytes(self.selection_body, stage="held-citation-selection",
                                               sources=self.input_pins, selected_fields=len(self.selections))
            self.source_evidence.event("held-citation-selection", sha256=self.selection_sha256,
                                       sources=self.input_pins, selected_fields=len(self.selections))
        prior = _selected_prior(self, output_dir / ".selected-held-prior")
        if prior is None:
            raise ValueError("Held citations require a complete selected print-citations receipt generation")
        work = output_dir / ".held-source"
        priors = restore_prior(prior, work)

        def download_prior(key, target):
            source = priors.get(key.removesuffix(".parquet"))
            if source is None:
                return False
            shutil.copyfile(source, target)
            return True

        with duckdb.connect(config={"memory_limit": "1GB", "threads": 2}) as con:
            load_public_http(con)
            for table, url in self.source_members.items():
                escaped = url.replace("'", "''")
                con.execute(f'CREATE VIEW "{table}" AS SELECT * FROM read_parquet(\'{escaped}\')')
            updated = build_held_citations(work, cursor=con, selections=self.selections,
                                            input_pins=self.input_pins, download_prior=download_prior,
                                            evidence=self.source_evidence)
        bundle = output_dir / ".held-native"
        manifest = migrate_outputs([updated], bundle, generation_id=self.receipt_generation_id)
        if any(manifest["refused_rows"].values()):
            raise ValueError("Held citation conversion refused")
        prior_manifest = json.loads((prior / "legislative-bundle.json").read_text())
        held_receipts = prior / prior_manifest["receipt_file"]
        file_receipts = select_receipts(held_receipts, output_dir / ".held-file-receipts.parquet", dataset=FILE_STATES)
        replaced_files = {
            r["receipt_id"] for r in read_attempts([file_receipts], FILE_POLICY,
                generation_id=prior_manifest["generation_id"])
            if r["processing_fields"]["file_state"]["dataset"] == "document_citations"
        }
        carried = output_dir / ".held-carried-receipts.parquet"
        with pq.ParquetWriter(carried, RECEIPT_SCHEMA, compression="zstd") as writer:
            for batch in pq.ParquetFile(held_receipts).iter_batches(batch_size=2000):
                rows = [rebind_receipt(r, generation_id=self.receipt_generation_id) for r in batch.to_pylist()
                        if r["dataset"] != "document_citations" and r["receipt_id"] not in replaced_files]
                if rows:
                    writer.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA))
        combine_receipts([bundle / "etl_receipts.parquet", carried], output_dir / "etl_receipts.parquet")
        outputs = []
        for key in self.outputs:
            dataset = key.removesuffix(".parquet")
            members = manifest["subjects"][dataset] if dataset == "document_citations" else prior_manifest["subjects"][dataset]
            if members != [key]:
                raise ValueError("Print citation sibling must have exactly one selected member")
            source = (bundle if dataset == "document_citations" else prior) / key
            target = output_dir / key
            shutil.copyfile(source, target)
            outputs.append(target)
        return tuple(outputs)


app = App(help=__doc__)


@app.default
def main(*, selection: Path, output_dir: Path | None = None, skip_upload: bool = True):
    load_dotenv()
    HeldCitationsRollup(selection=selection, output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
