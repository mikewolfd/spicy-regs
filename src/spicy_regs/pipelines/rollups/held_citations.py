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
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, SubjectReceiptRollup
from spicy_regs.legislative_rollups import family_policies
from spicy_regs.native_types import described_schema
from spicy_regs.sources import publication
from spicy_regs.transforms.held_citations import build_held_citations, parse_selections, write_citation_reads


class HeldCitationsRollup(SubjectReceiptRollup):
    """Explicit bounded field selection; reuse print-citations publication and source evidence."""

    name = "held-citations"
    publication_family = "print-citations"
    inputs = ("document_citations.parquet",)
    output = "document_citations.parquet"
    outputs = (
        "house_activity_reports.parquet",
        "budget_volumes.parquet",
        "bill_committee_actions.parquet",
        "document_citations.parquet",
        "document_citation_reads.parquet",
    )
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
        self.source_index = None

    def _observe_remote_inputs(self, output_dir, public_url):
        if not public_url:
            raise ValueError("Held citation extraction requires R2_PUBLIC_URL and explicit input generations")
        index = publication.current_index(public_url)
        self.source_index = index
        parents = {}
        for table, expected in self.expected.items():
            key = table + ".parquet"
            owner = publication.table_owner(index, key)
            if owner is None or owner[1]["artifactDigest"] != expected:
                raise ValueError(f"{table}: requested input generation is not selected; rebuild the selection")
            member = publication.single_member(index, key)
            pin = {
                "family": owner[0],
                "artifactDigest": expected,
                "sha256": member.sha256,
                "byteSize": member.byte_size,
            }
            parents[key] = pin
            self.input_pins[table] = pin
            self.source_members[table] = public_url.rstrip("/") + "/" + member.path
            if table == "house_communications":
                (receipt,) = publication.receipt_members(index, dataset=table)
                parents[table + "/etl_receipts.parquet"] = {
                    "family": owner[0], "artifactDigest": expected,
                    "sha256": receipt.sha256, "byteSize": receipt.byte_size,
                }
        return parents

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}

    def build(self, output_dir):
        if not self.source_members:
            self._observe_remote_inputs(output_dir, os.environ.get("R2_PUBLIC_URL"))
        if self.source_evidence:
            self.source_evidence.retain_bytes(
                self.selection_body,
                stage="held-citation-selection",
                sources=self.input_pins,
                selected_fields=len(self.selections),
            )
            self.source_evidence.event(
                "held-citation-selection",
                sha256=self.selection_sha256,
                sources=self.input_pins,
                selected_fields=len(self.selections),
            )

        def builder(work, *, download_prior):
            for key in self.outputs:
                if not download_prior(key, work / key):
                    raise ValueError("Held citations require a complete selected print-citations receipt generation")
            with duckdb.connect(config={"memory_limit": "1GB", "threads": 2}) as con:
                load_public_http(con)
                for table, url in self.source_members.items():
                    if table == "house_communications":
                        # Record entry text is retained in this selected family's receipts.
                        priors = SelectedPriors(work / "held-source-inputs", index=self.source_index)
                        restored = priors.get(table)
                        if restored is None:
                            raise ValueError("House communication fields require selected native receipts")
                        url = str(restored)
                    escaped = url.replace("'", "''")
                    con.execute(f"CREATE VIEW \"{table}\" AS SELECT * FROM read_parquet('{escaped}')")
                citations = build_held_citations(
                    work,
                    cursor=con,
                    selections=self.selections,
                    input_pins=self.input_pins,
                    download_prior=download_prior,
                    evidence=self.source_evidence,
                )
            write_citation_reads(work, citations)
            return tuple(work / key for key in self.source_outputs)

        return self.build_receipts(output_dir, builder)


app = App(help=__doc__)


@app.default
def main(*, selection: Path, output_dir: Path | None = None, skip_upload: bool = True):
    load_dotenv()
    HeldCitationsRollup(selection=selection, output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
