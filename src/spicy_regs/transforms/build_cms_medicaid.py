"""Selected Medicaid drug spending; Part D beneficiary measures do not apply."""

from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.cms_drug_spending import (
    observation as drug_observation,
    observations as drug_observations,
    spending_schema,
)

TABLE = "cms_medicaid_drug_spending"
OUTPUT = TABLE + ".parquet"
MEASURES = {
    "total_spending_usd": "Tot_Spndng",
    "dosage_units": "Tot_Dsg_Unts",
    "claims": "Tot_Clms",
    "weighted_spending_per_unit_usd": "Avg_Spnd_Per_Dsg_Unt_Wghtd",
    "spending_per_claim_usd": "Avg_Spnd_Per_Clm",
}
SCHEMA = spending_schema(MEASURES).insert(1, pa.field("program", pa.string()))


def observation(item, pin, *, year):
    row = drug_observation(item, pin, year=year, measures=MEASURES, label="Medicaid")
    return {**row, "program": "Medicaid"}


def observations(reader, *, year):
    return drug_observations(
        reader,
        year=year,
        series_title="Medicaid Spending by Drug",
        label="Medicaid",
        map_observation=observation,
    )


def build_cms_medicaid(reader, output_dir: Path, *, year: int) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return write_rows(observations(reader, year=year), output_dir / OUTPUT, SCHEMA)
