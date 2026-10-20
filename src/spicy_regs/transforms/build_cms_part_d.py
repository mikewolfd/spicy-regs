"""One selected Part D benefit year's drug/manufacturer observations."""

from pathlib import Path

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.cms_drug_spending import (
    IDENTITY_FIELDS as IDENTITY_FIELDS,
    json_value as json_value,
    measurement as measurement,
    observation as drug_observation,
    observations as drug_observations,
    spending_schema,
)

TABLE = "cms_part_d_drug_spending"
OUTPUT = TABLE + ".parquet"
MEASURES = {
    "total_spending_usd": "Tot_Spndng",
    "dosage_units": "Tot_Dsg_Unts",
    "claims": "Tot_Clms",
    "beneficiaries": "Tot_Benes",
    "weighted_spending_per_unit_usd": "Avg_Spnd_Per_Dsg_Unt_Wghtd",
    "spending_per_claim_usd": "Avg_Spnd_Per_Clm",
    "spending_per_beneficiary_usd": "Avg_Spnd_Per_Bene",
}
SCHEMA = spending_schema(MEASURES)


def observation(item, pin, *, year):
    return drug_observation(item, pin, year=year, measures=MEASURES, label="Part D")


def observations(reader, *, year):
    return drug_observations(
        reader,
        year=year,
        series_title="Medicare Part D Spending by Drug",
        label="Part D",
        map_observation=observation,
    )


def build_cms_part_d(reader, output_dir: Path, *, year: int) -> Path:
    """Replace the table only after the complete selected population passes."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return write_rows(observations(reader, year=year), output_dir / OUTPUT, SCHEMA)
