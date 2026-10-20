"""Build a selected CMS Medicaid benefit year from one admitted local source release."""

from pathlib import Path

from spicy_regs.pipelines import cms_drug_spending
from spicy_regs.transforms import build_cms_medicaid as table


def build(reader, output_dir: Path, *, year: int):
    return cms_drug_spending.build(
        reader,
        output_dir,
        year=year,
        label="Medicaid",
        family="cms-medicaid",
        measures=table.MEASURES,
        schema=table.SCHEMA,
        build_table=table.build_cms_medicaid,
    )


def main():
    cms_drug_spending.main(build, __doc__)


if __name__ == "__main__":
    main()
