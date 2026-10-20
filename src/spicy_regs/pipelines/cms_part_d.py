"""Build a selected CMS Part D benefit year from one admitted local source release."""

from pathlib import Path

from spicy_regs.pipelines import cms_drug_spending
from spicy_regs.transforms import build_cms_part_d as table


def build(reader, output_dir: Path, *, year: int):
    return cms_drug_spending.build(
        reader,
        output_dir,
        year=year,
        label="Part D",
        family="cms-part-d",
        measures=table.MEASURES,
        schema=table.SCHEMA,
        build_table=table.build_cms_part_d,
    )


def main():
    cms_drug_spending.main(build, __doc__)


if __name__ == "__main__":
    main()
