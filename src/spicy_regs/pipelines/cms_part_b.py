"""Build one selected CMS Part B benefit year from an admitted local source release."""

from pathlib import Path

from spicy_regs.pipelines import cms_drug_spending
from spicy_regs.transforms import build_cms_part_b as table


def build(reader, output_dir: Path, *, year: int):
    return cms_drug_spending.build(
        reader,
        output_dir,
        year=year,
        label="Part B",
        family="cms-part-b",
        measures=table.MEASURES,
        schema=table.SCHEMA,
        build_table=table.build_cms_part_b,
        dictionary_fields=table.required_fields(year),
    )


def main():
    cms_drug_spending.main(build, __doc__)


if __name__ == "__main__":
    main()
