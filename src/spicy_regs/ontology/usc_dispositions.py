"""What became of a U.S.C. section a positive-law recodification or editorial reclassification took away.

Vendored and adapted from RefSpec's ``refspec.registry.usc_disposition_tables``
against the nine SHA-256-pinned Parquet tables across all eight recodification
families (Titles 10 ROPMA, 31, 34 editorial reclassification, 41, 46 / 46 App.,
49 / 49 App., 49 positive-law companion, 51, and 54).

Every byte is verified against its pinned SHA-256 at load time and read through
DuckDB so lightweight serving environments without ``pyarrow`` work identically.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property, lru_cache
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import duckdb

__all__ = [
    "DISPOSITION_RULE",
    "KNOWN_PRINT_DISCREPANCIES",
    "NOT_IN_TABLE_CAVEATS",
    "RECODIFICATIONS",
    "RECODIFICATIONS_NOT_PINNED",
    "STATUSES",
    "VENDORED_DISPOSITION_DIR",
    "VERDICTS",
    "Disposition",
    "DispositionRow",
    "Recodification",
    "Successor",
    "UscDispositionTables",
    "disposition_table_digests",
    "load_disposition_tables",
    "normalize_section",
    "normalize_subsection",
]

DISPOSITION_RULE = "usc-disposition-tables/2"

VENDORED_DISPOSITION_DIR = Path(str(files("spicy_regs").joinpath("reference/refspec/usc-disposition-tables")))


@dataclass(frozen=True)
class Recodification:
    """One positive-law recodification or editorial reclassification, and the printed table that maps it."""

    name: str
    former_title: int
    table: str
    digest: str
    enacted_by: str
    deeming_provision: str
    source_url: str
    source_digest: str
    source_bytes: int
    source_fetched: str = "2026-11-18"
    former_titles: tuple[int, ...] = ()
    enacted_title: int | None = None
    covers_appendix: bool = False
    is_editorial_reclassification: bool = False

    @property
    def effective_former_titles(self) -> tuple[int, ...]:
        """Every former U.S.C. title this table is authorised to carry rows for."""
        return self.former_titles or (self.former_title,)


RECODIFICATIONS: tuple[Recodification, ...] = (
    Recodification(
        name="title-10-1994",
        former_title=10,
        table="usc-1996-title10-ropma-disposition.parquet",
        digest="sha256:5a08734b3c8ea948340203e0e3c67a256a6a62c95ece863a1328a662691eace6",
        enacted_by=(
            "Pub. L. 103-337, div. A, title XVI, Oct. 5, 1994, 108 Stat. 2924; "
            "Pub. L. 104-106, div. A, title XV, § 1501(c)(26), Feb. 10, 1996, 110 Stat. 499"
        ),
        deeming_provision="Pub. L. 103-337, § 1665(a)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-1996-title10/pdf/USCODE-1996-title10.pdf",
        source_digest="sha256:d1877b5aa58f17062263fed5a04da62f3ad885b4b01a0da7f0e41d0da15e0010",
        source_bytes=8_971_147,
        source_fetched="2026-11-18",
        enacted_title=10,
    ),
    Recodification(
        name="title-31-1982",
        former_title=31,
        table="usc-1994-title31-disposition.parquet",
        digest="sha256:febacca0f59193fc4524ce780d29ba5440d8ee47f8041f133fda161d0c765cd5",
        enacted_by=(
            "Pub. L. 97-258, § 1, Sept. 13, 1982, 96 Stat. 877; "
            "Pub. L. 97-452, § 1, Jan. 12, 1983, 96 Stat. 2467"
        ),
        deeming_provision="Pub. L. 97-258, § 4(b); Pub. L. 97-452, § 3(b)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-1994-title31/pdf/USCODE-1994-title31.pdf",
        source_digest="sha256:3b319dc182915b409d01c26d5fe46326e0416f5e725aa556738d17ba60340f0a",
        source_bytes=2_088_477,
        source_fetched="2026-11-18",
        enacted_title=31,
    ),
    Recodification(
        name="title-34-2017",
        former_title=42,
        table="usc-2017-title34-disposition.parquet",
        digest="sha256:17e063ddd410d39c135e4d14550dceadc62707a811dc9e7bd2f63999014dd047",
        enacted_by="OLRC editorial reclassification under 2 U.S.C. 285b, eff. Sept. 1, 2017 (through Pub. L. 115-31)",
        deeming_provision="2 U.S.C. 285b (editorial reclassification without substantive change)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-2017-title34/pdf/USCODE-2017-title34.pdf",
        source_digest="sha256:c121b7ca9147571955edbb9857da95e17292f21eb00f0a90741ae5b99c40bcd9",
        source_bytes=2_753_424,
        source_fetched="2026-11-18",
        former_titles=(42, 18, 28),
        enacted_title=34,
        is_editorial_reclassification=True,
    ),
    Recodification(
        name="title-41-2011",
        former_title=41,
        table="usc-2011-title41-disposition.parquet",
        digest="sha256:327de9b607eaf74679c21af8a6cf27086c7bf5aaf19c01dbcf3005959fa3a51b",
        enacted_by="Pub. L. 111-350, § 3, Jan. 4, 2011, 124 Stat. 3677",
        deeming_provision="Pub. L. 111-350, § 6(c)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-2011-title41/pdf/USCODE-2011-title41.pdf",
        source_digest="sha256:3b253b255aa058d05aa4b3e6b6aa2f345ee25a1f98cff5ea19f514c2d0a417c1",
        source_bytes=1_037_994,
        source_fetched="2026-11-18",
        enacted_title=41,
    ),
    Recodification(
        name="title-46-2006",
        former_title=46,
        table="usc-2006-title46-disposition.parquet",
        digest="sha256:2b6a315482a8b8ba142360ea3303e55e6e8e07d632ebe4c3a1a24d38a9efca84",
        enacted_by=(
            "Pub. L. 98-89, Aug. 26, 1983, 97 Stat. 500; "
            "Pub. L. 99-509, Oct. 21, 1986, 100 Stat. 1913; "
            "Pub. L. 100-424, Sept. 9, 1988, 102 Stat. 1591; "
            "Pub. L. 100-710, Nov. 23, 1988, 102 Stat. 4738; "
            "Pub. L. 109-304, Oct. 6, 2006, 120 Stat. 1485"
        ),
        deeming_provision="Pub. L. 98-89, § 2(b); Pub. L. 99-509, § 5103(b); Pub. L. 100-710, § 105(a); Pub. L. 109-304, § 18(c)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-2006-title46/pdf/USCODE-2006-title46.pdf",
        source_digest="sha256:6bda1578ab30e947d3f7e746bacd53048a197ef80c946f0f7300276c97a5887d",
        source_bytes=2_449_872,
        source_fetched="2026-11-18",
        enacted_title=46,
        covers_appendix=True,
    ),
    Recodification(
        name="title-49-1994",
        former_title=49,
        table="usc-1994-title49-disposition.parquet",
        digest="sha256:8403212c0193b3361accf7ff4be238420634beb5aa5740d78b9960fef5b2aedd",
        enacted_by="Pub. L. 103-272, July 5, 1994, 108 Stat. 745",
        deeming_provision="Pub. L. 103-272, § 6(b)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-1994-title49/pdf/USCODE-1994-title49.pdf",
        source_digest="sha256:66f004679e27e0d16356e14b79cb3b4f7ebf63d91307435fa8f53c95bcc2848d",
        source_bytes=5_165_242,
        source_fetched="2026-08-23",
        enacted_title=49,
        covers_appendix=True,
    ),
    Recodification(
        name="title-49-1994-positivelaw",
        former_title=49,
        table="usc-1994-title49-positivelaw-disposition.parquet",
        digest="sha256:6387ca0487ec234ae04811e90d1490e9bf93407d1b3dd5f3fa40836e3bc21510",
        enacted_by="Pub. L. 103-272, §§ 1(c), (d), (e), 4(j)(9)(A), (11)(B), July 5, 1994, 108 Stat. 745, 1367, 1368",
        deeming_provision="Pub. L. 103-272, § 6(b)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-1994-title49/pdf/USCODE-1994-title49.pdf",
        source_digest="sha256:66f004679e27e0d16356e14b79cb3b4f7ebf63d91307435fa8f53c95bcc2848d",
        source_bytes=5_165_242,
        source_fetched="2026-08-23",
        enacted_title=49,
        covers_appendix=False,
    ),
    Recodification(
        name="title-51-2010",
        former_title=42,
        table="usc-2010-title51-disposition.parquet",
        digest="sha256:507bdd8da5dd9a1ff6b5f395193408e1f36120803637ee367ddfecc3792d7c0a",
        enacted_by="Pub. L. 111-314, § 3, Dec. 18, 2010, 124 Stat. 3326",
        deeming_provision="Pub. L. 111-314, § 5(e)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-2010-title51/pdf/USCODE-2010-title51.pdf",
        source_digest="sha256:66d48661b3bae9b9e9e7a1964a64ff093dc22d94fae81eda6429a4f9551cea84",
        source_bytes=914_412,
        source_fetched="2026-11-18",
        former_titles=(42, 15),
        enacted_title=51,
    ),
    Recodification(
        name="title-54-2014",
        former_title=16,
        table="usc-2014-title54-disposition.parquet",
        digest="sha256:49e79253f9240b75edd85afa3d8fe9b513e231e060434d4fea0d6bab3ac60034",
        enacted_by="Pub. L. 113-287, § 3, Dec. 19, 2014, 128 Stat. 3094",
        deeming_provision="Pub. L. 113-287, § 6(e)",
        source_url="https://www.govinfo.gov/content/pkg/USCODE-2014-title54/pdf/USCODE-2014-title54.pdf",
        source_digest="sha256:548f857e19f2ce9c384211062872d422142a2b55129251d3f82e35c80eafcf21",
        source_bytes=1_085_260,
        source_fetched="2026-11-18",
        enacted_title=54,
    ),
)

_RECODIFICATION_BY_NAME: Mapping[str, Recodification] = {r.name: r for r in RECODIFICATIONS}

RECODIFICATIONS_NOT_PINNED: Mapping[int, str] = {}

VERDICTS = (
    "exists-as-recodified",
    "repealed-no-successor",
    "eliminated-no-successor",
    "stated-without-successor",
    "not-in-table",
    "no-table-for-title",
)

STATUSES = ("restated", "restated-as-note", "repealed", "eliminated", "see-reference")
_WITH_SUCCESSOR = frozenset({"restated", "restated-as-note"})

NOT_IN_TABLE_CAVEATS = ("repealed_before_the_recodification_not_listed",)

KNOWN_PRINT_DISCREPANCIES: Mapping[tuple[int, str], tuple[str, ...]] = {
    (42, "13136f"): ("olrc_front_matter_print_typo:printed_42:13136f_body_codification_42:14136f",),
    (42, "14136f"): ("olrc_front_matter_print_typo:printed_as_42:13136f_body_codification_34:40728",),
    (42, "3797ee-1"): ("olrc_front_matter_print_typo:printed_34:16902_body_codification_34:10692",),
    (41, "46"): ("olrc_hrn_divergence:printed_41:8502_body_hrn_also_cites_41:8501(2)",),
    (16, "458a"): ("olrc_front_matter_print_typo:printed_54:104908_body_hrn_54:104905",),
}
_KNOWN_PRINT_DISCREPANCY_CAVEATS = frozenset(
    caveat for caveats in KNOWN_PRINT_DISCREPANCIES.values() for caveat in caveats
)

_DASHES = str.maketrans(dict.fromkeys("‐‑‒–—―−\x96\x97", "-"))
_GROUP = re.compile(r"\(([0-9A-Za-z]{1,4})\)")
_SECTION_KEY = re.compile(r"^(\d+)([a-z]*)(.*)$")
_TAIL_PART = re.compile(r"(\d+|[a-z]+|[^0-9a-z]+)")


def _tail_order(tail: str) -> tuple[tuple[int, int, str], ...]:
    parts: list[tuple[int, int, str]] = []
    for part in _TAIL_PART.findall(tail):
        if part.isdigit():
            parts.append((1, int(part), ""))
        elif part.isalpha():
            parts.append((2, len(part), part))
        else:
            parts.append((0, 0, part))
    return tuple(parts)


def _section_order(section: str) -> tuple[int, int, str, tuple[tuple[int, int, str], ...]] | None:
    match = _SECTION_KEY.match(section)
    return (int(match.group(1)), len(match.group(2)), match.group(2), _tail_order(match.group(3))) if match else None


def normalize_section(value: object) -> str:
    """Lowercase, trim, and collapse every dash spelling."""
    return str(value or "").strip().lower().translate(_DASHES)


def normalize_subsection(value: object) -> tuple[str, ...]:
    """Normalize a subsection query to a path of labels: ``"(a)(1)"`` -> ``("a", "1")``."""
    text = str(value or "").strip().translate(_DASHES)
    if not text:
        return ()
    groups = _GROUP.findall(text)
    if groups and "".join(f"({g})" for g in groups) == text.replace(" ", ""):
        return tuple(g.lower() for g in groups)
    return (text.strip("()").lower(),)


@dataclass(frozen=True)
class Successor:
    """One section a printed value names as the home of a former provision."""

    title: int
    section: str
    subsection: str | None
    status: str

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"undeclared status: {self.status!r}")
        if self.status not in _WITH_SUCCESSOR:
            raise ValueError(f"a successor cannot carry the status {self.status!r}")


@dataclass(frozen=True)
class DispositionRow:
    """One printed row: the parsed address, the successor, and the verbatim print text."""

    former_section: str
    former_subsection: str | None
    former_note: str | None
    successor: Successor | None
    status: str
    former_text: str
    new_text: str
    page: int
    column: str

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"undeclared status: {self.status!r}")
        if (self.successor is not None) != (self.status in _WITH_SUCCESSOR):
            raise ValueError("a row names a successor exactly when its status says it has one")


@dataclass(frozen=True)
class Disposition:
    """What a pinned disposition table says about one former ``(title, section[, subsection])``."""

    former_title: int
    former_section: str
    subsection: tuple[str, ...]
    verdict: str
    recodification: str | None
    successors: tuple[Successor, ...]
    rows: tuple[DispositionRow, ...]
    subsection_resolved: bool | None
    former_section_end: str | None = None
    members: tuple[Disposition, ...] = ()
    caveats: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.verdict not in VERDICTS:
            raise ValueError(f"undeclared verdict: {self.verdict!r}")
        if self.verdict == "not-in-table":
            if NOT_IN_TABLE_CAVEATS[0] not in self.caveats:
                raise ValueError("an absence from a table is only as wide as the table, and must say so")
            unknown = set(self.caveats) - set(NOT_IN_TABLE_CAVEATS) - _KNOWN_PRINT_DISCREPANCY_CAVEATS
            if unknown:
                raise ValueError(f"undeclared caveat(s) on not-in-table disposition: {sorted(unknown)}")
        elif self.caveats and not set(self.caveats).issubset(_KNOWN_PRINT_DISCREPANCY_CAVEATS):
            raise ValueError("only an absence from the table or a verified print discrepancy carries a caveat")
        if (self.verdict == "exists-as-recodified") != bool(self.successors):
            raise ValueError("a recodified verdict names its successors, and no other verdict names any")
        if self.verdict in ("not-in-table", "no-table-for-title") and self.rows:
            raise ValueError("a verdict that found no row cannot carry rows")
        if self.members and self.former_section_end is None:
            raise ValueError("only a span has members")
        if any(member.members or member.former_section_end is not None for member in self.members):
            raise ValueError("a span's member is one former section, and cannot itself be a span")
        if self.members and self.successors != tuple(
            dict.fromkeys(one for member in self.members for one in member.successors)
        ):
            raise ValueError("a span's successors are the union over its members, in member order")

    @property
    def answered(self) -> bool:
        """Whether the table said anything at all about this section."""
        return self.verdict not in ("not-in-table", "no-table-for-title")

    @property
    def covers(self) -> tuple[str, ...]:
        """The former sections this verdict speaks for, in print order."""
        return tuple(member.former_section for member in self.members) or (self.former_section,)


def _verify_pinned_parquet_bytes(directory: Path, name: str, expected: str) -> bytes:
    """Hash one pinned table, refusing loudly on drift. Returns its verified bytes."""
    path = directory / name
    data = path.read_bytes()
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    if digest != expected:
        raise ValueError(f"pinned disposition table drifted: {name}; expected={expected}, observed={digest}")
    return data


def _read_pinned_parquet_rows(directory: Path, name: str, expected: str) -> list[dict[str, Any]]:
    """Verify SHA-256 and read Parquet rows through DuckDB without importing pyarrow."""
    data = _verify_pinned_parquet_bytes(directory, name, expected)
    with TemporaryDirectory(prefix="spicy-regs-usc-disp-") as tmp_dir:
        admitted = Path(tmp_dir) / name
        admitted.write_bytes(data)
        with duckdb.connect() as con:
            cursor = con.execute("SELECT * FROM read_parquet(?)", [str(admitted)])
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


@dataclass(frozen=True)
class UscDispositionTables:
    """The pinned recodification tables, loaded from verified vendored directories."""

    directory: Path
    recodifications: tuple[Recodification, ...] | None = None

    @property
    def _catalog(self) -> tuple[Recodification, ...]:
        return RECODIFICATIONS if self.recodifications is None else self.recodifications

    @classmethod
    def from_directory(
        cls, directory: Path | str, *, recodifications: tuple[Recodification, ...] | None = None
    ) -> UscDispositionTables:
        """Bind to a flat directory carrying every pinned table, verifying every digest first."""
        tables = cls(directory=Path(directory), recodifications=recodifications)
        tables.verify()
        return tables

    @classmethod
    def from_vendored(cls) -> UscDispositionTables:
        """Bind to the SHA-256-pinned vendored disposition tables bundled with ``spicy_regs``."""
        return cls.from_directory(VENDORED_DISPOSITION_DIR)

    def verify(self) -> None:
        """Hash every pinned table at bind time and refuse duplicate recodification names."""
        seen_names: set[str] = set()
        for recodification in self._catalog:
            if recodification.name in seen_names:
                raise ValueError(f"duplicate recodification name: {recodification.name!r}")
            seen_names.add(recodification.name)
        _ = self._by_title
        for recodification in self._catalog:
            _verify_pinned_parquet_bytes(self.directory, recodification.table, recodification.digest)

    @cached_property
    def _by_title(self) -> Mapping[int, tuple[Recodification, ...]]:
        by_title: dict[int, list[Recodification]] = {}
        for recodification in self._catalog:
            for ft in recodification.effective_former_titles:
                if recodification not in by_title.setdefault(ft, []):
                    by_title[ft].append(recodification)
        return {ft: tuple(recs) for ft, recs in by_title.items()}

    @cached_property
    def _tagged_rows(self) -> Mapping[int, Mapping[str, tuple[tuple[str, DispositionRow], ...]]]:
        """``former title -> former section -> (recodification name, DispositionRow)``, never overwriting."""
        _ = self._by_title
        out: dict[int, dict[str, list[tuple[str, DispositionRow]]]] = {}
        for recodification in self._catalog:
            allowed_titles = frozenset(recodification.effective_former_titles)
            rows = _read_pinned_parquet_rows(self.directory, recodification.table, recodification.digest)
            for row in rows:
                ft = int(row["former_title"])
                if ft not in allowed_titles:
                    raise ValueError(
                        f"{recodification.table} states former title {ft}, outside {recodification.effective_former_titles}"
                    )
                successor = (
                    Successor(
                        title=int(row["new_title"]),
                        section=normalize_section(row["new_section"]),
                        subsection=row["new_subsection"],
                        status=str(row["status"]),
                    )
                    if row["status"] in _WITH_SUCCESSOR
                    else None
                )
                section = normalize_section(row["former_section"])
                out.setdefault(ft, {}).setdefault(section, []).append(
                    (
                        recodification.name,
                        DispositionRow(
                            former_section=section,
                            former_subsection=row["former_subsection"],
                            former_note=row["former_note"],
                            successor=successor,
                            status=str(row["status"]),
                            former_text=str(row["former_text"]),
                            new_text=str(row["new_text"]),
                            page=int(row["page"]),
                            column=str(row["column"]),
                        ),
                    )
                )
        return {
            ft: {sec: tuple(items) for sec, items in sections.items()}
            for ft, sections in out.items()
        }

    @cached_property
    def _rows(self) -> Mapping[int, Mapping[str, tuple[DispositionRow, ...]]]:
        """``former title -> former section -> its printed rows``, in print order."""
        return {
            ft: {sec: tuple(row for _, row in items) for sec, items in sections.items()}
            for ft, sections in self._tagged_rows.items()
        }

    def _recodifications_for(self, former_title: int, *, appendix: bool = False) -> tuple[Recodification, ...]:
        recods = self._by_title.get(former_title, ())
        if appendix:
            return tuple(r for r in recods if r.covers_appendix)
        return recods

    def recodification_metadata(self, name: str) -> Recodification | None:
        """Return metadata for a catalog recodification by name, if present."""
        for rec in self._catalog:
            if rec.name == name:
                return rec
        return _RECODIFICATION_BY_NAME.get(name)

    def former_sections(self, former_title: int, *, appendix: bool = False) -> frozenset[str]:
        """Every former section the pinned table(s) for that title (and appendix scope) list."""
        if not appendix:
            return frozenset(self._rows.get(former_title, {}))
        allowed = {r.name for r in self._recodifications_for(former_title, appendix=True)}
        if not allowed:
            return frozenset()
        return frozenset(
            sec
            for sec, items in self._tagged_rows.get(former_title, {}).items()
            if any(name in allowed for name, _ in items)
        )

    def sections_in_span(
        self, former_title: int, former_section: str, section_end: object, *, appendix: bool = False
    ) -> tuple[str, ...]:
        """Every former section the pinned table(s) list inside a stated span, in Code order."""
        low = _section_order(normalize_section(former_section))
        high = _section_order(normalize_section(section_end))
        if low is None or high is None or high < low:
            return ()
        matched = [
            (key, section)
            for section in self.former_sections(former_title, appendix=appendix)
            if (key := _section_order(section)) is not None and low <= key <= high
        ]
        matched.sort(key=lambda item: item[0])
        return tuple(section for _, section in matched)

    def disposition(
        self,
        former_title: int,
        former_section: str,
        subsection: object = None,
        *,
        section_end: object = None,
        appendix: bool = False,
    ) -> Disposition:
        """What became of one former section, of one subsection, or of a span."""
        section = normalize_section(former_section)
        wanted = normalize_subsection(subsection)
        end = normalize_section(section_end) or None
        recods = self._recodifications_for(former_title, appendix=appendix)
        if not recods:
            return Disposition(
                former_title=former_title,
                former_section=section,
                former_section_end=end,
                subsection=wanted,
                verdict="no-table-for-title",
                recodification=None,
                successors=(),
                rows=(),
                subsection_resolved=None,
            )

        checked_names = "+".join(r.name for r in recods)
        allowed_names = {r.name for r in recods}
        print_caveats = KNOWN_PRINT_DISCREPANCIES.get((former_title, section), ())

        if end is not None:
            members = tuple(
                self.disposition(
                    former_title, member, subsection if member == section else None, appendix=appendix
                )
                for member in self.sections_in_span(former_title, section, end, appendix=appendix)
            )
            if not members:
                return Disposition(
                    former_title=former_title,
                    former_section=section,
                    former_section_end=end,
                    subsection=wanted,
                    verdict="not-in-table",
                    recodification=checked_names,
                    successors=(),
                    rows=(),
                    subsection_resolved=None,
                    caveats=NOT_IN_TABLE_CAVEATS + print_caveats,
                )
            matched = tuple(row for member in members for row in member.rows)
            start = next((member for member in members if member.former_section == section), None)
            span_recod = "+".join(
                dict.fromkeys(
                    part
                    for member in members
                    if member.recodification is not None
                    for part in member.recodification.split("+")
                )
            )
            span_caveats = tuple(dict.fromkeys(c for member in members for c in member.caveats))
            return Disposition(
                former_title=former_title,
                former_section=section,
                former_section_end=end,
                subsection=wanted,
                verdict=_verdict_over(matched),
                recodification=span_recod,
                successors=tuple(dict.fromkeys(one for member in members for one in member.successors)),
                rows=matched,
                subsection_resolved=None if start is None else start.subsection_resolved,
                members=members,
                caveats=span_caveats,
            )

        tagged = tuple(
            pair
            for pair in self._tagged_rows.get(former_title, {}).get(section, ())
            if pair[0] in allowed_names
        )
        if not tagged:
            return Disposition(
                former_title=former_title,
                former_section=section,
                subsection=wanted,
                verdict="not-in-table",
                recodification=checked_names,
                successors=(),
                rows=(),
                subsection_resolved=None,
                caveats=NOT_IN_TABLE_CAVEATS + print_caveats,
            )

        resolved: bool | None = None
        matched_tagged = tagged
        if wanted:
            narrowed = tuple(pair for pair in tagged if _row_covers(pair[1], wanted))
            resolved = bool(narrowed)
            matched_tagged = narrowed or tagged

        matched = tuple(row for _, row in matched_tagged)
        answering_names = "+".join(dict.fromkeys(name for name, _ in matched_tagged))
        return Disposition(
            former_title=former_title,
            former_section=section,
            subsection=wanted,
            verdict=_verdict_over(matched),
            recodification=answering_names,
            successors=tuple(dict.fromkeys(row.successor for row in matched if row.successor is not None)),
            rows=matched,
            subsection_resolved=resolved,
            caveats=print_caveats,
        )


def _verdict_over(matched: tuple[DispositionRow, ...]) -> str:
    """The verdict a set of printed rows carries."""
    if any(row.successor is not None for row in matched):
        return "exists-as-recodified"
    if all(row.status == "repealed" for row in matched):
        return "repealed-no-successor"
    if all(row.status == "eliminated" for row in matched):
        return "eliminated-no-successor"
    return "stated-without-successor"


_INTRODUCTORY_MATTER = re.compile(
    r"\((?:matter|words|provisions?|text)\s+(?:before|preceding)\s+"
    r"(?:(?:subsec\.|par\.|subpar\.|cl\.|subsection|paragraph|subparagraph|clause)\s*)?"
    r"\([0-9a-zA-Z]{1,4}\)",
    re.IGNORECASE,
)


def _trailing_child_labels_after_paren(text: str, open_idx: int) -> frozenset[str]:
    """Child labels listed after a balanced parenthetical qualifier, e.g. ``), (1) to (5)``."""
    depth = 0
    end_idx = len(text)
    for idx in range(open_idx, len(text)):
        ch = text[idx]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end_idx = idx + 1
                break
    tail = text[end_idx:].translate(_DASHES)
    if "(" not in tail:
        return frozenset()
    found: set[str] = set()
    for m_rng in re.finditer(
        r"\(([0-9a-zA-Z]{1,4})\)\s*(?:to|through|-)\s*\(([0-9a-zA-Z]{1,4})\)", tail, re.IGNORECASE
    ):
        lo, hi = m_rng.group(1), m_rng.group(2)
        if lo.isdigit() and hi.isdigit() and int(lo) <= int(hi):
            found.update(str(n) for n in range(int(lo), int(hi) + 1))
        elif len(lo) == 1 and len(hi) == 1 and lo.isalpha() and hi.isalpha() and lo.isupper() == hi.isupper() and lo <= hi:
            found.update(chr(c).lower() for c in range(ord(lo), ord(hi) + 1))
    for grp in _GROUP.findall(tail):
        found.add(grp.lower())
    return frozenset(found)


def _introductory_excludes_child(
    row: DispositionRow, labels: tuple[str, ...], wanted: tuple[str, ...]
) -> bool:
    """Whether ``row`` is an introductory-matter row at ``len(labels)`` that excludes ``wanted``."""
    m = _INTRODUCTORY_MATTER.search(row.former_text)
    if m is None:
        return False
    chapeau_depth = len(_GROUP.findall(row.former_text[: m.start()]))
    if len(labels) != chapeau_depth:
        return False
    trailing = _trailing_child_labels_after_paren(row.former_text, m.start())
    return not (trailing and wanted[chapeau_depth] in trailing)


def _row_covers(row: DispositionRow, wanted: tuple[str, ...]) -> bool:
    """Whether a printed row speaks for the queried subsection."""
    if row.former_note is not None:
        return False
    printed = row.former_subsection
    if printed is None:
        return not _introductory_excludes_child(row, (), wanted)
    labels = tuple(g.lower() for g in _GROUP.findall(printed.split("-")[0]))
    if not labels:
        return False
    if "-" in printed:
        return wanted[0] == labels[0]
    if len(wanted) > len(labels) and _introductory_excludes_child(row, labels, wanted):
        return False
    return labels[: len(wanted)] == wanted[: len(labels)]


def disposition_table_digests() -> dict[str, str]:
    """Return ``{table_filename: sha256_pin}`` for all 9 vendored disposition tables."""
    return {rec.table: rec.digest for rec in RECODIFICATIONS}


@lru_cache(maxsize=1)
def load_disposition_tables() -> UscDispositionTables:
    """Load and cache the verified vendored ``UscDispositionTables`` singleton."""
    return UscDispositionTables.from_vendored()
