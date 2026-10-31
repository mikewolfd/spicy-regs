"""RefSpec's U.S. Code reference tables, vendored byte for byte and queryable as SQL views.

The files live in ``reference/refspec/`` (see its README for provenance) and are never edited here. Each view
reads one file through ``read_parquet``; installing it refuses bytes whose sha256 is not the pinned one, and the
serving connection admits exactly these paths (:func:`reference_paths`).
"""

import hashlib
from functools import cache
from importlib.resources import files

from .sql_views import SQLView

#: Each view: (vendored path under ``reference/refspec``, sha256 RefSpec states for it, columns). The views share one
#: summary (:data:`MEANING`), as a relationship family's views do, so list_sources lists the family once.
_ORACLE = "usc-section-oracle/"
_TITLE = "U.S. Code title number."
_YEAR = "Annual edition year, 1994 to 2024."
_APPENDIX = "True for the title's appendix volume."
_STUB_BOUNDS = {"lo": "First section of the span, as printed.", "hi": "Last section of the span, as printed."}
REFERENCE_TABLES = {
    "usc_oracle_sections": (
        _ORACLE + "usc-oracle-sections.parquet",
        "bbb450afb00cc7a28e2fcab7943d47207e8c1899f1e83209bd92d893fe39daac",
        {"title": _TITLE, "section": "Section number as the release point prints it (case kept).",
         "status": "current, or the stub marker printed for a withdrawn section: repealed, transferred, omitted, "
                   "renumbered, vacant, reserved or unknown."}),
    "usc_oracle_ranges": (
        _ORACLE + "usc-oracle-ranges.parquet",
        "070226e36d324a15226bd84ce9f5e4a297b43ff83365dba37ed281d3186cd736",
        {"title": _TITLE, **_STUB_BOUNDS,
         "status": "The range stub's marker (repealed, omitted, transferred ...); the span is never expanded.",
         "raw": "The release point's identifier for the stub (/us/usc/t2/s44...46)."}),
    "usc_oracle_annual_sections": (
        _ORACLE + "usc-oracle-annual-sections.parquet",
        "4757327d2bbcd5258d0fc51f0658df7fc9999c21d5a40dbe0c2d466cf25e7323",
        {"year": _YEAR, "title": _TITLE, "appendix": _APPENDIX,
         "section": "Section number printed as live law that year; a bracketed stub, which the publisher mints no "
                    "identity, is left out."}),
    "usc_oracle_annual_ranges": (
        _ORACLE + "usc-oracle-annual-ranges.parquet",
        "c8cd96e1d13e4f429e1024f3a9fe25b8527036eae0adfd9ad29d9656522b43a3",
        {"year": _YEAR, "title": _TITLE, "appendix": _APPENDIX, **_STUB_BOUNDS}),
    "usc_oracle_subsections": (
        _ORACLE + "usc-oracle-subsections.parquet",
        "d7502945223eed92bdeb65e6a73cf52240febefe9814180675fc4df512cd08ed",
        {"title": _TITLE + " Non-appendix titles only.", "section": "Section number as printed.",
         "sub": "Subsection designation, (a) as a."}),
    "usc_oracle_chapters": (
        _ORACLE + "usc-oracle-chapters.parquet",
        "031ae7f3435f7ba2969bdf2c94b699170a63fe3183dd289ca23fa0ce45e90cb3",
        {"title": _TITLE, "chapter": "Chapter designation as printed."}),
    "usc_act_sections": (
        "usc-act-index/usc-act-sections.parquet",
        "9040d4460e90704aef911b4d4f704e9f432cebd7b58717f31f090a3008ca6159",
        {"table3_key": "OLRC's Table III search key for the enacting law (a pre-1957 date narrowed to its year: "
                       "1948:758).",
         "act_section": "Section of the Act as Table III states it; a record stating none is not here.",
         "usc_title": "U.S. Code title the Act section is classified to, as printed.",
         "usc_section": "U.S. Code section as printed, notes included (3701 nt).",
         "status": "Table III's status text (Rep., Elim., ...), NULL when none is stated.",
         "statutes_at_large_volume": "Statutes at Large volume of the classified text.",
         "statutes_at_large_page": "Statutes at Large page the classification begins on (a span narrowed to its "
                                   "first page)."}),
}

MEANING = ("RefSpec's vendored U.S. Code reference tables, byte-pinned (reference/refspec/README.md): what OLRC release "
           "point 119-102 prints (usc_oracle_sections, usc_oracle_ranges, usc_oracle_subsections, "
           "usc_oracle_chapters), what each annual edition 1994-2024 printed as live law (usc_oracle_annual_sections, "
           "usc_oracle_annual_ranges), and Table III's Act-section classifications at release point 119-73 "
           "(usc_act_sections). A section a table lacks may be in a later law or another edition.")


def _path(relative: str) -> str:
    return str(files("spicy_regs").joinpath("reference/refspec").joinpath(relative))


@cache
def reference_paths() -> tuple[str, ...]:
    """Each vendored file's path, once its bytes are shown to be the pinned ones."""
    paths = []
    for name, (relative, sha256, _columns) in REFERENCE_TABLES.items():
        path = _path(relative)
        with open(path, "rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != sha256:
                raise ValueError(f"{name}: vendored RefSpec bytes differ from their pin ({relative})")
        paths.append(path)
    return tuple(paths)


def _query(relative: str):
    def query(_publication) -> str:
        reference_paths()
        return f"SELECT * FROM read_parquet('{_path(relative)}')"
    return query


REFERENCE_VIEWS = tuple(
    SQLView(name, {}, _query(relative), MEANING, (), rule_version=f"refspec-sha256:{sha256[:12]}",
            role="reference", category="reference", column_descriptions=columns)
    for name, (relative, sha256, columns) in REFERENCE_TABLES.items()
)
