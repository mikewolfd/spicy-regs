"""Transforms, table builders, merge helpers and PDF-text entry points; import each from its own module.

This package re-exports nothing. Its former facade imported every builder eagerly, so importing any one transform
loaded 99 SpicyRegs modules (10 now, for ``regulations_receipts``), and every one of them entered the code a prepared
conversion's seal records, where an unrelated change to any builder refused the publication (ss-5tt8).
"""
