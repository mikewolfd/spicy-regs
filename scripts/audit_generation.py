#!/usr/bin/env python3
"""Audit one published generation read-only into a machine-readable JSON report.

Usage: ``uv run --frozen python scripts/audit_generation.py --family laws --prior 43130abc --output report.json``.
The checks live in ``spicy_regs.generation_audit``; this is only its command line.
"""

import sys

from spicy_regs.generation_audit import main

if __name__ == "__main__":
    sys.exit(main())
