#!/usr/bin/env python3
"""Plan which superseded generations decision 36 deletes into a JSON plan and a summary table; --execute a reviewed one.

Usage: ``uv run --frozen python scripts/plan_generation_retention.py --output plan.json`` with the R2
credentials set. The rules live in ``spicy_regs.generation_retention``; this is only its command line.
"""

import sys

from spicy_regs.generation_retention import main

if __name__ == "__main__":
    sys.exit(main())
