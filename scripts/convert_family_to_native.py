#!/usr/bin/env python3
"""Convert one old-shape rollup family to native subjects and receipts, once; ``--rollback`` restores its pointer.

Usage: ``uv run --frozen python scripts/convert_family_to_native.py amendments --allow amendments,treaties
--work DIR --expect-main SHA --expect-spicy-docs VERSION`` (add ``--publish`` to move the pointer). The steps and
checks live in ``spicy_regs.native_conversion``; this is only its command line. Runbook:
``docs/native-family-conversion.md``.
"""

import sys

from spicy_regs.native_conversion import main

if __name__ == "__main__":
    sys.exit(main())
