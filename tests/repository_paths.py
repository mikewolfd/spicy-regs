"""Sibling repository paths used by cross-product tests."""

from pathlib import Path

SPICY_REGS_ROOT = Path(__file__).resolve().parents[1]
WORK_ROOT = SPICY_REGS_ROOT.parent
REFSPEC_ROOT = WORK_ROOT / "RefSpec"
RULESPEC_ROOT = WORK_ROOT / "rulespec"
