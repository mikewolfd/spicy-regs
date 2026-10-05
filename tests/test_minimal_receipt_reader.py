"""Native serving readers must not import the ETL transform facade."""

import os
from pathlib import Path
import subprocess
import sys


def test_receipt_selection_without_etl_imports():
    root = Path(__file__).parents[1]
    script = root / "deploy/cloudflare/runtime_readers.py"
    code = f"""
import importlib.abc, runpy, sys
class RejectETL(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'spicy_regs.transforms' or fullname.split('.')[0] in {{'boto3', 'polars', 'spicy_docs'}}:
            raise ModuleNotFoundError('ETL import forbidden: ' + fullname)
sys.meta_path.insert(0, RejectETL())
runpy.run_path({str(script)!r}, run_name='__main__')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "Native citation receipt selection passed" in result.stdout
